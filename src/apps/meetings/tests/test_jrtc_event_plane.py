"""Focused contracts for Synq's authoritative JRTC event consumer."""

from __future__ import annotations

import asyncio
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import uuid4

from asgiref.sync import async_to_sync
from broka import AcknowledgementMode, Envelope
from django.test import SimpleTestCase, TestCase
from jrtc.messaging import DEFAULT_PHYSICAL_ROUTE, JANUS_EVENT_ROUTES

from apps.meetings.jrtc.config import JrtcEventConfig
from apps.meetings.jrtc.events.consumer import (
    JrtcEventConsumer,
    subscription_options,
)
from apps.meetings.jrtc.events.handlers import (
    DjangoJanusEventReconciler,
    SocketDispatch,
    SocketIoJanusEventEmitter,
)
from apps.meetings.jrtc.events.idempotency import DjangoEventReceiptStore
from apps.meetings.jrtc.events.schemas import JanusBrokerEvent, event_from_delivery
from apps.meetings.jrtc.ids import janus_event_to_wire
from apps.meetings.management.commands.run_jrtc_events import run_until_stopped
from apps.meetings.models import JrtcEventReceipt, JrtcEventReceiptStatus


def _config(
    *,
    engine: str = "memory",
    engine_options: dict[str, object] | None = None,
) -> JrtcEventConfig:
    return JrtcEventConfig(
        enabled=True,
        engine=engine,  # type: ignore[arg-type]
        physical_route=DEFAULT_PHYSICAL_ROUTE,
        publish_workers=1,
        publish_queue_capacity=8,
        publish_admission_timeout=0.1,
        publish_timeout=1.0,
        drain_timeout=1.0,
        consumer_concurrency=2,
        consumer_capacity=16,
        consumer_group="synq-tests",
        consumer_name="consumer-tests",
        engine_options=engine_options or {},
    )


def _event(
    *,
    payload: dict[str, object] | None = None,
    event_id=None,
) -> JanusBrokerEvent:
    selected_payload = {
        "janus": "event",
        "session_id": 101,
        "sender": 202,
        "plugindata": {"plugin": "janus.plugin.videoroom", "data": {}},
        **(payload or {}),
    }
    return JanusBrokerEvent(
        event_id=event_id or uuid4(),
        event_type="janus.event",
        janus_type="event",
        payload=selected_payload,
        session_id=101,
        sender=202,
        delivery_attempt=1,
    )


def _delivery(payload: dict[str, object], *, event_type: str = "janus.event"):
    return SimpleNamespace(
        envelope=Envelope.create(payload, type=event_type),
        attempt=1,
    )


class JrtcEventSchemaTests(SimpleTestCase):
    """Validate logical routing, immutable payload thawing, and strict IDs."""

    def test_delivery_uses_envelope_type_and_recursively_thaws_payload(self) -> None:
        delivery = _delivery(
            {
                "janus": "event",
                "session_id": 9_194_204_203_876_831,
                "sender": 8_888_888_888_888_888,
                "plugindata": {
                    "plugin": "janus.plugin.videoroom",
                    "data": {"publishers": [{"id": 77}]},
                },
            }
        )
        self.assertIsInstance(delivery.envelope.payload, MappingProxyType)

        event = event_from_delivery(delivery)

        self.assertEqual(event.event_type, "janus.event")
        self.assertEqual(event.session_id, 9_194_204_203_876_831)
        self.assertEqual(event.sender, 8_888_888_888_888_888)
        self.assertIsInstance(event.payload["plugindata"], dict)
        self.assertIsInstance(
            event.payload["plugindata"]["data"]["publishers"],
            list,
        )

    def test_numeric_strings_and_booleans_are_not_internal_janus_ids(self) -> None:
        for field_name, invalid in (
            ("session_id", "101"),
            ("session_id", True),
            ("sender", "202"),
            ("sender", True),
        ):
            with self.subTest(field=field_name, invalid=invalid):
                payload = {
                    "janus": "event",
                    "session_id": 101,
                    "sender": 202,
                    "plugindata": {
                        "plugin": "janus.plugin.videoroom",
                        "data": {},
                    },
                }
                payload[field_name] = invalid
                with self.assertRaises(TypeError):
                    event_from_delivery(_delivery(payload))

    def test_timeout_allows_no_sender_but_still_requires_a_session_id(self) -> None:
        event = event_from_delivery(
            _delivery(
                {"janus": "timeout", "session_id": 101},
                event_type="janus.timeout",
            )
        )
        self.assertEqual(event.session_id, 101)
        self.assertIsNone(event.sender)

    def test_envelope_and_payload_logical_types_must_match(self) -> None:
        with self.assertRaisesRegex(ValueError, "does not match"):
            event_from_delivery(
                _delivery(
                    {
                        "janus": "hangup",
                        "session_id": 101,
                        "sender": 202,
                    },
                    event_type="janus.media",
                )
            )

    def test_only_browser_boundary_stringifies_janus_ids(self) -> None:
        payload = {
            "janus": "event",
            "session_id": 9_194_204_203_876_831,
            "sender": 8_888_888_888_888_888,
            "plugindata": {
                "plugin": "janus.plugin.videoroom",
                "data": {
                    "publishers": [{"id": 7_488_603_522_389_459}],
                },
            },
        }

        wire = janus_event_to_wire(payload)

        self.assertIsInstance(payload["session_id"], int)
        self.assertEqual(wire["session_id"], "9194204203876831")
        self.assertEqual(wire["sender"], "8888888888888888")
        self.assertEqual(
            wire["plugindata"]["data"]["publishers"][0]["id"],
            "7488603522389459",
        )


class JrtcEventReceiptTests(TestCase):
    """Prove duplicates cannot repeat durable application side effects."""

    def test_duplicate_envelope_runs_operation_once(self) -> None:
        store = DjangoEventReceiptStore()
        event = _event()
        operation = Mock(return_value=("dispatch",))

        first = store.process_once(event, operation)
        second = store.process_once(event, operation)

        self.assertFalse(first.duplicate)
        self.assertTrue(second.duplicate)
        operation.assert_called_once_with()
        receipt = JrtcEventReceipt.objects.get(event_id=event.event_id)
        self.assertEqual(receipt.status, JrtcEventReceiptStatus.PROCESSED)
        self.assertEqual(receipt.duplicate_count, 1)
        self.assertEqual(receipt.delivery_attempts, 2)

    def test_failed_receipt_can_be_retried_without_leaking_event_payload(self) -> None:
        store = DjangoEventReceiptStore()
        event = _event(payload={"jsep": {"type": "offer", "sdp": "secret-sdp"}})

        with self.assertRaises(RuntimeError):
            store.process_once(event, Mock(side_effect=RuntimeError("do not store me")))

        failed = JrtcEventReceipt.objects.get(event_id=event.event_id)
        self.assertEqual(failed.status, JrtcEventReceiptStatus.FAILED)
        self.assertEqual(failed.last_error, "RuntimeError")
        self.assertNotIn("secret-sdp", str(failed.metadata))

        retried = store.process_once(event, Mock(return_value=()))
        failed.refresh_from_db()
        self.assertFalse(retried.duplicate)
        self.assertEqual(failed.status, JrtcEventReceiptStatus.PROCESSED)


class JrtcEventReconcilerTests(SimpleTestCase):
    """Preserve metadata reconciliation while suppressing duplicate JSEP."""

    def test_transaction_jsep_reconciles_before_browser_suppression(self) -> None:
        order: list[str] = []
        callback = Mock(side_effect=lambda *_args: order.append("callback"))
        reconciler = DjangoJanusEventReconciler(snapshot_callback=callback)
        media_handle = SimpleNamespace(janus_handle_id=202)
        event = _event(
            payload={
                "transaction": "transaction-1",
                "jsep": {"type": "answer", "sdp": "v=0\r\n"},
                "plugindata": {
                    "plugin": "janus.plugin.videoroom",
                    "data": {"publishers": [{"id": 303}]},
                },
            }
        )
        with (
            patch.object(
                reconciler,
                "_correlated_handles",
                return_value=[media_handle],
            ),
            patch.object(
                reconciler,
                "_persist_latest_snapshot",
                side_effect=lambda *_args: order.append("persist"),
            ),
            patch.object(reconciler, "_socket_dispatch") as socket_dispatch,
        ):
            dispatches = reconciler.reconcile(event)

        self.assertEqual(order, ["persist", "callback"])
        self.assertEqual(dispatches, ())
        socket_dispatch.assert_not_called()

    def test_transaction_without_jsep_is_still_forwarded(self) -> None:
        callback = Mock()
        reconciler = DjangoJanusEventReconciler(snapshot_callback=callback)
        media_handle = SimpleNamespace(janus_handle_id=202)
        expected = SocketDispatch(
            payload={"event": {}},
            socket_ids=("socket-1",),
            session_room="session:meeting-1",
        )
        event = _event(payload={"transaction": "transaction-1"})
        with (
            patch.object(
                reconciler,
                "_correlated_handles",
                return_value=[media_handle],
            ),
            patch.object(reconciler, "_persist_latest_snapshot"),
            patch.object(
                reconciler,
                "_socket_dispatch",
                return_value=expected,
            ),
        ):
            dispatches = reconciler.reconcile(event)

        self.assertEqual(dispatches, (expected,))
        callback.assert_called_once_with(media_handle, event.payload)

    def test_dispatch_table_comes_from_jrtc_logical_routes(self) -> None:
        reconciler = DjangoJanusEventReconciler(snapshot_callback=Mock())
        self.assertEqual(
            reconciler.logical_event_types,
            frozenset(JANUS_EVENT_ROUTES.values()),
        )

    def test_handle_lookup_uses_the_complete_janus_correlation_tuple(self) -> None:
        from apps.meetings.models import ParticipantMediaHandle

        expected_handle = SimpleNamespace(pk="handle-1")
        manager = MagicMock()
        queryset = manager.select_for_update.return_value.select_related.return_value
        filtered = queryset.filter.return_value
        filtered.__getitem__.return_value = [expected_handle]
        event = _event()

        with patch.object(ParticipantMediaHandle, "objects", manager):
            result = DjangoJanusEventReconciler._correlated_handles(event)

        self.assertEqual(result, [expected_handle])
        queryset.filter.assert_called_once_with(
            janus_session_id=101,
            janus_handle_id=202,
        )

    def test_socket_boundary_keeps_domain_uuid_separate_from_janus_ids(self) -> None:
        from apps.meetings.models import RealtimeConnectionStatus

        meeting_session_id = uuid4()
        room_id = uuid4()
        participant_id = uuid4()
        profile_id = uuid4()
        participant = SimpleNamespace(
            pk=participant_id,
            session_id=meeting_session_id,
            room_id=room_id,
            profile_id=profile_id,
            # Handle events must never discover recipients by walking every
            # socket associated with the logical participant.
            connections=SimpleNamespace(),
        )
        connection_id = uuid4()
        media_handle = SimpleNamespace(
            pk=uuid4(),
            _meta=SimpleNamespace(label_lower="meetings.participantmediahandle"),
            participant=participant,
            handle_type="subscriber",
            connection_id=connection_id,
            connection=SimpleNamespace(
                socket_id="socket-owner",
                status=RealtimeConnectionStatus.ACTIVE,
            ),
            opaque_id=None,
        )
        event = _event(
            payload={
                "plugindata": {
                    "plugin": "janus.plugin.videoroom",
                    "data": {"streams": [{"feed_id": 303}]},
                }
            }
        )

        dispatch = DjangoJanusEventReconciler._socket_dispatch(
            media_handle,
            event,
            original_handle_id=202,
        )

        self.assertEqual(dispatch.payload["session_id"], str(meeting_session_id))
        self.assertEqual(dispatch.socket_ids, ("socket-owner",))
        self.assertNotIn("socket_ids", dispatch.payload)
        self.assertEqual(dispatch.payload["connection_id"], str(connection_id))
        self.assertEqual(dispatch.payload["plugin_id"], "202")
        self.assertEqual(dispatch.payload["event"]["session_id"], "101")
        self.assertEqual(dispatch.payload["event"]["sender"], "202")
        self.assertEqual(
            dispatch.payload["event"]["plugindata"]["data"]["streams"][0][
                "feed_id"
            ],
            "303",
        )

    def test_private_negotiation_events_only_target_the_handle_owner(self) -> None:
        from apps.meetings.models import RealtimeConnectionStatus

        participant = SimpleNamespace(
            pk=uuid4(),
            session_id=uuid4(),
            room_id=uuid4(),
            profile_id=uuid4(),
            # This intentionally has no queryset API. Any attempt to fan out
            # through participant connections makes the regression fail.
            connections=SimpleNamespace(),
        )
        media_handle = SimpleNamespace(
            pk=uuid4(),
            _meta=SimpleNamespace(label_lower="meetings.participantmediahandle"),
            participant=participant,
            handle_type="subscriber",
            connection_id=uuid4(),
            connection=SimpleNamespace(
                socket_id="socket-owner",
                status=RealtimeConnectionStatus.SUBSCRIBED,
            ),
            opaque_id=None,
        )

        for private_payload in (
            {"jsep": {"type": "offer", "sdp": "v=0\r\n"}},
            {"janus": "trickle", "candidate": {"candidate": "candidate:1"}},
        ):
            with self.subTest(payload=private_payload):
                dispatch = DjangoJanusEventReconciler._socket_dispatch(
                    media_handle,
                    _event(payload=private_payload),
                    original_handle_id=202,
                )

                self.assertEqual(dispatch.socket_ids, ("socket-owner",))
                self.assertNotIn("socket_ids", dispatch.payload)

    def test_private_handle_event_has_no_target_when_owner_is_inactive(self) -> None:
        from apps.meetings.models import RealtimeConnectionStatus

        participant = SimpleNamespace(
            pk=uuid4(),
            session_id=uuid4(),
            room_id=uuid4(),
            profile_id=uuid4(),
            connections=SimpleNamespace(),
        )
        media_handle = SimpleNamespace(
            pk=uuid4(),
            _meta=SimpleNamespace(label_lower="meetings.participantmediahandle"),
            participant=participant,
            handle_type="publisher",
            connection_id=uuid4(),
            connection=SimpleNamespace(
                socket_id="socket-disconnected",
                status=RealtimeConnectionStatus.DISCONNECTED,
            ),
            opaque_id=None,
        )

        dispatch = DjangoJanusEventReconciler._socket_dispatch(
            media_handle,
            _event(payload={"jsep": {"type": "offer", "sdp": "v=0\r\n"}}),
            original_handle_id=202,
        )

        self.assertEqual(dispatch.socket_ids, ())
        self.assertNotIn("socket_ids", dispatch.payload)


class SocketIoJrtcEventEmitterTests(SimpleTestCase):
    """Keep the established meeting namespace and Janus event name."""

    def test_emits_to_each_correlated_socket(self) -> None:
        server = SimpleNamespace(emit=AsyncMock())
        emitter = SocketIoJanusEventEmitter(lambda: server)
        dispatch = SocketDispatch(
            payload={"event": {"janus": "webrtcup"}},
            socket_ids=("socket-1", "socket-2"),
            session_room="session:meeting-1",
        )

        async_to_sync(emitter.emit_many)((dispatch,))

        self.assertEqual(server.emit.await_count, 2)
        self.assertEqual(
            [call.kwargs["to"] for call in server.emit.await_args_list],
            ["socket-1", "socket-2"],
        )
        self.assertTrue(
            all(
                call.args[0] == "janus_event"
                and call.kwargs["namespace"] == "/meetings"
                for call in server.emit.await_args_list
            )
        )

    def test_private_handle_event_never_falls_back_to_a_session_room(self) -> None:
        server = SimpleNamespace(emit=AsyncMock())
        emitter = SocketIoJanusEventEmitter(lambda: server)
        dispatch = SocketDispatch(
            payload={
                "event": {
                    "janus": "event",
                    "jsep": {"type": "offer", "sdp": "private-sdp"},
                }
            },
            socket_ids=(),
            session_room="session:meeting-1",
        )

        async_to_sync(emitter.emit_many)((dispatch,))

        server.emit.assert_not_awaited()


class JrtcEventConsumerTests(SimpleTestCase):
    """Exercise lifecycle and manual ACK ordering without a live broker."""

    def test_start_subscribes_once_to_shared_physical_route_with_manual_ack(self) -> None:
        broker = SimpleNamespace(
            startup=AsyncMock(),
            shutdown=AsyncMock(),
            subscribe=AsyncMock(
                return_value=SimpleNamespace(close=AsyncMock())
            ),
        )
        consumer = JrtcEventConsumer(broker, _config(), dispatcher=MagicMock())

        async_to_sync(consumer.start)()

        broker.startup.assert_awaited_once_with()
        broker.subscribe.assert_awaited_once()
        route, handler = broker.subscribe.await_args.args
        options = broker.subscribe.await_args.kwargs["options"]
        self.assertEqual(route, DEFAULT_PHYSICAL_ROUTE)
        self.assertEqual(handler, consumer.handle_delivery)
        self.assertEqual(options.acknowledgement_mode, AcknowledgementMode.MANUAL)
        self.assertFalse(options.durable)
        async_to_sync(consumer.stop)()

    def test_delivery_is_acked_only_after_awaited_dispatch(self) -> None:
        order: list[str] = []

        async def dispatch(_event) -> None:
            order.append("dispatch")

        async def ack() -> None:
            order.append("ack")

        dispatcher = SimpleNamespace(dispatch=AsyncMock(side_effect=dispatch))
        consumer = JrtcEventConsumer(
            MagicMock(),
            _config(),
            dispatcher=dispatcher,
        )
        delivery = _delivery(
            {
                "janus": "event",
                "session_id": 101,
                "sender": 202,
                "plugindata": {
                    "plugin": "janus.plugin.videoroom",
                    "data": {},
                },
            }
        )
        delivery.ack = AsyncMock(side_effect=ack)

        async_to_sync(consumer.handle_delivery)(delivery)

        self.assertEqual(order, ["dispatch", "ack"])

    def test_failed_dispatch_is_not_acknowledged(self) -> None:
        dispatcher = SimpleNamespace(
            dispatch=AsyncMock(side_effect=RuntimeError("failed durable work"))
        )
        consumer = JrtcEventConsumer(
            MagicMock(),
            _config(),
            dispatcher=dispatcher,
        )
        delivery = _delivery(
            {
                "janus": "webrtcup",
                "session_id": 101,
                "sender": 202,
            },
            event_type="janus.webrtcup",
        )
        delivery.ack = AsyncMock()

        with self.assertRaises(RuntimeError):
            async_to_sync(consumer.handle_delivery)(delivery)

        delivery.ack.assert_not_awaited()

    def test_stop_closes_subscription_before_broker(self) -> None:
        order: list[str] = []

        async def close() -> None:
            order.append("subscription")

        async def shutdown() -> None:
            order.append("broker")

        subscription = SimpleNamespace(close=AsyncMock(side_effect=close))
        broker = SimpleNamespace(
            startup=AsyncMock(),
            shutdown=AsyncMock(side_effect=shutdown),
            subscribe=AsyncMock(return_value=subscription),
        )
        consumer = JrtcEventConsumer(broker, _config(), dispatcher=MagicMock())
        async_to_sync(consumer.start)()

        async_to_sync(consumer.stop)()

        self.assertEqual(order, ["subscription", "broker"])
        self.assertEqual(consumer.state, JrtcEventConsumer.STOPPED)

    def test_durable_options_match_backend_capabilities(self) -> None:
        streams = subscription_options(
            _config(engine="redis", engine_options={"mode": "streams"})
        )
        pubsub = subscription_options(
            _config(engine="redis", engine_options={"mode": "pubsub"})
        )
        rabbit = subscription_options(_config(engine="rabbitmq"))

        self.assertTrue(streams.durable)
        self.assertEqual(streams.consumer_group, "synq-tests")
        self.assertFalse(pubsub.durable)
        self.assertIsNone(pubsub.consumer_group)
        self.assertTrue(rabbit.durable)
        self.assertIsNone(rabbit.consumer_group)


class RunJrtcEventsCommandTests(SimpleTestCase):
    """Verify the command's long-lived loop always performs graceful cleanup."""

    def test_pre_signaled_run_starts_and_stops_consumer(self) -> None:
        order: list[str] = []

        async def scenario() -> None:
            stop_event = asyncio.Event()
            stop_event.set()
            consumer = SimpleNamespace(
                start=AsyncMock(side_effect=lambda: order.append("start")),
                stop=AsyncMock(side_effect=lambda: order.append("stop")),
            )
            await run_until_stopped(consumer, stop_event=stop_event)

        async_to_sync(scenario)()

        self.assertEqual(order, ["start", "stop"])
