"""Deterministic contracts for the split Janus Core and VideoRoom packages.

These tests deliberately stop at the package and application compatibility
boundaries.  They must never require a running Janus gateway.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.metadata
import re
import threading
import tomllib
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings
from janus_api.models.base import Jsep
from janus_api.models.request import PluginMessageRequest
from janus_videoroom_plugin import (
    PublisherConfigureRequest,
    PublisherJoinAndConfigureRequest,
    PublisherPublishRequest,
    StreamDescription,
    SubscribeTarget,
    SubscriberJoinRequest,
    SubscriberStartRequest,
    SubscriberUpdateRequest,
    UnsubscribeTarget,
    VideoRoomCreateRequest,
    VideoRoomCreated,
    VideoRoomDestroyRequest,
    VideoRoomKickRequest,
    VideoRoomPlugin,
    VideoRoomReply,
)

from apps.meetings.models import (
    MeetingSession,
    ParticipantConnection,
    ParticipantMediaHandle,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.realtime.namespace import MeetingNamespace
from apps.meetings.services.janus import (
    JanusProcessRuntime,
    NativeJanusIdVideoRoomPlugin,
    call_video_room_management_method,
    ensure_bound_plugin_attached,
    serialize_janus_response,
)
from apps.meetings.services.signaling import (
    _match_participant_for_publisher,
    _subscriber_is_joined,
    _with_subscriber_joined_state,
)
from core.hooks.janus import plugin_callback_factory
from core.models.fields.janus import (
    BoundPluginHandle,
    JanusPluginField,
    VideoRoomPublisherPluginField,
    VideoRoomSubscriberPluginField,
)


class NativeJanusIdAdapterContractTests(SimpleTestCase):
    """Keep Janus' numeric handle identity intact at every app boundary."""

    def test_native_integer_handle_survives_into_plugin_message_request(self) -> None:
        session_id = 7_488_603_522_389_459
        handle_id = 9_194_204_203_876_831
        plugin = NativeJanusIdVideoRoomPlugin(
            session=SimpleNamespace(id=session_id),
        )
        plugin._plugin_id = handle_id

        request = PluginMessageRequest(
            session_id=session_id,
            handle_id=plugin.id,
            body={"request": "exists", "room": 1},
        )
        payload = request.model_dump(mode="json", by_alias=True, exclude_none=True)

        self.assertEqual(payload["handle_id"], handle_id)
        self.assertIsInstance(payload["handle_id"], int)

    def test_management_helper_constructs_native_id_adapter(self) -> None:
        constructed: list[Any] = []

        class AdapterProbe:
            def __init__(self, *, session: Any) -> None:
                self.session = session
                self.attached = False
                constructed.append(self)

            async def attach(self) -> None:
                self.attached = True

            async def probe(self, value: str) -> str:
                if not self.attached:
                    raise AssertionError("management method ran before attach")
                return f"native:{value}"

            async def detach(self) -> None:
                self.attached = False

        session = object()
        with (
            patch(
                "apps.meetings.services.janus.resolve_janus_session",
                return_value=session,
            ),
            patch(
                "apps.meetings.services.janus.NativeJanusIdVideoRoomPlugin",
                AdapterProbe,
            ),
            patch(
                "apps.meetings.services.janus.janus_runtime.run",
                side_effect=asyncio.run,
            ),
        ):
            result = call_video_room_management_method(
                SimpleNamespace(),
                "probe",
                "room-7",
            )

        self.assertEqual(result, "native:room-7")
        self.assertEqual(len(constructed), 1)
        self.assertIs(constructed[0].session, session)

    def test_meeting_handle_fields_resolve_native_id_adapter(self) -> None:
        for model, field_name in (
            (MeetingSession, "control_handle_id"),
            (ParticipantMediaHandle, "janus_handle_id"),
        ):
            with self.subTest(model=model.__name__, field=field_name):
                field = model._meta.get_field(field_name)
                self.assertIs(field.plugin_class, NativeJanusIdVideoRoomPlugin)


SERVER_ROOT = Path(__file__).resolve().parents[4]


def _direct_dependencies() -> dict[str, str]:
    """Return canonical direct dependency names and their declarations."""

    with (SERVER_ROOT / "pyproject.toml").open("rb") as pyproject_file:
        dependencies = tomllib.load(pyproject_file)["project"]["dependencies"]

    result: dict[str, str] = {}
    for declaration in dependencies:
        match = re.match(r"\s*([A-Za-z0-9_.-]+)", declaration)
        if match is None:  # pragma: no cover - invalid declarations fail packaging first
            raise AssertionError(f"Cannot parse dependency declaration: {declaration!r}")
        name = match.group(1).lower().replace("_", "-")
        result[name] = declaration
    return result


class JanusPackageBoundaryTests(SimpleTestCase):
    """Lock the application to the intentionally split v3 package boundary."""

    def test_direct_dependencies_use_only_core_and_videoroom_distributions(self) -> None:
        dependencies = _direct_dependencies()

        self.assertNotIn("janus-api", dependencies)
        self.assertNotIn("janus-api-streaming", dependencies)
        self.assertNotIn("streaming-utils", dependencies)
        self.assertEqual(
            dependencies["janus-api-core"],
            "janus-api-core[asgi]==3.0.0",
        )
        self.assertEqual(
            dependencies["janus-videoroom-plugin"],
            "janus-videoroom-plugin==3.0.0",
        )

    def test_core_and_videoroom_packages_import_at_declared_versions(self) -> None:
        core_module = importlib.import_module("janus_api")
        videoroom_module = importlib.import_module("janus_videoroom_plugin")

        self.assertTrue(hasattr(core_module, "Plugin"))
        self.assertIs(videoroom_module.VideoRoomPlugin, VideoRoomPlugin)
        self.assertEqual(importlib.metadata.version("janus-api-core"), "3.0.0")
        self.assertEqual(
            importlib.metadata.version("janus-videoroom-plugin"),
            "3.0.0",
        )


class VideoRoomSerializationContractTests(SimpleTestCase):
    """Exercise the stable response envelope exposed to the rest of the app."""

    def test_typed_reply_without_raw_response_builds_legacy_envelope(self) -> None:
        reply = VideoRoomReply(
            data=VideoRoomCreated(
                videoroom="created",
                room="room-42",
                permanent=False,
            ),
            jsep=Jsep(type="answer", sdp="v=0\r\n"),
            transaction="transaction-42",
        )

        serialized = serialize_janus_response(reply)

        self.assertEqual(
            serialized["plugindata"],
            {
                "plugin": "janus.plugin.videoroom",
                "data": {
                    "videoroom": "created",
                    "room": "room-42",
                    "permanent": False,
                },
            },
        )
        self.assertEqual(serialized["jsep"], {"type": "answer", "sdp": "v=0\r\n"})
        self.assertEqual(serialized["transaction"], "transaction-42")

    def test_typed_reply_preserves_raw_core_mapping_when_available(self) -> None:
        raw = {
            "janus": "event",
            "sender": "handle-7",
            "plugindata": {
                "plugin": "janus.plugin.videoroom",
                "data": {"videoroom": "created", "room": "room-7"},
            },
        }
        reply = VideoRoomReply(
            data=VideoRoomCreated(
                videoroom="created",
                room="room-7",
                permanent=False,
            ),
            raw=raw,
        )

        self.assertEqual(serialize_janus_response(reply), raw)


class LegacyPluginFieldContractTests(SimpleTestCase):
    """Keep historic migration values while resolving the installed plugin."""

    def test_legacy_role_identifiers_resolve_to_videoroom(self) -> None:
        for field_class, legacy_identifier in (
            (VideoRoomPublisherPluginField, "publisher"),
            (VideoRoomSubscriberPluginField, "subscriber"),
        ):
            with self.subTest(identifier=legacy_identifier):
                field = field_class()

                self.assertEqual(field.resolve_identifier(SimpleNamespace()), "videoroom")
                _name, _path, _args, kwargs = field.deconstruct()
                self.assertEqual(kwargs["identifier"], legacy_identifier)
                self.assertEqual(kwargs["plugin_class"], "janus_api.Plugin")


class _RegisteredPlugin:
    def __init__(self, plugin_id: str) -> None:
        self.id = plugin_id
        self.attach_calls = 0

    def attach(self, *, opaque_id: str | None = None) -> None:
        del opaque_id
        self.attach_calls += 1


class _ReadySession:
    def __init__(self, plugin: _RegisteredPlugin) -> None:
        self.ready = True
        self.plugins = {plugin.id: plugin}


class RegisteredHandleContractTests(SimpleTestCase):
    """Protect process-local handle reuse from duplicate attachment."""

    def test_registered_plugin_is_reused_and_treated_as_attached(self) -> None:
        registered_plugin = _RegisteredPlugin("handle-17")
        session = _ReadySession(registered_plugin)

        def fail_if_constructed(**_kwargs: Any) -> Any:
            raise AssertionError("a registered handle must be reused")

        field = JanusPluginField(
            identifier="publisher",
            plugin_class=fail_if_constructed,
            janus_getter=lambda _instance, _field: session,
        )
        bound_handle = BoundPluginHandle(
            instance=SimpleNamespace(),
            field=field,
            session=session,
            raw_id="handle-17",
        )

        self.assertIs(bound_handle.plugin, registered_plugin)
        self.assertTrue(bound_handle.is_attached)
        self.assertIs(ensure_bound_plugin_attached(bound_handle), bound_handle)
        self.assertEqual(registered_plugin.attach_calls, 0)


class _AsyncBoundHandle:
    def __init__(self) -> None:
        self.is_attached = False
        self.attach_persist: bool | None = None
        self.attach_thread: int | None = None
        self.sync_thread: int | None = None

    def attach(self, *, persist: bool, opaque_id: str | None = None) -> Any:
        del opaque_id
        self.attach_persist = persist

        async def attach_remote() -> None:
            self.attach_thread = threading.get_ident()

        return attach_remote()

    def sync_from_plugin(self, *, persist: bool, update_fields: list[str]) -> None:
        self.sync_thread = threading.get_ident()
        self.sync_persist = persist
        self.sync_update_fields = update_fields


class SynchronousPersistenceBoundaryTests(SimpleTestCase):
    """Keep ORM persistence outside the event loop that owns Janus I/O."""

    def test_attachment_persists_only_after_remote_awaitable_returns(self) -> None:
        handle = _AsyncBoundHandle()
        caller_thread = threading.get_ident()

        with patch(
            "apps.meetings.services.janus.resolve_maybe_awaitable",
            side_effect=asyncio.run,
        ):
            result = ensure_bound_plugin_attached(
                handle,
                persist=True,
                update_fields=["janus_handle_id", "updated_at"],
            )

        self.assertIs(result, handle)
        self.assertFalse(handle.attach_persist)
        self.assertEqual(handle.sync_thread, caller_thread)
        self.assertTrue(handle.sync_persist)
        self.assertEqual(
            handle.sync_update_fields,
            ["janus_handle_id", "updated_at"],
        )


class _ManagementPlugin:
    last_instance: "_ManagementPlugin | None" = None

    def __init__(self, *, session: object) -> None:
        self.session = session
        self.attached = False
        self.detach_calls = 0
        type(self).last_instance = self

    async def attach(self) -> None:
        self.attached = True

    async def probe(self, value: str) -> str:
        self.assert_attached = self.attached
        return f"result:{value}"

    async def detach(self) -> None:
        self.detach_calls += 1
        raise RuntimeError("synthetic detach failure")


class ManagementHandleContractTests(SimpleTestCase):
    """A successful room command must not be retried only because cleanup failed."""

    def test_successful_command_survives_temporary_handle_detach_failure(self) -> None:
        with (
            patch(
                "apps.meetings.services.janus.resolve_janus_session",
                return_value=object(),
            ),
            patch(
                "apps.meetings.services.janus.NativeJanusIdVideoRoomPlugin",
                _ManagementPlugin,
            ),
            patch(
                "apps.meetings.services.janus.janus_runtime.run",
                side_effect=asyncio.run,
            ),
            patch("apps.meetings.services.janus.logger.exception"),
        ):
            result = call_video_room_management_method(
                SimpleNamespace(),
                "probe",
                "room-7",
            )

        plugin = _ManagementPlugin.last_instance
        self.assertEqual(result, "result:room-7")
        self.assertIsNotNone(plugin)
        self.assertTrue(plugin.assert_attached)
        self.assertEqual(plugin.detach_calls, 1)


class TypedVideoRoomRequestTests(SimpleTestCase):
    """Validate the typed payloads used by room and media signaling flows."""

    def test_room_create_and_destroy_requests(self) -> None:
        create_request = VideoRoomCreateRequest(
            room="meeting-room-1",
            description="Planning room",
            publishers=12,
            bitrate=1_024_000,
            audiocodec="opus",
            videocodec="vp8",
            notify_joining=True,
        )
        destroy_request = VideoRoomDestroyRequest(
            room=create_request.room,
            secret="room-secret",
        )
        kick_request = VideoRoomKickRequest(
            room=create_request.room,
            id="publisher-7",
            secret="room-secret",
        )

        create_payload = create_request.model_dump(mode="json", exclude_none=True)
        destroy_payload = destroy_request.model_dump(mode="json", exclude_none=True)
        self.assertEqual(create_payload["request"], "create")
        self.assertEqual(create_payload["room"], "meeting-room-1")
        self.assertEqual(create_payload["publishers"], 12)
        self.assertEqual(destroy_payload, {
            "request": "destroy",
            "room": "meeting-room-1",
            "secret": "room-secret",
        })
        self.assertEqual(
            kick_request.model_dump(mode="json", exclude_none=True),
            {
                "request": "kick",
                "room": "meeting-room-1",
                "id": "publisher-7",
                "secret": "room-secret",
            },
        )

    def test_publisher_join_publish_and_configure_requests(self) -> None:
        descriptions = [
            StreamDescription(mid="0", description="microphone"),
            StreamDescription(mid="1", description="camera"),
        ]
        join_request = PublisherJoinAndConfigureRequest(
            room="meeting-room-1",
            display="Ada",
            metadata={"participant_id": "participant-1"},
            descriptions=descriptions,
        )
        publish_request = PublisherPublishRequest(descriptions=descriptions)
        configure_request = PublisherConfigureRequest(
            bitrate=768_000,
            descriptions=descriptions,
        )

        join_payload = join_request.model_dump(mode="json", exclude_none=True)
        publish_payload = publish_request.model_dump(mode="json", exclude_none=True)
        configure_payload = configure_request.model_dump(mode="json", exclude_none=True)
        self.assertEqual(join_payload["request"], "joinandconfigure")
        self.assertEqual(join_payload["ptype"], "publisher")
        self.assertEqual(join_payload["descriptions"][1], {
            "mid": "1",
            "description": "camera",
        })
        self.assertEqual(publish_payload["request"], "publish")
        self.assertEqual(configure_payload["request"], "configure")
        self.assertEqual(configure_payload["bitrate"], 768_000)

    def test_subscriber_join_update_and_start_requests(self) -> None:
        target = SubscribeTarget(
            feed="publisher-1",
            mid="video-mid",
            crossrefid="publisher-1:video-mid",
        )
        join_request = SubscriberJoinRequest(
            room="meeting-room-1",
            private_id="private-1",
            streams=[target],
            use_msid=True,
            autoupdate=True,
        )
        update_request = SubscriberUpdateRequest(
            subscribe=[SubscribeTarget(feed="publisher-2", mid="audio-mid")],
            unsubscribe=[
                UnsubscribeTarget(
                    feed="publisher-1",
                    mid="video-mid",
                    sub_mid="subscriber-video-mid",
                )
            ],
        )
        start_request = SubscriberStartRequest()

        join_payload = join_request.model_dump(mode="json", exclude_none=True)
        update_payload = update_request.model_dump(mode="json", exclude_none=True)
        start_payload = start_request.model_dump(mode="json", exclude_none=True)
        self.assertEqual(join_payload["request"], "join")
        self.assertEqual(join_payload["ptype"], "subscriber")
        self.assertEqual(join_payload["streams"][0]["feed"], "publisher-1")
        self.assertEqual(update_payload["request"], "update")
        self.assertEqual(update_payload["subscribe"][0]["feed"], "publisher-2")
        self.assertEqual(
            update_payload["unsubscribe"][0]["sub_mid"],
            "subscriber-video-mid",
        )
        self.assertEqual(start_payload, {"request": "start"})


class _FakeSessionManager:
    def __init__(self, session: object) -> None:
        self.session_value = session
        self.requested_keys: list[str | int | None] = []

    def get_session(self, *, key: str | int | None = None) -> object:
        self.requested_keys.append(key)
        return self.session_value


class PersistentLoopBridgeContractTests(SimpleTestCase):
    """Prove synchronous callers execute work on the Janus owner's loop."""

    @override_settings(JANUS_SYNC_CALL_TIMEOUT=1, JANUS_SHUTDOWN_TIMEOUT=1)
    def test_bound_manager_session_and_awaitable_use_owner_thread(self) -> None:
        runtime = JanusProcessRuntime()
        expected_session = object()
        manager = _FakeSessionManager(expected_session)
        owner_started = threading.Event()
        owner_state: dict[str, Any] = {}

        def own_loop() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            owner_state["loop"] = loop
            owner_state["thread_id"] = threading.get_ident()
            runtime._claim_start(thread=threading.current_thread())
            runtime._bind(
                loop=loop,
                manager=manager,  # type: ignore[arg-type]
                lease=object(),
                owner_thread_id=threading.get_ident(),
                thread=threading.current_thread(),
            )
            owner_started.set()
            try:
                loop.run_forever()
            finally:
                runtime._begin_stop(manager)  # type: ignore[arg-type]
                runtime._finish_stop(manager)  # type: ignore[arg-type]
                asyncio.set_event_loop(None)
                loop.close()

        owner_thread = threading.Thread(target=own_loop, name="janus-test-loop")
        owner_thread.start()
        self.assertTrue(owner_started.wait(timeout=2), "owner loop did not start")

        async def probe_owner() -> tuple[int, int]:
            return threading.get_ident(), id(asyncio.get_running_loop())

        try:
            self.assertIs(runtime.session(key="meeting-42"), expected_session)
            owner_thread_id, owner_loop_id = runtime.run(probe_owner())
            self.assertEqual(owner_thread_id, owner_state["thread_id"])
            self.assertEqual(owner_loop_id, id(owner_state["loop"]))
            self.assertNotEqual(owner_thread_id, threading.get_ident())
            self.assertEqual(manager.requested_keys, ["meeting-42"])
        finally:
            owner_state["loop"].call_soon_threadsafe(owner_state["loop"].stop)
            owner_thread.join(timeout=2)

        self.assertFalse(owner_thread.is_alive())
        self.assertIsNone(runtime.manager)


class SubscriberRoleStateContractTests(SimpleTestCase):
    """Keep subscriber role membership separate from the current target set."""

    def test_joined_marker_survives_an_empty_subscription_set(self) -> None:
        state = _with_subscriber_joined_state(
            {"plugindata": {"data": {"videoroom": "updated", "streams": []}}},
            joined=True,
        )
        handle = SimpleNamespace(
            janus_state=state,
            lifecycle_state="attached",
        )

        self.assertTrue(_subscriber_is_joined(handle, set()))

        handle.janus_state = _with_subscriber_joined_state(state, joined=False)
        self.assertFalse(_subscriber_is_joined(handle, set()))


class SocketSignalingAuthorizationTests(SimpleTestCase):
    """Reject stale sockets and participants that are no longer present."""

    def test_removed_participant_cannot_signal_over_an_open_socket(self) -> None:
        participant = SimpleNamespace(
            session_id="session-7",
            status=ParticipantStatus.REMOVED,
        )
        connection = SimpleNamespace(
            participant=participant,
            status=RealtimeConnectionStatus.ACTIVE,
        )
        namespace = MeetingNamespace("/meetings")

        with patch.object(
            ParticipantConnection.objects,
            "select_related",
        ) as select_related:
            select_related.return_value.get.return_value = connection
            with self.assertRaisesRegex(ValueError, "active admitted participant"):
                namespace._get_participant_for_session_socket("session-7", "socket-7")

    def test_active_participant_on_active_connection_can_signal(self) -> None:
        participant = SimpleNamespace(
            session_id="session-7",
            status=ParticipantStatus.ACTIVE,
        )
        connection = SimpleNamespace(
            participant=participant,
            status=RealtimeConnectionStatus.ACTIVE,
        )
        namespace = MeetingNamespace("/meetings")

        with patch.object(
            ParticipantConnection.objects,
            "select_related",
        ) as select_related:
            select_related.return_value.get.return_value = connection
            resolved = namespace._get_participant_for_session_socket(
                "session-7",
                "socket-7",
            )

        self.assertIs(resolved, participant)


class _ParticipantMatches:
    def __init__(self, participants: list[SimpleNamespace]) -> None:
        self.participants = participants

    def present(self) -> "_ParticipantMatches":
        return self

    def filter(self, **criteria: Any) -> "_ParticipantMatches":
        return _ParticipantMatches(
            [
                participant
                for participant in self.participants
                if all(str(getattr(participant, field)) == str(value) for field, value in criteria.items())
            ]
        )

    def first(self) -> SimpleNamespace | None:
        return self.participants[0] if self.participants else None

    def __getitem__(self, item: slice) -> list[SimpleNamespace]:
        return self.participants[item]


class PublisherCorrelationTests(SimpleTestCase):
    """Prefer validated participant metadata and never guess ambiguous names."""

    def test_valid_metadata_resolves_before_duplicate_display_fallback(self) -> None:
        participant_one = SimpleNamespace(
            pk="participant-1",
            janus_publisher_id="",
            display_name="Alex",
        )
        participant_two = SimpleNamespace(
            pk="participant-2",
            janus_publisher_id="",
            display_name="Alex",
        )
        session = SimpleNamespace(
            pk="session-7",
            room_id="room-7",
            participants=_ParticipantMatches([participant_one, participant_two]),
        )

        resolved = _match_participant_for_publisher(
            session=session,
            publisher_id="publisher-7",
            display_name="Alex",
            metadata={
                "participant_id": "participant-2",
                "session_id": "session-7",
                "room_id": "room-7",
            },
        )

        self.assertIs(resolved, participant_two)

    def test_duplicate_display_without_metadata_is_not_guessed(self) -> None:
        participants = [
            SimpleNamespace(
                pk=f"participant-{index}",
                janus_publisher_id="",
                display_name="Alex",
            )
            for index in (1, 2)
        ]
        session = SimpleNamespace(
            pk="session-7",
            room_id="room-7",
            participants=_ParticipantMatches(participants),
        )

        resolved = _match_participant_for_publisher(
            session=session,
            publisher_id="publisher-7",
            display_name="Alex",
        )

        self.assertIsNone(resolved)

    def test_mismatched_metadata_cannot_fall_back_to_display_name(self) -> None:
        participant = SimpleNamespace(
            pk="participant-1",
            janus_publisher_id="",
            display_name="Alex",
        )
        session = SimpleNamespace(
            pk="session-7",
            room_id="room-7",
            participants=_ParticipantMatches([participant]),
        )

        resolved = _match_participant_for_publisher(
            session=session,
            publisher_id="publisher-7",
            display_name="Alex",
            metadata={
                "participant_id": "participant-1",
                "session_id": "another-session",
                "room_id": "room-7",
            },
        )

        self.assertIsNone(resolved)


class _CallbackField:
    name = "janus_handle_id"
    plugin_attr = "handle"

    @staticmethod
    def resolve_identifier(_instance: object) -> str:
        return "videoroom"

    @staticmethod
    def get_stored_value(_instance: object) -> None:
        return None


class TransactionalCallbackContractTests(SimpleTestCase):
    """Prevent one transactional SDP response from negotiating twice."""

    def test_transactional_jsep_is_not_broadcast_after_ack_path_handles_it(self) -> None:
        instance = SimpleNamespace(
            pk=None,
            janus_state={},
            _meta=SimpleNamespace(label_lower="meetings.fake"),
        )
        callback = plugin_callback_factory(instance, _CallbackField(), None)

        with patch("core.hooks.janus.dispatch_janus_event") as dispatch:
            callback(
                {
                    "janus": "event",
                    "transaction": "transaction-7",
                    "jsep": {"type": "offer", "sdp": "v=0\r\n"},
                }
            )

        dispatch.assert_not_called()

    def test_pushed_jsep_without_transaction_is_still_broadcast(self) -> None:
        instance = SimpleNamespace(
            pk=None,
            janus_state={},
            _meta=SimpleNamespace(label_lower="meetings.fake"),
        )
        callback = plugin_callback_factory(instance, _CallbackField(), None)

        with patch("core.hooks.janus.dispatch_janus_event") as dispatch:
            callback(
                {
                    "janus": "event",
                    "jsep": {"type": "offer", "sdp": "v=0\r\n"},
                }
            )

        dispatch.assert_called_once()
