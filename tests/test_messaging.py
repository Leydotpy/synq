from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest
from broka import DeliveryMode
from broka.observability.metrics import InMemoryMetrics

from janus_api.messaging import (
    JanusEventPublisher,
    JanusResponseDispatcher,
    create_broker,
)
from janus_api.models.response import (
    AckResponse,
    ErrorResponse,
    EventResponse,
    JanusError,
    MediaEventResponse,
    SuccessResponse,
)
from janus_api.transport.websocket import WebsocketTransportClient


def _event_response() -> EventResponse:
    return EventResponse.model_validate(
        {
            "janus": "event",
            "session_id": 10,
            "sender": 20,
            "plugindata": {
                "plugin": "janus.plugin.videoroom",
                "data": {"videoroom": "event", "configured": "ok"},
            },
            "jsep": {"type": "answer", "sdp": "v=0\r\ns=janus\r\n"},
        }
    )


class BlockingBroker:
    def __init__(self, *, release_after: int = 1) -> None:
        self.release_after = release_after
        self.release = asyncio.Event()
        self.started = asyncio.Event()
        self.published: list[tuple[Any, tuple[Any, ...], dict[str, Any]]] = []
        self.active = 0
        self.max_active = 0
        self.startup_calls = 0
        self.shutdown_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def shutdown(self) -> None:
        self.shutdown_calls += 1

    async def publish(self, message: Any, *args: Any, **kwargs: Any) -> Any:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.published.append((message, args, kwargs))
        if self.active >= self.release_after:
            self.started.set()
        try:
            await self.release.wait()
        finally:
            self.active -= 1
        return SimpleNamespace(accepted=True)


class FlakyStartupBroker(BlockingBroker):
    def __init__(self) -> None:
        super().__init__()
        self.fail_startup = True

    async def startup(self) -> None:
        self.startup_calls += 1
        if self.fail_startup:
            raise RuntimeError("injected startup failure")


class TimeoutThenSuccessBroker:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.first_started = asyncio.Event()
        self.second_completed = asyncio.Event()

    async def publish(self, _message: Any, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            self.first_started.set()
            await asyncio.Event().wait()
        self.second_completed.set()
        return SimpleNamespace(accepted=True)


class NotifyingBoundedSemaphore(asyncio.BoundedSemaphore):
    def __init__(self, value: int) -> None:
        super().__init__(value)
        self.acquired = asyncio.Event()

    async def acquire(self) -> bool:
        acquired = await super().acquire()
        self.acquired.set()
        return acquired


class ObservableStopPublisher(JanusEventPublisher):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.stop_wait_entered = asyncio.Event()

    async def _wait_for_queues(self, deadline: float | None) -> None:
        self.stop_wait_entered.set()
        await super()._wait_for_queues(deadline)


class RecordingPublisher:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, Any, Any]] = []

    async def admit(
        self,
        response: Any,
        *,
        session_id: Any = None,
        sender: Any = None,
    ) -> bool:
        self.calls.append((response, session_id, sender))
        return True


class FailingMetrics(InMemoryMetrics):
    def increment(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("metrics unavailable")

    def set_gauge(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("metrics unavailable")

    def observe(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("metrics unavailable")


async def test_publisher_runs_multiple_workers_concurrently_and_drains() -> None:
    broker = BlockingBroker(release_after=4)
    publisher = JanusEventPublisher(
        broker,
        workers=4,
        queue_capacity=16,
        admission_timeout=0.01,
        owns_broker=False,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
    )
    await publisher.start()

    sender_by_shard: dict[int, int] = {}
    sender = 1
    while len(sender_by_shard) < publisher.worker_count:
        sender_by_shard.setdefault(publisher._shard(f"1:{sender}"), sender)
        sender += 1
    ordered_senders = list(sender_by_shard.values())
    responses = [
        MediaEventResponse(
            janus="media",
            session_id=1,
            sender=ordered_senders[index % publisher.worker_count],
            type="video",
            receiving=bool(index % 2),
        )
        for index in range(8)
    ]
    accepted = await asyncio.gather(
        *(publisher.admit(response, session_id=1, sender=response.sender) for response in responses)
    )

    assert accepted == [True] * len(responses)
    await asyncio.wait_for(broker.started.wait(), timeout=1)
    assert broker.max_active == 4

    broker.release.set()
    await publisher.stop(drain=True, timeout=1)

    assert len(broker.published) == len(responses)
    assert broker.startup_calls == 0
    assert broker.shutdown_calls == 0
    assert all(call[2]["route"] == "janus.media" for call in broker.published)
    assert all(
        call[2]["options"].delivery_mode is DeliveryMode.AT_LEAST_ONCE for call in broker.published
    )


async def test_publisher_deadline_releases_a_shard_after_a_stalled_backend() -> None:
    broker = TimeoutThenSuccessBroker()
    publisher = JanusEventPublisher(
        broker,  # type: ignore[arg-type]
        workers=1,
        queue_capacity=2,
        admission_timeout=0.1,
        publish_timeout=0.01,
        owns_broker=False,
    )
    await publisher.start()
    response = _event_response()

    assert await publisher.admit(response)
    assert await publisher.admit(response)
    await asyncio.wait_for(broker.first_started.wait(), timeout=1)
    await asyncio.wait_for(broker.second_completed.wait(), timeout=1)
    await publisher.stop(drain=True, timeout=1)

    assert publisher.queue_depth == 0
    assert len(broker.calls) == 2
    assert all(call["options"].timeout == pytest.approx(0.01) for call in broker.calls)


async def test_publisher_rejects_admission_when_its_bounded_queue_is_full() -> None:
    broker = BlockingBroker()
    publisher = JanusEventPublisher(
        broker,
        workers=1,
        queue_capacity=2,
        admission_timeout=0.01,
        owns_broker=False,
    )
    await publisher.start()

    first = MediaEventResponse(janus="media", session_id=1, sender=1, type="audio", receiving=True)
    second = first.model_copy(update={"sender": 2})
    third = first.model_copy(update={"sender": 3})

    assert await publisher.admit(first, session_id=1, sender=1)
    await asyncio.wait_for(broker.started.wait(), timeout=1)
    assert await publisher.admit(second, session_id=1, sender=2)
    assert not await publisher.admit(third, session_id=1, sender=3)

    broker.release.set()
    await publisher.stop(drain=True, timeout=1)
    assert len(broker.published) == 2


async def test_owned_broker_lifecycle_is_started_and_stopped_once() -> None:
    broker = BlockingBroker()
    broker.release.set()
    publisher = JanusEventPublisher(broker, owns_broker=True)

    await publisher.start()
    await publisher.start()
    await publisher.stop()
    await publisher.stop()

    assert broker.startup_calls == 1
    assert broker.shutdown_calls == 1


async def test_cancelled_admission_releases_a_reserved_capacity_slot() -> None:
    broker = BlockingBroker()
    broker.release.set()
    publisher = JanusEventPublisher(
        broker,
        workers=1,
        queue_capacity=1,
        admission_timeout=1,
        owns_broker=False,
    )
    semaphore = NotifyingBoundedSemaphore(1)
    publisher._slots = semaphore
    response = MediaEventResponse(
        janus="media",
        session_id=1,
        sender=1,
        type="audio",
        receiving=True,
    )
    await publisher.start()
    await publisher._admission_lock.acquire()
    admission = asyncio.create_task(publisher.admit(response))
    try:
        await asyncio.wait_for(semaphore.acquired.wait(), timeout=1)
        admission.cancel()
        with pytest.raises(asyncio.CancelledError):
            await admission
    finally:
        publisher._admission_lock.release()

    assert publisher.queue_depth == 0
    assert await publisher.admit(response)
    await publisher.stop(drain=True, timeout=1)


async def test_owned_broker_startup_failure_rolls_back_and_can_retry() -> None:
    broker = FlakyStartupBroker()
    publisher = JanusEventPublisher(broker, owns_broker=True)

    with pytest.raises(RuntimeError, match="injected startup failure"):
        await publisher.start()

    assert not publisher.running
    assert publisher.queue_depth == 0
    assert broker.startup_calls == 1
    assert broker.shutdown_calls == 1

    broker.fail_startup = False
    broker.release.set()
    await publisher.start()
    assert publisher.running
    await publisher.stop(timeout=1)

    assert broker.startup_calls == 2
    assert broker.shutdown_calls == 2


async def test_stop_timeout_restores_depth_and_capacity_for_restart() -> None:
    broker = BlockingBroker()
    publisher = JanusEventPublisher(
        broker,
        workers=1,
        queue_capacity=1,
        owns_broker=False,
    )
    response = MediaEventResponse(
        janus="media",
        session_id=1,
        sender=1,
        type="video",
        receiving=True,
    )
    await publisher.start()
    assert await publisher.admit(response)
    await asyncio.wait_for(broker.started.wait(), timeout=1)

    await publisher.stop(drain=True, timeout=0.01)

    assert not publisher.running
    assert publisher.queue_depth == 0
    broker.release.set()
    await publisher.start()
    assert await publisher.admit(response)
    await publisher.stop(drain=True, timeout=1)
    assert publisher.queue_depth == 0


async def test_cancelled_stop_restores_depth_and_capacity_for_restart() -> None:
    broker = BlockingBroker()
    publisher = ObservableStopPublisher(
        broker,
        workers=1,
        queue_capacity=1,
        owns_broker=False,
    )
    response = MediaEventResponse(
        janus="media",
        session_id=1,
        sender=1,
        type="video",
        receiving=False,
    )
    await publisher.start()
    assert await publisher.admit(response)
    await asyncio.wait_for(broker.started.wait(), timeout=1)

    stopping = asyncio.create_task(publisher.stop(drain=True))
    await asyncio.wait_for(publisher.stop_wait_entered.wait(), timeout=1)
    stopping.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopping

    assert not publisher.running
    assert publisher.queue_depth == 0
    broker.release.set()
    await publisher.start()
    assert await publisher.admit(response)
    await publisher.stop(drain=True, timeout=1)
    assert publisher.queue_depth == 0


async def test_real_broka_memory_engine_delivers_one_whole_jsep_envelope() -> None:
    broker = create_broker(engine="memory", physical_route="janus.events")
    publisher = JanusEventPublisher(
        broker,
        physical_route="janus.events",
        workers=2,
        owns_broker=True,
    )
    received: list[Any] = []
    delivered = asyncio.Event()

    async def handle(delivery: Any) -> None:
        received.append(delivery)
        delivered.set()

    await publisher.start()
    subscription = await broker.subscribe("janus.events", handle)
    try:
        response = _event_response()
        assert await publisher.admit(response, session_id=10, sender=20)
        await asyncio.wait_for(delivered.wait(), timeout=1)

        assert len(received) == 1
        delivery = received[0]
        assert delivery.route == "janus.events"
        assert delivery.envelope.type == "janus.event"
        assert delivery.message["janus"] == "event"
        assert delivery.message["jsep"] == {
            "type": "answer",
            "sdp": "v=0\r\ns=janus\r\n",
        }
    finally:
        await subscription.close()
        await publisher.stop(drain=True, timeout=1)


async def test_dispio_dispatcher_routes_control_responses_and_publishes_webrtc() -> None:
    publisher = RecordingPublisher()
    transaction_calls: list[Any] = []
    acknowledgement_calls: list[Any] = []
    error_calls: list[Any] = []

    async def on_transaction(response: Any) -> None:
        transaction_calls.append(response)

    async def on_ack(response: Any) -> None:
        acknowledgement_calls.append(response)

    async def on_error(response: Any) -> None:
        error_calls.append(response)

    dispatcher = JanusResponseDispatcher(
        publisher=publisher,
        on_transaction=on_transaction,
        on_ack=on_ack,
        on_error=on_error,
    )
    success = SuccessResponse(janus="success", transaction="success-1")
    acknowledgement = AckResponse(janus="ack", transaction="ack-1")
    error = ErrorResponse(
        janus="error",
        transaction="error-1",
        error=JanusError(code=490, reason="test error"),
    )
    event = _event_response()

    await dispatcher.dispatch(success)
    await dispatcher.dispatch(acknowledgement)
    await dispatcher.dispatch(error)
    assert await dispatcher.dispatch(event, session_id=10, sender=20)

    assert transaction_calls == [success, event]
    assert acknowledgement_calls == [acknowledgement]
    assert error_calls == [error]
    assert publisher.calls == [(event, 10, 20)]
    assert publisher.calls[0][0].jsep == event.jsep
    assert "sdp" not in dispatcher.dispatchable_events


async def test_metrics_provider_failures_do_not_interrupt_dispatch_or_publication() -> None:
    publisher = RecordingPublisher()
    dispatcher = JanusResponseDispatcher(
        publisher=publisher,
        metrics=FailingMetrics(),
    )

    assert await dispatcher.dispatch(_event_response(), session_id=10, sender=20)
    assert len(publisher.calls) == 1


class IncomingMessages:
    def __init__(self, *messages: str) -> None:
        self._messages = iter(messages)

    def __aiter__(self) -> IncomingMessages:
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._messages)
        except StopIteration:
            raise StopAsyncIteration from None


async def test_websocket_transport_does_not_split_jsep_into_a_second_event() -> None:
    publisher = RecordingPublisher()
    transport = WebsocketTransportClient(event_publisher=publisher)
    response = _event_response()
    raw_message = response.model_dump_json(by_alias=True, exclude_none=True)

    await transport._process_message(IncomingMessages(json.dumps(json.loads(raw_message))))

    assert len(publisher.calls) == 1
    published, session_id, sender = publisher.calls[0]
    assert published.janus == "event"
    assert published.jsep is not None
    assert published.jsep.sdp == response.jsep.sdp
    assert (session_id, sender) == (10, 20)


async def test_http_transport_does_not_split_jsep_into_a_second_event() -> None:
    pytest.importorskip("httpx")
    from janus_api.transport.http import HttpTransportClient

    publisher = RecordingPublisher()
    transport = HttpTransportClient(event_publisher=publisher)
    response = _event_response()

    await transport._handle_response(response)

    assert publisher.calls == [(response, 10, 20)]
    assert publisher.calls[0][0].jsep is response.jsep
