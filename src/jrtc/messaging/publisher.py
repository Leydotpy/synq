"""Bounded, ordered asynchronous publication of Janus WebRTC events."""

from __future__ import annotations

import asyncio
import hashlib
import math
import time
from collections.abc import Awaitable
from contextlib import suppress
from dataclasses import dataclass

from broka import Broker, DeliveryMode, Destination, MetricsProvider, PublishOptions
from logvista import VisualLogger, get_logger

from jrtc.messaging.constants import (
    ADMISSION_TOTAL,
    DEFAULT_PHYSICAL_ROUTE,
    DROPPED_TOTAL,
    JANUS_EVENT_ROUTES,
    JANUS_LOGICAL_PATTERN,
    PUBLISH_FAILURES_TOTAL,
    PUBLISH_LATENCY_SECONDS,
    PUBLISHED_TOTAL,
    QUEUE_DEPTH,
    QUEUE_LATENCY_SECONDS,
)
from jrtc.messaging.metrics import LogVistaMetrics
from jrtc.models import JanusResponse

type JanusIdentifier = str | int


@dataclass(frozen=True, slots=True)
class _QueuedEvent:
    route: str
    payload: dict[str, object]
    ordering_key: str
    admitted_at: float


_STOP = object()


class JanusEventPublisher:
    """Decouple transport receive loops from a Broka backend.

    Admission waits for at most ``admission_timeout``. Accepted items consume a
    slot in one global bounded capacity and are assigned to a deterministic
    worker shard by session/sender, preserving order for that key. Backend
    failures are contained in workers and never escape :meth:`admit`.
    """

    def __init__(
        self,
        broker: Broker,
        *,
        physical_route: str | None = None,
        workers: int = 4,
        queue_capacity: int = 1024,
        admission_timeout: float = 0.05,
        publish_timeout: float = 5.0,
        delivery_mode: DeliveryMode | str | None = None,
        owns_broker: bool = True,
        metrics: MetricsProvider | None = None,
        logger: VisualLogger | None = None,
    ) -> None:
        if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
            raise ValueError("workers must be a positive integer")
        if (
            isinstance(queue_capacity, bool)
            or not isinstance(queue_capacity, int)
            or queue_capacity < 1
        ):
            raise ValueError("queue_capacity must be a positive integer")
        if not math.isfinite(admission_timeout) or admission_timeout <= 0:
            raise ValueError("admission_timeout must be finite and greater than zero")
        if not math.isfinite(publish_timeout) or publish_timeout <= 0:
            raise ValueError("publish_timeout must be finite and greater than zero")
        if physical_route is not None and (
            not isinstance(physical_route, str) or not physical_route.strip()
        ):
            raise ValueError("physical_route must be a non-empty string or None")

        self.broker = broker
        requested_route = None if physical_route is None else physical_route.strip()
        self.worker_count = workers
        self.queue_capacity = queue_capacity
        self.admission_timeout = float(admission_timeout)
        self.publish_timeout = float(publish_timeout)
        self.owns_broker = bool(owns_broker)
        self.logger = logger or get_logger("jrtc.messaging.publisher")
        broker_metrics = getattr(broker, "metrics", None)
        self.metrics = metrics or broker_metrics or LogVistaMetrics(self.logger)
        self.physical_route = self._ensure_route_mapping(requested_route)
        self.delivery_mode = (
            DeliveryMode(delivery_mode)
            if delivery_mode is not None
            else self._default_delivery_mode()
        )

        self._queues: tuple[asyncio.Queue[_QueuedEvent | object], ...] = tuple(
            asyncio.Queue() for _ in range(workers)
        )
        self._slots = asyncio.BoundedSemaphore(queue_capacity)
        self._tasks: tuple[asyncio.Task[None], ...] = ()
        self._lifecycle_lock = asyncio.Lock()
        self._admission_lock = asyncio.Lock()
        self._accepting = False
        self._started = False
        self._depth = 0
        with suppress(Exception):
            self.metrics.set_gauge(QUEUE_DEPTH, 0)

    @property
    def running(self) -> bool:
        """Return whether the publisher currently accepts events."""

        return self._started and self._accepting

    @property
    def queue_depth(self) -> int:
        """Return the number of admitted items not yet completed."""

        return self._depth

    async def start(self) -> None:
        """Start the owned broker, then the ordered publisher workers."""

        async with self._lifecycle_lock:
            if self._started:
                return
            broker_start_attempted = False
            tasks: list[asyncio.Task[None]] = []
            try:
                if self.owns_broker:
                    broker_start_attempted = True
                    await self.broker.startup()
                for index, queue in enumerate(self._queues):
                    tasks.append(
                        asyncio.create_task(
                            self._worker(index, queue),
                            name=f"janus-event-publisher-{index}",
                        )
                    )
                self._tasks = tuple(tasks)
                self._started = True
                async with self._admission_lock:
                    self._accepting = True
            except BaseException:
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                self._tasks = ()
                self._started = False
                self._accepting = False
                if broker_start_attempted:
                    with suppress(Exception):
                        await self.broker.shutdown()
                raise
            self.logger.debug(
                "Publisher lifecycle",
                "Janus event publisher started",
                context={
                    "delivery_mode": self.delivery_mode.value,
                    "queue_capacity": self.queue_capacity,
                    "workers": self.worker_count,
                },
            )

    async def stop(
        self,
        *,
        drain: bool = True,
        timeout: float | None = None,
    ) -> None:
        """Stop admission, optionally drain, and shut down an owned broker.

        A finite timeout cancels remaining worker activity and accounts queued
        items as dropped. Shutdown remains best-effort and idempotent.
        """

        if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout must be finite and greater than zero")
        async with self._lifecycle_lock:
            if not self._started:
                return
            # Serialize the admission boundary: an item is either fully queued
            # before draining starts or observes the stopped state and is
            # rejected. No item can be placed behind a worker stop marker.
            async with self._admission_lock:
                self._accepting = False
            deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
            timed_out = False
            try:
                if drain:
                    await self._wait_for_queues(deadline)
                else:
                    self._discard_queued("shutdown")
                for queue in self._queues:
                    queue.put_nowait(_STOP)
                await self._wait_for_tasks(deadline)
            except TimeoutError:
                timed_out = True
                for task in self._tasks:
                    task.cancel()
                await asyncio.gather(*self._tasks, return_exceptions=True)
                self._discard_queued("shutdown-timeout")
            except BaseException:
                for task in self._tasks:
                    task.cancel()
                await asyncio.gather(*self._tasks, return_exceptions=True)
                self._discard_queued("shutdown-cancelled")
                raise
            finally:
                self._tasks = ()
                self._started = False
                self._accepting = False
                if self.owns_broker:
                    try:
                        await self.broker.shutdown()
                    except Exception as exc:
                        with suppress(Exception):
                            self.metrics.increment(
                                PUBLISH_FAILURES_TOTAL,
                                labels={"result": "shutdown"},
                            )
                        self.logger.error(
                            "Broker shutdown failed",
                            "Broka failed during Janus publisher cleanup",
                            context={"error_type": type(exc).__name__},
                            exc_info=exc,
                        )
                self.logger.debug(
                    "Publisher lifecycle",
                    "Janus event publisher stopped",
                    context={"drained": bool(drain and not timed_out), "timed_out": timed_out},
                )

    async def admit(
        self,
        response: JanusResponse,
        *,
        session_id: JanusIdentifier | None = None,
        sender: JanusIdentifier | None = None,
    ) -> bool:
        """Admit one complete response for background publication.

        ``False`` means the response was unsupported, the publisher was not
        accepting, serialization failed, or bounded admission timed out.
        """

        route = JANUS_EVENT_ROUTES.get(response.janus)
        safe_type = response.janus if route is not None else "unknown"
        if route is None:
            self._record_drop("unsupported", safe_type)
            return False
        if not self._accepting:
            self._record_drop("not-running", safe_type)
            return False

        acquired = False
        stopped = False
        try:
            async with asyncio.timeout(self.admission_timeout):
                await self._slots.acquire()
                acquired = True
                async with self._admission_lock:
                    if not self._accepting:
                        stopped = True
                    else:
                        payload = response.model_dump(
                            mode="json",
                            by_alias=True,
                            exclude_none=True,
                        )
                        effective_session = (
                            session_id if session_id is not None else response.session_id
                        )
                        effective_sender = sender if sender is not None else response.sender
                        ordering_key = self._ordering_key(effective_session, effective_sender)
                        shard = self._shard(ordering_key)
                        item = _QueuedEvent(
                            route=route,
                            payload=payload,
                            ordering_key=ordering_key,
                            admitted_at=time.perf_counter(),
                        )
                        self._queues[shard].put_nowait(item)
                        self._depth += 1
                        # The queued item now owns the semaphore slot; its
                        # worker (or shutdown discard) releases it exactly once.
                        acquired = False
        except TimeoutError:
            if acquired:
                self._slots.release()
            with suppress(Exception):
                self.metrics.increment(
                    ADMISSION_TOTAL,
                    labels={"janus_type": safe_type, "result": "timeout"},
                )
            self._record_drop("admission-timeout", safe_type)
            return False
        except asyncio.CancelledError:
            # Cancellation can arrive while waiting for the admission lock
            # after capacity has already been reserved. Never leak that slot.
            if acquired:
                self._slots.release()
            raise
        except Exception as exc:
            if acquired:
                self._slots.release()
            with suppress(Exception):
                self.metrics.increment(
                    ADMISSION_TOTAL,
                    labels={"janus_type": safe_type, "result": "invalid"},
                )
            self._record_drop("serialization", safe_type)
            self.logger.error(
                "Event admission failed",
                "A Janus response could not be admitted for publication",
                context={"error_type": type(exc).__name__, "janus_type": safe_type},
                exc_info=exc,
            )
            return False

        if stopped:
            self._slots.release()
            self._record_drop("stopping", safe_type)
            return False
        with suppress(Exception):
            self.metrics.increment(
                ADMISSION_TOTAL,
                labels={"janus_type": safe_type, "result": "accepted"},
            )
            self.metrics.set_gauge(QUEUE_DEPTH, self._depth)
        return True

    def _ensure_route_mapping(self, requested_route: str | None) -> str:
        """Guarantee and return one exact destination for every Janus route."""

        router = getattr(self.broker, "router", None)
        destinations = getattr(router, "destinations", None)
        map_destination = getattr(router, "map_destination", None)
        if not callable(destinations) or not callable(map_destination):
            # Lightweight broker fakes used by applications/tests may implement
            # only lifecycle and publish. A real Broka Broker always has Router.
            return requested_route or DEFAULT_PHYSICAL_ROUTE

        resolved = {
            logical_route: tuple(destinations(logical_route))
            for logical_route in JANUS_EVENT_ROUTES.values()
        }
        if not any(resolved.values()):
            physical_route = requested_route or DEFAULT_PHYSICAL_ROUTE
            map_destination(
                JANUS_LOGICAL_PATTERN,
                Destination(physical_route),
            )
            return physical_route

        configured_names = {
            destination.name
            for route_destinations in resolved.values()
            for destination in route_destinations
        }
        configured_route = next(iter(configured_names)) if len(configured_names) == 1 else None
        physical_route = requested_route or configured_route or DEFAULT_PHYSICAL_ROUTE

        mismatched = {
            logical_route: tuple(destination.name for destination in route_destinations)
            for logical_route, route_destinations in resolved.items()
            if tuple(destination.name for destination in route_destinations) != (physical_route,)
        }
        if mismatched:
            raise ValueError(
                "broker routes must map every janus.* event to the single configured "
                f"physical destination {physical_route!r}"
            )
        return physical_route

    async def _worker(
        self,
        shard: int,
        queue: asyncio.Queue[_QueuedEvent | object],
    ) -> None:
        while True:
            item = await queue.get()
            if item is _STOP:
                queue.task_done()
                return
            assert isinstance(item, _QueuedEvent)
            route_type = item.route.removeprefix("janus.")
            started = time.perf_counter()
            try:
                with suppress(Exception):
                    self.metrics.observe(
                        QUEUE_LATENCY_SECONDS,
                        max(0.0, time.perf_counter() - item.admitted_at),
                        labels={"janus_type": route_type},
                    )
                async with asyncio.timeout(self.publish_timeout):
                    result = await self.broker.publish(
                        item.payload,
                        route=item.route,
                        options=PublishOptions(
                            delivery_mode=self.delivery_mode,
                            timeout=self.publish_timeout,
                            partition_key=item.ordering_key,
                            ordering_key=item.ordering_key,
                        ),
                    )
                if bool(getattr(result, "accepted", True)):
                    with suppress(Exception):
                        self.metrics.increment(
                            PUBLISHED_TOTAL,
                            labels={"janus_type": route_type},
                        )
                else:
                    with suppress(Exception):
                        self.metrics.increment(
                            PUBLISH_FAILURES_TOTAL,
                            labels={"janus_type": route_type, "result": "rejected"},
                        )
            except asyncio.CancelledError:
                self._record_drop("worker-cancelled", route_type)
                raise
            except Exception as exc:
                with suppress(Exception):
                    self.metrics.increment(
                        PUBLISH_FAILURES_TOTAL,
                        labels={"janus_type": route_type, "result": "error"},
                    )
                self.logger.error(
                    "Event publication failed",
                    "Broka failed to publish an admitted Janus response",
                    context={
                        "error_type": type(exc).__name__,
                        "janus_type": route_type,
                        "shard": shard,
                    },
                    exc_info=exc,
                )
            finally:
                with suppress(Exception):
                    self.metrics.observe(
                        PUBLISH_LATENCY_SECONDS,
                        max(0.0, time.perf_counter() - started),
                        labels={"janus_type": route_type},
                    )
                self._complete_item(queue)

    def _complete_item(self, queue: asyncio.Queue[_QueuedEvent | object]) -> None:
        queue.task_done()
        self._slots.release()
        self._depth = max(0, self._depth - 1)
        with suppress(Exception):
            self.metrics.set_gauge(QUEUE_DEPTH, self._depth)

    def _discard_queued(self, reason: str) -> None:
        for queue in self._queues:
            while True:
                try:
                    item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if item is not _STOP:
                    route_type = (
                        item.route.removeprefix("janus.")
                        if isinstance(item, _QueuedEvent)
                        else "unknown"
                    )
                    self._record_drop(reason, route_type)
                    self._slots.release()
                    self._depth = max(0, self._depth - 1)
                queue.task_done()
        with suppress(Exception):
            self.metrics.set_gauge(QUEUE_DEPTH, self._depth)

    async def _wait_for_queues(self, deadline: float | None) -> None:
        await self._with_deadline(
            asyncio.gather(*(queue.join() for queue in self._queues)),
            deadline,
        )

    async def _wait_for_tasks(self, deadline: float | None) -> None:
        await self._with_deadline(asyncio.gather(*self._tasks), deadline)

    @staticmethod
    async def _with_deadline(
        awaitable: Awaitable[object],
        deadline: float | None,
    ) -> None:
        if deadline is None:
            await awaitable
            return
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            if hasattr(awaitable, "cancel"):
                awaitable.cancel()
            raise TimeoutError
        async with asyncio.timeout(remaining):
            await awaitable

    def _default_delivery_mode(self) -> DeliveryMode:
        config = getattr(self.broker, "config", None)
        engine = getattr(config, "engine", None)
        if engine == "redis":
            assert config is not None
            settings = config.engine_settings("redis")
            if str(settings.get("mode", "streams")).casefold() == "pubsub":
                return DeliveryMode.AT_MOST_ONCE
        return DeliveryMode.AT_LEAST_ONCE

    @staticmethod
    def _ordering_key(
        session_id: JanusIdentifier | None,
        sender: JanusIdentifier | None,
    ) -> str:
        session = "-" if session_id is None else str(session_id)
        handle = "-" if sender is None else str(sender)
        return f"{session}:{handle}"

    def _shard(self, ordering_key: str) -> int:
        digest = hashlib.blake2s(ordering_key.encode("utf-8"), digest_size=4).digest()
        return int.from_bytes(digest, "big") % self.worker_count

    def _record_drop(self, reason: str, janus_type: str) -> None:
        with suppress(Exception):
            self.metrics.increment(
                DROPPED_TOTAL,
                labels={"janus_type": janus_type, "result": reason},
            )

    async def __aenter__(self) -> JanusEventPublisher:
        await self.start()
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.stop(drain=True)


__all__ = ["JanusEventPublisher", "JanusIdentifier"]
