"""Generic Janus plugin-handle foundation.

Concrete named plugins live in independent distributions.  This module knows
only how to attach a handle, send an opaque validated body, route events, and
clean up local resources.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, ClassVar, Literal, Self, TypedDict, cast, get_origin

from pydantic import BaseModel

from janus_api.core.exceptions import JanusConnectionClosed, PluginLoadError
from janus_api.lib.registry import Registry
from janus_api.models import JanusResponse
from janus_api.models.base import Jsep
from janus_api.models.request import (
    HangupRequest,
    PluginMessageRequest,
    TrickleCandidate,
    TrickleMessageRequest,
)

logger = logging.getLogger(__name__)

Listener = Callable[[Any], Awaitable[Any] | Any]


class PluginOptions(TypedDict, total=False):
    identifier: str
    plugin_id: str | int
    session: Any
    on_event: Listener
    event_queue_size: int


def _plugin_body(value: BaseModel | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, BaseModel):
        payload = value.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
            exclude_unset=True,
        )
        # Protocol discriminators are commonly Literal defaults (request,
        # ptype, textroom, etc.). Preserve every Literal field without also
        # serializing unrelated mutation defaults the caller omitted.
        for field_name, field in type(value).model_fields.items():
            if (
                field_name not in {"request", "textroom"}
                and get_origin(field.annotation) is not Literal
            ):
                continue
            serialized_name = field.serialization_alias or field.alias or field_name
            payload.setdefault(serialized_name, getattr(value, field_name))
        return payload
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError("plugin body must be a Pydantic model or mapping")


class Plugin:
    """Base class and backwards-compatible string factory for Janus plugins.

    External packages normally construct their concrete class directly, e.g.
    ``SipPlugin(session=session)``.  ``Plugin(identifier="sip", ...)`` remains
    available and lazily resolves the matching ``janus_api.plugins`` entry
    point without importing unrelated plugins.
    """

    identifier: ClassVar[str | None] = None
    name: ClassVar[str | None] = None
    registry: ClassVar[Registry[Plugin]] = Registry(entry_point_group="janus_api.plugins")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        declared_identifier = cls.__dict__.get("identifier")
        if declared_identifier:
            Plugin.registry.register(str(declared_identifier), cls)

    def __new__(cls, *args: Any, identifier: str | None = None, **kwargs: Any) -> Self:
        if cls is Plugin:
            if not identifier:
                raise TypeError("Plugin(identifier=...) requires a plugin identifier")
            concrete = cls.registry.resolve(identifier)
            if not issubclass(concrete, Plugin):
                raise PluginLoadError(
                    f"Registered object for {identifier!r} is not a Plugin subclass"
                )
            return cast(Self, object.__new__(concrete))
        return object.__new__(cls)

    def __init__(
        self,
        *,
        session: Any,
        plugin_id: str | int | None = None,
        identifier: str | None = None,
        on_event: Listener | None = None,
        event_queue_size: int = 1024,
        **_: Any,
    ) -> None:
        if session is None:
            raise TypeError("plugin requires an associated Janus session")
        if event_queue_size < 1:
            raise ValueError("event_queue_size must be positive")
        self._session = session
        self._plugin_id = plugin_id
        self._listeners: dict[str, list[Listener]] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._lifecycle_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._event_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=event_queue_size)
        self._event_task: asyncio.Task[None] | None = None
        self._dropped_events = 0
        self._closed = False
        self._on_event = on_event

    @classmethod
    def list_registered(cls) -> list[str]:
        """List concrete plugin identifiers imported in this process."""

        return list(cls.registry)

    @property
    def id(self) -> int:
        if self._plugin_id is None:
            raise RuntimeError("plugin has not been attached")
        return int(self._plugin_id)

    @property
    def session(self) -> Any:
        return self._session

    async def on(self, event: str, callback: Listener) -> None:
        if not event or not callable(callback):
            raise ValueError("event and callback are required")
        listeners = self._listeners.setdefault(event, [])
        if callback not in listeners:
            listeners.append(callback)

    async def off(self, event: str, callback: Listener) -> None:
        listeners = self._listeners.get(event)
        if listeners is None:
            return
        try:
            listeners.remove(callback)
        except ValueError:
            return
        if not listeners:
            self._listeners.pop(event, None)

    async def emit(
        self,
        event: str,
        payload: Any,
        *,
        wait: bool = False,
        timeout: float | None = None,
    ) -> list[Any]:
        callbacks = tuple(self._listeners.get(event, ()))
        tasks = [self._schedule_listener(callback, payload) for callback in callbacks]
        if not wait or not tasks:
            return tasks
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        return [task.result() for task in done if not task.cancelled() and task.exception() is None]

    @staticmethod
    async def _invoke_listener(callback: Listener, payload: Any) -> Any:
        if inspect.iscoroutinefunction(callback):
            return await callback(payload)
        result = await asyncio.to_thread(callback, payload)
        if inspect.isawaitable(result):
            return await result
        return result

    def _schedule_listener(self, callback: Listener, payload: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(self._invoke_listener(callback, payload))
        self._tasks.add(task)
        task.add_done_callback(self._listener_done)
        return task

    def _listener_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "Plugin event callback failed for handle %s",
                self._plugin_id,
                exc_info=(type(error), error, error.__traceback__),
            )

    def _dispatch_event(self, event: Any) -> None:
        """Route one event to this handle without blocking the transport loop."""

        if self._closed:
            return
        if self._event_task is None or self._event_task.done():
            self._event_task = asyncio.create_task(
                self._event_loop(),
                name=f"janus-plugin-events-{self._plugin_id or 'unbound'}",
            )
        try:
            self._event_queue.put_nowait(event)
        except asyncio.QueueFull:
            # Keep the newest state under overload and expose the loss.
            self._dropped_events += 1
            try:
                self._event_queue.get_nowait()
                self._event_queue.task_done()
            except asyncio.QueueEmpty:
                pass
            self._event_queue.put_nowait(event)
            logger.error(
                "Plugin handle %s event queue overflow; dropped=%d",
                self._plugin_id,
                self._dropped_events,
            )

    async def _event_loop(self) -> None:
        while not self._closed:
            event = await self._event_queue.get()
            try:
                callbacks = list(self._listeners.get("event", ()))
                janus_type = getattr(event, "janus", None)
                if isinstance(janus_type, str):
                    for callback in self._listeners.get(janus_type, ()):
                        if callback not in callbacks:
                            callbacks.append(callback)
                if self._on_event is not None and self._on_event not in callbacks:
                    callbacks.append(self._on_event)
                # Run callbacks in order inside the per-handle worker. This is
                # deliberate backpressure: callback storms cannot create an
                # unbounded task set, and a callback may safely close its own
                # plugin without a task-await cycle.
                for callback in callbacks:
                    try:
                        await self._invoke_listener(callback, event)
                    except asyncio.CancelledError:
                        task = asyncio.current_task()
                        if task is not None and task.cancelling():
                            raise
                        logger.warning(
                            "Plugin event callback cancelled itself for handle %s",
                            self._plugin_id,
                        )
                    except Exception:
                        logger.exception(
                            "Plugin event callback failed for handle %s",
                            self._plugin_id,
                        )
                if janus_type == "detached":
                    await self._invalidate_handle()
                    return
            finally:
                self._event_queue.task_done()

    @property
    def dropped_events(self) -> int:
        """Number of oldest events discarded after queue overflow."""

        return self._dropped_events

    # Compatibility lifecycle hooks.  Session ownership now controls actual
    # attach/detach and transport shutdown.
    def setup(self) -> None:
        return None

    def start(self) -> None:
        return None

    def stop(self) -> None:
        for task in tuple(self._tasks):
            task.cancel()

    async def attach(self, *, opaque_id: str | None = None) -> Self:
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("closed plugin handles cannot be attached")
            if self._plugin_id is not None:
                registered = self.session.plugins.get(self.id)
                if registered is None:
                    self.session.plugins.register(self.id, self)
                elif registered is not self:
                    raise RuntimeError(f"Janus handle {self.id} is owned by another plugin")
                if self._event_task is None or self._event_task.done():
                    self._event_task = asyncio.create_task(
                        self._event_loop(), name=f"janus-plugin-events-{self.id}"
                    )
                if self._closed:
                    if self.session.plugins.get(self.id) is self:
                        self.session.plugins.unregister(self.id)
                    raise RuntimeError("plugin was closed while adopting its handle")
                return self
            if not self.name:
                raise TypeError(f"{type(self).__name__} must define the Janus plugin package name")
            handle_id = await self.session.attach(self.name, opaque_id=opaque_id)
            self._plugin_id = handle_id
            try:
                if not getattr(self.session, "ready", True):
                    raise JanusConnectionClosed(
                        "Janus session was lost while attaching the plugin handle"
                    )
                self.session.plugins.register(handle_id, self)
                if self._closed:
                    raise RuntimeError("plugin was closed while attaching its handle")
            except BaseException:
                self._plugin_id = None
                try:
                    await self.session.detach(handle_id)
                except Exception:
                    logger.exception("Failed to roll back Janus handle %s", handle_id)
                raise
            self._event_task = asyncio.create_task(
                self._event_loop(), name=f"janus-plugin-events-{self.id}"
            )
            return self

    async def detach(self) -> Any | None:
        """Detach idempotently and release all per-handle listeners."""

        async with self._lifecycle_lock:
            if self._plugin_id is None:
                await self.aclose()
                return None
            handle_id = self._plugin_id
            try:
                return await self.session.detach(handle_id)
            finally:
                self._plugin_id = None
                await self.aclose()

    async def send(
        self,
        body: BaseModel | Mapping[str, Any],
        jsep: Jsep | None = None,
        *,
        timeout: float | None = None,
        wait_for_event: bool = True,
    ) -> JanusResponse:
        if self._closed or self._plugin_id is None:
            raise RuntimeError("plugin must be attached before sending messages")
        message = PluginMessageRequest(
            session_id=self.session.id,
            handle_id=self.id,
            body=_plugin_body(body),
            jsep=jsep,
        )
        return await self.session.send(
            message,
            timeout=timeout,
            wait_for_event=wait_for_event,
        )

    async def trickle(
        self,
        candidates: TrickleCandidate | Sequence[TrickleCandidate],
        *,
        timeout: float | None = None,
    ) -> JanusResponse:
        if self._closed or self._plugin_id is None:
            raise RuntimeError("plugin must be attached before trickling ICE")
        if isinstance(candidates, TrickleCandidate):
            request = TrickleMessageRequest(
                session_id=self.session.id,
                handle_id=self.id,
                candidate=candidates,
            )
        else:
            request = TrickleMessageRequest(
                session_id=self.session.id,
                handle_id=self.id,
                candidates=list(candidates),
            )
        return await self.session.send(request, timeout=timeout, wait_for_event=False)

    async def complete_trickle(self, *, timeout: float | None = None) -> JanusResponse:
        return await self.trickle(TrickleCandidate(completed=True), timeout=timeout)

    async def hangup(self, *, timeout: float | None = None) -> JanusResponse:
        if self._closed or self._plugin_id is None:
            raise RuntimeError("plugin must be attached before hanging up")
        request = HangupRequest(session_id=self.session.id, handle_id=self.id)
        return await self.session.send(request, timeout=timeout, wait_for_event=False)

    async def aclose(self) -> None:
        """Close local listeners and tasks without issuing a Janus detach.

        Use :meth:`detach` to release the remote handle immediately. Keeping a
        locally closed handle registered lets the owning session detach it
        during orderly session shutdown.
        """

        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            current = asyncio.current_task()
            event_task, self._event_task = self._event_task, None
            if event_task is not None and event_task is not current:
                event_task.cancel()
                await asyncio.gather(event_task, return_exceptions=True)
            tasks = tuple(task for task in self._tasks if task is not current)
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if current is not None:
                self._tasks.discard(current)
            self._listeners.clear()

    async def _aclose(self) -> None:
        """Backward-compatible alias for subclasses overriding old cleanup hooks."""

        await self.aclose()

    async def _invalidate_handle(self) -> None:
        """Forget a server-side handle invalidated with its owning session."""

        async with self._lifecycle_lock:
            handle_id, self._plugin_id = self._plugin_id, None
            if handle_id is not None and self.session.plugins.get(handle_id) is self:
                self.session.plugins.unregister(handle_id)
        await self.aclose()

    async def __aenter__(self) -> Self:
        return await self.attach()

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.detach()


# Compatibility alias for code that previously inspected the metaclass.
class PluginMeta:
    registry = Plugin.registry
