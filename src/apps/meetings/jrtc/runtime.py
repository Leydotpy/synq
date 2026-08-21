"""Process-local ownership for JRTC sessions, publishing, and live handles."""

from __future__ import annotations

import asyncio
import atexit
import inspect
import logging
import os
import threading
from collections.abc import AsyncIterator, Awaitable
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import asynccontextmanager
from typing import Any

from django.conf import settings as django_settings
from jrtc import JanusSessionManager
from jrtc.conf import Janus
from jrtc.messaging import JanusEventPublisher

from apps.meetings.jrtc.broker import build_event_publisher
from apps.meetings.jrtc.config import JrtcEventConfig, configure_jrtc_core, load_event_config
from apps.meetings.jrtc.errors import JrtcRuntimeUnavailable, JrtcSessionUnavailable
from apps.meetings.jrtc.handles import JrtcHandleRegistry
from apps.meetings.jrtc.ownership import new_runtime_owner_id
from apps.meetings.jrtc.videoroom import VideoRoomAdapter

logger = logging.getLogger(__name__)


class JanusProcessRuntime:
    """Own all event-loop-bound JRTC objects for exactly one process instance."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"

    def __init__(self) -> None:
        self._pid = os.getpid()
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._stop_requested = threading.Event()
        self._state = self.STOPPED
        self._loop: asyncio.AbstractEventLoop | None = None
        self._manager: JanusSessionManager | None = None
        self._publisher: JanusEventPublisher | None = None
        self._event_config: JrtcEventConfig | None = None
        self._manager_lease: object | None = None
        self._owner_thread_id: int | None = None
        self._thread: threading.Thread | None = None
        self._startup_error: BaseException | None = None
        self._owner_id = ""
        self._registry: JrtcHandleRegistry
        self._adapter: VideoRoomAdapter
        self._renew_identity()

    def _renew_identity(self) -> None:
        self._owner_id = new_runtime_owner_id()
        self._registry = JrtcHandleRegistry(self._owner_id)
        self._adapter = VideoRoomAdapter(self, self._registry)

    @property
    def manager(self) -> JanusSessionManager | None:
        with self._lock:
            return self._manager

    @property
    def publisher(self) -> JanusEventPublisher | None:
        with self._lock:
            return self._publisher

    @property
    def broker(self) -> Any | None:
        publisher = self.publisher
        return None if publisher is None else publisher.broker

    @property
    def registry(self) -> JrtcHandleRegistry:
        return self._registry

    @property
    def adapter(self) -> VideoRoomAdapter:
        return self._adapter

    @property
    def owner_id(self) -> str:
        return self._owner_id

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def inspect(self) -> dict[str, object]:
        """Return low-cardinality, credential-free runtime health data."""

        with self._lock:
            state = self._state
            manager = self._manager
            publisher = self._publisher
        sessions = () if manager is None else manager.sessions
        return {
            "state": state,
            "runtime_owner_id": self._owner_id,
            "manager_ready": bool(manager is not None and manager.ready),
            "session_count": len(sessions),
            "ready_session_count": sum(
                bool(getattr(session, "ready", False)) for session in sessions
            ),
            "active_handle_count": self._registry.active_count,
            "stale_handle_invalidations": self._registry.stale_invalidations,
            "publisher_running": bool(
                publisher is not None and getattr(publisher, "running", False)
            ),
            "publisher_queue_depth": (
                0 if publisher is None else int(getattr(publisher, "queue_depth", 0))
            ),
        }

    def reset_after_fork(self) -> None:
        """Discard inherited loops, transports, handles, and ownership in a child."""

        current_pid = os.getpid()
        if current_pid == self._pid:
            return
        self._registry.discard_after_fork()
        self._pid = current_pid
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._stop_requested = threading.Event()
        self._state = self.STOPPED
        self._loop = None
        self._manager = None
        self._publisher = None
        self._event_config = None
        self._manager_lease = None
        self._owner_thread_id = None
        self._thread = None
        self._startup_error = None
        self._renew_identity()
        Janus.set_manager(None)

    def _claim_start(self, *, thread: threading.Thread | None) -> None:
        self.reset_after_fork()
        with self._lock:
            if self._state not in {self.STOPPED, self.FAILED}:
                raise JrtcRuntimeUnavailable(
                    f"JRTC runtime cannot start while state={self._state}."
                )
            self._renew_identity()
            self._state = self.STARTING
            self._startup_error = None
            self._ready.clear()
            self._stop_requested.clear()
            self._thread = thread

    def _bind(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        manager: JanusSessionManager,
        publisher: JanusEventPublisher | None,
        event_config: JrtcEventConfig,
        lease: object,
        owner_thread_id: int,
        thread: threading.Thread | None,
    ) -> None:
        with self._lock:
            if self._state != self.STARTING:
                raise JrtcRuntimeUnavailable(
                    f"JRTC runtime cannot bind while state={self._state}."
                )
            self._loop = loop
            self._manager = manager
            self._publisher = publisher
            self._event_config = event_config
            self._manager_lease = lease
            self._owner_thread_id = owner_thread_id
            self._thread = thread
            self._startup_error = None
            self._state = self.RUNNING
            self._ready.set()

    def _begin_stop(self, manager: JanusSessionManager) -> None:
        with self._lock:
            if self._manager is manager:
                self._state = self.STOPPING

    def _finish_stop(
        self,
        manager: JanusSessionManager | None,
        *,
        error: BaseException | None = None,
    ) -> None:
        with self._lock:
            if manager is not None and self._manager not in (None, manager):
                return
            self._loop = None
            self._manager = None
            self._publisher = None
            self._event_config = None
            self._manager_lease = None
            self._owner_thread_id = None
            self._thread = None
            self._startup_error = error
            self._state = self.FAILED if error is not None else self.STOPPED
            if error is not None:
                self._ready.set()
            else:
                self._ready.clear()

    async def _start_owned(
        self,
    ) -> tuple[JanusSessionManager, JanusEventPublisher | None, JrtcEventConfig, object]:
        configure_jrtc_core()
        config = load_event_config(consumer_name=self._owner_id)
        publisher = build_event_publisher(config)
        manager: JanusSessionManager | None = None
        lease: object | None = None
        try:
            if publisher is not None:
                await publisher.start()
            manager = JanusSessionManager(
                fail_fast=bool(django_settings.JANUS_STARTUP_FAIL_FAST),
                event_publisher=publisher,
            )
            await manager.start()
            lease = Janus.install_manager(manager)
            return manager, publisher, config, lease
        except BaseException:
            if manager is not None:
                try:
                    await manager.stop()
                except Exception:
                    logger.exception("JRTC manager cleanup failed during startup")
            if lease is not None:
                Janus.remove_manager(lease)
            if publisher is not None:
                try:
                    await publisher.stop(drain=False)
                except Exception:
                    logger.exception("JRTC publisher cleanup failed during startup")
            raise

    async def _stop_owned(
        self,
        manager: JanusSessionManager | None,
        publisher: JanusEventPublisher | None,
        config: JrtcEventConfig | None,
    ) -> None:
        """Stop event production, drain publication, then clear local handles."""

        first_error: BaseException | None = None
        if manager is not None:
            try:
                await manager.stop()
            except BaseException as exc:
                first_error = exc
                logger.exception("Could not completely stop the JRTC session manager")
        if publisher is not None:
            try:
                await publisher.stop(
                    drain=True,
                    timeout=None if config is None else config.drain_timeout,
                )
            except BaseException as exc:
                first_error = first_error or exc
                logger.exception("Could not completely drain the JRTC event publisher")
        try:
            await self._registry.clear()
        except BaseException as exc:
            first_error = first_error or exc
            logger.exception("Could not completely clear the JRTC handle registry")
        if first_error is not None:
            raise first_error

    @asynccontextmanager
    async def lifespan(self, _app: Any) -> AsyncIterator[dict[str, Any]]:
        """Own JRTC on the ASGI server's long-lived event loop."""

        self._claim_start(thread=None)
        loop = asyncio.get_running_loop()
        manager: JanusSessionManager | None = None
        publisher: JanusEventPublisher | None = None
        config: JrtcEventConfig | None = None
        lease: object | None = None
        bound = False
        failure: BaseException | None = None
        try:
            manager, publisher, config, lease = await self._start_owned()
            self._bind(
                loop=loop,
                manager=manager,
                publisher=publisher,
                event_config=config,
                lease=lease,
                owner_thread_id=threading.get_ident(),
                thread=None,
            )
            bound = True
            yield {
                "jrtc": Janus,
                "session_manager": manager,
                "event_publisher": publisher,
                "handle_registry": self._registry,
                "runtime_owner_id": self._owner_id,
            }
        except BaseException as exc:
            failure = exc
            raise
        finally:
            shutdown_error: BaseException | None = None
            if bound and manager is not None:
                self._begin_stop(manager)
            try:
                await self._stop_owned(manager, publisher, config)
            except BaseException as exc:
                shutdown_error = exc
                if failure is None:
                    raise
            finally:
                if lease is not None:
                    Janus.remove_manager(lease)
                self._finish_stop(
                    manager if bound else None,
                    error=failure or shutdown_error,
                )

    def _background_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        manager: JanusSessionManager | None = None
        publisher: JanusEventPublisher | None = None
        config: JrtcEventConfig | None = None
        lease: object | None = None
        bound = False
        failure: BaseException | None = None
        try:
            manager, publisher, config, lease = loop.run_until_complete(self._start_owned())
            if self._stop_requested.is_set():
                return
            self._bind(
                loop=loop,
                manager=manager,
                publisher=publisher,
                event_config=config,
                lease=lease,
                owner_thread_id=threading.get_ident(),
                thread=threading.current_thread(),
            )
            bound = True
            loop.run_forever()
        except BaseException as exc:
            failure = exc
        finally:
            if bound and manager is not None:
                self._begin_stop(manager)
            try:
                loop.run_until_complete(self._stop_owned(manager, publisher, config))
            except BaseException as exc:
                failure = failure or exc
            if lease is not None:
                Janus.remove_manager(lease)
            self._finish_stop(manager if bound else None, error=failure)
            asyncio.set_event_loop(None)
            loop.close()

    def ensure_background(self) -> None:
        """Start the persistent bridge loop used by sync/Celery contexts."""

        self.reset_after_fork()
        thread_to_start: threading.Thread | None = None
        with self._lock:
            if self._state == self.RUNNING:
                return
            if self._state == self.STOPPING:
                raise JrtcRuntimeUnavailable("The process-local JRTC runtime is stopping.")
            thread = self._thread
            if self._state in {self.STOPPED, self.FAILED} and (
                thread is None or not thread.is_alive()
            ):
                self._renew_identity()
                thread_to_start = threading.Thread(
                    target=self._background_main,
                    name="jrtc-process-runtime",
                    daemon=True,
                )
                self._state = self.STARTING
                self._ready.clear()
                self._stop_requested.clear()
                self._startup_error = None
                self._thread = thread_to_start

        if thread_to_start is not None:
            try:
                thread_to_start.start()
            except BaseException as exc:
                self._finish_stop(None, error=exc)
                raise JrtcRuntimeUnavailable(
                    "Unable to start the process-local JRTC runtime thread."
                ) from exc

        startup_timeout = float(django_settings.JANUS_RUNTIME_STARTUP_TIMEOUT)
        if not self._ready.wait(timeout=startup_timeout):
            raise JrtcRuntimeUnavailable(
                f"JRTC runtime did not start within {startup_timeout:g} seconds."
            )
        with self._lock:
            startup_error = self._startup_error
            state = self._state
        if startup_error is not None:
            raise JrtcRuntimeUnavailable(
                "Unable to start the process-local JRTC runtime."
            ) from startup_error
        if state != self.RUNNING:
            raise JrtcRuntimeUnavailable(
                f"The process-local JRTC runtime did not become ready (state={state})."
            )

    def stop_background(self) -> None:
        """Stop a fallback loop; the ASGI server owns lifespan shutdown itself."""

        self.reset_after_fork()
        with self._lock:
            loop = self._loop
            thread = self._thread
            if self._state in {self.STARTING, self.RUNNING} and thread is not None:
                self._stop_requested.set()
            if self._state == self.RUNNING and loop is not None and thread is not None:
                self._state = self.STOPPING
        if thread is None or thread is threading.current_thread():
            return
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=float(django_settings.JANUS_SHUTDOWN_TIMEOUT) + 2.0)
        if thread.is_alive():
            logger.warning("JRTC background runtime did not stop before its deadline")

    def session(self, *, key: str | int | None = None) -> Any:
        """Return a ready session pinned by a stable domain key."""

        self.reset_after_fork()
        with self._lock:
            manager = self._manager
            state = self._state
        if state == self.STOPPING:
            raise JrtcRuntimeUnavailable("The process-local JRTC runtime is stopping.")
        if manager is None or state != self.RUNNING:
            self.ensure_background()
            with self._lock:
                manager = self._manager
                state = self._state
        if manager is None or state != self.RUNNING:
            raise JrtcRuntimeUnavailable(
                f"The process-local JRTC runtime is unavailable (state={state})."
            )
        session = manager.get_session(key=key)
        if session is None or not bool(getattr(session, "ready", False)):
            raise JrtcSessionUnavailable("No ready Janus session is available in this process.")
        return session

    def run(self, awaitable: Awaitable[Any]) -> Any:
        """Resolve an awaitable on the event loop that owns its JRTC transport."""

        self.reset_after_fork()
        with self._lock:
            manager = self._manager
            state = self._state
        if state == self.STOPPING:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise JrtcRuntimeUnavailable("The process-local JRTC runtime is stopping.")
        if manager is None or state != self.RUNNING:
            try:
                self.ensure_background()
            except BaseException:
                if inspect.iscoroutine(awaitable):
                    awaitable.close()
                raise

        with self._lock:
            loop = self._loop
            owner_thread_id = self._owner_thread_id
        if loop is None or not loop.is_running():
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise JrtcRuntimeUnavailable("The process-local JRTC event loop is not running.")
        if owner_thread_id == threading.get_ident():
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise JrtcRuntimeUnavailable(
                "A synchronous JRTC call cannot block its owner loop; await it directly."
            )

        async def _resolve() -> Any:
            return await awaitable

        future = asyncio.run_coroutine_threadsafe(_resolve(), loop)
        call_timeout = float(django_settings.JANUS_SYNC_CALL_TIMEOUT)
        try:
            return future.result(timeout=call_timeout)
        except FutureTimeoutError as exc:
            future.cancel()
            raise JrtcRuntimeUnavailable(
                f"JRTC operation exceeded the {call_timeout:g}-second bridge timeout."
            ) from exc


janus_runtime = JanusProcessRuntime()
atexit.register(janus_runtime.stop_background)


__all__ = ["JanusProcessRuntime", "janus_runtime"]
