"""Process-local, self-healing Janus session pool.

The pre-3.0 Redis leader/RPC implementation could not preserve plugin events or
fence stale leaders and has intentionally been removed from the production
path.  Each application worker should own its Janus control connections; use a
separate, authenticated broker when cross-process handle ownership is truly
required.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import math
from collections.abc import Callable
from functools import partial
from typing import TYPE_CHECKING, Any

from janus_api.auth import JanusCredentials
from janus_api.conf import settings
from janus_api.session.base import SessionState
from janus_api.session.websocket import WebsocketSession

if TYPE_CHECKING:
    from janus_api.messaging import JanusEventPublisher

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], WebsocketSession]


class JanusSessionManager:
    """Own a bounded pool of independent Janus sessions for one process."""

    def __init__(
        self,
        *,
        pool_size: int | None = None,
        session_factory: SessionFactory | None = None,
        monitor_interval: float = 2.0,
        restart_backoff: float = 1.0,
        fail_fast: bool = True,
        event_publisher: JanusEventPublisher | None = None,
    ) -> None:
        self._pool_size = int(
            pool_size if pool_size is not None else getattr(settings, "JANUS_SESSION_POOL_SIZE", 1)
        )
        if self._pool_size < 1:
            raise ValueError("pool_size must be at least one")
        if any(
            not math.isfinite(value) or value <= 0 for value in (monitor_interval, restart_backoff)
        ):
            raise ValueError("manager timing values must be finite and greater than zero")
        self._factory = session_factory or self._default_session_factory
        self._monitor_interval = monitor_interval
        self._restart_backoff = restart_backoff
        self._fail_fast = fail_fast
        self._event_publisher = event_publisher
        self._sessions: list[WebsocketSession] = []
        self._round_robin = itertools.count()
        self._lock = asyncio.Lock()
        self._monitor_task: asyncio.Task[None] | None = None
        self._replacement_tasks: dict[int, asyncio.Task[None]] = {}
        self._cleanup_tasks: set[asyncio.Task[None]] = set()
        self._stopping = asyncio.Event()

    def _default_session_factory(self) -> WebsocketSession:
        token = settings.JANUS_TOKEN
        api_secret = settings.JANUS_API_SECRET
        credentials = (
            JanusCredentials(token=token, api_secret=api_secret)
            if token is not None or api_secret is not None
            else None
        )
        return WebsocketSession(
            credentials=credentials,
            keepalive_interval=settings.JANUS_KEEPALIVE_INTERVAL,
            keepalive_failures=settings.JANUS_KEEPALIVE_FAILURES,
            shutdown_timeout=settings.JANUS_SHUTDOWN_TIMEOUT,
            detach_concurrency=settings.JANUS_DETACH_CONCURRENCY,
            event_publisher=self._event_publisher,
        )

    @property
    def sessions(self) -> tuple[WebsocketSession, ...]:
        return tuple(self._sessions)

    @property
    def ready(self) -> bool:
        return len(self._sessions) == self._pool_size and all(item.ready for item in self._sessions)

    def get_session(self, key: str | int | None = None) -> WebsocketSession | None:
        active = tuple(session for session in self._sessions if session.ready)
        if not active:
            return None
        if key is not None:
            return active[hash(str(key)) % len(active)]
        return active[next(self._round_robin) % len(active)]

    async def start(self) -> None:
        async with self._lock:
            if self._monitor_task is not None:
                return
            self._stopping.clear()
            created: list[WebsocketSession] = []
            try:
                for _ in range(self._pool_size):
                    created.append(self._factory())
                results = await asyncio.gather(
                    *(session.create() for session in created),
                    return_exceptions=True,
                )
            except BaseException:
                await asyncio.gather(
                    *(session.destroy() for session in created),
                    return_exceptions=True,
                )
                raise
            failures = [result for result in results if isinstance(result, BaseException)]
            failed_sessions = [
                session
                for session, result in zip(created, results, strict=True)
                if isinstance(result, BaseException)
            ]
            if failed_sessions:
                await asyncio.gather(
                    *(session.destroy() for session in failed_sessions),
                    return_exceptions=True,
                )
            if failures and self._fail_fast:
                await asyncio.gather(
                    *(session.destroy() for session in created), return_exceptions=True
                )
                raise RuntimeError(
                    f"Could not start {len(failures)} of {self._pool_size} Janus sessions"
                ) from failures[0]
            if failures:
                logger.error(
                    "Started Janus session manager in degraded mode: %d/%d sessions failed",
                    len(failures),
                    self._pool_size,
                )
            self._sessions = created
            self._monitor_task = asyncio.create_task(
                self._monitor(), name="janus-session-pool-monitor"
            )

    async def stop(self) -> None:
        async with self._lock:
            self._stopping.set()
            monitor, self._monitor_task = self._monitor_task, None
            if monitor is not None and monitor is not asyncio.current_task():
                monitor.cancel()
            replacements, self._replacement_tasks = (
                tuple(self._replacement_tasks.values()),
                {},
            )
            for replacement in replacements:
                replacement.cancel()
            sessions, self._sessions = tuple(self._sessions), []
        # Never wait for a task while holding the manager lock: replacement
        # tasks acquire it to commit or relinquish ownership.
        tasks = tuple(
            task
            for task in (monitor, *replacements)
            if task is not None and task is not asyncio.current_task()
        )
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        cleanup_tasks = tuple(self._cleanup_tasks)
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            self._cleanup_tasks.clear()
        if sessions:
            await asyncio.gather(
                *(session.destroy() for session in sessions), return_exceptions=True
            )

    async def _monitor(self) -> None:
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(self._monitor_interval)
                for index, session in enumerate(tuple(self._sessions)):
                    if session.state not in {SessionState.LOST, SessionState.CLOSED}:
                        continue
                    current = self._replacement_tasks.get(index)
                    if current is not None and not current.done():
                        continue
                    task = asyncio.create_task(
                        self._replace(index, session),
                        name=f"janus-session-replacement-{index}",
                    )
                    self._replacement_tasks[index] = task
                    task.add_done_callback(partial(self._replacement_done, index))
        except asyncio.CancelledError:
            raise

    def _replacement_done(self, index: int, task: asyncio.Task[None]) -> None:
        if self._replacement_tasks.get(index) is task:
            self._replacement_tasks.pop(index, None)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "Unexpected failure replacing Janus session %d",
                index,
                exc_info=(type(error), error, error.__traceback__),
            )

    def _track_cleanup(self, session: WebsocketSession, *, name: str) -> asyncio.Task[None]:
        async def cleanup() -> None:
            await session.destroy()

        task = asyncio.create_task(cleanup(), name=name)
        self._cleanup_tasks.add(task)
        task.add_done_callback(self._cleanup_done)
        return task

    def _cleanup_done(self, task: asyncio.Task[None]) -> None:
        self._cleanup_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.warning(
                "Could not fully clean up a replaced Janus session",
                exc_info=(type(error), error, error.__traceback__),
            )

    async def _replace(self, index: int, stale: WebsocketSession) -> None:
        delay = self._restart_backoff
        while not self._stopping.is_set():
            replacement: WebsocketSession | None = None
            committed = False
            try:
                replacement = self._factory()
                await replacement.create()
                async with self._lock:
                    if (
                        not self._stopping.is_set()
                        and index < len(self._sessions)
                        and self._sessions[index] is stale
                    ):
                        self._sessions[index] = replacement
                        committed = True
                if not committed:
                    return

                stale_cleanup = self._track_cleanup(
                    stale,
                    name=f"janus-stale-session-cleanup-{index}",
                )
                # Keep cleanup alive if this replacement task is cancelled
                # concurrently with manager shutdown.
                await asyncio.gather(asyncio.shield(stale_cleanup), return_exceptions=True)
                logger.info("Replaced lost Janus session at pool index %d", index)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Could not replace lost Janus session; retrying in %.1fs",
                    delay,
                )
            finally:
                if replacement is not None and not committed:
                    cleanup = asyncio.create_task(replacement.destroy())
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        await asyncio.gather(cleanup, return_exceptions=True)
                        raise
                    except Exception:
                        logger.warning(
                            "Could not clean up an uncommitted Janus replacement",
                            exc_info=True,
                        )

            if self._stopping.is_set():
                return
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)

    async def __aenter__(self) -> JanusSessionManager:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.stop()


def get_session(session_id: str | int | None = None) -> WebsocketSession:
    """Compatibility constructor; unlike 2.x it always returns a new instance."""

    return WebsocketSession(session_id=session_id)
