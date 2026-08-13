"""WebSocket-backed Janus session lifecycle."""

from __future__ import annotations

import asyncio
import math
import random
from typing import Any, Self

from logvista import get_logger

from jrtc.core.exceptions import JanusConnectionClosed
from jrtc.models.request import (
    ClaimSessionRequest,
    CreateSessionRequest,
    DestroySessionRequest,
    KeepAliveRequest,
)
from jrtc.models.response import SuccessResponse
from jrtc.session.base import AbstractBaseSession, SessionState

logger = get_logger(__name__)


class JanusSession(AbstractBaseSession):
    """A Janus session whose handles are scoped to one injected transport.

    WebSocket is the default transport. An HTTP endpoint or a custom
    :class:`~jrtc.transport.base.JanusTransport` can be supplied without
    changing the session or plugin API.
    """

    def __init__(
        self,
        *,
        keepalive_interval: float = 25.0,
        keepalive_failures: int = 3,
        shutdown_timeout: float = 10.0,
        detach_concurrency: int = 16,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if not math.isfinite(keepalive_interval) or keepalive_interval <= 0:
            raise ValueError("keepalive_interval must be finite and greater than zero")
        if keepalive_failures < 1:
            raise ValueError("keepalive_failures must be at least one")
        if not math.isfinite(shutdown_timeout) or shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be finite and greater than zero")
        if detach_concurrency < 1:
            raise ValueError("detach_concurrency must be at least one")
        self._keepalive_interval = keepalive_interval
        self._keepalive_failures = keepalive_failures
        self._shutdown_timeout = shutdown_timeout
        self._detach_concurrency = detach_concurrency
        self._keepalive_task: asyncio.Task[None] | None = None

    async def create(self) -> Self:
        async with self._lifecycle_lock:
            if self.ready:
                return self
            if self.state in {SessionState.CLOSING, SessionState.CLOSED}:
                raise RuntimeError("closed sessions cannot be recreated")
            old_keepalive, self._keepalive_task = self._keepalive_task, None
            if old_keepalive is not None and old_keepalive is not asyncio.current_task():
                old_keepalive.cancel()
                await asyncio.gather(old_keepalive, return_exceptions=True)
            self._state = SessionState.CREATING
            try:
                await self._setup()
                claim_id = self._claim_session_id
                request = (
                    ClaimSessionRequest(session_id=claim_id)
                    if claim_id is not None
                    else CreateSessionRequest()
                )
                response = await self.send(request)
                if not isinstance(response, SuccessResponse) or (
                    claim_id is None and (response.data is None or response.data.id is None)
                ):
                    raise JanusConnectionClosed(
                        f"session activation failed (janus={response.janus!r})"
                    )
                if claim_id is not None:
                    self._session_id = claim_id
                else:
                    data = response.data
                    if data is None or data.id is None:  # Defensive narrowing for type checkers.
                        raise JanusConnectionClosed("session activation returned no session ID")
                    self._session_id = data.id
                self._claim_session_id = None
                self._lost_session_id = None
                self._state = SessionState.ACTIVE
                self._keepalive_task = asyncio.create_task(
                    self._keepalive_loop(), name=f"janus-keepalive-{self.id}"
                )
                logger.info(
                    "Janus session created",
                    "Activated a Janus session",
                    context={"session_id": self.id, "state": self.state.value},
                )
                return self
            except BaseException:
                self._state = SessionState.NEW
                if self._owns_transport and self._transport is not None:
                    self._unregister_transport_listeners()
                    await self._transport.stop()
                    self._transport = None
                raise

    async def claim(self, session_id: str | int) -> Self:
        """Reclaim a timed-out Janus session when the server permits it.

        Janus must be configured with a non-zero ``reclaim_session_timeout``.
        A closed session object cannot be reused; construct a new one instead.
        """

        if self.state not in {SessionState.NEW, SessionState.LOST}:
            raise RuntimeError(f"session cannot be claimed while state={self.state}")
        self._claim_session_id = session_id
        return await self.create()

    def _invalidate(self, reason: str) -> None:
        previous = self.state
        super()._invalidate(reason)
        if previous in {SessionState.CLOSING, SessionState.CLOSED, SessionState.LOST}:
            return
        keepalive = self._keepalive_task
        if keepalive is not None and keepalive is not asyncio.current_task():
            keepalive.cancel()

    async def _keepalive_loop(self) -> None:
        failures = 0
        try:
            while self.ready:
                await asyncio.sleep(self._keepalive_interval * random.uniform(0.9, 1.1))
                if not self.ready:
                    break
                try:
                    await self.send(
                        KeepAliveRequest(session_id=self.id),
                        wait_for_event=False,
                    )
                    failures = 0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    failures += 1
                    logger.warning(
                        "Keepalive warning",
                        f"Janus keepalive failed ({failures}/{self._keepalive_failures}): {exc!s}",
                        exc,
                    )
                    if failures >= self._keepalive_failures:
                        self._invalidate("keepalive failure threshold reached")
                        break
        except asyncio.CancelledError:
            raise

    async def destroy(self) -> None:
        """Bound and complete shutdown even when the caller is cancelled."""

        async with self._lifecycle_lock:
            shutdown = asyncio.create_task(
                self._destroy_locked(),
                name=f"janus-session-shutdown-{self._session_id or 'unbound'}",
            )
            try:
                await asyncio.shield(shutdown)
            except asyncio.CancelledError:
                # The internal operation is bounded. Wait for it to release
                # sockets, handles, and subscriptions, then preserve the
                # caller's cancellation contract.
                await asyncio.gather(shutdown, return_exceptions=True)
                raise

    async def _destroy_locked(self) -> None:
        if self.state is SessionState.CLOSED:
            return
        previous_state = self.state
        session_id = self._session_id
        self._state = SessionState.CLOSING
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._shutdown_timeout
        error: BaseException | None = None

        try:
            keepalive, self._keepalive_task = self._keepalive_task, None
            if keepalive is not None and keepalive is not asyncio.current_task():
                keepalive.cancel()
                await asyncio.gather(keepalive, return_exceptions=True)

            if previous_state is SessionState.ACTIVE and session_id is not None:
                plugins = tuple(self.plugins.as_dict().values())
                if plugins:
                    semaphore = asyncio.Semaphore(self._detach_concurrency)

                    async def detach_one(plugin: Any) -> None:
                        async with semaphore:
                            try:
                                await plugin.detach()
                            except asyncio.CancelledError:
                                raise
                            except Exception as excp:
                                logger.warning(
                                    "Plugin warning",
                                    "Could not detach plugin during session shutdown",
                                    excp,
                                )

                    # Leave time for the session destroy and mandatory local
                    # cleanup even when a handle or transport stops responding.
                    detach_deadline = min(
                        deadline - (self._shutdown_timeout * 0.5),
                        loop.time() + (self._shutdown_timeout * 0.4),
                    )
                    try:
                        async with asyncio.timeout_at(max(loop.time(), detach_deadline)):
                            await asyncio.gather(*(detach_one(plugin) for plugin in plugins))
                    except TimeoutError as err:
                        logger.warning(
                            "Plugin exception",
                            f"Timed out detaching {len(plugins)} Janus plugin handle(s)",
                            err,
                        )

                if self._transport is not None and self._transport.open:
                    remaining = max(0.0, deadline - loop.time())
                    cleanup_reserve = min(
                        self._shutdown_timeout * 0.25,
                        remaining * 0.5,
                    )
                    remote_timeout = min(
                        self._request_timeout,
                        max(0.0, remaining - cleanup_reserve),
                    )
                    if remote_timeout > 0:
                        try:
                            request = self._authorized_copy(
                                DestroySessionRequest(session_id=session_id)
                            )
                            await self._transport.send(
                                request,
                                timeout=remote_timeout,
                                wait_for_event=False,
                            )
                        except BaseException as exc:
                            error = exc
        except BaseException as exc:
            error = exc
        finally:

            async def release_and_close() -> None:
                release = getattr(self._transport, "release_session", None)
                try:
                    if session_id is not None and callable(release):
                        await release(session_id)
                finally:
                    await self._close_local()

            cleanup = asyncio.create_task(release_and_close())
            try:
                remaining = max(0.001, deadline - loop.time())
                async with asyncio.timeout(remaining):
                    await asyncio.shield(cleanup)
            except BaseException as exc:
                cleanup.cancel()
                await asyncio.gather(cleanup, return_exceptions=True)
                if error is None:
                    error = exc
            finally:
                self._session_id = None
                self._claim_session_id = None
                self._state = SessionState.CLOSED
        if error is not None:
            raise error


# The 2.x name remains an alias; the session itself is transport-agnostic.
WebsocketSession = JanusSession

__all__ = ["JanusSession", "WebsocketSession"]
