"""Instance-scoped Janus session foundation."""

from __future__ import annotations

import asyncio
import enum
import inspect
import logging
import math
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Self

from dispio import Dispatcher, ExactMatcher

from janus_api.auth import CredentialSource, resolve_credentials
from janus_api.conf import settings
from janus_api.core.exceptions import (
    JanusConfigurationError,
    JanusConnectionClosed,
    PluginNotRegistered,
)
from janus_api.lib.manager import PluginManager
from janus_api.models import JanusRequest, JanusResponse
from janus_api.models.request import AttachPluginRequest, DetachPluginRequest
from janus_api.models.response import SuccessResponse
from janus_api.transport.base import JanusTransport
from janus_api.transport.websocket import WebsocketTransportClient

if TYPE_CHECKING:
    from janus_api.messaging import JanusEventPublisher

logger = logging.getLogger(__name__)

TransportFactory = Callable[[], JanusTransport | Awaitable[JanusTransport]]


class SessionState(enum.StrEnum):
    NEW = "new"
    CREATING = "creating"
    ACTIVE = "active"
    LOST = "lost"
    CLOSING = "closing"
    CLOSED = "closed"


def _default_transport_factory(
    url: str,
    request_timeout: float,
    event_publisher: JanusEventPublisher | None,
) -> TransportFactory:
    if url.startswith(("http://", "https://")):

        def http_factory() -> JanusTransport:
            try:
                from janus_api.transport.http import HttpTransportClient
            except ImportError as exc:
                raise JanusConfigurationError(
                    "Install janus-api-core[http] to use the REST transport"
                ) from exc
            return HttpTransportClient(
                url,
                request_timeout=request_timeout,
                event_publisher=event_publisher,
            )

        return http_factory
    if not url.startswith(("ws://", "wss://")):
        raise JanusConfigurationError(
            "Janus URLs must use ws://, wss://, http://, or https://. "
            "Inject a JanusTransport for message-queue or Unix-socket transports."
        )

    def factory() -> WebsocketTransportClient:
        return WebsocketTransportClient(
            url,
            request_timeout=request_timeout,
            event_publisher=event_publisher,
        )

    return factory


class AbstractBaseSession:
    """Base session with ordinary instance ownership and per-session handles."""

    def __init__(
        self,
        *,
        session_id: str | int | None = None,
        transport: JanusTransport | None = None,
        transport_factory: TransportFactory | None = None,
        url: str | None = None,
        credentials: CredentialSource = None,
        request_timeout: float | None = None,
        event_publisher: JanusEventPublisher | None = None,
    ) -> None:
        self._session_id: str | int | None = None
        self._claim_session_id: str | int | None = session_id
        self._state = SessionState.NEW
        self._transport = transport
        self._owns_transport = transport is None
        self._event_publisher = event_publisher
        self._request_timeout = float(
            request_timeout
            if request_timeout is not None
            else getattr(settings, "JANUS_REQUEST_TIMEOUT", 15.0)
        )
        if not math.isfinite(self._request_timeout) or self._request_timeout <= 0:
            raise ValueError("request_timeout must be finite and greater than zero")
        if transport is not None and transport_factory is not None:
            raise ValueError("transport and transport_factory are mutually exclusive")
        self._transport_factory: TransportFactory | None
        if transport_factory is not None:
            self._transport_factory = transport_factory
        elif transport is None:
            endpoint = str(
                url
                or getattr(
                    settings,
                    "JANUS_SESSION_URL",
                    "ws://localhost:8188/janus",
                )
            )
            self._transport_factory = _default_transport_factory(
                endpoint,
                self._request_timeout,
                event_publisher,
            )
        else:
            # An injected transport owns its endpoint semantics (AMQP, MQTT,
            # nanomsg, Unix sockets, or an application-specific transport).
            self._transport_factory = None
        self._credentials = credentials
        self._plugins: PluginManager[Any] = PluginManager()
        self._transport_listeners_registered = False
        self._setup_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._lost_session_id: str | int | None = None
        self._cleanup_tasks: set[asyncio.Task[Any]] = set()
        self._event_dispatcher = Dispatcher(name="janus.session.events")
        self._event_dispatcher.add(
            ExactMatcher("timeout"),
            self._handle_timeout,
            name="session-timeout",
        )
        self._event_dispatcher.default(
            self._route_plugin_event,
            name="session-plugin-event",
        )
        self._event_dispatcher.registry.freeze()

    @property
    def id(self) -> int:
        if self._session_id is None:
            raise RuntimeError(f"session has no active ID (state={self._state})")
        return int(self._session_id)

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def ready(self) -> bool:
        return (
            self._state is SessionState.ACTIVE
            and self._session_id is not None
            and self._transport is not None
            and self._transport.open
        )

    @property
    def lost_session_id(self) -> str | int | None:
        return self._lost_session_id

    @property
    def plugins(self) -> PluginManager[Any]:
        return self._plugins

    @property
    def transport(self) -> JanusTransport | None:
        return self._transport

    async def _setup(self) -> None:
        async with self._setup_lock:
            if self._transport is None:
                if self._transport_factory is None:
                    raise JanusConfigurationError("no Janus transport is configured")
                value = self._transport_factory()
                self._transport = await value if inspect.isawaitable(value) else value
            if not all(
                hasattr(self._transport, attribute)
                for attribute in (
                    "add_close_listener",
                    "add_message_listener",
                    "open",
                    "remove_close_listener",
                    "remove_message_listener",
                    "send",
                    "start",
                    "stop",
                )
            ):
                raise TypeError("transport_factory did not return a JanusTransport")
            self._register_transport_listeners()
            if not self._transport.open:
                await self._transport.start()

    def _register_transport_listeners(self) -> None:
        if self._transport is None or self._transport_listeners_registered:
            return
        self._transport.add_message_listener(self._route_event)
        self._transport.add_close_listener(self._transport_closed)
        self._transport_listeners_registered = True

    def _unregister_transport_listeners(self) -> None:
        if self._transport is None or not self._transport_listeners_registered:
            return
        self._transport.remove_message_listener(self._route_event)
        self._transport.remove_close_listener(self._transport_closed)
        self._transport_listeners_registered = False

    def _authorized_copy(self, request: JanusRequest) -> JanusRequest:
        copy = request.model_copy()
        credentials = resolve_credentials(self._credentials)
        if credentials is not None:
            credentials.apply(copy)
        return copy

    async def send(
        self,
        data: JanusRequest,
        *,
        timeout: float | None = None,
        wait_for_event: bool = False,
    ) -> JanusResponse:
        if self._state in {SessionState.LOST, SessionState.CLOSING, SessionState.CLOSED}:
            raise JanusConnectionClosed(f"session cannot send while state={self._state}")
        await self._setup()
        assert self._transport is not None
        response = await self._transport.send(
            self._authorized_copy(data),
            timeout=self._request_timeout if timeout is None else timeout,
            wait_for_event=wait_for_event,
        )
        if self._state is SessionState.LOST:
            raise JanusConnectionClosed("session was lost while the request was in flight")
        return response

    async def attach(self, plugin: str, *, opaque_id: str | None = None) -> str | int:
        if not self.ready:
            raise JanusConnectionClosed("session must be active before attaching a plugin")
        response = await self.send(
            AttachPluginRequest(
                session_id=self.id,
                plugin=plugin,
                opaque_id=opaque_id,
            )
        )
        if (
            not isinstance(response, SuccessResponse)
            or response.data is None
            or response.data.id is None
        ):
            raise JanusConnectionClosed(
                f"attach did not return a handle ID (janus={response.janus!r})"
            )
        if not self.ready:
            raise JanusConnectionClosed("session was lost while attaching a plugin handle")
        return response.data.id

    async def detach(self, handle_id: str | int) -> str | int:
        key = str(handle_id)
        if self._state in {SessionState.ACTIVE, SessionState.CLOSING}:
            try:
                request = DetachPluginRequest(session_id=self.id, handle_id=handle_id)
                if self._state is SessionState.ACTIVE:
                    await self.send(request, wait_for_event=False)
                elif self._transport is not None and self._transport.open:
                    await self._transport.send(
                        self._authorized_copy(request),
                        timeout=self._request_timeout,
                        wait_for_event=False,
                    )
            finally:
                with suppress(PluginNotRegistered):
                    self._plugins.unregister(key)
        else:
            with suppress(PluginNotRegistered):
                self._plugins.unregister(key)
        return handle_id

    def _route_event(self, response: JanusResponse) -> None:
        """Route one local response without involving the external subscriber path."""

        response_session_id = getattr(response, "session_id", None)
        if (
            response_session_id is not None
            and self._session_id is not None
            and str(response_session_id) != str(self._session_id)
        ):
            return
        self._event_dispatcher.dispatch(
            response,
            __dispatch_key=response.janus,
        )

    def _handle_timeout(self, _response: JanusResponse) -> None:
        self._invalidate("Janus reported a session timeout")

    def _route_plugin_event(self, response: JanusResponse) -> None:
        sender = getattr(response, "sender", None)
        if sender is None:
            return
        try:
            self._plugins.dispatch(sender, response)
        except PluginNotRegistered:
            logger.debug("No local plugin owns Janus handle %s", sender)
        except Exception:
            logger.exception("Could not route event for Janus handle %s", sender)

    def _transport_closed(self, _error: BaseException | None = None) -> None:
        self._invalidate("transport connection closed")

    def _invalidate(self, reason: str) -> None:
        if self._state in {SessionState.CLOSING, SessionState.CLOSED, SessionState.LOST}:
            return
        self._lost_session_id = self._session_id
        self._session_id = None
        self._state = SessionState.LOST
        plugins = tuple(self._plugins.as_dict().values())
        self._plugins.clear()
        for plugin in plugins:
            close = getattr(plugin, "_invalidate_handle", None)
            if not callable(close):
                close = getattr(plugin, "aclose", None)
            if callable(close):
                try:
                    task = asyncio.create_task(close())
                except RuntimeError:
                    logger.debug("No event loop available to close invalidated plugin")
                else:
                    self._cleanup_tasks.add(task)
                    task.add_done_callback(self._cleanup_tasks.discard)
        logger.warning("Janus session invalidated: %s", reason)

    async def _close_local(self) -> None:
        self._unregister_transport_listeners()
        plugins = tuple(self._plugins.as_dict().values())
        self._plugins.clear()
        if plugins:
            await asyncio.gather(
                *(plugin.aclose() for plugin in plugins if hasattr(plugin, "aclose")),
                return_exceptions=True,
            )
        cleanup_tasks = tuple(self._cleanup_tasks)
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            self._cleanup_tasks.clear()
        if self._owns_transport and self._transport is not None:
            await self._transport.stop()
        self._transport = None

    async def create(self) -> Self:
        raise NotImplementedError

    async def destroy(self) -> None:
        async with self._lifecycle_lock:
            self._state = SessionState.CLOSING
            try:
                await self._close_local()
            finally:
                self._session_id = None
                self._claim_session_id = None
                self._state = SessionState.CLOSED

    async def __aenter__(self) -> Self:
        return await self.create()

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.destroy()

    def __repr__(self) -> str:
        return f"{type(self).__name__}(id={self._session_id!r}, state={self._state!r})"

    def __str__(self) -> str:
        return str(self._session_id) if self._session_id is not None else self._state.value
