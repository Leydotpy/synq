"""Janus REST transport with managed long-poll notification streams."""

from __future__ import annotations

import asyncio
import inspect
import itertools
import logging
import math
import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import httpx
from logvista import get_logger

from jrtc.core.exceptions import (
    JanusConnectionClosed,
    JanusErrorResponse,
    JanusProtocolError,
    JanusRequestTimeout,
    JanusTransportError,
)
from jrtc.models import JanusRequest, JanusResponse
from jrtc.models.request import (
    ClaimSessionRequest,
    CreateSessionRequest,
    DestroySessionRequest,
    InfoRequest,
)
from jrtc.models.response import (
    SuccessResponse,
    TimeoutResponse,
    parse_janus_response,
)
from jrtc.transport.base import CloseListener, MessageListener

if TYPE_CHECKING:
    from jrtc.messaging import JanusEventPublisher

logger = get_logger(__name__)
_SENSITIVE_QUERY = re.compile(r"(?i)(token|apisecret)=([^&\s\"]+)")


class _SensitiveUrlFilter(logging.Filter):
    @staticmethod
    def _redact(value: Any) -> Any:
        text = str(value)
        redacted = _SENSITIVE_QUERY.sub(r"\1=<redacted>", text)
        return redacted if redacted != text else value

    def filter(self, record: logging.LogRecord) -> bool:
        if record.args:
            if isinstance(record.args, dict):
                record.args = {key: self._redact(value) for key, value in record.args.items()}
            elif isinstance(record.args, tuple):
                record.args = tuple(self._redact(value) for value in record.args)
            else:
                record.args = self._redact(record.args)
        record.msg = _SENSITIVE_QUERY.sub(r"\1=<redacted>", str(record.msg))
        return True


def _install_http_log_redaction() -> None:
    for name in ("httpx", "httpcore"):
        target = logging.getLogger(name)
        if not any(isinstance(item, _SensitiveUrlFilter) for item in target.filters):
            target.addFilter(_SensitiveUrlFilter())


def _transport_failure(operation: str, error: Exception) -> JanusTransportError:
    if isinstance(error, httpx.HTTPStatusError):
        return JanusTransportError(
            f"Janus HTTP {operation} failed with status {error.response.status_code}"
        )
    return JanusTransportError(f"Janus HTTP {operation} failed ({type(error).__name__})")


@dataclass(slots=True)
class _Pending:
    future: asyncio.Future[JanusResponse]
    wait_for_event: bool


class HttpTransportClient:
    """Plain HTTP transport implementing Janus path addressing and long polls."""

    def __init__(
        self,
        url: str = "http://localhost:8088/janus",
        *,
        request_timeout: float = 15.0,
        poll_timeout: float = 35.0,
        max_poll_events: int = 10,
        max_pending_transactions: int = 4096,
        client: httpx.AsyncClient | None = None,
        event_publisher: JanusEventPublisher | None = None,
    ) -> None:
        if (
            not math.isfinite(request_timeout)
            or not math.isfinite(poll_timeout)
            or request_timeout <= 0
            or poll_timeout <= 0
        ):
            raise ValueError("HTTP transport timeouts must be finite and greater than zero")
        if max_poll_events < 1 or max_poll_events > 100:
            raise ValueError("max_poll_events must be between 1 and 100")
        if max_pending_transactions < 1:
            raise ValueError("max_pending_transactions must be positive")
        _install_http_log_redaction()
        self._url = url.rstrip("/")
        self._request_timeout = request_timeout
        self._poll_timeout = poll_timeout
        self._max_poll_events = max_poll_events
        self._client = client
        self._owns_client = client is None
        self._open = client is not None
        self._message_listeners: set[MessageListener] = set()
        self._close_listeners: set[CloseListener] = set()
        self._pollers: dict[str, asyncio.Task[None]] = {}
        self._poll_credentials: dict[str, dict[str, str]] = {}
        self._pending: dict[str, _Pending] = {}
        self._pending_slots = asyncio.Semaphore(max_pending_transactions)
        self._state_lock = asyncio.Lock()
        self._rid = itertools.count(time.time_ns() // 1_000_000)
        from jrtc.messaging import JanusResponseDispatcher

        self._dispatcher = JanusResponseDispatcher(
            publisher=event_publisher,
            on_transaction=self._resolve_transaction,
            on_ack=self._resolve_ack,
            on_error=self._resolve_error,
        )
        self._metrics = {
            "received": 0,
            "resolved": 0,
            "errors": 0,
            "events": 0,
        }

    @property
    def open(self) -> bool:
        return self._open and self._client is not None

    @property
    def metrics(self) -> dict[str, int]:
        return dict(self._metrics)

    def add_message_listener(self, listener: MessageListener) -> None:
        if not callable(listener):
            raise TypeError("message listener must be callable")
        self._message_listeners.add(listener)

    def remove_message_listener(self, listener: MessageListener) -> None:
        self._message_listeners.discard(listener)

    def add_close_listener(self, listener: CloseListener) -> None:
        if not callable(listener):
            raise TypeError("close listener must be callable")
        self._close_listeners.add(listener)

    def remove_close_listener(self, listener: CloseListener) -> None:
        self._close_listeners.discard(listener)

    async def start(self) -> None:
        async with self._state_lock:
            if self.open:
                return
            if self._client is None:
                self._client = httpx.AsyncClient(
                    timeout=httpx.Timeout(self._request_timeout),
                    follow_redirects=False,
                    headers={"Accept": "application/json"},
                )
            self._open = True
            logger.debug("Transport metric", "Janus HTTP transport started")

    async def stop(self) -> None:
        async with self._state_lock:
            self._open = False
            pollers, self._pollers = tuple(self._pollers.values()), {}
            for poller in pollers:
                poller.cancel()
            if pollers:
                await asyncio.gather(*pollers, return_exceptions=True)
            self._poll_credentials.clear()
            error = JanusConnectionClosed("Janus HTTP transport stopped")
            for pending in tuple(self._pending.values()):
                if not pending.future.done():
                    pending.future.set_exception(error)
            self._pending.clear()
            client = self._client
            if self._owns_client:
                self._client = None
                if client is not None:
                    await client.aclose()
            await self._notify_close(error)

    @staticmethod
    def _ids(message: JanusRequest) -> tuple[str | None, str | None]:
        session = getattr(message, "session_id", None)
        handle = getattr(message, "handle_id", None)
        return (
            None if session is None else str(session),
            None if handle is None else str(handle),
        )

    def _endpoint(self, message: JanusRequest) -> str:
        session, handle = self._ids(message)
        if handle is not None:
            return f"{self._url}/{session}/{handle}"
        if session is not None:
            return f"{self._url}/{session}"
        return self._url

    @staticmethod
    def _payload(message: JanusRequest) -> dict[str, Any]:
        payload = message.model_dump(mode="json", by_alias=True, exclude_none=True)
        # HTTP addresses these values in the URL rather than the JSON body.
        payload.pop("session_id", None)
        payload.pop("handle_id", None)
        return payload

    async def send(
        self,
        message: JanusRequest,
        *,
        timeout: float | None = None,
        wait_for_event: bool = False,
    ) -> JanusResponse:
        if not self.open or self._client is None:
            raise JanusConnectionClosed("Janus HTTP transport is not open")
        effective = self._request_timeout if timeout is None else timeout
        if not math.isfinite(effective) or effective <= 0:
            raise ValueError("timeout must be finite and greater than zero")
        transaction = message.transaction
        future: asyncio.Future[JanusResponse] | None = None
        try:
            async with asyncio.timeout(effective):
                async with self._pending_slots:
                    if not self.open or self._client is None:
                        raise JanusConnectionClosed("Janus HTTP transport is not open")
                    if transaction in self._pending:
                        raise JanusProtocolError(f"duplicate in-flight transaction {transaction!r}")
                    future = asyncio.get_running_loop().create_future()
                    self._pending[transaction] = _Pending(future, wait_for_event)
                    session_id, _handle_id = self._ids(message)
                    if session_id in self._poll_credentials:
                        self._poll_credentials[session_id] = self._credentials(message)
                    try:
                        if isinstance(message, InfoRequest):
                            response = await self._client.get(
                                f"{self._url}/info",
                                params=self._credentials(message),
                                timeout=effective,
                            )
                        else:
                            response = await self._client.post(
                                self._endpoint(message),
                                json=self._payload(message),
                                timeout=effective,
                            )
                        response.raise_for_status()
                        decoded = response.json()
                    except httpx.TimeoutException as exc:
                        raise TimeoutError from exc
                    except (httpx.HTTPError, ValueError) as exc:
                        raise _transport_failure("request", exc) from None
                    parsed = parse_janus_response(decoded)
                    await self._handle_response(parsed)

                    if isinstance(
                        message, (CreateSessionRequest, ClaimSessionRequest)
                    ) and isinstance(parsed, SuccessResponse):
                        activated_id = (
                            message.session_id
                            if isinstance(message, ClaimSessionRequest)
                            else parsed.data.id
                            if parsed.data is not None
                            else None
                        )
                        if activated_id is not None:
                            self._start_poller(str(activated_id), message)

                    result = await future
                    if isinstance(message, DestroySessionRequest):
                        await self._stop_poller(str(message.session_id))
                    return result
        except TimeoutError as exc:
            raise JanusRequestTimeout(transaction, effective) from exc
        finally:
            if future is not None:
                pending = self._pending.get(transaction)
                if pending is not None and pending.future is future:
                    self._pending.pop(transaction, None)
                if not future.done():
                    future.cancel()

    def _start_poller(self, session_id: str, request: JanusRequest) -> None:
        if session_id in self._pollers:
            return
        self._poll_credentials[session_id] = self._credentials(request)
        self._pollers[session_id] = asyncio.create_task(
            self._poll(session_id), name=f"janus-http-poll-{session_id}"
        )

    async def _stop_poller(self, session_id: str) -> None:
        task = self._pollers.pop(session_id, None)
        self._poll_credentials.pop(session_id, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def release_session(self, session_id: str | int) -> None:
        """Stop long-poll ownership after a session's local lifecycle ends."""

        await self._stop_poller(str(session_id))

    async def _poll(self, session_id: str) -> None:
        delay = 0.25
        try:
            while self.open and session_id in self._pollers:
                client = self._client
                if client is None:
                    return
                params: dict[str, Any] = {
                    "rid": next(self._rid),
                    "maxev": self._max_poll_events,
                    **self._poll_credentials.get(session_id, {}),
                }
                try:
                    response = await client.get(
                        f"{self._url}/{session_id}",
                        params=params,
                        timeout=self._poll_timeout,
                    )
                    response.raise_for_status()
                    decoded = response.json()
                    messages = decoded if isinstance(decoded, list) else [decoded]
                    poll_error = False
                    for item in messages:
                        parsed = parse_janus_response(item)
                        await self._handle_response(parsed)
                        if parsed.janus == "error":
                            error_payload = getattr(parsed, "error", None)
                            if error_payload is not None and error_payload.code == 458:
                                return
                            poll_error = True
                        if isinstance(parsed, TimeoutResponse):
                            return
                    if poll_error:
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 5.0)
                    else:
                        delay = 0.25
                        if not messages:
                            await asyncio.sleep(0.05)
                except asyncio.CancelledError:
                    raise
                except (httpx.HTTPError, ValueError, JanusProtocolError) as exc:
                    logger.warning(
                        "Long poll failed",
                        "Janus HTTP long poll will retry",
                        context={"error_type": type(exc).__name__},
                    )
                    self._metrics["errors"] += 1
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 5.0)
        finally:
            if self._pollers.get(session_id) is asyncio.current_task():
                self._pollers.pop(session_id, None)
                self._poll_credentials.pop(session_id, None)

    @staticmethod
    def _credentials(request: JanusRequest) -> dict[str, str]:
        credentials = {}
        if request.token:
            credentials["token"] = request.token
        if request.apisecret:
            credentials["apisecret"] = request.apisecret
        return credentials

    async def _handle_response(self, response: JanusResponse) -> None:
        self._metrics["received"] += 1
        started = time.perf_counter()
        await self._dispatcher.dispatch(
            response,
            session_id=response.session_id,
            sender=response.sender,
        )
        if response.janus in self._dispatcher.dispatchable_events:
            self._metrics["events"] += 1
        logger.debug(
            "Transport metric",
            "Janus HTTP response dispatched",
            context={
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
                "janus_type": response.janus,
                "received": self._metrics["received"],
            },
        )

    def _pending_response(self, transaction: str | None) -> tuple[str, _Pending] | None:
        if not transaction:
            return None
        key = str(transaction)
        pending = self._pending.get(key)
        if pending is None or pending.future.done():
            return None
        return key, pending

    async def _resolve_ack(self, response: JanusResponse) -> None:
        entry = self._pending_response(response.transaction)
        if entry is None:
            return
        transaction, pending = entry
        if pending.wait_for_event:
            return
        pending.future.set_result(response)
        self._pending.pop(transaction, None)
        self._metrics["resolved"] += 1

    async def _resolve_error(self, response: JanusResponse) -> None:
        error_payload = getattr(response, "error", None)
        if error_payload is None:
            return
        entry = self._pending_response(response.transaction)
        error = JanusErrorResponse(
            error_payload.code,
            error_payload.reason,
            transaction=str(response.transaction) if response.transaction else None,
            response=response,
        )
        if entry is not None:
            transaction, pending = entry
            pending.future.set_exception(error)
            self._pending.pop(transaction, None)
        else:
            logger.warning(
                "Janus error response",
                "Received an uncorrelated Janus HTTP error",
                context={"code": error_payload.code},
            )
        self._metrics["errors"] += 1

    async def _resolve_transaction(self, response: JanusResponse) -> None:
        entry = self._pending_response(response.transaction)
        if entry is not None:
            transaction, pending = entry
            pending.future.set_result(response)
            self._pending.pop(transaction, None)
            self._metrics["resolved"] += 1
        if response.janus in self._dispatcher.dispatchable_events:
            await self._notify_message(response)

    async def _notify_message(self, response: JanusResponse) -> None:
        for listener in tuple(self._message_listeners):
            try:
                result = listener(response)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                logger.error(
                    "Local listener failed",
                    "Janus HTTP message listener raised",
                    context={"error_type": type(exc).__name__, "janus_type": response.janus},
                    exc_info=exc,
                )

    async def _notify_close(self, error: BaseException | None) -> None:
        for listener in tuple(self._close_listeners):
            try:
                result = listener(error)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                logger.error(
                    "Local listener failed",
                    "Janus HTTP close listener raised",
                    context={"error_type": type(exc).__name__},
                    exc_info=exc,
                )

    async def __aenter__(self) -> HttpTransportClient:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.stop()
