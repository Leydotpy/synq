"""Transport contracts shared by Janus sessions.

Inbound Janus events use small typed listener hooks.  Cross-process delivery is
owned by :mod:`jrtc.messaging`; the hooks here only maintain the local
session and plugin-handle lifecycle.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from jrtc.models import JanusRequest, JanusResponse

MessageListener = Callable[[JanusResponse], Awaitable[None] | None]
CloseListener = Callable[[BaseException | None], Awaitable[None] | None]


@runtime_checkable
class JanusTransport(Protocol):
    """Minimal contract implemented by bidirectional Janus transports."""

    @property
    def open(self) -> bool: ...

    def add_message_listener(self, listener: MessageListener) -> None: ...

    def remove_message_listener(self, listener: MessageListener) -> None: ...

    def add_close_listener(self, listener: CloseListener) -> None: ...

    def remove_close_listener(self, listener: CloseListener) -> None: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send(
        self,
        message: JanusRequest,
        *,
        timeout: float | None = None,
        wait_for_event: bool = False,
    ) -> JanusResponse: ...


__all__ = ["CloseListener", "JanusTransport", "MessageListener"]
