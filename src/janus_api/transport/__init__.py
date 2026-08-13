"""Janus transport interfaces and built-in clients."""

from typing import TYPE_CHECKING

from janus_api.transport.base import JanusTransport
from janus_api.transport.websocket import WebsocketTransportClient

if TYPE_CHECKING:
    from janus_api.transport.http import HttpTransportClient as HttpTransportClient


def __getattr__(name: str):
    if name == "HttpTransportClient":
        from janus_api.transport.http import HttpTransportClient

        return HttpTransportClient
    raise AttributeError(name)


# Optional transports remain explicitly importable through ``__getattr__`` but
# are excluded from star imports so a base install does not require ``httpx``.
__all__ = ("JanusTransport", "WebsocketTransportClient")
