"""Janus Core: plugin-agnostic Python bindings for Janus Gateway."""

from importlib.metadata import PackageNotFoundError, version

from jrtc.auth import JanusCredentialProvider, JanusCredentials
from jrtc.lib import Plugin
from jrtc.manager import JanusSessionManager
from jrtc.messaging import (
    JanusEventPublisher,
    JanusResponseDispatcher,
    LogVistaMetrics,
    create_broker,
)
from jrtc.models import JanusRequest, JanusResponse
from jrtc.session import JanusSession, SessionState, WebsocketSession
from jrtc.transport import JanusTransport, WebsocketTransportClient

try:
    __version__ = version("jrtc")
except PackageNotFoundError:  # source checkout
    __version__ = "3.1.0"


__all__ = (
    "JanusCredentialProvider",
    "JanusCredentials",
    "JanusEventPublisher",
    "JanusRequest",
    "JanusResponse",
    "JanusResponseDispatcher",
    "JanusSession",
    "JanusSessionManager",
    "JanusTransport",
    "LogVistaMetrics",
    "Plugin",
    "SessionState",
    "WebsocketSession",
    "WebsocketTransportClient",
    "create_broker",
)
