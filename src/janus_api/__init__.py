"""Janus Core: plugin-agnostic Python bindings for Janus Gateway."""

from importlib.metadata import PackageNotFoundError, version

from janus_api.auth import JanusCredentialProvider, JanusCredentials
from janus_api.lib import Plugin
from janus_api.manager import JanusSessionManager
from janus_api.messaging import (
    JanusEventPublisher,
    JanusResponseDispatcher,
    LogVistaMetrics,
    create_broker,
)
from janus_api.models import JanusRequest, JanusResponse
from janus_api.session import JanusSession, SessionState, WebsocketSession
from janus_api.transport import JanusTransport, WebsocketTransportClient

try:
    __version__ = version("janus-api-core")
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
