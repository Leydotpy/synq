"""Janus session types."""

from janus_api.session.base import AbstractBaseSession, SessionState
from janus_api.session.websocket import JanusSession, WebsocketSession


def __getattr__(name: str):
    """Resolve the manager lazily to avoid a session-package import cycle."""

    if name == "JanusSessionManager":
        from janus_api.manager import JanusSessionManager

        return JanusSessionManager
    raise AttributeError(name)


__all__ = (
    "AbstractBaseSession",
    "JanusSession",
    "JanusSessionManager",
    "SessionState",
    "WebsocketSession",
)
