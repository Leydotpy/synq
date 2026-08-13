"""Janus session types."""

from jrtc.session.base import AbstractBaseSession, SessionState
from jrtc.session.websocket import JanusSession, WebsocketSession


def __getattr__(name: str):
    """Resolve the manager lazily to avoid a session-package import cycle."""

    if name == "JanusSessionManager":
        from jrtc.manager import JanusSessionManager

        return JanusSessionManager
    raise AttributeError(name)


__all__ = (
    "AbstractBaseSession",
    "JanusSession",
    "JanusSessionManager",
    "SessionState",
    "WebsocketSession",
)
