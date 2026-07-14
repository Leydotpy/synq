"""Shared Socket.IO server and ASGI application wiring for the project."""

from __future__ import annotations

import socketio
from django.conf import settings

_socket_server: socketio.AsyncServer | None = None
_socket_application: socketio.ASGIApp | None = None


def build_client_manager():
    """Build a Redis-backed Socket.IO client manager when a message queue URL is configured."""

    if not settings.SOCKET_IO_REDIS_URL:
        return None
    return socketio.AsyncRedisManager(settings.SOCKET_IO_REDIS_URL)


def get_socket_server() -> socketio.AsyncServer:
    """Return the shared Socket.IO server instance for the current process."""

    global _socket_server
    if _socket_server is None:
        from apps.meetings.realtime.namespace import MeetingNamespace

        _socket_server = socketio.AsyncServer(
            async_mode="asgi",
            client_manager=build_client_manager(),
            cors_allowed_origins=settings.SOCKET_IO_CORS_ALLOWED_ORIGINS,
            logger=getattr(settings, "SOCKET_IO_LOGGER_ENABLED", False),
            engineio_logger=getattr(settings, "SOCKET_IO_ENGINEIO_LOGGER_ENABLED", False),
        )
        _socket_server.register_namespace(MeetingNamespace("/meetings"))
    return _socket_server


def get_socket_application():
    """Wrap Django's ASGI application with the shared Socket.IO ASGI app."""

    global _socket_application
    if _socket_application is None:
        _socket_application = socketio.ASGIApp(
            socketio_server=get_socket_server(),
            socketio_path=None,
        )
    return _socket_application
