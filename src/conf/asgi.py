"""ASGI entrypoint that serves Django HTTP routes and Socket.IO via ``janus_api``."""

import os

from django.conf import settings
from django.core.asgi import get_asgi_application
from janus_api import create_asgi_app

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "conf.settings")


def _normalize_mount_path(raw_path: str | None, *, default: str = "/socket.io") -> str:
    """Return a Starlette mount path with a single leading slash.

    ``SOCKETIO_PATH`` may be configured as ``socket.io``, ``/socket.io``, or
    another deployment-specific value. The ASGI router, not the Socket.IO app,
    owns this public path, so it is normalized once here into a format suitable
    for ``create_asgi_app(routes=[...])``.
    """

    normalized = (raw_path or "").strip()
    if not normalized:
        return default
    return f"/{normalized.strip('/')}"


django_application = get_asgi_application()
from conf.socketio import get_socket_application

socket_io_application = get_socket_application()

socketio_mount_path = _normalize_mount_path(
    getattr(settings, "SOCKET_IO_PATH", "socket.io"),
)

application = create_asgi_app(
    debug=bool(settings.DEBUG),
    mount_rest_api=False,
    routes=[
        {"path": socketio_mount_path, "app": socket_io_application},
        {"path": "/", "app": django_application},
    ],
)
