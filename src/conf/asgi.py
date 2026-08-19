"""ASGI entrypoint for Django, Socket.IO, and the process-local Janus runtime."""

import os

from django.conf import settings
from django.core.asgi import get_asgi_application
from starlette.applications import Starlette
from starlette.routing import Mount

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "conf.settings")


def _normalize_mount_path(raw_path: str | None, *, default: str = "/socket.io") -> str:
    """Return a Starlette mount path with a single leading slash.

    ``SOCKETIO_PATH`` may be configured as ``socket.io``, ``/socket.io``, or
    another deployment-specific value. The ASGI router, not the Socket.IO app,
    owns this public path, so it is normalized once here into a format suitable
    for a Starlette ``Mount`` route.
    """

    normalized = (raw_path or "").strip()
    if not normalized:
        return default
    return f"/{normalized.strip('/')}"


django_application = get_asgi_application()
from conf.socketio import get_socket_application
from apps.meetings.services.janus import janus_runtime

socket_io_application = get_socket_application()

socketio_mount_path = _normalize_mount_path(
    getattr(settings, "SOCKET_IO_PATH", "socket.io"),
)

application = Starlette(
    debug=bool(settings.DEBUG),
    routes=[
        Mount(socketio_mount_path, app=socket_io_application, name="socket.io"),
        Mount("/", app=django_application, name="django"),
    ],
    lifespan=janus_runtime.lifespan,
)
