"""ASGI entrypoint that serves Django HTTP routes and Socket.IO via ``janus_api``."""

import asyncio
import os
from contextlib import asynccontextmanager

from django.conf import settings
from django.core.asgi import get_asgi_application
from janus_api import create_asgi_app
# janus_api.conf.settings is also the name of a Python module. Importing it
# through the package can therefore return that module instead of the mutable
# runtime proxy used by JanusSessionManager.
from janus_api.conf._config import settings as janus_settings

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

# janus_api's built-in leader proxy forwards session methods but cannot carry
# stateful plugin objects/handles. Each application process therefore owns a
# real Janus session; persisted session ids below let a different process
# detect and replace stale handles safely.
janus_settings.LEADER_MODE = False
janus_settings.JANUS_ENABLE_ADMIN = bool(settings.JANUS_ENABLE_ADMIN)
janus_settings.JANUS_ENABLE_EVENTS = bool(settings.JANUS_ENABLE_EVENTS)

application = create_asgi_app(
    debug=bool(settings.DEBUG),
    mount_rest_api=False,
    # The dependency's colored logger repeatedly wraps stdout on Windows and
    # can recurse while Django renders an exception. Uvicorn/Django already
    # provide process logging, so keep that global mutation disabled here.
    initialize_logging=False,
    routes=[
        {"path": socketio_mount_path, "app": socket_io_application},
        {"path": "/", "app": django_application},
    ],
)


# Janus sessions and their aiohttp/websocket transports belong to the ASGI
# lifespan loop. Meeting services execute in Django worker threads, so they
# must submit awaitables back to this owning loop instead of creating a fresh
# temporary event loop for every plugin call.
from apps.meetings.services.janus import (
    register_janus_event_loop,
    unregister_janus_event_loop,
)

_janus_lifespan = application.router.lifespan_context


@asynccontextmanager
async def _coordinated_janus_lifespan(app):
    loop = asyncio.get_running_loop()
    register_janus_event_loop(loop)
    try:
        async with _janus_lifespan(app) as state:
            yield state
    finally:
        unregister_janus_event_loop(loop)


application.router.lifespan_context = _coordinated_janus_lifespan


@asynccontextmanager
async def _application_only_lifespan(_app):
    """Run HTTP and Socket.IO without Janus for explicit degraded/local mode."""

    yield {}


if not getattr(settings, "JANUS_ENABLED", True):
    application.router.lifespan_context = _application_only_lifespan
