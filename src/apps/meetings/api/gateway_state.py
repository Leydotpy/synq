"""Authenticated ASGI SSE and paginated gateway snapshots (opt-in)."""

import asyncio
from importlib import import_module
import json
import math
import threading
import time
import uuid

import jwt
from asgiref.sync import sync_to_async
from django.conf import settings
from django.contrib.auth import get_user
from django.db import close_old_connections
from django.http import HttpRequest, StreamingHttpResponse
from rest_framework import exceptions
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.api.api import CurrentProfileMixin
from apps.meetings.services.gateway_state import next_update, snapshot
from apps.meetings.services.gateway_tickets import GatewayDenied

POLL_SECONDS = 1
KEEPALIVE_SECONDS = 15
STREAM_SECONDS = 60
MAX_FRAME_BYTES = 128 * 1024


class StreamSlots:
    """Bounded per-ASGI-worker streams, released even before body iteration."""

    lock = threading.Lock()
    active = {}

    @classmethod
    def acquire(cls, user_id):
        with cls.lock:
            if sum(cls.active.values()) >= 32 or cls.active.get(user_id, 0) >= 4:
                raise exceptions.Throttled(wait=STREAM_SECONDS)
            cls.active[user_id] = cls.active.get(user_id, 0) + 1
        released = False

        def release():
            nonlocal released
            with cls.lock:
                if not released:
                    released = True
                    remaining = cls.active[user_id] - 1
                    if remaining:
                        cls.active[user_id] = remaining
                    else:
                        del cls.active[user_id]

        return release


class StateStreamResponse(StreamingHttpResponse):
    def __init__(self, iterator, release):
        self.release = release
        super().__init__(iterator, content_type="text/event-stream")

    def close(self):
        self.release()
        super().close()


def session_valid(session_key, user_id):
    request = HttpRequest()
    request.session = import_module(settings.SESSION_ENGINE).SessionStore(session_key)
    user = get_user(request)
    return user.is_authenticated and user.is_active and user.pk == user_id


def credentials(request):
    """Existing auth verifies credentials; the stream can only shorten its life."""
    deadline = time.time() + STREAM_SECONDS
    if request.auth:
        # Never use unverified claims to establish identity/permissions. The
        # already-authenticated token's expiry only caps this stream's deadline.
        try:
            expiry = jwt.decode(str(request.auth), options={"verify_signature": False})[
                "exp"
            ]
            if (
                not isinstance(expiry, (int, float))
                or isinstance(expiry, bool)
                or not math.isfinite(expiry)
            ):
                raise ValueError
            deadline = min(deadline, expiry)
        except (jwt.PyJWTError, KeyError, TypeError, ValueError):
            raise exceptions.AuthenticationFailed(
                "Invalid stream credential."
            ) from None
        session_key = None
    else:
        session_key = request._request.session.session_key
        if not session_key or not session_valid(session_key, request.user.pk):
            raise exceptions.AuthenticationFailed("Session expired.")
    if deadline <= time.time():
        raise exceptions.AuthenticationFailed("Session expired.")
    return deadline, session_key


def encode_frame(update):
    event, identity, payload = update
    frame = f"id: {identity}\nevent: {event}\ndata: {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}\n\n".encode()
    if len(frame) > MAX_FRAME_BYTES:
        raise GatewayDenied("UNAVAILABLE", 503)
    return frame


def pull_update(session_id, profile_id, user_id, session_key, cursor, replay):
    try:
        if session_key and not session_valid(session_key, user_id):
            raise GatewayDenied()
        return next_update(session_id, profile_id, cursor, replay=replay)
    finally:
        # No idle DB connection across the stream's wait/backpressure interval;
        # retain the repository's CONN_MAX_AGE=0 policy.
        close_old_connections()


async def state_stream(
    session_id, profile_id, user_id, cursor, deadline, session_key, release
):
    heartbeat = time.monotonic()
    replay = bool(cursor)
    try:
        yield b"retry: 1000\n\n"
        while time.time() < deadline:
            update = await sync_to_async(pull_update)(
                session_id, profile_id, user_id, session_key, cursor, replay
            )
            # Recheck deadline after a potentially delayed database operation.
            if time.time() >= deadline:
                return
            if update:
                cursor = update[1]
                yield encode_frame(update)
                heartbeat = time.monotonic()
            else:
                replay = False
                if time.monotonic() - heartbeat >= KEEPALIVE_SECONDS:
                    yield b": keepalive\n\n"
                    heartbeat = time.monotonic()
                await asyncio.sleep(min(POLL_SECONDS, max(0, deadline - time.time())))
    except GatewayDenied:
        # Reconnect must authenticate again; no stale/private payload on denial.
        return
    finally:
        release()


class GatewaySnapshotView(CurrentProfileMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, session_id):
        try:
            if request.auth:
                credentials(request)  # Also reject expired cached bearer credentials.
            if set(request.query_params) - {"after", "version"}:
                raise ValueError
            after = request.query_params.get("after", "")
            version = request.query_params.get("version")
            if after:
                after = str(uuid.UUID(after))
                if version is None:
                    raise ValueError
            if version is not None:
                if not version.isdecimal() or len(version) > 16:
                    raise ValueError
                version = int(version)
            payload = snapshot(
                session_id, self.get_profile().pk, after=after, version=version
            )
            return Response(payload, headers={"Cache-Control": "no-store"})
        except ValueError:
            raise exceptions.ValidationError("Invalid snapshot cursor.") from None
        except GatewayDenied as exc:
            return Response(
                {"code": exc.code},
                status=exc.status,
                headers={"Cache-Control": "no-store"},
            )


class GatewayStateStreamView(CurrentProfileMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, session_id):
        if request.query_params:
            raise exceptions.ValidationError(
                "Use credentials and Last-Event-ID headers."
            )
        cursor = request.headers.get("Last-Event-ID", "")
        if len(cursor) > 80:
            raise exceptions.ValidationError("Invalid event cursor.")
        deadline, session_key = credentials(request)
        profile_id = self.get_profile().pk
        try:
            # Authorize before HTTP 200; do not retain this possibly stale data.
            snapshot(session_id, profile_id)
        except GatewayDenied as exc:
            return Response(
                {"code": exc.code},
                status=exc.status,
                headers={"Cache-Control": "no-store"},
            )
        release = StreamSlots.acquire(request.user.pk)
        response = StateStreamResponse(
            state_stream(
                session_id,
                profile_id,
                request.user.pk,
                cursor,
                deadline,
                session_key,
                release,
            ),
            release,
        )
        response["Cache-Control"] = "no-store"
        response["X-Accel-Buffering"] = "no"
        return response
