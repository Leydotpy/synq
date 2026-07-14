"""Socket.IO authentication helpers that reuse Django sessions and Clerk credentials."""

from __future__ import annotations

from http import cookies

from django.conf import settings
from django.contrib.auth import SESSION_KEY, get_user_model
from django.contrib.sessions.backends.db import SessionStore
from django.http import HttpRequest
from django_clerk_sdk.core.auth.clerk.service import resolve_clerk_auth

from core.api.authentication import forget_clerk_token, token_is_expired


def extract_scope_headers(environ: dict) -> dict[str, str]:
    """Return lowercase ASGI request headers from a Socket.IO environment payload."""

    scope = environ.get("asgi.scope", {})
    raw_headers = scope.get("headers", [])
    return {key.decode("latin1").lower(): value.decode("latin1") for key, value in raw_headers}


def extract_ip_address(environ: dict) -> str | None:
    """Return the best-effort client IP address from an ASGI scope."""

    scope = environ.get("asgi.scope", {})
    client = scope.get("client")
    if isinstance(client, (list, tuple)) and client:
        return client[0]
    return None


def _extract_cookie_jar(headers: dict[str, str]) -> dict[str, str]:
    """Parse the Cookie header into a plain dictionary."""

    cookie_header = headers.get("cookie", "")
    if not cookie_header:
        return {}
    jar = cookies.SimpleCookie()
    jar.load(cookie_header)
    return {key: morsel.value for key, morsel in jar.items()}


def _build_clerk_request(environ: dict, auth: dict | None = None) -> HttpRequest:
    """Construct a minimal ``HttpRequest`` for Clerk SDK request authentication."""

    headers = extract_scope_headers(environ)
    auth = auth or {}
    request = HttpRequest()
    request.method = "GET"
    request.path = auth.get("path", "/socket.io/")
    request.META = {}
    for key, value in headers.items():
        header_name = f"HTTP_{key.upper().replace('-', '_')}"
        request.META[header_name] = value
    if auth.get("token"):
        request.META["HTTP_AUTHORIZATION"] = f"Bearer {auth['token']}"
    request.COOKIES = _extract_cookie_jar(headers)
    if auth.get("session_token"):
        request.COOKIES["__session"] = auth["session_token"]
    # request.scheme = "https"
    return request


def resolve_socket_user(environ: dict, auth: dict | None = None):
    """Resolve an authenticated Django user from Socket.IO cookies, session auth, or Clerk auth."""

    auth = auth or {}
    session_key = auth.get("session_key")
    headers = extract_scope_headers(environ)
    if not session_key:
        session_cookie_name = settings.SESSION_COOKIE_NAME
        session_key = _extract_cookie_jar(headers).get(session_cookie_name)
    if session_key:
        session_store = SessionStore(session_key=session_key)
        user_id = session_store.get(SESSION_KEY)
        if user_id:
            user = get_user_model().objects.filter(pk=user_id, is_active=True).first()
            if user is not None:
                return user

    clerk_request = _build_clerk_request(environ, auth)
    clerk_token = (
        auth.get("token")
        or auth.get("session_token")
        or clerk_request.COOKIES.get("__session")
    )
    if clerk_token and token_is_expired(str(clerk_token)):
        forget_clerk_token(str(clerk_token))
        return None
    clerk_result = resolve_clerk_auth(clerk_request)
    if getattr(clerk_result.user, "is_authenticated", False):
        return clerk_result.user
    return None
