from importlib import import_module
import hashlib
import time
from typing import Any

from django.apps import apps
from django.conf import settings
from django.core.cache import cache
from rest_framework.authentication import BaseAuthentication, SessionAuthentication

import jwt


AUTHORIZATION_META_KEYS = ("HTTP_AUTHORIZATION", "Authorization")
CLERK_AUTH_CACHE_PREFIX = "clerk:auth:"
DEFAULT_TOKEN_EXPIRY_LEEWAY_SECONDS = 0


def get_bearer_token(request: Any) -> str | None:
    django_request = getattr(request, "_request", request)
    headers = getattr(django_request, "headers", {})
    authorization = headers.get("Authorization")

    if not authorization:
        for key in AUTHORIZATION_META_KEYS:
            authorization = getattr(django_request, "META", {}).get(key)
            if authorization:
                break

    if not authorization:
        return None

    scheme, _, credentials = str(authorization).partition(" ")
    if scheme.lower() != "bearer":
        return None

    token = credentials.strip()
    return token or None


def get_clerk_token_cache_key(token: str) -> str:
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return f"{CLERK_AUTH_CACHE_PREFIX}{token_hash}"


def forget_clerk_token(token: str) -> None:
    cache.delete(get_clerk_token_cache_key(token))


def token_is_expired(token: str, *, now: float | None = None) -> bool:
    try:
        payload = jwt.decode(
            token,
            options={
                "verify_signature": False,
                "verify_exp": False,
                "verify_aud": False,
                "verify_iss": False,
            },
        )
    except jwt.PyJWTError:
        return False

    exp = payload.get("exp") if isinstance(payload, dict) else None
    if exp is None:
        return False

    try:
        expires_at = float(exp)
    except (TypeError, ValueError):
        return False

    leeway = getattr(
        settings,
        "CLERK_TOKEN_EXPIRY_LEEWAY_SECONDS",
        DEFAULT_TOKEN_EXPIRY_LEEWAY_SECONDS,
    )
    return (now if now is not None else time.time()) >= expires_at + float(leeway)


def request_has_expired_clerk_token(request: Any) -> bool:
    django_request = getattr(request, "_request", request)
    return bool(getattr(django_request, "_expired_clerk_token", False))


def mark_request_anonymous_for_expired_clerk_token(request: Any) -> None:
    from django.contrib.auth.models import AnonymousUser

    django_request = getattr(request, "_request", request)
    django_request._expired_clerk_token = True
    django_request.user = AnonymousUser()

    for key in AUTHORIZATION_META_KEYS:
        getattr(django_request, "META", {}).pop(key, None)

    getattr(django_request, "__dict__", {}).pop("headers", None)


class LazyClerkAuthentication(BaseAuthentication):
    """
    Defer django-clerk-sdk imports until request authentication time.

    django-clerk-sdk imports django.contrib.auth.models at module import time,
    but DRF may import DEFAULT_AUTHENTICATION_CLASSES while Django is still
    populating apps. Delaying the SDK import avoids AppRegistryNotReady during
    startup while preserving the SDK's runtime behavior.
    """

    _backend: Any = None

    @classmethod
    def _get_backend(cls) -> Any:
        if cls._backend is None:
            if not apps.ready:
                return None

            module = import_module(
                "django_clerk_sdk.core.auth.clerk.authentication"
            )
            cls._backend = module.ClerkAuthentication()
        return cls._backend

    def authenticate(self, request):
        if request_has_expired_clerk_token(request):
            return None

        token = get_bearer_token(request)
        if token and token_is_expired(token):
            forget_clerk_token(token)
            mark_request_anonymous_for_expired_clerk_token(request)
            return None

        backend = self._get_backend()
        if backend is None:
            return None

        return backend.authenticate(request)

    def authenticate_header(self, request):
        backend = self._get_backend()
        if backend is None:
            return "Bearer"

        return backend.authenticate_header(request)


class SessionOrClerkAuthentication(BaseAuthentication):
    """Authenticate first-party API requests with either sessions or Clerk."""

    def __init__(self) -> None:
        self._session_auth = SessionAuthentication()
        self._clerk_auth = LazyClerkAuthentication()

    def authenticate(self, request):
        if get_bearer_token(request):
            return self._clerk_auth.authenticate(request)

        session_result = self._session_auth.authenticate(request)
        if session_result is not None:
            return session_result

        return self._clerk_auth.authenticate(request)

    def authenticate_header(self, request):
        return self._clerk_auth.authenticate_header(request)
