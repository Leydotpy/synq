from __future__ import annotations

from http.cookies import CookieError, SimpleCookie

from django.conf import settings
from django.middleware.csrf import get_token

from core.api.clerk_authentication import (
    forget_clerk_token,
    get_bearer_token,
    mark_request_anonymous_for_expired_clerk_token,
    token_is_expired,
)


class EnsureApiCsrfCookieMiddleware:
    """Ensure browser clients receive a CSRF cookie before unsafe API calls."""

    SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}
    CSRF_PATH_PREFIXES = ("/api/rest/", "/admin/")

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.method in self.SAFE_METHODS and request.path.startswith(self.CSRF_PATH_PREFIXES):
            get_token(request)

        return self.get_response(request)


class StripClerkSessionCookieMiddleware:
    """
    Keep browser Clerk cookies from being treated as Django API credentials.

    On localhost, cookies are shared across ports. That means Clerk's
    ``__session`` cookie from the Next.js apps can be sent to Django admin and
    public API requests. The API authenticates Clerk users with bearer tokens,
    so stripping these cookies avoids stale-cookie 403s and admin timeouts.
    """

    CLERK_SESSION_COOKIE_PREFIX = "__session"

    def __init__(self, get_response):
        self.get_response = get_response

    @classmethod
    def _strip_raw_cookie_header(cls, raw_cookie: str | None) -> str | None:
        if not raw_cookie:
            return raw_cookie

        try:
            parsed = SimpleCookie()
            parsed.load(raw_cookie)
        except CookieError:
            cookies = []
            for cookie in raw_cookie.split(";"):
                name = cookie.strip().split("=", 1)[0]
                if name and not name.startswith(cls.CLERK_SESSION_COOKIE_PREFIX):
                    cookies.append(cookie.strip())
            return "; ".join(cookies)

        return "; ".join(
            f"{name}={morsel.value}"
            for name, morsel in parsed.items()
            if not name.startswith(cls.CLERK_SESSION_COOKIE_PREFIX)
        )

    def __call__(self, request):
        if not getattr(settings, "CLERK_ACCEPT_BROWSER_SESSION_COOKIES", False):
            for name in list(request.COOKIES):
                if name.startswith(self.CLERK_SESSION_COOKIE_PREFIX):
                    request.COOKIES.pop(name, None)

            raw_cookie = request.META.get("HTTP_COOKIE")
            stripped_cookie = self._strip_raw_cookie_header(raw_cookie)
            if stripped_cookie != raw_cookie:
                if stripped_cookie:
                    request.META["HTTP_COOKIE"] = stripped_cookie
                else:
                    request.META.pop("HTTP_COOKIE", None)

        return self.get_response(request)


class RejectExpiredClerkAuthorizationMiddleware:
    """
    Prevent expired Clerk bearer tokens from falling back to Django sessions.

    The Clerk SDK caches token-to-user lookups. Before the SDK middleware sees a
    request, detect bearer tokens whose JWT ``exp`` has already passed, delete
    the corresponding SDK cache entry, and force the request into an anonymous
    state. Public routes can continue as anonymous; guarded routes still fail
    through their normal permission classes.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        token = get_bearer_token(request)

        if token and token_is_expired(token):
            forget_clerk_token(token)
            mark_request_anonymous_for_expired_clerk_token(request)

        return self.get_response(request)
