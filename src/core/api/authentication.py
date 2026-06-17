"""Authentication classes shared by first-party Django REST Framework APIs."""

from __future__ import annotations

from django.contrib.auth import get_user as get_session_user
from django_clerk_sdk.core.auth.clerk.authentication import ClerkAuthentication
from django_clerk_sdk.core.auth.clerk.service import has_clerk_credentials
from rest_framework.authentication import BaseAuthentication, SessionAuthentication


class SessionOrClerkAuthentication(BaseAuthentication):
    """Route API authentication between Django sessions and Clerk credentials.

    Behavior:
    1. If the request carries a Bearer token, authenticate with Clerk.
    2. If Clerk credentials are present but CSRF artifacts are absent, use Clerk
       directly (typical frontend/third-party token-cookie traffic).
    3. Otherwise, resolve Django session auth for backend/admin usage.
    4. If no session user is found, fall back to Clerk.

    Why this is hardened for ``ClerkMiddleware``:
    - ``ClerkMiddleware`` may override ``request.user`` when Clerk credentials
      are present.
    - Session authentication here reads the authenticated session user directly
      from Django's session/auth backend instead of trusting ``request.user``.
    """

    def __init__(self) -> None:
        self._session_auth = SessionAuthentication()
        self._clerk_auth = ClerkAuthentication()

    @staticmethod
    def _has_bearer_token(request) -> bool:
        """Return whether the request includes an Authorization Bearer token."""

        authorization = (request.META.get("HTTP_AUTHORIZATION") or "").strip()
        return authorization.lower().startswith("bearer ")

    @staticmethod
    def _has_csrf_artifacts(request) -> bool:
        """Return whether request carries typical CSRF cookie/header artifacts."""

        csrf_cookie = request.COOKIES.get("csrftoken")
        csrf_header = (
            request.META.get("HTTP_X_CSRFTOKEN")
            or request.META.get("HTTP_X_CSRF_TOKEN")
        )
        return bool(csrf_cookie or csrf_header)

    def _authenticate_session(self, request):
        """Authenticate using Django session state, independent of request.user."""

        django_request = getattr(request, "_request", request)
        user = get_session_user(django_request)
        if not user or not user.is_active:
            return None

        # Keep DRF's CSRF behavior for session-authenticated requests.
        self._session_auth.enforce_csrf(request)
        return user, None

    def authenticate(self, request):
        """Authenticate request users according to request context."""

        if self._has_bearer_token(request):
            return self._clerk_auth.authenticate(request)

        has_clerk_creds = has_clerk_credentials(request)

        # Frontends/third parties often send Clerk credentials without CSRF
        # headers. In that case, avoid session auth to prevent CSRF coupling.
        if has_clerk_creds and not self._has_csrf_artifacts(request):
            return self._clerk_auth.authenticate(request)

        session_result = self._authenticate_session(request)
        if session_result is not None:
            return session_result

        if has_clerk_creds:
            return self._clerk_auth.authenticate(request)
        return None

    def authenticate_header(self, request):
        """Return a challenge header compatible with token-based auth clients."""

        clerk_header = self._clerk_auth.authenticate_header(request)
        if clerk_header:
            return clerk_header
        return self._session_auth.authenticate_header(request)
