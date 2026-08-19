"""Service-token helpers for trusted first-party backend integrations."""

from __future__ import annotations

import secrets

from django.conf import settings
from django.contrib.auth import get_user_model
from rest_framework import authentication, exceptions


class ServiceTokenAuthentication(authentication.BaseAuthentication):
    """Authenticate trusted backend services with a shared bearer token.

    This is intentionally narrow and should only be used on internal endpoints.
    Browser-facing APIs should continue to use the normal session/Clerk path.
    """

    keyword = "Bearer"

    def authenticate(self, request):
        configured_token = getattr(settings, "MEET_SERVICE_TOKEN", "")
        if not configured_token:
            return None

        authorization = (request.META.get("HTTP_AUTHORIZATION") or "").strip()
        prefix = f"{self.keyword} "
        if not authorization.startswith(prefix):
            return None

        supplied_token = authorization[len(prefix) :].strip()
        if not secrets.compare_digest(supplied_token, configured_token):
            raise exceptions.AuthenticationFailed("Invalid service token.")

        return self._get_service_user(), {"service": "law_firm_workspace"}

    @staticmethod
    def _get_service_user():
        user_model = get_user_model()
        username = getattr(settings, "MEET_SERVICE_USERNAME", "law-workspace-service")
        email = getattr(settings, "MEET_SERVICE_EMAIL", "law-workspace-service@synq.local")
        defaults = {
            "email": email,
            "is_active": True,
        }
        if any(field.name == "clerk_user_id" for field in user_model._meta.fields):
            defaults["clerk_user_id"] = f"service_{username}"
        user, created = user_model.objects.get_or_create(username=username, defaults=defaults)
        if created:
            user.set_unusable_password()
            user.save(update_fields=["password"])
        return user
