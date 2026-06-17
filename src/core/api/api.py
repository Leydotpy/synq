"""Shared REST API helpers used across the billing and membership endpoints."""

from __future__ import annotations

from rest_framework import pagination
from rest_framework.exceptions import NotAuthenticated

from core.utils import first_non_empty
from apps.profiles.models import Profile


class StandardResultsSetPagination(pagination.PageNumberPagination):
    """Conservative default pagination for list endpoints on a consumer-facing API."""

    page_size = 20
    page_size_query_param = "page_size"
    max_page_size = 100


def _get_or_create_profile_for_user(user) -> Profile:
    """Resolve a Django auth user into the domain-level ``Profile`` record."""

    profile, _ = Profile.objects.get_or_create(
        user=user,
        defaults={
            "display_name": first_non_empty(
                getattr(user, "get_full_name", lambda: "")(),
                getattr(user, "username", ""),
                getattr(user, "email", "").split("@")[0] if getattr(user, "email", "") else "",
                "Listener",
            )
        },
    )
    return profile


class CurrentProfileMixin:
    """Resolve the authenticated Django user into the domain-level ``Profile`` object."""

    def get_profile(self) -> Profile:
        """Return the current request's profile or raise when unauthenticated."""

        user = self.request.user
        if not user or not user.is_authenticated:
            raise NotAuthenticated("Authentication is required for this endpoint.")

        return _get_or_create_profile_for_user(user)


class OptionalCurrentProfileMixin:
    """Return a profile when the request is authenticated and ``None`` otherwise."""

    def get_optional_profile(self) -> Profile | None:
        """Return the current request's profile when available, else ``None``."""

        user = getattr(self.request, "user", None)
        if not user or not user.is_authenticated:
            return None
        return _get_or_create_profile_for_user(user)
