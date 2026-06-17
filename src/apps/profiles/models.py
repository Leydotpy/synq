"""Models for public-facing user profiles used across the platform."""

from __future__ import annotations

from django.db import models
from django.db.models.functions import Lower
from django_clerk_sdk.core.compat import AUTH_USER_MODEL

from core.models.common import UUIDTimestampedModel
from core.utils import first_non_empty, generate_unique_slug


class ProfileQuerySet(models.QuerySet):
    """Common profile filters used by creator and audience features."""

    def active(self):
        """Return only profiles that have not been deactivated."""

        return self.filter(is_active=True)


class Profile(UUIDTimestampedModel):
    """Public identity record linked one-to-one with Django's auth user.

    Fields:
        user: Underlying authentication user that owns the profile.
        handle: Public slug used in URLs and mentions.
        display_name: Human-readable name shown in UI.
        bio: Short self-description capped for profile pages.
        avatar_url: URL of the user's profile image.
        cover_image_url: URL of the banner or cover image.
        website_url: Optional external link for the user's website.
        country_code: ISO country code for localization and discovery.
        city: Free-form city name for local context.
        timezone: Preferred timezone for schedules and notifications.
        preferred_language: Default content language preference.
        is_active: Soft toggle for hiding or disabling the profile.
        allow_public_profile: Whether the profile should be publicly discoverable.
        allow_live_notifications: Whether product notifications may be sent.
        last_seen_at: Most recent activity heartbeat for presence-like features.
        metadata: Flexible JSON store for app-specific extensions.
    """

    # One-to-one link to the Django auth user this public profile belongs to.
    user = models.OneToOneField(
        AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="profile",
    )
    # Public slug shown in URLs and mentions and used externally as the main identifier.
    handle = models.SlugField(
        max_length=32,
        unique=True,
        help_text="A short public identifier used in profile and channel URLs.",
    )
    # Display name shown in the UI, separate from the auth username.
    display_name = models.CharField(max_length=120)
    # Optional biography text rendered on public profile screens.
    bio = models.TextField(max_length=500, blank=True)
    # Avatar image URL used in comments, lists, and profile pages.
    avatar_url = models.URLField(max_length=500, blank=True)
    # Cover/banner image URL used on profile headers.
    cover_image_url = models.URLField(max_length=500, blank=True)
    # Optional website or external link associated with the profile.
    website_url = models.URLField(max_length=500, blank=True)
    # ISO country code used for localization and discovery.
    country_code = models.CharField(max_length=2, blank=True)
    # Optional city value used for local context and discovery.
    city = models.CharField(max_length=120, blank=True)
    # Preferred timezone used for schedules, notifications, and presentation.
    timezone = models.CharField(max_length=64, blank=True)
    # Preferred language code used when tailoring content and text output.
    preferred_language = models.CharField(max_length=16, default="en")
    # Soft-delete style toggle that controls whether the profile is considered active.
    is_active = models.BooleanField(default=True, db_index=True)
    # Controls whether the profile should be publicly browsable.
    allow_public_profile = models.BooleanField(default=True)
    # Controls whether the user wants notifications about live events.
    allow_live_notifications = models.BooleanField(default=True)
    # Stores the latest known activity timestamp for presence-like features.
    last_seen_at = models.DateTimeField(null=True, blank=True, db_index=True)
    # Flexible JSON payload for app-specific profile extensions.
    metadata = models.JSONField(default=dict, blank=True)

    objects = ProfileQuerySet.as_manager()

    class Meta:
        """Declare ordering and case-insensitive uniqueness for public handles."""

        ordering = ["display_name", "handle"]
        constraints = [
            models.UniqueConstraint(
                Lower("handle"),
                name="profiles_profile_handle_ci_unique",
            ),
        ]

    def __str__(self) -> str:
        """Return the best available human-readable profile label."""

        return self.display_name or self.handle

    def clean(self):
        """Run Django's default model validation hooks."""

        super().clean()

    def build_handle_source(self) -> str:
        """Assemble fallback values used when auto-generating a public handle."""

        email = getattr(self.user, "email", "")
        email_local_part = email.split("@")[0] if email else ""
        return first_non_empty(
            self.handle,
            self.display_name,
            getattr(self.user, "username", ""),
            email_local_part,
            f"user-{self.user_id}",
        )

    def save(self, *args, **kwargs):
        """Auto-fill the profile handle and display name when they are blank."""

        if not self.handle:
            self.handle = generate_unique_slug(
                self.__class__,
                self.build_handle_source(),
                slug_field="handle",
                instance=self,
                max_length=32,
            )

        if not self.display_name:
            self.display_name = self.handle

        return super().save(*args, **kwargs)
