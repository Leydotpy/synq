"""Application configuration for the meetings domain."""

from django.apps import AppConfig


class MeetingsConfig(AppConfig):
    """Register the meetings app and expose a friendly application label."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.meetings"
    verbose_name = "Meetings"
