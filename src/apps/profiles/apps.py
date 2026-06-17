"""Django app configuration for user-facing profile records."""

from django.apps import AppConfig


class ProfilesConfig(AppConfig):
    """Register profile models and hook up signal-based profile creation."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.profiles"
    label = "profiles"
    verbose_name = "Profiles"

    def ready(self):
        """Import signal handlers once Django's app registry is ready."""

        from . import signals  # noqa: F401
