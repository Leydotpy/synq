"""Signal handlers that keep auth users and profile records in sync."""

from django.db.models.signals import post_save
from django.dispatch import receiver
from django_clerk_sdk.core.compat import get_user_model

from core.utils import first_non_empty
from .models import Profile


@receiver(post_save, sender=get_user_model())
def ensure_profile_exists(sender, instance, created, **kwargs):
    """Create a profile shell whenever a new auth user record is created."""

    if not created:
        return

    display_name = first_non_empty(
        getattr(instance, "get_full_name", lambda: "")(),
        getattr(instance, "username", ""),
        getattr(instance, "email", "").split("@")[0] if getattr(instance, "email", "") else "",
        "Listener",
    )

    Profile.objects.get_or_create(
        user=instance,
        defaults={
            "display_name": display_name,
            "handle": "",
        },
    )
