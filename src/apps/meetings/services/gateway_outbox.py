"""Only write committed authority intents; never publish inside a transaction."""

from django.conf import settings
from django.db import transaction

from apps.meetings.models import GatewayAuthorizationOutbox, GatewayGrant
from apps.meetings.services.gateway_tickets import GatewayDenied, current_permissions


def enabled(session):
    return (
        getattr(settings, "GATEWAY_ADMISSION_ENABLED", False)
        and session.room.feature_flags.get("gateway_control_plane") is True
    )


def record_authorization_intent(
    *,
    session,
    participant,
    action,
    actor_profile=None,
    reason_code="permission_changed",
):
    if not enabled(session):
        return None
    if not transaction.get_connection().in_atomic_block:
        raise RuntimeError("Authorization intent requires a decision transaction.")
    if participant.session_id != session.pk:
        raise GatewayDenied()
    if action not in {
        "participant.admit",
        "participant.permissions.update",
        "participant.deny",
        "participant.disconnect",
    }:
        raise GatewayDenied("INVALID_REQUEST", 400)
    if action in {"participant.admit", "participant.permissions.update"}:
        try:
            scopes, permissions = current_permissions(participant, session)
        except GatewayDenied:
            if action == "participant.admit":
                raise
            # Disabled subjects/ended meetings still need a durable revoke intent.
            return record_authorization_intent(
                session=session,
                participant=participant,
                action="participant.disconnect",
                actor_profile=actor_profile,
                reason_code="revoked",
            )
        grant, _ = GatewayGrant.objects.get_or_create(participant=participant)
        data = {
            "grant_id": str(grant.pk),
            "scopes": scopes,
            "permissions": {
                "can_publish_audio": permissions["audio"],
                "can_publish_video": permissions["video"],
                "can_share_screen": permissions["screen"],
                "can_subscribe": "media:subscribe" in scopes,
            },
        }
    else:
        data = {"reason_code": reason_code}
    record, _ = GatewayAuthorizationOutbox.objects.get_or_create(
        participant=participant,
        authorization_epoch=participant.authorization_epoch,
        defaults={
            "session": session,
            "action": action,
            "actor_id": str(actor_profile.pk) if actor_profile else "system",
            "data": data,
        },
    )
    return record
