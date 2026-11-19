"""Operator switches and the evidence required to reuse a room after a staged drain."""

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.models import (
    GatewayAuthorizationOutbox,
    GatewayCommand,
    GatewayCommandOutcome,
    GatewayCommandRoute,
    GatewayOwnerGeneration,
    MeetingRoom,
)
from apps.meetings.services.gateway_commands import OPEN


@transaction.atomic
def set_cohort(room_id, *, selected):
    room = MeetingRoom.objects.select_for_update().get(pk=room_id)
    if selected and not settings.GATEWAY_STAGED_ROUTING:
        raise MeetingDomainError("Enable staged routing before selecting a room.")
    room.gateway_canary = selected
    room.save(update_fields=["gateway_canary", "updated_at"])


def drain(room_id, session_id):
    from apps.meetings.services.lifecycle import MeetingLifecycleService

    with transaction.atomic():
        room = MeetingRoom.objects.select_for_update().get(pk=room_id)
        session = room.sessions.order_by("-created_at").first()
        if (
            not session
            or session.pk != session_id
            or session.signalling_backend != "gateway"
        ):
            raise MeetingDomainError(
                "The specified session is not this room's latest gateway meeting."
            )
        room.gateway_canary = False
        room.save(update_fields=["gateway_canary", "updated_at"])
    # Concurrent starts reuse this exact session until its end commits. A delayed
    # operator request never ends a replacement session.
    return MeetingLifecycleService.end_session(
        session=session, reason="Staged gateway failback"
    )


def settle_unowned(session):
    """The ended session's authority lock excludes a late first claim."""
    if (
        GatewayCommandRoute.objects.filter(session=session).exists()
        or GatewayOwnerGeneration.objects.filter(session=session).exists()
        or GatewayCommand.objects.filter(outbox__session=session).exists()
    ):
        return
    GatewayAuthorizationOutbox.objects.filter(session=session, status__in=OPEN).update(
        status="superseded",
        last_error="owner_lost",
        lease_id=None,
        lease_until=None,
        updated_at=timezone.now(),
    )


def require_drained(session):
    """Read monotonic evidence while the caller holds the room creation lock."""
    if session.ended_at is None or session.lifecycle_state != "ended":
        raise MeetingDomainError("The prior gateway meeting has not ended.")
    intents = GatewayAuthorizationOutbox.objects.filter(session=session)
    if intents.filter(status__in=OPEN).exists():
        raise MeetingDomainError(
            "The prior gateway meeting still has pending commands."
        )
    route = GatewayCommandRoute.objects.filter(session=session).first()
    if route is None:
        if (
            GatewayOwnerGeneration.objects.filter(session=session).exists()
            or GatewayCommand.objects.filter(outbox__session=session).exists()
        ):
            raise MeetingDomainError("The prior gateway owner cannot be verified.")
        return
    if not route.released:
        raise MeetingDomainError("The prior gateway owner has not confirmed release.")
    if not GatewayCommandOutcome.objects.filter(
        status="applied",
        command__status="applied",
        command__outbox__session=session,
        command__outbox__action="meeting.terminate",
        command__outbox__status="applied",
        command__outbox__authorization_epoch=session.authorization_epoch,
        command__envelope__expected_owner_generation=route.owner_generation,
        command__envelope__target_owner=route.owner_id,
    ).exists():
        raise MeetingDomainError(
            "Gateway VideoRoom termination has not been confirmed."
        )
