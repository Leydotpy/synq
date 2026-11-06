"""Authenticated, observation-only lifecycle inbox. No Janus or browser side effects."""

import uuid
from datetime import datetime, timedelta

from django.db import transaction
from django.utils import timezone

from apps.meetings.models import (
    GatewayCommandRoute,
    GatewayGrant,
    GatewayLifecycleQuarantine,
    GatewayLifecycleReceipt,
    GatewayOwnerGeneration,
    GatewaySessionObservation,
    MeetingSession,
    Participant,
)
from apps.meetings.services.gateway_tickets import GatewayDenied, current_permissions
from jrtc_signalling.bus import BusError, canonical, digest
from jrtc_signalling.contracts import validate_message

STREAM = (
    "participant_session_id",
    "generation",
    "gateway_owner_id",
    "gateway_generation",
)


def receive_event(producer, event, *, now=None):
    """Commit receipt and latest complete snapshot before acknowledgement.

    A higher sequence reconciles a gap because every event carries all fields.
    These snapshots are not live notification history and never create admission,
    role, permission, joined_at, Socket.IO or SSE mutations.
    """
    now = now or timezone.now()
    validate_message(event, schema="lifecycle-event")
    try:
        ids = {
            name: uuid.UUID(event[name])
            for name in (
                "event_id",
                "meeting_id",
                "participant_id",
                "participant_session_id",
            )
        }
        occurred = datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))
        if producer != event["gateway_owner_id"] or occurred > now + timedelta(
            seconds=5
        ):
            raise ValueError
    except (ValueError, TypeError):
        raise BusError("Invalid lifecycle identity or time.") from None
    fingerprint = digest(canonical(event))
    with transaction.atomic():
        session = (
            MeetingSession.objects.select_for_update()
            .select_related("room")
            .filter(pk=ids["meeting_id"])
            .first()
        )
        if session is None or session.signalling_backend != "gateway":
            raise BusError("Invalid lifecycle cohort.")
        participant = (
            Participant.objects.select_for_update(of=("self",))
            .select_related("profile__user")
            .filter(pk=ids["participant_id"], session=session)
            .first()
        )
        if participant is None:
            raise BusError("Invalid lifecycle participant.")
        receipt = GatewayLifecycleReceipt.objects.filter(pk=ids["event_id"]).first()
        if receipt:
            if (
                receipt.digest != fingerprint
                or receipt.participant_id != participant.pk
            ):
                raise BusError("Conflicting lifecycle receipt.")
            return "duplicate"
        grant = GatewayGrant.objects.filter(participant=participant).first()
        route = GatewayCommandRoute.objects.filter(session=session).first()
        owner = GatewayOwnerGeneration.objects.filter(
            session=session, owner_id=producer, generation=event["gateway_generation"]
        ).first()
        if owner is None or occurred < owner.created_at - timedelta(seconds=5):
            raise BusError("Unknown lifecycle owner.")
        observation = GatewaySessionObservation.objects.filter(
            participant=participant
        ).first()
        offline = event["snapshot"]["connection"] == "OFFLINE"
        current = (
            grant is not None
            and grant.participant_session_id == ids["participant_session_id"]
            and grant.generation == event["generation"]
            and participant.authorization_epoch == event["authorization_epoch"]
            and route is not None
            and route.owner_id == producer
            and route.owner_generation == event["gateway_generation"]
            and (
                offline
                or (
                    not route.released
                    and route.lease_expires_at > now
                    and occurred <= route.lease_expires_at
                )
            )
        )
        if current and not offline:
            try:
                scopes, permissions = current_permissions(participant, session)
                state = event["snapshot"]
                if (
                    "lobby" in scopes and state["connection"] != "LOBBY_CONNECTED"
                ) or any(
                    state[k] and not permissions[k]
                    for k in ("audio", "video", "screen")
                ):
                    current = False
            except GatewayDenied:
                current = False
        previous = observation.envelope if observation else None
        same = previous is not None and all(previous[k] == event[k] for k in STREAM)
        disposition = "stale"
        if current:
            sequence = previous["sequence"] if same else 0
            if (
                same
                and event["sequence"] == sequence
                and observation.digest != fingerprint
            ):
                raise BusError("Conflicting lifecycle sequence.")
            if event["sequence"] > sequence and not (
                same and previous["snapshot"]["connection"] == "OFFLINE"
            ):
                gaps = (observation.reconciled_gaps if observation else 0) + max(
                    0, event["sequence"] - sequence - 1
                )
                GatewaySessionObservation.objects.update_or_create(
                    participant=participant,
                    defaults={
                        "envelope": event,
                        "digest": fingerprint,
                        "reconciled_gaps": gaps,
                    },
                )
                disposition = (
                    "reconciled" if event["sequence"] > sequence + 1 else "recorded"
                )
        GatewayLifecycleReceipt.objects.create(
            id=ids["event_id"],
            participant=participant,
            digest=fingerprint,
            disposition=disposition,
        )
        # Keep replay fences in the observation indefinitely; receipts are a
        # bounded diagnostic sample. All producers serialize on the meeting row.
        old = list(
            GatewayLifecycleReceipt.objects.filter(participant=participant)
            .order_by("-created_at", "-pk")
            .values_list("pk", flat=True)[256:]
        )
        GatewayLifecycleReceipt.objects.filter(pk__in=old).delete()
        return disposition


def quarantine(packet_digest):
    GatewayLifecycleQuarantine.objects.get_or_create(
        digest=packet_digest, defaults={"reason_code": "untrusted_lifecycle"}
    )
    excess = list(
        GatewayLifecycleQuarantine.objects.order_by("-created_at", "-pk").values_list(
            "pk", flat=True
        )[1000:1100]
    )
    GatewayLifecycleQuarantine.objects.filter(pk__in=excess).delete()
