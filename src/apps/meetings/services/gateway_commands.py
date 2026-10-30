"""Transactional command preparation and outcome receipt; no network inside locks."""

import uuid
from datetime import datetime, timedelta

from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone

from apps.meetings.models import (
    GatewayAuthorizationOutbox,
    GatewayCommand,
    GatewayCommandOutcome,
    GatewayCommandQuarantine,
    GatewayCommandRoute,
    GatewayGrant,
    MeetingLifecycleState,
    MeetingSession,
    Participant,
)
from apps.meetings.services.gateway_outbox import enabled
from apps.meetings.services.gateway_tickets import GatewayDenied, current_permissions
from jrtc_signalling.bus import BusError, canonical, command_route, digest
from jrtc_signalling.contracts import validate_message

OPEN = ("pending", "accepted")
TERMINAL = ("applied", "failed", "timed_out", "superseded")


def _time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _current(row, session, participant):
    if row.participant_id is None:
        return (
            row.action == "meeting.terminate"
            and row.authorization_epoch == session.authorization_epoch
            and session.lifecycle_state == MeetingLifecycleState.ENDED
        )
    if participant.authorization_epoch != row.authorization_epoch:
        return False
    if row.action in {"participant.admit", "participant.permissions.update"}:
        try:
            scopes, permissions = current_permissions(participant, session)
        except GatewayDenied:
            return False
        grant = GatewayGrant.objects.filter(participant=participant).first()
        return grant is not None and row.data == {
            "grant_id": str(grant.pk),
            "scopes": scopes,
            "permissions": {
                "can_publish_audio": permissions["audio"],
                "can_publish_video": permissions["video"],
                "can_share_screen": permissions["screen"],
                "can_subscribe": "media:subscribe" in scopes,
            },
        }
    return row.action in {
        "participant.deny",
        "participant.disconnect",
    } and participant.status in {"left", "removed"}


def _bound(command, route, participant, now):
    body = command.envelope
    if (
        route is None
        or route.lease_expires_at <= now
        or route.owner_id != body["target_owner"]
        or route.owner_generation != body["expected_owner_generation"]
    ):
        return False
    if participant is None:
        return body["participant_session_id"] is None
    return GatewayGrant.objects.filter(
        participant=participant, participant_session_id=body["participant_session_id"]
    ).exists()


def _terminal(row, command, status, reason, now):
    row.status, row.last_error = status, reason
    row.lease_id = row.lease_until = None
    row.save(
        update_fields=["status", "last_error", "lease_id", "lease_until", "updated_at"]
    )
    if command:
        command.status, command.terminal_at = status, now
        command.save(update_fields=["status", "terminal_at", "updated_at"])


def prepare_delivery(outbox_id, *, now=None):
    """Lease one committed intent; preserve the exact wire command on every retry."""
    now = now or timezone.now()
    if not getattr(settings, "GATEWAY_COMMANDS_ENABLED", False):
        return None
    session_id = (
        GatewayAuthorizationOutbox.objects.filter(pk=outbox_id)
        .values_list("session_id", flat=True)
        .first()
    )
    if session_id is None:
        return None
    with transaction.atomic():
        sessions = MeetingSession.objects.select_related("room")
        sessions = (
            sessions.select_for_update(skip_locked=True)
            if connection.features.has_select_for_update_skip_locked
            else sessions.select_for_update()
        )
        session = sessions.filter(pk=session_id).first()
        if session is None or not enabled(session):
            return None
        row = GatewayAuthorizationOutbox.objects.select_for_update().get(pk=outbox_id)
        if (
            row.status not in OPEN
            or row.next_attempt_at > now
            or (row.lease_until and row.lease_until > now)
        ):
            return None
        command = GatewayCommand.objects.filter(outbox=row).first()
        participant = (
            Participant.objects.select_for_update()
            .filter(pk=row.participant_id)
            .first()
            if row.participant_id
            else None
        )
        if not _current(row, session, participant):
            _terminal(row, command, "superseded", "revoked", now)
            return None
        if row.created_at + timedelta(seconds=300) <= now or (
            command and _time(command.envelope["expires_at"]) <= now
        ):
            _terminal(row, command, "timed_out", "expired", now)
            return None
        if row.delivery_attempts >= 10:
            _terminal(row, command, "failed", row.last_error or "unavailable", now)
            return None
        route = GatewayCommandRoute.objects.filter(
            session=session, lease_expires_at__gt=now
        ).first()
        row.delivery_attempts += 1
        row.next_attempt_at = now + timedelta(
            seconds=min(30, 2 ** min(row.delivery_attempts, 5))
        )
        if route is None:
            row.last_error = "owner_lost"
            row.save(
                update_fields=[
                    "delivery_attempts",
                    "next_attempt_at",
                    "last_error",
                    "updated_at",
                ]
            )
            if row.delivery_attempts >= 10:
                _terminal(row, command, "failed", "owner_lost", now)
            return None
        if command and not _bound(command, route, participant, now):
            # Never mutate/re-target an existing wire command or silently execute
            # an uncertain command under a new ID. SG-07 reconciliation decides.
            _terminal(row, command, "failed", "owner_lost", now)
            return None
        if command is None:
            grant = (
                GatewayGrant.objects.filter(participant=participant).first()
                if participant
                else None
            )
            if participant is not None and grant is None:
                _terminal(row, None, "failed", "unavailable", now)
                return None
            command_id = uuid.uuid4()
            body = {
                "schema_version": 1,
                "command_id": str(command_id),
                "type": row.action,
                "meeting_id": str(session.pk),
                "participant_id": str(participant.pk) if participant else None,
                "participant_session_id": str(grant.participant_session_id)
                if grant
                else None,
                "authorization_epoch": row.authorization_epoch,
                "target_owner": route.owner_id,
                "expected_owner_generation": route.owner_generation,
                "issued_at": now.isoformat(),
                "expires_at": min(
                    now + timedelta(seconds=60), row.created_at + timedelta(seconds=300)
                ).isoformat(),
                "actor_id": row.actor_id,
                "trace_id": str(row.pk),
                "data": row.data,
            }
            try:
                validate_message(body, schema="command")
                command_route(route.owner_id)
            except ValueError:
                _terminal(row, None, "failed", "unavailable", now)
                return None
            command = GatewayCommand.objects.create(
                id=command_id, outbox=row, envelope=body
            )
        row.lease_id, row.lease_until = uuid.uuid4(), now + timedelta(seconds=10)
        row.last_error = ""
        row.save(
            update_fields=[
                "delivery_attempts",
                "next_attempt_at",
                "lease_id",
                "lease_until",
                "last_error",
                "updated_at",
            ]
        )
        return {
            "outbox_id": row.pk,
            "lease_id": row.lease_id,
            "command_id": command.pk,
            "envelope": command.envelope,
        }


def publication_finished(claim, *, success, now=None):
    now = now or timezone.now()
    with transaction.atomic():
        row = (
            GatewayAuthorizationOutbox.objects.select_for_update()
            .filter(pk=claim["outbox_id"], lease_id=claim["lease_id"])
            .first()
        )
        if row is None:
            return  # Late publishers cannot overwrite a newer claim or outcome.
        row.lease_id = row.lease_until = None
        if row.status in OPEN:
            row.last_error = "" if success else "unavailable"
        row.save(update_fields=["lease_id", "lease_until", "last_error", "updated_at"])
        if success:
            GatewayCommand.objects.filter(
                pk=claim["command_id"], status="pending"
            ).update(status="published", published_at=now)


def authorize_command(command, *, now=None):
    now = now or timezone.now()
    if not getattr(settings, "GATEWAY_COMMANDS_ENABLED", False):
        return False
    try:
        validate_message(command, schema="command")
        command_id = uuid.UUID(command["command_id"])
    except ValueError, KeyError, TypeError:
        return False
    stored = (
        GatewayCommand.objects.filter(pk=command_id).select_related("outbox").first()
    )
    if stored is None or stored.envelope != command or stored.status in TERMINAL:
        return False
    with transaction.atomic():
        session = (
            MeetingSession.objects.select_for_update()
            .select_related("room")
            .get(pk=stored.outbox.session_id)
        )
        row = GatewayAuthorizationOutbox.objects.select_for_update().get(
            pk=stored.outbox_id
        )
        participant = (
            Participant.objects.select_for_update()
            .filter(pk=row.participant_id)
            .first()
            if row.participant_id
            else None
        )
        route = GatewayCommandRoute.objects.filter(session=session).first()
        return (
            enabled(session)
            and row.status in OPEN
            and _time(command["issued_at"]) <= now < _time(command["expires_at"])
            and _current(row, session, participant)
            and _bound(stored, route, participant, now)
        )


def receive_outcome(producer, event, *, now=None):
    """Commit a verified outcome and receipt before transport acknowledgement."""
    now = now or timezone.now()
    validate_message(event, schema="command-outcome")
    try:
        command_id, event_id = (
            uuid.UUID(event["command_id"]),
            uuid.UUID(event["event_id"]),
        )
    except ValueError, TypeError:
        raise BusError("Unknown command outcome.") from None
    stored = (
        GatewayCommand.objects.filter(pk=command_id).select_related("outbox").first()
    )
    if stored is None:
        raise BusError("Unknown command outcome.")
    body = stored.envelope
    if (
        producer != body["target_owner"]
        or event["gateway_owner_id"] != producer
        or event["gateway_generation"] != body["expected_owner_generation"]
        or event["authorization_epoch"] != body["authorization_epoch"]
        or event["meeting_id"] != body["meeting_id"]
        or event["trace_id"] != body["trace_id"]
    ):
        raise BusError("Mismatched command outcome.")
    if _time(event["occurred_at"]) > now + timedelta(seconds=5) or _time(
        event["occurred_at"]
    ) < _time(body["issued_at"]) - timedelta(seconds=5):
        raise BusError("Invalid command outcome time.")
    event_digest = digest(canonical(event))
    with transaction.atomic():
        session = (
            MeetingSession.objects.select_for_update()
            .select_related("room")
            .get(pk=stored.outbox.session_id)
        )
        row = GatewayAuthorizationOutbox.objects.select_for_update().get(
            pk=stored.outbox_id
        )
        command = GatewayCommand.objects.select_for_update().get(pk=stored.pk)
        receipt = GatewayCommandOutcome.objects.filter(pk=event_id).first()
        if receipt:
            if receipt.digest != event_digest:
                raise BusError("Conflicting command outcome.")
            return "duplicate"
        GatewayCommandOutcome.objects.create(
            id=event_id,
            command=command,
            digest=event_digest,
            status=event["status"],
            reason_code=event["reason_code"],
        )
        if command.status in TERMINAL or row.status in TERMINAL:
            return "stale"
        participant = (
            Participant.objects.select_for_update()
            .filter(pk=row.participant_id)
            .first()
            if row.participant_id
            else None
        )
        route = GatewayCommandRoute.objects.filter(session=session).first()
        if not _current(row, session, participant) or not _bound(
            command, route, participant, now
        ):
            _terminal(row, command, "superseded", "revoked", now)
            return "stale"
        if event["status"] == "accepted":
            row.status, command.status = "accepted", "accepted"
            row.save(update_fields=["status", "updated_at"])
            command.save(update_fields=["status", "updated_at"])
        else:
            _terminal(row, command, event["status"], event["reason_code"], now)
        return "recorded"


def quarantine(packet_digest, reason):
    GatewayCommandQuarantine.objects.get_or_create(
        digest=packet_digest, defaults={"reason_code": reason}
    )
    # Metadata only; retain a bounded diagnostic sample, never raw signed packets.
    excess = list(
        GatewayCommandQuarantine.objects.order_by("-created_at").values_list(
            "pk", flat=True
        )[1000:1100]
    )
    GatewayCommandQuarantine.objects.filter(pk__in=excess).delete()
