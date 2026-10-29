"""Django-owned ticket issuance and shared replay/permission authority.

No Janus calls, broker I/O, or gateway observations can create admission here.
The meeting row serializes issuance, consumption and permission transitions.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from django.views.decorators.debug import sensitive_variables

from apps.meetings.models import (
    GatewayGrant,
    GatewayTicket,
    MeetingJoinRequestStatus,
    MeetingLifecycleState,
    MeetingRole,
    MeetingSession,
    Participant,
    ParticipantStatus,
)

MAX_COUNTER = 2**53 - 1
PURPOSES = {
    "connect": "session.hello",
    "resume": "session.resume",
    "refresh": "session.refresh",
}


class GatewayDenied(Exception):
    """Input-free error safe to return or record."""

    def __init__(self, code="FORBIDDEN", status=403):
        self.code, self.status = code, status
        super().__init__(code)


@dataclass(frozen=True, repr=False)
class SigningConfig:
    issuer: str
    kid: str
    key: RSAPrivateKey
    ttl: int


@sensitive_variables()
def signing_config() -> SigningConfig:
    if not getattr(settings, "GATEWAY_TICKETS_ENABLED", False):
        raise GatewayDenied("UNAVAILABLE", 503)
    try:
        issuer = settings.GATEWAY_TICKET_ISSUER
        kid = settings.GATEWAY_TICKET_KEY_ID
        ttl = settings.GATEWAY_TICKET_TTL_SECONDS
        if not isinstance(issuer, str) or not 1 <= len(issuer) <= 256:
            raise ValueError
        if not isinstance(kid, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", kid
        ):
            raise ValueError
        if type(ttl) is not int or not 1 <= ttl <= 120:
            raise ValueError
        raw = Path(settings.GATEWAY_TICKET_PRIVATE_KEY_FILE).read_bytes()
        key = serialization.load_pem_private_key(raw, password=None)
        if not isinstance(key, RSAPrivateKey) or key.key_size < 2048:
            raise ValueError
        return SigningConfig(issuer, kid, key, ttl)
    except AttributeError, OSError, ValueError, TypeError:
        raise GatewayDenied("UNAVAILABLE", 503) from None


def advance_epoch(participant: Participant) -> None:
    """Caller holds the meeting and participant locks and saves in that transaction."""
    if participant.authorization_epoch >= MAX_COUNTER:
        raise GatewayDenied("UNAVAILABLE", 503)
    participant.authorization_epoch += 1


def current_permissions(
    participant: Participant, session: MeetingSession
) -> tuple[list[str], dict]:
    if session.lifecycle_state not in {
        MeetingLifecycleState.PROVISIONING,
        MeetingLifecycleState.WAITING,
        MeetingLifecycleState.ACTIVE,
    }:
        raise GatewayDenied()
    if not participant.profile.is_active or not participant.profile.user.is_active:
        raise GatewayDenied()
    if participant.role not in MeetingRole.values:
        raise GatewayDenied()
    if participant.role in {MeetingRole.HOST, MeetingRole.CO_HOST}:
        if not session.room.memberships.filter(
            profile_id=participant.profile_id,
            is_active=True,
            role=participant.role,
        ).exists():
            raise GatewayDenied()
    permissions = {
        "audio": participant.can_publish_audio and not participant.is_muted,
        "video": participant.can_publish_video and not participant.is_camera_blocked,
        "screen": participant.can_share_screen,
    }
    if participant.status == ParticipantStatus.WAITING:
        if not session.join_requests.filter(
            pk=participant.join_request_id,
            profile_id=participant.profile_id,
            status=MeetingJoinRequestStatus.PENDING,
        ).exists():
            raise GatewayDenied()
        return ["lobby", "session:resume"], dict.fromkeys(permissions, False)
    if participant.status not in {
        ParticipantStatus.ADMITTED,
        ParticipantStatus.ACTIVE,
        ParticipantStatus.DISCONNECTED,
    }:
        raise GatewayDenied()
    scopes = ["media:subscribe", "session:resume"]
    if any(permissions.values()):
        scopes.insert(0, "media:publish")
    return scopes, permissions


def _locked_participant(session, profile_id):
    participant = (
        Participant.objects.select_for_update(of=("self",))
        .select_related(
            "profile__user",
        )
        .filter(session=session, profile_id=profile_id)
        .first()
    )
    if participant is None:
        # A pending Django-validated intent may obtain lobby access only.
        intent = (
            session.join_requests.filter(
                profile_id=profile_id,
                status=MeetingJoinRequestStatus.PENDING,
            )
            .select_related("profile__user")
            .first()
        )
        if intent is None or intent.requested_role != MeetingRole.PARTICIPANT:
            raise GatewayDenied()
        participant = Participant.objects.create(
            room=session.room,
            session=session,
            profile=intent.profile,
            join_request=intent,
            status=ParticipantStatus.WAITING,
            display_name=intent.requested_display_name or intent.profile.display_name,
        )
    return participant


@sensitive_variables()
def issue_ticket(
    *,
    session_id,
    profile_id,
    purpose="connect",
    participant_session_id=None,
    generation=None,
):
    config = signing_config()
    if purpose not in PURPOSES:
        raise GatewayDenied("INVALID_REQUEST", 400)
    now = timezone.now()
    with transaction.atomic():
        session = (
            MeetingSession.objects.select_for_update()
            .select_related("room")
            .filter(pk=session_id)
            .first()
        )
        if session is None:
            raise GatewayDenied()
        participant = _locked_participant(session, profile_id)
        scopes, _ = current_permissions(participant, session)
        grant = (
            GatewayGrant.objects.select_for_update()
            .filter(participant=participant)
            .first()
        )
        if purpose != "connect":
            if (
                grant is None
                or str(grant.participant_session_id) != str(participant_session_id)
                or type(generation) is not int
                or generation != grant.generation
            ):
                raise GatewayDenied()
        if grant is None:
            grant = GatewayGrant(participant=participant, issue_window_at=now)
        elif purpose == "connect":
            grant.participant_session_id = uuid.uuid4()
            grant.generation += 1
        elif purpose == "resume":
            grant.generation += 1
        if (
            grant.generation > MAX_COUNTER
            or participant.authorization_epoch > MAX_COUNTER
        ):
            raise GatewayDenied("UNAVAILABLE", 503)
        # One locked database row bounds issuance across workers/reconnects.
        if now >= grant.issue_window_at + timedelta(minutes=1):
            grant.issue_window_at, grant.issue_count = now, 0
        if grant.issue_count >= 10:
            raise GatewayDenied("RATE_LIMITED", 429)
        grant.issue_count += 1
        grant.save()
        issued = int(now.timestamp())
        ticket_id = uuid.uuid4()
        claims = {
            "iss": config.issuer,
            "aud": "jrtc-signalling",
            "sub": str(profile_id),
            "jti": str(ticket_id),
            "iat": issued,
            "nbf": issued,
            "exp": issued + config.ttl,
            "meeting_id": str(session.pk),
            "participant_id": str(participant.pk),
            "participant_session_id": str(grant.participant_session_id),
            "generation": grant.generation,
            "authorization_epoch": participant.authorization_epoch,
            "scopes": scopes,
            "grant_id": str(grant.pk),
        }
        GatewayTicket.objects.create(
            id=ticket_id,
            grant=grant,
            purpose=purpose,
            claims=claims,
            expires_at=datetime.fromtimestamp(claims["exp"], UTC),
        )
    # Crypto runs outside the row lock. A later revoke makes this ticket unusable.
    token = jwt.encode(
        claims, config.key, algorithm="RS256", headers={"kid": config.kid, "typ": "JWT"}
    )
    return {
        "ticket": token,
        "expires_at": claims["exp"],
        "participant_session_id": claims["participant_session_id"],
        "generation": grant.generation,
        "authorization_epoch": claims["authorization_epoch"],
        "scopes": scopes,
    }


def check_ticket(*, claims: dict, operation: str, consume: bool):
    """Authenticated service-only authority. Shared one-use receipt survives restart."""
    if not getattr(settings, "GATEWAY_TICKETS_ENABLED", False):
        raise GatewayDenied("UNAVAILABLE", 503)
    try:
        meeting_id = uuid.UUID(claims["meeting_id"])
        ticket_id = uuid.UUID(claims["jti"])
    except KeyError, ValueError, TypeError, AttributeError:
        raise GatewayDenied() from None
    now = timezone.now()
    with transaction.atomic():
        session = (
            MeetingSession.objects.select_for_update()
            .select_related("room")
            .filter(pk=meeting_id)
            .first()
        )
        ticket = GatewayTicket.objects.select_for_update().filter(pk=ticket_id).first()
        if (
            session is None
            or ticket is None
            or ticket.claims != claims
            or now >= ticket.expires_at
        ):
            raise GatewayDenied()
        grant = (
            GatewayGrant.objects.select_for_update()
            .select_related("participant__profile__user")
            .get(pk=ticket.grant_id)
        )
        participant = grant.participant
        if (
            participant.session_id != session.pk
            or claims["generation"] != grant.generation
            or claims["participant_session_id"] != str(grant.participant_session_id)
            or claims["authorization_epoch"] != participant.authorization_epoch
        ):
            raise GatewayDenied()
        scopes, permissions = current_permissions(participant, session)
        if claims["scopes"] != scopes:
            raise GatewayDenied()
        if consume:
            if ticket.consumed_at is not None or PURPOSES[ticket.purpose] != operation:
                raise GatewayDenied()
            ticket.consumed_at = now
            ticket.save(update_fields=["consumed_at", "updated_at"])
        elif ticket.consumed_at is None:
            raise GatewayDenied()
        return {
            "claims": claims,
            "permissions": permissions,
            "valid_until": claims["exp"],
        }
