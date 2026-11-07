"""Versioned, permission-aware gateway projection and bounded public snapshots.

Only semantic changes write the projection/log. Polls and transport refreshes do
not advance public versions. All writers serialize on the existing meeting lock.
Redis is a disposable observation cache, never a source of permission decisions.
"""

from datetime import datetime, timedelta
import uuid

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from apps.meetings.models import (
    GatewayCommandRoute,
    GatewayStateChange,
    GatewayStateProjection,
    MeetingSession,
    Participant,
    ParticipantStatus,
)
from apps.meetings.services.gateway_tickets import (
    MAX_COUNTER,
    GatewayDenied,
    current_permissions,
)

RETAIN = 256
PAGE_SIZE = 50
OBSERVATION_TTL = 120  # Every live gateway must refresh its <=120-second ticket.
ADMITTED = {
    ParticipantStatus.ADMITTED,
    ParticipantStatus.ACTIVE,
    ParticipantStatus.DISCONNECTED,
}
OFFLINE = {"connection": "OFFLINE", "audio": False, "video": False, "screen": False}


def enabled(session=None):
    return settings.GATEWAY_STATE_ENABLED and (
        session is None or session.signalling_backend == "gateway"
    )


def _session(session_id):
    if not enabled():
        raise GatewayDenied("UNAVAILABLE", 503)
    session = (
        MeetingSession.objects.select_for_update()
        .select_related("room")
        .filter(pk=session_id)
        .first()
    )
    if session is None or not enabled(session):
        raise GatewayDenied()
    return session


def _audience(session, profile_id):
    """Always re-evaluate current database authority, including during replay."""
    participant = (
        Participant.objects.select_related("profile__user")
        .filter(session=session, profile_id=profile_id)
        .first()
    )
    if participant is None:
        raise GatewayDenied()
    # Terminal meetings permit existing, non-removed attendees to learn the end.
    terminal = session.lifecycle_state in {"ended", "failed"}
    if terminal:
        if (
            participant.status not in ADMITTED | {ParticipantStatus.LEFT}
            or not participant.profile.is_active
            or not participant.profile.user.is_active
        ):
            raise GatewayDenied()
    else:
        current_permissions(participant, session)
    membership = session.room.memberships.filter(
        profile_id=profile_id, is_active=True
    ).first()
    moderator = bool(
        not terminal
        and membership
        and membership.can_manage_waiting_room
        and participant.status in ADMITTED
    )
    return participant, moderator


def _materialize(session, now):
    from apps.meetings.services.gateway_sdk import application_state, person_state

    route = GatewayCommandRoute.objects.filter(session=session).first()
    memberships = {
        m.profile_id: m for m in session.room.memberships.filter(is_active=True)
    }
    moderators = {
        key
        for key, membership in memberships.items()
        if membership.can_manage_waiting_room
    }
    people, presence = {}, {}
    participants = Participant.objects.filter(session=session).select_related(
        "profile__user",
        "gateway_grant",
        "gateway_observation",
        "join_request",
        "join_request__profile",
        "join_request__reviewed_by_profile",
    )
    for p in participants:
        grant = getattr(p, "gateway_grant", None)
        observation = getattr(p, "gateway_observation", None)
        observed = observation.envelope if observation else None
        media = dict(OFFLINE)
        allowed = False
        scopes, permissions = [], dict.fromkeys(("audio", "video", "screen"), False)
        try:
            scopes, permissions = current_permissions(p, session)
            allowed = True
        except GatewayDenied:
            pass
        valid_until = now
        if allowed and grant and observed and route:
            occurred = datetime.fromisoformat(
                observed["occurred_at"].replace("Z", "+00:00")
            )
            valid_until = min(
                occurred + timedelta(seconds=OBSERVATION_TTL), route.lease_expires_at
            )
            if (
                str(grant.participant_session_id) == observed["participant_session_id"]
                and grant.generation == observed["generation"]
                and p.authorization_epoch == observed["authorization_epoch"]
                and route.owner_id == observed["gateway_owner_id"]
                and route.owner_generation == observed["gateway_generation"]
                and not route.released
                and now < valid_until
                and (
                    "lobby" not in scopes
                    or observed["snapshot"]["connection"] == "LOBBY_CONNECTED"
                )
            ):
                media = dict(observed["snapshot"])
                for kind in permissions:
                    media[kind] = media[kind] and permissions[kind]
        state = {
            "sdk": person_state(p, memberships.get(p.profile_id)),
            "participant_id": str(p.pk),
            "display_name": p.display_name[:255],
            "admission": p.status,
            "role": p.role,
            "authorization_epoch": p.authorization_epoch,
            "participant_session_id": str(grant.participant_session_id)
            if grant
            else None,
            "generation": grant.generation if grant else 0,
            "permissions": permissions,
            "presence": media,
            "join_request": {
                "id": str(p.join_request_id),
                "status": p.join_request.status,
            }
            if p.join_request_id
            else None,
            # Internal visibility metadata is never copied into public payloads.
            "eligible": allowed,
            "moderator": p.profile_id in moderators
            and p.status in ADMITTED
            and allowed,
        }
        people[str(p.pk)] = state
        if media["connection"] != "OFFLINE":
            presence[str(p.pk)] = {
                "participant_session_id": state["participant_session_id"],
                "generation": state["generation"],
                "authorization_epoch": p.authorization_epoch,
                "gateway_generation": route.owner_generation,
                "expires_at": valid_until.timestamp(),
                **media,
            }
    return {
        "application": application_state(session),
        "meeting": {
            "id": str(session.pk),
            "lifecycle": session.lifecycle_state,
            "authorization_epoch": session.authorization_epoch,
        },
        "participants": people,
    }, presence


def _project(session, now):
    state, presence = _materialize(session, now)
    projection, _ = GatewayStateProjection.objects.get_or_create(session=session)
    if projection.state != state:
        if projection.version >= MAX_COUNTER:
            raise GatewayDenied("UNAVAILABLE", 503)
        previous = projection.state.get("participants", {})
        changed = sorted(
            key
            for key in previous.keys() | state["participants"].keys()
            if previous.get(key) != state["participants"].get(key)
        )
        projection.version += 1
        projection.state = state
        projection.save(update_fields=["version", "state", "updated_at"])
        GatewayStateChange.objects.create(
            session=session,
            version=projection.version,
            participant_ids=changed if len(changed) <= 256 else [],
        )
        GatewayStateChange.objects.filter(
            session=session, version__lte=projection.version - RETAIN
        ).delete()
    return projection, presence


def project_event(session, *, now=None):
    """Called in the inbox transaction with its meeting lock; no external I/O."""
    if enabled(session):
        _project(session, now or timezone.now())


@transaction.atomic
def reconcile(session_id, *, now=None):
    """Also repairs missed admission/permission changes and expired presence."""
    session = _session(session_id)
    now = now or timezone.now()
    projection, presence = _project(session, now)
    return {
        "meeting_id": str(session.pk),
        "projection_version": projection.version,
        "presence": presence,
        "as_of": now.timestamp(),
    }


def _visible(projection, participant, moderator):
    people = projection.state["participants"]
    own = str(participant.pk)
    return {
        key: value
        for key, value in people.items()
        if key == own
        or (
            participant.status in ADMITTED
            and value["eligible"]
            and (value["admission"] in ADMITTED or moderator)
        )
    }


def _public(person, *, own=False, moderator=False):
    keys = (
        "participant_id",
        "display_name",
        "admission",
        "role",
        "participant_session_id",
        "generation",
        "presence",
    )
    result = {key: person[key] for key in keys}
    if own or moderator:
        result.update(
            permissions=person["permissions"],
            authorization_epoch=person["authorization_epoch"],
            join_request=person["join_request"],
        )
    return result


def event_id(change):
    return f"{change.version}:{change.pk}"


def _snapshot(projection, participant, moderator, *, after="", sdk=False):
    visible = _visible(projection, participant, moderator)
    own = str(participant.pk)
    keys = sorted(key for key in visible if key != own and key > after)
    page = keys[:PAGE_SIZE]
    change = GatewayStateChange.objects.get(
        session_id=projection.session_id, version=projection.version
    )
    result = {
        "schema_version": 1,
        "meeting": projection.state["meeting"],
        "projection_version": projection.version,
        "event_id": event_id(change),
        "context": "snapshot",
        "can_manage_waiting_room": moderator,
        "local_participant": _public(visible[own], own=True),
        "participants": [_public(visible[key], moderator=moderator) for key in page],
        "next_after": page[-1] if len(keys) > PAGE_SIZE else None,
    }
    if sdk:
        from apps.meetings.services.gateway_sdk import sdk_snapshot

        result["sdk"] = sdk_snapshot(projection, participant, moderator, visible, page)
    return result


@transaction.atomic
def snapshot(session_id, profile_id, *, after="", version=None, sdk=False):
    session = _session(session_id)
    participant, moderator = _audience(session, profile_id)
    projection, _ = _project(session, timezone.now())
    if version is not None and version != projection.version:
        raise GatewayDenied("SNAPSHOT_CHANGED", 409)
    return _snapshot(projection, participant, moderator, after=after, sdk=sdk)


@transaction.atomic
def next_update(session_id, profile_id, cursor, *, replay=False):
    """One committed event per pull; reauthorize between every SSE yield.

    A missing/foreign/trimmed/future cursor repairs with a quiet current snapshot.
    Replays contain invalidations, never historic private participant payloads.
    """
    session = _session(session_id)
    participant, moderator = _audience(session, profile_id)
    projection, _ = _project(session, timezone.now())
    previous = None
    try:
        number, identity = cursor.split(":")
        if number.isdecimal() and len(number) <= 16:
            previous = GatewayStateChange.objects.filter(
                session=session, version=int(number), pk=uuid.UUID(identity)
            ).first()
    except (ValueError, AttributeError):
        pass
    if previous is None:
        state = _snapshot(projection, participant, moderator)
        return "state.snapshot", state["event_id"], state
    change = (
        GatewayStateChange.objects.filter(session=session, version__gt=previous.version)
        .order_by("version")
        .first()
    )
    if change is None:
        return None
    if change.version != previous.version + 1:
        state = _snapshot(projection, participant, moderator)
        return "state.snapshot", state["event_id"], state
    visible = _visible(projection, participant, moderator)
    return (
        "state.changed",
        event_id(change),
        {
            "schema_version": 1,
            "event_id": str(change.pk),
            "type": "state.changed",
            "meeting_id": str(session.pk),
            "projection_version": change.version,
            "previous_version": previous.version,
            "participant_ids": [
                key for key in change.participant_ids if key in visible
            ],
            "occurred_at": change.created_at.isoformat(),
            "context": "replay" if replay else "live",
        },
    )
