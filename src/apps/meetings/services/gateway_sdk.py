"""SDK-compatible application state without legacy sockets or media ownership.

Called under gateway_state's meeting transaction. Never serializes Django's
Janus handles, connection rows, private session metadata or another user's lobby.
"""

from django.db.models import Q
from django.utils import timezone

from apps.meetings.models import MeetingEvent, MeetingMessage, MeetingReaction
from apps.meetings.services.state import MeetingStateBuilder as State


def record_presence_event(session, participant, event, previous, same):
    """An accepted, ordered gateway observation is an event, never admission."""
    from django.conf import settings
    from apps.meetings.services.gateway_state import ADMITTED

    if not settings.GATEWAY_STATE_ENABLED or participant.status not in ADMITTED:
        return
    previous_state = previous["snapshot"]["connection"] if same else "OFFLINE"
    kind = None
    if (
        event["type"] == "participant.media_connected"
        and previous_state != "MEDIA_CONNECTED"
    ):
        kind = "participant_joined"
    elif (
        event["type"] == "participant.signaling_disconnected"
        and previous_state == "MEDIA_CONNECTED"
    ):
        kind = "participant_left"
    if kind:
        MeetingEvent.objects.create(
            session=session,
            actor_profile=participant.profile,
            actor_participant=participant,
            event_type=kind,
            payload={
                "participant_id": str(participant.pk),
                "gateway_event_id": event["event_id"],
            },
        )


def person_state(participant, membership):
    return {
        "profile": State.serialize_profile_summary(participant.profile),
        "coordinator_permissions": State.serialize_membership(membership),
        "join_request": State.serialize_join_request(participant.join_request)
        if participant.join_request_id
        else None,
        "can_chat": participant.can_chat,
        "can_react": participant.can_react,
        "is_muted": participant.is_muted,
        "is_camera_blocked": participant.is_camera_blocked,
    }


def application_state(session):
    summary = State.serialize_session(session)
    summary.pop("janus_room_id", None)
    # Session metrics/heartbeat writes are not semantic gateway state changes.
    summary.pop("state_version", None)
    summary.pop("last_synced_at", None)
    messages = list(
        MeetingMessage.objects.filter(session=session).order_by("-created_at", "-pk")[
            :10
        ]
    )
    reactions = list(
        MeetingReaction.objects.filter(session=session)
        .filter(Q(expires_at__isnull=True) | Q(expires_at__gt=timezone.now()))
        .order_by("-created_at", "-pk")[:25]
    )
    # Stable committed domain identities, never inferred participant diffs.
    fields = {
        "chat_message_sent": "message_id",
        "reaction_sent": "reaction_id",
        "join_request_created": "join_request_id",
        "join_request_reviewed": "join_request_id",
        "session_ended": None,
        "participant_joined": "participant_id",
        "participant_left": "participant_id",
    }
    events = list(
        MeetingEvent.objects.filter(session=session, event_type__in=fields).order_by(
            "-created_at", "-pk"
        )[:64]
    )
    return {
        "room": State.serialize_room(session),
        "session": summary,
        "messages": [State.serialize_message(item) for item in reversed(messages)],
        "recent_reactions": [
            State.serialize_reaction(item) for item in reversed(reactions)
        ],
        "events": [
            {
                "id": str(event.pk),
                "type": event.event_type,
                "subject_id": str(event.payload.get(fields[event.event_type], ""))
                if fields[event.event_type]
                else str(session.pk),
                "actor_profile_id": str(event.actor_profile_id)
                if event.actor_profile_id
                else None,
                "actor_participant_id": str(event.actor_participant_id)
                if event.actor_participant_id
                else None,
                "occurred_at": event.created_at.isoformat(),
            }
            for event in reversed(events)
        ],
    }


def sdk_snapshot(projection, participant, moderator, visible, page):
    from apps.meetings.services.gateway_state import ADMITTED

    own = visible[str(participant.pk)]
    admitted = participant.status in ADMITTED

    def serialize(person, *, local=False):
        sdk = person["sdk"]
        permissions = person["permissions"] if local else {}
        return {
            "id": person["participant_id"],
            "profile": sdk["profile"],
            "role": person["role"],
            "status": person["admission"],
            "display_name": person["display_name"],
            "can_publish_audio": permissions.get("audio", False),
            "can_publish_video": permissions.get("video", False),
            "can_share_screen": permissions.get("screen", False),
            "can_chat": admitted and sdk["can_chat"] if local else False,
            "can_react": admitted and sdk["can_react"] if local else False,
            "is_muted": sdk["is_muted"],
            "is_camera_blocked": sdk["is_camera_blocked"],
            "metadata": {},
            "connections": [],
            "media_handles": [],
            "gateway_presence": person["presence"],
        }

    application = projection.state["application"]
    # Each page includes only its visible participants/requests. The client
    # combines pages from exactly one projection version.
    requests = [
        visible[key]["sdk"]["join_request"]
        for key in page
        if moderator
        and visible[key]["sdk"]["join_request"]
        and visible[key]["sdk"]["join_request"]["status"] == "pending"
    ]
    request_ids = {
        p["sdk"]["join_request"]["id"]
        for p in visible.values()
        if p["sdk"]["join_request"]
    }
    events = [
        event
        for event in application["events"]
        if (
            event["type"] in {"chat_message_sent", "reaction_sent", "session_ended"}
            and admitted
            or event["type"].startswith("join_request_")
            and event["subject_id"] in request_ids
            or event["type"] in {"participant_joined", "participant_left"}
            and admitted
            and event["subject_id"] in visible
        )
    ]
    return {
        "event_context": {"kind": "snapshot", "session_id": str(projection.session_id)},
        "gateway_events": events,
        "room": application["room"],
        "session": {**application["session"], "state_version": projection.version},
        "current_profile": own["sdk"]["profile"],
        "coordinator_permissions": own["sdk"]["coordinator_permissions"]
        if admitted
        else None,
        "counts": {
            "participants": sum(p["admission"] in ADMITTED for p in visible.values()),
            "publishers": sum(
                any(p["presence"][kind] for kind in ("audio", "video", "screen"))
                for p in visible.values()
            ),
            "pending_join_requests": sum(
                p["admission"] == "waiting" for p in visible.values()
            )
            if moderator
            else 0,
        },
        "local_participant": serialize(own, local=True) if admitted else None,
        "remote_participants": [
            serialize(visible[key])
            for key in page
            if visible[key]["admission"] in ADMITTED
        ],
        "own_join_request": own["sdk"]["join_request"],
        "pending_join_requests": requests,
        "messages": application["messages"] if admitted else [],
        "recent_reactions": application["recent_reactions"] if admitted else [],
        "janus": {"publishers": []},
    }
