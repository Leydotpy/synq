"""Meeting state aggregation helpers used by APIs, Socket.IO, and Celery workers."""

from __future__ import annotations

from django.db.models import Prefetch, Q
from django.utils import timezone

from apps.meetings.models import (
    MeetingJoinRequest,
    MeetingJoinRequestStatus,
    MeetingMessage,
    MeetingReaction,
    MeetingRoomMembership,
    MeetingSession,
    Participant,
    ParticipantConnection,
    ParticipantMediaHandle,
    ParticipantStatus,
    RealtimeConnectionStatus,
)


VISIBLE_CONNECTION_STATUSES = {
    RealtimeConnectionStatus.CONNECTED,
    RealtimeConnectionStatus.SUBSCRIBED,
    RealtimeConnectionStatus.ACTIVE,
}
PRIVATE_SESSION_METADATA_KEYS = {
    "external_id",
    "external_provider",
    "integration_provider",
    "provider",
    "service_owner_id",
    "service_owner_profile_id",
    "source",
    "tenant_id",
}


class MeetingStateBuilder:
    """Build a client-facing session state snapshot from normalized meeting models."""

    @staticmethod
    def build(session: MeetingSession, authenticated_profile=None, message_limit: int = 50, reaction_limit: int = 25) -> dict:
        """Return a serialized state snapshot for a meeting session."""

        hydrated_session = MeetingSession.objects.select_related(
            "room",
            "room__created_by_profile",
            "started_by_profile",
        ).get(pk=session.pk)
        visible_connections = ParticipantConnection.objects.filter(
            status__in=VISIBLE_CONNECTION_STATUSES,
        )
        media_handles = ParticipantMediaHandle.objects.prefetch_related("streams")
        participants = list(
            Participant.objects.filter(session=hydrated_session)
            .exclude(status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED])
            .select_related("profile", "membership")
            .prefetch_related(
                Prefetch("connections", queryset=visible_connections),
                Prefetch("media_handles", queryset=media_handles),
            )
        )
        local_participant = next(
            (
                participant
                for participant in participants
                if authenticated_profile is not None and participant.profile_id == authenticated_profile.id
            ),
            None,
        )
        visible_participants = [
            participant
            for participant in participants
            if participant.status == ParticipantStatus.ACTIVE
        ]
        membership = None
        if authenticated_profile is not None:
            membership = (
                MeetingRoomMembership.objects.filter(room=hydrated_session.room, profile=authenticated_profile, is_active=True)
                .order_by("created_at")
                .first()
            )
        can_manage_waiting_room = bool(membership and membership.can_manage_waiting_room)
        can_view_meeting_content = bool(local_participant or membership)
        remote_participants = (
            [
                participant
                for participant in visible_participants
                if local_participant is None or participant.pk != local_participant.pk
            ]
            if can_view_meeting_content
            else []
        )
        pending_requests = MeetingJoinRequest.objects.filter(
            session=hydrated_session,
            status=MeetingJoinRequestStatus.PENDING,
        ).select_related("profile", "reviewed_by_profile")
        visible_pending_requests = list(pending_requests) if can_manage_waiting_room else []
        own_join_request = None
        if authenticated_profile is not None:
            own_join_request = (
                MeetingJoinRequest.objects.filter(
                    session=hydrated_session,
                    profile=authenticated_profile,
                )
                .select_related("profile", "reviewed_by_profile")
                .order_by("-created_at")
                .first()
            )
        messages = (
            list(
                MeetingMessage.objects.filter(session=hydrated_session)
                .order_by("-created_at")[:message_limit]
            )
            if can_view_meeting_content
            else []
        )
        reactions = (
            list(
                MeetingReaction.objects.filter(
                    session=hydrated_session,
                ).filter(
                    Q(expires_at__isnull=True) | Q(expires_at__gt=timezone.now())
                ).order_by("-created_at")[:reaction_limit]
            )
            if can_view_meeting_content
            else []
        )
        messages.reverse()
        reactions.reverse()
        return {
            "room": MeetingStateBuilder.serialize_room(hydrated_session),
            "session": MeetingStateBuilder.serialize_session(hydrated_session),
            "counts": {
                "participants": hydrated_session.participant_count,
                "publishers": hydrated_session.active_publisher_count,
                "pending_join_requests": pending_requests.count() if can_manage_waiting_room else 0,
            },
            "current_profile": MeetingStateBuilder.serialize_profile_summary(authenticated_profile),
            "coordinator_permissions": MeetingStateBuilder.serialize_membership(membership),
            "local_participant": MeetingStateBuilder.serialize_participant(local_participant),
            "remote_participants": [MeetingStateBuilder.serialize_participant(item) for item in remote_participants],
            "pending_join_requests": [MeetingStateBuilder.serialize_join_request(item) for item in visible_pending_requests],
            "own_join_request": MeetingStateBuilder.serialize_join_request(own_join_request) if own_join_request else None,
            "messages": [MeetingStateBuilder.serialize_message(item) for item in messages],
            "recent_reactions": [MeetingStateBuilder.serialize_reaction(item) for item in reactions],
            "janus": MeetingStateBuilder.serialize_janus_topology(hydrated_session)
            if can_view_meeting_content
            else {},
        }

    @staticmethod
    def serialize_profile_summary(profile) -> dict | None:
        """Serialize a compact profile summary for meeting state payloads."""

        if profile is None:
            return None
        return {
            "id": str(profile.pk),
            "handle": profile.handle,
            "display_name": profile.display_name,
            "avatar_url": profile.avatar_url,
        }

    @staticmethod
    def serialize_room(session: MeetingSession) -> dict:
        """Serialize immutable or slow-changing room data for client consumption."""

        room = session.room
        return {
            "id": str(room.pk),
            "title": room.title,
            "slug": room.slug,
            "description": room.description,
            "access_policy": room.access_policy,
            "is_waiting_room_enabled": room.is_waiting_room_enabled,
            "scheduled_start_at": room.scheduled_start_at.isoformat() if room.scheduled_start_at else None,
            "scheduled_end_at": room.scheduled_end_at.isoformat() if room.scheduled_end_at else None,
            "max_participants": room.max_participants,
            "feature_flags": room.feature_flags,
            "created_by_profile": MeetingStateBuilder.serialize_profile_summary(room.created_by_profile),
        }

    @staticmethod
    def serialize_session(session: MeetingSession) -> dict:
        """Serialize hot session metadata and lifecycle state for client consumption."""

        return {
            "id": str(session.pk),
            "started_by_profile": MeetingStateBuilder.serialize_profile_summary(session.started_by_profile),
            "lifecycle_state": session.lifecycle_state,
            "janus_room_id": session.janus_room_id,
            "state_version": session.state_version,
            "started_at": session.started_at.isoformat() if session.started_at else None,
            "ended_at": session.ended_at.isoformat() if session.ended_at else None,
            "last_synced_at": session.last_synced_at.isoformat() if session.last_synced_at else None,
            "metadata": MeetingStateBuilder.serialize_public_session_metadata(
                session.metadata
            ),
        }

    @staticmethod
    def serialize_public_session_metadata(metadata: dict | None) -> dict:
        """Remove backend integration identities from participant snapshots."""

        return {
            str(key): value
            for key, value in (metadata or {}).items()
            if str(key) not in PRIVATE_SESSION_METADATA_KEYS
            and not str(key).startswith("_")
        }

    @staticmethod
    def serialize_membership(membership: MeetingRoomMembership | None) -> dict | None:
        """Serialize coordinator permissions derived from a durable room membership."""

        if membership is None:
            return None
        return {
            "membership_id": str(membership.pk),
            "role": membership.role,
            "can_manage_waiting_room": membership.can_manage_waiting_room,
            "can_manage_participants": membership.can_manage_participants,
            "can_manage_media": membership.can_manage_media,
            "can_manage_permissions": membership.can_manage_permissions,
            "can_manage_chat": membership.can_manage_chat,
            "can_manage_reactions": membership.can_manage_reactions,
            "can_manage_recording": membership.can_manage_recording,
        }

    @staticmethod
    def serialize_participant(participant: Participant | None) -> dict | None:
        """Serialize a participant, including nested connection, handle, and stream state."""

        if participant is None:
            return None
        return {
            "id": str(participant.pk),
            "profile": MeetingStateBuilder.serialize_profile_summary(participant.profile),
            "role": participant.role,
            "status": participant.status,
            "display_name": participant.display_name,
            "can_publish_audio": participant.can_publish_audio,
            "can_publish_video": participant.can_publish_video,
            "can_share_screen": participant.can_share_screen,
            "can_chat": participant.can_chat,
            "can_react": participant.can_react,
            "is_muted": participant.is_muted,
            "is_camera_blocked": participant.is_camera_blocked,
            "raised_hand_at": participant.raised_hand_at.isoformat() if participant.raised_hand_at else None,
            "janus_publisher_id": participant.janus_publisher_id,
            "joined_at": participant.joined_at.isoformat() if participant.joined_at else None,
            "left_at": participant.left_at.isoformat() if participant.left_at else None,
            "last_seen_at": participant.last_seen_at.isoformat() if participant.last_seen_at else None,
            "metadata": participant.metadata,
            "connections": [
                {
                    "transport": connection.transport,
                    "status": connection.status,
                    "connected_at": connection.connected_at.isoformat() if connection.connected_at else None,
                    "disconnected_at": connection.disconnected_at.isoformat() if connection.disconnected_at else None,
                    "last_heartbeat_at": connection.last_heartbeat_at.isoformat() if connection.last_heartbeat_at else None,
                }
                for connection in participant.connections.all()
            ],
            "media_handles": [
                {
                    "id": str(handle.pk),
                    "handle_type": handle.handle_type,
                    "lifecycle_state": handle.lifecycle_state,
                    "selected_streams": handle.selected_streams,
                    "last_event_at": handle.last_event_at.isoformat() if handle.last_event_at else None,
                    "streams": [
                        {
                            "id": str(stream.pk),
                            "direction": stream.direction,
                            "media_kind": stream.media_kind,
                            "janus_mid": stream.janus_mid,
                            "janus_feed_id": stream.janus_feed_id,
                            "janus_feed_mid": stream.janus_feed_mid,
                            "codec": stream.codec,
                            "is_active": stream.is_active,
                            "is_ready": stream.is_ready,
                            "is_moderated": stream.is_moderated,
                            "metadata": stream.metadata,
                            "source_participant_id": str(stream.source_participant_id) if stream.source_participant_id else None,
                        }
                        for stream in handle.streams.all()
                    ],
                }
                for handle in participant.media_handles.all()
            ],
        }

    @staticmethod
    def serialize_join_request(join_request: MeetingJoinRequest) -> dict:
        """Serialize a waiting-room join request for coordinator review surfaces."""

        return {
            "id": str(join_request.pk),
            "profile": MeetingStateBuilder.serialize_profile_summary(join_request.profile),
            "status": join_request.status,
            "requested_display_name": join_request.requested_display_name,
            "requested_role": join_request.requested_role,
            "note": join_request.note,
            "reviewed_by_profile": MeetingStateBuilder.serialize_profile_summary(join_request.reviewed_by_profile),
            "reviewed_at": join_request.reviewed_at.isoformat() if join_request.reviewed_at else None,
            "resolution_reason": join_request.resolution_reason,
            "created_at": join_request.created_at.isoformat(),
        }

    @staticmethod
    def serialize_janus_topology(session: MeetingSession) -> dict:
        """Expose only the publisher topology needed for client resubscription."""

        raw_participants = (session.janus_state or {}).get("participants") or []
        if not isinstance(raw_participants, list):
            return {"participants": []}
        participants = []
        for raw_participant in raw_participants:
            if not isinstance(raw_participant, dict):
                continue
            raw_streams = raw_participant.get("streams") or []
            streams = []
            if isinstance(raw_streams, list):
                for raw_stream in raw_streams:
                    if not isinstance(raw_stream, dict):
                        continue
                    streams.append(
                        {
                            key: raw_stream.get(key)
                            for key in (
                                "mid",
                                "type",
                                "codec",
                                "description",
                                "disabled",
                                "moderated",
                            )
                            if key in raw_stream
                        }
                    )
            participants.append(
                {
                    key: raw_participant.get(key)
                    for key in ("id", "display", "publisher", "talking")
                    if key in raw_participant
                }
                | {"streams": streams}
            )
        return {"participants": participants}

    @staticmethod
    def serialize_admission_result(result) -> dict:
        """Serialize a centralized admission decision for the joining client."""

        session = None
        if result.participant is not None:
            session = result.participant.session
        elif result.join_request is not None:
            session = result.join_request.session
        action = "enter" if result.participant is not None else "wait"
        return {
            "status": result.status,
            "action": action,
            "requires_approval": action == "wait",
            "participant_status": result.participant.status if result.participant is not None else result.status,
            "direct_entry": result.direct_entry,
            "session": MeetingStateBuilder.serialize_session(session) if session is not None else None,
            "token": None,
            "participant": MeetingStateBuilder.serialize_participant(result.participant),
            "join_request": MeetingStateBuilder.serialize_join_request(result.join_request) if result.join_request else None,
            "join_request_id": str(result.join_request_id) if getattr(result, "join_request_id", None) else (str(result.join_request.pk) if result.join_request else None),
        }

    @staticmethod
    def serialize_message(message: MeetingMessage) -> dict:
        """Serialize a chat or system message for room timeline rendering."""

        return {
            "id": str(message.pk),
            "participant_id": str(message.participant_id) if message.participant_id else None,
            "kind": message.kind,
            "body": message.body,
            "metadata": message.metadata,
            "created_at": message.created_at.isoformat(),
        }

    @staticmethod
    def serialize_reaction(reaction: MeetingReaction) -> dict:
        """Serialize a recent reaction for ephemeral timeline or overlay rendering."""

        return {
            "id": str(reaction.pk),
            "participant_id": str(reaction.participant_id),
            "reaction": reaction.reaction,
            "expires_at": reaction.expires_at.isoformat() if reaction.expires_at else None,
            "metadata": reaction.metadata,
            "created_at": reaction.created_at.isoformat(),
        }
