"""Meeting state aggregation helpers used by APIs, Socket.IO, and Celery workers."""

from __future__ import annotations

from apps.meetings.models import (
    MeetingJoinRequest,
    MeetingJoinRequestStatus,
    MeetingMessage,
    MeetingReaction,
    MeetingRoomMembership,
    MeetingSession,
    Participant,
)


class MeetingStateBuilder:
    """Build a client-facing session state snapshot from normalized meeting models."""

    @staticmethod
    def build(session: MeetingSession, authenticated_profile=None, message_limit: int = 50, reaction_limit: int = 25) -> dict:
        """Return a serialized state snapshot for a meeting session."""

        hydrated_session = (
            MeetingSession.objects.select_related("room", "started_by_profile")
            .prefetch_related(
                "participants__profile",
                "participants__membership",
                "participants__connections",
                "participants__media_handles__streams",
                "participants__streams",
                "join_requests__profile",
                "join_requests__reviewed_by_profile",
                "join_requests__connection",
                "messages__participant__profile",
                "reactions__participant__profile",
            )
            .get(pk=session.pk)
        )
        participants = list(hydrated_session.participants.all())
        local_participant = next(
            (
                participant
                for participant in participants
                if authenticated_profile is not None and participant.profile_id == authenticated_profile.id
            ),
            None,
        )
        remote_participants = [participant for participant in participants if local_participant is None or participant.pk != local_participant.pk]
        membership = None
        if authenticated_profile is not None:
            membership = (
                MeetingRoomMembership.objects.filter(room=hydrated_session.room, profile=authenticated_profile, is_active=True)
                .order_by("created_at")
                .first()
            )
        can_manage_waiting_room = bool(membership and membership.can_manage_waiting_room)
        pending_requests = hydrated_session.join_requests.filter(status=MeetingJoinRequestStatus.PENDING)
        visible_pending_requests = pending_requests if can_manage_waiting_room else []
        messages = list(hydrated_session.messages.all().order_by("-created_at")[:message_limit])
        reactions = list(hydrated_session.reactions.all().order_by("-created_at")[:reaction_limit])
        messages.reverse()
        return {
            "room": MeetingStateBuilder.serialize_room(hydrated_session),
            "session": MeetingStateBuilder.serialize_session(hydrated_session),
            "counts": {
                "participants": hydrated_session.participant_count or len(participants),
                "publishers": hydrated_session.active_publisher_count,
                "pending_join_requests": pending_requests.count() if can_manage_waiting_room else 0,
            },
            "coordinator_permissions": MeetingStateBuilder.serialize_membership(membership),
            "local_participant": MeetingStateBuilder.serialize_participant(local_participant),
            "remote_participants": [MeetingStateBuilder.serialize_participant(item) for item in remote_participants],
            "pending_join_requests": [MeetingStateBuilder.serialize_join_request(item) for item in visible_pending_requests],
            "messages": [MeetingStateBuilder.serialize_message(item) for item in messages],
            "recent_reactions": [MeetingStateBuilder.serialize_reaction(item) for item in reactions],
            "janus": hydrated_session.janus_state,
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
            "control_handle_id": session.control_handle_id,
            "state_version": session.state_version,
            "started_at": session.started_at.isoformat() if session.started_at else None,
            "ended_at": session.ended_at.isoformat() if session.ended_at else None,
            "last_synced_at": session.last_synced_at.isoformat() if session.last_synced_at else None,
            "metadata": session.metadata,
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
            "janus_private_id": participant.janus_private_id,
            "janus_state": participant.janus_state,
            "joined_at": participant.joined_at.isoformat() if participant.joined_at else None,
            "left_at": participant.left_at.isoformat() if participant.left_at else None,
            "last_seen_at": participant.last_seen_at.isoformat() if participant.last_seen_at else None,
            "metadata": participant.metadata,
            "connections": [
                {
                    "id": str(connection.pk),
                    "socket_id": connection.socket_id,
                    "transport": connection.transport,
                    "status": connection.status,
                    "client_session_key": connection.client_session_key,
                    "connected_at": connection.connected_at.isoformat() if connection.connected_at else None,
                    "disconnected_at": connection.disconnected_at.isoformat() if connection.disconnected_at else None,
                    "last_heartbeat_at": connection.last_heartbeat_at.isoformat() if connection.last_heartbeat_at else None,
                    "metadata": connection.metadata,
                }
                for connection in participant.connections.all()
            ],
            "media_handles": [
                {
                    "id": str(handle.pk),
                    "handle_type": handle.handle_type,
                    "lifecycle_state": handle.lifecycle_state,
                    "janus_session_id": handle.janus_session_id,
                    "janus_handle_id": handle.janus_handle_id,
                    "opaque_id": handle.opaque_id,
                    "selected_streams": handle.selected_streams,
                    "janus_state": handle.janus_state,
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
            "connection_id": str(join_request.connection_id) if join_request.connection_id else None,
            "status": join_request.status,
            "requested_display_name": join_request.requested_display_name,
            "requested_role": join_request.requested_role,
            "note": join_request.note,
            "client_state": join_request.client_state,
            "reviewed_by_profile": MeetingStateBuilder.serialize_profile_summary(join_request.reviewed_by_profile),
            "reviewed_at": join_request.reviewed_at.isoformat() if join_request.reviewed_at else None,
            "resolution_reason": join_request.resolution_reason,
            "created_at": join_request.created_at.isoformat(),
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
