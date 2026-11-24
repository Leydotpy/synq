"""Meeting state aggregation helpers for authenticated HTTP clients."""

from __future__ import annotations

from apps.meetings.models import (
    MeetingJoinRequest,
    MeetingMessage,
    MeetingReaction,
    MeetingRoomMembership,
    MeetingSession,
    Participant,
)

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
    def build(
        session: MeetingSession,
        authenticated_profile=None,
        message_limit: int = 50,
        reaction_limit: int = 25,
    ) -> dict:
        """Return a serialized state snapshot for a meeting session."""

        from django.core.exceptions import PermissionDenied
        from apps.meetings.services.gateway_state import snapshot
        from apps.meetings.services.gateway_tickets import GatewayDenied

        if authenticated_profile is None:
            raise PermissionDenied
        try:
            state = snapshot(session.pk, authenticated_profile.pk, sdk=True)
            # The original HTTP endpoint also exposes the SDK presentation fields.
            # Both shapes come from the same authorized projection and version.
            sdk = state.pop("sdk")
            sdk.pop("janus", None)
            local = {**(sdk["local_participant"] or {}), **state["local_participant"]}
            return {**sdk, **state, "local_participant": local}
        except GatewayDenied:
            raise PermissionDenied from None

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
            "scheduled_start_at": room.scheduled_start_at.isoformat()
            if room.scheduled_start_at
            else None,
            "scheduled_end_at": room.scheduled_end_at.isoformat()
            if room.scheduled_end_at
            else None,
            "max_participants": room.max_participants,
            "feature_flags": room.feature_flags,
            "created_by_profile": MeetingStateBuilder.serialize_profile_summary(
                room.created_by_profile
            ),
        }

    @staticmethod
    def serialize_session(session: MeetingSession) -> dict:
        """Serialize hot session metadata and lifecycle state for client consumption."""

        return {
            "id": str(session.pk),
            "started_by_profile": MeetingStateBuilder.serialize_profile_summary(
                session.started_by_profile
            ),
            "lifecycle_state": session.lifecycle_state,
            "state_version": session.state_version,
            "started_at": session.started_at.isoformat()
            if session.started_at
            else None,
            "ended_at": session.ended_at.isoformat() if session.ended_at else None,
            "last_synced_at": session.last_synced_at.isoformat()
            if session.last_synced_at
            else None,
            "metadata": MeetingStateBuilder.serialize_public_session_metadata(
                session.metadata,
            ),
        }

    @staticmethod
    def serialize_public_session_metadata(metadata: dict | None) -> dict:
        """Remove service integration identities from client snapshots."""

        if not isinstance(metadata, dict):
            return {}
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
        """Serialize a participant with the historical transport fields left empty."""

        if participant is None:
            return None
        return {
            "id": str(participant.pk),
            "profile": MeetingStateBuilder.serialize_profile_summary(
                participant.profile
            ),
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
            "raised_hand_at": participant.raised_hand_at.isoformat()
            if participant.raised_hand_at
            else None,
            "joined_at": participant.joined_at.isoformat()
            if participant.joined_at
            else None,
            "left_at": participant.left_at.isoformat() if participant.left_at else None,
            "last_seen_at": participant.last_seen_at.isoformat()
            if participant.last_seen_at
            else None,
            "metadata": participant.metadata,
            "connections": [],
            "media_handles": [],
        }

    @staticmethod
    def serialize_join_request(join_request: MeetingJoinRequest) -> dict:
        """Serialize a waiting-room join request for coordinator review surfaces."""

        return {
            "id": str(join_request.pk),
            "profile": MeetingStateBuilder.serialize_profile_summary(
                join_request.profile
            ),
            "status": join_request.status,
            "requested_display_name": join_request.requested_display_name,
            "requested_role": join_request.requested_role,
            "note": join_request.note,
            "reviewed_by_profile": MeetingStateBuilder.serialize_profile_summary(
                join_request.reviewed_by_profile
            ),
            "reviewed_at": join_request.reviewed_at.isoformat()
            if join_request.reviewed_at
            else None,
            "resolution_reason": join_request.resolution_reason,
            "created_at": join_request.created_at.isoformat(),
        }

    @staticmethod
    def serialize_admission_result(result) -> dict:
        """Serialize the direct-entry or waiting-room decision expected by the client."""

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
            "participant_status": (
                result.participant.status
                if result.participant is not None
                else result.status
            ),
            "direct_entry": result.direct_entry,
            "session": MeetingStateBuilder.serialize_session(session)
            if session
            else None,
            "token": None,
            "participant": MeetingStateBuilder.serialize_participant(
                result.participant
            ),
            "join_request": (
                MeetingStateBuilder.serialize_join_request(result.join_request)
                if result.join_request
                else None
            ),
            "join_request_id": (
                str(result.join_request.pk) if result.join_request else None
            ),
        }

    @staticmethod
    def serialize_message(message: MeetingMessage) -> dict:
        """Serialize a chat or system message for room timeline rendering."""

        return {
            "id": str(message.pk),
            "participant_id": str(message.participant_id)
            if message.participant_id
            else None,
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
            "expires_at": reaction.expires_at.isoformat()
            if reaction.expires_at
            else None,
            "metadata": reaction.metadata,
            "created_at": reaction.created_at.isoformat(),
        }
