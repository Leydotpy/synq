"""Authorization helpers for room coordination and participant interaction rules."""

from __future__ import annotations

from apps.meetings.exceptions import MeetingPermissionDeniedError
from apps.meetings.models import MeetingRoom, MeetingRoomMembership, MeetingSession, Participant
from core.api.api import _get_or_create_profile_for_user


class MeetingPermissionService:
    """Centralize meeting-domain permission checks in one explicit service layer."""

    @staticmethod
    def _resolve_profile(profile_or_user):
        """Normalize a profile or authenticated auth user into a ``Profile`` instance."""

        if profile_or_user is None:
            return None
        if hasattr(profile_or_user, "_meta") and profile_or_user._meta.label_lower == "profiles.profile":
            return profile_or_user
        if getattr(profile_or_user, "is_authenticated", False):
            return _get_or_create_profile_for_user(profile_or_user)
        return None

    @staticmethod
    def get_room_membership(room: MeetingRoom, profile_or_user) -> MeetingRoomMembership | None:
        """Return the active room membership for a profile, if one exists."""

        profile = MeetingPermissionService._resolve_profile(profile_or_user)
        if profile is None:
            return None
        return (
            MeetingRoomMembership.objects.filter(room=room, profile=profile, is_active=True)
            .order_by("created_at")
            .first()
        )

    @staticmethod
    def require_room_permission(room: MeetingRoom, profile_or_user, permission_field: str) -> MeetingRoomMembership:
        """Require a durable room permission and raise a domain exception when missing."""

        membership = MeetingPermissionService.get_room_membership(room=room, profile_or_user=profile_or_user)
        if not membership or not getattr(membership, permission_field, False):
            raise MeetingPermissionDeniedError(f"Profile does not have '{permission_field}' in room '{room}'.")
        return membership

    @staticmethod
    def require_session_permission(session: MeetingSession, profile_or_user, permission_field: str) -> MeetingRoomMembership:
        """Require a session-scoped coordinator permission derived from the room membership."""

        return MeetingPermissionService.require_room_permission(
            room=session.room,
            profile_or_user=profile_or_user,
            permission_field=permission_field,
        )

    @staticmethod
    def participant_can_interact(participant: Participant, capability_field: str) -> bool:
        """Return whether a participant currently has a given interaction capability."""

        return bool(getattr(participant, capability_field, False))

    @staticmethod
    def require_participant_capability(participant: Participant, capability_field: str) -> None:
        """Require a participant interaction capability and raise a domain exception when missing."""

        if not MeetingPermissionService.participant_can_interact(participant=participant, capability_field=capability_field):
            raise MeetingPermissionDeniedError(f"Participant '{participant}' does not have '{capability_field}'.")
