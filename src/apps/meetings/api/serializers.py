"""DRF serializers for meeting room creation and session workflow inputs."""

from __future__ import annotations

from rest_framework import serializers

from apps.meetings.models import MeetingAccessPolicy, MeetingRole, MeetingRoom
from apps.meetings.services.lifecycle import MeetingLifecycleService


class ObjectJSONField(serializers.JSONField):
    """Accept JSON objects while rejecting array/scalar payloads early."""

    default_error_messages = {"not_object": "Expected a JSON object."}

    def to_internal_value(self, data):
        """Decode JSON, then enforce the mapping contract used by services."""

        value = super().to_internal_value(data)
        if not isinstance(value, dict):
            self.fail("not_object")
        return value


def validate_scheduled_range(attrs: dict) -> dict:
    """Reject inverted meeting schedules consistently across API surfaces."""

    scheduled_start_at = attrs.get("scheduled_start_at")
    scheduled_end_at = attrs.get("scheduled_end_at")
    if (
        scheduled_start_at
        and scheduled_end_at
        and scheduled_end_at < scheduled_start_at
    ):
        raise serializers.ValidationError(
            {
                "scheduled_end_at": (
                    "Must be greater than or equal to scheduled_start_at."
                ),
            },
        )
    return attrs


class MeetingRoomSerializer(serializers.ModelSerializer):
    """Serialize room details and delegate creation to the lifecycle service."""

    passcode = serializers.CharField(required=False, allow_blank=True, write_only=True)
    janus_room_configuration = ObjectJSONField(required=False)
    feature_flags = ObjectJSONField(required=False)
    metadata = ObjectJSONField(required=False)

    class Meta:
        """Expose room fields relevant to room setup and administration workflows."""

        model = MeetingRoom
        fields = (
            "id",
            "title",
            "slug",
            "description",
            "access_policy",
            "is_waiting_room_enabled",
            "scheduled_start_at",
            "scheduled_end_at",
            "max_participants",
            "janus_room_configuration",
            "feature_flags",
            "metadata",
            "passcode",
            "created_at",
            "updated_at",
        )
        read_only_fields = ("id", "slug", "created_at", "updated_at")

    def validate(self, attrs: dict) -> dict:
        """Validate the room's optional scheduled time range."""

        return validate_scheduled_range(attrs)

    def create(self, validated_data: dict) -> MeetingRoom:
        """Create a room using the meeting lifecycle service."""

        request = self.context["request"]
        passcode = validated_data.pop("passcode", None)
        return MeetingLifecycleService.create_room(
            creator_profile=self.context["profile"],
            passcode=passcode,
            **validated_data,
        )


class MeetingSessionStartSerializer(serializers.Serializer):
    """Validate session startup requests."""

    metadata = ObjectJSONField(required=False)


class MeetingSessionEndSerializer(serializers.Serializer):
    """Validate an explicit coordinator request to end a meeting."""

    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)


class MeetingSessionCreateSerializer(serializers.Serializer):
    """Validate browser-facing room, session, and invite provisioning requests."""

    title = serializers.CharField(required=False, allow_blank=True, max_length=255)
    description = serializers.CharField(required=False, allow_blank=True, max_length=2000)
    scheduled_start_at = serializers.DateTimeField(required=False, allow_null=True)
    scheduled_end_at = serializers.DateTimeField(required=False, allow_null=True)
    access_policy = serializers.ChoiceField(
        choices=MeetingAccessPolicy.choices,
        required=False,
        default=MeetingAccessPolicy.APPROVAL_REQUIRED,
    )
    is_waiting_room_enabled = serializers.BooleanField(required=False, default=True)
    max_participants = serializers.IntegerField(required=False, min_value=2, max_value=500)
    passcode = serializers.CharField(required=False, allow_blank=True, write_only=True)
    participant_emails = serializers.ListField(
        child=serializers.EmailField(),
        required=False,
        allow_empty=True,
        max_length=50,
    )
    message = serializers.CharField(required=False, allow_blank=True, max_length=2000)
    metadata = ObjectJSONField(required=False)
    expires_in_seconds = serializers.IntegerField(required=False, min_value=300, max_value=60 * 60 * 24 * 30)

    def validate_participant_emails(self, value: list[str]) -> list[str]:
        """Normalize invited emails while preserving first-seen ordering."""

        normalized: list[str] = []
        seen: set[str] = set()
        for email in value:
            cleaned = email.strip().lower()
            if cleaned and cleaned not in seen:
                normalized.append(cleaned)
                seen.add(cleaned)
        return normalized

    def validate(self, attrs: dict) -> dict:
        """Ensure scheduled ranges remain chronologically valid."""

        return validate_scheduled_range(attrs)


class MeetingJoinRequestCreateSerializer(serializers.Serializer):
    """Validate waiting-room join request input."""

    display_name = serializers.CharField(required=False, allow_blank=True, max_length=255)
    requested_role = serializers.ChoiceField(
        choices=[(MeetingRole.PARTICIPANT, "Participant")],
        required=False,
        default=MeetingRole.PARTICIPANT,
    )
    note = serializers.CharField(required=False, allow_blank=True)
    client_state = ObjectJSONField(required=False)
    passcode = serializers.CharField(required=False, allow_blank=True)
    invite_token = serializers.CharField(required=False, allow_blank=True)

    def validate_requested_role(self, value: str) -> str:
        """Restrict requested roles to safe session-facing values."""

        if value != MeetingRole.PARTICIPANT:
            raise serializers.ValidationError("Unsupported requested role.")
        return value


class MeetingAdmissionSerializer(MeetingJoinRequestCreateSerializer):
    """Validate the client's single Join-button admission request."""

    client_session_key = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=255,
    )


class MeetingJoinRequestReviewSerializer(serializers.Serializer):
    """Validate waiting-room review actions."""

    approve = serializers.BooleanField()
    reason = serializers.CharField(required=False, allow_blank=True)


class MeetingSessionShareSerializer(serializers.Serializer):
    """Validate meeting invite link sharing requests."""

    emails = serializers.ListField(
        child=serializers.EmailField(),
        allow_empty=False,
        max_length=50,
    )
    message = serializers.CharField(required=False, allow_blank=True, max_length=2000)
    expires_in_seconds = serializers.IntegerField(required=False, min_value=300, max_value=60 * 60 * 24 * 30)


class MeetingServiceSessionCreateSerializer(serializers.Serializer):
    """Validate backend-to-backend room/session provisioning requests."""

    external_id = serializers.CharField(max_length=255)
    title = serializers.CharField(max_length=255)
    description = serializers.CharField(required=False, allow_blank=True)
    scheduled_start_at = serializers.DateTimeField(required=False, allow_null=True)
    scheduled_end_at = serializers.DateTimeField(required=False, allow_null=True)
    participant_emails = serializers.ListField(
        child=serializers.EmailField(),
        required=False,
        max_length=50,
    )
    metadata = ObjectJSONField(required=False)

    def validate(self, attrs: dict) -> dict:
        """Validate the service-created room's scheduled time range."""

        return validate_scheduled_range(attrs)
    expires_in_seconds = serializers.IntegerField(required=False, min_value=300, max_value=60 * 60 * 24 * 30)


class ParticipantUpdateSerializer(serializers.Serializer):
    """Validate participant capability and moderation updates."""

    updates = ObjectJSONField()


class ParticipantRemovalSerializer(serializers.Serializer):
    """Validate participant removal input."""

    reason = serializers.CharField(required=False, allow_blank=True)
