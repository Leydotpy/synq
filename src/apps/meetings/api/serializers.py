"""DRF serializers for meeting room creation and session workflow inputs."""

from __future__ import annotations

from rest_framework import serializers

from apps.meetings.models import MeetingAccessPolicy, MeetingRole, MeetingRoom
from apps.meetings.services.lifecycle import MeetingLifecycleService


RESERVED_INTEGRATION_METADATA_KEYS = frozenset(
    {
        "created_from",
        "external_id",
        "external_provider",
        "integration_provider",
        "provider",
        "service_owner_id",
        "service_owner_profile_id",
        "source",
    }
)


def validate_public_metadata(value: dict) -> dict:
    """Prevent browser-writable metadata from claiming integration identities."""

    reserved_keys = sorted(RESERVED_INTEGRATION_METADATA_KEYS & set(value))
    if reserved_keys:
        raise serializers.ValidationError(
            f"Reserved integration metadata key(s): {', '.join(reserved_keys)}."
        )
    return value


def validate_schedule_range(attrs: dict) -> dict:
    """Validate the chronological invariant shared by all scheduling inputs."""

    scheduled_start_at = attrs.get("scheduled_start_at")
    scheduled_end_at = attrs.get("scheduled_end_at")
    if scheduled_start_at and scheduled_end_at and scheduled_end_at < scheduled_start_at:
        raise serializers.ValidationError(
            "scheduled_end_at must be greater than or equal to scheduled_start_at."
        )
    return attrs


class ObjectJSONField(serializers.JSONField):
    """Accept only JSON objects for fields consumed as dictionaries."""

    default_error_messages = {"not_object": "Expected a JSON object."}

    def to_internal_value(self, data):
        value = super().to_internal_value(data)
        if not isinstance(value, dict):
            self.fail("not_object")
        return value


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

    def create(self, validated_data: dict) -> MeetingRoom:
        """Create a room using the meeting lifecycle service."""

        passcode = validated_data.pop("passcode", None)
        return MeetingLifecycleService.create_room(
            creator_profile=self.context["profile"],
            passcode=passcode,
            **validated_data,
        )

    def validate_metadata(self, value: dict) -> dict:
        """Reject integration-owned keys on public room creation."""

        return validate_public_metadata(value)

    def validate(self, attrs: dict) -> dict:
        """Validate the optional room schedule before model creation."""

        return validate_schedule_range(attrs)


class MeetingSessionStartSerializer(serializers.Serializer):
    """Validate session startup requests."""

    metadata = ObjectJSONField(required=False)

    def validate_metadata(self, value: dict) -> dict:
        """Reject integration-owned keys on public room-session starts."""

        return validate_public_metadata(value)


class MeetingSessionEndSerializer(serializers.Serializer):
    """Validate an explicit coordinator request to end a live session."""

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

    def validate_metadata(self, value: dict) -> dict:
        """Reject integration-owned keys on public one-shot session creation."""

        return validate_public_metadata(value)

    def validate(self, attrs: dict) -> dict:
        """Ensure scheduled ranges remain chronologically valid."""

        return validate_schedule_range(attrs)


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
    """Validate the single Join-button admission request."""

    client_session_key = serializers.CharField(required=False, allow_blank=True, max_length=255)


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
    expires_in_seconds = serializers.IntegerField(required=False, min_value=300, max_value=60 * 60 * 24 * 30)

    def validate(self, attrs: dict) -> dict:
        """Reject inverted schedules before creating a service-owned room."""

        return validate_schedule_range(attrs)


class ParticipantUpdateSerializer(serializers.Serializer):
    """Validate participant capability and moderation updates."""

    updates = ObjectJSONField()

    def validate_updates(self, value: dict) -> dict:
        """Reject unsupported fields and invalid moderation value types."""

        boolean_fields = {
            "can_publish_audio",
            "can_publish_video",
            "can_share_screen",
            "can_chat",
            "can_react",
            "is_muted",
            "is_camera_blocked",
        }
        allowed_fields = boolean_fields | {"raised_hand_at"}
        unknown_fields = sorted(set(value) - allowed_fields)
        if unknown_fields:
            raise serializers.ValidationError(
                f"Unsupported participant update field(s): {', '.join(unknown_fields)}."
            )
        for field_name in boolean_fields & set(value):
            if not isinstance(value[field_name], bool):
                raise serializers.ValidationError({field_name: "Expected a boolean."})
        if "raised_hand_at" in value:
            value["raised_hand_at"] = serializers.DateTimeField(allow_null=True).run_validation(
                value["raised_hand_at"],
            )
        return value


class ParticipantRemovalSerializer(serializers.Serializer):
    """Validate participant removal input."""

    reason = serializers.CharField(required=False, allow_blank=True)
