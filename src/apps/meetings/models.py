"""Persistence models for collaborative rooms, session state, and Janus attachments."""

from __future__ import annotations

from typing import Any

from django.contrib.auth.hashers import check_password, make_password
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models
from django.utils import timezone
from django.utils.text import slugify

from apps.profiles.models import Profile
from core.models import UUIDTimestampedModel


def coordinator_permission_defaults(role: str) -> dict[str, bool]:
    """Return the coordinator permissions granted by a durable room role."""

    if role == MeetingRole.HOST:
        return {
            "can_manage_waiting_room": True,
            "can_manage_participants": True,
            "can_manage_media": True,
            "can_manage_permissions": True,
            "can_manage_chat": True,
            "can_manage_reactions": True,
            "can_manage_recording": True,
        }
    if role == MeetingRole.CO_HOST:
        return {
            "can_manage_waiting_room": True,
            "can_manage_participants": True,
            "can_manage_media": True,
            "can_manage_permissions": True,
            "can_manage_chat": True,
            "can_manage_reactions": True,
            "can_manage_recording": False,
        }
    return {
        "can_manage_waiting_room": False,
        "can_manage_participants": False,
        "can_manage_media": False,
        "can_manage_permissions": False,
        "can_manage_chat": False,
        "can_manage_reactions": False,
        "can_manage_recording": False,
    }


def participant_capability_defaults(role: str) -> dict[str, bool]:
    """Return the in-session interaction permissions granted by a participant role."""

    capabilities = {
        "can_publish_audio": True,
        "can_publish_video": True,
        "can_share_screen": False,
        "can_chat": True,
        "can_react": True,
    }
    if role in {MeetingRole.HOST, MeetingRole.CO_HOST}:
        capabilities["can_share_screen"] = True
    return capabilities


class MeetingAccessPolicy(models.TextChoices):
    """Enumerate how prospective attendees may enter a room."""

    OPEN = "open", "Open"
    APPROVAL_REQUIRED = "approval_required", "Approval Required"
    INVITE_ONLY = "invite_only", "Invite Only"


class ExternalMeetingProvider(models.TextChoices):
    """Enumerate trusted systems that bind an external meeting identity."""

    LAW_FIRM_WORKSPACE = "law_firm_workspace", "Law firm workspace"


class MeetingRole(models.TextChoices):
    """Enumerate durable and in-session meeting roles."""

    HOST = "host", "Host"
    CO_HOST = "co_host", "Co-host"
    PARTICIPANT = "participant", "Participant"


class MeetingLifecycleState(models.TextChoices):
    """Enumerate the lifecycle phases of a live meeting session."""

    SCHEDULED = "scheduled", "Scheduled"
    PROVISIONING = "provisioning", "Provisioning"
    WAITING = "waiting", "Waiting"
    ACTIVE = "active", "Active"
    ENDING = "ending", "Ending"
    ENDED = "ended", "Ended"
    FAILED = "failed", "Failed"


class MeetingJoinRequestStatus(models.TextChoices):
    """Enumerate the lifecycle phases of a waiting-room join request."""

    PENDING = "pending", "Pending"
    ADMITTED = "admitted", "Admitted"
    REJECTED = "rejected", "Rejected"
    CANCELLED = "cancelled", "Cancelled"
    EXPIRED = "expired", "Expired"


class ParticipantStatus(models.TextChoices):
    """Enumerate the lifecycle phases of a logical participant presence."""

    WAITING = "waiting", "Waiting"
    ADMITTED = "admitted", "Admitted"
    ACTIVE = "active", "Active"
    DISCONNECTED = "disconnected", "Disconnected"
    LEFT = "left", "Left"
    REMOVED = "removed", "Removed"


class RealtimeConnectionTransport(models.TextChoices):
    """Enumerate the client platforms that may connect to the realtime gateway."""

    WEB = "web", "Web Browser"
    MOBILE = "mobile", "Mobile App"
    DESKTOP = "desktop", "Desktop App"
    BOT = "bot", "Automation"


class RealtimeConnectionStatus(models.TextChoices):
    """Enumerate the lifecycle phases of a Socket.IO connection."""

    CONNECTED = "connected", "Connected"
    SUBSCRIBED = "subscribed", "Subscribed"
    ACTIVE = "active", "Active"
    DISCONNECTED = "disconnected", "Disconnected"
    STALE = "stale", "Stale"


class JanusHandleType(models.TextChoices):
    """Enumerate the Janus plugin roles tracked for participants."""

    PUBLISHER = "publisher", "Publisher"
    SUBSCRIBER = "subscriber", "Subscriber"
    TEXTROOM = "textroom", "TextRoom"


class JanusHandleLifecycleState(models.TextChoices):
    """Enumerate the lifecycle phases of a Janus plugin attachment."""

    ATTACHING = "attaching", "Attaching"
    ATTACHED = "attached", "Attached"
    JOINING = "joining", "Joining"
    READY = "ready", "Ready"
    DETACHING = "detaching", "Detaching"
    DETACHED = "detached", "Detached"
    FAILED = "failed", "Failed"


class MediaDirection(models.TextChoices):
    """Enumerate whether a tracked media stream is inbound or outbound."""

    OUTBOUND = "outbound", "Outbound"
    INBOUND = "inbound", "Inbound"


class MediaKind(models.TextChoices):
    """Enumerate the media kinds carried within the meeting."""

    AUDIO = "audio", "Audio"
    VIDEO = "video", "Video"
    DATA = "data", "Data"
    SCREEN = "screen", "Screen"


class MessageKind(models.TextChoices):
    """Enumerate persisted meeting message categories."""

    CHAT = "chat", "Chat"
    SYSTEM = "system", "System"
    MODERATION = "moderation", "Moderation"


class MeetingEventType(models.TextChoices):
    """Enumerate audit events persisted for observability and replay."""

    ROOM_CREATED = "room_created", "Room Created"
    SESSION_CREATED = "session_created", "Session Created"
    SESSION_PROVISIONED = "session_provisioned", "Session Provisioned"
    SESSION_ENDED = "session_ended", "Session Ended"
    JOIN_REQUEST_CREATED = "join_request_created", "Join Request Created"
    JOIN_REQUEST_REVIEWED = "join_request_reviewed", "Join Request Reviewed"
    PARTICIPANT_JOINED = "participant_joined", "Participant Joined"
    PARTICIPANT_LEFT = "participant_left", "Participant Left"
    PARTICIPANT_REMOVED = "participant_removed", "Participant Removed"
    PARTICIPANT_UPDATED = "participant_updated", "Participant Updated"
    CHAT_MESSAGE_SENT = "chat_message_sent", "Chat Message Sent"
    REACTION_SENT = "reaction_sent", "Reaction Sent"
    JANUS_HANDLE_ATTACHED = "janus_handle_attached", "Janus Handle Attached"
    JANUS_HANDLE_FAILED = "janus_handle_failed", "Janus Handle Failed"
    STATE_SYNCED = "state_synced", "State Synced"
    CLEANUP_COMPLETED = "cleanup_completed", "Cleanup Completed"


class JrtcEventReceiptStatus(models.TextChoices):
    """Track whether an admitted JRTC broker event completed its durable work."""

    RECEIVED = "received", "Received"
    PROCESSED = "processed", "Processed"
    FAILED = "failed", "Failed"


class JrtcBrowserOutboxStatus(models.TextChoices):
    """Track durable browser forwarding for one authorized event target."""

    PENDING = "pending", "Pending"
    DELIVERING = "delivering", "Delivering"
    DELIVERED = "delivered", "Delivered"
    DISCARDED = "discarded", "Discarded"


class MeetingSessionQuerySet(models.QuerySet):
    """Reusable queryset helpers for meeting sessions."""

    def live(self) -> "MeetingSessionQuerySet":
        """Return sessions that are still operational or winding down."""

        return self.exclude(lifecycle_state__in=[MeetingLifecycleState.ENDED, MeetingLifecycleState.FAILED])


class ParticipantQuerySet(models.QuerySet):
    """Reusable queryset helpers for participant presence state."""

    def present(self) -> "ParticipantQuerySet":
        """Return participants that are still logically present in the session."""

        return self.exclude(status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED])

    def coordinators(self) -> "ParticipantQuerySet":
        """Return participants with a coordinator role in the current session."""

        return self.filter(role__in=[MeetingRole.HOST, MeetingRole.CO_HOST])


class MeetingRoom(UUIDTimestampedModel):
    """Persistent collaboration space that owns admission policy and durable coordinators."""

    # Human-readable room title shown in meeting lists, invitations, and waiting-room views.
    title = models.CharField(max_length=255)
    # URL-safe public slug used in shareable meeting links and API lookups.
    slug = models.SlugField(max_length=255, unique=True)
    # Optional rich description that explains the purpose or agenda of the room.
    description = models.TextField(blank=True)
    # Profile that originally created and owns the room definition.
    created_by_profile = models.ForeignKey(Profile, on_delete=models.PROTECT, related_name="created_meeting_rooms")
    # Policy describing whether attendees join directly or require coordinator approval.
    access_policy = models.CharField(max_length=32, choices=MeetingAccessPolicy.choices, default=MeetingAccessPolicy.APPROVAL_REQUIRED)
    # Toggle that determines whether the room enforces a waiting-room workflow.
    is_waiting_room_enabled = models.BooleanField(default=True)
    # Optional scheduled start timestamp used for reminders and UX.
    scheduled_start_at = models.DateTimeField(blank=True, null=True)
    # Optional scheduled end timestamp used for reminders and cleanup automation.
    scheduled_end_at = models.DateTimeField(blank=True, null=True)
    # Hard participant ceiling enforced by application logic and mirrored to Janus room options.
    max_participants = models.PositiveIntegerField(default=100)
    # Password hash for an optional room passcode required before join requests are accepted.
    passcode_hash = models.CharField(max_length=255, blank=True)
    # Room-level Janus configuration merged into worker-time defaults during provisioning.
    janus_room_configuration = models.JSONField(default=dict, blank=True)
    # Product feature toggles controlling optional room experiences such as reactions or recording.
    feature_flags = models.JSONField(default=dict, blank=True)
    # Unstructured room metadata reserved for analytics labels or product-specific extensions.
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        """Declare ordering and indexes for core room retrieval paths."""

        ordering = ("title",)
        indexes = [
            models.Index(fields=("slug",), name="meeting_room_slug_idx"),
            models.Index(fields=("created_by_profile",), name="meeting_room_creator_idx"),
            models.Index(fields=("access_policy",), name="meeting_room_policy_idx"),
            models.Index(fields=("scheduled_end_at",), name="meet_room_sched_end_idx"),
        ]

    def __str__(self) -> str:
        """Return the room title for admin screens and logs."""

        return self.title

    def clean(self) -> None:
        """Validate cross-field invariants before persisting the room."""

        if self.scheduled_start_at and self.scheduled_end_at and self.scheduled_end_at < self.scheduled_start_at:
            raise ValidationError("scheduled_end_at must be greater than or equal to scheduled_start_at.")
        if self.max_participants < 2:
            raise ValidationError("max_participants must be at least 2.")

    def save(self, *args: Any, **kwargs: Any) -> None:
        """Generate a stable slug automatically when one has not been provided."""

        if not self.slug:
            timestamp = timezone.now().strftime("%Y%m%d%H%M%S")
            base_slug = slugify(self.title).strip("-") or f"meeting-{timestamp}"
            candidate = base_slug
            suffix = 1
            while MeetingRoom.objects.exclude(pk=self.pk).filter(slug=candidate).exists():
                suffix += 1
                candidate = f"{base_slug}-{suffix}"
            self.slug = candidate
        self.full_clean()
        super().save(*args, **kwargs)

    def set_passcode(self, raw_passcode: str | None) -> None:
        """Hash and store a room passcode."""

        self.passcode_hash = make_password(raw_passcode) if raw_passcode else ""

    def check_passcode(self, raw_passcode: str | None) -> bool:
        """Verify a candidate passcode against the stored room hash."""

        if not self.passcode_hash:
            return True
        if not raw_passcode:
            return False
        return check_password(raw_passcode, self.passcode_hash)


class ExternalMeetingBinding(UUIDTimestampedModel):
    """Bind a trusted provider identity to one service-owned room."""

    provider = models.CharField(
        max_length=64,
        choices=ExternalMeetingProvider.choices,
        default=ExternalMeetingProvider.LAW_FIRM_WORKSPACE,
    )
    external_id = models.CharField(max_length=255)
    room = models.ForeignKey(
        MeetingRoom,
        on_delete=models.CASCADE,
        related_name="external_bindings",
    )
    service_owner_profile = models.ForeignKey(
        Profile,
        on_delete=models.PROTECT,
        related_name="owned_external_meeting_bindings",
    )

    class Meta:
        """Make provider identities and provider-to-room mappings unambiguous."""

        ordering = ("provider", "external_id")
        constraints = [
            models.UniqueConstraint(
                fields=("provider", "external_id"),
                name="meet_ext_provider_id_uniq",
            ),
            models.UniqueConstraint(
                fields=("provider", "room"),
                name="meet_ext_provider_room_uniq",
            ),
        ]
        indexes = [
            models.Index(
                fields=("service_owner_profile", "provider"),
                name="meet_ext_owner_provider_idx",
            ),
        ]

    def __str__(self) -> str:
        """Return a concise provider identity for diagnostics."""

        return f"{self.provider}:{self.external_id}"


class MeetingRoomMembership(UUIDTimestampedModel):
    """Durable room membership that grants coordinator permissions outside any one session."""

    # Room whose durable permissions and role assignment this membership belongs to.
    room = models.ForeignKey(MeetingRoom, on_delete=models.CASCADE, related_name="memberships")
    # Profile that has been granted room access or coordinator privileges.
    profile = models.ForeignKey(Profile, on_delete=models.CASCADE, related_name="meeting_memberships")
    # Durable room role that seeds coordinator powers and in-session participant defaults.
    role = models.CharField(max_length=32, choices=MeetingRole.choices, default=MeetingRole.PARTICIPANT)
    # Toggle allowing administrators to deactivate a membership without deleting history.
    is_active = models.BooleanField(default=True)
    # Permission allowing the member to admit, reject, or otherwise manage the waiting room.
    can_manage_waiting_room = models.BooleanField(default=False)
    # Permission allowing the member to remove attendees or otherwise manage room presence.
    can_manage_participants = models.BooleanField(default=False)
    # Permission allowing the member to mute participants or alter media publication rules.
    can_manage_media = models.BooleanField(default=False)
    # Permission allowing the member to grant or revoke in-session participant capabilities.
    can_manage_permissions = models.BooleanField(default=False)
    # Permission allowing the member to moderate or clear chat activity.
    can_manage_chat = models.BooleanField(default=False)
    # Permission allowing the member to moderate or suppress reactions.
    can_manage_reactions = models.BooleanField(default=False)
    # Permission allowing the member to control recording workflows.
    can_manage_recording = models.BooleanField(default=False)
    # Profile that invited or granted this room membership.
    invited_by_profile = models.ForeignKey(Profile, on_delete=models.SET_NULL, related_name="meeting_memberships_granted", blank=True, null=True)
    # Timestamp recording when the membership first became active.
    joined_at = models.DateTimeField(default=timezone.now)
    # Unstructured metadata reserved for invitation context or enterprise policy overlays.
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        """Declare constraints and indexes that support authorization lookups."""

        ordering = ("room", "profile")
        constraints = [models.UniqueConstraint(fields=("room", "profile"), name="meeting_room_membership_unique")]
        indexes = [
            models.Index(fields=("room", "role"), name="meeting_room_member_role_idx"),
            models.Index(fields=("profile", "role"), name="meeting_room_profile_role_idx"),
        ]

    def __str__(self) -> str:
        """Return a readable description of the membership for diagnostics."""

        return f"{self.profile} in {self.room} ({self.role})"

    def apply_role_defaults(self, *, preserve_explicit_grants: bool = False) -> None:
        """Project coordinator permissions from the durable role."""

        for field_name, value in coordinator_permission_defaults(self.role).items():
            if preserve_explicit_grants and getattr(self, field_name, False):
                continue
            setattr(self, field_name, value)

    def save(self, *args: Any, **kwargs: Any) -> None:
        """Seed role-derived permissions while preserving explicit custom grants."""

        self.apply_role_defaults(preserve_explicit_grants=True)
        super().save(*args, **kwargs)


class MeetingSession(UUIDTimestampedModel):
    """Live occurrence of a room that tracks Janus provisioning, state sync, and cleanup."""

    objects = MeetingSessionQuerySet.as_manager()

    # Persistent room definition from which this live session was created.
    room = models.ForeignKey(MeetingRoom, on_delete=models.CASCADE, related_name="sessions")
    # Profile that explicitly started this live session instance.
    started_by_profile = models.ForeignKey(Profile, on_delete=models.PROTECT, related_name="started_meeting_sessions")
    # Legacy control-handle identifier retained for diagnostics and forward
    # schema compatibility. Management handles themselves are process-local.
    control_handle_id = models.PositiveBigIntegerField(
        blank=True,
        null=True,
        db_index=True,
        validators=[MinValueValidator(1)],
    )
    # Lifecycle phase used by APIs, workers, and Socket.IO flows to coordinate cleanup and UX.
    lifecycle_state = models.CharField(max_length=32, choices=MeetingLifecycleState.choices, default=MeetingLifecycleState.SCHEDULED)
    # Janus VideoRoom identifier provisioned for this live meeting session.
    janus_room_id = models.PositiveBigIntegerField(
        blank=True,
        null=True,
        validators=[MinValueValidator(1)],
    )
    # Secret used for privileged Janus room actions such as destroy or moderation commands.
    janus_room_secret = models.CharField(max_length=255, blank=True)
    # Optional participant PIN forwarded to Janus when the room requires a join secret.
    janus_room_pin = models.CharField(max_length=255, blank=True)
    # Janus backend endpoint selected for this session, useful in multi-backend deployments.
    janus_backend_server = models.CharField(max_length=255, blank=True)
    # Latest known Janus VideoRoom payload cached for state fan-out and operational debugging.
    janus_state = models.JSONField(default=dict, blank=True)
    # Monotonic counter incremented whenever session state changes and clients should refresh.
    state_version = models.PositiveBigIntegerField(default=1)
    # Cached count of present participants to avoid expensive aggregation on hot paths.
    participant_count = models.PositiveIntegerField(default=0)
    # Cached count of participants currently publishing media into the Janus room.
    active_publisher_count = models.PositiveIntegerField(default=0)
    # Timestamp recording when the room became live for end-user participation.
    started_at = models.DateTimeField(blank=True, null=True)
    # Timestamp recording when the session was formally ended.
    ended_at = models.DateTimeField(blank=True, null=True)
    # Timestamp recording when cleanup completed successfully.
    cleanup_completed_at = models.DateTimeField(blank=True, null=True)
    # Lease metadata used to prevent overlapping Beat sweeps from queueing the
    # same remote Janus teardown more than once.
    cleanup_requested_at = models.DateTimeField(blank=True, null=True)
    cleanup_request_id = models.UUIDField(blank=True, editable=False, null=True)
    # Timestamp recording when application state was last synchronized from Janus or the database.
    last_synced_at = models.DateTimeField(blank=True, null=True)
    # Unstructured metadata reserved for workflow state and deployment-specific attributes.
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        """Declare ordering and indexes that support hot session retrieval."""

        ordering = ("-created_at",)
        constraints = [
            models.CheckConstraint(
                condition=models.Q(control_handle_id__isnull=True)
                | models.Q(control_handle_id__gt=0),
                name="meet_sess_control_gt_zero",
            ),
            models.CheckConstraint(
                condition=models.Q(janus_room_id__isnull=True)
                | models.Q(janus_room_id__gt=0),
                name="meet_sess_room_gt_zero",
            ),
        ]
        indexes = [
            models.Index(fields=("room", "lifecycle_state"), name="meeting_session_room_state_idx"),
            models.Index(fields=("janus_room_id",), name="meeting_session_janus_room_idx"),
            models.Index(fields=("started_by_profile",), name="meeting_session_starter_idx"),
            models.Index(
                fields=("lifecycle_state", "updated_at"),
                name="meet_sess_state_updated_idx",
            ),
            models.Index(
                fields=("lifecycle_state", "cleanup_completed_at"),
                name="meet_sess_state_cleanup_idx",
            ),
            models.Index(
                fields=(
                    "lifecycle_state",
                    "cleanup_completed_at",
                    "cleanup_requested_at",
                ),
                name="meet_sess_cleanup_lease_idx",
            ),
        ]

    def __str__(self) -> str:
        """Return a readable label for the session in logs and admin screens."""

        return f"{self.room.title} [{self.pk}]"

    @property
    def is_live(self) -> bool:
        """Return whether the session is still operational or winding down."""

        return self.lifecycle_state not in {MeetingLifecycleState.ENDED, MeetingLifecycleState.FAILED}

    def bump_state_version(self) -> None:
        """Increment the session state version so clients can detect a new snapshot."""

        self.state_version += 1


class MeetingInvitation(UUIDTimestampedModel):
    """Persist recipient delivery state for scheduled meeting reminders."""

    session = models.ForeignKey(
        MeetingSession,
        on_delete=models.CASCADE,
        related_name="email_invitations",
    )
    issuer_profile = models.ForeignKey(
        Profile,
        on_delete=models.SET_NULL,
        related_name="issued_meeting_invitations",
        blank=True,
        null=True,
    )
    issuer_name = models.CharField(max_length=255)
    recipient_email = models.EmailField()
    message = models.TextField(blank=True)
    expires_in_seconds = models.PositiveIntegerField()
    initial_email_sent_at = models.DateTimeField(blank=True, null=True)
    ready_email_sent_at = models.DateTimeField(blank=True, null=True)
    last_delivery_attempt_at = models.DateTimeField(blank=True, null=True)
    delivery_attempts = models.PositiveIntegerField(default=0)
    last_delivery_error = models.TextField(blank=True)

    class Meta:
        """Keep one reminder state per recipient and meeting occurrence."""

        ordering = ("created_at",)
        constraints = [
            models.UniqueConstraint(
                fields=("session", "recipient_email"),
                name="meet_invite_session_email_uniq",
            ),
        ]
        indexes = [
            models.Index(
                fields=("ready_email_sent_at", "created_at"),
                name="meet_invite_ready_created_idx",
            ),
        ]

    def __str__(self) -> str:
        """Return a concise invitation label for diagnostics."""

        return f"{self.recipient_email} invited to {self.session}"


class MeetingJoinRequest(UUIDTimestampedModel):
    """Admission request created when a profile asks to enter a live meeting session."""

    # Persistent room that the requester is attempting to enter.
    room = models.ForeignKey(MeetingRoom, on_delete=models.CASCADE, related_name="join_requests")
    # Live session that currently owns the waiting-room workflow.
    session = models.ForeignKey(MeetingSession, on_delete=models.CASCADE, related_name="join_requests")
    # Profile that requested admission into the meeting.
    profile = models.ForeignKey(Profile, on_delete=models.CASCADE, related_name="meeting_join_requests")
    # Socket.IO connection from which the request originated, when known.
    connection = models.ForeignKey("ParticipantConnection", on_delete=models.SET_NULL, related_name="join_requests", blank=True, null=True)
    # Display name the requester wants shown to participants if admitted.
    requested_display_name = models.CharField(max_length=255, blank=True)
    # Requested effective role for the eventual participant record.
    requested_role = models.CharField(max_length=32, choices=MeetingRole.choices, default=MeetingRole.PARTICIPANT)
    # Free-form note submitted alongside the join request.
    note = models.TextField(blank=True)
    # Lifecycle status of the admission request.
    status = models.CharField(max_length=32, choices=MeetingJoinRequestStatus.choices, default=MeetingJoinRequestStatus.PENDING)
    # Snapshot of client-side state submitted when the join request was created.
    client_state = models.JSONField(default=dict, blank=True)
    # Profile that reviewed the request, when one has been reviewed.
    reviewed_by_profile = models.ForeignKey(Profile, on_delete=models.SET_NULL, related_name="reviewed_meeting_join_requests", blank=True, null=True)
    # Timestamp recording when the join request was reviewed.
    reviewed_at = models.DateTimeField(blank=True, null=True)
    # Human-readable explanation for the review outcome.
    resolution_reason = models.TextField(blank=True)

    class Meta:
        """Declare ordering, constraints, and indexes for waiting-room queries."""

        ordering = ("created_at",)
        constraints = [
            models.UniqueConstraint(
                fields=("session", "profile"),
                condition=models.Q(status=MeetingJoinRequestStatus.PENDING),
                name="meeting_pending_join_request_unique_per_session_profile",
            ),
        ]
        indexes = [
            models.Index(fields=("session", "status"), name="meet_join_sess_status_idx"),
            models.Index(fields=("profile", "status"), name="meet_join_profile_status_idx"),
            models.Index(
                fields=("status", "created_at"),
                name="meet_join_status_created_idx",
            ),
        ]

    def __str__(self) -> str:
        """Return a readable waiting-room label."""

        return f"{self.profile} requesting {self.session}"

    def mark_reviewed(self, *, reviewer: Profile, status: str, reason: str = "") -> None:
        """Apply the review outcome metadata to the join request."""

        self.status = status
        self.reviewed_by_profile = reviewer
        self.reviewed_at = timezone.now()
        self.resolution_reason = reason


class Participant(UUIDTimestampedModel):
    """Logical admitted presence for a profile inside a live session."""

    objects = ParticipantQuerySet.as_manager()

    # Room whose durable permissions and defaults this presence belongs to.
    room = models.ForeignKey(MeetingRoom, on_delete=models.CASCADE, related_name="participants")
    # Live session in which this logical participant is or was present.
    session = models.ForeignKey(MeetingSession, on_delete=models.CASCADE, related_name="participants")
    # Profile represented by this participant presence.
    profile = models.ForeignKey(Profile, on_delete=models.CASCADE, related_name="meeting_participants")
    # Durable room membership that seeded coordinator privileges and participant defaults.
    membership = models.ForeignKey(MeetingRoomMembership, on_delete=models.SET_NULL, related_name="session_participants", blank=True, null=True)
    # Admission request that produced this participant presence.
    join_request = models.ForeignKey(MeetingJoinRequest, on_delete=models.SET_NULL, related_name="admitted_participants", blank=True, null=True)
    # Effective role for this participant inside the current session.
    role = models.CharField(max_length=32, choices=MeetingRole.choices, default=MeetingRole.PARTICIPANT)
    # Current lifecycle phase of the participant presence.
    status = models.CharField(max_length=32, choices=ParticipantStatus.choices, default=ParticipantStatus.ADMITTED)
    # Display name shown to other attendees in the UI and Janus metadata.
    display_name = models.CharField(max_length=255)
    # Permission indicating whether the participant is allowed to publish microphone audio.
    can_publish_audio = models.BooleanField(default=True)
    # Permission indicating whether the participant is allowed to publish camera video.
    can_publish_video = models.BooleanField(default=True)
    # Permission indicating whether the participant is allowed to share their screen.
    can_share_screen = models.BooleanField(default=False)
    # Permission indicating whether the participant is allowed to post chat messages.
    can_chat = models.BooleanField(default=True)
    # Permission indicating whether the participant is allowed to send ephemeral reactions.
    can_react = models.BooleanField(default=True)
    # Moderator-applied mute state used to represent audio suppression.
    is_muted = models.BooleanField(default=False)
    # Moderator-applied camera block state used to represent video suppression.
    is_camera_blocked = models.BooleanField(default=False)
    # Timestamp recording when the participant most recently raised their hand.
    raised_hand_at = models.DateTimeField(blank=True, null=True)
    # Janus publisher identifier returned when the participant joins the VideoRoom as a publisher.
    janus_publisher_id = models.PositiveBigIntegerField(
        blank=True,
        null=True,
        validators=[MinValueValidator(1)],
    )
    # Janus private identifier returned for subscriber operations tied to the publisher session.
    janus_private_id = models.PositiveBigIntegerField(
        blank=True,
        null=True,
        validators=[MinValueValidator(1)],
    )
    # Latest known Janus payload for this participant, including media and moderation state.
    janus_state = models.JSONField(default=dict, blank=True)
    # Timestamp recording when the participant was effectively admitted into the live meeting.
    joined_at = models.DateTimeField(blank=True, null=True)
    # Timestamp recording when the participant left voluntarily or was removed.
    left_at = models.DateTimeField(blank=True, null=True)
    # Timestamp recording the most recent presence heartbeat across all active socket connections.
    last_seen_at = models.DateTimeField(blank=True, null=True)
    # Unstructured metadata reserved for client capability or analytics context.
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        """Declare ordering, indexes, and uniqueness for participant state."""

        ordering = ("display_name",)
        constraints = [
            models.UniqueConstraint(
                fields=("session", "profile"),
                name="meeting_participant_unique_per_session_profile",
            ),
            models.CheckConstraint(
                condition=models.Q(janus_publisher_id__isnull=True)
                | models.Q(janus_publisher_id__gt=0),
                name="meet_part_pub_gt_zero",
            ),
            models.CheckConstraint(
                condition=models.Q(janus_private_id__isnull=True)
                | models.Q(janus_private_id__gt=0),
                name="meet_part_private_gt_zero",
            ),
        ]
        indexes = [
            models.Index(fields=("session", "status"), name="meet_part_sess_status_idx"),
            models.Index(fields=("session", "role"), name="meet_part_sess_role_idx"),
            models.Index(fields=("janus_publisher_id",), name="meet_part_jpub_idx"),
        ]

    def __str__(self) -> str:
        """Return a readable participant label for logs and diagnostics."""

        return f"{self.display_name} in {self.session}"

    def apply_membership_defaults(self) -> None:
        """Project interaction permissions from the effective participant role."""

        for field_name, value in participant_capability_defaults(self.role).items():
            setattr(self, field_name, value)

    def mark_joined(self) -> None:
        """Move the participant into an active state and stamp presence timestamps."""

        timestamp = timezone.now()
        self.status = ParticipantStatus.ACTIVE
        self.joined_at = self.joined_at or timestamp
        self.left_at = None
        self.last_seen_at = timestamp

    def mark_left(self, status: str = ParticipantStatus.LEFT) -> None:
        """Mark the participant as no longer present inside the session."""

        timestamp = timezone.now()
        self.status = status
        self.left_at = timestamp
        self.last_seen_at = timestamp

    def get_media_handle_record(self, handle_type: str) -> "ParticipantMediaHandle | None":
        """Return the tracked Janus handle record for the supplied handle type."""

        return self.media_handles.filter(handle_type=handle_type).first()

    @property
    def publisher_mediahandle_record(self) -> "ParticipantMediaHandle | None":
        """Return the persisted publisher handle record, if one exists."""

        return self.get_media_handle_record(JanusHandleType.PUBLISHER)

    @property
    def subscriber_mediahandle_record(self) -> "ParticipantMediaHandle | None":
        """Return the persisted subscriber handle record, if one exists."""

        return self.get_media_handle_record(JanusHandleType.SUBSCRIBER)

    @property
    def textroom_mediahandle_record(self) -> "ParticipantMediaHandle | None":
        """Return the persisted textroom handle record, if one exists."""

        return self.get_media_handle_record(JanusHandleType.TEXTROOM)

    @property
    def publisher_mediahandle(self) -> "ParticipantMediaHandle | None":
        """Return the persisted publisher record as a compatibility alias.

        Live JRTC plugins are process-local and must be resolved through the
        runtime handle registry; an ORM participant never materializes one.
        """

        return self.publisher_mediahandle_record

    @property
    def subscriber_mediahandle(self) -> "ParticipantMediaHandle | None":
        """Return the persisted subscriber record as a compatibility alias.

        Live JRTC plugins are process-local and must be resolved through the
        runtime handle registry; an ORM participant never materializes one.
        """

        return self.subscriber_mediahandle_record

    @property
    def textroom_mediahandle(self) -> "ParticipantMediaHandle | None":
        """Return the persisted text-room record as a compatibility alias.

        Live JRTC plugins are process-local and must be resolved through the
        runtime handle registry; an ORM participant never materializes one.
        """

        return self.textroom_mediahandle_record


class ParticipantConnection(UUIDTimestampedModel):
    """Socket.IO connection metadata used to map profiles to realtime sessions."""

    # Live session that the socket is currently subscribed to, if any.
    session = models.ForeignKey(MeetingSession, on_delete=models.CASCADE, related_name="connections", blank=True, null=True)
    # Logical participant associated with the socket once the profile is admitted.
    participant = models.ForeignKey(Participant, on_delete=models.SET_NULL, related_name="connections", blank=True, null=True)
    # Profile that owns the Socket.IO connection.
    profile = models.ForeignKey(Profile, on_delete=models.CASCADE, related_name="meeting_connections")
    # Socket.IO session id used to target realtime responses.
    socket_id = models.CharField(max_length=255, unique=True)
    # Client platform classification used for analytics and troubleshooting.
    transport = models.CharField(max_length=32, choices=RealtimeConnectionTransport.choices, default=RealtimeConnectionTransport.WEB)
    # Lifecycle state of the realtime connection.
    status = models.CharField(max_length=32, choices=RealtimeConnectionStatus.choices, default=RealtimeConnectionStatus.CONNECTED)
    # Optional client-generated connection identifier for deduplicating reconnect storms.
    client_session_key = models.CharField(max_length=255, blank=True)
    # Browser or device user-agent string captured when the socket connected.
    user_agent = models.TextField(blank=True)
    # Client IP address captured when the socket connected.
    ip_address = models.GenericIPAddressField(blank=True, null=True)
    # Timestamp recording when the socket connection was first observed.
    connected_at = models.DateTimeField(default=timezone.now)
    # Timestamp recording when the socket disconnected cleanly.
    disconnected_at = models.DateTimeField(blank=True, null=True)
    # Timestamp recording the most recent heartbeat from the client.
    last_heartbeat_at = models.DateTimeField(default=timezone.now)
    # Unstructured metadata reserved for client capability and troubleshooting details.
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        """Declare ordering and indexes optimized for socket presence queries."""

        ordering = ("-connected_at",)
        indexes = [
            models.Index(fields=("session", "status"), name="meet_conn_sess_status_idx"),
            models.Index(fields=("participant", "status"), name="meet_conn_part_status_idx"),
            models.Index(fields=("profile", "status"), name="meet_conn_profile_status_idx"),
            models.Index(
                fields=("status", "last_heartbeat_at"),
                name="meet_conn_status_heartbeat_idx",
            ),
        ]

    def __str__(self) -> str:
        """Return a readable label for diagnostics and admin screens."""

        return f"{self.profile} via {self.socket_id}"

    def mark_heartbeat(self) -> None:
        """Update heartbeat time without changing the connection generation."""

        self.last_heartbeat_at = timezone.now()

    def mark_disconnected(self) -> None:
        """Mark the socket as disconnected while preserving historical metadata."""

        self.status = RealtimeConnectionStatus.DISCONNECTED
        self.disconnected_at = timezone.now()
        self.last_heartbeat_at = self.disconnected_at


class ParticipantMediaHandle(UUIDTimestampedModel):
    """Tracked Janus plugin handle for an admitted participant."""

    # Participant that owns this Janus plugin handle.
    participant = models.ForeignKey(Participant, on_delete=models.CASCADE, related_name="media_handles")
    # Realtime connection that initiated the handle attachment, when known.
    connection = models.ForeignKey(ParticipantConnection, on_delete=models.SET_NULL, related_name="media_handles", blank=True, null=True)
    # Janus plugin role indicating whether the handle publishes, subscribes, or carries text-room traffic.
    handle_type = models.CharField(max_length=32, choices=JanusHandleType.choices)
    # Lifecycle phase of the Janus handle attachment and readiness flow.
    lifecycle_state = models.CharField(max_length=32, choices=JanusHandleLifecycleState.choices, default=JanusHandleLifecycleState.ATTACHING)
    # Diagnostic owner-session identifier; it is never portable across processes.
    janus_session_id = models.PositiveBigIntegerField(
        blank=True,
        null=True,
        validators=[MinValueValidator(1)],
    )
    # Diagnostic Janus handle identifier. Live plugin objects are owned by the
    # process-local JRTC registry and are never materialized by the ORM.
    janus_handle_id = models.PositiveBigIntegerField(
        blank=True,
        null=True,
        validators=[MinValueValidator(1)],
    )
    # Process/pod identity that owns the corresponding live registry binding.
    # A persisted owner is correlation metadata, not proof that a handle lives.
    runtime_owner_id = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        db_index=True,
    )
    # Short-lived persistence fence for one resolve/attach attempt.  Domain
    # lifecycle events are allowed to update ``lifecycle_state`` while this
    # claim is active, so the two concerns must not share one column.
    runtime_claim_id = models.UUIDField(
        blank=True,
        null=True,
        editable=False,
        default=None,
    )
    # Opaque identifier sent to Janus and echoed back for correlating logs or client requests.
    opaque_id = models.CharField(max_length=255, blank=True, null=True)
    # Most recent SDP offer associated with the handle, retained for diagnostics and renegotiation.
    jsep_offer = models.JSONField(default=dict, blank=True)
    # Most recent SDP answer associated with the handle, retained for diagnostics and renegotiation.
    jsep_answer = models.JSONField(default=dict, blank=True)
    # Selected inbound or outbound streams tracked for subscriber and publisher synchronization.
    selected_streams = models.JSONField(default=list, blank=True)
    # Latest known Janus event payload associated with the handle.
    janus_state = models.JSONField(default=dict, blank=True)
    # Timestamp recording when the most recent Janus event was observed for the handle.
    last_event_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        """Declare indexes optimized for Janus handle reconciliation."""

        ordering = ("participant", "handle_type")
        constraints = [
            models.UniqueConstraint(
                fields=("participant", "handle_type"),
                name="meeting_media_handle_unique_per_type",
            ),
            models.CheckConstraint(
                condition=models.Q(janus_session_id__isnull=True)
                | models.Q(janus_session_id__gt=0),
                name="meet_mh_session_gt_zero",
            ),
            models.CheckConstraint(
                condition=models.Q(janus_handle_id__isnull=True)
                | models.Q(janus_handle_id__gt=0),
                name="meet_mh_handle_gt_zero",
            ),
        ]
        indexes = [
            models.Index(fields=("participant", "handle_type"), name="meet_mh_part_type_idx"),
            models.Index(fields=("janus_handle_id",), name="meet_mh_jhandle_idx"),
            models.Index(
                fields=("janus_session_id", "janus_handle_id"),
                name="meet_mh_jsess_jhandle_idx",
            ),
            models.Index(fields=("lifecycle_state",), name="meet_mh_state_idx"),
        ]

    def __str__(self) -> str:
        """Return a readable description of the handle for logs and diagnostics."""

        return f"{self.participant} [{self.handle_type}]"

    def mark_failed(self, payload: dict[str, Any] | None = None) -> None:
        """Persist failure diagnostics and mark the handle as failed."""

        self.lifecycle_state = JanusHandleLifecycleState.FAILED
        self.last_event_at = timezone.now()
        if payload:
            self.janus_state = payload


class ParticipantStream(UUIDTimestampedModel):
    """Normalized Janus stream record representing published and subscribed media."""

    # Participant for whom the stream is relevant in the current session state.
    participant = models.ForeignKey(Participant, on_delete=models.CASCADE, related_name="streams")
    # Janus handle that owns or consumes the stream.
    media_handle = models.ForeignKey(ParticipantMediaHandle, on_delete=models.CASCADE, related_name="streams")
    # Participant that originally published the stream when the current row represents a subscription.
    source_participant = models.ForeignKey(Participant, on_delete=models.SET_NULL, related_name="downstream_streams", blank=True, null=True)
    # Direction describing whether the stream is published by or delivered to the participant.
    direction = models.CharField(max_length=32, choices=MediaDirection.choices)
    # Media category describing whether the stream carries audio, video, data, or screenshare content.
    media_kind = models.CharField(max_length=32, choices=MediaKind.choices)
    # Janus MID uniquely identifying the stream inside SDP and Janus events.
    janus_mid = models.CharField(max_length=64)
    # Janus publisher feed identifier associated with the stream when applicable.
    janus_feed_id = models.PositiveBigIntegerField(
        blank=True,
        null=True,
        validators=[MinValueValidator(1)],
    )
    # Source MID used by Janus for subscriber-side feed mapping.
    janus_feed_mid = models.CharField(max_length=64, blank=True, null=True)
    # Codec currently negotiated for the stream when known.
    codec = models.CharField(max_length=64, blank=True, null=True)
    # Flag indicating whether the stream is presently active according to Janus state.
    is_active = models.BooleanField(default=False)
    # Flag indicating whether the stream is fully ready for media flow.
    is_ready = models.BooleanField(default=False)
    # Flag indicating whether the stream is currently moderated or otherwise blocked.
    is_moderated = models.BooleanField(default=False)
    # Unstructured stream metadata mirrored from Janus payloads for future expansion.
    metadata = models.JSONField(default=dict, blank=True)
    # Timestamp recording when this stream row was last refreshed from Janus state.
    last_synced_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        """Declare indexes that support stream reconciliation and state fan-out."""

        ordering = ("participant", "direction", "media_kind", "janus_mid")
        constraints = [
            models.UniqueConstraint(
                fields=("media_handle", "janus_mid"),
                name="meeting_stream_unique_per_handle_mid",
            ),
            models.CheckConstraint(
                condition=models.Q(janus_feed_id__isnull=True)
                | models.Q(janus_feed_id__gt=0),
                name="meet_stream_feed_gt_zero",
            ),
        ]
        indexes = [
            models.Index(fields=("participant", "direction"), name="meet_stream_part_dir_idx"),
            models.Index(fields=("janus_feed_id",), name="meeting_stream_feed_idx"),
            models.Index(fields=("is_active", "is_ready"), name="meet_stream_act_ready_idx"),
        ]

    def __str__(self) -> str:
        """Return a readable description of the stream for diagnostics."""

        return f"{self.participant} {self.media_kind} [{self.direction}]"


class MeetingMessage(UUIDTimestampedModel):
    """Persisted chat or system message associated with a live meeting session."""

    # Live session in which the message was created.
    session = models.ForeignKey(MeetingSession, on_delete=models.CASCADE, related_name="messages")
    # Participant that authored the message, or null for system-generated output.
    participant = models.ForeignKey(Participant, on_delete=models.SET_NULL, related_name="messages", blank=True, null=True)
    # Message category describing whether the payload is chat, system output, or moderation output.
    kind = models.CharField(max_length=32, choices=MessageKind.choices, default=MessageKind.CHAT)
    # Message body as rendered to end users.
    body = models.TextField()
    # Unstructured metadata reserved for rich formatting or delivery diagnostics.
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        """Declare ordering and indexes optimized for room chat history retrieval."""

        ordering = ("created_at",)
        indexes = [
            models.Index(fields=("session", "created_at"), name="meet_msg_sess_ct_idx"),
            models.Index(fields=("kind",), name="meeting_message_kind_idx"),
        ]

    def __str__(self) -> str:
        """Return a concise preview for diagnostics."""

        return f"{self.kind}: {self.body[:32]}"


class MeetingReaction(UUIDTimestampedModel):
    """Ephemeral or durable reaction emitted by a participant inside a meeting session."""

    # Live session in which the reaction was emitted.
    session = models.ForeignKey(MeetingSession, on_delete=models.CASCADE, related_name="reactions")
    # Participant that emitted the reaction.
    participant = models.ForeignKey(Participant, on_delete=models.CASCADE, related_name="reactions")
    # Short symbolic reaction identifier such as thumbs_up, clap, or raised_hand.
    reaction = models.CharField(max_length=64)
    # Optional expiry timestamp used for transient reactions that should fade out automatically.
    expires_at = models.DateTimeField(blank=True, null=True)
    # Unstructured metadata reserved for animation hints or future UI affordances.
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        """Declare ordering and indexes optimized for recent reaction retrieval."""

        ordering = ("created_at",)
        indexes = [
            models.Index(fields=("session", "created_at"), name="meet_react_sess_ct_idx"),
            models.Index(fields=("reaction",), name="meeting_reaction_type_idx"),
        ]

    def __str__(self) -> str:
        """Return a readable reaction label for diagnostics."""

        return f"{self.participant} reacted with {self.reaction}"


class MeetingEvent(UUIDTimestampedModel):
    """Audit log entry describing a meaningful lifecycle or moderation event."""

    # Live session whose lifecycle or moderation activity this event belongs to.
    session = models.ForeignKey(MeetingSession, on_delete=models.CASCADE, related_name="events")
    # Profile that initiated the event when known.
    actor_profile = models.ForeignKey(Profile, on_delete=models.SET_NULL, related_name="meeting_events", blank=True, null=True)
    # Logical participant presence that initiated the event when known.
    actor_participant = models.ForeignKey(Participant, on_delete=models.SET_NULL, related_name="audit_events", blank=True, null=True)
    # Machine-readable event type used for replay, analytics, and troubleshooting.
    event_type = models.CharField(max_length=64, choices=MeetingEventType.choices)
    # Structured payload describing the event with minimal denormalized context.
    payload = models.JSONField(default=dict, blank=True)

    class Meta:
        """Declare ordering and indexes optimized for audit feeds and debugging."""

        ordering = ("created_at",)
        indexes = [
            models.Index(fields=("session", "created_at"), name="meet_evt_sess_ct_idx"),
            models.Index(fields=("event_type", "created_at"), name="meet_evt_type_ct_idx"),
        ]

    def __str__(self) -> str:
        """Return a readable event label for diagnostics and admin screens."""

        return f"{self.event_type} for {self.session}"


class JrtcEventReceipt(UUIDTimestampedModel):
    """Durably admit one broker envelope before applying idempotent side effects.

    Consumers must create this row in the same database transaction as their
    domain mutations. The unique broker ``event_id`` turns redelivery into a
    harmless uniqueness conflict rather than a second set of side effects.
    """

    event_id = models.UUIDField(unique=True, editable=False)
    event_type = models.CharField(max_length=128)
    status = models.CharField(
        max_length=16,
        choices=JrtcEventReceiptStatus.choices,
        default=JrtcEventReceiptStatus.RECEIVED,
    )
    received_at = models.DateTimeField(auto_now_add=True)
    processed_at = models.DateTimeField(blank=True, null=True)
    delivery_attempts = models.PositiveIntegerField(default=1)
    duplicate_count = models.PositiveBigIntegerField(default=0)
    last_error = models.TextField(blank=True)
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        """Support operational scans without high-cardinality index labels."""

        ordering = ("received_at",)
        indexes = [
            models.Index(
                fields=("status", "received_at"),
                name="meet_jrtc_receipt_status_idx",
            ),
            models.Index(
                fields=("event_type", "received_at"),
                name="meet_jrtc_receipt_type_idx",
            ),
        ]

    def __str__(self) -> str:
        """Return a concise broker receipt label for diagnostics."""

        return f"{self.event_type}:{self.event_id} ({self.status})"


class JrtcBrowserEventOutbox(UUIDTimestampedModel):
    """Durable, at-least-once Socket.IO forwarding derived from one event."""

    receipt = models.ForeignKey(
        JrtcEventReceipt,
        on_delete=models.CASCADE,
        related_name="browser_outbox",
    )
    dispatch_index = models.PositiveIntegerField()
    socket_id = models.CharField(max_length=255)
    payload = models.JSONField(default=dict)
    status = models.CharField(
        max_length=16,
        choices=JrtcBrowserOutboxStatus.choices,
        default=JrtcBrowserOutboxStatus.PENDING,
    )
    delivery_attempts = models.PositiveIntegerField(default=0)
    delivered_at = models.DateTimeField(blank=True, null=True)
    last_error = models.CharField(max_length=255, blank=True)

    class Meta:
        """Fence duplicate targets and support pending-delivery scans."""

        ordering = ("created_at", "dispatch_index")
        constraints = [
            models.UniqueConstraint(
                fields=("receipt", "dispatch_index", "socket_id"),
                name="meet_jrtc_outbox_target_uniq",
            )
        ]
        indexes = [
            models.Index(
                fields=("status", "created_at"),
                name="meet_jrtc_outbox_status_idx",
            ),
            models.Index(
                fields=("status", "updated_at"),
                name="meet_jrtc_outbox_retry_idx",
            ),
        ]
