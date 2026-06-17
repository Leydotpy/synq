"""HTTP views for room setup, session state, admission, and moderation."""

from __future__ import annotations

from django.db.models import Q
from django.shortcuts import get_object_or_404
from django.core.exceptions import PermissionDenied as DjangoPermissionDenied
from django.utils import timezone
from rest_framework import generics, permissions, response, status, views
from rest_framework.exceptions import PermissionDenied, ValidationError

from apps.meetings.api.serializers import (
    MeetingJoinRequestCreateSerializer,
    MeetingJoinRequestReviewSerializer,
    MeetingRoomSerializer,
    MeetingSessionCreateSerializer,
    MeetingServiceSessionCreateSerializer,
    MeetingSessionStartSerializer,
    MeetingSessionShareSerializer,
    ParticipantRemovalSerializer,
    ParticipantUpdateSerializer,
)
from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.models import MeetingJoinRequest, MeetingRoom, MeetingSession, Participant
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.services.invitations import MeetingInvitationService
from apps.meetings.services.permissions import MeetingPermissionService
from apps.meetings.services.state import MeetingStateBuilder
from core.api.api import CurrentProfileMixin
from core.api.service_auth import ServiceTokenAuthentication


class MeetingRoomListCreateView(CurrentProfileMixin, generics.ListCreateAPIView):
    """List accessible rooms or create a new room for the authenticated profile."""

    permission_classes = [permissions.IsAuthenticated]
    serializer_class = MeetingRoomSerializer

    def get_queryset(self):
        """Return rooms created by or shared with the current profile."""

        profile = self.get_profile()
        return MeetingRoom.objects.filter(
            Q(created_by_profile=profile) | Q(memberships__profile=profile, memberships__is_active=True),
        ).distinct()

    def get_serializer_context(self):
        """Expose the authenticated profile to serializers that delegate domain creation."""

        context = super().get_serializer_context()
        context["profile"] = self.get_profile()
        return context

    def perform_create(self, serializer):
        """Translate domain exceptions into API-friendly validation responses."""

        try:
            serializer.save()
        except MeetingDomainError as exc:
            raise ValidationError(str(exc)) from exc


class MeetingRoomDetailView(CurrentProfileMixin, generics.RetrieveAPIView):
    """Return a single accessible room by slug."""

    permission_classes = [permissions.IsAuthenticated]
    serializer_class = MeetingRoomSerializer
    lookup_field = "slug"

    def get_queryset(self):
        """Return rooms created by or shared with the current profile."""

        profile = self.get_profile()
        return MeetingRoom.objects.filter(
            Q(created_by_profile=profile) | Q(memberships__profile=profile, memberships__is_active=True),
        ).distinct()


class MeetingSessionStartView(CurrentProfileMixin, views.APIView):
    """Start or reuse the live session for a room."""

    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, slug: str):
        """Start the room's session after enforcing coordinator permissions."""

        serializer = MeetingSessionStartSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        profile = self.get_profile()
        room = get_object_or_404(MeetingRoom, slug=slug)
        try:
            MeetingPermissionService.require_room_permission(room=room, profile_or_user=profile, permission_field="can_manage_waiting_room")
            session = MeetingLifecycleService.start_session(
                room=room,
                started_by_profile=profile,
                metadata=serializer.validated_data.get("metadata", {}),
            )
        except DjangoPermissionDenied as exc:
            raise PermissionDenied(str(exc)) from exc
        except MeetingDomainError as exc:
            raise ValidationError(str(exc)) from exc
        return response.Response(
            MeetingStateBuilder.build(session=session, authenticated_profile=profile),
            status=status.HTTP_201_CREATED,
        )


class MeetingSessionCreateView(CurrentProfileMixin, views.APIView):
    """Create a room, start its session, and return a frontend join link."""

    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        """Provision a web-app meeting in one request for instant and scheduled starts."""

        serializer = MeetingSessionCreateSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        profile = self.get_profile()
        payload = serializer.validated_data
        now_label = timezone.localtime().strftime("%b %d, %H:%M")
        title = (payload.get("title") or "").strip() or f"Synq Meeting {now_label}"
        description = (payload.get("description") or "").strip()
        participant_emails = payload.get("participant_emails") or []
        expires_in_seconds = payload.get("expires_in_seconds")
        metadata = {
            **payload.get("metadata", {}),
            "source": "synq_webapp",
            "created_from": "homepage",
        }
        try:
            room = MeetingLifecycleService.create_room(
                creator_profile=profile,
                title=title,
                description=description,
                access_policy=payload.get("access_policy", "approval_required"),
                is_waiting_room_enabled=payload.get("is_waiting_room_enabled", True),
                max_participants=payload.get("max_participants", 100),
                passcode=payload.get("passcode"),
                scheduled_start_at=payload.get("scheduled_start_at"),
                scheduled_end_at=payload.get("scheduled_end_at"),
                metadata=metadata,
            )
            session = MeetingLifecycleService.start_session(
                room=room,
                started_by_profile=profile,
                metadata=metadata,
            )
            invite_token = MeetingInvitationService.create_invite_token(
                session=session,
                issuer_profile=profile,
                expires_in_seconds=expires_in_seconds,
            )
            join_url = MeetingInvitationService.build_frontend_join_url(
                session=session,
                invite_token=invite_token,
            )
            share_payload = None
            if participant_emails:
                share_payload = MeetingInvitationService.share_session(
                    session=session,
                    issuer_profile=profile,
                    emails=participant_emails,
                    message=payload.get("message", ""),
                    expires_in_seconds=expires_in_seconds,
                )
        except MeetingDomainError as exc:
            raise ValidationError(str(exc)) from exc

        return response.Response(
            {
                "provider": "synq_meet",
                "room_id": str(room.pk),
                "room_slug": room.slug,
                "session_id": str(session.pk),
                "join_url": join_url,
                "invite_token": invite_token,
                "shared_invites": share_payload,
                "room": MeetingRoomSerializer(room, context=self.get_serializer_context()).data,
            },
            status=status.HTTP_201_CREATED,
        )

    def get_serializer_context(self):
        """Mirror generic views so nested serializers can receive request context."""

        return {"request": self.request, "profile": self.get_profile()}


class MeetingSessionStateView(CurrentProfileMixin, views.APIView):
    """Return a personalized state snapshot for a session."""

    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, session_id: str):
        """Serialize the current session state for the authenticated profile."""

        session = get_object_or_404(MeetingSession.objects.select_related("room", "started_by_profile"), pk=session_id)
        return response.Response(MeetingStateBuilder.build(session=session, authenticated_profile=self.get_profile()))


class MeetingJoinRequestCreateView(CurrentProfileMixin, views.APIView):
    """Create a waiting-room join request for the authenticated profile."""

    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, session_id: str):
        """Persist a pending waiting-room request for the authenticated profile."""

        serializer = MeetingJoinRequestCreateSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        session = get_object_or_404(MeetingSession.objects.select_related("room"), pk=session_id)
        try:
            join_request = MeetingLifecycleService.request_join(
                session=session,
                profile=self.get_profile(),
                requested_display_name=serializer.validated_data.get("display_name", ""),
                requested_role=serializer.validated_data.get("requested_role", "participant"),
                note=serializer.validated_data.get("note", ""),
                client_state=serializer.validated_data.get("client_state", {}),
                passcode=serializer.validated_data.get("passcode"),
                invite_token=serializer.validated_data.get("invite_token"),
            )
        except MeetingDomainError as exc:
            raise ValidationError(str(exc)) from exc
        return response.Response(MeetingStateBuilder.serialize_join_request(join_request), status=status.HTTP_201_CREATED)


class MeetingSessionShareView(CurrentProfileMixin, views.APIView):
    """Create and email signed frontend join links for a live meeting session."""

    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, session_id: str):
        """Share a frontend waiting-room link after enforcing coordinator permissions."""

        serializer = MeetingSessionShareSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        session = get_object_or_404(MeetingSession.objects.select_related("room"), pk=session_id)
        profile = self.get_profile()
        try:
            MeetingPermissionService.require_session_permission(
                session=session,
                profile_or_user=profile,
                permission_field="can_manage_waiting_room",
            )
            payload = MeetingInvitationService.share_session(
                session=session,
                issuer_profile=profile,
                emails=serializer.validated_data["emails"],
                message=serializer.validated_data.get("message", ""),
                expires_in_seconds=serializer.validated_data.get("expires_in_seconds"),
            )
        except DjangoPermissionDenied as exc:
            raise PermissionDenied(str(exc)) from exc
        except MeetingDomainError as exc:
            raise ValidationError(str(exc)) from exc
        return response.Response(payload, status=status.HTTP_201_CREATED)


class MeetingJoinRequestReviewView(CurrentProfileMixin, views.APIView):
    """Approve or reject a pending waiting-room join request."""

    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, session_id: str, join_request_id: str):
        """Review a join request and return the resulting participant state when admitted."""

        serializer = MeetingJoinRequestReviewSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        join_request = get_object_or_404(
            MeetingJoinRequest.objects.select_related("session", "room", "profile"),
            pk=join_request_id,
            session_id=session_id,
        )
        try:
            participant = MeetingLifecycleService.review_join_request(
                join_request=join_request,
                reviewer_profile=self.get_profile(),
                approve=serializer.validated_data["approve"],
                reason=serializer.validated_data.get("reason", ""),
            )
        except DjangoPermissionDenied as exc:
            raise PermissionDenied(str(exc)) from exc
        except MeetingDomainError as exc:
            raise ValidationError(str(exc)) from exc
        return response.Response(
            {
                "join_request": MeetingStateBuilder.serialize_join_request(join_request),
                "participant": MeetingStateBuilder.serialize_participant(participant),
            },
            status=status.HTTP_200_OK,
        )


class MeetingParticipantDetailView(CurrentProfileMixin, views.APIView):
    """Update or remove a participant inside a live session."""

    permission_classes = [permissions.IsAuthenticated]

    def patch(self, request, session_id: str, participant_id: str):
        """Update participant interaction or moderation state."""

        serializer = ParticipantUpdateSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        participant = get_object_or_404(Participant.objects.select_related("session", "room", "profile"), pk=participant_id, session_id=session_id)
        try:
            participant = MeetingLifecycleService.update_participant_permissions(
                session=participant.session,
                actor_profile=self.get_profile(),
                participant=participant,
                updates=serializer.validated_data["updates"],
            )
        except DjangoPermissionDenied as exc:
            raise PermissionDenied(str(exc)) from exc
        except MeetingDomainError as exc:
            raise ValidationError(str(exc)) from exc
        return response.Response(MeetingStateBuilder.serialize_participant(participant))

    def delete(self, request, session_id: str, participant_id: str):
        """Remove a participant from a live session."""

        serializer = ParticipantRemovalSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        participant = get_object_or_404(Participant.objects.select_related("session", "room", "profile"), pk=participant_id, session_id=session_id)
        try:
            participant = MeetingLifecycleService.remove_participant(
                session=participant.session,
                actor_profile=self.get_profile(),
                participant=participant,
                reason=serializer.validated_data.get("reason", ""),
            )
        except DjangoPermissionDenied as exc:
            raise PermissionDenied(str(exc)) from exc
        except MeetingDomainError as exc:
            raise ValidationError(str(exc)) from exc
        return response.Response(MeetingStateBuilder.serialize_participant(participant), status=status.HTTP_200_OK)


class MeetingServiceSessionCreateView(CurrentProfileMixin, views.APIView):
    """Provision a room/session for a trusted external application."""

    authentication_classes = [ServiceTokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        serializer = MeetingServiceSessionCreateSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        profile = self.get_profile()
        payload = serializer.validated_data
        metadata = {
            **payload.get("metadata", {}),
            "external_id": payload["external_id"],
            "source": "law_firm_workspace",
        }
        try:
            room = MeetingRoom.objects.filter(metadata__external_id=payload["external_id"]).first()
            if room is None:
                room = MeetingLifecycleService.create_room(
                    creator_profile=profile,
                    title=payload["title"],
                    description=payload.get("description", ""),
                    scheduled_start_at=payload.get("scheduled_start_at"),
                    scheduled_end_at=payload.get("scheduled_end_at"),
                    metadata=metadata,
                )
            session = MeetingLifecycleService.start_session(
                room=room,
                started_by_profile=profile,
                metadata=metadata,
            )
            invite_token = MeetingInvitationService.create_invite_token(
                session=session,
                issuer_profile=profile,
                expires_in_seconds=payload.get("expires_in_seconds"),
            )
            join_url = MeetingInvitationService.build_frontend_join_url(
                session=session,
                invite_token=invite_token,
            )
            share_payload = None
            participant_emails = payload.get("participant_emails") or []
            if participant_emails:
                share_payload = MeetingInvitationService.share_session(
                    session=session,
                    issuer_profile=profile,
                    emails=participant_emails,
                    message=payload.get("description", ""),
                    expires_in_seconds=payload.get("expires_in_seconds"),
                )
        except MeetingDomainError as exc:
            raise ValidationError(str(exc)) from exc

        return response.Response(
            {
                "provider": "synq_meet",
                "room_id": str(room.pk),
                "room_slug": room.slug,
                "session_id": str(session.pk),
                "join_url": join_url,
                "invite_token": invite_token,
                "shared_invites": share_payload,
            },
            status=status.HTTP_201_CREATED,
        )
