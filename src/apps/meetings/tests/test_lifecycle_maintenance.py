"""Regression coverage for scheduled meeting lifecycle maintenance."""

from __future__ import annotations

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from apps.meetings.models import (
    MeetingEvent,
    MeetingEventType,
    MeetingJoinRequest,
    MeetingJoinRequestStatus,
    MeetingLifecycleState,
    MeetingRole,
    MeetingRoomMembership,
    MeetingSession,
    Participant,
    ParticipantStatus,
)
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.tasks import (
    end_scheduled_sessions,
    expire_pending_join_requests,
)
from apps.profiles.models import Profile


class MeetingLifecycleTestMixin:
    """Create isolated meeting fixtures without running worker callbacks."""

    def make_profile(self, handle: str) -> Profile:
        """Create an auth user and return the signal-created profile."""

        user = get_user_model().objects.create_user(
            username=handle,
            email=f"{handle}@example.com",
            password=None,
            clerk_user_id=f"clerk_{handle}",
        )
        profile = user.profile
        profile.display_name = handle.title()
        profile.save(update_fields=["display_name", "updated_at"])
        return profile

    def make_session(
        self,
        *,
        handle: str,
        title: str,
        scheduled_end_at=None,
        lifecycle_state: str = MeetingLifecycleState.WAITING,
    ):
        """Create a room and session without executing post-commit worker calls."""

        host = self.make_profile(handle)
        room = MeetingLifecycleService.create_room(
            creator_profile=host,
            title=title,
            scheduled_end_at=scheduled_end_at,
        )
        session = MeetingLifecycleService.start_session(
            room=room,
            started_by_profile=host,
        )
        MeetingSession.objects.filter(pk=session.pk).update(
            lifecycle_state=lifecycle_state,
        )
        session.refresh_from_db()
        return host, room, session


class MeetingLifecycleMaintenanceTests(MeetingLifecycleTestMixin, TestCase):
    """Exercise the recovery sweeps and their idempotency boundaries."""


    def test_scheduled_end_sweep_is_due_only_and_idempotent(
        self,
    ):
        """Only overdue live sessions end, with related state transitioned exactly once."""

        now = timezone.now()
        _, due_room, due_session = self.make_session(
            handle="due-host",
            title="Due session",
            scheduled_end_at=now - timedelta(minutes=1),
        )
        _, _, future_session = self.make_session(
            handle="future-host",
            title="Future session",
            scheduled_end_at=now + timedelta(hours=1),
        )
        guest = self.make_profile("due-guest")
        guest_participant = Participant.objects.create(
            room=due_room,
            session=due_session,
            profile=guest,
            role=MeetingRole.PARTICIPANT,
            status=ParticipantStatus.ADMITTED,
            display_name="Due Guest",
        )
        pending_request = MeetingJoinRequest.objects.create(
            room=due_room,
            session=due_session,
            profile=self.make_profile("waiting-guest"),
        )

        with self.captureOnCommitCallbacks(execute=True):
            first_count = end_scheduled_sessions.run()
        with self.captureOnCommitCallbacks(execute=True):
            second_count = end_scheduled_sessions.run()

        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 0)
        due_session.refresh_from_db()
        future_session.refresh_from_db()
        guest_participant.refresh_from_db()
        pending_request.refresh_from_db()
        self.assertEqual(due_session.lifecycle_state, MeetingLifecycleState.ENDED)
        self.assertIsNotNone(due_session.ended_at)
        self.assertEqual(future_session.lifecycle_state, MeetingLifecycleState.WAITING)
        self.assertEqual(guest_participant.status, ParticipantStatus.LEFT)
        self.assertIsNotNone(guest_participant.left_at)
        self.assertFalse(
            due_session.participants.exclude(
                status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED],
            ).exists(),
        )
        self.assertEqual(due_session.participant_count, 0)
        self.assertEqual(due_session.gateway_outbox.filter(action="meeting.terminate").count(), 1)
        self.assertEqual(pending_request.status, MeetingJoinRequestStatus.CANCELLED)
        self.assertIsNotNone(pending_request.reviewed_at)
        self.assertEqual(
            MeetingEvent.objects.filter(
                session=due_session,
                event_type=MeetingEventType.SESSION_ENDED,
            ).count(),
            1,
        )

    @override_settings(MEETING_JOIN_REQUEST_TTL_SECONDS=900)
    def test_join_request_expiry_is_old_pending_only_and_idempotent(
        self,
    ):
        """The expiry sweep leaves fresh requests alone and never re-emits an expiry."""

        _, room, session = self.make_session(
            handle="expiry-host",
            title="Expiry session",
        )
        old_request = MeetingJoinRequest.objects.create(
            room=room,
            session=session,
            profile=self.make_profile("old-requester"),
        )
        fresh_request = MeetingJoinRequest.objects.create(
            room=room,
            session=session,
            profile=self.make_profile("fresh-requester"),
        )
        MeetingJoinRequest.objects.filter(pk=old_request.pk).update(
            created_at=timezone.now() - timedelta(hours=1),
        )

        with self.captureOnCommitCallbacks(execute=True):
            first_count = expire_pending_join_requests.run()
        with self.captureOnCommitCallbacks(execute=True):
            second_count = expire_pending_join_requests.run()

        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 0)
        old_request.refresh_from_db()
        fresh_request.refresh_from_db()
        self.assertEqual(old_request.status, MeetingJoinRequestStatus.EXPIRED)
        self.assertIsNotNone(old_request.reviewed_at)
        self.assertTrue(old_request.resolution_reason)
        self.assertEqual(fresh_request.status, MeetingJoinRequestStatus.PENDING)
        self.assertTrue(session.events.filter(event_type=MeetingEventType.JOIN_REQUEST_REVIEWED,
                                             payload__join_request_id=str(old_request.pk)).exists())


class MeetingSessionEndEndpointTests(MeetingLifecycleTestMixin, TestCase):
    """Verify the public end-session contract and authorization boundary."""

    def test_host_can_end_session_repeatedly_without_duplicate_side_effects(
        self,
    ):
        """Repeated coordinator requests return terminal state but emit cleanup only once."""

        host, _, session = self.make_session(
            handle="endpoint-host",
            title="Endpoint session",
        )
        client = APIClient()
        client.force_authenticate(user=host.user)
        url = f"/api/v1/meetings/sessions/{session.pk}/end/"

        with self.captureOnCommitCallbacks(execute=True):
            first_response = client.post(url, {"reason": "Agenda complete."}, format="json")
        with self.captureOnCommitCallbacks(execute=True):
            second_response = client.post(url, {"reason": "Duplicate click."}, format="json")

        self.assertEqual(first_response.status_code, status.HTTP_200_OK)
        self.assertEqual(second_response.status_code, status.HTTP_200_OK)
        self.assertEqual(first_response.json()["meeting"]["lifecycle"], MeetingLifecycleState.ENDED)
        self.assertEqual(second_response.json()["meeting"]["lifecycle"], MeetingLifecycleState.ENDED)
        ended_events = MeetingEvent.objects.filter(
            session=session,
            event_type=MeetingEventType.SESSION_ENDED,
        )
        self.assertEqual(ended_events.count(), 1)
        self.assertEqual(ended_events.get().payload["reason"], "Agenda complete.")

    def test_regular_room_member_cannot_end_session(self):
        """A membership without participant-management permission receives HTTP 403."""

        _, room, session = self.make_session(
            handle="permission-host",
            title="Permission session",
        )
        member = self.make_profile("regular-member")
        MeetingRoomMembership.objects.create(
            room=room,
            profile=member,
            role=MeetingRole.PARTICIPANT,
        )
        client = APIClient()
        client.force_authenticate(user=member.user)

        response = client.post(
            f"/api/v1/meetings/sessions/{session.pk}/end/",
            {"reason": "Not allowed."},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        session.refresh_from_db()
        self.assertEqual(session.lifecycle_state, MeetingLifecycleState.WAITING)
        self.assertFalse(
            MeetingEvent.objects.filter(
                session=session,
                event_type=MeetingEventType.SESSION_ENDED,
            ).exists(),
        )

    def test_end_reason_rejects_more_than_one_thousand_characters(self):
        """The API caps the persisted and broadcast end reason at 1000 characters."""

        host, _, session = self.make_session(
            handle="reason-host",
            title="Reason validation session",
        )
        client = APIClient()
        client.force_authenticate(user=host.user)

        response = client.post(
            f"/api/v1/meetings/sessions/{session.pk}/end/",
            {"reason": "x" * 1001},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        session.refresh_from_db()
        self.assertEqual(session.lifecycle_state, MeetingLifecycleState.WAITING)
