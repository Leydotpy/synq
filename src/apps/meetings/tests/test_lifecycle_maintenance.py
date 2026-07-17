"""Regression coverage for scheduled meeting lifecycle maintenance."""

from __future__ import annotations

import uuid
from datetime import timedelta
from unittest.mock import patch

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
    ParticipantConnection,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.realtime.emitter import MeetingSocketEmitter
from apps.meetings.realtime.events import MeetingSocketEvents
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.tasks import (
    end_scheduled_sessions,
    expire_pending_join_requests,
    recover_stale_provisioning_sessions,
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

    @override_settings(MEETING_PROVISIONING_STALE_SECONDS=60)
    @patch("apps.meetings.tasks.provision_janus_room_for_session.delay")
    def test_recovery_claims_only_stale_unprovisioned_sessions_once(self, provision_delay):
        """A recovery sweep leases eligible rows so overlapping sweeps do not duplicate work."""

        _, _, stale = self.make_session(
            handle="stale-host",
            title="Stale provisioning",
            lifecycle_state=MeetingLifecycleState.PROVISIONING,
        )
        _, _, fresh = self.make_session(
            handle="fresh-host",
            title="Fresh provisioning",
            lifecycle_state=MeetingLifecycleState.PROVISIONING,
        )
        _, _, already_provisioned = self.make_session(
            handle="ready-host",
            title="Already provisioned",
            lifecycle_state=MeetingLifecycleState.PROVISIONING,
        )
        _, _, terminal = self.make_session(
            handle="terminal-host",
            title="Terminal session",
            lifecycle_state=MeetingLifecycleState.ENDED,
        )
        stale_timestamp = timezone.now() - timedelta(minutes=5)
        MeetingSession.objects.filter(pk=stale.pk).update(updated_at=stale_timestamp)
        MeetingSession.objects.filter(pk=already_provisioned.pk).update(
            janus_room_id="janus-room-ready",
            updated_at=stale_timestamp,
        )
        MeetingSession.objects.filter(pk=terminal.pk).update(updated_at=stale_timestamp)

        first_count = recover_stale_provisioning_sessions.run()
        second_count = recover_stale_provisioning_sessions.run()

        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 0)
        provision_delay.assert_called_once_with(str(stale.pk))
        stale.refresh_from_db()
        fresh.refresh_from_db()
        self.assertGreater(stale.updated_at, stale_timestamp)
        self.assertEqual(fresh.lifecycle_state, MeetingLifecycleState.PROVISIONING)

    @patch("apps.meetings.realtime.emitter.MeetingSocketEmitter.emit_session_state")
    @patch("apps.meetings.realtime.emitter.MeetingSocketEmitter.emit_session_ended")
    @patch("apps.meetings.tasks.destroy_janus_room_for_session.delay")
    def test_scheduled_end_sweep_is_due_only_and_idempotent(
        self,
        destroy_delay,
        emit_session_ended,
        emit_session_state,
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
        self.assertEqual(pending_request.status, MeetingJoinRequestStatus.CANCELLED)
        self.assertIsNotNone(pending_request.reviewed_at)
        self.assertEqual(
            MeetingEvent.objects.filter(
                session=due_session,
                event_type=MeetingEventType.SESSION_ENDED,
            ).count(),
            1,
        )
        destroy_delay.assert_called_once()
        self.assertEqual(destroy_delay.call_args.args[0], str(due_session.pk))
        uuid.UUID(destroy_delay.call_args.args[1])
        emit_session_ended.assert_called_once()
        emit_session_state.assert_called_once()

    @override_settings(MEETING_JOIN_REQUEST_TTL_SECONDS=900)
    @patch("apps.meetings.realtime.emitter.MeetingSocketEmitter.emit_session_state")
    @patch("apps.meetings.realtime.emitter.MeetingSocketEmitter.emit_join_request_reviewed")
    def test_join_request_expiry_is_old_pending_only_and_idempotent(
        self,
        emit_reviewed,
        emit_session_state,
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
        emit_reviewed.assert_called_once()
        self.assertIsNone(emit_reviewed.call_args.kwargs["participant"])
        emit_session_state.assert_called_once_with(session=old_request.session)


class MeetingSessionEndEndpointTests(MeetingLifecycleTestMixin, TestCase):
    """Verify the public end-session contract and authorization boundary."""

    @patch("apps.meetings.realtime.emitter.MeetingSocketEmitter.emit_session_state")
    @patch("apps.meetings.realtime.emitter.MeetingSocketEmitter.emit_session_ended")
    @patch("apps.meetings.tasks.destroy_janus_room_for_session.delay")
    def test_host_can_end_session_repeatedly_without_duplicate_side_effects(
        self,
        destroy_delay,
        emit_session_ended,
        emit_session_state,
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
        self.assertEqual(first_response.json()["session"]["lifecycle_state"], MeetingLifecycleState.ENDED)
        self.assertEqual(second_response.json()["session"]["lifecycle_state"], MeetingLifecycleState.ENDED)
        ended_events = MeetingEvent.objects.filter(
            session=session,
            event_type=MeetingEventType.SESSION_ENDED,
        )
        self.assertEqual(ended_events.count(), 1)
        self.assertEqual(ended_events.get().payload["reason"], "Agenda complete.")
        destroy_delay.assert_called_once()
        self.assertEqual(destroy_delay.call_args.args[0], str(session.pk))
        uuid.UUID(destroy_delay.call_args.args[1])
        emit_session_ended.assert_called_once()
        emit_session_state.assert_called_once()

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


class MeetingSessionEndedRealtimeTests(MeetingLifecycleTestMixin, TestCase):
    """Protect the client-facing terminal Socket.IO event contract."""

    @patch.object(MeetingSocketEmitter, "_emit")
    def test_session_ended_event_targets_live_session_connections(self, emit):
        """Admitted and waiting live sockets receive the terminal event payload."""

        host, _, session = self.make_session(
            handle="socket-host",
            title="Socket end session",
        )
        session.ended_at = timezone.now()
        session.save(update_fields=["ended_at", "updated_at"])
        ParticipantConnection.objects.create(
            session=session,
            profile=host,
            socket_id="connected-socket",
            status=RealtimeConnectionStatus.ACTIVE,
        )
        ParticipantConnection.objects.create(
            session=session,
            profile=self.make_profile("waiting-socket-user"),
            socket_id="waiting-socket",
            status=RealtimeConnectionStatus.SUBSCRIBED,
        )
        ParticipantConnection.objects.create(
            session=session,
            profile=host,
            socket_id="disconnected-socket",
            status=RealtimeConnectionStatus.DISCONNECTED,
        )

        MeetingSocketEmitter.emit_session_ended(session=session, reason="Agenda complete.")

        self.assertEqual(emit.call_count, 2)
        self.assertEqual(
            {item.kwargs["room"] for item in emit.call_args_list},
            {"connected-socket", "waiting-socket"},
        )
        expected_payload = {
            "session_id": str(session.pk),
            "reason": "Agenda complete.",
            "ended_at": session.ended_at.isoformat(),
        }
        for item in emit.call_args_list:
            self.assertEqual(item.kwargs["event"], MeetingSocketEvents.SESSION_ENDED)
            self.assertEqual(item.kwargs["payload"], expected_payload)
