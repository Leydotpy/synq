"""Focused coverage for waiting-room authorization and invite links."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, patch

from asgiref.sync import async_to_sync
from django.contrib.auth import get_user_model
from django.core import mail
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from apps.meetings.exceptions import MeetingPermissionDeniedError
from apps.meetings.models import (
    MeetingAccessPolicy,
    MeetingRole,
    MeetingRoom,
    MeetingRoomMembership,
    ParticipantConnection,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.realtime.namespace import MeetingNamespace
from apps.meetings.services.invitations import MeetingInvitationService
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.tasks import send_meeting_invitation_email
from apps.profiles.models import Profile


class MeetingAdmissionInviteTests(TestCase):
    """Verify who can review join requests and how signed invite links behave."""

    def make_profile(self, handle: str) -> Profile:
        """Create an auth user and return the signal-created profile."""

        user = get_user_model().objects.create_user(
            username=handle,
            email=f"{handle}@example.com",
            password="password",
            clerk_user_id=f"clerk_{handle}",
        )
        profile = user.profile
        profile.display_name = handle.title()
        profile.save(update_fields=["display_name", "updated_at"])
        return profile

    def make_live_session(self, *, passcode: str | None = None):
        """Create a room and start a live session as the host."""

        host = self.make_profile("host")
        room = MeetingLifecycleService.create_room(
            creator_profile=host,
            title="Design Review",
            passcode=passcode,
        )
        session = MeetingLifecycleService.start_session(room=room, started_by_profile=host)
        return host, room, session

    @staticmethod
    def deliver_invitation_immediately(*args):
        """Execute only the invitation task eagerly while keeping other tasks mocked."""

        return send_meeting_invitation_email.apply(args=args, throw=True)

    def test_custom_waiting_room_permission_can_review_join_request(self):
        host, room, session = self.make_live_session()
        reviewer = self.make_profile("reviewer")
        requester = self.make_profile("requester")
        membership = MeetingRoomMembership.objects.create(
            room=room,
            profile=reviewer,
            role=MeetingRole.PARTICIPANT,
            can_manage_waiting_room=True,
        )
        membership.refresh_from_db()
        self.assertTrue(membership.can_manage_waiting_room)

        join_request = MeetingLifecycleService.request_join(session=session, profile=requester)

        participant = MeetingLifecycleService.review_join_request(
            join_request=join_request,
            reviewer_profile=reviewer,
            approve=True,
        )

        self.assertEqual(participant.profile_id, requester.id)
        join_request.refresh_from_db()
        self.assertEqual(join_request.reviewed_by_profile_id, reviewer.id)

    def test_regular_participant_cannot_review_join_request(self):
        _, room, session = self.make_live_session()
        reviewer = self.make_profile("regular")
        requester = self.make_profile("candidate")
        MeetingRoomMembership.objects.create(
            room=room,
            profile=reviewer,
            role=MeetingRole.PARTICIPANT,
        )
        join_request = MeetingLifecycleService.request_join(session=session, profile=requester)

        with self.assertRaises(MeetingPermissionDeniedError):
            MeetingLifecycleService.review_join_request(
                join_request=join_request,
                reviewer_profile=reviewer,
                approve=True,
            )

    def test_review_endpoint_returns_forbidden_for_unprivileged_reviewer(self):
        _, room, session = self.make_live_session()
        reviewer = self.make_profile("endpoint-regular")
        requester = self.make_profile("endpoint-candidate")
        MeetingRoomMembership.objects.create(
            room=room,
            profile=reviewer,
            role=MeetingRole.PARTICIPANT,
        )
        join_request = MeetingLifecycleService.request_join(session=session, profile=requester)
        client = APIClient()
        client.force_authenticate(user=reviewer.user)

        response = client.post(
            f"/api/v1/meetings/sessions/{session.pk}/join-requests/{join_request.pk}/review/",
            {"approve": True},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @override_settings(MEETING_FRONTEND_BASE_URL="https://meet.example", EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend")
    @patch.object(send_meeting_invitation_email, "delay")
    def test_share_session_emails_signed_frontend_join_link(self, invitation_delay):
        host, _, session = self.make_live_session(passcode="secret-passcode")
        invitation_delay.side_effect = self.deliver_invitation_immediately

        payload = MeetingInvitationService.share_session(
            session=session,
            issuer_profile=host,
            emails=["guest@example.com"],
            message="Bring the prototype.",
        )

        self.assertEqual(payload["sent_count"], 0)
        self.assertEqual(payload["queued_count"], 1)
        self.assertEqual(payload["delivery_status"], "queued")
        self.assertEqual(len(mail.outbox), 1)
        self.assertIn("https://meet.example/meetings/", payload["join_url"])
        self.assertIn("invite=", payload["join_url"])
        self.assertIn(payload["join_url"], mail.outbox[0].body)
        MeetingInvitationService.validate_invite_token(session=session, token=payload["invite_token"])

    def test_signed_invite_token_bypasses_manual_passcode_for_join_request(self):
        host, _, session = self.make_live_session(passcode="secret-passcode")
        requester = self.make_profile("invited")
        token = MeetingInvitationService.create_invite_token(session=session, issuer_profile=host)

        join_request = MeetingLifecycleService.request_join(
            session=session,
            profile=requester,
            invite_token=token,
        )

        self.assertEqual(join_request.profile_id, requester.id)

    def test_admission_binds_matching_subscribed_socket_connection(self):
        _, room, session = self.make_live_session()
        room.access_policy = MeetingAccessPolicy.OPEN
        room.save(update_fields=["access_policy", "updated_at"])
        requester = self.make_profile("socket-guest")
        stale_connection = ParticipantConnection.objects.create(
            session=session,
            profile=requester,
            socket_id="old-socket",
            status=RealtimeConnectionStatus.ACTIVE,
            client_session_key="join-key",
            last_heartbeat_at=timezone.now() - timedelta(minutes=5),
        )
        active_connection = ParticipantConnection.objects.create(
            session=session,
            profile=requester,
            socket_id="live-socket",
            status=RealtimeConnectionStatus.SUBSCRIBED,
            client_session_key="join-key",
            last_heartbeat_at=timezone.now(),
        )
        client = APIClient()
        client.force_authenticate(user=requester.user)

        response = client.post(
            f"/api/v1/meetings/sessions/{session.pk}/admission/",
            {"client_session_key": "join-key"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertEqual(data["action"], "enter")
        self.assertEqual(data["participant_status"], ParticipantStatus.ACTIVE)
        active_connection.refresh_from_db()
        stale_connection.refresh_from_db()
        participant = session.participants.get(profile=requester)
        self.assertEqual(active_connection.participant_id, participant.pk)
        self.assertEqual(active_connection.status, RealtimeConnectionStatus.ACTIVE)
        self.assertIsNone(active_connection.disconnected_at)
        self.assertEqual(stale_connection.status, RealtimeConnectionStatus.DISCONNECTED)

    def test_reviewing_join_request_binds_requester_socket_connection(self):
        host, _, session = self.make_live_session()
        requester = self.make_profile("waiting-guest")
        connection = ParticipantConnection.objects.create(
            session=session,
            profile=requester,
            socket_id="waiting-socket",
            status=RealtimeConnectionStatus.SUBSCRIBED,
            client_session_key="approval-key",
        )
        join_request = MeetingLifecycleService.request_join(
            session=session,
            profile=requester,
            connection=connection,
            client_session_key="approval-key",
        )

        participant = MeetingLifecycleService.review_join_request(
            join_request=join_request,
            reviewer_profile=host,
            approve=True,
        )

        connection.refresh_from_db()
        join_request.refresh_from_db()
        self.assertEqual(participant.status, ParticipantStatus.ACTIVE)
        self.assertEqual(connection.participant_id, participant.pk)
        self.assertEqual(connection.status, RealtimeConnectionStatus.ACTIVE)
        self.assertEqual(join_request.connection_id, connection.pk)

    def test_media_socket_lookup_repairs_admitted_connection_binding(self):
        _, room, session = self.make_live_session()
        room.access_policy = MeetingAccessPolicy.OPEN
        room.save(update_fields=["access_policy", "updated_at"])
        session.refresh_from_db()
        requester = self.make_profile("repair-guest")
        participant = MeetingLifecycleService.request_admission(
            session=session,
            profile=requester,
        ).participant
        participant.status = ParticipantStatus.ADMITTED
        participant.save(update_fields=["status", "updated_at"])
        connection = ParticipantConnection.objects.create(
            session=session,
            profile=requester,
            socket_id="repair-socket",
            status=RealtimeConnectionStatus.SUBSCRIBED,
            client_session_key="repair-key",
        )

        resolved_participant, resolved_connection = MeetingNamespace("/meetings")._get_participant_connection_for_session_socket(
            str(session.pk),
            "repair-socket",
        )

        connection.refresh_from_db()
        participant.refresh_from_db()
        self.assertEqual(resolved_participant.pk, participant.pk)
        self.assertEqual(resolved_connection.pk, connection.pk)
        self.assertEqual(connection.participant_id, participant.pk)
        self.assertEqual(connection.status, RealtimeConnectionStatus.ACTIVE)
        self.assertEqual(participant.status, ParticipantStatus.ACTIVE)

    def test_namespace_connect_errors_are_not_swallowed(self):
        class RejectingMeetingNamespace(MeetingNamespace):
            async def on_connect(self, sid, environ, auth):
                raise RuntimeError("connect failed")

        namespace = RejectingMeetingNamespace("/meetings")

        with self.assertRaisesMessage(RuntimeError, "connect failed"):
            async_to_sync(namespace.trigger_event)("connect", "socket-id", {}, {})

    def test_namespace_command_errors_return_negative_acknowledgements(self):
        class FailingMeetingNamespace(MeetingNamespace):
            async def on_test_command(self, sid, data):
                del sid, data
                raise RuntimeError("command failed")

        namespace = FailingMeetingNamespace("/meetings")
        with patch.object(namespace, "emit", new=AsyncMock()) as emit:
            acknowledgement = async_to_sync(namespace.trigger_event)(
                "test_command",
                "socket-id",
                {},
            )

        self.assertFalse(acknowledgement["ok"])
        self.assertEqual(acknowledgement["error"]["message"], "command failed")
        self.assertEqual(acknowledgement["error"]["event"], "test_command")
        emit.assert_awaited_once()

    @override_settings(MEETING_FRONTEND_BASE_URL="https://meet.example", EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend")
    @patch.object(send_meeting_invitation_email, "delay")
    def test_session_create_endpoint_returns_join_link_and_sends_invites(self, invitation_delay):
        host = self.make_profile("planner")
        invitation_delay.side_effect = self.deliver_invitation_immediately
        client = APIClient()
        client.force_authenticate(user=host.user)
        scheduled_start_at = timezone.now() + timedelta(days=1)
        scheduled_end_at = scheduled_start_at + timedelta(hours=1)

        response = client.post(
            "/api/v1/meetings/sessions/",
            {
                "title": "Roadmap Sync",
                "description": "Review next release priorities.",
                "scheduled_start_at": scheduled_start_at.isoformat(),
                "scheduled_end_at": scheduled_end_at.isoformat(),
                "participant_emails": ["Guest@Example.com", "guest@example.com"],
                "message": "Bring release notes.",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        room = MeetingRoom.objects.get(pk=data["room_id"])
        self.assertEqual(data["provider"], "synq_meet")
        self.assertEqual(data["room"]["title"], "Roadmap Sync")
        self.assertEqual(data["room_slug"], room.slug)
        self.assertEqual(data["session_id"], str(room.sessions.first().pk))
        self.assertIn("https://meet.example/meetings/", data["join_url"])
        self.assertIn("invite=", data["join_url"])
        self.assertEqual(data["shared_invites"]["emails"], ["guest@example.com"])
        self.assertEqual(mail.outbox[0].to, ["guest@example.com"])
        self.assertEqual(room.scheduled_start_at, scheduled_start_at)
        MeetingInvitationService.validate_invite_token(
            session=room.sessions.first(),
            token=data["invite_token"],
        )

        state_response = client.get(f"/api/v1/meetings/sessions/{data['session_id']}/state/")

        self.assertEqual(state_response.status_code, status.HTTP_200_OK)
        state_data = state_response.json()
        self.assertEqual(state_data["local_participant"]["profile"]["id"], str(host.pk))
        self.assertTrue(state_data["coordinator_permissions"]["can_manage_waiting_room"])

    @override_settings(
        MEET_SERVICE_TOKEN="service-secret",
        MEETING_FRONTEND_BASE_URL="https://meet.example",
        EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    )
    @patch.object(send_meeting_invitation_email, "delay")
    def test_service_session_endpoint_provisions_room_for_trusted_backend(self, invitation_delay):
        invitation_delay.side_effect = self.deliver_invitation_immediately
        client = APIClient()
        payload = {
            "external_id": "consultation:abc-123",
            "title": "Client intake",
            "description": "Privileged consultation",
            "participant_emails": ["lawyer@example.com"],
            "metadata": {"tenant_id": "tenant-1"},
        }

        response = client.post(
            "/api/v1/meetings/internal/service-sessions/",
            payload,
            format="json",
            HTTP_AUTHORIZATION="Bearer service-secret",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        room = MeetingRoom.objects.get(metadata__external_id="consultation:abc-123")
        self.assertEqual(data["provider"], "synq_meet")
        self.assertEqual(data["room_id"], str(room.pk))
        self.assertIn("https://meet.example/meetings/", data["join_url"])
        self.assertIn("invite=", data["join_url"])
        self.assertEqual(mail.outbox[0].to, ["lawyer@example.com"])

        second_response = client.post(
            "/api/v1/meetings/internal/service-sessions/",
            payload,
            format="json",
            HTTP_AUTHORIZATION="Bearer service-secret",
        )

        self.assertEqual(second_response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(MeetingRoom.objects.filter(metadata__external_id="consultation:abc-123").count(), 1)
        self.assertEqual(second_response.json()["session_id"], data["session_id"])
