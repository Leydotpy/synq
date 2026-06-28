"""Focused coverage for waiting-room authorization and invite links."""

from __future__ import annotations

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.core import mail
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from apps.meetings.exceptions import MeetingPermissionDeniedError
from apps.meetings.models import (
    MeetingRole,
    MeetingRoom,
    MeetingRoomMembership,
    MeetingJoinRequestStatus,
    Participant,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.services.invitations import MeetingInvitationService
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.services.state import MeetingStateBuilder
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

    def test_start_session_does_not_mark_creator_as_joined(self):
        host, _, session = self.make_live_session()

        state = MeetingStateBuilder.build(session=session, authenticated_profile=host)

        self.assertEqual(Participant.objects.filter(session=session, profile=host).count(), 0)
        self.assertEqual(session.join_requests.count(), 0)
        self.assertEqual(state["counts"]["participants"], 0)
        self.assertIsNone(state["local_participant"])

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

    def test_admission_endpoint_admits_host_without_join_request(self):
        host, _, session = self.make_live_session()
        client = APIClient()
        client.force_authenticate(user=host.user)

        response = client.post(
            f"/api/v1/meetings/sessions/{session.pk}/admission/",
            {"display_name": "Host"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        participant = session.participants.get(profile=host)
        self.assertEqual(data["status"], "admitted")
        self.assertTrue(data["direct_entry"])
        self.assertIsNone(data["join_request"])
        self.assertEqual(data["participant"]["id"], str(participant.pk))
        self.assertIsNone(participant.join_request_id)
        self.assertIsNone(participant.joined_at)
        self.assertEqual(session.join_requests.filter(profile=host).count(), 0)

    def test_admission_endpoint_creates_join_request_for_ordinary_participant(self):
        _, _, session = self.make_live_session()
        requester = self.make_profile("ordinary")
        client = APIClient()
        client.force_authenticate(user=requester.user)

        response = client.post(
            f"/api/v1/meetings/sessions/{session.pk}/admission/",
            {"display_name": "Ordinary"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        self.assertEqual(data["status"], "waiting")
        self.assertFalse(data["direct_entry"])
        self.assertIsNone(data["participant"])
        self.assertEqual(data["join_request"]["status"], MeetingJoinRequestStatus.PENDING)
        self.assertEqual(session.join_requests.filter(profile=requester, status=MeetingJoinRequestStatus.PENDING).count(), 1)

    def test_active_room_member_can_enter_without_join_request(self):
        _, room, session = self.make_live_session()
        member = self.make_profile("member")
        membership = MeetingRoomMembership.objects.create(
            room=room,
            profile=member,
            role=MeetingRole.PARTICIPANT,
        )
        connection = MeetingLifecycleService.bind_connection_to_session(
            socket_id="socket-member",
            session=session,
            profile=member,
            transport="web",
            client_session_key="browser-tab-member",
        )

        admission = MeetingLifecycleService.request_admission(session=session, profile=member, connection=connection)

        connection.refresh_from_db()
        participant = session.participants.get(profile=member)
        self.assertEqual(admission.status, "admitted")
        self.assertIsNone(admission.join_request)
        self.assertEqual(participant.membership_id, membership.id)
        self.assertIsNone(participant.join_request_id)
        self.assertEqual(participant.status, ParticipantStatus.ACTIVE)
        self.assertEqual(connection.participant_id, participant.id)
        self.assertEqual(session.join_requests.filter(profile=member).count(), 0)

    @override_settings(MEETING_FRONTEND_BASE_URL="https://meet.example", EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend")
    def test_share_session_emails_signed_frontend_join_link(self):
        host, _, session = self.make_live_session(passcode="secret-passcode")

        payload = MeetingInvitationService.share_session(
            session=session,
            issuer_profile=host,
            emails=["guest@example.com"],
            message="Bring the prototype.",
        )

        self.assertEqual(payload["sent_count"], 1)
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

    @override_settings(MEETING_FRONTEND_BASE_URL="https://meet.example", EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend")
    def test_session_create_endpoint_returns_join_link_and_sends_invites(self):
        host = self.make_profile("planner")
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
        self.assertEqual(Participant.objects.filter(session_id=data["session_id"], profile=host).count(), 0)
        MeetingInvitationService.validate_invite_token(
            session=room.sessions.first(),
            token=data["invite_token"],
        )

        state_response = client.get(f"/api/v1/meetings/sessions/{data['session_id']}/state/")

        self.assertEqual(state_response.status_code, status.HTTP_200_OK)
        state_data = state_response.json()
        self.assertEqual(state_data["current_profile"]["id"], str(host.pk))
        self.assertIsNone(state_data["local_participant"])
        self.assertEqual(state_data["counts"]["participants"], 0)
        self.assertTrue(state_data["coordinator_permissions"]["can_manage_waiting_room"])

    @override_settings(
        MEET_SERVICE_TOKEN="service-secret",
        MEETING_FRONTEND_BASE_URL="https://meet.example",
        EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    )
    def test_service_session_endpoint_provisions_room_for_trusted_backend(self):
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

    def test_reconnect_replaces_previous_connection_and_restores_active_presence(self):
        host, _, session = self.make_live_session()

        first_connection = MeetingLifecycleService.bind_connection_to_session(
            socket_id="socket-old",
            session=session,
            profile=host,
            transport="web",
            client_session_key="browser-tab-1",
        )
        admission = MeetingLifecycleService.request_admission(session=session, profile=host, connection=first_connection)
        session.refresh_from_db()
        first_connection.refresh_from_db()
        admission.participant.refresh_from_db()
        self.assertEqual(session.participant_count, 1)
        self.assertTrue(admission.direct_entry)
        self.assertIsNone(admission.join_request)
        self.assertEqual(first_connection.participant.status, ParticipantStatus.ACTIVE)
        self.assertEqual(session.join_requests.filter(profile=host).count(), 0)

        MeetingLifecycleService.mark_connection_disconnected(socket_id="socket-old")
        session.refresh_from_db()
        first_connection.participant.refresh_from_db()
        self.assertEqual(session.participant_count, 0)
        self.assertEqual(first_connection.participant.status, ParticipantStatus.DISCONNECTED)

        second_connection = MeetingLifecycleService.bind_connection_to_session(
            socket_id="socket-new",
            session=session,
            profile=host,
            transport="web",
            client_session_key="browser-tab-1",
        )

        session.refresh_from_db()
        first_connection.refresh_from_db()
        second_connection.participant.refresh_from_db()
        state = MeetingStateBuilder.build(session=session, authenticated_profile=host)

        self.assertEqual(session.participant_count, 1)
        self.assertEqual(first_connection.status, RealtimeConnectionStatus.DISCONNECTED)
        self.assertEqual(second_connection.participant.status, ParticipantStatus.ACTIVE)
        self.assertEqual(state["counts"]["participants"], 1)
        self.assertEqual(state["local_participant"]["connections"][0]["socket_id"], "socket-new")

    def test_explicit_leave_removes_participant_from_active_session_state(self):
        host, _, session = self.make_live_session()
        connection = MeetingLifecycleService.bind_connection_to_session(
            socket_id="socket-leave",
            session=session,
            profile=host,
            transport="web",
            client_session_key="browser-tab-2",
        )
        MeetingLifecycleService.request_admission(session=session, profile=host, connection=connection)
        connection.refresh_from_db()
        participant = connection.participant

        MeetingLifecycleService.leave_participant(session=session, profile=host, socket_id="socket-leave", reason="user_left")

        participant.refresh_from_db()
        connection.refresh_from_db()
        session.refresh_from_db()
        state = MeetingStateBuilder.build(session=session, authenticated_profile=host)

        self.assertEqual(participant.status, ParticipantStatus.LEFT)
        self.assertEqual(connection.status, RealtimeConnectionStatus.DISCONNECTED)
        self.assertEqual(session.participant_count, 0)
        self.assertEqual(state["counts"]["participants"], 0)
        self.assertIsNone(state["local_participant"])

    def test_approving_stale_lobby_request_does_not_mark_participant_active(self):
        host, _, session = self.make_live_session()
        requester = self.make_profile("stale-lobby")
        connection = MeetingLifecycleService.bind_connection_to_session(
            socket_id="socket-waiting",
            session=session,
            profile=requester,
            transport="web",
            client_session_key="browser-tab-3",
        )
        join_request = MeetingLifecycleService.request_join(session=session, profile=requester, connection=connection)

        MeetingLifecycleService.leave_participant(session=session, profile=requester, socket_id="socket-waiting", reason="left_lobby")
        participant = MeetingLifecycleService.review_join_request(
            join_request=join_request,
            reviewer_profile=host,
            approve=True,
        )

        connection.refresh_from_db()
        session.refresh_from_db()
        state = MeetingStateBuilder.build(session=session, authenticated_profile=requester)

        self.assertEqual(participant.status, ParticipantStatus.ADMITTED)
        self.assertEqual(connection.status, RealtimeConnectionStatus.CONNECTED)
        self.assertEqual(session.participant_count, 0)
        self.assertIsNone(state["local_participant"])

    def test_approving_http_join_request_binds_active_lobby_connection(self):
        host, _, session = self.make_live_session()
        requester = self.make_profile("http-lobby")
        connection = MeetingLifecycleService.bind_connection_to_session(
            socket_id="socket-http-lobby",
            session=session,
            profile=requester,
            transport="web",
            client_session_key="browser-tab-http-lobby",
        )

        join_request = MeetingLifecycleService.request_join(
            session=session,
            profile=requester,
        )
        participant = MeetingLifecycleService.review_join_request(
            join_request=join_request,
            reviewer_profile=host,
            approve=True,
        )

        join_request.refresh_from_db()
        connection.refresh_from_db()
        session.refresh_from_db()
        state = MeetingStateBuilder.build(session=session, authenticated_profile=requester)

        self.assertEqual(join_request.connection_id, connection.id)
        self.assertEqual(participant.status, ParticipantStatus.ACTIVE)
        self.assertEqual(connection.participant_id, participant.id)
        self.assertEqual(connection.status, RealtimeConnectionStatus.ACTIVE)
        self.assertEqual(session.participant_count, 1)
        self.assertEqual(state["local_participant"]["id"], str(participant.id))

    def test_direct_admission_is_idempotent_for_host_without_join_request(self):
        host, _, session = self.make_live_session()

        admission = MeetingLifecycleService.request_admission(session=session, profile=host)

        participant = session.participants.get(profile=host)
        session.refresh_from_db()
        state = MeetingStateBuilder.build(session=session, authenticated_profile=host)

        self.assertEqual(admission.status, "admitted")
        self.assertTrue(admission.direct_entry)
        self.assertIsNone(admission.join_request)
        self.assertEqual(participant.status, ParticipantStatus.ADMITTED)
        self.assertIsNone(participant.joined_at)
        self.assertEqual(session.participant_count, 0)
        self.assertEqual(session.join_requests.filter(profile=host).count(), 0)
        self.assertIsNone(state["local_participant"])

        connection = MeetingLifecycleService.bind_connection_to_session(
            socket_id="socket-host-rejoin",
            session=session,
            profile=host,
            transport="web",
            client_session_key="browser-tab-host",
        )
        repeated_admission = MeetingLifecycleService.request_admission(session=session, profile=host, connection=connection)

        participant.refresh_from_db()
        connection.refresh_from_db()
        session.refresh_from_db()
        state = MeetingStateBuilder.build(session=session, authenticated_profile=host)

        self.assertEqual(repeated_admission.participant.id, participant.id)
        self.assertIsNone(repeated_admission.join_request)
        self.assertEqual(connection.participant_id, participant.pk)
        self.assertEqual(participant.status, ParticipantStatus.ACTIVE)
        self.assertIsNotNone(participant.joined_at)
        self.assertEqual(session.participant_count, 1)
        self.assertEqual(session.join_requests.filter(profile=host).count(), 0)
        self.assertEqual(state["local_participant"]["profile"]["id"], str(host.pk))

    def test_waiting_room_coordinator_admission_bypasses_join_request(self):
        _, room, session = self.make_live_session()
        coordinator = self.make_profile("coordinator")
        membership = MeetingRoomMembership.objects.create(
            room=room,
            profile=coordinator,
            role=MeetingRole.CO_HOST,
        )
        connection = MeetingLifecycleService.bind_connection_to_session(
            socket_id="socket-coordinator-lobby",
            session=session,
            profile=coordinator,
            transport="web",
            client_session_key="browser-tab-coordinator",
        )

        self.assertIsNone(connection.participant_id)
        self.assertEqual(connection.status, RealtimeConnectionStatus.SUBSCRIBED)

        admission = MeetingLifecycleService.request_admission(
            session=session,
            profile=coordinator,
            requested_display_name="Coordinator",
            connection=connection,
        )

        connection.refresh_from_db()
        session.refresh_from_db()
        participant = session.participants.get(profile=coordinator)
        state = MeetingStateBuilder.build(session=session, authenticated_profile=coordinator)

        self.assertEqual(admission.status, "admitted")
        self.assertTrue(admission.direct_entry)
        self.assertIsNone(admission.join_request)
        self.assertEqual(participant.membership_id, membership.id)
        self.assertIsNone(participant.join_request_id)
        self.assertEqual(participant.status, ParticipantStatus.ACTIVE)
        self.assertEqual(connection.participant_id, participant.id)
        self.assertEqual(connection.status, RealtimeConnectionStatus.ACTIVE)
        self.assertEqual(Participant.objects.filter(session=session, profile=coordinator).count(), 1)
        self.assertEqual(session.join_requests.filter(profile=coordinator).count(), 0)
        self.assertEqual(session.participant_count, 1)
        self.assertEqual(state["local_participant"]["id"], str(participant.id))

        repeated_admission = MeetingLifecycleService.request_admission(
            session=session,
            profile=coordinator,
            connection=connection,
        )

        self.assertEqual(repeated_admission.participant.id, participant.id)
        self.assertIsNone(repeated_admission.join_request)
        self.assertEqual(Participant.objects.filter(session=session, profile=coordinator).count(), 1)
