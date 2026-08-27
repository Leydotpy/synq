"""Cross-layer admission and client-state contract regression tests."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from rest_framework import status
from rest_framework.test import APIClient

from apps.meetings.models import (
    JanusHandleType,
    MeetingAccessPolicy,
    MeetingLifecycleState,
    MeetingRole,
    Participant,
    ParticipantConnection,
    ParticipantMediaHandle,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.realtime.emitter import MeetingSocketEmitter
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.services.state import MeetingStateBuilder


class MeetingRuntimeLifecycleContractTests(TestCase):
    """Exercise the server contracts used by the current web client."""

    def make_profile(self, handle: str):
        """Create a user and return its signal-created profile."""

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

    def make_session(self, *, access_policy=MeetingAccessPolicy.APPROVAL_REQUIRED, max_participants=100):
        """Create a session ready for admission decisions without contacting Janus."""

        host = self.make_profile(f"host-{self._testMethodName}")
        room = MeetingLifecycleService.create_room(
            creator_profile=host,
            title="Runtime contract",
            access_policy=access_policy,
            max_participants=max_participants,
        )
        session = MeetingLifecycleService.start_session(
            room=room,
            started_by_profile=host,
        )
        session.lifecycle_state = MeetingLifecycleState.WAITING
        session.save(update_fields=["lifecycle_state", "updated_at"])
        return host, room, session

    def test_forced_disconnect_routes_once_per_socket_generation(self):
        """The Socket.IO manager can route revocation to the owning worker."""

        server = SimpleNamespace(disconnect=AsyncMock())
        with patch("conf.socketio.get_socket_server", return_value=server):
            MeetingSocketEmitter.disconnect_sockets(
                ("socket-owner", "socket-owner", "", None)
            )

        server.disconnect.assert_awaited_once_with(
            "socket-owner",
            namespace=MeetingSocketEmitter.namespace,
        )

    def admission_post(self, *, profile, session, payload=None):
        """Post to the same endpoint used by the client Join button."""

        client = APIClient()
        client.force_authenticate(user=profile.user)
        return client.post(
            f"/api/v1/meetings/sessions/{session.pk}/admission/",
            payload or {},
            format="json",
        )

    def test_open_room_admits_directly(self):
        """An open room returns the client's enter action and one participant."""

        _, _, session = self.make_session(access_policy=MeetingAccessPolicy.OPEN)
        guest = self.make_profile("open-guest")

        response = self.admission_post(profile=guest, session=session)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()["action"], "enter")
        self.assertFalse(response.json()["requires_approval"])
        self.assertEqual(
            Participant.objects.get(session=session, profile=guest).status,
            ParticipantStatus.ADMITTED,
        )

    def test_approval_room_returns_waiting_decision(self):
        """Approval-required rooms create a single pending request."""

        _, _, session = self.make_session()
        guest = self.make_profile("waiting-guest")

        first = self.admission_post(profile=guest, session=session)
        second = self.admission_post(profile=guest, session=session)

        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.assertEqual(second.status_code, status.HTTP_201_CREATED)
        self.assertEqual(first.json()["action"], "wait")
        self.assertEqual(first.json()["join_request_id"], second.json()["join_request_id"])
        self.assertEqual(session.join_requests.count(), 1)

    def test_invite_only_room_rejects_missing_invite(self):
        """Invite-only cannot be bypassed merely because the room has no passcode."""

        _, _, session = self.make_session(access_policy=MeetingAccessPolicy.INVITE_ONLY)
        guest = self.make_profile("uninvited-guest")

        response = self.admission_post(profile=guest, session=session)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("valid meeting invitation", str(response.json()))

    def test_removed_profile_and_full_room_cannot_reenter(self):
        """Admission preserves removals and enforces capacity under the session lock."""

        _, room, session = self.make_session(
            access_policy=MeetingAccessPolicy.OPEN,
            max_participants=2,
        )
        removed_profile = self.make_profile("removed-guest")
        Participant.objects.create(
            room=room,
            session=session,
            profile=removed_profile,
            role=MeetingRole.PARTICIPANT,
            status=ParticipantStatus.REMOVED,
            display_name="Removed",
        )
        removed_response = self.admission_post(
            profile=removed_profile,
            session=session,
        )
        self.assertEqual(removed_response.status_code, status.HTTP_400_BAD_REQUEST)

        present_profile = self.make_profile("present-guest")
        Participant.objects.create(
            room=room,
            session=session,
            profile=present_profile,
            role=MeetingRole.PARTICIPANT,
            status=ParticipantStatus.ACTIVE,
            display_name="Present",
        )
        overflow_profile = self.make_profile("overflow-guest")
        overflow_response = self.admission_post(
            profile=overflow_profile,
            session=session,
        )
        self.assertEqual(overflow_response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("maximum participant capacity", str(overflow_response.json()))

    def test_state_is_personalized_and_internal_identifiers_are_redacted(self):
        """Waiting sockets see their own request, while raw transport secrets stay private."""

        host, _, session = self.make_session()
        guest = self.make_profile("state-guest")
        host_participant = Participant.objects.get(session=session, profile=host)
        ParticipantConnection.objects.create(
            session=session,
            participant=host_participant,
            profile=host,
            socket_id="host-secret-socket",
            client_session_key="host-secret-key",
            status=RealtimeConnectionStatus.ACTIVE,
            metadata={"secret": "connection-secret"},
        )
        ParticipantMediaHandle.objects.create(
            participant=host_participant,
            handle_type=JanusHandleType.PUBLISHER,
            opaque_id="private-opaque-id",
            janus_session_id=7_488_603_522_389_459,
            janus_state={"secret": "private-handle-state"},
        )
        guest_connection = ParticipantConnection.objects.create(
            session=session,
            profile=guest,
            socket_id="guest-socket",
            status=RealtimeConnectionStatus.SUBSCRIBED,
        )
        MeetingLifecycleService.request_join(
            session=session,
            profile=guest,
            connection=guest_connection,
        )
        session.janus_state = {
            "secret": "raw-janus-secret",
            "participants": [
                {
                    "id": 42,
                    "private_id": 99,
                    "streams": [{"mid": "0", "codec": "opus", "secret": "stream-secret"}],
                },
            ],
        }
        session.save(update_fields=["janus_state", "updated_at"])

        host_state = MeetingStateBuilder.build(session=session, authenticated_profile=host)
        guest_state = MeetingStateBuilder.build(session=session, authenticated_profile=guest)
        serialized = json.dumps(host_state)

        self.assertEqual(len(host_state["pending_join_requests"]), 1)
        self.assertIsNotNone(guest_state["own_join_request"])
        self.assertEqual(guest_state["remote_participants"], [])
        for secret in (
            "host-secret-socket",
            "host-secret-key",
            "connection-secret",
            "private-opaque-id",
            "7488603522389459",
            "private-handle-state",
            "raw-janus-secret",
            "stream-secret",
        ):
            self.assertNotIn(secret, serialized)
        self.assertEqual(host_state["janus"]["participants"][0]["id"], "42")
        self.assertEqual(
            MeetingSocketEmitter.active_participant_socket_ids(session.pk),
            ["host-secret-socket"],
        )

        with patch.object(MeetingSocketEmitter, "_emit") as emit:
            MeetingSocketEmitter.emit_session_state(session=session)
        payloads = {call.kwargs["room"]: call.kwargs["payload"] for call in emit.call_args_list}
        self.assertIn("host-secret-socket", payloads)
        self.assertIn("guest-socket", payloads)
        self.assertEqual(payloads["guest-socket"]["remote_participants"], [])
        self.assertIsNotNone(payloads["guest-socket"]["own_join_request"])
