"""Regression coverage for cross-layer meeting and runtime contracts."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone

from conf.settings.base import DEFAULT_JANUS_SESSION_URL
from apps.meetings.exceptions import (
    JanusGatewayError,
    MeetingDomainError,
    MeetingPermissionDeniedError,
)
from apps.meetings.models import (
    MeetingAccessPolicy,
    MeetingReaction,
    MeetingRole,
    ParticipantConnection,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.realtime.emitter import MeetingSocketEmitter
from apps.meetings.services.invitations import MeetingInvitationService
from apps.meetings.services.janus import (
    build_room_payload,
    janus_settings,
    register_janus_event_loop,
    resolve_maybe_awaitable,
    resolve_janus_session,
    unregister_janus_event_loop,
)
from janus_api.models.request import PluginMessageRequest
from janus_api.models.videoroom import VideoRoomExistsRequest
from janus_api.servers._proxy import settings as janus_manager_settings
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.services.state import MeetingStateBuilder
from apps.meetings.services.signaling import _validate_track_descriptors_against_sdp
from apps.profiles.models import Profile


class HealthProbeContractTests(SimpleTestCase):
    """Keep deployment probes public, small, and dependency-aware."""

    def test_liveness_probe_is_public(self):
        response = self.client.get("/health/live/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    @patch("conf.health._janus_is_ready", return_value=True)
    @patch("conf.health._redis_is_ready", return_value=True)
    @patch("conf.health._database_is_ready", return_value=True)
    def test_readiness_probe_reports_component_status(self, *_checks):
        response = self.client.get("/health/ready/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "status": "ok",
                "checks": {"database": True, "redis": True, "janus": True},
            },
        )

    @patch("conf.health._janus_is_ready", return_value=False)
    @patch("conf.health._redis_is_ready", return_value=True)
    @patch("conf.health._database_is_ready", return_value=True)
    def test_readiness_probe_returns_503_when_a_dependency_is_down(self, *_checks):
        response = self.client.get("/health/ready/")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["checks"]["janus"], False)


class PublisherSdpContractTests(SimpleTestCase):
    """Prevent clients from hiding or relabelling active publishing m-lines."""

    SDP = "\r\n".join(
        [
            "v=0",
            "a=sendrecv",
            "m=audio 9 UDP/TLS/RTP/SAVPF 111",
            "a=mid:0",
            "a=sendonly",
            "m=video 9 UDP/TLS/RTP/SAVPF 96",
            "a=mid:1",
            "a=sendonly",
            "",
        ]
    )

    def test_active_sdp_sections_require_matching_descriptors(self):
        tracks = [
            {"mid": "0", "kind": "audio", "source": "microphone"},
            {"mid": "1", "kind": "video", "source": "camera"},
        ]

        _validate_track_descriptors_against_sdp(self.SDP, tracks)

        with self.assertRaises(MeetingDomainError):
            _validate_track_descriptors_against_sdp(self.SDP, tracks[:1])

    def test_descriptor_kind_and_source_cannot_be_spoofed(self):
        with self.assertRaises(MeetingDomainError):
            _validate_track_descriptors_against_sdp(
                self.SDP,
                [
                    {"mid": "0", "kind": "video", "source": "camera"},
                    {"mid": "1", "kind": "video", "source": "camera"},
                ],
            )

        with self.assertRaises(MeetingDomainError):
            _validate_track_descriptors_against_sdp(
                self.SDP,
                [
                    {"mid": "0", "kind": "audio", "source": "camera"},
                    {"mid": "1", "kind": "video", "source": "camera"},
                ],
            )

    def test_inactive_or_rejected_m_lines_do_not_require_descriptors(self):
        sdp = "\n".join(
            [
                "v=0",
                "m=audio 9 UDP/TLS/RTP/SAVPF 111",
                "a=mid:0",
                "a=sendonly",
                "m=video 0 UDP/TLS/RTP/SAVPF 96",
                "a=mid:1",
                "a=sendonly",
                "m=video 9 UDP/TLS/RTP/SAVPF 96",
                "a=mid:2",
                "a=inactive",
            ]
        )

        _validate_track_descriptors_against_sdp(
            sdp,
            [{"mid": "0", "kind": "audio", "source": "microphone"}],
        )


class MeetingRuntimeContractTests(TestCase):
    """Verify admission, privacy, capacity, and realtime fan-out invariants."""

    def make_profile(self, handle: str) -> Profile:
        user = get_user_model().objects.create_user(
            username=handle,
            email=f"{handle}@example.com",
            password="password",
            clerk_user_id=f"clerk_{handle}",
        )
        return user.profile

    def make_session(
        self,
        *,
        access_policy: str = MeetingAccessPolicy.APPROVAL_REQUIRED,
        max_participants: int = 100,
    ):
        host = self.make_profile("host-contract")
        room = MeetingLifecycleService.create_room(
            creator_profile=host,
            title="Contract Review",
            access_policy=access_policy,
            max_participants=max_participants,
        )
        session = MeetingLifecycleService.start_session(
            room=room,
            started_by_profile=host,
        )
        return host, room, session

    def test_lobby_state_redacts_participants_messages_and_janus(self):
        host, _, session = self.make_session()
        outsider = self.make_profile("state-outsider")
        session.janus_state = {"private": "topology"}
        session.metadata = {
            "external_id": "consultation:private",
            "tenant_id": "tenant-private",
            "recording": True,
        }
        session.save(update_fields=["janus_state", "metadata", "updated_at"])

        host_state = MeetingStateBuilder.build(
            session=session,
            authenticated_profile=host,
        )
        outsider_state = MeetingStateBuilder.build(
            session=session,
            authenticated_profile=outsider,
        )

        self.assertEqual(host_state["local_participant"]["profile"]["id"], str(host.pk))
        self.assertTrue(host_state["coordinator_permissions"]["can_manage_waiting_room"])
        # Admitted clients receive only the browser-safe publisher topology;
        # arbitrary gateway internals stored on the session never cross the API.
        self.assertEqual(host_state["janus"], {"participants": []})
        self.assertEqual(host_state["session"]["metadata"], {"recording": True})
        self.assertIsNone(outsider_state["local_participant"])
        self.assertEqual(outsider_state["remote_participants"], [])
        self.assertEqual(outsider_state["messages"], [])
        self.assertEqual(outsider_state["janus"], {})
        self.assertEqual(outsider_state["current_profile"]["id"], str(outsider.pk))

    def test_session_state_fanout_targets_only_active_admitted_sockets(self):
        _, _, session = self.make_session(access_policy=MeetingAccessPolicy.OPEN)
        admitted = self.make_profile("active-socket")
        lobby = self.make_profile("lobby-socket")
        active_connection = ParticipantConnection.objects.create(
            session=session,
            profile=admitted,
            socket_id="active-sid",
            status=RealtimeConnectionStatus.SUBSCRIBED,
        )
        MeetingLifecycleService.request_admission(
            session=session,
            profile=admitted,
            connection=active_connection,
        )
        ParticipantConnection.objects.create(
            session=session,
            profile=lobby,
            socket_id="lobby-sid",
            status=RealtimeConnectionStatus.SUBSCRIBED,
        )

        with patch.object(MeetingSocketEmitter, "_emit") as emit:
            MeetingSocketEmitter.emit_session_state(session=session)

            self.assertEqual(emit.call_count, 1)
            self.assertEqual(emit.call_args.kwargs["room"], "active-sid")
            self.assertEqual(
                emit.call_args.kwargs["payload"]["local_participant"]["profile"]["id"],
                str(admitted.pk),
            )

    def test_session_end_fanout_also_reaches_waiting_room_sockets(self):
        _, _, session = self.make_session(access_policy=MeetingAccessPolicy.OPEN)
        lobby = self.make_profile("end-event-lobby")
        ParticipantConnection.objects.create(
            session=session,
            profile=lobby,
            socket_id="end-event-lobby-sid",
            status=RealtimeConnectionStatus.SUBSCRIBED,
        )

        with patch.object(MeetingSocketEmitter, "_emit") as emit:
            MeetingSocketEmitter.emit_session_ended(
                session=session,
                reason="Meeting complete.",
            )

        self.assertEqual(emit.call_count, 1)
        self.assertEqual(emit.call_args.kwargs["room"], "end-event-lobby-sid")
        self.assertEqual(emit.call_args.kwargs["payload"]["session_id"], str(session.pk))

    def test_invite_only_room_requires_a_signed_invitation(self):
        host, _, session = self.make_session(access_policy=MeetingAccessPolicy.INVITE_ONLY)
        outsider = self.make_profile("invite-only-outsider")

        with self.assertRaisesMessage(
            MeetingDomainError,
            "A valid meeting invitation is required",
        ):
            MeetingLifecycleService.request_join(session=session, profile=outsider)

        token = MeetingInvitationService.create_invite_token(
            session=session,
            issuer_profile=host,
        )
        join_request = MeetingLifecycleService.request_join(
            session=session,
            profile=outsider,
            invite_token=token,
        )
        self.assertEqual(join_request.profile_id, outsider.pk)

    @override_settings(MEETING_INVITE_MAX_AGE_SECONDS=3_600)
    def test_requested_invite_lifetime_is_not_capped_by_the_default(self):
        host, _, session = self.make_session()
        token = MeetingInvitationService.create_invite_token(
            session=session,
            issuer_profile=host,
            expires_in_seconds=7_200,
        )

        with patch("django.core.signing.time.time", return_value=time.time() + 3_700):
            payload = MeetingInvitationService.validate_invite_token(
                session=session,
                token=token,
            )

        self.assertEqual(payload["session_id"], str(session.pk))

    def test_removed_participant_cannot_rejoin_the_same_session(self):
        host, _, session = self.make_session(access_policy=MeetingAccessPolicy.OPEN)
        guest = self.make_profile("removed-rejoin")
        participant = MeetingLifecycleService.request_admission(
            session=session,
            profile=guest,
        ).participant
        MeetingLifecycleService.remove_participant(
            session=session,
            actor_profile=host,
            participant=participant,
            reason="Removed by moderator",
        )

        with self.assertRaisesMessage(MeetingDomainError, "cannot rejoin"):
            MeetingLifecycleService.request_admission(
                session=session,
                profile=guest,
            )

    def test_capacity_is_enforced_for_direct_and_reviewed_admission(self):
        host, _, session = self.make_session(
            access_policy=MeetingAccessPolicy.OPEN,
            max_participants=2,
        )
        first = self.make_profile("capacity-first")
        second = self.make_profile("capacity-second")

        result = MeetingLifecycleService.request_admission(
            session=session,
            profile=first,
        )
        self.assertEqual(result.participant.status, "admitted")
        with self.assertRaisesMessage(MeetingDomainError, "maximum participant capacity"):
            MeetingLifecycleService.request_admission(session=session, profile=second)

        # The same invariant applies when a pending request is reviewed.
        room_two = MeetingLifecycleService.create_room(
            creator_profile=host,
            title="Review Capacity",
            max_participants=2,
        )
        session_two = MeetingLifecycleService.start_session(
            room=room_two,
            started_by_profile=host,
        )
        waiting = self.make_profile("capacity-waiting")
        filler = self.make_profile("capacity-filler")
        join_request = MeetingLifecycleService.request_join(
            session=session_two,
            profile=waiting,
        )
        room_two.access_policy = MeetingAccessPolicy.OPEN
        room_two.save(update_fields=["access_policy", "updated_at"])
        session_two.refresh_from_db()
        MeetingLifecycleService.request_admission(session=session_two, profile=filler)
        with self.assertRaisesMessage(MeetingDomainError, "maximum participant capacity"):
            MeetingLifecycleService.review_join_request(
                join_request=join_request,
                reviewer_profile=host,
                approve=True,
            )
        join_request.refresh_from_db()
        self.assertEqual(join_request.status, "pending")

    def test_only_participant_or_moderator_can_change_raised_hand(self):
        host, _, session = self.make_session(access_policy=MeetingAccessPolicy.OPEN)
        guest = self.make_profile("hand-guest")
        guest_participant = MeetingLifecycleService.request_admission(
            session=session,
            profile=guest,
        ).participant
        host_participant = session.participants.get(profile=host)

        own_update = MeetingLifecycleService.update_participant_permissions(
            session=session,
            actor_profile=guest,
            participant=guest_participant,
            updates={"raised_hand_at": timezone.now()},
        )
        self.assertIsNotNone(own_update.raised_hand_at)
        with self.assertRaises(MeetingPermissionDeniedError):
            MeetingLifecycleService.update_participant_permissions(
                session=session,
                actor_profile=guest,
                participant=host_participant,
                updates={"raised_hand_at": timezone.now()},
            )

    def test_expired_reactions_are_not_replayed_in_state(self):
        host, _, session = self.make_session()
        participant = session.participants.get(profile=host)
        expired = MeetingReaction.objects.create(
            session=session,
            participant=participant,
            reaction="clap",
            expires_at=timezone.now() - timedelta(seconds=1),
        )
        live = MeetingReaction.objects.create(
            session=session,
            participant=participant,
            reaction="heart",
            expires_at=timezone.now() + timedelta(seconds=30),
        )

        state = MeetingStateBuilder.build(session=session, authenticated_profile=host)
        reaction_ids = {item["id"] for item in state["recent_reactions"]}
        self.assertNotIn(str(expired.pk), reaction_ids)
        self.assertIn(str(live.pk), reaction_ids)

    def test_only_active_participants_are_counted_and_exposed(self):
        host, _, session = self.make_session(access_policy=MeetingAccessPolicy.OPEN)
        guest = self.make_profile("presence-guest")
        participant = MeetingLifecycleService.request_admission(
            session=session,
            profile=guest,
        ).participant
        self.assertEqual(participant.status, ParticipantStatus.ADMITTED)

        waiting_state = MeetingStateBuilder.build(
            session=session,
            authenticated_profile=host,
        )
        self.assertEqual(waiting_state["counts"]["participants"], 1)
        self.assertEqual(waiting_state["remote_participants"], [])

        MeetingLifecycleService.bind_connection_to_session(
            socket_id="presence-sid",
            session=session,
            profile=guest,
            transport="websocket",
        )
        active_state = MeetingStateBuilder.build(
            session=session,
            authenticated_profile=host,
        )
        self.assertEqual(active_state["counts"]["participants"], 2)
        self.assertEqual(
            [item["profile"]["id"] for item in active_state["remote_participants"]],
            [str(guest.pk)],
        )

    def test_chat_and_reaction_payloads_are_bounded(self):
        host, _, session = self.make_session()
        participant = session.participants.get(profile=host)

        with self.assertRaisesMessage(MeetingDomainError, "cannot be empty"):
            MeetingLifecycleService.record_chat_message(
                session=session,
                participant=participant,
                body="   ",
            )
        with self.assertRaisesMessage(MeetingDomainError, "4000 characters"):
            MeetingLifecycleService.record_chat_message(
                session=session,
                participant=participant,
                body="x" * 4_001,
            )
        with self.assertRaisesMessage(MeetingDomainError, "64 characters"):
            MeetingLifecycleService.record_reaction(
                session=session,
                participant=participant,
                reaction="x" * 65,
            )
        with self.assertRaisesMessage(MeetingDomainError, "between 1 and 60"):
            MeetingLifecycleService.record_reaction(
                session=session,
                participant=participant,
                reaction="clap",
                expires_in_seconds=120,
            )


class JanusLoopBoundaryTests(SimpleTestCase):
    """Ensure SDK coroutines execute on their transport-owning event loop."""

    def test_awaitables_are_submitted_to_registered_janus_loop(self):
        loop = asyncio.new_event_loop()
        started = threading.Event()

        def run_loop() -> None:
            asyncio.set_event_loop(loop)
            started.set()
            loop.run_forever()

        thread = threading.Thread(target=run_loop, daemon=True)
        thread.start()
        self.assertTrue(started.wait(2))
        register_janus_event_loop(loop)

        async def get_thread_id() -> int:
            return threading.get_ident()

        try:
            self.assertEqual(resolve_maybe_awaitable(get_thread_id()), thread.ident)
        finally:
            unregister_janus_event_loop(loop)
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=2)
            loop.close()

    def test_redis_cache_is_configured_to_fail_soft(self):
        # This succeeds whether Redis is running or not; the contract is that a
        # cache miss/outage cannot escape as an authentication 500.
        self.assertIsNone(cache.get("runtime-contract-missing-key"))

    def test_default_janus_websocket_url_uses_the_gateway_endpoint(self):
        self.assertEqual(
            DEFAULT_JANUS_SESSION_URL,
            "ws://127.0.0.1:8188/janus",
        )

    def test_meeting_code_mutates_the_session_managers_runtime_settings(self):
        self.assertIs(janus_settings, janus_manager_settings)

    def test_janus_wire_payload_uses_numeric_ids_and_omits_null_jsep(self):
        request = PluginMessageRequest(
            janus="message",
            session_id="123",
            handle_id="456",
            body=VideoRoomExistsRequest(request="exists", room="789"),
        )

        payload = json.loads(request.model_dump_json())

        self.assertEqual(payload["session_id"], 123)
        self.assertEqual(payload["handle_id"], 456)
        self.assertEqual(payload["body"]["room"], 789)
        self.assertNotIn("jsep", payload)

    def test_default_janus_room_id_is_stable_numeric_and_browser_safe(self):
        class SessionStub:
            pk = "83bdff26-b963-429d-a87b-4cbe6d45e94f"
            janus_room_id = ""

            class room:
                janus_room_configuration = {}
                title = "Runtime room"
                max_participants = 10

            janus_room_secret = ""
            janus_room_pin = ""

        first = build_room_payload(SessionStub())["room"]
        second = build_room_payload(SessionStub())["room"]

        self.assertTrue(str(first).isdecimal())
        self.assertEqual(first, second)
        self.assertLessEqual(int(first), 9_007_199_254_740_991)

    @override_settings(JANUS_ENABLED=False)
    def test_disabled_janus_fails_fast_without_starting_a_worker_loop(self):
        with self.assertRaisesMessage(JanusGatewayError, "disabled"):
            resolve_janus_session()
