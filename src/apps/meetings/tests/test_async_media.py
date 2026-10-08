"""Real ORM/registry integration; only Janus transport is replaced with a double."""
import asyncio
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from asgiref.sync import sync_to_async
from django.contrib.auth import get_user_model
from django.db import connections
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from jrtc_video.models import parse_videoroom_response

from apps.meetings.jrtc.errors import JrtcHandleOwnershipError, JrtcStaleHandleError
from apps.meetings.jrtc.handles import JrtcHandleRegistry
from apps.meetings.jrtc.loss import clear_lost_projection
from apps.meetings.jrtc.runtime import janus_runtime
from apps.meetings.jrtc.videoroom.adapter import VideoRoomAdapter
from apps.meetings.models import ParticipantConnection, ParticipantMediaHandle, MeetingEventType
from apps.meetings.realtime.namespace import MeetingNamespace
from apps.meetings.services.lifecycle import MeetingLifecycleService, record_session_event
from apps.meetings.realtime.emitter import MeetingSocketEmitter
from apps.meetings.services.state import MeetingStateBuilder
from apps.meetings.services.signaling import MeetingMediaSignalService


class FakeSession:
    id = 101
    generation = 1
    ready = True

    def __init__(self):
        self.plugins = {}


class FakePlugin:
    next_id = 200

    def __init__(self, *, session):
        self.session = session
        self.id = None
        self.calls = []
        self.after_candidates = None

    async def attach(self, **kwargs):
        FakePlugin.next_id += 1
        self.id = FakePlugin.next_id
        self.session.plugins[self.id] = self

    async def detach(self):
        self.session.plugins.pop(self.id, None)
        self.id = None

    async def aclose(self):
        pass

    async def join_and_configure(self, body, offer):
        janus_runtime.require_owner_loop()
        return parse_videoroom_response({"videoroom": "joined", "room": 301, "id": 401, "private_id": 501})

    async def unpublish(self):
        janus_runtime.require_owner_loop()
        return parse_videoroom_response({"videoroom": "event", "unpublished": "ok"})

    async def join_subscriber(self, body):
        janus_runtime.require_owner_loop()
        return parse_videoroom_response({"videoroom": "attached", "room": 301, "streams": []})

    async def start(self, **kwargs):
        janus_runtime.require_owner_loop()
        return parse_videoroom_response({"videoroom": "event", "started": "ok"})

    async def trickle(self, candidates):
        janus_runtime.require_owner_loop()
        self.calls.append([item.candidate for item in candidates])
        if self.after_candidates is not None:
            await self.after_candidates()

    async def complete_trickle(self):
        self.calls.append("completed")


class AsyncMediaTests(TestCase):
    def setUp(self):
        user = get_user_model().objects.create_user(username="async-host", clerk_user_id="clerk_async", email="async@example.com")
        self.profile = user.profile
        self.room = MeetingLifecycleService.create_room(creator_profile=self.profile, title="Async meeting")
        self.session = MeetingLifecycleService.start_session(room=self.room, started_by_profile=self.profile)
        self.session.janus_room_id = 301
        self.session.save(update_fields=["janus_room_id"])
        self.participant = self.session.participants.select_related("session", "profile").get(profile=self.profile)
        self.connection = ParticipantConnection.objects.create(session=self.session, participant=self.participant,
            profile=self.profile, socket_id="async-socket", status="active")

    def runtime(self):
        stack = ExitStack()
        registry = JrtcHandleRegistry("owner-test")
        adapter = VideoRoomAdapter(janus_runtime, registry)
        session = FakeSession()
        for name, value in [("_state", janus_runtime.RUNNING), ("_owner_id", "owner-test"),
            ("_loop", asyncio.get_running_loop()), ("_registry", registry), ("_adapter", adapter)]:
            stack.enter_context(patch.object(janus_runtime, name, value))
        stack.enter_context(patch.object(janus_runtime, "run", side_effect=AssertionError("No realtime sync bridge")))
        stack.enter_context(patch.object(janus_runtime, "session", return_value=session))
        stack.enter_context(patch.object(janus_runtime, "observe_session"))
        stack.enter_context(patch("apps.meetings.jrtc.handles.VideoRoomPlugin", FakePlugin))
        stack.enter_context(patch("apps.meetings.realtime.emitter.MeetingSocketEmitter.aemit_session_state", new=AsyncMock()))
        stack.enter_context(patch.object(adapter, "management_command", new=AsyncMock(return_value=
            parse_videoroom_response({"videoroom": "participants", "room": 301,
                "participants": [{"id": 402, "display": "Remote", "publisher": True}]}))))
        return stack

    async def publish(self):
        return await MeetingMediaSignalService.publish_offer(participant=self.participant, connection=self.connection,
            offer={"type": "offer", "sdp": "v=0\r\n"}, tracks=[])

    async def test_native_publish_subscribe_start_and_unpublish(self):
        with self.runtime():
            namespace = MeetingNamespace("/meetings")
            ack = await namespace.on_session_media_publish("async-socket", {"session_id": str(self.session.pk), "offer": {"type": "offer", "sdp": "v=0\r\n"}})
            self.assertEqual(ack["action"], "join_and_configure")
            ack = await namespace.on_session_media_sync_subscriptions("async-socket", {"session_id": str(self.session.pk)})
            self.assertEqual(ack["action"], "join")
            ack = await namespace.on_session_media_start_subscriber("async-socket", {"session_id": str(self.session.pk), "answer": {"type": "answer", "sdp": "v=0\r\n"}})
            self.assertEqual(ack["lifecycle_state"], "ready")
            ack = await namespace.on_session_media_unpublish("async-socket", {"session_id": str(self.session.pk)})
            self.assertEqual(ack["action"], "unpublish")

    async def test_healthy_ice_has_no_claim_or_activity_writes_for_both_roles(self):
        with self.runtime():
            await self.publish()
            await MeetingMediaSignalService.sync_subscriptions(participant=self.participant, connection=self.connection)
            for role in ["publisher", "subscriber"]:
                handle = await ParticipantMediaHandle.objects.aget(participant=self.participant, handle_type=role)
                before = handle.last_event_at
                capture = await sync_to_async(lambda: CaptureQueriesContext(connections["default"]))()
                await sync_to_async(capture.__enter__)()
                try:
                    await MeetingMediaSignalService.trickle(participant=self.participant, connection=self.connection, handle_type=role,
                        candidates=[{"candidate": f"candidate:{i}", "sdpMid": "0", "sdpMLineIndex": 0} for i in range(3)], completed=True)
                finally:
                    await sync_to_async(capture.__exit__)(None, None, None)
                self.assertFalse([q for q in capture.captured_queries if q["sql"].lstrip().split()[0].upper() in {"INSERT", "UPDATE", "DELETE"}])
                self.assertEqual(len(capture.captured_queries), 4)  # handle, auth, final auth, stream ACK
                await handle.arefresh_from_db()
                self.assertEqual(handle.last_event_at, before)
                binding = await janus_runtime.registry.get(str(handle.pk))
                self.assertEqual(binding.plugin.calls, [["candidate:0", "candidate:1", "candidate:2"], "completed"])

    async def test_revocation_between_candidates_and_completion_blocks_completion(self):
        with self.runtime():
            await self.publish()
            handle = await ParticipantMediaHandle.objects.aget(participant=self.participant, handle_type="publisher")
            binding = await janus_runtime.registry.get(str(handle.pk))
            async def revoke():
                await ParticipantConnection.objects.filter(pk=self.connection.pk).aupdate(status="disconnected")
            binding.plugin.after_candidates = revoke
            with self.assertRaises(JrtcHandleOwnershipError):
                await MeetingMediaSignalService.trickle(participant=self.participant, connection=self.connection,
                    handle_type="publisher", candidates=[{"candidate": "candidate:1", "sdpMid": "0"}], completed=True)
            self.assertEqual(len(binding.plugin.calls), 1)

    async def test_lost_registry_cannot_reconstitute_persisted_handle_for_ice(self):
        with self.runtime():
            await self.publish()
            handle = await ParticipantMediaHandle.objects.aget(participant=self.participant, handle_type="publisher")
            await janus_runtime.registry.invalidate(str(handle.pk))
            with self.assertRaises(JrtcStaleHandleError):
                await MeetingMediaSignalService.trickle(participant=self.participant, connection=self.connection,
                    handle_type="publisher", candidates=[], completed=True)
            await handle.arefresh_from_db()
            self.assertIsNone(handle.runtime_claim_id)

    async def test_loss_projection_does_not_clear_replacement_then_clears_lost_generation(self):
        with self.runtime():
            await self.publish()
            snapshot = await ParticipantMediaHandle.objects.aget(participant=self.participant, handle_type="publisher")
            await ParticipantMediaHandle.objects.filter(pk=snapshot.pk).aupdate(janus_handle_id=999)
            await sync_to_async(clear_lost_projection)(snapshot)
            current = await ParticipantMediaHandle.objects.aget(pk=snapshot.pk)
            self.assertEqual(current.janus_handle_id, 999)
            await sync_to_async(clear_lost_projection)(current)
            await current.arefresh_from_db()
            self.assertIsNone(current.janus_handle_id)
            self.assertIsNone(current.runtime_owner_id)
            self.assertEqual(current.lifecycle_state, "failed")
            await self.participant.arefresh_from_db()
            self.assertIsNone(self.participant.janus_publisher_id)

    def test_presence_is_committed_semantic_event_and_snapshots_remain_marked(self):
        with patch.object(MeetingSocketEmitter, "_emit") as emit:
            with self.captureOnCommitCallbacks(execute=True):
                event = record_session_event(session=self.session, event_type=MeetingEventType.PARTICIPANT_JOINED,
                    actor_profile=self.profile, actor_participant=self.participant)
                emit.assert_not_called()
            payload = emit.call_args.kwargs["payload"]
            self.assertEqual(payload["status"], "joined")
            self.assertEqual(payload["event_context"]["event_id"], f"participant_joined:{event.pk}")
            self.assertEqual(payload["event_context"]["actor_participant_id"], str(self.participant.pk))
            self.assertEqual(payload["event_context"]["kind"], "live")
            self.assertTrue(payload["event_context"]["occurred_at"])
        state = MeetingStateBuilder.build(session=self.session, authenticated_profile=self.profile)
        self.assertEqual(state["event_context"], {"kind": "snapshot", "session_id": str(self.session.pk)})
