"""Real boundary models and adapter ordering, without Janus/network fixtures."""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from apps.meetings.jrtc.errors import VideoRoomProtocolError
from apps.meetings.jrtc.ice import normalize_ice_request
from apps.meetings.jrtc.videoroom.adapter import VideoRoomAdapter


def candidate(index=1):
    return {"candidate": f"candidate:{index}", "sdpMid": "0", "sdpMLineIndex": 0}


class IceValidationTests(unittest.TestCase):
    def test_strict_wire_shapes(self):
        valid = candidate()
        for payload in (
            {"candidates": []}, {"candidates": [valid, {}]},
            {"candidate": valid, "candidates": [valid]},
            {"candidates": [valid], "completed": "false"},
            {"candidates": [valid], "completed": 1},
            {"candidates": [valid] * 33},
            {"candidate": {**valid, "sdpMLineIndex": True}},
            {"candidate": {**valid, "candidate": "x" * 65536}},
            {"candidate": {"completed": True}},
        ):
            with self.subTest(payload_keys=list(payload)), self.assertRaises(VideoRoomProtocolError):
                normalize_ice_request(payload)
        self.assertEqual(len(normalize_ice_request({"candidate": valid}).candidates), 1)
        self.assertTrue(normalize_ice_request({"candidates": [], "completed": True}).completed)


class IceAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_final_batch_uses_one_fence_for_both_handle_roles(self):
        for role in ("publisher", "subscriber"):
            calls = []
            plugin = SimpleNamespace(
                trickle=AsyncMock(side_effect=lambda batch: calls.append([x.candidate for x in batch])),
                complete_trickle=AsyncMock(side_effect=lambda: calls.append("completed")),
            )
            async def invoke(binding, operation):
                self.assertEqual(binding, role)
                calls.append("lock")
                await operation(plugin)
                calls.append("unlock")
            registry = SimpleNamespace(invoke=AsyncMock(side_effect=invoke))
            adapter = VideoRoomAdapter(SimpleNamespace(), registry)
            batch = normalize_ice_request({"candidates": [candidate(i) for i in range(3)], "completed": True})
            await adapter.trickle_batch(role, batch.candidates, completed=batch.completed)
            self.assertEqual(calls, ["lock", ["candidate:0", "candidate:1", "candidate:2"], "completed", "unlock"])
            registry.invoke.assert_awaited_once()

    async def test_failed_candidates_do_not_send_completion(self):
        plugin = SimpleNamespace(trickle=AsyncMock(side_effect=RuntimeError("unknown outcome")), complete_trickle=AsyncMock())
        async def invoke(binding, operation):
            await operation(plugin)
        adapter = VideoRoomAdapter(SimpleNamespace(), SimpleNamespace(invoke=invoke))
        with self.assertRaises(RuntimeError):
            await adapter.trickle_batch(None, normalize_ice_request({"candidate": candidate()}).candidates, completed=True)
        plugin.complete_trickle.assert_not_awaited()

    async def test_namespace_validates_before_loading_any_database_objects(self):
        from apps.meetings.realtime.namespace import MeetingNamespace
        namespace = MeetingNamespace("/meetings")
        with patch.object(namespace, "_aget_participant_for_session_socket") as load:
            with self.assertRaises(VideoRoomProtocolError):
                await namespace.on_session_media_trickle("socket", {"session_id": "session", "handle_type": "publisher", "candidates": [candidate(), {}]})
            load.assert_not_called()
