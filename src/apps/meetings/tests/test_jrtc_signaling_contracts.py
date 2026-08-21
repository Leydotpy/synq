"""Focused contracts for the JRTC-backed meeting signaling boundary."""

from __future__ import annotations

from types import SimpleNamespace

from django.test import SimpleTestCase
from jrtc_video import SubscribeTarget

from apps.meetings.exceptions import JanusGatewayError
from apps.meetings.services.signaling import (
    _build_subscriber_targets,
    _janus_id_from_persisted_json,
    _preserve_feed_wide_targets,
    _require_internal_janus_id,
    _serialize_selected_streams,
)


class JrtcSignalingIdentifierContractTests(SimpleTestCase):
    def test_internal_ids_reject_wire_strings_and_booleans(self) -> None:
        identifier = 9_007_199_254_740_993
        self.assertEqual(
            _require_internal_janus_id(identifier, kind="Janus feed ID"),
            identifier,
        )
        for invalid in (str(identifier), True, False, 0, -1, None):
            with self.subTest(invalid=invalid), self.assertRaises(JanusGatewayError):
                _require_internal_janus_id(invalid, kind="Janus feed ID")

    def test_persisted_selection_ids_require_canonical_decimal_strings(self) -> None:
        identifier = 9_007_199_254_740_993
        self.assertEqual(
            _janus_id_from_persisted_json(
                str(identifier),
                kind="Janus feed ID",
            ),
            identifier,
        )
        for invalid in (identifier, "01", "+1", " 1", "1 ", "0", True, None):
            with self.subTest(invalid=invalid), self.assertRaises(JanusGatewayError):
                _janus_id_from_persisted_json(invalid, kind="Janus feed ID")

    def test_subscriber_targets_stay_integer_until_explicit_wire_serialization(
        self,
    ) -> None:
        local_id = 9_007_199_254_740_993
        remote_id = local_id + 1
        participant = SimpleNamespace(janus_publisher_id=local_id)

        targets = _build_subscriber_targets(
            participant=participant,
            publisher_payloads=[
                {"id": local_id, "publisher": True, "streams": None},
                {
                    "id": remote_id,
                    "publisher": True,
                    "streams": [{"type": "video", "mid": "1"}],
                },
            ],
        )

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].feed, remote_id)
        self.assertIsInstance(targets[0].feed, int)
        self.assertEqual(
            _serialize_selected_streams(targets),
            [
                {
                    "feed": str(remote_id),
                    "mid": "1",
                    "crossrefid": f"{remote_id}:1",
                    "sub_mid": None,
                }
            ],
        )

    def test_live_publisher_payload_cannot_smuggle_a_string_into_jrtc(self) -> None:
        participant = SimpleNamespace(janus_publisher_id=None)
        with self.assertRaises(JanusGatewayError):
            _build_subscriber_targets(
                participant=participant,
                publisher_payloads=[
                    {"id": "123", "publisher": True, "streams": None}
                ],
            )

    def test_feed_wide_persisted_target_is_restored_as_an_integer_model(self) -> None:
        feed_id = 9_007_199_254_740_993
        targets = [
            SubscribeTarget(feed=feed_id, mid="0"),
            SubscribeTarget(feed=feed_id, mid="1"),
        ]

        preserved = _preserve_feed_wide_targets(
            targets,
            [{"feed": str(feed_id), "mid": None}],
        )

        self.assertEqual(len(preserved), 1)
        self.assertEqual(preserved[0].feed, feed_id)
        self.assertIsInstance(preserved[0].feed, int)
        self.assertIsNone(preserved[0].mid)
