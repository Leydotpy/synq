"""Focused contracts for durable JRTC identifiers and browser boundaries."""

from __future__ import annotations

import uuid
from types import SimpleNamespace

from django.db import IntegrityError, models, transaction
from django.test import SimpleTestCase, TestCase

from apps.meetings.models import (
    JrtcEventReceipt,
    MeetingSession,
    Participant,
    ParticipantMediaHandle,
    ParticipantStream,
)
from apps.meetings.services.state import MeetingStateBuilder, _serialize_janus_id


class _Collection:
    """Minimal related-manager stand-in used by pure serialization tests."""

    def __init__(self, *items: object) -> None:
        self._items = items

    def all(self) -> tuple[object, ...]:
        return self._items


class JrtcPersistenceContractTests(TestCase):
    """Keep persistent correlation data separate from live plugin ownership."""

    def test_active_janus_identifiers_are_nullable_positive_bigints(self) -> None:
        for model, field_name in (
            (MeetingSession, "control_handle_id"),
            (MeetingSession, "janus_room_id"),
            (Participant, "janus_publisher_id"),
            (Participant, "janus_private_id"),
            (ParticipantMediaHandle, "janus_session_id"),
            (ParticipantMediaHandle, "janus_handle_id"),
            (ParticipantStream, "janus_feed_id"),
        ):
            with self.subTest(model=model.__name__, field=field_name):
                field = model._meta.get_field(field_name)
                self.assertIsInstance(field, models.PositiveBigIntegerField)
                self.assertTrue(field.null)
                self.assertTrue(field.blank)

    def test_models_do_not_materialize_live_plugins(self) -> None:
        self.assertFalse(hasattr(MeetingSession, "control_handle"))
        self.assertFalse(hasattr(ParticipantMediaHandle, "handle"))

        owner_field = ParticipantMediaHandle._meta.get_field("runtime_owner_id")
        self.assertIsInstance(owner_field, models.CharField)
        self.assertTrue(owner_field.null)
        self.assertTrue(owner_field.db_index)
        self.assertIn(
            ("janus_session_id", "janus_handle_id"),
            tuple(
                tuple(index.fields)
                for index in ParticipantMediaHandle._meta.indexes
            ),
        )

    def test_participant_mediahandle_compatibility_aliases_return_records(self) -> None:
        class PersistedRecord:
            @property
            def handle(self):
                raise AssertionError("ORM compatibility aliases accessed a live handle")

        record = PersistedRecord()
        contracts = (
            ("publisher_mediahandle", "publisher_mediahandle_record"),
            ("subscriber_mediahandle", "subscriber_mediahandle_record"),
            ("textroom_mediahandle", "textroom_mediahandle_record"),
        )

        for compatibility_name, record_name in contracts:
            with self.subTest(property=compatibility_name):
                participant = SimpleNamespace(**{record_name: record})
                compatibility_property = getattr(Participant, compatibility_name)
                self.assertIs(compatibility_property.fget(participant), record)

                participant = SimpleNamespace(**{record_name: None})
                self.assertIsNone(compatibility_property.fget(participant))

    def test_event_receipt_has_a_unique_broker_identity(self) -> None:
        event_id = JrtcEventReceipt._meta.get_field("event_id")
        self.assertIsInstance(event_id, models.UUIDField)
        self.assertTrue(event_id.unique)
        self.assertEqual(
            JrtcEventReceipt._meta.get_field("duplicate_count").default,
            0,
        )

        envelope_id = uuid.uuid4()
        JrtcEventReceipt.objects.create(
            event_id=envelope_id,
            event_type="janus.event",
        )
        with self.assertRaises(IntegrityError), transaction.atomic():
            JrtcEventReceipt.objects.create(
                event_id=envelope_id,
                event_type="janus.event",
            )


class JrtcBrowserBoundaryContractTests(SimpleTestCase):
    """Ensure browser JSON never receives unsafe JavaScript numeric IDs."""

    def test_decimal_id_serializer_rejects_non_protocol_values(self) -> None:
        self.assertEqual(_serialize_janus_id(9_194_204_203_876_831), "9194204203876831")
        self.assertEqual(_serialize_janus_id("00042"), "42")
        self.assertIsNone(_serialize_janus_id(None))
        self.assertIsNone(_serialize_janus_id(True))
        self.assertIsNone(_serialize_janus_id("named-room"))
        self.assertIsNone(_serialize_janus_id(0))

    def test_session_participant_and_stream_ids_are_decimal_strings(self) -> None:
        session = SimpleNamespace(
            pk=uuid.uuid4(),
            started_by_profile=None,
            lifecycle_state="active",
            janus_room_id=7_488_603_522_389_459,
            state_version=3,
            started_at=None,
            ended_at=None,
            last_synced_at=None,
            metadata={},
        )
        serialized_session = MeetingStateBuilder.serialize_session(session)
        self.assertEqual(serialized_session["janus_room_id"], "7488603522389459")

        stream = SimpleNamespace(
            pk=uuid.uuid4(),
            direction="outbound",
            media_kind="video",
            janus_mid="0",
            janus_feed_id=9_194_204_203_876_831,
            janus_feed_mid="0",
            codec="vp8",
            is_active=True,
            is_ready=True,
            is_moderated=False,
            metadata={},
            source_participant_id=None,
        )
        handle = SimpleNamespace(
            pk=uuid.uuid4(),
            handle_type="publisher",
            lifecycle_state="ready",
            selected_streams=[],
            last_event_at=None,
            streams=_Collection(stream),
        )
        participant = SimpleNamespace(
            pk=uuid.uuid4(),
            profile=None,
            role="participant",
            status="active",
            display_name="Participant",
            can_publish_audio=True,
            can_publish_video=True,
            can_share_screen=False,
            can_chat=True,
            can_react=True,
            is_muted=False,
            is_camera_blocked=False,
            raised_hand_at=None,
            janus_publisher_id=8_888_888_888_888_888,
            joined_at=None,
            left_at=None,
            last_seen_at=None,
            metadata={},
            connections=_Collection(),
            media_handles=_Collection(handle),
        )

        serialized_participant = MeetingStateBuilder.serialize_participant(participant)
        self.assertEqual(
            serialized_participant["janus_publisher_id"],
            "8888888888888888",
        )
        self.assertEqual(
            serialized_participant["media_handles"][0]["streams"][0]["janus_feed_id"],
            "9194204203876831",
        )

        session.janus_room_id = None
        participant.janus_publisher_id = None
        stream.janus_feed_id = None
        self.assertIsNone(
            MeetingStateBuilder.serialize_session(session)["janus_room_id"],
        )
        null_participant = MeetingStateBuilder.serialize_participant(participant)
        self.assertIsNone(null_participant["janus_publisher_id"])
        self.assertIsNone(
            null_participant["media_handles"][0]["streams"][0]["janus_feed_id"],
        )
