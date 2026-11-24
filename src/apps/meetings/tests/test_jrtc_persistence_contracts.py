"""Focused contracts for durable JRTC identifiers and browser boundaries."""

from __future__ import annotations

import uuid
from types import SimpleNamespace

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import IntegrityError, models, transaction
from django.test import TestCase

from apps.meetings.models import (
    JanusHandleType,
    JrtcEventReceipt,
    MediaDirection,
    MediaKind,
    MeetingRoom,
    MeetingSession,
    Participant,
    ParticipantMediaHandle,
    ParticipantStream,
)



JANUS_ID_FIELDS = (
    (MeetingSession, "control_handle_id"),
    (MeetingSession, "janus_room_id"),
    (Participant, "janus_publisher_id"),
    (Participant, "janus_private_id"),
    (ParticipantMediaHandle, "janus_session_id"),
    (ParticipantMediaHandle, "janus_handle_id"),
    (ParticipantStream, "janus_feed_id"),
)


class JrtcPersistenceContractTests(TestCase):
    """Keep persistent correlation data separate from live plugin ownership."""

    def test_active_janus_identifiers_are_nullable_positive_bigints(self) -> None:
        for model, field_name in JANUS_ID_FIELDS:
            with self.subTest(model=model.__name__, field=field_name):
                field = model._meta.get_field(field_name)
                self.assertIsInstance(field, models.PositiveBigIntegerField)
                self.assertTrue(field.null)
                self.assertTrue(field.blank)
                self.assertTrue(
                    any(
                        isinstance(validator, MinValueValidator)
                        and validator.limit_value == 1
                        for validator in field.validators
                    )
                )
                with self.assertRaises(ValidationError):
                    field.run_validators(0)

    def test_database_rejects_zero_for_every_nullable_janus_identifier(self) -> None:
        user = get_user_model().objects.create_user(
            username="strict-janus-ids",
            email="strict-janus-ids@example.com",
            password=None,
            clerk_user_id="clerk_strict_janus_ids",
        )
        profile = user.profile
        room = MeetingRoom.objects.create(
            title="Strict Janus ID room",
            created_by_profile=profile,
        )
        session = MeetingSession.objects.create(
            room=room,
            started_by_profile=profile,
        )
        participant = Participant.objects.create(
            room=room,
            session=session,
            profile=profile,
            display_name="Strict IDs",
        )
        media_handle = ParticipantMediaHandle.objects.create(
            participant=participant,
            handle_type=JanusHandleType.PUBLISHER,
        )
        stream = ParticipantStream.objects.create(
            participant=participant,
            media_handle=media_handle,
            direction=MediaDirection.OUTBOUND,
            media_kind=MediaKind.VIDEO,
            janus_mid="0",
        )
        instances = {
            MeetingSession: session,
            Participant: participant,
            ParticipantMediaHandle: media_handle,
            ParticipantStream: stream,
        }

        for model, field_name in JANUS_ID_FIELDS:
            with self.subTest(model=model.__name__, field=field_name):
                instance = instances[model]
                with self.assertRaises(IntegrityError), transaction.atomic():
                    model.objects.filter(pk=instance.pk).update(
                        **{field_name: 0}
                    )
                self.assertEqual(
                    model.objects.filter(pk=instance.pk).update(
                        **{field_name: 1}
                    ),
                    1,
                )
                self.assertEqual(
                    model.objects.filter(pk=instance.pk).update(
                        **{field_name: None}
                    ),
                    1,
                )

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
