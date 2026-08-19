"""Focused idempotency contracts for Janus lifecycle Celery tasks."""

from __future__ import annotations

import uuid
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone
from janus_videoroom_plugin import VideoRoomCreated, VideoRoomReply, VideoRoomSuccess

from apps.meetings.exceptions import JanusGatewayError
from apps.meetings.models import (
    MeetingEvent,
    MeetingEventType,
    MeetingLifecycleState,
    MeetingSession,
    ParticipantConnection,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.services.janus import janus_room_id_for_session
from apps.meetings.tasks import (
    destroy_janus_room_for_session,
    mark_stale_connections,
    provision_janus_room_for_session,
)


class JanusLifecycleTaskIdempotencyTests(TestCase):
    """Keep retries, overlapping workers, and lifecycle races side-effect safe."""

    def make_session(self, handle: str = "task-host"):
        """Create a live session without executing its post-commit task callbacks."""

        user = get_user_model().objects.create_user(
            username=handle,
            email=f"{handle}@example.com",
            password=None,
            clerk_user_id=f"clerk_{handle}",
        )
        profile = user.profile
        profile.display_name = handle.title()
        profile.save(update_fields=["display_name", "updated_at"])
        room = MeetingLifecycleService.create_room(
            creator_profile=profile,
            title=f"{handle.title()} room",
        )
        session = MeetingLifecycleService.start_session(
            room=room,
            started_by_profile=profile,
        )
        return profile, room, session

    @staticmethod
    def created_reply(room_id: int | str) -> VideoRoomReply:
        """Build the typed Janus v3 response accepted by the task adapter."""

        return VideoRoomReply(
            data=VideoRoomCreated(
                videoroom="created",
                room=room_id,
                permanent=False,
            ),
        )

    @patch("apps.meetings.tasks.call_video_room_management_method")
    def test_provision_missing_session_is_a_noop(self, janus_call) -> None:
        missing_id = str(uuid.uuid4())

        result = provision_janus_room_for_session.run(missing_id)

        self.assertEqual(result, {})
        janus_call.assert_not_called()

    @patch("apps.meetings.tasks.call_video_room_management_method")
    def test_provision_terminal_session_states_are_noops(self, janus_call) -> None:
        _profile, _room, session = self.make_session("terminal-host")
        expected_state = {"sentinel": "terminal-state"}

        for lifecycle_state in (
            MeetingLifecycleState.ENDING,
            MeetingLifecycleState.ENDED,
            MeetingLifecycleState.FAILED,
        ):
            with self.subTest(lifecycle_state=lifecycle_state):
                MeetingSession.objects.filter(pk=session.pk).update(
                    lifecycle_state=lifecycle_state,
                    janus_state=expected_state,
                )

                result = provision_janus_room_for_session.run(str(session.pk))

                self.assertEqual(result, expected_state)

        janus_call.assert_not_called()
        self.assertFalse(
            MeetingEvent.objects.filter(
                session=session,
                event_type=MeetingEventType.SESSION_PROVISIONED,
            ).exists(),
        )

    @patch("apps.meetings.tasks.call_video_room_management_method")
    def test_provision_already_provisioned_live_session_is_a_noop(self, janus_call) -> None:
        _profile, _room, session = self.make_session("provisioned-host")
        expected_state = {"plugindata": {"data": {"room": "existing-room"}}}

        for lifecycle_state in (
            MeetingLifecycleState.WAITING,
            MeetingLifecycleState.ACTIVE,
        ):
            with self.subTest(lifecycle_state=lifecycle_state):
                MeetingSession.objects.filter(pk=session.pk).update(
                    lifecycle_state=lifecycle_state,
                    janus_room_id="existing-room",
                    janus_state=expected_state,
                )

                result = provision_janus_room_for_session.run(str(session.pk))

                self.assertEqual(result, expected_state)

        janus_call.assert_not_called()
        self.assertFalse(
            MeetingEvent.objects.filter(
                session=session,
                event_type=MeetingEventType.SESSION_PROVISIONED,
            ).exists(),
        )

    def test_duplicate_provisioning_creates_persists_and_emits_once(self) -> None:
        _profile, _room, session = self.make_session("duplicate-host")
        expected_room_id = janus_room_id_for_session(session)
        reply = self.created_reply(expected_room_id)

        with (
            patch(
                "apps.meetings.tasks.call_video_room_management_method",
                return_value=reply,
            ) as janus_call,
            patch(
                "apps.meetings.tasks.MeetingSocketEmitter.emit_session_state",
            ) as emit_state,
        ):
            with self.captureOnCommitCallbacks(execute=True):
                first_result = provision_janus_room_for_session.run(str(session.pk))
            with self.captureOnCommitCallbacks(execute=True):
                second_result = provision_janus_room_for_session.run(str(session.pk))

        session.refresh_from_db()
        self.assertEqual(session.janus_room_id, str(expected_room_id))
        self.assertEqual(session.lifecycle_state, MeetingLifecycleState.WAITING)
        self.assertEqual(first_result, session.janus_state)
        self.assertEqual(second_result, session.janus_state)
        janus_call.assert_called_once()
        self.assertEqual(janus_call.call_args.args[1], "create")
        emit_state.assert_called_once()
        self.assertEqual(
            MeetingEvent.objects.filter(
                session=session,
                event_type=MeetingEventType.SESSION_PROVISIONED,
            ).count(),
            1,
        )

    def test_duplicate_create_reconciliation_preserves_numeric_room_id(self) -> None:
        """The post-create exists probe must not stringify a numeric Janus ID."""

        _profile, _room, session = self.make_session("exists-probe-host")
        expected_room_id = janus_room_id_for_session(session)
        exists_reply = VideoRoomReply(
            data=VideoRoomSuccess(
                videoroom="success",
                room=expected_room_id,
                exists=True,
            ),
        )

        def reconcile_duplicate(_session, method, request):
            self.assertIs(type(request.room if method == "create" else request), int)
            self.assertEqual(
                request.room if method == "create" else request,
                expected_room_id,
            )
            if method == "create":
                raise JanusGatewayError("Room already exists.")
            self.assertEqual(method, "exists")
            return exists_reply

        with (
            patch(
                "apps.meetings.tasks.call_video_room_management_method",
                side_effect=reconcile_duplicate,
            ) as janus_call,
            patch("apps.meetings.tasks.MeetingSocketEmitter.emit_session_state"),
        ):
            result = provision_janus_room_for_session.run(str(session.pk))

        session.refresh_from_db()
        self.assertEqual(janus_call.call_count, 2)
        self.assertEqual(session.janus_room_id, str(expected_room_id))
        self.assertTrue(result["reconciled_existing_room"])

    @patch("apps.meetings.tasks.call_video_room_management_method")
    def test_destroy_missing_session_is_a_noop(self, janus_call) -> None:
        result = destroy_janus_room_for_session.run(str(uuid.uuid4()))

        self.assertEqual(result, {})
        janus_call.assert_not_called()

    @patch("apps.meetings.tasks.call_video_room_management_method")
    def test_destroy_already_cleaned_session_is_a_noop(self, janus_call) -> None:
        _profile, _room, session = self.make_session("cleaned-host")
        expected_destroy_state = {
            "videoroom": "destroyed",
            "room": str(session.pk),
        }
        MeetingSession.objects.filter(pk=session.pk).update(
            lifecycle_state=MeetingLifecycleState.ENDED,
            cleanup_completed_at=timezone.now(),
            janus_state={"destroy": expected_destroy_state},
        )

        result = destroy_janus_room_for_session.run(str(session.pk))

        self.assertEqual(result, expected_destroy_state)
        janus_call.assert_not_called()
        self.assertFalse(
            MeetingEvent.objects.filter(
                session=session,
                event_type=MeetingEventType.CLEANUP_COMPLETED,
            ).exists(),
        )

    @override_settings(MEETING_CONNECTION_STALE_SECONDS=90)
    def test_mark_stale_preserves_active_participant_with_live_sibling(self) -> None:
        profile, _room, session = self.make_session("multi-device-host")
        participant = session.participants.get(profile=profile)
        stale_connection = ParticipantConnection.objects.create(
            session=session,
            participant=participant,
            profile=profile,
            socket_id="socket-stale",
            status=RealtimeConnectionStatus.ACTIVE,
            last_heartbeat_at=timezone.now() - timedelta(minutes=5),
        )
        live_connection = ParticipantConnection.objects.create(
            session=session,
            participant=participant,
            profile=profile,
            socket_id="socket-live",
            status=RealtimeConnectionStatus.ACTIVE,
            last_heartbeat_at=timezone.now(),
        )

        with patch(
            "apps.meetings.tasks.MeetingSocketEmitter.emit_session_state",
        ) as emit_state:
            count = mark_stale_connections.run()

        stale_connection.refresh_from_db()
        live_connection.refresh_from_db()
        participant.refresh_from_db()
        self.assertEqual(count, 1)
        self.assertEqual(stale_connection.status, RealtimeConnectionStatus.STALE)
        self.assertEqual(live_connection.status, RealtimeConnectionStatus.ACTIVE)
        self.assertEqual(participant.status, ParticipantStatus.ACTIVE)
        emit_state.assert_called_once()

    def test_provisioning_that_crosses_end_persists_room_and_queues_teardown(self) -> None:
        _profile, _room, session = self.make_session("race-host")
        expected_room_id = janus_room_id_for_session(session)
        reply = self.created_reply(expected_room_id)

        def end_session_during_create(_session, method, request):
            self.assertEqual(method, "create")
            self.assertEqual(request.room, expected_room_id)
            MeetingSession.objects.filter(pk=session.pk).update(
                lifecycle_state=MeetingLifecycleState.ENDED,
                ended_at=timezone.now(),
            )
            return reply

        with (
            patch(
                "apps.meetings.tasks.call_video_room_management_method",
                side_effect=end_session_during_create,
            ),
            patch("apps.meetings.tasks.queue_janus_room_cleanup") as queue_cleanup,
            patch(
                "apps.meetings.tasks.MeetingSocketEmitter.emit_session_state",
            ) as emit_state,
        ):
            with self.captureOnCommitCallbacks(execute=True):
                result = provision_janus_room_for_session.run(str(session.pk))

        session.refresh_from_db()
        self.assertEqual(session.lifecycle_state, MeetingLifecycleState.ENDED)
        self.assertEqual(session.janus_room_id, str(expected_room_id))
        self.assertIsNone(session.cleanup_completed_at)
        self.assertEqual(result, session.janus_state)
        queue_cleanup.assert_called_once_with(str(session.pk))
        emit_state.assert_not_called()
        self.assertFalse(
            MeetingEvent.objects.filter(
                session=session,
                event_type=MeetingEventType.SESSION_PROVISIONED,
            ).exists(),
        )
