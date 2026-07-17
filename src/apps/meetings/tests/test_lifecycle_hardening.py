"""Focused regressions for lifecycle races and malformed persisted/API JSON."""

from __future__ import annotations

import uuid
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from apps.meetings.exceptions import JanusGatewayError
from apps.meetings.models import (
    JanusHandleLifecycleState,
    JanusHandleType,
    MediaDirection,
    MediaKind,
    MeetingEvent,
    MeetingEventType,
    MeetingLifecycleState,
    ParticipantConnection,
    ParticipantMediaHandle,
    ParticipantStream,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.services.janus import build_room_payload
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.tasks import (
    attach_participant_media_handles,
    detach_participant_media_handles,
    mark_stale_connections,
    sync_janus_participants,
)


class MeetingLifecycleHardeningTests(TestCase):
    """Pin down race-safe sweeps, Janus cleanup, and JSON boundaries."""

    def make_profile(self, handle: str):
        """Create a profile through the project's user/profile signal contract."""

        user = get_user_model().objects.create_user(
            username=handle,
            email=f"{handle}@example.com",
            password=None,
            clerk_user_id=f"clerk_{handle}",
        )
        profile = user.profile
        profile.display_name = handle.title()
        profile.save(update_fields=["display_name", "updated_at"])
        return profile

    def make_session(self, handle: str):
        """Create a live meeting graph without executing deferred broker callbacks."""

        profile = self.make_profile(handle)
        room = MeetingLifecycleService.create_room(
            creator_profile=profile,
            title=f"{handle.title()} room",
        )
        session = MeetingLifecycleService.start_session(
            room=room,
            started_by_profile=profile,
        )
        return profile, room, session

    @override_settings(MEETING_CONNECTION_STALE_SECONDS=90)
    def test_stale_sweep_cas_does_not_overwrite_racing_heartbeat(self):
        """A heartbeat after candidate discovery wins the conditional stale claim."""

        profile, _room, session = self.make_session("heartbeat-race-host")
        participant = session.participants.get(profile=profile)
        stale_at = timezone.now() - timedelta(minutes=5)
        connection = ParticipantConnection.objects.create(
            session=session,
            participant=participant,
            profile=profile,
            socket_id="racing-heartbeat-socket",
            status=RealtimeConnectionStatus.ACTIVE,
            last_heartbeat_at=stale_at,
        )
        manager = ParticipantConnection.objects
        original_filter = manager.filter
        raced_at = timezone.now()
        race_was_injected = False

        def filter_with_racing_heartbeat(*args, **kwargs):
            nonlocal race_was_injected
            if (
                not race_was_injected
                and kwargs.get("pk") == connection.pk
                and "last_heartbeat_at__lt" in kwargs
            ):
                race_was_injected = True
                original_filter(pk=connection.pk).update(
                    last_heartbeat_at=raced_at,
                    updated_at=raced_at,
                )
            return original_filter(*args, **kwargs)

        with (
            patch.object(manager, "filter", side_effect=filter_with_racing_heartbeat),
            patch(
                "apps.meetings.tasks.MeetingLifecycleService.refresh_session_metrics",
            ) as refresh_metrics,
            patch(
                "apps.meetings.tasks.MeetingSocketEmitter.emit_session_state",
            ) as emit_state,
        ):
            claimed = mark_stale_connections.run()

        self.assertTrue(race_was_injected)
        self.assertEqual(claimed, 0)
        connection.refresh_from_db()
        participant.refresh_from_db()
        self.assertEqual(connection.status, RealtimeConnectionStatus.ACTIVE)
        self.assertEqual(connection.last_heartbeat_at, raced_at)
        self.assertEqual(participant.status, ParticipantStatus.ACTIVE)
        refresh_metrics.assert_not_called()
        emit_state.assert_not_called()

    @override_settings(MEETING_CONNECTION_STALE_SECONDS=90)
    def test_multiple_stale_connections_refresh_and_emit_once_per_session(self):
        """One sweep batches metrics and state fanout for all claims in a session."""

        profile, _room, session = self.make_session("batched-stale-host")
        participant = session.participants.get(profile=profile)
        stale_at = timezone.now() - timedelta(minutes=5)
        connections = [
            ParticipantConnection.objects.create(
                session=session,
                participant=participant,
                profile=profile,
                socket_id=f"batched-stale-socket-{index}",
                status=RealtimeConnectionStatus.ACTIVE,
                last_heartbeat_at=stale_at,
            )
            for index in range(2)
        ]

        with (
            patch(
                "apps.meetings.tasks.MeetingLifecycleService.refresh_session_metrics",
            ) as refresh_metrics,
            patch(
                "apps.meetings.tasks.MeetingSocketEmitter.emit_session_state",
            ) as emit_state,
        ):
            claimed = mark_stale_connections.run()

        self.assertEqual(claimed, 2)
        self.assertEqual(
            set(
                ParticipantConnection.objects.filter(
                    pk__in=[connection.pk for connection in connections],
                ).values_list("status", flat=True),
            ),
            {RealtimeConnectionStatus.STALE},
        )
        refresh_metrics.assert_called_once()
        self.assertEqual(refresh_metrics.call_args.kwargs["session"].pk, session.pk)
        emit_state.assert_called_once()
        self.assertEqual(emit_state.call_args.kwargs["session"].pk, session.pk)

    def test_janus_room_payload_rejects_non_object_legacy_configuration(self):
        """Legacy JSON arrays and scalars cannot silently alter Janus defaults."""

        _profile, room, session = self.make_session("legacy-janus-config-host")

        for invalid_configuration in ([], ["vp8"], "vp8", 17, True):
            with self.subTest(configuration=invalid_configuration):
                room.__class__.objects.filter(pk=room.pk).update(
                    janus_room_configuration=invalid_configuration,
                )
                reloaded_session = session.__class__.objects.select_related("room").get(
                    pk=session.pk,
                )
                with self.assertRaises(JanusGatewayError):
                    build_room_payload(reloaded_session)

    def test_active_participant_media_handles_are_prepared_normally(self):
        """A live participant still receives one publisher and subscriber row."""

        profile, _room, session = self.make_session("active-attach-host")
        session.lifecycle_state = MeetingLifecycleState.WAITING
        session.save(update_fields=["lifecycle_state", "updated_at"])
        participant = session.participants.get(profile=profile)

        with (
            patch(
                "apps.meetings.tasks.MeetingLifecycleService.refresh_session_metrics",
            ) as refresh_metrics,
            patch(
                "apps.meetings.tasks.MeetingSocketEmitter.emit_session_state",
            ) as emit_state,
            self.captureOnCommitCallbacks(execute=True),
        ):
            prepared = attach_participant_media_handles.run(str(participant.pk))

        self.assertEqual(
            set(prepared),
            {JanusHandleType.PUBLISHER, JanusHandleType.SUBSCRIBER},
        )
        self.assertEqual(participant.media_handles.count(), 2)
        self.assertEqual(
            set(participant.media_handles.values_list("lifecycle_state", flat=True)),
            {JanusHandleLifecycleState.ATTACHING},
        )
        refresh_metrics.assert_called_once()
        emit_state.assert_called_once()

    def test_terminal_cleanup_blocks_delayed_media_handle_attach(self):
        """A late task cannot resurrect detached handles after cleanup completed."""

        profile, _room, session = self.make_session("terminal-attach-host")
        participant = session.participants.get(profile=profile)
        for handle_type in (JanusHandleType.PUBLISHER, JanusHandleType.SUBSCRIBER):
            ParticipantMediaHandle.objects.create(
                participant=participant,
                handle_type=handle_type,
                lifecycle_state=JanusHandleLifecycleState.DETACHED,
            )
        session.lifecycle_state = MeetingLifecycleState.ENDED
        session.cleanup_completed_at = timezone.now()
        session.save(
            update_fields=[
                "lifecycle_state",
                "cleanup_completed_at",
                "updated_at",
            ],
        )

        with (
            patch(
                "apps.meetings.tasks.MeetingLifecycleService.refresh_session_metrics",
            ) as refresh_metrics,
            patch(
                "apps.meetings.tasks.MeetingSocketEmitter.emit_session_state",
            ) as emit_state,
        ):
            prepared = attach_participant_media_handles.run(str(participant.pk))

        self.assertEqual(prepared, {})
        self.assertEqual(
            set(participant.media_handles.values_list("lifecycle_state", flat=True)),
            {JanusHandleLifecycleState.DETACHED},
        )
        refresh_metrics.assert_not_called()
        emit_state.assert_not_called()

    def test_departed_participant_blocks_delayed_media_handle_attach(self):
        """A late join callback cannot recreate media state after participant leave."""

        profile, _room, session = self.make_session("departed-attach-host")
        session.lifecycle_state = MeetingLifecycleState.WAITING
        session.save(update_fields=["lifecycle_state", "updated_at"])
        participant = session.participants.get(profile=profile)
        participant.status = ParticipantStatus.LEFT
        participant.save(update_fields=["status", "updated_at"])

        with (
            patch(
                "apps.meetings.tasks.MeetingLifecycleService.refresh_session_metrics",
            ) as refresh_metrics,
            patch(
                "apps.meetings.tasks.MeetingSocketEmitter.emit_session_state",
            ) as emit_state,
        ):
            prepared = attach_participant_media_handles.run(str(participant.pk))

        self.assertEqual(prepared, {})
        self.assertFalse(participant.media_handles.exists())
        refresh_metrics.assert_not_called()
        emit_state.assert_not_called()

    def test_terminal_and_missing_sessions_block_delayed_janus_sync(self):
        """Late sync deliveries do not probe rooms that no longer exist."""

        _profile, _room, session = self.make_session("terminal-sync-host")
        with (
            patch(
                "apps.meetings.tasks.MeetingMediaSignalService.sync_publishers",
            ) as sync_publishers,
            patch(
                "apps.meetings.tasks.MeetingSocketEmitter.emit_session_state",
            ) as emit_state,
        ):
            self.assertEqual(sync_janus_participants.run(str(uuid.uuid4())), {})
            for lifecycle_state in (
                MeetingLifecycleState.ENDING,
                MeetingLifecycleState.ENDED,
                MeetingLifecycleState.FAILED,
            ):
                with self.subTest(lifecycle_state=lifecycle_state):
                    type(session).objects.filter(pk=session.pk).update(
                        lifecycle_state=lifecycle_state,
                    )
                    self.assertEqual(
                        sync_janus_participants.run(str(session.pk)),
                        {},
                    )
            type(session).objects.filter(pk=session.pk).update(
                lifecycle_state=MeetingLifecycleState.WAITING,
                cleanup_completed_at=timezone.now(),
            )
            self.assertEqual(sync_janus_participants.run(str(session.pk)), {})

        sync_publishers.assert_not_called()
        emit_state.assert_not_called()
        self.assertFalse(
            MeetingEvent.objects.filter(
                session=session,
                event_type=MeetingEventType.STATE_SYNCED,
            ).exists(),
        )

    def _create_foreign_handles(self, *, handle: str, publisher_id: str):
        """Create stale cross-process handles carrying state that must be erased."""

        profile, _room, session = self.make_session(handle)
        participant = session.participants.get(profile=profile)
        participant.janus_publisher_id = publisher_id
        participant.janus_private_id = "private-77"
        participant.save(
            update_fields=["janus_publisher_id", "janus_private_id", "updated_at"],
        )
        handles = []
        for index, handle_type in enumerate(
            (JanusHandleType.PUBLISHER, JanusHandleType.SUBSCRIBER),
        ):
            media_handle = ParticipantMediaHandle.objects.create(
                participant=participant,
                handle_type=handle_type,
                lifecycle_state=JanusHandleLifecycleState.READY,
                janus_session_id="foreign-process-session",
                janus_handle_id=f"foreign-handle-{handle}-{index}",
                selected_streams=[{"feed": "remote-feed"}],
                janus_state={"private": "legacy-state"},
            )
            ParticipantStream.objects.create(
                participant=participant,
                media_handle=media_handle,
                direction=MediaDirection.OUTBOUND,
                media_kind=MediaKind.VIDEO,
                janus_mid=str(index),
            )
            handles.append(media_handle)
        return session, participant, handles

    def _assert_foreign_handles_cleared(self, *, participant, handles):
        """Assert cleanup removed every non-portable reference and stream row."""

        participant.refresh_from_db()
        self.assertEqual(participant.janus_publisher_id, "")
        self.assertEqual(participant.janus_private_id, "")
        for media_handle in handles:
            media_handle.refresh_from_db()
            self.assertIsNone(media_handle.janus_handle_id)
            self.assertEqual(media_handle.janus_session_id, "")
            self.assertEqual(media_handle.selected_streams, [])
            self.assertEqual(media_handle.janus_state, {})
            self.assertEqual(
                media_handle.lifecycle_state,
                JanusHandleLifecycleState.DETACHED,
            )
            self.assertFalse(media_handle.streams.exists())

    def test_foreign_handles_are_detached_after_publisher_kick(self):
        """A successful room-level kick finalizes every foreign-owned handle."""

        _session, participant, handles = self._create_foreign_handles(
            handle="foreign-kick-host",
            publisher_id="publisher-42",
        )

        with (
            patch("apps.meetings.tasks.resolve_owned_janus_session", return_value=None),
            patch(
                "apps.meetings.tasks.call_video_room_management_method",
                return_value=SimpleNamespace(videoroom="success"),
            ) as kick,
            patch(
                "apps.meetings.tasks.MeetingLifecycleService.refresh_session_metrics",
            ),
            patch("apps.meetings.tasks.MeetingSocketEmitter.emit_session_state"),
        ):
            detach_participant_media_handles.run(str(participant.pk))

        kick.assert_called_once()
        self.assertEqual(kick.call_args.args[1], "kick")
        self.assertEqual(kick.call_args.args[2].id, "publisher-42")
        self._assert_foreign_handles_cleared(
            participant=participant,
            handles=handles,
        )

    def test_numeric_publisher_id_is_kicked_with_its_native_json_type(self):
        """Decimal IDs persisted in text fields return to Janus as integers."""

        _session, participant, handles = self._create_foreign_handles(
            handle="numeric-kick-host",
            publisher_id="927",
        )

        with (
            patch("apps.meetings.tasks.resolve_owned_janus_session", return_value=None),
            patch(
                "apps.meetings.tasks.call_video_room_management_method",
                return_value=SimpleNamespace(videoroom="success"),
            ) as kick,
            patch(
                "apps.meetings.tasks.MeetingLifecycleService.refresh_session_metrics",
            ),
            patch("apps.meetings.tasks.MeetingSocketEmitter.emit_session_state"),
        ):
            detach_participant_media_handles.run(str(participant.pk))

        kick.assert_called_once()
        self.assertEqual(kick.call_args.args[1], "kick")
        self.assertEqual(kick.call_args.args[2].id, 927)
        self.assertIs(type(kick.call_args.args[2].id), int)
        self._assert_foreign_handles_cleared(
            participant=participant,
            handles=handles,
        )

    def test_foreign_handles_are_detached_when_publisher_is_already_absent(self):
        """An authoritative empty Janus listing also finalizes stale handles."""

        _session, participant, handles = self._create_foreign_handles(
            handle="foreign-absent-host",
            publisher_id="",
        )

        with (
            patch("apps.meetings.tasks.resolve_owned_janus_session", return_value=None),
            patch(
                "apps.meetings.tasks._video_room_participants",
                return_value=([], {"videoroom": "participants"}),
            ) as list_participants,
            patch(
                "apps.meetings.tasks.call_video_room_management_method",
            ) as kick,
            patch(
                "apps.meetings.tasks.MeetingLifecycleService.refresh_session_metrics",
            ),
            patch("apps.meetings.tasks.MeetingSocketEmitter.emit_session_state"),
        ):
            detach_participant_media_handles.run(str(participant.pk))

        list_participants.assert_called_once()
        kick.assert_not_called()
        self._assert_foreign_handles_cleared(
            participant=participant,
            handles=handles,
        )

    def test_object_json_api_fields_reject_arrays_with_http_400(self):
        """Every public object-shaped JSON field rejects an array before services run."""

        profile, room, session = self.make_session("json-boundary-host")
        session.lifecycle_state = MeetingLifecycleState.WAITING
        session.save(update_fields=["lifecycle_state", "updated_at"])
        participant = session.participants.get(profile=profile)
        client = APIClient()
        client.force_authenticate(user=profile.user)

        requests = [
            (
                "room metadata",
                client.post,
                "/api/v1/meetings/rooms/",
                {"title": "Invalid metadata", "metadata": []},
            ),
            (
                "room feature flags",
                client.post,
                "/api/v1/meetings/rooms/",
                {"title": "Invalid flags", "feature_flags": []},
            ),
            (
                "room Janus configuration",
                client.post,
                "/api/v1/meetings/rooms/",
                {"title": "Invalid Janus", "janus_room_configuration": []},
            ),
            (
                "session metadata",
                client.post,
                "/api/v1/meetings/sessions/",
                {"title": "Invalid session", "metadata": []},
            ),
            (
                "session start metadata",
                client.post,
                f"/api/v1/meetings/rooms/{room.slug}/sessions/",
                {"metadata": []},
            ),
            (
                "admission client state",
                client.post,
                f"/api/v1/meetings/sessions/{session.pk}/admission/",
                {"client_state": []},
            ),
            (
                "participant updates",
                client.patch,
                (
                    f"/api/v1/meetings/sessions/{session.pk}/participants/"
                    f"{participant.pk}/"
                ),
                {"updates": []},
            ),
        ]

        for label, method, url, payload in requests:
            with self.subTest(label=label):
                response = method(url, payload, format="json")
                self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
