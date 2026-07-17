"""Focused contracts for Socket.IO namespace error and connection handling."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from asgiref.sync import async_to_sync
from django.test import SimpleTestCase

from apps.meetings.realtime.events import MeetingSocketEvents
from apps.meetings.models import RealtimeConnectionStatus
from apps.meetings.realtime.namespace import MeetingNamespace


class MeetingNamespaceErrorHandlingTests(SimpleTestCase):
    """Keep connection failures and command acknowledgements observable to clients."""

    def test_connect_errors_are_not_swallowed(self) -> None:
        class RejectingMeetingNamespace(MeetingNamespace):
            async def on_connect(self, sid, environ, auth):
                del sid, environ, auth
                raise RuntimeError("connect failed")

        namespace = RejectingMeetingNamespace("/meetings")

        with self.assertRaisesMessage(RuntimeError, "connect failed"):
            async_to_sync(namespace.trigger_event)("connect", "socket-id", {}, {})

    def test_command_errors_return_negative_acknowledgement(self) -> None:
        class FailingMeetingNamespace(MeetingNamespace):
            async def on_test_command(self, sid, data):
                del sid, data
                raise RuntimeError("command failed")

        namespace = FailingMeetingNamespace("/meetings")
        with (
            patch.object(namespace, "emit", new=AsyncMock()) as emit,
            self.assertLogs("apps.meetings.realtime.namespace", level="ERROR"),
        ):
            acknowledgement = async_to_sync(namespace.trigger_event)(
                "test_command",
                "socket-id",
                {},
            )

        self.assertEqual(
            acknowledgement,
            {
                "ok": False,
                "error": {"message": "command failed", "event": "test_command"},
            },
        )
        emit.assert_awaited_once_with(
            MeetingSocketEvents.ERROR,
            {"message": "command failed", "event": "test_command"},
            to="socket-id",
        )

    def test_connect_does_not_print_environment_or_auth_data(self) -> None:
        namespace = MeetingNamespace("/meetings")
        user = SimpleNamespace(pk="user-id")
        profile = SimpleNamespace(pk="profile-id")
        environ = {"HTTP_COOKIE": "__session=secret-cookie"}
        auth = {"token": "secret-token"}

        with (
            patch("apps.meetings.realtime.namespace.resolve_socket_user", return_value=user),
            patch("apps.meetings.realtime.namespace._get_or_create_profile_for_user", return_value=profile),
            patch.object(namespace, "save_session", new=AsyncMock()) as save_session,
            patch.object(namespace, "enter_room", new=AsyncMock()) as enter_room,
            patch("builtins.print") as print_mock,
        ):
            async_to_sync(namespace.on_connect)("socket-id", environ, auth)

        print_mock.assert_not_called()
        save_session.assert_awaited_once_with(
            "socket-id",
            {"user_id": "user-id", "profile_id": "profile-id"},
        )
        enter_room.assert_awaited_once_with("socket-id", "profile:profile-id")

    def test_waiting_subscription_is_not_added_to_content_room(self) -> None:
        """Lobby sockets receive direct state but cannot hear participant-only broadcasts."""

        namespace = MeetingNamespace("/meetings")
        namespace.server = SimpleNamespace(get_environ=lambda *args, **kwargs: {})
        profile = SimpleNamespace(pk="profile-id")
        session = SimpleNamespace(pk="session-id", room=SimpleNamespace())
        connection = SimpleNamespace(
            participant_id=None,
            status=RealtimeConnectionStatus.SUBSCRIBED,
            socket_id="socket-id",
        )
        namespace._get_profile = AsyncMock(return_value=profile)
        namespace._get_session = AsyncMock(return_value=session)

        with (
            patch(
                "apps.meetings.realtime.namespace.MeetingLifecycleService.bind_connection_to_session",
                return_value=connection,
            ),
            patch(
                "apps.meetings.realtime.namespace.MeetingLifecycleService.mark_connection_heartbeat",
            ),
            patch(
                "apps.meetings.realtime.namespace.MeetingPermissionService.get_room_membership",
                return_value=None,
            ),
            patch(
                "apps.meetings.realtime.namespace.MeetingStateBuilder.build",
                return_value={"session": "personalized"},
            ),
            patch.object(namespace, "enter_room", new=AsyncMock()) as enter_room,
            patch.object(namespace, "emit", new=AsyncMock()) as emit,
        ):
            async_to_sync(namespace.on_session_subscribe)(
                "socket-id",
                {"session_id": "session-id"},
            )

        enter_room.assert_not_awaited()
        emit.assert_awaited_once_with(
            MeetingSocketEvents.SESSION_STATE,
            {"session": "personalized"},
            to="socket-id",
        )

    def test_intentional_leave_unsubscribes_both_session_rooms(self) -> None:
        """The client leave command receives a positive acknowledgement and exits fan-out rooms."""

        namespace = MeetingNamespace("/meetings")
        profile = SimpleNamespace(pk="profile-id")
        session = SimpleNamespace(pk="session-id")
        participant = SimpleNamespace(pk="participant-id")
        namespace._get_profile = AsyncMock(return_value=profile)
        namespace._get_session = AsyncMock(return_value=session)

        with (
            patch(
                "apps.meetings.realtime.namespace.MeetingLifecycleService.leave_participant",
                return_value=participant,
            ) as leave_participant,
            patch(
                "apps.meetings.realtime.namespace.MeetingStateBuilder.serialize_participant",
                return_value={"id": "participant-id"},
            ),
            patch.object(namespace, "leave_room", new=AsyncMock()) as leave_room,
        ):
            acknowledgement = async_to_sync(namespace.on_session_leave)(
                "socket-id",
                {"session_id": "session-id", "reason": "user_left"},
            )

        leave_participant.assert_called_once_with(
            session=session,
            profile=profile,
            socket_id="socket-id",
            reason="user_left",
        )
        self.assertEqual(leave_room.await_count, 2)
        self.assertEqual(
            acknowledgement,
            {"ok": True, "participant": {"id": "participant-id"}},
        )
