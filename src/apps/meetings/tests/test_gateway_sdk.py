"""Gateway SDK state/HTTPS compatibility without Socket.IO presence mutations."""

from django.test import TestCase, override_settings

from apps.meetings.models import (
    GatewayAuthorizationOutbox,
    MeetingMessage,
    MeetingReaction,
    Participant,
    ParticipantConnection,
    ParticipantStatus,
)
from apps.meetings.services.gateway_state import snapshot
from apps.meetings.tests.test_gateway_state import StateFixture


@override_settings(GATEWAY_STATE_ENABLED=True)
class GatewaySDKTests(StateFixture, TestCase):
    def test_presence_notifications_require_actual_ordered_media_events(self):
        self.receive(self.event())
        self.assertFalse(self.sdk()["sdk"]["gateway_events"])
        self.receive(self.event(2, "MEDIA_CONNECTED"))
        first = self.sdk()["sdk"]["gateway_events"]
        self.assertEqual([event["type"] for event in first], ["participant_joined"])
        self.receive(self.event(3, "MEDIA_CONNECTED"))
        self.assertEqual(self.sdk()["sdk"]["gateway_events"], first)
        self.participant.refresh_from_db()
        self.assertEqual(self.participant.status, ParticipantStatus.ADMITTED)
        self.assertIsNone(self.participant.joined_at)
        self.receive(self.event(4, "OFFLINE"))
        self.assertEqual(
            self.sdk()["sdk"]["gateway_events"][-1]["type"], "participant_left"
        )

    def sdk(self, profile=None):
        return snapshot(self.session.pk, (profile or self.host).pk, sdk=True)

    def post(self, operation, payload, *, secure=True):
        return self.client.post(
            f"/api/v1/meetings/sessions/{self.session.pk}/gateway/{operation}/",
            payload,
            format="json",
            secure=secure,
        )

    def test_sdk_matches_identity_and_version_without_claiming_presence(self):
        result = self.sdk()
        sdk = result["sdk"]
        self.assertEqual(sdk["room"]["id"], str(self.room.pk))
        self.assertEqual(sdk["local_participant"]["profile"]["id"], str(self.host.pk))
        self.assertEqual(sdk["session"]["state_version"], result["projection_version"])
        self.assertEqual(sdk["event_context"]["kind"], "snapshot")
        self.assertNotIn("janus_room_id", sdk["session"])
        self.assertFalse(
            ParticipantConnection.objects.filter(session=self.session).exists()
        )
        self.participant.refresh_from_db()
        self.assertEqual(self.participant.status, ParticipantStatus.ADMITTED)
        self.assertIsNone(self.participant.joined_at)

    def test_waiting_sdk_hides_people_chat_and_coordinator_fields(self):
        self.post("chat", {"body": "Private discussion"})
        self.intent()
        sdk = self.sdk(self.guest)["sdk"]
        self.assertIsNone(sdk["local_participant"])
        self.assertIsNone(sdk["coordinator_permissions"])
        self.assertEqual(sdk["messages"], [])
        self.assertEqual(sdk["remote_participants"], [])
        self.assertEqual(sdk["pending_join_requests"], [])
        self.assertEqual(sdk["own_join_request"]["id"], self.intent_id)
        self.assertEqual(len(self.sdk()["sdk"]["pending_join_requests"]), 1)
        self.assertFalse(
            any(e["type"] == "chat_message_sent" for e in sdk["gateway_events"])
        )

    def test_approval_changes_sdk_admission_but_not_connected_presence(self):
        self.intent()
        waiting = self.sdk(self.guest)
        self.decision()
        admitted = self.sdk(self.guest)
        self.assertGreater(
            admitted["projection_version"], waiting["projection_version"]
        )
        self.assertIsNotNone(admitted["sdk"]["local_participant"])
        self.assertEqual(
            admitted["local_participant"]["presence"]["connection"], "OFFLINE"
        )

    def test_message_reaction_and_real_event_identity_advance_projection(self):
        before = self.sdk()
        message = self.post("chat", {"body": "Hello"})
        self.assertEqual(message.status_code, 201)
        reaction = self.post("reactions", {"reaction": "👍", "expires_in_seconds": 8})
        self.assertEqual(reaction.status_code, 201)
        after = self.sdk()
        self.assertGreater(after["projection_version"], before["projection_version"])
        self.assertEqual(after["sdk"]["messages"][0]["id"], message.json()["id"])
        events = after["sdk"]["gateway_events"]
        self.assertEqual(
            {event["type"] for event in events}, {"chat_message_sent", "reaction_sent"}
        )
        self.assertTrue(
            all(
                event["actor_participant_id"] == str(self.participant.pk)
                for event in events
            )
        )
        self.assertEqual(self.sdk()["projection_version"], after["projection_version"])

    def test_waiting_removed_or_disabled_participants_cannot_send_application_commands(
        self,
    ):
        self.intent()
        self.client.force_authenticate(self.guest.user)
        self.assertEqual(self.post("chat", {"body": "blocked"}).status_code, 403)
        self.assertEqual(self.post("reactions", {"reaction": "👍"}).status_code, 403)
        self.decision()
        Participant.objects.filter(pk=self.guest_participant.pk).update(
            status=ParticipantStatus.REMOVED
        )
        self.assertEqual(self.post("chat", {"body": "blocked"}).status_code, 403)
        self.assertFalse(MeetingMessage.objects.exists())
        self.assertFalse(MeetingReaction.objects.exists())

    def test_current_capabilities_and_session_authority_are_checked(self):
        self.participant.can_chat = False
        self.participant.can_react = False
        self.participant.save(update_fields=["can_chat", "can_react"])
        self.assertEqual(self.post("chat", {"body": "blocked"}).status_code, 400)
        self.assertEqual(self.post("reactions", {"reaction": "👍"}).status_code, 400)
        self.assertFalse(MeetingMessage.objects.exists())

    def test_https_strict_payload_and_feature_flag_fail_before_writes(self):
        self.assertEqual(
            self.post("chat", {"body": "Hello"}, secure=False).status_code, 403
        )
        self.assertEqual(
            self.post("chat", {"body": "Hello", "sdp": "forbidden"}).status_code, 400
        )
        self.assertEqual(self.post("chat", {"body": "x" * 4001}).status_code, 400)
        with override_settings(GATEWAY_STATE_ENABLED=False):
            self.assertEqual(self.post("chat", {"body": "Hello"}).status_code, 503)
        self.assertFalse(MeetingMessage.objects.exists())

    def test_explicit_leave_fences_the_grant_without_creating_a_socket(self):
        epoch = self.participant.authorization_epoch
        self.assertEqual(self.post("leave", {"reason": "Leaving"}).status_code, 204)
        self.participant.refresh_from_db()
        self.assertEqual(self.participant.status, ParticipantStatus.LEFT)
        self.assertGreater(self.participant.authorization_epoch, epoch)
        self.assertTrue(
            GatewayAuthorizationOutbox.objects.filter(
                participant=self.participant, action="participant.disconnect"
            ).exists()
        )
        self.assertFalse(
            ParticipantConnection.objects.filter(session=self.session).exists()
        )
        self.assertEqual(self.post("chat", {"body": "blocked"}).status_code, 403)

    def test_optional_sdk_shape_has_identical_scoped_pagination(self):
        self.intent()
        before = self.sdk()
        response = self.client.get(self.state_url, {"sdk": "1"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["sdk"], before["sdk"])
        self.assertNotIn("sdk", self.client.get(self.state_url).json())
        self.assertEqual(
            self.client.get(self.state_url, {"sdk": "true"}).status_code, 400
        )
        self.assertEqual(
            self.client.get(
                self.state_url,
                {"sdk": "1", "version": before["projection_version"] + 1},
            ).status_code,
            409,
        )

    def test_remote_private_fields_and_legacy_media_ids_are_not_reintroduced(self):
        self.intent()
        self.decision()
        self.guest_participant.janus_publisher_id = 123456789012345678
        self.guest_participant.metadata = {"private": "not public"}
        self.guest_participant.save(update_fields=["janus_publisher_id", "metadata"])
        sdk = self.sdk()["sdk"]
        remote = sdk["remote_participants"][0]
        self.assertEqual(remote["profile"]["id"], str(self.guest.pk))
        self.assertEqual(remote["metadata"], {})
        self.assertNotIn("janus_publisher_id", remote)
        self.assertEqual(remote["connections"], [])
        self.assertEqual(remote["media_handles"], [])
