"""Authoritative gateway admission, idempotency and durable decision regression tests."""

import uuid
from unittest.mock import patch

from django.db import OperationalError, transaction
from django.test import (
    TestCase,
    TransactionTestCase,
    override_settings,
    skipUnlessDBFeature,
)

from apps.meetings.models import (
    GatewayAdmissionDecision,
    GatewayAuthorizationOutbox,
    MeetingAccessPolicy,
    MeetingEvent,
    MeetingEventType,
    MeetingJoinRequest,
    MeetingJoinRequestStatus,
    MeetingRole,
    MeetingRoomMembership,
    Participant,
    ParticipantStatus,
)
from apps.meetings.services.gateway_admission import request_intent, review_intent
from apps.meetings.services.gateway_tickets import GatewayDenied, check_ticket
from apps.meetings.services.lifecycle import MeetingLifecycleService as Lifecycle
from apps.meetings.tests import test_gateway_ticket_races as race_support
from apps.meetings.tests.test_gateway_tickets import GatewayTicketFixture


class GatewayAdmissionFixture(GatewayTicketFixture):
    def setUp(self):
        super().setUp()
        self.flags = override_settings(GATEWAY_ADMISSION_ENABLED=True)
        self.flags.enable()
        self.addCleanup(self.flags.disable)
        self.room.feature_flags = {"gateway_control_plane": True}
        self.room.save(update_fields=["feature_flags"])

    def intent(self):
        result = request_intent(session_id=self.session.pk, profile_id=self.guest.pk)
        self.intent_id = result["join_request_id"]
        self.guest_participant = Participant.objects.get(pk=result["participant_id"])
        return result

    def decision(self, **changes):
        arguments = {
            "session_id": self.session.pk,
            "join_request_id": self.intent_id,
            "actor_profile_id": self.host.pk,
            "decision_id": uuid.uuid4(),
            "approve": True,
            "expected_epoch": 0,
        }
        arguments.update(changes)
        return review_intent(**arguments)


class GatewayAdmissionTests(GatewayAdmissionFixture, TestCase):
    def test_pending_intent_is_idempotent_and_lobby_only(self):
        first = self.intent()
        second = request_intent(session_id=self.session.pk, profile_id=self.guest.pk)
        self.assertEqual(first, second)
        self.assertEqual(first["participant_status"], ParticipantStatus.WAITING)
        self.assertEqual(
            MeetingJoinRequest.objects.filter(profile=self.guest).count(), 1
        )
        self.assertFalse(GatewayAuthorizationOutbox.objects.exists())
        self.assertEqual(
            MeetingEvent.objects.filter(
                event_type=MeetingEventType.JOIN_REQUEST_CREATED
            ).count(),
            1,
        )
        _, claims = self.issue(self.guest)
        self.assertEqual(claims["scopes"], ["lobby", "session:resume"])

    def test_approval_is_not_connected_and_outbox_is_atomic(self):
        self.intent()
        with patch("apps.meetings.services.lifecycle.dispatch_task") as dispatch:
            result = self.decision()
        dispatch.assert_not_called()
        self.guest_participant.refresh_from_db()
        self.assertEqual(result["participant_status"], ParticipantStatus.ADMITTED)
        self.assertEqual(result["enforcement_status"], "pending")
        self.assertIsNone(self.guest_participant.joined_at)
        self.assertIsNone(self.guest_participant.janus_publisher_id)
        outbox = GatewayAuthorizationOutbox.objects.get()
        self.assertEqual(outbox.action, "participant.admit")
        self.assertEqual(outbox.authorization_epoch, 1)
        self.assertEqual(outbox.data["permissions"]["can_subscribe"], True)
        self.assertEqual(outbox.actor_id, str(self.host.pk))
        self.assertFalse(
            MeetingEvent.objects.filter(
                actor_participant=self.guest_participant,
                event_type=MeetingEventType.PARTICIPANT_JOINED,
            ).exists()
        )
        _, claims = self.issue(self.guest)
        self.assertIn("media:publish", claims["scopes"])

    def test_same_decision_retry_has_one_epoch_and_outbox(self):
        self.intent()
        key = uuid.uuid4()
        first = self.decision(decision_id=key)
        self.assertEqual(self.decision(decision_id=key), first)
        self.assertEqual(GatewayAdmissionDecision.objects.count(), 1)
        self.assertEqual(GatewayAuthorizationOutbox.objects.count(), 1)
        self.assertEqual(first["authorization_epoch"], 1)

    def test_decision_key_cannot_change_action_or_expected_epoch(self):
        self.intent()
        key = uuid.uuid4()
        self.decision(decision_id=key)
        for changes in ({"approve": False}, {"expected_epoch": 1}):
            with (
                self.subTest(changes=changes),
                self.assertRaises(GatewayDenied) as error,
            ):
                self.decision(decision_id=key, **changes)
            self.assertEqual(error.exception.code, "IDEMPOTENCY_CONFLICT")

    def test_regular_or_inactive_reviewer_cannot_approve(self):
        self.intent()
        for actor in (self.guest, self.profile("outsider")):
            with self.assertRaises(GatewayDenied):
                self.decision(actor_profile_id=actor.pk)
        self.room.memberships.filter(profile=self.host).update(is_active=False)
        with self.assertRaises(GatewayDenied):
            self.decision()
        self.assertFalse(GatewayAdmissionDecision.objects.exists())

    def test_custom_waiting_room_moderator_can_approve_but_not_spoof_host(self):
        self.intent()
        moderator = self.profile("moderator")
        MeetingRoomMembership.objects.create(
            room=self.room,
            profile=moderator,
            role=MeetingRole.PARTICIPANT,
            can_manage_waiting_room=True,
        )
        key = uuid.uuid4()
        self.decision(actor_profile_id=moderator.pk, decision_id=key)
        self.assertEqual(
            GatewayAdmissionDecision.objects.get().actor_profile_id, moderator.pk
        )
        with self.assertRaises(GatewayDenied):
            self.decision(actor_profile_id=self.host.pk, decision_id=key)

    def test_requested_role_is_never_authority(self):
        self.intent()
        MeetingJoinRequest.objects.filter(pk=self.intent_id).update(
            requested_role=MeetingRole.HOST
        )
        self.decision()
        self.guest_participant.refresh_from_db()
        self.assertEqual(self.guest_participant.role, MeetingRole.PARTICIPANT)

    def test_denied_cannot_mint_or_reenter_even_if_room_opens(self):
        self.intent()
        _, claims = self.issue(self.guest)
        self.decision(approve=False)
        self.assertEqual(
            GatewayAuthorizationOutbox.objects.get().action, "participant.deny"
        )
        with self.assertRaises(GatewayDenied):
            self.issue(self.guest)
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="session.hello", consume=True)
        self.room.access_policy = MeetingAccessPolicy.OPEN
        self.room.save(update_fields=["access_policy"])
        with self.assertRaises(GatewayDenied):
            request_intent(session_id=self.session.pk, profile_id=self.guest.pk)

    def test_stale_epoch_expired_intent_and_wrong_meeting_reject(self):
        self.intent()
        with self.assertRaises(GatewayDenied):
            self.decision(expected_epoch=1)
        with self.assertRaises(GatewayDenied):
            self.decision(session_id=uuid.uuid4())
        MeetingJoinRequest.objects.filter(pk=self.intent_id).update(
            status=MeetingJoinRequestStatus.EXPIRED
        )
        with self.assertRaises(GatewayDenied):
            self.decision()
        self.assertFalse(GatewayAuthorizationOutbox.objects.exists())

    def test_approval_retry_after_removal_reports_superseded_and_cannot_restore(self):
        self.intent()
        key = uuid.uuid4()
        self.decision(decision_id=key)
        Lifecycle.remove_participant(
            session=self.session,
            actor_profile=self.host,
            participant=self.guest_participant,
        )
        result = self.decision(decision_id=key)
        self.assertTrue(result["superseded"])
        self.assertEqual(result["participant_status"], ParticipantStatus.REMOVED)
        self.assertEqual(result["authorization_epoch"], 2)
        self.assertEqual(GatewayAuthorizationOutbox.objects.count(), 2)
        with self.assertRaises(GatewayDenied):
            self.decision()

    def test_removal_before_approval_wins(self):
        self.intent()
        Lifecycle.remove_participant(
            session=self.session,
            actor_profile=self.host,
            participant=self.guest_participant,
        )
        with self.assertRaises(GatewayDenied):
            self.decision()
        self.assertFalse(GatewayAdmissionDecision.objects.exists())

    def test_direct_entry_requires_django_policy_and_does_not_create_media(self):
        self.room.access_policy = MeetingAccessPolicy.OPEN
        self.room.save(update_fields=["access_policy"])
        with patch("apps.meetings.services.lifecycle.dispatch_task") as dispatch:
            result = self.intent()
        self.assertEqual(result["participant_status"], ParticipantStatus.ADMITTED)
        self.assertIsNone(self.guest_participant.joined_at)
        self.assertEqual(
            GatewayAuthorizationOutbox.objects.get().action, "participant.admit"
        )
        self.assertEqual(self.intent(), result)
        dispatch.assert_not_called()

    def test_passcode_invite_and_capacity_gates_are_reused(self):
        self.room.access_policy = MeetingAccessPolicy.OPEN
        self.room.set_passcode("private-passcode")
        self.room.save()
        with self.assertRaises(GatewayDenied) as error:
            self.intent()
        self.assertNotIn("private-passcode", str(error.exception))
        request_intent(
            session_id=self.session.pk,
            profile_id=self.guest.pk,
            passcode="private-passcode",
        )
        self.room.max_participants = 2
        self.room.save()
        extra = self.profile("extra")
        with self.assertRaises(GatewayDenied):
            request_intent(
                session_id=self.session.pk,
                profile_id=extra.pk,
                passcode="private-passcode",
            )

    def test_outbox_failure_rolls_back_decision_and_authorization(self):
        self.intent()
        with (
            patch(
                "apps.meetings.services.gateway_outbox.GatewayAuthorizationOutbox.objects.get_or_create",
                side_effect=OperationalError("storage unavailable"),
            ),
            self.assertRaises(OperationalError),
        ):
            self.decision()
        self.guest_participant.refresh_from_db()
        self.assertEqual(self.guest_participant.status, ParticipantStatus.WAITING)
        self.assertEqual(self.guest_participant.authorization_epoch, 0)
        self.assertFalse(GatewayAdmissionDecision.objects.exists())
        self.assertEqual(
            MeetingJoinRequest.objects.get(pk=self.intent_id).status,
            MeetingJoinRequestStatus.PENDING,
        )

    def test_transaction_rollback_removes_outbox_and_decision(self):
        self.intent()
        with transaction.atomic():
            self.decision()
            transaction.set_rollback(True)
        self.assertFalse(GatewayAuthorizationOutbox.objects.exists())
        self.assertFalse(GatewayAdmissionDecision.objects.exists())

    def test_permission_revocation_leave_and_meeting_end_record_intents(self):
        self.intent()
        self.decision()
        Lifecycle.update_participant_permissions(
            session=self.session,
            actor_profile=self.host,
            participant=self.guest_participant,
            updates={"is_muted": True},
        )
        changed = GatewayAuthorizationOutbox.objects.get(
            participant=self.guest_participant, authorization_epoch=2
        )
        self.assertEqual(changed.action, "participant.permissions.update")
        self.assertFalse(changed.data["permissions"]["can_publish_audio"])
        Lifecycle.leave_participant(session=self.session, profile=self.guest)
        self.assertEqual(
            GatewayAuthorizationOutbox.objects.get(
                participant=self.guest_participant, authorization_epoch=3
            ).action,
            "participant.disconnect",
        )
        Lifecycle.end_session(session=self.session, actor_profile=self.host)
        self.assertTrue(
            GatewayAuthorizationOutbox.objects.filter(
                participant=self.participant, action="participant.disconnect"
            ).exists()
        )

    def test_default_flag_and_room_selection_are_required(self):
        with (
            override_settings(GATEWAY_ADMISSION_ENABLED=False),
            self.assertRaises(GatewayDenied),
        ):
            self.intent()
        self.room.feature_flags = {}
        self.room.save(update_fields=["feature_flags"])
        with self.assertRaises(GatewayDenied):
            self.intent()

    def test_http_uses_authenticated_actor_and_rejects_role_and_coerced_booleans(self):
        url = f"/api/v1/meetings/sessions/{self.session.pk}/gateway/join-intents/"
        self.client.force_authenticate(self.guest.user)
        self.assertEqual(
            self.client.post(
                url, {"role": "host"}, format="json", secure=True
            ).status_code,
            400,
        )
        result = self.client.post(url, {}, format="json", secure=True)
        self.assertEqual(result.status_code, 200)
        review = f"{url}{result.json()['join_request_id']}/decision/"
        payload = {
            "decision_id": str(uuid.uuid4()),
            "approve": True,
            "expected_epoch": 0,
        }
        self.assertEqual(
            self.client.post(review, payload, format="json", secure=True).status_code,
            403,
        )
        self.client.force_authenticate(self.host.user)
        self.assertEqual(
            self.client.post(
                review,
                {**payload, "actor_profile_id": str(self.host.pk)},
                format="json",
                secure=True,
            ).status_code,
            400,
        )
        self.assertEqual(
            self.client.post(
                review, {**payload, "approve": "false"}, format="json", secure=True
            ).status_code,
            400,
        )
        accepted = self.client.post(review, payload, format="json", secure=True)
        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(accepted["Cache-Control"], "no-store")
        self.assertEqual(
            self.client.post(review, payload, format="json", secure=True).json(),
            accepted.json(),
        )


@skipUnlessDBFeature("has_select_for_update")
class GatewayAdmissionRaceTests(GatewayAdmissionFixture, TransactionTestCase):
    race = race_support.GatewayTicketRaceTests.race

    def setUp(self):
        self.dispatch = patch("apps.meetings.services.lifecycle.dispatch_task")
        self.emitter = patch("apps.meetings.realtime.emitter.MeetingSocketEmitter")
        self.dispatch.start()
        self.emitter.start()
        self.addCleanup(self.dispatch.stop)
        self.addCleanup(self.emitter.stop)
        super().setUp()

    def test_concurrent_retries_create_one_decision_and_one_outbox(self):
        self.intent()
        key = uuid.uuid4()
        first, second = self.race(
            lambda: self.decision(decision_id=key),
            lambda: self.decision(decision_id=key),
        )
        self.assertEqual(first, second)
        self.assertNotEqual(first, "denied")
        self.assertEqual(GatewayAdmissionDecision.objects.count(), 1)
        self.assertEqual(GatewayAuthorizationOutbox.objects.count(), 1)

    def test_admission_removal_race_ends_revoked(self):
        self.intent()

        def remove():
            Lifecycle.remove_participant(
                session=self.session,
                actor_profile=self.host,
                participant=self.guest_participant,
            )
            return "removed"

        self.assertIn("removed", self.race(self.decision, remove))
        self.guest_participant.refresh_from_db()
        self.assertEqual(self.guest_participant.status, ParticipantStatus.REMOVED)
        latest = (
            GatewayAuthorizationOutbox.objects.filter(
                participant=self.guest_participant
            )
            .order_by("-authorization_epoch")
            .first()
        )
        self.assertEqual(latest.action, "participant.disconnect")
        self.assertEqual(
            latest.authorization_epoch, self.guest_participant.authorization_epoch
        )
        with self.assertRaises(GatewayDenied):
            self.issue(self.guest)
