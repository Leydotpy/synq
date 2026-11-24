from django.contrib.auth import get_user_model
from django.test import TestCase
from rest_framework import status
from rest_framework.test import APIClient
from apps.meetings.models import MeetingLifecycleState
from apps.meetings.services.lifecycle import MeetingLifecycleService


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
