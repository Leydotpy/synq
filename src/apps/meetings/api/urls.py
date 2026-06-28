"""URL routes for the meeting HTTP API."""

from django.urls import path

from apps.meetings.api.views import (
    MeetingAdmissionView,
    MeetingJoinRequestCreateView,
    MeetingJoinRequestReviewView,
    MeetingParticipantDetailView,
    MeetingRoomDetailView,
    MeetingServiceSessionCreateView,
    MeetingRoomListCreateView,
    MeetingSessionCreateView,
    MeetingSessionShareView,
    MeetingSessionStartView,
    MeetingSessionStateView,
)

app_name = "meetings-api"

urlpatterns = [
    path("internal/service-sessions/", MeetingServiceSessionCreateView.as_view(), name="service-session-create"),
    path("rooms/", MeetingRoomListCreateView.as_view(), name="room-list"),
    path("rooms/<slug:slug>/", MeetingRoomDetailView.as_view(), name="room-detail"),
    path("rooms/<slug:slug>/sessions/", MeetingSessionStartView.as_view(), name="session-start"),
    path("sessions/", MeetingSessionCreateView.as_view(), name="session-create"),
    path("sessions/<uuid:session_id>/state/", MeetingSessionStateView.as_view(), name="session-state"),
    path("sessions/<uuid:session_id>/admission/", MeetingAdmissionView.as_view(), name="session-admission"),
    path("sessions/<uuid:session_id>/share/", MeetingSessionShareView.as_view(), name="session-share"),
    path("sessions/<uuid:session_id>/join-requests/", MeetingJoinRequestCreateView.as_view(), name="join-request-create"),
    path(
        "sessions/<uuid:session_id>/join-requests/<uuid:join_request_id>/review/",
        MeetingJoinRequestReviewView.as_view(),
        name="join-request-review",
    ),
    path(
        "sessions/<uuid:session_id>/participants/<uuid:participant_id>/",
        MeetingParticipantDetailView.as_view(),
        name="participant-detail",
    ),
]
