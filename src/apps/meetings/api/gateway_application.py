"""Authenticated application commands for the gateway cohort; never SDP/ICE."""

from django.db import transaction
from rest_framework import exceptions, response, serializers

from core.api.api import CurrentProfileMixin
from apps.meetings.api.gateway import GatewayAPIView, StrictSerializer
from apps.meetings.api.gateway_state import credentials
from apps.meetings.services.gateway_state import ADMITTED, _audience, _session
from apps.meetings.services.gateway_tickets import GatewayDenied
from apps.meetings.services.lifecycle import MeetingLifecycleService as Lifecycle
from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.services.state import MeetingStateBuilder as State


class ChatRequest(StrictSerializer):
    body = serializers.CharField(max_length=4000)
    metadata = serializers.DictField(required=False)


class ReactionRequest(StrictSerializer):
    reaction = serializers.CharField(max_length=64)
    metadata = serializers.DictField(required=False)
    expires_in_seconds = serializers.IntegerField(min_value=1, max_value=300, default=8)


class LeaveRequest(StrictSerializer):
    reason = serializers.CharField(max_length=256, allow_blank=True, default="")


class GatewayApplicationView(CurrentProfileMixin, GatewayAPIView):
    @transaction.atomic
    def post(self, request, session_id, operation):
        if request.auth:
            credentials(request)
        serializer_class = {
            "chat": ChatRequest,
            "reactions": ReactionRequest,
            "leave": LeaveRequest,
        }[operation]
        payload = serializer_class(data=request.data)
        payload.is_valid(raise_exception=True)
        session = _session(session_id)
        profile = self.get_profile()
        participant, _ = _audience(session, profile.pk)
        if operation != "leave" and participant.status not in ADMITTED:
            raise GatewayDenied()
        try:
            if operation == "leave":
                Lifecycle.leave_participant(
                    session=session, profile=profile, **payload.validated_data
                )
                return response.Response(status=204)
            if operation == "chat":
                value = Lifecycle.record_chat_message(
                    session=session, participant=participant, **payload.validated_data
                )
                result = State.serialize_message(value)
            else:
                value = Lifecycle.record_reaction(
                    session=session, participant=participant, **payload.validated_data
                )
                result = State.serialize_reaction(value)
        except MeetingDomainError as exc:
            raise exceptions.ValidationError(str(exc)) from exc
        result["event_context"] = {"kind": "snapshot", "session_id": str(session.pk)}
        return response.Response(result, status=201)
