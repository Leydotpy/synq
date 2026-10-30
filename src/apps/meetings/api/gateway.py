"""Narrow HTTPS ticket and service-authority endpoints (no media payloads)."""

import json
import secrets
import uuid
from types import SimpleNamespace

from django.conf import settings
from django.db import DatabaseError
from django.utils.decorators import method_decorator
from django.views.decorators.debug import sensitive_post_parameters
from rest_framework import (
    authentication,
    exceptions,
    permissions,
    response,
    serializers,
    views,
)
from rest_framework.parsers import JSONParser

from apps.meetings.services.gateway_admission import request_intent, review_intent
from apps.meetings.services.gateway_tickets import (
    GatewayDenied,
    check_ticket,
    issue_ticket,
)
from core.api.api import CurrentProfileMixin


class StrictJSONParser(JSONParser):
    def parse(self, stream, media_type=None, parser_context=None):
        raw = stream.read(8193)
        if len(raw) > 8192:
            raise exceptions.ParseError("Invalid request.")

        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ValueError
                result[key] = value
            return result

        def constant(_):
            raise ValueError

        try:
            parsed = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
            if not isinstance(parsed, dict):
                raise ValueError
            return parsed
        except ValueError, UnicodeError, RecursionError:
            raise exceptions.ParseError("Invalid request.") from None


class StrictSerializer(serializers.Serializer):
    def to_internal_value(self, data):
        if not isinstance(data, dict) or set(data) - set(self.fields):
            raise exceptions.ValidationError({"non_field_errors": ["Invalid request."]})
        return super().to_internal_value(data)


class TicketRequest(StrictSerializer):
    purpose = serializers.ChoiceField(
        choices=["connect", "resume", "refresh"], default="connect"
    )
    participant_session_id = serializers.UUIDField(required=False)
    generation = serializers.IntegerField(
        min_value=1, max_value=2**53 - 1, required=False
    )

    def validate(self, data):
        binding = {"participant_session_id", "generation"}
        if data["purpose"] == "connect":
            if binding.intersection(data):
                raise serializers.ValidationError("Invalid request.")
        elif not binding.issubset(data):
            raise serializers.ValidationError("Invalid request.")
        return data


class AuthorityRequest(StrictSerializer):
    claims = serializers.DictField()
    operation = serializers.CharField(max_length=64)
    consume = serializers.BooleanField()


class GatewayServiceAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        configured = getattr(settings, "GATEWAY_AUTHORITY_TOKEN", "")
        supplied = authentication.get_authorization_header(request)
        if not isinstance(configured, str) or len(configured) < 32:
            raise exceptions.AuthenticationFailed("Service authentication unavailable.")
        if not secrets.compare_digest(supplied, ("Bearer " + configured).encode()):
            raise exceptions.AuthenticationFailed("Invalid service credential.")
        return SimpleNamespace(is_authenticated=True), None

    def authenticate_header(self, request):
        return "Bearer"


@method_decorator(sensitive_post_parameters(), name="dispatch")
class GatewayAPIView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]
    parser_classes = [StrictJSONParser]

    def initial(self, request, *args, **kwargs):
        self.correlation_id = str(uuid.uuid4())
        if not request.is_secure():
            raise exceptions.PermissionDenied("HTTPS required.")
        if not getattr(settings, "GATEWAY_TICKETS_ENABLED", False):
            raise exceptions.NotFound()
        super().initial(request, *args, **kwargs)

    def handle_exception(self, exc):
        if isinstance(exc, GatewayDenied):
            return response.Response({"error": {"code": exc.code}}, status=exc.status)
        if isinstance(exc, DatabaseError):
            return response.Response({"error": {"code": "UNAVAILABLE"}}, status=503)
        return super().handle_exception(exc)

    def finalize_response(self, request, result, *args, **kwargs):
        result = super().finalize_response(request, result, *args, **kwargs)
        result["Cache-Control"] = "no-store"
        result["Pragma"] = "no-cache"
        result["X-Correlation-ID"] = self.correlation_id
        return result


class GatewayTicketView(CurrentProfileMixin, GatewayAPIView):
    def post(self, request, session_id):
        payload = TicketRequest(data=request.data)
        payload.is_valid(raise_exception=True)
        return response.Response(
            issue_ticket(
                session_id=session_id,
                profile_id=self.get_profile().pk,
                **payload.validated_data,
            )
        )


class GatewayAuthorityView(GatewayAPIView):
    authentication_classes = [GatewayServiceAuthentication]

    def post(self, request):
        payload = AuthorityRequest(data=request.data)
        payload.is_valid(raise_exception=True)
        return response.Response(check_ticket(**payload.validated_data))


class JoinIntentRequest(StrictSerializer):
    display_name = serializers.CharField(
        max_length=255, allow_blank=True, required=False
    )
    passcode = serializers.CharField(max_length=128, required=False, write_only=True)
    invite_token = serializers.CharField(
        max_length=2048, required=False, write_only=True
    )


class AdmissionDecisionRequest(StrictSerializer):
    decision_id = serializers.UUIDField()
    approve = serializers.BooleanField()
    expected_epoch = serializers.IntegerField(min_value=0, max_value=2**53 - 1)

    def to_internal_value(self, data):
        if (
            not isinstance(data, dict)
            or type(data.get("approve")) is not bool
            or type(data.get("expected_epoch")) is not int
        ):
            raise exceptions.ValidationError({"non_field_errors": ["Invalid request."]})
        return super().to_internal_value(data)


class GatewayJoinIntentView(CurrentProfileMixin, GatewayAPIView):
    def post(self, request, session_id):
        payload = JoinIntentRequest(data=request.data)
        payload.is_valid(raise_exception=True)
        return response.Response(
            request_intent(
                session_id=session_id,
                profile_id=self.get_profile().pk,
                **payload.validated_data,
            )
        )


class GatewayAdmissionDecisionView(CurrentProfileMixin, GatewayAPIView):
    def post(self, request, session_id, join_request_id):
        payload = AdmissionDecisionRequest(data=request.data)
        payload.is_valid(raise_exception=True)
        return response.Response(
            review_intent(
                session_id=session_id,
                join_request_id=join_request_id,
                actor_profile_id=self.get_profile().pk,
                **payload.validated_data,
            )
        )


class CommandAuthorizationRequest(StrictSerializer):
    command = serializers.DictField()


class GatewayCommandAuthorityView(GatewayAPIView):
    authentication_classes = [GatewayServiceAuthentication]

    def post(self, request):
        from apps.meetings.services.gateway_commands import authorize_command

        if not settings.GATEWAY_COMMANDS_ENABLED:
            raise GatewayDenied("UNAVAILABLE", 404)
        payload = CommandAuthorizationRequest(data=request.data)
        payload.is_valid(raise_exception=True)
        return response.Response(
            {"authorized": authorize_command(payload.validated_data["command"])}
        )


class GatewayCommandStatusView(CurrentProfileMixin, GatewayAPIView):
    def get(self, request, session_id, request_id):
        from apps.meetings.models import GatewayAuthorizationOutbox, GatewayCommand
        from apps.meetings.services.gateway_outbox import enabled

        profile = self.get_profile()
        row = (
            GatewayAuthorizationOutbox.objects.select_related(
                "session__room", "participant"
            )
            .filter(pk=request_id, session_id=session_id)
            .first()
        )
        if row is None or not enabled(row.session):
            raise GatewayDenied("UNAVAILABLE", 404)
        if not profile.is_active or not profile.user.is_active:
            raise GatewayDenied()
        own = row.participant is not None and row.participant.profile_id == profile.pk
        moderator = row.session.room.memberships.filter(
            profile=profile, is_active=True, can_manage_waiting_room=True
        ).exists()
        if not own and not moderator:
            raise GatewayDenied()
        command = GatewayCommand.objects.filter(outbox=row).first()
        return response.Response(
            {
                "request_id": str(row.pk),
                "command_id": str(command.pk) if command else None,
                "action": row.action,
                "authorization_epoch": row.authorization_epoch,
                "enforcement_status": row.status,
                "reason_code": row.last_error or None,
                "delivery_attempts": row.delivery_attempts,
            }
        )
