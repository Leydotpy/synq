"""Immutable cohort selection and authenticated, fenced gateway owner leases."""

import time
import uuid
from datetime import timedelta

from django.conf import settings
from django.db import transaction
from django.utils import timezone
from jrtc_signalling.bus import BusConfig, BusError
from jrtc_signalling.ownership import LEASE_SECONDS

from apps.meetings.exceptions import JanusGatewayError
from apps.meetings.models import (
    GatewayCommandRoute,
    GatewayOwnerGeneration,
    MeetingSession,
)
from apps.meetings.services.gateway_tickets import GatewayDenied


def selected_backend(room):
    flags = room.feature_flags if isinstance(room.feature_flags, dict) else {}
    selected = (
        getattr(settings, "GATEWAY_RUNTIME_ENABLED", False)
        and getattr(settings, "GATEWAY_ADMISSION_ENABLED", False)
        and getattr(settings, "GATEWAY_COMMANDS_ENABLED", False)
        and flags.get("gateway_control_plane") is True
        and flags.get("gateway_runtime") is True
    )
    return "gateway" if selected else "legacy"


def require_legacy(session):
    if session.signalling_backend != "legacy":
        raise JanusGatewayError("This meeting is owned by the signalling gateway.")


def require_legacy_key(session_id):
    if (
        session_id is not None
        and MeetingSession.objects.filter(
            pk=session_id, signalling_backend="gateway"
        ).exists()
    ):
        raise JanusGatewayError("This meeting is owned by the signalling gateway.")


def ownership_request(packet, *, config=None, now=None):
    try:
        config = config or BusConfig.read(
            settings.GATEWAY_BUS_CONFIG_FILE, gateway=False
        )
        owner, request = config.verifier.verify(packet, "owner-request")
        if (
            owner == "django-control"
            or not -5 <= time.time() - request["issued_at"] <= 10
        ):
            raise BusError("Expired ownership request.")
    except BusError, ValueError, OSError:
        raise GatewayDenied("FORBIDDEN", 403) from None
    if request["action"] != "release" and not getattr(
        settings, "GATEWAY_RUNTIME_ENABLED", False
    ):
        raise GatewayDenied("UNAVAILABLE", 503)
    now = now or timezone.now()
    incarnation = uuid.UUID(request["incarnation"])
    with transaction.atomic():
        session = (
            MeetingSession.objects.select_for_update(of=("self",))
            .select_related("room")
            .get(pk=request["meeting_id"])
        )
        if session.signalling_backend != "gateway":
            raise GatewayDenied("FORBIDDEN", 403)
        action = request["action"]
        if action != "release" and (
            selected_backend(session.room) != "gateway" or session.ended_at is not None
        ):
            raise GatewayDenied("FORBIDDEN", 403)
        route = (
            GatewayCommandRoute.objects.select_for_update()
            .filter(session=session)
            .first()
        )
        matches = (
            route is not None
            and route.owner_id == owner
            and route.owner_incarnation == incarnation
        )
        if action == "claim":
            if request["generation"] != 0:
                raise GatewayDenied("CONFLICT", 409)
            if route is None:
                route = GatewayCommandRoute.objects.create(
                    session=session,
                    owner_id=owner,
                    owner_incarnation=incarnation,
                    owner_generation=1,
                    lease_expires_at=now + timedelta(seconds=LEASE_SECONDS),
                )
                GatewayOwnerGeneration.objects.create(
                    session=session,
                    incarnation=incarnation,
                    owner_id=owner,
                    generation=1,
                )
            elif matches and not route.released and route.lease_expires_at > now:
                pass  # Lost response retry; don't extend its original lease.
            elif route.released and not matches:
                if GatewayOwnerGeneration.objects.filter(
                    session=session, incarnation=incarnation
                ).exists():
                    raise GatewayDenied("CONFLICT", 409)
                route.owner_id, route.owner_incarnation = owner, incarnation
                route.owner_generation += 1
                GatewayOwnerGeneration.objects.create(
                    session=session,
                    incarnation=incarnation,
                    owner_id=owner,
                    generation=route.owner_generation,
                )
                route.released = False
                route.lease_expires_at = now + timedelta(seconds=LEASE_SECONDS)
                route.save()
            else:
                # Expiry is not proof that remote Janus handles were destroyed.
                # SG-15 adds reconciled takeover; SG-07 fails closed.
                raise GatewayDenied("CONFLICT", 409)
        else:
            if not matches or request["generation"] != route.owner_generation:
                raise GatewayDenied("CONFLICT", 409)
            if action == "renew":
                if route.released or route.lease_expires_at <= now:
                    raise GatewayDenied("CONFLICT", 409)
                route.lease_expires_at = now + timedelta(seconds=LEASE_SECONDS)
            else:
                route.released = True
                route.lease_expires_at = now
            route.save(update_fields=["released", "lease_expires_at", "updated_at"])
        return {
            "request_id": request["request_id"],
            "meeting_id": str(session.pk),
            "owner_id": route.owner_id,
            "incarnation": str(route.owner_incarnation),
            "generation": route.owner_generation,
            "lease_seconds": max(0, (route.lease_expires_at - now).total_seconds()),
        }
