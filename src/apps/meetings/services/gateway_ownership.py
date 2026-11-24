"""Immutable cohort selection and authenticated, fenced gateway owner leases."""

import time
import uuid
from datetime import timedelta

from apps.meetings.exceptions import MeetingDomainError
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from jrtc_signalling.bus import BusConfig, BusError
from jrtc_signalling.ownership import LEASE_SECONDS

from apps.meetings.models import (
    GatewayCommandRoute,
    GatewayOwnerGeneration,
    MeetingSession,
)
from apps.meetings.services.gateway_tickets import GatewayDenied


def require_gateway_start(room):
    """A paused room or an incomplete gateway stack cannot start a new meeting."""
    if room.gateway_canary is False or (
        settings.GATEWAY_STAGED_ROUTING and room.gateway_canary is not True
    ):
        raise MeetingDomainError("Gateway admission is paused for this room.")
    if not all(
        (
            settings.GATEWAY_TICKETS_ENABLED,
            settings.GATEWAY_ADMISSION_ENABLED,
            settings.GATEWAY_COMMANDS_ENABLED,
            settings.GATEWAY_RUNTIME_ENABLED,
            settings.GATEWAY_EVENTS_ENABLED,
            settings.GATEWAY_STATE_ENABLED,
        )
    ):
        raise MeetingDomainError("The signalling gateway is unavailable.")


def _lease_result(request, route, now):
    return {
        "request_id": request["request_id"],
        "meeting_id": request["meeting_id"],
        "owner_id": route.owner_id,
        "incarnation": str(route.owner_incarnation),
        "generation": route.owner_generation,
        "lease_seconds": max(0, (route.lease_expires_at - now).total_seconds()),
    }


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
    incarnation = uuid.UUID(request["incarnation"])
    with transaction.atomic():
        session = MeetingSession.objects.select_for_update().get(
            pk=request["meeting_id"]
        )
        if session.signalling_backend != "gateway":
            raise GatewayDenied("FORBIDDEN", 403)
        action = request["action"]
        ended = session.ended_at is not None or session.lifecycle_state in {
            "ended",
            "failed",
        }
        # Every gateway session can have a terminal command delayed by the broker.
        # The existing holder may read its remaining time to finish disposal.
        finishing = ended and action == "renew"
        if action != "release" and (
            (ended and not finishing)
            or not settings.GATEWAY_ADMISSION_ENABLED
            or not settings.GATEWAY_COMMANDS_ENABLED
        ):
            raise GatewayDenied("FORBIDDEN", 403)
        route = (
            GatewayCommandRoute.objects.select_for_update()
            .filter(session=session)
            .first()
        )
        # Observe time after taking the serialization lock. Lock contention must
        # never extend a stale lease or make a live one appear expired.
        now = now or timezone.now()
        matches = (
            route is not None
            and route.owner_id == owner
            and route.owner_incarnation == incarnation
        )
        if action in {"inspect", "elect"}:
            if not getattr(settings, "GATEWAY_MULTIWORKER_ENABLED", False):
                raise GatewayDenied("UNAVAILABLE", 503)
            if (
                action == "elect"
                and matches
                and not route.released
                and route.lease_expires_at > now
                and route.janus_pool == request["pool"]
                and route.janus_session_id == request["session_id"]
                and route.owner_generation == request["generation"] + 1
            ):
                # A lost reply can be retried without extending the committed lease.
                return _lease_result(request, route, now)
            previous = route.owner_generation if route else 0
            if route and (
                (route.janus_pool and route.janus_pool != request["pool"])
                or (
                    not route.released
                    and (not route.janus_pool or route.lease_expires_at > now)
                )
            ):
                raise GatewayDenied("CONFLICT", 409)
            if GatewayOwnerGeneration.objects.filter(
                session=session, incarnation=incarnation
            ).exists():
                raise GatewayDenied("CONFLICT", 409)
            if action == "inspect":
                if request["generation"] != 0:
                    raise GatewayDenied("CONFLICT", 409)
                return {
                    "request_id": request["request_id"],
                    "meeting_id": str(session.pk),
                    "owner_id": owner,
                    "incarnation": str(incarnation),
                    "generation": previous,
                    "previous_session": route.janus_session_id if route else None,
                }
            if previous != request["generation"] or previous >= 2**53 - 1:
                raise GatewayDenied("CONFLICT", 409)
            # The trusted elector has confirmed disposal of exactly the inspected
            # generation. Persist the new session before it can create any handles.
            route, _ = GatewayCommandRoute.objects.update_or_create(
                session=session,
                defaults={
                    "owner_id": owner,
                    "owner_incarnation": incarnation,
                    "owner_generation": previous + 1,
                    "janus_pool": request["pool"],
                    "janus_session_id": request["session_id"],
                    "released": False,
                    "lease_expires_at": now + timedelta(seconds=LEASE_SECONDS),
                },
            )
            GatewayOwnerGeneration.objects.create(
                session=session,
                incarnation=incarnation,
                owner_id=owner,
                generation=previous + 1,
            )
        elif action == "claim":
            if route and route.janus_pool:
                raise GatewayDenied("CONFLICT", 409)
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
                # The legacy protocol has no physical recovery evidence.
                raise GatewayDenied("CONFLICT", 409)
        else:
            if not matches or request["generation"] != route.owner_generation:
                raise GatewayDenied("CONFLICT", 409)
            if action == "renew":
                if route.janus_pool and not getattr(
                    settings, "GATEWAY_MULTIWORKER_ENABLED", False
                ):
                    raise GatewayDenied("UNAVAILABLE", 503)
                if route.released or route.lease_expires_at <= now:
                    raise GatewayDenied("CONFLICT", 409)
                if finishing:
                    return _lease_result(request, route, now)
                route.lease_expires_at = now + timedelta(seconds=LEASE_SECONDS)
            else:
                route.released = True
                route.lease_expires_at = now
            route.save(update_fields=["released", "lease_expires_at", "updated_at"])
        return _lease_result(request, route, now)
