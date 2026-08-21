"""Validated, mutable representations of JRTC broker events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from broka import Delivery
from jrtc.messaging import JANUS_EVENT_ROUTES

from apps.meetings.jrtc.ids import require_janus_id, thaw_json


LOGICAL_JANUS_EVENT_TYPES = frozenset(JANUS_EVENT_ROUTES.values())


@dataclass(frozen=True, slots=True)
class JanusBrokerEvent:
    """One validated JRTC event detached from Broka's immutable containers."""

    event_id: UUID
    event_type: str
    janus_type: str
    payload: dict[str, Any]
    session_id: int
    sender: int | None
    delivery_attempt: int

    @property
    def suppress_browser_jsep(self) -> bool:
        """Whether the command plane already owns this JSEP negotiation."""

        return bool(self.payload.get("transaction")) and isinstance(
            self.payload.get("jsep"),
            dict,
        )


def event_from_delivery(delivery: Delivery[Any]) -> JanusBrokerEvent:
    """Validate a Broka delivery using its logical envelope, never its route.

    JRTC publishes all supported logical event types to one physical
    destination.  ``delivery.route`` is therefore intentionally ignored for
    event selection.
    """

    envelope = delivery.envelope
    event_type = envelope.type
    if event_type not in LOGICAL_JANUS_EVENT_TYPES:
        raise ValueError(f"Unsupported JRTC logical event type {event_type!r}.")

    payload = thaw_json(envelope.payload)
    if not isinstance(payload, dict):
        raise TypeError("A JRTC event envelope payload must be a mapping.")

    janus_type = payload.get("janus")
    if not isinstance(janus_type, str):
        raise TypeError("A JRTC event payload must contain a string 'janus' type.")
    expected_event_type = JANUS_EVENT_ROUTES.get(janus_type)
    if expected_event_type != event_type:
        raise ValueError(
            "JRTC envelope type does not match its Janus payload type: "
            f"{event_type!r} != {expected_event_type!r}."
        )

    session_id = require_janus_id(
        payload.get("session_id"),
        name="Janus session_id",
    )
    raw_sender = payload.get("sender")
    if raw_sender is None and janus_type == "timeout":
        sender = None
    else:
        sender = require_janus_id(raw_sender, name="Janus sender")

    attempt = delivery.attempt
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise TypeError("Broka delivery attempt must be a positive integer.")

    try:
        event_id = UUID(str(envelope.id))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("JRTC broker envelope ID must be a UUID.") from exc

    return JanusBrokerEvent(
        event_id=event_id,
        event_type=event_type,
        janus_type=janus_type,
        payload=payload,
        session_id=session_id,
        sender=sender,
        delivery_attempt=attempt,
    )


__all__ = [
    "JanusBrokerEvent",
    "LOGICAL_JANUS_EVENT_TYPES",
    "event_from_delivery",
]
