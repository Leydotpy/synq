"""Migration compatibility for the retired ORM-local Janus callbacks.

Broka consumption in ``apps.meetings.jrtc.events`` is the authoritative
application event plane.  JRTC ``Plugin.on_event`` may only be used for local
diagnostics and lifecycle instrumentation, so this historical callback factory
performs no database writes or Socket.IO fan-out.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from apps.meetings.jrtc.ids import thaw_json

logger = logging.getLogger(__name__)


def _normalize_event_payload(event: Any) -> dict[str, Any]:
    """Return a detached diagnostic payload without assuming mutable dicts."""

    if hasattr(event, "model_dump"):
        value = event.model_dump(mode="json", by_alias=True, exclude_none=True)
    elif isinstance(event, Mapping):
        value = thaw_json(event)
    elif hasattr(event, "__dict__"):
        value = {
            key: item
            for key, item in vars(event).items()
            if not key.startswith("_")
        }
    else:
        value = {"raw": repr(event)}
    payload = value if isinstance(value, dict) else {"value": value}
    payload.setdefault("type", "janus.event")
    return payload


def plugin_callback_factory(instance: Any, field: Any, raw_id: int | None):
    """Return a process-local tracing callback for migration compatibility."""

    model_label = getattr(getattr(instance, "_meta", None), "label_lower", None)
    model_pk = getattr(instance, "pk", None)
    field_name = getattr(field, "name", None)

    def _on_event(event: Any) -> None:
        payload = _normalize_event_payload(event)
        logger.debug(
            "Local JRTC plugin event observed",
            extra={
                "model": model_label,
                "model_pk": None if model_pk is None else str(model_pk),
                "plugin_field": field_name,
                "persisted_handle_id": raw_id,
                "janus_type": payload.get("janus"),
            },
        )

    return _on_event


def dispatch_janus_event(payload: dict[str, Any]) -> None:
    """Reject the obsolete local application-event path explicitly."""

    del payload
    raise RuntimeError(
        "Direct Janus callback fan-out is retired; publish and consume through Broka."
    )


__all__ = ["dispatch_janus_event", "plugin_callback_factory"]
