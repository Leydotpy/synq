"""Strict Janus identifier helpers and explicit browser-boundary conversion."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import re
from typing import Any

from jrtc.models import JanusId


def require_janus_id(value: object, *, name: str = "Janus ID") -> JanusId:
    """Return one strict positive integer, rejecting strings and booleans."""

    if type(value) is not int or value <= 0:
        raise TypeError(f"{name} must be a positive integer")
    return value


def optional_janus_id(value: object, *, name: str = "Janus ID") -> JanusId | None:
    """Validate a nullable internal Janus identifier."""

    if value is None:
        return None
    return require_janus_id(value, name=name)


_DECIMAL_JANUS_ID = re.compile(r"[1-9][0-9]*\Z")


def janus_id_from_wire(value: object, *, name: str = "Janus ID") -> JanusId:
    """Parse one browser/persisted-JSON decimal string at an explicit boundary."""

    if not isinstance(value, str) or _DECIMAL_JANUS_ID.fullmatch(value) is None:
        raise TypeError(f"{name} must be a positive canonical decimal string")
    return require_janus_id(int(value), name=name)


def optional_janus_id_from_wire(
    value: object,
    *,
    name: str = "Janus ID",
) -> JanusId | None:
    """Parse a nullable browser/persisted-JSON identifier."""

    if value is None:
        return None
    return janus_id_from_wire(value, name=name)


def janus_id_to_wire(value: object, *, name: str = "Janus ID") -> str:
    """Convert one validated internal identifier to a decimal browser string."""

    return str(require_janus_id(value, name=name))


def optional_janus_id_to_wire(value: object, *, name: str = "Janus ID") -> str | None:
    """Convert a nullable validated identifier at a JSON/browser boundary."""

    identifier = optional_janus_id(value, name=name)
    return None if identifier is None else str(identifier)


def thaw_json(value: Any) -> Any:
    """Recursively copy Broka's immutable JSON containers into plain values."""

    if isinstance(value, Mapping):
        return {str(key): thaw_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [thaw_json(item) for item in value]
    return value


_PLUGIN_ID_KEYS = frozenset(
    {
        "feed",
        "feed_id",
        "handle_id",
        "id",
        "private_id",
        "plugin_id",
        "publisher_id",
        "room",
        "sender",
        "session_id",
        "stream_id",
    }
)


def _wire_plugin_value(value: Any, *, key: str | None = None) -> Any:
    if isinstance(value, Mapping):
        return {
            str(child_key): (
                # Application metadata and SDP/ICE documents may contain
                # unrelated fields named ``id`` or ``session_id`` (including
                # Synq UUIDs). They are opaque JSON, not Janus identifiers.
                thaw_json(child_value)
                if str(child_key) in {"metadata", "jsep", "candidate", "candidates"}
                else _wire_plugin_value(child_value, key=str(child_key))
            )
            for child_key, child_value in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if key == "source_ids":
            return [janus_id_to_wire(item, name="source ID") for item in value]
        return [_wire_plugin_value(item) for item in value]
    if key in _PLUGIN_ID_KEYS and value is not None:
        return janus_id_to_wire(value, name=key)
    if key in {"leaving", "unpublished"} and value not in (None, "ok"):
        return janus_id_to_wire(value, name=key)
    return value


def janus_event_to_wire(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Build a detached browser event with every known Janus ID stringified.

    The domain and JRTC layers continue to use integers.  Only this explicit
    application boundary converts values that may exceed JavaScript's safe
    integer range.
    """

    thawed = thaw_json(payload)
    return _wire_plugin_value(thawed)


__all__ = [
    "janus_event_to_wire",
    "janus_id_from_wire",
    "janus_id_to_wire",
    "optional_janus_id",
    "optional_janus_id_from_wire",
    "optional_janus_id_to_wire",
    "require_janus_id",
    "thaw_json",
]
