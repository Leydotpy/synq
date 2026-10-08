"""Strict, side-effect-free browser ICE boundary (32 candidates / 64 KiB)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from jrtc.models.request import TrickleCandidate
from pydantic import ValidationError

from .errors import VideoRoomProtocolError

MAX_ICE_CANDIDATES = 32
MAX_ICE_PAYLOAD_BYTES = 64 * 1024


@dataclass(frozen=True, slots=True)
class IceBatch:
    candidates: tuple[TrickleCandidate, ...]
    completed: bool


def normalize_ice_request(payload: dict[str, Any]) -> IceBatch:
    """Reject the whole request before authentication queries or resource writes.

    Legacy singular input is normalized here only. usernameFragment is validated
    but not sent: the generic JRTC candidate model has no such wire field.
    """
    if not isinstance(payload, dict):
        raise VideoRoomProtocolError("ICE payload must be an object.")
    completed = payload.get("completed", False)
    if type(completed) is not bool:
        raise VideoRoomProtocolError("ICE completed must be a boolean.")
    if "candidate" in payload and "candidates" in payload:
        raise VideoRoomProtocolError("Use either candidate or candidates, not both.")
    candidates = payload.get("candidates", [payload["candidate"]] if "candidate" in payload else [])
    if not isinstance(candidates, list) or len(candidates) > MAX_ICE_CANDIDATES:
        raise VideoRoomProtocolError("ICE candidates must be an array of at most 32 entries.")
    if not candidates and not completed:
        raise VideoRoomProtocolError("An empty ICE batch requires completed=true.")
    try:
        size = len(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"))
    except (TypeError, ValueError, UnicodeError) as exc:
        raise VideoRoomProtocolError("ICE payload must contain valid JSON values.") from exc
    if size > MAX_ICE_PAYLOAD_BYTES:
        raise VideoRoomProtocolError("ICE payload exceeds 64 KiB.")
    result = []
    for candidate in candidates:
        if not isinstance(candidate, dict) or set(candidate) - {"candidate", "sdpMid", "sdpMLineIndex", "usernameFragment"}:
            raise VideoRoomProtocolError("ICE candidate has unsupported fields.")
        fragment = candidate.get("usernameFragment")
        if fragment is not None and not isinstance(fragment, str):
            raise VideoRoomProtocolError("ICE usernameFragment must be a string or null.")
        try:
            result.append(TrickleCandidate.model_validate({key: value for key, value in candidate.items() if key != "usernameFragment"}))
        except ValidationError as exc:
            # Do not expose pydantic's input repr (candidate/network text).
            raise VideoRoomProtocolError("Malformed ICE candidate.") from None
    return IceBatch(tuple(result), completed)
