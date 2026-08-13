"""Small plugin-agnostic helpers retained by the public runtime."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from janus_api.models import JanusResponse

DEFAULT_UUID_LENGTH = 6


def extract_plugin_data(response: JanusResponse) -> dict[str, Any] | None:
    """Return an opaque plugin payload when the response contains one."""

    plugindata = getattr(response, "plugindata", None)
    data = getattr(plugindata, "data", None)
    return data if isinstance(data, dict) else None


def extract_response_id(response: JanusResponse) -> str | int | None:
    data = getattr(response, "data", None)
    return getattr(data, "id", None)


def generate_short_uuid(length: int = DEFAULT_UUID_LENGTH) -> str:
    if length < 1 or length > 32:
        raise ValueError("length must be between 1 and 32")
    return uuid4().hex[:length]
