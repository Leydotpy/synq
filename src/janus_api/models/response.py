"""Typed, plugin-agnostic inbound Janus protocol envelopes."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, TypeAdapter, ValidationError

from janus_api.core.exceptions import JanusProtocolError
from janus_api.models.base import Jsep
from janus_api.models.common import JanusId, LooseBaseModel


class JanusError(LooseBaseModel):
    code: int
    reason: str = Field(min_length=1)


class JanusBaseResponse(LooseBaseModel):
    janus: str
    transaction: str | None = None
    session_id: JanusId | None = None
    sender: JanusId | None = None

    kind: str = Field(default="", exclude=True)


class SuccessData(LooseBaseModel):
    id: JanusId | None = None


class SuccessResponse(JanusBaseResponse):
    janus: Literal["success"]
    data: SuccessData | None = None


class ErrorResponse(JanusBaseResponse):
    janus: Literal["error"]
    error: JanusError


class KeepAliveResponse(JanusBaseResponse):
    janus: Literal["keepalive"]


class AckResponse(JanusBaseResponse):
    janus: Literal["ack"]


class PongResponse(JanusBaseResponse):
    janus: Literal["pong"]


class PluginData(LooseBaseModel):
    """Opaque plugin payload; external packages own its schema."""

    plugin: str = Field(min_length=1)
    data: dict[str, Any]


class EventResponse(JanusBaseResponse):
    janus: Literal["event"]
    sender: JanusId
    plugindata: PluginData
    jsep: Jsep | None = None


class WebRTCUpResponse(JanusBaseResponse):
    janus: Literal["webrtcup"]
    sender: JanusId


class MediaEventResponse(JanusBaseResponse):
    janus: Literal["media"]
    sender: JanusId
    type: Literal["audio", "video", "data"]
    receiving: bool
    mid: str | None = None


class SlowLinkResponse(JanusBaseResponse):
    janus: Literal["slowlink"]
    sender: JanusId
    uplink: bool
    lost: int = Field(ge=0)
    mid: str | None = None


class HangupResponse(JanusBaseResponse):
    janus: Literal["hangup"]
    sender: JanusId
    reason: str | None = None


class DetachedResponse(JanusBaseResponse):
    janus: Literal["detached"]
    sender: JanusId


class TimeoutResponse(JanusBaseResponse):
    janus: Literal["timeout"]


class TrickleResponse(JanusBaseResponse):
    janus: Literal["trickle"]
    sender: JanusId
    candidate: dict[str, Any]


class TransportPluginInfo(LooseBaseModel):
    name: str
    author: str | None = None
    description: str | None = None
    version_string: str | None = None
    version: int | None = None


class PluginInfo(TransportPluginInfo):
    pass


class InfoResponse(JanusBaseResponse):
    janus: Literal["server_info"]
    name: str
    version_string: str
    version: int
    author: str
    data_channels: bool
    ipv6: bool
    ice_tcp: bool = Field(alias="ice-tcp")
    transports: dict[str, TransportPluginInfo]
    plugins: dict[str, PluginInfo]


type KnownJanusResponse = (
    SuccessResponse
    | ErrorResponse
    | KeepAliveResponse
    | AckResponse
    | PongResponse
    | EventResponse
    | WebRTCUpResponse
    | MediaEventResponse
    | SlowLinkResponse
    | HangupResponse
    | DetachedResponse
    | TimeoutResponse
    | TrickleResponse
    | InfoResponse
)
type JanusResponse = KnownJanusResponse | JanusBaseResponse
type WebRTCEvent = (
    EventResponse
    | WebRTCUpResponse
    | MediaEventResponse
    | SlowLinkResponse
    | HangupResponse
    | DetachedResponse
    | TimeoutResponse
    | TrickleResponse
)

_RESPONSE_MODELS: dict[str, type[JanusBaseResponse]] = {
    "success": SuccessResponse,
    "error": ErrorResponse,
    "keepalive": KeepAliveResponse,
    "ack": AckResponse,
    "pong": PongResponse,
    "event": EventResponse,
    "webrtcup": WebRTCUpResponse,
    "media": MediaEventResponse,
    "slowlink": SlowLinkResponse,
    "hangup": HangupResponse,
    "detached": DetachedResponse,
    "timeout": TimeoutResponse,
    "trickle": TrickleResponse,
    "server_info": InfoResponse,
}


def parse_janus_response(payload: Any) -> JanusResponse:
    """Validate a Janus response while retaining unknown future event types."""

    if not isinstance(payload, dict):
        raise JanusProtocolError("Janus response must be a JSON object")
    janus_type = payload.get("janus")
    if not isinstance(janus_type, str) or not janus_type:
        raise JanusProtocolError("Janus response is missing a string 'janus' field")
    model = _RESPONSE_MODELS.get(janus_type, JanusBaseResponse)
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        raise JanusProtocolError(f"Invalid {janus_type!r} Janus response") from exc


# Kept for callers that need a reusable Pydantic adapter for the generic shape.
JanusResponseAdapter = TypeAdapter(JanusBaseResponse)
