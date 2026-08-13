"""Plugin-agnostic Pydantic primitives shared by core and plugin packages."""

from __future__ import annotations

from typing import Annotated, Any, Literal
from uuid import uuid4

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field


def _validate_janus_id(value: Any) -> int | str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("Janus IDs must be positive integers or non-empty strings")
    if isinstance(value, int) and value <= 0:
        raise ValueError("numeric Janus IDs must be positive")
    if isinstance(value, str) and (not value or value.strip() != value):
        raise ValueError("string Janus IDs must be non-empty and contain no outer whitespace")
    return value


type JanusId = Annotated[int | str, BeforeValidator(_validate_janus_id)]
type JsonObject = dict[str, Any]
type HeadersMap = dict[str, str]
type StringList = list[str]


class StrictBaseModel(BaseModel):
    """Base for documented outbound payloads that reject unknown fields."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        validate_assignment=True,
    )


class LooseBaseModel(BaseModel):
    """Forward-compatible base for inbound or partially documented payloads."""

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        validate_assignment=True,
    )


class JanusJsep(LooseBaseModel):
    """A JSEP session description carried outside a plugin message body.

    Janus occasionally adds negotiation hints to this object.  Inbound unknown
    keys are therefore retained, while the universally required ``type`` and
    ``sdp`` fields remain validated.
    """

    type: Literal["offer", "answer", "pranswer", "rollback"]
    sdp: str
    trickle: bool | None = None
    restart: bool | None = None


class PluginErrorResponse(LooseBaseModel):
    """Common plugin-level error shape used by external plugin packages."""

    error_code: int
    error: str


class TransactionalDataMessage(StrictBaseModel):
    """Base for DataChannel messages whose transaction is echoed by a plugin."""

    transaction: str = Field(default_factory=lambda: str(uuid4()))
