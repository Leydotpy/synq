"""Compatibility model bases used by independently installed plugin packages."""

from pydantic import BaseModel, ConfigDict, Field

from jrtc.models.common import JanusJsep, StrictBaseModel


class Jsep(JanusJsep):
    """Public name for a Janus JSEP description."""


class PluginMessageBase(StrictBaseModel):
    """Base for a validated external plugin request body."""

    request: str


class PluginResponseBase(BaseModel):
    """Forward-compatible base for external plugin response bodies."""

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        validate_assignment=True,
    )

    kind: str = Field(default="", exclude=True)
