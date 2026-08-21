"""Historical migration compatibility for the retired Janus plugin field.

Active models use ordinary ``PositiveBigIntegerField`` columns and resolve live
JRTC plugins through ``JrtcHandleRegistry``.  This module remains importable so
the existing migration graph can be replayed; it deliberately never constructs
or adopts a live plugin from a persisted handle identifier.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, TypeVar, cast

from django.core.exceptions import ValidationError
from django.db import models

DEFAULT_PLUGIN_CLASS = "jrtc.Plugin"
LEGACY_PLUGIN_IDENTIFIERS = {
    "publisher": "videoroom",
    "subscriber": "videoroom",
}

PluginT = TypeVar("PluginT")


class JanusPluginIdentifier(str, Enum):
    """Legacy identifiers retained for migration serialization."""

    PUBLISHER = "publisher"
    SUBSCRIBER = "subscriber"
    TEXTROOM = "textroom"
    STREAMING = "streaming"
    AUDIOBRIDGE = "audiobridge"
    SIP = "sip"
    NOSIP = "nosip"
    ECHOTEST = "echotest"
    VIDEOCALL = "videocall"


def _dotted_path(value: object | None, *, label: str) -> str | None:
    """Return a stable dotted path without importing the referenced object."""

    if value is None or isinstance(value, str):
        return value
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if not module or not qualname or "<locals>" in qualname:
        raise TypeError(
            f"{label} must be a dotted import path or top-level importable object; "
            f"got {value!r}.",
        )
    return f"{module}.{qualname}"


@dataclass(slots=True)
class BoundPluginHandle(Generic[PluginT]):
    """Read-only compatibility wrapper; it cannot materialize a live plugin."""

    instance: models.Model
    field: "JanusPluginField[PluginT]"
    raw_id: int | None = None
    session: object | None = None
    _plugin: PluginT | None = None

    @property
    def id(self) -> int | None:
        return self.field.extract_plugin_id(self._plugin) if self._plugin is not None else self.raw_id

    @property
    def plugin(self) -> PluginT:
        raise RuntimeError(
            "JanusPluginField no longer materializes live handles; use the "
            "process-local JRTC handle registry.",
        )


class JanusPluginField(models.PositiveBigIntegerField, Generic[PluginT]):
    """Migration-only numeric field compatible with historical constructor args."""

    description = "Legacy Janus plugin handle identifier"

    def __init__(
        self,
        *args: Any,
        identifier: str | JanusPluginIdentifier,
        plugin_class: object = DEFAULT_PLUGIN_CLASS,
        plugin_attr: str | None = None,
        callback_factory: object | None = None,
        janus_getter: object | None = None,
        identifier_getter: object | None = None,
        plugin_kwargs_factory: object | None = None,
        **kwargs: Any,
    ) -> None:
        identifier_value = identifier.value if isinstance(identifier, Enum) else str(identifier)
        if not identifier_value:
            raise TypeError("'identifier' is required for JanusPluginField.")

        self.identifier = identifier_value
        self._plugin_class_ref = plugin_class
        self._declared_plugin_attr = plugin_attr
        self.plugin_attr = plugin_attr
        self._callback_factory_ref = callback_factory
        self._janus_getter_ref = janus_getter
        self._identifier_getter_ref = identifier_getter
        self._plugin_kwargs_factory_ref = plugin_kwargs_factory
        super().__init__(*args, **kwargs)

    @staticmethod
    def extract_plugin_id(plugin: object | None) -> int | None:
        if plugin is None:
            return None
        try:
            value = getattr(plugin, "id", None)
        except RuntimeError:
            return None
        return JanusPluginField._validate_id(value)

    @staticmethod
    def _validate_id(value: object) -> int | None:
        if value is None:
            return None
        if type(value) is not int or value <= 0:
            raise ValidationError("Janus handle identifiers must be positive integers or None.")
        return value

    def normalize_raw_id(self, value: object) -> int | None:
        if isinstance(value, BoundPluginHandle):
            value = value.id
        elif value is not None and type(value) is not int:
            value = self.extract_plugin_id(value)
        return self._validate_id(value)

    def from_db_value(self, value: Any, expression: Any, connection: Any) -> int | None:
        return self._validate_id(value)

    def to_python(self, value: Any) -> int | None:
        return self.normalize_raw_id(value)

    def get_prep_value(self, value: Any) -> int | None:
        normalized = self.normalize_raw_id(value)
        if normalized is None:
            return None
        return cast(int, super().get_prep_value(normalized))

    def get_stored_value(self, instance: models.Model) -> int | None:
        return self.to_python(instance.__dict__.get(self.attname))

    def resolve_identifier(self, instance: models.Model) -> str:
        return LEGACY_PLUGIN_IDENTIFIERS.get(self.identifier, self.identifier)

    def deconstruct(self) -> tuple[str, str, list[Any], dict[str, Any]]:
        name, path, args, kwargs = super().deconstruct()
        kwargs["identifier"] = self.identifier
        kwargs["plugin_class"] = _dotted_path(self._plugin_class_ref, label="plugin_class")
        if self._declared_plugin_attr is not None:
            kwargs["plugin_attr"] = self._declared_plugin_attr
        for key, value in (
            ("callback_factory", self._callback_factory_ref),
            ("janus_getter", self._janus_getter_ref),
            ("identifier_getter", self._identifier_getter_ref),
            ("plugin_kwargs_factory", self._plugin_kwargs_factory_ref),
        ):
            dotted = _dotted_path(value, label=key)
            if dotted is not None:
                kwargs[key] = dotted
        return name, path, args, kwargs


class VideoRoomPublisherPluginField(JanusPluginField[PluginT]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.PUBLISHER)
        super().__init__(*args, **kwargs)


class VideoRoomSubscriberPluginField(JanusPluginField[PluginT]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.SUBSCRIBER)
        super().__init__(*args, **kwargs)


class TextRoomPluginField(JanusPluginField[PluginT]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.TEXTROOM)
        super().__init__(*args, **kwargs)


class StreamingPluginField(JanusPluginField[PluginT]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.STREAMING)
        super().__init__(*args, **kwargs)


class AudioBridgePluginField(JanusPluginField[PluginT]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.AUDIOBRIDGE)
        super().__init__(*args, **kwargs)


class SIPPluginField(JanusPluginField[PluginT]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.SIP)
        super().__init__(*args, **kwargs)


class NoSIPPluginField(JanusPluginField[PluginT]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.NOSIP)
        super().__init__(*args, **kwargs)


class EchoTestPluginField(JanusPluginField[PluginT]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.ECHOTEST)
        super().__init__(*args, **kwargs)


class VideoCallPluginField(JanusPluginField[PluginT]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.VIDEOCALL)
        super().__init__(*args, **kwargs)
