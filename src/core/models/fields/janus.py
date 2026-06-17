"""Custom Django model fields for storing Janus plugin handles transparently."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import Any, Awaitable, Callable, Generic, Mapping, Protocol, Sequence, Type, TypeVar, cast

from django.core import checks
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.query_utils import DeferredAttribute
from django.utils.module_loading import import_string
from janus_api import Janus
from janus_api.session.base import AbstractBaseSession

DEFAULT_PLUGIN_CLASS = "janus_api.Plugin"

JanusEvent = Mapping[str, Any]
RxEventCallback = Callable[[JanusEvent], None]

PluginT = TypeVar("PluginT", bound="SupportsPlugin")
ImportableT = TypeVar("ImportableT")


class JanusPluginIdentifier(str, Enum):
    """Known Janus plugin identifiers supported by ``janus_api``."""

    PUBLISHER = "publisher"
    SUBSCRIBER = "subscriber"
    TEXTROOM = "textroom"
    STREAMING = "streaming"
    AUDIOBRIDGE = "audiobridge"
    SIP = "sip"
    NOSIP = "nosip"
    ECHOTEST = "echotest"
    VIDEOCALL = "videocall"


class SupportsPlugin(Protocol):
    """Structural protocol for ``janus_api`` plugin objects."""

    identifier: str
    id: str | None

    def attach(self) -> Any:
        """Attach the plugin to Janus."""

    def detach(self) -> Any:
        """Detach the plugin from Janus."""


class PluginFactory(Protocol[PluginT]):
    """Constructor contract for the base Janus plugin factory."""

    def __call__(
        self,
        *,
        plugin_id: str | None,
        session: AbstractBaseSession,
        identifier: str,
        on_rx_event: RxEventCallback,
        **kwargs: Any,
    ) -> PluginT:
        """Return a plugin instance."""


CallbackFactory = Callable[[models.Model, "JanusPluginField[Any]", str | None], RxEventCallback]
JanusGetter = Callable[[models.Model, "JanusPluginField[Any]"], AbstractBaseSession | None]
IdentifierGetter = Callable[[models.Model, "JanusPluginField[Any]"], str]
PluginKwargsFactory = Callable[[models.Model, "JanusPluginField[Any]", str | None], Mapping[str, Any]]


def _resolve_importable(value: str | ImportableT) -> ImportableT:
    """Resolve a dotted-path import or return an already-imported object."""

    if isinstance(value, str):
        return cast(ImportableT, import_string(value))
    return value


def _importable_to_dotted_path(value: str | object | None, *, label: str) -> str | None:
    """Return a dotted import path for deconstruction-safe callables and classes."""

    if value is None:
        return None

    if isinstance(value, str):
        return value

    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)

    if not module or not qualname or "<locals>" in qualname:
        raise TypeError(
            f"{label} must be a dotted import path or a top-level importable object. "
            f"Got {value!r}.",
        )

    return f"{module}.{qualname}"


@dataclass(slots=True)
class BoundPluginHandle(Generic[PluginT]):
    """Python-facing wrapper exposed on model instances as ``instance.<plugin_attr>``."""

    instance: models.Model
    field: "JanusPluginField[PluginT]"
    session: AbstractBaseSession | None
    raw_id: str | None = None
    _plugin: PluginT | None = None

    @property
    def id(self) -> str | None:
        """Return the best-known Janus handle identifier."""

        plugin = self._plugin
        if plugin is not None:
            plugin_id = self.field.extract_plugin_id(plugin)
            if plugin_id is not None:
                return plugin_id
        return self.raw_id

    @property
    def plugin(self) -> PluginT:
        """Materialize and cache the bound plugin object for the owning model instance."""

        if self._plugin is None:
            self._plugin = self.field.build_plugin(
                instance=self.instance,
                raw_id=self.raw_id,
            )
        return self._plugin

    @property
    def is_attached(self) -> bool:
        """Return whether the current plugin has already been attached in Janus."""

        return self.id not in (None, "")

    def unwrap(self) -> PluginT:
        """Return the underlying plugin instance."""

        return self.plugin

    def sync_from_plugin(
        self,
        *,
        persist: bool = False,
        using: str | None = None,
        update_fields: Sequence[str] | None = None,
    ) -> str | None:
        """Copy the current ``plugin.id`` back into the raw model field."""

        plugin_id = self.field.extract_plugin_id(self.plugin)
        self.raw_id = plugin_id
        self.field.set_stored_value(
            self.instance,
            plugin_id,
            clear_plugin_cache=False,
        )
        self.instance.__dict__[self.field.plugin_cache_name] = self

        if persist:
            if self.instance.pk is None:
                raise ValueError("Cannot persist plugin_id for an unsaved model instance.")

            fields_to_update = list(update_fields or [])
            if self.field.attname not in fields_to_update:
                fields_to_update.append(self.field.attname)

            self.instance.save(update_fields=fields_to_update, using=using)

        return plugin_id

    def attach(
        self,
        *,
        persist: bool = False,
        using: str | None = None,
        update_fields: Sequence[str] | None = None,
    ) -> Any:
        """Attach the plugin and sync the resulting Janus handle id back to the model."""

        result = self.plugin.attach()

        if inspect.isawaitable(result):

            async def _await_and_sync() -> Any:
                resolved = await cast(Awaitable[Any], result)
                self.sync_from_plugin(
                    persist=persist,
                    using=using,
                    update_fields=update_fields,
                )
                return resolved

            return _await_and_sync()

        self.sync_from_plugin(
            persist=persist,
            using=using,
            update_fields=update_fields,
        )
        return result

    def ensure_attached(
        self,
        *,
        persist: bool = False,
        using: str | None = None,
        update_fields: Sequence[str] | None = None,
    ) -> "BoundPluginHandle[PluginT] | Awaitable[BoundPluginHandle[PluginT]]":
        """Attach the plugin only when it has not already been attached."""

        if self.is_attached:
            return self

        result = self.attach(
            persist=persist,
            using=using,
            update_fields=update_fields,
        )

        if inspect.isawaitable(result):

            async def _await_handle() -> BoundPluginHandle[PluginT]:
                await cast(Awaitable[Any], result)
                return self

            return _await_handle()

        return self

    def __getattr__(self, item: str) -> Any:
        """Delegate unknown attributes to the underlying plugin instance."""

        return getattr(self.plugin, item)

    def __repr__(self) -> str:
        """Return a concise debug representation."""

        return (
            f"{self.__class__.__name__}("
            f"model={self.instance._meta.label!r}, "
            f"field={self.field.name!r}, "
            f"raw_id={self.raw_id!r}, "
            f"identifier={self.field.identifier!r})"
        )


class JanusPluginIdDeferredAttribute(DeferredAttribute):
    """Descriptor that preserves the raw stored Janus handle id on the model field."""

    def __init__(self, field: "JanusPluginField[Any]") -> None:
        super().__init__(field)
        self.field = field

    def __set__(self, instance: models.Model, value: object) -> None:
        """Normalize incoming values before storing them on the model instance."""

        normalized = self.field.normalize_raw_id(value)
        self.field.set_stored_value(
            instance,
            normalized,
            clear_plugin_cache=True,
        )


class BoundPluginDescriptor(Generic[PluginT]):
    """Secondary descriptor installed as ``instance.<plugin_attr>``."""

    def __init__(self, field: "JanusPluginField[PluginT]") -> None:
        self.field = field

    def __get__(
        self,
        instance: models.Model | None,
        owner: type[models.Model] | None = None,
    ) -> "BoundPluginHandle[PluginT] | BoundPluginDescriptor[PluginT]":
        """Return a cached or newly-bound plugin handle for the current instance."""

        if instance is None:
            return self

        raw_id = self.field.get_stored_value(instance)
        cached = instance.__dict__.get(self.field.plugin_cache_name)

        if isinstance(cached, BoundPluginHandle) and cached.raw_id == raw_id:
            return cast(BoundPluginHandle[PluginT], cached)

        handle = BoundPluginHandle[PluginT](
            instance=instance,
            field=self.field,
            raw_id=raw_id,
            session=self.field.resolve_janus(instance),
        )
        instance.__dict__[self.field.plugin_cache_name] = handle
        return handle

    def __set__(self, instance: models.Model, value: object) -> None:
        """Allow assignment from raw ids, plugin objects, or another bound handle."""

        if value is None:
            self.field.set_stored_value(instance, None, clear_plugin_cache=True)
            return

        if isinstance(value, BoundPluginHandle):
            value.instance = instance
            value.field = self.field
            value.raw_id = self.field.normalize_raw_id(value.raw_id)
            value.session = self.field.resolve_janus(instance)
            instance.__dict__[self.field.plugin_cache_name] = value
            self.field.set_stored_value(
                instance,
                value.raw_id,
                clear_plugin_cache=False,
            )
            return

        if isinstance(value, str):
            self.field.set_stored_value(instance, value, clear_plugin_cache=True)
            return

        plugin = cast(PluginT, value)
        plugin_id = self.field.extract_plugin_id(plugin)
        handle = BoundPluginHandle[PluginT](
            instance=instance,
            field=self.field,
            raw_id=plugin_id,
            session=self.field.resolve_janus(instance),
            _plugin=plugin,
        )
        instance.__dict__[self.field.plugin_cache_name] = handle
        self.field.set_stored_value(
            instance,
            plugin_id,
            clear_plugin_cache=False,
        )


class JanusPluginField(models.CharField, Generic[PluginT]):
    """Store a Janus plugin id in the database while exposing a bound plugin object."""

    descriptor_class = JanusPluginIdDeferredAttribute
    description = "Janus plugin handle identifier"

    def __init__(
        self,
        *args: Any,
        identifier: str | JanusPluginIdentifier,
        plugin_class: str | PluginFactory[PluginT] = DEFAULT_PLUGIN_CLASS,
        plugin_attr: str | None = None,
        callback_factory: str | CallbackFactory | None = None,
        janus_getter: str | JanusGetter | None = None,
        identifier_getter: str | IdentifierGetter | None = None,
        plugin_kwargs_factory: str | PluginKwargsFactory | None = None,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("max_length", 255)

        identifier_value = identifier.value if isinstance(identifier, Enum) else str(identifier)
        if not identifier_value:
            raise TypeError("'identifier' is required for JanusPluginField.")

        self.identifier = identifier_value
        self._plugin_class_ref = plugin_class
        self._callback_factory_ref = callback_factory
        self._janus_getter_ref = janus_getter
        self._identifier_getter_ref = identifier_getter
        self._plugin_kwargs_factory_ref = plugin_kwargs_factory
        self._declared_plugin_attr = plugin_attr
        self.plugin_attr: str | None = plugin_attr

        super().__init__(*args, **kwargs)

    @property
    def plugin_cache_name(self) -> str:
        """Return the per-instance cache slot used for bound plugin handles."""

        return f"__janus_bound_plugin_{self.attname}"

    @cached_property
    def plugin_class(self) -> PluginFactory[PluginT]:
        """Resolve the plugin factory class lazily."""

        return _resolve_importable(self._plugin_class_ref)

    @cached_property
    def callback_factory(self) -> CallbackFactory | None:
        """Resolve the callback factory lazily."""

        if self._callback_factory_ref is None:
            return None
        return _resolve_importable(self._callback_factory_ref)

    @cached_property
    def janus_getter(self) -> JanusGetter | None:
        """Resolve the Janus session getter lazily."""

        if self._janus_getter_ref is None:
            return None
        return _resolve_importable(self._janus_getter_ref)

    @cached_property
    def identifier_getter(self) -> IdentifierGetter | None:
        """Resolve the dynamic identifier getter lazily."""

        if self._identifier_getter_ref is None:
            return None
        return _resolve_importable(self._identifier_getter_ref)

    @cached_property
    def plugin_kwargs_factory(self) -> PluginKwargsFactory | None:
        """Resolve the per-instance plugin kwargs factory lazily."""

        if self._plugin_kwargs_factory_ref is None:
            return None
        return _resolve_importable(self._plugin_kwargs_factory_ref)

    def _default_plugin_attr_name(self, field_name: str) -> str:
        """Derive a sensible plugin attribute name from the raw database field name."""

        if field_name.endswith("_id"):
            return field_name[:-3]
        return f"{field_name}_plugin"

    def contribute_to_class(
        self,
        cls: type[models.Model],
        name: str,
        private_only: bool = False,
    ) -> None:
        """Install both Django's raw-id descriptor and a secondary bound-plugin descriptor."""

        super().contribute_to_class(cls, name, private_only=private_only)

        self.plugin_attr = self._declared_plugin_attr or self._default_plugin_attr_name(name)

        if self.plugin_attr == name:
            raise TypeError(
                f"{self.__class__.__name__} cannot use plugin_attr={self.plugin_attr!r} "
                f"because it collides with the field name.",
            )

        setattr(cls, self.plugin_attr, BoundPluginDescriptor(self))

    def check(self, **kwargs: Any) -> list[checks.CheckMessage]:
        """Extend Django field checks with importability validation for hook callables."""

        errors = super().check(**kwargs)
        errors.extend(self._check_importables())
        return errors

    def _check_importables(self) -> list[checks.CheckMessage]:
        """Validate that dotted-path field hook options are importable."""

        messages: list[checks.CheckMessage] = []

        for label, value, error_id in (
            ("plugin_class", self._plugin_class_ref, "janus_api.E001"),
            ("callback_factory", self._callback_factory_ref, "janus_api.E002"),
            ("janus_getter", self._janus_getter_ref, "janus_api.E003"),
            ("identifier_getter", self._identifier_getter_ref, "janus_api.E004"),
            ("plugin_kwargs_factory", self._plugin_kwargs_factory_ref, "janus_api.E005"),
        ):
            try:
                _importable_to_dotted_path(value, label=label)
            except TypeError as exc:
                messages.append(
                    checks.Error(
                        str(exc),
                        obj=self,
                        id=error_id,
                    ),
                )

        return messages

    def normalize_raw_id(self, value: object) -> str | None:
        """Normalize raw ids, bound handles, or plugin objects to a database string."""

        if value in (None, ""):
            return None

        if isinstance(value, str):
            return value

        if isinstance(value, BoundPluginHandle):
            return value.id

        plugin_id = self.extract_plugin_id(value)
        if plugin_id is not None:
            return plugin_id

        raise TypeError(
            f"{self.__class__.__name__} {self.name!r} accepts only a raw string id, "
            "a BoundPluginHandle, a plugin instance exposing '.id', or None.",
        )

    @staticmethod
    def extract_plugin_id(plugin: object) -> str | None:
        """Extract a plugin id while tolerating unattached ``janus_api`` plugin objects."""

        try:
            plugin_id = getattr(plugin, "id", None)
        except RuntimeError:
            return None

        if plugin_id in (None, ""):
            return None
        if not isinstance(plugin_id, str):
            raise TypeError(
                f"Expected plugin.id to be a string or None, got {type(plugin_id).__name__}.",
            )
        return plugin_id

    @staticmethod
    def set_plugin_id(plugin: PluginT, plugin_id: str | None) -> None:
        """Set the id on an already-instantiated plugin object."""

        setattr(plugin, "id", plugin_id)

    def set_stored_value(
        self,
        instance: models.Model,
        value: str | None,
        *,
        clear_plugin_cache: bool,
    ) -> None:
        """Update the raw stored value and optionally clear the bound-plugin cache."""

        normalized = self.to_python(value)
        instance.__dict__[self.attname] = normalized
        if clear_plugin_cache:
            instance.__dict__.pop(self.plugin_cache_name, None)

    def get_stored_value(self, instance: models.Model) -> str | None:
        """Return the raw stored plugin id, respecting deferred Django fields."""

        if self.attname not in instance.__dict__:
            value = getattr(instance, self.attname)
            return self.to_python(value)
        return self.to_python(instance.__dict__.get(self.attname))

    def resolve_identifier(self, instance: models.Model) -> str:
        """Return the plugin identifier to use for the supplied model instance."""

        if self.identifier_getter is not None:
            identifier = self.identifier_getter(instance, self)
            if identifier:
                return str(identifier)
        return self.identifier

    def resolve_janus(self, instance: models.Model) -> AbstractBaseSession | None:
        """Return the Janus session associated with the supplied model instance."""

        if self.janus_getter is not None:
            session = self.janus_getter(instance, self)
            if session is not None:
                return session
        return Janus.get_session()

    def resolve_plugin_kwargs(self, instance: models.Model, raw_id: str | None) -> dict[str, Any]:
        """Return plugin constructor kwargs derived from the owning model instance."""

        if self.plugin_kwargs_factory is None:
            return {}
        return dict(self.plugin_kwargs_factory(instance, self, raw_id))

    def build_on_rx_event(
        self,
        instance: models.Model,
        raw_id: str | None,
    ) -> RxEventCallback:
        """Build the per-instance reactive event callback."""

        factory = self.callback_factory
        if factory is None:
            return self.default_on_rx_event(instance, raw_id)
        return factory(instance, self, raw_id)

    @staticmethod
    def default_on_rx_event(
        instance: models.Model,
        raw_id: str | None,
    ) -> RxEventCallback:
        """Return a no-op callback when no callback factory is configured."""

        def _noop(event: JanusEvent) -> None:
            return None

        return _noop

    def build_plugin(
        self,
        *,
        instance: models.Model,
        raw_id: str | None,
    ) -> PluginT:
        """Construct the bound plugin instance for the supplied model instance."""

        session = self.resolve_janus(instance)
        if session is None:
            raise RuntimeError(
                "No Janus session is available for this process. "
                "Ensure the ASGI lifespan has started or configure janus_getter for worker processes.",
            )

        plugin = self.plugin_class(
            identifier=self.resolve_identifier(instance),
            session=session,
            on_rx_event=self.build_on_rx_event(instance, raw_id),
            plugin_id=raw_id,
            **self.resolve_plugin_kwargs(instance, raw_id),
        )
        return plugin

    def from_db_value(
        self,
        value: Any,
        expression: Any,
        connection: Any,
    ) -> str | None:
        """Convert the raw database value into its Python representation."""

        if value in (None, ""):
            return None
        return str(value)

    def to_python(self, value: Any) -> str | None:
        """Convert incoming values to a normalized plugin id string."""

        if value in (None, ""):
            return None

        if isinstance(value, str):
            return value

        if isinstance(value, BoundPluginHandle):
            return value.id

        plugin_id = self.extract_plugin_id(value)
        if plugin_id is not None:
            return plugin_id

        raise ValidationError(f"Cannot coerce value {value!r} into a Janus plugin id string.")

    def get_prep_value(self, value: Any) -> str | None:
        """Prepare the normalized raw id for database persistence."""

        normalized = self.normalize_raw_id(value)
        if normalized is None:
            return None
        return cast(str, super().get_prep_value(normalized))

    def pre_save(self, model_instance: models.Model, add: bool) -> str | None:
        """Return the raw stored value during model persistence."""

        return self.get_stored_value(model_instance)

    def value_from_object(self, obj: models.Model) -> str | None:
        """Return the raw plugin id from the supplied model instance."""

        return self.get_stored_value(obj)

    def value_to_string(self, obj: models.Model) -> str:
        """Return the serialized raw plugin id for Django fixtures."""

        value = self.value_from_object(obj)
        return "" if value is None else value

    def deconstruct(self) -> tuple[str, str, list[Any], dict[str, Any]]:
        """Serialize the field into migration-friendly constructor arguments."""

        name, path, args, kwargs = super().deconstruct()

        kwargs["identifier"] = self.identifier
        kwargs["plugin_class"] = _importable_to_dotted_path(
            self._plugin_class_ref,
            label="plugin_class",
        )

        if self._declared_plugin_attr is not None:
            kwargs["plugin_attr"] = self._declared_plugin_attr

        for key, value in (
            ("callback_factory", self._callback_factory_ref),
            ("janus_getter", self._janus_getter_ref),
            ("identifier_getter", self._identifier_getter_ref),
            ("plugin_kwargs_factory", self._plugin_kwargs_factory_ref),
        ):
            dotted_path = _importable_to_dotted_path(value, label=key)
            if dotted_path is not None:
                kwargs[key] = dotted_path

        return name, path, args, kwargs


class VideoRoomPublisherPluginField(JanusPluginField[PluginT]):
    """Specialized field for Janus VideoRoom publisher plugins."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.PUBLISHER)
        super().__init__(*args, **kwargs)


class VideoRoomSubscriberPluginField(JanusPluginField[PluginT]):
    """Specialized field for Janus VideoRoom subscriber plugins."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.SUBSCRIBER)
        super().__init__(*args, **kwargs)


class TextRoomPluginField(JanusPluginField[PluginT]):
    """Specialized field for Janus TextRoom plugins."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.TEXTROOM)
        super().__init__(*args, **kwargs)


class StreamingPluginField(JanusPluginField[PluginT]):
    """Specialized field for Janus Streaming plugins."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.STREAMING)
        super().__init__(*args, **kwargs)


class AudioBridgePluginField(JanusPluginField[PluginT]):
    """Specialized field for Janus AudioBridge plugins."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.AUDIOBRIDGE)
        super().__init__(*args, **kwargs)


class SIPPluginField(JanusPluginField[PluginT]):
    """Specialized field for Janus SIP plugins."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.SIP)
        super().__init__(*args, **kwargs)


class NoSIPPluginField(JanusPluginField[PluginT]):
    """Specialized field for Janus NoSIP plugins."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.NOSIP)
        super().__init__(*args, **kwargs)


class EchoTestPluginField(JanusPluginField[PluginT]):
    """Specialized field for Janus EchoTest plugins."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.ECHOTEST)
        super().__init__(*args, **kwargs)


class VideoCallPluginField(JanusPluginField[PluginT]):
    """Specialized field for Janus VideoCall plugins."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("identifier", JanusPluginIdentifier.VIDEOCALL)
        super().__init__(*args, **kwargs)
