"""Thread-safe registry with lazy Python entry-point discovery.

Named Janus plugins are separate distributions.  They advertise plugin classes
through the ``jrtc.plugins`` entry-point group, allowing the core package to
remain free of concrete plugin imports and import-time directory scanning.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator, MutableMapping
from importlib import metadata

from jrtc.core.exceptions import (
    PluginAlreadyRegistered,
    PluginLoadError,
    PluginNotRegistered,
)

logger = logging.getLogger(__name__)


class Registry[T](MutableMapping[str, type[T]]):
    """Map stable identifiers to classes and resolve optional entry points.

    Registration is strict by default: silently replacing a plugin class makes
    behavior depend on import order, which is unsafe in extensible applications.
    Re-registering the exact same class is idempotent.
    """

    def __init__(self, *, entry_point_group: str | None = None) -> None:
        self._map: dict[str, type[T]] = {}
        self._lock = threading.RLock()
        self._entry_point_group = entry_point_group

    @staticmethod
    def _normalize(identifier: str) -> str:
        value = identifier.strip().lower()
        if not value:
            raise ValueError("identifier must be a non-empty string")
        return value

    def register(self, identifier: str, cls: type[T], *, replace: bool = False) -> None:
        key = self._normalize(identifier)
        if not isinstance(cls, type):
            raise TypeError("registered plugin must be a class")
        with self._lock:
            current = self._map.get(key)
            if current is cls:
                return
            if current is not None and not replace:
                raise PluginAlreadyRegistered(
                    f"Plugin identifier {key!r} is already registered by "
                    f"{current.__module__}.{current.__qualname__}."
                )
            self._map[key] = cls
        logger.debug("Registered plugin %s -> %s.%s", key, cls.__module__, cls.__qualname__)

    def resolve(self, identifier: str) -> type[T]:
        """Resolve a class, loading only the matching installed entry point."""

        key = self._normalize(identifier)
        with self._lock:
            registered = self._map.get(key)
        if registered is not None:
            return registered

        self._load_entry_point(key)
        with self._lock:
            registered = self._map.get(key)
        if registered is None:
            raise PluginNotRegistered(
                f"No Janus plugin is registered as {key!r}. Install the matching "
                "janus-*-plugin distribution or import a custom Plugin subclass."
            )
        return registered

    def _load_entry_point(self, identifier: str) -> None:
        group = self._entry_point_group
        if group is None:
            return
        with self._lock:
            if identifier in self._map:
                return
            # Import while holding the re-entrant lock. Plugin class creation
            # may register itself, while concurrent resolvers must not observe
            # a half-loaded entry point.
            candidates = metadata.entry_points().select(group=group, name=identifier)

            if len(candidates) > 1:
                providers = ", ".join(sorted(item.value for item in candidates))
                raise PluginLoadError(
                    f"Multiple entry points provide Janus plugin {identifier!r}: {providers}"
                )
            if not candidates:
                return

            entry_point = next(iter(candidates))
            try:
                loaded = entry_point.load()
            except Exception as exc:
                raise PluginLoadError(
                    f"Could not load Janus plugin {identifier!r} from {entry_point.value!r}."
                ) from exc
            if not isinstance(loaded, type):
                raise PluginLoadError(
                    f"Janus plugin entry point {entry_point.value!r} did not resolve to a class."
                )
            declared = getattr(loaded, "identifier", None)
            if declared is not None and self._normalize(str(declared)) != identifier:
                raise PluginLoadError(
                    f"Janus plugin entry point {identifier!r} loaded a class declaring "
                    f"identifier {declared!r}."
                )
            if identifier not in self._map:
                self.register(identifier, loaded)

    def unregister(self, identifier: str) -> type[T]:
        key = self._normalize(identifier)
        with self._lock:
            try:
                return self._map.pop(key)
            except KeyError as exc:
                raise PluginNotRegistered(key) from exc

    def __getitem__(self, key: str) -> type[T]:
        return self.resolve(key)

    def __setitem__(self, key: str, value: type[T]) -> None:
        self.register(key, value)

    def __delitem__(self, key: str) -> None:
        self.unregister(key)

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(tuple(self._map))

    def __len__(self) -> int:
        with self._lock:
            return len(self._map)

    def get_registered(self, identifier: str) -> type[T] | None:
        """Return an already imported class without loading entry points."""

        key = self._normalize(identifier)
        with self._lock:
            return self._map.get(key)
