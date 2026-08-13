"""Per-session ownership and event routing for attached plugin handles."""

from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from typing import TypeVar, overload

from logvista import get_logger, lazy

from jrtc.core.exceptions import PluginAlreadyRegistered, PluginNotRegistered

logger = get_logger(__name__)

D = TypeVar("D")


class PluginManager[P](MutableMapping[str, P]):
    """A small handle registry owned by exactly one Janus session.

    Janus IDs may arrive as JSON numbers while application code stores strings.
    Keys are normalized at every boundary to prevent dropped events.
    """

    def __init__(self) -> None:
        self._registry: dict[str, P] = {}

    @staticmethod
    def _key(handle_id: str | int) -> str:
        return str(handle_id)

    def register(self, handle_id: str | int, plugin: P, *, replace: bool = False) -> None:
        key = self._key(handle_id)
        current = self._registry.get(key)
        if current is plugin:
            return
        if current is not None and not replace:
            raise PluginAlreadyRegistered(f"Plugin handle {key!r} is already registered")
        self._registry[key] = plugin
        logger.debug(
            "Plugin manager metric",
            "Registered plugin handle",
            lazy(
                lambda: {
                    "operation": "register",
                    "plugin_count": len(self._registry),
                    "replaced": current is not None,
                }
            ),
        )

    def unregister(self, handle_id: str | int) -> P:
        key = self._key(handle_id)
        try:
            plugin = self._registry.pop(key)
        except KeyError as exc:
            raise PluginNotRegistered(f"Plugin handle {key!r} is not registered") from exc
        logger.debug(
            "Plugin manager metric",
            "Unregistered plugin handle",
            lazy(
                lambda: {
                    "operation": "unregister",
                    "plugin_count": len(self._registry),
                }
            ),
        )
        return plugin

    def dispatch(self, handle_id: str | int, event: object) -> None:
        plugin = self.get(handle_id)
        if plugin is None:
            raise PluginNotRegistered(f"Plugin handle {handle_id!r} is not registered")
        dispatcher = getattr(plugin, "_dispatch_event", None)
        if callable(dispatcher):
            dispatcher(event)
            self._log_dispatch("internal")
            return
        callback = getattr(plugin, "on_event", None)
        if callable(callback):
            callback(event)
            self._log_dispatch("callback")
            return
        raise TypeError(f"Registered handle {handle_id!r} cannot receive events")

    def _log_dispatch(self, route: str) -> None:
        logger.debug(
            "Plugin manager metric",
            "Dispatched plugin event",
            lazy(
                lambda: {
                    "operation": "dispatch",
                    "plugin_count": len(self._registry),
                    "route": route,
                }
            ),
        )

    def __getitem__(self, handle_id: str) -> P:
        key = self._key(handle_id)
        try:
            return self._registry[key]
        except KeyError as exc:
            raise PluginNotRegistered(f"Plugin handle {key!r} is not registered") from exc

    def __setitem__(self, handle_id: str, plugin: P) -> None:
        self.register(handle_id, plugin)

    def __delitem__(self, handle_id: str) -> None:
        self.unregister(handle_id)

    def __iter__(self) -> Iterator[str]:
        return iter(tuple(self._registry))

    def __len__(self) -> int:
        return len(self._registry)

    @overload
    def get(self, handle_id: str | int) -> P | None: ...

    @overload
    def get(self, handle_id: str | int, default: P) -> P: ...

    @overload
    def get(self, handle_id: str | int, default: D) -> P | D: ...

    def get(self, handle_id: str | int, default: D | None = None) -> P | D | None:
        return self._registry.get(self._key(handle_id), default)

    def clear(self) -> None:
        plugins = tuple(self._registry.values())
        self._registry.clear()
        logger.debug(
            "Plugin manager metric",
            "Cleared plugin handles",
            lazy(
                lambda: {
                    "operation": "clear",
                    "cleared_count": len(plugins),
                    "plugin_count": 0,
                }
            ),
        )
        for plugin in plugins:
            stop = getattr(plugin, "stop", None)
            if callable(stop):
                stop()

    def as_dict(self) -> dict[str, P]:
        return dict(self._registry)
