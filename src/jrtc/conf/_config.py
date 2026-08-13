"""Small, deterministic settings proxy.

Applications may point ``JANUS_SETTINGS_MODULE`` at their own typed module.
Environment parsing belongs in that module, avoiding the old heuristic that
could turn numeric secrets into integers or comma-containing URLs into lists.
"""

from __future__ import annotations

import importlib
import os
import threading
from collections.abc import Mapping
from types import ModuleType
from typing import Any
from urllib.parse import SplitResult, urlsplit, urlunsplit

_DEFAULT_MODULE = "jrtc.conf.settings"
_SENSITIVE_PARTS = ("SECRET", "TOKEN", "PASSWORD", "PASS", "CREDENTIAL", "API_KEY", "DSN")


def _sensitive(name: str) -> bool:
    normalized = name.upper().replace("-", "_")
    return any(part in normalized for part in _SENSITIVE_PARTS)


def _redact_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        if not parsed.scheme or parsed.hostname is None or parsed.username is None:
            return value
        host = parsed.hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
        return urlunsplit(
            SplitResult(
                parsed.scheme, f"<redacted>@{host}", parsed.path, parsed.query, parsed.fragment
            )
        )
    except ValueError:
        return "<redacted-url>" if "://" in value and "@" in value else value


def _redact_value(value: Any, *, name: str = "") -> Any:
    if _sensitive(name) and value not in (None, ""):
        return "<redacted>"
    if isinstance(value, Mapping):
        return {str(key): _redact_value(item, name=str(key)) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact_value(item) for item in value]
    return _redact_url(value) if isinstance(value, str) else value


class SettingsLoadError(RuntimeError):
    """Raised when a configured settings module cannot be loaded."""


class Settings:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._module_name = os.getenv("JANUS_SETTINGS_MODULE", _DEFAULT_MODULE)
        self._module: ModuleType | None = None
        self._defaults: dict[str, Any] = {}
        self._overrides: dict[str, Any] = {}
        self._frozen = False

    def configure(
        self,
        *,
        module: str | None = None,
        defaults: dict[str, Any] | None = None,
        overrides: dict[str, Any] | None = None,
        freeze: bool = False,
    ) -> None:
        """Select a settings module and explicit programmatic values.

        Precedence is ``overrides > module > defaults``.  The operation resets
        prior overrides so test/application lifecycles do not retain stale
        configuration.
        """

        with self._lock:
            if module is not None:
                self._module_name = module
            self._module = None
            self._defaults = dict(defaults or {})
            self._overrides = dict(overrides or {})
            self._frozen = freeze

    def reload(self) -> None:
        with self._lock:
            self._module = None

    def _load(self) -> ModuleType:
        with self._lock:
            if self._module is not None:
                return self._module
            try:
                module = importlib.import_module(self._module_name)
            except Exception as exc:
                raise SettingsLoadError(
                    f"Could not import Janus settings module {self._module_name!r}"
                ) from exc
            self._module = module
            return module

    def __getattr__(self, name: str) -> Any:
        if not name.isupper():
            raise AttributeError(name)
        with self._lock:
            if name in self._overrides:
                return self._overrides[name]
            module = self._load()
            if hasattr(module, name):
                return getattr(module, name)
            if name in self._defaults:
                return self._defaults[name]
        raise AttributeError(f"Janus setting {name!r} is not defined")

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        if not name.isupper():
            raise AttributeError("settings names must be uppercase")
        with self._lock:
            if self._frozen:
                raise RuntimeError("settings are frozen")
            self._overrides[name] = value

    def as_dict(self, *, redact: bool = True) -> dict[str, Any]:
        module = self._load()
        values = {name: value for name, value in vars(module).items() if name.isupper()}
        values = {**self._defaults, **values, **self._overrides}
        if redact:
            values = {name: _redact_value(value, name=name) for name, value in values.items()}
        return values

    def inspect_settings(self) -> dict[str, dict[str, Any]]:
        module = self._load()
        keys = set(self._defaults) | set(self._overrides)
        keys.update(name for name in vars(module) if name.isupper())
        result: dict[str, dict[str, Any]] = {}
        for name in sorted(keys):
            if name in self._overrides:
                source = "override"
            elif hasattr(module, name):
                source = "module"
            else:
                source = "default"
            value = _redact_value(getattr(self, name), name=name)
            result[name] = {"value": value, "source": source}
        return result


settings = Settings()


def configure(
    *,
    module: str | None = None,
    defaults: dict[str, Any] | None = None,
    overrides: dict[str, Any] | None = None,
    freeze: bool = False,
) -> None:
    settings.configure(
        module=module,
        defaults=defaults,
        overrides=overrides,
        freeze=freeze,
    )
