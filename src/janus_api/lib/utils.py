"""Compatibility helpers for the pre-3.0 process-global plugin runtime."""

from __future__ import annotations

import warnings


async def shutdown_all(timeout: float | None = None, clear_cache: bool = False) -> None:
    """Deprecated no-op; plugin handles are now owned by their session.

    Applications must close each Janus session or session manager.  The old
    arguments remain accepted so existing lifespan hooks fail safely.
    """

    del timeout, clear_cache
    warnings.warn(
        "shutdown_all() is deprecated; close the owning Janus session manager",
        DeprecationWarning,
        stacklevel=2,
    )
