"""Explicit application-level access to an installed session manager."""

from __future__ import annotations

from typing import Any


class Janus:
    """Compatibility accessor populated explicitly by a host application.

    The manager lifecycle remains explicit; importing this module starts no
    thread, event loop, network connection, or background task.
    """

    _manager: Any | None = None
    _manager_owner: object | None = None

    @classmethod
    def set_manager(cls, manager: Any | None) -> None:
        """Install a manager for compatibility with single-app hosts.

        Multi-lifecycle hosts should use :meth:`install_manager`, which prevents
        one owner from clearing another owner's manager.
        """

        if manager is not None and cls._manager is not None and cls._manager is not manager:
            raise RuntimeError("a Janus session manager is already installed in this process")
        cls._manager = manager
        cls._manager_owner = None

    @classmethod
    def install_manager(cls, manager: Any) -> object:
        """Install one process-global compatibility manager and return its lease."""

        if cls._manager is not None and cls._manager is not manager:
            raise RuntimeError("a Janus session manager is already installed in this process")
        owner = object()
        cls._manager = manager
        cls._manager_owner = owner
        return owner

    @classmethod
    def remove_manager(cls, owner: object) -> None:
        """Remove a manager only when ``owner`` holds the current lease."""

        if cls._manager_owner is owner:
            cls._manager = None
            cls._manager_owner = None

    @classmethod
    def get_manager(cls) -> Any | None:
        return cls._manager

    @classmethod
    def get_session(cls, key: str | int | None = None) -> Any | None:
        manager = cls._manager
        return None if manager is None else manager.get_session(key)

    @classmethod
    async def setup(cls) -> None:
        if cls._manager is None:
            raise RuntimeError("configure a JanusSessionManager before setup()")
        await cls._manager.start()

    @classmethod
    async def teardown(cls) -> None:
        manager, cls._manager = cls._manager, None
        cls._manager_owner = None
        if manager is not None:
            await manager.stop()
