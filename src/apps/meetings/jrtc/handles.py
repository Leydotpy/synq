"""Explicit process-local ownership of live JRTC VideoRoom handles.

Database identifiers are correlation data only.  This registry never creates a
plugin with a persisted ``plugin_id`` and therefore cannot silently adopt a
stale handle on a new Janus session.  Records are event-loop-bound and must be
cleared when their runtime stops or forks.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from jrtc_video import VideoRoomPlugin

from apps.meetings.jrtc.errors import (
    JrtcHandleOwnershipError,
    JrtcHandleUnavailable,
    JrtcStaleHandleError,
)
from apps.meetings.jrtc.ids import optional_janus_id, require_janus_id


_ResultT = TypeVar("_ResultT")


@dataclass(slots=True)
class _ResolutionFence:
    """Reference-counted per-domain lock safe for queued asyncio waiters."""

    lock: asyncio.Lock
    users: int = 0


@dataclass(frozen=True, slots=True)
class HandleBindingSpec:
    """Detached domain data required to resolve or recreate one live handle."""

    model_id: str
    session_key: str | int | None = None
    persisted_session_id: int | None = None
    persisted_handle_id: int | None = None
    persisted_owner_id: str | None = None
    connection_id: str | None = None
    opaque_id: str | None = None

    def __post_init__(self) -> None:
        if not self.model_id:
            raise ValueError("model_id must not be empty")
        optional_janus_id(self.persisted_session_id, name="persisted session_id")
        optional_janus_id(self.persisted_handle_id, name="persisted handle_id")


@dataclass(slots=True)
class BoundVideoRoomHandle:
    """One live plugin plus its verified process/session ownership."""

    model_id: str
    session_id: int
    handle_id: int
    plugin: VideoRoomPlugin
    owner_id: str
    connection_id: str | None = None
    active: bool = True


@dataclass(frozen=True, slots=True)
class HandleResolution:
    """Result returned to the synchronous persistence boundary."""

    binding: BoundVideoRoomHandle
    recreated: bool
    replaced_stale: bool


class JrtcHandleRegistry:
    """Own domain-to-plugin mappings for exactly one process runtime instance."""

    def __init__(self, owner_id: str) -> None:
        if not owner_id:
            raise ValueError("owner_id must not be empty")
        self.owner_id = owner_id
        self._bindings: dict[str, BoundVideoRoomHandle] = {}
        self._lock = asyncio.Lock()
        self._resolution_locks: dict[str, _ResolutionFence] = {}
        self._stale_invalidations = 0
        self._closed = False

    @property
    def active_count(self) -> int:
        return sum(binding.active for binding in self._bindings.values())

    @property
    def stale_invalidations(self) -> int:
        return self._stale_invalidations

    def snapshot(self) -> tuple[BoundVideoRoomHandle, ...]:
        """Return an immutable diagnostic view without exposing mutable storage."""

        return tuple(self._bindings.values())

    @staticmethod
    def _binding_is_live(binding: BoundVideoRoomHandle) -> bool:
        if not binding.active:
            return False
        session = binding.plugin.session
        if not bool(getattr(session, "ready", False)):
            return False
        try:
            if require_janus_id(session.id, name="session_id") != binding.session_id:
                return False
            if require_janus_id(binding.plugin.id, name="handle_id") != binding.handle_id:
                return False
        except (RuntimeError, TypeError):
            return False
        return session.plugins.get(binding.handle_id) is binding.plugin

    async def _resolution_lock(
        self,
        model_id: str,
        *,
        require_open: bool = True,
    ) -> _ResolutionFence:
        """Return the one operation fence for a domain handle."""

        async with self._lock:
            if require_open and self._closed:
                raise JrtcHandleUnavailable("The JRTC handle registry is shutting down.")
            fence = self._resolution_locks.setdefault(
                model_id,
                _ResolutionFence(lock=asyncio.Lock()),
            )
            fence.users += 1
            return fence

    async def _drop_unused_resolution_lock(
        self,
        model_id: str,
        fence: _ResolutionFence,
    ) -> None:
        """Discard idle per-handle locks after their binding has gone away."""

        async with self._lock:
            if (
                self._bindings.get(model_id) is None
                and self._resolution_locks.get(model_id) is fence
            ):
                fence.users -= 1
                if fence.users == 0:
                    self._resolution_locks.pop(model_id, None)
            else:
                fence.users -= 1
            if fence.users < 0:
                raise RuntimeError("JRTC handle fence reference count became negative")

    async def _get_locked(self, model_id: str) -> BoundVideoRoomHandle | None:
        """Return a live binding while the caller holds its operation fence."""

        stale: BoundVideoRoomHandle | None = None
        ownership_error: JrtcHandleOwnershipError | None = None
        async with self._lock:
            binding = self._bindings.get(model_id)
            if binding is None:
                return None
            if binding.owner_id != self.owner_id:
                binding.active = False
                self._bindings.pop(binding.model_id, None)
                stale = binding
                ownership_error = JrtcHandleOwnershipError(
                    f"Handle {binding.model_id} belongs to runtime {binding.owner_id}."
                )
            elif not self._binding_is_live(binding):
                binding.active = False
                self._bindings.pop(binding.model_id, None)
                self._stale_invalidations += 1
                stale = binding
            else:
                return binding
        if stale is not None:
            try:
                await stale.plugin.aclose()
            except Exception:
                pass
        if ownership_error is not None:
            raise ownership_error
        return None

    async def get(self, model_id: str) -> BoundVideoRoomHandle | None:
        """Return a verified live binding or invalidate a stale local record."""

        key = str(model_id)
        fence = await self._resolution_lock(key)
        try:
            async with fence.lock:
                return await self._get_locked(key)
        finally:
            await self._drop_unused_resolution_lock(key, fence)

    async def _bind_locked(
        self,
        model_id: str,
        plugin: VideoRoomPlugin,
        *,
        connection_id: str | None = None,
    ) -> BoundVideoRoomHandle:
        """Register an attached plugin while holding its operation fence."""

        session = plugin.session
        session_id = require_janus_id(session.id, name="session_id")
        handle_id = require_janus_id(plugin.id, name="handle_id")
        if not bool(getattr(session, "ready", False)):
            raise JrtcHandleUnavailable("Cannot bind a plugin whose Janus session is not active.")
        if session.plugins.get(handle_id) is not plugin:
            raise JrtcHandleOwnershipError("JRTC's session registry does not own this plugin.")
        binding = BoundVideoRoomHandle(
            model_id=str(model_id),
            session_id=session_id,
            handle_id=handle_id,
            plugin=plugin,
            owner_id=self.owner_id,
            connection_id=connection_id,
        )
        previous: BoundVideoRoomHandle | None = None
        async with self._lock:
            if self._closed:
                raise JrtcHandleUnavailable("The JRTC handle registry is shutting down.")
            previous = self._bindings.get(binding.model_id)
            if previous is not None and previous.plugin is not plugin:
                previous.active = False
            self._bindings[binding.model_id] = binding
        if previous is not None and previous.plugin is not plugin:
            try:
                await previous.plugin.detach()
            except Exception:
                pass
            try:
                await previous.plugin.aclose()
            except Exception:
                pass
        return binding

    async def bind(self, model_id: str, plugin: VideoRoomPlugin) -> BoundVideoRoomHandle:
        """Register an already-attached plugin after strict ownership validation."""

        key = str(model_id)
        fence = await self._resolution_lock(key)
        try:
            async with fence.lock:
                return await self._bind_locked(key, plugin)
        finally:
            await self._drop_unused_resolution_lock(key, fence)

    async def resolve_or_attach(
        self,
        spec: HandleBindingSpec,
        *,
        session: Any,
        recreate: bool,
    ) -> HandleResolution:
        """Resolve a live binding or attach a fresh plugin when permitted.

        A DB-only handle is always stale.  Its IDs are used solely to report and
        persist replacement; they are never supplied to ``VideoRoomPlugin``.
        """

        model_id = str(spec.model_id)
        fence = await self._resolution_lock(model_id)
        try:
            async with fence.lock:
                return await self._resolve_or_attach_locked(
                    spec,
                    session=session,
                    recreate=recreate,
                )
        finally:
            await self._drop_unused_resolution_lock(model_id, fence)

    async def _resolve_or_attach_locked(
        self,
        spec: HandleBindingSpec,
        *,
        session: Any,
        recreate: bool,
    ) -> HandleResolution:
        """Perform one serialized resolve/attach operation for a domain key."""

        binding = await self._get_locked(str(spec.model_id))
        owner_mismatch = spec.persisted_owner_id not in (None, "", self.owner_id)
        if owner_mismatch:
            # A persisted foreign owner may still be live. Without a lease or
            # heartbeat there is no safe way to distinguish it from a dead
            # process, so recreation must fail closed rather than steal it.
            if binding is not None:
                await self._invalidate_locked(str(spec.model_id), close_local=True)
            raise JrtcHandleOwnershipError(
                f"Persisted handle belongs to runtime {spec.persisted_owner_id!r}."
            )

        if binding is not None:
            if binding.connection_id != spec.connection_id:
                raise JrtcHandleOwnershipError(
                    "The live handle belongs to a different connection generation."
                )
            if spec.persisted_session_id not in (None, binding.session_id) or (
                spec.persisted_handle_id not in (None, binding.handle_id)
            ):
                # The process-local registry is authoritative for liveness.
                # A same-owner/ownerless ORM snapshot can lag immediately
                # after recovery; converge persistence on this binding rather
                # than detaching it and creating a second unjoined plugin.
                return HandleResolution(
                    binding=binding,
                    recreated=False,
                    replaced_stale=True,
                )
            return HandleResolution(binding=binding, recreated=False, replaced_stale=False)

        persisted = spec.persisted_session_id is not None or spec.persisted_handle_id is not None
        if persisted and not recreate:
            raise JrtcStaleHandleError(
                "Persisted Janus IDs do not identify a live handle in this process."
            )
        if not recreate:
            raise JrtcHandleUnavailable("No live JRTC VideoRoom handle is registered.")
        if not bool(getattr(session, "ready", False)):
            raise JrtcHandleUnavailable("Cannot attach a handle without an active Janus session.")

        plugin = VideoRoomPlugin(session=session)
        try:
            await plugin.attach(opaque_id=spec.opaque_id)
            binding = await self._bind_locked(
                str(spec.model_id),
                plugin,
                connection_id=spec.connection_id,
            )
        except BaseException:
            try:
                await plugin.detach()
            except Exception:
                pass
            try:
                await plugin.aclose()
            except Exception:
                pass
            raise
        return HandleResolution(
            binding=binding,
            recreated=True,
            replaced_stale=persisted,
        )

    async def invoke(
        self,
        binding: BoundVideoRoomHandle,
        operation: Callable[[VideoRoomPlugin], Awaitable[_ResultT]],
    ) -> _ResultT:
        """Run one command under the same fence used by detach/invalidate."""

        model_id = str(binding.model_id)
        fence = await self._resolution_lock(model_id)
        try:
            async with fence.lock:
                current = await self._get_locked(model_id)
                if current is None or current is not binding:
                    raise JrtcHandleUnavailable(
                        "The VideoRoom handle is no longer live in this process."
                    )
                return await operation(current.plugin)
        finally:
            await self._drop_unused_resolution_lock(model_id, fence)

    async def _invalidate_locked(
        self,
        model_id: str,
        *,
        close_local: bool,
    ) -> BoundVideoRoomHandle | None:
        """Remove a binding while the caller holds its operation fence."""

        async with self._lock:
            binding = self._bindings.pop(model_id, None)
            if binding is not None:
                binding.active = False
        if binding is not None and close_local:
            try:
                await binding.plugin.detach()
            except Exception:
                pass
            await binding.plugin.aclose()
        return binding

    async def invalidate(
        self,
        model_id: str,
        *,
        close_local: bool = True,
        expected_session_id: int | None = None,
        expected_handle_id: int | None = None,
    ) -> BoundVideoRoomHandle | None:
        """Remove one binding and optionally close its process-local callbacks."""

        key = str(model_id)
        fence = await self._resolution_lock(key)
        try:
            async with fence.lock:
                binding = await self._get_locked(key)
                if binding is None:
                    return None
                if (
                    expected_session_id is not None
                    and binding.session_id != expected_session_id
                ) or (
                    expected_handle_id is not None
                    and binding.handle_id != expected_handle_id
                ):
                    raise JrtcHandleOwnershipError(
                        "The live handle changed before the requested invalidation."
                    )
                return await self._invalidate_locked(key, close_local=close_local)
        finally:
            await self._drop_unused_resolution_lock(key, fence)

    async def detach(
        self,
        model_id: str,
        *,
        expected: BoundVideoRoomHandle | None = None,
    ) -> Any | None:
        """Detach a live locally owned handle; never reconstruct a DB-only one."""

        key = str(model_id)
        fence = await self._resolution_lock(key)
        try:
            async with fence.lock:
                binding = await self._get_locked(key)
                if binding is None:
                    raise JrtcHandleUnavailable("No live local handle is available to detach.")
                if expected is not None and binding is not expected:
                    raise JrtcHandleOwnershipError(
                        "The live handle changed before the requested detach."
                    )
                try:
                    return await binding.plugin.detach()
                finally:
                    await self._invalidate_locked(key, close_local=False)
        finally:
            await self._drop_unused_resolution_lock(key, fence)

    async def invalidate_connection(
        self,
        connection_id: str,
    ) -> tuple[BoundVideoRoomHandle, ...]:
        """Detach every exact local binding created for one socket generation."""

        expected_connection_id = str(connection_id)
        async with self._lock:
            candidates = tuple(
                binding
                for binding in self._bindings.values()
                if binding.connection_id == expected_connection_id
            )
        invalidated: list[BoundVideoRoomHandle] = []
        for expected in candidates:
            key = str(expected.model_id)
            fence = await self._resolution_lock(key, require_open=False)
            try:
                async with fence.lock:
                    current = await self._get_locked(key)
                    if current is not expected:
                        continue
                    binding = await self._invalidate_locked(key, close_local=True)
                    if binding is not None:
                        invalidated.append(binding)
            finally:
                await self._drop_unused_resolution_lock(key, fence)
        return tuple(invalidated)

    async def clear(self) -> None:
        """Release all local records after the owning manager has stopped."""

        async with self._lock:
            self._closed = True
            resolution_fences = tuple(self._resolution_locks.values())
        acquired_fences: list[_ResolutionFence] = []
        try:
            for fence in resolution_fences:
                await fence.lock.acquire()
                acquired_fences.append(fence)
            async with self._lock:
                bindings = tuple(self._bindings.values())
                self._bindings.clear()
                self._resolution_locks.clear()
                for binding in bindings:
                    binding.active = False
        finally:
            for fence in reversed(acquired_fences):
                fence.lock.release()
        if bindings:
            await asyncio.gather(
                *(binding.plugin.aclose() for binding in bindings),
                return_exceptions=True,
            )

    def discard_after_fork(self) -> None:
        """Forget inherited event-loop objects without touching parent transports."""

        for binding in self._bindings.values():
            binding.active = False
        self._bindings = {}
        self._lock = asyncio.Lock()
        self._resolution_locks = {}
        self._closed = False


__all__ = [
    "BoundVideoRoomHandle",
    "HandleBindingSpec",
    "HandleResolution",
    "JrtcHandleRegistry",
]
