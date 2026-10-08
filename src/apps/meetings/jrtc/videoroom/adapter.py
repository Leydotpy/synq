"""Synq-owned adapter around the independently packaged JRTC VideoRoom API.

Commands execute directly on live process-local plugins and return typed
``VideoRoomReply`` values through JRTC's transaction Futures.  No command is
implemented as broker RPC.  The adapter also owns strict ID validation,
session-owned management services, stale-binding recovery, and exception
translation.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import Any, Protocol

from jrtc.core.exceptions import JanusException
from jrtc.models.base import Jsep
from jrtc.models.request import TrickleCandidate
from jrtc_video import (
    PublisherConfigureRequest,
    PublisherJoinAndConfigureRequest,
    PublisherPublishRequest,
    SubscriberJoinRequest,
    SubscriberUpdateRequest,
    VideoRoomError,
    VideoRoomPlugin,
    VideoRoomService,
    VideoRoomProtocolError as PackageVideoRoomProtocolError,
)

from apps.meetings.jrtc.errors import (
    JrtcHandleUnavailable,
    JrtcSessionUnavailable,
    VideoRoomCommandError,
    VideoRoomProtocolError,
)
from apps.meetings.jrtc.handles import (
    BoundVideoRoomHandle,
    HandleBindingSpec,
    HandleResolution,
    JrtcHandleRegistry,
)

logger = logging.getLogger(__name__)


class RuntimeProtocol(Protocol):
    """Narrow runtime surface used by the adapter."""

    def session(self, *, key: str | int | None = None) -> Any: ...

    def observe_session(self, session: Any) -> None: ...


class VideoRoomAdapter:
    """Resolve live handles and issue direct typed VideoRoom commands."""

    def __init__(self, runtime: RuntimeProtocol, registry: JrtcHandleRegistry) -> None:
        self.runtime = runtime
        self.registry = registry
        self._services: dict[tuple[int, object], VideoRoomService] = {}
        self._service_lock = asyncio.Lock()
        self._closing = False

    def get_session(self, key: str | int | None = None) -> Any:
        """Return the process-local ready session selected for ``key``."""

        try:
            session = self.runtime.session(key=key)
        except Exception as exc:
            raise JrtcSessionUnavailable("No process-local Janus session is available.") from exc
        if session is None or not bool(getattr(session, "ready", False)):
            raise JrtcSessionUnavailable("The selected Janus session is not active.")
        self.runtime.observe_session(session)
        return session

    async def resolve_handle(
        self,
        spec: HandleBindingSpec,
        *,
        recreate: bool = True,
    ) -> HandleResolution:
        """Resolve the live binding or attach a new plugin without adopting DB IDs."""

        session = self.get_session(spec.session_key)
        try:
            return await self.registry.resolve_or_attach(
                spec,
                session=session,
                recreate=recreate,
            )
        except (JrtcHandleUnavailable, JrtcSessionUnavailable):
            raise
        except Exception as exc:
            raise JrtcHandleUnavailable("Unable to resolve the VideoRoom handle.") from exc

    async def attach_publisher(self, spec: HandleBindingSpec) -> HandleResolution:
        return await self.resolve_handle(spec, recreate=True)

    async def attach_subscriber(self, spec: HandleBindingSpec) -> HandleResolution:
        return await self.resolve_handle(spec, recreate=True)

    async def management_command(
        self,
        *,
        session_key: str | int | None,
        method_name: str,
        args: Sequence[Any] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> Any:
        """Reuse the package-owned control service on its actual session."""
        session = self.get_session(session_key)
        key = (id(session), getattr(session, "generation", None))
        try:
            async with self._service_lock:
                if self._closing:
                    raise JrtcSessionUnavailable("VideoRoom services are closing.")
                for old_key, old in tuple(self._services.items()):
                    if (old.session is session and old_key != key) or not old.session.ready:
                        await old.aclose(graceful=False)
                        self._services.pop(old_key, None)
                service = self._services.get(key)
                if service is None:
                    service = VideoRoomService(session)
                    self._services[key] = service
            return await service.management_command(method_name, args=args, kwargs=kwargs)
        except PackageVideoRoomProtocolError as exc:
            raise VideoRoomProtocolError(str(exc)) from exc
        except (VideoRoomError, JanusException, TimeoutError, RuntimeError) as exc:
            raise VideoRoomCommandError(
                f"VideoRoom management command {method_name!r} failed."
            ) from exc

    async def prune_lost_services(self) -> None:
        async with self._service_lock:
            for key, service in tuple(self._services.items()):
                if not service.session.ready or key != (id(service.session), getattr(service.session, "generation", None)):
                    await service.aclose(graceful=False)
                    self._services.pop(key, None)

    async def aclose(self) -> None:
        """Close control services while their owning sessions still exist."""
        self._closing = True
        async with self._service_lock:
            results = await asyncio.gather(
                *(service.aclose(graceful=False) for service in self._services.values()),
                return_exceptions=True,
            )
            self._services = {key: service for key, service in self._services.items() if not service.closed}
        failures = [value for value in results if isinstance(value, BaseException)]
        if failures:
            raise BaseExceptionGroup("VideoRoom service shutdown failed", failures)

    async def invoke(
        self,
        binding: BoundVideoRoomHandle,
        method_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Invoke one direct command after revalidating the runtime binding."""

        async def operation(plugin: VideoRoomPlugin) -> Any:
            method = getattr(plugin, method_name, None)
            if not callable(method):
                raise VideoRoomProtocolError(
                    f"VideoRoomPlugin does not expose command {method_name!r}."
                )
            return await method(*args, **kwargs)

        try:
            # Registry invocation holds the per-domain operation fence through
            # the command, so detach/invalidate cannot race validation.
            return await self.registry.invoke(binding, operation)
        except (JrtcHandleUnavailable, VideoRoomProtocolError):
            raise
        except PackageVideoRoomProtocolError as exc:
            raise VideoRoomProtocolError(str(exc)) from exc
        except (VideoRoomError, JanusException, TimeoutError, RuntimeError) as exc:
            raise VideoRoomCommandError(
                f"VideoRoom command {method_name!r} failed."
            ) from exc

    async def join_and_configure(
        self,
        binding: BoundVideoRoomHandle,
        body: PublisherJoinAndConfigureRequest,
        offer: Jsep,
    ) -> Any:
        return await self.invoke(binding, "join_and_configure", body, offer)

    async def publish(
        self,
        binding: BoundVideoRoomHandle,
        offer: Jsep,
        *,
        body: PublisherPublishRequest | None = None,
    ) -> Any:
        return await self.invoke(binding, "publish", offer, body=body)

    async def configure_publisher(
        self,
        binding: BoundVideoRoomHandle,
        body: PublisherConfigureRequest | None = None,
        *,
        offer: Jsep | None = None,
    ) -> Any:
        return await self.invoke(binding, "configure_publisher", body, offer=offer)

    async def unpublish(self, binding: BoundVideoRoomHandle) -> Any:
        return await self.invoke(binding, "unpublish")

    async def join_subscriber(
        self,
        binding: BoundVideoRoomHandle,
        body: SubscriberJoinRequest,
    ) -> Any:
        return await self.invoke(binding, "join_subscriber", body)

    async def update_subscription(
        self,
        binding: BoundVideoRoomHandle,
        body: SubscriberUpdateRequest,
    ) -> Any:
        return await self.invoke(binding, "update_subscription", body)

    async def start_subscriber(
        self,
        binding: BoundVideoRoomHandle,
        *,
        answer: Jsep | None = None,
    ) -> Any:
        return await self.invoke(binding, "start", answer=answer)

    async def trickle(
        self,
        binding: BoundVideoRoomHandle,
        candidates: TrickleCandidate | Sequence[TrickleCandidate],
    ) -> Any:
        return await self.invoke(binding, "trickle", candidates)

    async def trickle_batch(
        self,
        binding: BoundVideoRoomHandle,
        candidates: Sequence[TrickleCandidate],
        *,
        completed: bool,
        authorize=None,
    ) -> None:
        """One authorization fence covers candidates and explicit completion."""
        async def operation(plugin: VideoRoomPlugin) -> None:
            if authorize is not None:
                await authorize()
            if candidates:
                await plugin.trickle(candidates)
            if completed:
                if candidates and authorize is not None:
                    await authorize()
                await plugin.complete_trickle()

        await self.registry.invoke(binding, operation)

    async def complete_trickle(self, binding: BoundVideoRoomHandle) -> Any:
        return await self.invoke(binding, "complete_trickle")

    async def hangup(self, binding: BoundVideoRoomHandle) -> Any:
        # Janus hangup and plugin detach are distinct lifecycle operations.
        # Callers that intend to destroy the handle must invoke ``detach``.
        return await self.invoke(binding, "hangup")

    async def detach(self, binding: BoundVideoRoomHandle) -> Any:
        return await self.registry.detach(binding.model_id, expected=binding)


__all__ = ["RuntimeProtocol", "VideoRoomAdapter"]
