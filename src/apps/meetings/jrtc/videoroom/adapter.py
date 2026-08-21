"""Synq-owned adapter around the independently packaged JRTC VideoRoom API.

Commands execute directly on live process-local plugins and return typed
``VideoRoomReply`` values through JRTC's transaction Futures.  No command is
implemented as broker RPC.  The adapter also owns strict ID validation,
short-lived management handles, stale-binding recovery, and exception
translation.
"""

from __future__ import annotations

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


class VideoRoomAdapter:
    """Resolve live handles and issue direct typed VideoRoom commands."""

    def __init__(self, runtime: RuntimeProtocol, registry: JrtcHandleRegistry) -> None:
        self.runtime = runtime
        self.registry = registry

    def get_session(self, key: str | int | None = None) -> Any:
        """Return the process-local ready session selected for ``key``."""

        try:
            session = self.runtime.session(key=key)
        except Exception as exc:
            raise JrtcSessionUnavailable("No process-local Janus session is available.") from exc
        if session is None or not bool(getattr(session, "ready", False)):
            raise JrtcSessionUnavailable("The selected Janus session is not active.")
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
        """Invoke one command through a fresh attach/invoke/detach handle.

        Once Janus has returned a successful command response, a best-effort
        detach failure is logged and does not make callers repeat a state-
        changing room operation.
        """

        session = self.get_session(session_key)
        plugin = VideoRoomPlugin(session=session)
        attached = False
        command_succeeded = False
        try:
            await plugin.attach()
            attached = True
            method = getattr(plugin, method_name, None)
            if not callable(method):
                raise VideoRoomProtocolError(
                    f"VideoRoomPlugin does not expose command {method_name!r}."
                )
            result = await method(*tuple(args), **dict(kwargs or {}))
            command_succeeded = True
            return result
        except PackageVideoRoomProtocolError as exc:
            raise VideoRoomProtocolError(str(exc)) from exc
        except VideoRoomProtocolError:
            raise
        except (VideoRoomError, JanusException, TimeoutError, RuntimeError) as exc:
            raise VideoRoomCommandError(
                f"VideoRoom management command {method_name!r} failed."
            ) from exc
        finally:
            if attached:
                try:
                    await plugin.detach()
                except Exception:
                    if command_succeeded:
                        logger.warning(
                            "VideoRoom command %s succeeded but temporary handle detach failed",
                            method_name,
                            exc_info=True,
                        )
                    else:
                        logger.debug(
                            "Temporary VideoRoom handle cleanup also failed",
                            exc_info=True,
                        )

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

    async def complete_trickle(self, binding: BoundVideoRoomHandle) -> Any:
        return await self.invoke(binding, "complete_trickle")

    async def hangup(self, binding: BoundVideoRoomHandle) -> Any:
        # Janus hangup and plugin detach are distinct lifecycle operations.
        # Callers that intend to destroy the handle must invoke ``detach``.
        return await self.invoke(binding, "hangup")

    async def detach(self, binding: BoundVideoRoomHandle) -> Any:
        return await self.registry.detach(binding.model_id, expected=binding)


__all__ = ["RuntimeProtocol", "VideoRoomAdapter"]
