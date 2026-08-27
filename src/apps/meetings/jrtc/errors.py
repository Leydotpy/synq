"""Stable Synq exceptions for the process-local JRTC integration.

Package and broker exceptions are deliberately translated at this boundary so
Socket.IO and HTTP clients never depend on JRTC, JRTC Video, or Broka classes.
Detailed causes remain available through normal exception chaining.
"""

from __future__ import annotations

from apps.meetings.exceptions import JanusGatewayError


class JrtcError(JanusGatewayError):
    """Base class for failures in Synq's JRTC integration."""


class JrtcRuntimeUnavailable(JrtcError):
    """The process-local runtime is stopped, starting, or failed."""


class JrtcSessionUnavailable(JrtcError):
    """No active Janus session can own the requested command."""


class JrtcHandleUnavailable(JrtcError):
    """No usable live plugin handle is bound to the domain record."""


class JrtcHandleOwnershipError(JrtcHandleUnavailable):
    """A live handle belongs to a different runtime owner."""


class JrtcStaleHandleError(JrtcHandleUnavailable):
    """Persisted correlation IDs no longer identify a live local handle."""


class JrtcEventCorrelationError(JrtcError):
    """A broker event could not be correlated to a current domain handle."""


class JrtcBrokerUnavailable(JrtcError):
    """The configured application event broker is unavailable."""


class JrtcBrokerPublishFailure(JrtcError):
    """An asynchronous Janus event could not be published."""


class JrtcBrokerConsumerFailure(JrtcError):
    """The authoritative event consumer could not continue safely."""


class JrtcBrowserDispatchFailure(JrtcBrokerConsumerFailure):
    """An authorized durable event could not reach its Socket.IO target."""


class VideoRoomCommandError(JrtcError):
    """Janus rejected or failed a VideoRoom command."""


class VideoRoomProtocolError(VideoRoomCommandError):
    """A VideoRoom request or response violated the integration contract."""


__all__ = [
    "JrtcBrowserDispatchFailure",
    "JrtcBrokerConsumerFailure",
    "JrtcBrokerPublishFailure",
    "JrtcBrokerUnavailable",
    "JrtcError",
    "JrtcEventCorrelationError",
    "JrtcHandleOwnershipError",
    "JrtcHandleUnavailable",
    "JrtcRuntimeUnavailable",
    "JrtcSessionUnavailable",
    "JrtcStaleHandleError",
    "VideoRoomCommandError",
    "VideoRoomProtocolError",
]
