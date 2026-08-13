"""Exception hierarchy for the Janus client runtime.

The exceptions in this module are intentionally plugin agnostic.  Named plugin
packages should translate their own ``error_code`` payloads into package-local
exceptions while preserving :class:`JanusErrorResponse` as the cause.
"""

from __future__ import annotations

from typing import Any


class JanusException(Exception):
    """Base class for errors raised by the toolkit."""


class JanusConfigurationError(JanusException):
    """Raised when required runtime configuration is invalid or missing."""


class JanusProtocolError(JanusException):
    """Raised when a peer sends a malformed Janus protocol message."""


class JanusTransportError(JanusException):
    """Raised when a transport cannot send, receive, or maintain a connection."""


class JanusConnectionClosed(JanusTransportError):
    """Raised when an operation is interrupted because its transport closed."""


class JanusRequestTimeout(JanusTransportError, TimeoutError):
    """Raised when a Janus transaction does not complete before its deadline."""

    def __init__(self, transaction: str, timeout: float) -> None:
        self.transaction = transaction
        self.timeout = timeout
        super().__init__(f"Janus transaction {transaction!r} timed out after {timeout:g}s")


class JanusErrorResponse(JanusException):
    """A structured ``janus: error`` response returned by the gateway."""

    def __init__(
        self,
        code: int,
        reason: str,
        *,
        transaction: str | None = None,
        response: Any | None = None,
    ) -> None:
        self.code = code
        self.reason = reason
        self.transaction = transaction
        self.response = response
        super().__init__(f"Janus error {code}: {reason}")


class PluginManagerError(JanusException):
    """Base class for plugin registration and handle ownership errors."""


class PluginAlreadyRegistered(PluginManagerError):
    """Raised when a handle or plugin identifier is registered twice."""


class PluginNotRegistered(PluginManagerError, KeyError):
    """Raised when a plugin identifier or handle cannot be resolved."""


class PluginLoadError(PluginManagerError):
    """Raised when an installed plugin entry point cannot be imported safely."""
