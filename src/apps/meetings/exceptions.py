"""Domain-specific exceptions for meeting lifecycle and moderation workflows."""

from django.core.exceptions import PermissionDenied


class MeetingDomainError(Exception):
    """Base exception raised when a meeting-domain invariant is violated."""


class MeetingPermissionDeniedError(PermissionDenied, MeetingDomainError):
    """Raised when a user attempts an action without the required coordinator privileges."""


class MeetingJoinRequestStateError(MeetingDomainError):
    """Raised when a join request is modified from an invalid lifecycle state."""


class JanusGatewayError(MeetingDomainError):
    """Raised when Janus room or handle operations fail."""
