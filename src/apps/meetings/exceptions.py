"""Domain-specific exceptions for meeting lifecycle and moderation workflows."""

from django.core.exceptions import PermissionDenied
from logvista import get_logger


logger = get_logger(__name__)


class MeetingDomainError(Exception):
    """Base exception raised when a meeting-domain invariant is violated."""

    def __init__(self, msg: str) -> None:
        super().__init__(msg)
        logger.error("meeting.domain_error", message=type(self).__name__)


class MeetingPermissionDeniedError(PermissionDenied, MeetingDomainError):
    """Raised when a user attempts an action without the required coordinator privileges."""


class MeetingJoinRequestStateError(MeetingDomainError):
    """Raised when a join request is modified from an invalid lifecycle state."""


class JanusGatewayError(MeetingDomainError):
    """Raised when Janus room or handle operations fail."""
