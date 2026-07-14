"""Compatibility imports for the canonical API authentication module.

New code should import from :mod:`core.api.authentication`.  This module keeps
the previous dotted paths usable for deployments that still reference them.
"""

from core.api.authentication import (
    CLERK_AUTH_CACHE_PREFIX,
    DEFAULT_TOKEN_EXPIRY_LEEWAY_SECONDS,
    LazyClerkAuthentication,
    forget_clerk_token,
    get_bearer_token,
    get_clerk_token_cache_key,
    mark_request_anonymous_for_expired_clerk_token,
    request_has_expired_clerk_token,
    token_is_expired,
)

__all__ = (
    "CLERK_AUTH_CACHE_PREFIX",
    "DEFAULT_TOKEN_EXPIRY_LEEWAY_SECONDS",
    "LazyClerkAuthentication",
    "forget_clerk_token",
    "get_bearer_token",
    "get_clerk_token_cache_key",
    "mark_request_anonymous_for_expired_clerk_token",
    "request_has_expired_clerk_token",
    "token_is_expired",
)
