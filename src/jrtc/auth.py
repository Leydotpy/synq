"""Credential providers for authenticated Janus API sessions."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from jrtc.models.request import BaseJanusRequest


@dataclass(frozen=True, slots=True, repr=False)
class JanusCredentials:
    """Outer-envelope credentials required by an authenticated Janus server.

    Values are deliberately redacted from ``repr`` and are applied immediately
    before transport serialization.  Both mechanisms may be configured by a
    server, although most deployments use one or the other.
    """

    token: str | None = None
    api_secret: str | None = None

    def __post_init__(self) -> None:
        if self.token is not None and not self.token:
            raise ValueError("token cannot be empty")
        if self.api_secret is not None and not self.api_secret:
            raise ValueError("api_secret cannot be empty")

    def __repr__(self) -> str:
        return (
            "JanusCredentials("
            f"token={'<redacted>' if self.token else None}, "
            f"api_secret={'<redacted>' if self.api_secret else None})"
        )

    def apply(self, request: BaseJanusRequest) -> None:
        if request.token is None:
            request.token = self.token
        if request.apisecret is None:
            request.apisecret = self.api_secret


@runtime_checkable
class JanusCredentialProvider(Protocol):
    """Provider used when credentials need rotation between requests."""

    def __call__(self) -> JanusCredentials: ...


type CredentialSource = JanusCredentials | Callable[[], JanusCredentials] | None


def resolve_credentials(source: CredentialSource) -> JanusCredentials | None:
    if source is None or isinstance(source, JanusCredentials):
        return source
    value = source()
    if not isinstance(value, JanusCredentials):
        raise TypeError("credential provider must return JanusCredentials")
    return value
