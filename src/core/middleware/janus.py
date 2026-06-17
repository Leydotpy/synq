"""Middleware that exposes the process-local Janus session on Django requests."""

from __future__ import annotations

from janus_api import Janus


class JanusSessionMiddleware:
    """Attach the current ``janus_api`` session handle to each incoming request."""

    def __init__(self, get_response):
        """Store Django's downstream request callable."""

        self.get_response = get_response

    def __call__(self, request):
        """Expose the shared Janus session so views do not need to import global state."""

        request.janus = Janus.get_session()
        request.janus_session = request.janus
        return self.get_response(request)
