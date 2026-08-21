"""Deprecated compatibility middleware for the retired request Janus coupling."""

from __future__ import annotations

class JanusSessionMiddleware:
    """Pass requests through without acquiring process-local JRTC resources.

    The class remains importable for deployments whose settings have not yet
    been rolled forward.  Media services acquire JRTC explicitly; ordinary
    HTTP requests no longer receive ``request.janus`` or
    ``request.janus_session`` attributes.
    """

    def __init__(self, get_response):
        """Store Django's downstream request callable."""

        self.get_response = get_response

    def __call__(self, request):
        """Continue the middleware chain without hidden session acquisition."""

        return self.get_response(request)
