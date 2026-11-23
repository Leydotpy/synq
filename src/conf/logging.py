"""Django logging configuration for private meeting requests."""

import logging
from logging.config import dictConfig

HTTP_METHODS = {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}


class MeetingRequestFilter(logging.Filter):
    def filter(self, record):
        args = record.args if isinstance(record.args, tuple) else ()
        request = getattr(record, "request", None)
        paths = (getattr(request, "path_info", None), *args)
        if not any(
            isinstance(path, str) and "/api/v1/meetings/" in path for path in paths
        ):
            return True

        # Error handlers can serialize the request or exception independently of msg.
        record.request = None
        record.exc_info = record.exc_text = record.stack_info = None
        if record.name == "uvicorn.access" and len(args) == 5:
            _, method, _, version, status = args
            record.msg = '%s - "%s %s HTTP/%s" %d'
            record.args = (
                "-",
                method if method in HTTP_METHODS else "-",
                "/api/v1/meetings/",
                version if version in {"1.0", "1.1", "2", "3"} else "-",
                status if type(status) is int else 0,
            )
        else:
            status = getattr(record, "status_code", None)
            record.msg = "Meeting HTTP response (%s)"
            record.args = (status if type(status) is int else "unknown",)
        return True


_meeting_filter = MeetingRequestFilter()


def configure_logging(config):
    if config:
        dictConfig(config)
    for name in (
        "django.request",
        "django.security.csrf",
        "django.server",
        "uvicorn.access",
    ):
        logging.getLogger(name).addFilter(_meeting_filter)
