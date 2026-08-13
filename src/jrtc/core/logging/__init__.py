"""Opt-in logging utilities; importing Janus Core never configures logging."""

from jrtc.core.logging._json import JsonFormatter
from jrtc.core.logging.formatting import ColoredFormatter
from jrtc.core.logging.utils import (
    get_colored_stream_handler,
    get_json_file_handler,
    get_plain_file_handler,
    install_colored_logging,
)

__all__ = [
    "ColoredFormatter",
    "JsonFormatter",
    "get_colored_stream_handler",
    "get_json_file_handler",
    "get_plain_file_handler",
    "install_colored_logging",
]
