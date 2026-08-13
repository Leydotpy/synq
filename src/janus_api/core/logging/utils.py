"""Explicit logging handler factories with no import-time configuration."""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Mapping
from contextlib import suppress
from logging.handlers import RotatingFileHandler, WatchedFileHandler
from pathlib import Path
from typing import TextIO

from janus_api.core.logging._json import JsonFormatter
from janus_api.core.logging.formatting import ColoredFormatter

_DEFAULT_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
_DEFAULT_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
_OWNED_HANDLER = "_janus_core_owned_handler"


def _prepare_log_path(path: str | Path) -> Path:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(target, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    os.close(descriptor)
    with suppress(OSError):
        target.chmod(0o600)
    return target


def _file_handler(
    path: str | Path,
    *,
    rotation: str,
    max_bytes: int,
    backup_count: int,
) -> logging.FileHandler:
    target = _prepare_log_path(path)
    if rotation == "watched":
        return WatchedFileHandler(target, encoding="utf-8")
    if rotation != "size":
        raise ValueError("rotation must be 'size' or 'watched'")
    if max_bytes < 1 or backup_count < 1:
        raise ValueError("max_bytes and backup_count must be positive")
    return RotatingFileHandler(
        target,
        mode="a",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )


def get_colored_stream_handler(
    level: int = logging.INFO,
    fmt: str | None = _DEFAULT_FORMAT,
    datefmt: str | None = _DEFAULT_DATE_FORMAT,
    level_styles: Mapping[int, str] | None = None,
    use_color: bool = True,
    stream: TextIO | None = None,
) -> logging.StreamHandler[TextIO]:
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(
        ColoredFormatter(
            fmt=fmt,
            datefmt=datefmt,
            level_styles=level_styles,
            use_color=use_color,
        )
    )
    return handler


def get_json_file_handler(
    path: str | Path,
    level: int = logging.INFO,
    datefmt: str | None = _DEFAULT_DATE_FORMAT,
    *,
    rotation: str = "size",
    max_bytes: int = 50 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.FileHandler:
    handler = _file_handler(
        path,
        rotation=rotation,
        max_bytes=max_bytes,
        backup_count=backup_count,
    )
    handler.setLevel(level)
    handler.setFormatter(JsonFormatter(datefmt=datefmt))
    return handler


def get_plain_file_handler(
    path: str | Path,
    level: int = logging.INFO,
    fmt: str | None = _DEFAULT_FORMAT,
    datefmt: str | None = _DEFAULT_DATE_FORMAT,
    *,
    rotation: str = "size",
    max_bytes: int = 50 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.FileHandler:
    handler = _file_handler(
        path,
        rotation=rotation,
        max_bytes=max_bytes,
        backup_count=backup_count,
    )
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    return handler


def install_colored_logging(
    *,
    level: int = logging.INFO,
    root_logger: logging.Logger | None = None,
    keep_existing_handlers: bool = True,
    color_stdout: bool = True,
    logfile: str | Path | None = None,
    rotation: str = "size",
    max_bytes: int = 50 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """Idempotently install handlers owned by Janus Core.

    Use ``rotation='watched'`` when an external tool such as logrotate owns
    rotation. Size rotation is the safe standalone default.
    """

    target = root_logger or logging.getLogger()
    target.setLevel(level)
    if not keep_existing_handlers:
        for handler in tuple(target.handlers):
            target.removeHandler(handler)
            handler.close()
    for handler in tuple(target.handlers):
        if getattr(handler, _OWNED_HANDLER, False):
            target.removeHandler(handler)
            handler.close()
    stream_handler = get_colored_stream_handler(level=level, use_color=color_stdout)
    setattr(stream_handler, _OWNED_HANDLER, True)
    target.addHandler(stream_handler)
    if logfile is not None:
        file_handler = get_json_file_handler(
            logfile,
            level=level,
            rotation=rotation,
            max_bytes=max_bytes,
            backup_count=backup_count,
        )
        setattr(file_handler, _OWNED_HANDLER, True)
        target.addHandler(file_handler)
    return target


__all__ = [
    "get_colored_stream_handler",
    "get_json_file_handler",
    "get_plain_file_handler",
    "install_colored_logging",
]
