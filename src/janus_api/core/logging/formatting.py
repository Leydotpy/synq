"""Small ANSI-aware formatter for explicitly configured console logging."""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from typing import Literal

RESET = "\x1b[0m"
DEFAULT_LEVEL_STYLES: Mapping[int, str] = {
    logging.DEBUG: "\x1b[96m",
    logging.INFO: "\x1b[92m",
    logging.WARNING: "\x1b[1;93m",
    logging.ERROR: "\x1b[1;91m",
    logging.CRITICAL: "\x1b[1;91m",
}


class ColoredFormatter(logging.Formatter):
    """Color selected fields on a copied record without mutating shared records."""

    _ansi = re.compile(r"\x1b\[[0-9;]*m")

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Literal["%", "{", "$"] = "%",
        level_styles: Mapping[int, str] | None = None,
        use_color: bool = True,
        color_targets: Sequence[str] = ("levelname", "message"),
    ) -> None:
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)
        self.level_styles = dict(level_styles or DEFAULT_LEVEL_STYLES)
        self.use_color = use_color
        self.color_targets = frozenset(color_targets)

    def format(self, record: logging.LogRecord) -> str:
        prefix = self.level_styles.get(record.levelno, "") if self.use_color else ""
        if not prefix:
            return super().format(record)
        copy = logging.makeLogRecord(record.__dict__.copy())
        if "name" in self.color_targets:
            copy.name = f"{prefix}{copy.name}{RESET}"
        if "levelname" in self.color_targets:
            copy.levelname = f"{prefix}{copy.levelname}{RESET}"
        if "message" in self.color_targets:
            copy.msg = f"{prefix}{record.getMessage()}{RESET}"
            copy.args = ()
        return super().format(copy)

    @classmethod
    def strip_ansi(cls, value: str) -> str:
        return cls._ansi.sub("", value)


__all__ = ["DEFAULT_LEVEL_STYLES", "RESET", "ColoredFormatter"]
