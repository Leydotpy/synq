"""Opt-in JSON Lines logging formatter."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar


class JsonFormatter(logging.Formatter):
    """Serialize standard log fields and safe ``extra`` values as one JSON object."""

    _standard: ClassVar[frozenset[str]] = frozenset(logging.makeLogRecord({}).__dict__)
    _sensitive: ClassVar[tuple[str, ...]] = (
        "authorization",
        "cookie",
        "apisecret",
        "secret",
        "token",
        "password",
        "credential",
        "api_key",
    )
    _sensitive_text: ClassVar[re.Pattern[str]] = re.compile(
        r"(?i)((?:token|apisecret|api[_-]?key|authorization|cookie|password|secret)"
        r"(?:\s*[:=]\s*|%3[dD]))([^&\s,;]+)"
    )
    _authorization_text: ClassVar[re.Pattern[str]] = re.compile(
        r"(?i)(authorization\s*:\s*)(?:(?:bearer|basic)\s+)?[^\s,;]+"
    )

    @classmethod
    def _redact_text(cls, value: str) -> str:
        value = cls._authorization_text.sub(r"\1<redacted>", value)
        return cls._sensitive_text.sub(r"\1<redacted>", value)

    @classmethod
    def _sanitize(cls, value: Any, *, depth: int = 0) -> Any:
        if depth >= 6:
            return "<max-depth>"
        if isinstance(value, str):
            return cls._redact_text(value[:16_384])
        if isinstance(value, Mapping):
            mapping_output: dict[str, Any] = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= 256:
                    mapping_output["<truncated>"] = len(value) - index
                    break
                label = str(key)
                mapping_output[label] = (
                    "<redacted>"
                    if any(part in label.lower() for part in cls._sensitive)
                    else cls._sanitize(item, depth=depth + 1)
                )
            return mapping_output
        if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
            items = list(value[:256])
            sequence_output = [cls._sanitize(item, depth=depth + 1) for item in items]
            if len(value) > len(items):
                sequence_output.append(f"<{len(value) - len(items)} items truncated>")
            return sequence_output
        return value

    @classmethod
    def _extra(cls, record: logging.LogRecord) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for key, value in record.__dict__.items():
            if key in cls._standard or key in {"message", "asctime"} or key.startswith("_"):
                continue
            values[key] = (
                "<redacted>"
                if any(part in key.lower() for part in cls._sensitive)
                else cls._sanitize(value)
            )
        return values

    def format(self, record: logging.LogRecord) -> str:
        document: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "logger": record.name,
            "level": record.levelname,
            "message": self._redact_text(record.getMessage()),
        }
        extras = self._extra(record)
        if extras:
            document["extra"] = extras
        if record.exc_info:
            document["exception"] = self._redact_text(self.formatException(record.exc_info))
        if record.stack_info:
            document["stack"] = self.formatStack(record.stack_info)
        return json.dumps(document, ensure_ascii=False, default=str, separators=(",", ":"))


__all__ = ["JsonFormatter"]
