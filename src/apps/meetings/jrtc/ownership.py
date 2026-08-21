"""Process identity for live JRTC session and plugin ownership."""

from __future__ import annotations

import os
import socket
from uuid import uuid4


def new_runtime_owner_id() -> str:
    """Return a unique, non-secret identity for one process runtime instance."""

    # Keep the diagnostic identity within the model's 255-character column
    # even on platforms that permit unusually long host names.
    host = (socket.gethostname() or "unknown-host")[:128]
    return f"{host}:{os.getpid()}:{uuid4()}"


__all__ = ["new_runtime_owner_id"]
