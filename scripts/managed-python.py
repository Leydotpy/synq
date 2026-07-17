"""Run a Python entry point while publishing this process's identity.

The PowerShell supervisor starts Python through ``uv``, so the process it owns
directly is a wrapper rather than the long-lived application process.  This
launcher writes the actual Python PID before executing the requested script in
the same process, allowing shutdown to target it safely even if ``uv`` exits.
"""

from __future__ import annotations

import json
import os
import runpy
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) < 4:
        raise SystemExit(
            "usage: managed-python.py IDENTITY_PATH RUN_TOKEN "
            "(ENTRY_POINT | -m MODULE) [ARG ...]"
        )

    identity_path = Path(sys.argv[1])
    run_token = sys.argv[2]

    identity_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = identity_path.with_name(
        f"{identity_path.name}.{os.getpid()}.tmp"
    )
    temporary_path.write_text(
        json.dumps({"pid": os.getpid(), "runToken": run_token}),
        encoding="utf-8",
    )
    os.replace(temporary_path, identity_path)

    if sys.argv[3] == "-m":
        if len(sys.argv) < 5:
            raise SystemExit("managed-python.py: -m requires a module name")
        module_name = sys.argv[4]
        sys.path[0] = os.getcwd()
        sys.argv = [module_name, *sys.argv[5:]]
        runpy.run_module(module_name, run_name="__main__", alter_sys=True)
        return

    entry_point = Path(sys.argv[3]).resolve()
    entry_arguments = sys.argv[4:]
    # Match ``python entry_point ...`` closely: the entry-point directory is
    # importable and the application receives the original argv shape.
    sys.path[0] = str(entry_point.parent)
    sys.argv = [str(entry_point), *entry_arguments]
    runpy.run_path(str(entry_point), run_name="__main__")


if __name__ == "__main__":
    main()
