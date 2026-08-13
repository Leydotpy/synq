"""Workspace distribution-boundary tests for core and the operations server."""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path

import jrtc

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CORE_SOURCE = PROJECT_ROOT / "src" / "jrtc"
SERVER_SOURCE = PROJECT_ROOT / "packages" / "japi" / "src" / "japi"


def _imports(source_file: Path) -> set[str]:
    tree = ast.parse(source_file.read_text(encoding="utf-8"), filename=str(source_file))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    return imported


def _offending_imports(source_root: Path, forbidden_roots: set[str]) -> list[str]:
    offenders: list[str] = []
    for source_file in source_root.rglob("*.py"):
        for module in _imports(source_file):
            if module.partition(".")[0] in forbidden_roots:
                relative = source_file.relative_to(PROJECT_ROOT).as_posix()
                offenders.append(f"{relative}: {module}")
    return sorted(offenders)


def test_session_manager_remains_a_public_core_api() -> None:
    from jrtc import JanusSessionManager
    from jrtc.session import JanusSessionManager as SessionManager

    assert JanusSessionManager is SessionManager
    assert "JanusSessionManager" in jrtc.__all__


def test_core_does_not_depend_on_the_operations_server_stack() -> None:
    assert (
        _offending_imports(
            CORE_SOURCE,
            {"fastapi", "starlette", "asyncpg", "japi"},
        )
        == []
    )


def test_server_never_uses_aiokafka_producer_directly() -> None:
    assert SERVER_SOURCE.is_dir(), "the japi source package must exist"

    direct_imports = _offending_imports(SERVER_SOURCE, {"aiokafka"})
    direct_references: list[str] = []
    for source_file in SERVER_SOURCE.rglob("*.py"):
        source = source_file.read_text(encoding="utf-8")
        if "AIOKafkaProducer" in source:
            direct_references.append(source_file.relative_to(PROJECT_ROOT).as_posix())

    assert direct_imports == []
    assert direct_references == []


def test_server_contains_no_manager_rest_surface() -> None:
    manager_paths = [
        path.relative_to(PROJECT_ROOT).as_posix()
        for path in SERVER_SOURCE.rglob("*")
        if path.name.casefold() == "manager" or path.name.casefold().startswith("manager_")
    ]
    manager_mounts: list[str] = []
    for source_file in SERVER_SOURCE.rglob("*.py"):
        source = source_file.read_text(encoding="utf-8")
        if '"/manager"' in source or "'/manager'" in source:
            manager_mounts.append(source_file.relative_to(PROJECT_ROOT).as_posix())

    assert manager_paths == []
    assert manager_mounts == []


def test_distribution_metadata_enforces_the_one_way_dependency_boundary() -> None:
    core = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    server = tomllib.loads(
        (PROJECT_ROOT / "packages" / "japi" / "pyproject.toml").read_text(
            encoding="utf-8"
        )
    )

    assert core["project"]["name"] == "jrtc"
    assert server["project"]["name"] == "japi"
    assert core["tool"]["setuptools"]["packages"]["find"]["include"] == ["jrtc*"]
    assert server["tool"]["setuptools"]["packages"]["find"]["include"] == ["japi*"]

    core_requirements = "\n".join(core["project"]["dependencies"]).casefold()
    assert "fastapi" not in core_requirements
    assert "starlette" not in core_requirements
    assert "asyncpg" not in core_requirements
    assert "japi" not in core_requirements

    server_requirements = "\n".join(server["project"]["dependencies"]).casefold()
    assert "jrtc==3.1.0" in server_requirements
    assert "fastapi" in server_requirements
    assert server["tool"]["uv"]["sources"]["jrtc"] == {"workspace": True}
