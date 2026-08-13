from __future__ import annotations

import ast
import re
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"


def _declared_dependency_names() -> set[str]:
    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    requirements: list[str] = list(project["project"].get("dependencies", ()))
    for group in project["project"].get("optional-dependencies", {}).values():
        requirements.extend(group)
    for group in project.get("dependency-groups", {}).values():
        requirements.extend(item for item in group if isinstance(item, str))

    return {
        re.split(r"[<>=!~;@\[\s]", requirement, maxsplit=1)[0].casefold().replace("_", "-")
        for requirement in requirements
    }


def test_reactivex_is_not_a_declared_dependency() -> None:
    assert "reactivex" not in _declared_dependency_names()


def test_source_contains_no_reactivex_imports_or_references() -> None:
    import_offenders: list[str] = []
    reference_offenders: list[str] = []

    for source_file in SOURCE_ROOT.rglob("*.py"):
        source = source_file.read_text(encoding="utf-8")
        relative_path = source_file.relative_to(PROJECT_ROOT).as_posix()
        if "reactivex" in source.casefold():
            reference_offenders.append(relative_path)

        tree = ast.parse(source, filename=str(source_file))
        for node in ast.walk(tree):
            modules: tuple[str, ...] = ()
            if isinstance(node, ast.Import):
                modules = tuple(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = (node.module,)
            if any(module.split(".", maxsplit=1)[0] == "reactivex" for module in modules):
                import_offenders.append(relative_path)

    assert import_offenders == []
    assert reference_offenders == []


def test_separate_sdp_event_api_is_removed() -> None:
    offenders: list[str] = []
    for source_file in SOURCE_ROOT.rglob("*.py"):
        source = source_file.read_text(encoding="utf-8")
        if "janus.sdp" in source or "Events.SDP" in source:
            offenders.append(source_file.relative_to(PROJECT_ROOT).as_posix())

    assert offenders == []
