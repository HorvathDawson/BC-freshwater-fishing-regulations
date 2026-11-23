"""T1031 (owner 2026-11-23: "'B.C. Synopsis' everywhere"): THE BOOK'S NAME IN WHAT THE PIPELINE WRITES FOR A SCREEN.

A section note or a salmon verdict the deliver stage writes is shown as is, so it names the book as the app does:
`pipeline/common/source_names.py` `SYNOPSIS`, equal to the app's one name (`sourceNames.ts`
`SOURCE_AUTHORITIES.synopsis.name`). "rule book", in any case, is refused in every string the deliver stage writes
(docstrings and comments are not written; the app's own guard is `app/tools/source-names.test.ts`).
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

from pipeline.common.source_names import SYNOPSIS

REPO = Path(__file__).resolve().parents[2]
DELIVER = REPO / "pipeline" / "deliver"
APP_NAMES = REPO / "app" / "packages" / "core" / "src" / "sourceNames.ts"


def written_strings(code: str) -> list[str]:
    """Every string constant of a module that is not a docstring (f-string parts included)."""
    tree = ast.parse(code)
    docs = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                docs.add(id(first.value))
    return [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docs]


def offences(code: str) -> list[str]:
    return [s for s in written_strings(code) if "rule book" in s.lower()]


def test_the_pipeline_names_the_book_as_the_app_does():
    m = re.search(r'synopsis:\s*\{\s*name:\s*"([^"]+)"', APP_NAMES.read_text(encoding="utf-8"))
    assert m, "sourceNames.ts: no SOURCE_AUTHORITIES.synopsis.name"
    assert SYNOPSIS == m.group(1)


def test_no_deliver_module_writes_rule_book():
    hits = [f"{p.relative_to(REPO)}: {s[:80]!r}" for p in sorted(DELIVER.rglob("*.py"))
            for s in offences(p.read_text(encoding="utf-8"))]
    assert hits == []


def test_the_writers_read_the_one_name():
    from pipeline.deliver import salmon
    from pipeline.deliver.bundle import outside_note
    assert salmon.BOOK_WORDS == f"Closed to all fishing by the {SYNOPSIS}"
    assert salmon.BOOK_UNLESS_WORDS == f"Closed by the {SYNOPSIS} unless"
    got = outside_note.notes([("d", "area:x", "s", ["i"])], {"area:x": "X Reserve"}, {"area:x": [9]}, {"i": [1]})
    assert got == {1: "Mostly outside X Reserve: its closure (B.C. Synopsis p.9) does not apply here."}


def test_the_check_catches_a_written_name_and_not_a_docstring_or_comment():
    assert offences('X = "its closure (rule book p.9)"') == ["its closure (rule book p.9)"]
    assert offences('def f(p):\n    return f"Closed by the Rule Book {p}"') == ["Closed by the Rule Book "]
    assert offences('"""the rule book, documented"""\n# a rule book comment\nY = "B.C. Synopsis"') == []
