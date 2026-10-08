"""THE ONE-DATA-FLOW GATES (DATAFLOW §3), structural: they read the SOURCE as an AST, never a regex
a new spelling can walk past (finding H2: three closure spellings walked past the old grep).

  1 reader    nothing outside `bundle/read.py`, `deliver/verdicts/` and the named ORACLES calls
              `effective_rules` / `effective_rules_bound`: every other stage looks the verdict up
  2 closure   nothing downstream of the bundle reads `may_target` but `bundle/rules.py` (the one
              closure predicate, `closure_grade`, and `catch_and_release`) and the export's record
              writer that ships the field
  3 parts     no SQL naming `province_except` beside `section_ruleset` outside the bundle builder:
              the part partition is written once, by the bundle (`part`)
  4 calendar  `_day_index` / `_LAST_DAY` are read in `deliver/calendar.py` and the catalogue only

Each gate is MUTATION-PINNED: a violation planted in a copy of the sources turns it red.

`PENDING` lists the call sites a later phase of the refactor removes, each with that phase; the
list only shrinks, and at the end of the refactor it is empty (`test_pending_is_empty_at_the_end`
names what is left). An oracle is not pending: it re-asks the reader on purpose, to check a
stored answer against an output it did not build.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from pipeline.common.curated import REPO_ROOT

PIPELINE = REPO_ROOT / "pipeline"


def _sources() -> dict[str, str]:
    """Every production module of the pipeline (tests excluded), by repo-relative path."""
    return {str(p.relative_to(REPO_ROOT)): p.read_text(encoding="utf-8")
            for p in sorted(PIPELINE.rglob("*.py"))
            if "tests" not in p.relative_to(PIPELINE).parts and "__pycache__" not in p.parts}


class _Walk(ast.NodeVisitor):
    """Visits a module keeping the qualified name of the enclosing defs/classes (`where`) and of
    the OUTERMOST function holding the node (`unit`: a method's name with its class, nested
    helpers rolled into their def)."""

    def __init__(self, on):
        self.stack: list[tuple[str, bool]] = []
        self.on = on

    def _scoped(self, node, is_def: bool):
        self.stack.append((node.name, is_def))
        self.generic_visit(node)
        self.stack.pop()

    def visit_FunctionDef(self, node):
        self._scoped(node, True)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node):
        self._scoped(node, False)

    def generic_visit(self, node):
        names = [n for n, _ in self.stack]
        first = next((i for i, (_, d) in enumerate(self.stack) if d), None)
        unit = ".".join(names[:first + 1]) if first is not None else (".".join(names) or "<module>")
        self.on(node, ".".join(names) or "<module>", unit)
        super().generic_visit(node)


def _visit(text: str, on) -> None:
    _Walk(on).visit(ast.parse(text))


# --------------------------------------------------------------------------------------------
# 1 the reader
# --------------------------------------------------------------------------------------------

READER = {"effective_rules", "effective_rules_bound"}

#: Re-ask the reader ON PURPOSE: each checks a stored answer against an output it did not build.
READER_ORACLES = {
    ("pipeline/deliver/status_index.py", "status_by_reader"),
    ("pipeline/tools/export_ui_rules.py", "closures_combine_problems"),
}

#: Call sites a later phase removes (phase in the value). Only shrinks.
READER_PENDING: dict[tuple[str, str], str] = {
    ("pipeline/tools/export_ui_rules.py", "_Cases.answer"): "P5",
    ("pipeline/tools/export_ui_rules.py", "_closure_scan"): "P5",
    ("pipeline/tools/export_ui_rules.py", "closures_combine.speaks"): "P5",
    ("pipeline/tools/guide_examples.py", "examples"): "P5",
    ("pipeline/deliver/answers/answers.py", "_ladder_prepare"): "P6",
    ("pipeline/deliver/answers/rows.py", "open_states"): "P6",
    ("pipeline/deliver/answers/gear.py", "states"): "P6",
}


def reader_calls(sources: dict[str, str]) -> set[tuple[str, str]]:
    """(module, qualname) of every call of the reader outside its own module and the verdicts."""
    out: set = set()
    for path, text in sources.items():
        if path == "pipeline/deliver/bundle/read.py" or path.startswith("pipeline/deliver/verdicts/"):
            continue

        def on(node, where, unit, path=path):
            if isinstance(node, ast.Call):
                f = node.func
                name = f.attr if isinstance(f, ast.Attribute) else f.id if isinstance(f, ast.Name) \
                    else None
                if name in READER:
                    out.add((path, where))
        _visit(text, on)
    return out


def test_gate_1_only_the_verdicts_call_the_reader():
    got = reader_calls(_sources()) - READER_ORACLES
    assert got - set(READER_PENDING) == set(), "a new reader call outside the verdicts stage"
    assert set(READER_PENDING) - got == set(), \
        "a PENDING call site is gone: take it off READER_PENDING (the list only shrinks)"


def test_gate_1_goes_red_on_a_planted_call():
    src = dict(_sources())
    src["pipeline/deliver/answers/display.py"] += (
        "\n\ndef planted(b, md):\n    from pipeline.deliver.bundle import read\n"
        "    return read.effective_rules_bound(b, False, md, 'RB')\n")
    assert ("pipeline/deliver/answers/display.py", "planted") in reader_calls(src)


# --------------------------------------------------------------------------------------------
# 2 closure
# --------------------------------------------------------------------------------------------

#: The export's record writer ships `may_target` as a field (0/1 -> false/true); it decides nothing.
MAY_TARGET_WRITERS = {("pipeline/tools/export_ui_rules.py", "_rule_record")}


def may_target_reads(sources: dict[str, str]) -> set[tuple[str, str]]:
    """(module, qualname) of every READ of `may_target` — `x["may_target"]`, `x.get("may_target")`,
    `getattr(x, "may_target")`, `x.may_target`, a comparison with "may_target" — downstream of the
    bundle (`pipeline/deliver`, `pipeline/tools`), outside `bundle/rules.py` and the bundle writer."""
    out: set = set()
    for path, text in sources.items():
        if not path.startswith(("pipeline/deliver/", "pipeline/tools/")):
            continue
        if path in ("pipeline/deliver/bundle/rules.py", "pipeline/deliver/bundle/build.py"):
            continue

        def on(node, where, unit, path=path):
            hit = False
            if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant) \
                    and node.slice.value == "may_target":
                hit = True
            elif isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant) \
                    and node.args[0].value == "may_target" and isinstance(node.func, ast.Attribute) \
                    and node.func.attr in ("get", "pop", "setdefault"):
                hit = True
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                    and node.func.id == "getattr" and len(node.args) > 1 \
                    and isinstance(node.args[1], ast.Constant) and node.args[1].value == "may_target":
                hit = True
            elif isinstance(node, ast.Attribute) and node.attr == "may_target":
                hit = True
            elif isinstance(node, ast.Compare) and any(
                    isinstance(c, ast.Constant) and c.value == "may_target"
                    for c in [node.left, *node.comparators]):
                hit = True
            if hit:
                out.add((path, where))
        _visit(text, on)
    return out


def test_gate_2_closure_is_read_in_one_place():
    assert may_target_reads(_sources()) - MAY_TARGET_WRITERS == set()


def test_gate_2_goes_red_on_a_planted_spelling():
    for planted in ("v = x.get('may_target')\n    return v is not None and not v",
                    "return x['may_target'] == 0", "return not getattr(x, 'may_target')"):
        src = dict(_sources())
        src["pipeline/deliver/answers/rows.py"] += f"\n\ndef planted(x):\n    {planted}\n"
        assert ("pipeline/deliver/answers/rows.py", "planted") in may_target_reads(src), planted


# --------------------------------------------------------------------------------------------
# 3 parts
# --------------------------------------------------------------------------------------------

#: The bundle builder writes the partition (`part`): the only SQL that groups sections into parts.
PART_WRITERS = {"pipeline/deliver/bundle/build.py", "pipeline/deliver/bundle/derived.py",
                "pipeline/deliver/bundle/licensing.py"}

#: Counts sections per table (the export's `about.sections`): names both tables, groups nothing.
PART_TALLIES = {("pipeline/tools/export_ui_rules.py", "section_counts")}

PARTS_PENDING: dict[tuple[str, str], str] = {
    ("pipeline/deliver/answers/licence.py", "section_keys"): "P6",
}


def part_sql(sources: dict[str, str]) -> set[tuple[str, str]]:
    """(module, qualname) of every def whose string constants name both `province_except` and
    `section_ruleset` — a section grouping by the part's facts — outside the bundle builder."""
    out: set = set()
    for path, text in sources.items():
        if path in PART_WRITERS:
            continue
        seen: dict[str, set] = {}

        def on(node, where, unit):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                    and ("FROM " in node.value or "JOIN " in node.value):       # SQL, not prose
                for tok in ("province_except", "section_ruleset"):
                    if tok in node.value:
                        seen.setdefault(unit, set()).add(tok)
        _visit(text, on)
        out |= {(path, unit) for unit, toks in seen.items() if len(toks) == 2}
    return out


def test_gate_3_the_part_partition_is_written_once():
    got = part_sql(_sources()) - PART_TALLIES
    assert got - set(PARTS_PENDING) == set(), "a new grouping of sections into parts"
    assert set(PARTS_PENDING) - got == set(), \
        "a PENDING part SQL is gone: take it off PARTS_PENDING (the list only shrinks)"


def test_gate_3_goes_red_on_planted_part_sql():
    src = dict(_sources())
    src["pipeline/deliver/answers/display.py"] += (
        "\n\ndef planted(db):\n    return db.execute('SELECT r.set_id, p.area_kind FROM "
        "section_ruleset r JOIN province_except p ON p.sid = r.sid')\n")
    assert ("pipeline/deliver/answers/display.py", "planted") in part_sql(src)


# --------------------------------------------------------------------------------------------
# 4 calendar
# --------------------------------------------------------------------------------------------

CALENDAR_OWNERS = {"pipeline/deliver/calendar.py", "pipeline/regs/parsing/catalogue.py"}


def calendar_uses(sources: dict[str, str]) -> set[tuple[str, str]]:
    out: set = set()
    for path, text in sources.items():
        if path in CALENDAR_OWNERS:
            continue

        def on(node, where, unit, path=path):
            if isinstance(node, ast.Name) and node.id in ("_day_index", "_LAST_DAY"):
                out.add((path, where))
            elif isinstance(node, ast.Attribute) and node.attr in ("_day_index", "_LAST_DAY"):
                out.add((path, where))
            elif isinstance(node, ast.alias) and node.name in ("_day_index", "_LAST_DAY"):
                out.add((path, where))
        _visit(text, on)
    return out


def test_gate_4_one_calendar():
    assert calendar_uses(_sources()) == set()


def test_gate_4_goes_red_on_a_planted_calendar():
    src = dict(_sources())
    src["pipeline/tools/export_ui_rules.py"] += (
        "\n\ndef planted(m, d):\n    return C._day_index(m, d)\n")
    assert ("pipeline/tools/export_ui_rules.py", "planted") in calendar_uses(src)


# --------------------------------------------------------------------------------------------
# The refactor's end state
# --------------------------------------------------------------------------------------------

@pytest.mark.xfail(strict=True, reason="DATAFLOW refactor in progress: P-phases still pending")
def test_pending_is_empty_at_the_end():
    assert READER_PENDING == {} and PARTS_PENDING == {}
