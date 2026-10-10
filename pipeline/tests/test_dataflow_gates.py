"""THE ONE-DATA-FLOW GATES (DATAFLOW §3), structural: they read the SOURCE as an AST, never a regex
a new spelling can walk past (finding H2: three closure spellings walked past the old grep).

  1 reader    nothing outside `bundle/read.py`, `deliver/verdicts/` and the named ORACLES calls
              `effective_rules` / `effective_rules_bound`: every other stage looks the verdict up
  2 closure   nothing downstream of the bundle reads `may_target` but `bundle/rules.py` (the one
              closure predicate, `closure_grade`, and `catch_and_release`) and the export's record
              writer that ships the field
  3 parts     no SQL naming `province_except` beside `section_ruleset` outside the bundle builder:
              the part partition is written once, by the bundle (`part`)
  4 calendar  ONE CALENDAR SPEC (`pipeline/common/calendar_spec.py`): its tables (months, month
              lengths, days before each month, weekdays) are spelled there and nowhere else — not
              in Python, not in the app's TypeScript, not in the reference page's JS, whose copies
              are GENERATED (`python -m pipeline.tools.emit_calendar`); `_day_index` / `_LAST_DAY`
              are read in `deliver/calendar.py` and the catalogue only

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

#: The catalogue's calendar lives in its `dates` part; the `catalogue.py` facade re-exports it.
#: All three read THE CALENDAR SPEC, which is the only module that spells a calendar table.
CALENDAR_SPEC = "pipeline/common/calendar_spec.py"
CALENDAR_OWNERS = {"pipeline/deliver/calendar.py", "pipeline/regs/parsing/catalogue.py",
                   "pipeline/regs/parsing/catalogue_parts/dates.py", CALENDAR_SPEC}


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


def _calendar_tables() -> list[tuple]:
    """The spec's tables, each as the tuple a hand-written copy would spell (names lower-cased)."""
    from pipeline.common import calendar_spec as S
    return [tuple(m.lower() for m in S.MONTHS), S.LAST_DAY, S.COMMON_YEAR_LAST_DAY, S.DAYS_BEFORE,
            tuple(w.lower() for w in S.WEEKDAYS)]


def calendar_tables_py(sources: dict[str, str]) -> set[tuple[str, str]]:
    """(module, qualname) of every calendar TABLE spelled outside the spec: a list / tuple / set
    literal, a dict's values or keys, or a string's words equal to the months, the month lengths
    (leap or book year), the days before each month or the weekdays."""
    tables = set(_calendar_tables())

    def norm(xs):
        if all(isinstance(x, ast.Constant) for x in xs):
            return tuple(x.value.lower() if isinstance(x.value, str) else x.value for x in xs)
        return None
    out: set = set()
    for path, text in sources.items():
        if path == CALENDAR_SPEC:
            continue

        def on(node, where, unit, path=path):
            got = []
            if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
                got.append(norm(node.elts))
            elif isinstance(node, ast.Dict):
                got += [norm(node.values), norm([k for k in node.keys if k is not None])]
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                got.append(tuple(node.value.lower().split()))
            if any(g in tables for g in got if g):
                out.add((path, where))
        _visit(text, on)
    return out


#: Hand-written TypeScript / JS the gate reads: the app's sources and tools, and the reference
#: page's scripts. GENERATED files are the copies the spec writes, and page_v35.js is pinned by
#: sha256 in `test_answers_reference.py` (a frozen exhibit, never edited).
CALENDAR_JS_EXEMPT = {"pipeline/deliver/answers/reference/page_v35.js"}


def _js_sources() -> dict[str, str]:
    roots = [REPO_ROOT / "app" / "packages", REPO_ROOT / "app" / "apps", REPO_ROOT / "app" / "tools",
             PIPELINE / "deliver" / "answers" / "reference"]
    out = {}
    for root in roots:
        for p in sorted(root.rglob("*")):
            if p.suffix not in (".ts", ".tsx", ".js", ".mjs") or ".generated." in p.name:
                continue
            if {"node_modules", "dist", ".expo", "build"} & set(p.relative_to(REPO_ROOT).parts):
                continue
            rel = str(p.relative_to(REPO_ROOT))
            if rel not in CALENDAR_JS_EXEMPT:
                out[rel] = p.read_text(encoding="utf-8")
    return out


def calendar_tables_js(sources: dict[str, str]) -> set[tuple[str, int]]:
    """(file, line) of every calendar table spelled in TypeScript / JS: an array literal whose
    items are the months, a month-length table, the days before each month or the weekdays."""
    import re
    tables = set(_calendar_tables())
    out: set = set()
    for path, text in sources.items():
        for m in re.finditer(r"\[([^\[\]]{20,400})\]", text):
            items = [x.strip() for x in m.group(1).split(",") if x.strip()]
            vals = tuple(int(x) if x.isdigit() else x.strip("'\"`").lower() for x in items)
            if vals in tables:
                out.add((path, text.count("\n", 0, m.start()) + 1))
    return out


def test_gate_4_one_calendar_spec():
    """The spec's tables are spelled ONCE: every other copy is read from it or generated."""
    assert calendar_tables_py(_sources()) == set()
    assert calendar_tables_js(_js_sources()) == set()


@pytest.mark.parametrize("path,planted", [
    ("pipeline/deliver/answers/common.py",
     '\n\nDAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")\n'),
    ("pipeline/tools/export_ui_rules.py",
     '\n\nMONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()\n'),
    ("pipeline/deliver/status_index.py",
     "\n\nLAST = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, "
     "11: 30, 12: 31}\n"),
])
def test_gate_4_goes_red_on_a_planted_python_table(path, planted):
    src = dict(_sources())
    src[path] += planted
    assert (path, "<module>") in calendar_tables_py(src)


@pytest.mark.parametrize("path,planted", [
    ("app/packages/core/src/statusIndex.ts",
     "\nconst BEFORE = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];\n"),
    ("pipeline/deliver/answers/reference/page_v36.js",
     "\nconst DIM2 = [31,28,31,30,31,30,31,31,30,31,30,31];\n"),
    ("pipeline/deliver/answers/reference/page_v36.js",
     "\nconst W = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];\n"),
])
def test_gate_4_goes_red_on_a_planted_js_table(path, planted):
    src = dict(_js_sources())
    assert path in src
    src[path] += planted
    assert any(p == path for p, _ in calendar_tables_js(src))


# --------------------------------------------------------------------------------------------
# The refactor's end state
# --------------------------------------------------------------------------------------------

def test_pending_is_empty_at_the_end():
    assert READER_PENDING == {} and PARTS_PENDING == {}
