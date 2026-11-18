"""THE WRITER = READER VERSION PIN (trout table plan, ticket I0; fresh review blocker B2): every answers section the
pipeline writes is at the version the app's reader was written for, and no section is on one side only.

    writer   `answers.SECTIONS` (ladder, rows, gear, licence, display and, since the cutover, `trout`) + `glossary.VERSION`
    reader   app/packages/core/src/regs/versions.ts: `KNOWN_VERSIONS` (required) and `OPTIONAL_VERSIONS` (optional;
             the app chat's ticket M0 adds it — until then its absence reads as "no optional sections")

Which writer sections are REQUIRED is read from the release contract, not typed here: a section with a column in the
regs files' `cell` table (`contract._REGION_TABLES["cell"]`) is in every release; a section in `answers.SECTIONS`
without one is optional (contract 2.1: `trout` stayed out of `cell` until the cutover, T810 ticket L, moved it in: rows 34
and `trout` required — at 2 since its band kind "size", T810 L 2026-11-18 — so THE PIN FAILS until the app's ticket S
lists `rows: 34` and `trout: 2` in KNOWN_VERSIONS and empties OPTIONAL_VERSIONS — the two land in one merge).

The mutation pins run on an app line built from the writer (`AGREED`: what S writes), so they hold the mechanism whatever
the app file says today; only the pin itself reads the app's file.

The versions stay hand-typed on the app side on purpose (a reader says what it was WRITTEN for); this test is what
stops one side bumping alone: a `rows` 34 in Python without the app's `rows: 34`, or the reverse, fails here. A
section on one side only fails too, except a section named in `AWAITING_ONE_SIDE`, which may be on ONE side, at the
version named there, until both list it; then its line there must go (a stale allowance fails).
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import glossary as G
from pipeline.deliver.release import contract as C

#: the app's reader: what each section must be for the app to read it
APP_VERSIONS = Path(__file__).resolve().parents[2] / "app/packages/core/src/regs/versions.ts"

#: `cell`'s key columns; every other column is one section's frame id
CELL_KEYS = ("akey", "s")

#: {section: (version, why)}: an OPTIONAL section allowed on one side only while the other side's ticket lands. Never
#: a skip: the side that has it must have it at this version, and once both sides list it this line must be deleted.
#: (T810) empty since the app's M0 listed `trout: 1` in OPTIONAL_VERSIONS beside ticket I's Section('trout', 1): both
#: sides list it, so its allowance went (the mechanism stays pinned below on `_TROUT_WAITING`, the case it was made for).
AWAITING_ONE_SIDE: Dict[str, Tuple[int, str]] = {}

Versions = Dict[str, int]


def _frozen(src: str, name: str) -> Optional[Versions]:
    """`export const <name> = Object.freeze({ a: 1, b: 2 } as const);` as {a: 1, b: 2}; None when no such export."""
    if not re.search(rf"\bexport const {name}\b", src):
        return None
    m = re.search(rf"export const {name} = Object\.freeze\(\{{([^}}]*)\}}", src)
    assert m, f"{APP_VERSIONS}: `export const {name}` is not the `Object.freeze({{ name: n, … }})` line this test reads"
    body = re.sub(r"\bas const\b", "", m.group(1))
    got = {k: int(v) for k, v in re.findall(r"(\w+):\s*(\d+)", body)}
    left = re.sub(r"(\w+):\s*(\d+)", "", body).replace(",", "").strip()
    assert not left, f"{APP_VERSIONS}: {name} holds something that is not `name: number`: {left!r}"
    return got


def app_reader_versions(text: str | None = None) -> Versions:
    """KNOWN_VERSIONS as the app's reader types it ({section: version}); raises when the line cannot be read."""
    src = APP_VERSIONS.read_text(encoding="utf-8") if text is None else text
    got = _frozen(src, "KNOWN_VERSIONS")
    assert got is not None, f"{APP_VERSIONS}: no `export const KNOWN_VERSIONS = Object.freeze({{...}})` line"
    assert got, f"{APP_VERSIONS}: KNOWN_VERSIONS names no section"
    return got


def app_optional_versions(text: str | None = None) -> Versions:
    """OPTIONAL_VERSIONS as the app's reader types it; {} when the file has no such export yet (before the app's M0:
    the app reads no optional section). An export this test cannot read raises, never reads as {}."""
    src = APP_VERSIONS.read_text(encoding="utf-8") if text is None else text
    got = _frozen(src, "OPTIONAL_VERSIONS")
    return {} if got is None else got


def writer_versions(sections=None, glossary_version: Optional[int] = None,
                    cell: Optional[List[str]] = None) -> Tuple[Versions, Versions]:
    """(required, optional) as the pipeline writes them: `answers.SECTIONS` split by the contract's `cell` columns,
    plus the glossary's version (a static the app's KNOWN_VERSIONS also names)."""
    sections = A.SECTIONS if sections is None else sections
    cell = C._REGION_TABLES["cell"] if cell is None else cell
    assert tuple(cell[:len(CELL_KEYS)]) == CELL_KEYS, f"`cell` no longer starts with {CELL_KEYS}: {cell}"
    cols = cell[len(CELL_KEYS):]
    names = [s.name for s in sections]
    assert len(set(names)) == len(names), f"answers.SECTIONS names a section twice: {names}"
    written = {s.name: s.version for s in sections}
    orphan = [c for c in cols if c not in written]
    assert not orphan, f"`cell` has a column for {orphan}, which answers.SECTIONS does not write"
    required = {n: v for n, v in written.items() if n in cols}
    required["glossary"] = G.VERSION if glossary_version is None else glossary_version
    optional = {n: v for n, v in written.items() if n not in cols}
    return required, optional


def pin_problems(writer: Tuple[Versions, Versions], reader: Tuple[Versions, Versions],
                 awaiting: Dict[str, Tuple[int, str]] = AWAITING_ONE_SIDE) -> List[str]:
    """Every way the writer's (required, optional) versions and the reader's differ; [] when they agree."""
    (w_req, w_opt), (r_req, r_opt) = writer, reader
    out = []
    both_sides = set(w_req) & set(w_opt) | set(r_req) & set(r_opt)
    out += [f"{n}: both required and optional on one side" for n in sorted(both_sides)]
    for n in sorted(set(w_req) | set(r_req)):
        if n not in r_req:
            where = "optional in the app" if n in r_opt else "not in the app's KNOWN_VERSIONS"
            out.append(f"{n}: the pipeline writes it in every release (v{w_req[n]}), {where}")
        elif n not in w_req:
            where = "optional in the pipeline" if n in w_opt else "not written by the pipeline"
            out.append(f"{n}: the app requires it (v{r_req[n]}), {where}")
        elif w_req[n] != r_req[n]:
            out.append(f"{n}: the pipeline writes v{w_req[n]}, the app reads v{r_req[n]}: bump both sides together")
    for n in sorted(set(w_opt) | set(r_opt)):
        if n in w_req or n in r_req:
            continue                                           # said above
        w, r = w_opt.get(n), r_opt.get(n)
        if w is not None and r is not None:
            if w != r:
                out.append(f"{n} (optional): the pipeline writes v{w}, the app reads v{r}: bump both sides together")
            if n in awaiting:
                out.append(f"{n}: both sides list it now; delete its AWAITING_ONE_SIDE line")
            continue
        side, v = ("the pipeline", w) if w is not None else ("the app", r)
        if n not in awaiting:
            out.append(f"{n} (optional, v{v}) is on one side only ({side}): add it to the other side")
        elif v != awaiting[n][0]:
            out.append(f"{n} (optional) is v{v} in {side}, but it waits on one side at v{awaiting[n][0]}: "
                       "bump the other side and AWAITING_ONE_SIDE together")
    return out


# ---------------------------------------------------------------------------------------------------------- the pin

def test_every_answers_section_is_at_the_apps_reader_version():
    """THE PIN (I0) on the app's own file. Since the cutover (ticket L) it holds rows 34 and `trout` required: it fails
    until the app's S lands in the same merge."""
    writer = writer_versions()
    reader = (app_reader_versions(), app_optional_versions())
    problems = pin_problems(writer, reader)
    assert not problems, "the pipeline and the app disagree on the answers versions:\n  " + "\n  ".join(problems)
    # the pin covers every section the pipeline writes, not a chosen few (B2: only the tidal three were pinned)
    assert {s.name for s in A.SECTIONS} <= set(writer[0]) | set(writer[1])
    assert set(writer[0]) >= {"ladder", "rows", "gear", "licence", "display", "glossary"}


def test_the_apps_regs_format_is_the_contracts():
    """The regs store's own format check is hand-typed in the app (`REGS_FORMATS.region`, versions.ts): it must be the
    contract's `regs-region/2` (ticket L), or the app opens no regs file of the cutover release. Fails until S."""
    m = re.search(r'export const REGS_FORMATS = Object\.freeze\(\{[^}]*\bregion:\s*"([^"]+)"', _src())
    assert m, f"{APP_VERSIONS}: no REGS_FORMATS line with `region`"
    assert m.group(1) == C.REGS_REGION_FORMAT


def test_the_cutover_writes_rows_34_and_trout_required():
    """Ticket L: `trout` has a `cell` column (required), rows is 34, nothing is optional."""
    req, opt = writer_versions()
    assert req["rows"] == 34 and req["trout"] == 2 and opt == {}            # trout 2: the band kind "size"
    assert [s.optional for s in A.SECTIONS if s.name == "trout"] == [False]


def test_the_pin_holds_with_the_app_s_will_write():
    """The app line S writes (`AGREED`: KNOWN_VERSIONS with rows 34 and trout 2, OPTIONAL_VERSIONS empty) agrees."""
    assert pin_problems(writer_versions(), _reader(AGREED)) == []
    assert app_reader_versions(AGREED)["trout"] == 2 and app_optional_versions(AGREED) == {}


def test_an_allowance_names_only_an_optional_section_nobody_requires():
    (w_req, _), r_req = writer_versions(), app_reader_versions()
    for n in AWAITING_ONE_SIDE:
        assert n not in w_req and n not in r_req, f"{n} is required on a side: it cannot wait in AWAITING_ONE_SIDE"


# ---------------------------------------------------------------------------- mutation pins: each change must fail

def _src() -> str:
    return APP_VERSIONS.read_text(encoding="utf-8")


def _app_src(required: Versions, optional: Optional[Versions]) -> str:
    """A versions.ts in the app's line shape: KNOWN_VERSIONS (and OPTIONAL_VERSIONS when given), then the anchor line
    the optional list is inserted before (`_with_m0`)."""
    def line(name, v):
        return f"export const {name} = Object.freeze({{ {', '.join(f'{k}: {n}' for k, n in v.items())} }} as const);\n"
    return line("KNOWN_VERSIONS", required) + ("" if optional is None else line("OPTIONAL_VERSIONS", optional)) \
        + "/** The blob codecs the data layer can read (`meta.blob_codec`). */\n"


def _reader(src: str):
    return app_reader_versions(src), app_optional_versions(src)


#: the app's line as ticket S writes it: the writer's versions exactly (rows 34, trout 2 required, nothing optional)
AGREED = _app_src(*writer_versions())


def _bump(sections, name: str, by: int = 1):
    from dataclasses import replace
    return tuple(replace(s, version=s.version + by) if s.name == name else s for s in sections)


@pytest.mark.parametrize("name", ["ladder", "rows", "gear", "licence", "display", "glossary"])
def test_mutation_the_app_bumping_one_section_alone_fails(name):
    src = AGREED
    v = app_reader_versions(src)[name]
    bumped = re.sub(rf"\b{name}:\s*{v}\b", f"{name}: {v + 1}", src, count=1)
    assert bumped != src
    assert any(p.startswith(f"{name}:") for p in pin_problems(writer_versions(), _reader(bumped)))


@pytest.mark.parametrize("name", ["ladder", "rows", "gear", "licence", "display", "glossary"])
def test_mutation_the_pipeline_bumping_one_section_alone_fails(name):
    if name == "glossary":
        writer = writer_versions(glossary_version=G.VERSION + 1)
    else:
        writer = writer_versions(_bump(A.SECTIONS, name))
    assert any(p.startswith(f"{name}:") for p in pin_problems(writer, _reader(AGREED)))


def test_mutation_a_section_dropped_from_the_apps_line_fails():
    src = AGREED
    dropped = re.sub(r"\bgear:\s*\d+,\s*", "", src, count=1)
    assert "gear" not in app_reader_versions(dropped)
    v = writer_versions()[0]["gear"]
    assert f"gear: the pipeline writes it in every release (v{v}), not in the app's KNOWN_VERSIONS" in \
        pin_problems(writer_versions(), _reader(dropped))


def test_mutation_a_section_added_to_the_pipeline_only_fails():
    from pipeline.deliver.answers.answers import Section
    extra = A.SECTIONS + (Section("bait", 1, lambda *a: None),)
    # optional (no `cell` column) and on one side, with no allowance
    assert "bait (optional, v1) is on one side only (the pipeline): add it to the other side" in \
        pin_problems(writer_versions(extra), _reader(AGREED))
    # required (given a `cell` column), and the app does not list it
    cell = C._REGION_TABLES["cell"] + ["bait"]
    assert "bait: the pipeline writes it in every release (v1), not in the app's KNOWN_VERSIONS" in \
        pin_problems(writer_versions(extra, cell=cell), _reader(AGREED))


def test_mutation_a_section_added_to_the_app_only_fails():
    src = AGREED.replace("display: ", "bait: 1, display: ", 1)
    assert "bait: the app requires it (v1), not written by the pipeline" in pin_problems(writer_versions(), _reader(src))


def test_mutation_a_cell_column_no_section_writes_fails():
    with pytest.raises(AssertionError, match="does not write"):
        writer_versions(cell=C._REGION_TABLES["cell"] + ["bait"])


# ------------------------------------------------- the optional list (the app's M0) and the `trout` allowance

M0_LINE = "export const OPTIONAL_VERSIONS = Object.freeze({ trout: 1 } as const);\n"

#: the allowance `trout` held while one side listed it (T810: the app's M0 and the pipeline's ticket I landed apart);
#: the mutation pins below hold the mechanism on it as it was before the cutover (`PRE_CELL`: `trout` optional, no `cell`
#: column), now that `trout` is required and AWAITING_ONE_SIDE is empty
_TROUT_WAITING: Dict[str, Tuple[int, str]] = {"trout": (1, "the trout table (T810): one side listed it first")}


#: `cell` before the cutover (ticket L moved `trout` in): the optional mechanism's ground
PRE_CELL = [c for c in C._REGION_TABLES["cell"] if c != "trout"]


def _pre(sections, cell=None):
    return writer_versions(sections, cell=PRE_CELL if cell is None else cell)


def _with_m0(src: str, line: str = M0_LINE) -> str:
    return src.replace("/** The blob codecs", line + "/** The blob codecs", 1)


def _trout(sections, version: int = 1):
    """The sections with `trout` at `version` (ticket I lists it in `answers.SECTIONS`: replaced, never twice)."""
    from pipeline.deliver.answers.answers import Section
    return _no_trout(sections) + (Section("trout", version, lambda *a: None, optional=True),)


def _no_trout(sections):
    """The sections as they were before ticket I listed `trout` (the app's M0 landing first)."""
    return tuple(s for s in sections if s.name != "trout")


def test_the_optional_list_is_read_and_its_absence_is_no_optional_section():
    src = _src()
    if "export const OPTIONAL_VERSIONS" not in src:
        assert app_optional_versions(src) == {}
    assert app_optional_versions(_with_m0(src.replace("export const OPTIONAL_VERSIONS", "const _GONE"))) == {"trout": 1}
    assert app_optional_versions("export const OPTIONAL_VERSIONS = Object.freeze({} as const);") == {}
    # an export in another shape is a failure, never read as "none"
    with pytest.raises(AssertionError):
        app_optional_versions("export const OPTIONAL_VERSIONS: Readonly<Record<string, number>> = {};")
    with pytest.raises(AssertionError):
        app_optional_versions("export const OPTIONAL_VERSIONS = Object.freeze({ trout: one } as const);")


def _no_m0(src: str) -> str:
    return re.sub(r"export const OPTIONAL_VERSIONS = Object\.freeze\(\{[^}]*\}[^\n]*\n", "", src)


def _base() -> str:
    """The app's line as it was before M0 (the pre-cutover sections required, no optional list)."""
    return _app_src(_pre(_no_trout(A.SECTIONS))[0], None)


def test_the_trout_allowance_holds_either_side_alone_at_its_version():
    base = _base()
    # the app's M0 first: the app lists trout 1, the pipeline does not write it yet
    assert pin_problems(_pre(_no_trout(A.SECTIONS)), _reader(_with_m0(base)), _TROUT_WAITING) == []
    # the pipeline's ticket I first: trout 1 written (optional: no `cell` column), the app does not list it yet
    assert pin_problems(_pre(_trout(A.SECTIONS)), _reader(base), _TROUT_WAITING) == []


def test_mutation_trout_bumped_on_one_side_alone_fails():
    base = _base()
    two = M0_LINE.replace("trout: 1", "trout: 2")
    # app alone at 2 (pipeline not yet); pipeline alone at 2 (app not yet); both listed, one at 2
    assert any("trout (optional) is v2 in the app" in p
               for p in pin_problems(_pre(_no_trout(A.SECTIONS)), _reader(_with_m0(base, two)), _TROUT_WAITING))
    assert any("trout (optional) is v2 in the pipeline" in p
               for p in pin_problems(_pre(_trout(A.SECTIONS, 2)), _reader(base), _TROUT_WAITING))
    assert any("trout (optional): the pipeline writes v2, the app reads v1" in p
               for p in pin_problems(_pre(_trout(A.SECTIONS, 2)), _reader(_with_m0(base))))


def test_mutation_the_allowance_goes_stale_once_both_sides_list_trout():
    base = _base()
    assert pin_problems(_pre(_trout(A.SECTIONS)), _reader(_with_m0(base)), _TROUT_WAITING) == [
        "trout: both sides list it now; delete its AWAITING_ONE_SIDE line"]
    assert pin_problems(_pre(_trout(A.SECTIONS)), _reader(_with_m0(base)), awaiting={}) == []


def test_mutation_an_optional_section_without_an_allowance_fails_on_one_side():
    base = _base()
    assert pin_problems(_pre(_no_trout(A.SECTIONS)), _reader(_with_m0(base)), awaiting={}) == [
        "trout (optional, v1) is on one side only (the app): add it to the other side"]
    assert pin_problems(_pre(_trout(A.SECTIONS)), _reader(base), awaiting={}) == [
        "trout (optional, v1) is on one side only (the pipeline): add it to the other side"]


def test_mutation_trout_required_on_one_side_and_optional_on_the_other_fails():
    base = _base()
    required_in_app = base.replace("display: ", "trout: 1, display: ", 1)
    probs = pin_problems(_pre(_trout(A.SECTIONS)), _reader(required_in_app))
    assert "trout: the app requires it (v1), optional in the pipeline" in probs
    probs = pin_problems(writer_versions(_trout(A.SECTIONS)), _reader(_with_m0(base)))
    assert "trout: the pipeline writes it in every release (v1), optional in the app" in probs
