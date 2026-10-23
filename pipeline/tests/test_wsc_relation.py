"""T117 — ONE owner for how a watershed code is read: `pipeline.common.utils.wsc`.

There were more than ten spellings of "is this code inside that one" — with a dash guard and without, an
empty parent a yes in the atlas's edge filter and a no in the gauge shed. Every caller now asks
`wsc.relation` (or a name built on it); what a MISSING code means stays the caller's call, made in the
open.

Mutations, each named where it fails:
  M1 drop the dash guard in `relation`             -> test_relation_whole_table (100-0259560 below 100-025956)
  M2 read an empty code as SAME / BELOW            -> test_relation_whole_table, test_callers_keep_their_unknown
  M3 a module slices or prefix-compares a code again -> test_no_module_outside_wsc_reads_a_code_by_hand
"""
from __future__ import annotations

import re
from pathlib import Path

from pipeline.common.utils import wsc


def test_relation_whole_table():
    pairs = [("100-1-2", "100-1"), ("100-1", "100-1-2"), ("100-1", "100-1"), ("100-2", "100-1"),
             ("100-0259560", "100-025956"), ("", "100"), ("100", ""), ("", "")]
    assert [wsc.relation(a, b) for a, b in pairs] == [
        wsc.BELOW, wsc.ABOVE, wsc.SAME, wsc.APART, wsc.APART, wsc.UNKNOWN, wsc.UNKNOWN, wsc.UNKNOWN]
    assert [wsc.is_at_or_below(a, b) for a, b in pairs] == [
        True, False, True, False, False, False, False, False]
    assert [wsc.is_strictly_below(a, b) for a, b in pairs] == [
        True, False, False, False, False, False, False, False]


def test_the_named_readings():
    assert (wsc.groups("100-077501"), wsc.groups(""), wsc.root("100-077501"), wsc.root(""),
            wsc.parent("100-077501-094860"), wsc.parent("100"), wsc.ancestors("100-1-2"),
            wsc.ancestors("100"), wsc.join("900", "359486"), wsc.head("910-665062-1", 2)) == (
        ["100", "077501"], [], "100", "", "100-077501", "100", ("100", "100-1"), (), "900-359486",
        "910-665062")
    assert [wsc.is_unknown(c) for c in ("", None, "999-123", "100-1")] == [True, True, True, False]
    assert [wsc.is_no_code(c) for c in ("999-999999-000000", "999-123", "")] == [True, False, False]
    assert [wsc.is_coastal(c) for c in ("900-359486", "920-1", "100-019698", "999", "")] == [
        True, True, False, False, False]
    assert [wsc.in_nine_hundreds(c) for c in ("901-1", "999", "100")] == [True, True, False]
    assert [wsc.has_prefix(c, p) for c, p in (("900-105574-087851-1", ("900-105574",)),
                                              ("100-1", ("900",)), ("100-1", ()), ("", ("100",)))] == [
        True, False, False, False]
    assert [wsc.group_after(c, "100-1") for c in ("100-1-002500-7", "100-1", "100-2-5", "")] == [
        "002500", None, None, None]


def test_callers_keep_their_unknown():
    """What a missing code means is each caller's own call. The atlas's edge filter keeps an edge into a
    water it cannot place; the gauge shed claims nothing it cannot prove; the tributary walk's code test
    says nothing (the order guard decides)."""
    from pipeline.atlas.graph.graph import _wsc_descendant
    from pipeline.atlas.graph.tributaries import _code_says_tributary
    from pipeline.gauges.consume.shed import drains_through

    class N:
        def __init__(self, w):
            self.wsc = w
    assert [_wsc_descendant(a, b) for a, b in (("100-1", ""), ("", "100"), ("100-1", "100"),
                                               ("100", "100"), ("200", "100"))] == [
        True, False, True, True, False]
    assert [drains_through(a, b) for a, b in (("100-1", ""), ("", "100"), ("100-1", "100"))] == [
        False, False, True]
    assert [_code_says_tributary(N(a), N(b)) for a, b in (("100-1", "100"), ("100", "100"),
                                                          ("100-10", "100-1"), ("", "100"))] == [
        True, False, False, False]


# --- the pin: no second copy ----------------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
#: the one owner, and the module that MINTS codes for added streams (it builds codes, it does not compare them)
OWNERS = {"pipeline/common/utils/wsc.py", "pipeline/atlas/waters/added_streams/wsc.py"}
#: a line that names a code and slices it, splits it on "-", joins groups, or prefix-compares it
_HAND = re.compile(r"""\.(startswith|endswith)\(|\.r?split\(\s*["']-["']|["']-["']\.join\(|\[\s*:\s*-?\d+\s*\]|\[\s*-?\d+\s*:\s*\]|\[len\(""")
_CODE = re.compile(r"(?i)wsc|watershed_code|\bcode\b|basin_code|\bcodes\b")
#: lines that match both but are not a watershed code read by hand — each with why
ALLOWED = {
    # a registry ref id ("wsc:100-1"), not a code
    ("pipeline/regs/dfo_salmon/splitwork.py", 'x.startswith("wsc:")'),
    # a basin AREA id ("area:basin:100-342455-"): the id's prefix, then the code is handed to `wsc`
    ("pipeline/atlas/registry/basins.py", "str(area_id).startswith(BASIN)"),
    ("pipeline/atlas/registry/basins.py", "code = str(area_id)[len(BASIN):]"),
    # not watershed codes: a source digest, a region code, a message
    ("pipeline/build.py", "hexdigest()[:16]"),
    ("pipeline/deliver/answers/glossary.py", "pages[:2]"),
    ("pipeline/deliver/bundle/rules.py", "loose[:3]"),
}


def _sources():
    for top in ("pipeline", "curation-review"):
        for p in sorted((ROOT / top).rglob("*.py")):
            rel = p.relative_to(ROOT).as_posix()
            if "/tests/" in rel or "/archive/" in rel or rel in OWNERS or "node_modules" in rel:
                continue
            yield rel, p


def test_no_module_outside_wsc_reads_a_code_by_hand():
    """M3: every slice, `-` split, group join and prefix test of a watershed code lives in `wsc.py`."""
    bad = []
    for rel, p in _sources():
        for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
            code = line.split("#", 1)[0]
            if not (_HAND.search(code) and _CODE.search(code)):
                continue
            if any(rel == f and frag in code for f, frag in ALLOWED):
                continue
            bad.append(f"{rel}:{i}: {line.strip()}")
    assert bad == []
