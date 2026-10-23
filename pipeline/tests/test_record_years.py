"""T110 — how many years of record a station has FOR ONE QUANTITY is decided in one place, `record_years`.

The bundle took `max(years)` across quantities (07ED001: 59 years of discharge, 4 of level → 59 in Level) and the basin
roster took whichever `gauge_stats` row came last (08LD001: 115 of discharge, 7 of level → 7 in Flow). Now each
quantity carries its own record from `record_years`, and one `MIN_RECORD_YEARS` is the climatology's floor, the
panel's gate and (mirrored by `emit_gauge_policy`) the app's.

Mutation pins (each fails the named test):
  · `record_years` returning the longest record of the station           → test_record_years_is_the_quantity_asked
  · `panel_for` gating/weighing every quantity by the longest record      → test_a_short_level_record_loses_its_level_seat
  · `Member.years` / `member_rows` carrying one number                    → test_the_member_rows_carry_the_record_per_quantity
  · `weight_of` without the floor                                         → test_weight_of_holds_the_floor
  · a second record-length reduction anywhere in the pipeline             → test_nothing_else_computes_record_years
"""
from __future__ import annotations

import inspect
import json
import re
import textwrap
from pathlib import Path

import pytest

from pipeline.atlas.gauges import panel as P
from pipeline.atlas.gauges.build_panels import build
from pipeline.atlas.gauges.panel import MEMBERS_PER_QUANTITY, panel_for, weight_of
from pipeline.gauges.feed import climatology, record
from pipeline.gauges.feed.record import MIN_RECORD_YEARS, QUANTITIES, record_years
from pipeline.tests.test_build_panels import MODEL, E, N, g

ROOT = Path(__file__).resolve().parents[2]

#: 07ED001 as clim.json / gauge_stats state it (bundle.sqlite, 2026-10-23).
STATS = {"07ED001": {"discharge": {"from_year": 1938, "to_year": 2024, "years": 59, "days": 15641},
                     "level": {"from_year": 2021, "to_year": 2024, "years": 4, "days": 1221}}}


def test_record_years_is_the_quantity_asked():
    assert [record_years(STATS, "07ED001", q) for q in QUANTITIES] == [59, 4]
    assert record_years(STATS, "NOPE", "level") == 0
    assert record_years({"X": {"discharge": {"years": 12}}}, "X", "level") == 0
    with pytest.raises(ValueError):
        record_years(STATS, "07ED001", "temperature")


def test_one_floor():
    """The climatology, the panel gate and the record module hold the SAME object — and the app reads it generated."""
    assert climatology.MIN_RECORD_YEARS is record.MIN_RECORD_YEARS is P.MIN_RECORD_YEARS == 3
    from pipeline.tools import emit_gauge_policy as EG
    assert json.loads(EG.render_json())["record"] == {"minYears": MIN_RECORD_YEARS, "quantities": list(QUANTITIES)}
    assert f"export const MIN_RECORD_YEARS = {MIN_RECORD_YEARS};" in EG.render()
    assert not hasattr(climatology, "MIN_YEARS")          # its own constant is gone


def test_weight_of_holds_the_floor():
    assert weight_of(1.0, "up", MIN_RECORD_YEARS - 1) == 0.0
    assert weight_of(1.0, "up", 0) == 0.0
    assert weight_of(1.0, "up", MIN_RECORD_YEARS) == pytest.approx(MIN_RECORD_YEARS / 20)
    assert weight_of(1.0, "up", 4) == pytest.approx(4 / 20)


def _cand(st, area, years):
    return (st, "up", area, years, False, False, True)


def test_a_short_level_record_loses_its_level_seat():
    """07ED001 (59/4) against eight level-only gauges of 10 years at the same size. In LEVEL it weighs 4/20 against their
    10/20, so the eight hold the level seats; it keeps its place through discharge. Weighed by the longest record (59)
    it took the first level seat and pushed L7 out."""
    cand = [_cand(f"L{i}", 100.0, {"level": 10}) for i in range(MEMBERS_PER_QUANTITY)]
    cand.append(_cand("07ED001", 100.0, {"discharge": 59, "level": 4}))
    measures = {**{f"L{i}": {"level"} for i in range(MEMBERS_PER_QUANTITY)}, "07ED001": {"discharge", "level"}}
    got = [(d.station, round(d.weight, 6)) for d in panel_for(cand, 100.0, measures=measures)]
    # the order is the overall weight — the better of a donor's quantities, what the longest record used to give
    assert got == [("07ED001", 1.0)] + [(f"L{i}", 0.5) for i in range(MEMBERS_PER_QUANTITY)]

    old = "by_q = {q: years_in(years, q) for q in QUANTITIES}"
    src = textwrap.dedent(inspect.getsource(panel_for))
    assert src.count(old) == 1, "moved; update this pin"
    ns = dict(vars(P))
    exec(compile(src.replace(old, "by_q = {q: max(years_in(years, x) for x in QUANTITIES) for q in QUANTITIES}"),
                 P.__file__, "exec"), ns)
    longest = [d.station for d in ns["panel_for"](cand, 100.0, measures=measures)]
    assert f"L{MEMBERS_PER_QUANTITY - 1}" not in longest


def test_a_quantity_under_the_floor_is_not_ranked_in_it():
    """A 2-year level record ranks nothing in Level, while the discharge record still seats the donor."""
    cand = [_cand("D", 100.0, {"discharge": 30, "level": 2}),
            _cand("Lonly", 100.0, {"level": 2})]
    got = [d.station for d in panel_for(cand, 100.0, measures={"D": {"discharge", "level"}, "Lonly": {"level"}})]
    assert got == ["D"]


def test_the_member_rows_carry_the_record_per_quantity():
    graph = g([N("s0", 3), N("s1", 3)], [E("s0", "s1")])
    panels = build(graph, MODEL, [("07ED001", "s0", {"discharge": 59, "level": 4}, False)])
    rows = {r[2]: r for r in panels.member_rows()}
    pid, _ = panels.by_section["s1"]
    assert rows["07ED001"][5:7] == (59, 4)
    assert [r for r in panels.member_rows() if r[0] == pid] == [(pid, 0, "07ED001", "up", rows["07ED001"][4],
                                                                  59, 4, 0, 1, 0)]


# ------------------------------------------------------------------ the grep pin

#: Every place a record length is READ out of clim stats / gauge_stats / a member row, with how many times. `record.py`
#: is the reader; climatology.py is the producer (its run summary prints the median); the bundle COPIES clim stats into
#: `gauge_stats` once. Anything else is a second opinion about how long a record is.
ALLOWED = {"pipeline/gauges/feed/record.py": 1, "pipeline/gauges/feed/climatology.py": 1,
           "pipeline/deliver/bundle/build.py": 1}
READS = re.compile(r"""\.get\(\s*["']years["']|\[\s*["']years["']\s*\]|max\(\s*years\s*\)|"""
                   r"""\byears\b[^\n"]*\bFROM\s+(?:gauge_stats|panel_member|basin_member)""")


def test_nothing_else_computes_record_years():
    found: dict[str, int] = {}
    for p in sorted((ROOT / "pipeline").rglob("*.py")):
        rel = p.relative_to(ROOT).as_posix()
        if rel.startswith("pipeline/tests/"):
            continue
        code = "\n".join(line for line in p.read_text(encoding="utf-8").splitlines()
                         if not line.lstrip().startswith("#"))
        n = len(READS.findall(code))
        if n:
            found[rel] = n
    assert found == ALLOWED
