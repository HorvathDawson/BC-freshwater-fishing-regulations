"""THE CONDITIONS THAT SELECT A TABLE, AND THE ONE FUNCTION THAT BUILDS ONE.

A region does not have a table. It has a table PER CONDITION, and there are two conditions:

    WHERE   a named area inside the region — Management Units 1-1 to 1-6, a National Park,
            a wildlife management area. These are area-scoped rules, which by construction
            cannot enter a region's base (that rule is what stops a rule written for one
            river binding a whole region), so before this they were on no table at all.
            Region 1's summer closure of every stream in six management units is printed in
            bold in the synopsis and appeared nowhere on the review page.
    WHEN    a day. A season is not a note beside a number, it is the number.

These used to be two different mechanisms — the date was a parameter to the row builders and
the area was nothing — so "Region 1 streams, inside MUs 1-1 to 1-6, on July 20" was a question
with no way to ask it. Both are now INPUTS to one generator: `conditions()` says what a region
can be asked, `state()` answers. Every combination is reachable by the same path, which is what
lets a test walk all of them instead of checking the two or three someone thought of.

Nothing here resolves anything. `state()` chooses a rule set and hands it to the same builders
a section uses; the settling is the ledger's, unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import List, Optional, Tuple

from pipeline.regs.table.authority import source_of
from pipeline.regs.table.corpus import rid, rules as all_rules
from pipeline.regs.table.ledger import Ledger
from pipeline.regs.table.method_build import region_base
from pipeline.regs.table.quota_print import base_ledger
from pipeline.regs.table.rows import Row, rows as build_rows, schedule

#: The regions the synopsis prints a chapter for, plus Haida Gwaii, which is an AREA inside
#: Region 1 with a table of its own — see `Area` below for why it is not listed as one.
REGIONS = ("province", "1", "1hg", "2", "3", "4", "5", "6", "7a", "7b", "8")
KINDS = ("lake", "stream")


@dataclass(frozen=True)
class Area:
    """A named place inside a region, and the rules that are true there and nowhere else.

    Hashable and carrying rule IDS rather than rule dicts, so a state can be cached and two
    callers naming the same area get the same table.
    """
    name: str
    rules: Tuple[str, ...]

    @property
    def rule_dicts(self) -> List[dict]:
        want = set(self.rules)
        return [x for x in all_rules() if rid(x) in want]


def _chapter(entry: str) -> str:
    """Whose chapter an entry is in: `z1:` → Region 1, `zp:` → every region."""
    head = entry.split(":")[0]
    return "" if head == "zp" else head[1:]


@lru_cache(maxsize=None)
def areas(region: str, kind: str) -> Tuple[Area, ...]:
    """Every named area this region can be asked about.

    HAIDA GWAII IS NOT ONE OF THEM. It is an area by the same test — `z1:hg_*`, area-scoped —
    but it has a region button of its own and those rules ARE its base there, so listing it
    inside Region 1 as well would offer the same table twice under two names.
    """
    out = {}
    for x in all_rules():
        s = source_of(x)
        if s.is_base or s.scope.value != "area" or not s.place:
            continue
        if x["entry"].startswith("z1:hg_"):
            continue
        if _chapter(x["entry"]) not in ("", region):
            continue
        w = x.get("water")
        if w and w != kind:
            continue
        out.setdefault(s.place, []).append(rid(x))
    return tuple(Area(name, tuple(sorted(ids))) for name, ids in sorted(out.items()))


@dataclass
class State:
    """One table, under one set of conditions."""
    region: str
    kind: str
    area: Optional[Area]
    on: Optional[Tuple[int, int]]
    ledger: Ledger
    rows: List[Row]
    gear: object                      # MethodTable, or None for the province

    def schedule(self) -> List[dict]:
        """The stretches of the year over which THIS state holds one table — the quota rows
        and the gear terms together, because a bait ban that touches no number still changes
        what a reader may do that day."""
        terms = list(getattr(self.gear, "terms", ()) or ())
        return schedule(self.rows, terms)


@lru_cache(maxsize=None)
def state(region: str, kind: str, area: Optional[Area] = None,
          on: Optional[Tuple[int, int]] = None) -> State:
    """The region's table under these conditions. `area=None` is anywhere in the region that
    no area rule reaches; `on=None` is the table of the year rather than of a day."""
    extra = area.rule_dicts if area is not None else []
    L = base_ledger(region, kind, extra)
    G = None if region in ("province", "p") else region_base(region, kind, extra)
    return State(region, kind, area, on, L, build_rows(L), G)


def conditions(region: str, kind: str) -> List[Tuple[Optional[Area], dict]]:
    """EVERY DISTINCT TABLE THIS REGION HAS, as (area, stretch) pairs — what a reviewer has to
    read to have read all of it, and what a test has to walk to have checked all of it."""
    out = []
    for area in (None,) + areas(region, kind):
        for seg in state(region, kind, area).schedule():
            out.append((area, seg))
    return out
