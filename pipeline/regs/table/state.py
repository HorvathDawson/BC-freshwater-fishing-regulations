"""SELECTION AND CONSTRUCTION, KEPT APART.

The table builder takes a list of rules and nothing else. Everything that knows where rules
come from — which chapter a region's are in, which are an area's, which reach a stretch of
water — lives in `rules_for` and nowhere else. So:

    rules_for(precondition) -> [rule]        knows the corpus
    build(rules, kind, here, label) -> Table  knows nothing but the rules it is handed

and every table on the page is `build(rules_for(...))`. That is the whole arrangement, and it
is what lets the builder be lifted out later, and what lets a new kind of precondition — a
named water, a stretch of one — be added without the builder learning anything.

A PRECONDITION is the answer to two questions:

    WHERE   a region, or a named area inside one (Management Units 1-1 to 1-6, a National
            Park, a wildlife management area), or a water and which stretch of it. Areas are
            area-scoped, and an area-scoped rule CANNOT ENTER A REGION'S BASE by construction
            — that is the rule that stops one river's regulation binding a whole region — so
            before this they were on no table at all. Region 1's summer closure of every
            stream in six management units is printed in bold in the synopsis and was nowhere.
    WHEN    a day. A season is not a note beside a number; it is the number.

`conditions()` enumerates every one a region has, so a reviewer reading them all and a test
walking them all are looking at the same list.

Nothing here settles anything. `build` hands the rules to the same two-stage builders a
section uses — `build.ledger` for what you may keep, `method_build.table` for how you may
fish — and those were already pure functions of a rule list.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import FrozenSet, List, Optional, Tuple

from pipeline.regs.table.authority import source_of
from pipeline.regs.table.build import (ledger as build_ledger, section_kind, section_label,
                                       section_regions, section_rules)
from pipeline.regs.table.corpus import rid, rules as all_rules
from pipeline.regs.table.ledger import Ledger
from pipeline.regs.table.method_build import is_gear, provincial_base, table as gear_table
from pipeline.regs.table.rows import Row, rows as build_rows, schedule

#: The chapters the synopsis prints, plus Haida Gwaii — an AREA inside Region 1 with a table
#: of its own, which is why it has a key here and is not also listed among Region 1's areas.
REGIONS = ("province", "1", "1hg", "2", "3", "4", "5", "6", "7a", "7b", "8")
KINDS = ("lake", "stream")


# ----------------------------------------------------------------------------------------
# SELECTION — the only code that knows where a rule comes from
# ----------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Area:
    """A named place inside a region, and the rules true there and nowhere else. Hashable,
    and carrying rule IDS rather than dicts, so two callers naming the same area get the same
    table out of the cache."""
    name: str
    rules: Tuple[str, ...]

    @property
    def rule_dicts(self) -> List[dict]:
        want = set(self.rules)
        return [x for x in all_rules() if rid(x) in want]


def chapter(entry: str) -> str:
    """Whose chapter an entry is in: `z1:` → Region 1, `zp:` → every region."""
    head = entry.split(":")[0]
    return "" if head == "zp" else head[1:]


def region_rules(region: str) -> List[dict]:
    """What a region's standing table is made of.

    HAIDA GWAII IS AN AREA, NOT A CHAPTER: its rules are in Region 1's chapter under `hg_`
    and are area-scoped, so neither half of the ordinary test finds them — `is_base` is false
    and `"z1hg"` is a prefix no entry has. It is named here once, so the quota side and the
    gear side cannot disagree about what Haida Gwaii is (they did: the gear table was the
    province's rules and nothing else, and the page said "Roe may be used" for the half of
    the year the book bans bait in every stream there)."""
    if region == "1hg":
        return [x for x in all_rules() if x["entry"].startswith("z1:hg_") or
                (x["entry"].startswith("zp:") and source_of(x).is_base)]
    return [x for x in all_rules()
            if (x["entry"].startswith(f"z{region}:") or x["entry"].startswith("zp:"))
            and source_of(x).is_base]


@lru_cache(maxsize=None)
def areas(region: str, kind: str) -> Tuple[Area, ...]:
    """Every named area this region can be asked about. Haida Gwaii is not one of them: it is
    an area by the same test, but it has a key of its own and those rules ARE its base there,
    so listing it inside Region 1 as well would offer one table under two names."""
    out = {}
    for x in all_rules():
        s = source_of(x)
        if s.is_base or s.scope.value != "area" or not s.place:
            continue
        if x["entry"].startswith("z1:hg_"):
            continue
        if chapter(x["entry"]) not in ("", region):
            continue
        w = x.get("water")
        if w and w != kind:
            continue
        out.setdefault(s.place, []).append(rid(x))
    return tuple(Area(name, tuple(sorted(ids))) for name, ids in sorted(out.items()))


def rules_for(region: str = "", kind: str = "stream", area: Optional[Area] = None,
              water: str = "", run: int = 0) -> Tuple[List[dict], FrozenSet[str], str]:
    """THE PRECONDITION, AS A RULE LIST. Returns the rules, the region ids they are measured
    against, and the label of the place — everything `build` needs and nothing about where
    any of it came from.

    A named water is the third kind of precondition and needs no new machinery: the atlas
    already answers it, and its answer is a list of rules like any other."""
    if water:
        return (section_rules(water, run), frozenset(section_regions(water, run)),
                section_label(water, run))
    rs = region_rules(region)
    if area is not None:
        have = {rid(x) for x in rs}
        rs = rs + [x for x in area.rule_dicts if rid(x) not in have]
    return rs, frozenset({region}), ""


# ----------------------------------------------------------------------------------------
# CONSTRUCTION — takes a list of rules and knows nothing else
# ----------------------------------------------------------------------------------------
@dataclass
class Table:
    """One table: what you may keep, how you may fish, and the conditions it was built for."""
    ledger: Ledger
    rows: List[Row]
    gear: object                      # MethodTable, or None where there is no gear table
    kind: str
    here: FrozenSet[str]
    label: str = ""
    #: filled in by `state` / `water_state`, for a caller that wants to say what it asked for
    region: str = ""
    area: Optional[Area] = None
    on: Optional[Tuple[int, int]] = None

    def schedule(self) -> List[dict]:
        """The stretches of the year over which this table holds — the quota rows and the
        gear terms together, because a bait ban that touches no number still changes what a
        reader may do that day."""
        return schedule(self.rows, list(getattr(self.gear, "terms", ()) or ()))


def build(rules: List[dict], kind: str, here: FrozenSet[str] = frozenset(),
          label: str = "", province: bool = False) -> Table:
    """A LIST OF RULES IN, A TABLE OUT. The two builders it calls were already pure functions
    of a rule list; this only puts the two halves of one table in one object, so a caller
    cannot settle the quota from one rule set and the gear from another."""
    L = build_ledger(rules, kind, here, label)
    G = (provincial_base(kind) if province
         else gear_table([x for x in rules if is_gear(x)], kind, here, label))
    return Table(L, build_rows(L), G, kind, here, label)


# ----------------------------------------------------------------------------------------
# the two composed, and every condition there is
# ----------------------------------------------------------------------------------------
@lru_cache(maxsize=None)
def state(region: str, kind: str, area: Optional[Area] = None,
          on: Optional[Tuple[int, int]] = None) -> Table:
    """A region's table, or a named area inside it. `area=None` is anywhere in the region no
    area rule reaches; `on=None` is the table of the year rather than of a day."""
    rules, here, label = rules_for(region, kind, area)
    t = build(rules, kind, here, label, province=region in ("province", "p"))
    t.region, t.area, t.on = region, area, on
    return t


@lru_cache(maxsize=None)
def water_state(water: str, run: int = 0, on: Optional[Tuple[int, int]] = None) -> Table:
    """One stretch of one named water — the same builder, one more precondition. This is the
    whole point of keeping selection and construction apart: nothing here is new."""
    kind = section_kind(water)
    rules, here, label = rules_for(kind=kind, water=water, run=run)
    t = build(rules, kind, here, label)
    t.on = t.on or on
    return t


def conditions(region: str, kind: str) -> List[Tuple[Optional[Area], dict]]:
    """EVERY DISTINCT TABLE THIS REGION HAS, as (area, stretch) pairs — what a reviewer has to
    read to have read all of it, and what a test has to walk to have checked all of it."""
    out = []
    for area in (None,) + areas(region, kind):
        for seg in state(region, kind, area).schedule():
            out.append((area, seg))
    return out
