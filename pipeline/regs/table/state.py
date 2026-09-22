"""WHICH RULES APPLY HERE. Selection, and nothing else.

    rules_for(precondition) -> [rule]

A PRECONDITION is the answer to two questions:

    WHERE   a region, or a named area inside one (Management Units 1-1 to 1-6, a National Park,
            a wildlife management area), or a water and which stretch of it. Areas are
            area-scoped, and an area-scoped rule CANNOT ENTER A REGION'S BASE by construction —
            that is what stops one river's regulation binding a whole region.
    WHEN    a day. A season is not a note beside a number; it is the number. Selection does not
            apply it: the rules carry their own `windows`, and whatever settles them decides.

NOTHING HERE SETTLES ANYTHING. There used to be a `build(rules, kind, here, label) -> Table`
below, and the pair was the whole arrangement: selection knew the corpus, construction knew
nothing but the rules it was handed. The construction half has been removed from the repository
until it is rebuilt properly (see `pipeline/docs/06-ui-data-contract.md`); this half is unchanged,
because it was never the part that was in question — it is how a rule is found, not what is made
of it.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import FrozenSet, List, Optional, Tuple

from pipeline.regs.table.authority import source_of
from pipeline.regs.table.corpus import (rid, rules as all_rules, section_kind, section_label,
                                        section_regions, section_rules)

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
    return _fold(out)


def _ground(ids) -> Tuple[frozenset, frozenset]:
    """The ground an area's rules stand on: the area KINDS they name wholesale, and the
    individual area IDS they name one by one."""
    want = set(ids)
    kinds, spots = set(), set()
    for x in all_rules():
        if rid(x) not in want:
            continue
        for e in (x.get("extents") or []):
            if e.get("area_kind"):
                kinds.add(e["area_kind"])
            if e.get("area_id"):
                spots.add(e["area_id"])
    return frozenset(kinds), frozenset(spots)


def _fold(out: Dict[str, List[str]]) -> Tuple[Area, ...]:
    """TWO NAMES FOR ONE PLACE ARE ONE PLACE.

    The book closes the National Parks in one sentence and then closes Pacific Rim, Gwaii
    Haanas and the Gulf Islands in another — and those three ARE National Park Reserves, so the
    second sentence is the first one again with the places spelled out. Offered as two entries
    in a Where picker they read as two different states of the table, and a reader has to open
    both to find they are the same water shut by the same authority twice.

    The test is not "the tables match" — Ecological Reserves produce an identical table and are
    a genuinely different place. It is that one area's ground is INSIDE the other's: every area
    id it names belongs to a kind the other claims wholesale. Then the narrower name is not a
    separate place, and its rule joins the wider one, where it is still shown and still cited."""
    ground = {name: _ground(ids) for name, ids in out.items()}
    folded, gone = dict(out), set()
    for a, (ka, sa) in ground.items():
        if ka or not sa:
            continue                     # names no individual places: nothing to fold in
        for b, (kb, _) in ground.items():
            if a == b or not kb:
                continue
            if all(any(spot.startswith(f"area:{k}:") for k in kb) for spot in sa):
                folded[b] = sorted(set(folded[b]) | set(out[a]))
                gone.add(a)
                break
    return tuple(Area(name, tuple(sorted(ids))) for name, ids in sorted(folded.items())
                 if name not in gone)


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
        rs = rs + [_here(x) for x in area.rule_dicts if rid(x) not in have]
    return rs, frozenset({region}), ""


def _here(x: dict) -> dict:
    """AN AREA'S OWN RULE IS NOT "SOMEWHERE" ONCE YOU ARE STANDING IN IT.

    A rule whose extent is prose nothing can draw becomes a caveat rather than a counter —
    `Applies("somewhere")` — because no map can say where inside the region it bites. That is
    right when the question is "Region 1's lakes", and wrong the moment the question is "the
    Pacific Rim National Park Reserve", because THAT AREA WAS BUILT FROM THIS RULE'S OWN
    EXTENTS. The place has been drawn; it is the one selected.

    Left unhandled it printed a flat contradiction: the reserve's only rule reads "All fresh
    waters within Pacific Rim National Park Reserve … are closed to fishing" and the page said
    the place changes nothing about what you may keep, because the closure was in force
    nowhere. The National Parks closure beside it, identical in every field that decides a
    closure and differing only in having no prose extent, shut its area properly."""
    return dict(x, extent_text=None) if x.get("extent_text") else x


