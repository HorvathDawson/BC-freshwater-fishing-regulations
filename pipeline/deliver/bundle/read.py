"""Reading the regulations back OUT of a built bundle — the only source a reader may use.

Two things live here, and both were in `pipeline/regs/table/` until that package went (it read a
`sections.json` nothing wrote any more, for a settling layer removed on 2026-09-22):

    rules(bundle)     every rule as the bundle ships it, `conditions` hoisted to the top level
    source_of(rule)   WHO wrote a rule and WHAT it binds to — two typed axes, and the ladder
                      rank derived from them (the export's `provenance`)

NOTHING HERE READS A CURATED FILE. A fallback to the entry files is how a reader answers with
something the bundle never agreed to (a size fix shipped half-applied that way), so there is none.
"""
from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet, List

from pipeline.common.curated import GENERATED

BUNDLE = str(GENERATED.bundle / "bundle.sqlite")


# --------------------------------------------------------------------------------------------
# The rules
# --------------------------------------------------------------------------------------------

def rules(path: str = BUNDLE) -> List[dict]:
    """Every rule in the bundle, flattened: its columns, with `conditions` hoisted to the top level
    and the JSON columns decoded, plus `entry`/`rule` (the pair that is its identity — see `rid`)
    and `entry_name`. A rule's `extents` are its own; nothing carries its entry's beside them.

    `exempts` is a COLUMN (resolved at build to the entry each lift reaches); a bundle from before
    it would read as a corpus in which nothing lifts anything, so it is refused."""
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        cols = [r[1] for r in db.execute("PRAGMA table_info(rule)")]
        if "exempts" not in cols:
            raise SystemExit(f"read.rules: {path} has no `rule.exempts` column — rebuild the "
                             f"bundle (`python -m pipeline.deliver.bundle`)")
        names = dict(db.execute("SELECT entry_id, name FROM entry"))
        out = []
        for row in db.execute("SELECT * FROM rule"):
            d = dict(zip(cols, row))
            cond = json.loads(d.pop("conditions") or "{}")
            if "exempts" in cond:
                raise SystemExit(f"read.rules: {d['entry_id']}::{d['rule_id']} carries `exempts` "
                                 f"in `conditions` as well as in its column")
            for k in ("species", "species_except"):
                d[k] = json.loads(d[k]) if d.get(k) else []
            d.update(cond)
            for col, name in (("when_", "when"), ("while_", "while"), ("exempts", "exempts")):
                v = d.pop(col, None)
                if v:
                    d[name] = json.loads(v)
            if d.pop("standing", 0):
                d["standing"] = True
            d["rule"], d["entry"] = d["rule_id"], d["entry_id"]
            d["entry_name"] = names.get(d["entry_id"]) or ""
            d.setdefault("extents", [])
            out.append(d)
        return out
    finally:
        db.close()


def rid(x: dict) -> str:
    """The identity of a rule, which is NOT its `rule_id`: 408 rules share 140 rule ids
    (`species_quotas.r1` is nine rules, one per region). Only `(entry, rule)` is unique."""
    return f"{x['entry']}::{x['rule']}"


# --------------------------------------------------------------------------------------------
# Who wrote it, and what it binds to
# --------------------------------------------------------------------------------------------

class Authority(str, Enum):
    superior = "superior"     # federal listings, national parks, ecological reserves
    province = "province"
    region = "region"


class Scope(str, Enum):
    region = "region"         # the whole region (or the whole province): the standing table
    area = "area"             # a named area inside the region — MUs 1-1 to 1-6, a WMA, a park
    water = "water"           # one named water, or a cut piece of it
    inherited = "inherited"   # reached from a downstream water by the tributary walk


@dataclass(frozen=True)
class Source:
    """Where a rule comes from. `rank` used to be one stored integer that folded two facts —
    who wrote it, what it binds to — and a Region 5 rule for one river then sat at the rung of
    Region 5's whole table. Held as two typed values, the rank is derived and never stored."""
    authority: Authority
    scope: Scope
    region: str = ""          # "4", "7a" — the region whose table wrote it
    place: str = ""           # the area / water the scope names
    regions: FrozenSet[str] = frozenset()   # a province-wide rule the book limits to regions

    @property
    def rank(self) -> int:
        """The ladder as a sort key; SMALLER SPEAKS FIRST. Scope decides before authority — a
        provincial rule written for one lake speaks there before the region's table — and among
        region-wide rules the region speaks before the province. A superior authority is outside
        the ladder: nothing below it opens what it closed."""
        if self.authority is Authority.superior:
            return -1
        return {Scope.water: 0, Scope.inherited: 1, Scope.area: 2,
                Scope.region: 3 if self.authority is Authority.region else 4}[self.scope]

    def words(self) -> str:
        """Both axes, for a reader: "Region 2 · region-wide", "Region 2 · for this water"."""
        wrote = ("Federal or Parks" if self.authority is Authority.superior
                 else "Provincial" if self.authority is Authority.province
                 else "Region " + self.region.upper())
        if self.scope is Scope.region:
            where = ("in " + region_words(self.regions) if self.regions
                     else "province-wide" if self.authority is Authority.province
                     else "region-wide")
        elif self.scope is Scope.area:
            where = self.place or "an area of the region"
        elif self.scope is Scope.water:
            where = "for this water" + (f" ({self.place})" if self.place else "")
        else:
            where = "inherited from " + (self.place or "a downstream water")
        return f"{wrote} · {where}"


def region_words(regions) -> str:
    r = [x.upper() for x in sorted(regions, key=lambda x: (int(re.sub(r"\D", "", x) or 0), x))]
    return ("Region " + r[0]) if len(r) == 1 else \
        "Regions " + ", ".join(r[:-1]) + " and " + r[-1]


def _area_words(area_id: str) -> str:
    """`area:mu_group:management_units_1_1_to_1_6` -> "MUs 1-1 to 1-6"; a watershed by FWA code
    (`area:basin:100-`) -> the river it drains to, "Fraser River watershed" (`registry.basins`)."""
    from pipeline.atlas.registry.basins import basin_name
    named = basin_name(area_id)
    if named:
        return named
    kind, _, slug = area_id.partition(":")[2].partition(":")
    words = slug.replace("_", " ")
    words = re.sub(r"management units (\d+) (\d+)(?: (?:and|to) (\d+) (\d+))?",
                   lambda m: "MUs " + m.group(1) + "-" + m.group(2)
                   + ((" " + ("to" if "to" in m.group(0) else "and") + " " + m.group(3) + "-"
                       + m.group(4)) if m.group(3) else ""), words)
    return words.title() if kind in ("wma", "national_parks") else words


def source_of(rule: dict) -> Source:
    """The one place a rule's provenance is decided, from ITS OWN extents (nothing inherits the
    entry's — a caller offering `entry_extents` is refused).

    A water entry (`r<n>:`) is written for that water. A zone entry's rule is region-wide when
    every extent is `within area:region:N` (or `area_kind: region`), an area when it names a
    smaller area, and a water when it names one. A rule with no extents names its place in words
    (`extent_text`, failing that `unresolved_locators`, failing that `undrawn_part`); one naming
    no place at all is refused — what it binds to is unknown."""
    entry = str(rule["entry"])
    prefix = entry.split(":", 1)[0]
    if "entry_extents" in rule:
        raise ValueError(f"source_of {rid(rule)}: `entry_extents` — a rule's scope is its own "
                         f"extents; nothing inherits the entry's")
    if prefix == "zp":
        auth, region = Authority.province, ""
    else:
        auth, region = Authority.region, prefix[1:] if prefix[:1] in ("z", "r") else ""
    if str(rule.get("authority") or "") == "superior":
        auth = Authority.superior
    place = str(rule.get("entry_name") or "")
    if not prefix.startswith("z"):
        return Source(auth, Scope.water, region, place)
    exts = list(rule.get("extents") or [])
    if not exts:
        named = (str(rule.get("extent_text") or "").strip()
                 or "; ".join(rule.get("unresolved_locators") or [])
                 or str(rule.get("undrawn_part") or "").strip())
        if not named:
            raise ValueError(f"source_of {rid(rule)}: the rule has no extents and names no "
                             f"place — what it binds to is unknown")
        return Source(auth, Scope.water, region, named)
    within = [e for e in exts if e.get("op") == "within"]
    if len(within) < len(exts):
        # A region's table writing about one lake is an override on that lake.
        return Source(auth, Scope.water, region, str(rule.get("extent_text") or place))
    areas = [str(e.get("area_id") or "") for e in within]
    kinds = [str(e.get("area_kind") or "") for e in within]
    other = [a for a in areas if a and not a.startswith("area:region:")]
    other_kinds = [k for k in kinds if k and k != "region"]
    if other or other_kinds:
        label = ", ".join(_area_words(a) for a in other) or ", ".join(
            k.replace("_", " ").title() for k in other_kinds)
        return Source(auth, Scope.area, region, label)
    regions = frozenset(a.split(":")[-1] for a in areas if a.startswith("area:region:"))
    if auth is Authority.province and regions and "region" not in kinds:
        return Source(auth, Scope.region, region, place, regions)
    return Source(auth, Scope.region, region, place)
