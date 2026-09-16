"""WHO WROTE IT, and WHAT IT BINDS TO — two axes, typed, never one integer again.

`rank` used to be one number: 3 provincial, 2 zone, 1 inherited, 0 this water. That folded two
independent facts together:

    AUTHORITY   who wrote the rule — the province, a region's table, or something above both
                (a federal listing, a national park). This decides precedence, in the domain
                owner's words: "Regional always overrides provincial (except full closure), and
                this water overrides regional always (except closures unless they are lifted in
                this water's regs)."

    SCOPE       what the rule binds to — the whole region, an area inside it (a group of
                management units, a wildlife management area, a park), one named water, or a
                water reached by the tributary walk from a downstream one. This decides which
                STAGE the rule lands in: a region-wide rule is part of the region's STANDING
                TABLE, computed once per (region, kind of water) and checkable against the
                printed synopsis; anything narrower is an OVERRIDE on top of it.

Collapsed into one integer, a Region 5 rule written for one named river sat at the same rung
as Region 5's region-wide table, and three of them bound all twenty Fraser stretches. Held as
two typed values, a water-scoped rule cannot enter a base table — the type makes it unsayable.

The ladder is still a total order for sorting, and `rank` still exists — but it is DERIVED from
the two axes, not stored: a rule bound to this water speaks before one bound to the region,
whoever wrote it, and among region-wide rules the region speaks before the province.
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet, Optional


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
    """Where a rule comes from — every number and bound in the table carries one of these."""
    authority: Authority
    scope: Scope
    region: str = ""          # "4", "7a" — the region whose table wrote it
    place: str = ""           # the area / water / downstream water the scope names
    regions: FrozenSet[str] = frozenset()   # a region-wide rule the book limits to some regions
    rule_id: str = ""         # entry::rule — the only unique identity a rule has
    verbatim: str = ""        # the synopsis sentence, exactly

    @property
    def rank(self) -> int:
        """The ladder as a sort key. SMALLER SPEAKS FIRST.

        Scope decides before authority: "this water overrides regional" is a statement about
        what a rule binds to, not who wrote it — a province-authored rule written for one lake
        (the Kootenay rainbow stamp) speaks on that lake before the region's standing table.
        Among region-wide rules the region speaks before the province. A superior authority is
        outside the ladder: nothing below it can open what it closed.
        """
        if self.authority is Authority.superior:
            return -1
        return {Scope.water: 0, Scope.inherited: 1, Scope.area: 2,
                Scope.region: 3 if self.authority is Authority.region else 4}[self.scope]

    @property
    def is_base(self) -> bool:
        """Does this rule belong in the region's standing table? Only a region-wide scope."""
        return self.scope is Scope.region

    @property
    def who(self) -> str:
        """The short label the tables print — who wrote it, in three words or fewer."""
        if self.authority is Authority.superior: return "Federal or Parks"
        if self.scope is Scope.water:           return "this water"
        if self.scope is Scope.inherited:       return "inherited"
        if self.authority is Authority.province: return "Provincial"
        return "Region " + self.region.upper()

    @property
    def tag(self) -> str:
        """The two-to-ten-character form a table cell can afford: `Prov`, `R4`, `this water`,
        `trib`, `area`, `Parks/Fed`. The full words and the sentence sit behind it."""
        if self.authority is Authority.superior: return "Parks/Fed"
        if self.scope is Scope.water:           return "this water"
        if self.scope is Scope.inherited:       return "trib"
        if self.scope is Scope.area:            return "area"
        return "Prov" if self.authority is Authority.province else "R" + self.region.upper()

    def words(self) -> str:
        """Both axes, for a reader: "Region 2 · region-wide", "Region 2 · for this water"."""
        wrote = ("Federal or Parks" if self.authority is Authority.superior
                 else "Provincial" if self.authority is Authority.province
                 else "Region " + self.region.upper())
        if self.scope is Scope.region:
            where = ("province-wide" if self.authority is Authority.province and not self.regions
                     else "region-wide")
            if self.regions:
                where = "in " + region_words(self.regions)
        elif self.scope is Scope.area:
            where = self.place or "an area of the region"
        elif self.scope is Scope.water:
            where = "for this water" + (f" ({self.place})" if self.place else "")
        else:
            where = "inherited from " + (self.place or "a downstream water")
        return f"{wrote} · {where}"


def region_words(regions) -> str:
    r = sorted(regions, key=lambda x: (int(re.sub(r"\D", "", x) or 0), x))
    r = [x.upper() for x in r]
    if len(r) == 1:
        return "Region " + r[0]
    return "Regions " + ", ".join(r[:-1]) + " and " + r[-1]


def _area_words(area_id: str) -> str:
    """`area:mu_group:management_units_1_1_to_1_6` -> "MUs 1-1 to 1-6"."""
    kind, _, slug = area_id.partition(":")[2].partition(":")
    words = slug.replace("_", " ")
    words = re.sub(r"management units (\d+) (\d+)(?: (?:and|to) (\d+) (\d+))?",
                   lambda m: "MUs " + m.group(1) + "-" + m.group(2)
                   + ((" " + ("to" if "to" in m.group(0) else "and") + " " + m.group(3) + "-"
                       + m.group(4)) if m.group(3) else ""), words)
    if kind == "mu_group":
        return words
    if kind == "wma":
        return words.title()
    if kind == "national_parks":
        return words.title()
    return words


def source_of(rule: dict) -> Source:
    """The one place a raw rule's provenance is decided.

    `extents` are the typed statement of scope — `within area:region:N` is region-wide,
    `within area:mu_group:...` / `area:wma:...` / `area:national_parks:...` is an area, and
    `whole` / `upstream_of` / `between` / `downstream_of` name a water. A rule with no extents
    at all binds nowhere (AGENTS.md rule 13), and is scoped as `water` here only so that it has
    a place to be reported from.
    """
    entry = str(rule.get("entry") or rule.get("entry_id") or "")
    prefix = entry.split(":", 1)[0]
    superior = str(rule.get("authority") or "") == "superior"
    if prefix == "zp":
        auth, region = Authority.province, ""
    elif prefix.startswith("z"):
        auth, region = Authority.region, prefix[1:]
    else:
        auth, region = Authority.region, prefix[1:] if prefix.startswith("r") else ""
    if superior:
        auth = Authority.superior
    place = str(rule.get("entry_name") or "")
    if rule.get("via") == "trib":
        return Source(auth, Scope.inherited, region, place, frozenset(),
                      _rid(rule), rule.get("verbatim") or "")
    if not prefix.startswith("z"):
        return Source(auth, Scope.water, region, place, frozenset(),
                      _rid(rule), rule.get("verbatim") or "")
    exts = list(rule.get("extents") or [])
    areas = [str(e.get("area_id") or "") for e in exts if e.get("op") == "within"]
    kinds = [str(e.get("area_kind") or "") for e in exts if e.get("op") == "within"]
    named = [e for e in exts if e.get("op") != "within"]
    if named or not exts:
        # A region's table writing about one lake — "Shuswap Lake: annual quota" — is an
        # override on that lake, not a line in the region's standing table. The place is the
        # extent the rule names, since the entry is the region's table and names nothing.
        return Source(auth, Scope.water, region, str(rule.get("extent_text") or place),
                      frozenset(), _rid(rule), rule.get("verbatim") or "")
    regions = frozenset(a.split(":")[-1] for a in areas if a.startswith("area:region:"))
    other = [a for a in areas if a and not a.startswith("area:region:")]
    other_kinds = [k for k in kinds if k and k != "region"]
    if other or other_kinds:
        label = ", ".join(_area_words(a) for a in other) or ", ".join(
            k.replace("_", " ").title() for k in other_kinds)
        return Source(auth, Scope.area, region, label, frozenset(),
                      _rid(rule), rule.get("verbatim") or "")
    # Region-wide. A provincial rule the book limits to some regions keeps that list; a rule
    # with `area_kind: region` and no list is every region.
    if auth is Authority.province and regions and "region" not in kinds:
        return Source(auth, Scope.region, region, place, regions,
                      _rid(rule), rule.get("verbatim") or "")
    return Source(auth, Scope.region, region, place, frozenset(),
                  _rid(rule), rule.get("verbatim") or "")


def _rid(x: dict) -> str:
    return f"{x.get('entry') or x.get('entry_id')}::{x.get('rule') or x.get('rule_id')}"
