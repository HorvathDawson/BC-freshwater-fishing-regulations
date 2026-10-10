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
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
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
        return rules_from(db, path)
    finally:
        db.close()


#: Columns a reader never sees on a rule dict: decided once at bundle time from the rule's own
#: fields (`rule.closure_grade`, read through `rules.closure_grade` everywhere in Python).
_DERIVED_COLUMNS = ("closure_grade",)


def rules_from(db: sqlite3.Connection, where: str = "the bundle") -> List[dict]:
    """`rules` on an open connection (the bundle builder reads its own rows back through this:
    `derived.write`)."""
    cols = [r[1] for r in db.execute("PRAGMA table_info(rule)")]
    if "exempts" not in cols:
        raise SystemExit(f"read.rules: {where} has no `rule.exempts` column — rebuild the "
                         f"bundle (`python -m pipeline.deliver.bundle`)")
    names = dict(db.execute("SELECT entry_id, name FROM entry"))
    out = []
    for row in db.execute("SELECT * FROM rule"):
        d = dict(zip(cols, row))
        for c in _DERIVED_COLUMNS:
            d.pop(c, None)
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
    exts = list(rule.get("extents") or [])
    if not prefix.startswith("z"):
        # AN AREA ROW OF A WATER TABLE (SP-3, 2026-09-29): "CRESTON VALLEY WILDLIFE MANAGEMENT
        # AREA (CVWMA) WATERS — Bass daily quota = unlimited … EXCEPT Duck Lake (see separate
        # entry)", Bowron Lake Park waters, the Liard River watershed. Every extent is `within` a
        # named area, so the row binds every water of the area — it is not written for one of
        # them. Ranked as the water it lands on, it TIED with Duck Lake's own "Bass daily quota
        # = 3" and both spoke; a named water's own row must beat it, as it beats a zone's area.
        areas = [str(e.get("area_id") or e.get("area_kind") or "") for e in exts]
        if exts and all(e.get("op") == "within" for e in exts) and all(areas):
            return Source(auth, Scope.area, region, place)
        return Source(auth, Scope.water, region, place)
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


# --------------------------------------------------------------------------------------------
# Who speaks — the ladder, executable
# --------------------------------------------------------------------------------------------
#
# THE REFERENCE SEMANTICS THE APP MUST MATCH. The app has no rules engine yet; the export's
# `guide.ladder` states the ruling in words and this states it in code, so the words can be
# tested on real sections. It is deliberately small and reads the bundle only. When
# app/packages/core/src/regulations.ts grows its reader, it answers these same questions the same
# way, and `pipeline/tests/test_competition.py` is the list of cases it must reproduce.


def _day(on) -> int:
    """`datetime.date` or `(month, day)` -> the catalogue's day index (1..366): the delivery's one
    calendar (`pipeline.deliver.calendar.day_of`)."""
    from pipeline.deliver.calendar import day_of
    return day_of(on)


#: What `in_force` answers, in wire order (`types.InForce`: 0 no, 1 yes, 2 part).
IN_FORCE = ("no", "yes", "part")


@lru_cache(maxsize=None)
def _days_of(dates_json: str):
    from pipeline.regs.parsing.catalogue import DateRange, _days
    dates = [DateRange.model_validate(d) for d in json.loads(dates_json)]
    return frozenset(_days(dates)) if dates else None


def in_force(when: dict | None, on, at=None) -> str:
    """Whether a `when` (the bundle's JSON) holds on a day, at a MOMENT: "yes", "no", or "part".

    `at` is a `calendar.Moment` — a class of weekdays and, where the key holds an hours rule,
    inside or outside its window (`calendar.moments`). Asked at a moment, a weekday or hours rule
    DECIDES (user ruling 2026-10-08, review D2/G5): "yes" on the weekdays and hours it covers,
    "no" at the others — Kootenay Lake's Lower West Arm keeps 5 kokanee on a Saturday and releases
    them on a Monday; the Fraser above Mission is closed from one hour after sunset to one hour
    before sunrise and open by day. A moment that straddles a rule's weekdays, or names another
    hours window, is refused: `moments` cuts every key so that none does.

    Without a moment (`at` None: the day only), such a rule reads "part", as does a season that
    could not be read (`unparsed`, always): shown BESIDE what it would displace, displacing
    nothing — the export's own scans, which ask a day and not a moment, still read it so."""
    if not when:
        return "yes"
    days = _days_of(json.dumps(when.get("dates") or [], sort_keys=True))
    if days is not None and _day(on) not in days:
        return "no"
    if when.get("unparsed"):
        return "part"
    wd, hr = when.get("weekdays"), when.get("hours")
    if not (wd or hr):
        return "yes"
    if at is None:
        return "part"
    from pipeline.deliver.calendar import ALL_WEEK, hours_key, weekday_mask
    if wd:
        m = weekday_mask(wd)
        if not at.weekdays & m:
            return "no"
        if at.weekdays & ~m & ALL_WEEK:
            raise ValueError(f"in_force: moment {at} straddles the weekdays {wd}")
    if hr:
        if at.hours != hours_key(hr):
            raise ValueError(f"in_force: moment {at} does not cut the hours window {hr}")
        if not at.inside:
            return "no"
    return "yes"


def speaks_for(rule: dict, fish: str) -> bool:
    """Whether a rule says anything about ONE fish (a leaf code of the book's list, "DV"). A rule
    that names no species binds whatever you catch; `ALL_FIN_FISH` is every fish but crayfish;
    the other open subjects (`PROTECTED_SPECIES`, `SALMON`) hold no game fish and so speak for
    none — but `SALMON` speaks for a salmon the book names (`catalogue.SALMON_FISH`: chinook);
    otherwise the fish must be in the expansion of `species` and not of `species_except`. A
    bait or tackle rule for a TARGET speaks only when the fish asked about is that target."""
    from pipeline.regs.parsing.catalogue import SALMON_FISH, expand_species
    tgt = rule.get("when_targeting") or []
    if tgt and fish not in expand_species(list(tgt)):
        return False
    sp = list(rule.get("species") or [])
    if not sp:
        return True
    if fish in expand_species(list(rule.get("species_except") or [])):
        return False
    if "ALL_FIN_FISH" in sp:
        # "Fin fish" is not crayfish: "release all fin fish caught in your trap" keeps the
        # crayfish, and `rules._lift_terms` leaves a crayfish quota standing under "all fish".
        return fish != "CRA"
    if fish in SALMON_FISH and SALMON_FISH[fish] in sp:
        return True
    return fish in expand_species(sp)


def names_fish(rule: dict, fish: str) -> bool:
    """Whether a rule NAMES this fish rather than a group holding it. "Bull trout … release" names
    the Dolly Varden/bull trout; "Trout/char daily quota = 2" names a group. "Char" names each char
    (`catalogue.NAMING_GROUPS`): once trout include char (p.80) it is the book's only way to name
    char apart from trout — Region 1's "you must release: All char (includes Dolly Varden)" names
    the char a lake's "Trout daily quota = 2" (a trout/char quota) would otherwise let be kept."""
    from pipeline.regs.parsing.catalogue import NAMING_GROUPS, SPECIES_GROUPS
    return any(c == fish or (c in NAMING_GROUPS and fish in SPECIES_GROUPS[c])
               for c in rule.get("species") or [])


def not_yet_mapped(rule: dict) -> bool:
    """A rule held on its water as a note because the part it names is not drawn (`undrawn_part`:
    Kinbasket Lake's "No Fishing within 200 m of Bush-Sullivan Bridge"). It is bound to the whole
    water only so it can be SHOWN there; it NEVER governs the whole water — it never competes,
    never displaces, never lifts, and never holds a record dormant (`effective_rules`)."""
    return bool(str(rule.get("undrawn_part") or "").strip())


#: A rainbow longer than this is a steelhead where anadromous rainbow are found (p.80).
def _steelhead_min_cm() -> int:
    from pipeline.regs.parsing.catalogue import DEFINITIONAL_SIZE
    return int(DEFINITIONAL_SIZE["ST"]["min_cm"])


def steelhead_rules(db, section: int) -> bool:
    """Whether the provincial steelhead rules apply on this section (`section_steelhead_rules`:
    the reach run's `rules`, stored with the presence code). Where they do not, a rainbow of any
    size is a rainbow (user ruling 2026-10-03). A bundle without the view is refused."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'section_steelhead_rules'") \
            .fetchone():
        if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'rule'").fetchone():
            raise SystemExit("read: the bundle has no `section_steelhead_rules` view — rebuild it "
                             "(`python -m pipeline.deliver.bundle`)")
        return False
    return db.execute("SELECT 1 FROM section_steelhead_rules WHERE sid = ?",
                      (section,)).fetchone() is not None


def home_region(db, section: int) -> str | None:
    """The region a STRADDLING section takes its zone rules from (`section_home`, the atlas's
    `region_home.json`), or None for a section in one region."""
    row = db.execute("SELECT region FROM section_home WHERE sid = ?", (section,)).fetchone()
    return row[0] if row else None


def steelhead_water(db, section: int) -> bool:
    """Whether the book's steelhead definition holds on this section (`steelhead_water`): a
    rainbow over 50 cm here is a steelhead. A bundle without the table is refused."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'steelhead_water'").fetchone():
        if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'rule'").fetchone():
            raise SystemExit("read: the bundle has no `steelhead_water` table — rebuild it "
                             "(`python -m pipeline.deliver.bundle`)")
        return False                                    # a hand-made test bundle of rule sets
    return db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (section,)).fetchone() \
        is not None


#: `section_steelhead.code` -> its word (schema.sql).
STEELHEAD_CODES = {1: "known", 2: "possible"}


def steelhead_presence(db, section: int) -> str | None:
    """How sure we are that steelhead are on this section (`section_steelhead`, user rulings
    2026-10-01/02): "known" (a steelhead row's own water or a rule of one binds it, or the curated
    known-steelhead
    list names its water — a presence indicator that changes no answer by itself;
    `steelhead_water` follows where steelhead rules apply),
    "possible" (any other stream the steelhead rules bind), or None. A rule bundle without the
    table is refused."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'section_steelhead'").fetchone():
        if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'rule'").fetchone():
            raise SystemExit("read: the bundle has no `section_steelhead` table — rebuild it "
                             "(`python -m pipeline.deliver.bundle`)")
        return None
    row = db.execute("SELECT code FROM section_steelhead WHERE sid = ?", (section,)).fetchone()
    return STEELHEAD_CODES[row[0]] if row else None


def as_rainbow(x: dict) -> dict | None:
    """A rule READ FOR A RAINBOW WHERE A RAINBOW OVER 50 CM IS A STEELHEAD: its length bands over
    that range (every rainbow here is 50 cm or less). `None` when it speaks only of rainbow over 50
    cm; the rule unchanged when it has no bands or none lies outside; with the bands outside dropped
    otherwise — and with no bands at all when the one left covers every rainbow ("hatchery rainbow
    trout catch and release (50 cm or less)" is then an outright release of hatchery rainbow)."""
    bands = x.get("lengths") or []
    if not bands:
        return x
    top = _steelhead_min_cm()
    inside = [b for b in bands if b.get("min_cm") is None or b["min_cm"] < top]
    if not inside:
        return None
    if len(inside) == 1 and inside[0].get("min_cm") is None \
            and (inside[0].get("max_cm") is None or inside[0]["max_cm"] >= top):
        y = {k: v for k, v in x.items() if k != "lengths"}
        if inside[0].get("take") is not None:
            y["take"] = inside[0]["take"]
        # A size rule ("none under 50 cm" has no count of its own: `daily/size`) that now holds
        # at every rainbow length is a count: it competes where counts do.
        if y.get("take") is not None and "/size" in str(y.get("dimension") or ""):
            y["dimension"] = str(y["dimension"]).replace("/size", "", 1)
        return y
    return x if len(inside) == len(bands) else dict(x, lengths=inside)


def _spoken_lengths(x: dict, top: float = float("inf")) -> list | None:
    """THE LENGTHS A RULE SPEAKS ABOUT, as merged inclusive ranges `[(lo, hi), …]` over
    `[0, top]` — `None` when it speaks about every length (no `lengths`). A length no band covers
    is not spoken about by the rule (`catalogue.LengthBand`, "first match wins")."""
    bands = x.get("lengths") or []
    if not bands:
        return None
    got = sorted((float(b.get("min_cm") or 0), min(float(b["max_cm"]) if b.get("max_cm")
                                                   is not None else float("inf"), top))
                 for b in bands)
    out: list = []
    for lo, hi in got:
        if lo > hi:
            continue
        if out and lo <= out[-1][1] + 1:                    # whole centimetres: 29 | 30
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def covers(o: dict, k: dict, top: float = float("inf")) -> bool:
    """DOES `o` SPEAK ABOUT EVERY LENGTH `k` DOES? A rule displaces another only over the fish it
    speaks about (N-5, 2026-09-29): Lakelse Lake's "Rainbow trout over 50 cm catch and release"
    says nothing about a 30 cm rainbow, so it must not silence Region 6's "Trout/char: 5" — the
    5 still counts every rainbow under 50 cm — and Chilko Lake's "no rainbow trout over 70 cm"
    leaves the lake's own "Trout/char daily quota = 2" speaking for rainbow. `top` caps the
    lengths asked about (a rainbow where a larger one is a steelhead: 50 cm)."""
    a = _spoken_lengths(o, top)
    if a is None:
        return True
    b = _spoken_lengths(k, top)
    if b is None:
        b = [(0.0, top)]
    return all(any(lo >= x and hi <= y for x, y in a) for lo, hi in b)


def released_on_water(x: dict) -> str | None:
    """THE WATER KIND A ZONE RELEASE IS LIMITED TO — "stream" for Region 3's "you must release:
    Bull trout (Dolly Varden) from streams, Aug 1-Oct 31", Region 4's "Trout/char release: in
    streams from Nov 1-Mar 31" — or `None`.

    Only an outright release that is no closure (`take: 0`, the fish may still be fished for:
    `rules.release_origins`), carrying a `water`, whose every extent draws only that kind of
    water (`feature_types: [water]`), so that being bound to a section IS being on that kind of
    water. `effective_rules` step 4b lets such a release displace its own table's keeping quotas
    for the fish, exactly as a release printed without `water` does."""
    from pipeline.deliver.bundle.rules import closure_grade, release_origins
    water = x.get("water")
    if not water or not release_origins(x) or closure_grade(x) is not None:
        return None
    exts = x.get("extents") or []
    if not exts or any(list(e.get("feature_types") or []) != [water] for e in exts):
        return None
    return str(water)


def _base_dimension(x: dict) -> str:
    """A rule's dimension without its conditions: "daily@water=stream" -> "daily"."""
    return str(x.get("dimension") or "").split("@", 1)[0]


_RULES_BY_PATH: dict = {}

#: POLICY (user ruling 2026-10-05, option A): a zone-side size-only clause made moot by an outright
#: release or closure of the same fish is not shown (`effective_rules` step 5b). Named so a test can
#: switch it off and prove the step is what hides the clause.
MOOT_SIZE_CLAUSE_HIDDEN = True

#: POLICY (user ruling 2026-10-06, DENETIAH): a water's OWN full closure in force is the most dominant
#: rule — it silences every keeping rule for the fish it covers, of any source and any key, save a
#: superior authority's (`effective_rules` step 4c). Named so a test can switch it off and prove the
#: step is what silences them.
WATER_CLOSURE_DOMINANT = True

#: POLICY (user ruling Q5, 2026-10-07, KETTLE -> GRANBY / WEST KETTLE): a tributary with a row of its
#: own gets BOTH its own row's rules and the rules it inherits by the tributary walk ("Kettle River's
#: tributaries"), and where the two speak to the same thing (one competition key, one fish) ITS OWN
#: ROW SPEAKS — before naming: Granby River's own "trout/char daily quota = 1" (upstream of Burrell
#: Creek) beats the inherited "Rainbow trout catch and release" for a rainbow, although the inherited
#: line names the fish. A CLOSURE IS NOT A STATEMENT AN OWN ROW OUT-RANKS: an inherited "No Fishing
#: Jul 25-Sept 15" still closes Granby River on its dates (it gives way only to a lift — a printed
#: exemption, or a dated opening inside it — exactly as a zone closure does under the 2026-10-05
#: strict-lift ruling), and an inherited keeping rule an own CLOSURE beats is silenced as before.
#: `effective_rules` step 4; named so a test can switch it off and prove the step decides it.
OWN_ROW_BEATS_INHERITED = True

#: POLICY (user ruling C-5, 2026-10-08, KETTLE -> GRANBY): INHERITED RULES BEHAVE LIKE ZONE RULES —
#: a rule of the water's own row REPLACES an inherited rule of the same kind (type and dimension)
#: on every day, as a row's dated bait ban replaces its zone's all-year one (L8). Granby River's
#: own "bait ban Apr 1-Oct 31" replaces Kettle River's tributaries' all-year "bait ban": on Nov 15
#: Granby has no bait ban. Quotas keep Q5 (`OWN_ROW_BEATS_INHERITED`, on the days both hold, as a
#: water's quota meets a zone's) and closures still combine (L20). `effective_rules` step 3b;
#: named so a test can switch it off and prove the step decides it.
OWN_ROW_REPLACES_INHERITED = True

#: The states of a rule that is IN the answer (`effective_rules`). With `trace=True` the answer also
#: holds the rules that took part and lost, each with a state outside this set ("lifted",
#: "displaced", "moot"), its `reason` (`LOSS_REASONS`) and `by` (the rule that beat it).
SPEAKERS = ("speaks", "beside", "shown", "not_yet_mapped")
SPEAKER_STATES = frozenset(SPEAKERS)

#: Why a rule that took part lost — one reason per rule, the step that removed it first.
LOSS_REASONS = {
    "lifted": "lifted",                      # step 3: an exemption in force lifts it
    "ladder": "displaced",                   # step 4: a better rung of its key beats it
    "own_row": "displaced",                  # step 4: the water's own row beats an inherited rule
    "water_dates": "displaced",              # step 4a: a water row's own dates override it
    "zone_release": "displaced",             # step 4b: its table's release empties it
    "closure": "displaced",                  # step 4c: a full closure beats it by the ladder
    "water_closure": "displaced",            # step 4c: the water's own full closure (DENETIAH)
    "size_release": "displaced",             # step 4d: a water's size-limited release covers it
    "water_release": "displaced",            # step 5: the water's release silences the zone
    "same_row_release": "displaced",         # step 5: its row's dated release or closure (RU-3)
    "moot_size_clause": "moot",              # step 5b: "doesn't matter today"
    "stricter_region": "displaced",          # step 6: the other region's stricter rule
    "same_as_peer": "displaced",             # step 6: the other region's identical line (RU-8)
}

#: The states a loser takes (`LOSS_REASONS` values, in their first order): lifted, displaced, moot.
LOSER_STATES = tuple(dict.fromkeys(LOSS_REASONS.values()))

#: The origins a reader may ask about (`effective_rules(origin=…)`); None = not known.
ASKABLE_ORIGINS = ("hatchery", "wild")

#: How a rule reaches a section (`ruleset.via`): by its own extents, or by the tributary walk.
VIAS = ("reach", "trib")


def place(rule: dict, via: str) -> int:
    """A rule's rung WHERE IT IS BOUND — `source_of(rule).rank`, with a water row reaching the
    section by the tributary walk at the `inherited` rung (1): the ladder's place, as step 4 reads
    it. The ONE spelling (DATAFLOW M1); every consumer that orders rules by place asks this."""
    if via not in VIAS:
        raise ValueError(f"read.place: via {via!r} is not one of {VIAS}")
    r = rule["_rank"]                 # `_rules_of` works it out once per rule (`source_of`)
    return 1 if via == "trib" and r == 0 else r


_ladder_place = place


def _rules_of(path: str) -> dict:
    """Every rule of a bundle by `(entry, rule)`, each with its ladder rank (`source_of`) worked
    out once — read once per bundle path."""
    got = _RULES_BY_PATH.get(path)
    if got is None:
        got = _RULES_BY_PATH[path] = {(x["entry"], x["rule"]): x for x in rules(path)}
        for x in got.values():
            x["_rank"] = source_of(x).rank
    return got


def base_region(entry_id: str) -> str | None:
    """`z3:…` -> "3", `z7a:…` -> "7a"; `None` for the province's table (`zp:`) and every row."""
    head = str(entry_id).split(":", 1)[0]
    if head.startswith("z") and head != "zp":
        return head[1:]
    return None


def _count(x: dict) -> float:
    return float("inf") if x.get("unlimited") else float(x["take"])


def stricter(a: dict, b: dict) -> bool:
    """Is rule `a` STRICTER than rule `b` about the same fish — so that, between two regions'
    zone rules on one lake, `a` applies and `b` does not (`effective_rules` step 6)?

      a closure (take 0, may not fish for it; at every size, whatever the means) beats a retention
        rule that is not a closure;
      an outright release (`rules.release_origins`) beats a quota keeping only origins it releases
        (`rules.yields_to_release`);
      of two quotas stating the same thing (`rules.same_statement`), the lower beats the higher.

    Anything else is not stricter: two statements sit beside each other, and a gear or method rule
    is never displaced by one of another region (both apply)."""
    from pipeline.deliver.bundle.rules import (closure_grade, release_origins, same_statement,
                                               yields_to_release)
    if b.get("type") != "retention_limit" or a.get("type") != "retention_limit":
        return False
    # a FULL closure (`rules.closure_grade`): unconditional, at every size, whatever the means
    shut = lambda x: closure_grade(x) == "full"          # noqa: E731
    if shut(a):
        return not shut(b)
    if shut(b):
        return False
    rel, keeps = release_origins(a), yields_to_release(b)
    if rel and keeps and keeps <= rel:
        return True
    ka = yields_to_release(a)
    if ka and keeps and a.get("take") is not None and (b.get("take") is not None
                                                        or b.get("unlimited")) \
            and same_statement(a, b):
        return _count(a) < _count(b)
    return False


def effective_rules(section: int, on, fish: str, path: str = BUNDLE, *,
                    by_naming: bool = True, origin: str | None = None,
                    trace: bool = False, at=None) -> List[dict]:
    """THE RULES THAT SPEAK FOR ONE FISH, ON ONE SECTION, ON ONE DAY — the ladder as code.

    `section` is a bundle `sid`, `on` a `datetime.date` or `(month, day)`, `fish` a leaf species
    code. Returns the rules bound to the section that say something about that fish on that day,
    each a `rules()` dict with `state` added: "speaks", "beside" (of unreadable season, or on one
    half of the channel only — `side` — or, asked without a moment, in force only some hours or
    weekdays: shown, never displacing), "shown" (never competes), or
    "not_yet_mapped" (holds only in a part nothing draws). Sorted by `rid`.

    `at` is the MOMENT asked (`calendar.Moment`: a class of weekdays, inside or outside the key's
    hours window). Asked at one, a weekday or hours rule DECIDES like any other rule — in force at
    the moments it covers, out at the others (`in_force`; user ruling 2026-10-08). The verdicts
    ask every moment of every key (`calendar.moments`).

      0. A RAINBOW OVER 50 CM IS A STEELHEAD where the bundle says anadromous rainbow are found
         (`steelhead_water`, p.80): asked about "RB" there, every rule is read over rainbow of 50
         cm or less (`as_rainbow`) — one speaking only of rainbow over 50 cm speaks for no
         rainbow, and a rainbow release "(50 cm or less)" is an outright release. The larger
         fish is asked about as "ST".
      1. IN FORCE ON THE DAY. A rule whose `when` excludes the day is out, and so is one dormant
         under `suspended_while` while its named closure is in force here (no rule carries it
         today — licensing records do; the branch holds the reading for when one does).
      2. ABOUT THIS FISH (`speaks_for`). Competition is PER FISH: two rules compete only for the
         fish both speak for, so Zone B's "Bull trout … release" never touches what "Trout/char:
         5" says about a rainbow.
      3. LIFTS. A lift from a rule in force here removes the lifted rule for this fish — outright,
         or when its `species` holds the fish and its `when` holds the day. A lift that holds only
         while fishing FOR something (`when_targeting`) or while doing something (`while`), or
         only some hours or weekdays when no moment is asked, or only for a fish of some origin or size (`origin`, `lengths`: known
         only once it is caught), leaves the rule standing (the angler is unknown). Lifts are
         printed exemptions, or DERIVED at build (`basis: names_the_fish`, `rules._named_lifts`): a
         water row naming a fish its region closes lifts that closure for the fish both name — the
         only way a closure leaves (step 4 never displaces one) — never further than the lifter
         covers, and never a closure that prints its own exemption list.
      4. COMPETITION, on `(type, dimension)`. Among competitors, for this fish:
           a SUPERIOR authority first (nothing below it opens what it closed); then
           NAMING — a rule that names the fish beats one naming a group that holds it (Zone B's
           bull trout release beats Kakwa Lake's "Trout/char daily quota = 2" for bull trout,
           although the lake row is the more specific place); then
           (a `within` clause is named at its parent quota's level — "Trout/char: 5, but not more
           than 1 bull trout" is a trout/char quota, and does not name bull trout over a water's
           "Trout/char catch and release"); then
           PLACE — `source_of(rule).rank`: this water, then inherited by the tributary walk (a
           water rule reaching this section `via: trib`), then an area, the region, the province.
         THE WATER'S OWN ROW BEATS WHAT IT INHERITS, BEFORE NAMING (user ruling Q5, 2026-10-07,
         `OWN_ROW_BEATS_INHERITED`): a tributary with a row of its own (Granby River under "Kettle
         River's tributaries") gets both, and on one key its own rule speaks — "trout/char daily
         quota = 1" over the inherited "Rainbow trout catch and release" for a rainbow. Never a
         closure either way: an inherited closure is only lifted, an own closure beats as before.
         So a water row that itself names the fish ("Bull trout daily quota = 1") beats the
         zone's bull trout rule: both name it, and the water is more specific.
         A rule is displaced only by a better rule of ANOTHER quota family: a `within` clause and
         its parent quota are one statement ("Trout/char: 5, but not more than 3 lake trout")
         and never displace each other. Ties all speak.
         A RULE THAT IS ITSELF DISPLACED DISPLACES NOTHING (SP-4, 2026-09-29): a rule is out
         only when a rule that stands beats it (Cheslatta Lake, Nov 15: Region 6's lake trout
         release, overridden by the lake's own dated quota, no longer takes Region 6's
         "Trout/char: 5" and "3 Dolly Varden/bull trout and/or lake trout" with it).
         A RULE DISPLACES ONLY OVER THE LENGTHS IT SPEAKS ABOUT (`covers`, N-5): "Rainbow trout
         over 50 cm catch and release" never displaces a quota counting smaller rainbow.
         A WATER'S QUOTA AND THE ZONE'S (user rulings 2026-09-26), both keeping fish — a quota
         written for this water (or reaching it by the walk) and a zone, area or provincial one:
           the SAME statement (`rules.same_statement`: the same fish or group, size bounds,
             origin, water kind, means, target and clock) — the WATER's number replaces the
             zone's, larger or smaller, whatever naming says: Tranquille Lake's "kokanee daily
             quota = 10" replaces Region 3's "Kokanee: 5" (never the smaller of the two);
           DIFFERENT statements sit beside each other and both speak: the Dean's "Steelhead
             daily quota = 1" counts toward Region 5's "Trout/char: 5";
           a LARGER number for a fish than the zone's aggregate gives it is printed as a lift of
             the zone's quota for that fish (`exempts`, step 3): Kootenay Lake's "rainbow trout
             daily quota = 10 (any size)" lifts Region 4's "Trout/char: 5" and its "1 rainbow or
             cutthroat over 50 cm" for rainbow, so the 10 speaks alone.
         A DATED ZONE RELEASE OR CLOSURE is never displaced by a water's quota WITH NO DATES OF
           ITS OWN (user ruling 2026-09-28) unless the water's rule is the exact same statement on
           the same dates: Shuswap Lake's "Char daily quota = 1" leaves Region 3's "Lake trout
           from Oct 15-Jan 31" release speaking beside it on those dates. Only a lift removes it
           otherwise.
         A WATER ROW PRINTING ITS OWN DATES FOR THE FISH overrides a dated zone release or quota
           for that fish on the days both hold (user ruling 2026-09-28, second): Cheslatta
           Lake's "Lake trout … quotas = 3" (Nov 1-Sept 14) replaces Region 6's "Lake trout from
           Fraser and Skeena Watersheds, Sept 15-Nov 30" release on Nov 1-30. Only for the same
           fish or subject, a quota or release against a release or quota — never a closure.
         A CLOSURE ("No fishing": take 0, may not fish for it) is never displaced — "this water
         overrides regional always, except closures unless they are lifted in this water's regs".
         Only a lift removes it; it still displaces what ranks below it — and it speaks for every
         fish it covers AS IF IT NAMED IT, so a water's "No Fishing, Nov 1-Apr 30" silences the
         zone's "Burbot: 5" on its dates (read as a group rule, it let the 5 speak beside it).
     4a. A WATER ROW'S OWN DATES OVERRIDE A ZONE RELEASE LIMITED TO A KIND OF WATER (user
         ruling 2026-09-29): as `water_dates_override` does for any dated zone release, on the
         base dimension — the release's `water: stream` is part of its dimension, so step 4
         never set it against the row. Michel Creek's own "Trout/char catch and release, June
         15-Mar 31" replaces Region 4's "Trout/char release: in streams from Nov 1-Mar 31" on
         the overlap. The overridden release displaces nothing in 4b and releases nothing in 5.
     4b. A ZONE RELEASE LIMITED TO A KIND OF WATER (`water: stream`, `released_on_water`), in
         force on that kind of water, displaces its own region's table's quotas and clauses that
         keep the fish, in the same dimension read without conditions — as Region 3's "Lake trout
         from Oct 15-Jan 31" does with no `water`: "Bull trout (Dolly Varden) from streams, Aug
         1-Oct 31" silences "Trout/char: 5", "1 over 50 cm" and "1 bull trout or lake trout" for
         a bull trout on a Region 3 stream; Region 4's "Trout/char release: in streams from Nov
         1-Mar 31" silences Region 4's quotas for every trout and char on its streams. A size
         clause of another dimension ("none under 60 cm"), a closure, a water row and another
         region's rule are untouched.
         GENERALISED (RU-4, 2026-10-04): a zone release printed with NO water kind (and no
         `while` / `when_targeting` condition) displaces its own table's keeping rules in the
         same base dimension whatever THEIR water condition, THE SAME KEY INCLUDED — Region 5's
         "ALL STEELHEAD" release empties "2 per day … from streams" for a steelhead too, and
         the 7B grayling release May 1-Jun 15 silences that table's "2 per day" and "1 over 45
         cm" on its dates (a tie step 4 left standing). "Base dimension" is the dimension without
         its `@` conditions: a size clause with no count ("none under 60 cm", `daily/size`) is
         another one, which 4b and 4c do not reach — step 5b does.
     4c. A FULL CLOSURE DISPLACES THE KEEPING RULES IT BEATS BY THE LADDER WHATEVER THEIR KEY
         (RU-7, 2026-10-04): a blanket stream closure (`water: stream`) silences the region's
         "5 per day" and "1 over 50 cm" as it silences its own key's "4 from streams".
         A WATER'S OWN FULL CLOSURE IS THE MOST DOMINANT RULE (user ruling 2026-10-06, DENETIAH):
         a full closure written for this water (a row's rule bound here by its own place, not the
         tributary walk, not an area row) silences EVERY rule that would let the fish be kept, of
         any source and any key — zone, area rows, other water rows reached by the walk, other
         dimensions (possession, annual, sizes) — on its dates. Denetiah Creek's "No fishing, Jul
         1-15" silences the Liard River watershed row's bull trout "1 in possession"; the ladder
         and naming no longer matter there. Only a SUPERIOR authority's rule stands, and the
         closure's own lifters (a rule exempting it in part) speak beside it.
     4d. A WATER'S SIZE-LIMITED RELEASE MEETS THE ZONE'S SIZE CLAUSE (RU-5, 2026-10-04):
         Lakelse Lake's "none over 50 cm" displaces Region 6's "no more than 1 over 50 cm" — a
         zone keeping rule whose spoken lengths lie wholly inside the class the water releases.
      5. A WATER'S RELEASE SILENCES THE ZONE FOR THAT FISH (user ruling, 2026-09-25): an outright
         release in force here, written for this water or reached by the tributary walk,
         displaces every zone/area/provincial quota that would keep the fish, WHATEVER its
         conditions, when every origin that quota keeps is released here (see step 5 below,
         `rules.release_origins`, `rules.yields_to_release`). Such a release counts even when
         step 4 put it behind a zone release naming the fish, and one step 4 put behind a
         looser zone rule naming the fish (one that lets it be kept) speaks again.
         AND THE ROW'S OWN UNDATED QUOTA (RU-3, 2026-10-04): a dated outright release OR
         CLOSURE of the same row displaces that row's undated quota for the fish on its dates
         (the Thompson's May catch and release over its "2 per day"; Adams, Big and Sulphurous
         lakes' lake trout releases over their "1 per day"; and, as built, a row's dated
         closure over its own quota — Quatse r1 May 1-Jun 15 over r2, the Region 7 lakes'
         "No fishing Nov 1-Apr 30" over their quotas, Kitimat r2 Mar 16-May 31 over its
         hatchery steelhead 2, the Okanagan's Oct 1-Nov 15 over its perch quota).
     5b. A ZONE SIZE CLAUSE MADE MOOT BY AN OUTRIGHT RELEASE OF THE SAME FISH is not shown (user
         ruling 2026-10-05): a zone-side size-only clause ("none under 60 cm") gives way to any
         surviving outright release or closure in force here — water, zone or superior — that
         releases every origin it keeps, over its lengths: Bonaparte Lake, Nov 1, lake trout shows
         Region 3's release and not the clause. The release or closure itself is untouched.
      6. TWO REGIONS' BASES — THE MOST STRICT APPLIES (user ruling 2026-09-25). A lake straddling
         a region line binds both regions' zone rules; neither outranks the other (step 4 does not
         set them against each other). Per fish, a zone rule of one region is displaced by a
         `stricter` zone rule of the other: a closure beats open, a release beats a quota, the
         lower of two quotas stating the same thing beats the higher; different statements sit
         beside; gear and method rules of both apply. Two regions' IDENTICAL statements (the
         same number) are shown once (RU-8, 2026-10-04).
      0'. A STEELHEAD IS A RAINBOW WHERE NO STEELHEAD RULE APPLIES (RU-6, user ruling
         2026-10-03): "ST" on a section the provincial steelhead set does not reach
         (`steelhead_rules`) is answered as "RB", over every length.
      Rules that never compete pass through with state "shown": `standing`, the information
      family. A "beside" rule neither displaces nor is displaced. Lift-only rules (dimension
      `lift`) state nothing and are not returned.
      A RULE IN A PART NOBODY HAS DRAWN (`not_yet_mapped`: `undrawn_part` — "No Fishing within 200
      m of Bush-Sullivan Bridge" held on the whole of Kinbasket Lake) is returned with state
      "not_yet_mapped" when it is in force and about the fish, and takes no part in anything
      above: it displaces nothing, lifts nothing, silences nothing and suspends nothing (user
      ruling 2026-09-26). It is a place on the water the map cannot show yet, never the water.

    `by_naming=False` ranks by place alone — the ladder before the naming ruling — and exists
    only so an audit can list what the ruling changed.

    `origin` ("hatchery" / "wild"; None, the default, is today's answer: the origin is not known)
    answers for a fish of that origin (gap G2, the consumer page's reading): a lift limited to one
    origin LIFTS OUTRIGHT when that origin is asked (Kitimat's "hatchery rainbow 5" lifts Region
    6's quotas for a hatchery rainbow), and a lift for the other origin does not lift at all. With
    no origin asked such a lift leaves its target standing, `partly_lifted`. Nothing else reads the
    origin: rules of the other origin are still returned, as they are today.

    `trace=True` (gap G1) also returns every rule that TOOK PART and LOST — in force, about the
    fish, not a lift-only rule — with `state` "lifted", "displaced" or "moot" (a size clause that
    doesn't matter today, step 5b), `reason` (`LOSS_REASONS`: the step that removed it first) and
    `by` (the rid of the rule that beat it); a partly lifted rule carries `lifted_in_part_by`. The
    same code path: the rules in `SPEAKER_STATES` are exactly the untraced answer, and with the
    default off the answer is unchanged."""
    _leaf_fish(fish)
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        bound = db.execute("SELECT r.entry_id, r.rule_id, r.via FROM section_ruleset s JOIN ruleset r "
                           "ON r.set_id = s.set_id WHERE s.sid = ?", (section,)).fetchall()
        steelhead_here = fish == "RB" and steelhead_water(db, section)
        # WHETHER STEELHEAD RULES APPLY HERE (`section_steelhead_rules`, the reach run's answer).
        # A hand-made bundle of rule sets (the tests' `_tiny`: no `rule` table, no view) says
        # nothing of steelhead: the fish is answered as asked. A REAL bundle missing the view is
        # refused by `steelhead_rules` (review F7: probing the view here and defaulting to True
        # answered ST as asked on such a bundle, silently).
        hand_made = not db.execute("SELECT 1 FROM sqlite_master WHERE name IN "
                                   "('rule', 'section_steelhead_rules')").fetchone()
        rules_here = True if fish != "ST" or hand_made else steelhead_rules(db, section)
    finally:
        db.close()
    return effective_rules_bound(bound, steelhead_here, on, fish, path, by_naming=by_naming,
                                 steelhead_rules_here=rules_here, origin=origin, trace=trace,
                                 at=at)


def origin_matters(bound, path: str = BUNDLE) -> bool:
    """WHETHER THE ORIGIN ASKED CAN CHANGE THIS ANSWER: only a lift limited to one origin reads it
    (`effective_rules_bound` step 3 — "Nothing else reads the origin"). Where no rule of the set
    lifts anything for one origin, the hatchery and wild answers ARE the answer with the origin
    not known, and need not be asked (the verdicts stage's shortcut; proved over every key by
    `test_verdicts.py`). The reader owns this predicate, beside the only code that reads it."""
    every = _rules_of(path)
    return any(x.get("origin") for e, r, _ in bound for x in every[(e, r)].get("exempts") or [])


def _leaf_fish(fish: str) -> None:
    from pipeline.regs.parsing.catalogue import expand_species
    if expand_species([fish]) != [fish]:
        raise ValueError(f"effective_rules: {fish!r} is a group, not one fish — ask about a leaf "
                         f"code ({', '.join(expand_species([fish]))})")


def effective_rules_bound(bound, steelhead_here: bool, on, fish: str, path: str = BUNDLE, *,
                          by_naming: bool = True, steelhead_rules_here: bool = True,
                          origin: str | None = None, trace: bool = False,
                          at=None) -> List[dict]:
    """`effective_rules` with the section's bindings already in hand — the SAME code, minus the
    three lookups that are all a section contributes: its `(entry_id, rule_id, via)` rows (its
    ruleset), whether the steelhead definition holds there (`steelhead_water`, which matters
    only for "RB") and whether the steelhead rules apply there (`steelhead_rules`, which matters
    only for "ST"). Every section sharing a ruleset (and the two flags) gets the same answer,
    which is what lets `pipeline.deliver.status_index` evaluate 2,103 rulesets instead of 1.9 M
    sections. Not a second reading of the ladder: `effective_rules` calls this.

    A STEELHEAD IS A RAINBOW WHERE NO STEELHEAD RULE APPLIES (user ruling 2026-10-03, RU-6): asked
    about "ST" on a section the provincial steelhead set does not reach (the Okanagan River, the
    Fraser in 7A — `steelhead_rules_here` False), the answer is the RAINBOW's, read over every
    length (no 50 cm cap: the fish is a rainbow of any size there). The export says the same in
    words (`steelhead_rules: false`); this is the one definition the status index and every
    client reader follow.

    `origin`, `trace` and `at` (the moment): see `effective_rules`.

    The steps of `effective_rules`' docstring run in order, one function each, over one `_Ask`
    (the question and every set the steps share); every removal goes through `_Ask.lose`."""
    _leaf_fish(fish)
    if origin is not None and origin not in ASKABLE_ORIGINS:
        raise ValueError(f"effective_rules: origin {origin!r} — ask "
                         f"{' or '.join(ASKABLE_ORIGINS)}, or None (not known)")
    if fish == "ST" and not steelhead_rules_here:
        fish, steelhead_here = "RB", False
    steelhead_here = bool(steelhead_here) and fish == "RB"
    orig = _rules_of(path)
    s = _Ask(orig=orig, every=orig, here={(e, r): via for e, r, via in bound if (e, r) in orig},
             fish=fish, on=on, at=at, origin=origin, by_naming=by_naming)
    _step0_steelhead_sizes(s, steelhead_here)
    _step1_in_force(s)
    _step3_lifts(s)
    _step3b_own_row_replaces(s)
    _step2_about_this_fish(s)
    _step4_competition(s)
    _step4a_water_dates_on_a_water_kind(s)
    _step4b_zone_release(s)
    _step4c_closures(s)
    _step4d_size_release(s)
    _step5_water_release(s)
    _step5b_moot_size_clause(s)
    _step6_two_regions(s)
    return _answer(s, trace)


@dataclass(eq=False, slots=True)
class _Ask:
    """ONE QUESTION TO THE LADDER (`effective_rules_bound`): one ruleset, one fish, one day (and
    moment, and origin) — and every set its steps hand on to the next. The predicates the steps
    share are its methods; a step's own helpers sit beside the step."""
    orig: dict                # every rule of the bundle by (entry, rule), as stored
    every: dict               # the same, each read over the rainbow's sizes where step 0 says so
    here: dict                # (entry, rule) -> via ("reach" / "trib"): the section's ruleset
    fish: str
    on: object
    at: object
    origin: str | None
    by_naming: bool
    # WHY EACH LOSER LOST (gap G1): rule -> (reason, the rule that beat it), kept on every call —
    # the steps remove rules only through `lose`, so the trace is the same code path as the
    # answer; `trace` only decides whether the losers are returned.
    why: dict = field(default_factory=dict)
    partly_by: dict = field(default_factory=dict)
    no_rainbow: set = field(default_factory=set)      # step 0: speaks for no rainbow here
    top: float = float("inf")                          # step 0: the longest fish asked about
    state: dict = field(default_factory=dict)          # step 1: in_force, "yes" / "part" / "no"
    undrawn: set = field(default_factory=set)          # `not_yet_mapped` rules
    live: set = field(default_factory=set)             # step 1: in force at all
    lifted: set = field(default_factory=set)           # step 3
    partly: set = field(default_factory=set)           # step 3: lifted in part
    replaced: set = field(default_factory=set)         # step 3b
    cand: set = field(default_factory=set)             # step 2: about this fish, not lifted
    keyed: dict = field(default_factory=dict)          # step 4: competitors by (type, dimension)
    out_: set = field(default_factory=set)             # the answer, as the steps leave it
    overridden: set = field(default_factory=set)       # zone rules a surviving water row's own dates overrode
    rel: dict = field(default_factory=dict)            # step 5: releaser -> the origins it releases

    def lose(self, k, reason: str, by) -> None:
        """Take `k` out of the answer: `reason` (`LOSS_REASONS`) and `by`, the rule that beat it —
        the first removal is the one recorded."""
        self.out_.discard(k)
        self.why.setdefault(k, (reason, by))

    def competes(self, k) -> bool:
        x = self.every[k]
        return self.state[k] == "yes" and not x.get("standing") \
            and x.get("family") != "information" and k not in self.undrawn

    def closure(self, k) -> bool:
        # any closure, conditioned or not (`rules.closure_grade`): it speaks for every fish it
        # covers, and the condition travels with it
        from pipeline.deliver.bundle.rules import closure_grade
        return closure_grade(self.every[k]) is not None

    def order(self, k) -> tuple:
        x = self.every[k]
        rank = x["_rank"]
        if self.here[k] == "trib" and rank == 0:
            rank = 1
        # A `within` clause is named at its PARENT's level: "Trout/char: 5, but not more than 1
        # bull trout" is a trout/char quota with a sub-limit, not a bull trout rule — read as one,
        # it would reopen bull trout at a water printing "Trout/char catch and release".
        parent = self.every.get((k[0], x["within"])) if x.get("within") else None
        named = 0 if (not self.by_naming or self.closure(k)
                      or names_fish(parent or x, self.fish)) else 1
        return (0 if rank < 0 else 1, named, rank)

    def family(self, k) -> tuple:
        x = self.every[k]
        return (k[0], x.get("within") or x.get("condition_of") or k[1])

    def place(self, k) -> int:
        return _ladder_place(self.every[k], self.here[k])

    def row_area(self, k) -> bool:
        """A WATER TABLE'S AREA ROW (`source_of`: Scope.area on an `r<n>:` entry — the CVWMA
        waters, Bowron Lake Park waters, the Liard River watershed). It ranks as an area, so a
        named water's own row beats it by place; against the ZONE it is still a row of the water
        tables, and the water-vs-zone rulings below read it as one (`water_side`)."""
        return str(k[0]).startswith("r") and self.every[k]["_rank"] == 2

    def water_side(self, k) -> bool:
        """Written for this water (or reaching it by the walk), or a water table's area row."""
        return 0 <= self.place(k) <= 1 or self.row_area(k)

    def zone_side(self, k) -> bool:
        """A zone, area or provincial table's rule — not a water table's row."""
        return self.place(k) >= 2 and not self.row_area(k)

    def water_and_zone(self, o, k) -> bool:
        """Two quotas that both let the fish be kept, one written for this water (or reaching it
        by the walk), the other a zone, area or provincial one — never a superior authority's."""
        from pipeline.deliver.bundle.rules import yields_to_release
        return bool(yields_to_release(self.every[o]) and yields_to_release(self.every[k])
                    and ((self.water_side(o) and self.zone_side(k))
                         or (self.water_side(k) and self.zone_side(o))))

    def dated_zone_release(self, k) -> bool:
        """A zone, area or provincial rule keeping NONE of the fish (take 0: a release, or a
        closure) on printed dates — Region 3's "you must release … Lake trout from Oct 15-Jan 31"."""
        x = self.every[k]
        return self.zone_side(k) and x.get("type") == "retention_limit" and x.get("take") == 0 \
            and bool((x.get("when") or {}).get("dates"))

    def exact_same(self, o, k) -> bool:
        """The water's rule says EXACTLY what the dated zone rule says — the same fish, sizes,
        origin, water kind, means and target (`rules.statement`) on the same dates."""
        from pipeline.deliver.bundle.rules import same_statement
        every = self.every
        return same_statement(every[o], every[k]) and \
            (every[o].get("when") or {}).get("dates") == every[k]["when"]["dates"]

    def dated_zone_retention(self, k) -> bool:
        """A zone, area or provincial retention rule on printed dates that states a NUMBER for
        the fish — a release (take 0) or a quota — and is no closure: Region 6's "Lake trout from
        Fraser and Skeena Watersheds, Sept 15-Nov 30" (must release)."""
        x = self.every[k]
        return self.zone_side(k) and x.get("type") == "retention_limit" and not self.closure(k) \
            and (x.get("take") is not None or bool(x.get("unlimited"))) \
            and not x.get("record_retention") and bool((x.get("when") or {}).get("dates"))

    def water_dates_override(self, o, k) -> bool:
        """A WATER ROW PRINTING ITS OWN DATES FOR THE FISH overrides a dated zone rule for that
        fish, on the days both hold (user ruling 2026-09-28). Cheslatta and Murray lakes print
        "Lake trout catch and release, Sept 15-Oct 31" and "quotas = 3" (Nov 1-Sept 14); Region
        6 prints "Lake trout from Fraser and Skeena Watersheds, Sept 15-Nov 30" (release). On Nov
        1-30 both are in force and the lake's 3 replaces the region's release; on Sept 15-Oct 31
        the lake's own release speaks. The overlap needs no arithmetic: both rules are in force on
        the day asked, or this is never asked.

        Only COMPATIBLE rules: `o` is written for this water (or reaches it by the walk) and
        states a number for the fish (a quota or a release — not a size clause, a duty, a lift)
        on dates of its OWN; `k` is a dated zone retention rule stating a number
        (`dated_zone_retention` — never a CLOSURE: a blanket spring closure still closes); and
        both are about THE SAME FISH OR SUBJECT — the water row names this fish, or the two
        state the same set of fish (`rules.statement`). The water rule must hold for every fish
        the zone rule does: no narrower origin, water kind, means or target. A water row with no
        dates of its own leaves the dated zone release speaking (Shuswap, `beats`)."""
        from pipeline.deliver.bundle.rules import statement
        x, z = self.every[o], self.every[k]
        if not (self.water_side(o) and x.get("type") == "retention_limit"
                and (x.get("take") is not None or x.get("unlimited"))
                and not x.get("within") and not x.get("record_retention")
                and bool((x.get("when") or {}).get("dates")) and self.dated_zone_retention(k)):
            return False
        if not (names_fish(x, self.fish) or statement(x)[0] == statement(z)[0]):
            return False
        for c in ("origin", "water"):
            if x.get(c) and x.get(c) != z.get(c):
                return False
        for c in ("while", "caught", "when_targeting"):
            if x.get(c) and set(x[c]) != set(z.get(c) or []):
                return False
        return True

    def zone_line_restated(self, o, k) -> bool:
        """A ZONE TABLE'S LINE ABOUT ONE WATER, SAID AGAIN BY THAT WATER'S ROW (UI consumer's report,
        2026-10-03). Region 3 prints "Annual catch quota for Shuswap Lake: … Char-Lake trout and
        Bull trout (Dolly Varden): 5 over 60 cm" (p.28) and the Shuswap row "Char daily quota = 1
        (none under 60 cm), annual quota = 5" (p.32): one limit, printed twice. The zone line names
        the lake (an item extent), so it ranks as the water's own and neither displaced the other —
        the page showed the annual 5 twice. When a zone table's (`z<region>:`) keeping quota and
        the water row's (`r…`) are the SAME STATEMENT (`rules.same_statement`), the row's speaks,
        as a water's number replaces the zone's (`water_and_zone`)."""
        from pipeline.deliver.bundle.rules import same_statement, yields_to_release
        return (str(o[0]).startswith("r") and base_region(k[0]) is not None
                and self.water_side(o) and self.water_side(k)
                and bool(yields_to_release(self.every[o])) and bool(yields_to_release(self.every[k]))
                and same_statement(self.every[o], self.every[k]))

    def beats(self, o, k) -> bool:
        """Does `o` displace `k` for this fish (of another quota family, not two regions' peers)?

        Between a WATER quota and a ZONE quota that both keep fish (user rulings 2026-09-26):
          THE SAME STATEMENT (`rules.same_statement`: the same fish or group, size bounds,
            origin, water kind, means, target and clock) — the WATER's number replaces the
            zone's, larger or smaller: never the smaller of the two, and never undone by naming
            (Tranquille Lake's "kokanee daily quota = 10" replaces Region 3's "Kokanee: 5");
          DIFFERENT STATEMENTS sit beside each other — both speak (the Dean's "Steelhead: 1"
            counts toward Region 5's "Trout/char: 5"). A water row printing a LARGER number for a
            fish than the zone's aggregate is a different statement: it takes the fish out of the
            aggregate by LIFTING the zone's quota for that fish (`exempts`, step 3 — Kootenay
            Lake's "rainbow trout daily quota = 10 (any size)" lifts Region 4's "Trout/char: 5"
            for rainbow), never by this comparison.
        A DATED ZONE RELEASE OR CLOSURE IS NOT SILENCED BY A WATER'S QUOTA (user ruling
        2026-09-28): a water rule that lets the fish be kept displaces a zone rule keeping none of
        it on printed dates only when it is the EXACT same statement on the same dates
        (`exact_same`). Shuswap Lake's "Char daily quota = 1" names lake trout, and by naming and
        place it silenced Region 3's "Lake trout from Oct 15-Jan 31" release — the stricter rule,
        and no override of it. A lift the row prints, or a derived lift of a closure that sends
        the reader to the tables, still removes it (step 3).
        A WATER ROW WITH DATES OF ITS OWN FOR THE FISH overrides a dated zone release or quota
        for it on the days both hold (user ruling 2026-09-28, `water_dates_override`): Cheslatta
        Lake's "Lake trout daily and possession quotas = 3" (Nov 1-Sept 14) replaces Region 6's
        "Lake trout from Fraser and Skeena Watersheds, Sept 15-Nov 30" release on Nov 1-30.
        Everything else: the better rung (`order`) displaces.

        A RULE DISPLACES ONLY OVER THE LENGTHS IT SPEAKS ABOUT (`covers`, N-5): "Rainbow trout
        over 50 cm catch and release" never displaces a rule that speaks about smaller fish."""
        from pipeline.deliver.bundle.rules import same_statement, yields_to_release
        every = self.every
        if not covers(every[o], every[k], self.top):
            return False
        if self.water_dates_override(o, k):
            return True
        if self.water_and_zone(o, k):
            return self.water_side(o) and same_statement(every[o], every[k])
        if self.water_side(o) and yields_to_release(every[o]) and self.dated_zone_release(k):
            return self.exact_same(o, k)
        if self.zone_line_restated(o, k):
            return True
        if self.own_over_inherited(o, k):
            return True
        if self.own_over_inherited(k, o):
            return False
        return self.order(o) < self.order(k)

    def own_over_inherited(self, o, k) -> bool:
        """THE WATER'S OWN ROW BEATS WHAT IT INHERITS (user ruling Q5, `OWN_ROW_BEATS_INHERITED`):
        `o` is written for this water (bound here by its own place, not the walk) and `k` reaches
        it only by the tributary walk, and neither is a closure (an inherited closure is lifted,
        never out-ranked; an own closure already beats every keeper by the ladder)."""
        return OWN_ROW_BEATS_INHERITED and self.place(o) == 0 and self.here[k] == "trib" \
            and self.every[k]["_rank"] == 0 and not self.closure(o) and not self.closure(k)

    def base(self, k) -> str | None:
        """The region whose OWN table (`z<region>:`, not the province's) wrote the rule."""
        return base_region(k[0]) if self.every[k]["_rank"] >= 2 else None

    def peers(self, o, k) -> bool:
        """Two regions' zone rules on one section — a lake straddling their line (step 6)."""
        a, b = self.base(o), self.base(k)
        return a is not None and b is not None and a != b

    def clock(self, k) -> str:
        # the rule's clock alone: "daily/size" (sizes with no count) and "daily" are one clock
        return _base_dimension(self.every[k]).split("/", 1)[0]

    def said(self, k) -> str:
        if k in self.undrawn:
            return "not_yet_mapped"
        return "speaks" if self.competes(k) else "beside" if self.state[k] == "part" else "shown"


def _step0_steelhead_sizes(s: _Ask, steelhead_here: bool) -> None:
    # 0. WHERE A RAINBOW OVER 50 CM IS A STEELHEAD (p.80), a rainbow rule speaks only for rainbow
    #    of 50 cm or less: each rule is read over that range (`as_rainbow`), and one that speaks
    #    only of rainbow over 50 cm ("1 over 50 cm") speaks for no rainbow here — the fish is a
    #    steelhead, asked about as "ST".
    # the lengths a rule may speak about here (`covers`): every rainbow is 50 cm or less where a
    # larger one is a steelhead
    s.top = float(_steelhead_min_cm()) if steelhead_here else float("inf")
    if steelhead_here:
        orig = s.orig
        s.every = every = dict(orig)
        for k in s.here:
            if speaks_for(orig[k], s.fish):
                v = as_rainbow(orig[k])
                if v is None:
                    s.no_rainbow.add(k)
                else:
                    every[k] = v


def _step1_in_force(s: _Ask) -> None:
    """1. IN FORCE ON THE DAY (and moment), the half of the channel, the undrawn parts, and the
    rules dormant under their closure."""
    every, here = s.every, s.here
    state = s.state = {k: in_force(every[k].get("when"), s.on, s.at) for k in here}
    # ONE HALF OF THE CHANNEL (`side`: Kitimat River's "No Fishing on the west half of river …"):
    # the rule holds on part of the section's width, so, like a rule holding some hours, it is
    # shown BESIDE the rules the other half answers to and displaces none (user ruling 2026-09-28).
    for k in here:
        if every[k].get("side") and state[k] == "yes":
            state[k] = "part"
    undrawn = s.undrawn = {k for k in here if not_yet_mapped(every[k])}
    for k in here:                                          # 1. dormant under its closure
        sw = every[k].get("suspended_while")
        if sw and state.get((k[0], sw)) == "yes" and (k[0], sw) not in undrawn:
            state[k] = "no"
    s.live = {k for k, st in state.items() if st != "no"}


def _step3_lifts(s: _Ask) -> None:
    """3. LIFTS (see `effective_rules`)."""
    every, state, live, undrawn = s.every, s.state, s.live, s.undrawn
    fish, origin, on, at = s.fish, s.origin, s.on, s.at
    lifted, partly, partly_by, why = s.lifted, s.partly, s.partly_by, s.why
    for k in sorted(live):                                   # 3. lifts
        if state[k] != "yes" or k in undrawn:
            continue
        for x in every[k].get("exempts") or []:
            t = (x["entry_id"], x["rule_id"])
            # the lift's fish, read as a rule's species ("ALL_FIN_FISH" is every fin fish)
            if t not in live or ("species" in x
                                 and not speaks_for({"species": x["species"]}, fish)):
                continue
            # A lift for some anglers (a target, a means) or some fish (an origin, a size — which
            # the angler learns only once it is caught) leaves the rule standing, partly lifted.
            # ASKED FOR ONE ORIGIN (gap G2): a lift for that origin alone lifts it outright, a lift
            # for the other origin not at all.
            if origin is not None and x.get("origin"):
                if x["origin"] != origin:
                    continue
                some = x.get("when_targeting") or x.get("while") or x.get("lengths")
            else:
                some = x.get("when_targeting") or x.get("while") or x.get("origin") \
                    or x.get("lengths")
            if some:
                partly.add(t)
                partly_by.setdefault(t, []).append(k)
                continue
            got = in_force(x.get("when"), on, at) if "when" in x else "yes"
            if got == "yes":
                lifted.add(t)
                why.setdefault(t, ("lifted", k))
            elif got == "part":
                partly.add(t)
                partly_by.setdefault(t, []).append(k)


# 3b. THE WATER'S OWN ROW REPLACES WHAT IT INHERITS OF THE SAME KIND (user ruling C-5,
#     `OWN_ROW_REPLACES_INHERITED`): an inherited rule (a water row reaching the section by the
#     tributary walk) gives way, ON EVERY DAY, to a rule of the water's own row of the same type
#     and dimension that speaks for the fish at least as widely — whether or not the own rule
#     is in force today. Not a quota (Q5 decides those on the days both hold, step 4) and not
#     a closure (an inherited closure is lifted, never replaced).
def _own_replacement(s: _Ask, k):
    from pipeline.deliver.bundle.rules import closure_grade
    every, here = s.every, s.here
    x = every[k]
    if here[k] != "trib" or x["_rank"] != 0 or x.get("type") == "retention_limit" \
            or x.get("dimension") == "lift" or closure_grade(x) is not None:
        return None
    for o in sorted(here):
        y = every[o]
        if here[o] == "reach" and y["_rank"] == 0 and o not in s.undrawn \
                and (y.get("type"), y.get("dimension")) == (x.get("type"), x.get("dimension")) \
                and not y.get("standing") and not y.get("exempts") \
                and closure_grade(y) is None and speaks_for(y, s.fish) \
                and all(not y.get(c) or y.get(c) == x.get(c)
                        for c in ("when_targeting", "while", "caught", "origin", "water", "lengths",
                                  "side", "within", "condition_of")):
            return o
    return None


def _step3b_own_row_replaces(s: _Ask) -> None:
    """3b. THE WATER'S OWN ROW REPLACES WHAT IT INHERITS OF THE SAME KIND (`_own_replacement`)."""
    if OWN_ROW_REPLACES_INHERITED:
        for k in sorted(s.live - s.lifted):
            o = _own_replacement(s, k)
            if o is not None:
                s.replaced.add(k)
                s.why.setdefault(k, ("own_row", o))


def _step2_about_this_fish(s: _Ask) -> None:
    """2. ABOUT THIS FISH (`speaks_for`): the candidates — in force, not lifted, not replaced
    (3b), not a rainbow rule for the steelhead's sizes (0), not a lift-only rule — and, of them,
    the competitors keyed on `(type, dimension)` for step 4."""
    every, fish = s.every, s.fish
    s.cand = {k for k in s.live - s.lifted - s.no_rainbow - s.replaced
              if speaks_for(every[k], fish) and every[k].get("dimension") != "lift"}
    keyed = s.keyed
    for k in s.cand:
        if s.competes(k):
            keyed.setdefault((every[k]["type"], every[k]["dimension"]), []).append(k)


def _step4_competition(s: _Ask) -> None:
    """4. COMPETITION, on `(type, dimension)` (see `effective_rules`; `_Ask.beats`)."""
    # A DISPLACED RULE DISPLACES NOTHING (SP-4, 2026-09-29). At Cheslatta Lake on Nov 15 Region
    # 6's lake trout release (Sept 15-Nov 30) names the fish and so beats the region's "Trout/char:
    # 5" and "3 Dolly Varden/bull trout and/or lake trout"; the lake's own dated "quotas = 3"
    # overrides that release (`water_dates_override`). Read as "beaten by ANY competitor", the
    # release — itself gone — still took the 5 and the 3 with it, and they vanished on exactly the
    # days the release was overridden. A rule is out only when a rule that SURVIVES beats it: the
    # rules nothing beats stand; whatever a standing rule beats is out; a rule every one of whose
    # beaters is out stands; repeat until nothing moves. (A cycle of rules beating each other —
    # none in the corpus — falls back to "beaten by any rule still undecided".)
    s.out_ = set(s.cand)
    family, peers, beats, closure = s.family, s.peers, s.beats, s.closure
    for group in s.keyed.values():
        beaters = {k: [o for o in group if o != k and family(o) != family(k)
                       and not peers(o, k) and beats(o, k)]
                   for k in group if not closure(k)}
        stands, falls = {k for k in group if not beaters.get(k)}, set()
        moved = True
        while moved:
            moved = False
            for k in group:
                if k in stands or k in falls:
                    continue
                if any(o in stands for o in beaters[k]):
                    falls.add(k)
                    moved = True
                elif all(o in falls for o in beaters[k]):
                    stands.add(k)
                    moved = True
        falls |= {k for k in group if k not in stands}        # a cycle: the old reading
        for k in sorted(falls):
            won = sorted(o for o in beaters[k] if o in stands) or sorted(beaters[k])
            s.lose(k, "water_dates" if s.water_dates_override(won[0], k)
                   else "own_row" if s.own_over_inherited(won[0], k) else "ladder", won[0])
        s.overridden |= {k for k in falls
                         if any(o in stands and s.water_dates_override(o, k) for o in beaters[k])}


def _step4a_water_dates_on_a_water_kind(s: _Ask) -> None:
    # 4a. A WATER ROW'S OWN DATES OVERRIDE A ZONE RELEASE LIMITED TO A KIND OF WATER (user ruling
    #     2026-09-29). "Bull trout (Dolly Varden) from streams, Aug 1-Oct 31" carries `water:
    #     stream`, part of its dimension (`daily@water=stream`), so it never met a stream row's own
    #     dated "Bull trout daily quota = 1, Sept 1-Oct 31" in step 4, and `water_dates_override`
    #     never ran. It runs here, on the base dimension: a surviving water rule printing its own
    #     dates for the fish overrides such a release on the days both hold, exactly as it does a
    #     dated zone release with no water kind. The overridden release then displaces nothing
    #     (step 4b) and releases nothing (step 5).
    every, out_, top = s.every, s.out_, s.top
    for k in sorted(out_):
        if not (s.competes(k) and s.zone_side(k) and released_on_water(every[k])):
            continue
        won = [o for o in sorted(s.cand)
               if o in out_ and s.competes(o) and o != k
               and _base_dimension(every[o]) == _base_dimension(every[k])
               and covers(every[o], every[k], top) and s.water_dates_override(o, k)]
        if won:
            s.lose(k, "water_dates", won[0])
            s.overridden.add(k)


def _step4b_zone_release(s: _Ask) -> None:
    # 4b. A ZONE RELEASE LIMITED TO A KIND OF WATER, ON THAT WATER (ZS-2, 2026-09-29). Region 3's
    #     "Lake trout from Oct 15-Jan 31" (no `water`) meets "Trout/char: 5" and its clauses in
    #     step 4 and displaces them; "Bull trout (Dolly Varden) from streams, Aug 1-Oct 31" carries
    #     `water: stream`, which is part of its dimension, so it met none of them and "Dolly Varden
    #     — 1 per day" spoke beside "release all" on the same stream on the same day. A release
    #     `released_on_water`, surviving step 4, displaces its OWN REGION'S TABLE's rules that keep
    #     the fish (`rules.yields_to_release`, every origin kept released here) in the same
    #     dimension read without conditions ("daily": the quota, its "1 over 50 cm", "4 from
    #     streams"), never a size clause of another dimension ("none under 60 cm" stays beside),
    #     never a closure (it keeps nothing), never a water row, never another region's rule.
    #     Bound here is being on its kind of water (its extents draw only that kind).
    #     GENERALISED (RU-4, 2026-10-04): a zone release printed with NO water kind met only the
    #     rules of its own key in step 4, so Region 5's "you must release: ALL STEELHEAD" silenced
    #     "Trout/char: 5" and left "2 per day … from streams" (`daily@water=stream`) speaking for a
    #     steelhead on every Region 5 stream. Its own table's keeping rules give way in the same
    #     base dimension WHATEVER their water condition: the release holds on every water, so the
    #     "from streams" clause is a clause of the quota it empties for that fish.
    #     ITS REAL REACH (review F4): "keepers in the base dimension" includes the SAME key, so it
    #     also breaks a same-key tie INSIDE one table — the 7B grayling release May 1-Jun 15
    #     silences that table's "2 per day" and "1 over 45 cm" on its dates (book-correct: the
    #     fish is released then). Only an UNCONDITIONED release reaches this far:
    #     `release_origins` is None for a `while` / `when_targeting` release (pinned by
    #     test_a_conditioned_zone_release_leaves_its_tables_quotas).
    from pipeline.deliver.bundle.rules import closure_grade, release_origins, yields_to_release
    every, out_ = s.every, s.out_
    competes, place, base, family = s.competes, s.place, s.base, s.family
    for k in sorted(out_):
        if not (competes(k) and place(k) >= 2 and base(k) is not None):
            continue
        kind = released_on_water(every[k])
        if kind is None:
            if not (release_origins(every[k]) and not every[k].get("water")
                    and closure_grade(every[k]) is None):
                continue
            kind = "*"                                     # a release on every kind of water
        freed = release_origins(every[k])
        for o in sorted(out_):
            keeps = yields_to_release(every[o])
            if o != k and competes(o) and place(o) >= 2 and base(o) == base(k) \
                    and family(o) != family(k) and keeps and keeps <= freed \
                    and (every[o].get("water") in (None, kind) if kind != "*" else
                         # the keeper's only condition beyond the base is its water kind
                         str(every[o].get("dimension")) in (
                             _base_dimension(every[k]),
                             f"{_base_dimension(every[k])}@water={every[o].get('water')}")) \
                    and _base_dimension(every[o]) == _base_dimension(every[k]):
                s.lose(o, "zone_release", k)


def _on_its_water(x: dict) -> bool:
    """A rule printed for a kind of water is bound here only on that kind (every extent
    draws it: `feature_types: [water]`), or it carries no water kind at all."""
    water = x.get("water")
    if not water:
        return True
    exts = x.get("extents") or []
    return bool(exts) and all(list(e.get("feature_types") or []) == [water] for e in exts)


def _own_water_closure(s: _Ask, k) -> bool:
    return WATER_CLOSURE_DOMINANT and str(k[0]).startswith("r") and s.place(k) == 0 \
        and not s.row_area(k)


def _step4c_closures(s: _Ask) -> None:
    # 4c. A CLOSURE SPEAKS FOR EVERY FISH IT COVERS AS IF IT NAMED IT, WHATEVER ITS KEY (RU-7,
    #     2026-10-04). Step 4 lets a closure displace the keeping rules of its own key; a blanket
    #     stream closure carries `water: stream` (`daily@water=stream`) and so never met the
    #     region's "5 per day" or "1 over 50 cm" (`daily`): Michel Creek on May 1 showed Region 4's
    #     "No fishing in streams, Apr 1-Jun 14" beside its "5 per day". A FULL closure in force
    #     here (`rules.closure_grade`), bound on its kind of water, displaces every rule that keeps
    #     the fish in the same base dimension that it BEATS BY THE LADDER (`order`: exactly what
    #     step 4 asks of two rules of one key, so a water row naming the fish still speaks beside
    #     a zone closure here as it does there), over the lengths it covers — never two regions'
    #     peers (step 6 reads those), never its own family. Page noise only: the status index
    #     already read the closure.
    #     A WATER'S OWN FULL CLOSURE IS THE MOST DOMINANT RULE (user ruling 2026-10-06, DENETIAH).
    #     Denetiah Creek prints "No fishing, Jul 1-15"; the Liard River watershed row (an AREA row
    #     of the water tables, so `water_side`, which step 5 never lets a water release silence)
    #     prints "Dolly Varden/bull trout — 1 in possession (30-50 cm only)". Step 4 took the
    #     row's daily 1 (same key, better rung) but the possession 1 is another key, and it spoke
    #     beside the closure. A closure WRITTEN FOR THIS WATER (a row's rule — `r…` entry — bound
    #     here at rank 0: not reached by the walk, not an area row) in force here silences, for
    #     every fish it covers, every rule that would let the fish be kept, WHATEVER ITS SOURCE
    #     AND KEY: zone, area rows, other rows (by the walk), possession, annual, size-only — the
    #     book closes the water, nothing is kept there those days. Two exceptions: a SUPERIOR
    #     authority's rule (the ladder's top: nothing here outranks it), and the closure's OWN
    #     LIFTERS (a rule that exempts it in part — an origin, a target — speaks beside it; one
    #     that lifts it outright has already removed it in step 3). A zone closure keeps RU-7's
    #     reach (by the ladder, its own base dimension).
    from pipeline.deliver.bundle.rules import closure_grade, yields_to_release
    every, out_, top = s.every, s.out_, s.top
    competes, family, order, peers = s.competes, s.family, s.order, s.peers
    for k in sorted(out_):
        x = every[k]
        if not (competes(k) and closure_grade(x) == "full" and _on_its_water(x)):
            continue
        own = _own_water_closure(s, k)
        lifters = {o for o in s.here
                   if any((t["entry_id"], t["rule_id"]) == k for t in every[o].get("exempts") or [])}
        for o in sorted(out_):
            if o == k or not competes(o) or family(o) == family(k) \
                    or not yields_to_release(every[o]) or not covers(x, every[o], top):
                continue
            if own and order(o)[0] == 1 and o not in lifters:
                s.lose(o, "water_closure", k)
            elif not peers(o, k) and _base_dimension(every[o]) == _base_dimension(x) \
                    and order(k) < order(o):
                s.lose(o, "closure", k)


def _released_lengths(x: dict, top: float) -> list | None:
    # the class the rule releases outright: its take-0 bands, for every angler (no `while`,
    # no target, not a clause) — the origin it holds for is checked against the keeper's
    if x.get("while") or x.get("caught") or x.get("when_targeting") or x.get("within"):
        return None
    bands = [b for b in (x.get("lengths") or []) if b.get("take") == 0]
    return _spoken_lengths({"lengths": bands}, top) if bands else None


def _step4d_size_release(s: _Ask) -> None:
    # 4d. A WATER'S SIZE-LIMITED RELEASE MEETS THE ZONE'S SIZE CLAUSE FOR THE SAME SIZES (RU-5,
    #     2026-10-04). Lakelse Lake's "Rainbow trout (none over 50 cm)" is sizes with no count
    #     (`daily/size`); Region 6's "no more than 1 over 50 cm" is a count (`daily`), so the two
    #     never met and the page said "1 over 50 cm" on a lake where none over 50 cm may be kept.
    #     The stricter-at-water ruling (2026-09-25) and `covers` decide it: a water-side rule
    #     RELEASING a size class (a band with take 0) displaces a zone-side keeping rule of the
    #     same base dimension whose spoken lengths lie wholly inside the released class. The
    #     zone's "5 per day" (every length) stays: the 5 still counts the rainbow under 50 cm.
    from pipeline.deliver.bundle.rules import ORIGINS, yields_to_release
    every, out_, top = s.every, s.out_, s.top
    competes, family, clock = s.competes, s.family, s.clock
    for o in sorted(out_):
        freed_lengths = _released_lengths(every[o], top) if competes(o) and s.water_side(o) \
            and every[o].get("type") == "retention_limit" else None
        if not freed_lengths:
            continue
        freed_origins = frozenset({every[o]["origin"]}) if every[o].get("origin") else ORIGINS
        for k in sorted(out_):
            keeps = yields_to_release(every[k])
            spoken = _spoken_lengths(every[k], top) if competes(k) and s.zone_side(k) \
                and keeps and keeps <= freed_origins and not s.closure(k) \
                and family(o) != family(k) and clock(k) == clock(o) else None
            if spoken and all(any(lo >= x and hi <= y for x, y in freed_lengths)
                              for lo, hi in spoken):
                s.lose(k, "size_release", o)


def _beaten_by(s: _Ask, k) -> list:
    every = s.every
    g = s.keyed.get((every[k]["type"], every[k]["dimension"]), [])
    return [o for o in g if s.order(o) < s.order(k) and s.family(o) != s.family(k)
            and not s.peers(o, k)]


def _same_row_dated_release(s: _Ask, k) -> bool:
    """A DATED OUTRIGHT RELEASE OF THE SAME ROW DISPLACES THE ROW'S OWN UNDATED QUOTA FOR
    THE FISH ON ITS DATES (RU-3, 2026-10-04). The Thompson below Kamloops Lake prints "Trout
    and char — 2 per day" and, for its CNR stretch, "trout/char catch and release … May
    1-31"; Adams Lake "Lake trout — release all, Oct 15-Jan 31" beside "bull trout and lake
    trout — 1 per day". Two rules of one row rank alike (same place, same naming), so step 4
    tied them and the page said "2 per day" and "release all" at once in the one month the
    row singles out. The dated release is the row's own exception to its general number: on
    its dates the number is silent, as a water's release silences the zone's. Only this
    shape — the release DATED, the quota UNDATED, both the row's own (`water_side`), another
    family — never the reverse (an undated release beside a dated keeping window is a window
    the row prints to OPEN the fish, and both stand as printed).

    ITS REAL REACH (review F4): `rel` holds every rule `release_origins` reads as a release,
    and a CLOSURE (take 0, may not fish) is one — so a row's DATED CLOSURE silences the row's
    own undated quota on its dates too (Quatse r1 over r2, the Region 7 lakes' winter
    closures over their quotas, Kitimat's Mar 16-May 31 over its hatchery 2). That is the
    book: on those dates the water is closed to the fish. 356 answers over every key."""
    return _same_row_releaser(s, k) is not None


def _same_row_releaser(s: _Ask, k):
    """The dated release or closure of `k`'s own row that silences it (`_same_row_dated_release`),
    or None."""
    from pipeline.deliver.bundle.rules import yields_to_release
    every, rel = s.every, s.rel
    x = every[k]
    if (x.get("when") or {}).get("dates") or not (x.get("take") or x.get("unlimited")):
        return None                       # a size clause is its own subject (Koocanusa)
    return next((o for o in sorted(rel)
                 if o != k and o[0] == k[0] and s.water_side(o) and s.family(o) != s.family(k)
                 and bool((every[o].get("when") or {}).get("dates"))
                 and _base_dimension(every[o]) == _base_dimension(x)
                 and yields_to_release(x) <= rel[o]), None)


def _step5_water_release(s: _Ask) -> None:
    # 5. A WATER'S RELEASE SILENCES THE ZONE FOR THAT FISH (user ruling, 2026-09-25). Competition
    #    keys on (type, dimension), and a zone quota's conditions are part of its dimension, so
    #    Coquihalla's "Trout/char (including steelhead) catch and release" never met Region 2's
    #    "2 hatchery steelhead" (`daily@origin=hatchery`) and both spoke. An outright release in
    #    force here (`release_origins`) that is written for this water (or reaches it by the
    #    tributary walk) displaces every zone or provincial quota that would let the angler keep
    #    that fish (`yields_to_release`), whatever conditions the quota holds under — provided
    #    every origin the quota could keep is released here (by the water row or by the zone's
    #    own releases: Chilliwack's "hatchery cutthroat catch and release" beside Region 2's
    #    "wild trout/char from streams" releases every cutthroat, so "Trout/char: 4" is silent;
    #    Morris Lake's "Wild trout/char catch and release" leaves the region's 4 for hatchery
    #    trout). A rule about another fish never gets here (`speaks_for`), and a closure is never
    #    displaced (a closure keeps nothing).
    #    The releases are read from EVERY competitor in force, not only step 4's survivors: a
    #    water release that lost step 4 to a release NAMING the fish (Pine River's "Catch and
    #    release all fish" under Zone B's "Bull trout … release") still releases it, and one that
    #    lost to a looser zone rule naming the fish (Adams River's "Rainbow trout and char catch
    #    and release" under Region 3's "Lake trout: none under 60 cm") is the stricter water rule
    #    and speaks again — naming only lets a STRICTER zone rule beat a water row's group. A
    #    release displaced by a superior authority that lets the fish be kept stays displaced and
    #    releases nothing here (one displaced by a superior CLOSURE — a national park — still
    #    releases: nothing the region keeps survives either).
    from pipeline.deliver.bundle.rules import release_origins, yields_to_release
    every, out_, why, rel = s.every, s.out_, s.why, s.rel
    competes, water_side, zone_side = s.competes, s.water_side, s.zone_side
    for k in s.cand:
        o = release_origins(every[k]) if competes(k) else None
        # A release a water row's own dates overrode releases nothing here (SP-12): it is not in
        # force at this water on this day, whatever step 4's naming said about it. Nor does an
        # INHERITED release the water's own row beat (Q5): the own row answers for the fish here.
        if not o or k in s.overridden or (k not in out_ and why.get(k, ("",))[0] == "own_row"):
            continue
        by = _beaten_by(s, k)
        if any(every[b]["_rank"] < 0 and yields_to_release(every[b]) for b in by):
            continue
        rel[k] = o
        if k not in out_ and water_side(k) and by \
                and all(zone_side(b) and yields_to_release(every[b]) for b in by):
            out_.add(k)
            why.pop(k, None)
    water_rel = frozenset().union(*[o for k, o in rel.items() if water_side(k)])
    released = frozenset().union(*rel.values())

    if water_rel:
        for k in sorted(out_):
            keeps = yields_to_release(every[k])
            if not (keeps and competes(k) and keeps <= released and keeps & water_rel):
                continue
            if zone_side(k):
                s.lose(k, "water_release",
                       next(o for o in sorted(rel) if water_side(o) and rel[o] & keeps))
            elif water_side(k) and _same_row_dated_release(s, k):
                s.lose(k, "same_row_release", _same_row_releaser(s, k))


def _step5b_moot_size_clause(s: _Ask) -> None:
    # 5b. A ZONE SIZE CLAUSE MADE MOOT BY AN OUTRIGHT RELEASE OF THE SAME FISH IS NOT SHOWN (user
    #     ruling 2026-10-05, CLEAN round, option A — what the consumer's front end already does). A
    #     clause stating only sizes ("none under 60 cm", `daily/size`) is its own dimension, so
    #     steps 4b/4c (base dimension "daily") never reached it and it spoke beside Region 3's
    #     "Lake trout from Oct 15-Jan 31" on Bonaparte Lake on Nov 1, beside the stream release and
    #     the spring stream closure on Eleven Mile Creek, and beside a national park's closure —
    #     while a water's own release (step 5) already silenced it. One rule now, WHOEVER wrote the
    #     release: a zone-side size-only keeper (`zone_side`: zone, area, province — not a row) gives
    #     way to a SURVIVING, competing outright release or closure (`release_origins`; in force on
    #     this day, whatever its rank — water, zone, superior) of another family that releases every
    #     origin the clause keeps (a wild-only release leaves a hatchery-only clause standing), over
    #     every length the clause speaks of (`covers`), on the same clock. The release or closure is
    #     untouched: a closure is still a closure (no gear in the water), a release still a release.
    #     The clause has nothing left to say today: it is not in the answer, and a traced answer
    #     returns it as "moot" ("doesn't matter today"), `by` the release.
    from pipeline.deliver.bundle.rules import release_origins, yields_to_release
    every, out_, top = s.every, s.out_, s.top
    competes, family, clock = s.competes, s.family, s.clock
    moot_by = [o for o in out_ if competes(o) and release_origins(every[o])] \
        if MOOT_SIZE_CLAUSE_HIDDEN else []
    for k in sorted(out_):
        x = every[k]
        if not (competes(k) and s.zone_side(k) and x.get("type") == "retention_limit"
                and x.get("take") is None and not x.get("unlimited") and x.get("lengths")):
            continue
        keeps = yields_to_release(x)
        won = [o for o in sorted(moot_by)
               if o != k and family(o) != family(k) and keeps <= release_origins(every[o])
               and covers(every[o], x, top) and clock(o) == clock(k)] if keeps else []
        if won:
            s.lose(k, "moot_size_clause", won[0])


def _step6_two_regions(s: _Ask) -> None:
    # 6. TWO REGIONS' BASES: THE MOST STRICT APPLIES (user ruling 2026-09-25). A lake drawn across
    #    a region line binds both regions' zone rules (`registry.regions.in_region`) — Ahbau Lake
    #    (51 % Region 5, 49 % Zone 7A), Mara Lake (61 % Region 3, 39 % Region 8). Neither table
    #    outranks the other by place, so step 4 set them against each other not at all (`peers`);
    #    here, per fish, a rule of one region is displaced by a STRICTER rule of the other:
    #      a closure beats a retention rule that is not one (closed beats open);
    #      an outright release beats a quota keeping only origins it releases;
    #      the lower of two quotas stating the same thing (`rules.same_statement`) beats the
    #      higher — "Kokanee: 5" over "Kokanee: 10".
    #    Quotas stating different things sit beside each other (the stricter binds by itself), and
    #    gear and method rules are never displaced here: both regions' restrictions apply.
    from pipeline.deliver.bundle.rules import same_statement, yields_to_release
    every, out_, base, peers = s.every, s.out_, s.base, s.peers
    mine = [k for k in out_ if s.competes(k) and base(k) is not None]
    if len({base(k) for k in mine}) > 1:
        gone = {k for k in mine
                if any(peers(o, k) and stricter(every[o], every[k]) for o in mine)}
        for k in sorted(gone):
            s.lose(k, "stricter_region", next(o for o in sorted(mine)
                                              if peers(o, k) and stricter(every[o], every[k])))
        # TWO REGIONS' IDENTICAL STATEMENTS ARE ONE (RU-8, 2026-10-04): Ahbau Lake (Regions 5
        # and 7A) printed "Trout and char — 5 per day" and "1 over 50 cm" twice, once per table.
        # Of two peers that are the same statement with the same number, one is shown — the
        # lower entry id (z5 before z7a: deterministic, no section lookup; display only, the
        # number is the same either way).
        left = sorted(k for k in mine if k not in gone)
        for i, k in enumerate(left):
            for o in left[:i]:
                if o in out_ and k in out_ and peers(o, k) \
                        and every[o].get("type") == "retention_limit" \
                        and every[k].get("type") == "retention_limit" \
                        and yields_to_release(every[o]) and yields_to_release(every[k]) \
                        and same_statement(every[o], every[k]) \
                        and every[o].get("take") == every[k].get("take") \
                        and bool(every[o].get("unlimited")) == bool(every[k].get("unlimited")):
                    s.lose(k, "same_as_peer", o)


def _answer(s: _Ask, trace: bool) -> List[dict]:
    """The answer: every rule left in, with its state; with `trace`, every loser too."""
    every, orig, partly, partly_by = s.every, s.orig, s.partly, s.partly_by

    def name(k) -> str:
        return f"{k[0]}::{k[1]}"

    def body(k) -> dict:
        return {a: b for a, b in orig[k].items() if a != "_rank"}

    answer = [(name(k), dict(body(k), state=s.said(k),
                             **({"partly_lifted": True} if k in partly else {}),
                             **({"lifted_in_part_by": sorted(name(o) for o in partly_by[k])}
                                if trace and k in partly_by else {})))
              for k in s.out_]
    if trace:
        # THE LOSERS (gap G1): every rule that took part — in force, about this fish, not a
        # lift-only rule, not a rainbow rule for the steelhead's sizes (step 0: about another
        # fish) — and is not in the answer, with the step that removed it and the rule that won.
        took_part = {k for k in s.live - s.no_rainbow
                     if speaks_for(every[k], s.fish) and every[k].get("dimension") != "lift"}
        for k in took_part - s.out_:
            reason, by = s.why[k]
            answer.append((name(k), dict(body(k), state=LOSS_REASONS[reason], reason=reason,
                                         by=name(by))))
    return [x for _, x in sorted(answer, key=lambda p: p[0])]


# --------------------------------------------------------------------------------------------
# Licensing: which requirements are in force on a section, on a day
# --------------------------------------------------------------------------------------------
# The reference reader for WHERE AND WHEN a requirement holds — the angler is unknown, so `who`
# and `doing` are left for the sentence (app/packages/core/src/regulations.ts) and every record
# that holds is returned. Licensing never votes on open/closed: this answers what holds while the
# water is open, and a closed water needs no licence by construction.


def designations_in_force(db, section: int, on) -> list[dict]:
    """The designations bound to `section` that are in force on the day: their `when` holds
    (a `part` day counts — it holds at some hours), and no `suspended_while` closure of their entry
    binds the section that day. Each is its record, with `entry_id` and `id`. Public: the licence
    answer reads it for the designation boxes and for which `licence_terms` (classified class,
    units) price the water (consumer 7.7 step 3)."""
    out = []
    rules_here: set | None = None
    for eid, did, rec in db.execute(
            "SELECT d.entry_id, d.designation_id, d.record FROM designation_section ds "
            "JOIN designation d ON d.entry_id = ds.entry_id AND d.designation_id = "
            "ds.designation_id WHERE ds.sid = ?", (section,)):
        r = json.loads(rec)
        if in_force(r.get("when"), on) == "no":
            continue
        asleep = False
        for s in r.get("suspended_while") or []:
            if rules_here is None:
                rules_here = {(e, x) for e, x in db.execute(
                    "SELECT rs.entry_id, rs.rule_id FROM section_ruleset sr JOIN ruleset rs "
                    "ON rs.set_id = sr.set_id WHERE sr.sid = ?", (section,))}
            if (eid, s["rule_id"]) in rules_here:
                w = db.execute("SELECT when_ FROM rule WHERE entry_id = ? AND rule_id = ?",
                               (eid, s["rule_id"])).fetchone()
                if in_force(json.loads(w[0]) if w and w[0] else None, on) == "yes":
                    asleep = True
        if not asleep:
            out.append(dict(r, entry_id=eid, id=did))
    return out


def stamp_waived_here(db, section: int, on) -> list[str]:
    """The designations (`entry_id#id`) that waive the Steelhead Stamp OUTRIGHT on this section
    on this day — "(Steelhead Stamp not required)", in force (`Designation.waives_every_stamp`).
    Where one holds, no steelhead stamp is required (user ruling 2026-10-02)."""
    from pipeline.regs.parsing.catalogue import Designation
    return sorted(f"{d['entry_id']}#{d['id']}" for d in designations_in_force(db, section, on)
                  if Designation.model_validate({k: v for k, v in d.items()
                                                 if k != "entry_id"}).waives_every_stamp)


def requirements_in_force(db, section: int, on) -> dict:
    """Every requirement in force on `section` on the day, `{entry_id#req_id: why}` under
    `"holds"`, and the ones an outright stamp waiver lifts there, under `"waived"`:
    `{entry_id#req_id: [designation]}`;
    under `"also_printed"`, `{entry_id#req_id: [entry_id#req_id, …]}`, the records that RESTATE a
    holding one (one obligation printed twice is one key; RU-12) — and a zone table's
    `on_designation` restatement holds on its own region's designations only.

    A requirement holds where it is placed (`requirement_section`; `province` everywhere but its
    `province_except` kind and tidal water; `on_designation` wherever a designation satisfying its
    `on` is in force), on the days of its `when`; one with `on` and sections needs that designation
    in force too. `waived_where: steelhead_stamp_waived` lifts it where `stamp_waived_here` holds.

    A REQUIREMENT IN A PART NOBODY HAS DRAWN (`undrawn_part`: the Creston Valley WMA permit on
    the south end of Kootenay Lake's Main Body) never holds of the section: it is listed under
    `"not_yet_mapped"`, `{entry_id#req_id: part}`, to be SHOWN on the water, as a rule in an undrawn
    part is (`not_yet_mapped`).

    A REQUIREMENT FOR THE OTHER KIND OF WATER DOES NOT HOLD (G5, adopted 2026-10-06): one printed
    for streams (`water: stream` — the province's Classified Waters Licence,
    `zp:steelhead#steelhead_targeting`) does not hold on a lake or a wetland. A record that
    RESTATES another binds as that record (`as_bound`: its `who`, `satisfied_by` and `water` —
    one obligation). The section's kind is its named water's (`item.kind`); a section in no named
    water (a walked tributary) keeps it — the tributary walk walks streams only. Listed under
    `"wrong_water"`, `{entry_id#req_id: the water it is for}`, as `waived` lists the waived ones.

    A SUPERIOR AUTHORITY'S REQUIREMENT DISPLACES THE PROVINCIAL ONES (G5, adopted 2026-10-06): where
    a requirement with `authority: superior` holds (a national park's fishing permit), every other
    holding requirement that has a way to satisfy it (`satisfied_by`, as bound) is NOT VALID there —
    "a federal or park authority's requirement replaces provincial licences". It is moved to
    `"displaced"`, `{entry_id#req_id: [superior entry_id#req_id, …]}`. (`province_except` already
    keeps the province-placed records out of national parks; this reaches the sections- and
    designation-placed ones.)"""
    desig = designations_in_force(db, section, on)
    have = {"classified_period": bool(desig),
            "steelhead_period": any(
                d.get("steelhead_stamp_during") is not None
                and in_force(d["steelhead_stamp_during"].get("when"), on) != "no" for d in desig)}
    # A ZONE TABLE'S RESTATEMENT HOLDS ON ITS OWN REGION'S DESIGNATIONS ONLY (RU-12, 2026-10-04):
    # Region 4's "Classified Waters Licence … many East Kootenay rivers (map p.33)" is Region 4's
    # wording and held on the Dean (Region 5) and the Seymour (Region 1) beside the province's.
    desig_regions = {_entry_region(d["entry_id"]) for d in desig}

    def own_region(eid: str) -> bool:
        r = base_region(eid)
        return r is None or _book_region(r) in desig_regions
    placed = {(e, r) for e, r in db.execute(
        "SELECT entry_id, req_id FROM requirement_section WHERE sid = ?", (section,))}
    tidal = db.execute("SELECT 1 FROM tidal WHERE sid = ?", (section,)).fetchone() is not None
    excepted = {k for (k,) in db.execute(
        "SELECT area_kind FROM province_except WHERE sid = ?", (section,))}
    waivers = stamp_waived_here(db, section, on)
    holds: dict[str, str] = {}
    waived: dict[str, list[str]] = {}
    undrawn: dict[str, str] = {}
    restated: dict[str, str] = {}
    records = {f"{e}#{r}": (e, r, p, json.loads(rec)) for e, r, p, rec in db.execute(
        "SELECT entry_id, req_id, placement, record FROM requirement")}
    for eid, rid, placement, r in records.values():
        if placement == "sections":
            if (eid, rid) not in placed:
                continue
            why = "sections"
        elif placement == "province":
            kinds = {x.get("outside_area_kind") for x in r.get("extents") or []} - {None}
            if tidal or kinds & excepted:
                continue
            why = "province"
        elif placement == "on_designation":
            if not own_region(eid):
                continue
            why = "on_designation"
        else:
            continue                                     # unresolved: the reader says "check"
        if r.get("on") and not have[r["on"]]:
            continue
        if in_force(r.get("when"), on) == "no":
            continue
        key = f"{eid}#{rid}"
        if not_yet_mapped(r):
            undrawn[key] = str(r["undrawn_part"]).strip()
            continue
        if r.get("waived_where") == "steelhead_stamp_waived" and waivers:
            waived[key] = waivers
            continue
        holds[key] = why
        if r.get("restates"):
            restated[key] = f"{r['restates']['entry_id']}#{r['restates']['id']}"
    # ONE OBLIGATION, PRINTED TWICE, IS ONE KEY (RU-12, 2026-10-04): a record that `restates`
    # another (Region 4's Classified Waters Licence line, the Shuswap row's char stamp, the
    # Creston permit on the CVWMA row) is folded into the record it restates when that one holds
    # too — listed under `also_printed` on the restated key, not as an obligation of its own.
    also: dict[str, list[str]] = {}
    for key, of in sorted(restated.items()):
        if of in holds and of != key:
            also.setdefault(of, []).append(key)
            holds.pop(key)
    # THE OTHER KIND OF WATER (G5): read on the obligation as bound — after the fold, so a
    # restatement goes where the record it restates goes
    kind = section_kind(db, section)
    every_req = {x: v[3] for x, v in records.items()}
    bound = {k: as_bound(records[k][3], every_req) for k in holds}
    wrong: dict[str, str] = {}
    if kind is not None:
        for k in list(holds):
            w = bound[k].get("water")
            if w and w != kind:
                wrong[k] = w
                holds.pop(k)
    # A SUPERIOR AUTHORITY DISPLACES (G5)
    superior = sorted(k for k in holds if records[k][3].get("authority") == "superior")
    displaced: dict[str, list[str]] = {}
    if superior:
        for k in list(holds):
            if records[k][3].get("authority") != "superior" and bound[k].get("satisfied_by"):
                displaced[k] = superior
                holds.pop(k)
    also = {k: v for k, v in also.items() if k in holds or k in displaced}
    return {"holds": holds, "waived": waived, "not_yet_mapped": undrawn, "also_printed": also,
            "wrong_water": wrong, "displaced": displaced,
            "considered": considered_records(db, section, tidal, excepted)}


#: Every licensing table and its id column: the records a section's sources may list.
LICENSING_TABLES = (("requirement", "req_id"), ("designation", "designation_id"),
                    ("exemption", "exemption_id"), ("alternative", "alternative_id"),
                    ("licence_terms", "terms_id"), ("not_classified", "not_classified_id"))


def considered_records(db, section: int, tidal: bool, excepted: set) -> list[str]:
    """EVERY RECORD A SECTION'S "ALL LICENCE SOURCES" LISTS (consumer 7.7 step 2, M4: decided here,
    beside the requirements in force, not again by the answers): its licensing set's records, in
    (entry, record) order, then every record placed `province`, `on_designation` or `not_placed`
    — unless an extent stops it at one of the section's province-exception kinds, or the water is
    tidal, where no province-wide record holds."""
    row = db.execute("SELECT set_id FROM section_licensing WHERE sid = ?", (section,)).fetchone()
    own = [f"{e}#{r}" for e, r in db.execute(
        "SELECT entry_id, record_id FROM licensing_set WHERE set_id = ? ORDER BY entry_id, "
        "record_id", (row[0],))] if row else []
    glob: list[str] = []
    if not tidal:
        recs = []
        for tab, idcol in LICENSING_TABLES:
            cols = [c[1] for c in db.execute(f"PRAGMA table_info({tab})")]
            for x in db.execute(f"SELECT * FROM {tab}"):
                d = dict(zip(cols, x))
                recs.append((f"{d['entry_id']}#{d[idcol]}", d.get("placement"),
                             json.loads(d["record"])))
        glob = [k for k, placement, r in sorted(recs, key=lambda t: t[0])
                if placement in ("province", "on_designation", "not_placed")
                and not any(x.get("outside_area_kind") in excepted for x in r.get("extents") or [])]
    return list(dict.fromkeys(own + glob))


def section_kind(db, section: int) -> str | None:
    """The kind of the named water a section is part of (`item.kind`: lake, stream, wetland), or
    None for a section of no named water."""
    got = db.execute("SELECT i.kind FROM item_section s JOIN item i ON i.ord = s.ord "
                     "WHERE s.sid = ?", (section,)).fetchone()
    return got[0] if got else None


def as_bound(r: dict, requirements: dict) -> dict:
    """A requirement as it binds: one that RESTATES another (`restates`) reads the restated record's
    `who`, `satisfied_by` and `water` — one obligation printed twice (RU-12). `requirements` is
    every requirement record by `entry_id#req_id`."""
    rs = r.get("restates")
    if rs:
        o = requirements.get(f"{rs['entry_id']}#{rs['id']}")
        if o:
            return dict(r, who=o.get("who"), satisfied_by=o.get("satisfied_by"),
                        water=o.get("water"))
    return r


def _entry_region(entry_id: str) -> str | None:
    """`r6:…` / `z4:…` -> "6" / "4"; `zp:` and anything else -> None."""
    head = str(entry_id).split(":", 1)[0]
    return head[1:] if head[:1] in ("r", "z") and head != "zp" and head[1:] else None


def _book_region(region: str) -> str:
    """A zone table's region as the ROWS name it: the book prints Region 7's rows under one
    "Region 7" (`r7:`) while its zone tables are 7A and 7B (`z7a:`, `z7b:`) — so "7a" -> "7".
    Without it a 7A/7B `on_designation` restatement could never hold on a Region 7 designation
    (review F7; none exists today, only z3 and z4)."""
    return region[:-1] if len(region) > 1 and region[-1] in "ab" and region[:-1].isdigit() \
        else region
