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
    """`datetime.date` or `(month, day)` -> the catalogue's day index (1..366)."""
    from pipeline.regs.parsing.catalogue import _day_index
    m, d = (on.month, on.day) if hasattr(on, "month") else on
    return _day_index(m, d)


@lru_cache(maxsize=None)
def _days_of(dates_json: str):
    from pipeline.regs.parsing.catalogue import DateRange, _days
    dates = [DateRange.model_validate(d) for d in json.loads(dates_json)]
    return frozenset(_days(dates)) if dates else None


def in_force(when: dict | None, on) -> str:
    """Whether a `when` (the bundle's JSON) holds on a day: "yes" all of it, "no", or "part" —
    it holds on that day only at some hours or weekdays, or its season could not be read
    (`unparsed`). A "part" rule is shown BESIDE what it would displace and displaces nothing."""
    if not when:
        return "yes"
    days = _days_of(json.dumps(when.get("dates") or [], sort_keys=True))
    if days is not None and _day(on) not in days:
        return "no"
    if when.get("hours") or when.get("weekdays") or when.get("unparsed"):
        return "part"
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
    (`catalogue.NAMING_GROUPS`): once trout include char (p.86) it is the book's only way to name
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


#: A rainbow longer than this is a steelhead where anadromous rainbow are found (p.86).
def _steelhead_min_cm() -> int:
    from pipeline.regs.parsing.catalogue import DEFINITIONAL_SIZE
    return int(DEFINITIONAL_SIZE["ST"]["min_cm"])


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
        return y
    return x if len(inside) == len(bands) else dict(x, lengths=inside)


_RULES_BY_PATH: dict = {}


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
    from pipeline.deliver.bundle.rules import (release_origins, same_statement,
                                               yields_to_release)
    if b.get("type") != "retention_limit" or a.get("type") != "retention_limit":
        return False
    shut = lambda x: x.get("take") == 0 and x.get("may_target") == 0 and not (
        x.get("lengths") or x.get("while") or x.get("when_targeting") or x.get("within"))
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
                    by_naming: bool = True) -> List[dict]:
    """THE RULES THAT SPEAK FOR ONE FISH, ON ONE SECTION, ON ONE DAY — the ladder as code.

    `section` is a bundle `sid`, `on` a `datetime.date` or `(month, day)`, `fish` a leaf species
    code. Returns the rules bound to the section that say something about that fish on that day,
    each a `rules()` dict with `state` added: "speaks", "beside" (in force only some hours or
    weekdays, or of unreadable season, or on one half of the channel only — `side`: shown, never
    displacing), "shown" (never competes), or
    "not_yet_mapped" (holds only in a part nothing draws). Sorted by `rid`.

      0. A RAINBOW OVER 50 CM IS A STEELHEAD where the bundle says anadromous rainbow are found
         (`steelhead_water`, p.86): asked about "RB" there, every rule is read over rainbow of 50
         cm or less (`as_rainbow`) — one speaking only of rainbow over 50 cm speaks for no
         rainbow, and a rainbow release "(50 cm or less)" is an outright release. The larger
         fish is asked about as "ST".
      1. IN FORCE ON THE DAY. A rule whose `when` excludes the day is out, and so is one dormant
         under `suspended_while` while its named closure is in force here.
      2. ABOUT THIS FISH (`speaks_for`). Competition is PER FISH: two rules compete only for the
         fish both speak for, so Zone B's "Bull trout … release" never touches what "Trout/char:
         5" says about a rainbow.
      3. LIFTS. A lift from a rule in force here removes the lifted rule for this fish — outright,
         or when its `species` holds the fish and its `when` holds the day. A lift that holds only
         while fishing FOR something (`when_targeting`) or while doing something (`while`), or
         only some hours, or only for a fish of some origin or size (`origin`, `lengths`: known
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
         So a water row that itself names the fish ("Bull trout daily quota = 1") beats the
         zone's bull trout rule: both name it, and the water is more specific.
         A rule is displaced only by a better rule of ANOTHER quota family: a `within` clause and
         its parent quota are one statement ("Trout/char: 5, but not more than 3 lake trout")
         and never displace each other. Ties all speak.
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
      5. A WATER'S RELEASE SILENCES THE ZONE FOR THAT FISH (user ruling, 2026-09-25): an outright
         release in force here, written for this water or reached by the tributary walk,
         displaces every zone/area/provincial quota that would keep the fish, WHATEVER its
         conditions, when every origin that quota keeps is released here (see step 5 below,
         `rules.release_origins`, `rules.yields_to_release`). Such a release counts even when
         step 4 put it behind a zone release naming the fish, and one step 4 put behind a
         looser zone rule naming the fish (one that lets it be kept) speaks again.
      6. TWO REGIONS' BASES — THE MOST STRICT APPLIES (user ruling 2026-09-25). A lake straddling
         a region line binds both regions' zone rules; neither outranks the other (step 4 does not
         set them against each other). Per fish, a zone rule of one region is displaced by a
         `stricter` zone rule of the other: a closure beats open, a release beats a quota, the
         lower of two quotas stating the same thing beats the higher; different statements sit
         beside; gear and method rules of both apply.
      Rules that never compete pass through with state "shown": `standing`, the information
      family. A "beside" rule neither displaces nor is displaced. Lift-only rules (dimension
      `lift`) state nothing and are not returned.
      A RULE IN A PART NOBODY HAS DRAWN (`not_yet_mapped`: `undrawn_part` — "No Fishing within 200
      m of Bush-Sullivan Bridge" held on the whole of Kinbasket Lake) is returned with state
      "not_yet_mapped" when it is in force and about the fish, and takes no part in anything
      above: it displaces nothing, lifts nothing, silences nothing and suspends nothing (user
      ruling 2026-09-26). It is a place on the water the map cannot show yet, never the water.

    `by_naming=False` ranks by place alone — the ladder before the naming ruling — and exists
    only so an audit can list what the ruling changed."""
    from pipeline.regs.parsing.catalogue import expand_species
    if expand_species([fish]) != [fish]:
        raise ValueError(f"effective_rules: {fish!r} is a group, not one fish — ask about a leaf "
                         f"code ({', '.join(expand_species([fish]))})")
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        bound = db.execute("SELECT r.entry_id, r.rule_id, r.via FROM section_ruleset s JOIN ruleset r "
                           "ON r.set_id = s.set_id WHERE s.sid = ?", (section,)).fetchall()
        steelhead_here = fish == "RB" and steelhead_water(db, section)
    finally:
        db.close()
    orig = _rules_of(path)
    every = orig
    here = {(e, r): via for e, r, via in bound if (e, r) in every}
    # 0. WHERE A RAINBOW OVER 50 CM IS A STEELHEAD (p.86), a rainbow rule speaks only for rainbow
    #    of 50 cm or less: each rule is read over that range (`as_rainbow`), and one that speaks
    #    only of rainbow over 50 cm ("1 over 50 cm") speaks for no rainbow here — the fish is a
    #    steelhead, asked about as "ST".
    no_rainbow: set = set()
    if steelhead_here:
        every = dict(orig)
        for k in here:
            if speaks_for(orig[k], fish):
                v = as_rainbow(orig[k])
                if v is None:
                    no_rainbow.add(k)
                else:
                    every[k] = v
    state = {k: in_force(every[k].get("when"), on) for k in here}
    # ONE HALF OF THE CHANNEL (`side`: Kitimat River's "No Fishing on the west half of river …"):
    # the rule holds on part of the section's width, so, like a rule holding some hours, it is
    # shown BESIDE the rules the other half answers to and displaces none (user ruling 2026-09-28).
    for k in here:
        if every[k].get("side") and state[k] == "yes":
            state[k] = "part"
    undrawn = {k for k in here if not_yet_mapped(every[k])}
    for k in here:                                          # 1. dormant under its closure
        sw = every[k].get("suspended_while")
        if sw and state.get((k[0], sw)) == "yes" and (k[0], sw) not in undrawn:
            state[k] = "no"
    live = {k for k, s in state.items() if s != "no"}
    lifted, partly = set(), set()
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
            if x.get("when_targeting") or x.get("while") or x.get("origin") or x.get("lengths"):
                partly.add(t)
                continue
            got = in_force(x.get("when"), on) if "when" in x else "yes"
            if got == "yes":
                lifted.add(t)
            elif got == "part":
                partly.add(t)
    cand = {k for k in live - lifted - no_rainbow
            if speaks_for(every[k], fish) and every[k].get("dimension") != "lift"}

    def competes(k) -> bool:
        x = every[k]
        return state[k] == "yes" and not x.get("standing") and x.get("family") != "information" \
            and k not in undrawn

    def closure(k) -> bool:
        x = every[k]
        return x.get("take") == 0 and x.get("may_target") == 0

    def order(k) -> tuple:
        x = every[k]
        rank = x["_rank"]
        if here[k] == "trib" and rank == 0:
            rank = 1
        # A `within` clause is named at its PARENT's level: "Trout/char: 5, but not more than 1
        # bull trout" is a trout/char quota with a sub-limit, not a bull trout rule — read as one,
        # it would reopen bull trout at a water printing "Trout/char catch and release".
        parent = every.get((k[0], x["within"])) if x.get("within") else None
        named = 0 if (not by_naming or closure(k) or names_fish(parent or x, fish)) else 1
        return (0 if rank < 0 else 1, named, rank)

    def family(k) -> tuple:
        x = every[k]
        return (k[0], x.get("within") or x.get("condition_of") or k[1])

    keyed: dict = {}
    for k in cand:
        if competes(k):
            keyed.setdefault((every[k]["type"], every[k]["dimension"]), []).append(k)

    from pipeline.deliver.bundle.rules import (release_origins, same_statement, statement,
                                               yields_to_release)

    def place(k) -> int:
        return 1 if here[k] == "trib" and every[k]["_rank"] == 0 else every[k]["_rank"]

    def water_and_zone(o, k) -> bool:
        """Two quotas that both let the fish be kept, one written for this water (or reaching it
        by the walk), the other a zone, area or provincial one — never a superior authority's."""
        return bool(yields_to_release(every[o]) and yields_to_release(every[k])
                    and min(place(o), place(k)) in (0, 1) and max(place(o), place(k)) >= 2)

    def dated_zone_release(k) -> bool:
        """A zone, area or provincial rule keeping NONE of the fish (take 0: a release, or a
        closure) on printed dates — Region 3's "you must release … Lake trout from Oct 15-Jan 31"."""
        x = every[k]
        return place(k) >= 2 and x.get("type") == "retention_limit" and x.get("take") == 0 \
            and bool((x.get("when") or {}).get("dates"))

    def exact_same(o, k) -> bool:
        """The water's rule says EXACTLY what the dated zone rule says — the same fish, sizes,
        origin, water kind, means and target (`rules.statement`) on the same dates."""
        return same_statement(every[o], every[k]) and \
            (every[o].get("when") or {}).get("dates") == every[k]["when"]["dates"]

    def dated_zone_retention(k) -> bool:
        """A zone, area or provincial retention rule on printed dates that states a NUMBER for
        the fish — a release (take 0) or a quota — and is no closure: Region 6's "Lake trout from
        Fraser and Skeena Watersheds, Sept 15-Nov 30" (must release)."""
        x = every[k]
        return place(k) >= 2 and x.get("type") == "retention_limit" and not closure(k) \
            and (x.get("take") is not None or bool(x.get("unlimited"))) \
            and not x.get("record_retention") and bool((x.get("when") or {}).get("dates"))

    def water_dates_override(o, k) -> bool:
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
        x, z = every[o], every[k]
        if not (0 <= place(o) <= 1 and x.get("type") == "retention_limit"
                and (x.get("take") is not None or x.get("unlimited"))
                and not x.get("within") and not x.get("record_retention")
                and bool((x.get("when") or {}).get("dates")) and dated_zone_retention(k)):
            return False
        if not (names_fish(x, fish) or statement(x)[0] == statement(z)[0]):
            return False
        for c in ("origin", "water"):
            if x.get(c) and x.get(c) != z.get(c):
                return False
        for c in ("while", "when_targeting"):
            if x.get(c) and set(x[c]) != set(z.get(c) or []):
                return False
        return True

    def beats(o, k) -> bool:
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
        Everything else: the better rung (`order`) displaces."""
        if water_dates_override(o, k):
            return True
        if water_and_zone(o, k):
            return place(o) <= 1 and same_statement(every[o], every[k])
        if 0 <= place(o) <= 1 and yields_to_release(every[o]) and dated_zone_release(k):
            return exact_same(o, k)
        return order(o) < order(k)

    def base(k) -> str | None:
        """The region whose OWN table (`z<region>:`, not the province's) wrote the rule."""
        return base_region(k[0]) if every[k]["_rank"] >= 2 else None

    def peers(o, k) -> bool:
        """Two regions' zone rules on one section — a lake straddling their line (step 6)."""
        a, b = base(o), base(k)
        return a is not None and b is not None and a != b

    out_ = set(cand)
    for group in keyed.values():
        for k in group:
            if not closure(k) and any(o != k and family(o) != family(k)
                                      and not peers(o, k) and beats(o, k) for o in group):
                out_.discard(k)

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
    def beaten_by(k) -> list:
        g = keyed.get((every[k]["type"], every[k]["dimension"]), [])
        return [o for o in g if order(o) < order(k) and family(o) != family(k)
                and not peers(o, k)]

    rel = {}
    for k in cand:
        o = release_origins(every[k]) if competes(k) else None
        if not o:
            continue
        by = beaten_by(k)
        if any(every[b]["_rank"] < 0 and yields_to_release(every[b]) for b in by):
            continue
        rel[k] = o
        if k not in out_ and 0 <= place(k) <= 1 and by \
                and all(place(b) >= 2 and yields_to_release(every[b]) for b in by):
            out_.add(k)
    water_rel = frozenset().union(*[o for k, o in rel.items() if 0 <= place(k) <= 1])
    released = frozenset().union(*rel.values())
    if water_rel:
        for k in sorted(out_):
            keeps = yields_to_release(every[k])
            if keeps and competes(k) and place(k) >= 2 and keeps <= released \
                    and keeps & water_rel:
                out_.discard(k)

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
    mine = [k for k in out_ if competes(k) and base(k) is not None]
    if len({base(k) for k in mine}) > 1:
        gone = {k for k in mine
                if any(peers(o, k) and stricter(every[o], every[k]) for o in mine)}
        out_ -= gone

    def said(k) -> str:
        if k in undrawn:
            return "not_yet_mapped"
        return "speaks" if competes(k) else "beside" if state[k] == "part" else "shown"

    return [dict({a: b for a, b in orig[k].items() if a != "_rank"}, state=said(k),
                 **({"partly_lifted": True} if k in partly else {}))
            for k in sorted(out_, key=lambda k: f"{k[0]}::{k[1]}")]
