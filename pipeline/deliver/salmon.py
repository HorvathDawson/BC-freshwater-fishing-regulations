"""THE DFO SALMON FILE (T211, option A + the Standard / Salmon map toggle): the federal salmon rules, read ONCE here and
shipped as answers per day, so the app decides nothing about a salmon rule (app/MAP.md: every answer is looked up).

    curated  data/curated/dfo_salmon/places/   locations: precedence, water, scope words
    feed     <generated>/regs/dfo_salmon/typed/            the typed rules, keyed by fingerprint
    reach    <reach run>/dfo_salmon.jsonl.gz               each location's sections (`pipeline.atlas.reach.dfo_salmon`)
       └─ build() ─▶ the bundle's `salmon_doc` + `salmon_section` ─▶ the release file `salmon` (`salmon/4`, encode())

THE UNIT IS A CHAIN: the DFO locations that hold on a section, NARROWEST FIRST (precedence: a water 3, a tidal-area
default 2, a section default or closure 1, the region default 0, a T349 page default -1; then, within a tier, the place covering FEWER sections —
T229, owner 2026-10-28: the more specific place wins; then location id — `chain_order`). Region 6 is a cascade — read flat,
section A opens coho on the Kispiox while B(i) closes it and the water reopens a five-week window
(`test_the_skeena_cascade_resolves_through_the_join`) — so per species and day the FIRST location of the chain with a
rule for that species on that day answers, never a wider one.

A DFO PLACE ANSWERS FOR ITS OWN REGION (T668, `SectionRegions`): an undrawn place rides only its own region's sections;
a region-wide place binds only its region's members; on a section in TWO regions (the one membership) both regions'
places hold — the chain's `s` names each region's places (indices into `l`), each cascades apart, and the MOST STRICT
answer per fish speaks (`strictest`; the live feed composes the same, `live.compose`).

PER CHAIN, FOR EVERY DAY OF THE YEAR (366: Feb 29 included; DFO's dates carry no year), precomputed here:
  * a VERDICT per species (chinook, coho, sockeye, pink, chum): `open` (a limit above zero), `release` (fish, keep
    none), `closed` (no fishing), `check` (DFO printed a season nobody can read, "to be determined") — or none: no rule
    of the chain holds that species that day, and the app says so rather than guessing;
  * the GEAR lines that hold (bait bans, hook rules) — the first location with any that day;
  * the MAP STATUS the Salmon colouring paints: `open` if any species may be kept, else `release` if any may be fished,
    else `check` if DFO printed a season nobody can read, else `closed`.

CLOSED UNLESS DFO OPENS IT (owner 2026-10-27): a species no location of the chain speaks for on a day is closed
(`NOT_LISTED`, `d: 1`), and a water DFO does not list at all is the UNLISTED chain (chain 0: every species closed, every
day) — an explicit chain, so the app reads "closed" from the file, never from a missing row.
Each is stored as runs (start day, value) — a chain's year is a handful of runs.

A LOCATION WITHOUT AN EXTENT IS NEVER DRAWN. It ships as a one-location chain under its water's item ids (`unmapped`),
so the card can say "location not yet mapped" with DFO's own rules for it.

THE REGION HEADER (owner 2026-10-28): each region's page header — daylight only, the facility closure, the aggregate
limits, minimum sizes, what an "adult chinook" is — as typed notes (`regs.dfo_salmon.header`) on `regions[slug].notes`,
DFO's words carried whole; a note naming waters holds only on their items (`scopes[slug][key]`: the curated DFO water
bindings, a watershed with every item upstream of it by the registry's `item_down`). The live feed carries the same
notes and wins (the release's are the offline answer of last resort, read from the cached page its rules came from).

THE FILES (`salmon/4`, split per bundle region — owner 2026-10-28: a water page's card fetches only its region's slice;
T332: an open verdict's quota is `tree`, a TreeTier of the game-fish quota tree, and a region's `bucket.tree` its total
tier; T356 (salmon/4): what the app used to work out itself is in the file — a verdict closed by its region header
carries `c` (the note id) and a chain closed by it every day `c` (the note ids; its map says closed),
a region carries `shared` (the scoped aggregate limits, `quota.shared_buckets`, with or without a bucket) and `sizes`
(the header sizes per species as `LengthBand`s, ordered, the first that holds wins — `quota.sizes`); the bucket's
`min_cm` is gone and a header note's length is `lengths` (T358). An older app refuses this file rather than mis-read it),
little-endian, varints LEB128 (`pipeline.deliver.binary`):
    salmon       magic b"BCSA", version u8 = 4, handles 8 bytes (the bundle's section_handles digest, raw), u32 JSON
                 length, the UTF-8 payload: {credit, regions, verdicts, gears, chains, unmapped, scopes, shards, x?}
                 (T321) x: location id -> the rows its card shows in DFO's words, {k: new|unread|note, sp, d, t}
    salmon-<r>   magic b"BCSR", version u8 = 1, handles 8 bytes, u8 region length + the region (ASCII), then a varint
                 run count and per run: sid gap (sid - previous run's end), count, chain index — the sections of bundle
                 region <r> (`release.shards.REGIONS`; a section in two regions is in both; `x`: a section in none)
A section in no run is chain 0, UNLISTED: closed to salmon fishing (`payload.default`). `decode_files` is the reference
reader the tests hold the app's to.
"""
from __future__ import annotations

import json
import re
import struct
from collections import defaultdict
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pipeline.common import calendar_spec
from pipeline.deliver.binary import put_varint, read_varint

FORMAT = "salmon/4"
SHARD_FORMAT = "salmon-shard/1"
MAGIC = b"BCSA"
VERSION = 4
SHARD_MAGIC = b"BCSR"
SHARD_VERSION = 1
#: the shard of a section in no bundle region (tidal, outside B.C.): the map reads it, no page does
NO_REGION = "x"
#: chinook, coho, sockeye, pink, chum — the order every card row and every per-species array is in
SPECIES: Tuple[str, ...] = ("CH", "CO", "SK", "PK", "CM")
ALL_SALMON = "SA"
#: the map status codes, as the file stores them ("none" only for a chain with no verdict at all — never built today)
MAP_CODES: Tuple[str, ...] = ("none", "open", "release", "closed", "check")
#: THE OWNER'S RULE (2026-10-27): a water DFO does not list for salmon, in any way, is CLOSED to salmon fishing; and on a
#: listed water a species no DFO row opens that day is closed too (the pages list openings). Stated in the data, never
#: left as an absence: this verdict is `verdicts[0]`, and chain 0 is the UNLISTED chain — every section in no run.
NOT_LISTED = {"s": "closed", "t": "Not opened by DFO: closed to salmon fishing", "d": 1}
UNLISTED_CHAIN = 0
CREDIT = "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations"
PAGE_URL = "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region{slug}-eng.html"
_GEAR_TYPES = {"tackle_restriction", "bait_restriction", "gear_restriction", "method_restriction"}

#: every calendar day as MMDD, Jan 1 → Dec 31 of a leap year (index = the app's day index)
DAYS: Tuple[int, ...] = tuple(m * 100 + d for m, d in map(calendar_spec.month_day, range(1, calendar_spec.DAYS + 1)))


class SalmonError(SystemExit):
    pass


# ── reading a rule on a day ──────────────────────────────────────────────────────────────────────────────────────────

def _md(m: int, d: int) -> int:
    return m * 100 + d


def holds_on(rule: dict, md: int) -> Optional[bool]:
    """Does the rule hold on day `md` (MMDD)? None: its season is printed but unreadable (`when.unparsed`). No dates is
    all year (the synopsis' reading; DFO's empty dates cell the same)."""
    w = rule.get("when") or {}
    if w.get("unparsed") and not w.get("dates"):
        return None
    dates = w.get("dates") or []
    if not dates:
        return True
    for d in dates:
        a, b = _md(d["from_month"], d["from_day"]), _md(d["to_month"], d["to_day"])
        if (a <= md <= b) if a <= b else (md >= a or md <= b):
            return True
    return False


def _species_of(rule: dict) -> Sequence[str]:
    return rule.get("species") or rule.get("when_targeting") or ()


def _covers(rule: dict, sp: str) -> bool:
    s = _species_of(rule)
    return sp in s or ALL_SALMON in s


def _is_gear(rule: dict) -> bool:
    return bool(rule.get("gear")) or rule.get("type") in _GEAR_TYPES


def _other_fish(rule: dict) -> bool:
    """A row about a fish that is not a salmon ("No fishing for eulachon", Region 6 B(ii)): never a salmon verdict, shown
    as its own line beside the gear lines (T229, owner 2026-10-28)."""
    from pipeline.regs.dfo_salmon.typed import OTHER_FISH
    return rule.get("type") == "advisory" and str(rule.get("review_reason") or "").startswith(OTHER_FISH)


def _when_words(rule: dict) -> str:
    from pipeline.deliver.answers.common import when_words
    return when_words(rule.get("when") or {}, "plain")


def _text(rules: Sequence[dict], subs: Mapping[str, List[dict]]) -> str:
    bits = []
    for r in rules:
        t = r.get("verbatim") or ""
        kids = [k.get("verbatim") or "" for k in subs.get(r.get("rule_id") or "", [])]
        if kids:
            t = f"{t}, {', '.join(k for k in kids if k)}"
        w = _when_words(r)
        bits.append(f"{t} ({w})" if w else t)
    return "; ".join(dict.fromkeys(b for b in bits if b))


def _notice(rules: Sequence[dict]) -> Optional[str]:
    ns = sorted({r["notice"] for r in rules if r.get("notice")})
    return ", ".join(ns) or None


#: T349 (owner 2026-11-02): SALMON NON-RETENTION IS CLOSED TO SALMON. "Non-retention", "No retention of salmon" or a
#: limit of 0 means the same as no fishing for salmon — catch-and-release exists only so OTHER fish (trout) may be fished
#: where salmon are present. A row answers `release` only where DFO's words say catch and release outright.
_RE_CATCH_AND_RELEASE = re.compile(r"\bcatch[- ]and[- ]release\b", re.I)


def says_catch_and_release(rule: dict) -> bool:
    """Does the rule's own text say catch-and-release (not just non-retention)?"""
    return bool(_RE_CATCH_AND_RELEASE.search(rule.get("verbatim") or ""))


def _opens(rule: dict) -> bool:
    return (rule.get("take") or 0) > 0


def unread_row(rule: dict) -> bool:
    """(T671 D2) A ROW NOBODY COULD TYPE: an advisory that is not about another fish (`_other_fish`) and not a gear
    permission — "to be determined", a season that does not parse, a species cell or wording the table does not know.
    It is never dropped: on the days it holds it reads "check DFO" for the species its cell names (`_reads_for`)."""
    from pipeline.regs.dfo_salmon.typed import GEAR_PERMISSION
    return (rule.get("type") == "advisory" and not rule.get("within") and not _other_fish(rule)
            and not str(rule.get("review_reason") or "").startswith(GEAR_PERMISSION))


def _reads_for(rule: dict, sp: str) -> bool:
    """A row speaks for the species its cell names; "All" for every salmon; a cell naming no fish the table knows for
    every salmon too (nothing says which — never for none)."""
    return not _species_of(rule) or _covers(rule, sp)


def verdict_on(chain: Sequence[Sequence[dict]], sp: str, md: int) -> Optional[dict]:
    """The species' answer on day `md`: the narrowest location of the chain with a limit rule for it that day, or a
    row for it nobody could type (`unread_row`: "check").
    `{s, t, n?}` — s one of open / release / closed / check; None when no location says anything that day."""
    for rules in chain:
        subs: Dict[str, List[dict]] = defaultdict(list)
        for r in rules:
            if r.get("within"):
                subs[r["within"]].append(r)
        limits = [r for r in rules if not r.get("within") and not _is_gear(r) and _covers(r, sp)
                  and ("take" in r or "may_target" in r)]
        held = [r for r in limits if holds_on(r, md)]
        # (T671 D2) the rows for this species nobody can read today: a season that does not parse (any row), or a row
        # nobody could type on a day it holds — for the species its cell names, never dropped
        unread = [r for r in rules if not r.get("within") and _reads_for(r, sp)
                  and (holds_on(r, md) is None or (unread_row(r) and holds_on(r, md)))]
        if held:
            own = [r for r in held if sp in _species_of(r)] or held     # the species' own row beats an all-salmon row
            opens = [r for r in own if _opens(r)]
            release = [r for r in own if r.get("may_target") is True and says_catch_and_release(r)]
            if opens:
                s, why = "open", opens
            elif release:
                s, why = "release", release
            else:
                s, why = "closed", own
            out = {"s": s, "t": _text(why, subs)}
            if s == "open":
                # T320 / T332: the species' numbers under the region's bucket (`regs.dfo_salmon.quota`) — `tree`, a
                # node of the game-fish quota tree; `u` where they are not one reading — the app then shows `t`,
                # DFO's words, and no number
                from pipeline.regs.dfo_salmon.quota import species_tree
                out.update(species_tree(why, subs) or {})
            n = _notice(why)
            if n:
                out["n"] = n
            hc = sorted({r["header_note"] for r in why if r.get("header_note")})
            if hc:
                # T356: the region header's closure (`quota.header_closure`) — its note id, so the app reads "closed by
                # the header" from the verdict, never from a location id
                out["c"] = hc[0]
            return out
        if unread:
            return {"s": "check", "t": _text(unread, subs)}
    return None


def gear_on(chain: Sequence[Sequence[dict]], md: int) -> Tuple[str, ...]:
    """The gear lines that hold on day `md`: the narrowest location with any that day. A row about another fish
    ("No fishing for eulachon") is one of its lines (`_other_fish`)."""
    for rules in chain:
        held = [r for r in rules if holds_on(r, md) and (
                (_is_gear(r) and (not _species_of(r) or any(_covers(r, sp) for sp in SPECIES))) or _other_fish(r))]
        if held:
            return tuple(dict.fromkeys(_text([r], {}) for r in held))
    return ()


def map_status(verdicts: Sequence[Optional[dict]]) -> str:
    """What the Salmon colouring paints for one day's five verdicts (None is a species nothing opens: closed)."""
    s = [v["s"] if v else "closed" for v in verdicts]
    if not s:
        return "none"
    for st in ("open", "release", "check"):
        if st in s:
            return st
    return "closed"


def runs_of(values: Sequence[int]) -> List[int]:
    """[start0, v0, start1, v1, …] over the year — a new run where the value changes."""
    out: List[int] = []
    prev = object()
    for i, v in enumerate(values):
        if v != prev:
            out += [i, v]
            prev = v
    return out


# ── the chain order ──────────────────────────────────────────────────────────────────────────────────────────────────

def extent_sizes(placed: Iterable[dict]) -> Dict[str, int]:
    """location id -> how many sections its drawn extent covers (the reach run's rows; an undrawn place has none)."""
    return {r["location_id"]: len(r.get("sections") or ()) for r in placed if r.get("mapped")}


def chain_order(precedence: Mapping[str, int], size: Mapping[str, int],
                rides: Iterable[str] = ()) -> Callable[[str], tuple]:
    """THE SORT KEY OF A CHAIN, NARROWEST FIRST (T229, owner 2026-10-28): the tier (precedence: a water 3, a tidal-area
    default 2, a section default or closure 1, the region default 0, a T349 page default -1), then WITHIN A TIER THE MORE SPECIFIC PLACE — the
    one covering fewer sections (a mouth closure before the reach around it, a reach before its whole river) — then the
    location id. It was the id alone within a tier, so "Skeena mainstem downstream of the CNR bridge" (123 sections)
    sorted before its Kitsumkalum-mouth closure (4) and opened coho, pink and sockeye the closure shuts Jul 1-Aug 31.
    (T611) `rides`: the "all the other places" places (`op: other_places`, T607) — after the places they ride on, between
    the size and the id: with ONE placed sibling the two cover the same sections, and the id alone let the other-places
    place answer first and open what its sibling closes."""
    rides = frozenset(rides)
    return lambda lid: (-precedence.get(lid, 0), size.get(lid, 1 << 30), int(lid in rides), lid)


def rides_on_others(locs: Iterable) -> frozenset:
    """(T611) the location ids whose extent is `other_places` — the union of their siblings' sections."""
    return frozenset(l.location_id for l in locs
                     if any(getattr(e.op, "value", e.op) == "other_places"
                            for e in (getattr(getattr(l, "binding", None), "extents", None) or ())))


# ── the regions a section lies in (T668) ─────────────────────────────────────────────────────────────────────────────
#
# A DFO PLACE ANSWERS FOR ITS OWN REGION (T668: D1 default; D2/D3 the owner's answer 2026-11-11, "some streams genuinely
# have 2 regions like the parts of the Fraser that straddle boundaries" — the freshwater straddler rule, AGENTS 59 /
# T183, RULINGS Z4). DFO prints one page per region, and a place from one page was answering on another region's water:
#   D1  an UNDRAWN place (no extent) rides only its water's sections that are members of its own region (the Fraser's
#       Region 2 places were on every Fraser section up to Prince George); a straddler is a member of both;
#   D2  on a section in ONE region, a tie of tier between two regions' places goes to that region's place (size still
#       breaks a tie within one region). On a GENUINE STRADDLER (a member of two regions) neither is dropped: each
#       region's places cascade on their own (narrowest first) and THE MOST STRICT ANSWER PER FISH speaks
#       (`strictest`; the chain's `s` names the groups);
#   D3  a REGION-WIDE place (every extent an area `within`, one clipped to a region) binds exactly the sections that are
#       members of its region (or of an unclipped area it also names) — a straddler takes both regions' region-wide
#       places, strictest per fish. Named waters (Sustut, Skeena B(i)) keep binding across the line: DFO names them.
# A section's regions are THE ONE MEMBERSHIP (`registry.regions.section_regions`); a DFO page's region is its number
# (5A and 5B are both Region 5, DFO's 7 is the atlas's 7A + 7B).

def region_number(key: str) -> int:
    """`"7a"` -> 7: the number of an atlas region key (the DFO page's `region_number`)."""
    m = re.match(r"\d+", str(key))
    if not m:
        raise SalmonError(f"salmon: {key!r} is not a region key")
    return int(m.group())


class SectionRegions:
    """Each section's regions (atlas keys, `{sid: frozenset}`, the one membership) and the sections of the non-region
    areas a region-wide place also names (`areas`, `{area_id: frozenset(sid)}`)."""

    def __init__(self, member: Mapping[int, frozenset], areas: Optional[Mapping[str, frozenset]] = None):
        self.member = member
        self.areas = dict(areas or {})

    def keys(self, sid: int) -> frozenset:
        return self.member.get(sid, frozenset())

    def numbers(self, sid: int) -> frozenset:
        return frozenset(region_number(k) for k in self.keys(sid))

    def area(self, area_id: str) -> frozenset:
        if area_id not in self.areas:
            raise SalmonError(f"salmon: no sections loaded for area {area_id} (a region-wide DFO place names it)")
        return self.areas[area_id]


def region_clip(loc) -> Optional[Tuple[frozenset, Tuple[str, ...]]]:
    """A REGION-WIDE place (D3): every extent a `within`, at least one clipped to a region (`area_id` or `within_area` a
    region item) -> (the region keys it is clipped to, the other areas it names unclipped — Region 6 A's Haida Gwaii MU
    group). None for any other place: a named water, a basin, an MU group, an undrawn place."""
    from pipeline.atlas.registry import regions as RG
    exts = list(getattr(getattr(loc, "binding", None), "extents", None) or [])
    if not exts or any(getattr(e.op, "value", e.op) != "within" for e in exts):
        return None
    clipped, other = set(), []
    for e in exts:
        hit = [a for a in (e.area_id, e.within_area) if a and a.startswith(RG.REGION_PREFIX)]
        if hit:
            clipped.update(a[len(RG.REGION_PREFIX):] for a in hit)
        elif e.area_id:
            other.append(e.area_id)
    return (frozenset(clipped), tuple(sorted(set(other)))) if clipped else None


def section_regions_of(registry, sid_of: Mapping[str, int], areas: Iterable[str] = ()) -> SectionRegions:
    """`SectionRegions` of this atlas, by handle -> sid: the one membership (`regions.section_regions`) and the sections
    of `areas` (registry items). A registry with no region item STOPS the build: every DFO place would answer
    everywhere."""
    from pipeline.atlas.registry import regions as RG
    by = RG.section_regions(registry)
    if not by:
        raise SalmonError("salmon: the registry has no region items — DFO places cannot be held to their regions")
    member = {sid_of[h]: r for h, r in by.items() if h in sid_of}
    got = {}
    for a in sorted(set(areas)):
        if a not in registry:
            raise SalmonError(f"salmon: a region-wide DFO place names area {a}, which the registry does not hold")
        got[a] = frozenset(sid_of[h] for h in registry[a].section_ids if h in sid_of)
    return SectionRegions(member, got)


#: (T668 D2/D3, RULINGS Z4) how strict a verdict is, most strict first: no fishing, catch and release, a season DFO
#: printed that nobody can read, open
STRICT = ("closed", "release", "check", "open")


def _caps(v: Mapping) -> Optional[List[dict]]:
    """An opening's limits as the freshwater reader's rules (`bundle.read` shape): its daily count, and each size tier
    as its own cap — "none over X cm" (take 0 over X), "only k over X cm" (take k over X) — so two regions' openings
    are compared by the SAME primitives a two-region freshwater section is (`ladder.stricter`, `statement`). None where
    the number cannot be read (`u`), or the tree holds a kind (hatchery / wild), a monthly or a pooled limit."""
    tree = v.get("tree")
    if v.get("u") or not tree or tree.get("tier") != "count" or not isinstance(tree.get("n"), int):
        return None
    sp = list(tree.get("species") or [])
    out = [{"type": "retention_limit", "take": tree["n"], "species": sp}]
    for c in tree.get("children") or ():
        if c.get("node") != "tier" or c.get("tier") != "size" or c.get("period") or not isinstance(c.get("over_cm"), int):
            return None
        band = {"min_cm": c["over_cm"], **({"take": 0} if c["n"] == 0 else {})}
        out.append({"type": "retention_limit", "take": c["n"], "species": sp, "lengths": [band]})
    return out


def _both_bind(a: List[dict], b: List[dict]) -> List[dict]:
    """RULINGS Z4 as the freshwater reader reads it (`read.effective_rules` step 6): of two caps saying the same thing
    (`rules.statement`) the stricter (`ladder.stricter`; equal: the first region's) — every other cap binds beside it."""
    from pipeline.deliver.bundle.read.ladder import stricter
    from pipeline.deliver.bundle.rules.predicates import statement
    out = {statement(x): x for x in a}
    for y in b:
        k = statement(y)
        if k not in out or stricter(y, out[k]):
            out[k] = y
    return list(out.values())


def stricter_opening(a: dict, b: dict) -> dict:
    """(owner 2026-11-11, RULINGS Z4a: "straddles should always take the most strict rules from both regions") two
    regions' openings of one species and day, read as the freshwater reader reads two regions' quotas (`_both_bind`):
    where the caps that bind are one region's own, that region's opening; where they mix (a lower count from one, a
    size cap from the other), or a number cannot be read, BOTH regions' words with no number (`u`) — the app then
    shows DFO's words, and no limit is dropped."""
    if a == b:
        return a
    ca, cb = _caps(a), _caps(b)
    if ca is not None and cb is not None:
        bind = _both_bind(ca, cb)
        key = lambda xs: sorted(json.dumps(x, sort_keys=True) for x in xs)       # noqa: E731
        if key(bind) == key(ca):
            return a
        if key(bind) == key(cb):
            return b
    out = {"s": "open", "t": "; ".join(dict.fromkeys(x for x in (a.get("t"), b.get("t")) if x)), "u": 1}
    n = ", ".join(sorted({x for v in (a, b) for x in str(v.get("n") or "").split(", ") if x}))
    if n:
        out["n"] = n
    return out


def strictest(answers: Sequence[Optional[dict]]) -> dict:
    """The most strict of each region's answer for one species and day (regions by number); a region saying nothing
    that day is closed to it (`NOT_LISTED`: closed unless DFO opens it). Two openings: the stricter limit
    (`stricter_opening`). The live feed composes the same (`regs.dfo_salmon.live.compose`)."""
    best = None
    for v in answers:
        v = v or NOT_LISTED
        if best is None or STRICT.index(v["s"]) < STRICT.index(best["s"]):
            best = v
        elif v["s"] == best["s"] == "open":
            best = stricter_opening(best, v)
    return best


def _within(a: Mapping, b: Mapping) -> bool:
    """Hours `a` lie inside hours `b` (same sun anchors, a starts no earlier and ends no later)."""
    sa, ea, sb, eb = a.get("start") or {}, a.get("end") or {}, b.get("start") or {}, b.get("end") or {}
    if sa.get("solar") != sb.get("solar") or ea.get("solar") != eb.get("solar") or not sa.get("solar") \
            or not ea.get("solar"):
        return False
    return sa.get("offset_min", 0) >= sb.get("offset_min", 0) and ea.get("offset_min", 0) <= eb.get("offset_min", 0)


def tighter_hours(by_region: Mapping[str, Optional[Mapping]]) -> List[str]:
    """(RULINGS Z4a) of a straddler's regions' salmon fishing hours (`header_answers` `hours`, None: the page prints
    none, so no limit), the regions whose line shows: the tightest — one whose hours lie inside every other's — else
    every region that prints hours (not comparable: both shown, never one dropped). [] where none prints any."""
    have = {r: h["hours"] for r, h in by_region.items() if h and h.get("hours")}
    for r, h in sorted(have.items()):
        if all(_within(h, o) for o in have.values()):
            return [r]
    return sorted(have)


def both_gear(lines: Sequence[Sequence[str]]) -> Tuple[str, ...]:
    """(owner 2026-11-11) a straddler's gear and method lines: every region's, in region order, a line said once —
    two regions' lines are DFO's words and are not ranked, so none is dropped."""
    return tuple(dict.fromkeys(x for ls in lines for x in ls))


# ── the build ────────────────────────────────────────────────────────────────────────────────────────────────────────

class _Interner:
    def __init__(self):
        self.items: List = []
        self._ix: Dict[str, int] = {}

    def __call__(self, v) -> int:
        k = json.dumps(v, sort_keys=True)
        if k not in self._ix:
            self._ix[k] = len(self.items)
            self.items.append(v)
        return self._ix[k]


def closed_by_header(sp_runs: Sequence[Sequence[int]], verdicts: Sequence[dict]) -> List[str]:
    """(T356) The header notes closing a chain (`c`): every species, every day, answered by a region header's closure
    (`quota.header_closure` — its verdicts carry `c`); else []. `deliver.salmon.chain_calendar` and `live.compose`
    both read it so."""
    ids = set()
    for runs in sp_runs:
        for k in range(1, len(runs), 2):
            v = runs[k]
            c = verdicts[v].get("c") if v >= 0 else None
            if not c:
                return []
            ids.add(c)
    return sorted(ids)


def chain_calendar(chain: Sequence[Sequence[dict]], verdicts: _Interner, gears: _Interner,
                   undrawn_closed: bool = True, speaks: bool = False,
                   groups: Optional[Sequence[Sequence[int]]] = None) -> dict:
    """One chain's year: per species runs of a verdict index (`NOT_LISTED` where nothing opens it), the gear runs (-1:
    no gear line), the map-status runs. (T372, owner 2026-11-04: the trout way) a DFO place on the water not drawn yet
    never paints the map — it is the card's caveat, as an undrawn trout rule is (`status_index`: it never colours a
    section). (T356) `c`: the header notes that close the chain every day (`closed_by_header`) — set only where its
    undrawn places, if any, are closed by their header too (`undrawn_closed`). (T372) `n`, on an undrawn place's own
    chain (`speaks`): the days DFO speaks for it (1: some species has a verdict other than "not opened"), the trout
    caveat's "in force today" — the card lists it first on those days. (T668) `groups` (a chain on a section in two
    regions, its places of each region as indices into `chain`): each group cascades on its own and the most strict
    answer per fish speaks (`strictest`, two openings by their limits); the gear lines are every group's
    (`both_gear`) — as the live feed composes them (`live.compose`)."""
    per_sp: List[List[int]] = [[] for _ in SPECIES]
    gear: List[int] = []
    mapc: List[int] = []
    subs = [[chain[i] for i in g] for g in groups] if groups else None
    for md in DAYS:
        if subs:
            vs = [strictest([verdict_on(c, sp, md) for c in subs]) for sp in SPECIES]
            g = both_gear([gear_on(c, md) for c in subs])
        else:
            vs = [verdict_on(chain, sp, md) for sp in SPECIES]
            g = gear_on(chain, md)
        for k, v in enumerate(vs):
            per_sp[k].append(verdicts(v or NOT_LISTED))
        gear.append(gears(list(g)) if g else -1)
        mapc.append(MAP_CODES.index(map_status(vs)))
    sp = [runs_of(x) for x in per_sp]
    c = closed_by_header(sp, verdicts.items) if undrawn_closed else []
    n = speaks_runs(per_sp, verdicts.items) if speaks else None
    return {"sp": sp, "g": runs_of(gear), "m": runs_of(mapc), **({"c": c} if c else {}), **({"n": n} if n else {})}


def speaks_runs(per_sp: Sequence[Sequence[int]], verdicts: Sequence[dict]) -> List[int]:
    """(T372) Per day 1 where some species' verdict is DFO's own (not `d`, "not opened"), else 0 — as runs."""
    return runs_of([int(any(not verdicts[per_sp[k][d]].get("d") for k in range(len(per_sp))))
                    for d in range(len(per_sp[0]))])


def _label(loc) -> str:
    area = (loc.source_text or {}).get("specific_area") or ""
    return f"{loc.water} — {area}" if loc.water and area else (loc.water or area or loc.location_id)


#: (T343, owner 2026-10-31) header notes never shown: the 100 m facility closure and R1's stream bait ban (the
#: freshwater regs carry the same rules — the bait ban with its exceptions as each river's own season — on the same
#: page, the bait ban as the card's gear line) and how a fish is measured
DROPPED_KINDS = ("facility", "measure", "bait_ban")


def header_answers(notes: Sequence[dict]) -> dict:
    """(T356) What a region's header notes resolve to, beside them — the release's region and the live feed's
    (`live._set_bucket`) alike: `bucket` (the combined daily limit, `quota.bucket`; absent where none is printed),
    `shared` (the scoped aggregate limits, `quota.shared_buckets`, whether or not there is a bucket) and `sizes` (the
    header sizes per species, `quota.sizes`). Always `shared` and `sizes`, empty or not: the app reads them, never a
    missing key. (T343) `notes` again, each said elsewhere marked `fold`: `bucket` / `sizes` / `shared` / `closure` /
    `hours` (the structure that says it — only where that structure did read it), `drop` (`DROPPED_KINDS`). The card
    shows a note as words only without one. `hours`: the page's salmon fishing hours (a `daylight` note), `{hours, says,
    id}` — absent where the page prints none."""
    from pipeline.regs.dfo_salmon import quota as Q
    b = Q.bucket(notes)
    shared = Q.shared_buckets(notes)
    sized: set = set()
    sizes = Q.sizes(notes, sized)
    closure = Q.header_closure(notes, "")
    day = next((n for n in notes if n.get("k") == "daylight" and n.get("hours") and not n.get("w")), None)
    hours = {"hours": day["hours"], "id": day.get("id"),
             "says": "Salmon fishing in daylight only: " + _when_words({"when": {"hours": day["hours"]}})} if day else None
    fold = {**({day["id"]: "hours"} if day and day.get("id") else {}),**{i: "sizes" for i in sized}, **{i: "bucket" for i in (b or {}).get("ids") or ()},
            **{x["id"]: "shared" for x in shared if x.get("id")},
            **({closure[0]["header_note"]: "closure"} if closure else {})}
    marked = []
    for n in notes:
        n = {k: v for k, v in n.items() if k != "fold"}
        how = "drop" if n.get("k") in DROPPED_KINDS else fold.get(n.get("id"))
        marked.append({**n, "fold": how} if how else n)
    return {"notes": marked, **({"bucket": b} if b else {}), **({"hours": hours} if hours else {}), "shared": shared,
            "sizes": sizes}


def build(entry_files: Mapping[str, object], feeds: Mapping[str, dict], placed: Iterable[dict],
          sid_of: Mapping[str, int], item_sections: Optional[Mapping[str, Iterable[int]]] = None,
          headers: Optional[Mapping[str, Sequence[dict]]] = None,
          scope_headers: Optional[Mapping[str, Sequence[dict]]] = None,
          upstream: Optional[Callable[[Sequence[str]], Iterable[str]]] = None, *,
          section_regions: Optional[SectionRegions]) -> Tuple[dict, List[Tuple[int, int]]]:
    """(payload, [(sid, chain)] sorted by sid). `entry_files`: slug → `entries.EntryFile`; `feeds`: slug → the typed
    feed (`typed/region-<slug>.json`); `placed`: the reach run's rows; `sid_of`: section id → handle.

    `section_regions` (T668, D1-D3 above): each section's regions (`section_regions_of`) — required, so no caller can
    forget it; the bundle always passes the atlas's. `None` is said only by a unit fixture with no regions at all, and
    then no place is held to a region.

    A location joins the feed by any of its fingerprints; one with no rules on the page today (dormant) still sits in
    its chains and says nothing (the live feed fills it in when DFO lists it again). A placed section the handle table does not know stops the build: the reach run and
    this atlas are not the same build.

    PARTIAL COVERAGE (owner 2026-10-28): `item_sections` (item id → its sids). Every section of a water that has a DFO
    place not drawn yet (`unmapped`) carries those places in its chain (`u`); while one of them has rules (the release:
    on today's page; the feed: listed) the map paints the section `check` and the card says parts of the water are not
    mapped — we cannot tell which stretch the place's rules cover, so the whole water (Kitimat 2026-10-28: the area-6
    coho non-retention answered for a river whose own coho opening sits on an undrawn stretch)."""
    # THE REGION HEADER: `headers` slug -> the page's notes (`header.parse_header`), resolved to items here
    from pipeline.regs.dfo_salmon import header as H
    from pipeline.regs.dfo_salmon import quota as Q
    rules_of: Dict[str, List[dict]] = {}
    words_of: Dict[str, List[dict]] = {}
    locs: Dict[str, object] = {}
    regions: Dict[str, dict] = {}
    scopes: Dict[str, Dict[str, List[str]]] = {}
    for slug, ef in sorted(entry_files.items()):
        feed = feeds.get(slug) or {}
        by_fp = {l["fingerprint"]: l["rules"] for l in feed.get("locators") or []}
        words_fp = {l["fingerprint"]: l.get("words") or [] for l in feed.get("locators") or []}
        notes = list((headers or {}).get(slug) or [])
        # the scopes are resolved for the release's notes AND the newest page's (`scope_headers`): the feed's notes
        # name waters by these keys, so a page newer than the release's rules still finds its waters here
        sc, unresolved = H.resolve([*notes, *((scope_headers or {}).get(slug) or [])], ef, upstream)
        regions[slug] = {"name": getattr(ef, "region_name", slug), "asof": feed.get("date_modified"),
                         "url": PAGE_URL.format(slug=slug), "notes": notes, **header_answers(notes),
                         **({"unresolved": unresolved} if unresolved else {})}
        if sc:
            scopes[slug] = sc
        closure = Q.header_closure(notes, slug)
        for loc in ef.locations:
            # EVERY location, ruled today or not (T211 part 2): a dormant one says nothing today (no rules, so it never
            # answers in its chain), but it is IN its chains, so the live feed (`regs.dfo_salmon.live`) can paint it
            # the day DFO lists it again — without a rebuild.
            rules_of[loc.location_id] = Q.mark_shared([r for fp in loc.fingerprints for r in by_fp.get(fp, [])],
                                                      Q.pool_of(loc))     # T349 / T358: a quota pooled across areas
            if closure and rules_of[loc.location_id]:
                rules_of[loc.location_id] = closure         # T320: the header's closure wins over the table's rows
            # T321: the rows the card shows in DFO's words (new wording, an unread number, a note) — not under a closure
            ws = [] if closure else [{k: w[k] for k in ("k", "sp", "d", "t")}
                                     for fp in loc.fingerprints for w in words_fp.get(fp, [])]
            if ws:
                words_of[loc.location_id] = ws
            locs[loc.location_id] = (slug, loc, ef)

    placed = list(placed)
    prec, size = {lid: locs[lid][1].precedence for lid in locs}, extent_sizes(placed)
    rides = rides_on_others(v[1] for v in locs.values())
    order = chain_order(prec, size, rides)

    def plan_at(sid: int, lids: Iterable[str]) -> Tuple[Callable[[str], tuple], bool]:
        """(T668 D2) the chain order on THIS section and whether it is a two-region chain. On a section in one region,
        within a tier that region's place first, then `chain_order`'s size and id. On a GENUINE STRADDLER (a member of
        two regions) carrying places of more than one region: `chain_order`, and each region's places cascade apart,
        the most strict answer per fish speaking (`chain_calendar` `groups`). A section in no region: `chain_order`."""
        nums = section_regions.numbers(sid) if section_regions is not None else frozenset()
        if len(nums) > 1:
            return order, len({locs[l][1].region_number for l in lids}) > 1
        if not nums:
            return order, False
        (n,) = nums
        return (lambda lid: (-prec.get(lid, 0), 0 if locs[lid][1].region_number == n else 1,
                             size.get(lid, 1 << 30), int(lid in rides), lid)), False

    # (T668 D3) a region-wide place binds exactly the members of its region (the one membership) — a straddler both
    clip = {lid: region_clip(locs[lid][1]) for lid in locs} if section_regions is not None else {}

    def binds(lid: str, sid: int) -> bool:
        c = clip.get(lid)
        if c is None:
            return True
        return bool(c[0] & section_regions.keys(sid)) or any(sid in section_regions.area(a) for a in c[1])

    covering: Dict[int, List[str]] = defaultdict(list)
    unmapped_locs: List[str] = []
    missing: List[str] = []
    for row in placed:
        lid = row["location_id"]
        if lid not in locs:
            continue
        if not row.get("mapped"):
            # T449: a place accepted as wholly in tidal water lies off the freshwater map — not a part of its water
            # still to draw, so it neither paints the water "check" nor rides the card as "not yet mapped"
            if not locs[lid][1].tidal_water:
                unmapped_locs.append(lid)
            continue
        for s in row["sections"]:
            sid = sid_of.get(s)
            if sid is None:
                missing.append(s)
                continue
            if binds(lid, sid):
                covering[sid].append(lid)
    if missing:
        raise SalmonError(f"salmon: {len(missing):,} placed sections are not in the handle table (e.g. {missing[:3]}) "
                          "— the reach run and this atlas are not the same build")

    verdicts, gears = _Interner(), _Interner()
    verdicts(NOT_LISTED)                                     # verdicts[0]
    # chain 0: THE UNLISTED WATER — no DFO location, every species closed every day (owner 2026-10-27)
    chains: List[dict] = [{"r": [], "w": "", "l": [], **chain_calendar([], verdicts, gears)}]
    chain_ix: Dict[tuple, int] = {((), (), ()): UNLISTED_CHAIN}
    # (T668 fix 4) each place's own year, for `a` (a table of its own: the release's verdicts stay as they were)
    from pipeline.deliver.salmon_who import answering_places, places_named
    from pipeline.regs.dfo_salmon import live as _live
    who_v, who_g, who_sp = _Interner(), _Interner(), {}

    def own_sp(lid: str):
        if lid not in who_sp:
            who_sp[lid] = _live.location_calendar(rules_of[lid], who_v, who_g)["sp"]
        return who_sp[lid]

    def chain_for(lids: Sequence[str], u: Iterable[str] = (), speaks: bool = False, at=order,
                  grouped: bool = False) -> int:
        key = tuple(sorted(set(lids), key=at))
        uk = tuple(sorted(set(u)))
        # (T668) a two-region chain: `s`, each region's places (indices into `l`, by region number) — they cascade
        # apart and the most strict answer per fish speaks
        groups = tuple(tuple(i for i, l in enumerate(key) if locs[l][1].region_number == n)
                       for n in sorted({locs[l][1].region_number for l in key})) if grouped else ()
        if (key, uk, groups) not in chain_ix:
            chain_ix[(key, uk, groups)] = len(chains)
            first = locs[(key or uk)[0]][1]
            rs = sorted({locs[l][0] for l in key + uk})
            cal = chain_calendar([rules_of[l] for l in key], verdicts, gears,
                                 undrawn_closed=all(all(r.get("header_note") for r in rules_of[l]) for l in uk),
                                 speaks=speaks, groups=groups or None)
            # (T668 fix 4, owner 2026-11-11) `a`: per species the place that answers (an index into `l`), so the card
            # names its region and its level of DFO's cascade (`salmon_who`; the places are payload `p`)
            who = {"a": answering_places(cal["sp"], [own_sp(l) for l in key], groups or None,
                                         lambda c, o: verdicts.items[c] == (who_v.items[o] if o >= 0 else NOT_LISTED))
                   } if key else {}
            chains.append({"r": rs, "w": _label(first) if key else first.water,
                           "l": list(key), **({"u": list(uk)} if uk else {}),
                           **({"s": [list(g) for g in groups]} if groups else {}), **cal, **who})
        return chain_ix[(key, uk, groups)]

    # the DFO places not drawn yet, by the items of their water, onto those items' sections — (T668 D1) the sections in
    # the place's own region only (the Fraser's Region 2 places stop at Region 2's line)
    undrawn_on: Dict[int, set] = defaultdict(set)
    for lid in unmapped_locs:
        slug, loc, ef = locs[lid]
        water = ef.water(loc.water_id) if loc.water_id else None
        for it in (water.item_ids if water else ()):
            for sid in (item_sections or {}).get(it, ()):
                if section_regions is None or loc.region_number in section_regions.numbers(sid):
                    undrawn_on[sid].add(lid)
    sections = []
    for sid in sorted(set(covering) | set(undrawn_on)):
        at, grouped = plan_at(sid, covering.get(sid, ()))
        sections.append((sid, chain_for(covering.get(sid, ()), undrawn_on.get(sid, ()), at=at, grouped=grouped)))
    unmapped = []
    for lid in sorted(unmapped_locs):
        slug, loc, ef = locs[lid]
        water = ef.water(loc.water_id) if loc.water_id else None
        # `d: 1` — DORMANT at build (no rules on the page): carried so the live feed can show it the day DFO lists it
        # again; the app leaves it off the card until then (core `pickSalmon` / `withSalmonFeed`)
        unmapped.append({"items": sorted(water.item_ids) if water else [], "r": slug, "w": _label(loc),
                         "chain": chain_for([lid], speaks=True), **({} if rules_of[lid] else {"d": 1})})
    if item_sections is not None:
        # (T343) a region's note is read only on a page whose places are that region's (core `salmonNotes`, its chains'
        # `r`): a scope keeps the items that have one — R6's "other than Fraser River Watershed" is 6,713 items across
        # every region, a few dozen of them in a Region 6 chain. Every place is in a chain, dormant ones too.
        sid_items: Dict[int, List[str]] = defaultdict(list)
        for it, sids in item_sections.items():
            for sid in sids:
                sid_items[sid].append(it)
        mine: Dict[str, set] = defaultdict(set)
        for sid, c in sections:
            for r in chains[c]["r"]:
                mine[r].update(sid_items.get(sid, ()))
        for u in unmapped:
            mine[u["r"]].update(u["items"])
        scopes = {slug: {k: [i for i in v if i in mine[slug]] for k, v in sc.items()} for slug, sc in scopes.items()}
    # (owner 2026-11-11, RULINGS Z4a) a two-region chain's salmon fishing hours: the tighter region's line (`h`, the
    # region slugs whose header hours line shows), both where neither is tighter
    for c in chains:
        if c.get("s"):
            c["h"] = tighter_hours({sl: (regions.get(sl) or {}).get("hours")
                                    for sl in sorted({locs[c["l"][g[0]]][0] for g in c["s"]})})
    off = places_off(locs, placed, chains)
    for lid, o in off.items():
        print(f"     salmon: {lid} is in no chain — {o['why']}")
    payload = {"format": FORMAT, "credit": CREDIT, "default": UNLISTED_CHAIN, "regions": regions, "verdicts": verdicts.items,
               "gears": gears.items, "chains": chains, "unmapped": unmapped, "scopes": scopes,
               **({"x": dict(sorted(words_of.items()))} if words_of else {}), **({"off": off} if off else {}),
               "p": places_named(locs, (l for c in chains for l in c["l"]))}
    return payload, sections


#: (T549) why a curated DFO place is in no chain of the release — said in the file (`off`), never dropped
OFF_TIDAL = "tidal water: off the freshwater map (T449)"
OFF_UNPLACED = "not placed by the reach run (a place curated after it: re-run the reach step)"
OFF_NO_SECTION = "binds no section of its region (T668)"


#: (T549 app half) the reason's code beside its words, for the card's text row (core `SALMON_WORDS.offPlace`)
OFF_KIND = {OFF_TIDAL: "tidal", OFF_UNPLACED: "unplaced", OFF_NO_SECTION: "no_section"}


def places_off(locs: Mapping[str, tuple], placed: Sequence[dict], chains: Sequence[dict]) -> Dict[str, dict]:
    """(T549) every curated place no chain names (`l` or `u`), with why — so the release states its whole location set
    and the feed can be checked against it (`feed_vs_release`). Each is `{why, k, r, w, items}`: the reason (words and
    `OFF_KIND` code), its region slug, its label and its water's app items — the card shows it as a text row on that
    water's page (core `salmonCard`), never painted."""
    shown = {l for c in chains for l in (*c["l"], *(c.get("u") or ()))}
    rows = {r["location_id"]: r for r in placed}
    out: Dict[str, dict] = {}
    for lid in sorted(locs):
        if lid in shown:
            continue
        slug, loc, ef = locs[lid]
        row = rows.get(lid)
        why = OFF_UNPLACED if row is None else OFF_TIDAL if loc.tidal_water and not row.get("mapped") \
            else OFF_NO_SECTION
        water = ef.water(loc.water_id) if getattr(loc, "water_id", None) else None
        out[lid] = {"why": why, "k": OFF_KIND[why], "r": slug, "w": _label(loc),
                    "items": sorted(water.item_ids) if water else []}
    return out


def location_set(payload: Mapping) -> frozenset:
    """Every DFO place the release can show: in a chain's `l` or `u` (an undrawn place's own chain is one of them)."""
    return frozenset(l for c in payload["chains"] for l in (*c["l"], *(c.get("u") or ())))


def feed_vs_release(payload: Mapping, feed: Mapping) -> Dict[str, List[str]]:
    """(T549) the live feed's places (`feed["locs"]`) against the release's: `feed_only` — the feed has rules for a place
    the release neither shows nor states (`off`): its rules reach no angler and nobody is told; `said` — feed places
    the release states it does not show (`off`, e.g. tidal); `release_only` — release places the feed does not carry
    (dormant, or a region the feed holds)."""
    have, feed_ids = location_set(payload), set(feed.get("locs") or {})
    off = set(payload.get("off") or {})
    return {"feed_only": sorted(feed_ids - have - off), "said": sorted((feed_ids - have) & off),
            "release_only": sorted(have - feed_ids)}


def _in_window(day: int, on: Sequence[Sequence[int]]) -> bool:
    return not on or any((a <= day <= b) if a <= b else (day >= a or day <= b) for a, b in on)


def notes_for(payload: Mapping, regions: Sequence[str], item: Optional[str], day: int) -> Dict[str, List[str]]:
    """THE HEADER NOTES A PAGE SHOWS on day `day` (the reference the app's `salmonNotes` is held to by the golden):
    `region` — every note of the page's DFO regions that names no water (one excepting this water's item is left out;
    an excepted PART of a water is not an exception: it shows, verbatim); `water` — a note naming waters, only where
    `item` is one of their items (`scopes`) and the day is in its windows. Note ids, in page order."""
    out: Dict[str, List[str]] = {"region": [], "water": []}
    scopes = payload.get("scopes") or {}
    for slug in sorted(set(regions)):
        sc = scopes.get(slug) or {}
        for n in (payload["regions"].get(slug) or {}).get("notes") or []:
            if n.get("w"):
                hit = item is not None and any(item in (sc.get(w["key"]) or ()) for w in n["w"])
                if hit and _in_window(day, n.get("on") or ()):
                    out["water"].append(n["id"])
            elif not (item is not None and any(item in (sc.get(w["key"]) or ()) for w in n.get("x") or ())):
                out["region"].append(n["id"])
    return out


def carry_side_channels(placed: Iterable[dict], area_bound: Iterable[str], sid_of: Mapping[str, int],
                        ord_of: Mapping[int, int], off_stem: Iterable[int],
                        touch: Mapping[int, Iterable[int]]) -> List[dict]:
    """A WATER-BOUND DFO PLACE COVERS ITS WATER'S SIDE CHANNELS TOO (T228, owner 2026-10-28: a place bound to only some
    of a water's sections by a placement shortfall applies to the whole water — memory unplaced-extent-rules: carry it
    on the water). The reach run's walk follows the main stem and its tributaries' stems; an off-stem section (a side
    channel, `section_span.off_stem`) of the SAME water touching what a place covers is added to it, repeatedly (a
    channel off a channel). Tomahnous Creek: the Tatshenshini's "including tributaries" covered the creek's stem, not
    its side channel. An AREA-BOUND place (`area_bound`: a `within` extent — a region, an MU group) is left alone: its
    edge is a real line, never carried (Klinaklini leaves MUs 5-6 to 5-11 there). `placed` rows as the reach run's
    (handles in `sections`); returns new rows, the others as they were."""
    area_bound = set(area_bound)
    off = set(off_stem)
    handle_of = {v: k for k, v in sid_of.items()}
    stem_of: Dict[int, set] = defaultdict(set)              # each water's main-stem (on-stem) sections
    for s_, o in ord_of.items():
        if s_ not in off:
            stem_of[o].add(s_)
    out = []
    for row in placed:
        if not row.get("mapped") or row["location_id"] in area_bound:
            out.append(row)
            continue
        have = {sid_of[h] for h in row["sections"] if h in sid_of}
        # only on a water the place covers WHOLE (every main-stem section): a place bound to part of a water (Morice
        # "from the signs ... to Lamprey Creek") must not creep past its own cut along a side channel
        whole = {o for o in {ord_of.get(x) for x in have} - {None} if stem_of[o] and stem_of[o] <= have}
        todo, add = [x for x in have if ord_of.get(x) in whole], []
        while todo:
            x = todo.pop()
            for y in touch.get(x, ()):
                if y in off and y not in have and ord_of.get(y) in whole and ord_of.get(x) == ord_of[y] \
                        and y in handle_of:
                    have.add(y)
                    add.append(y)
                    todo.append(y)
        out.append({**row, "sections": list(row["sections"]) + [handle_of[y] for y in sorted(add)]} if add else row)
    return out


def load_inputs(run_dir: Path, registry=None, *, entries_dir: Optional[Path] = None,
                feed_dir: Optional[Path] = None) -> Tuple[dict, dict, List[dict]]:
    """The curated entry files, the typed feed and the reach run's placements. A missing feed STOPS the build: a
    release without the salmon would draw a Salmon toggle with nothing behind it."""
    from pipeline.atlas.reach import dfo_salmon as placed_io
    from pipeline.common.curated import GENERATED
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    from pipeline.regs.dfo_salmon.typed import FORMAT as TYPED_FORMAT
    feed_dir = Path(feed_dir or Path(GENERATED.regs.dfo_salmon) / "typed")
    slugs = [s for s in ALL_SLUGS if not PAGES[s].is_stub]
    files = {s: E.load(s, entries_dir or E.ENTRIES_DIR, registry=registry) for s in slugs}
    feeds = {}
    for s in slugs:
        p = feed_dir / f"region-{s}.json"
        if not p.exists():
            raise SalmonError(f"salmon: no typed DFO feed {p} — write it:\n    python -m pipeline.regs.dfo_salmon.feed")
        feeds[s] = json.loads(p.read_text(encoding="utf-8"))
        if feeds[s].get("typed_format") != TYPED_FORMAT:
            # (T671) rows typed by older code would ship the old reading with nothing saying so
            raise SalmonError(f"salmon: {p} was typed by older code (typed_format {feeds[s].get('typed_format')!r}, "
                              f"today's is {TYPED_FORMAT}) — type its scraped rows again (no network, no credits):\n"
                              "    python -m pipeline.regs.dfo_salmon.feed")
    return files, feeds, list(placed_io.read(run_dir))


def load_headers(feeds: Mapping[str, dict], cache_dir: Optional[Path] = None) -> Dict[str, List[dict]]:
    """Each region's header notes: THE TYPED FEED'S OWN (`notes`, T366 — written with its rows from the same page, so
    the header can never go missing from the rules it belongs to), in today's `header.FORMAT` (`header_format`, T358).
    A typed file written before T366 has none, and one before T358 has them in an older shape: then the
    cached page the rules came from (the raw page when its "Date modified" is the feed's, else the history page of that
    date). Neither -> SalmonError (T357: no release without its region's header — a header closure would vanish)."""
    from pipeline.regs.dfo_salmon import header as H
    from pipeline.regs.dfo_salmon.fetch import DEFAULT_CACHE
    from pipeline.regs.dfo_salmon.parse import _RE_DATE_MOD
    cache = Path(cache_dir or DEFAULT_CACHE)
    out: Dict[str, List[dict]] = {}
    missing: List[str] = []
    for slug, feed in sorted(feeds.items()):
        if "notes" in feed and feed.get("header_format") == H.FORMAT:
            out[slug] = list(feed["notes"] or [])
            continue
        # notes in an older header format (T358: `cm` before `lengths`) are read again from the page they came from
        want = feed.get("date_modified")
        pages = [cache / "raw" / f"region{slug}-eng.html",
                 *sorted((cache / "history").glob(f"region{slug}_*.html"), reverse=True)]
        for p in pages:
            if not p.exists():
                continue
            html = p.read_text(encoding="utf-8", errors="replace")
            m = _RE_DATE_MOD.search(html)
            if want and m and m.group(1) == want:
                out[slug] = H.parse_header(html, slug)
                break
        else:
            missing.append(f"region {slug} (typed rows of {want or 'no date'})")
    if missing:
        raise SalmonError("salmon: no header notes for " + ", ".join(missing) + " — the typed feed carries none and no "
                          "cached page has its date. Refresh the region (its rows and header from one page):\n"
                          "    python -m pipeline.regs.dfo_salmon.live run --regions <n> --dry-run   (then without)")
    return out


def load_current_headers(slugs: Iterable[str], cache_dir: Optional[Path] = None) -> Dict[str, List[dict]]:
    """The newest cached page's header notes per region (`cache/dfo_salmon/raw`): what the live feed reads today — used
    for the scopes only, so the feed's water notes find their items in this release."""
    from pipeline.regs.dfo_salmon import header as H
    from pipeline.regs.dfo_salmon.fetch import DEFAULT_CACHE
    out: Dict[str, List[dict]] = {}
    for slug in sorted(slugs):
        p = Path(cache_dir or DEFAULT_CACHE) / "raw" / f"region{slug}-eng.html"
        if p.exists():
            out[slug] = H.parse_header(p.read_text(encoding="utf-8", errors="replace"), slug)
    return out


# ── the bundle's tables and the shipped file ─────────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS salmon_doc (k TEXT PRIMARY KEY, j TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS salmon_section (sid INTEGER PRIMARY KEY, chain INTEGER NOT NULL) WITHOUT ROWID;
"""


def write_bundle(db, payload: dict, sections: Sequence[Tuple[int, int]]) -> None:
    db.executescript(SCHEMA)
    db.execute("INSERT OR REPLACE INTO salmon_doc VALUES ('payload', ?)",
               (json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")),))
    db.executemany("INSERT INTO salmon_section VALUES (?, ?)", sections)


def read_bundle(b) -> Tuple[dict, List[Tuple[int, int]]]:
    row = b.execute("SELECT j FROM salmon_doc WHERE k = 'payload'").fetchone()
    if row is None:
        raise SalmonError("release: the bundle has no salmon_doc — it predates T211; rebuild it: "
                          "python -m pipeline.deliver bundle")
    return json.loads(row[0]), [(int(s), int(c)) for s, c in b.execute("SELECT sid, chain FROM salmon_section ORDER BY sid")]


def _runs(sections: Sequence[Tuple[int, int]]) -> bytes:
    out = bytearray()
    runs: List[List[int]] = []
    for sid, chain in sections:
        if runs and runs[-1][0] + runs[-1][1] == sid and runs[-1][2] == chain:
            runs[-1][1] += 1
        else:
            runs.append([sid, 1, chain])
    put_varint(out, len(runs), "salmon")
    end = 0
    for sid, n, chain in runs:
        put_varint(out, sid - end, "salmon")
        put_varint(out, n, "salmon")
        put_varint(out, chain, "salmon")
        end = sid + n
    return bytes(out)


def _read_runs(b: bytes, i: int) -> Tuple[List[Tuple[int, int]], int]:
    count, i = read_varint(b, i)
    out, end = [], 0
    for _ in range(count):
        gap, i = read_varint(b, i)
        k, i = read_varint(b, i)
        chain, i = read_varint(b, i)
        start = end + gap
        out += [(s, chain) for s in range(start, start + k)]
        end = start + k
    return out, i


def shard_name(region: str) -> str:
    return f"salmon-{region}"


def encode_files(payload: dict, sections: Sequence[Tuple[int, int]], handles: str,
                 regions_of: Callable[[int], Iterable[str]]) -> Dict[str, bytes]:
    """{"salmon": the head, "salmon-<r>": each bundle region's runs}. `regions_of(sid)`: the bundle regions a section
    lies in (empty: `NO_REGION`). Every region a section names gets a shard, so a page's card reads its own region's."""
    by: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
    for sid, chain in sections:
        for r in (sorted(set(regions_of(sid))) or [NO_REGION]):
            by[r].append((sid, chain))
    from pipeline.deliver.release.shards import REGIONS
    shards = [r for r in (*REGIONS, NO_REGION) if r in by or r in REGIONS]
    head = bytearray(MAGIC)
    head.append(VERSION)
    head += bytes.fromhex(handles)
    body = json.dumps({**payload, "shards": shards}, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")
    head += struct.pack("<I", len(body)) + body
    out = {"salmon": bytes(head)}
    for r in shards:
        rb = r.encode("ascii")
        out[shard_name(r)] = SHARD_MAGIC + bytes([SHARD_VERSION]) + bytes.fromhex(handles) + bytes([len(rb)]) + rb \
            + _runs(by.get(r, []))
    return out


def decode_head(b: bytes) -> Tuple[str, dict]:
    if b[:4] != MAGIC or b[4] != VERSION:
        raise SalmonError(f"salmon: not a {FORMAT} file")
    (n,) = struct.unpack_from("<I", b, 13)
    if 17 + n != len(b):
        raise SalmonError(f"salmon: {len(b) - 17 - n} trailing bytes")
    return b[5:13].hex(), json.loads(b[17:17 + n].decode("utf-8"))


def decode_shard(b: bytes) -> Tuple[str, str, List[Tuple[int, int]]]:
    """(handles, region, [(sid, chain)])."""
    if b[:4] != SHARD_MAGIC or b[4] != SHARD_VERSION:
        raise SalmonError(f"salmon: not a {SHARD_FORMAT} file")
    k = b[13]
    region = b[14:14 + k].decode("ascii")
    secs, i = _read_runs(b, 14 + k)
    if i != len(b):
        raise SalmonError(f"salmon: {len(b) - i} trailing bytes")
    return b[5:13].hex(), region, secs


def read_release(release_dir: Path) -> dict:
    """The salmon head's payload of a shipped release (`<release>/manifest.json` names its file). Raises naming the
    file when the release has none."""
    import gzip
    release_dir = Path(release_dir)
    man = release_dir / "manifest.json"
    if not man.exists():
        raise SalmonError(f"salmon: no {man}")
    f = (json.loads(man.read_text(encoding="utf-8")).get("files") or {}).get("salmon")
    if not f:
        raise SalmonError(f"salmon: {man} names no salmon file")
    blob = (release_dir.parent / f["url"]).read_bytes()
    return decode_head(gzip.decompress(blob) if f["url"].endswith(".gz") else blob)[1]


def decode_files(files: Mapping[str, bytes]) -> Tuple[str, dict, List[Tuple[int, int]]]:
    """(handles, payload, [(sid, chain)]) from the head and every shard — the reference reader."""
    handles, payload = decode_head(files["salmon"])
    secs: Dict[int, int] = {}
    for r in payload["shards"]:
        h, region, rows = decode_shard(files[shard_name(r)])
        if h != handles or region != r:
            raise SalmonError(f"salmon: shard {r} is region {region} of handles {h}, not {r} of {handles}")
        for sid, chain in rows:
            if secs.setdefault(sid, chain) != chain:
                raise SalmonError(f"salmon: section {sid} is chain {secs[sid]} in one shard and {chain} in {r}")
    return handles, payload, sorted(secs.items())
