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

A CLOSED AREA INSIDE A PLACE (T573 entry B: the Thompson's Deadman / Juniper / Ashcroft, Babine's mouths) is a location
of its own (`regs.dfo_salmon.closures.build_pieces`, appended by `load_inputs`): its place's tier, its own rule ("No
fishing, year-round" + DFO's words for the area) and fewer sections, so `chain_order` puts it first on its sections —
no new precedence. One the reach run could not bind (`carried`) rides its water as an undrawn part (`u`).

A "REST OF" PLACE ANSWERS AFTER THE PLACES IT LEAVES OUT (T761, DFO's "except in those areas and times listed below"):
on its listed places' sections inside its bounds (the reach row's `behind`) it is in the chain right after them
(`rests_behind`, `rest_after`), so a listed stretch speaks on the days it has a rule for a fish and the rest's rule on
the other days; elsewhere it sorts as any place. Its map binding stays the rest (bounds less the listed places).

A PLACE FOR ONE BANK IS READ BESIDE (T754, owner 2026-11-22 23:45: "Other bank: the water's other rules"; AGENTS rule 49,
as the book's `side`): DFO's "on west bank between signs at Kitimat hatchery outfall", "from the centre line to the
northern bank" (`regs.dfo_salmon.locations.bank_of`). Placed, it rides exactly its own bound sections in their chains' `u`
— never `l` — so the stretch answers and paints by the water's other places, and its own chain (`unmapped`, `n`) gives the
card its rules for that bank, the bank named on `p[lid]["side"]` (core: the "one bank" caveat, not "not drawn yet"). Not
placed, it is an ordinary undrawn place of its water: never widened, its words name the bank. Supersedes RULINGS E19's
whole channel for the Nakina.

A DFO PLACE ANSWERS FOR ITS OWN REGION (T668, `SectionRegions`): an undrawn place rides only its own region's sections;
a region-wide place binds only its region's members; on a section in TWO regions (the one membership) both regions'
places hold — the chain's `s` names each region's places (indices into `l`), each cascades apart, and the MOST STRICT
answer per fish speaks (`strictest`; the live feed composes the same, `live.compose`).

PER CHAIN, FOR EVERY DAY OF THE YEAR (366: Feb 29 included; DFO's dates carry no year), precomputed here:
  * a VERDICT per species (chinook, coho, sockeye, pink, chum): `open` (a limit above zero), `release` (fish, keep
    none), `closed` (no fishing), `check` (DFO printed a season nobody can read, "to be determined") — or none: no rule
    of the chain holds that species that day, and the app says so rather than guessing. (T796 A6) A closed verdict
    from DFO's "Finfish closure" carries `f: 1`: ALL fishing is closed there that day, trout included (the app closes
    the page from it);
  * the GEAR lines that hold (bait bans, hook rules) — every location's, narrowest first, the same words once (T770);
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
                 (T870) pr: location id -> its DFO table rows as printed, the rows in force (`printed_rows`): the card's
                 Source (DFO) lists them as the rows behind its answers (optional: an app before T870 ignores it)
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
import sys
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


def holds_kind(rule: dict, md: int):
    """(T676) How the rule holds on day `md`: "dated" (a printed range holds, or no dates: all year), "open" (an "until
    further notice" rule on a day before its start — it runs on with no end, but any dated rule of its place outranks it
    that day), False, or None (its season did not read). The live feed's `salmon.ts` `holdsKind` the same."""
    h = holds_on(rule, md)
    if h is None:
        return None
    if h:
        return "dated"
    w = rule.get("when") or {}
    return "open" if w.get("open_ended") and w.get("dates") else False


def _held(rules: Sequence[dict], md: int) -> List[dict]:
    """(T676) The rules that hold on `md`: those dated that day; only where none is, the open-ended ones running on."""
    kinds = [(r, holds_kind(r, md)) for r in rules]
    return [r for r, h in kinds if h == "dated"] or [r for r, h in kinds if h == "open"]


def _days_of(rule: dict) -> frozenset:
    return frozenset(md for md in DAYS if holds_on(rule, md))


def _all_closure_wins(own: Sequence[dict], alls: Sequence[dict], releasing: Sequence[dict]) -> List[dict]:
    """(T671 D1, owner 2026-11-12: "Yes to option 1") AN "ALL" CLOSURE AGAINST A SPECIES' OWN ROW, ON ONE PLACE: the
    All closure wins UNLESS the species row is dated inside the All row's dates. Tatshenshini below the border: `All |
    Jul 24 to Aug 14 | No fishing for salmon` closes coho through `Coho | Apr 1 to Mar 31 | 2 per day` (22 days); the
    Cowichan shape keeps speaking (`All | Sep 1 to July 14 | …catch and release` with `Coho | Nov 1 to Dec 31 | 1 per
    day` inside it: the All row there is no closure, and coho's dates sit inside it anyway). Returns the All closures
    that override `own` today ([] where the species rows still speak). On the saved pages only the Tatshenshini below
    the border and the Koksilah (undrawn) meet it (`pipeline/docs/GOTCHAS.md`).
    Returns the rows that speak: the species rows dated inside every All closure held today, else the closures.
    `releasing`: the rows the verdict reads as catch and release (`verdict_on` is the one reader of `may_target`)."""
    closures = [a for a in alls if not _opens(a) and not any(a is x for x in releasing)]
    if not own or not closures:
        return list(own)
    inside = [r for r in own if all(_days_of(r) <= _days_of(a) for a in closures)]
    return inside or closures


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
    `{s, t, n?}` — s one of open / release / closed / check; None when no location says anything that day.
    (T676) A rule whose date cell did not read (`check`) makes the species "check" on EVERY day at its place (it may
    open or close any of them — never a silently missing limit), and an "until further notice" rule speaks on a day
    only where no dated rule of its place does (`_held`). The live feed's `salmon.ts` `verdictOn` reads the same."""
    for rules in chain:
        subs: Dict[str, List[dict]] = defaultdict(list)
        for r in rules:
            if r.get("within"):
                subs[r["within"]].append(r)
        checks = [r for r in rules if r.get("check") and not r.get("within") and not _is_gear(r) and _reads_for(r, sp)]
        if checks:
            # each rule's own `check` leads its words: a date cell that did not read (T676 `DATES_UNPARSED`) or a row
            # that did not type (T679 D6 `feed.ROW_UNREAD`)
            why = "; ".join(dict.fromkeys(r["check"] for r in checks))
            out = {"s": "check", "t": f"{why[0].upper()}{why[1:]}: {_text(checks, subs)}"}
            n = _notice(checks)
            if n:
                out["n"] = n
            return out
        limits = [r for r in rules if not r.get("within") and not _is_gear(r) and _covers(r, sp)
                  and ("take" in r or "may_target" in r)]
        held = _held(limits, md)
        # (T671 D2) the rows for this species nobody can read today: a season that does not parse (any row), or a row
        # nobody could type on a day it holds — for the species its cell names, never dropped
        unread = [r for r in rules if not r.get("within") and _reads_for(r, sp)
                  and (holds_on(r, md) is None or (unread_row(r) and holds_kind(r, md)))]
        if held:
            # the species' own row beats an all-salmon row — (T671 D1) unless an All closure that day is not one it is
            # dated inside
            releasing = [r for r in held if r.get("may_target") is True and says_catch_and_release(r)]
            own = [r for r in held if sp in _species_of(r)]
            own = _all_closure_wins(own, [r for r in held if sp not in _species_of(r)], releasing) if own else held
            opens = [r for r in own if _opens(r)]
            release = [r for r in own if any(r is x for x in releasing)]
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
            if s == "closed" and any(r.get("finfish") for r in why):
                # (T796 A6, owner 2026-11-16: "Closes the whole page") DFO's "Finfish closure" closes ALL fishing on
                # this stretch today, not only salmon: `f: 1`, read by the app from this verdict to close the page
                out["f"] = 1
            hc = sorted({r["header_note"] for r in why if r.get("header_note")})
            if hc:
                # T356: the region header's closure (`quota.header_closure`) — its note id, so the app reads "closed by
                # the header" from the verdict, never from a location id
                out["c"] = hc[0]
            return out
        if unread:
            return {"s": "check", "t": _text(unread, subs)}
    return None


def place_gear(rules: Sequence[dict], md: int) -> List[str]:
    """ONE place's gear lines on day `md`, in its rows' order. A row about another fish that is its closure ("No fishing
    for eulachon") is NOT one of them: it is the place's closure line (`place_closures`). Another fish's row that is no
    closure stays a line here, read APART from the gear rows (T770: a bait row never decides whether it holds).
    (T676) Per kind, dated lines first, open-ended ones only where none is (`_held`); a line whose date cell did not
    read is shown every day, prefixed with its `check`."""
    gear = [r for r in rules if _is_gear(r) and (not _species_of(r) or any(_covers(r, sp) for sp in SPECIES))]
    other = [r for r in rules if _other_fish(r) and not _is_gear(r) and not _fish_closure(r)]
    held = {id(r) for r in (*_held(gear, md), *_held(other, md))}
    lines = [r for r in rules if id(r) in held] + [r for r in (*gear, *other) if r.get("check")]
    return list(dict.fromkeys((f"{r['check']}: " if r.get("check") else "") + gear_text(r) for r in lines))


def gear_text(rule: dict) -> str:
    """(T870 item 3, owner 2026-11-18: "Why is the bait ban all year defined from that month to that month") a gear line's
    words: DFO's row and its dates, where dates that hold EVERY day of the year ("Apr 1 to Mar 31") read "all year"
    ("Bait ban (all year)", as `closure_line` and the provincial list's `BAIT.WHEN_ALL_YEAR` say it). A row printed with
    no dates, or one whose season did not read, keeps its words as they were. The worker's `salmon.ts` `gearText` the same."""
    w = rule.get("when") or {}
    if w.get("dates") and not w.get("unparsed") and not w.get("open_ended") and not w.get("hours") \
            and all(holds_on(rule, md) for md in DAYS):
        return f"{rule.get('verbatim') or ''} (all year)"
    return _text([rule], {})


#: (T770, owner 2026-11-16 19:09: "As a closure line (Recommended)") a row CLOSING another fish ("All | No fishing for
#: eulachon", Region 6 B(ii)) is its own closure line, apart from the gear and bait lines: chain `o`.
_RE_FISH_CLOSED = re.compile(r"^\s*no fishing for\b", re.I)


def _fish_closure(rule: dict) -> bool:
    """Is the row about another fish the CLOSURE of that fish ("No fishing for eulachon")?"""
    return _other_fish(rule) and bool(_RE_FISH_CLOSED.match(rule.get("verbatim") or ""))


def closure_line(rule: dict) -> str:
    """A fish closure in words, the fish and its days first, then DFO's row: "Eulachon closed all year: No fishing for
    eulachon" ("all year" when the row holds every day, else its dates: "Eulachon closed Apr 1–Jun 30: …")."""
    from pipeline.regs.dfo_salmon.typed import OTHER_FISH
    fish = str(rule.get("review_reason") or "")[len(OTHER_FISH):].strip() or "this fish"
    every = all(holds_on(rule, md) for md in DAYS)
    when = "all year" if every else (_when_words(rule) or "all year")
    return f"{fish[:1].upper()}{fish[1:]} closed {when}: {rule.get('verbatim') or ''}".rstrip(": ")


def place_closures(rules: Sequence[dict], md: int) -> List[str]:
    """ONE place's closure lines of other fish on day `md` (`closure_line`), read apart from every other row (its own
    kind: a dated bait row never hides it); a row whose date cell did not read shows every day with its `check`."""
    fish = [r for r in rules if _fish_closure(r) and not _is_gear(r)]
    held = {id(r) for r in _held(fish, md)}
    lines = [r for r in fish if id(r) in held] + [r for r in fish if r.get("check")]
    return list(dict.fromkeys((f"{r['check']}: " if r.get("check") else "") + closure_line(r) for r in lines))


def closures_on(chain: Sequence[Sequence[dict]], md: int) -> Tuple[str, ...]:
    """(T770) THE CLOSURE LINES OF OTHER FISH ON DAY `md`: every place's (`place_closures`), narrowest first, the same
    words once — never displaced by another place's line, a gear line or another closure (the T770 guarantee)."""
    return tuple(dict.fromkeys(x for rules in chain for x in place_closures(rules, md)))


def gear_on(chain: Sequence[Sequence[dict]], md: int) -> Tuple[str, ...]:
    """(T770, owner 2026-11-16: "Show both") THE GEAR LINES ON DAY `md`: every place's that hold that day
    (`place_gear`), narrowest place first, the same words once (at the narrowest place). A wider place's line is never
    hidden by a narrower one's — a different value of the same kind of rule shows both, as DFO prints both and both
    bind (the ticket's "replaces": only the same line). It was the narrowest place with any line, which hid a
    whole-river hook rule under a stretch's fly-only. The live feed composes the same from each place's year
    (`live.compose`). Another fish's closure is not a gear line (`closures_on`)."""
    return tuple(dict.fromkeys(x for rules in chain for x in place_gear(rules, md)))


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


def rests_behind(placed: Iterable[dict], locs: Mapping[str, tuple], sid_of: Mapping[str, int],
                 covering: Dict[int, List[str]], binds: Callable[[str, int], bool],
                 missing: List[str], beside_on: Optional[Mapping[str, set]] = None) -> Dict[str, frozenset]:
    """(T761, owner 2026-11-15: DFO's "except in those AREAS AND TIMES listed below") A "REST OF" PLACE STANDS IN THE
    CHAIN BEHIND ITS LISTED PLACES on their sections inside its bounds (the reach row's `behind`,
    `reach.build.REST_BEHIND`): a listed stretch answers on the days it has a rule for that fish, the rest's rule on the
    others. Added to `covering` only where the listed place itself covers the section (its own tidal / region limits
    hold). Returns rest id -> the listed places it answers after (`rest_after`). A section the handle table does not
    know goes to `missing`, as the placed sections do. (T754) A listed ONE-BANK place (`beside_on`: id -> its sections)
    still counts as covering its own sections: it stands beside there, so the rest answers in the chain for the other
    bank (`rest_after` leaves it where `chain_order` put it, its listed place not being in the chain)."""
    after: Dict[str, set] = defaultdict(set)
    beside_on = beside_on or {}
    for row in placed:
        lid = row["location_id"]
        if lid not in locs or not row.get("behind"):
            continue
        for first, secs in sorted(row["behind"].items()):
            for s in secs:
                sid = sid_of.get(s)
                if sid is None:
                    missing.append(s)
                    continue
                if (first in covering.get(sid, ()) or sid in beside_on.get(first, ())) and binds(lid, sid):
                    after[lid].add(first)
                    if lid not in covering[sid]:
                        covering[sid].append(lid)
    return {k: frozenset(v) for k, v in after.items()}


def dormant_listed(locations: Iterable) -> Dict[str, frozenset]:
    """(T783, general audit 2026-11-16) A REST STANDS BEHIND ITS DORMANT LISTED PLACES TOO. A dormant place a rest
    leaves out takes nothing out of it (`entries.rest_listing`, T761), so both cover the dormant place's stretch; the
    reach row's `behind` names only the LIVE ones. The day DFO prints the dormant wording again the live feed gives it
    rules, and it must answer before the rest there, as a live listed place does — so the release's chain puts it first
    now (Stamp River chains 20, 55, 56: the 2-section rest sorted before the 7-section "upstream of signs at Girl
    Guide"). Rest id -> its dormant listed places, among one region's `locations` (a place with no binding: none)."""
    from pipeline.regs.dfo_salmon import entries as E
    locs = [l for l in locations if getattr(l, "binding", None) is not None]
    by_id = {l.location_id: l for l in locs}
    out: Dict[str, frozenset] = {}
    for l in locs:
        if any(getattr(e.op, "value", e.op) == "rest" for e in l.binding.extents):
            got = E.rest_listing(l, by_id)["dormant"]
            if got:
                out[l.location_id] = frozenset(got)
    return out


def rest_after(key: Sequence[str], after: Mapping[str, frozenset]) -> Tuple[str, ...]:
    """(T761) The chain with each rest-of place moved to just AFTER the last of its listed places in it — whatever the
    sizes or ids (a listed stretch with its side streams can be larger than the rest). Elsewhere (none of its listed
    places in the chain) it keeps the place `chain_order` gave it. The chain stays a plain ordered list of ids."""
    out = list(key)
    for r in [l for l in key if after.get(l)]:
        if not any(l in after[r] for l in out):
            continue
        out.remove(r)
        last = max(i for i, l in enumerate(out) if l in after[r])
        out.insert(last + 1, r)
    return tuple(out)


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


#: (T669 item 3) the kind names `regs.dfo_salmon.quota._one` gives — the tree carries no origin field, its kind's name
#: says it: "hatchery-marked coho" (folded into the count: hatchery-marked only), "unmarked (wild) coho" (of the count,
#: up to n unmarked). `test_strictest_cases` builds its trees with `_one`, so a renamed kind fails there.
_HATCHERY_KIND, _WILD_KIND = "hatchery-marked ", "unmarked (wild) "


def _cap(sp: List[str], take: int, **kw) -> dict:
    return {"type": "retention_limit", "take": take, "species": sp, **kw}


#: (T669 item 3, owner 2026-11-20 "Name the region") on a MIXED strictest line — numbers taken from both regions — each
#: number's words end with the region it came from: "Up to 4 coho a day in total, … (Region 5B) · only 1 over 50 cm
#: (Region 6)". A line one region's own stays as DFO's words. The label is part of the store's words (`says`), so the
#: app draws it as it draws any tree and decides nothing; `_caps` reads it back when a third region folds in.
_LABEL = re.compile(r" \((Region [^()]+)\)$")


def region_label(lids: Iterable[str]) -> str:
    """A group's region as a merged line names it: its DFO places' page slugs ("5b:region:section" -> "Region 5B"; a
    rule-book place, `book:…`, names none). "" where none."""
    slugs = sorted({str(l).split(":", 1)[0] for l in lids if not str(l).startswith("book:")})
    return f"Region {', '.join(x.upper() for x in slugs)}" if slugs else ""


def _said(node: Mapping, region: str) -> Tuple[str, str]:
    """A node's words without a region label, and its region (the label it carries, else `region`)."""
    says = str(node.get("says") or "")
    m = _LABEL.search(says)
    return (says[:m.start()], m.group(1)) if m else (says, region)


def _caps(v: Mapping, region: str = "") -> Optional[List[dict]]:
    """An opening's limits as the freshwater reader's rules (`bundle.read` shape), so two regions' openings are compared
    by the SAME primitives a two-region freshwater section is (`ladder.stricter`, `rules.statement`):
      the count tier                     its daily count;
      the aggregate tier (T669 item 3)   `total` with `whose` (a count pooled over a set of waters, `quota._one`): the
                                         count with its `whose` (and the pool's words, `says`, `hatchery`);
      a hatchery-marked kind (folded)    hatchery-marked only = no unmarked fish: a release of wild ones (take 0, origin
                                         wild) — so a region's wild share is ranked against it;
      an unmarked (wild) kind            "up to k unmarked": take k, origin wild;
      a plain kind (folded)              the count's own fish, with its ceiling (the "none over" size tier says it);
      a size tier                        "none over X cm" (take 0 over X), "only k over X cm" (take k over X), a
                                         monthly one with its `period`.
    None where the number cannot be read (`u`), or the tree holds anything else (both regions' words then stand).
    `region` (the answer's region, `region_label`): each cap carries the region it came from (`region`; a node's own
    label wins — a merged answer folding in a third region), so a mixed line can name it."""
    tree = v.get("tree")
    if v.get("u") or not tree or tree.get("node", "tier") != "tier" or tree.get("tier") not in ("count", "total") \
            or not isinstance(tree.get("n"), int) or (tree["tier"] == "total") != bool(tree.get("whose")):
        return None
    sp = list(tree.get("species") or [])
    tag = lambda node: ({"region": r} if (r := _said(node, region)[1]) else {})        # noqa: E731
    root = _cap(sp, tree["n"], **tag(tree))
    out = [root]
    hatchery = wild = False
    for c in tree.get("children") or ():
        if c.get("node") == "kind":
            name = str(c.get("name") or "")
            if list(c.get("species") or []) != sp or not isinstance(c.get("n"), int):
                return None
            if c.get("folded") and c["n"] == tree["n"] and not name.startswith(_WILD_KIND):
                if name.startswith(_HATCHERY_KIND):
                    hatchery = True
                    out.append(_cap(sp, 0, origin="wild", **tag(tree)))
                continue
            if not c.get("folded") and name.startswith(_HATCHERY_KIND) and c["n"] == tree["n"]:
                hatchery = True                     # a merged pool's hatchery-marked kind, said on its own line
                out.append(_cap(sp, 0, origin="wild", **tag(c)))
                continue
            if c.get("folded") or not name.startswith(_WILD_KIND):
                return None
            wild = True
            out.append(_cap(sp, c["n"], origin="wild", **tag(c)))
            continue
        if c.get("node") != "tier" or c.get("tier") != "size" or c.get("children") \
                or c.get("period") not in (None, "monthly") or not isinstance(c.get("over_cm"), int) \
                or not isinstance(c.get("n"), int):
            return None
        band = {"min_cm": c["over_cm"], **({"take": 0} if c["n"] == 0 and not c.get("period") else {})}
        out.append(_cap(sp, c["n"], lengths=[band], **({"period": c["period"]} if c.get("period") else {}), **tag(c)))
    if hatchery and wild:
        return None
    if tree["tier"] == "total":
        root.update(whose=tree["whose"], says=_said(tree, "")[0], hatchery=hatchery and any(
            k.get("folded") and str(k.get("name") or "").startswith(_HATCHERY_KIND) for k in tree.get("children") or ()))
    return out


def _both_bind(a: List[dict], b: List[dict]) -> List[dict]:
    """RULINGS Z4 as the freshwater reader reads it (`read.effective_rules` step 6): of two caps saying the same thing
    (`rules.statement`) the stricter (`ladder.stricter`; equal: the first region's) — every other cap binds beside it.
    (T669 item 3) A POOLED count is not the same cap as a count of this water alone: the pool holds every water of its
    set, so a plain count NEVER displaces it (`ladder.A_NARROWER_CAP_NEVER_DISPLACES_A_WIDER`, over waters) and both
    bind; a pool at or under a plain count holds every fish that count holds, so it displaces it."""
    from pipeline.deliver.bundle.read.ladder import stricter
    from pipeline.deliver.bundle.rules.predicates import statement
    key = lambda x: (statement(x), x.get("whose"))                     # noqa: E731
    out = {key(x): x for x in a}
    for y in b:
        k = key(y)
        if k not in out or stricter(y, out[k]):
            out[k] = y
    pools = [x for x in out.values() if x.get("whose")]
    return [x for x in out.values()
            if x.get("whose") or not any(statement(p) == statement(x) and p["take"] <= x["take"] for p in pools)]


def _moot(caps: List[dict]) -> List[dict]:
    """The caps less the size caps another cap already holds (the same answer, said once): "only k over Y" or "none
    over Y" under a "none over X" or "only j over X" (X <= Y, j <= k), and "only k over Y" at or above the daily
    count. Two regions' caps on one straddler: "4 a day, only 1 over 50 cm" and "only 1 over 65 cm" is the first."""
    sizes = [c for c in caps if c.get("lengths") and not c.get("period")]
    roots = [c["take"] for c in caps if not c.get("lengths") and not c.get("origin")]
    lo = lambda c: c["lengths"][0]["min_cm"]                            # noqa: E731

    def held(c) -> bool:
        if not c.get("lengths") or c.get("period"):
            return False
        if roots and 0 < min(roots) <= c["take"]:
            return True
        return any(o is not c and lo(o) <= lo(c) and o["take"] <= c["take"] and (o["take"], lo(o)) != (c["take"], lo(c))
                   for o in sizes)
    return [c for c in caps if not held(c)]


def _tree_of(caps: List[dict]) -> Optional[dict]:
    """(T669 item 3) the caps that bind on a straddler as ONE quota tree, the shape a DFO row with those numbers gets
    (`quota._one`), so the card says numbers: the count (the lower region's), hatchery-marked only where a region
    releases unmarked fish, the stricter wild share, every size cap that binds; a pool that binds is the `total` tier
    (in its own words), over the count when a plain count binds beside it. None where the caps are not one tree (two
    pools, two plain counts)."""
    from pipeline.regs.dfo_salmon import quota as Q
    roots = [c for c in caps if not c.get("lengths") and not c.get("origin")]
    wild = [c for c in caps if c.get("origin") == "wild"]
    pools, plain = [c for c in roots if c.get("whose")], [c for c in roots if not c.get("whose")]
    if len(pools) > 1 or len(plain) > 1 or not roots or len(wild) > 1:
        return None
    root = plain[0] if plain else pools[0]
    hatchery = bool(wild) and wild[0]["take"] == 0
    subs: List[dict] = [{"origin": "wild", "take": wild[0]["take"]}] if wild and not hatchery else []
    for c in caps:
        if c.get("lengths"):
            band = c["lengths"][0]
            subs.append({"take": c["take"], "lengths": [dict(band)], **({"period": c["period"]} if c.get("period") else {})})
    rule = _cap(list(root["species"]), root["take"], **({"origin": "hatchery"} if hatchery else {}))
    q = Q._one(rule, subs)
    if q.get("u"):
        return None
    tree = q["tree"]
    if pools:
        p = pools[0]
        pool = {**tree, "tier": "total", "whose": p["whose"], "says": p["says"], "n": p["take"]}
        if plain:            # a plain count under the pool: the pool over this water's count
            pool["children"] = [tree]
        elif hatchery and not p["hatchery"]:   # hatchery-marked only from the other region: said, the pool does not
            pool["children"] = [{**k, "folded": False} if k.get("node") == "kind" and k.get("folded") else k
                                for k in tree["children"]]
        tree = pool
    _name_regions(tree, caps, pools, plain, wild, hatchery)
    seq = iter(range(1, 1000))

    def ids(t: dict) -> dict:          # the tiers numbered in the order said (pre-order), as `_one` numbers them
        return {**t, "id": f"t{next(seq)}",
                "children": [ids(c) if c.get("node") == "tier" else c for c in t.get("children") or ()]}
    return ids(tree)


def _name_regions(tree: dict, caps: List[dict], pools: List[dict], plain: List[dict], wild: List[dict],
                  hatchery: bool) -> None:
    """(owner 2026-11-20 "Name the region") a tree of caps from MORE THAN ONE region: each tier and each kind said on its
    own line ends with the region(s) its number came from (`_LABEL`); the count says hatchery-marked only in its own
    words, so it names that region too. Caps of one region (or with no region given): the words as built."""
    if len({c.get("region") for c in caps}) < 2 or not all(c.get("region") for c in caps):
        return

    def name(node: dict, *cs) -> None:
        regs = sorted({c["region"] for c in cs if c})
        node["says"] = f"{node['says']} ({', '.join(regs)})"
    h = wild[0] if hatchery else None
    top = tree
    if pools:
        if plain:
            name(top, pools[0])
            top = top["children"][0]
            name(top, plain[0], h)
        else:
            folded = any(k.get("node") == "kind" and k.get("folded") and str(k.get("name")).startswith(_HATCHERY_KIND)
                         for k in top["children"])
            name(top, pools[0], h if folded else None)
    else:
        name(top, plain[0], h)
    sizes = [c for c in caps if c.get("lengths")]
    for k in top["children"]:
        if k.get("node") == "kind" and not k.get("folded"):
            name(k, wild[0])
        elif k.get("node") == "tier":
            got = [c for c in sizes if c["lengths"][0]["min_cm"] == k.get("over_cm") and c["take"] == k.get("n")
                   and c.get("period") == k.get("period")]
            if got:
                name(k, *got)


def stricter_opening(a: dict, b: dict, ra: str = "", rb: str = "") -> dict:
    """(owner 2026-11-11, RULINGS Z4a: "straddles should always take the most strict rules from both regions") two
    regions' openings of one species and day, read as the freshwater reader reads two regions' quotas (`_both_bind`):
    where the caps that bind are one region's own, that region's opening; (T669 item 3) where they mix (a lower count
    from one, a size cap from the other, a pool, a hatchery or wild share), ONE quota tree of the caps that bind
    (`_tree_of`) — numbers, beside both regions' words (`t`); where a number cannot be read, or the caps are not one
    tree, BOTH regions' words with no number (`u`) — the app then shows DFO's words, and no limit is dropped.
    `ra` / `rb`: the two answers' regions (`region_label`); given, a mixed tree names the region of each number."""
    if a == b:
        return a
    ca, cb = _caps(a, ra), _caps(b, rb)
    tree = None
    if ca is not None and cb is not None:
        bind = _moot(_both_bind(ca, cb))
        key = lambda xs: sorted(json.dumps({k: v for k, v in x.items() if k != "region"}, sort_keys=True)  # noqa: E731
                                for x in xs)
        if key(bind) == key(_moot(ca)):
            return a
        if key(bind) == key(_moot(cb)):
            return b
        tree = _tree_of(bind)
    out = {"s": "open", "t": "; ".join(dict.fromkeys(x for x in (a.get("t"), b.get("t")) if x)),
           **({"tree": tree} if tree else {"u": 1})}
    n = ", ".join(sorted({x for v in (a, b) for x in str(v.get("n") or "").split(", ") if x}))
    if n:
        out["n"] = n
    return out


def strictest(answers: Sequence[Optional[dict]], regions: Optional[Sequence[str]] = None) -> dict:
    """The most strict of each region's answer for one species and day (regions by number); a region saying nothing
    that day is closed to it (`NOT_LISTED`: closed unless DFO opens it). Two openings: the stricter limit
    (`stricter_opening`; `regions`, each answer's `region_label`, name the regions on a mixed line). The live feed
    composes the same (`regs.dfo_salmon.live.compose`)."""
    best, rb = None, ""
    for i, v in enumerate(answers):
        v, r = v or NOT_LISTED, (regions[i] if regions else "")
        if best is None or STRICT.index(v["s"]) < STRICT.index(best["s"]):
            best, rb = v, r
        elif v["s"] == best["s"] == "open":
            got = stricter_opening(best, v, rb, r)
            best, rb = got, (rb if got is best else r if got is v else "")
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
                   groups: Optional[Sequence[Sequence[int]]] = None, labels: Optional[Sequence[str]] = None) -> dict:
    """One chain's year: per species runs of a verdict index (`NOT_LISTED` where nothing opens it), the gear runs (-1:
    no gear line), the map-status runs. (T372, owner 2026-11-04: the trout way) a DFO place on the water not drawn yet
    never paints the map — it is the card's caveat, as an undrawn trout rule is (`status_index`: it never colours a
    section). (T356) `c`: the header notes that close the chain every day (`closed_by_header`) — set only where its
    undrawn places, if any, are closed by their header too (`undrawn_closed`). (T372) `n`, on an undrawn place's own
    chain (`speaks`): the days DFO speaks for it (1: some species has a verdict other than "not opened"), the trout
    caveat's "in force today" — the card lists it first on those days. (T668) `groups` (a chain on a section in two
    regions, its places of each region as indices into `chain`): each group cascades on its own and the most strict
    answer per fish speaks (`strictest`, two openings by their limits); the gear lines are every group's
    (`both_gear`) — as the live feed composes them (`live.compose`). `labels`: each group's `region_label`."""
    per_sp: List[List[int]] = [[] for _ in SPECIES]
    gear: List[int] = []
    shut: List[int] = []
    mapc: List[int] = []
    subs = [[chain[i] for i in g] for g in groups] if groups else None
    for md in DAYS:
        if subs:
            vs = [strictest([verdict_on(c, sp, md) for c in subs], labels) for sp in SPECIES]
            g = both_gear([gear_on(c, md) for c in subs])
            o = both_gear([closures_on(c, md) for c in subs])
        else:
            vs = [verdict_on(chain, sp, md) for sp in SPECIES]
            g = gear_on(chain, md)
            o = closures_on(chain, md)
        for k, v in enumerate(vs):
            per_sp[k].append(verdicts(v or NOT_LISTED))
        gear.append(gears(list(g)) if g else -1)
        shut.append(gears(list(o)) if o else -1)
        mapc.append(MAP_CODES.index(map_status(vs)))
    sp = [runs_of(x) for x in per_sp]
    c = closed_by_header(sp, verdicts.items) if undrawn_closed else []
    n = speaks_runs(per_sp, verdicts.items) if speaks else None
    # (T770, owner 2026-11-16 19:09) `o`: another fish's closure lines per day, runs of an index into `gears` (-1 none);
    # absent on a chain with none
    return {"sp": sp, "g": runs_of(gear), "m": runs_of(mapc), **({"o": runs_of(shut)} if any(x >= 0 for x in shut) else {}),
            **({"c": c} if c else {}), **({"n": n} if n else {})}


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


def printed_rows(locator: Mapping) -> List[str]:
    """(T870) A typed locator's DFO table rows AS PRINTED ("Species | Dates | Limits", `feed.row_text`; a page default's
    sentence), the rows in force: a row from a table DFO commented out (`hidden`, T787) answers nothing and is left out.
    Each once, in page order."""
    hidden = set(locator.get("hidden") or ())
    return list(dict.fromkeys(r for i, r in enumerate(locator.get("rows") or ()) if i not in hidden and r))


def location_rules(loc, by_fp: Mapping[str, Sequence[dict]], closure: Sequence[dict]) -> List[dict]:
    """A curated place's rules from the typed feed (`by_fp`: fingerprint -> rules), as the release reads them: T349 /
    T358 a quota pooled across areas (`quota.mark_shared`); T320 the region header's closure (`quota.header_closure`)
    wins over the table's rows. `build` and the straddler pair table (`salmon_pairs`) read places so. (T787) Only the
    rows in force (`in_force`): a place whose rows are all hidden has no rule today — it stays in its chains, says
    nothing, and the header (or a wider place) answers."""
    from pipeline.regs.dfo_salmon import quota as Q
    rules = Q.mark_shared(in_force(r for fp in loc.fingerprints for r in by_fp.get(fp, [])), Q.pool_of(loc))
    return list(closure) if closure and rules else rules


def in_force(rules: Iterable[dict]) -> List[dict]:
    """(T787, owner 2026-11-16: "keep reading them for provenance but mark them hidden somehow so they are ignored")
    THE ROWS THAT ANSWER: every typed rule but one from a table DFO commented out (`hidden`, `parse.table_hidden`).
    Off-season DFO keeps its table of dated openings inside an HTML comment and shows only the header's "no fishing for
    salmon until further notice" (Regions 4, 5A, 7). The release (`location_rules`) and the live feed
    (`live.location_calendar`, `live.run`) read places through this; the reviewer still sees the hidden rows."""
    return [r for r in rules if not r.get("hidden")]


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
    on today's page; the feed: listed) the card says parts of the water are not mapped — we cannot tell which stretch
    the place's rules cover, so the whole water (Kitimat 2026-10-28). (T372, owner 2026-11-04) the map paints the DRAWN
    answer, never `check` for the undrawn place; (T679 D2, owner 2026-11-12) the map and the card's bar both give that
    species verdict — `m` is the one source (core `salmonDayStatus` reads it)."""
    # THE REGION HEADER: `headers` slug -> the page's notes (`header.parse_header`), resolved to items here
    from pipeline.regs.dfo_salmon import header as H
    from pipeline.regs.dfo_salmon import quota as Q
    rules_of: Dict[str, List[dict]] = {}
    words_of: Dict[str, List[dict]] = {}
    rows_by: Dict[str, List[str]] = {}
    locs: Dict[str, object] = {}
    regions: Dict[str, dict] = {}
    scopes: Dict[str, Dict[str, List[str]]] = {}
    for slug, ef in sorted(entry_files.items()):
        feed = feeds.get(slug) or {}
        by_fp = {l["fingerprint"]: l["rules"] for l in feed.get("locators") or []}
        words_fp = {l["fingerprint"]: l.get("words") or [] for l in feed.get("locators") or []}
        rows_fp = {l["fingerprint"]: printed_rows(l) for l in feed.get("locators") or []}
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
        # (T679 D6) the locators whose rows did not type: each reads "check" (`feed.unread_rule`) — said here, by place
        errs = {l["fingerprint"]: l["error"] for l in feed.get("locators") or [] if l.get("error")}
        for loc in ef.locations:
            for fp in loc.fingerprints:
                if fp in errs:
                    print(f"salmon: DFO Region {slug}: {loc.location_id} reads 'check DFO' — its row did not type "
                          f"({errs[fp]})", file=sys.stderr)
            # EVERY location, ruled today or not (T211 part 2): a dormant one says nothing today (no rules, so it never
            # answers in its chain), but it is IN its chains, so the live feed (`regs.dfo_salmon.live`) can paint it
            # the day DFO lists it again — without a rebuild.
            own = getattr(loc, "closure_rule", None)
            # T573: a closed area of a place (`closures.ClosureLocation`) carries its own rule, never a wording's
            rules_of[loc.location_id] = ((list(closure) if closure else [own]) if own
                                         else location_rules(loc, by_fp, closure))
            # T321: the rows the card shows in DFO's words (new wording, an unread number, a note) — not under a closure
            ws = [] if closure else [{k: w[k] for k in ("k", "sp", "d", "t")}
                                     for fp in loc.fingerprints for w in words_fp.get(fp, [])]
            if ws:
                words_of[loc.location_id] = ws
            # T870: the place's DFO rows as printed, the rows behind its answers (the card's Source (DFO)) — not under a
            # closure, as `x`
            pr = [] if closure else list(dict.fromkeys(r for fp in loc.fingerprints for r in rows_fp.get(fp, [])))
            if pr:
                rows_by[loc.location_id] = pr
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

    # T754: the places DFO words for one bank of their river (`locations.bank_of`: "on west bank", "from the centre line
    # to the northern bank") — the bank each names
    from pipeline.regs.dfo_salmon.locations import bank_of
    banks = {lid: b for lid, v in locs.items() if (b := bank_of((getattr(v[1], "source_text", None) or {})
                                                                .get("specific_area")))}
    beside: Dict[str, str] = {}
    covering: Dict[int, List[str]] = defaultdict(list)
    unmapped_locs: List[str] = []
    missing: List[str] = []
    #: T573: a CARRIED closed area (`reach.dfo_salmon.resolve_closures` `carried`) rides its water as an undrawn part —
    #: on the sections a circle only touches (`marked`) when the run knew them, else its water's, never its own bound ones
    carried_on: Dict[str, Optional[set]] = {}
    for row in placed:
        lid = row["location_id"]
        if lid not in locs:
            continue
        if row.get("carried"):
            carried_on[lid] = {sid_of[h] for h in row["marked"] if h in sid_of} if row.get("marked") else None
            if row.get("mapped"):
                unmapped_locs.append(lid)
        if not row.get("mapped"):
            # T449: a place accepted as wholly in tidal water lies off the freshwater map — not a part of its water
            # still to draw, so it neither paints the water "check" nor rides the card as "not yet mapped"
            # T761: a rest-of whose listed places take all of its bounds still answers BEHIND them (`behind`): drawn
            # nowhere of its own, but placed — never an undrawn part of its water
            if not locs[lid][1].tidal_water and not row.get("behind"):
                unmapped_locs.append(lid)
            continue
        if lid in banks:
            # T754 (owner 2026-11-22): a place for ONE BANK is read BESIDE the water's other places on its own sections
            # (AGENTS rule 49, as the book's `side`): in their chains' `u`, never `l`, so the stretch answers — and
            # paints — by the water's other rules and the card names the bank (`p[lid]["side"]`). ONLY ITS OWN RIVER'S
            # sections (review of #61): a bank is a bank of that river, so the sections its tributary walk took (the
            # Kitimat's 12 creeks from "(including tributaries)") carry no bank caveat and answer by their other places
            on = {sid_of[s] for s in row["sections"] if s in sid_of}
            missing.extend(s for s in row["sections"] if s not in sid_of)
            if item_sections is None:
                raise SalmonError(f"salmon: {lid} names one bank: its river's sections (`item_sections`) are needed")
            slug, loc, ef = locs[lid]
            water = ef.water(loc.water_id) if loc.water_id else None
            stem = {sid for it in (water.item_ids if water else ()) for sid in item_sections.get(it, ())}
            mine = {sid for sid in on if sid in stem and binds(lid, sid)}
            unmapped_locs.append(lid)
            if mine:
                carried_on[lid] = mine
                beside[lid] = banks[lid]
            else:
                # placed off its own river only: never widened, an undrawn place of its water (said, not dropped)
                print(f"     salmon: {lid} names one bank but binds no section of its own river — an undrawn place "
                      "of its water", file=sys.stderr)
            continue
        for s in row["sections"]:
            sid = sid_of.get(s)
            if sid is None:
                missing.append(s)
                continue
            if binds(lid, sid):
                covering[sid].append(lid)
    after = rests_behind(placed, locs, sid_of, covering, binds, missing,
                         {lid: carried_on[lid] for lid in beside})
    # (T783) and behind its dormant listed places, where both cover a section (`dormant_listed`)
    for _slug, ef in sorted(entry_files.items()):
        for rest, dormant in dormant_listed(ef.locations).items():
            if rest in locs:
                after[rest] = after.get(rest, frozenset()) | frozenset(d for d in dormant if d in locs)
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
        key = rest_after(tuple(sorted(set(lids), key=at)), after)
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
                                 speaks=speaks, groups=groups or None,
                                 labels=[region_label(key[i] for i in g) for g in groups] or None)
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
        if carried_on.get(lid) is not None:           # T573: a circle's marked sections, exactly
            for sid in carried_on[lid]:
                if lid not in covering.get(sid, ()):
                    undrawn_on[sid].add(lid)
            continue
        water = ef.water(loc.water_id) if loc.water_id else None
        for it in (water.item_ids if water else ()):
            for sid in (item_sections or {}).get(it, ()):
                if lid in covering.get(sid, ()):          # T573: a carried closed area's own bound sections
                    continue
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
               **({"pr": dict(sorted(rows_by.items()))} if rows_by else {}),
               "p": places_named(locs, (l for c in chains for l in c["l"]))}
    # T573: a closed-area piece says so (`piece: 1`; `bound: 1` when it covers a section), so the app reads it without
    # parsing its id: where the live feed lacks a bound one, the answer is "not known, check the notice", never the
    # place's opening (T757)
    # T754: a placed one-bank place says its bank (`side`: north|south|east|west, the book's `side` words); one the reach
    # run did not place is an ordinary undrawn place of its water (its words name the bank), never widened
    for lid, b in beside.items():
        payload["p"][lid]["side"] = b
    bound = {l for ls in covering.values() for l in ls}
    for lid, v in payload["p"].items():
        if getattr(locs[lid][1], "closure_rule", None):
            v["piece"] = 1
            if lid in bound:
                v["bound"] = 1
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
    # T573: each region's live CLOSED AREAS (entry B) join its places as locations of their own, with their own rule —
    # the same pieces the reach run placed (`reach.dfo_salmon.resolve_closures`, the same confirmed file)
    from pipeline.regs.dfo_salmon import closures as K
    confirmed = placed_io.load_closed_areas()
    files = {s: K.with_pieces(E.load(s, entries_dir or E.ENTRIES_DIR, registry=registry), confirmed) for s in slugs}
    feeds = {}
    for s in slugs:
        p = feed_dir / f"region-{s}.json"
        if not p.exists():
            raise SalmonError(f"salmon: no typed DFO feed {p} — write it:\n    python -m pipeline.regs.dfo_salmon.feed")
        feeds[s] = json.loads(p.read_text(encoding="utf-8"))
        if feeds[s].get("typed_format") != TYPED_FORMAT:
            # (T671) rows typed by older code would ship the old reading with nothing saying so
            raise SalmonError(f"salmon: {p} was typed by older code (typed_format {feeds[s].get('typed_format')!r}, "
                              f"today's is {TYPED_FORMAT}) — scrape the cached pages and type the rows again (no "
                              "network, no credits):\n"
                              "    python -m pipeline.regs.dfo_salmon.locations && python -m pipeline.regs.dfo_salmon.feed")
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


# ── the rule book's full closures, first in the salmon chain (T796 A5) ───────────────────────────────────────────────
#
# (Owner 2026-11-16, question 29; the design agreed with the app chat the same day) THE BOOK p.77: "when fresh waters
# are closed to fishing, or have gear restrictions outlined in this Synopsis, the regulations apply to fishing for
# salmon as well as for non-salmon species." A stretch the book closes to ALL fishing reads closed for salmon too: the
# book's closure is the FIRST place of that stretch's salmon chains, a release-side place marked static (DFO's feed
# never carries it; under a live feed the app keeps it from the release). WHICH CLOSURES: the status index's CLOSED
# test from the stored verdicts, and only a rule that BY ITSELF closes every game fish (`verdicts.project.closing`): a
# full closure ("No Fishing", a park or an ecological reserve), on the days it is in force after every lift, at every
# moment of the day; never a gear, species or angler rule, never a mix of species closures. Only on sections DFO
# lists (a section in no DFO chain is closed already), never tidal or outside B.C. Release side, because the stored
# verdicts are made after the bundle: `book_inputs` + `with_book_closures`, called by `release.build`.

BOOK_PREFIX = "book:"
BOOK_WORDS = "Closed to all fishing by the rule book"
#: (T796 X2) the head of a closure the book makes conditional: "Closed by the rule book unless opened under …"
BOOK_UNLESS_WORDS = "Closed by the rule book unless"
#: POLICY (size, measured on r-46e4355a7277 2026-11-16): a book place joins only a chain where DFO lets some salmon be
#: fished on some day (a verdict open, release or check). On a chain DFO closes every day the answer is closed either
#: way (only the words would change), and carrying the book there too made the salmon head 92 kB gzipped against its
#: 60 kB gate (620,930 sections, 736 new chains); with this, 50 kB. The cost: a chain DFO closes all year in the
#: release and opens in season through the live feed has no book place under that feed. False = every listed chain.
#: (Owner 2026-11-16, question dialog: "Raise to 100 kB (Recommended)"; RULINGS O5 "raise now, shrink later", O9) FALSE:
#: the book place joins every DFO-listed chain it closes, so it holds under the live feed too; the head's gate is
#: 100 kB (`release.build.SIZE_GATES["salmon"]`).
BOOK_ONLY_WHERE_DFO_LETS_FISH = False


def dfo_lets_fish(chain: Mapping, verdicts: Sequence[dict]) -> bool:
    """Does the chain answer anything but closed for some salmon on some day (`BOOK_ONLY_WHERE_DFO_LETS_FISH`)?"""
    return any(verdicts[r[k + 1]]["s"] != "closed" for r in chain["sp"] for k in range(0, len(r), 2))


def book_closures(store, keyed: Mapping[int, int]) -> Dict[int, Dict[str, frozenset]]:
    """sid -> {book rule ("entry::rule") -> the day indexes (0..365, `DAYS`) it closes the section to all fishing}.
    `store`: the bundle's `VerdictStore`; `keyed`: sid -> rule key (`section_ruleset`). A day counts when the key is
    closed at EVERY moment (`reading.closed`, the status index's CLOSED) and the rule closes every game fish in each
    moment's reading."""
    from pipeline.deliver import types as T
    from pipeline.deliver.verdicts.project import closing
    game = frozenset(T.GAME_FISH)
    by_key: Dict[int, Dict[str, frozenset]] = {}
    memo: Dict[Tuple[int, int], frozenset] = {}

    def closers(key: int, reading: int) -> frozenset:
        if (key, reading) not in memo:
            got = closing(lambda f: store.rows(store.verdict_id(key, reading, f, "none")), store.grade)
            memo[(key, reading)] = frozenset(store.rule_ids[r] for r, fish in got.items() if game <= set(fish))
        return memo[(key, reading)]

    out: Dict[int, Dict[str, frozenset]] = {}
    for sid, key in sorted(keyed.items()):
        if key not in by_key:
            n = len(store.moments(key))
            days: Dict[str, set] = defaultdict(set)
            for d in range(1, len(DAYS) + 1):
                rs = [store.reading_of(key, d, m if n > 1 else None) for m in range(n)]
                if not all(store.closed(key, r) for r in rs):
                    continue
                rules = frozenset.intersection(*(closers(key, r) for r in rs))
                for rule in rules:
                    days[rule].add(d - 1)
            by_key[key] = {r: frozenset(v) for r, v in sorted(days.items())}
        if by_key[key]:
            out[sid] = by_key[key]
    return out


def book_inputs(bundle: Path, sections: Sequence[Tuple[int, int]], verdicts: Optional[Path] = None) \
        -> Tuple[Dict[int, Dict[str, frozenset]], Dict[str, dict], Dict[int, Dict[str, str]]]:
    """The book closures of the sections DFO lists (`sections`: (sid, chain), chain 0 left out), tidal and outside
    B.C. left out, each closing rule's words: {rule: {verbatim, pages, name, region}} — `verbatim` is THE BOOK'S LINE
    (`book_line`: the rule's words where the book printed them, whole), and each section's LIFTS: {sid: {rule: words}}
    where a rule closes fewer days than it prints (`book_lifts`). `verdicts`: the stored verdicts (default beside the
    bundle, as the status index reads them); a missing or foreign file raises (`VerdictStore.open`)."""
    import sqlite3

    from pipeline.deliver.status_index import special_sections
    from pipeline.deliver.verdicts.store import VerdictStore
    from pipeline.common import entry_id as EID
    store = VerdictStore.open(verdicts or Path(bundle).with_name("verdicts.sqlite"), bundle)
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        tidal, outside = special_sections(db)
        listed = {sid for sid, c in sections if c != UNLISTED_CHAIN} - tidal - outside
        keyed = {sid: k for sid, k in db.execute("SELECT sid, key_ix FROM section_ruleset") if sid in listed}
        closures = book_closures(store, keyed)
        rules = sorted({r for v in closures.values() for r in v})
        words: Dict[str, dict] = {}
        for rule in rules:
            entry_id, rule_id = rule.split("::", 1)
            w = _entry_line(db, entry_id, rule_id)
            if w is None:
                raise SalmonError(f"salmon: the verdicts name book rule {rule}, which the bundle does not hold")
            region = EID.region_of(entry_id)              # a water row's or a zone table's; none for `zp:`
            words[rule] = {**w, **({"region": region} if region else {})}
        lifts = book_lifts(db, closures)
    finally:
        db.close()
    return closures, words, lifts


def _entry_line(db, entry_id: str, rule_id: str) -> Optional[dict]:
    """{verbatim: the book's line (`book_line`), pages, name} of one rule of the bundle; None: no such rule."""
    row = db.execute("SELECT r.verbatim, e.verbatim, e.pages, e.name FROM rule r JOIN entry e USING (entry_id) "
                     "WHERE r.entry_id = ? AND r.rule_id = ?", (entry_id, rule_id)).fetchone()
    if row is None:
        return None
    sibs = [v for (v,) in db.execute("SELECT verbatim FROM rule WHERE entry_id = ? AND rule_id != ?",
                                     (entry_id, rule_id)) if v]
    return {"verbatim": book_line(row[0] or "", row[1] or "", sibs), "pages": json.loads(row[2] or "[]"),
            "name": row[3] or ""}


# (T796 hard cases S2, S4, X3, 2026-11-16) THE BOOK'S LINE, as the card quotes it. A rule's `verbatim` is the
# parser's cut of its row's text, and three cuts are not the book's line: the ✱ tributary mark (the bundle's
# "[Includes Tributaries]") ended the rule before its dates ("No Fishing downstream of Forest Kerr Canyon✱, Apr 1-Apr
# 30" quoted as "… Forest Kerr Canyon"); the PDF's line break stays inside the words ("… power line crossing⏎
# (approximately …"); a rule after a ";" is a fragment ("except Thelwood Creek is closed all year" of "Fly fishing
# only; except Thelwood Creek is closed all year"). The line is read back from the row's own text (`entry.verbatim`):
# the rule's words found there, then the rest of ITS clause (to the next ";", line, sentence or the next rule of the
# row), a fragment from the start of its line; the bold marks and the tributary mark dropped, line breaks joined.
_BOOK_MARK = re.compile(r"\[[^\]\n]*\]")
_FRAGMENT = re.compile(r"^(?:except|and|or|but|including|unless)\b")
#: (T796 X2, owner 2026-11-16: "Closed, book's wording (Recommended)") a closure the book makes CONDITIONAL — "Freshwater
#: fishing is prohibited in National Parks unless opened under the National Parks Fishing Regulations" (p.9, RULINGS
#: Z12: closed by default) — is never "closed to all fishing" outright: its head says the proviso (`book_rule`).
_UNLESS = re.compile(r"\bunless (opened under [^.;]+?)\s*(?:[.;]|$)")


def _book_norm(text: str) -> str:
    """`text` without bold marks (`**`) and tributary marks (`[…]`), each run of white space one character: a line
    break where the run breaks the line and the next starts a new line of the row (a capital or a figure, outside
    brackets), else a space (the PDF's wrap)."""
    out: List[str] = []
    i, n = 0, len(text)
    depth = 0
    while i < n:
        if text.startswith("**", i):
            i += 2
            continue
        m = _BOOK_MARK.match(text, i)
        if m:
            i = m.end()
            continue
        c = text[i]
        if c.isspace():
            j = i
            while j < n and (text[j].isspace() or text.startswith("**", j)):
                j += 2 if text.startswith("**", j) else 1
            brk = "\n" in text[i:j] and depth <= 0 and j < n and (text[j].isupper() or text[j].isdigit())
            if out and j < n:
                out.append("\n" if brk else " ")
            i = j
            continue
        depth += (c == "(") - (c == ")")
        out.append(c)
        i += 1
    return "".join(out)


def book_line(rule_verbatim: str, entry_verbatim: str, siblings: Sequence[str] = ()) -> str:
    """THE BOOK'S LINE for one rule (see above): `rule_verbatim` found in its row's `entry_verbatim` and taken to the
    end of its clause — never into a sibling rule of the row (`siblings`, their verbatims) — and from the start of
    its line when it is a fragment ("except …", "and …"). Not found once (exactly), or no row text: the rule's own
    words with line breaks joined."""
    rule = " ".join((rule_verbatim or "").replace("**", "").split())
    rule = " ".join(_BOOK_MARK.sub("", rule).split())
    norm = _book_norm(entry_verbatim or "")
    flat = norm.replace("\n", " ")
    if not rule or flat.count(rule) != 1:
        return rule
    i = flat.index(rule)
    j = i + len(rule)
    if _FRAGMENT.match(rule):
        i = norm.rfind("\n", 0, i) + 1
    tail = norm[j:]
    if rule.endswith(".") and re.match(r"\s+[A-Z]", tail):
        tail = ""
    cut = len(tail)
    for m in (re.search(r"[;\n]", tail), re.search(r"(?<=\.)\s+(?=[A-Z])", tail)):
        if m:
            cut = min(cut, m.start())
    tail = tail[:cut]
    low = tail.lower()
    for sib in siblings:
        s = " ".join(_BOOK_MARK.sub("", sib.replace("**", "")).split()).lower()
        k = low.find(s[:24]) if len(s) >= 6 else -1
        if k >= 0:
            tail, low = tail[:k], low[:k]
    tail = tail.rstrip(" ,;")
    if not tail and _UNLESS.search(rule):
        # (T796 X2) a closure "unless opened under …" keeps the book's next sentence when it says what holds where
        # open ("Where open, anglers require a National Park Fishing Permit.")
        m = re.match(r"\s+(Where open\b[^.;\n]*\.)", norm[j:])
        if m:
            tail = " " + m.group(1)
    return (norm[i:j] + tail).replace("\n", " ")


def book_lifts(db, closures: Mapping[int, Mapping[str, frozenset]]) -> Dict[int, Dict[str, str]]:
    """(T796 hard case S1, 2026-11-16) sid -> {rule: the words of what lifts it}, for a book closure that closes the
    section on FEWER days than its own printed dates (`book_closures` reads the days after every lift): the card's
    dates would contradict the line it quotes ("No Fishing Oct 1-May 31 (Oct 1–Apr 30)"). The lifting rules are the
    section's rules whose `exempts` name the closure on a missing day; their words: "lifted <days> by <water> (p.N):
    <its line>". A closure missing days that no rule of its section lifts STOPS the build (never a silent gap)."""
    from pipeline.deliver.answers.common import when_words
    own_memo: Dict[str, Optional[frozenset]] = {}
    lifter_memo: Dict[int, List[Tuple[str, str, dict, Optional[str]]]] = {}
    line_memo: Dict[Tuple[str, str], Optional[dict]] = {}

    def own(rule: str) -> Optional[frozenset]:
        if rule not in own_memo:
            e, r = rule.split("::", 1)
            row = db.execute("SELECT when_ FROM rule WHERE entry_id = ? AND rule_id = ?", (e, r)).fetchone()
            w = json.loads(row[0]) if row and row[0] else {}
            hs = [holds_on({"when": w}, md) for md in DAYS]
            own_memo[rule] = None if None in hs else frozenset(d for d, h in enumerate(hs) if h)
        return own_memo[rule]

    def lifters(set_id: int):
        if set_id not in lifter_memo:
            lifter_memo[set_id] = [
                (e, r, x, w) for e, r, ex, w in db.execute(
                    "SELECT r.entry_id, r.rule_id, r.exempts, r.when_ FROM ruleset s JOIN rule r USING (entry_id, rule_id) "
                    "WHERE s.set_id = ? AND r.exempts IS NOT NULL ORDER BY r.entry_id, r.rule_id", (set_id,))
                for x in json.loads(ex)]
        return lifter_memo[set_id]

    feb29 = DAYS.index(229)
    out: Dict[int, Dict[str, str]] = {}
    for sid, v in sorted(closures.items()):
        for rule, days in sorted(v.items()):
            mine = own(rule)
            if mine is None:
                continue
            missing = mine - days
            if not missing - {feb29}:          # "to Feb 28" closes Feb 29 too, as the verdicts read it
                continue
            got = db.execute("SELECT set_id FROM section_ruleset WHERE sid = ?", (sid,)).fetchone()
            e0, r0 = rule.split("::", 1)
            said = []
            for e, r, x, w in lifters(got[0] if got else -1):
                if x.get("entry_id") != e0 or x.get("rule_id") != r0:
                    continue
                when = x.get("when") or (json.loads(w) if w else {})
                lifted = frozenset(d for d, md in enumerate(DAYS) if holds_on({"when": when}, md))
                if not lifted & missing:
                    continue
                if (e, r) not in line_memo:
                    line_memo[(e, r)] = _entry_line(db, e, r)
                ln = line_memo[(e, r)]
                pages = ", ".join(str(p) for p in ln["pages"])
                said.append(f"{ln['name']} (p.{pages}): {ln['verbatim']}" if pages else f"{ln['name']}: {ln['verbatim']}")
            if not said:
                raise SalmonError(f"salmon: book closure {rule} closes section {sid} on fewer days than it prints "
                                  f"({when_words({'dates': _day_ranges(missing)}, 'plain')} missing) and no rule "
                                  f"of the section lifts it there — its card would contradict its line")
            out.setdefault(sid, {})[rule] = \
                f"lifted {when_words({'dates': _day_ranges(missing)}, 'plain')} by {'; '.join(dict.fromkeys(said))}"
    return out


def _day_ranges(days: Iterable[int]) -> List[dict]:
    """Day indexes (0..365) -> `when.dates` ranges; a run over the year end is one wrapped range."""
    ds = sorted(set(days))
    runs: List[List[int]] = []
    for d in ds:
        if runs and runs[-1][1] == d - 1:
            runs[-1][1] = d
        else:
            runs.append([d, d])
    if len(runs) > 1 and runs[0][0] == 0 and runs[-1][1] == len(DAYS) - 1:
        runs = [[runs[-1][0], runs[0][1]], *runs[1:-1]]
    out = []
    for a, b in runs:
        (fm, fd), (tm, td) = divmod(DAYS[a], 100), divmod(DAYS[b], 100)
        out.append({"from_month": fm, "from_day": fd, "to_month": tm, "to_day": td})
    return out


def book_rule(lid: str, rule: str, days: Iterable[int], words: Mapping[str, dict], lift: str = "") -> dict:
    """The static place's one rule, as a DFO row reads: every salmon closed on the book's days, in the book's words;
    `lift` (`book_lifts`): what lifts it on the printed days it does not close, said after the line."""
    w = words[rule]
    pages = ", ".join(str(p) for p in w.get("pages") or ())
    line = w["verbatim"]
    unless = _UNLESS.search(line)
    words = f"{BOOK_UNLESS_WORDS} {unless.group(1)}" if unless else BOOK_WORDS
    head = f"{words} (p.{pages})" if pages else words
    if lift:
        line = f"{line} {lift[0].upper()}{lift[1:]}" if line.endswith(".") else f"{line}; {lift}"
    return {"rule_id": lid, "type": "retention_limit", "species": [ALL_SALMON], "take": 0, "may_target": False,
            "verbatim": f"{head}: {line}",
            **({"when": {"dates": _day_ranges(days)}} if len(set(days)) < len(DAYS) else {})}


def with_book_closures(payload: dict, sections: Sequence[Tuple[int, int]], closures: Mapping[int, Mapping[str, frozenset]],
                       words: Mapping[str, dict], lifts: Optional[Mapping[int, Mapping[str, str]]] = None) \
        -> Tuple[dict, List[Tuple[int, int]]]:
    """(T796 A5) The release's salmon with each book closure FIRST in the chains of the sections it closes. A new chain
    per (old chain, its book places): per species and day the first book place that closes it answers, else the old
    chain's answer — DFO's places keep their order and answers behind it; gear lines as before (a book place has none);
    the map status from the five; `a` names the book place on its days (the old indices shifted); a two-region chain's
    groups each gain the book places first. Payload `p[id]` marks it (`static: 1`, `book: {rule, pages}`) and payload
    `static[id]` holds its own year (the live feed's place shape) for the app to compose under a live feed.
    `lifts` (`book_lifts`): per section, what lifts a closure on printed days it does not close — said in its words, so
    a place is one (rule, days, lift)."""
    from pipeline.regs.dfo_salmon import live as _live
    lifts = lifts or {}
    if BOOK_ONLY_WHERE_DFO_LETS_FISH:
        chain_of = dict(sections)
        lets = {c: dfo_lets_fish(payload["chains"][c], payload["verdicts"]) for c in set(chain_of.values())}
        closures = {sid: v for sid, v in closures.items() if lets.get(chain_of.get(sid, UNLISTED_CHAIN))}
    if not closures:
        return payload, list(sections)
    out = {**payload, "chains": list(payload["chains"]), "p": dict(payload.get("p") or {}),
           "static": dict(payload.get("static") or {})}
    verdicts, gears = _Interner(), _Interner()
    for v in payload["verdicts"]:
        verdicts(v)
    for g in payload["gears"]:
        gears(g)
    if len(verdicts.items) != len(payload["verdicts"]) or len(gears.items) != len(payload["gears"]):
        raise SalmonError("salmon: the payload's verdicts / gears are not unique — not a built payload")
    # one static place per (rule, days, lift): the rule's commonest day set takes the plain id, the others `~2` ...
    seen: Dict[str, Dict[Tuple[frozenset, str], int]] = defaultdict(lambda: defaultdict(int))
    for sid, v in closures.items():
        for rule, days in v.items():
            seen[rule][(days, (lifts.get(sid) or {}).get(rule, ""))] += 1
    lid_of: Dict[Tuple[str, frozenset, str], str] = {}
    for rule in sorted(seen):
        for i, (days, lift) in enumerate(sorted(seen[rule], key=lambda k: (-seen[rule][k], sorted(k[0]), k[1]))):
            lid = f"{BOOK_PREFIX}{rule}" + (f"~{i + 1}" if i else "")
            lid_of[(rule, days, lift)] = lid
            cal = _live.location_calendar([book_rule(lid, rule, days, words, lift)], verdicts, gears)
            out["static"][lid] = cal
            w = words[rule]
            out["p"][lid] = {**({"r": w["region"]} if w.get("region") else {}),
                             **({"water": w["name"]} if w.get("name") else {}),
                             "static": 1, "book": {"rule": rule, "pages": list(w.get("pages") or [])}}
    memo: Dict[Tuple[int, Tuple[str, ...]], int] = {}
    new_sections: List[Tuple[int, int]] = []
    for sid, c in sections:
        cl = closures.get(sid)
        if not cl or c == UNLISTED_CHAIN:
            new_sections.append((sid, c))
            continue
        statics = tuple(sorted(lid_of[(r, d, (lifts.get(sid) or {}).get(r, ""))] for r, d in cl.items()))
        if (c, statics) not in memo:
            memo[(c, statics)] = len(out["chains"])
            out["chains"].append(_book_chain(payload["chains"][c], statics, out["static"], verdicts))
        new_sections.append((sid, memo[(c, statics)]))
    out["verdicts"], out["gears"] = verdicts.items, gears.items
    return out, new_sections


def _book_chain(old: Mapping, statics: Sequence[str], cals: Mapping[str, dict], verdicts: _Interner) -> dict:
    from pipeline.deliver.salmon_who import _run_at, _runs_of
    k = len(statics)
    sp, who = [], []
    for s in range(len(SPECIES)):
        vals, names = [], []
        for d in range(len(DAYS)):
            j = next((i for i, l in enumerate(statics) if _run_at(cals[l]["sp"][s], d) >= 0), None)
            if j is not None:
                vals.append(_run_at(cals[statics[j]]["sp"][s], d))
                names.append(j)
            else:
                vals.append(_run_at(old["sp"][s], d))
                a = _run_at(old["a"][s], d) if old.get("a") else -1
                names.append(a + k if a >= 0 else a)
        sp.append(_runs_of(vals))
        who.append(_runs_of(names))
    mapc = [MAP_CODES.index(map_status([verdicts.items[_run_at(r, d)] for r in sp])) for d in range(len(DAYS))]
    new = {**old, "l": [*statics, *old["l"]], "sp": sp, "m": runs_of(mapc), "a": who}
    if old.get("s"):
        new["s"] = [[*range(k), *(i + k for i in g)] for g in old["s"]]
    new.pop("c", None)
    if old.get("c"):
        c = closed_by_header(sp, verdicts.items)
        if c:
            new["c"] = c
    return new


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
