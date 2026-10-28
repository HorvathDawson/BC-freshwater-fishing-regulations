"""THE DFO SALMON FILE (T211, option A + the Standard / Salmon map toggle): the federal salmon rules, read ONCE here and
shipped as answers per day, so the app decides nothing about a salmon rule (app/MAP.md: every answer is looked up).

    curated  data/curated/regulations/entries/dfo_salmon/   locations: precedence, water, scope words
    feed     <generated>/regs/dfo_salmon/typed/            the typed rules, keyed by fingerprint
    reach    <reach run>/dfo_salmon.jsonl.gz               each location's sections (`pipeline.atlas.reach.dfo_salmon`)
       └─ build() ─▶ the bundle's `salmon_doc` + `salmon_section` ─▶ the release file `salmon` (`salmon/1`, encode())

THE UNIT IS A CHAIN: the DFO locations that hold on a section, NARROWEST FIRST (precedence: a water 3, a tidal-area
default 2, a section default or closure 1, the region default 0; then location id). Region 6 is a cascade — read flat,
section A opens coho on the Kispiox while B(i) closes it and the water reopens a five-week window
(`test_the_skeena_cascade_resolves_through_the_join`) — so per species and day the FIRST location of the chain with a
rule for that species on that day answers, never a wider one.

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

THE FILES (`salmon/2`, split per bundle region — owner 2026-10-28: a water page's card fetches only its region's slice),
little-endian, varints LEB128 (`pipeline.deliver.binary`):
    salmon       magic b"BCSA", version u8 = 2, handles 8 bytes (the bundle's section_handles digest, raw), u32 JSON
                 length, the UTF-8 payload: {credit, regions, verdicts, gears, chains, unmapped, scopes, shards}
    salmon-<r>   magic b"BCSR", version u8 = 1, handles 8 bytes, u8 region length + the region (ASCII), then a varint
                 run count and per run: sid gap (sid - previous run's end), count, chain index — the sections of bundle
                 region <r> (`release.shards.REGIONS`; a section in two regions is in both; `x`: a section in none)
A section in no run is chain 0, UNLISTED: closed to salmon fishing (`payload.default`). `decode_files` is the reference
reader the tests hold the app's to.
"""
from __future__ import annotations

import json
import struct
from collections import defaultdict
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pipeline.common import calendar_spec
from pipeline.deliver.binary import put_varint, read_varint

FORMAT = "salmon/2"
SHARD_FORMAT = "salmon-shard/1"
MAGIC = b"BCSA"
VERSION = 2
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


def verdict_on(chain: Sequence[Sequence[dict]], sp: str, md: int) -> Optional[dict]:
    """The species' answer on day `md`: the narrowest location of the chain with a limit rule for it that day.
    `{s, t, n?}` — s one of open / release / closed / check; None when no location says anything that day."""
    for rules in chain:
        subs: Dict[str, List[dict]] = defaultdict(list)
        for r in rules:
            if r.get("within"):
                subs[r["within"]].append(r)
        limits = [r for r in rules if not r.get("within") and not _is_gear(r) and _covers(r, sp)
                  and ("take" in r or "may_target" in r)]
        held = [r for r in limits if holds_on(r, md)]
        if held:
            own = [r for r in held if sp in _species_of(r)] or held     # the species' own row beats an all-salmon row
            opens = [r for r in own if (r.get("take") or 0) > 0]
            if opens:
                s, why = "open", opens
            elif any(r.get("may_target") is True for r in own):
                s, why = "release", [r for r in own if r.get("may_target") is True]
            else:
                s, why = "closed", own
            out = {"s": s, "t": _text(why, subs)}
            n = _notice(why)
            if n:
                out["n"] = n
            return out
        unread = [r for r in rules if not r.get("within") and holds_on(r, md) is None
                  and (not _species_of(r) or _covers(r, sp))]
        if unread:
            return {"s": "check", "t": _text(unread, subs)}
    return None


def gear_on(chain: Sequence[Sequence[dict]], md: int) -> Tuple[str, ...]:
    """The gear lines that hold on day `md`: the narrowest location with any that day."""
    for rules in chain:
        held = [r for r in rules if _is_gear(r) and holds_on(r, md)
                and (not _species_of(r) or any(_covers(r, sp) for sp in SPECIES))]
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


def chain_calendar(chain: Sequence[Sequence[dict]], verdicts: _Interner, gears: _Interner, partial: bool = False) -> dict:
    """One chain's year: per species runs of a verdict index (`NOT_LISTED` where nothing opens it), the gear runs (-1:
    no gear line), the map-status runs. `partial`: the section's water has a DFO place with rules that is not drawn
    yet, so no stretch of it can be answered with confidence — the map says `check` every day (PARTIAL COVERAGE)."""
    per_sp: List[List[int]] = [[] for _ in SPECIES]
    gear: List[int] = []
    mapc: List[int] = []
    for md in DAYS:
        vs = [verdict_on(chain, sp, md) for sp in SPECIES]
        for k, v in enumerate(vs):
            per_sp[k].append(verdicts(v or NOT_LISTED))
        g = gear_on(chain, md)
        gear.append(gears(list(g)) if g else -1)
        mapc.append(MAP_CODES.index(map_status(vs)))
    m = [0, MAP_CODES.index("check")] if partial else runs_of(mapc)
    return {"sp": [runs_of(x) for x in per_sp], "g": runs_of(gear), "m": m}


def _label(loc) -> str:
    area = (loc.source_text or {}).get("specific_area") or ""
    return f"{loc.water} — {area}" if loc.water and area else (loc.water or area or loc.location_id)


def build(entry_files: Mapping[str, object], feeds: Mapping[str, dict], placed: Iterable[dict],
          sid_of: Mapping[str, int], item_sections: Optional[Mapping[str, Iterable[int]]] = None,
          headers: Optional[Mapping[str, Sequence[dict]]] = None,
          upstream: Optional[Callable[[Sequence[str]], Iterable[str]]] = None) -> Tuple[dict, List[Tuple[int, int]]]:
    """(payload, [(sid, chain)] sorted by sid). `entry_files`: slug → `entries.EntryFile`; `feeds`: slug → the typed
    feed (`typed/region-<slug>.json`); `placed`: the reach run's rows; `sid_of`: section id → handle.

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
    rules_of: Dict[str, List[dict]] = {}
    locs: Dict[str, object] = {}
    regions: Dict[str, dict] = {}
    scopes: Dict[str, Dict[str, List[str]]] = {}
    for slug, ef in sorted(entry_files.items()):
        feed = feeds.get(slug) or {}
        by_fp = {l["fingerprint"]: l["rules"] for l in feed.get("locators") or []}
        notes = list((headers or {}).get(slug) or [])
        sc, unresolved = H.resolve(notes, ef, upstream)
        regions[slug] = {"name": getattr(ef, "region_name", slug), "asof": feed.get("date_modified"),
                         "url": PAGE_URL.format(slug=slug), "notes": notes,
                         **({"unresolved": unresolved} if unresolved else {})}
        if sc:
            scopes[slug] = sc
        for loc in ef.locations:
            # EVERY location, ruled today or not (T211 part 2): a dormant one says nothing today (no rules, so it never
            # answers in its chain), but it is IN its chains, so the live feed (`regs.dfo_salmon.live`) can paint it
            # the day DFO lists it again — without a rebuild.
            rules_of[loc.location_id] = [r for fp in loc.fingerprints for r in by_fp.get(fp, [])]
            locs[loc.location_id] = (slug, loc, ef)

    order = lambda lid: (-locs[lid][1].precedence, lid)          # noqa: E731 — narrowest first
    covering: Dict[int, List[str]] = defaultdict(list)
    unmapped_locs: List[str] = []
    missing: List[str] = []
    for row in placed:
        lid = row["location_id"]
        if lid not in locs:
            continue
        if not row.get("mapped"):
            unmapped_locs.append(lid)
            continue
        for s in row["sections"]:
            sid = sid_of.get(s)
            if sid is None:
                missing.append(s)
                continue
            covering[sid].append(lid)
    if missing:
        raise SalmonError(f"salmon: {len(missing):,} placed sections are not in the handle table (e.g. {missing[:3]}) "
                          "— the reach run and this atlas are not the same build")

    verdicts, gears = _Interner(), _Interner()
    verdicts(NOT_LISTED)                                     # verdicts[0]
    # chain 0: THE UNLISTED WATER — no DFO location, every species closed every day (owner 2026-10-27)
    chains: List[dict] = [{"r": [], "w": "", "l": [], **chain_calendar([], verdicts, gears)}]
    chain_ix: Dict[Tuple[Tuple[str, ...], Tuple[str, ...]], int] = {((), ()): UNLISTED_CHAIN}

    def chain_for(lids: Sequence[str], u: Iterable[str] = ()) -> int:
        key = tuple(sorted(set(lids), key=order))
        uk = tuple(sorted(set(u)))
        if (key, uk) not in chain_ix:
            chain_ix[(key, uk)] = len(chains)
            first = locs[(key or uk)[0]][1]
            chains.append({"r": sorted({locs[l][0] for l in key + uk}), "w": _label(first) if key else first.water,
                           "l": list(key), **({"u": list(uk)} if uk else {}),
                           **chain_calendar([rules_of[l] for l in key], verdicts, gears,
                                            partial=any(rules_of[l] for l in uk))})
        return chain_ix[(key, uk)]

    # the DFO places not drawn yet, by the items of their water, onto those items' sections
    undrawn_on: Dict[int, set] = defaultdict(set)
    for lid in unmapped_locs:
        slug, loc, ef = locs[lid]
        water = ef.water(loc.water_id) if loc.water_id else None
        for it in (water.item_ids if water else ()):
            for sid in (item_sections or {}).get(it, ()):
                undrawn_on[sid].add(lid)
    sections = sorted((sid, chain_for(covering.get(sid, ()), undrawn_on.get(sid, ())))
                      for sid in set(covering) | set(undrawn_on))
    unmapped = []
    for lid in sorted(unmapped_locs):
        slug, loc, ef = locs[lid]
        water = ef.water(loc.water_id) if loc.water_id else None
        # `d: 1` — DORMANT at build (no rules on the page): carried so the live feed can show it the day DFO lists it
        # again; the app leaves it off the card until then (core `pickSalmon` / `withSalmonFeed`)
        unmapped.append({"items": sorted(water.item_ids) if water else [], "r": slug, "w": _label(loc),
                         "chain": chain_for([lid]), **({} if rules_of[lid] else {"d": 1})})
    payload = {"format": FORMAT, "credit": CREDIT, "default": UNLISTED_CHAIN, "regions": regions, "verdicts": verdicts.items,
               "gears": gears.items, "chains": chains, "unmapped": unmapped, "scopes": scopes}
    return payload, sections


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


def load_inputs(run_dir: Path, registry=None, *, entries_dir: Optional[Path] = None,
                feed_dir: Optional[Path] = None) -> Tuple[dict, dict, List[dict]]:
    """The curated entry files, the typed feed and the reach run's placements. A missing feed STOPS the build: a
    release without the salmon would draw a Salmon toggle with nothing behind it."""
    from pipeline.atlas.reach import dfo_salmon as placed_io
    from pipeline.common.curated import GENERATED
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    feed_dir = Path(feed_dir or Path(GENERATED.regs.dfo_salmon) / "typed")
    slugs = [s for s in ALL_SLUGS if not PAGES[s].is_stub]
    files = {s: E.load(s, entries_dir or E.ENTRIES_DIR, registry=registry) for s in slugs}
    feeds = {}
    for s in slugs:
        p = feed_dir / f"region-{s}.json"
        if not p.exists():
            raise SalmonError(f"salmon: no typed DFO feed {p} — write it:\n    python -m pipeline.regs.dfo_salmon.feed")
        feeds[s] = json.loads(p.read_text(encoding="utf-8"))
    return files, feeds, list(placed_io.read(run_dir))


def load_headers(feeds: Mapping[str, dict], cache_dir: Optional[Path] = None) -> Dict[str, List[dict]]:
    """Each region's header notes, read from the cached page the typed rules came from (the raw page when its "Date
    modified" is the feed's, else the history page of that date). A region with no such page ships no notes (said on
    stdout): the live feed carries them, and a header is never read from another day's page than its rules."""
    from pipeline.regs.dfo_salmon import header as H
    from pipeline.regs.dfo_salmon.fetch import DEFAULT_CACHE
    from pipeline.regs.dfo_salmon.parse import _RE_DATE_MOD
    cache = Path(cache_dir or DEFAULT_CACHE)
    out: Dict[str, List[dict]] = {}
    for slug, feed in sorted(feeds.items()):
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
            print(f"  salmon: region {slug}: no cached page dated {want} — the release carries no header notes for it")
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
