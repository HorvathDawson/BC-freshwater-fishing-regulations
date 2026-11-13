"""THE LIVE DFO SALMON FEED (T211 part 2 + part 4): the current DFO pages, typed, joined to the curated locations and
written as ONE small file the app reads, so a rule change reaches the app without a rebuild.

    python -m pipeline.regs.dfo_salmon.live run --regions 2 --dry-run  # T366: fetch + parse, WRITE NOTHING, print
                                                                       #   what would change (`refresh.text`)
    python -m pipeline.regs.dfo_salmon.live [run --regions 2]    # fetch, type, join, write <out>/index.json, archive
                                                                 #   the page, re-emit typed/ — and the same summary
    python -m pipeline.regs.dfo_salmon.live --offline            # the cached pages, no network
    python -m pipeline.regs.dfo_salmon.live queue [--regions 6]  # put the held new / reworded places in DFO mode

THE REFRESH (T366, owner 2026-11-03): a run compares each page with the typed rows in use (`refresh.preview`) and prints
new / reworded / dropped / moved PLACES (the only thing the owner re-reviews), every ROW change per place (old -> new,
FN numbers, how it was read: one digest notice per page, `urgent: False` — no mark goes stale on it, `marks._place_only`),
the owner marks that would move, and the URGENT rows (not read from the table; a place matching no curated place). The
real run then re-emits locations/ rules/ typed/ from the same page (stamped with its sha256, dateModified and header
notes), so the review app, the release and the feed read one vintage.

THE SPLIT (pipeline/docs/19 §1): the geography never moves, the rules churn. The release ships the GEOMETRY — which
sections each chain of DFO locations covers (`pipeline/deliver/salmon.py`, `chains[].l`). This feed ships the RULES as
answers per LOCATION and day: per species the verdict index (or -1: this location says nothing that day) and the gear
lines. The app re-composes each release chain from it, narrowest location first, exactly as `deliver.salmon.
chain_calendar` does (`compose` here is the reference, pinned against it), so the app still decides nothing.

    index.json    {format: "dfo-feed/3", at, regions: {slug: {name, url, asof, sha256, changed, checked, stale, held,
                   failures, notes, header, unresolved, bucket?, shared, sizes}}, verdicts, gears, locs: {location_id: {r, sp: [5 runs], g: runs, x?}},
                   unplaced: n}      notes: the region header's typed notes (`header.py`); header: their digest
                   x (T321): the rows the location's card shows in DFO's words ({k: new|unread|note, sp, d, t})
    unplaced.json the places the page lists that no curated location knows (part 4) — their rules HELD, never dropped
    <out>.notified.json   BESIDE the feed dir, never synced: the notices already sent (`gauges.feed.notify`)

Per region, every run (no credits, no secrets — safe on a cron):
  * fetch (validated, atomic; `fetch.fetch_region`), every page fetched kept in `cache/dfo_salmon/history/` once
    (`fetch.archive`: the wording archive dates it);
    a failed fetch keeps the last feed's answers for the region, marked `stale`; two failures in a row notify;
  * parse → untangle → `verify()` → locations → typed (`feed.build`); a failure holds the region at the last feed and
    notifies;
  * `entries.reconcile`: a STRUCTURAL change holds the whole region (last feed) and notifies; a `section_moved`
    location keeps its last answers and notifies;
  * the join, by fingerprint (`EntryFile.by_fingerprint`, read only — a curator's confirmed rewording reaches the app on
    the next run with no rebuild). A fingerprint nobody curated is UNPLACED (part 4): its rules go to `unplaced.json`,
    the owner is told once per (region, fingerprint) — drift (a near location on the same water), new (none) or
    new_water (the water's name is unknown too) — and `queue` seeds it into the entry files, where the review app's
    DFO mode lists it (the same queue as every other DFO location; no separate review path).

Every notice carries `urgent` (T366): held / unplaced / new wording / unread rows / header and text changes are urgent;
the row-change digest is not.

Nothing here writes a curated file except `queue`, which is run by a person (the seed never edits an existing record).
The schedule (every 6 h in season) and the R2 sync of the feed dir to `feeds/dfo/` are the owner's
(handoff/PLAN/OWNER-TODO.md).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from pipeline.common.curated import GENERATED
from pipeline.common.feed_formats import FEED_FORMATS
from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import fetch as F

logger = logging.getLogger(__name__)

#: T332 (/2): a verdict's quota is `tree` (a game-fish TreeTier), a bucket `{tree, …}`. T356 (/3): a header closure's
#: verdict carries `c`, a region `shared` + `sizes` (`deliver.salmon.header_answers`), a note's length is `lengths`
FORMAT = FEED_FORMATS["dfo_feed"]          # (T556) typed once, in pipeline/common/feed_formats.py
OUT_DIR = Path(GENERATED.regs.dfo_salmon) / "feed"
#: two failed fetches in a row tell the owner (one is a blip)
FAIL_NOTIFY = 2
QUEUE_ACTION = "python -m pipeline.regs.dfo_salmon.live queue --regions {slug}"
#: (T321) an in-season notice number in a cell, as printed
Q_FN = re.compile(r"\bFN\s*\d{3,4}\b", re.I)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


# ── the calendar of one location, and the chain composed from them ───────────────────────────────────────────────────

def location_calendar(rules: Sequence[dict], verdicts: S._Interner, gears: S._Interner) -> dict:
    """One location's year: per species runs of a verdict index (-1: says nothing that day), gear runs (-1: none)."""
    chain = [list(rules)]
    per_sp: List[List[int]] = [[] for _ in S.SPECIES]
    gear: List[int] = []
    for md in S.DAYS:
        for k, sp in enumerate(S.SPECIES):
            v = S.verdict_on(chain, sp, md)
            per_sp[k].append(verdicts(v) if v else -1)
        g = S.gear_on(chain, md)
        gear.append(gears(list(g)) if g else -1)
    return {"sp": [S.runs_of(x) for x in per_sp], "g": S.runs_of(gear)}


def _at(runs: Sequence[int], day: int) -> int:
    v = runs[1] if len(runs) > 1 else -1
    for k in range(0, len(runs), 2):
        if runs[k] > day:
            break
        v = runs[k + 1]
    return v


def compose(lids: Sequence[str], locs: Mapping[str, dict], verdicts: Sequence[dict], undrawn: Sequence[str] = (),
            speaks: bool = False, groups: Optional[Sequence[Sequence[int]]] = None,
            intern: Optional[Callable[[dict], int]] = None, gears: Optional[Sequence[Sequence[str]]] = None,
            gear_intern: Optional[Callable[[list], int]] = None) -> dict:
    """A release chain's year from the feed: per species and day the FIRST location (narrowest first, the chain's own
    order) with an answer, else `NOT_LISTED` (index 0); gear the first with any; the map status from the five. The
    reference the app's `withSalmonFeed` is held to — equal to `deliver.salmon.chain_calendar` over the same rules.
    `undrawn`: the chain's `u` (DFO places on its water not drawn yet) — never the map's (T372, the trout way), only
    whether the header closure `c` holds for them too. `speaks`: an undrawn place's own chain carries `n`, the days DFO
    speaks for it (`deliver.salmon.speaks_runs`). (T668) `groups`: the chain's `s` — on a section in two regions each
    region's places (indices into `lids`) answer apart and the most strict per fish speaks (`deliver.salmon.strictest`;
    a group with no answer is `NOT_LISTED`; two openings by their limits, which may be a NEW verdict — both regions'
    words, no number — added through `intern`, which appends to `verdicts`); the gear lines are every group's (`deliver.salmon.both_gear`, a new
    list added through `gear_intern`, `gears` the feed's). A merge with no interner to add it to STOPS (never a guess)."""
    subs = [[locs[lids[i]] for i in g if lids[i] in locs] for g in groups] if groups else \
        [[locs[l] for l in lids if l in locs]]
    per_sp: List[List[int]] = [[] for _ in S.SPECIES]
    gear: List[int] = []
    mapc: List[int] = []
    first = lambda xs, none=0: next((v for v in xs if v >= 0), none)            # noqa: E731

    def one(ixs: List[int]) -> int:
        if len(ixs) == 1:
            return ixs[0]
        got = S.strictest([verdicts[i] for i in ixs])
        hit = next((i for i in ixs if verdicts[i] is got), None)
        if hit is not None:
            return hit
        if intern is None:
            raise S.SalmonError("live.compose: two regions' openings need a merged verdict and no `intern` was given")
        ix = intern(got)
        if not 0 <= ix < len(verdicts) or verdicts[ix] != got:
            raise S.SalmonError("live.compose: `intern` must add to `verdicts` (the same list)")
        return ix

    def gear_of(gs: List[int]) -> int:
        have = [g for g in gs if g >= 0]
        if len(have) <= 1:
            return have[0] if have else -1
        if gears is None or gear_intern is None:
            raise S.SalmonError("live.compose: two regions' gear lines need `gears` and `gear_intern`")
        merged = list(S.both_gear([gears[g] for g in have]))
        same = next((g for g in have if list(gears[g]) == merged), None)
        return same if same is not None else gear_intern(merged)

    for day in range(len(S.DAYS)):
        vs = []
        for k in range(len(S.SPECIES)):
            ix = one([first(_at(c["sp"][k], day) for c in cals) for cals in subs])
            per_sp[k].append(ix)
            vs.append(verdicts[ix])
        gear.append(gear_of([first((_at(c["g"], day) for c in cals), -1) for cals in subs]))
        mapc.append(S.MAP_CODES.index(S.map_status(vs)))
    sp = [S.runs_of(x) for x in per_sp]
    # T356: closed by the region header every day — and so is every undrawn place the feed lists — `c`
    shut = all(S.closed_by_header(locs[l]["sp"], verdicts) for l in undrawn if l in locs)
    c = S.closed_by_header(sp, verdicts) if shut else []
    n = S.speaks_runs(per_sp, verdicts) if speaks else None
    return {"sp": sp, "g": S.runs_of(gear), "m": S.runs_of(mapc), **({"c": c} if c else {}), **({"n": n} if n else {})}


# ── one region ───────────────────────────────────────────────────────────────────────────────────────────────────────

def type_page(html: str, slug: str):
    """The page → (typed feed, locations, structure). Raises when the page does not parse or verify."""
    from pipeline.regs.dfo_salmon import feed as FD
    from pipeline.regs.dfo_salmon.locations import extract
    from pipeline.regs.dfo_salmon.parse import parse_region
    from pipeline.regs.dfo_salmon.untangle import untangle, verify
    parsed = parse_region(html, slug)
    u = untangle(parsed)
    verify(parsed, u)
    locs, rules, structure = extract(u)
    scraped = {"region": slug, "date_modified": parsed.date_modified, "page": F.page_stamp(html),
               "preamble": parsed.preamble, "rules": [r.to_dict() for r in rules]}
    try:                                                       # T366: the header rides with its rows
        from pipeline.regs.dfo_salmon.header import FORMAT, parse_header
        scraped["notes"], scraped["header_format"] = parse_header(html, slug), FORMAT
    except Exception:                                          # noqa: BLE001 — told by `_read_header`; no notes key
        pass
    return FD.build(scraped, robust=False), locs, structure


def _water_known(ef, water: str) -> bool:
    from pipeline.common.names import key
    k = key(water or "")
    return not k or any(key(w.name) == k or k in {key(a) for a in w.aliases} for w in ef.waters)


def _items_of(ef, water: str) -> List[str]:
    """(T521) the app item ids of the binding the DFO water name (or an alias) names, sorted; [] when none does — the
    water page that shows an unplaced row as "DFO rule, location not yet on the map"."""
    from pipeline.common.names import key
    k = key(water or "")
    if not k:
        return []
    return sorted({i for w in ef.waters if key(w.name) == k or k in {key(a) for a in w.aliases} for i in w.item_ids})


def join(slug: str, ef, typed: dict, locations: Sequence, report) -> Tuple[Dict[str, List[dict]], List[dict]]:
    """(rules by location_id, unplaced). A fingerprint the entry file knows joins its location; any other is UNPLACED:
    drift (a near location on the same water), new, or new_water — its rules kept with it."""
    index = ef.by_fingerprint()
    by_fp = {l.fingerprint: l for l in locations}
    outcome = {o.fingerprint: o for o in report.outcomes}
    rules: Dict[str, List[dict]] = {}
    unplaced: List[dict] = []
    for loc in typed.get("locators") or []:
        fp = loc["fingerprint"]
        hit = index.get(fp)
        if hit is not None:
            from pipeline.regs.dfo_salmon.quota import mark_shared, pool_of
            rules.setdefault(hit.location_id, []).extend(mark_shared(loc["rules"], pool_of(hit)))
            continue
        if fp.startswith("page:"):
            continue                # T349: a page default's sentence with no default location here is not a new place
        src = by_fp.get(fp)
        o = outcome.get(fp)
        water = src.water if src else ""
        kind = "new_water" if not _water_known(ef, water) else ("drift" if o and o.status == "drift" else "new")
        unplaced.append({"region": slug, "fingerprint": fp, "kind": kind, "water": water,
                         "scope": src.specific_area if src else "", "section": src.section if src else None,
                         "candidates": list(o.candidates) if o else [], "rows": loc["rows"], "rules": loc["rules"],
                         "page_date": typed.get("date_modified"), "items": _items_of(ef, water)})
    return rules, unplaced


def words_by_location(ef, typed: dict) -> Dict[str, List[dict]]:
    """(T321) location id -> the rows its card shows in DFO's words (`wording.row_words`: new wording, an unread number,
    a note) as `{k, sp, d, t}` — the feed's `locs[lid].x`."""
    index = ef.by_fingerprint()
    out: Dict[str, List[dict]] = {}
    for loc in typed.get("locators") or []:
        hit = index.get(loc["fingerprint"])
        if hit is not None and loc.get("words"):
            out.setdefault(hit.location_id, []).extend(
                {k: w[k] for k in ("k", "sp", "d", "t")} for w in loc["words"])
    return out


def wording_notices(slug: str, ef, typed: dict) -> List[dict]:
    """(T321, owner 2026-10-31: "It should be urgent") a clause no saved page ever printed: one URGENT notice per new
    TEMPLATE (the key is the template, so a later run, or another region, never tells it again). What it says, where,
    the notice number, the whole cell, and what to do."""
    from pipeline.regs.dfo_salmon import wording as W
    index = ef.by_fingerprint()
    out: List[dict] = []
    for loc in typed.get("locators") or []:
        hit = index.get(loc["fingerprint"])
        for w in loc.get("words") or ():
            for c in w.get("new") or ():
                t = W.template(c)
                fn = Q_FN.search(w["t"])
                where = hit.location_id if hit is not None else loc["fingerprint"]
                if any(n["key"] == f"dfo-wording:{t}" for n in out):
                    continue                                    # one notice a template, wherever it is printed
                out.append({"key": f"dfo-wording:{t}", "urgent": True,
                            "text": f"DFO Region {slug}: new wording in a limits cell, {c!r}, on {where} "
                                    f"({w['sp'] or 'no species'}, {w['d'] or 'all year'}"
                                    f"{', ' + fn.group(0) if fn else ''}): {w['t']!r} — the row shows DFO's words and "
                                    f"no number until {W.REGISTRY.relative_to(W.REPO_ROOT)} has an entry for "
                                    f"{t!r} (and pipeline/tests/test_dfo_wording.py a line for it)"})
    return out


# ── notices ──────────────────────────────────────────────────────────────────────────────────────────────────────────

def notice_key(n: dict) -> str:
    return n["key"]


def notice_text(n: dict) -> str:
    return n["text"]


def unplaced_notice(u: dict) -> dict:
    slug, n = u["region"], len(u["rules"])
    held = f"{n} rule{'s' if n != 1 else ''} held unplaced"
    act = QUEUE_ACTION.format(slug=slug)
    if u["kind"] == "drift" and u["candidates"]:
        c = u["candidates"][0]
        text = (f"DFO Region {slug}: new wording on {u['water']}, likely {c['location_id']} ({c['score']:.2f}) — "
                f"{held}. Confirm in DFO review after: {act}")
    elif u["kind"] == "new_water":
        text = (f"DFO Region {slug}: a water no entry knows, {u['water']!r}: {u['scope']!r} — {held}. "
                f"Bind it in DFO review after: {act}")
    else:
        text = (f"DFO Region {slug}: new location on {u['water']}: {u['scope']!r} — {held}. "
                f"Place it in DFO review after: {act}")
    # T366: a row whose place matches no curated DFO place is URGENT (and a place question: `live queue`)
    return {"key": f"dfo-{u['kind']}:{slug}:{u['fingerprint']}", "urgent": True, "text": text}


# ── the run ──────────────────────────────────────────────────────────────────────────────────────────────────────────

def _read(p: Path, default):
    """The last run's file, or `default` when there is none. (T357) A file that is there but unreadable STOPS the run: a
    truncated index.json read as "none" silently dropped every answer the run carries (a held or failed region's)."""
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"{p}: the last run's file is unreadable ({type(exc).__name__}: {exc}); "
                           "restore it or remove it to start without it") from exc


def _write(p: Path, obj) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, p)


def _carry(prev: dict, slug: str, verdicts: S._Interner, gears: S._Interner, only: Optional[set] = None) -> dict:
    """The last feed's answers for a region (or for some of its locations), re-indexed into this run's tables."""
    pv, pg = prev.get("verdicts") or [], prev.get("gears") or []
    out = {}
    for lid, c in (prev.get("locs") or {}).items():
        if c.get("r") != slug or (only is not None and lid not in only):
            continue
        sp = [[x if k % 2 == 0 or x < 0 else verdicts(pv[x]) for k, x in enumerate(runs)] for runs in c["sp"]]
        g = [x if k % 2 == 0 or x < 0 else gears(pg[x]) for k, x in enumerate(c["g"])]
        out[lid] = {"r": slug, "sp": sp, "g": g, **({"x": c["x"]} if c.get("x") else {})}
    return out


Fetcher = Callable[[str], Tuple[str, Optional[str]]]


def _read_header(slug: str, html: str, reg: dict, was: dict, entries_dir: Optional[Path], notices: List[dict]) -> None:
    """The page's header notes onto the region (`header.parse_header`). A change of its words tells the owner once (the
    existing notify path); a water a scoped note names that no curated DFO water binds is listed (`unresolved`) and told
    once — never matched by nearness. A header that will not parse keeps the last one and tells the owner."""
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.dfo_salmon import header as H
    try:
        notes = H.parse_header(html, slug)
    except Exception as exc:                                   # noqa: BLE001 — the last header stands
        notices.append({"key": f"dfo-header-parse:{slug}:{reg['sha256'][:12]}", "urgent": True,
                        "text": f"DFO Region {slug}: the region header did not parse ({type(exc).__name__}); the last "
                                "one stands"})
        return
    if not notes and was.get("notes"):
        notices.append({"key": f"dfo-header-empty:{slug}:{reg['sha256'][:12]}",
                        "text": f"DFO Region {slug}: the page has no region header any more; the last one stands"})
        return
    d = H.digest(notes)[:16]
    _, unresolved = H.resolve(notes, E.load(slug, entries_dir or E.ENTRIES_DIR))
    for u in unresolved:
        notices.append({"key": f"dfo-header-name:{slug}:{u['name']}",
                        "text": f"DFO Region {slug}: the header names {u['name']!r}, which no curated DFO water binds — "
                                "its note shows on no water until it is bound (DFO review)"})
    reg["notes"], reg["header"], reg["unresolved"] = notes, d, unresolved
    _set_bucket(reg)


def _same_format(prev: dict) -> dict:
    """(T357) The last feed, only in this run's format: the dfo-feed/1 upgrade is gone (the local feed and staging's are
    dfo-feed/2; T356 made it /3 — remove the /2 once), so an older one stops the run rather than being carried in a shape this feed does not write."""
    if prev and prev.get("format") != FORMAT:
        raise RuntimeError(f"the last feed is {prev.get('format')!r}, not {FORMAT}: remove it (or run once with an "
                           "empty --out) to start without it")
    return prev


def _set_bucket(reg: dict) -> None:
    """(T320) the salmon bucket — the header's combined daily / possession — beside the notes it is read from; (T356)
    with the region's `shared` and `sizes`, as the release's region carries them (`deliver.salmon.header_answers`)."""
    reg.pop("bucket", None)
    reg.update(S.header_answers(reg.get("notes") or []))


def page_texts(html: str, slug: str) -> List[str]:
    """(T320) THE PAGE'S GENERAL TEXT: every sentence above the table (the header, the "unless otherwise stated" lines),
    the section banners and the notes under the table — what the feed does NOT read into rules, in page order."""
    from pipeline.regs.dfo_salmon.parse import parse_region
    p = parse_region(html, slug)
    out = [*p.preamble, *(s.title for s in p.sections), *(n.get("text") or "" for n in p.notes)]
    return list(dict.fromkeys(t for t in out if t))


def _read_texts(slug: str, html: str, reg: dict, was: dict, notices: List[dict]) -> None:
    """(T320, owner 2026-10-31) A change in the page's general text (`page_texts`) is for a human: the feed reads no rule
    out of it. One URGENT notice per new text, old vs new, with what to do. The first read only stores the text."""
    try:
        texts = page_texts(html, slug)
    except Exception:                                          # noqa: BLE001 — the page's own parse failure is told
        return
    old = list(was.get("texts") or [])
    if old and texts != old:
        gone = [t for t in old if t not in texts]
        new = [t for t in texts if t not in old]
        d = hashlib.sha256("\n".join(texts).encode("utf-8")).hexdigest()[:16]
        lines = [f"- was: {t}" for t in gone] + [f"+ now: {t}" for t in new]
        notices.append({"key": f"dfo-text:{slug}:{d}", "urgent": True,
                        "text": f"DFO Region {slug}: the page's general text changed — review the header; the feed "
                                f"reads no rule from it ({len(gone)} gone, {len(new)} new)\n" + "\n".join(lines)})
    reg["texts"] = texts


def run(out: Path, *, fetcher: Fetcher, regions: Optional[Sequence[str]] = None, entries_dir: Optional[Path] = None,
        history_dir: Optional[Path] = None, now: Optional[str] = None, notify_url: Optional[str] = None,
        sender=None, typed_dir: Optional[Path] = None, scrape_dir: Optional[Path] = None, dry_run: bool = False,
        marks_path: Optional[Path] = None, with_marks: bool = True) -> dict:
    """One feed run. `fetcher(slug)` → (html, fetched_at) or raises. Writes `<out>/index.json` + `unplaced.json`,
    sends the due notices; returns `{index, unplaced, notices, sent, previews}`.

    T366: `typed_dir` — the typed feed the review app and the release read: each region read is compared with it
    (`refresh.preview`: places, rows changed, marks that would move, urgent rows) and its row changes go to ONE digest
    notice per page (`urgent: False`); `scrape_dir` — when set, a region read and not held is RE-EMITTED there
    (locations/, rules/, typed/: one vintage with the feed, stamped with its page and header); `dry_run` — fetch and
    parse only: no history copy, no feed files, no re-emit, nothing sent."""
    from pipeline.regs.dfo_salmon import refresh as R
    from pipeline.gauges.feed import notify
    from pipeline.regs.dfo_salmon import entries as E

    out = Path(out)
    at = now or _now()
    prev = _same_format(_read(out / "index.json", {}))
    prev_unplaced = {(u["region"], u["fingerprint"]): u for u in _read(out / "unplaced.json", [])}
    todo = [F.normalize_slug(s) for s in regions] if regions else [s for s in F.ALL_SLUGS if not F.PAGES[s].is_stub]
    verdicts, gears = S._Interner(), S._Interner()
    verdicts(S.NOT_LISTED)                                     # verdicts[0], as the release's
    locs: Dict[str, dict] = {}
    region_out: Dict[str, dict] = {}
    unplaced: List[dict] = []
    notices: List[dict] = []
    previews: Dict[str, dict] = {}
    for slug in F.ALL_SLUGS:                                   # regions not asked this run keep their last answers
        if slug not in todo and slug in (prev.get("regions") or {}):
            locs.update(_carry(prev, slug, verdicts, gears))
            region_out[slug] = prev["regions"][slug]
            unplaced += [u for (r, _), u in prev_unplaced.items() if r == slug]
    for slug in todo:
        page = F.PAGES[slug]
        was = (prev.get("regions") or {}).get(slug) or {}
        reg = {"name": page.name, "url": F._BASE.format(slug=slug), "asof": was.get("asof"),
               "sha256": was.get("sha256"), "changed": was.get("changed"), "checked": was.get("checked"),
               "stale": False, "held": None, "failures": 0,
               # THE REGION HEADER (owner 2026-10-28): DFO's region-wide rules as typed notes (`header.py`), the last
               # read's kept through a failed fetch or a held page like the answers
               "notes": was.get("notes") or [], "header": was.get("header"), "unresolved": was.get("unresolved") or [],
               # (T320) the page's general text, kept to tell the owner what changed (`_read_texts`)
               "texts": was.get("texts") or []}
        _set_bucket(reg)
        region_out[slug] = reg

        def hold(why: str, key: str) -> None:
            reg["held"] = why
            locs.update(_carry(prev, slug, verdicts, gears))
            unplaced.extend(u for (r, _), u in prev_unplaced.items() if r == slug)
            notices.append({"key": f"dfo-held:{slug}:{key}", "urgent": True,
                            "text": f"DFO Region {slug} held at the last feed: {why}"})

        try:
            html, fetched = fetcher(slug)
        except Exception as exc:                               # noqa: BLE001 — a page that failed keeps its last answers
            reg["stale"], reg["failures"] = True, int(was.get("failures") or 0) + 1
            locs.update(_carry(prev, slug, verdicts, gears))
            unplaced.extend(u for (r, _), u in prev_unplaced.items() if r == slug)
            logger.warning("region %s: fetch failed (%s); the last feed stands", slug, exc)
            if reg["failures"] >= FAIL_NOTIFY:
                notices.append({"key": f"dfo-fetch:{slug}:{was.get('checked') or 'never'}",
                                "text": f"DFO Region {slug}: {reg['failures']} fetches in a row failed ({exc}); "
                                        f"the feed shows the page of {reg['asof'] or 'no date'}"})
            continue
        sha = hashlib.sha256(html.encode("utf-8", "replace")).hexdigest()
        if sha != reg["sha256"]:
            reg["changed"] = at
        if history_dir is not None and not dry_run:            # T366 item 3: every fetch archived (one copy a page)
            F.archive(Path(history_dir), slug, html, fetched or at)
        reg["sha256"], reg["checked"] = sha, fetched or at
        try:
            typed, found, structure = type_page(html, slug)
        except Exception as exc:                               # noqa: BLE001
            hold(f"the page did not parse ({type(exc).__name__}: {str(exc)[:120]})", sha[:12])
            continue
        reg["asof"] = typed.get("date_modified")
        ef = E.load(slug, entries_dir or E.ENTRIES_DIR)
        rep = E.reconcile(ef, found, structure)
        if rep.structural:
            hold("; ".join(rep.structural), sha[:12])
            continue
        _read_header(slug, html, reg, was, entries_dir, notices)    # a held page keeps its last header
        _read_texts(slug, html, reg, was, notices)
        rules, held = join(slug, ef, typed, found, rep)
        moved = {o.location_id for o in rep.outcomes if o.status == "section_moved" and o.location_id}
        from pipeline.regs.dfo_salmon.quota import header_closure
        closure = header_closure(reg.get("notes") or [], slug)     # T320: the header's closure wins
        words = {} if closure else words_by_location(ef, typed)    # T321: a closed page's rows are not in force
        notices.extend(wording_notices(slug, ef, typed))
        for lid, rs in sorted(rules.items()):
            if lid not in moved:
                locs[lid] = {"r": slug, **location_calendar(closure if closure and rs else rs, verdicts, gears),
                             **({"x": words[lid]} if words.get(lid) else {})}
        if moved:
            locs.update(_carry(prev, slug, verdicts, gears, only=moved))
            for o in rep.outcomes:
                if o.status == "section_moved":
                    notices.append({"key": f"dfo-section:{slug}:{o.fingerprint}",
                                    "text": f"DFO Region {slug}: {o.location_id} moved section ({o.detail}); its last "
                                            "answers stand until it is reviewed in DFO mode"})
        for u in held:
            u["first_seen"] = (prev_unplaced.get((slug, u["fingerprint"])) or {}).get("first_seen") or at
            unplaced.append(u)
            notices.append(unplaced_notice(u))
        # T366: what this page changes against the typed rows in use — places, rows (the digest), marks, urgent rows
        old = _read_typed(typed_dir, slug) if typed_dir is not None else None
        pv = R.preview(slug, ef, rep, typed, old, held, typed_dir=typed_dir, entries_dir=entries_dir,
                       marks_path=marks_path, with_marks=with_marks and typed_dir is not None)
        previews[slug] = pv
        for u in pv["unread"]:
            notices.append({"key": f"dfo-unread:{slug}:{u['fingerprint']}:{hashlib.sha256(u['t'].encode()).hexdigest()[:12]}",
                            "urgent": True,
                            "text": f"DFO Region {slug}: a row the table could not be read into rules ({u['k']}) on "
                                    f"{u['location_id'] or u['fingerprint']}: {u['sp'] or '-'} | {u['d'] or '-'} | "
                                    f"{u['t']!r} — the card shows DFO's words; read it and add the wording "
                                    "(pipeline/regs/dfo_salmon/wording.json)"})
        if pv["rows"]:
            notices.append({"key": f"dfo-rows:{slug}:{sha[:12]}", "urgent": False,
                            "text": f"DFO Region {slug}: {len(pv['rows'])} place(s) with changed rows on the page of "
                                    f"{typed.get('date_modified') or '?'} (digest — the places stand, no re-review; "
                                    f"fix anything misread):\n" + R.rows_text(pv)})
        if scrape_dir is not None and not dry_run:              # one vintage: the typed rows ARE this page's
            emit_region(slug, html, Path(scrape_dir), typed_dir)

    index = {"format": FORMAT, "at": at, "regions": {s: region_out[s] for s in F.ALL_SLUGS if s in region_out},
             "verdicts": verdicts.items, "gears": gears.items, "locs": dict(sorted(locs.items())),
             "unplaced": len(unplaced)}
    if dry_run:
        return {"index": index, "unplaced": unplaced, "notices": notices, "sent": None, "previews": previews}
    _write(out / "index.json", index)
    _write(out / "unplaced.json", sorted(unplaced, key=lambda u: (u["region"], u["fingerprint"])))
    sent = notify.send_due(notices, out, url=notify_url, sender=sender, key_of=notice_key, message_of=notice_text)
    return {"index": index, "unplaced": unplaced, "notices": notices, "sent": sent, "previews": previews}


def _read_typed(typed_dir: Path, slug: str) -> Optional[dict]:
    """The typed feed in use for a region, or None when there is none yet (a first emit: every row is new)."""
    p = Path(typed_dir) / f"region-{slug}.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def emit_region(slug: str, html: str, scrape_dir: Path, typed_dir: Optional[Path] = None) -> None:
    """(T366) Re-emit one region from the page just read: locations/ + rules/ (`locations.emit_html`, stamped with the
    page and its header) and typed/ (`feed.emit`) — so the typed rows, the header and the feed are one vintage."""
    from pipeline.regs.dfo_salmon import feed as FD
    from pipeline.regs.dfo_salmon import locations as LC
    LC.emit_html(slug, html, scrape_dir)
    FD.emit(slug, rules_dir=scrape_dir / "rules", out_dir=Path(typed_dir or scrape_dir / "typed"))


def live_fetcher(cache_dir: Path) -> Fetcher:
    def get(slug: str) -> Tuple[str, Optional[str]]:
        snap = F.fetch_region(slug, cache_dir=cache_dir)
        manifest = F._load_manifest(Path(cache_dir))
        manifest[slug] = snap.to_dict()
        F._write_manifest(Path(cache_dir), manifest)
        return F.load_cached(slug, cache_dir), snap.fetched_at
    return get


def peek_fetcher() -> Fetcher:
    """(T366) The preview: the live page, validated, NOTHING written (no cache, no manifest)."""
    def get(slug: str) -> Tuple[str, Optional[str]]:
        body, snap = F.fetch_page(slug)
        return body, snap.fetched_at
    return get


def cached_fetcher(cache_dir: Path) -> Fetcher:
    """The dry run: the pages already in the cache, no network (their `checked` is the cache file's time)."""
    def get(slug: str) -> Tuple[str, Optional[str]]:
        p = Path(cache_dir) / "raw" / f"region{slug}-eng.html"
        t = datetime.fromtimestamp(p.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")
        return p.read_text(encoding="utf-8"), t.replace("+00:00", "Z")
    return get


def queue(out: Path, regions: Optional[Sequence[str]] = None, *, cache_dir: Path = F.DEFAULT_CACHE,
          scrape_dir: Optional[Path] = None, entries_dir: Optional[Path] = None) -> List[str]:
    """PART 4's review hand-off (a person runs it): the regions holding unplaced places are re-scraped from the cache,
    SEEDED into the entry files (`entries seed`: adds the new places, never edits one) and grouped (`entries group`:
    a rewording is proposed as a variant of the location it drifts from), and the typed rules refreshed — so each
    shows in the review app's DFO mode like every other location. Returns the regions done."""
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.dfo_salmon import feed as FD
    from pipeline.regs.dfo_salmon import locations as L
    scrape = Path(scrape_dir or GENERATED.regs.dfo_salmon)
    held = _read(Path(out) / "unplaced.json", [])
    slugs = sorted({u["region"] for u in held if not regions or u["region"] in regions}, key=F.ALL_SLUGS.index)
    if not slugs:
        return []
    from pipeline.regs.dfo_salmon import refresh as R
    for w in R.vintage(scrape / "typed", cache_dir):                # T366: say it before the rows are replaced
        print(f"  WARNING {w['text']}")
    before = {s: _read_typed(scrape / "typed", s) for s in slugs}
    for s in slugs:
        L.emit(s, scrape, cache_dir)
    FD.main(["--regions", *slugs, "--rules-dir", str(scrape / "rules"), "--out", str(scrape / "typed")])
    for s in slugs:                                                 # the rows that moved (digest: no mark stales)
        rows = R.row_changes(E.load(s, entries_dir or E.ENTRIES_DIR), R.feed_map(before[s]),
                             R.feed_map(_read_typed(scrape / "typed", s)))
        if rows:
            print(f"  region {s}: {len(rows)} place(s) with changed rows (re-emitted from the saved page):\n"
                  + R.rows_text({"rows": rows}))
    ed = ["--entries-dir", str(entries_dir)] if entries_dir else []
    E.main(["seed", "--regions", *slugs, "--scrape", str(scrape), *ed])
    E.main(["group", "--regions", *slugs, *ed])
    return slugs


def check_release(out: Path, release: Optional[Path] = None) -> int:
    """(T549) the feed and the release agree on the location set: every place the feed (`<out>/index.json`) has rules
    for is one the release shows, or one it states it does not (`off`). 1 when a feed place is neither — its rules reach
    no angler (a place curated after the release: the next deliver carries it)."""
    from pipeline.deliver.release.manifest import live
    rel = Path(release) if release else live(GENERATED.release)
    ix = Path(out) / "index.json"
    if not ix.exists():
        raise SystemExit(f"live check-release: no feed {ix} — write it: python -m pipeline.regs.dfo_salmon.live run")
    got = S.feed_vs_release(S.read_release(rel), json.loads(ix.read_text(encoding="utf-8")))
    print(f"feed {ix} vs release {rel.name}: {len(got['feed_only'])} feed place(s) the release does not know, "
          f"{len(got['said'])} it states it does not show, {len(got['release_only'])} release place(s) not in the feed")
    for lid in got["feed_only"]:
        print(f"  NOT IN THE RELEASE: {lid}")
    return 1 if got["feed_only"] else 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", nargs="?", default="run", choices=["run", "queue", "check-release"])
    ap.add_argument("--release", type=Path, help="check-release: the release dir (default: the live one, LATEST)")
    ap.add_argument("--regions", nargs="+", choices=F.ALL_SLUGS)
    ap.add_argument("--out", type=Path, default=OUT_DIR)
    ap.add_argument("--cache-dir", type=Path, default=F.DEFAULT_CACHE)
    ap.add_argument("--offline", action="store_true", help="the cached pages, no network")
    ap.add_argument("--dry-run", action="store_true",
                    help="T366: fetch + parse, write NOTHING (no cache, history, feed, typed rows), print what would change")
    ap.add_argument("--no-marks", action="store_true", help="skip the marks check in the summary (faster)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.command == "check-release":
        return check_release(args.out, args.release)

    if args.command == "queue":
        done = queue(args.out, args.regions, cache_dir=args.cache_dir)
        if not done:
            # (T549 item 9) the owner's email says "First: live queue", but the WORKER's unplaced.json is on R2, never
            # here: a calm "nothing" exit hid that the place it emailed about was not seeded
            print(f"live queue: nothing unplaced in {Path(args.out) / 'unplaced.json'} for regions "
                  f"{args.regions or 'any'} — this reads THIS machine's feed, not the worker's. Run the region here "
                  "first so its unplaced places land in it, then queue again:\n"
                  f"    python -m pipeline.regs.dfo_salmon.live run --regions {' '.join(args.regions or ['<n>'])}",
                  file=sys.stderr)
            return 1
        print(f"seeded and grouped regions {done}; review them in DFO mode, then commit the entry files")
        return 0

    from pipeline.regs.dfo_salmon import feed as FD
    from pipeline.regs.dfo_salmon import refresh as R
    scrape = Path(GENERATED.regs.dfo_salmon)
    fetcher = (cached_fetcher(args.cache_dir) if args.offline else
               peek_fetcher() if args.dry_run else live_fetcher(args.cache_dir))
    got = run(args.out, fetcher=fetcher, regions=args.regions, dry_run=args.dry_run,
              history_dir=None if args.offline else Path(args.cache_dir) / "history",
              typed_dir=FD.FEED_DIR, scrape_dir=scrape, with_marks=not args.no_marks)
    for slug, pv in got["previews"].items():                    # T366: the same summary, dry or real
        print(R.text(pv) + "\n")
    urgent = [n for n in got["notices"] if n.get("urgent")]
    if urgent:
        print(f"URGENT notices ({len(urgent)}):")
        for n in urgent:
            print("  " + n["text"].replace("\n", "\n    "))
        print()
    ix = got["index"]
    for slug, r in ix["regions"].items():
        n = sum(1 for c in ix["locs"].values() if c["r"] == slug)
        flag = " HELD: " + r["held"] if r["held"] else (" STALE" if r["stale"] else "")
        print(f"region {slug:<3} {r['name']:<38} page {r['asof'] or '?':<10} locations={n:<4}{flag}")
    if args.dry_run:
        print(f"\nDRY RUN: {len(ix['locs'])} locations, {ix['unplaced']} unplaced, {len(got['notices'])} notice(s); "
              "nothing written. Run again without --dry-run to refresh.")
        return 0
    size = (args.out / "index.json").stat().st_size
    print(f"\n{len(ix['locs'])} locations, {ix['unplaced']} unplaced, {len(got['notices'])} notice(s) "
          f"-> {args.out}/index.json ({size / 1024:.1f} KB); typed rows re-emitted -> {FD.FEED_DIR}")
    for w in R.vintage(FD.FEED_DIR, args.cache_dir):
        print(f"  WARNING {w['text']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
