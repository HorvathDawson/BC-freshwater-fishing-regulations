"""THE LIVE DFO SALMON FEED (T211 part 2 + part 4): the current DFO pages, typed, joined to the curated locations and
written as ONE small file the app reads, so a rule change reaches the app without a rebuild.

    python -m pipeline.regs.dfo_salmon.live                      # fetch, type, join, write <out>/index.json
    python -m pipeline.regs.dfo_salmon.live --offline            # the local dry run: the cached pages, no network
    python -m pipeline.regs.dfo_salmon.live queue [--regions 6]  # put the held new / reworded places in DFO mode

THE SPLIT (pipeline/docs/19 §1): the geography never moves, the rules churn. The release ships the GEOMETRY — which
sections each chain of DFO locations covers (`pipeline/deliver/salmon.py`, `chains[].l`). This feed ships the RULES as
answers per LOCATION and day: per species the verdict index (or -1: this location says nothing that day) and the gear
lines. The app re-composes each release chain from it, narrowest location first, exactly as `deliver.salmon.
chain_calendar` does (`compose` here is the reference, pinned against it), so the app still decides nothing.

    index.json    {format: "dfo-feed/1", at, regions: {slug: {name, url, asof, sha256, changed, checked, stale, held,
                   failures}}, verdicts, gears, locs: {location_id: {r, sp: [5 runs], g: runs}}, unplaced: n}
    unplaced.json the places the page lists that no curated location knows (part 4) — their rules HELD, never dropped
    <out>.notified.json   BESIDE the feed dir, never synced: the notices already sent (`gauges.feed.notify`)

Per region, every run (no credits, no secrets — safe on a cron):
  * fetch (validated, atomic; `fetch.fetch_region`), a changed page kept in `cache/dfo_salmon/history/` (the corpus);
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
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from pipeline.common.curated import GENERATED
from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import fetch as F

logger = logging.getLogger(__name__)

FORMAT = "dfo-feed/1"
OUT_DIR = Path(GENERATED.regs.dfo_salmon) / "feed"
#: two failed fetches in a row tell the owner (one is a blip)
FAIL_NOTIFY = 2
QUEUE_ACTION = "python -m pipeline.regs.dfo_salmon.live queue --regions {slug}"


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


def compose(lids: Sequence[str], locs: Mapping[str, dict], verdicts: Sequence[dict]) -> dict:
    """A release chain's year from the feed: per species and day the FIRST location (narrowest first, the chain's own
    order) with an answer, else `NOT_LISTED` (index 0); gear the first with any; the map status from the five. The
    reference the app's `withSalmonFeed` is held to — equal to `deliver.salmon.chain_calendar` over the same rules."""
    cals = [locs[l] for l in lids if l in locs]
    per_sp: List[List[int]] = [[] for _ in S.SPECIES]
    gear: List[int] = []
    mapc: List[int] = []
    for day in range(len(S.DAYS)):
        vs = []
        for k in range(len(S.SPECIES)):
            ix = next((v for v in (_at(c["sp"][k], day) for c in cals) if v >= 0), 0)
            per_sp[k].append(ix)
            vs.append(verdicts[ix])
        gear.append(next((g for g in (_at(c["g"], day) for c in cals) if g >= 0), -1))
        mapc.append(S.MAP_CODES.index(S.map_status(vs)))
    return {"sp": [S.runs_of(x) for x in per_sp], "g": S.runs_of(gear), "m": S.runs_of(mapc)}


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
    typed = FD.build({"region": slug, "date_modified": parsed.date_modified, "preamble": parsed.preamble,
                      "rules": [r.to_dict() for r in rules]})
    return typed, locs, structure


def _water_known(ef, water: str) -> bool:
    from pipeline.common.names import key
    k = key(water or "")
    return not k or any(key(w.name) == k or k in {key(a) for a in w.aliases} for w in ef.waters)


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
            rules.setdefault(hit.location_id, []).extend(loc["rules"])
            continue
        src = by_fp.get(fp)
        o = outcome.get(fp)
        water = src.water if src else ""
        kind = "new_water" if not _water_known(ef, water) else ("drift" if o and o.status == "drift" else "new")
        unplaced.append({"region": slug, "fingerprint": fp, "kind": kind, "water": water,
                         "scope": src.specific_area if src else "", "section": src.section if src else None,
                         "candidates": list(o.candidates) if o else [], "rows": loc["rows"], "rules": loc["rules"],
                         "page_date": typed.get("date_modified")})
    return rules, unplaced


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
    return {"key": f"dfo-{u['kind']}:{slug}:{u['fingerprint']}", "text": text}


# ── the run ──────────────────────────────────────────────────────────────────────────────────────────────────────────

def _read(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:                                           # noqa: BLE001 — missing or unreadable: none
        return default


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
        out[lid] = {"r": slug, "sp": sp, "g": g}
    return out


Fetcher = Callable[[str], Tuple[str, Optional[str]]]


def run(out: Path, *, fetcher: Fetcher, regions: Optional[Sequence[str]] = None, entries_dir: Optional[Path] = None,
        history_dir: Optional[Path] = None, now: Optional[str] = None, notify_url: Optional[str] = None,
        sender=None) -> dict:
    """One feed run. `fetcher(slug)` → (html, fetched_at) or raises. Writes `<out>/index.json` + `unplaced.json`,
    sends the due notices; returns `{index, unplaced, notices}`."""
    from pipeline.gauges.feed import notify
    from pipeline.regs.dfo_salmon import entries as E

    out = Path(out)
    at = now or _now()
    prev = _read(out / "index.json", {})
    prev_unplaced = {(u["region"], u["fingerprint"]): u for u in _read(out / "unplaced.json", [])}
    todo = [F.normalize_slug(s) for s in regions] if regions else [s for s in F.ALL_SLUGS if not F.PAGES[s].is_stub]
    verdicts, gears = S._Interner(), S._Interner()
    verdicts(S.NOT_LISTED)                                     # verdicts[0], as the release's
    locs: Dict[str, dict] = {}
    region_out: Dict[str, dict] = {}
    unplaced: List[dict] = []
    notices: List[dict] = []
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
               "stale": False, "held": None, "failures": 0}
        region_out[slug] = reg

        def hold(why: str, key: str) -> None:
            reg["held"] = why
            locs.update(_carry(prev, slug, verdicts, gears))
            unplaced.extend(u for (r, _), u in prev_unplaced.items() if r == slug)
            notices.append({"key": f"dfo-held:{slug}:{key}", "text": f"DFO Region {slug} held at the last feed: {why}"})

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
            if history_dir is not None:
                stamp = at.replace("-", "").replace(":", "").replace("T", "")[:14]
                Path(history_dir).mkdir(parents=True, exist_ok=True)
                (Path(history_dir) / f"region{slug}_{stamp}.html").write_text(html, encoding="utf-8")
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
        rules, held = join(slug, ef, typed, found, rep)
        moved = {o.location_id for o in rep.outcomes if o.status == "section_moved" and o.location_id}
        for lid, rs in sorted(rules.items()):
            if lid not in moved:
                locs[lid] = {"r": slug, **location_calendar(rs, verdicts, gears)}
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

    index = {"format": FORMAT, "at": at, "regions": {s: region_out[s] for s in F.ALL_SLUGS if s in region_out},
             "verdicts": verdicts.items, "gears": gears.items, "locs": dict(sorted(locs.items())),
             "unplaced": len(unplaced)}
    _write(out / "index.json", index)
    _write(out / "unplaced.json", sorted(unplaced, key=lambda u: (u["region"], u["fingerprint"])))
    sent = notify.send_due(notices, out, url=notify_url, sender=sender, key_of=notice_key, message_of=notice_text)
    return {"index": index, "unplaced": unplaced, "notices": notices, "sent": sent}


def live_fetcher(cache_dir: Path) -> Fetcher:
    def get(slug: str) -> Tuple[str, Optional[str]]:
        snap = F.fetch_region(slug, cache_dir=cache_dir)
        manifest = F._load_manifest(Path(cache_dir))
        manifest[slug] = snap.to_dict()
        F._write_manifest(Path(cache_dir), manifest)
        return F.load_cached(slug, cache_dir), snap.fetched_at
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
    for s in slugs:
        L.emit(s, scrape, cache_dir)
    FD.main(["--regions", *slugs, "--rules-dir", str(scrape / "rules"), "--out", str(scrape / "typed")])
    ed = ["--entries-dir", str(entries_dir)] if entries_dir else []
    E.main(["seed", "--regions", *slugs, "--scrape", str(scrape), *ed])
    E.main(["group", "--regions", *slugs, *ed])
    return slugs


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", nargs="?", default="run", choices=["run", "queue"])
    ap.add_argument("--regions", nargs="+", choices=F.ALL_SLUGS)
    ap.add_argument("--out", type=Path, default=OUT_DIR)
    ap.add_argument("--cache-dir", type=Path, default=F.DEFAULT_CACHE)
    ap.add_argument("--offline", action="store_true", help="the cached pages, no network (the local dry run)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.command == "queue":
        done = queue(args.out, args.regions, cache_dir=args.cache_dir)
        print(f"seeded and grouped regions {done or 'none (nothing unplaced)'}; review them in DFO mode, then "
              "commit the entry files")
        return 0

    fetcher = cached_fetcher(args.cache_dir) if args.offline else live_fetcher(args.cache_dir)
    got = run(args.out, fetcher=fetcher, regions=args.regions,
              history_dir=None if args.offline else Path(args.cache_dir) / "history")
    ix = got["index"]
    for slug, r in ix["regions"].items():
        n = sum(1 for c in ix["locs"].values() if c["r"] == slug)
        flag = " HELD: " + r["held"] if r["held"] else (" STALE" if r["stale"] else "")
        print(f"region {slug:<3} {r['name']:<38} page {r['asof'] or '?':<10} locations={n:<4}{flag}")
    size = (args.out / "index.json").stat().st_size
    print(f"\n{len(ix['locs'])} locations, {ix['unplaced']} unplaced, {len(got['notices'])} notice(s) "
          f"-> {args.out}/index.json ({size / 1024:.1f} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
