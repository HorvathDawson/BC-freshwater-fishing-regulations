"""churn — measure what actually moves between versions of a DFO region page.

The design question this exists to answer: the *places* a DFO rule binds to look
stable, and only the gear/limits/dates look seasonal. If that is true, the expensive
half of the work (resolving a reach description to geometry) can be curated once and
kept, while the rules are re-scraped freely. If it is false, curation is throwaway.

So measure it instead of assuming it. Snapshots come from the Wayback Machine's CDX
index, are re-parsed with the *current* parser (so any difference is the source
changing, not us), and are compared at three levels:

    waters    (section, name)                       — does the place list move?
    reaches   (section, name, scope)                — do the scopes on it move?
    rules     (section, name, scope, species, dates, limits)

Reach changes are further split, because they are not the same thing:

    DRIFT     the same reach on the same water, reworded ("Highway 37 Bridge" ->
              "Highway 37 bridge"). No geography changed; a naive text key breaks.
    ADDED     a scope with no near-match on that water in the previous version.
    REMOVED   likewise, gone. Often seasonal — it comes back next year.

CLI
---
    .venv/bin/python -m pipeline.regs.dfo_salmon.churn --regions 6 --from 2024
    .venv/bin/python -m pipeline.regs.dfo_salmon.churn --regions 1 2 6 --detail
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from pipeline.regs.dfo_salmon.fetch import (
    ALL_SLUGS,
    PAGES,
    _USER_AGENT,
    load_cached,
    normalize_slug,
)
from pipeline.regs.dfo_salmon.parse import parse_region
from pipeline.regs.dfo_salmon.untangle import Untangled, untangle

logger = logging.getLogger(__name__)

_CDX = "https://web.archive.org/cdx/search/cdx"
_WB = "https://web.archive.org/web/{ts}id_/https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region{slug}-eng.html"
_HISTORY = Path("cache/dfo_salmon/history")

#: Above this similarity, two scopes on the same water are the same reach reworded.
_DRIFT_THRESHOLD = 0.75


def _session():
    try:
        from curl_cffi import requests as r  # type: ignore

        return r, {"impersonate": "chrome124"}
    except ImportError:  # pragma: no cover
        import requests as r  # type: ignore

        return r, {}


def list_snapshots(slug: str, since: str = "2024", attempts: int = 3) -> List[str]:
    """Timestamps of distinct-content archives since `since`. CDX 504s often."""
    session, extra = _session()
    params = {
        "url": f"pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region{slug}-eng.html",
        "output": "text", "fl": "timestamp", "collapse": "digest", "from": since,
    }
    for attempt in range(1, attempts + 1):
        try:
            resp = session.get(_CDX, params=params, timeout=90,
                               headers={"User-Agent": _USER_AGENT}, **extra)
            lines = [l.strip() for l in resp.text.splitlines() if l.strip().isdigit()]
            if lines:
                return lines
        except Exception as exc:
            logger.debug("cdx attempt %d: %s", attempt, exc)
        if attempt < attempts:
            time.sleep(3.0 * attempt)
    logger.warning("region %s: CDX returned nothing (the index 504s under load)", slug)
    return []


def fetch_snapshot(slug: str, ts: str, cache_dir: Path = _HISTORY) -> Optional[str]:
    """One archived page, cached on disk. `id_` gets the original, un-rewritten HTML."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    dest = cache_dir / f"region{slug}_{ts}.html"
    if dest.exists():
        return dest.read_text(encoding="utf-8", errors="replace")
    session, extra = _session()
    try:
        resp = session.get(_WB.format(ts=ts, slug=slug), timeout=120,
                           headers={"User-Agent": _USER_AGENT}, **extra)
        if resp.status_code != 200 or "<table" not in resp.text:
            logger.warning("region %s @ %s: HTTP %s, unusable", slug, ts, resp.status_code)
            return None
        dest.write_text(resp.text, encoding="utf-8")
        return resp.text
    except Exception as exc:
        logger.warning("region %s @ %s: %s", slug, ts, exc)
        return None


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


@dataclass
class Level:
    added: int = 0
    removed: int = 0
    drifted: int = 0


@dataclass
class RegionChurn:
    region: str
    versions: List[str] = field(default_factory=list)
    waters: Level = field(default_factory=Level)
    reaches: Level = field(default_factory=Level)
    rules: Level = field(default_factory=Level)
    stable_waters: int = 0
    detail: List[str] = field(default_factory=list)


def _keys(u: Untangled) -> Tuple[Set, Set, Set]:
    waters = {(w.section, w.name) for w in u.waters}
    reaches = {(w.section, w.name, r.scope) for w in u.waters for r in w.reaches}
    rules = {(w.section, w.name, r.scope, ru.species, ru.dates, ru.limits_gear)
             for w in u.waters for r in w.reaches for ru in r.rules}
    for d in u.defaults:
        for ru in d.rules:
            rules.add((d.key, "<default>", d.scope, ru.species, ru.dates, ru.limits_gear))
    return waters, reaches, rules


def compare_reaches(prev: Set, cur: Set) -> Tuple[List, List, List]:
    """Split reach changes into (drifted, added, removed).

    A scope is "drifted" when the same water lost a scope that is textually close to
    the one it gained — the same place, reworded.
    """
    added, removed = sorted(cur - prev), sorted(prev - cur)
    drifted, real_added, matched_removed = [], [], set()
    for a in added:
        cands = [r for r in removed if r[:2] == a[:2] and r not in matched_removed]
        best, ratio = None, 0.0
        for c in cands:
            score = difflib.SequenceMatcher(None, c[2], a[2]).ratio()
            if score > ratio:
                best, ratio = c, score
        if best is not None and ratio >= _DRIFT_THRESHOLD:
            drifted.append((best, a, ratio))
            matched_removed.add(best)
        else:
            real_added.append(a)
    return drifted, real_added, [r for r in removed if r not in matched_removed]


def measure(slug: str, versions: List[Tuple[str, Untangled]], detail: bool = False) -> RegionChurn:
    out = RegionChurn(region=slug, versions=[v[0] or "?" for v in versions])
    prev = None
    all_waters: List[Set] = []
    for dm, u in versions:
        cur = _keys(u)
        all_waters.append(cur[0])
        if prev:
            out.waters.added += len(cur[0] - prev[0])
            out.waters.removed += len(prev[0] - cur[0])
            drifted, added, removed = compare_reaches(prev[1], cur[1])
            out.reaches.drifted += len(drifted)
            out.reaches.added += len(added)
            out.reaches.removed += len(removed)
            out.rules.added += len(cur[2] - prev[2])
            out.rules.removed += len(prev[2] - cur[2])
            if detail:
                for was, now, ratio in drifted:
                    out.detail.append(f"[{dm}] DRIFT {now[1]} ({ratio:.0%})")
                    out.detail.append(f"          was: {was[2][:95]}")
                    out.detail.append(f"          now: {now[2][:95]}")
                for a in added:
                    out.detail.append(f"[{dm}] ADDED   {a[1]}: {a[2][:95]}")
                for r in removed:
                    out.detail.append(f"[{dm}] REMOVED {r[1]}: {r[2][:95]}")
        prev = cur
    if all_waters:
        out.stable_waters = len(set.intersection(*all_waters))
    return out


def load_versions(slug: str, since: str, limit: int,
                  include_live: bool = True) -> List[Tuple[str, Untangled]]:
    """Archived versions (oldest first), then the current snapshot if cached."""
    stamps = list_snapshots(slug, since)
    if limit and len(stamps) > limit:
        step = len(stamps) / limit
        stamps = [stamps[int(i * step)] for i in range(limit)]

    versions: List[Tuple[str, Untangled]] = []
    for ts in stamps:
        html = fetch_snapshot(slug, ts)
        if not html:
            continue
        try:
            u = untangle(parse_region(html, slug))
        except Exception as exc:
            logger.warning("region %s @ %s: parse failed: %s", slug, ts, exc)
            continue
        if u.waters or u.defaults:
            versions.append((u.date_modified or ts[:8], u))

    if include_live:
        try:
            u = untangle(parse_region(load_cached(slug), slug))
            versions.append((u.date_modified or "live", u))
        except FileNotFoundError:
            pass

    # One row per distinct dateModified; the archive samples the same version twice.
    seen, deduped = set(), []
    for dm, u in versions:
        if dm not in seen:
            seen.add(dm)
            deduped.append((dm, u))
    return deduped


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--regions", nargs="+", choices=ALL_SLUGS, default=["6"])
    ap.add_argument("--from", dest="since", default="2024", help="earliest archive year")
    ap.add_argument("--limit", type=int, default=8, help="max archived versions per region")
    ap.add_argument("--detail", action="store_true", help="list every reach change")
    ap.add_argument("--json", type=Path)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    results = []
    for slug in args.regions:
        slug = normalize_slug(slug)
        if PAGES[slug].is_stub:
            continue
        versions = load_versions(slug, args.since, args.limit)
        if len(versions) < 2:
            print(f"region {slug}: only {len(versions)} version(s) available — nothing to compare")
            continue
        c = measure(slug, versions, detail=args.detail)
        results.append(c)

        first, last = versions[0][1], versions[-1][1]
        print(f"\n=== region {slug} — {len(versions)} versions, {c.versions[0]} -> {c.versions[-1]} ===")
        print(f"  waters   {len(first.waters):>4} -> {len(last.waters):<4} "
              f"+{c.waters.added} -{c.waters.removed}   "
              f"({c.stable_waters} present in EVERY version)")
        nr_first = sum(len(w.reaches) for w in first.waters)
        nr_last = sum(len(w.reaches) for w in last.waters)
        print(f"  reaches  {nr_first:>4} -> {nr_last:<4} "
              f"+{c.reaches.added} -{c.reaches.removed} ~{c.reaches.drifted} reworded")
        print(f"  rules    {'':>4}    {'':<4} +{c.rules.added} -{c.rules.removed}")
        for line in c.detail:
            print("    " + line)

    if args.json and results:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps([{
            "region": c.region, "versions": c.versions, "stable_waters": c.stable_waters,
            "waters": vars(c.waters), "reaches": vars(c.reaches), "rules": vars(c.rules),
        } for c in results], indent=2) + "\n", encoding="utf-8")
        print(f"\n-> {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
