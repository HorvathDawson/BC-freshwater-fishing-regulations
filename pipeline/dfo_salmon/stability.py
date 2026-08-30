"""stability — prove the DFO fetch actually holds up before anything depends on it.

Answers four questions with measurements rather than assertions:

1. **Reliability** — does every region return a valid page, every round?
2. **Determinism** — is the body byte-identical across rounds? If a region's sha256
   moves between two rounds seconds apart, the page is dynamic (rotating tokens, a
   timestamp) and content-hash change detection is worthless for it.
3. **Rate limiting** — does hammering with no throttle degrade the success rate or
   the latency? This is the thing that gets a scraper blocked.
4. **Is TLS impersonation load-bearing?** `--compare-clients` fetches every region
   with plain `requests` (no browser fingerprint) alongside curl_cffi. If plain
   works today, impersonation is cheap insurance; if it fails, it is mandatory and
   the fallback in `fetch.py` must never be relied on.

CLI
---
    .venv/bin/python -m pipeline.dfo_salmon.stability                    # 3 polite rounds
    .venv/bin/python -m pipeline.dfo_salmon.stability --rounds 5 --burst
    .venv/bin/python -m pipeline.dfo_salmon.stability --compare-clients
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

from pipeline.dfo_salmon.fetch import (
    ALL_SLUGS,
    PAGES,
    REGIONS,
    FetchError,
    _USER_AGENT,
    _validate,
    fetch_region,
)

logger = logging.getLogger(__name__)


def _pct(values: List[float], q: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    idx = min(len(s) - 1, int(round(q * (len(s) - 1))))
    return s[idx]


def run_rounds(
    regions: List[str],
    rounds: int,
    throttle: float,
) -> Dict[int, dict]:
    """Fetch every region `rounds` times; collect status, latency and body hash."""
    obs: Dict[str, dict] = {
        r: {"hashes": [], "latencies": [], "ok": 0, "fail": 0, "errors": [], "attempts": []}
        for r in regions
    }
    for rnd in range(1, rounds + 1):
        logger.info("--- round %d/%d ---", rnd, rounds)
        for region in regions:
            if throttle:
                time.sleep(throttle)
            t0 = time.monotonic()
            try:
                # write=False: a stability probe must never touch the real snapshot.
                snap = fetch_region(region, write=False, max_attempts=2)
                obs[region]["ok"] += 1
                obs[region]["hashes"].append(snap.sha256)
                obs[region]["latencies"].append(snap.elapsed_s)
                obs[region]["attempts"].append(snap.attempts)
            except FetchError as exc:
                obs[region]["fail"] += 1
                obs[region]["errors"].append(str(exc))
                obs[region]["latencies"].append(time.monotonic() - t0)
    return obs


def compare_clients(regions: List[str]) -> List[dict]:
    """Fetch each region with plain `requests` to see if impersonation is required."""
    try:
        import requests  # type: ignore
    except ImportError:
        logger.warning("plain `requests` not installed; skipping client comparison")
        return []

    out = []
    for region in regions:
        url = f"https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region{region}-eng.html"
        row = {"region": region, "status": None, "valid": False, "error": None, "elapsed_s": None}
        t0 = time.monotonic()
        try:
            resp = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=30)
            row["status"] = resp.status_code
            row["valid"] = resp.status_code == 200 and _validate(resp.text) is None
        except Exception as exc:
            row["error"] = f"{type(exc).__name__}: {exc}"
        row["elapsed_s"] = round(time.monotonic() - t0, 3)
        out.append(row)
        time.sleep(0.5)
    return out


def report(obs: Dict[str, dict], rounds: int) -> dict:
    """Print a per-region table and return the machine-readable verdict."""
    print(f"\n{'reg':<5}{'name':<38}{'ok':<7}{'stable':<8}{'p50 s':<8}{'p95 s':<8}{'max s':<8}retries")
    print("-" * 88)
    verdict = {"rounds": rounds, "regions": {}, "all_reliable": True, "all_deterministic": True}

    for region, o in sorted(obs.items(), key=lambda kv: ALL_SLUGS.index(kv[0])):
        total = o["ok"] + o["fail"]
        uniq = set(o["hashes"])
        deterministic = len(uniq) <= 1
        reliable = o["fail"] == 0 and o["ok"] == rounds
        retries = sum(a - 1 for a in o["attempts"])
        lat = o["latencies"]
        print(
            f"{region:<5}{REGIONS[region]:<38}{o['ok']}/{total:<5}"
            f"{('yes' if deterministic else f'NO ({len(uniq)})'):<8}"
            f"{_pct(lat, 0.5):<8.2f}{_pct(lat, 0.95):<8.2f}{max(lat) if lat else 0:<8.2f}{retries}"
        )
        verdict["regions"][region] = {
            "name": REGIONS[region],
            "ok": o["ok"], "fail": o["fail"],
            "reliable": reliable,
            "deterministic": deterministic,
            "distinct_hashes": len(uniq),
            "sha256": sorted(uniq)[0] if uniq else None,
            "p50_s": round(_pct(lat, 0.5), 3),
            "p95_s": round(_pct(lat, 0.95), 3),
            "max_s": round(max(lat), 3) if lat else None,
            "retries": retries,
            "errors": o["errors"],
        }
        verdict["all_reliable"] &= reliable
        verdict["all_deterministic"] &= deterministic

    all_lat = [x for o in obs.values() for x in o["latencies"]]
    tot_ok = sum(o["ok"] for o in obs.values())
    tot = sum(o["ok"] + o["fail"] for o in obs.values())
    verdict["requests"] = tot
    verdict["success_rate"] = round(tot_ok / tot, 4) if tot else 0.0
    verdict["mean_latency_s"] = round(statistics.fmean(all_lat), 3) if all_lat else None

    print("-" * 88)
    print(f"{tot_ok}/{tot} requests OK ({verdict['success_rate']:.1%}), "
          f"mean {verdict['mean_latency_s']:.2f}s")
    for r, o in sorted(obs.items(), key=lambda kv: ALL_SLUGS.index(kv[0])):
        for e in o["errors"]:
            print(f"  region {r} error: {e}")
    return verdict


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--regions", nargs="+", choices=ALL_SLUGS, help="default: all real pages")
    ap.add_argument("--throttle", type=float, default=1.5, help="seconds between requests")
    ap.add_argument("--burst", action="store_true",
                    help="also run a zero-throttle round to probe for rate limiting")
    ap.add_argument("--compare-clients", action="store_true",
                    help="also fetch with plain requests (no TLS impersonation)")
    ap.add_argument("--json", type=Path, help="write the verdict to this path")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    regions = args.regions or [s for s in ALL_SLUGS if not PAGES[s].is_stub]

    print(f"== polite: {args.rounds} rounds x {len(regions)} regions, {args.throttle}s throttle ==")
    verdict = report(run_rounds(regions, args.rounds, args.throttle), args.rounds)

    if args.burst:
        print(f"\n== burst: 1 round x {len(regions)} regions, NO throttle ==")
        verdict["burst"] = report(run_rounds(regions, 1, 0.0), 1)
        polite, burst = verdict["success_rate"], verdict["burst"]["success_rate"]
        verdict["rate_limited"] = burst < polite
        print(f"\nrate limiting: {'DETECTED' if verdict['rate_limited'] else 'none observed'} "
              f"(polite {polite:.0%} vs burst {burst:.0%})")

    if args.compare_clients:
        print("\n== plain requests, no TLS impersonation ==")
        rows = compare_clients(regions)
        verdict["plain_requests"] = rows
        for r in rows:
            mark = "ok" if r["valid"] else "FAIL"
            print(f"  region {r['region']}: {mark} status={r['status']} "
                  f"{r['elapsed_s']}s {r['error'] or ''}")
        n_ok = sum(1 for r in rows if r["valid"])
        verdict["impersonation_required"] = n_ok < len(rows)
        print(f"  -> impersonation {'REQUIRED' if n_ok < len(rows) else 'not required today'} "
              f"({n_ok}/{len(rows)} plain fetches valid)")

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(verdict, indent=2) + "\n", encoding="utf-8")
        print(f"\nverdict -> {args.json}")

    ok = verdict["all_reliable"] and verdict["all_deterministic"]
    print(f"\nVERDICT: {'STABLE' if ok else 'UNSTABLE — see above'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
