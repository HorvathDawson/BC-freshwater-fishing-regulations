"""THE CLOSED-AREA PIECES GATE ON A RELEASE (T573 / T757, 2026-11-15).

    FEEDS_URL=<the worker reading the bucket> python -m pipeline.deliver.release.pieces_gate data/release/<r>/manifest.json

A release whose salmon file BINDS a closed-area piece (T573 entry B: `<water_id>#closed:<key>` in a chain's `l`,
marked `piece: 1, bound: 1` in the payload's `p`) is REFUSED for an environment whose feeds worker does not emit pieces: the app
composes each chain from the live feed (`live.compose` / core `withSalmonFeed`) and skips a location the feed lacks,
so the place answers on the piece's sections — the Thompson's year-round closed areas would read OPEN once chinook
opens. The worker says what it emits on `/status.json`: `dfo.pieces` (the piece shape, `PIECES_SHAPE`), served by the
T757 worker. A release with no bound piece needs no check. An unreachable worker, one from before the field, or no
FEEDS_URL at all refuse a release with bound pieces (never assumed). Both upload scripts call this (staging and
production, whoever runs them).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Callable, Optional

#: the piece shape a worker must serve as `/status.json` `dfo.pieces` (>=): 1 = one `locs["<water_id>#closed:<key>"]`
#: per closed area today's row names (`regs.dfo_salmon.closures.live_piece_rules`)
PIECES_SHAPE = 1


def bound_pieces(payload: dict) -> list[str]:
    """the closed-area pieces a salmon payload BINDS: its `p` places marked `piece` and `bound` (a carried piece is in
    `p` through its own unmapped chain, and binds nothing)"""
    return sorted(lid for lid, v in (payload.get("p") or {}).items() if v.get("piece") and v.get("bound"))


def fetch_status(url: str, timeout: float = 20.0) -> Optional[dict]:
    """`<url>/status.json` as JSON, or None when the worker gives no answer that reads (never raises)."""
    import urllib.request
    try:
        with urllib.request.urlopen(f"{url.rstrip('/')}/status.json", timeout=timeout) as r:   # noqa: S310
            return json.loads(r.read().decode("utf-8"))
    except Exception:                                    # noqa: BLE001 — unreachable is an answer: "cannot be asked"
        return None


def problems(payload: dict, feeds_url: Optional[str],
             status: Callable[[str], Optional[dict]] = fetch_status) -> list[str]:
    """why this release may not go to the environment `feeds_url` serves; [] when it may"""
    got = bound_pieces(payload)
    if not got:
        return []
    head = (f"the release binds {len(got)} closed-area piece(s) (T573, e.g. {got[0]}), and the app reads them from the "
            "live feed")
    fix = ("deploy the feeds worker that emits closed-area pieces (T757: `/status.json` `dfo.pieces` >= "
           f"{PIECES_SHAPE}) to this environment first, then upload again")
    if not feeds_url:
        return [f"{head}; no feeds worker was named (FEEDS_URL): {fix}"]
    s = status(feeds_url)
    if s is None:
        return [f"{head}; the feeds worker {feeds_url} cannot be asked (no readable /status.json): {fix}"]
    shape = (s.get("dfo") or {}).get("pieces") if isinstance(s.get("dfo"), dict) else None
    if not isinstance(shape, int) or shape < PIECES_SHAPE:
        return [f"{head}; the feeds worker {feeds_url} does not emit them (/status.json dfo.pieces is "
                f"{shape!r}): its chains would skip the pieces and answer by the place (open where DFO says no "
                f"fishing). {fix}"]
    return []


def main(argv=None) -> int:
    from pipeline.deliver import salmon as S
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("manifest", type=Path)
    ap.add_argument("--feeds-url", default=os.environ.get("FEEDS_URL") or None)
    a = ap.parse_args(argv)
    bad = problems(S.read_release(a.manifest.parent), a.feeds_url)
    for p in bad:
        print(f"upload: refused — {p}", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
