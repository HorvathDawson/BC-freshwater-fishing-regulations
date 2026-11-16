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

THE MIRROR, ON A WORKER DEPLOY (T757 item 7): a worker can go backwards after the upload (a rollback, an older
checkout deployed). `--worker-deploy` refuses deploying a feeds worker whose code does not serve `dfo.pieces >=
PIECES_SHAPE` (its `DFO_SHAPE`, read by `deploy/lib/worker-shape.mjs`) while the environment's LIVE release binds a
piece; an unreadable live release or worker shape refuses too. `deploy/staging/feeds.sh` and `deploy/prod/feeds.sh`
call it before `wrangler deploy`:

    python -m pipeline.deliver.release.pieces_gate --worker-deploy --live-manifest <the bucket's release/manifest.json> \
        --worker-shape "$(node deploy/lib/worker-shape.mjs)"
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


def worker_problems(payload: Optional[dict], shape: object, release: str = "the live release") -> list[str]:
    """(T757 item 7) why a feeds worker whose code serves `shape` (its `DFO_SHAPE`; None: it exports none) may not be
    deployed beside the live release `payload`; [] when it may. `payload` None: the live release could not be read;
    `shape` the string "unread": the worker's code could not be read. Neither is ever a pass."""
    if payload is None:
        return [f"{release} could not be read, so whether it binds closed-area pieces (T573) is not known: a worker "
                "that does not emit them would show those closed areas open. Make the live release readable, then deploy again"]
    if shape == "unread":
        return ["the feeds worker's code could not be read (deploy/lib/worker-shape.mjs on app/workers/feeds/src/status.ts), "
                "so what feed shape it serves is not known. Fix that, then deploy again"]
    got = bound_pieces(payload)
    if not got:
        return []
    pieces = shape.get("pieces") if isinstance(shape, dict) else None
    if isinstance(pieces, int) and pieces >= PIECES_SHAPE:
        return []
    return [f"{release} binds {len(got)} closed-area piece(s) (T573, e.g. {got[0]}), and this feeds worker's code does "
            f"not emit them (DFO_SHAPE.pieces is {pieces!r}, needs >= {PIECES_SHAPE}): the app would read the release's own "
            "salmon file instead of the live feed. Deploy the worker that emits closed-area pieces (T757)"]


def read_live(manifest: Path, releases: Path) -> tuple[Optional[dict], str]:
    """the live release's salmon payload, from the bucket's `release/manifest.json` (a local copy) and the release
    folder it names under `releases`; (None, its name or why) when either cannot be read"""
    from pipeline.deliver import salmon as S
    try:
        rel = json.loads(manifest.read_text(encoding="utf-8"))["data_release"]
        if not isinstance(rel, str) or not rel or "/" in rel or rel.startswith("."):
            raise ValueError(f"data_release is {rel!r}")
    except Exception as e:                               # noqa: BLE001 — unreadable is an answer: refused
        return None, f"the live release manifest {manifest} ({type(e).__name__}: {e})"
    try:
        return S.read_release(releases / rel), f"the live release {rel}"
    except (Exception, S.SalmonError) as e:              # noqa: BLE001 — SalmonError is a SystemExit
        return None, f"the live release {rel} ({releases / rel}: {e})"


def main(argv=None) -> int:
    from pipeline.deliver import salmon as S
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("manifest", type=Path, nargs="?")
    ap.add_argument("--feeds-url", default=os.environ.get("FEEDS_URL") or None)
    ap.add_argument("--worker-deploy", action="store_true", help="the worker-deploy mirror (T757 item 7)")
    ap.add_argument("--live-manifest", type=Path, help="--worker-deploy: the bucket's live release/manifest.json")
    ap.add_argument("--releases", type=Path, default=Path("data/release"), help="--worker-deploy: the release folders")
    ap.add_argument("--worker-shape", default="", help="--worker-deploy: deploy/lib/worker-shape.mjs's JSON line")
    a = ap.parse_args(argv)
    if a.worker_deploy:
        if a.live_manifest is None:
            payload, name = None, "the live release (no --live-manifest given)"
        else:
            payload, name = read_live(a.live_manifest, a.releases)
        try:
            shape = json.loads(a.worker_shape) if a.worker_shape.strip() else "unread"
        except ValueError:
            shape = "unread"
        bad = worker_problems(payload, shape, name)
        for p in bad:
            print(f"feeds deploy: refused — {p}", file=sys.stderr)
        return 1 if bad else 0
    if a.manifest is None:
        ap.error("the release manifest is required")
    bad = problems(S.read_release(a.manifest.parent), a.feeds_url)
    for p in bad:
        print(f"upload: refused — {p}", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
