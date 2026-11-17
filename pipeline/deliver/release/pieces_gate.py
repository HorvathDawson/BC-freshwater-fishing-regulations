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
piece. A worker that DOES serve the shape is safe beside any release, so it passes WITHOUT the live release being read
(2026-11-16: read first, a first production deploy was refused for having no live release yet, while the upload gate
above refused the release until that worker was up: each waited on the other). The live release is read, and
unreadable = refused, only for a worker that lacks the shape; an unreadable worker shape is refused always.
`deploy/staging/feeds.sh` and `deploy/prod/feeds.sh` call it before `wrangler deploy`:

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


def status_request(url: str):
    """the request for `<url>/status.json`. It NAMES ITS CLIENT: the worker's host (Cloudflare) answers Python's
    default `Python-urllib` user agent with 403 "error code: 1010", which `fetch_status` reads as "cannot be asked"
    — on 2026-11-16 that refused the staging upload of r-46e4355a7277 while the worker was up and emitting pieces
    (`site_gate.fetch` names its client for the same reason)."""
    import urllib.request
    return urllib.request.Request(f"{url.rstrip('/')}/status.json",
                                  headers={"User-Agent": "bc-fishing-pieces-gate", "Cache-Control": "no-cache"})


def fetch_status(url: str, timeout: float = 20.0) -> Optional[dict]:
    """`<url>/status.json` as JSON, or None when the worker gives no answer that reads (never raises)."""
    import urllib.request
    try:
        with urllib.request.urlopen(status_request(url), timeout=timeout) as r:   # noqa: S310
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


#: (T757) how a deploy names where the live release folder is (said in every refusal about finding it)
NAME_THE_RELEASES = ("A deploy from another checkout or a scratch worktree names the main checkout's folder: RELEASES_DIR=/path/to/main/checkout/data/release (deploy/staging/feeds.sh, deploy/prod/feeds.sh) or --releases <path> (the gate)")


def serves_pieces(shape: object) -> bool:
    """does a worker whose code serves `shape` (its `DFO_SHAPE`) emit closed-area pieces: `pieces` an int >= PIECES_SHAPE"""
    pieces = shape.get("pieces") if isinstance(shape, dict) else None
    return isinstance(pieces, int) and not isinstance(pieces, bool) and pieces >= PIECES_SHAPE


def worker_problems(payload: Optional[dict], shape: object, release: str = "the live release",
                    unread: Optional[str] = None) -> list[str]:
    """(T757 item 7) why a feeds worker whose code serves `shape` (its `DFO_SHAPE`; None: it exports none) may not be
    deployed beside the live release `payload`; [] when it may. `shape` the string "unread": the worker's code could
    not be read, never a pass. A worker that serves the pieces shape passes whatever the release (`serves_pieces`): it
    cannot be the worker the gate exists to stop. Otherwise `payload` None (the live release could not be read) is
    never a pass. `unread`: the whole refusal for an unread release when the reader knows why (`read_live`)."""
    if shape == "unread":
        return ["the feeds worker's feed shape (DFO_SHAPE) could not be evaluated from app/workers/feeds/src/status.ts "
                "(deploy/lib/worker-shape.mjs), so whether it emits closed-area pieces is not known. Fix that, then deploy again"]
    if serves_pieces(shape):
        return []
    if payload is None:
        return [unread] if unread else [f"{release} could not be read, so whether it binds closed-area pieces (T573) is not known: a worker "
                "that does not emit them would show those closed areas open. Make the live release readable, then deploy again"]
    got = bound_pieces(payload)
    if not got:
        return []
    pieces = shape.get("pieces") if isinstance(shape, dict) else None
    return [f"{release} binds {len(got)} closed-area piece(s) (T573, e.g. {got[0]}), and this feeds worker's code does "
            f"not emit them (DFO_SHAPE.pieces is {pieces!r}, needs >= {PIECES_SHAPE}): the app would read the release's own "
            "salmon file instead of the live feed. Deploy the worker that emits closed-area pieces (T757)"]


def read_live(manifest: Path, releases: Path) -> tuple[Optional[dict], str, Optional[str]]:
    """the live release's salmon payload, from the bucket's `release/manifest.json` (a local copy) and the release
    folder it names under `releases`: (payload, its name, None), or (None, its name, the refusal in words) when either
    cannot be read. A `releases` folder that is not there is refused as such, never read from the default instead."""
    from pipeline.deliver import salmon as S
    unknown = "so whether it binds closed-area pieces (T573) is not known"
    if not releases.is_dir():
        return None, "the live release", (f"the release folder {releases} does not exist, so the live release cannot be "
                                          f"read and {unknown.removeprefix('so ')}. {NAME_THE_RELEASES}")
    try:
        rel = json.loads(manifest.read_text(encoding="utf-8"))["data_release"]
        if not isinstance(rel, str) or not rel or "/" in rel or rel.startswith("."):
            raise ValueError(f"data_release is {rel!r}")
    except Exception as e:                               # noqa: BLE001 — unreadable is an answer: refused
        return None, f"the live release manifest {manifest} ({type(e).__name__}: {e})", None
    name = f"the live release {rel}"
    if not (releases / rel / "manifest.json").is_file():
        return None, name, (f"{name} was looked for under {releases} and is not there ({releases / rel}/manifest.json), "
                            f"{unknown}. {NAME_THE_RELEASES}")
    try:
        return S.read_release(releases / rel), name, None
    except (Exception, S.SalmonError) as e:              # noqa: BLE001 — SalmonError is a SystemExit
        return None, name, (f"{name} under {releases} could not be read ({e}), {unknown}. "
                            "Make it readable, then deploy again")


def main(argv=None) -> int:
    from pipeline.deliver import salmon as S
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("manifest", type=Path, nargs="?")
    ap.add_argument("--feeds-url", default=os.environ.get("FEEDS_URL") or None)
    ap.add_argument("--worker-deploy", action="store_true", help="the worker-deploy mirror (T757 item 7)")
    ap.add_argument("--live-manifest", type=Path, help="--worker-deploy: the bucket's live release/manifest.json")
    ap.add_argument("--releases", type=Path, default=Path("data/release"),
                    help="--worker-deploy: the release folders (the scripts pass RELEASES_DIR, default data/release)")
    ap.add_argument("--worker-shape", default="", help="--worker-deploy: deploy/lib/worker-shape.mjs's JSON line")
    a = ap.parse_args(argv)
    if a.worker_deploy:
        try:
            shape = json.loads(a.worker_shape) if a.worker_shape.strip() else "unread"
        except ValueError:
            shape = "unread"
        if shape == "unread" or serves_pieces(shape):
            # decided by the worker alone: the live release is not read (there may be none yet: a first deploy)
            payload, name, unread = None, "the live release (not read)", None
        elif a.live_manifest is None:
            payload, name, unread = None, "the live release (no --live-manifest given)", None
        else:
            payload, name, unread = read_live(a.live_manifest, a.releases)
        bad = worker_problems(payload, shape, name, unread)
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
