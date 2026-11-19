"""PRUNE THE PRODUCTION BUCKET (T865, owner 2026-11-18: "That clean up should be automatic"; R2's free tier is 10 GB).
`deploy/prod/upload.sh` runs `releases` after every pointer move; `deploy/prod/drop-v1.sh` runs `v1`, once, at the switch.

    python -m pipeline.deliver.release.prod_prune releases --remote r2:<bucket> [--dry-run --pending <manifest.json>]
    python -m pipeline.deliver.release.prod_prune v1 --remote r2:<bucket> [--site https://canifishthis.ca/] [--dry-run]

THE BUCKET'S LAYOUT (who writes what; `deploy/prod/upload.sh`, `app/workers/feeds`, `.github/workflows/feeds-*.yml`):

    release/r-<id>/**          data files + synopsis of one release           PRUNED (release-scoped)
    release/tiles/<id>/**      atlas pmtiles, mask, map-assets-1/ of one map  PRUNED (release-scoped)
    release/basemap/<id>/**    the ground of one release                      PRUNED (release-scoped)
    release/manifest.json, release/manifest.previous.json   the two pointers  never
    map-assets/v1/**, sql-wasm*.wasm   fixed keys, overwritten each promote   never
    runtiming/**               feeds-runtiming.yml's feed (its own index.json pointer; prod copies staging's)   never
    feeds/**, private/**       the feeds worker's (seeds go up with --ignore-existing)                          never
    app/android/play.json      build-apk.sh                                                                     never
    feedback/**                the v1 worker's stored user reports: content, not layout (kept; the owner decides) never
    everything else            not ours to judge                                                                never

`releases`: every object under the three release-scoped prefixes whose folder neither `release/manifest.json` (live) nor
`release/manifest.previous.json` (what deploy/prod/rollback.sh goes back to) names is deleted. An ALLOWLIST: a key
outside those prefixes is never a candidate, whatever it is. A kept manifest's folders are kept whole, and so is any
folder whose newest object went up less than RECENT ago (server time, `--use-server-modtime`): a web tab opened on a
release promoted earlier the same day still reads its files (app boot.ts). Refused, nothing deleted, exit 1: a
pointer that cannot be read or parsed, a file a kept manifest names that is not in the bucket, both pointers naming
the same release (a rollback would have nowhere to go). No previous pointer yet (the first promote): nothing pruned,
said, exit 0. `--dry-run --pending <manifest>` (upload.sh's dry run, whose pointer was not written): the pending
manifest stands in for the live one and the bucket's current pointer for the previous one, as the pointer step would
leave them; the pending release's files are not checked (nothing was uploaded).

`v1`: the old Vite site's data (the v1 layout, read from `main`: scripts/seed-r2.sh uploads output/pipeline/deploy whole,
pipeline/enrichment/builder.py + tile_exporter write it, the three cron workflows write cron/**, the old webapp reads
it). Never automatic: a site rollback to the Vite build would load with no data. `deploy/prod/drop-v1.sh` runs it at the
switch, on the owner's word, and it still fails closed: deleted ONLY when `--site` serves the new web export (its
index.html loads `/_expo/static/js/web/index-<hash>.js`, the Expo export deploy/prod/site.sh greps, and lacks the old
build's `<!-- build: webapp UI fixes` comment). Without the marker: v1 kept, the reason printed, exit 1.

Both print each key once it IS deleted (or that a dry run would delete) and the bytes freed; a delete that fails part
way says how many objects and bytes went before it stopped.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, NamedTuple, Optional, Protocol, Set, Tuple

LIVE = "release/manifest.json"
PREVIOUS = "release/manifest.previous.json"
#: the release-scoped prefixes, and the one folder (unit) of each that a manifest keeps or drops whole
RELEASE_UNIT = re.compile(r"^release/(?:r-[^/]+|tiles/[^/]+|basemap/[^/]+)/")
RELEASE_INCLUDES = ("/release/r-*/**", "/release/tiles/**", "/release/basemap/**")
#: a folder uploaded more recently than this is kept even when no pointer names it (a web tab open across two promotes)
RECENT = timedelta(hours=24)

#: the v1 layout, from `main` (git show main:<path>): root files and whole folders
V1_FILES = (
    # pipeline/enrichment/builder.py + pipeline/deploy/r2_sharder.py (deploy root) + scripts/seed-r2.sh
    "tier0.json", "admin_visibility.json", "match_table.json", "poly_reaches.json", "data_version.json",
    # pipeline/tiles/tile_exporter.py
    "layer_manifest.json",
    # the webapp's older reads (waterbodyDataService.ts / RegulationsService history; 632a3526a dropped the root copies)
    "in_season.json", "stocking.json", "waterbody_data.json", "regulation_index.json", "regulations.json",
    # the map sources the webapp's Map.tsx named over its history (bc.pmtiles = the basemap, builder.py)
    "bc.pmtiles", "freshwater_atlas.pmtiles", "regulations_merged.pmtiles", "regulations_merged_tiered.pmtiles",
    "regulations_individual.pmtiles", "non_streams.pmtiles", "streams_hi.pmtiles", "streams_low.pmtiles",
    "waterbodies_bc.pmtiles",
)
V1_DIRS = (
    "cron",        # update-hydro/-in-season/-stocking.yml + builder.py: cron/hydro/{stations.json, gauges.geojson,
                   # gauge_matches.json, recent/, history/, climatology/, ...}, cron/in-season/, cron/stocking/
    "shards",      # r2_sharder.py: shards/v<N>/{fids,reaches,polys}/ (the v1 worker's /api/resolve)
    "mobile",      # mobile_sharder.py: mobile/v<N>/regulations.sqlite(.gz)
    "row_images",  # builder.py
    "bathymetry",  # builder.py
)
V1_INCLUDES = tuple(f"/{f}" for f in V1_FILES) + tuple(f"/{d}/**" for d in V1_DIRS)

NEW_SITE = re.compile(r"/_expo/static/js/web/index-[0-9a-f]+\.js")
OLD_SITE = "<!-- build: webapp UI fixes"
SITE = "https://canifishthis.ca/"


class Refused(Exception):
    """A prune that must not go ahead; the message says why."""


class PartialDelete(Exception):
    """A delete that stopped part way: `deleted` are the keys that did go."""

    def __init__(self, deleted: List[str], why: str):
        super().__init__(why)
        self.deleted = deleted


class Obj(NamedTuple):
    size: int
    mtime: datetime          # when it went up (the server's time)


class Bucket(Protocol):
    def list(self, includes: Iterable[str]) -> Dict[str, Obj]: ...    # for keys matching an include
    def cat(self, key: str) -> Optional[bytes]: ...                   # None: no such object; raises on a failed read
    def delete(self, keys: List[str], on_deleted: Callable[[str], None]) -> None: ...   # raises PartialDelete


def deleted_key(line: str) -> Optional[str]:
    """The key a `rclone delete -v --use-json-log` line reports deleted, else None."""
    try:
        rec = json.loads(line)
    except ValueError:
        return None
    return rec.get("object") if isinstance(rec, dict) and rec.get("msg") == "Deleted" else None


def _when(s: str) -> datetime:
    return datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", s.replace("Z", "+00:00"))).astimezone(timezone.utc)


class Rclone:
    """The bucket through rclone (`r2:<bucket>`, or any rclone remote/path for a rehearsal). Includes are rclone
    filter rules: `/<key>` or `/<prefix>/**` (anchored at the bucket root)."""

    def __init__(self, remote: str):
        self.remote = remote.rstrip("/")

    def _run(self, *args: str) -> str:
        p = subprocess.run(["rclone", *args, "--s3-no-check-bucket"], capture_output=True, text=True)
        if p.returncode != 0:
            raise Refused(f"rclone {args[0]} failed (exit {p.returncode}): {p.stderr.strip()[-500:]}")
        return p.stdout

    def list(self, includes):
        inc = [a for i in includes for a in ("--include", i)]
        rows = json.loads(self._run("lsjson", "-R", "--files-only", "--no-mimetype", "--use-server-modtime",
                                    self.remote, *inc) or "[]")
        return {r["Path"]: Obj(int(r["Size"]), _when(r["ModTime"])) for r in rows}

    def cat(self, key):
        if key not in self.list([f"/{key}"]):
            return None
        return self._run("cat", f"{self.remote}/{key}").encode()

    def delete(self, keys, on_deleted):
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            f.write("".join(k + "\n" for k in keys))
        done: List[str] = []
        tail: List[str] = []
        try:
            p = subprocess.Popen(["rclone", "delete", self.remote, "--files-from-raw", f.name, "-v", "--use-json-log",
                                  "--stats", "0", "--s3-no-check-bucket"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            assert p.stderr is not None
            for line in p.stderr:                    # each key reported as it goes, never ahead of rclone
                k = deleted_key(line)
                if k is not None:
                    done.append(k)
                    on_deleted(k)
                else:
                    tail = (tail + [line.strip()])[-5:]
            rc = p.wait()
        finally:
            Path(f.name).unlink()
        if rc != 0:
            raise PartialDelete(done, f"rclone delete failed (exit {rc}): {' | '.join(tail)[-500:]}")


def _manifest(bucket: Bucket, key: str) -> Optional[dict]:
    """The pointer at `key` parsed; None when there is none. Unreadable or unparseable: Refused."""
    try:
        raw = bucket.cat(key)
    except Refused as e:
        raise Refused(f"{key} could not be read: {e}") from None
    if raw is None:
        return None
    try:
        m = json.loads(raw)
        rel = m["data_release"]
    except (ValueError, KeyError, TypeError) as e:
        raise Refused(f"{key} could not be parsed ({type(e).__name__}: {e})") from None
    if not isinstance(rel, str) or not rel:
        raise Refused(f"{key} names no data_release")
    return m


def named(m: dict) -> List[str]:
    """Every bucket key the manifest names (each relative `url`, anywhere in it), under release/."""
    out: List[str] = []

    def walk(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k == "url" and isinstance(v, str) and "://" not in v:
                    out.append("release/" + v.lstrip("/"))
                else:
                    walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(m)
    if not out:
        raise Refused(f"the manifest of {m.get('data_release')} names no file")
    return out


def _unit(key: str) -> Optional[str]:
    hit = RELEASE_UNIT.match(key)
    return hit.group(0) if hit else None


def plan_releases(bucket: Bucket, pending: Optional[dict] = None, now: Optional[datetime] = None
                  ) -> Tuple[List[str], Dict[str, Obj], List[str]]:
    """(kept release ids, {key: Obj} to delete, notes). Raises Refused."""
    now = now or datetime.now(timezone.utc)
    notes: List[str] = []
    current = _manifest(bucket, LIVE)
    if pending is not None:                           # the dry run: as the pointer step would leave the two pointers
        live = pending
        if current is None:
            prev = None
        elif current["data_release"] == pending["data_release"]:
            prev = _manifest(bucket, PREVIOUS)
        else:
            prev = current
        notes.append(f"dry run: {live['data_release']} stands in for the live pointer (not written); its files are "
                     "not checked (nothing was uploaded)")
    else:
        if current is None:
            raise Refused(f"there is no {LIVE}: nothing to keep by")
        live, prev = current, _manifest(bucket, PREVIOUS)
    if prev is None:
        notes.append(f"no previous pointer ({PREVIOUS}): the first promote, nothing pruned")
        return [live["data_release"]], {}, notes
    if prev["data_release"] == live["data_release"]:
        raise Refused(f"both pointers name {live['data_release']}: a rollback would have nowhere to go")
    listing = bucket.list(RELEASE_INCLUDES)
    kept: Set[str] = set()
    for m, check in ((live, pending is None), (prev, True)):
        keys = named(m)
        kept |= {u for u in map(_unit, keys) if u}
        if not check:
            continue
        for k in keys:
            ok = any(x.startswith(k) for x in listing) if k.endswith("/") else (k in listing)
            if not ok and not _unit(k):             # named outside the pruned prefixes: look it up on its own
                ok = bool(bucket.list([f"/{k}**" if k.endswith("/") else f"/{k}"]))
            if not ok:
                raise Refused(f"{k}, named by {m['data_release']}'s manifest, is not in the bucket")
    newest: Dict[str, datetime] = {}
    for k, o in listing.items():
        u = _unit(k)
        if u and u not in kept:
            newest[u] = max(newest.get(u, o.mtime), o.mtime)
    for u, t in sorted(newest.items()):
        if now - t < RECENT:
            kept.add(u)
            notes.append(f"keep {u}: no pointer names it, but it went up {t:%Y-%m-%d %H:%M} UTC, under "
                         f"{RECENT.total_seconds() / 3600:.0f} h ago (a web tab opened on it may still read it)")
    gone = {k: o for k, o in sorted(listing.items()) if _unit(k) and _unit(k) not in kept}
    return [live["data_release"], prev["data_release"]], gone, notes


def plan_v1(bucket: Bucket, site: str, fetch: Callable[[str], str]) -> Tuple[Optional[str], Dict[str, Obj]]:
    """(why v1 is kept or None, {key: Obj} to delete). A site that cannot be read raises Refused."""
    try:
        html = fetch(site)
    except Exception as e:                            # noqa: BLE001 — any failure keeps v1, and is shown + exit 1
        raise Refused(f"v1 kept: {site} could not be read ({type(e).__name__}: {e})") from None
    if OLD_SITE in html:
        return f"{site} still serves the old Vite site (`{OLD_SITE}`)", {}
    if not NEW_SITE.search(html):
        return f"{site} does not load the new web export (no /_expo/static/js/web/index-<hash>.js)", {}
    return None, dict(sorted(bucket.list(V1_INCLUDES).items()))


def _fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "prod-prune", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as r:
        if r.status != 200:
            raise OSError(f"HTTP {r.status}")
        return r.read().decode("utf-8", "replace")


def _gb(n: int) -> str:
    return f"{n:,} B ({n / 1e9:.2f} GB)"


def _delete(bucket: Bucket, gone: Dict[str, Obj], dry: bool, what: str) -> None:
    if dry:
        for k, o in gone.items():
            print(f"would delete {k}  ({o.size:,} B)")
        print(f"{what}: {len(gone)} object(s), {_gb(sum(o.size for o in gone.values()))} would be freed (dry run)")
        return
    done: List[str] = []

    def said(k: str) -> None:
        done.append(k)
        print(f"deleted {k}  ({gone[k].size if k in gone else 0:,} B)", flush=True)
    try:
        if gone:
            bucket.delete(list(gone), said)
    except PartialDelete as e:
        n = sum(gone[k].size for k in e.deleted if k in gone)
        raise PartialDelete(e.deleted, f"{what}: the delete stopped part way, {len(e.deleted)} of {len(gone)} "
                                       f"object(s), {_gb(n)} deleted before it did: {e}") from None
    n = sum(gone[k].size for k in done if k in gone)
    print(f"{what}: {len(done)} object(s), {_gb(n)} freed"
          + (f" ({len(gone) - len(done)} listed were already gone)" if len(done) != len(gone) else ""))


def main(argv=None, bucket: Optional[Bucket] = None, fetch: Callable[[str], str] = _fetch,
         now: Optional[datetime] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("step", choices=("releases", "v1"))
    ap.add_argument("--remote", required=bucket is None, help="r2:<bucket> (or any rclone remote for a rehearsal)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--pending", type=Path, help="releases, --dry-run only: the manifest the pointer step would write")
    ap.add_argument("--site", default=SITE, help=f"v1: the site whose page decides (default {SITE})")
    a = ap.parse_args(argv)
    if a.pending and not a.dry_run:
        ap.error("--pending is for a --dry-run (a real run reads the pointer it wrote)")
    b = bucket or Rclone(a.remote)
    try:
        if a.step == "releases":
            pending = None
            if a.pending:
                try:
                    pending = json.loads(a.pending.read_text())
                    pending["data_release"]
                except (OSError, ValueError, KeyError, TypeError) as e:
                    raise Refused(f"{a.pending} could not be read ({type(e).__name__}: {e})") from None
            kept, gone, notes = plan_releases(b, pending, now)
            for n in notes:
                print(f"prune: {n}")
            print(f"prune: keep {', '.join(kept)} (live{' + previous' if len(kept) > 1 else ''})")
            _delete(b, gone, a.dry_run, "old releases")
        else:
            why, gone = plan_v1(b, a.site, fetch)
            if why:
                print(f"v1 kept, nothing deleted: {why}. Deploy the new site first (deploy/prod/site.sh).",
                      file=sys.stderr)
                return 1
            print(f"v1: {a.site} serves the new web export: the v1 layout goes")
            _delete(b, gone, a.dry_run, "v1 layout")
    except Refused as e:
        print(f"prune REFUSED, nothing deleted: {e}", file=sys.stderr)
        return 1
    except PartialDelete as e:
        print(f"prune FAILED: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
