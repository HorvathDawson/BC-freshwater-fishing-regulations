"""THE RELEASE UPLOAD PLAN (P10): what an upload of a release sends, in order. It SENDS NOTHING.

    python -m pipeline.deliver.release.publish [--release data/release/<r>]          # the plan, nothing uploaded

ORDER IS THE CONTRACT (MINIMAL-PACKAGING §6.4): the tile version folder (archive, mask, map assets) first, the basemap
(T75, when the manifest names one), then the data
files, then `manifest.json` LAST. An app reads the manifest first, so until the last upload lands nobody is pointed at a
file that is not there yet. Every file but the manifest is named by its content and never changes: it is sent
`Cache-Control: immutable`. The manifest is `no-cache`. Data files are stored gzipped and sent `Content-Encoding: gzip`
(the app checks the decoded bytes against the manifest's sha256, `fetchVerified`).

(T994 #1) NO UPLOAD FROM HERE. `--yes` once sent this plan to whatever bucket `R2_BUCKET` named, past every deploy gate
(app_gate, pieces_gate, site_gate, the staging-equals-production check) and re-stamped the local manifest, so the next
production promote's byte check failed. A release reaches R2 ONLY by the one release path: `python -m pipeline release
--staging` (its stage 7 runs `deploy/staging/upload.sh`, staging bucket only), then the owner's
`CONFIRM_PROD=<r-id> python -m pipeline release --prod <r-id>` (`deploy/prod/upload.sh`). `--yes` and
`--min-app-version` are refused with those commands. `plan` stays the upload's own list: `pipeline release` checks a
candidate against it.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, NamedTuple

from pipeline.common.curated import GENERATED

IMMUTABLE = "public, max-age=31536000, immutable"


class Put(NamedTuple):
    key: str
    path: Path
    content_type: str
    encoding: str | None
    cache: str


def _type(p: Path) -> str:
    n = p.name
    return ("application/json" if n.endswith(".json") else "application/geo+json" if n.endswith(".geojson")
            else "image/png" if n.endswith(".png") else "application/x-protobuf" if n.endswith(".pbf")
            else "application/octet-stream")


def plan(release: Path, root: Path) -> List[Put]:
    """Every upload, in order: tiles (+ mask + assets), the basemap, the data files, then the manifest. Keys are relative to `root`
    (data/release), which is the bucket's layout."""
    m = json.loads((release / "manifest.json").read_text())
    out: List[Put] = []
    t = m.get("tiles") or {}
    if t.get("current"):
        vdir = root / "tiles" / t["current"]["id"]
        for p in sorted(x for x in vdir.rglob("*") if x.is_file()):
            out.append(Put(str(p.relative_to(root)), p, _type(p), None, IMMUTABLE))
    bm = m.get("basemap")
    if bm:                                   # T75: the ground, beside the tiles and before any data file
        p = root / bm["url"]
        if not p.exists():
            raise SystemExit(f"publish: {p} is the manifest's basemap but missing")
        out.append(Put(bm["url"], p, _type(p), None, IMMUTABLE))
    for name, e in sorted(m["files"].items()):
        p = root / e["url"]
        if not p.exists():
            raise SystemExit(f"publish: {p} is listed in the manifest but missing")
        out.append(Put(e["url"], p, "application/octet-stream", "gzip", IMMUTABLE))
    syn = m.get("synopsis")
    if syn:                                  # T215: our PDF, raw (already compressed), content-named
        p = root / syn["url"]
        if not p.exists():
            raise SystemExit(f"publish: {p} is the manifest's synopsis but missing")
        out.append(Put(syn["url"], p, "application/pdf", None, IMMUTABLE))
    out.append(Put("manifest.json", release / "manifest.json", "application/json", None, "no-cache"))
    return out


#: (T994 #1) the one way a release reaches R2, said when someone asks this module to upload
REFUSED = ("publish: this module only prints the plan; it uploads nothing (T994 #1). A release reaches R2 by the one "
           "release path: `python -m pipeline release --staging` (staging), then the owner's "
           "`CONFIRM_PROD=<r-id> python -m pipeline release --prod <r-id>` (production, human-only).")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--release", type=Path, default=None, help="default: the release data/release/LATEST names")
    ap.add_argument("--yes", action="store_true", help="REFUSED (T994 #1): uploads go through `pipeline release`")
    ap.add_argument("--min-app-version", default=None,
                    help="REFUSED (T994 #1): the staging upload stamps it (`pipeline release --min-app-version`)")
    a = ap.parse_args(argv)
    if a.yes or a.min_app_version is not None:
        print(REFUSED, file=sys.stderr)
        return 2
    root = GENERATED.release
    from pipeline.deliver.release.manifest import latest
    release = a.release or latest(root)          # THE release on disk: one answer (T133)
    if release is None:
        raise SystemExit(f"publish: no release under {root} — python -m pipeline.deliver release")
    puts = plan(Path(release), root)
    for p in puts:
        print(f"plan {p.key}  ({p.path.stat().st_size:,} B, {p.cache}{', gzip' if p.encoding else ''})")
    print(f"\n{len(puts)} uploads planned; nothing sent (this module never sends: `python -m pipeline release "
          "--staging`).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
