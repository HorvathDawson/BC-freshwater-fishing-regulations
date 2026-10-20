"""PUBLISH A RELEASE TO R2 (P10). HUMAN-RUN: it spends nothing, but it changes what every installed app downloads.

    python -m pipeline.tools.publish [--release data/release/<r>]          # the plan, nothing uploaded (default)
    python -m pipeline.tools.publish --release data/release/<r> --yes      # upload it

ORDER IS THE CONTRACT (MINIMAL-PACKAGING §6.4): the tile version folder (archive, mask, map assets) first, then the data
files, then `manifest.json` LAST. An app reads the manifest first, so until the last upload lands nobody is pointed at a
file that is not there yet. Every file but the manifest is named by its content and never changes: it is sent
`Cache-Control: immutable`. The manifest is `no-cache`. Data files are stored gzipped and sent `Content-Encoding: gzip`
(the app checks the decoded bytes against the manifest's sha256, `fetchVerified`).

Credentials from the environment, never the repo: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET.
`boto3` (R2 speaks the S3 API) is needed only for `--yes`: `pip install boto3`.
"""
from __future__ import annotations

import argparse
import json
import os
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
    """Every upload, in order: tiles (+ mask + assets), the data files, then the manifest. Keys are relative to `root`
    (data/release), which is the bucket's layout."""
    m = json.loads((release / "manifest.json").read_text())
    out: List[Put] = []
    t = m.get("tiles") or {}
    if t.get("current"):
        vdir = root / "tiles" / t["current"]["id"]
        for p in sorted(x for x in vdir.rglob("*") if x.is_file()):
            out.append(Put(str(p.relative_to(root)), p, _type(p), None, IMMUTABLE))
    for name, e in sorted(m["files"].items()):
        p = root / e["url"]
        if not p.exists():
            raise SystemExit(f"publish: {p} is listed in the manifest but missing")
        out.append(Put(e["url"], p, "application/octet-stream", "gzip", IMMUTABLE))
    out.append(Put("manifest.json", release / "manifest.json", "application/json", None, "no-cache"))
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--release", type=Path, default=None, help="default: the newest data/release/r-*")
    ap.add_argument("--yes", action="store_true", help="upload (without it, print the plan only)")
    a = ap.parse_args(argv)
    root = GENERATED.release
    release = a.release or max((p for p in root.glob("r-*") if p.is_dir()), key=lambda p: p.stat().st_mtime,
                               default=None)
    if release is None:
        raise SystemExit(f"publish: no release under {root} — python -m pipeline.deliver release")
    puts = plan(Path(release), root)
    for p in puts:
        print(f"{'PUT ' if a.yes else 'plan'} {p.key}  ({p.path.stat().st_size:,} B, {p.cache}"
              f"{', gzip' if p.encoding else ''})")
    if not a.yes:
        print(f"\n{len(puts)} uploads planned; nothing sent. Add --yes to publish.", file=sys.stderr)
        return 0
    need = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"]
    missing = [k for k in need if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"publish: set {', '.join(missing)}")
    import boto3
    s3 = boto3.client("s3", endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
                      aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"])
    for p in puts:
        extra = {"ContentType": p.content_type, "CacheControl": p.cache,
                 **({"ContentEncoding": p.encoding} if p.encoding else {})}
        s3.upload_file(str(p.path), os.environ["R2_BUCKET"], p.key, ExtraArgs=extra)
    print(f"published {release.name}: {len(puts)} objects, manifest last")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
