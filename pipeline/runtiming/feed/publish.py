"""PUT A BUILT RUN-TIMING FEED ON R2 — STAGING ONLY (`runtiming/`; owner 2026-10-28: production is the owner's).

    python -m pipeline.runtiming.feed.publish --dir <out>/runtiming --via wrangler|aws [--yes]

ORDER IS THE CONTRACT (as the release's `publish.py`): the content-named files first (immutable, stored gzipped and
sent `Content-Encoding: gzip`; the app checks the DECODED bytes against the index), then `index.json` LAST (no-cache),
so no app is ever pointed at a file that is not there yet. Without `--yes`, the plan only.

  --via wrangler   the owner's Mac: `npx --prefix app wrangler r2 object put … --remote` (wrangler's own login)
  --via aws        a runner: `aws s3 cp … --endpoint-url $R2_ENDPOINT` (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY: an
                   R2 token scoped to the staging bucket)

Old content-named files are left in place: an app holding the previous index can still finish reading it. They are a
few MB a refresh; prune by hand when that matters.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import List, NamedTuple, Optional

BUCKET = "bc-fishing-regulations-staging"       # STAGING ONLY: this module never names a production bucket
PREFIX = "runtiming"
IMMUTABLE = "public, max-age=31536000, immutable"


class Put(NamedTuple):
    key: str
    path: Path
    encoding: Optional[str]
    cache: str


def plan(d: Path) -> List[Put]:
    index = json.loads((d / "index.json").read_text())
    out = []
    for name, f in sorted(index["files"].items()):
        p = d / f["url"]
        if not p.is_file():
            raise SystemExit(f"publish: the index names {f['url']} ({name}) and {d} has no such file")
        out.append(Put(f"{PREFIX}/{f['url']}", p, "gzip", IMMUTABLE))
    out.append(Put(f"{PREFIX}/index.json", d / "index.json", None, "no-cache"))
    return out


def command(p: Put, via: str) -> List[str]:
    if via == "wrangler":
        cmd = ["npx", "--prefix", "app", "wrangler", "r2", "object", "put", f"{BUCKET}/{p.key}", "--file", str(p.path),
               "--content-type", "application/json", "--cache-control", p.cache, "--remote"]
        return cmd + (["--content-encoding", p.encoding] if p.encoding else [])
    endpoint = os.environ.get("R2_ENDPOINT")
    if not endpoint:
        raise SystemExit("publish --via aws: R2_ENDPOINT is not set")
    cmd = ["aws", "s3", "cp", str(p.path), f"s3://{BUCKET}/{p.key}", "--endpoint-url", endpoint,
           "--content-type", "application/json", "--cache-control", p.cache, "--only-show-errors"]
    return cmd + (["--content-encoding", p.encoding] if p.encoding else [])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.runtiming.feed.publish", description=__doc__.split("\n\n")[0])
    ap.add_argument("--dir", type=Path, required=True, help="the built `runtiming/` directory (index.json + files)")
    ap.add_argument("--via", choices=("wrangler", "aws"), required=True)
    ap.add_argument("--yes", action="store_true", help="send it (without: print the plan)")
    a = ap.parse_args(argv)
    puts = plan(a.dir)
    for p in puts:
        cmd = command(p, a.via)
        if not a.yes:
            print("plan:", " ".join(cmd))
            continue
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
        print(f"put {BUCKET}/{p.key}")
    if not a.yes:
        print("(nothing sent: add --yes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
