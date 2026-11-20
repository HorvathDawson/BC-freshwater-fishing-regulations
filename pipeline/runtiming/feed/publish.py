"""PUT A BUILT RUN-TIMING FEED ON R2 (`runtiming/`; staging by default, owner 2026-10-28: production is the owner's).

    python -m pipeline.runtiming.feed.publish --dir <out>/runtiming --via wrangler|aws [--bucket NAME] [--yes]

(T404) `--bucket` names the target and defaults to STAGING; only the two known buckets are accepted, and the production one only with CONFIRM_PROD=canifishthis.ca (the other prod writers' guard). The workflow passes
the BUCKET it computed from its `target` input; its production option stays commented until the owner enables it.

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

BUCKET = "bc-fishing-regulations-staging"       # the default target
PROD_BUCKET = "bc-fishing-regulations"          # (T404) only on an explicit --bucket
BUCKETS = (BUCKET, PROD_BUCKET)


def checked_bucket(name: str, who: str = "publish") -> str:
    """The bare bucket name, or exit. (T404 review) The production bucket needs CONFIRM_PROD=canifishthis.ca, as every
    deploy/prod script (deploy/lib/bucket.sh): checked on the bare, lower-cased name, plan or send alike."""
    b = name.split(":", 1)[-1].strip("/").lower()
    if b not in BUCKETS:
        raise SystemExit(f"{who}: --bucket {name!r} is not one of {', '.join(BUCKETS)}")
    if b == PROD_BUCKET and os.environ.get("CONFIRM_PROD") != "canifishthis.ca":
        raise SystemExit(f"{who}: set CONFIRM_PROD=canifishthis.ca to use the production bucket {PROD_BUCKET}")
    return b
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


def command(p: Put, via: str, bucket: str = BUCKET) -> List[str]:
    if via == "wrangler":
        cmd = ["npx", "--prefix", "app", "wrangler", "r2", "object", "put", f"{bucket}/{p.key}", "--file", str(p.path),
               "--content-type", "application/json", "--cache-control", p.cache, "--remote"]
        return cmd + (["--content-encoding", p.encoding] if p.encoding else [])
    endpoint = os.environ.get("R2_ENDPOINT")
    if not endpoint:
        raise SystemExit("publish --via aws: R2_ENDPOINT is not set")
    cmd = ["aws", "s3", "cp", str(p.path), f"s3://{bucket}/{p.key}", "--endpoint-url", endpoint,
           "--content-type", "application/json", "--cache-control", p.cache, "--only-show-errors"]
    return cmd + (["--content-encoding", p.encoding] if p.encoding else [])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.runtiming.feed.publish", description=__doc__.split("\n\n")[0])
    ap.add_argument("--dir", type=Path, required=True, help="the built `runtiming/` directory (index.json + files)")
    ap.add_argument("--via", choices=("wrangler", "aws"), required=True)
    ap.add_argument("--bucket", default=BUCKET, help=f"target bucket, one of {', '.join(BUCKETS)} (default: staging)")
    ap.add_argument("--yes", action="store_true", help="send it (without: print the plan)")
    a = ap.parse_args(argv)
    a.bucket = checked_bucket(a.bucket)
    puts = plan(a.dir)
    for p in puts:
        cmd = command(p, a.via, a.bucket)
        if not a.yes:
            print("plan:", " ".join(cmd))
            continue
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
        print(f"put {a.bucket}/{p.key}")
    if not a.yes:
        print("(nothing sent: add --yes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
