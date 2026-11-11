"""T554 item 4: an upload never puts an OLDER HYDAT's clim.json back over a newer one on R2.

    python -m pipeline.gauges.feed.clim_guard --local data/generated/gauges/feeds/clim.json --remote r2:<bucket>/feeds/live/clim.json

clim.json has two writers: the HYDAT workflow (.github/workflows/feeds-hydat.yml, started by the feeds worker when a new
HYDAT is listed) and the release uploads (deploy/staging/upload.sh, deploy/prod/upload.sh) copying the Mac's file. In the
window between the workflow and the owner's scripts/update-hydat.sh, a release upload restored the older envelope and
nobody was told. The uploads now ask this first:

    exit 0  put it: R2 holds none, or an older or the same HYDAT (same HYDAT: rclone --checksum puts it only if it differs)
    exit 3  skip it, said on stderr: R2 holds a NEWER HYDAT than the Mac (run scripts/update-hydat.sh), or either side's
            release is not a HYDAT day (nothing is compared on a guess, and nothing is overwritten)
    exit 1  the local file cannot be read, or R2 lists the file but it cannot be read

`release` is clim.json's first key (climatology.py), so only the head of the remote file is read.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

HEAD_BYTES = 4096
_RELEASE = re.compile(r'"release"\s*:\s*"([^"]*)"')
_DAY = re.compile(r"^\d{8}$")


def release_of_head(head: str) -> str | None:
    """The `release` named in the head of a clim.json, or None."""
    m = _RELEASE.search(head)
    return m.group(1) if m else None


def decide(local: str | None, remote: str | None, remote_exists: bool) -> tuple[bool, str]:
    """(put?, why) for a local clim.json on HYDAT `local` over R2's on `remote`."""
    if not remote_exists:
        return True, "R2 holds no clim.json"
    if not (local and _DAY.match(local)):
        return False, f"the local clim.json names no HYDAT day ({local!r}); not put"
    if not (remote and _DAY.match(remote)):
        return False, f"R2's clim.json names no HYDAT day ({remote!r}); not replaced — check it by hand"
    if local < remote:
        return False, (f"R2's clim.json is on HYDAT {remote}, newer than the local {local}; not replaced. "
                       "Run scripts/update-hydat.sh, then upload again")
    return True, f"HYDAT {local} over R2's {remote}"


def _rclone(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["rclone", *args, "--s3-no-check-bucket"], capture_output=True, text=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--local", type=Path, required=True)
    ap.add_argument("--remote", required=True, help="the rclone path of R2's clim.json")
    a = ap.parse_args(argv)
    try:
        local = json.loads(a.local.read_text(encoding="utf-8")).get("release")
    except (OSError, ValueError) as ex:
        print(f"clim_guard: {a.local} cannot be read: {ex}", file=sys.stderr)
        return 1
    ls = _rclone("lsf", a.remote)
    if ls.returncode != 0 and "not found" not in (ls.stderr or "").lower():
        print(f"clim_guard: cannot list {a.remote}: {ls.stderr.strip()}", file=sys.stderr)
        return 1
    exists = ls.returncode == 0 and bool(ls.stdout.strip())
    remote = None
    if exists:
        got = _rclone("cat", a.remote, "--head", str(HEAD_BYTES))
        if got.returncode != 0:
            print(f"clim_guard: {a.remote} is listed but cannot be read: {got.stderr.strip()}", file=sys.stderr)
            return 1
        remote = release_of_head(got.stdout)
    put, why = decide(local, remote, exists)
    print(f"clim.json: {why}", file=sys.stderr)
    return 0 if put else 3


if __name__ == "__main__":
    raise SystemExit(main())
