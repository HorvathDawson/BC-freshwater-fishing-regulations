"""T416: THE BUILD'S PINNED INPUTS — the big `data/source/` files that are not in git, mirrored to the build-assets
bucket by content, so a fresh clone builds from the same bytes this Mac did.

    python -m pipeline.tools.source_pins fetch            # download every pin that is missing or differs, then verify
    python -m pipeline.tools.source_pins check            # verify only (exit 1 naming each file that is off)
    python -m pipeline.tools.source_pins pin PATH [...]   # hash + upload a data/source file and record it (rclone)

The manifest is `data/source/pins.json` (in git): per file its size, sha256, the URL it is fetched from, and where its
bytes first came from; a GeoPackage the fetch dated (`<stem>.source.json`, `pipeline.common.source_dates`) also carries
`layer_dates`, the day each layer's source is from. A pinned key is `source/<sha256[:16]>/<name>` — a new version is a new key, so a pin never
changes under a release that was built from it. `data/fetch_data.py` stays the way to REFRESH from upstream (its live
layers drift); pin the result again when a build should move to it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

from pipeline.common import source_dates
from pipeline.common.curated import ROOT, SOURCE

MANIFEST = SOURCE / "pins.json"
#: a pin keyed `generated/<path>` is a git-ignored GENERATED input the build reads (T416: the synopsis extraction and
#: its row boxes, which no build stage makes); every other key is under data/source
GENERATED_PREFIX = "generated/"
GENERATED_ROOT = ROOT / "data" / "generated"
BUCKET = "r2:bc-fishing-build-assets"
BASE_URL = "https://build.canifishthis.ca"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1 << 24):
            h.update(chunk)
    return h.hexdigest()


def load() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def local(rel: str) -> Path:
    """Where a pin's key lives on disk."""
    return GENERATED_ROOT / rel[len(GENERATED_PREFIX):] if rel.startswith(GENERATED_PREFIX) else SOURCE / rel


def key_of(p: Path) -> str:
    """A file's pin key: its path under data/source, or `generated/<path>` under data/generated (else refused)."""
    p = p.resolve()
    for root, prefix in ((Path(SOURCE).resolve(), ""), (GENERATED_ROOT.resolve(), GENERATED_PREFIX)):
        if p.is_relative_to(root):
            return prefix + p.relative_to(root).as_posix()
    raise SystemExit(f"pins: {p} is under neither {SOURCE} nor {GENERATED_ROOT}")


def off(rel: str, pin: dict) -> str | None:
    """Why the local copy is not the pin (None when it is). Size first: a full hash of 10 GB takes a minute."""
    p = local(rel)
    if not p.exists():
        return "missing"
    if p.stat().st_size != pin["bytes"]:
        return f"size {p.stat().st_size} != {pin['bytes']}"
    got = sha256(p)
    return None if got == pin["sha256"] else f"sha256 {got[:16]} != {pin['sha256'][:16]}"


def check() -> int:
    bad = {rel: why for rel, pin in load()["files"].items() if (why := off(rel, pin))}
    for rel, why in bad.items():
        print(f"  ✗ {rel}: {why}")
    print(f"pins: {len(load()['files']) - len(bad)} ok, {len(bad)} off")
    return 1 if bad else 0


def fetch() -> int:
    """Each pin that is off is downloaded (curl, resumable into `<name>.part`), then hashed; a mismatch fails the run."""
    failed = []
    for rel, pin in load()["files"].items():
        if not off(rel, pin):
            print(f"  ✓ {rel}")
            continue
        dest = local(rel)
        part = dest.with_name(dest.name + ".part")
        dest.parent.mkdir(parents=True, exist_ok=True)
        print(f"  ↓ {rel} ({pin['bytes'] / 1e9:.2f} GB) <- {pin['url']}")
        rc = subprocess.call(["curl", "-fL", "--retry", "5", "-C", "-", "-o", str(part), pin["url"]])
        if rc != 0:
            failed.append(f"{rel}: curl exit {rc} (rerun to resume)")
            continue
        got = sha256(part)
        if got != pin["sha256"]:
            part.unlink()
            failed.append(f"{rel}: downloaded sha256 {got[:16]} != pinned {pin['sha256'][:16]}")
            continue
        part.rename(dest)
    for f in failed:
        print(f"  ✗ {f}")
    return 1 if failed else 0


def pin(paths: list[str], upstream: str | None) -> int:
    man = load() if MANIFEST.exists() else {"files": {}}
    for raw in paths:
        p = Path(raw).resolve()
        rel = key_of(p)
        digest = sha256(p)
        key = f"source/{digest[:16]}/{p.name}"
        print(f"  ↑ {rel} -> {BUCKET}/{key}")
        subprocess.run(["rclone", "copyto", "--s3-no-check-bucket", str(p), f"{BUCKET}/{key}"], check=True)
        old = man["files"].get(rel, {})
        man["files"][rel] = {"bytes": p.stat().st_size, "sha256": digest, "url": f"{BASE_URL}/{key}",
                             "upstream": upstream or old.get("upstream", "")}
        layers = source_dates.read(p)
        if layers is not None:
            # T561: the day each layer's SOURCE is from, as the fetch recorded it beside the file
            man["files"][rel]["layer_dates"] = layers
    man["files"] = dict(sorted(man["files"].items()))
    MANIFEST.write_text(json.dumps(man, indent=2) + "\n", encoding="utf-8")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("fetch")
    sub.add_parser("check")
    p = sub.add_parser("pin")
    p.add_argument("paths", nargs="+")
    p.add_argument("--upstream", help="where these bytes first came from (kept from the old pin when omitted)")
    a = ap.parse_args(argv)
    if a.cmd == "pin":
        return pin(a.paths, a.upstream)
    return fetch() if a.cmd == "fetch" else check()


if __name__ == "__main__":
    sys.exit(main())
