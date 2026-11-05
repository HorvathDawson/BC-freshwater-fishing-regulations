"""THE APP-VERSION GATE ON A RELEASE (guard 2026-10-30; moved here from deploy/staging/upload.sh for T408).

    python -m pipeline.deliver.release.app_gate data/release/<r>/manifest.json [--min-app-version X.Y.Z]

An unstamped paint/5 release reached staging and broke 0.2.0, which reads paint/4 at most: a release whose paint (or
salmon) format needs a newer app than the oldest the manifest lets apply it is REFUSED. The oldest app allowed is
`--min-app-version` (the upload's MIN_APP_VERSION), else the manifest's own `minAppVersion` stamp. One copy: the
staging upload and `pipeline release`'s checks both call `problems`.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

#: paint format -> the first app that reads it. Add a row whenever paint.py's FORMAT moves.
PAINT_NEEDS = {5: "0.3.0"}
#: (T332/T349) salmon/3 = salmon quotas in the game-fish quota tree; (T356) salmon/4 = header closure, shared limits and
#: sizes resolved in the file. 0.4.0 reads salmon/2; 0.5.0 reads salmon/4.
SALMON_NEEDS = {3: "0.5.0", 4: "0.5.0"}


def ver(s: str) -> tuple:
    return tuple(int(x) for x in s.split("-")[0].split("+")[0].split("."))


def problems(manifest: dict, min_app_version: str | None = None) -> list[str]:
    """Every file whose format needs a newer app than the oldest one allowed; [] when the release may ship."""
    have = min_app_version or manifest.get("minAppVersion")
    out = []
    for key, needs in (("paint", PAINT_NEEDS), ("salmon", SALMON_NEEDS)):
        f = manifest["files"].get(key)
        if not f:
            continue
        fmt = int(f["format"].split("/")[1])
        need = max((v for k, v in needs.items() if fmt >= k), default=None, key=ver)
        if need and (not have or ver(have) < ver(need)):
            out.append(f"{key}/{fmt} needs app >= {need} but the oldest app allowed is {have or 'any'}; "
                       f"rerun with MIN_APP_VERSION={need} (or newer)")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("manifest", type=Path)
    ap.add_argument("--min-app-version", default=os.environ.get("MIN_APP_VERSION") or None)
    a = ap.parse_args(argv)
    bad = problems(json.loads(a.manifest.read_text(encoding="utf-8")), a.min_app_version)
    for p in bad:
        print(f"upload: refused — {p}", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
