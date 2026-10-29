"""(T251) STAMP A RELEASE'S MANIFEST AT PUBLISH TIME — never inside the deterministic build, which must not read the clock.

    python -m pipeline.deliver.release.stamp data/release/<r>/manifest.json [--min-app-version 0.2.0] [--today 2026-10-28]

Two fields, both outside every hash the release is named by (`data_release` is the sha of its FILES; the manifest is not
one of them), so stamping never changes a file, the tiles block or the data release id — a release without geometry
changes keeps pointing at the same tiles vintage, and nothing is rebuilt or re-uploaded because of a stamp:

    published       the ISO day the release was first published; the regs page says "Regulations as of <date>". Kept
                    once set: publishing the same release again (staging, then production) keeps its first day.
                    `--restamp` sets it to today again.
    minAppVersion   the oldest app (semver) that may apply it; an older app keeps its data and says "Update the app to
                    get new regulations". Set only when asked (`--min-app-version`); `--min-app-version ""` removes it.

The file is rewritten in place (so the local copy is what R2 serves) in the build's own format.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from pathlib import Path

SEMVER = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$")
_UNSET = object()


def stamp(m: dict, *, today: dt.date, min_app_version=_UNSET, restamp: bool = False) -> dict:
    """A copy of `m` with `published` (kept if already set, unless `restamp`) and, when given, `minAppVersion`."""
    out = dict(m)
    if restamp or not out.get("published"):
        out["published"] = today.isoformat()
    if min_app_version is not _UNSET:
        if min_app_version in (None, ""):
            out.pop("minAppVersion", None)
        elif not SEMVER.match(str(min_app_version)):
            raise SystemExit(f"stamp: --min-app-version {min_app_version!r} is not a semver (e.g. 0.2.0)")
        else:
            out["minAppVersion"] = str(min_app_version)
    return out


def stamp_file(path: Path, *, today: dt.date, min_app_version=_UNSET, restamp: bool = False) -> dict:
    text = path.read_text()
    m = json.loads(text)
    out = stamp(m, today=today, min_app_version=min_app_version, restamp=restamp)
    if out != m:
        path.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")     # the build's own format (build.py)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("manifest", type=Path)
    ap.add_argument("--min-app-version", default=None)
    ap.add_argument("--today", default=None, help="ISO day (default: today, UTC)")
    ap.add_argument("--restamp", action="store_true", help="set published to today even when already set")
    a = ap.parse_args(argv)
    today = dt.date.fromisoformat(a.today) if a.today else dt.datetime.now(dt.timezone.utc).date()
    kw = {} if a.min_app_version is None else {"min_app_version": a.min_app_version}
    out = stamp_file(a.manifest, today=today, restamp=a.restamp, **kw)
    print(f"stamped {a.manifest}: published {out['published']}"
          + (f", minAppVersion {out['minAppVersion']}" if out.get("minAppVersion") else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
