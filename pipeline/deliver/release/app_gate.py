"""THE APP-VERSION GATE ON A RELEASE (guard 2026-10-30; moved here from deploy/staging/upload.sh for T408).

    python -m pipeline.deliver.release.app_gate data/release/<r>/manifest.json [--min-app-version X.Y.Z]

An unstamped paint/5 release reached staging and broke 0.2.0, which reads paint/4 at most: a release one of whose files
is in a format the oldest app allowed to apply it cannot read is REFUSED. The oldest app allowed is `--min-app-version`
(the upload's MIN_APP_VERSION), else the manifest's own `minAppVersion` stamp. One copy: the staging upload and
`pipeline release`'s checks both call `problems`.

(T555 item 4) EVERY FORMAT IS GATED, FROM ONE GENERATED TABLE. The gate used to know paint and salmon only, from two
hand tables ("add a row whenever paint.py's FORMAT moves"); water/3 and geo/2 went out on a stamp set by hand. Now
`app_reads.json` (beside this file; written and kept by `pipeline.tools.emit.app_reads`, whose `--check` runs in
`pnpm check`) says what each app version reads, per file of the release contract, and the oldest app that reads ALL of
a manifest's formats is derived here (`oldest_reader`). The rule is the app's own (`readsRelease`): a file the app's
contract does not know is a newer release's and is not read (it passes); a file it knows must be in a format it takes.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def ver(s: str) -> tuple:
    return tuple(int(x) for x in s.split("-")[0].split("+")[0].split("."))


def table() -> dict[str, dict[str, list[str]]]:
    """{app version: {file: [formats it reads]}} — the committed table (missing: a raise, never an empty table)."""
    from pipeline.tools.emit.app_reads import load
    return {v: row["reads"] for v, row in load()["apps"].items()}


def needs(manifest: dict, apps: dict[str, dict[str, list[str]]]) -> dict[str, str | None]:
    """{format: the oldest recorded app from which EVERY recorded app reads it} for each format the manifest ships —
    None where the newest recorded app does not read it. The rule is the app's `readsRelease`: a file an app's contract
    does not know is a newer release's and is not read, so it asks nothing of that app."""
    order = sorted(apps, key=ver)
    out: dict[str, str | None] = {}
    for name, f in sorted(manifest["files"].items()):
        fmt, need = f["format"], None
        for v in reversed(order):
            if name in apps[v] and fmt not in apps[v][name]:
                break
            need = v
        # several files share a format (the regional slices): the format needs the newest of their answers
        if fmt not in out or need is None or (out[fmt] is not None and ver(need) > ver(out[fmt])):
            out[fmt] = need
    return out


def oldest_reader(manifest: dict, apps: dict[str, dict[str, list[str]]] | None = None) -> str | None:
    """The oldest recorded app that reads ALL of the manifest's formats (and every newer one does too); None when the
    newest recorded app cannot read one of them."""
    got = needs(manifest, table() if apps is None else apps)
    return None if any(v is None for v in got.values()) else max(got.values(), key=ver, default=None)


def problems(manifest: dict, min_app_version: str | None = None, apps: dict | None = None) -> list[str]:
    """Every format that needs a newer app than the oldest one allowed; [] when the release may ship."""
    apps = table() if apps is None else apps
    have = min_app_version or manifest.get("minAppVersion")
    got = needs(manifest, apps)
    first, newest = min(apps, key=ver), max(apps, key=ver)
    overall = None if any(v is None for v in got.values()) else max(got.values(), key=ver, default=None)
    out = []
    for fmt in sorted(got):
        need = got[fmt]
        if need is None:
            out.append(f"{fmt} is read by no current app (the newest recorded is {newest}): a new format ships after the "
                       "app that reads it is recorded (bump its version, `python -m pipeline.tools.emit.app_reads`); "
                       "an old one is not shipped")
        elif (have and ver(have) < ver(need)) or (not have and need != first):
            out.append(f"{fmt} needs app >= {need} but the oldest app allowed is {have or 'any'}; "
                       f"rerun with MIN_APP_VERSION={overall or need} (or newer)")
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
