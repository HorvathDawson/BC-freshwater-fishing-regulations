"""THE SITE-VERSION GATE ON A STAGING UPLOAD (T483 fast-fail, 2026-11-09).

    python -m pipeline.deliver.release.site_gate data/release/<r>/manifest.json [--min-app-version X.Y.Z] [--site URL]

caught (2026-11-09): release r-d14bdb180f55 went up with minAppVersion 0.7.0 while the deployed staging web site was an
older build (0.6.9): the site's app refused the release and the smoke test failed AFTER the upload ("The map data could
not be loaded"). The site must be at least the oldest app the release lets apply it, so the site is redeployed FIRST.

The live site's version: its index.html names the web bundle (`/_expo/static/js/web/index-<hash>.js`), and that bundle
carries the version stamp inlined at export (`version-stamp.js` → `resolveAppVersion({version:"X.Y.Z",build:"N",...})`).
A site whose version cannot be read is a refusal too (never a silent pass); SITE_CHECK_SKIP=<reason> skips the gate and
prints the reason. The oldest app is `--min-app-version` (the upload's MIN_APP_VERSION), else the manifest's stamp, and
at least what the release's formats need (`app_gate`).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

from pipeline.deliver.release.app_gate import oldest_reader, ver

STAGING_SITE = "https://staging.canifishthis.ca"
BUNDLE_RE = re.compile(r'/_expo/static/js/web/index-[0-9a-f]+\.js')
VERSION_RE = re.compile(r'resolveAppVersion\)?\(\{version:"(\d+\.\d+\.\d+[^"]*)",build:"([^"]*)"')


def redeploy_steps(rid: str | None) -> str:
    del rid     # (T544) the one command builds on LATEST, which is the release about to be uploaded
    return ("redeploy the staging site first (export, SEO pages on LATEST, deploy, smoke), then upload again:\n"
            "    deploy/staging/site-all.sh")


def fetch_text(url: str, timeout: float = 30) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "bc-fishing-site-gate", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:   # noqa: S310 — a fixed https host
        return r.read().decode("utf-8", errors="replace")


def site_version(site: str = STAGING_SITE, fetch=fetch_text) -> tuple[str | None, str]:
    """(the live site's app version, how it was read) — (None, why) when it cannot be read."""
    site = site.rstrip("/")
    try:
        html = fetch(f"{site}/")
    except Exception as e:  # noqa: BLE001 — any failure to read is a refusal, said with its cause
        return None, f"{site}/ could not be fetched ({e})"
    m = BUNDLE_RE.search(html)
    if not m:
        return None, f"{site}/ names no web bundle (/_expo/static/js/web/index-*.js)"
    try:
        js = fetch(f"{site}{m.group(0)}")
    except Exception as e:  # noqa: BLE001
        return None, f"{site}{m.group(0)} could not be fetched ({e})"
    v = VERSION_RE.search(js)
    if not v:
        return None, f"{site}{m.group(0)} carries no version stamp (resolveAppVersion({{version:...}}))"
    return v.group(1), f"{site} serves app {v.group(1)} (build {v.group(2)}, {m.group(0).rsplit('/', 1)[-1]})"


def required(manifest: dict | None, min_app: str | None, apps: dict | None = None) -> str | None:
    """The oldest app the release may be applied by: the stamp, raised to what its formats need (`app_gate`'s table:
    every file of the contract, T555 item 4). A format no recorded app reads is `app_gate.problems`' refusal, not this
    gate's: it asks nothing more of the site here."""
    have = min_app or (manifest or {}).get("minAppVersion")
    needs = [have] if have else []
    if manifest and manifest.get("files"):
        need = oldest_reader(manifest, apps)
        if need:
            needs.append(need)
    return max(needs, key=ver) if needs else None


def problems(site_ver: str | None, how: str, need: str | None, rid: str | None = None) -> list[str]:
    """[] when the site may take the release; else what is wrong and the commands that fix it."""
    if site_ver is None:
        return [f"the staging site's app version is unknown: {how}. {redeploy_steps(rid)}\n"
                "(SITE_CHECK_SKIP=<reason> skips this gate and prints the reason)"]
    if need and ver(site_ver) < ver(need):
        return [f"the staging site is app {site_ver}, older than {need} (the oldest app this release lets apply it): "
                f"its app would refuse the release (\"The map data could not be loaded\"). {redeploy_steps(rid)}"]
    return []


def check(manifest: dict | None, min_app: str | None, *, site: str = STAGING_SITE, fetch=fetch_text,
          rid: str | None = None) -> tuple[list[str], str]:
    """(problems, what was found). SITE_CHECK_SKIP=<reason> returns no problems and says so."""
    skip = os.environ.get("SITE_CHECK_SKIP", "").strip()
    if skip:
        return [], f"site check SKIPPED (SITE_CHECK_SKIP): {skip}"
    need = required(manifest, min_app)
    v, how = site_version(site, fetch)
    return problems(v, how, need, rid), f"{how}; the release needs app >= {need or 'any'}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("manifest", type=Path)
    ap.add_argument("--min-app-version", default=os.environ.get("MIN_APP_VERSION") or None)
    ap.add_argument("--site", default=STAGING_SITE)
    a = ap.parse_args(argv)
    m = json.loads(a.manifest.read_text(encoding="utf-8"))
    bad, said = check(m, a.min_app_version, site=a.site, rid=m.get("data_release"))
    print(f"site gate: {said}")
    for p in bad:
        print(f"upload: refused before anything was sent — {p}", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
