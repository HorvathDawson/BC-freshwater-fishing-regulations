"""fetch — stable snapshot fetcher for the DFO region salmon pages.

Design notes (each line is here because of a measured property of the source):

* **curl_cffi + `impersonate`**, same as `archive/pipeline/recurring/in_season/scraper.py`.
  DFO Pacific is IIS and answers plain `requests` today, but the BC-gov Akamai WAF
  taught this repo once already that a datacenter IP with a non-browser TLS
  fingerprint gets dropped. Impersonating costs nothing and removes the failure mode.
* **No conditional GET.** Measured: the server sends no `ETag` and no `Last-Modified`
  (IIS with `Content-Length` only), so `If-None-Match` has nothing to bind to.
  Change detection is content `sha256` plus the page's own `<time property="dateModified">`.
* **Validation gate before write.** A 200 that is a WAF interstitial, a maintenance
  page, or a truncated body is a *failed attempt*, not a snapshot. A good snapshot on
  disk is never replaced by a bad fetch.
* **Atomic write.** `.tmp` + `os.replace`, so a killed run cannot leave half a page.

CLI
---
    .venv/bin/python -m pipeline.dfo_salmon.fetch                 # all regions
    .venv/bin/python -m pipeline.dfo_salmon.fetch --regions 2 6
    .venv/bin/python -m pipeline.dfo_salmon.fetch --force         # ignore unchanged
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------

_BASE = "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region{n}-eng.html"

#: Region number -> the name DFO gives it. Region 5 (Cariboo) publishes no table;
#: it is fetched anyway so that the day it grows one, the manifest diff shows it.
REGIONS: Dict[int, str] = {
    1: "Vancouver Island",
    2: "Lower Mainland",
    3: "Thompson-Nicola",
    4: "Kootenays",
    5: "Cariboo",
    6: "Skeena",
    7: "Omineca-Peace",
    8: "Okanagan",
}

DEFAULT_CACHE = Path("cache/dfo_salmon")

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_IMPERSONATE = "chrome124"

_REQUEST_TIMEOUT = 30
_MAX_ATTEMPTS = 4
_BACKOFF_BASE = 2.0  # seconds: 2, 4, 8 (+ jitter)
_THROTTLE = 1.5      # seconds between regions; polite, and keeps us off any rate limit

#: A body that does not contain all of these is not the page we asked for.
_SENTINELS = (
    'id="wb-cont"',                       # WET-BOEW main heading anchor
    "Recreational salmon fishing limits",  # the page's own subject
    'property="dateModified"',            # the footer stamp we key change detection on
)
_MIN_BYTES = 8_000

_RE_DATE_MODIFIED = re.compile(
    r'<time[^>]*property="dateModified"[^>]*>\s*([0-9]{4}-[0-9]{2}-[0-9]{2})', re.I
)
_RE_TITLE = re.compile(r"<title>([^<]*)</title>", re.I)


class FetchError(RuntimeError):
    """Every attempt for one region failed."""


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


@dataclass
class RegionSnapshot:
    """One region's raw page plus everything needed to detect that it moved."""

    region: int
    region_name: str
    url: str
    fetched_at: str
    http_status: int
    bytes: int
    sha256: str
    date_modified: Optional[str]
    title: str
    has_table: bool
    attempts: int
    elapsed_s: float
    changed: bool = True
    path: Optional[str] = None
    #: Non-fatal problems worth surfacing (retried statuses, validation misses).
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def _session():
    """A curl_cffi session, or plain requests if curl_cffi is unavailable.

    curl_cffi is in requirements.txt; the fallback exists so a bare checkout can
    still run the parser tests without the browser-impersonation wheel.
    """
    try:
        from curl_cffi import requests as cffi_requests  # type: ignore

        return cffi_requests, {"impersonate": _IMPERSONATE}
    except ImportError:  # pragma: no cover - exercised only on bare checkouts
        import requests as plain_requests  # type: ignore

        logger.warning("curl_cffi unavailable; falling back to requests (no TLS impersonation)")
        return plain_requests, {}


def _validate(body: str) -> Optional[str]:
    """Return a reason string if `body` is not a real region page, else None."""
    if len(body) < _MIN_BYTES:
        return f"body too small ({len(body)} B < {_MIN_BYTES} B)"
    missing = [s for s in _SENTINELS if s not in body]
    if missing:
        return f"missing sentinel(s): {', '.join(missing)}"
    return None


def _sleep_backoff(attempt: int, retry_after: Optional[float] = None) -> None:
    if retry_after is not None:
        delay = retry_after
    else:
        delay = _BACKOFF_BASE * (2 ** (attempt - 1))
    delay += random.uniform(0, 0.75)  # jitter: never sync up with another runner
    logger.info("  retrying in %.1fs", delay)
    time.sleep(delay)


def fetch_region(
    region: int,
    *,
    cache_dir: Path = DEFAULT_CACHE,
    force: bool = False,
    write: bool = True,
    timeout: int = _REQUEST_TIMEOUT,
    max_attempts: int = _MAX_ATTEMPTS,
) -> RegionSnapshot:
    """Fetch one region page, validate it, and write it to `cache_dir` if it moved.

    Raises `FetchError` if every attempt fails validation or transport. An existing
    good snapshot on disk is left untouched in that case.
    """
    if region not in REGIONS:
        raise ValueError(f"unknown region {region!r}; known: {sorted(REGIONS)}")

    url = _BASE.format(n=region)
    session, extra = _session()
    headers = {
        "User-Agent": _USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-CA,en;q=0.9",
    }

    warnings: List[str] = []
    started = time.monotonic()
    body: Optional[str] = None
    status = 0

    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, headers=headers, timeout=timeout, **extra)
            status = resp.status_code
            if status in (429, 500, 502, 503, 504):
                retry_after = None
                raw = resp.headers.get("Retry-After")
                if raw and raw.strip().isdigit():
                    retry_after = min(float(raw.strip()), 60.0)
                warnings.append(f"attempt {attempt}: HTTP {status}")
                if attempt < max_attempts:
                    _sleep_backoff(attempt, retry_after)
                    continue
                raise FetchError(f"region {region}: HTTP {status} after {attempt} attempts")
            resp.raise_for_status()

            candidate = resp.text
            reason = _validate(candidate)
            if reason:
                warnings.append(f"attempt {attempt}: {reason}")
                if attempt < max_attempts:
                    _sleep_backoff(attempt)
                    continue
                raise FetchError(f"region {region}: {reason} (after {attempt} attempts)")

            body = candidate
            break

        except FetchError:
            raise
        except Exception as exc:  # transport: timeout, reset, DNS, TLS
            warnings.append(f"attempt {attempt}: {type(exc).__name__}: {exc}")
            if attempt >= max_attempts:
                raise FetchError(f"region {region}: {type(exc).__name__}: {exc}") from exc
            _sleep_backoff(attempt)

    assert body is not None
    elapsed = time.monotonic() - started
    digest = hashlib.sha256(body.encode("utf-8", "replace")).hexdigest()

    dm = _RE_DATE_MODIFIED.search(body)
    title = _RE_TITLE.search(body)

    snap = RegionSnapshot(
        region=region,
        region_name=REGIONS[region],
        url=url,
        fetched_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        http_status=status,
        bytes=len(body.encode("utf-8", "replace")),
        sha256=digest,
        date_modified=dm.group(1) if dm else None,
        title=(title.group(1).split("|")[0].strip() if title else ""),
        has_table="<table" in body,
        attempts=attempt,
        elapsed_s=round(elapsed, 3),
        warnings=warnings,
    )

    if write:
        cache_dir = Path(cache_dir)
        raw_dir = cache_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        dest = raw_dir / f"region{region}-eng.html"
        snap.path = str(dest)

        prev = _load_manifest(cache_dir).get(str(region))
        snap.changed = force or not dest.exists() or (prev or {}).get("sha256") != digest
        if snap.changed:
            tmp = dest.with_suffix(".tmp")
            tmp.write_text(body, encoding="utf-8")
            os.replace(tmp, dest)

    return snap


def fetch_all(
    regions: Optional[Iterable[int]] = None,
    *,
    cache_dir: Path = DEFAULT_CACHE,
    force: bool = False,
    throttle: float = _THROTTLE,
) -> List[RegionSnapshot]:
    """Fetch every region, throttled. One region's failure does not abort the rest."""
    todo = sorted(regions) if regions is not None else sorted(REGIONS)
    cache_dir = Path(cache_dir)
    manifest = _load_manifest(cache_dir)
    out: List[RegionSnapshot] = []

    for i, region in enumerate(todo):
        if i:
            time.sleep(throttle)
        try:
            snap = fetch_region(region, cache_dir=cache_dir, force=force)
        except FetchError as exc:
            logger.error("region %d FAILED: %s", region, exc)
            continue
        flag = "changed" if snap.changed else "same"
        logger.info(
            "region %d %-16s %6d B  mod=%s  %s  %.2fs",
            snap.region, snap.region_name, snap.bytes,
            snap.date_modified, flag, snap.elapsed_s,
        )
        manifest[str(region)] = snap.to_dict()
        out.append(snap)

    _write_manifest(cache_dir, manifest)
    return out


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def manifest_path(cache_dir: Path = DEFAULT_CACHE) -> Path:
    return Path(cache_dir) / "manifest.json"


def _load_manifest(cache_dir: Path) -> dict:
    p = manifest_path(cache_dir)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("manifest at %s is corrupt; starting fresh", p)
        return {}


def _write_manifest(cache_dir: Path, manifest: dict) -> None:
    p = manifest_path(cache_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, p)


def load_cached(region: int, cache_dir: Path = DEFAULT_CACHE) -> str:
    """Read a region's cached HTML. Raises FileNotFoundError if never fetched."""
    p = Path(cache_dir) / "raw" / f"region{region}-eng.html"
    return p.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--regions", type=int, nargs="+", choices=sorted(REGIONS), help="default: all")
    ap.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE)
    ap.add_argument("--force", action="store_true", help="rewrite snapshots even if unchanged")
    ap.add_argument("--throttle", type=float, default=_THROTTLE, help="seconds between regions")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    snaps = fetch_all(args.regions, cache_dir=args.cache_dir, force=args.force, throttle=args.throttle)

    want = len(args.regions) if args.regions else len(REGIONS)
    changed = sum(1 for s in snaps if s.changed)
    print(f"\n{len(snaps)}/{want} regions fetched, {changed} changed -> {args.cache_dir}")
    return 0 if len(snaps) == want else 1


if __name__ == "__main__":
    sys.exit(main())
