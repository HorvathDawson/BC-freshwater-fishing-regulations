"""T237: THE REPORTS TAB — problem reports the app sent (`POST /report` on the feeds worker), read back for review.

The reports live in the staging bucket's PRIVATE part (`private/reports/<day>/<id>.json`); the feeds worker serves them
to the run token only (`GET /reports?since=`). This module fetches them (no process is started: an HTTPS GET) and
says, for each, which water and which piece of it the report was about on THIS review app's build:

    fetch(since)            the worker's reports, newest first (raises ReportsUnavailable with the reason)
    enrich(report)          + `item` (the water's name / kind from the registry), and, when the report's map tiles are
                            this build's vintage (the tiles id starts with the section-handle digest), the tapped
                            section's node id and the water(s) it belongs to. A report from another vintage says so:
                            its handle names a different piece here and is never resolved.
    section_geojson(sid, tiles)   the tapped section's geometry (the map's highlight), same vintage rule

READ ONLY: nothing is written here. The reviewer's status (new / looking / fixed / won't fix) is kept in the browser
(localStorage), per viewer — it is a to-do list, never a mark (AGENTS 66).

Config: CURATION_REPORTS_URL (default: the staging feeds worker), CURATION_REPORTS_TOKEN (default: the run token in
app/workers/feeds/.run-token, git-ignored).
"""
from __future__ import annotations

import json
import os
import sqlite3
import urllib.error
import urllib.request
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

import reuse

DEFAULT_URL = "https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/reports"
_TOKEN_FILE = Path(__file__).resolve().parents[2] / "app" / "workers" / "feeds" / ".run-token"


class ReportsUnavailable(Exception):
    """The worker could not be asked (no token, offline, refused)."""


def _url() -> str:
    return os.environ.get("CURATION_REPORTS_URL") or DEFAULT_URL


def _token() -> str | None:
    t = os.environ.get("CURATION_REPORTS_TOKEN")
    if t:
        return t.strip()
    try:
        return _TOKEN_FILE.read_text(encoding="utf-8").strip() or None
    except OSError:
        return None


def fetch(since: str | None = None, opener=None) -> list[dict]:
    """Every report the worker holds since `since` (YYYY-MM-DD; default 90 days back), newest first."""
    since = since or (date.today() - timedelta(days=90)).isoformat()
    tok = _token()
    if not tok:
        raise ReportsUnavailable(f"no run token (set CURATION_REPORTS_TOKEN or write {_TOKEN_FILE})")
    req = urllib.request.Request(f"{_url()}?since={since}", headers={"Authorization": f"Bearer {tok}",
                                                                     "User-Agent": "curation-review/reports"})
    try:
        with (opener or urllib.request.urlopen)(req, timeout=30) as r:
            body = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise ReportsUnavailable(f"the worker answered HTTP {e.code}") from e
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise ReportsUnavailable(f"the worker could not be reached ({e})") from e
    reps = body.get("reports") if isinstance(body, dict) else None
    if not isinstance(reps, list):
        raise ReportsUnavailable("the worker's answer holds no report list")
    return reps


# ── the build this app serves: its section-handle vintage, and the bundle beside it ──────────────────────────────────

@lru_cache(maxsize=1)
def _vintage() -> str | None:
    """The served build's section-handle digest (the tiles id's prefix), or None when it cannot be read."""
    try:
        from pipeline.common.section_handles import digest_for
        return digest_for(reuse._BUILD)
    except (SystemExit, OSError):
        return None


def _same_vintage(tiles: str | None) -> bool:
    v = _vintage()
    return bool(tiles and v and tiles.split(".", 1)[0] == v)


def _node_of(sid: int) -> str | None:
    from pipeline.common.section_handles import id_at, read
    try:
        return id_at(read(reuse._BUILD)[0], int(sid))
    except (KeyError, SystemExit, OSError, ValueError):
        return None


def _bundle_items(sid: int) -> list[str]:
    """The waters the bundle files this section under (the bundle shares the build's handle table)."""
    from pipeline.deliver.bundle import read as R
    try:
        db = sqlite3.connect(f"file:{R.BUNDLE}?mode=ro", uri=True)
    except sqlite3.Error:
        return []
    try:
        meta = dict(db.execute("SELECT k, v FROM meta WHERE k = 'section_handles'"))
        if meta.get("section_handles") != _vintage():
            return []
        return [r[0] for r in db.execute("SELECT i.item_id FROM item_section x JOIN item i ON i.ord = x.ord "
                                         "WHERE x.sid = ? ORDER BY i.item_id", (int(sid),))]
    except sqlite3.Error:
        return []
    finally:
        db.close()


def _item_view(item_id: str | None) -> dict | None:
    if not item_id:
        return None
    it = reuse._registry().get(item_id)
    if it is None:
        return {"id": item_id, "name": None, "kind": None, "known": False}
    return {"id": item_id, "name": it.name, "kind": it.kind, "known": True}


def enrich(r: dict) -> dict:
    """The report, plus what this build says about where it points."""
    c = r.get("context") or {}
    sid = c.get("section")
    same = _same_vintage(c.get("tiles"))
    section = None
    if isinstance(sid, int):
        if same:
            section = {"sid": sid, "node": _node_of(sid), "items": _bundle_items(sid), "vintage": "same"}
        else:
            section = {"sid": sid, "node": None, "items": [],
                       "vintage": "unknown" if not c.get("tiles") else "other"}
    water = c.get("water") or (section["items"][0] if section and section["items"] else None)
    return {**r, "item": _item_view(water), "section": section}


def section_geojson(sid: int, tiles: str) -> dict:
    """The tapped section as map geometry, or an empty collection when it is another vintage's handle."""
    if not _same_vintage(tiles):
        return {"type": "FeatureCollection", "features": [], "vintage": "other"}
    node = _node_of(sid)
    if not node:
        return {"type": "FeatureCollection", "features": [], "vintage": "same"}
    return reuse._json_safe({"type": "FeatureCollection", "features": reuse._sections_geojson([node], "preview"),
                             "vintage": "same", "node": node})
