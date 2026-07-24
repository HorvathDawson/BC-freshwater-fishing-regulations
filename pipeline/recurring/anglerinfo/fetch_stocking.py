#!/usr/bin/env python3
"""
Fish stocking records — FIDQ (BC gov canonical source)
========================================================

Replicated copy: this is `../stocking/fetch_stocking.py`, unchanged except
for DB_PATH — it writes into this directory's shared `anglerinfo.db`
instead of `stocking/`'s own `stocking.db`, so this folder's testbed (see
this dir's README) has stocking/marker data alongside bathymetry and WDIC in
one file. `../stocking/` stays the standalone original; this copy isn't
imported from it, so the two can drift — resync manually if `../stocking/`'s
fetch logic changes and this testbed still needs it.

Fetches **every BC lake FIDQ knows about** and full official stocking
history for the relevant ones, from the province's own Fish Inventories
Data Queries tool (FIDQ,
https://a100.gov.bc.ca/pub/fidq/) — the public front end for the BC
Geographic Warehouse's WHSE_FISH.FISH_RELEASES_PUBLIC_VW view (see the
"Provincial Fish Stocking Reports" dataset in the BC Data Catalogue). This is
the *canonical* government source: every record already carries the same
WATERBODY_IDENTIFIER scheme this project's FWA data and
pipeline/matching/bathymetry_matcher.py use (confirmed live: DEKA LAKE's
FIDQ waterbodyIdentifier is 00154BRID, the same wbid gofishbc's map
independently carries), plus a full watershed code, gazetted name, and
official Fish & Wildlife region on every record.

This is now the **sole** source for waterbody discovery and stocking
records — it replaces the old gofishbc.com-based fetch_stocking.py entirely
for that purpose. gofishbc's "Where To Fish" map is still fetched here (the
`markers` stage) purely for its enrichment fields (amenities, access,
bathymetry PDF links, photos, directions) that FIDQ has no equivalent for;
markers are linked to a FIDQ waterbody by wbid, not used to resolve
anything.

Three stages run by default (`update` runs all three in one go), plus a
fourth (`waterbodies`) that is no longer a mandatory prerequisite for
anything in the default path — see below:
  * `waterbodies` — sweep FIDQ's search by watershed-code prefix (see
    ENUMERATION below) to build a province-wide reference table of every
    lake FIDQ knows about, named or not (136,630 confirmed live) — the
    direct FIDQ equivalent of the WSA bathymetry reference CSV
    (data/fetch_data.py's `bathymetry_maps`). **Not run automatically by
    `update` anymore**: the default `detail` method (bulk Preview, below)
    doesn't need it at all, and match_waterbodies.py now matches directly
    off `fidq_stocking_records` rather than this table too. The one thing
    that still needs it — `detail --legacy`'s scope filter — produces it
    itself (see below), so `fidq_waterbodies` is only ever built where
    something actually needs it; this remains available as a standalone
    command purely for manual/diagnostic use.
  1. `markers` — fetch gofishbc.com's "Where To Fish" map markers (ported
     verbatim from the old fetch_stocking.py) and report how many link to a
     `fidq_waterbodies` row by wbid — a quick local sanity count, not the
     real match (see match_waterbodies.py's `marker_stocking_matches` for
     that); prints a note and skips gracefully if `fidq_waterbodies` hasn't
     been populated, which is now the common case under the default path.
  2. `detail` — fetch full official stocking history. Default: the bulk
     Preview method, self-discovering, no `fidq_waterbodies` dependency at
     all (see BULK DETAIL below). `--legacy`: the original
     one-request-per-waterbody method, scoped to named lakes ∪
     marker-linked lakes — it now calls `discover_waterbodies` itself
     first rather than assuming a separate `waterbodies` run already
     happened, so `--legacy` is fully self-contained on its own.
  3. `records` — bulk-fetch gofishbc.com's own stocking report, one request
     per region (ported from this module's original gofishbc-only design —
     see BULK RECORDS below). Not a dependency for anything else here: a
     cross-check that `detail`'s FIDQ-sourced history is actually capturing
     everything gofishbc itself reports, stored separately in
     `stocking_records` rather than merged into `fidq_stocking_records`.

--------------------------------------------------------------------------
DATA SOURCE
--------------------------------------------------------------------------
FIDQ is a plain session-cookie web app (Java/Tomcat, no auth) — a request's
JSESSIONID cookie is all that's needed to move between its search and detail
forms; the ``;jsessionid=...`` suffix some of its own links carry is only a
cookie-less fallback (confirmed live: a plain cookie-jar session works
without it, so this uses one shared urllib opener + CookieJar for the whole
run rather than parsing jsessionid out of HTML).

  * Search:
      POST https://a100.gov.bc.ca/pub/fidq/searchFishStocking.do
      searchCriteria.watershedCode=<numeric prefix, e.g. "129">
      searchCriteria.waterbodyType=L
      -> HTML page with an embedded ``var data = {...};`` JSON blob:
      records: [{watershedCode, gazettedName, aliases, waterbodyIdentifier,
                 waterbodyType, waterbodyId}, ...]
      ``searchCriteria.watershedCode`` matches on a *prefix* of FWA's own
      numeric WATERSHED_CODE (confirmed live: "129" alone returns every
      lake whose code starts with 129) — a different scheme from the
      4-letter WATERSHED_GROUP_CODE mnemonic embedded in wbid (confirmed
      live: searching "BRID" this way returns 0 rows). A waterbody can also
      be looked up by its exact WATERBODY_IDENTIFIER
      (searchCriteria.waterbodyIdentifier=...) or by a substring of its
      gazetted name/alias (searchCriteria.gazettedName=...).

  * Detail:
      POST https://a100.gov.bc.ca/pub/fidq/infoFishStocking.do
      waterbodyId=<id from search>
      querySelected(DET_SI)=on
      -> HTML page with an embedded ``var dataStocking = {...};`` JSON blob:
      every release for that waterbody back to whenever records begin
      (confirmed live: DEKA LAKE alone has 179 releases), each carrying
      releaseDate, speciesName, broodYear, strainName, sourceName, origin,
      lifeStage, markDescription, releasedQuantity, averageWeight,
      genotypeName, watershedCode, gazettedName, aliases,
      waterbodyIdentifier, wbodyType, wbodyId, primaryRegionCode,
      primaryRegionName, stockId — richer than gofishbc's old report table
      (which had no brood year, source, genotype, watershed code, or
      region).

--------------------------------------------------------------------------
ENUMERATION (watershed-code-prefix sweep)
--------------------------------------------------------------------------
There is no public bulk export for this data: the underlying BCGW view is
"indirect access" only (not on the public WFS service — confirmed live
against openmaps.gov.bc.ca's GetCapabilities), and FIDQ's own "Multiple
Waterbodies Query" bulk option requires an email address and waits for an
async emailed CSV, not something this script automates. Blank/unscoped
searches are also rejected ("Please supply at least one search parameter").

So `waterbodies` sweeps every real 3-digit prefix of FWA's own
WATERSHED_CODE_50K (``_WATERSHED_CODE_PREFIXES`` below — derived once, live,
from data/bc_fisheries_data.gpkg's `lakes` layer; hardcoded here rather than
computed at runtime so this script stays dependency-free, same as before).
One caveat, confirmed live: a "999-999999-..." watershed code is FIDQ's own
sentinel for "unattributed/unlocated" waterbodies — one placeholder entry
per watershed group (93,998 rows live, all sharing the exact same code, all
with a wbid but *no* gazetted name or alias) — so "999" is deliberately
excluded from the prefix list; that sentinel is the only thing skipped.
Every other row is kept, named or not (136,630 confirmed live): only 6,585
carry an actual gazetted name, but a real, distinct lake with a blank
gazetted name is common (confirmed live against gofishbc's own map
markers — 141 of 763 marker wbids only resolved once unnamed rows were
kept too, several with a real *alias* despite no gazetted name), and T1's
identifier join (see match_waterbodies.py) never needs a name at all when
the group code alone is unique. `detail` (stage 3) still scopes its
(network-bound) fetch to a smaller relevant subset — see that stage's own
docstring — but the reference table itself keeps everything.

--------------------------------------------------------------------------
BULK RECORDS (gofishbc, stage 4 — cross-check only)
--------------------------------------------------------------------------
gofishbc's own stocking-report page, server-rendered directly into the page
HTML (a plain GET + HTML table parse), accepts a report **without** a
`waterbody` param — just `reportType=lake&region={REGION}` — returning every
lake's history for that region in one page, filterable by `start`/`end`
(confirmed live: ~1,000-2,200 rows per region for a full 10-year window,
so one request per region safely covers a decade — a request wide enough to
span *every* region at once silently truncates at 10,000 rows, so this
always scopes one region at a time). `fetch_gofishbc_records` reads back
whatever date window the page actually applied (from the date filter's own
echoed input values) so its DELETE-before-insert always matches the exact
range fetched.

--------------------------------------------------------------------------
POLITENESS
--------------------------------------------------------------------------
No throttling/blocking observed live across a handful of rapid sequential
requests (~0.7-1.1s server latency each, no 403s) — still throttled and
retried with backoff, out of politeness and because a full `detail` run is
thousands of requests.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import http.cookiejar
import json
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

from .paths import DB_PATH  # centralized output/pipeline/anglerinfo/ location

FIDQ_BASE = "https://a100.gov.bc.ca/pub/fidq"
SEARCH_URL = FIDQ_BASE + "/searchFishStocking.do"
INFO_URL = FIDQ_BASE + "/infoFishStocking.do"

# gofishbc.com's "Where To Fish" map — kept only for its enrichment fields
# (amenities, access, bathymetry PDF links, photos, directions), which FIDQ
# has no equivalent for. Reused verbatim from the old fetch_stocking.py.
GOFISHBC_BASE = "https://www.gofishbc.com"
MARKERS_URL = GOFISHBC_BASE + "/wp-json/wpgmza/v1/markers"
MARKERS_MAP_ID = "15"
# gofishbc's own bulk stocking report — a cross-check against FIDQ, not a
# dependency for anything else here (see the `records` stage below).
REPORT_URL = GOFISHBC_BASE + "/stocked-fish/"
GOFISHBC_REGIONS = [
    "CARIBOO", "EAST KOOTENAY", "LOWER MAINLAND", "OKANAGAN", "OMINECA",
    "PEACE", "SKEENA", "THOMPSON-NICOLA", "VANCOUVER ISLAND", "WEST KOOTENAY",
]

USER_AGENT = "BC-fishing-regs-stocking-poc/0.1 (proof of concept)"

# One shared cookie jar/opener for the whole process: FIDQ's session cookie
# (JSESSIONID) is all it needs to move between the search and detail forms
# (confirmed live — see module docstring), and http.cookiejar.CookieJar is
# internally lock-protected, so sharing it across the ThreadPoolExecutor's
# worker threads is safe.
_COOKIE_JAR = http.cookiejar.CookieJar()
_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_COOKIE_JAR))

REQUEST_DELAY_S = 0.8
DEFAULT_WORKERS = 8

# Every real 3-digit prefix of FWA's own WATERSHED_CODE_50K among the
# `lakes` layer of data/bc_fisheries_data.gpkg (confirmed live, 111 total
# minus the "999" sentinel — see ENUMERATION above). Derived via:
#   gpd.read_file(gpkg, layer="lakes", columns=["WATERSHED_CODE_50K"])
#       ["WATERSHED_CODE_50K"].dropna().str[:3].unique()
_WATERSHED_CODE_PREFIXES = [
    "100", "110", "119", "120", "128", "129", "150", "160", "170", "180",
    "182", "190", "200", "210", "211", "212", "213", "214", "215", "216",
    "217", "218", "219", "220", "228", "230", "231", "232", "233", "234",
    "235", "236", "237", "238", "239", "300", "310", "320", "330", "340",
    "349", "360", "370", "380", "390", "400", "410", "420", "430", "440",
    "450", "460", "470", "480", "490", "500", "510", "520", "530", "540",
    "550", "560", "570", "580", "590", "600", "610", "620", "630", "640",
    "650", "660", "670", "680", "690", "700", "730", "770", "780", "782",
    "785", "787", "788", "789", "790", "810", "817", "818", "819", "820",
    "830", "836", "837", "838", "839", "900", "905", "910", "915", "920",
    "925", "930", "935", "940", "945", "950", "955", "960", "970", "990",
]


def attribution() -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    return (
        "Extracted from the BC government's Fish Inventories Data Queries "
        f"tool (https://a100.gov.bc.ca/pub/fidq/) on {today}"
    )


def _post(url: str, data: dict, retries: int = 4) -> bytes:
    """POST with retry-with-backoff on 403/429/5xx (see module docstring)."""
    body = urllib.parse.urlencode(data).encode("utf-8")
    last_exc: "Exception | None" = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(
            url, data=body, headers={"User-Agent": USER_AGENT}, method="POST",
        )
        try:
            with _OPENER.open(req, timeout=60) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            last_exc = e
            if e.code in (403, 429, 500, 502, 503, 504) and attempt < retries:
                wait = 8 * attempt
                print(f"  ⚠️  {e} on {url}; retrying in {wait}s "
                      f"({attempt}/{retries - 1})...", file=sys.stderr)
                time.sleep(wait)
                continue
            raise
        except urllib.error.URLError as e:
            last_exc = e
            if attempt < retries:
                wait = 8 * attempt
                print(f"  ⚠️  {e} on {url}; retrying in {wait}s "
                      f"({attempt}/{retries - 1})...", file=sys.stderr)
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"POST failed for {url}: {last_exc}") from last_exc


def _get(url: str, retries: int = 4) -> bytes:
    """GET with retry-with-backoff on 403/429/5xx (see module docstring)."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_exc: "Exception | None" = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            last_exc = e
            if e.code in (403, 429, 500, 502, 503, 504) and attempt < retries:
                wait = 8 * attempt
                print(f"  ⚠️  {e} on {url}; retrying in {wait}s "
                      f"({attempt}/{retries - 1})...", file=sys.stderr)
                time.sleep(wait)
                continue
            raise
        except urllib.error.URLError as e:
            last_exc = e
            if attempt < retries:
                wait = 8 * attempt
                print(f"  ⚠️  {e} on {url}; retrying in {wait}s "
                      f"({attempt}/{retries - 1})...", file=sys.stderr)
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"GET failed for {url}: {last_exc}") from last_exc


# --- Database -----------------------------------------------------------

def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS fidq_waterbodies (
            waterbody_id          TEXT PRIMARY KEY,
            gazetted_name         TEXT,
            aliases                TEXT,
            waterbody_identifier    TEXT,
            watershed_code           TEXT,
            waterbody_type             TEXT,
            fetched_at                  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_fidq_waterbodies_wbid
            ON fidq_waterbodies (waterbody_identifier);
        CREATE INDEX IF NOT EXISTS idx_fidq_waterbodies_name
            ON fidq_waterbodies (gazetted_name);

        CREATE TABLE IF NOT EXISTS map_markers (
            marker_id      TEXT PRIMARY KEY,
            title          TEXT NOT NULL,
            lat            REAL,
            lng            REAL,
            wbid           TEXT,
            stocking_report_url TEXT,
            region         TEXT,
            nearest_town   TEXT,
            surface_area   TEXT,
            elevation      TEXT,
            bathymetry_pdf TEXT,
            road_access    TEXT,
            boat_launch    TEXT,
            fishing_dock   TEXT,
            campsite       TEXT,
            washroom       TEXT,
            wheelchair_access TEXT,
            hike_in_required  TEXT,
            more_info      TEXT,
            description    TEXT,
            photo_url      TEXT,
            gpx_file       TEXT,
            updated_at     TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_map_markers_title ON map_markers (title);
        CREATE INDEX IF NOT EXISTS idx_map_markers_wbid ON map_markers (wbid);

        CREATE TABLE IF NOT EXISTS fidq_stocking_records (
            waterbody_id            TEXT NOT NULL,
            stock_id                TEXT,
            release_date            TEXT,
            species_name            TEXT,
            brood_year              TEXT,
            strain_name             TEXT,
            source_name             TEXT,
            origin                  TEXT,
            life_stage               TEXT,
            mark_description         TEXT,
            released_quantity        INTEGER,
            average_weight            REAL,
            genotype_name             TEXT,
            watershed_code            TEXT,
            gazetted_name             TEXT,
            aliases                   TEXT,
            waterbody_identifier      TEXT,
            wbody_type                TEXT,
            primary_region_code       TEXT,
            primary_region_name       TEXT,
            attribution               TEXT,
            fetched_at                TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_fidq_stocking_wbid
            ON fidq_stocking_records (waterbody_id);

        CREATE TABLE IF NOT EXISTS stocking_records (
            region        TEXT NOT NULL,
            waterbody     TEXT NOT NULL,
            release_date  TEXT,
            nearest_town  TEXT,
            species       TEXT,
            strain        TEXT,
            genotype      TEXT,
            life_stage    TEXT,
            avg_size_g    REAL,
            quantity      INTEGER,
            attribution   TEXT,
            fetched_at    TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_stocking_waterbody
            ON stocking_records (region, waterbody);
        """
    )
    conn.commit()
    return conn


# --- JS-object extraction (both search and detail pages embed one) ----------

# Search's zero-result page and the detail page's zero-history "Stocking
# Information" section use different wording for the same "nothing to show"
# state (confirmed live) — neither ever emits the `var ... = {...};` block.
_NO_RESULTS_MARKERS = ("did not return any results", "0 results found")


def _extract_js_object(html: str, var_name: str) -> dict:
    m = re.search(rf"var\s+{re.escape(var_name)}\s*=\s*(\{{.*?\}});", html, re.S)
    if not m:
        if any(marker in html for marker in _NO_RESULTS_MARKERS):
            return {"records": []}
        raise ValueError(f"No `var {var_name} = {{...}};` block found in response")
    return json.loads(m.group(1))


# --- Search -------------------------------------------------------------

def search_by_watershed_code(prefix: str, waterbody_type: str = "L") -> list[dict]:
    html = _post(SEARCH_URL, {
        "numSearchResults": "-1",
        "searchCriteria.gazettedName": "",
        "searchCriteria.waterbodyIdentifier": "",
        "searchCriteria.watershedCode": prefix,
        "searchCriteria.waterbodyType": waterbody_type,
        "searchCriteria.orderBy": "GN",
    }).decode("utf-8", "replace")
    data = _extract_js_object(html, "data")
    return data.get("records") or []


def search_by_wbid(wbid: str, waterbody_type: str = "L") -> list[dict]:
    html = _post(SEARCH_URL, {
        "numSearchResults": "-1",
        "searchCriteria.gazettedName": "",
        "searchCriteria.waterbodyIdentifier": wbid,
        "searchCriteria.watershedCode": "",
        "searchCriteria.waterbodyType": waterbody_type,
        "searchCriteria.orderBy": "GN",
    }).decode("utf-8", "replace")
    data = _extract_js_object(html, "data")
    return data.get("records") or []


# --- Detail (waterbody_id -> full official stocking history) ----------------

def fetch_stocking_detail(waterbody_id: str) -> list[dict]:
    html = _post(INFO_URL, {
        "waterbodyId": waterbody_id,
        "querySelected(DET_SI)": "on",
    }).decode("utf-8", "replace")
    data = _extract_js_object(html, "dataStocking")
    return data.get("records") or []


# --- Stage 1: waterbodies (watershed-code-prefix sweep) ---------------------

def fetch_all_lake_waterbodies(workers: int = DEFAULT_WORKERS) -> list[dict]:
    """Sweep every real watershed-code prefix and return every lake FIDQ
    knows about, deduped on waterbody_id (see ENUMERATION in the module
    docstring — the "999" sentinel prefix is the only thing excluded; named
    and unnamed rows are both kept, since T1's identifier join never needs
    a name)."""
    print(f"[waterbodies] sweeping {len(_WATERSHED_CODE_PREFIXES)} watershed-code "
          f"prefix(es) on FIDQ ...")

    def _worker(prefix: str) -> tuple[str, list[dict]]:
        records = search_by_watershed_code(prefix)
        time.sleep(REQUEST_DELAY_S)
        return prefix, records

    by_id: dict[str, dict] = {}
    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_worker, p): p for p in _WATERSHED_CODE_PREFIXES}
        for future in concurrent.futures.as_completed(futures):
            prefix = futures[future]
            completed += 1
            try:
                _, records = future.result()
            except Exception as e:  # noqa: BLE001
                print(f"  [{completed}/{len(_WATERSHED_CODE_PREFIXES)}] prefix {prefix!r}: "
                      f"ERROR {e}", file=sys.stderr)
                continue
            for r in records:
                wid = r.get("waterbodyId")
                if not wid:
                    continue
                by_id[wid] = r
            print(f"  [{completed}/{len(_WATERSHED_CODE_PREFIXES)}] prefix {prefix:<4} "
                  f"-> {len(records)} row(s), {len(by_id)} lake(s) so far")

    print(f"[waterbodies] done: {len(by_id)} lake(s) province-wide.")
    return list(by_id.values())


def discover_waterbodies(conn: sqlite3.Connection, workers: int = DEFAULT_WORKERS) -> int:
    now = datetime.now(timezone.utc).isoformat()
    waterbodies = fetch_all_lake_waterbodies(workers=workers)
    conn.execute("DELETE FROM fidq_waterbodies")
    conn.executemany(
        """INSERT INTO fidq_waterbodies
           (waterbody_id, gazetted_name, aliases, waterbody_identifier,
            watershed_code, waterbody_type, fetched_at)
           VALUES (?,?,?,?,?,?,?)""",
        [
            (w["waterbodyId"], w["gazettedName"], w.get("aliases"),
             w.get("waterbodyIdentifier"), w.get("watershedCode"),
             w.get("waterbodyType"), now)
            for w in waterbodies
        ],
    )
    conn.commit()
    return len(waterbodies)


# --- Stage 2: markers (gofishbc.com enrichment, linked to fidq_waterbodies) -

_RE_WBID = re.compile(r"wbid=([A-Za-z0-9]+)")
_RE_BATHY_FILENAME = re.compile(r"filename=([^&\"']+)", re.IGNORECASE)
_RE_HREF = re.compile(r'href="([^"]+)"')


def _custom_field(marker: dict, name: str) -> "str | None":
    for field in marker.get("custom_field_data") or []:
        if field.get("name") == name:
            value = field.get("value")
            return value.strip() if isinstance(value, str) else value
    return None


def _custom_field_any(marker: dict, *names: str) -> "str | None":
    """First non-empty value among several custom-field name variants (the
    site isn't fully consistent — e.g. most markers use "Elevation" but one
    uses "Elevation (Metres)" instead)."""
    for name in names:
        value = _custom_field(marker, name)
        if value:
            return value
    return None


def fetch_map_markers() -> list[dict]:
    """Fetch every lake marker from gofishbc.com's "Where To Fish" map in
    one request (ported verbatim from the old fetch_stocking.py — see that
    module's git history for the original). Each marker's
    ``custom_field_data`` carries the fields parsed here: the wbid and
    hyperlink-ready URL (from a "Stocking Report Link"), a bathymetric
    depth-map PDF filename (from a "Bathymetric Map Link"), region/
    nearest-town/surface-area/elevation/access text, and amenity/access
    fields (Campsite, Washroom, Wheelchair Access, Hike-in Required, More
    Info driving/trail directions, a GPX track filename). ``description``
    and ``pic`` (a representative photo URL) come from the marker itself,
    not its custom fields. ~17 of ~780 markers have no wbid (blank Stocking
    Report Link) — those still carry a name and precise lat/lng, just no
    link to a fidq_waterbodies row.
    """
    url = f"{MARKERS_URL}?map_id={MARKERS_MAP_ID}"
    raw = _get(url).decode("utf-8", "replace")
    markers = json.loads(raw)

    out = []
    for m in markers:
        title = m.get("title")
        lat, lng = m.get("lat"), m.get("lng")
        if not title or lat is None or lng is None:
            continue
        report_link = _custom_field(m, "Stocking Report Link") or ""
        wbid_match = _RE_WBID.search(report_link)
        href_match = _RE_HREF.search(report_link)
        report_url = None
        if href_match:
            href = href_match.group(1)
            report_url = href if href.startswith("http") else GOFISHBC_BASE + href
        bathy_link = _custom_field(m, "Bathymetric Map Link") or ""
        pdf_match = _RE_BATHY_FILENAME.search(bathy_link)
        out.append({
            "marker_id": str(m.get("id") or ""),
            "title": title.strip(),
            "lat": float(lat),
            "lng": float(lng),
            "wbid": wbid_match.group(1) if wbid_match else None,
            "stocking_report_url": report_url,
            "region": _custom_field(m, "Region"),
            "nearest_town": _custom_field(m, "Nearest Town"),
            "surface_area": _custom_field_any(m, "Surface Area", "Surface Area (Hectares)"),
            "elevation": _custom_field_any(m, "Elevation", "Elevation (Metres)"),
            "bathymetry_pdf": pdf_match.group(1) if pdf_match else None,
            "road_access": _custom_field(m, "Road Access"),
            "boat_launch": _custom_field(m, "Boat Launch"),
            "fishing_dock": _custom_field(m, "Fishing Dock"),
            "campsite": _custom_field(m, "Campsite"),
            "washroom": _custom_field(m, "Washroom"),
            "wheelchair_access": _custom_field(m, "Wheelchair Access"),
            "hike_in_required": _custom_field(m, "Hike-in Required"),
            "more_info": _custom_field(m, "More Info"),
            "description": (m.get("description") or "").strip() or None,
            "photo_url": m.get("pic") or None,
            "gpx_file": _custom_field(m, "GPX File"),
        })
    return out


def discover_map_markers(conn: sqlite3.Connection) -> int:
    now = datetime.now(timezone.utc).isoformat()
    markers = fetch_map_markers()
    conn.executemany(
        """INSERT INTO map_markers
           (marker_id, title, lat, lng, wbid, stocking_report_url, region,
            nearest_town, surface_area, elevation, bathymetry_pdf,
            road_access, boat_launch, fishing_dock, campsite, washroom,
            wheelchair_access, hike_in_required, more_info, description,
            photo_url, gpx_file, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(marker_id) DO UPDATE SET
             title = excluded.title, lat = excluded.lat, lng = excluded.lng,
             wbid = excluded.wbid, stocking_report_url = excluded.stocking_report_url,
             region = excluded.region,
             nearest_town = excluded.nearest_town,
             surface_area = excluded.surface_area, elevation = excluded.elevation,
             bathymetry_pdf = excluded.bathymetry_pdf,
             road_access = excluded.road_access, boat_launch = excluded.boat_launch,
             fishing_dock = excluded.fishing_dock, campsite = excluded.campsite,
             washroom = excluded.washroom, wheelchair_access = excluded.wheelchair_access,
             hike_in_required = excluded.hike_in_required, more_info = excluded.more_info,
             description = excluded.description, photo_url = excluded.photo_url,
             gpx_file = excluded.gpx_file, updated_at = excluded.updated_at""",
        [
            (m["marker_id"], m["title"], m["lat"], m["lng"], m["wbid"],
             m["stocking_report_url"], m["region"], m["nearest_town"],
             m["surface_area"], m["elevation"], m["bathymetry_pdf"],
             m["road_access"], m["boat_launch"], m["fishing_dock"],
             m["campsite"], m["washroom"], m["wheelchair_access"],
             m["hike_in_required"], m["more_info"], m["description"],
             m["photo_url"], m["gpx_file"], now)
            for m in markers
        ],
    )
    conn.commit()
    with_wbid = sum(1 for m in markers if m["wbid"])
    print(f"[markers] {len(markers)} lake marker(s) ({with_wbid} with a wbid).")
    _report_marker_links(conn)
    return len(markers)


def _report_marker_links(conn: sqlite3.Connection) -> None:
    """Local join (no network) reporting how many map markers' wbids land
    on a row in fidq_waterbodies — the "linking" step. Requires
    `waterbodies` to have been run first; prints a note and skips
    otherwise rather than failing the `markers` stage over it."""
    fidq_count = conn.execute("SELECT COUNT(*) FROM fidq_waterbodies").fetchone()[0]
    if fidq_count == 0:
        print("[markers] fidq_waterbodies is empty — run `waterbodies` first to link markers.")
        return
    fidq_wbids = {row[0] for row in conn.execute(
        "SELECT waterbody_identifier FROM fidq_waterbodies WHERE waterbody_identifier IS NOT NULL"
    )}
    marker_rows = conn.execute(
        "SELECT title, wbid FROM map_markers WHERE wbid IS NOT NULL AND wbid != ''"
    ).fetchall()
    linked = [t for t, w in marker_rows if w in fidq_wbids]
    unlinked = [(t, w) for t, w in marker_rows if w not in fidq_wbids]
    print(f"[markers] linked to fidq_waterbodies: {len(linked)}/{len(marker_rows)} "
          f"marker(s) with a wbid ({len(unlinked)} not found in FIDQ's catalogue at all).")
    if unlinked:
        print("  unlinked (wbid not found among fidq_waterbodies):")
        for title, wbid in sorted(unlinked):
            print(f"    {title:<30} | {wbid}")


# --- Stage: gofishbc bulk records (cross-check that FIDQ captured -----------
#     everything gofishbc itself reports) -------------------------------

def _gofishbc_attribution() -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    return (
        "Extracted from the Freshwater Fisheries Society of BC's GoFishBC "
        f"stocking report tool (https://www.gofishbc.com/stocked-fish/) on {today}"
    )


class _ReportTableParser(HTMLParser):
    """Extract <tr><td>...</td></tr> rows from <table id="report_table">."""

    def __init__(self):
        super().__init__()
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._current_row: list[str] = []
        self._current_cell = ""
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table" and attrs_dict.get("id") == "report_table":
            self._in_table = True
        elif self._in_table and tag == "tr":
            self._in_row = True
            self._current_row = []
        elif self._in_row and tag == "td":
            self._in_cell = True
            self._current_cell = ""

    def handle_endtag(self, tag):
        if tag == "table":
            self._in_table = False
        elif tag == "tr" and self._in_row:
            if self._current_row:
                self.rows.append(self._current_row)
            self._in_row = False
        elif tag == "td" and self._in_cell:
            self._current_row.append(self._current_cell.strip())
            self._in_cell = False

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell += data


_REPORT_COLUMNS = (
    "release_date", "waterbody", "nearest_town", "species", "strain",
    "genotype", "life_stage", "avg_size_g", "quantity",
)
_RE_START_DATE = re.compile(r'id="startDate"[^>]*value="([^"]+)"')
_RE_END_DATE = re.compile(r'id="endDate"[^>]*value="([^"]+)"')


def _num(s: str) -> "float | None":
    # Strip thousands separators first, else re.search below matches only the
    # leading group ("10,000" -> "10") and silently truncates the quantity.
    s = s.strip().replace(",", "")
    if not s:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group()) if m else None


def parse_report_html(html: str) -> list[dict]:
    parser = _ReportTableParser()
    parser.feed(html)
    out = []
    for row in parser.rows:
        if len(row) != len(_REPORT_COLUMNS):
            continue  # defensive: skip malformed rows rather than crash
        rec = dict(zip(_REPORT_COLUMNS, row))
        rec["avg_size_g"] = _num(rec["avg_size_g"])
        rec["quantity"] = int(_num(rec["quantity"]) or 0)
        out.append(rec)
    return out


def fetch_stocking_report_bulk(
    region: str, start: "str | None" = None, end: "str | None" = None,
) -> "tuple[list[dict], str, str]":
    """One region's full stocking history for a date window, in one request
    (see live-data/stocking/README.md for the 10,000-row cap and why this
    stays scoped to one region at a time)."""
    params = {"reportType": "lake", "region": region}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    url = f"{REPORT_URL}?{urllib.parse.urlencode(params)}"
    html = _get(url).decode("utf-8", "replace")
    rows = parse_report_html(html)
    start_match = _RE_START_DATE.search(html)
    end_match = _RE_END_DATE.search(html)
    actual_start = start_match.group(1) if start_match else (start or "")
    actual_end = end_match.group(1) if end_match else (end or "")
    return rows, actual_start, actual_end


def fetch_gofishbc_records(
    conn: sqlite3.Connection, regions: "list[str] | None" = None,
    start: "str | None" = None, end: "str | None" = None,
    workers: int = DEFAULT_WORKERS,
) -> int:
    """Bulk-fetch gofishbc's own stocking report, one request per region —
    a cross-check that FIDQ's per-waterbody detail (stage 3) is capturing
    everything gofishbc itself reports, not a data source anything else here
    depends on."""
    regions = regions or GOFISHBC_REGIONS
    attribution_str = _gofishbc_attribution()
    now = datetime.now(timezone.utc).isoformat()
    total = 0

    print(f"[records] {len(regions)} gofishbc region(s), bulk fetch, {workers} worker(s) ...")

    def _worker(region: str) -> "tuple[str, list[dict], str, str]":
        rows, actual_start, actual_end = fetch_stocking_report_bulk(region, start, end)
        time.sleep(REQUEST_DELAY_S)
        return region, rows, actual_start, actual_end

    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_worker, region): region for region in regions}
        for future in concurrent.futures.as_completed(futures):
            region = futures[future]
            completed += 1
            try:
                _, rows, actual_start, actual_end = future.result()
            except Exception as e:  # noqa: BLE001
                print(f"  [{completed}/{len(regions)}] {region}: ERROR {e}", file=sys.stderr)
                continue

            if actual_start and actual_end:
                conn.execute(
                    "DELETE FROM stocking_records WHERE region = ? "
                    "AND release_date >= ? AND release_date <= ?",
                    (region, actual_start, actual_end),
                )
            else:
                conn.execute("DELETE FROM stocking_records WHERE region = ?", (region,))
            conn.executemany(
                """INSERT INTO stocking_records
                   (region, waterbody, release_date, nearest_town, species,
                    strain, genotype, life_stage, avg_size_g, quantity,
                    attribution, fetched_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (region, r["waterbody"], r["release_date"], r["nearest_town"], r["species"],
                     r["strain"], r["genotype"], r["life_stage"], r["avg_size_g"],
                     r["quantity"], attribution_str, now)
                    for r in rows
                ],
            )
            conn.commit()
            total += len(rows)
            print(f"  [{completed}/{len(regions)}] {region}: {len(rows)} record(s) "
                  f"({actual_start} to {actual_end})")

    print(f"[records] done: {total} record(s) stored across {len(regions)} region(s).")
    return total


# --- Stage 3: detail (fetch full history for the relevant subset) -----------

def _warm_up_session() -> None:
    """Establish a FIDQ session before any detail fetch.

    Confirmed live: ``infoFishStocking.do`` (detail) 500s every time on a
    cold session that has never hit ``searchFishStocking.do`` (search) —
    the server evidently keys some session-side state off the search step,
    and jumping straight to detail (as a standalone `detail` invocation
    would, in a fresh process with a fresh cookie jar) trips it. One cheap
    search is enough to unblock every subsequent detail call for the rest
    of the process (shared cookie jar — see module docstring).
    """
    search_by_wbid("00154BRID")  # DEKA LAKE — small, known-good response


def fetch_all_details_legacy(conn: sqlite3.Connection, workers: int = DEFAULT_WORKERS) -> int:
    """Original `detail` implementation: one infoFishStocking.do request per
    waterbody. Superseded by `fetch_all_details_bulk` (~7x faster, same
    data — confirmed live, see test_bulk_preview.py) as the default; kept
    here and reachable via `detail --legacy` as a fallback/cross-check, not
    because it's still the better choice day to day.

    Self-contained: populates `fidq_waterbodies` itself (via
    `discover_waterbodies`) rather than assuming a separate `waterbodies`
    run already happened — this is the only thing in the default pipeline
    that still needs that table (see the module docstring), so it's
    produced here, on demand, rather than as an always-run stage 1 for
    everyone.

    Scoped to a relevant subset rather than the entire (136,630-row, mostly
    unnamed/insignificant) reference table: every waterbody with an actual
    gazetted name (~6,585), unioned with any waterbody a gofishbc map
    marker's wbid points at even without one (~141 confirmed live — see
    `markers`). This keeps the fetch to a few thousand requests instead of
    six figures while still covering everything either source considers
    worth naming or mapping.
    """
    discover_waterbodies(conn, workers=workers)
    print()
    _warm_up_session()
    attribution_str = attribution()
    now = datetime.now(timezone.utc).isoformat()
    waterbody_ids = [
        row[0] for row in conn.execute(
            """SELECT waterbody_id FROM fidq_waterbodies
               WHERE (gazetted_name IS NOT NULL AND gazetted_name != '')
                  OR waterbody_identifier IN (
                       SELECT wbid FROM map_markers
                       WHERE wbid IS NOT NULL AND wbid != ''
                     )"""
        )
    ]
    print(f"[detail] {len(waterbody_ids)} relevant waterbod(y/ies) (named or "
          f"marker-linked) to fetch full stocking history for ...")

    def _worker(wid: str) -> tuple[str, list[dict]]:
        records = fetch_stocking_detail(wid)
        time.sleep(REQUEST_DELAY_S)
        return wid, records

    completed = 0
    total = 0
    n_with_records = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_worker, wid): wid for wid in waterbody_ids}
        for future in concurrent.futures.as_completed(futures):
            wid = futures[future]
            completed += 1
            try:
                _, records = future.result()
            except Exception as e:  # noqa: BLE001
                print(f"  [{completed}/{len(waterbody_ids)}] waterbody_id={wid}: ERROR {e}", file=sys.stderr)
                continue
            conn.execute("DELETE FROM fidq_stocking_records WHERE waterbody_id = ?", (wid,))
            if records:
                n_with_records += 1
                conn.executemany(
                    """INSERT INTO fidq_stocking_records
                       (waterbody_id, stock_id, release_date, species_name, brood_year,
                        strain_name, source_name, origin, life_stage, mark_description,
                        released_quantity, average_weight, genotype_name, watershed_code,
                        gazetted_name, aliases, waterbody_identifier, wbody_type,
                        primary_region_code, primary_region_name, attribution, fetched_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    [
                        (wid, r.get("stockId"), r.get("releaseDate"), r.get("speciesName"),
                         r.get("broodYear"), r.get("strainName"), r.get("sourceName"), r.get("origin"),
                         r.get("lifeStage"), r.get("markDescription"),
                         int(r["releasedQuantity"]) if r.get("releasedQuantity") not in (None, "") else None,
                         float(r["averageWeight"]) if r.get("averageWeight") not in (None, "") else None,
                         r.get("genotypeName"), r.get("watershedCode"), r.get("gazettedName"),
                         r.get("aliases"), r.get("waterbodyIdentifier"), r.get("wbodyType"),
                         r.get("primaryRegionCode"), r.get("primaryRegionName"), attribution_str, now)
                        for r in records
                    ],
                )
            conn.commit()
            total += len(records)
            if completed % 200 == 0 or completed == len(waterbody_ids):
                print(f"  [{completed}/{len(waterbody_ids)}] ... {total} record(s), "
                      f"{n_with_records} waterbod(y/ies) with history so far")

    print(f"[detail] done: {total} record(s) across {n_with_records}/{len(waterbody_ids)} "
          f"waterbod(y/ies) with any stocking history.")
    return total


# --- Stage 3 (default): bulk detail via the Multiple Waterbodies Query's --
#     Preview action — one request per watershed group instead of one per --
#     waterbody. See test_bulk_preview.py's module docstring for how this --
#     was found and confirmed live (~7x faster, identical record counts). --

MULTI_URL = "https://a100.gov.bc.ca/pub/fidq/searchMultipleWaterbodies.do"
PREVIEW_CAP = 4000

# Wide enough that even a busy watershed group's total per chunk stays
# comfortably under PREVIEW_CAP — confirmed live for OKAN, the busiest
# group hit in testing: split into these four chunks it totals 5,230
# records, more than the single uncapped request's capped-at-4,000 count.
_PREVIEW_DATE_CHUNKS = [
    ("01-Jan-1900", "31-Dec-1979"),
    ("01-Jan-1980", "31-Dec-1999"),
    ("01-Jan-2000", "31-Dec-2009"),
    ("01-Jan-2010", "31-Dec-2026"),
]


def _fix_bulk_escapes(blob: str) -> str:
    """Preview's JSON has a stray backslash before HTML entities (e.g.
    ``\\&#039;`` for an apostrophe) — invalid JSON otherwise. Confirmed
    live only on this endpoint (the per-waterbody detail endpoint's own
    output is clean). Also decodes the entity itself (``&#039;`` -> ``'``)
    — the fix above alone leaves valid-but-still-HTML-encoded JSON string
    values (confirmed live: FIDQ waterbody 37841's own alias is literally
    stored as ``BOBB&#039;S``, not ``BOBB'S``, across 751 rows), which would
    otherwise leak the raw entity into gazetted_name/aliases and anything
    downstream that displays them verbatim (e.g. a promoted display_name)."""
    return html.unescape(blob.replace("\\&", "&"))


def fetch_watershed_groups() -> list[str]:
    """Every watershed-group code FIDQ's own Multiple Waterbodies Query
    dropdown offers (250 confirmed live) — the enumeration scope for the
    bulk Preview method below."""
    html_text = _get("https://a100.gov.bc.ca/pub/fidq/viewMultipleWaterbodies.do").decode("utf-8", "replace")
    return re.findall(r'<option value="([A-Z]{4})">[A-Z]{4} - ', html_text)


def _fetch_group_chunk(
    group: str, from_date: "str | None" = None, to_date: "str | None" = None,
) -> list[dict]:
    payload = {
        "numSearchResults": "-1",
        "searchCriteria.action": "PREVIEW",
        "searchCriteria.queryType": "DET_SI",
        "searchCriteria.watershedCode": "",
        "searchCriteria.waterbodyType": "L",
        "searchCriteria.watershedGroup": group,
        "searchCriteria.emailAddress": "",
    }
    if from_date:
        payload["searchCriteria.fromDate"] = from_date
    if to_date:
        payload["searchCriteria.toDate"] = to_date
    html_text = _post(MULTI_URL, payload).decode("utf-8", "replace")
    m = re.search(r"var\s+dataStocking\s*=\s*(\{.*?\});", html_text, re.S)
    if not m:
        # Confirmed live: a genuine zero-result Preview (common for groups
        # with no lakes at all, e.g. marine/species-named groups like
        # "CYAG" - Shiner Perch) just re-renders the blank form — no
        # distinguishing marker text at all, unlike the search/detail
        # endpoints' explicit "did not return any results" / "0 results
        # found". Since this request shape has no other failure mode
        # observed live, treat "no block" as empty rather than an error.
        return []
    data = json.loads(_fix_bulk_escapes(m.group(1)))
    return data.get("records") or []


def fetch_group_stocking(group: str) -> list[dict]:
    """All stocking records for one watershed group, in one request unless
    that request hits the 4,000-row Preview cap — then re-fetched in date
    chunks instead (see _PREVIEW_DATE_CHUNKS)."""
    records = _fetch_group_chunk(group)
    if len(records) < PREVIEW_CAP:
        return records
    chunked: list[dict] = []
    for start, end in _PREVIEW_DATE_CHUNKS:
        chunked.extend(_fetch_group_chunk(group, start, end))
    return chunked


def fetch_all_details_bulk(conn: sqlite3.Connection, workers: int = DEFAULT_WORKERS) -> int:
    """Default `detail` implementation: one request per watershed group
    (250, ~300 counting date-chunked busy groups) instead of one request
    per waterbody (~6,700) — confirmed live ~7x faster with identical
    per-waterbody record counts (test_bulk_preview.py). Unlike the legacy
    method, this needs no pre-guessed "relevant subset": every waterbody
    that actually has stocking history simply shows up in its group's
    results, so nothing is scoped/filtered going in.
    """
    _warm_up_session()
    attribution_str = attribution()
    now = datetime.now(timezone.utc).isoformat()
    groups = fetch_watershed_groups()
    print(f"[detail] {len(groups)} watershed group(s) to fetch via bulk Preview ...")

    def _worker(group: str) -> tuple[str, list[dict]]:
        records = fetch_group_stocking(group)
        time.sleep(REQUEST_DELAY_S)
        return group, records

    completed = 0
    total = 0
    by_waterbody: "dict[str, list[dict]]" = defaultdict(list)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_worker, g): g for g in groups}
        for future in concurrent.futures.as_completed(futures):
            group = futures[future]
            completed += 1
            try:
                _, records = future.result()
            except Exception as e:  # noqa: BLE001
                print(f"  [{completed}/{len(groups)}] group={group}: ERROR {e}", file=sys.stderr)
                continue
            for r in records:
                wid = r.get("wbody_id")
                if wid:
                    by_waterbody[wid].append(r)
            total += len(records)
            if completed % 25 == 0 or completed == len(groups):
                print(f"  [{completed}/{len(groups)}] ... {total} record(s), "
                      f"{len(by_waterbody)} waterbod(y/ies) so far")

    print(f"[detail] fetched {total} record(s) across {len(by_waterbody)} waterbod(y/ies); "
          f"writing to anglerinfo.db ...")
    for wid, records in by_waterbody.items():
        conn.execute("DELETE FROM fidq_stocking_records WHERE waterbody_id = ?", (wid,))
        conn.executemany(
            """INSERT INTO fidq_stocking_records
               (waterbody_id, stock_id, release_date, species_name, brood_year,
                strain_name, source_name, origin, life_stage, mark_description,
                released_quantity, average_weight, genotype_name, watershed_code,
                gazetted_name, aliases, waterbody_identifier, wbody_type,
                primary_region_code, primary_region_name, attribution, fetched_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                (wid, r.get("stock_id"), r.get("release_date"), r.get("species_name"),
                 r.get("brood_year"), r.get("strain_name"), r.get("source_name"), r.get("origin"),
                 r.get("life_stage"), r.get("mark_description"),
                 int(r["released_quantity"]) if r.get("released_quantity") not in (None, "") else None,
                 float(r["average_weight"]) if r.get("average_weight") not in (None, "") else None,
                 r.get("genotype_name"), r.get("watershed_code"), r.get("gazetted_name"),
                 r.get("aliases"), r.get("waterbody_identifier"), r.get("wbody_type"),
                 r.get("primary_region_code"), r.get("primary_region_name"), attribution_str, now)
                for r in records
            ],
        )
    conn.commit()
    print(f"[detail] done: {total} record(s) across {len(by_waterbody)} waterbod(y/ies) "
          f"with any stocking history.")
    return total


# --- Workflow -----------------------------------------------------------

def run_update(conn: sqlite3.Connection, workers: int = DEFAULT_WORKERS, legacy: bool = False) -> None:
    print("=== UPDATING FIDQ STOCKING DATA ===")
    # No `discover_waterbodies` here anymore: the default bulk `detail`
    # below is self-discovering and doesn't need `fidq_waterbodies` at all;
    # `--legacy` populates it itself on demand (see fetch_all_details_legacy).
    discover_map_markers(conn)
    print()
    if legacy:
        fetch_all_details_legacy(conn, workers=workers)
    else:
        fetch_all_details_bulk(conn, workers=workers)
    print()
    fetch_gofishbc_records(conn, workers=workers)
    print("=== UPDATE COMPLETE ===")


# --- CLI ---------------------------------------------------------------------

def main(argv: "list[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(description="BC fish stocking record fetcher (FIDQ, canonical gov source)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_update = sub.add_parser(
        "update",
        help="Single run-everything workflow: waterbodies, markers, detail, then "
             "gofishbc's own bulk records as a cross-check.",
    )
    p_update.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    p_update.add_argument(
        "--legacy", action="store_true",
        help="Use the old one-request-per-waterbody detail fetch instead of the "
             "default bulk per-watershed-group Preview method (~7x slower).",
    )

    p_wb = sub.add_parser(
        "waterbodies",
        help="Sweep FIDQ by watershed-code prefix for every lake (stage 1)",
    )
    p_wb.add_argument("--workers", type=int, default=DEFAULT_WORKERS)

    sub.add_parser("markers", help="Fetch gofishbc.com map markers and link them to fidq_waterbodies (stage 2)")

    p_detail = sub.add_parser(
        "detail",
        help="Fetch full stocking history (stage 3) — bulk per-watershed-group by "
             "default, one request per waterbody with --legacy.",
    )
    p_detail.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    p_detail.add_argument(
        "--legacy", action="store_true",
        help="Use the old one-request-per-waterbody detail fetch instead of the "
             "default bulk per-watershed-group Preview method (~7x slower).",
    )

    p_records = sub.add_parser(
        "records",
        help="Bulk-fetch gofishbc's own stocking report per region — a cross-check "
             "that FIDQ's detail (stage 3) captured everything gofishbc itself reports.",
    )
    p_records.add_argument("--region", choices=GOFISHBC_REGIONS, help="limit to one region (default: all)")
    p_records.add_argument("--start", default=None, help="YYYY-MM-DD (default: gofishbc's own default — last 10 years)")
    p_records.add_argument("--end", default=None, help="YYYY-MM-DD (default: today)")
    p_records.add_argument("--workers", type=int, default=DEFAULT_WORKERS)

    args = parser.parse_args(argv)
    conn = connect()
    try:
        if args.command == "update":
            run_update(conn, workers=args.workers, legacy=args.legacy)
        elif args.command == "waterbodies":
            n = discover_waterbodies(conn, workers=args.workers)
            print(f"[waterbodies] {n} lake(s) stored.")
        elif args.command == "markers":
            discover_map_markers(conn)
        elif args.command == "detail":
            if args.legacy:
                fetch_all_details_legacy(conn, workers=args.workers)
            else:
                fetch_all_details_bulk(conn, workers=args.workers)
        elif args.command == "records":
            regions = [args.region] if args.region else GOFISHBC_REGIONS
            fetch_gofishbc_records(conn, regions, start=args.start, end=args.end, workers=args.workers)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
