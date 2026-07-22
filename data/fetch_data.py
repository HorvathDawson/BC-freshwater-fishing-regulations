import os
import json
import argparse
import logging
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
import zipfile
import shutil
import fiona
import geopandas as gpd
import pandas as pd
import osmnx as ox
from pathlib import Path
from shapely.geometry import Point, Polygon, LineString
from shapely.ops import polygonize, linemerge
from tqdm import tqdm

from project_config import get_config

logger = logging.getLogger(__name__)

# Some CDNs (e.g. Cloudflare in front of R2) reject the default
# ``Python-urllib`` User-Agent with HTTP 403, so downloads send a descriptive
# agent instead.
_DOWNLOAD_USER_AGENT = "BC-FishRegs-DataFetch/1.0"


def _download_with_progress(url, dest_path, desc=None):
    """Download ``url`` to ``dest_path`` showing a tqdm byte progress bar.

    Sends an explicit User-Agent so CDN-fronted hosts (e.g. Cloudflare/R2)
    don't reject the request with HTTP 403. Falls back gracefully when the
    server does not report a content length (e.g. some FTP responses), in
    which case the bar tracks bytes downloaded without a known total.
    """
    desc = desc or os.path.basename(str(dest_path))
    req = urllib.request.Request(
        url, headers={"User-Agent": _DOWNLOAD_USER_AGENT}
    )
    chunk_size = 1 << 16
    with urllib.request.urlopen(req) as resp:
        total = int(resp.headers.get("Content-Length") or 0)
        with open(dest_path, "wb") as out, tqdm(
            total=total or None,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            miniters=1,
            desc=desc,
        ) as bar:
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                out.write(chunk)
                bar.update(len(chunk))


def _download_quiet(url, dest_path, timeout=60):
    """Download ``url`` to ``dest_path`` without a per-file progress bar.

    Intended for bulk loops (e.g. thousands of small PDFs) where an outer
    progress bar tracks overall completion and per-file bars would be noise.
    Sends the same descriptive User-Agent as ``_download_with_progress`` so
    CDN-fronted hosts don't reject the request with HTTP 403.
    """
    req = urllib.request.Request(
        url, headers={"User-Agent": _DOWNLOAD_USER_AGENT}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp, open(
        dest_path, "wb"
    ) as out:
        shutil.copyfileobj(resp, out)


_OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def _overpass_query(query: str, what: str, timeout: int = 240, retries: int = 3) -> dict:
    """POST an Overpass QL query, retrying transient server errors.

    The public overpass-api.de instance is shared infrastructure and
    occasionally returns 502/503/504 or times out under load, independent of
    whether the query itself is fine — a short backoff retry clears most of
    these without changing the query. ``what`` is used only in the error
    message if every attempt fails.
    """
    encoded = urllib.parse.urlencode({"data": query})
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(
            _OVERPASS_URL,
            data=encoded.encode("utf-8"),
            headers={"User-Agent": _DOWNLOAD_USER_AGENT},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:  # noqa: BLE001
            last_exc = e
            # HTTPError is a URLError subclass, so it must be checked first —
            # otherwise a permanent 400 would look "transient" via the
            # broader URLError branch below.
            if isinstance(e, urllib.error.HTTPError):
                transient = e.code in (429, 502, 503, 504)
            else:
                transient = isinstance(e, (urllib.error.URLError, TimeoutError))
            if attempt < retries and transient:
                wait = 10 * attempt
                print(f"  ⚠️  Overpass request failed ({e}); retrying in {wait}s "
                      f"({attempt}/{retries - 1})...", file=sys.stderr)
                time.sleep(wait)
                continue
            break
    raise RuntimeError(f"Overpass fetch failed for {what}: {last_exc}") from last_exc


# ==========================================
# 1. CORE FUNCTIONS
# ==========================================


def fetch_wfs_paginated(
    short_name, type_name, gpkg_path, temp_dir, sort_field="OBJECTID"
):
    print(f"\n[WFS] Fetching: {short_name} ({type_name})")
    start_index = 0
    max_features = 10000
    all_chunks = []
    progress = tqdm(desc=f"WFS {short_name}", unit=" feat", unit_scale=True)
    while True:
        params = {
            "SERVICE": "WFS",
            "VERSION": "2.0.0",
            "REQUEST": "GetFeature",
            "typeNames": type_name,
            "outputFormat": "json",
            "SRSNAME": "EPSG:4326",
            "count": max_features,
            "startIndex": start_index,
            "sortBy": sort_field,
        }
        url = f"https://openmaps.gov.bc.ca/geo/pub/ows?{urllib.parse.urlencode(params)}"
        try:
            chunk = gpd.read_file(url)
            if chunk.empty:
                break
            all_chunks.append(chunk)
            progress.update(len(chunk))
            if len(chunk) < max_features:
                break
            start_index += max_features
        except Exception as e:
            progress.close()
            raise RuntimeError(f"WFS fetch failed for '{type_name}': {e}") from e
    progress.close()
    if all_chunks:
        final_gdf = pd.concat(all_chunks, ignore_index=True)
        if final_gdf.crs and final_gdf.crs.to_epsg() != 3005:
            final_gdf = final_gdf.to_crs(epsg=3005)
        final_gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")


# PARCEL_CLASS values that never represent a meaningful ground footprint for
# an access-to-water question: condo/apartment units (each is its own title
# stacked on the same building footprint), airspace parcels, non-physical
# legal interests/easements, strata common areas, and road right-of-way.
# Checked live against WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_SVW: these
# account for 754,201 of 2,044,965 Private parcels (37%) and 204,101 of
# 445,392 non-private parcels (46%).
_PARCEL_NOISE_CLASSES = (
    "Building Strata", "Air Space", "Interest", "Common Ownership", "Road",
)


def fetch_parcel_fabric(
    short_name: str, zip_url: str, gdb_layer: str, gpkg_path: Path, temp_dir: Path
) -> None:
    """Fetch ParcelMap BC and dissolve every parcel into one merged polygon per OWNER_TYPE.

    Uses the BC Data Catalogue's bulk File Geodatabase download rather than
    paginated WFS — one ~358MB zip for all 2.49M parcels province-wide,
    already in EPSG:3005, vs. ~150+ paginated HTTP requests. Same
    download+extract pattern as the FWA hydrography (:func:`ensure_ftp_extracted`),
    just over HTTPS instead of FTP.

    Most of those 2.49M parcels don't matter for "is this shoreline private
    or public": condo units, road allowances, and legal interests carry no
    distinct ground footprint. Those noise classes are dropped at read-time
    via pyogrio's ``where=`` (an attribute filter pushed down to GDAL, so
    excluded rows are never even parsed into memory).

    Every remaining parcel — Private included — is dissolved into one merged
    polygon per ``OWNER_TYPE`` (Private, Crown Provincial, Crown Agency,
    Federal, Local Government, First Nations, Untitled Provincial, Mixed
    Ownership, Unclassified), written to a single ``{short_name}_crown``
    layer. Individual parcel-level boundaries aren't kept for Private either
    — for a province-wide "is this land private" advisory, only the
    ownership category matters, not the ~1.29M individual property lines,
    so this collapses what would otherwise be well over a million slivers
    (across all ownership types) into a small handful of polygons, the same
    way the previous crown-only dissolve did.
    """
    gdb_path = ensure_ftp_extracted(zip_url, temp_dir)
    noise_list = ", ".join(f"'{c}'" for c in _PARCEL_NOISE_CLASSES)

    print(f"\n[GDB] Reading parcels from {gdb_path.name} ...")
    parcels_gdf = gpd.read_file(
        gdb_path, layer=gdb_layer, engine="pyogrio",
        where=f"PARCEL_CLASS NOT IN ({noise_list})",
    )
    if parcels_gdf.empty:
        print(f"  ⚠️  '{short_name}_crown': no parcels found.")
        return
    if parcels_gdf.crs and parcels_gdf.crs.to_epsg() != 3005:
        parcels_gdf = parcels_gdf.to_crs(epsg=3005)

    print(f"  Dissolving {len(parcels_gdf):,} parcels by OWNER_TYPE (Private included)...")
    dissolved = parcels_gdf.dissolve(by="OWNER_TYPE", as_index=False)
    dissolved = dissolved[["OWNER_TYPE", "geometry"]]
    dissolved.to_file(gpkg_path, layer=f"{short_name}_crown", driver="GPKG", engine="pyogrio")
    print(f"  ✅ '{short_name}_crown': {len(dissolved)} merged polygon(s) "
          f"(from {len(parcels_gdf):,} source parcels)")


def fetch_overpass_aboriginal_lands(short_name: str, gpkg_path: Path) -> None:
    """Fetch all boundary=aboriginal_lands polygons in BC from the Overpass API.

    Queries for relations tagged ``boundary=aboriginal_lands`` within the
    bounding box of British Columbia.  Each polygon is saved with its OSM
    tags (name, name:en, indigenous name, url, wikidata, wikipedia).
    """
    print(f"\n[OVERPASS] Fetching aboriginal lands for BC...")

    # BC bounding box (lat/lon): south, west, north, east
    bbox = "48.2,-139.1,60.0,-114.0"

    query = f"""
[out:json][timeout:120];
(
  relation["boundary"="aboriginal_lands"]({bbox});
  way["boundary"="aboriginal_lands"]({bbox});
);
out body;
>;
out skel qt;
"""

    print("  -> Querying Overpass API (this may take a minute)...")
    raw = _overpass_query(query, "aboriginal lands")

    elements = raw.get("elements", [])
    nodes = {e["id"]: e for e in elements if e["type"] == "node"}
    ways = {e["id"]: e for e in elements if e["type"] == "way"}
    relations = [e for e in elements if e["type"] == "relation"]
    standalone_ways = [
        e
        for e in elements
        if e["type"] == "way"
        and e.get("tags", {}).get("boundary") == "aboriginal_lands"
    ]

    def _way_coords(way_element):
        """Return list of (lon, lat) for a way's node refs."""
        return [
            (nodes[nid]["lon"], nodes[nid]["lat"])
            for nid in way_element.get("nodes", [])
            if nid in nodes
        ]

    def _build_polygon_from_relation(rel):
        """Assemble a polygon from a relation's outer/inner way members."""
        outers, inners = [], []
        for member in rel.get("members", []):
            if member["type"] != "way" or member["ref"] not in ways:
                continue
            coords = _way_coords(ways[member["ref"]])
            if len(coords) < 2:
                continue
            role = member.get("role", "outer")
            if role == "inner":
                inners.append(coords)
            else:
                outers.append(coords)

        if not outers:
            return None

        # Merge outer way segments into closed rings
        outer_lines = [LineString(c) for c in outers if len(c) >= 2]
        if not outer_lines:
            return None
        merged = linemerge(outer_lines)
        outer_polys = list(polygonize(merged))

        inner_polys = []
        if inners:
            inner_lines = [LineString(c) for c in inners if len(c) >= 2]
            if inner_lines:
                inner_merged = linemerge(inner_lines)
                inner_polys = list(polygonize(inner_merged))

        if not outer_polys:
            return None

        # Subtract inner rings from outer polygons
        result = outer_polys[0]
        for op in outer_polys[1:]:
            result = result.union(op)
        for ip in inner_polys:
            result = result.difference(ip)

        return result if not result.is_empty else None

    def _build_polygon_from_way(way_el):
        """Build a polygon from a standalone closed way."""
        coords = _way_coords(way_el)
        if len(coords) >= 4:
            return Polygon(coords)
        return None

    def _tags_to_record(osm_id, tags, geom, group_name=""):
        """Build a GeoDataFrame-ready dict from OSM tags.

        ``group_name`` is the top-level grouping this feature rolls up to
        (e.g. the tribal council above a First Nation band); it is retained
        for reference even though ``name`` holds the displayed band name.
        """
        return {
            "osm_id": str(osm_id),
            "name": tags.get("name", ""),
            "name_en": tags.get("name:en", tags.get("name", "")),
            # Best-effort indigenous name: first name:* tag that isn't en/fr.
            # OSM tag order is non-deterministic so this is approximate.
            "name_indigenous": next(
                (
                    v
                    for k, v in tags.items()
                    if k.startswith("name:") and k != "name:en" and k != "name:fr"
                ),
                "",
            ),
            # Top-level grouping (e.g. tribal council); empty when this feature
            # is itself the top of its hierarchy or has no parent grouping.
            "name_group": group_name,
            "boundary": tags.get("boundary", ""),
            "type": "aboriginal_lands",
            "url": tags.get("url", tags.get("website", "")),
            "wikidata": tags.get("wikidata", ""),
            "wikipedia": tags.get("wikipedia", ""),
            "geometry": geom,
        }

    records = []

    # OSM models Indigenous lands as a nested hierarchy of
    # boundary=aboriginal_lands relations, e.g.:
    #     Stó:lō Tribal Council      (top-level grouping)
    #       └─ Cheam First Nation    (band grouping)
    #            └─ Cheam 1           (individual reserve / leaf)  <-- emitted
    # A parent lists its child areas as ``subarea`` members and shares the same
    # ground as the union of those children, so emitting parents as well would
    # stack overlapping polygons over a single location. Worse, a parent's member
    # ways are often an incomplete subset (and reserves such as "Pekw'Xe:yles"
    # belong to *two* bands at once), so building parent polygons can smear one
    # band's name across a neighbouring band's territory.
    #
    # To mirror what the OSM map actually renders, we emit only the leaf reserves
    # — the real land parcels — each with its own name and its own geometry, and
    # drop every parent grouping. The parent band/tribal-council name is still
    # surfaced on each reserve via ``name_group`` for reference.
    rel_by_id = {r["id"]: r for r in relations}

    def _subarea_child_ids(rel):
        return [
            m["ref"]
            for m in rel.get("members", [])
            if m.get("role") == "subarea" and m["type"] == "relation"
        ]

    def _has_subarea(rel):
        return any(m.get("role") == "subarea" for m in rel.get("members", []))

    # Leaf relations are individual reserves with no child subareas.
    leaf_ids = {rid for rid, r in rel_by_id.items() if not _has_subarea(r)}

    # Map each child area to its parent grouping so we can surface the top-level
    # grouping (e.g. tribal council) name on every reserve we emit.
    parent_of = {}
    for rid, r in rel_by_id.items():
        for cid in _subarea_child_ids(r):
            parent_of.setdefault(cid, rid)

    def _top_group_name(rid):
        """Walk up the subarea hierarchy and return the top-level grouping name.

        Returns "" when ``rid`` is itself the top of its hierarchy.
        """
        top = rid
        seen = {rid}
        while parent_of.get(top) is not None and parent_of[top] not in seen:
            top = parent_of[top]
            seen.add(top)
        if top == rid:
            return ""
        return rel_by_id.get(top, {}).get("tags", {}).get("name", "")

    # Emit each leaf reserve as its own polygon, built from its own outer ways.
    # A reserve shared by multiple bands (e.g. "Pekw'Xe:yles") is a single leaf,
    # so it is emitted exactly once with its own name rather than duplicated
    # under each band.
    kept_reserves = 0
    for rid in leaf_ids:
        rel = rel_by_id[rid]
        geom = _build_polygon_from_relation(rel)
        if geom is None:
            continue
        records.append(
            _tags_to_record(rid, rel.get("tags", {}), geom, _top_group_name(rid))
        )
        kept_reserves += 1

    dropped = len(relations) - kept_reserves
    print(
        f"  -> Emitted {kept_reserves} reserve polygon(s); "
        f"dropped {dropped} parent grouping relation(s)"
    )

    # Process standalone ways (not part of a relation)
    relation_way_ids = set()
    for rel in relations:
        for m in rel.get("members", []):
            if m["type"] == "way":
                relation_way_ids.add(m["ref"])
    for way_el in standalone_ways:
        if way_el["id"] in relation_way_ids:
            continue
        tags = way_el.get("tags", {})
        geom = _build_polygon_from_way(way_el)
        if geom is None:
            continue
        records.append(_tags_to_record(way_el["id"], tags, geom))

    if not records:
        print("  ⚠️  No aboriginal lands polygons found.")
        return

    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=3005)

    # Clean any list-type columns
    for col in gdf.columns:
        if col != "geometry" and gdf[col].apply(lambda x: isinstance(x, list)).any():
            gdf[col] = gdf[col].apply(str)

    gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")
    print(f"  ✅ '{short_name}' written ({len(gdf)} aboriginal lands polygons)")


_RESTRICTIVE_ACCESS_VALUES = {
    "private",
    "permissive",
    "permit",
    "destination",
    "customers",
    "no",
}

# access values (or landuse=military, which often carries no explicit
# access tag at all) that mean "the public cannot get in" — vs. the
# remaining restrictive values, which mean "conditional/permission-based
# entry." Drives the `restriction_level` column below, which the regulation
# pipeline uses to split a single generic "Closed" advisory from a lighter
# "Restricted" one (see LAND_ACCESS_CLOSED / LAND_ACCESS_RESTRICTED in
# base_regulations.json).
_CLOSED_ACCESS_VALUES = {"no", "private"}

# Small hardcoded list of named areas with real-world access restrictions
# that don't carry a matching OSM access/landuse tag (so the tag-based query
# above wouldn't find them) — geocoded by name via the same mechanism the
# old, now-retired `osm_admin_boundaries` dataset used. Folded in here so
# `land_access` is the single source of truth for this kind of restriction,
# rather than a separate always-one-hardcoded-query dataset.
_NAMED_RESTRICTED_AREAS = ["Malcolm Knapp Research Forest"]


def fetch_overpass_land_access(short_name: str, gpkg_path: Path) -> None:
    """Fetch BC land polygons with restrictive access from OSM via Overpass.

    Captures land the public cannot freely enter: restricted protected
    areas & nature reserves (e.g. Metro Vancouver's Coquitlam Watershed —
    ``access=private``, ``boundary=protected_area``, ``leisure=nature_reserve``,
    ``protect_class=12``), DND/military land (``landuse=military``, which
    often carries no ``access`` tag of its own), and municipal watersheds
    (``landuse=reservoir_watershed``).

    Only closed ways / multipolygon relations are kept — an open way tagged
    ``highway``/``barrier`` (a private driveway, gate, trail) is a linear
    feature, not land, and is excluded at the query level to keep the result
    set to actual parcels.

    The raw Overpass query matches any closed way/relation carrying a
    restrictive ``access`` value (private, permissive, permit, destination,
    customers, no — see https://wiki.openstreetmap.org/wiki/Tag:access=private),
    which pulls in a lot more than land-access restrictions: checked live,
    91% of raw results were backyard/hotel swimming pools or unlabeled
    private parking lots. Everything not tagged ``landuse=military``,
    ``boundary=protected_area``, ``leisure=nature_reserve``, or
    ``landuse=reservoir_watershed`` is filtered back out before writing.

    Also folds in a small supplemental pass of named areas (geocoded by
    name, not by tag) that are known to be access-restricted but don't
    carry a matching OSM tag — see ``_NAMED_RESTRICTED_AREAS``.
    """
    print(f"\n[OVERPASS] Fetching land access layer for BC...")

    # Scope to the actual BC administrative boundary (not a lat/lon bounding
    # box) so results don't bleed into neighbouring Alberta/Washington.
    access_re = "|".join(sorted(_RESTRICTIVE_ACCESS_VALUES))

    query = f"""
[out:json][timeout:180];
area["ISO3166-2"="CA-BC"]["admin_level"="4"]->.bc;
(
  way["access"~"^({access_re})$"]["highway"!~".*"]["barrier"!~".*"](area.bc);
  relation["access"~"^({access_re})$"](area.bc);
  way["landuse"="military"]["highway"!~".*"]["barrier"!~".*"](area.bc);
  relation["landuse"="military"](area.bc);
);
out body;
>;
out skel qt;
"""

    print("  -> Querying Overpass API (this may take a minute)...")
    raw = _overpass_query(query, "land access")

    elements = raw.get("elements", [])
    nodes = {e["id"]: e for e in elements if e["type"] == "node"}
    ways = {e["id"]: e for e in elements if e["type"] == "way"}
    relations = [e for e in elements if e["type"] == "relation"]
    tagged_ways = [
        e
        for e in elements
        if e["type"] == "way"
        and (
            e.get("tags", {}).get("access") in _RESTRICTIVE_ACCESS_VALUES
            or e.get("tags", {}).get("landuse") == "military"
        )
    ]

    def _way_coords(way_element):
        return [
            (nodes[nid]["lon"], nodes[nid]["lat"])
            for nid in way_element.get("nodes", [])
            if nid in nodes
        ]

    def _is_closed(coords):
        return len(coords) >= 4 and coords[0] == coords[-1]

    def _build_polygon_from_relation(rel):
        """Assemble a polygon from a relation's outer/inner way members."""
        outers, inners = [], []
        for member in rel.get("members", []):
            if member["type"] != "way" or member["ref"] not in ways:
                continue
            coords = _way_coords(ways[member["ref"]])
            if len(coords) < 2:
                continue
            role = member.get("role", "outer")
            (inners if role == "inner" else outers).append(coords)

        if not outers:
            return None
        outer_lines = [LineString(c) for c in outers if len(c) >= 2]
        if not outer_lines:
            return None
        merged = linemerge(outer_lines)
        outer_polys = list(polygonize(merged))
        if not outer_polys:
            return None

        inner_polys = []
        if inners:
            inner_lines = [LineString(c) for c in inners if len(c) >= 2]
            if inner_lines:
                inner_merged = linemerge(inner_lines)
                inner_polys = list(polygonize(inner_merged))

        result = outer_polys[0]
        for op in outer_polys[1:]:
            result = result.union(op)
        for ip in inner_polys:
            result = result.difference(ip)
        return result if not result.is_empty else None

    def _restriction_level(tags):
        if tags.get("access") in _CLOSED_ACCESS_VALUES or tags.get("landuse") == "military":
            return "closed"
        return "restricted"

    def _tags_to_record(osm_id, tags, geom, osm_type):
        return {
            "osm_id": str(osm_id),
            "osm_type": osm_type,
            "name": tags.get("name", ""),
            "access": tags.get("access", ""),
            "boundary": tags.get("boundary", ""),
            "leisure": tags.get("leisure", ""),
            "landuse": tags.get("landuse", ""),
            "protect_class": tags.get("protect_class", ""),
            "owner": tags.get("owner", ""),
            "operator": tags.get("operator", ""),
            "restriction_level": _restriction_level(tags),
            "geometry": geom,
        }

    records = []
    for rel in relations:
        geom = _build_polygon_from_relation(rel)
        if geom is None:
            continue
        records.append(_tags_to_record(rel["id"], rel.get("tags", {}), geom, "relation"))

    # Standalone closed ways (not already covered as a relation member).
    relation_way_ids = {
        m["ref"]
        for rel in relations
        for m in rel.get("members", [])
        if m["type"] == "way"
    }
    for way_el in tagged_ways:
        if way_el["id"] in relation_way_ids:
            continue
        coords = _way_coords(way_el)
        if not _is_closed(coords):
            continue  # open way (e.g. a private trail) — not land
        records.append(
            _tags_to_record(way_el["id"], way_el.get("tags", {}), Polygon(coords), "way")
        )

    # Drop tag-driven noise: the raw Overpass query above matches ANY closed
    # way/relation carrying a restrictive `access` value, regardless of what
    # kind of feature it actually is. Checked live against a full BC fetch:
    # 91% of the 16,360 raw results were backyard/hotel swimming pools
    # (leisure=swimming_pool, 33%) or completely unlabeled parking lots
    # (58% — named "Impark", "Visitor Parking", "Staff Parking", etc, with
    # no boundary/leisure/landuse tag of any kind) — none of which bear on
    # "can the public reach this water to fish." Keep only the tag
    # combinations that represent a real land-access restriction: military
    # bases, protected areas / nature reserves, and municipal watersheds.
    before = len(records)
    records = [
        r for r in records
        if r["landuse"] == "military"
        or r["boundary"] == "protected_area"
        or r["leisure"] == "nature_reserve"
        or r["landuse"] == "reservoir_watershed"
    ]
    print(f"  Filtered {before:,} raw tagged polygons → {len(records):,} "
          f"real land-access restrictions (dropped pools/parking/other noise)")

    # Supplemental named-area pass — areas known to be access-restricted in
    # the real world but not reliably tagged in OSM for the query above.
    # Folds in what the retired `osm_admin_boundaries` dataset used to do.
    # Exempt from the noise filter above: this is a small, hand-curated
    # list, not raw crowd-sourced tagging, so being on it already means
    # it's a real restriction regardless of what tags it carries.
    for name in _NAMED_RESTRICTED_AREAS:
        print(f"  -> Geocoding named restricted area: {name}")
        try:
            named_gdf = ox.geocoder.geocode_to_gdf(name)
        except Exception as e:
            logger.warning("Named-area geocode failed for '%s': %s", name, e)
            continue
        if named_gdf.empty:
            print(f"     ⚠️ No results for {name}")
            continue
        if named_gdf.crs and named_gdf.crs.to_epsg() != 4326:
            named_gdf = named_gdf.to_crs(epsg=4326)
        row = named_gdf.iloc[0]
        tags = {"name": row.get("name") or name, "access": "permit"}
        records.append(
            _tags_to_record(row.get("osm_id"), tags, row.geometry, "relation")
        )
        print(f"     ✅ Added {name}")

    if not records:
        print("  ⚠️  No land-access polygons found.")
        return

    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=3005)

    for col in gdf.columns:
        if col != "geometry" and gdf[col].apply(lambda x: isinstance(x, list)).any():
            gdf[col] = gdf[col].apply(str)

    gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")
    print(f"  ✅ '{short_name}' written ({len(gdf)} land-access polygons)")


def fetch_overpass_water_access(short_name: str, gpkg_path: Path) -> None:
    """Fetch OSM physical water-access points across BC via Overpass.

    Combines every point-of-interest tag relevant to "can I get to the
    water, and what's there when I do" into one layer, distinguished by a
    ``poi_type`` column:

    - ``leisure=slipway``   — boat launch/ramp
    - ``man_made=pier``     — dock/pier
    - ``leisure=fishing``   — designated fishing platform/spot

    Marina (``leisure=marina``) deliberately excluded — not relevant to
    "can I get to the water to fish" (a marina is boat moorage, not a
    fishing access point), so it was dropped as noise rather than kept as
    a fourth category.

    Not a permission/regulation layer — BC's own regulations already cover
    where fishing is allowed, so this is deliberately just the physical
    infrastructure. Almost all features are nodes; piers occasionally come
    through as a way (the structure's outline/edge).
    """
    print(f"\n[OVERPASS] Fetching water access points for BC...")

    query = """
[out:json][timeout:180];
area["ISO3166-2"="CA-BC"]["admin_level"="4"]->.bc;
(
  node["leisure"="slipway"](area.bc);
  way["leisure"="slipway"](area.bc);
  node["man_made"="pier"](area.bc);
  way["man_made"="pier"](area.bc);
  node["leisure"="fishing"](area.bc);
  way["leisure"="fishing"](area.bc);
);
out body;
>;
out skel qt;
"""

    print("  -> Querying Overpass API (this may take a minute)...")
    raw = _overpass_query(query, "water access")

    elements = raw.get("elements", [])
    nodes = {e["id"]: e for e in elements if e["type"] == "node"}

    def _poi_type(tags):
        if tags.get("leisure") == "slipway":
            return "boat_launch"
        if tags.get("man_made") == "pier":
            return "pier"
        if tags.get("leisure") == "fishing":
            return "fishing_platform"
        return None

    def _way_coords(way_element):
        return [
            (nodes[nid]["lon"], nodes[nid]["lat"])
            for nid in way_element.get("nodes", [])
            if nid in nodes
        ]

    def _tags_to_record(osm_id, tags, geom, osm_type, poi_type):
        return {
            "osm_id": str(osm_id),
            "osm_type": osm_type,
            "poi_type": poi_type,
            "name": tags.get("name", tags.get("alt_name", "")),
            "access": tags.get("access", ""),
            "mooring": tags.get("mooring", ""),
            "fee": tags.get("fee", ""),
            "surface": tags.get("surface", ""),
            "motorboat": tags.get("motorboat", ""),
            "trailer": tags.get("trailer", ""),
            "operator": tags.get("operator", ""),
            "geometry": geom,
        }

    # A feature carrying a private-access signal isn't a real public water
    # access point, even though it's tagged with the right leisure/man_made
    # kind — e.g. a private dock: `man_made=pier, access=private,
    # mooring=private`. Checked both `access` (the general OSM access tag)
    # and `mooring` (boat-mooring-specific, can be set without a matching
    # `access` tag) so a private mooring doesn't slip through either way.
    _PRIVATE_ACCESS_VALUES = {"private", "no"}

    def _is_publicly_accessible(tags):
        if tags.get("access") in _PRIVATE_ACCESS_VALUES:
            return False
        if tags.get("mooring") in _PRIVATE_ACCESS_VALUES:
            return False
        return True

    records = []
    dropped_private = 0
    for el in elements:
        tags = el.get("tags", {})
        poi_type = _poi_type(tags)
        if poi_type is None:
            continue
        if not _is_publicly_accessible(tags):
            dropped_private += 1
            continue
        if el["type"] == "node":
            geom = Point(el["lon"], el["lat"])
        elif el["type"] == "way":
            coords = _way_coords(el)
            if len(coords) < 2:
                continue
            geom = LineString(coords)
        else:
            continue
        records.append(_tags_to_record(el["id"], tags, geom, el["type"], poi_type))

    if dropped_private:
        print(f"  Dropped {dropped_private:,} private-access water access points "
              f"(access=private/no or mooring=private/no)")

    if not records:
        print("  ⚠️  No water access features found.")
        return

    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=3005)

    for col in gdf.columns:
        if col != "geometry" and gdf[col].apply(lambda x: isinstance(x, list)).any():
            gdf[col] = gdf[col].apply(str)

    gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")
    print(f"  ✅ '{short_name}' written ({len(gdf)} water access feature(s))")


def fetch_overpass_waterfalls(short_name: str, gpkg_path: Path) -> None:
    """Fetch OSM waterfall points/ways across BC via Overpass.

    ``waterway=waterfall`` marks a waterfall on a stream — almost always a
    node, occasionally a short way. Not access-related; this is a points-
    of-interest layer for the map.
    """
    print(f"\n[OVERPASS] Fetching waterfalls for BC...")

    query = """
[out:json][timeout:180];
area["ISO3166-2"="CA-BC"]["admin_level"="4"]->.bc;
(
  node["waterway"="waterfall"](area.bc);
  way["waterway"="waterfall"](area.bc);
);
out body;
>;
out skel qt;
"""

    print("  -> Querying Overpass API (this may take a minute)...")
    raw = _overpass_query(query, "waterfalls")

    elements = raw.get("elements", [])
    nodes = {e["id"]: e for e in elements if e["type"] == "node"}
    tagged_nodes = [
        e for e in elements if e["type"] == "node" and e.get("tags", {}).get("waterway") == "waterfall"
    ]
    tagged_ways = [
        e for e in elements if e["type"] == "way" and e.get("tags", {}).get("waterway") == "waterfall"
    ]

    def _way_coords(way_element):
        return [
            (nodes[nid]["lon"], nodes[nid]["lat"])
            for nid in way_element.get("nodes", [])
            if nid in nodes
        ]

    def _tags_to_record(osm_id, tags, geom, osm_type):
        return {
            "osm_id": str(osm_id),
            "osm_type": osm_type,
            "name": tags.get("name", ""),
            "height": tags.get("height", ""),
            "geometry": geom,
        }

    records = []
    for el in tagged_nodes:
        records.append(
            _tags_to_record(el["id"], el.get("tags", {}), Point(el["lon"], el["lat"]), "node")
        )
    for way_el in tagged_ways:
        coords = _way_coords(way_el)
        if len(coords) < 2:
            continue
        records.append(
            _tags_to_record(way_el["id"], way_el.get("tags", {}), LineString(coords), "way")
        )

    if not records:
        print("  ⚠️  No waterfalls found.")
        return

    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=3005)

    for col in gdf.columns:
        if col != "geometry" and gdf[col].apply(lambda x: isinstance(x, list)).any():
            gdf[col] = gdf[col].apply(str)

    gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")
    print(f"  ✅ '{short_name}' written ({len(gdf)} waterfall feature(s))")


def ensure_ftp_extracted(ftp_url, temp_dir):
    zip_path = temp_dir / os.path.basename(ftp_url)
    gdb_path = temp_dir / zip_path.name.replace(".zip", ".gdb")
    if not zip_path.exists():
        print(f"\n[FTP] Downloading: {zip_path.name}")
        _download_with_progress(ftp_url, zip_path, desc=zip_path.name)
    if not gdb_path.exists():
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            members = zip_ref.namelist()
            for member in tqdm(members, desc=f"Extracting {zip_path.name}", unit="file"):
                zip_ref.extract(member, temp_dir)
    return gdb_path


def extract_gdb_layer(short_name, ftp_url, gdb_layer, gpkg_path, temp_dir):
    print(f"\n[GDB] Extracting: {short_name}")
    gdb_path = ensure_ftp_extracted(ftp_url, temp_dir)
    gdf = gpd.read_file(gdb_path, layer=gdb_layer, engine="pyogrio")
    if gdf.crs and gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs(epsg=3005)
    gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")


def combine_streams(short_name, ftp_url, gpkg_path, temp_dir):
    print(f"\n[STREAMS] Merging watershed blocks...")
    gdb_path = ensure_ftp_extracted(ftp_url, temp_dir)
    layers = [lyr for lyr in fiona.listlayers(str(gdb_path)) if isinstance(lyr, str)]
    is_first = True
    for lyr in tqdm(layers):
        gdf = gpd.read_file(gdb_path, layer=lyr, engine="pyogrio")
        if gdf.empty:
            continue
        # Non-spatial GDB layers (attribute tables) come back as plain
        # DataFrames with no geometry/CRS; skip them so the merge doesn't fail.
        if not isinstance(gdf, gpd.GeoDataFrame) or gdf.geometry.isna().all():
            continue
        if gdf.crs and gdf.crs.to_epsg() != 3005:
            gdf = gdf.to_crs(epsg=3005)
        mode = "w" if is_first else "a"
        gdf.to_file(
            gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio", mode=mode
        )
        is_first = False


def fetch_csv_download(short_name: str, url: str, dest_path: Path) -> None:
    """Download a reference CSV to the data folder (not the GeoPackage).

    Used for tabular BC Data Catalogue datasets such as the WSA lake bathymetry
    reference table (one row per bathymetric survey map: waterbody identifier,
    gazetted name, watershed code, and the PDF map URL), consumed by
    ``pipeline.matching.bathymetry_matcher``.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n[CSV] Fetching '{short_name}' -> {dest_path}")
    _download_with_progress(url, dest_path, desc=short_name)
    try:
        with open(dest_path, encoding="utf-8", errors="replace") as fh:
            n_rows = max(sum(1 for _ in fh) - 1, 0)
        print(f"  ✅ '{short_name}' saved ({n_rows} row(s)) -> {dest_path}")
    except OSError as exc:
        print(f"  ⚠️  saved but could not count rows: {exc}")


def _bathy_pdf_filename(url: object) -> "str | None":
    """Extract the canonical PDF filename from a WSA download URL.

    Both bathymetry sources point at the same gov BC endpoint
    (``downloadBathymetricMap.do?filename=00045501.pdf``); the ``filename``
    query param is the authoritative per-sheet key shared across them, so we key
    every download on it rather than on a per-source filename column.  Returns
    the lowercased filename, or ``None`` when the URL carries no usable name.
    """
    if not isinstance(url, str) or not url.strip():
        return None
    query = urllib.parse.urlparse(url.strip()).query
    value = urllib.parse.parse_qs(query).get("filename")
    if not value or not value[0].strip():
        return None
    fname = value[0].strip().lower()
    # Reject degenerate names (a handful of source rows carry ``filename=.pdf``
    # or an empty stem); they map to no waterbody and would just be dead weight.
    if not fname.endswith(".pdf") or not fname[: -len(".pdf")]:
        return None
    return fname


def fetch_bathymetry_pdfs(
    short_name: str,
    csv_path: Path,
    dest_dir: Path,
    gpkg_path: "Path | None" = None,
    poly_layer: str = "bathymetry_polygons",
) -> None:
    """Bulk-download every WSA bathymetric survey map PDF into ``dest_dir``.

    Unions the PDF links from **both** bathymetry sources so no survey map is
    missed:

      * the reference CSV produced by the ``bathymetry_maps`` download, and
      * the ``bathymetry_polygons`` survey-map-sheets layer in the gpkg
        (``WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW``).

    The two catalogues do not fully overlap (each lists ~90 sheets the other
    omits), so relying on either alone leaves gaps.  Every map is keyed on the
    ``filename=`` query param of ``PDF_FILE_URL`` — the one identifier both
    sources share — and saved as ``<filename>.pdf``.  These local copies are the
    "just in case" archive, later pushed to R2 under ``bathymetry/`` by
    ``scripts/seed-r2.sh`` so the web app serves depth maps from our own bucket
    instead of hot-linking the gov BC endpoint.

    A single survey (``WATERBODY_IDENTIFIER_WSA_50K``) may span several map
    sheets, so the PDF filename — not the identifier — is the unique key.

    Idempotent: files already present (non-empty) are skipped, so re-running
    only fetches missing/new maps.  Individual download failures are collected
    and reported at the end rather than aborting the whole batch; the partial
    file for a failed download is removed so a later run retries it.
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Bathymetry CSV not found: {csv_path}. "
            "Fetch the 'bathymetry_maps' layer first."
        )
    dest_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[PDF] Downloading bathymetry survey maps -> {dest_dir}")

    url_col = "PDF_FILE_URL"

    # Collect (filename -> url) from both catalogues, keyed on the shared
    # filename query param.  CSV is added first so its https URL wins over the
    # polygon layer's http URL when a sheet appears in both.
    seen: dict = {}

    def _harvest(urls, source: str) -> int:
        added = 0
        for url in urls:
            fname = _bathy_pdf_filename(url)
            if fname is None or fname in seen:
                continue
            seen[fname] = url.strip()
            added += 1
        return added

    df = pd.read_csv(csv_path, dtype=str)
    if url_col not in df.columns:
        raise KeyError(f"Bathymetry CSV missing expected column: {url_col}")
    csv_added = _harvest(df[url_col], "csv")

    poly_added = 0
    if gpkg_path is not None and Path(gpkg_path).exists():
        try:
            available = set(fiona.listlayers(str(gpkg_path)))
        except Exception:
            available = set()
        if poly_layer in available:
            poly = gpd.read_file(str(gpkg_path), layer=poly_layer, ignore_geometry=True)
            if url_col in poly.columns:
                poly_added = _harvest(poly[url_col], "polygons")
            else:
                print(f"  ⚠️  '{poly_layer}' has no {url_col} column; skipping.")
        else:
            print(f"  ⚠️  Layer '{poly_layer}' not in gpkg; using CSV only.")

    print(
        f"  [PDF] {len(seen)} unique maps "
        f"(CSV +{csv_added}, polygons +{poly_added} beyond CSV)."
    )

    jobs = [(url, fname) for fname, url in seen.items()]

    downloaded = skipped = 0
    failures = []
    for url, fname in tqdm(jobs, desc=short_name, unit="pdf"):
        dest = dest_dir / fname
        if dest.exists() and dest.stat().st_size > 0:
            skipped += 1
            continue
        try:
            _download_quiet(url, dest)
            downloaded += 1
        except Exception as exc:
            # Collect and report; never abort the whole batch on one bad URL.
            failures.append((fname, str(exc)))
            dest.unlink(missing_ok=True)

    print(
        f"  ✅ '{short_name}': {downloaded} downloaded, {skipped} already present, "
        f"{len(failures)} failed ({len(jobs)} unique maps)."
    )
    if failures:
        print(f"  ⚠️  {len(failures)} PDF(s) failed to download:")
        for fname, msg in failures[:20]:
            print(f"       - {fname}: {msg}")
        if len(failures) > 20:
            print(f"       … and {len(failures) - 20} more.")


def fetch_osm_places(short_name: str, dest_path: Path) -> None:
    """Fetch BC populated places (city/town/village/hamlet) from OSM Overpass.

    Writes a compact JSON list ``[{"name", "lat", "lon", "place"}, …]`` to
    ``dest_path``.  This is the town gazetteer used by the enrichment step to
    tag every named waterbody with its nearest towns, so the web-app search can
    surface a lake when a user searches the town beside it.

    Uses the same Overpass endpoint + BC bounding box as
    :func:`fetch_overpass_aboriginal_lands`.  Border towns just outside BC are
    harmless (a BC lake genuinely closest to one is a valid nearest town).

    Idempotent: an existing non-empty file is left untouched so re-running the
    fetch does not re-query Overpass.
    """
    if dest_path.exists() and dest_path.stat().st_size > 0:
        print(f"\n[OSM] Places already present -> {dest_path} (skipping)")
        return

    print(f"\n[OSM] Fetching BC populated places (towns) from Overpass...")

    # BC bounding box (lat/lon): south, west, north, east
    bbox = "48.2,-139.1,60.0,-114.0"
    query = f"""
[out:json][timeout:180];
(
  node["place"~"^(city|town|village|hamlet)$"]["name"]({bbox});
);
out qt;
"""

    print("  -> Querying Overpass API (this may take a minute)...")
    raw = _overpass_query(query, "OSM places")

    places = []
    seen = set()
    for el in raw.get("elements", []):
        if el.get("type") != "node":
            continue
        tags = el.get("tags", {})
        name = (tags.get("name") or "").strip()
        lat, lon = el.get("lat"), el.get("lon")
        if not name or lat is None or lon is None:
            continue
        # Dedup exact (name, rounded coord) duplicates.
        key = (name, round(float(lat), 5), round(float(lon), 5))
        if key in seen:
            continue
        seen.add(key)
        places.append(
            {
                "name": name,
                "lat": float(lat),
                "lon": float(lon),
                "place": tags.get("place", ""),
            }
        )

    if not places:
        raise RuntimeError(
            "Overpass returned no populated places for BC — refusing to write an "
            "empty gazetteer (check the query / Overpass availability)."
        )

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dest_path, "w", encoding="utf-8") as fh:
        json.dump(places, fh, ensure_ascii=False)
    print(f"  ✅ '{short_name}': {len(places)} places saved -> {dest_path}")


def fetch_r2_gpkg_layer(short_name, r2_url, source_layer, gpkg_path):
    """Download a specific layer from an R2-hosted GPKG and write it into the local GPKG.

    Downloads the remote GPKG to a temp file, reads the named layer, reprojects
    to EPSG:3005, and writes it as ``short_name`` in the output GPKG.
    """
    import tempfile

    print(f"\n[R2] Fetching layer '{source_layer}' from {r2_url}")
    with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        _download_with_progress(r2_url, tmp_path, desc=f"{short_name} (R2)")
        gdf = gpd.read_file(tmp_path, layer=source_layer, engine="pyogrio")
        if gdf.crs and gdf.crs.to_epsg() != 3005:
            gdf = gdf.to_crs(epsg=3005)
        gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")
        print(f"  ✅ '{short_name}' written ({len(gdf)} row(s))")
    finally:
        tmp_path.unlink(missing_ok=True)


# ==========================================
# 2. MAIN EXECUTION
# ==========================================


def fetch_r2_file(name, url, dest_path):
    """Download a static asset from our R2-backed data domain into ``data/``.

    Used for large binaries that aren't produced by the pipeline but are needed
    for local dev / deploy (e.g. the ``bc.pmtiles`` basemap). Skips the download
    when a non-empty file already exists so re-runs stay cheap.
    """
    dest_path = Path(dest_path)
    if dest_path.exists() and dest_path.stat().st_size > 0:
        logger.info(
            "%s: %s already present — skipping download", name, dest_path.name
        )
        return
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("%s: downloading %s", name, url)
    _download_with_progress(url, dest_path, desc=dest_path.name)


# Build-assets bucket (custom domain) that hosts backed-up, expensive-to-
# regenerate pipeline inputs.  Only referenced when the user explicitly opts in
# via --add-parsing-data so a rebuild can skip the extraction + LLM parsing steps.
BUILD_ASSETS_BASE_URL = "https://build.canifishthis.ca"


def fetch_parsing_backup(config):
    """Restore backed-up extraction + parsing outputs from the build-assets bucket.

    Downloads the expensive-to-regenerate pipeline inputs (raw synopsis extraction
    and LLM-parsed regulations) so the pipeline can be rebuilt without re-running
    the extraction and parsing stages.  Overwrites any local copies so the restore
    is deterministic.
    """
    restores = [
        (
            "extraction/synopsis_raw_data.json",
            config.extraction_dir / "synopsis_raw_data.json",
        ),
        ("parsing/synopsis_parsed.json", config.parsing_dir / "synopsis_parsed.json"),
        ("parsing/session_state.json", config.parsing_dir / "session_state.json"),
    ]
    print(f"\n[Backup] Restoring parsing data from {BUILD_ASSETS_BASE_URL}")
    for key, dest in restores:
        dest.parent.mkdir(parents=True, exist_ok=True)
        _download_with_progress(
            f"{BUILD_ASSETS_BASE_URL}/{key}", dest, desc=dest.name
        )
        print(f"  ✅ {key} → {dest}")


def _layer_already_fetched(name: str, cfg: dict, gpkg_out: Path, existing_layers: set) -> bool:
    """Best-effort "has this already been fetched" check, for --missing-only.

    Most dataset types write a single gpkg layer named after the dataset key
    — those are checked against ``existing_layers``. A few write to plain
    files or split into multiple layers, so those types get their own check.
    """
    t = cfg["type"]
    if t == "PARCEL_FABRIC":
        return f"{name}_crown" in existing_layers
    if t in ("R2_FILE", "CSV_DOWNLOAD", "OSM_PLACES"):
        dest = gpkg_out.parent / cfg["dest"]
        return dest.exists() and dest.stat().st_size > 0
    if t == "PDF_BULK":
        dest_dir = gpkg_out.parent / cfg["dest_dir"]
        return dest_dir.exists() and any(dest_dir.iterdir())
    return name in existing_layers


def main():
    config = get_config()
    gpkg_out = Path(config.fetch_output_gpkg_path)
    temp_dir = Path(config.fetch_temp_dir)

    FTP_FWA = "ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/FWA_BC.zip"
    FTP_STR = "ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/FWA_STREAM_NETWORKS_SP.zip"

    # BC Data Catalogue — ParcelMap BC bulk File Geodatabase download (all
    # ~2.49M parcels province-wide, ~358MB). Same bulk download+extract
    # pattern as FTP_FWA/FTP_STR above, just over HTTPS.
    PARCEL_FABRIC_ZIP = (
        "https://pub.data.gov.bc.ca/datasets/4cf233c2-f020-4f7a-9b87-1923252fbc24/"
        "pmbc_parcel_fabric_poly_svw.zip"
    )

    # BC Data Catalogue — "Bathymetry Open Reference Table and Maps" (one row per
    # bathymetric survey map, with the PDF map URL).  Tabular CSV, saved to data/.
    BATHY_CSV_URL = (
        "https://catalogue.data.gov.bc.ca/dataset/1427d389-cd21-4fe2-8ed9-282d9bdcb7e2/"
        "resource/d1d89c2e-1994-4f7d-a269-55b40e26067d/download/"
        "bathyopenreferencetableandmapsaug22_2023final.csv"
    )

    DATASETS = {
        # Aboriginal / Indigenous lands from OpenStreetMap (Overpass API)
        "aboriginal_lands": {
            "type": "OVERPASS_ABORIGINAL",
        },
        # Land with restrictive public access from OpenStreetMap (Overpass API):
        # private protected areas / nature reserves (e.g. watersheds) plus any
        # area tagged access=private|permissive|permit|destination|customers|no.
        "land_access": {
            "type": "OVERPASS_LAND_ACCESS",
        },
        # OSM physical water-access points — boat launches, docks/piers,
        # designated fishing platforms. Not a permission layer (BC's
        # own regulations already cover where fishing is allowed) — just the
        # physical "can I get to the water, and what's there" infrastructure.
        "water_access_points": {
            "type": "OVERPASS_WATER_ACCESS",
        },
        # BC Forest Service Roads (active/deactivated/gated status) — sourced
        # from DataBC rather than OSM since BC's backcountry road network is
        # far more complete/authoritative there than OSM tagging.
        "forest_service_roads": {
            "type": "WFS",
            "source": "WHSE_FOREST_TENURE.FTEN_ROAD_SECTION_LINES_SVW",
        },
        # ParcelMap BC — parcel-level land ownership. Bulk File Geodatabase
        # download (~358MB, all 2.49M parcels), same pattern as FWA_BC.zip
        # above. fetch_parcel_fabric() drops condo/road/interest noise
        # classes at read-time and dissolves every remaining parcel —
        # Private included — into one merged polygon per OWNER_TYPE,
        # writing a single "{name}_crown" layer. Still a heavier pull than
        # most layers here, same as `streams` — included in every run by
        # default, not gated behind `--layers`.
        "land_parcels": {
            "type": "PARCEL_FABRIC",
            "url": PARCEL_FABRIC_ZIP,
            "layer": "PMBC_PARCEL_FABRIC_POLY_SVW",
        },
        # OSM waterfalls (waterway=waterfall), mostly points with occasional
        # short ways marking the falls location on a stream.
        "waterfalls": {
            "type": "OVERPASS_WATERFALLS",
        },
        "wma": {"type": "WFS", "source": "WHSE_TANTALIS.TA_WILDLIFE_MGMT_AREAS_SVW"},
        "wmu": {
            "type": "WFS",
            "source": "WHSE_WILDLIFE_MANAGEMENT.WAA_WILDLIFE_MGMT_UNITS_SVW",
        },
        "parks_bc": {"type": "WFS", "source": "WHSE_TANTALIS.TA_PARK_ECORES_PA_SVW"},
        "parks_nat": {"type": "WFS", "source": "WHSE_ADMIN_BOUNDARIES.CLAB_NATIONAL_PARKS"},
        "historic_sites": {"type": "WFS", "source": "WHSE_HUMAN_CULTURAL_ECONOMIC.HIST_HISTORIC_ENVIRONMNT_PA_SV"},
        "lakes": {"type": "FWA_GDB", "ftp": FTP_FWA, "layer": "FWA_LAKES_POLY"},
        "wetlands": {"type": "FWA_GDB", "ftp": FTP_FWA, "layer": "FWA_WETLANDS_POLY"},
        "manmade": {"type": "FWA_GDB", "ftp": FTP_FWA, "layer": "FWA_MANMADE_WATERBODIES_POLY"},
        "watersheds": {"type": "FWA_GDB", "ftp": FTP_FWA, "layer": "FWA_NAMED_WATERSHEDS_POLY"},
        "streams": {"type": "FWA_STREAMS", "ftp": FTP_STR},
        "tidal_boundary": {
            "type": "R2_GPKG",
            "url": "https://build.canifishthis.ca/DFO_TIDAL_BOUNDARY.gpkg",
            "layer": "tidal_boundary",
        },
        # Protomaps basemap tiles (OSM-derived). Not produced by the pipeline —
        # downloaded from our R2 data domain so local dev / deploy can serve the
        # map background at /bc.pmtiles.
        "basemap": {
            "type": "R2_FILE",
            "url": "https://data.canifishthis.ca/bc.pmtiles",
            "dest": "bc.pmtiles",
        },
        "bathymetry_maps": {
            "type": "CSV_DOWNLOAD",
            "url": BATHY_CSV_URL,
            "dest": "wsa_bathymetry_maps.csv",
        },
        # Bulk download of every survey map PDF referenced by bathymetry_maps.
        # Saved to data/bathymetry_pdfs/ (gitignored) and later mirrored to R2
        # under bathymetry/ by scripts/seed-r2.sh.  Must run after
        # bathymetry_maps so the reference CSV already exists.
        "bathymetry_pdfs": {
            "type": "PDF_BULK",
            "csv": "wsa_bathymetry_maps.csv",
            "dest_dir": "bathymetry_pdfs",
        },
        # Spatial companion to bathymetry_maps: the survey polygons (lake outlines)
        # from the BC Geographic Warehouse, keyed by WATERBODY_IDENTIFIER.  Enables
        # the spatial best-overlap tier in pipeline.matching.bathymetry_matcher.
        "bathymetry_polygons": {
            "type": "WFS",
            "source": "WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW",
        },
        # OSM populated places (city/town/village/hamlet) gazetteer.  Saved to
        # data/bc_places.json and used by the enrichment step to tag every named
        # waterbody with its nearest towns (search-index only).
        "osm_places": {
            "type": "OSM_PLACES",
            "dest": "bc_places.json",
        },
    }
    parser = argparse.ArgumentParser(description="BC Fresh Water Data Fetcher")
    parser.add_argument("--layers", nargs="+", help="Explicitly list layers to fetch")
    parser.add_argument(
        "--skip-streams", action="store_true", help="Skip heavy stream network merge"
    )
    parser.add_argument(
        "--skip-ftp", action="store_true", help="Skip all heavy FTP downloads"
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help=(
            "Skip layers already present in the output gpkg/files — fetch only "
            "what's new or missing (e.g. to resume after a partial/failed run "
            "without re-fetching everything already done)."
        ),
    )
    parser.add_argument(
        "--add-parsing-data",
        action="store_true",
        help=(
            "Restore backed-up extraction + parsing outputs from the build-assets "
            "bucket (build.canifishthis.ca) so the pipeline can be rebuilt without "
            "re-running extraction/parsing, then exit."
        ),
    )
    args = parser.parse_args()

    if not temp_dir.exists():
        temp_dir.mkdir(parents=True)

    if args.add_parsing_data:
        fetch_parsing_backup(config)
        print("\n✅ Parsing data restored from backup!")
        return

    # Filtering Logic
    if args.layers:
        to_fetch = {k: v for k, v in DATASETS.items() if k in args.layers}
    else:
        to_fetch = DATASETS.copy()
        if args.skip_ftp:
            to_fetch = {
                k: v
                for k, v in to_fetch.items()
                if "ftp" not in v and v["type"] != "FWA_STREAMS"
            }
        elif args.skip_streams:
            to_fetch.pop("streams", None)

    if args.missing_only:
        existing_layers = set(fiona.listlayers(gpkg_out)) if gpkg_out.exists() else set()
        before = set(to_fetch)
        to_fetch = {
            k: v for k, v in to_fetch.items()
            if not _layer_already_fetched(k, v, gpkg_out, existing_layers)
        }
        skipped = before - set(to_fetch)
        if skipped:
            print(f"--missing-only: skipping {len(skipped)} already-fetched layer(s): "
                  f"{', '.join(sorted(skipped))}")

    for name, cfg in tqdm(
        to_fetch.items(), total=len(to_fetch), desc="Datasets", unit="layer"
    ):
        try:
            if cfg["type"] == "WFS":
                fetch_wfs_paginated(name, cfg["source"], gpkg_out, temp_dir)
            elif cfg["type"] == "PARCEL_FABRIC":
                fetch_parcel_fabric(name, cfg["url"], cfg["layer"], gpkg_out, temp_dir)
            elif cfg["type"] == "FWA_GDB":
                extract_gdb_layer(name, cfg["ftp"], cfg["layer"], gpkg_out, temp_dir)
            elif cfg["type"] == "FWA_STREAMS":
                combine_streams(name, cfg["ftp"], gpkg_out, temp_dir)
            elif cfg["type"] == "R2_GPKG":
                fetch_r2_gpkg_layer(name, cfg["url"], cfg["layer"], gpkg_out)
            elif cfg["type"] == "R2_FILE":
                fetch_r2_file(name, cfg["url"], gpkg_out.parent / cfg["dest"])
            elif cfg["type"] == "CSV_DOWNLOAD":
                fetch_csv_download(name, cfg["url"], gpkg_out.parent / cfg["dest"])
            elif cfg["type"] == "PDF_BULK":
                fetch_bathymetry_pdfs(
                    name,
                    gpkg_out.parent / cfg["csv"],
                    gpkg_out.parent / cfg["dest_dir"],
                    gpkg_path=gpkg_out,
                    poly_layer=cfg.get("poly_layer", "bathymetry_polygons"),
                )
            elif cfg["type"] == "OVERPASS_ABORIGINAL":
                fetch_overpass_aboriginal_lands(name, gpkg_out)
            elif cfg["type"] == "OVERPASS_LAND_ACCESS":
                fetch_overpass_land_access(name, gpkg_out)
            elif cfg["type"] == "OVERPASS_WATER_ACCESS":
                fetch_overpass_water_access(name, gpkg_out)
            elif cfg["type"] == "OVERPASS_WATERFALLS":
                fetch_overpass_waterfalls(name, gpkg_out)
            elif cfg["type"] == "OSM_PLACES":
                fetch_osm_places(name, gpkg_out.parent / cfg["dest"])
        except Exception as e:
            print(f"❌ Error on {name}: {e}")

    print("\n✅ Data fetch complete!")


if __name__ == "__main__":
    main()
