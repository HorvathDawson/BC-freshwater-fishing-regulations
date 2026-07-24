"""
match_fwa.py — first-pass gauge → FWA waterbody matcher (location + name).

For every hydrometric gauge in ``hydro.db`` this finds the real FWA waterbody
the gauge sits on, using **location + name together** (see
``HANDOFF_gauge_fwa_matching.md``):

  1. Reproject the gauge lat/lon (EPSG:4326) → EPSG:3005 (the atlas CRS).
  2. Find every *unique* waterbody within a 5 km radius across
     streams + under_lake_streams + lakes + wetlands + manmade.
  3. Parse the gauge name → strip the qualifier (NEAR/AT/ABOVE/BELOW/…) →
     ``normalize_name()`` → ``gauge_wb_name`` (e.g. "ATLIN LAKE").
  4. Keep only candidates whose normalized name is a substring of
     ``gauge_wb_name``. Because ``normalize_name()`` keeps the LAKE/CREEK/RIVER
     type word, this disambiguates "ATLIN LAKE" (polygon) from "ATLIN RIVER"
     (stream) for free.
  5. Record, per gauge: how many unique waterbodies were in 5 km, how many
     name-matched, and the matched feature(s).

Results land in a new ``gauge_fwa_match`` table in ``hydro.db`` and a headline
summary is printed (exactly-one / zero / >1). Everything goes through the
cached ``FreshWaterAtlas`` pickle — no direct GPKG reads.

Run:  .venv/bin/python live-data/hydro/match_fwa.py
"""

from __future__ import annotations

import logging
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

from pyproj import Transformer
from shapely.geometry import Point
from shapely.strtree import STRtree

# Run as a module (``python -m pipeline.recurring.hydro.match_fwa``) so these
# absolute imports resolve without a sys.path hack.
from pipeline.atlas.freshwater_atlas import FreshWaterAtlas
from pipeline.matching.base_entry_builder import normalize_name
from pipeline.matching.display_name_resolver import DisplayNameResolver
from pipeline.matching.match_table import FEATURE_DISPLAY_NAMES_PATH

from .hydro_poc import DB_PATH as HYDRO_DB

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("match_fwa")

FEATURE_DISPLAY_NAMES = FEATURE_DISPLAY_NAMES_PATH
RADIUS_M = 5_000.0

# Manual station → FWA stream overrides. The tidal Fraser mainstem reach is
# excluded from the atlas, so these gauges name-match nothing (they land in the
# "zero" bucket). Link each by hand to the nearest *named* Fraser River mainstem
# segment (fid) — the fid is the stream→reach join key (resolved fid→reach_id
# from the deploy shards at export time). blk / geometry are looked up from the
# atlas at run time (no second source of truth for the join key).
# (08HB087 "Comox Harbour", also tidal, is intentionally left unmatched.)
_MANUAL_STREAM_OVERRIDES = {
    "08MH028": "701651753",  # Fraser R. at Steveston
    "08MH044": "701649123",  # Fraser R. at Whonock
    "08MH126": "701651908",  # Fraser R. at Port Mann
}

# Qualifier keywords that separate "<WATERBODY NAME>" from "<PLACE>" in an
# ECCC station name. Longest-first so "ABOVE THE" wins over "ABOVE".
_QUALIFIERS = [
    "UPSTREAM OF",
    "DOWNSTREAM OF",
    "ABOVE THE",
    "BELOW THE",
    "SOUTH OF",
    "NORTH OF",
    "EAST OF",
    "WEST OF",
    "NEAR",
    "ABOVE",
    "BELOW",
    "AT",
]
_QUALIFIER_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(q) for q in _QUALIFIERS) + r")\b"
)


def parse_waterbody_name(station_name: str) -> str:
    """Strip the ECCC qualifier tail → normalized waterbody name.

    "ADAMS RIVER NEAR SQUILAX" → "ADAMS RIVER". Names that carry no qualifier
    keyword (e.g. "COQUITLAM LAKE FOREBAY") fall through whole — the leading
    token run is still the waterbody name, so the substring test below handles
    them.
    """
    m = _QUALIFIER_RE.search(station_name)
    head = station_name[: m.start()] if m else station_name
    return normalize_name(head)


def _atlas_path() -> Path:
    from project_config import ProjectConfig

    return ProjectConfig().get_path(
        "output", "pipeline", "atlas", default="output/pipeline/atlas/atlas.pkl"
    )


def build_candidate_index(atlas: FreshWaterAtlas, resolver: DisplayNameResolver):
    """Flatten every searchable atlas feature into parallel lists.

    Returns (geoms, meta) where meta[i] is a dict describing geoms[i]:
      layer, fwa_id, waterbody_key, blk, norm_names, cand_key.

    ``norm_names`` is the set of ALL normalized names a gauge may match against
    for that feature — the FWA gnis_name PLUS any display-override and
    ``name_variants`` from ``feature_display_names.json`` (the single tier0
    source of aliases: reservoir names, local/gauge names like "Cedar Creek",
    and names given to previously-unnamed streams). This is why the alias is
    never defined twice — the same file feeds front-end search and this matcher.

    ``cand_key`` collapses a physical waterbody to one candidate (polygons →
    waterbody_key; streams → resolved display name, so a named river's hundreds
    of segments — and a formerly-unnamed blue line now named via override —
    become one candidate). Features with no name at all are skipped.
    """
    geoms: list = []
    meta: list = []

    def _norm_set(*names: str) -> list:
        seen: dict = {}
        for n in names:
            nn = normalize_name(n)
            if nn:
                seen[nn] = None
        return list(seen)

    # Polygon layers: one record per waterbody_key already.
    for layer in ("lakes", "wetlands", "manmade"):
        for wbk, rec in getattr(atlas, layer).items():
            display = resolver.resolve_polygon(wbk, rec.display_name)
            norm_names = _norm_set(
                display, rec.display_name, *resolver.variants_for_polygon(wbk)
            )
            if not norm_names:
                continue
            geoms.append(rec.geometry)
            meta.append(
                {
                    "layer": layer,
                    "fwa_id": wbk,
                    "waterbody_key": wbk,
                    "blk": "",
                    "norm_names": norm_names,
                    "cand_key": ("poly", wbk),
                }
            )

    # Streams (open-air + under-lake). Many segments per named river.
    for layer in ("streams", "under_lake_streams"):
        for fid, rec in getattr(atlas, layer).items():
            display = resolver.resolve_stream(rec.blk, rec.display_name, fid=fid)
            norm_names = _norm_set(
                display,
                rec.display_name,
                *resolver.variants_for_stream(blk=rec.blk, fid=fid),
            )
            if not norm_names:
                continue
            # Collapse a river's segments by its resolved display name; fall
            # back to the first name so unnamed-but-aliased blue lines still
            # dedupe consistently.
            key_name = normalize_name(display) or norm_names[0]
            geoms.append(rec.geometry)
            meta.append(
                {
                    "layer": layer,
                    "fwa_id": fid,
                    "waterbody_key": rec.waterbody_key,
                    "blk": rec.blk,
                    "norm_names": norm_names,
                    "cand_key": ("stream", key_name),
                }
            )

    logger.info("Candidate index: %s named features", f"{len(geoms):,}")
    return geoms, meta


def build_openair_blk_index(atlas: FreshWaterAtlas) -> dict:
    """blk → list of (fid, geometry) for open-air ``streams`` segments only.

    Reaches are built from open-air streams; ``under_lake_streams`` are excluded,
    so an under-lake nearest segment never resolves to a reach. When a gauge's
    best stream match is under-lake we swap in the geometrically nearest open-air
    segment on the *same blue line* (same physical watercourse) — that fid is in
    a reach, so it resolves. See ``_openair_fid``.
    """
    idx: dict = defaultdict(list)
    for fid, rec in atlas.streams.items():
        idx[rec.blk].append((fid, rec.geometry))
    return idx


def _openair_fid(blk_index: dict, blk: str, pt: Point) -> str | None:
    """Nearest open-air stream fid on ``blk`` to ``pt`` (None if blk unknown)."""
    sibs = blk_index.get(blk)
    if not sibs:
        return None
    return min(sibs, key=lambda fg: pt.distance(fg[1]))[0]


def match_all(atlas: FreshWaterAtlas | None = None) -> None:
    """Run the name+location match and write the tables.

    Accepts a pre-loaded atlas so callers (e.g. match_closest.py) can share the
    expensive 5 GB pickle load across several analyses in one process.
    """
    if atlas is None:
        atlas = FreshWaterAtlas.load(_atlas_path())
    resolver = DisplayNameResolver(FEATURE_DISPLAY_NAMES)
    geoms, meta = build_candidate_index(atlas, resolver)
    openair_blk = build_openair_blk_index(atlas)

    logger.info("Building STRtree …")
    tree = STRtree(geoms)

    transformer = Transformer.from_crs(4326, 3005, always_xy=True)

    conn = sqlite3.connect(HYDRO_DB)
    stations = conn.execute(
        "SELECT station_id, name, lat, lon FROM stations "
        "WHERE lat IS NOT NULL AND lon IS NOT NULL"
    ).fetchall()
    logger.info("Stations to match: %s", len(stations))

    # (station_id, layer, fwa_id, waterbody_key, blk, distance_m, is_primary,
    #  resolution). Stream links join by fwa_id (fid→reach_id at export);
    #  polygon links by waterbody_key.
    match_rows: list[tuple] = []
    # per-station bookkeeping for the headline
    n_exactly_one = n_zero = n_ambiguous = 0
    n_override = 0
    summary_rows: list[tuple] = []

    for station_id, name, lat, lon in stations:
        x, y = transformer.transform(lon, lat)
        pt = Point(x, y)
        gauge_wb = parse_waterbody_name(name)

        # Manual override (tidal Fraser): resolve straight from the atlas and
        # skip the name/radius match entirely — the reach isn't in the atlas.
        override_fid = _MANUAL_STREAM_OVERRIDES.get(station_id)
        if override_fid is not None:
            rec = atlas.streams.get(override_fid)
            if rec is None:
                raise KeyError(
                    f"override fid {override_fid} for {station_id} not in atlas.streams"
                )
            d = round(pt.distance(rec.geometry), 1)
            match_rows.append(
                (station_id, "streams", override_fid, rec.waterbody_key,
                 rec.blk, d, 1, "override")
            )
            n_override += 1
            summary_rows.append((station_id, gauge_wb, 0, 0))
            continue

        # 5 km radius query via bounding-box prefilter, then exact distance.
        idxs = tree.query(pt.buffer(RADIUS_M))

        # Collapse to unique waterbodies within the radius, tracking the
        # closest geometry per candidate.
        nearby: dict = {}  # cand_key -> (dist, meta)
        for i in idxs:
            m = meta[i]
            d = pt.distance(geoms[i])
            if d > RADIUS_M:
                continue
            key = m["cand_key"]
            prev = nearby.get(key)
            if prev is None or d < prev[0]:
                nearby[key] = (d, m)

        n_unique = len(nearby)

        # Name match: ANY of the candidate's names (gnis + display-override +
        # aliases) appears in the gauge name.
        matched = [
            (d, m)
            for (d, m) in nearby.values()
            if gauge_wb and any(nn in gauge_wb for nn in m["norm_names"])
        ]
        matched.sort(key=lambda t: t[0])
        n_matched = len(matched)

        if n_matched == 1:
            n_exactly_one += 1
        elif n_matched == 0:
            n_zero += 1
        else:
            n_ambiguous += 1

        # The primary match is the single confident waterbody the export links
        # the gauge to. Exactly-one → itself; >1 → closest-distance tiebreak
        # (matched is sorted ascending by distance, so index 0). All matched
        # rows are still written for review.
        for rank, (d, m) in enumerate(matched):
            is_primary = 1 if rank == 0 else 0
            resolution = (
                "single" if n_matched == 1 else "closest"
            ) if is_primary else "ambiguous"
            layer = m["layer"]
            fwa_id = m["fwa_id"]
            # Under-lake segments aren't in any reach; swap to the nearest
            # open-air segment on the same blue line so fid→reach_id resolves.
            if layer == "under_lake_streams":
                swap = _openair_fid(openair_blk, m["blk"], pt)
                if swap is not None:
                    layer, fwa_id = "streams", swap
            match_rows.append(
                (
                    station_id,
                    layer,
                    fwa_id,
                    m["waterbody_key"],
                    m["blk"],
                    round(d, 1),
                    is_primary,
                    resolution,
                )
            )

        summary_rows.append((station_id, gauge_wb, n_unique, n_matched))

    _write_tables(conn, match_rows, summary_rows)
    conn.close()

    total = len(stations)
    logger.info("\n===== headline =====")
    logger.info("stations matched:        %s", total)
    logger.info("exactly-one (clean):     %s (%.1f%%)", n_exactly_one, 100 * n_exactly_one / total)
    logger.info("zero name-match:         %s (%.1f%%)", n_zero, 100 * n_zero / total)
    logger.info(">1 (ambiguous):          %s (%.1f%%)", n_ambiguous, 100 * n_ambiguous / total)
    logger.info("manual overrides:        %s", n_override)
    n_primary = n_exactly_one + n_ambiguous + n_override
    logger.info("primary (export-linked): %s (%.1f%%)", n_primary, 100 * n_primary / total)


def _write_tables(conn, match_rows, summary_rows) -> None:
    conn.execute("DROP TABLE IF EXISTS gauge_fwa_match")
    conn.execute(
        """
        CREATE TABLE gauge_fwa_match (
            station_id    TEXT,
            layer         TEXT,
            fwa_id        TEXT,
            waterbody_key TEXT,
            blue_line_key TEXT,
            distance_m    REAL,
            is_primary    INTEGER,
            resolution    TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO gauge_fwa_match VALUES (?,?,?,?,?,?,?,?)", match_rows
    )

    conn.execute("DROP TABLE IF EXISTS gauge_fwa_match_summary")
    conn.execute(
        """
        CREATE TABLE gauge_fwa_match_summary (
            station_id     TEXT PRIMARY KEY,
            parsed_name    TEXT,
            n_unique_5km   INTEGER,
            n_name_matched INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO gauge_fwa_match_summary VALUES (?,?,?,?)", summary_rows
    )
    conn.commit()
    logger.info("Wrote %s match rows for %s stations", len(match_rows), len(summary_rows))


if __name__ == "__main__":
    match_all()
