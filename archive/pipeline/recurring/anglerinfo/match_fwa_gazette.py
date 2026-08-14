#!/usr/bin/env python3
"""
match_fwa_gazette.py — second-pass corroboration over match.py's own
`match_wbid`: an independent *current* FWA waterbody for every row WDIC
already matched, found by name rather than identifier.

Input: match_wbid only
--------------------------
This module reads *exclusively* from `match_wbid` (match.py's own output)
— never `fidq_stocking_records`/`bathy_surveys`/`map_markers` directly. Every
name this searches on (`name` plus each of `name_variants` — see
load_match_wbid()) and the anchor point it disambiguates by
(`approx_lat`/`approx_lng`) already went through match.py's own
identifier-match pass; this module has no independent read of the raw
per-source tables at all.

Why this exists
-----------------
match.py deliberately stops at "does this identifier exist in WDIC" — no
FWA join at all (see its own docstring). WDIC is a frozen 1:50K reference
snapshot, though: it can't tell you what FWA calls that same waterbody
*today* (FWA's own generalized polygons can drift from what an identifier
meant when WDIC's snapshot was taken — confirmed live, PEAR LAKE's FWA
WATERSHED_GROUP_CODE changed after FIDQ's own snapshot). This module bridges
that gap independently of the identifier scheme entirely: a gazetteer
name search (`pipeline/matching/base_entry_builder.py`'s own
`_natural_search()` — the same name-matching machinery the main regulations
pipeline and `common/waterbody_matcher.py`'s own T3 tier already use, reused
here rather than reimplemented) against `name` *and every* `name_variants`
entry from `match_wbid` (FIDQ's own comma-separated aliases, already split
into individual names by match.py — see its own `_split_variants()` — and
bathymetry's MAP_TITLE), since any one of those can be the name FWA's
gazetteer actually carries even when the primary name doesn't hit.

Overrides — same table the regulation matching uses, checked first
----------------------------------------------------------------------
Like the main regulations pipeline (`pipeline/matching/match_table.py`'s
`MatchTable` — "override always wins over a base entry"), a curated
correction is checked *before* the automated name search, not as a
last resort. This module doesn't reimplement that lookup or touch
`MatchTable` itself, though: `common/waterbody_matcher.py::build_override_index()`
already reads the exact same `pipeline/matching/overrides.json`
(`OverrideEntry`) table and indexes it by normalized name alone — the
production `MatchTable` key is fully qualified (region + name + MUs), which
this module has no region/MU to build (FIDQ/WSA/gofishbc records carry
neither), so it's already built to run "all regions at once" by construction
(the whole reason live-data's own matchers use it instead of `MatchTable`
directly — see that function's own docstring). Reused here unchanged, same
as `stocking/match_waterbodies.py` and `bathymetry/match_bathymetry.py`
already do for their own T0/T4 override tiers.

An override match here is gated by distance to the row's own WDIC anchor
point (`_MAX_OVERRIDE_ANCHOR_KM`) rather than accepted unconditionally:
`common/waterbody_matcher.py`'s own `override_correction_is_plausible()`
exists for exactly this reason — confirmed live there, an unscoped name-only
override lookup once silently "corrected" a bathymetry survey's "ROSE LAKE"
(a common name shared by multiple unrelated lakes) to one 587km away. This
module has something that production-pipeline override checks don't: a real
independent coordinate (WDIC's own) to check a candidate against directly,
so the gate here is a straightforward point-to-polygon distance rather than
pool-to-pool centroid comparison.

Disambiguation (no matching override): distance, not watershed code
------------------------------------------------------------------------
Whatever no override resolves falls through to the natural-search tier: a
name search routinely returns more than one FWA candidate (common names like
"Blue Lake" have 9+ province-wide). `common/waterbody_matcher.py`'s own T3
tier narrows that with the row's *own* watershed-group-code suffix. This
module has something more direct available instead: `match_wbid.approx_lat`
/`approx_lng`, the representative point of the row's own WDIC polygon (see
match.py) — real geographic ground truth from an independent source, not
derived from the name search at all. So the disambiguation here is
purely geometric: reproject the WDIC point to EPSG:3005 and keep whichever
deduplicated gazetteer candidate's FWA polygon is physically closest to it
(0 if the point falls inside that polygon). Only rows `match.py` itself
resolved (`status = "matched"`) have that anchor point — this module only
processes those; a row WDIC itself couldn't match has nothing to disambiguate
by, so it isn't looked up here at all (`match.py`'s own report is where an
unmatched-in-WDIC row should be reviewed).

CLI
---
    python -m pipeline.recurring.anglerinfo.match_fwa_gazette                 # run after match.py; writes match_fwa_gazette
    python -m pipeline.recurring.anglerinfo.match_fwa_gazette --dry-run        # print only, no DB writes
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from pyproj import Transformer
from shapely.geometry import Point

from . import waterbody_matcher as wm
from pipeline.matching.base_entry_builder import _natural_search, normalize_name
from project_config import ProjectConfig

from .fetch_wdic import DB_PATH
from .match import osm_url

logger = logging.getLogger(__name__)

# WDIC's own representative_point() is stored WGS84 (see match.py); FWA
# polygons are BC Albers — same reprojection pattern
# common/waterbody_matcher.py's own locate_at_point() uses.
_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)

# Max distance (km) an override candidate may sit from the row's own WDIC
# anchor point and still be trusted — mirrors common/waterbody_matcher.py's
# own _MAX_OVERRIDE_CORRECTION_KM (same 50km convention, same reason: a
# common-name collision like "Rose Lake" shouldn't silently "correct" a row
# to an unrelated lake elsewhere in the province).
_MAX_OVERRIDE_ANCHOR_KM = 50.0


# Must match match.py's own _VARIANT_JOIN exactly — this is the other end
# of that split/join round trip through the match_wbid.name_variants column.
_VARIANT_JOIN = "; "


@dataclass
class WbidMatchRow:
    source: str
    source_id: str
    identifier: str
    name: str
    name_variants: List[str]
    approx_lat: float
    approx_lng: float


def load_match_wbid(conn: sqlite3.Connection) -> List[WbidMatchRow]:
    """Only rows match.py itself resolved against WDIC (status = "matched")
    — those are the only ones with an approx_lat/approx_lng anchor to
    disambiguate a name search's candidates by (see module docstring). Reads
    only match_wbid — see module docstring's "Input: match_wbid only"."""
    rows = conn.execute(
        """SELECT source, source_id, identifier, name, name_variants, approx_lat, approx_lng
           FROM match_wbid WHERE status = 'matched'"""
    ).fetchall()
    return [
        WbidMatchRow(source, source_id, identifier, name,
                     [v for v in (variants or "").split(_VARIANT_JOIN) if v], lat, lng)
        for source, source_id, identifier, name, variants, lat, lng in rows
    ]


@dataclass
class GazetteMatch:
    source: str
    source_id: str
    identifier: str
    name: str
    name_variants: List[str]
    approx_lat: float               # carried through from match_wbid — match_fwa_identifier.py's own anchor, reading only this table (see its module docstring)
    approx_lng: float
    status: str                # matched | unmatched (no override or gazetteer hit on name or any variant)
    tier: str = ""                # override | gazetteer_search
    n_candidates: int = 0        # deduplicated candidate count, before distance picks a winner
    waterbody_key: str = ""
    gnis_name: str = ""
    source_layer: str = ""
    distance_m: Optional[float] = None   # winning candidate's polygon distance from the WDIC anchor point (0 = point falls inside it)


_RE_SPACES = re.compile(r"\s+")


def _expand_lake_abbreviations(text: str) -> str:
    """Expand WSA MAP_TITLE's common abbreviations ("L."/"LK"/"LKS."/"CR."
    -> "LAKE"/"LAKES"/"CREEK") — deliberately *not*
    `pipeline/matching/bathymetry_matcher.py::normalize_map_title()`, which
    also strips "#N"/"NO. N" suffixes. This project's own gazetteer search
    treats a numbered lake as a genuinely different waterbody from the plain
    name (confirmed live: "COLLIERY #3" and "COLLIERY" — two distinct,
    separately-overridden identifiers in wbid_overrides.json, not the same
    lake with a typo) — stripping the number risks a *false* match between
    them instead of just a missed one, so this only expands the abbreviation
    itself, never touches numbering."""
    text = text.strip().upper()
    text = re.sub(r"\bLKS\.?\b", "LAKES", text)
    text = re.sub(r"\bLK\.?\b", "LAKE", text)
    text = re.sub(r"\bL\.\b", "LAKE", text)
    text = re.sub(r"\bL\b", "LAKE", text)
    text = re.sub(r"\bCR\.?\b", "CREEK", text)
    # \bL\.\b never actually matches (the trailing "." isn't itself a word
    # boundary) — it's the bare \bL\b rule above that catches "L." (matching
    # just the letter, leaving the period attached as "...LAKE."), so a
    # trailing/leading period still needs stripping here — confirmed live,
    # without this "TATLATUI L." expanded to "TATLATUI LAKE." (trailing
    # period intact) and missed the gazetteer lookup entirely.
    return _RE_SPACES.sub(" ", text).strip().strip(".").strip()


def _search_candidates(index: "wm.LakeIndex", *names: str) -> Set[str]:
    """Deduplicated waterbody_key candidates across every non-blank name
    (primary name, then each name_variants entry) — a plain union, since any
    one of them independently naming the right FWA waterbody is equally good
    corroboration.

    Three lookups per name, in order:
      1. _natural_search() against index.gazetteer_index (full GNIS names,
         title-cased) — the primary search.
      2. index.name_variant_index (generic-suffix-stripped, normalize_name()
         -keyed) — gofishbc marker titles routinely drop the generic type
         word a full gazetted name always carries (confirmed live: "Abbott"/
         "Agur"/"Aileen" vs FWA's "Abbott Lake"/"Agur Lake"/"Aileen Lake" —
         the exact problem live-data/stocking/match_waterbodies.py's own
         T2_name_only tier documents; without this fallback marker rows
         matched ~1.2% instead of ~70%+).
      3. `_expand_lake_abbreviations()` (this module's own, narrower than
         `pipeline/matching/bathymetry_matcher.py::normalize_map_title()` —
         see that function's own docstring for why "#N"/"NO. N" numbering is
         deliberately never stripped here) against the gazetteer index again
         — confirmed live, WSA bathymetry MAP_TITLE "TATLATUI L." doesn't hit
         either of the first two lookups (FWA's own gazetteer only has the
         full "TATLATUI LAKE"), but this expands it to exactly that, and the
         distance-to-anchor pick then lands on the same waterbody_key
         (329136015) `bathymetry/bathy_overrides.json` already curated for
         this exact identifier independently — confirming the expansion is
         correct, not a coincidence.
    All three reuse existing machinery rather than reimplementing any of it."""
    keys: Set[str] = set()
    for raw_name in names:
        if not raw_name or not raw_name.strip():
            continue
        refs, _ = _natural_search(index.gazetteer_index, raw_name, None, [])
        if refs:
            keys.update(r["waterbody_key"] for r in refs)
            continue
        variant_hit = index.name_variant_index.get(normalize_name(raw_name), set())
        if variant_hit:
            keys.update(variant_hit)
            continue
        expanded = _expand_lake_abbreviations(raw_name)
        if expanded and expanded != normalize_name(raw_name):
            refs, _ = _natural_search(index.gazetteer_index, expanded, None, [])
            keys.update(r["waterbody_key"] for r in refs)
    return keys


def _override_candidates(
    override_index: Dict[str, List["wm.OverrideCandidate"]], *names: str,
) -> Set[str]:
    """Deduplicated waterbody_key candidates from the curated override table
    (see module docstring) across the primary name and every name_variants
    entry — same "any known name is equally good corroboration" reasoning
    as _search_candidates()."""
    keys: Set[str] = set()
    for raw_name in names:
        norm = normalize_name(raw_name)
        if not norm:
            continue
        keys.update(c.waterbody_key for c in override_index.get(norm, []))
    return keys


def _closest(keys: Set[str], anchor: Point, index: "wm.LakeIndex") -> Tuple[Optional[str], Optional[float]]:
    """Whichever of ``keys`` has a polygon physically closest to ``anchor``
    (0 if the point falls inside it) — shared by both the override tier and
    the gazetteer-search tier below, since both need the same "pick the one
    candidate nearest the WDIC anchor" step, just over a different candidate
    set."""
    best_key, best_dist = None, None
    for key in keys:
        geom = index.key_geom.get(key)
        if geom is None:
            continue
        dist = geom.distance(anchor)
        if best_dist is None or dist < best_dist:
            best_key, best_dist = key, dist
    return best_key, best_dist


def match_gazette(
    rows: List[WbidMatchRow], index: "wm.LakeIndex",
    override_index: Dict[str, List["wm.OverrideCandidate"]],
) -> List[GazetteMatch]:
    results: List[GazetteMatch] = []
    for r in rows:
        x, y = _TO_ALBERS.transform(r.approx_lng, r.approx_lat)
        anchor = Point(x, y)

        # Tier 1 — curated override, checked first (see module docstring's
        # "Overrides" section): "override always wins", same philosophy as
        # the main regulations pipeline's own MatchTable, gated by distance
        # to the WDIC anchor instead of MatchTable's region/MU scoping
        # (which this module has none of to key by).
        override_keys = _override_candidates(override_index, r.name, *r.name_variants)
        if override_keys:
            key, dist = _closest(override_keys, anchor, index)
            if key is not None and dist is not None and dist <= _MAX_OVERRIDE_ANCHOR_KM * 1000:
                results.append(GazetteMatch(
                    r.source, r.source_id, r.identifier, r.name, r.name_variants,
                    r.approx_lat, r.approx_lng, "matched",
                    tier="override", n_candidates=len(override_keys), waterbody_key=key,
                    gnis_name=index.display_name.get(key, ""),
                    source_layer=index.source_layer.get(key, ""),
                    distance_m=dist,
                ))
                continue
            # Override found by name but implausibly far from the WDIC
            # anchor (or missing geometry) — likely a name collision (see
            # ROSE LAKE in the module docstring), not a correction for this
            # specific row. Don't trust it; fall through to the automated
            # gazetteer tier below instead of blocking on a bad name match.

        # Tier 2 — gazetteer natural-language search + name_variant_index
        # fallback, disambiguated by the same distance-to-anchor pick.
        candidates = _search_candidates(index, r.name, *r.name_variants)
        if not candidates:
            results.append(GazetteMatch(
                r.source, r.source_id, r.identifier, r.name, r.name_variants,
                r.approx_lat, r.approx_lng, "unmatched",
            ))
            continue

        key, dist = _closest(candidates, anchor, index)
        if key is None:
            # Every candidate's own geometry was missing from the index
            # (shouldn't happen — gazetteer_index and key_geom are built from
            # the same FWA rows — but degrade to "found names, no geometry to
            # disambiguate by" rather than silently picking one at random).
            results.append(GazetteMatch(
                r.source, r.source_id, r.identifier, r.name, r.name_variants,
                r.approx_lat, r.approx_lng, "unmatched",
                n_candidates=len(candidates),
            ))
            continue

        results.append(GazetteMatch(
            r.source, r.source_id, r.identifier, r.name, r.name_variants,
            r.approx_lat, r.approx_lng, "matched",
            tier="gazetteer_search", n_candidates=len(candidates), waterbody_key=key,
            gnis_name=index.display_name.get(key, ""),
            source_layer=index.source_layer.get(key, ""),
            distance_m=dist,
        ))
    return results


def write_matches(conn: sqlite3.Connection, results: List[GazetteMatch]) -> None:
    conn.execute("DROP TABLE IF EXISTS match_fwa_gazette")
    conn.execute(
        """CREATE TABLE match_fwa_gazette (
            source              TEXT NOT NULL,
            source_id           TEXT NOT NULL,
            identifier          TEXT,
            name                TEXT,
            name_variants       TEXT,
            approx_lat          REAL,
            approx_lng          REAL,
            approx_map_url      TEXT,
            status              TEXT NOT NULL,
            tier                TEXT,
            n_candidates        INTEGER,
            waterbody_key       TEXT,
            gnis_name           TEXT,
            source_layer        TEXT,
            distance_m          REAL,
            matched_at          TEXT,
            PRIMARY KEY (source, source_id)
        )"""
    )
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        """INSERT INTO match_fwa_gazette
           (source, source_id, identifier, name, name_variants, approx_lat, approx_lng, approx_map_url,
            status, tier, n_candidates, waterbody_key, gnis_name, source_layer, distance_m, matched_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (r.source, r.source_id, r.identifier, r.name, _VARIANT_JOIN.join(r.name_variants),
             r.approx_lat, r.approx_lng, osm_url(r.approx_lat, r.approx_lng), r.status, r.tier, r.n_candidates,
             r.waterbody_key, r.gnis_name, r.source_layer, r.distance_m, now)
            for r in results
        ],
    )
    conn.commit()


def _print_breakdown(results: List[GazetteMatch]) -> None:
    print("\n" + "=" * 78)
    print("WBID_MATCHES -> FWA GAZETTEER SEARCH (name+variants, closest to WDIC anchor)")
    print("=" * 78)
    for source in ("stocking", "bathymetry", "map_markers"):
        subset = [r for r in results if r.source == source]
        if not subset:
            continue
        matched = [r for r in subset if r.status == "matched"]
        total = len(subset)
        pct = f"{len(matched) / total:.1%}" if total else "—"
        print(f"\n{source}: {len(matched)}/{total} matched ({pct})")

    total = len(results)
    matched = [r for r in results if r.status == "matched"]
    print(f"\nCOMBINED: {len(matched)}/{total} matched ({len(matched)/total:.1%})" if total else "")

    if matched:
        by_tier = {t: sum(1 for r in matched if r.tier == t) for t in ("override", "gazetteer_search")}
        print(f"  by tier: {by_tier['override']} override, {by_tier['gazetteer_search']} gazetteer_search")

    ties = [r for r in matched if r.n_candidates > 1]
    if ties:
        avg_dist = sum(r.distance_m for r in ties if r.distance_m is not None) / len(ties)
        print(f"  {len(ties)} row(s) had >1 candidate before the distance pick "
              f"(avg winning distance {avg_dist:,.0f}m from the WDIC anchor)")


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    cfg = ProjectConfig()
    parser = argparse.ArgumentParser(
        description="Corroborate match_wbid against FWA by gazetteer name search, "
                    "disambiguated by distance to each row's own WDIC anchor point.",
    )
    parser.add_argument(
        "--gpkg", type=Path,
        default=cfg.get_path("data", "fetch", "output_gpkg", default="data/bc_fisheries_data.gpkg"),
        help="FWA GeoPackage (lakes/wetlands/manmade layers).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Match and print the breakdown without writing match_fwa_gazette to anglerinfo.db.",
    )
    args = parser.parse_args(argv)

    if not DB_PATH.exists():
        raise FileNotFoundError(f"{DB_PATH} not found — run fetch_all.py then match.py first.")
    if not args.gpkg.exists():
        raise FileNotFoundError(f"FWA GeoPackage not found at {args.gpkg}")

    conn = sqlite3.connect(DB_PATH)
    try:
        rows = load_match_wbid(conn)
        if not rows:
            raise RuntimeError(
                f"{DB_PATH} has no matched match_wbid rows — run match.py first."
            )
        logger.info("%d match_wbid row(s) with a WDIC anchor point to search FWA against.", len(rows))

        logger.info("Building FWA lake index from %s ...", args.gpkg)
        index = wm.build_lake_index(args.gpkg)
        override_index = wm.build_override_index()
        logger.info("%d curated override name(s) loaded from %s.", len(override_index), wm.OVERRIDES_PATH)

        results = match_gazette(rows, index, override_index)

        if args.dry_run:
            logger.info("--dry-run: not writing match_fwa_gazette to anglerinfo.db")
        else:
            write_matches(conn, results)
    finally:
        conn.close()

    _print_breakdown(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
