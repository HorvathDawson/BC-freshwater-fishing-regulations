"""
bathymetry_matcher.py — join BC WSA lake bathymetric survey maps to FWA waterbodies.

Each row of the WSA bathymetry catalogue describes one bathymetric survey (a PDF
map) keyed by a 50K waterbody identifier / watershed code.  This module resolves
every survey to the FWA polygon(s) it covers using two tiers only: an exact
identifier join, and — for everything the identifier cannot settle — a geometric
best-overlap match.  Every match records *which* tier fired and *what assumption*
it relied on, so the output is fully auditable.

Matching is restricted to polygon waterbodies — **lakes and wetlands only**
(never streams): a bathymetric survey is always of a standing waterbody.

The bathy identifier decomposes as ``zfill(WATERBODY_KEY_50K, 5) + WATERSHED_GROUP_CODE_50K`` — e.g. ``01042CARP`` is waterbody-key-50K ``1042`` in watershed group ``CARP``.  When the group code covers a single waterbody the join is unambiguous; when it covers several, the normalised name must uniquely select one (both ``GNIS_NAME_1`` and ``GNIS_NAME_2`` are indexed).  Everything the identifier cannot settle is handed to the geometric tier.

Tier cascade (first hit wins)
-----------------------------
  T1  identifier            Exact WATERBODY_KEY_GROUP_CODE_50K join.  When the
                            group code covers >1 waterbody, the normalised name
                            must uniquely select one.
  T3  name + wsc-50K        Deterministic GNIS name + WATERSHED_CODE_50K join.
                            The 50K watershed code is essentially unique per
                            waterbody, so a survey whose name *and* code both
                            match is a safe join even without the identifier.
  T2  spatial best-overlap   Everything the deterministic tiers cannot settle:
                            intersect the survey's own polygon
                            (WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW, keyed by
                            WATERBODY_IDENTIFIER) with the FWA lakes/wetlands and
                            keep the waterbody with the largest overlap area,
                            provided it clears a minimum overlap fraction.

Anything no tier can resolve is reported as ambiguous with its PDF URL, so it
can be settled by hand.

CLI
---
    python -m pipeline.matching.bathymetry_matcher
    python -m pipeline.matching.bathymetry_matcher \\
        --bathy data/wsa_bathymetry_maps.csv \\
        --out output/pipeline/matching/bathymetry_matches.csv
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import geopandas as gpd
import pandas as pd

from project_config import ProjectConfig
from pipeline.extraction.extract_synopsis import _normalize_unicode

logger = logging.getLogger(__name__)

# --- WSA bathymetry CSV column names -----------------------------------------
COL_ID = "WATERBODY_IDENTIFIER_WSA_50K"
COL_NAME = "GAZETTED_NAME"
COL_WSC = "WATERSHED_CODE_WSA_50K"
COL_MAPTITLE = "MAP_TITLE"
COL_PDF = "PDF_FILE_URL"
# The unique per-map-sheet filename (e.g. ``00045501.PDF``).  A single survey
# (COL_ID) can span several sheets, so this — not the identifier — is the unique
# key for a downloadable PDF.  Downloaded/served lowercased (see fetch_data.py
# and seed-r2.sh), so all filename handling here lowercases to match the R2 object.
COL_PDF_FILENAME = "MAP_IMAGE_FILENAME_PDF"

# Polygon layers eligible for a bathymetric survey (never streams).  Manmade
# waterbodies (reservoirs, dugouts, some dammed 'lakes' like Goose Lake) live in
# their own FWA layer, so they must be included alongside natural lakes/wetlands.
POLYGON_LAYERS = ("lakes", "wetlands", "manmade")

# Spatial best-overlap tier (T6): the survey-polygon layer fetched from the BC
# Geographic Warehouse (WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW) into the same gpkg.
BATHY_POLYGON_LAYER = "bathymetry_polygons"
COL_BATHY_POLY_ID = "WATERBODY_IDENTIFIER"
# A spatial match is only accepted when the survey polygon overlaps the winning
# FWA waterbody by at least this fraction of the survey's own area (a 1:50K survey
# and its 1:20K FWA lake should overlap heavily; below this is treated as noise).
_MIN_SPATIAL_OVERLAP = 0.10
_SPATIAL_BBOX_PAD_M = 500.0

# Watershed codes that carry no location signal (mainstem / unknown sentinels).
_SENTINELS = {"9" * 45, "3" + "0" * 44, "1" + "0" * 44}

_RE_BRACKETS = re.compile(r"\s*\([^)]*\)\s*")   # mirrors generate_overrides parenthetical strip
_RE_SPACES = re.compile(r"\s+")


# ---------------------------------------------------------------------------
# Name normalisation (reuses the pipeline's own unicode folder)
# ---------------------------------------------------------------------------
def normalize_name(value: object) -> str:
    """Codebase-style waterbody-name key: unicode-fold, drop parentheticals,
    collapse whitespace, uppercase.  Keeps the LAKE/CREEK type word so that
    'Goose Lake' can never collide with 'Goose Creek'."""
    if not isinstance(value, str):
        return ""
    text = _normalize_unicode(value)
    text = _RE_BRACKETS.sub(" ", text)
    return _RE_SPACES.sub(" ", text).strip().upper()


def normalize_map_title(value: object) -> str:
    """Normalise an abbreviated MAP_TITLE into a comparable name.

    Used only as a fallback when GAZETTED_NAME is missing: expands the common
    'L.' / 'LK' / 'LKS.' / 'CR.' abbreviations and strips '#N' / 'NO. N' and
    asterisk clutter.
    """
    text = normalize_name(value)
    text = text.replace("*", " ")
    text = re.sub(r"\bNO\.?\s*\d+\b", " ", text)
    text = re.sub(r"#\s*\d+", " ", text)
    text = re.sub(r"\bLKS\.?\b", "LAKES", text)
    text = re.sub(r"\bLK\.?\b", "LAKE", text)
    text = re.sub(r"\bL\.\b", "LAKE", text)
    text = re.sub(r"\bL\b", "LAKE", text)
    text = re.sub(r"\bCR\.?\b", "CREEK", text)
    return _RE_SPACES.sub(" ", text).strip()


def strip_wsc(value: object) -> str:
    """Dash-strip a dashed watershed code so it aligns with FWA WATERSHED_CODE_50K."""
    if not isinstance(value, str):
        return ""
    return value.replace("-", "").strip()


# ---------------------------------------------------------------------------
# FWA polygon index
# ---------------------------------------------------------------------------
@dataclass
class PolygonIndex:
    """Lookup structures over the FWA lake + wetland polygons."""

    key_names: Dict[str, Set[str]]             # waterbody key -> {normalised GNIS names 1 & 2}
    key_wsc: Dict[str, str]
    group_code_map: Dict[str, Set[str]]        # WATERBODY_KEY_GROUP_CODE_50K -> keys
    wsc_map: Dict[str, Set[str]]               # WATERSHED_CODE_50K -> keys
    name_index: Dict[str, Set[str]]            # normalised name -> keys (province-wide; for reporting only)
    group_name_index: Dict[Tuple[str, str], Set[str]]   # (watershed group code, name) -> keys


def build_polygon_index(gpkg_path: Path) -> PolygonIndex:
    """Read lakes + wetlands from the GeoPackage and build all lookup tables."""
    cols = [
        "WATERBODY_KEY",
        "GNIS_NAME_1",
        "GNIS_NAME_2",
        "WATERSHED_GROUP_CODE",
        "WATERSHED_GROUP_CODE_50K",
        "WATERBODY_KEY_GROUP_CODE_50K",
        "WATERSHED_CODE_50K",
    ]
    frames = []
    for layer in POLYGON_LAYERS:
        logger.info("Reading FWA layer %r ...", layer)
        frames.append(gpd.read_file(gpkg_path, layer=layer, columns=cols))
    poly = pd.concat(frames, ignore_index=True)
    poly["nm"] = poly["GNIS_NAME_1"].fillna("").map(normalize_name)
    poly["nm2"] = poly["GNIS_NAME_2"].fillna("").map(normalize_name)
    poly["wsc"] = poly["WATERSHED_CODE_50K"].fillna("")
    # Watershed group code: prefer the 50K variant, fall back to the base code
    # (some polygons — e.g. Lower Arrow Lake — have null 50K codes but a valid base).
    grp_50k = poly["WATERSHED_GROUP_CODE_50K"].fillna("").str.strip()
    grp_base = poly["WATERSHED_GROUP_CODE"].fillna("").str.strip()
    poly["grp"] = grp_50k.where(grp_50k != "", grp_base)

    key_wsc = dict(zip(poly["WATERBODY_KEY"], poly["wsc"]))

    grp = poly.dropna(subset=["WATERBODY_KEY_GROUP_CODE_50K"])
    group_code_map = (
        grp.groupby("WATERBODY_KEY_GROUP_CODE_50K")["WATERBODY_KEY"]
        .agg(lambda x: set(x))
        .to_dict()
    )
    wsc_rows = poly[poly["wsc"] != ""]
    wsc_map = wsc_rows.groupby("wsc")["WATERBODY_KEY"].agg(lambda x: set(x)).to_dict()

    # Index BOTH gazetteer names (GNIS_NAME_1 and GNIS_NAME_2 / GNIS_ID_1 & 2) so a
    # survey's gazetted name resolves against the primary name OR a recorded alias
    # — mirrors base_entry_builder._build_name_index.
    key_names: Dict[str, Set[str]] = defaultdict(set)
    name_index: Dict[str, Set[str]] = defaultdict(set)
    group_name_index: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for key, nm1, nm2, group in zip(poly["WATERBODY_KEY"], poly["nm"], poly["nm2"], poly["grp"]):
        for name in (nm1, nm2):
            if not name:
                continue
            key_names[key].add(name)
            name_index[name].add(key)
            if group:
                group_name_index[(group, name)].add(key)

    return PolygonIndex(
        key_names=key_names,
        key_wsc=key_wsc,
        group_code_map=group_code_map,
        wsc_map=wsc_map,
        name_index=name_index,
        group_name_index=group_name_index,
    )


# ---------------------------------------------------------------------------
# Match result
# ---------------------------------------------------------------------------
@dataclass
class MatchResult:
    identifier: str
    name: str
    watershed_code: str
    tier: str
    method: str
    assumption: str
    waterbody_keys: List[str] = field(default_factory=list)
    status: str = "resolved"          # resolved | ambiguous
    ambiguity_reason: str = ""
    pdf_url: str = ""

    @property
    def n_waterbodies(self) -> int:
        return len(self.waterbody_keys)

    def to_row(self) -> Dict[str, object]:
        return {
            "bathy_identifier": self.identifier,
            "bathy_name": self.name,
            "watershed_code": self.watershed_code,
            "status": self.status,
            "match_tier": self.tier,
            "match_method": self.method,
            "assumption": self.assumption,
            "n_waterbodies": self.n_waterbodies,
            "waterbody_keys": ";".join(str(k) for k in self.waterbody_keys),
            "ambiguity_reason": self.ambiguity_reason,
            # PDF carried for ambiguous rows and for spatially-resolved rows (per
            # requirement + so a reviewer can verify an automatic overlap match).
            "pdf_url": self.pdf_url
            if self.status == "ambiguous" or self.tier == "T2_spatial_overlap"
            else "",
        }


def _disambiguate_by_name(cands: Set[str], name: str, index: PolygonIndex) -> Optional[Set[str]]:
    """Reduce a candidate set to a single waterbody using either GNIS name (1 or 2)."""
    if len(cands) == 1:
        return set(cands)
    if not name:
        return None
    named = {k for k in cands if name in index.key_names.get(k, set())}
    return set(named) if len(named) == 1 else None


def match_row(row: pd.Series, index: PolygonIndex) -> MatchResult:
    """Resolve one bathymetry survey row through the tier cascade."""
    identifier = str(row.get(COL_ID, "") or "")
    raw_name = row.get(COL_NAME)
    name = normalize_name(raw_name) or normalize_map_title(row.get(COL_MAPTITLE))
    wsc = strip_wsc(row.get(COL_WSC))
    pdf_url = str(row.get(COL_PDF, "") or "")

    display_name = raw_name if isinstance(raw_name, str) and raw_name.strip() else str(row.get(COL_MAPTITLE, ""))

    def result(tier: str, method: str, assumption: str, keys: Set[str]) -> MatchResult:
        return MatchResult(
            identifier=identifier,
            name=display_name,
            watershed_code=wsc,
            tier=tier,
            method=method,
            assumption=assumption,
            waterbody_keys=sorted(keys),
            status="resolved",
            pdf_url=pdf_url,
        )

    # T1 — exact group-code identifier join
    if identifier in index.group_code_map:
        picked = _disambiguate_by_name(index.group_code_map[identifier], name, index)
        if picked:
            note = "unique group code" if len(index.group_code_map[identifier]) == 1 else "group code + name"
            return result(
                "T1_identifier",
                "WATERBODY_KEY_GROUP_CODE_50K exact join",
                f"bathy identifier == FWA group code ({note})",
                picked,
            )

    # T3 — deterministic GNIS name + watershed-code-50K join.  The 50K watershed
    # code is essentially unique per waterbody, so a survey whose GNIS name *and*
    # watershed code both match a polygon is a safe join even without the
    # identifier.  Multiple hits all share the exact code => co-located basins of
    # a single waterbody (the survey covers the whole thing).
    if name and wsc and wsc not in _SENTINELS:
        cands = {
            k for k in index.name_index.get(name, set())
            if index.key_wsc.get(k, "") == wsc
        }
        if cands:
            return result(
                "T3_name_wsc",
                "GNIS name + watershed code (50K)",
                f"GNIS name (1 or 2) matches and watershed code {wsc} selects "
                f"{len(cands)} waterbody(ies)",
                cands,
            )

    # --- unresolved by identifier / name+code: hand off to the geometric tier ---
    return MatchResult(
        identifier=identifier,
        name=display_name,
        watershed_code=wsc,
        tier="AMBIGUOUS",
        method="none",
        assumption="",
        waterbody_keys=[],
        status="ambiguous",
        ambiguity_reason="identifier and name+watershed-code tiers did not settle a single waterbody — needs spatial overlap",
        pdf_url=pdf_url,
    )


def _spatial_resolve(
    results: List[MatchResult],
    gpkg_path: Path,
    min_overlap: float = _MIN_SPATIAL_OVERLAP,
) -> None:
    """Best-overlap fallback for surveys the deterministic tiers left ambiguous.

    Mutates the ``ambiguous`` :class:`MatchResult` objects in ``results`` in place:
    each survey's own polygon (from ``BATHY_POLYGON_LAYER``) is intersected with the
    nearby FWA lakes/wetlands, and the waterbody with the largest overlap area is
    adopted when it clears ``min_overlap`` (as a fraction of the survey polygon's
    area).  Rows that stay unresolved keep ``status == "ambiguous"`` with an updated
    reason explaining why the spatial step could not settle them.
    """
    ambiguous = [r for r in results if r.status == "ambiguous"]
    if not ambiguous:
        return

    ids = {r.identifier for r in ambiguous}
    try:
        bp = gpd.read_file(gpkg_path, layer=BATHY_POLYGON_LAYER)
    except Exception as exc:  # layer missing / unreadable — make it loud, not silent
        logger.warning(
            "Spatial tier skipped: cannot read layer %r from %s (%s). "
            "Fetch it first with: python -m data.fetch_data --layers bathymetry_polygons",
            BATHY_POLYGON_LAYER, gpkg_path, exc,
        )
        return
    if COL_BATHY_POLY_ID not in bp.columns:
        logger.warning(
            "Spatial tier skipped: layer %r is missing the %r column.",
            BATHY_POLYGON_LAYER, COL_BATHY_POLY_ID,
        )
        return

    bp = bp[bp[COL_BATHY_POLY_ID].isin(ids)]
    # A survey can span several map-sheet polygons — union them per identifier.
    geom_by_id = bp.dissolve(by=COL_BATHY_POLY_ID)["geometry"].to_dict()
    target_crs = bp.crs
    logger.info(
        "Spatial tier: best-overlap for %d ambiguous survey(s) (%d have polygons).",
        len(ambiguous), len(geom_by_id),
    )

    for r in ambiguous:
        geom = geom_by_id.get(r.identifier)
        if geom is None or geom.is_empty:
            r.ambiguity_reason = (
                f"no survey polygon in {BATHY_POLYGON_LAYER} — spatial match unavailable"
            )
            continue

        minx, miny, maxx, maxy = geom.bounds
        pad = _SPATIAL_BBOX_PAD_M
        bbox = (minx - pad, miny - pad, maxx + pad, maxy + pad)
        cand_frames = []
        for layer in POLYGON_LAYERS:
            chunk = gpd.read_file(
                gpkg_path, layer=layer, columns=["WATERBODY_KEY", "GNIS_NAME_1"], bbox=bbox
            )
            if not chunk.empty:
                cand_frames.append(chunk)
        if not cand_frames:
            r.ambiguity_reason = "no FWA lake/wetland near the survey footprint — spatial match failed"
            continue

        cand = pd.concat(cand_frames, ignore_index=True)
        if cand.crs != target_crs:
            cand = cand.to_crs(target_crs)
        cand = cand.assign(_ov=cand.geometry.intersection(geom).area)
        cand = cand[cand["_ov"] > 0]
        if cand.empty:
            r.ambiguity_reason = "survey polygon overlaps no FWA lake/wetland — spatial match failed"
            continue

        best = cand.loc[cand["_ov"].idxmax()]
        frac = float(best["_ov"]) / float(geom.area) if geom.area else 0.0
        if frac < min_overlap:
            r.ambiguity_reason = (
                f"best FWA overlap only {frac:.0%} (< {min_overlap:.0%} threshold) — spatial match too weak"
            )
            continue

        key = int(best["WATERBODY_KEY"])
        nm = best.get("GNIS_NAME_1")
        name_note = f" ({nm})" if isinstance(nm, str) and nm.strip() else ""
        r.status = "resolved"
        r.tier = "T2_spatial_overlap"
        r.method = "polygon best-overlap"
        r.waterbody_keys = [key]
        r.ambiguity_reason = ""
        r.assumption = (
            f"survey polygon overlaps FWA waterbody {key}{name_note} by {frac:.0%} "
            f"(largest of {len(cand)} overlapping polygon(s))"
        )


def resolve_surveys(bathy: pd.DataFrame, gpkg_path: Path) -> List[MatchResult]:
    """Resolve a bathymetry survey DataFrame to FWA waterbodies.

    Shared resolution core used by both :func:`run_matcher` (the audit CLI) and
    :func:`build_wbk_bathymetry_map` (the enrichment accessor).  Runs the
    deterministic tier cascade per row then the last-resort spatial best-overlap
    tier, returning the resolved :class:`MatchResult` list in input order.

    ``bathy`` is expected to already be reduced to one row per unique ``COL_ID``.
    """
    index = build_polygon_index(gpkg_path)
    results = [match_row(row, index) for _, row in bathy.iterrows()]
    # Last-resort spatial tier for whatever the deterministic tiers left ambiguous.
    _spatial_resolve(results, gpkg_path)
    return results


def run_matcher(bathy_csv: Path, gpkg_path: Path) -> pd.DataFrame:
    """Match every bathymetry survey row and return the breakdown table."""
    if not bathy_csv.exists():
        raise FileNotFoundError(
            f"WSA bathymetry CSV not found at {bathy_csv}.\n"
            "Source: BC Data Catalogue 'WSA Lakes — bathymetric maps' extract "
            "(columns include WATERBODY_IDENTIFIER_WSA_50K, GAZETTED_NAME, "
            "WATERSHED_CODE_WSA_50K, MAP_TITLE, PDF_FILE_URL). "
            "Place the CSV there or pass --bathy."
        )
    if not gpkg_path.exists():
        raise FileNotFoundError(f"FWA GeoPackage not found at {gpkg_path}")

    bathy = pd.read_csv(bathy_csv, dtype=str)
    missing = {COL_ID, COL_NAME, COL_WSC, COL_MAPTITLE, COL_PDF} - set(bathy.columns)
    if missing:
        raise ValueError(f"Bathymetry CSV missing required columns: {sorted(missing)}")

    before = len(bathy)
    bathy = bathy.dropna(subset=[COL_ID]).drop_duplicates(COL_ID)
    logger.info("Bathymetry surveys: %d rows -> %d unique identifiers", before, len(bathy))

    results = resolve_surveys(bathy, gpkg_path)
    df = pd.DataFrame([r.to_row() for r in results])
    # Surface the rows that still need manual spatial review: ambiguous first,
    # then resolved.  Stable sort preserves the original order within each group.
    df = df.sort_values(
        "status",
        key=lambda s: s.ne("ambiguous"),
        kind="stable",
    ).reset_index(drop=True)
    return df


def build_wbk_bathymetry_map(
    bathy_csv: Path, gpkg_path: Path
) -> Dict[str, List[Dict[str, str]]]:
    """Resolve every survey and return ``str(WATERBODY_KEY) -> [survey sheets]``.

    The enrichment accessor (consumed by ``reach_builder``).  Unlike the audit
    :func:`run_matcher` — which keeps one row per identifier and blanks the PDF
    for deterministically-resolved rows — this preserves **every** map sheet and
    always carries its downloadable filename, so a lake that owns multiple survey
    sheets links to all of them.

    Each list entry is ``{"pdf": <filename>, "title": <map title>, "name":
    <gazetted name>}``:
      * ``pdf``   — ``MAP_IMAGE_FILENAME_PDF`` lowercased, matching the object
                    served at ``bathymetry/<pdf>`` (see fetch_data.py / seed-r2.sh).
      * ``title`` — ``MAP_TITLE`` (falls back to ``GAZETTED_NAME``) for display.
      * ``name``  — ``GAZETTED_NAME`` (falls back to ``MAP_TITLE``), used by the
                    reach builder as an extra searchable name variant.

    Returns an empty dict (never raises) when the CSV/columns are unusable, so a
    build without the optional bathymetry data degrades gracefully.
    """
    if not bathy_csv.exists():
        logger.warning(
            "Bathymetry accessor: CSV not found at %s — no depth maps will be "
            "attached to reaches.", bathy_csv,
        )
        return {}
    if not gpkg_path.exists():
        logger.warning(
            "Bathymetry accessor: GeoPackage not found at %s — skipping.", gpkg_path,
        )
        return {}

    bathy = pd.read_csv(bathy_csv, dtype=str)
    missing = {COL_ID, COL_NAME, COL_MAPTITLE, COL_PDF_FILENAME} - set(bathy.columns)
    if missing:
        logger.warning(
            "Bathymetry accessor: CSV missing column(s) %s — skipping.",
            sorted(missing),
        )
        return {}

    # 1. Collect every map sheet per identifier from the FULL csv (no dedup on
    #    identifier), keyed unique on (identifier, pdf filename).
    sheets_by_id: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    seen_sheets: Set[Tuple[str, str]] = set()
    for _, row in bathy.iterrows():
        identifier = str(row.get(COL_ID) or "").strip()
        raw_pdf = row.get(COL_PDF_FILENAME)
        if not identifier or not isinstance(raw_pdf, str) or not raw_pdf.strip():
            continue
        pdf = raw_pdf.strip().lower()
        key = (identifier, pdf)
        if key in seen_sheets:
            continue
        seen_sheets.add(key)
        gazetted = str(row.get(COL_NAME) or "").strip()
        map_title = str(row.get(COL_MAPTITLE) or "").strip()
        sheets_by_id[identifier].append(
            {
                "pdf": pdf,
                "title": map_title or gazetted,
                "name": gazetted or map_title,
            }
        )

    # 2. Resolve each unique identifier to its FWA waterbody key(s).
    unique = bathy.dropna(subset=[COL_ID]).drop_duplicates(COL_ID)
    results = resolve_surveys(unique, gpkg_path)

    # 3. Fan the identifier's sheets out to every waterbody key it resolved to,
    #    deduping sheets per key on the PDF filename (two identifiers can resolve
    #    to the same waterbody).
    wbk_map: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    seen_by_wbk: Dict[str, Set[str]] = defaultdict(set)
    for r in results:
        if r.status != "resolved":
            continue
        sheets = sheets_by_id.get(r.identifier)
        if not sheets:
            continue
        for wbk in r.waterbody_keys:
            wbk_str = str(wbk)
            for sheet in sheets:
                if sheet["pdf"] in seen_by_wbk[wbk_str]:
                    continue
                seen_by_wbk[wbk_str].add(sheet["pdf"])
                wbk_map[wbk_str].append(sheet)

    logger.info(
        "Bathymetry accessor: %d waterbodies matched to %d survey sheet(s).",
        len(wbk_map),
        sum(len(v) for v in wbk_map.values()),
    )
    return dict(wbk_map)


def _print_breakdown(df: pd.DataFrame) -> None:
    total = len(df)
    resolved = df[df["status"] == "resolved"]
    ambiguous = df[df["status"] == "ambiguous"]
    unique = resolved[resolved["n_waterbodies"] == 1]
    clusters = resolved[resolved["n_waterbodies"] > 1]

    print("\n=== Bathymetry → FWA matching breakdown "
          f"({' + '.join(POLYGON_LAYERS)}) ===")
    print(f"  surveys                 : {total}")
    print(f"  resolved                : {len(resolved)}  "
          f"(unique={len(unique)}, chain/cluster groups={len(clusters)})")
    print(f"  ambiguous               : {len(ambiguous)}")
    print("\n  by tier:")
    for tier, n in df[df["status"] == "resolved"]["match_tier"].value_counts().sort_index().items():
        print(f"    {tier:<26} {n}")

    if len(ambiguous):
        print("\n=== Unresolved surveys (deterministic + spatial tiers failed) — with PDF ===")
        for _, r in ambiguous.iterrows():
            print(f"  {r['bathy_identifier']:<11} | {str(r['bathy_name'])[:26]:<26} | {r['ambiguity_reason']}")
            print(f"              PDF: {r['pdf_url']}")


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = ProjectConfig()

    parser = argparse.ArgumentParser(description="Match WSA lake bathymetry surveys to FWA waterbodies.")
    parser.add_argument(
        "--bathy",
        type=Path,
        default=cfg.project_root / "data" / "wsa_bathymetry_maps.csv",
        help="WSA bathymetry catalogue CSV (default: data/wsa_bathymetry_maps.csv).",
    )
    parser.add_argument(
        "--gpkg",
        type=Path,
        default=cfg.get_path("data", "gpkg_path", default="data/bc_fisheries_data.gpkg"),
        help="FWA GeoPackage (default from config data.gpkg_path).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=cfg.get_path("output", "pipeline", "matching", default="output/pipeline/matching")
        / "bathymetry_matches.csv",
        help="Output breakdown CSV.",
    )
    args = parser.parse_args(argv)

    df = run_matcher(args.bathy, args.gpkg)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    logger.info("Wrote %d rows -> %s", len(df), args.out)

    _print_breakdown(df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
