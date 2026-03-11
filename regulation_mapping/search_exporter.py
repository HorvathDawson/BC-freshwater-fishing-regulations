"""
SearchIndexBuilder — builds ``waterbody_data.json`` for the frontend.

Reads canonical features from ``CanonicalDataStore`` and produces:
  - ``waterbodies``: full searchable entries (short keys for Fuse.js)
  - ``reg_sets``: deduplicated regulation-ID strings
  - ``compact``: ``{frontend_group_id: reg_set_index}`` for unnamed features
  - ``identity_meta``: per-identity data (synopsis only, deduplicated)
  - ``regulations``: slimmed regulation rule dicts

Uses ``orjson`` for fast serialisation.

No geometry IO happens here — this module only builds the JSON index.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fwa_pipeline.metadata_gazetteer import FeatureType

from .canonical_store import CanonicalDataStore
from .geometry_utils import extract_geoms, geoms_to_wgs84_bbox
from .regulation_resolvers import parse_base_regulation_id
from .logger_config import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# orjson — required dependency
# ---------------------------------------------------------------------------
try:
    import orjson
except ImportError as exc:
    raise ImportError(
        "orjson is required for SearchIndexBuilder.  "
        "Install it with:  pip install orjson"
    ) from exc


# ---------------------------------------------------------------------------
# SearchIndexBuilder
# ---------------------------------------------------------------------------


class SearchIndexBuilder:
    """Builds the ``waterbody_data.json`` search index from a CanonicalDataStore.

    No tile/GPKG logic leaks into this class.  The only geometry work is
    bounding-box computation (delegated to ``geometry_utils``).
    """

    def __init__(self, store: CanonicalDataStore) -> None:
        self.store = store

    # ------------------------------------------------------------------
    # Name-variant merging (stateless helper)
    # ------------------------------------------------------------------

    @staticmethod
    def _merge_name_variants(
        target: Dict[str, bool], variants: List[Dict[str, Any]]
    ) -> None:
        """Merge name variant dicts into *target* (name → from_tributary).

        ``False`` (direct name match) always wins over ``True`` (tributary).
        """
        for nv in variants:
            name = nv["name"]
            is_trib = nv["from_tributary"]
            if name in target:
                if not is_trib:
                    target[name] = False
            else:
                target[name] = is_trib

    # ------------------------------------------------------------------
    # Core build
    # ------------------------------------------------------------------

    def _build_waterbodies_list(self) -> Dict[str, Any]:
        """Build the compact waterbody data export.

        Groups canonical features by physical identity; features with a
        GNIS name, display name, or waterbody-specific regulations become
        full entries in ``waterbodies``.  Unnamed zone-only features get
        a minimal ``compact`` entry (``frontend_group_id → reg_set_index``)
        so the frontend can resolve their regulations at click-time from
        the JSON alone.

        Returns a dict with:
            - ``waterbodies``: list of full search entries (short keys)
            - ``reg_sets``: deduplicated regulation-ID strings (list)
            - ``compact``: ``{frontend_group_id: reg_set_index}``
        """
        canonical_features = self.store.get_canonical_features()

        # --- Regulation-set dedup table ---
        _reg_set_list: List[str] = []
        _reg_set_index: Dict[str, int] = {}

        def _get_ri(reg_ids_str: str) -> int:
            if reg_ids_str in _reg_set_index:
                return _reg_set_index[reg_ids_str]
            idx = len(_reg_set_list)
            _reg_set_list.append(reg_ids_str)
            _reg_set_index[reg_ids_str] = idx
            return idx

        # --- Phase 1: Group canonical features by search key ---
        physical_groups: Dict[tuple, dict] = defaultdict(
            lambda: {
                "features": [],
                "zone_to_name": {},
                "mgmt_units": set(),
                "wb_keys": set(),
                "watershed_codes": set(),
                "name_variants": {},
            }
        )

        for feat in canonical_features:
            ftype = feat["feature_type"]
            display_name = feat["display_name"]

            if not feat["regulation_ids"]:
                continue

            if ftype == FeatureType.STREAM.value:
                wsc = feat["fwa_watershed_code"] or ""
                grouping_id = (
                    wsc if wsc and not wsc.startswith("999-") else feat["group_id"]
                )
            else:
                grouping_id = str(feat["waterbody_key"] or "") or feat["group_id"]

            if not grouping_id:
                grouping_id = feat["group_id"]

            search_key = (grouping_id, display_name, ftype)
            sg = physical_groups[search_key]
            sg["features"].append(feat)

            if feat["waterbody_key"]:
                sg["wb_keys"].add(str(feat["waterbody_key"]))
            if feat.get("fwa_watershed_code"):
                sg["watershed_codes"].add(feat["fwa_watershed_code"])

            zones = feat["zones"].split(",") if feat["zones"] else []
            region_names = feat["region_name"].split(",") if feat["region_name"] else []
            for z, n in zip(zones, region_names):
                if z:
                    sg["zone_to_name"][z] = n
            sg["mgmt_units"].update(
                mu
                for mu in (feat["mgmt_units"].split(",") if feat["mgmt_units"] else [])
                if mu
            )

            nvs = json.loads(feat["name_variants"]) if feat["name_variants"] else []
            self._merge_name_variants(sg["name_variants"], nvs)

        # --- Phase 2: Build search items ---
        search_items: list = []
        compact_entries: Dict[str, int] = {}
        skipped_unnamed = 0

        for (grouping_id, display_name_key, ftype_val), data in physical_groups.items():
            feats = data["features"]
            if not feats:
                continue

            all_geoms: list = []
            for f in feats:
                geom = f["geometry"]
                if geom:
                    all_geoms.extend(extract_geoms(geom))
            if not all_geoms:
                continue

            # Consolidate features by regulation set into segments
            consolidated: Dict[str, dict] = {}
            for f in feats:
                reg_key = f["regulation_ids"]
                if reg_key in consolidated:
                    seg = consolidated[reg_key]
                    seg["length_m"] += f["length_m"]
                    seg["feature_ids_set"].update(
                        fid for fid in f["feature_ids"].split(",") if fid
                    )
                    seg_geom = f["geometry"]
                    if seg_geom:
                        seg["geoms"].extend(extract_geoms(seg_geom))
                    seg_nvs = (
                        json.loads(f["name_variants"]) if f["name_variants"] else []
                    )
                    self._merge_name_variants(seg["name_variants"], seg_nvs)
                    seg["group_ids"].append(f["group_id"])
                    seg["frontend_group_ids"].add(f["frontend_group_id"])
                    if f.get("waterbody_key"):
                        seg["waterbody_keys"].add(str(f["waterbody_key"]))
                else:
                    seg_geom = f["geometry"]
                    seg_geoms = extract_geoms(seg_geom) if seg_geom else []
                    seg_nvs = (
                        json.loads(f["name_variants"]) if f["name_variants"] else []
                    )
                    nv_dict = {nv["name"]: nv["from_tributary"] for nv in seg_nvs}
                    consolidated[reg_key] = {
                        "regulation_ids": reg_key,
                        "length_m": f["length_m"],
                        "feature_ids_set": set(
                            fid for fid in f["feature_ids"].split(",") if fid
                        ),
                        "geoms": list(seg_geoms),
                        "name_variants": nv_dict,
                        "display_name": f["display_name"],
                        "group_ids": [f["group_id"]],
                        "frontend_group_ids": {f["frontend_group_id"]},
                        "waterbody_group": f.get("waterbody_group") or "",
                        "waterbody_keys": (
                            {str(f["waterbody_key"])} if f.get("waterbody_key") else set()
                        ),
                    }

            # For streams, exclude segments that are entirely under a lake
            # (their geometry is excluded from PMTiles so clicking them zooms
            # to empty space).  The under-lake streams are available in a
            # separate tile layer for optional dotted-line rendering.
            if ftype_val == FeatureType.STREAM.value:
                lake_wbkeys = self.store.get_lake_manmade_wbkeys()
                visible_segments = [
                    seg for seg in consolidated.values()
                    if not (
                        seg["waterbody_keys"]
                        and seg["waterbody_keys"] <= lake_wbkeys
                    )
                ]
            else:
                visible_segments = list(consolidated.values())

            sorted_segments = sorted(
                visible_segments, key=lambda s: s["length_m"], reverse=True
            )

            all_reg_ids: set = set()
            all_group_ids: list = []
            total_feature_ids: set = set()
            for seg in sorted_segments:
                all_reg_ids.update(
                    seg["regulation_ids"].split(",") if seg["regulation_ids"] else []
                )
                all_group_ids.extend(seg["group_ids"])
                total_feature_ids.update(seg["feature_ids_set"])
            reg_ids = sorted(all_reg_ids)

            # Classify: full entry or compact?
            has_name = bool(display_name_key)

            if not has_name:
                skipped_unnamed += 1
                reg_ids_str = ",".join(reg_ids)
                ri = _get_ri(reg_ids_str)
                for seg in sorted_segments:
                    for fgid in seg["frontend_group_ids"]:
                        if fgid:
                            compact_entries[fgid] = ri
                continue

            # Full entry
            min_zoom = min(f["tippecanoe:minzoom"] for f in feats)
            wgs84_bounds = geoms_to_wgs84_bbox(all_geoms)

            name_variants = [
                {"name": name, "ft": is_trib}
                for name, is_trib in sorted(data["name_variants"].items())
            ]

            total_length_m = sum(seg["length_m"] for seg in sorted_segments)
            if ftype_val == FeatureType.STREAM.value:
                length_km = round(total_length_m / 1000.0, 2)
            elif ftype_val == FeatureType.UNGAZETTED.value:
                length_km = 0.0
            else:
                length_km = round(total_length_m / 1_000_000.0, 2)

            if ftype_val == FeatureType.STREAM.value:
                stream_key = next(iter(data["watershed_codes"]), "") or grouping_id
            else:
                stream_key = next(iter(sorted(data["wb_keys"])), "") or grouping_id

            reg_segments: list = []
            for seg in sorted_segments:
                seg_reg_ids_str = ",".join(
                    sorted(
                        seg["regulation_ids"].split(",")
                        if seg["regulation_ids"]
                        else []
                    )
                )
                frontend_group_id = next(iter(seg["frontend_group_ids"]))
                seg_bbox_wgs84 = (
                    list(geoms_to_wgs84_bbox(seg["geoms"])) if seg["geoms"] else None
                )
                seg_name_variants = [
                    {"name": name, "ft": is_trib}
                    for name, is_trib in sorted(seg["name_variants"].items())
                ]
                reg_segments.append(
                    {
                        "fgid": frontend_group_id,
                        "gid": seg["group_ids"][0],
                        "ri": _get_ri(seg_reg_ids_str),
                        "dn": seg["display_name"],
                        "nv": seg_name_variants,
                        "wbg": seg.get("waterbody_group") or "",
                        "lkm": (
                            round(seg["length_m"] / 1000.0, 2)
                            if ftype_val == FeatureType.STREAM.value
                            else round(seg["length_m"] / 1_000_000.0, 2)
                        ),
                        "bbox": seg_bbox_wgs84,
                    }
                )

            all_frontend_group_ids = [
                seg["fgid"] for seg in reg_segments if seg.get("fgid")
            ]

            # Skip streams where all segments were under lakes (nothing to show)
            if not reg_segments:
                skipped_unnamed += 1
                continue

            search_items.append(
                {
                    "id": f"{display_name_key}|{stream_key}|{ftype_val}",
                    "gn": display_name_key,
                    "fgids": all_frontend_group_ids,
                    "dn": display_name_key,
                    "nv": name_variants,
                    "type": ftype_val,
                    "z": sorted(data["zone_to_name"].keys()),
                    "mu": sorted(data["mgmt_units"]),
                    "rn": [
                        data["zone_to_name"][z]
                        for z in sorted(data["zone_to_name"].keys())
                    ],
                    "ri": _get_ri(",".join(reg_ids)),
                    "tlkm": length_km,
                    "bbox": list(wgs84_bounds),
                    "mz": min_zoom,
                    "props": {
                        "gid": all_group_ids[0] if all_group_ids else "",
                        "wk": ",".join(sorted(data["wb_keys"])),
                        "fwc": stream_key,
                        "rc": len(reg_ids),
                        "wbg": next(
                            (
                                s.get("waterbody_group")
                                for s in sorted_segments
                                if s.get("waterbody_group")
                            ),
                            "",
                        ),
                    },
                    "rs": reg_segments,
                }
            )

        logger.info(
            f"Waterbody data: {len(search_items)} full entries, "
            f"{skipped_unnamed} unnamed zone-only ({len(compact_entries)} compact fgids), "
            f"{len(_reg_set_list)} unique reg sets"
        )

        return {
            "waterbodies": search_items,
            "reg_sets": _reg_set_list,
            "compact": compact_entries,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Identity extraction — synopsis only
    # ------------------------------------------------------------------

    # Fields that belong on the identity (deduplicated per synopsis entry).
    _IDENTITY_FIELDS = frozenset(
        {
            "waterbody_name",
            "region",
            "management_units",
            "source_image",
            "exclusions",
        }
    )

    # Fields stripped entirely — unused by the frontend.
    _DEAD_FIELDS = frozenset(
        {
            "lookup_name",
            "is_direct_match",
            "includes_tributaries",
        }
    )

    @staticmethod
    def _build_identity_meta(
        regulations: Dict[str, Dict[str, Any]],
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Extract per-identity metadata from flat regulation dicts.

        Synopsis regulations share identity-level fields (waterbody_name,
        region, management_units, source_image, exclusions) across their
        ``_ruleN`` siblings.  This method extracts those into a separate
        ``identity_meta`` dict keyed by base regulation ID.

        Zone and provincial regulations are left flat — they are singletons
        with no deduplication benefit.

        Returns ``(identity_meta, slimmed_regulations)``.

        Identity meta short-key mapping::

            wn=waterbody_name  rg=region  mu=management_units
            img=source_image   ex=exclusions
        """
        identity_meta: Dict[str, Dict[str, Any]] = {}
        slimmed: Dict[str, Dict[str, Any]] = {}

        for reg_id, detail in regulations.items():
            source = detail.get("source")

            if source != "synopsis":
                # Zone / provincial: strip dead fields only, keep everything else flat.
                slim = {
                    k: v
                    for k, v in detail.items()
                    if k not in SearchIndexBuilder._DEAD_FIELDS
                }
                slimmed[reg_id] = slim
                continue

            # --- Synopsis regulation ---
            base_id = parse_base_regulation_id(reg_id)

            # Build / update identity_meta entry (merge across sibling rules).
            if base_id not in identity_meta:
                entry: Dict[str, Any] = {"wn": detail.get("waterbody_name")}
                rg = detail.get("region")
                if rg:
                    entry["rg"] = rg
                mu = detail.get("management_units")
                if mu:
                    entry["mu"] = mu
                img = detail.get("source_image")
                if img:
                    entry["img"] = img
                ex = detail.get("exclusions")
                if ex:
                    entry["ex"] = ex
                identity_meta[base_id] = entry
            else:
                # Merge: pick up data from sibling rules if the first was missing it.
                existing = identity_meta[base_id]
                if "rg" not in existing:
                    rg = detail.get("region")
                    if rg:
                        existing["rg"] = rg
                if "mu" not in existing:
                    mu = detail.get("management_units")
                    if mu:
                        existing["mu"] = mu
                if "ex" not in existing:
                    ex = detail.get("exclusions")
                    if ex:
                        existing["ex"] = ex
                if "img" not in existing:
                    img = detail.get("source_image")
                    if img:
                        existing["img"] = img

            # Slim the regulation: keep only rule-specific fields + iid + source.
            slim = {
                k: v
                for k, v in detail.items()
                if k not in SearchIndexBuilder._IDENTITY_FIELDS
                and k not in SearchIndexBuilder._DEAD_FIELDS
            }
            if not base_id:
                raise ValueError(
                    f"parse_base_regulation_id returned empty for {reg_id!r}"
                )
            slim["iid"] = base_id
            slimmed[reg_id] = slim

        return identity_meta, slimmed

    def export_waterbody_data(self, output_path: Path) -> Path:
        """Export unified ``waterbody_data.json``.

        This is the **single source of truth** for the frontend:

        - ``reg_sets``: Deduplicated regulation-ID strings (list)
        - ``compact``: ``{frontend_group_id: reg_set_index}``
        - ``waterbodies``: Full search entries (short keys)
        - ``identity_meta``: Per-identity synopsis data (short keys)
        - ``regulations``: Slimmed regulation rule dicts

        Short key mapping (backend → frontend):
            gn=gnis_name  dn=display_name  fgids=frontend_group_ids
            nv=name_variants  ft=from_tributary  z=zones  mu=mgmt_units
            rn=region_name  ri=reg_set_index  tlkm=total_length_km
            mz=min_zoom  rs=regulation_segments  fgid=frontend_group_id
            gid=group_id  lkm=length_km  wk=waterbody_key
            fwc=fwa_watershed_code  rc=regulation_count

        Identity meta short keys:
            wn=waterbody_name  rg=region  mu=management_units
            img=source_image   ex=exclusions
        """
        build_result = self._build_waterbodies_list()
        waterbodies = build_result["waterbodies"]
        reg_sets = build_result["reg_sets"]
        compact = build_result["compact"]
        raw_regulations = dict(self.store.pipeline_result.regulation_details)

        identity_meta, regulations = self._build_identity_meta(raw_regulations)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload: Dict[str, Any] = {
            "reg_sets": reg_sets,
            "compact": compact,
            "waterbodies": waterbodies,
            "regulations": regulations,
        }
        if identity_meta:
            payload["identity_meta"] = identity_meta
        output_path.write_bytes(orjson.dumps(payload))

        logger.info(
            f"Created {output_path} "
            f"({output_path.stat().st_size / 1048576:.1f} MB, "
            f"{len(waterbodies)} waterbodies, "
            f"{len(reg_sets)} reg_sets, "
            f"{len(compact)} compact, "
            f"{len(regulations)} regulations, "
            f"{len(identity_meta)} identity_meta)"
        )
        return output_path
