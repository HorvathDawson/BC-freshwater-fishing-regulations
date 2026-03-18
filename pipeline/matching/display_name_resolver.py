"""Shared display-name resolution for tiles and reach building.

Single source of truth for the display name priority chain:

    1. Manual override  — feature_display_names.json (BLK / WBK)
    2. GNIS name        — from atlas StreamRecord / PolygonRecord
    3. Regulation name  — from per-fid enrichment data (caller provides)
                          OR prebuilt BLK/WBK/fid lookup from match_table.json

The prebuilt regulation-name lookup is built from match_table.json and
overrides.json: only non-skip entries with direct feature identifiers
(blue_line_keys, waterbody_keys, linear_feature_ids) contribute.
Admin-only entries are excluded — their regulation names describe
administrative zones, not waterbodies.

Both ``tile_exporter`` and ``reach_builder`` share this resolver so that
tile labels and the regulation panel show identical names.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def _is_direct_entry(raw: dict) -> bool:
    """True if a serialised entry has any direct feature identifiers."""
    return bool(
        raw.get("gnis_ids")
        or raw.get("waterbody_keys")
        or raw.get("fwa_watershed_codes")
        or raw.get("blue_line_keys")
        or raw.get("linear_feature_ids")
        or raw.get("waterbody_poly_ids")
        or raw.get("ungazetted_waterbody_id")
    )


class DisplayNameResolver:
    """Resolves display names for atlas features.

    Instantiate once per pipeline run, then call ``resolve_stream`` or
    ``resolve_polygon`` for every feature that needs a display name.
    """

    def __init__(
        self,
        feature_dn_path: Path,
        match_table_path: Optional[Path] = None,
        overrides_path: Optional[Path] = None,
    ) -> None:
        self._blk_dn: Dict[str, str] = {}
        self._wbk_dn: Dict[str, str] = {}
        self._blk_reg_name: Dict[str, str] = {}
        self._wbk_reg_name: Dict[str, str] = {}
        self._fid_reg_name: Dict[str, str] = {}

        self._load_feature_display_names(feature_dn_path)
        self._load_reg_name_fallbacks(match_table_path, overrides_path)

    # ── Public API ─────────────────────────────────────────────────

    @property
    def blk_overrides(self) -> Dict[str, str]:
        """BLK → display name from feature_display_names.json."""
        return self._blk_dn

    @property
    def wbk_overrides(self) -> Dict[str, str]:
        """WBK → display name from feature_display_names.json."""
        return self._wbk_dn

    def resolve_stream(
        self,
        blk: str,
        gnis_name: str,
        direct_reg_name: str = "",
        fid: str = "",
    ) -> str:
        """Resolve display name for a stream feature.

        Args:
            blk: Blue line key of the stream segment.
            gnis_name: GNIS name from atlas ("" if unnamed).
            direct_reg_name: Per-fid regulation name from enrichment
                (caller computes — only non-tributary, non-admin regs).
            fid: Linear feature ID for fid-level reg name lookup.
        """
        return (
            self._blk_dn.get(blk)
            or gnis_name
            or direct_reg_name
            or self._blk_reg_name.get(blk, "")
            or (self._fid_reg_name.get(fid, "") if fid else "")
        )

    def resolve_polygon(
        self,
        wbk: str,
        gnis_name: str,
        direct_reg_name: str = "",
    ) -> str:
        """Resolve display name for a polygon (lake / wetland / manmade)."""
        return (
            self._wbk_dn.get(wbk)
            or gnis_name
            or direct_reg_name
            or self._wbk_reg_name.get(wbk, "")
        )

    # ── Loaders ────────────────────────────────────────────────────

    def _load_feature_display_names(self, path: Path) -> None:
        if not path.exists():
            logger.warning("feature_display_names.json not found: %s", path)
            return
        with open(path, encoding="utf-8") as f:
            entries = json.load(f)
        for entry in entries:
            dn = entry["display_name"]
            for blk in entry.get("blue_line_keys", []):
                self._blk_dn[blk] = dn
            for wbk in entry.get("waterbody_keys", []):
                self._wbk_dn[wbk] = dn
        logger.info(
            "  %d BLK + %d WBK display name overrides loaded",
            len(self._blk_dn),
            len(self._wbk_dn),
        )

    def _load_reg_name_fallbacks(
        self,
        match_table_path: Optional[Path],
        overrides_path: Optional[Path],
    ) -> None:
        """Build BLK / WBK / fid → regulation name from direct-match entries."""
        sources: List[list] = []

        if match_table_path and match_table_path.exists():
            with open(match_table_path, encoding="utf-8") as f:
                sources.append(json.load(f))

        if overrides_path and overrides_path.exists():
            with open(overrides_path, encoding="utf-8") as f:
                sources.append(json.load(f))

        if not sources:
            return

        count = 0
        for entries in sources:
            for raw in entries:
                if raw.get("skip", False):
                    continue
                name = raw.get("criteria", {}).get("name_verbatim", "")
                if not name:
                    continue
                has_direct = _is_direct_entry(raw)
                has_admin_only = bool(raw.get("admin_targets")) and not has_direct
                if has_admin_only:
                    continue

                for blk in raw.get("blue_line_keys", []):
                    self._blk_reg_name.setdefault(blk, name)
                    count += 1
                for wbk in raw.get("waterbody_keys", []):
                    self._wbk_reg_name.setdefault(wbk, name)
                    count += 1
                for fid in raw.get("linear_feature_ids", []):
                    self._fid_reg_name.setdefault(fid, name)
                    count += 1

        if count:
            logger.info(
                "  %d reg-name fallbacks (%d BLK, %d WBK, %d fid)",
                count,
                len(self._blk_reg_name),
                len(self._wbk_reg_name),
                len(self._fid_reg_name),
            )
