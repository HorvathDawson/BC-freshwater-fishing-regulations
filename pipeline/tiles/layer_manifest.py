"""Layer manifest — single source of truth for frontend layer config.

This module holds ``LAYER_MANIFEST`` and nothing else heavy, so the manifest
JSON can be regenerated in milliseconds without importing the atlas / shapely /
geopandas stack that ``tile_exporter`` pulls in. ``TileExporter`` imports
``LAYER_MANIFEST`` from here; the normal pipeline run still emits the JSON as a
side effect of tile export.

Fast standalone regeneration (for iterating on the dev server without a full
pipeline run)::

    python -m pipeline.tiles.layer_manifest output/pipeline/deploy/layer_manifest.json

With no path argument it writes to ``output/pipeline/deploy/layer_manifest.json``
relative to the repo root.

Per-entry fields:
  visible    — default map visibility (the menu checkbox's initial state).
  label/type — menu label + geometry type.
  toggleable — render a user checkbox in the layer menu for this layer
               (absent = false). This manifest is the SOURCE OF TRUTH for
               which layers are user-toggleable, their label, and default;
               the frontend only maps each key → its MapLibre style layers.
  style      — appearance knobs the frontend applies on top of its base
               style-layer definitions (see LAYER_STYLE_MAP / applyManifestStyle
               in the webapp). Only FLAT values live here — colors, static
               opacities, line widths, dash arrays, and the border fade-in
               zoom. Data-driven appearance (match/interpolate expressions —
               e.g. BC-parks colour keyed on admin_type, area-based lake
               outline width) stays in code and is deliberately omitted so
               the manifest never has to encode an expression. Any key the
               frontend doesn't recognise is ignored, and any omitted key
               leaves the code default untouched. Sub-keys:
                 fill_color / line_color     — hex; omit for data-driven colour
                 fill_opacity / line_opacity — static 0..1; omit if zoom-based
                 line_width                  — static px; omit if zoom-based
                 line_dash                   — [dash, gap] array
                 border_minzoom              — zoom the polygon border fades in
  min_tile_zoom — OPTIONAL floor applied at tile-generation time
               (tippecanoe minzoom = max(feature minzoom, this)). Only set it
               where the WHOLE layer (fill + line + label) is hidden below
               this zoom in the frontend, so dropping the features from
               lower-zoom tiles is lossless — it shrinks the tiles instead of
               shipping geometry the map never draws. Consumed by the layer
               writers; a change here only takes effect on the next tile run.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

# Admin layers default to not visible — the frontend enables them when
# instructed (e.g. user toggles a layer, or a regulation references an admin_id
# and the UI highlights it).
LAYER_MANIFEST: Dict[str, Dict[str, Any]] = {
    "streams": {
        "visible": True,
        "toggleable": True,
        "label": "Streams",
        "type": "line",
        "group": "water",
        "description": (
            "Rivers, creeks, and stream reaches from the BC Freshwater Atlas "
            "stream network, styled by stream order."
        ),
        "sources": [
            {
                "name": "BC Freshwater Atlas — Stream Network",
                "url": "https://catalogue.data.gov.bc.ca/dataset/freshwater-atlas-stream-network",
            },
        ],
        "style": {"line_color": "#4A90E2", "line_opacity": 0.8},
    },
    "under_lake_streams": {
        "visible": True,
        "label": "Under-Lake Streams",
        "type": "line",
        "group": "water",
        "description": (
            "Flow lines representing stream connectivity through lakes, drawn "
            "as subtle dashed construction lines beneath lake fills."
        ),
        "sources": [
            {
                "name": "BC Freshwater Atlas — Stream Network",
                "url": "https://catalogue.data.gov.bc.ca/dataset/freshwater-atlas-stream-network",
            },
        ],
        "style": {
            "line_color": "#3A7BD5",
            "line_opacity": 0.35,
            "line_dash": [4, 3],
        },
        # Frontend layer + label both start at z10 — nothing draws below it.
        "min_tile_zoom": 10,
    },
    "lakes": {
        "visible": True,
        "toggleable": True,
        "label": "Lakes",
        "type": "polygon",
        "group": "water",
        "description": (
            "Freshwater lakes and their shorelines from the BC Freshwater Atlas."
        ),
        "sources": [
            {
                "name": "BC Freshwater Atlas — Lakes",
                "url": "https://catalogue.data.gov.bc.ca/dataset/freshwater-atlas-lakes",
            },
        ],
        # line_color intentionally the streams blue (dark outline).
        "style": {
            "fill_color": "#64B5F6",
            "fill_opacity": 0.4,
            "line_color": "#4A90E2",
            "line_opacity": 0.8,
        },
    },
    "wetlands": {
        "visible": True,
        "toggleable": True,
        "label": "Wetlands",
        "type": "polygon",
        "group": "water",
        "description": (
            "Marshes, swamps, bogs, and other wetlands from the BC Freshwater "
            "Atlas."
        ),
        "sources": [
            {
                "name": "BC Freshwater Atlas — Wetlands",
                "url": "https://catalogue.data.gov.bc.ca/dataset/freshwater-atlas-wetlands",
            },
        ],
        # fill_opacity is a zoom ramp in code → omitted here on purpose.
        "style": {
            "fill_color": "#81C784",
            "line_color": "#81C784",
            "line_opacity": 0.6,
        },
    },
    "manmade": {
        "visible": True,
        "toggleable": True,
        "label": "Manmade Water",
        "type": "polygon",
        "group": "water",
        "description": (
            "Human-made waterbodies — reservoirs, dugouts, and canals — from "
            "the BC Freshwater Atlas."
        ),
        "sources": [
            {
                "name": "BC Freshwater Atlas — Manmade Waterbodies",
                "url": "https://catalogue.data.gov.bc.ca/dataset/freshwater-atlas-manmade-waterbodies",
            },
        ],
        "style": {
            "fill_color": "#9575CD",
            "fill_opacity": 0.35,
            "line_color": "#9575CD",
            "line_opacity": 0.7,
            "line_dash": [3, 2],
        },
    },
    "tidal_boundary": {
        "visible": True,
        "label": "Tidal Boundary",
        "type": "polygon",
        "group": "boundaries",
        "description": (
            "Approximate boundary between tidal (saltwater) and non-tidal "
            "(freshwater) fishing jurisdictions. Tidal waters fall under "
            "federal DFO regulation, not the BC freshwater synopsis."
        ),
        "sources": [
            # Derived from the DFO Pacific Fishery Management Area subareas
            # layer (dfo_bc_pfma_subareas_chs_v3) — see data/build_tidal_boundary.py.
            {
                "name": "Fisheries and Oceans Canada — Pacific Fishery Management Area Subareas",
                "url": "https://open.canada.ca/data/en/dataset?q=Pacific+Fishery+Management+Area+subareas",
            },
        ],
        # fill/line opacity are zoom ramps in code → omitted.
        "style": {
            "fill_color": "#94A3B8",
            "line_color": "#6B7280",
            "line_dash": [4, 3],
        },
    },
    "regions": {
        "visible": True,
        "label": "Region Boundaries",
        "type": "line",
        "group": "boundaries",
        "description": (
            "The eight BC freshwater fishing regions, aggregated from the "
            "Wildlife Management Unit boundaries."
        ),
        "sources": [
            {
                "name": "DataBC — Wildlife Management Units",
                "url": "https://catalogue.data.gov.bc.ca/dataset/wildlife-management-units",
            },
        ],
        # line_color is data-driven (['get','stroke_color']) → omitted.
        "style": {"line_opacity": 0.8},
    },
    "regions_fill": {
        "visible": True,
        "label": "Regions",
        "type": "polygon",
        "group": "boundaries",
        "description": (
            "Filled version of the eight BC freshwater fishing regions "
            "(aggregated from Management Units), used for the low-zoom region "
            "number labels."
        ),
        "sources": [
            {
                "name": "DataBC — Wildlife Management Units",
                "url": "https://catalogue.data.gov.bc.ca/dataset/wildlife-management-units",
            },
        ],
    },
    "wmu": {
        "visible": True,
        "label": "Management Units",
        "type": "polygon",
        "group": "boundaries",
        "description": (
            "Wildlife Management Units (MUs) — the sub-region grid many "
            "regulations are keyed to."
        ),
        "sources": [
            {
                "name": "DataBC — Wildlife Management Units",
                "url": "https://catalogue.data.gov.bc.ca/dataset/wildlife-management-units",
            },
        ],
    },
    "wmu_boundary": {
        "visible": True,
        "label": "Management Unit Boundaries",
        "type": "line",
        "group": "boundaries",
        "description": (
            "Boundary lines of the Wildlife Management Units, with MU numbers "
            "labelled along them."
        ),
        "sources": [
            {
                "name": "DataBC — Wildlife Management Units",
                "url": "https://catalogue.data.gov.bc.ca/dataset/wildlife-management-units",
            },
        ],
        "style": {"line_color": "#555555", "line_dash": [2, 3]},
        # Boundary line + labels both start at z7.
        "min_tile_zoom": 7,
    },
    "parks_nat": {
        "visible": True,
        "label": "National Parks",
        "type": "polygon",
        "group": "boundaries",
        "description": (
            "National Parks within BC. Fishing in national parks is federally "
            "regulated and generally closed unless a park-specific permit "
            "applies — shown here as a no-fishing (crimson) overlay."
        ),
        "sources": [
            {
                "name": "DataBC — National Parks of Canada within BC",
                "url": "https://catalogue.data.gov.bc.ca/dataset/national-parks-of-canada-within-british-columbia",
            },
        ],
        "style": {
            "fill_color": "#C22E2E",
            "fill_opacity": 0.10,
            "line_color": "#C22E2E",
            "line_width": 3.0,
            "line_opacity": 0.35,
            "border_minzoom": 11,
        },
    },
    "eco_reserves": {
        "visible": True,
        "label": "Parks & Eco Reserves",
        "type": "polygon",
        "group": "boundaries",
        "description": (
            "BC provincial parks, protected areas, recreation areas, and "
            "ecological reserves. Ecological reserves are typically closed to "
            "fishing; other categories vary by waterbody."
        ),
        "sources": [
            {
                "name": "DataBC — BC Parks, Ecological Reserves, and Protected Areas",
                "url": "https://catalogue.data.gov.bc.ca/dataset/bc-parks-ecological-reserves-and-protected-areas",
            },
        ],
        # Fill/line colour is data-driven (match on admin_type) → omitted;
        # only the shared border fade-in zoom is manifest-driven.
        "style": {"border_minzoom": 11},
    },
    "wma": {
        "visible": True,
        "label": "Wildlife Management Areas",
        "type": "polygon",
        "group": "boundaries",
        "description": (
            "Wildlife Management Areas (WMAs) — provincially designated areas "
            "managed for wildlife, some carrying specific fishing regulations."
        ),
        "sources": [
            {
                "name": "DataBC — Wildlife Management Areas",
                "url": "https://catalogue.data.gov.bc.ca/dataset/wildlife-management-areas",
            },
        ],
        "style": {
            "fill_color": "#7B2D8B",
            "fill_opacity": 0.12,
            "line_color": "#7B2D8B",
            "line_width": 1.5,
            "line_opacity": 0.35,
            "border_minzoom": 11,
        },
    },
    "historic_sites": {
        "visible": True,
        "label": "Historic Sites",
        "type": "polygon",
        "group": "boundaries",
        "description": (
            "Protected historic and heritage sites. Included for situational "
            "context near waterbodies; access may be restricted."
        ),
        "sources": [
            # DataBC WFS: WHSE_HUMAN_CULTURAL_ECONOMIC.HIST_HISTORIC_ENVIRONMNT_PA_SV
            {
                "name": "DataBC — Historic Environment (Provincially Approved)",
                "url": "https://catalogue.data.gov.bc.ca/dataset?q=historic+environment+provincially+approved",
            },
        ],
        "style": {
            "fill_color": "#795548",
            "fill_opacity": 0.15,
            "line_color": "#795548",
            "line_width": 1.5,
            "line_opacity": 0.35,
            "border_minzoom": 11,
        },
    },
    "watersheds": {
        "visible": True,
        "label": "Named Watersheds",
        "type": "polygon",
        "group": "boundaries",
        "description": (
            "Named watershed groups from the BC Freshwater Atlas — the "
            "drainage areas some regulations reference by name."
        ),
        "sources": [
            {
                "name": "BC Freshwater Atlas — Watershed Groups",
                "url": "https://catalogue.data.gov.bc.ca/dataset/freshwater-atlas-watershed-groups",
            },
        ],
        "style": {
            "fill_color": "#006D77",
            "fill_opacity": 0.1,
            "line_color": "#006D77",
            "line_width": 1.5,
            "line_opacity": 0.35,
            "border_minzoom": 11,
        },
    },
    "land_access": {
        "visible": True,
        "label": "Land Access",
        "type": "polygon",
        "group": "land",
        "description": (
            "Areas closed to public access — private land, military (DND) "
            "land, and closed watersheds. Shown in red; check the exact "
            "boundary before visiting."
        ),
        "sources": [
            # OSM Overpass — restrictive-access land polygons (access=private,
            # boundary=protected_area, landuse=military/reservoir_watershed,
            # leisure=nature_reserve). See data/fetch_data.py::fetch_overpass_land_access.
            {
                "name": "OpenStreetMap contributors",
                "url": "https://www.openstreetmap.org/copyright",
            },
        ],
        # Fill/line colour is data-driven (match on restriction_level) → omitted.
        "style": {
            "fill_opacity": 0.14,
            "line_width": 1.5,
            "line_opacity": 0.35,
            "border_minzoom": 11,
        },
    },
    "aboriginal_lands": {
        "visible": True,
        "label": "Indigenous Lands",
        "type": "polygon",
        "group": "land",
        "description": (
            "First Nations reserves and Indigenous lands. Access and fishing "
            "may be governed by the respective Nation — confirm locally."
        ),
        "sources": [
            # OSM Overpass — see data/fetch_data.py::fetch_overpass_aboriginal_lands.
            {
                "name": "OpenStreetMap contributors",
                "url": "https://www.openstreetmap.org/copyright",
            },
        ],
        # line_width is a zoom ramp in code → omitted.
        "style": {
            "fill_color": "#8B6508",
            "fill_opacity": 0.10,
            "line_color": "#8B6508",
            "line_opacity": 0.35,
            "border_minzoom": 11,
        },
    },
    # Menu key is `land_parcels_private` because only PRIVATE parcels are
    # surfaced as a toggle. The underlying tile SOURCE layer stays
    # `land_parcels_crown` (it holds the full parcel fabric — crown/public/
    # private — filtered client-side); this manifest entry is menu config,
    # not the tile-writer name. Crown/Public is the inverse of private, so
    # it's implied and not shown as its own layer.
    "land_parcels_private": {
        "visible": False,
        "toggleable": True,
        "label": "Private Land",
        "type": "polygon",
        "group": "land",
        "description": (
            "Privately owned parcels, derived from the ParcelMap BC parcel "
            "fabric. Useful for spotting where riverbank/lakeshore access may "
            "cross private property."
        ),
        "sources": [
            {
                "name": "DataBC — ParcelMap BC Parcel Fabric",
                "url": "https://catalogue.data.gov.bc.ca/dataset/parcelmap-bc-parcel-fabric",
            },
        ],
        "style": {
            "fill_color": "#8D6E63",
            "fill_opacity": 0.22,
            "line_color": "#8D6E63",
            "line_width": 1.5,
            "line_opacity": 0.35,
            "border_minzoom": 11,
        },
    },
    # OSM trails/paths — not a tile layer of ours: this menu entry maps to the
    # basemap trail overlay (protomaps roads_other_early) so it can be toggled.
    # Default ON (matches the previous always-on behaviour). No `style` block —
    # the basemap layer keeps its own zoom-based styling unless the user edits it.
    "osm_trails": {
        "visible": True,
        "toggleable": True,
        "label": "Trails & Paths",
        "type": "line",
        "group": "roads",
        "description": (
            "Backcountry trails, paths, and minor tracks from OpenStreetMap "
            "(the basemap's trail layer)."
        ),
        "sources": [
            {
                "name": "OpenStreetMap contributors",
                "url": "https://www.openstreetmap.org/copyright",
            },
        ],
    },
    "forest_service_roads": {
        # Default OFF — OSM's own road coverage usually reads better; users opt in.
        "visible": False,
        "toggleable": True,
        "label": "Forest Service Roads",
        "type": "line",
        "group": "roads",
        "description": (
            "BC Forest Service Roads (FSRs) — backcountry resource roads. "
            "Solid = active; dotted = retired/deactivated (may be gated or "
            "impassable)."
        ),
        "sources": [
            {
                "name": "DataBC — Forest Tenure Road Section Lines",
                "url": "https://catalogue.data.gov.bc.ca/dataset/forest-tenure-road-section-lines",
            },
        ],
        # line_dash is data-driven (match on status) → omitted.
        "style": {"line_color": "#8B6D3F", "line_opacity": 0.75},
        # Road line + labels both start at z9.
        "min_tile_zoom": 9,
    },
    "water_access_points": {
        "visible": True,
        "label": "Water Access Points",
        "type": "point",
        "group": "points",
        "description": (
            "Boat launches, piers/docks, and designated fishing platforms, "
            "sourced from OpenStreetMap."
        ),
        "sources": [
            {
                "name": "OpenStreetMap contributors",
                "url": "https://www.openstreetmap.org/copyright",
            },
        ],
    },
    "waterfalls": {
        "visible": True,
        "label": "Waterfalls",
        "type": "point",
        "group": "points",
        "description": (
            "Waterfalls, sourced from OpenStreetMap. Often natural barriers to "
            "fish passage."
        ),
        "sources": [
            {
                "name": "OpenStreetMap contributors",
                "url": "https://www.openstreetmap.org/copyright",
            },
        ],
    },
    # Not user-facing (renders the grey mask outside BC) — no group so it never
    # appears in the layer menu; kept in the manifest for style completeness.
    "bc_mask": {
        "visible": True,
        "label": "Outside BC",
        "type": "polygon",
        "description": "Grey mask dimming the area outside British Columbia.",
        "style": {"fill_color": "#374151", "fill_opacity": 0.4},
    },
}


def _default_output() -> Path:
    """Repo-root-relative default: output/pipeline/deploy/layer_manifest.json."""
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "output" / "pipeline" / "deploy" / "layer_manifest.json"


def write_manifest(path: Path) -> None:
    """Serialise LAYER_MANIFEST to ``path`` as indented JSON."""
    try:
        import orjson

        data = orjson.dumps(LAYER_MANIFEST, option=orjson.OPT_INDENT_2)
    except ImportError:
        import json

        data = json.dumps(LAYER_MANIFEST, indent=2).encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else _default_output()
    write_manifest(out)
    print(f"Layer manifest → {out}")
