"""Enumerations shared across the pipeline data model (str-valued for JSON-friendliness)."""

from __future__ import annotations

from enum import Enum


class NameSource(str, Enum):
    """Provenance of a name; also the display-priority order (high -> low)."""
    override = "override"                 # manual name_variants.json / feature_display_names.json
    gazette = "gazette"                   # direct GNIS (stream GNIS_NAME; lake GNIS_NAME_1/2/3)
    side_channel = "side-channel"         # inherited from same-WSC main-channel BLK
    gauge = "gauge"                       # hydrometric gauge (WSC) name
    regulation = "regulation"             # name a regulation entry / synopsis uses (by id)
    stocking = "stocking"                 # stocking DB common name (by wbk)
    bathymetry = "bathymetry"             # bathymetry map name (by wbk)
    marker = "marker"                     # bathymetry map marker / point-of-interest name (by wbk)
    vessel_restriction = "vessel_restriction"  # lake named in the Vessel Operation Restriction Regs (SOR-2008-120)
    lake_survey = "lake_survey"           # GAZETTED_NAME from the BC lake-survey table (by wbk)
    dfo = "dfo"                           # name the DFO salmon regulations use for a water FWA names
                                          # differently (Braverman River / Braverman Creek) or not at
                                          # all (Docee River, Rainy Creek). SEARCHABLE, and ranked
                                          # below gazette so it never relabels a gazetted water —
                                          # a DFO-only water still displays it via `display: true`.
    alias = "alias"                       # alternate name — SEARCHABLE but never beats gazette
    synopsis = "synopsis"                 # a verbatim name used by a regulation entry


class NodeKind(str, Enum):
    stream = "stream"           # a BLK-merged stream (a section after splitting)
    lake = "lake"               # a lake/manmade waterbody node
    wetland = "wetland"         # a named marsh/pond/slough. Only ever MINTED (edgeless, no sidecar
                                # geometry): a stream overlays a wetland rather than being noded by
                                # it, so a wetland is never built from fid runs the way a lake is.
                                # Identity + naming treat it exactly like a lake (keyed by wbk);
                                # everything topological ignores it, as it has no edges.


class BoundaryKind(str, Enum):
    outlet = "outlet"           # natural mouth end (no named boundary)
    headwaters = "headwaters"   # natural source end
    lake = "lake"               # abuts a lake node
    split = "split"             # a point/line curated cut
    confluence = "confluence"   # a curated cut at a tributary's mouth
    mu = "mu"                   # a curated cut on an MU/zone boundary line
    border = "border"           # the BC provincial boundary (auto split; beyond it = out_of_bc)
    area = "area"               # an admin/park polygon boundary (curated closure; inside = in_areas)


class AnchorType(str, Enum):
    """How a cut GEOMETRY is defined. Every cut is a LINE or a polygon BOUNDARY."""
    point = "point"             # a coord -> auto perpendicular cut line at the target mainstem
    line = "line"              # an explicit cut line (list of coords)
    lake = "lake"              # a lake polygon boundary (wbk)
    mu_boundary = "mu_boundary"  # the shared boundary line between two MUs
    confluence = "confluence"    # a cut line at where a tributary BLK meets the mainstem
    border = "border"           # the BC provincial outline (auto, not hand-authored)
    area_boundary = "area_boundary"  # an admin/park polygon boundary; cut a named water + its WSC
                                # descendants where they cross it, then flag INSIDE pieces (in_areas).
    gauge = "gauge"             # a hydrometric station's own position on the blue line it measures.
                                # GENERATED, not authored — see pipeline/hydro/splits.py. A reach
                                # that runs past a gauge claims that gauge's reading for water the
                                # gauge never saw; cutting there is what stops one number colouring
                                # 300 km of the Fraser.


WATERBODY_KINDS = frozenset({NodeKind.lake, NodeKind.wetland})
"""Node kinds identified by WATERBODY_KEY rather than by blue line. Naming and registry identity
treat these alike; topology (edges, splits, pruning, tributaries) only ever sees streams."""
