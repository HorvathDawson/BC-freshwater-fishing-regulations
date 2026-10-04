"""Split definitions (hand-authored cut geometry) and resolved cut points (04)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from pipeline.common.models.enums import AnchorType


@dataclass(frozen=True)
class SplitAnchor:
    """Defines the CUT GEOMETRY (always a line or a polygon boundary). Only the fields
    relevant to ``type`` are populated.

    - point       : a coord -> an auto perpendicular cut line across the target mainstem
                    (length bounded by SplitDef.proximity_m, so it also catches nearby side
                    channels but nothing far away).
    - line        : an explicit cut line (>=2 coords).
    - lake        : the lake polygon boundary (wbk).
    - mu_boundary : the shared boundary line between mu_a and mu_b (needs BOTH).
    - confluence  : an auto cut line where tributary_blk meets the target mainstem.

    A ``point``/``confluence``/``lake`` anchor may carry an ALONG-CHANNEL offset: the cut lands
    ``offset_m`` metres ``offset_dir`` ("upstream"|"downstream") from the projected coord/mouth,
    following the channel (not straight-line). Lets a reg like "100 m downstream of the falls" be
    authored from the falls point alone; clamped to the channel ends (with a concern if clamped).
    """
    type: AnchorType
    coord: Optional[tuple[float, float]] = None       # point
    coords: tuple[tuple[float, float], ...] = ()      # line (>=2 vertices)
    is_lonlat: bool = False
    offset_m: float = 0.0         # point/confluence: shift the cut this many metres ALONG the
    offset_dir: str = ""          # channel, in offset_dir ("upstream"|"downstream"). 0 => no shift.
    wbk: str = ""                 # lake
    mu_a: str = ""                # mu_boundary (one side)
    mu_b: str = ""                # mu_boundary (other side)
    tributary_blk: str = ""       # confluence: the tributary's BLK (an id, not a name)
    tributary_wsc: str = ""       # confluence: OR the tributary's WSC (trimmed) — its mouth
    area_layer: str = ""          # area_boundary: the polygon layer (e.g. "parks_bc")
    area_name_field: str = ""     # area_boundary: the layer field to match on (e.g. PROTECTED_LANDS_NAME)
    area_name: str = ""           # area_boundary: the value to match (e.g. "GARIBALDI PARK")
    wsc_descendants: bool = False # area_boundary: target the whole WSC subtree (prefix) not exact WSC

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "SplitAnchor":
        t = AnchorType(d["type"])
        coord = d.get("coord")
        coords = tuple((float(c[0]), float(c[1])) for c in d.get("coords", []))
        if t == AnchorType.mu_boundary and not (d.get("mu_a") and d.get("mu_b")):
            raise ValueError("mu_boundary anchor needs both mu_a and mu_b")
        if t == AnchorType.line and len(coords) < 2:
            raise ValueError("line anchor needs >=2 coords")
        if t == AnchorType.gauge and not coord:
            raise ValueError("gauge anchor needs the station's coord")
        if t == AnchorType.confluence and not (d.get("tributary_blk") or d.get("tributary_wsc")):
            raise ValueError("confluence anchor needs tributary_blk or tributary_wsc")
        if t == AnchorType.lake and not d.get("wbk"):
            raise ValueError("lake anchor needs wbk")
        if t == AnchorType.area_boundary and not (d.get("area_layer") and d.get("area_name")):
            raise ValueError("area_boundary anchor needs area_layer and area_name")
        offset_m = float(d.get("offset_m", 0.0))
        offset_dir = str(d.get("offset_dir", ""))
        if offset_m < 0:
            raise ValueError("offset_m must be >= 0 (give the direction via offset_dir)")
        if offset_m:
            if t not in (AnchorType.point, AnchorType.gauge, AnchorType.confluence,
                         AnchorType.lake):
                raise ValueError(f"offset_m only valid on point/confluence/lake anchors, not {t.value}")
            if offset_dir not in ("upstream", "downstream"):
                raise ValueError("offset_m requires offset_dir 'upstream' or 'downstream'")
        return cls(
            type=t,
            coord=(float(coord[0]), float(coord[1])) if coord else None,
            coords=coords, is_lonlat=bool(d.get("is_lonlat", False)),
            offset_m=offset_m, offset_dir=offset_dir,
            wbk=str(d.get("wbk", "")), mu_a=str(d.get("mu_a", "")), mu_b=str(d.get("mu_b", "")),
            tributary_blk=str(d.get("tributary_blk", "")),
            tributary_wsc=str(d.get("tributary_wsc", "")),
            area_layer=str(d.get("area_layer", "")),
            area_name_field=str(d.get("area_name_field", "")),
            area_name=str(d.get("area_name", "")),
            wsc_descendants=bool(d.get("wsc_descendants", False)),
        )


@dataclass(frozen=True)
class SplitDef:
    """One hand-authored split. ``id`` is a STABLE key (part of the section_id ABI).

    A split's cut geometry (anchor) intersects streams; each crossed channel within
    ``proximity_m`` and matching the optional target scope is cut where it crosses.

    Target scope (optional, at most one) narrows which channels are eligible:
      - ``blk``     : only this blue line.
      - ``wsc``     : this river + its side channels (share the WSC) — proximity-limited.
      - ``gnis_id`` : resolve to the named stream's blk(s).
    A ``point``/``confluence`` anchor REQUIRES a target (it needs a mainstem to cut across).
    """
    id: str
    anchor: SplitAnchor
    blk: str = ""
    wsc: str = ""
    gnis_id: str = ""
    stream_name: str = ""
    label: str = ""
    proximity_m: float = 500.0    # max distance a channel may be from the cut geometry
    concern: str = ""             # optional free-text caveat (inferred name, multi-crossing collapse,
                                  # unresolved) — surfaced in splits.resolved.json + the gpkg, never silent

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "SplitDef":
        targets = [k for k in ("blk", "wsc", "gnis_id") if d.get(k)]
        if len(targets) > 1:
            raise ValueError(f"split {d.get('id')!r}: at most one of blk/wsc/gnis_id, got {targets}")
        anchor = SplitAnchor.from_dict(d["anchor"])
        if anchor.type in (AnchorType.point, AnchorType.gauge, AnchorType.confluence,
                           AnchorType.area_boundary) and not targets:
            raise ValueError(f"split {d.get('id')!r}: {anchor.type.value} anchor requires a target (blk/wsc/gnis_id)")
        return cls(
            id=str(d["id"]), anchor=anchor,
            blk=str(d.get("blk", "")), wsc=str(d.get("wsc", "")),
            gnis_id=str(d.get("gnis_id", "")), stream_name=str(d.get("stream_name", "")),
            label=str(d.get("label", "")), proximity_m=float(d.get("proximity_m", 500.0)),
            concern=str(d.get("_concern", d.get("concern", ""))),
        )


@dataclass(frozen=True)
class SplitPoint:
    """A resolved cut on one blue line. Written to splits.resolved.json for reviewable builds."""
    split_id: str
    blk: str                # the specific blue line this cut lands on (one per crossed channel)
    route_measure: float    # absolute DOWNSTREAM_ROUTE_MEASURE cut position
    fid: str                # containing fid (stored back)
    label: str
    anchor_type: AnchorType
    offset_m: float = 0.0   # distance from the cut geometry to the channel crossing (review aid)
    proximity_m: float = 0.0  # authored pickup radius (carried from SplitDef) — reuse an existing
                            # boundary within this many route-metres instead of cutting a duplicate
    concern: str = ""       # carried from SplitDef.concern (+ resolver-added caveats)
    picked_up: bool = False # True => this curated split reused an existing (lake/border) boundary
                            # within proximity instead of cutting a new one (docs/04 proximity pickup)
    # THE AUTHORED OFFSET (SplitAnchor.offset_m/offset_dir: "100 m downstream of the falls") — not
    # `offset_m` above, which is the resolver's review aid. Carried into splits.resolved.json so the
    # bundle names the cut ("100 m downstream of the Morrison Creek confluence") from the atlas.
    anchor_offset_m: float = 0.0
    anchor_offset_dir: str = ""
    # Where the cut came from: "curated" (splits.json), "gauge" (gauge_match.json), "area",
    # "length", "border". The bundle takes a cut's book name from the CURATED rows only.
    source: str = ""
