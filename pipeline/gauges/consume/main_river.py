"""A water reads a gauge about its own size, on its own main river (T825).

THE FAULT. The shed walk (`shed.py`) ranks a gauge for a section by the ratio of their FWA line magnitudes. A side
channel, slough or wide pool carries a tiny magnitude of its own (often 1-3), so the gauge that "fits" it best is a
small one far up the river: the lower Fraser's sloughs read the Fraser at Red Pass, 1,200 km upstream, while Agassiz,
Mission and Port Mann stand beside them. Measured on r-46e4355a7277: 245 sections whose gauge drains less than a tenth
of their water (the drainage the map already colours them with, `section_panel.area_km2`: a side channel there is
sized by the river it is a channel of, `panels.mainstem_of`).

THE RULE (owner 2026-11-17 15:04, verbatim pick "Yes, use the rule (Recommended)", on the list of all 245): a section
whose chosen gauge drains less than a tenth of its water reads instead the NEAREST gauge on its own MAIN RIVER that
measures flow and drains about as much country (a tenth to ten times as much); if that river has none, it reads none.
  * its main river: for a side channel `panels.mainstem_candidates` adopts, the blue line of the river section it is
    a channel of; otherwise its own blue line, or, for a polygon, the biggest line of its own water;
  * nearest: along that river, by route measure (metres from its mouth) — the section's midpoint and the station's
    coordinate projected onto the river's line;
  * measures flow: a `discharge` record ("Flow leads, Level auxiliary": a lead reads flow); a station that may speak
    (`liveness.may_speak`) and is a donor (it has an area in the panels).
A section whose gauge is not that much smaller is untouched: the rule re-chooses only where today's choice is wrong.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Callable, Iterable, Mapping, Optional

from pipeline.gauges.consume.shed import REL_DOWN, REL_ON, REL_UP, TRUST_BANDS, GaugeLink

#: How much more country a section's water drains than its gauge before the gauge is no longer its own: the owner's
#: "a tenth" (T825), and the bound the fit uses either way.
SMALLER = 10.0

#: The mutation pin: False and every link stands as the shed walk chose it (the 245 read their far gauges again).
MAIN_RIVER_GAUGE = True


@dataclass(frozen=True)
class Place:
    """Where a point stands on a river: the river's blue line and the route measure (metres from its mouth)."""
    blk: str
    m: float


def too_small(link: GaugeLink, target_area: Mapping[str, float], donor_area: Mapping[str, float]) -> bool:
    """The trigger: the section's water drains at least `SMALLER` times its gauge's."""
    area, a = target_area.get(link.section_id), donor_area.get(link.station)
    return bool(MAIN_RIVER_GAUGE and area and a and area >= SMALLER * a)


def fits(area: float, gauge_area: Optional[float]) -> bool:
    """About the same size: a tenth to ten times the section's water."""
    return bool(gauge_area) and area / SMALLER <= gauge_area <= area * SMALLER


def band(ratio: float) -> str:
    """The trust band of a size ratio (`shed.TRUST_BANDS`, the same floors as the walk's)."""
    for name, floor in TRUST_BANDS:
        if ratio >= floor:
            return name
    return TRUST_BANDS[-1][0]


def rechoose(links: Iterable[GaugeLink], *, target_area: Mapping[str, float], donor_area: Mapping[str, float],
             flows: frozenset | set, section_place: Mapping[str, Place],
             gauge_place: Mapping[str, Place]) -> tuple[list[GaugeLink], dict]:
    """Every link, with the rule applied. A re-chosen link carries the new station, its trust band by size ratio, the
    along-river distance and `rel` up / down / on as the gauge stands to the section; a section left with no gauge
    is dropped. Returns `(links, report)` with `report["rechosen"]` / `report["none"]` as `(section, old, new)`."""
    out: list[GaugeLink] = []
    report: dict = {"rechosen": [], "none": []}
    for link in links:
        if not too_small(link, target_area, donor_area):
            out.append(link)
            continue
        area = target_area[link.section_id]
        here = section_place.get(link.section_id)
        best = None
        if here is not None:
            for st, gp in gauge_place.items():
                if gp.blk != here.blk or st not in flows or not fits(area, donor_area.get(st)):
                    continue
                key = (abs(gp.m - here.m), st)
                if best is None or key < best:
                    best = key
        if best is None:
            report["none"].append((link.section_id, link.station, None))
            continue
        dist, st = best
        gm = gauge_place[st].m
        ga = donor_area[st]
        rel = REL_ON if dist < 1.0 else (REL_DOWN if gm < here.m else REL_UP)
        out.append(replace(link, station=st, trust=band(min(area, ga) / max(area, ga)),
                           ratio=round(min(area, ga) / max(area, ga), 6), hops=0, rel=rel,
                           dist_m=int(round(dist)), joined=False))
        report["rechosen"].append((link.section_id, link.station, st))
    return out, report


# ---------------------------------------------------------------- placing sections and stations on their river

def main_blks(sections: Iterable[str], graph, adopt: Mapping[str, list[str]], water_of: Mapping[str, str]) -> dict:
    """`section -> the blue line (blk) of its main river`: the river section a side channel is adopted onto (the
    first of `adopt`, `panels.mainstem_candidates`), else its own line, else (a polygon) its water's biggest line."""
    biggest: dict[str, tuple[int, str]] = {}
    want = {water_of.get(s) for s in sections if s not in adopt and not _is_line(graph, s)}
    if want - {None}:
        for sec, node in graph.nodes.items():
            w = water_of.get(sec)
            if w in want and _is_line(graph, sec):
                k = (int(getattr(node, "stream_magnitude", 0) or 0), sec)
                if w not in biggest or k > biggest[w]:
                    biggest[w] = k
    out = {}
    for s in sections:
        if s in adopt and adopt[s]:
            out[s] = _blk(adopt[s][0])
        elif _is_line(graph, s):
            out[s] = _blk(s)
        elif water_of.get(s) in biggest:
            out[s] = _blk(biggest[water_of[s]][1])
    return out


def place(blk_of: Mapping[str, str], geoms: Mapping, points: Mapping[str, object]) -> dict[str, Place]:
    """`key -> Place` for every point (a shapely Point in EPSG:3005) whose river `blk_of[key]` has drawn sections:
    projected onto the nearest section of that river, measure = the section's own start + the distance along it
    (FWA lines run mouth first)."""
    need = set(blk_of.values())
    lines: dict[str, list] = {}
    for k in sorted(geoms):
        if not isinstance(k, str) or ":" not in k or k.startswith(("connector", "lake")):
            continue
        b, m = k.split(":", 1)
        g = geoms[k]
        if b in need and g is not None and not g.is_empty:
            lines.setdefault(b, []).append((float(m), g))
    out = {}
    for key, pt in sorted(points.items()):
        b = blk_of.get(key)
        best = None
        for m0, g in lines.get(b, ()):
            d = g.distance(pt)
            if best is None or d < best[0]:
                best = (d, m0 + g.project(pt))
        if best is not None:
            out[key] = Place(b, best[1])
    return out


def midpoint(g):
    """A section's point: halfway along a line, a point inside a polygon."""
    return g.interpolate(0.5, normalized=True) if "Line" in g.geom_type else g.representative_point()


def _blk(section_id: str) -> str:
    return section_id.split(":", 1)[0]


def _is_line(graph, sec: str) -> bool:
    node = graph.nodes.get(sec)
    k = getattr(node, "kind", "") if node is not None else ""
    k = k.value if hasattr(k, "value") else str(k).split(".")[-1]
    return k == "stream" and not sec.startswith("lake")


def apply(links: list[GaugeLink], graph, geoms, *, target_area: Mapping[str, float],
          donor_area: Mapping[str, float], flows: set, station_xy: Mapping[str, tuple[float, float]],
          station_section: Mapping[str, str], water_of: Mapping[str, str],
          adopt: Callable[[], Mapping[str, list[str]]]) -> tuple[list[GaugeLink], dict]:
    """The rule over a build: find the links it fires on, place them and the candidate stations on their main rivers,
    re-choose. `adopt` is called only when some link fires (it walks every side channel)."""
    from pyproj import Transformer
    from shapely.geometry import Point

    fired = sorted({l.section_id for l in links if too_small(l, target_area, donor_area)})
    if not fired:
        return list(links), {"rechosen": [], "none": []}
    rivers = main_blks(fired, graph, adopt(), water_of)
    pts = {s: midpoint(geoms[s]) for s in fired if geoms.get(s) is not None}
    sec_place = place(rivers, geoms, pts)
    tf = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)
    on = {st: _blk(sec) for st, sec in station_section.items() if st in station_xy and not sec.startswith("lake")}
    on = {st: b for st, b in on.items() if b in set(rivers.values())}
    gpts = {st: Point(*tf.transform(*station_xy[st])) for st in on}
    return rechoose(links, target_area=target_area, donor_area=donor_area, flows=flows,
                    section_place=sec_place, gauge_place=place(on, geoms, gpts))
