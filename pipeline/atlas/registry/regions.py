"""THE REGION A WATER LIES IN — one region per section, for the zone rules (user ruling 2026-09-25).

A water takes the zone rules of the region it lies IN. Region membership (`area:region:*` registry
items) is by INTERSECTION: the atlas flags every piece that reaches into a region polygon
(`splits.border.mark_inside_areas`), and a lake is never cut, so a lake drawn across a region line
sits in both — and bound BOTH regions' standing tables, tied. Mara Lake (61 % Region 3, 39 % Region
8 by area; the book prints it under Shuswap Lake in Region 3) carried Region 3's "Trout/char: 5"
and Region 8's "Trout/char: 5" side by side. Measured on the promoted build (2026-09-25): 215
sections sit in more than one region — 160 lakes and 55 stream pieces the area cutter's
first-enter/last-exit left straddling — and 41 rule sets (214 sections) carried two regions' base.

THE HOME REGION is the region holding the largest share of the section: a waterbody by its OUTLINE's
area (the same geometry the membership pass tests), a stream piece by its line's length. A tie
(never seen on real data) goes to the smaller region id, so the answer is deterministic. Nothing is
cut: the straddling section is MARKED as its home region's for the zone rules
(`straddling-sections-mark-not-cut`), and only there.

ONLY A STREAM PIECE IS HELD TO ITS HOME (user ruling 2026-09-25, second half). A LAKE straddling a
region line keeps BOTH regions' zone rules and the most strict wins, per fish (`in_region`;
`deliver.bundle.read.effective_rules` step 6). The home of a lake is still measured and reported.

WHAT IT DOES NOT TOUCH. A regional ROW is still limited to the regions its id names by the region
polygons it touches (`reach.outside.region_sections`): the book prints Mara Lake in Region 8's table
too ("No powered boats south of the CPR bridge"), and that row still binds the lake. Nor does it
move the province's border: a section in any region polygon is inside B.C., whichever is home.

Computed from the ATLAS the reach run reads — region polygons (`area_catalog.gpkg`), waterbody
outlines (`waterbody_polys.pkl`) and section lines (`geometries.pkl`) — about ten seconds. The reach
CLI and the review app both attach it to the graph they resolve against (`attach`), so the two
cannot disagree; `extent.area_sections` reads it for every `area:region:*` key.
"""
from __future__ import annotations

from pathlib import Path
from types import MappingProxyType
from typing import Mapping

REGION_PREFIX = "area:region:"

#: The graph attribute the home map is held under (see `attach`). A graph without it resolves
#: region areas by plain membership — a unit-test fixture, which has no polygons to measure.
_ATTR = "_region_home"


def straddlers(registry) -> dict[str, tuple[str, ...]]:
    """`{section: (region, …)}` for every section in more than one `area:region:*` item."""
    member: dict[str, set[str]] = {}
    for k in sorted(registry):
        if str(k).startswith(REGION_PREFIX):
            region = str(k)[len(REGION_PREFIX):]
            for s in registry[k].section_ids:
                member.setdefault(s, set()).add(region)
    return {s: tuple(sorted(r)) for s, r in sorted(member.items()) if len(r) > 1}


def home_of(shares: Mapping[str, float]) -> str:
    """The region with the largest share; a tie goes to the smaller region id."""
    return sorted(shares.items(), key=lambda kv: (-round(kv[1], 12), kv[0]))[0][0]


def region_shares(build_dir, sections: Mapping[str, tuple[str, ...]]) -> dict[str, dict[str, float]]:
    """`{section: {region: share}}` — the share of each straddling section inside each region it is
    a member of: area for a waterbody's outline, length for a line. Raises when a section has no
    geometry or a region no polygon: a home cannot be guessed."""
    import fiona
    from shapely.geometry import shape

    from pipeline.common.io.serialize import read_artifact

    if not sections:
        return {}
    build = Path(build_dir)
    wanted = {r for rs in sections.values() for r in rs}
    polys: dict = {}
    with fiona.open(str(build / "area_catalog.gpkg"), layer="areas") as src:
        for f in src:
            aid = str(f["properties"]["area_id"])
            if aid.startswith(REGION_PREFIX) and aid[len(REGION_PREFIX):] in wanted:
                polys[aid[len(REGION_PREFIX):]] = shape(f["geometry"])
    missing = sorted(wanted - set(polys))
    if missing:
        raise SystemExit(f"regions: no polygon for region(s) {missing} in {build}/area_catalog.gpkg")
    outlines = read_artifact(str(build / "waterbody_polys.pkl"))
    lines = read_artifact(str(build / "geometries.pkl"))
    out: dict[str, dict[str, float]] = {}
    for s, regions in sections.items():
        g = outlines.get(s) if s.startswith("lake:") else None
        if g is None:
            g = lines.get(s)
        if g is None or g.is_empty:
            raise SystemExit(f"regions: section {s} (in regions {list(regions)}) has no geometry "
                             f"to measure its home region by")
        area = g.geom_type.endswith("Polygon")
        whole = g.area if area else g.length
        out[s] = {r: ((g.intersection(polys[r]).area if area else g.intersection(polys[r]).length)
                      / whole if whole else 0.0) for r in regions}
    return out


def homes(build_dir, registry) -> dict[str, str]:
    """`{section: home region}` for every straddling section of this atlas."""
    return {s: home_of(sh) for s, sh in region_shares(build_dir, straddlers(registry)).items()}


def attach(graph, home: Mapping[str, str]) -> None:
    """Hold the home map on the graph the resolver reads (frozen)."""
    setattr(graph, _ATTR, MappingProxyType(dict(home)))


#: A section id naming a waterbody polygon — a lake, which is never cut.
LAKE_PREFIX = "lake:"


def in_region(graph, region: str, sections: set[str]) -> set[str]:
    """`sections` (one region's members) less the STREAM PIECES whose home is another region.

    A LAKE STRADDLING A REGION LINE IS IN BOTH (user ruling 2026-09-25, Ahbau Lake 51/49, Mara Lake
    61/39): a lake is one water that is never cut, and an angler on either shore is in either
    region, so it binds BOTH regions' zone rules and the MOST STRICT applies — decided per fish
    where the rules are read (`deliver.bundle.read.effective_rules`, step 6). A stream piece
    wandering across the line is a line with a length: it keeps its home region by length."""
    home = getattr(graph, _ATTR, None) if graph is not None else None
    if not home:
        return sections
    return {s for s in sections
            if s.startswith(LAKE_PREFIX) or home.get(s, region) == region}
