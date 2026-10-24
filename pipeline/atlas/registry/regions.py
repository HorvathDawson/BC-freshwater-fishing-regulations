"""THE REGIONS A WATER LIES IN — for the zone rules and every per-region reader.

A water takes the zone rules of the region it lies IN. Region membership (`area:region:*` registry
items) is by INTERSECTION: the atlas flags every piece that reaches into a region polygon by more
than `SLIVER_M` (`splits.border.mark_inside_areas`, `holds` below — a touch is not membership), and
a lake is never cut, so a lake drawn across a region line sits in both. Mara Lake (61 % Region 3, 39 %
Region 8 by area; the book prints it under Shuswap Lake in Region 3) carries Region 3's "Trout/char:
5" and Region 8's "Trout/char: 5". Measured on the promoted build (2026-09-25): 215 sections sit in
more than one region — 160 lakes and 55 stream pieces the area cutter's first-enter/last-exit left
straddling.

A STRADDLER CARRIES BOTH REGIONS' RULES (T183, owner 2026-10-24, reversing T115 and the stream half of
the 2026-09-25 ruling): "those straddle pieces are supposed to get both regions' regulations". A
stream piece drawn across a region line is in both regions, as a lake is — both zone tables, both
regions' per-region rows (the Fraser's Region 3 and Region 5 rows on 356364114:418405) — and the most
strict applies, per fish, where the rules are read (`deliver.bundle.read.effective_rules` step 6).
Moving a length cut therefore never moves a piece's region rules.

THE HOME REGION is still measured: the region holding the largest share of the section (a waterbody
by its OUTLINE's area, a stream piece by its line's length; a tie goes to the smaller region id). It
is a fact for display (the bundle's `section_home`, the export's `home_region`) and the T115 mutation
pin (`in_region`); it filters no membership. Computed ONCE, BY THE ATLAS BUILD — from region polygons
(`area_catalog.gpkg`), waterbody outlines (`waterbody_polys.pkl`) and section lines
(`geometries.pkl`) — and written to `region_home.json` beside `registry.json` (`write_homes`). The
reach CLI and the review app attach that file to the graph they resolve against (`attach_from`).
"""
from __future__ import annotations

from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from pipeline.common.tolerances import SLIVER_M

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


#: The straddling sections that are STANDING WATER (a lake or wetland by the registry's kind):
#: in every region they touch. A river's polygon is held to its home like the river.
_ATTR_BOTH = "_region_both"


def attach(graph, home: Mapping[str, str], both=()) -> None:
    """Hold the home map on the graph the resolver reads (frozen), and `both` — the straddling
    sections that are lakes for the regulations (`in_region` keeps them in every region)."""
    setattr(graph, _ATTR, MappingProxyType(dict(home)))
    setattr(graph, _ATTR_BOTH, frozenset(both))


def lakes_among(graph, registry, home: Mapping[str, str]) -> frozenset[str]:
    """The straddling sections whose WATER KIND is lake or wetland (`reach.water_kind.kind_of`:
    the registry's answer, so a river's polygon is a river here too)."""
    from pipeline.atlas.reach.water_kind import kind_of
    return frozenset(s for s in home if kind_of(graph, registry, s) in ("lake", "wetland"))


#: The atlas file the home map is written to, beside `registry.json` — `{section: home region}`
#: for every straddling section, sorted. A fact of the atlas, computed once by the build.
HOME_FILE = "region_home.json"


def write_homes(build_dir, registry, out_dir=None) -> dict[str, str]:
    """Measure `homes` for this atlas (its geometry is read from `build_dir`) and write
    `region_home.json` into `out_dir` (default: `build_dir` itself; the sidecars writer names a
    side directory so a promoted atlas is never written into). Returns the map."""
    import json
    home = homes(build_dir, registry)
    (Path(out_dir if out_dir is not None else build_dir) / HOME_FILE).write_text(
        json.dumps(dict(sorted(home.items())), indent=0, sort_keys=True) + "\n", encoding="utf-8")
    return home


def read_homes(build_dir) -> dict[str, str]:
    """The home map the atlas wrote. RAISES when the file is absent: an atlas without it predates
    the field (rebuild it, or write the sidecars: `python -m pipeline.atlas.sidecars`); reading
    `{}` would silently bind both regions' tables to every straddling stream piece."""
    import json
    p = Path(build_dir) / HOME_FILE
    if not p.exists():
        raise SystemExit(f"regions: no {p} — this atlas predates region homes. Write them from the "
                         f"atlas's own geometry:\n    python -m pipeline.atlas.sidecars --build "
                         f"{build_dir}")
    got = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(got, dict) or any(not isinstance(v, str) for v in got.values()):
        raise SystemExit(f"regions: {p} is not {{section: region}}")
    return got


def attach_from(build_dir, graph, registry) -> dict[str, str]:
    """`attach` the atlas's written home map (`read_homes`) to `graph`, with the straddling LAKES
    told apart by the registry's kind (`lakes_among`); returns the map."""
    home = read_homes(build_dir)
    attach(graph, home, lakes_among(graph, registry, home))
    return home


class HomeView:
    """THE HOME MAP WITHOUT THE GRAPH (T140): what `in_region` reads off a graph (`attach`), held on
    its own so a reader that has released the 5 GB graph (the bundle's rules, read after
    `graph.pkl` is dropped) still asks the one membership. Pass it wherever a `graph` carrying the
    home map is taken by the functions below (`region_sections`, `section_regions`, …)."""
    __slots__ = (_ATTR, _ATTR_BOTH)


def home_view(graph) -> HomeView:
    """The home map attached to `graph` (`attach` / `attach_from`), as a `HomeView`. RAISES when none
    is attached: a view of nothing would silently be plain polygon membership."""
    home = getattr(graph, _ATTR, None) if graph is not None else None
    if home is None:
        raise ValueError("regions.home_view: no home map is attached to this graph "
                         "(`regions.attach_from` first)")
    v = HomeView()
    setattr(v, _ATTR, home)
    setattr(v, _ATTR_BOTH, getattr(graph, _ATTR_BOTH, frozenset()))
    return v


def home_view_from(build_dir, registry) -> HomeView:
    """`home_view` of the atlas at `build_dir`, loading its `graph.pkl` to tell the straddling lakes
    apart (`attach_from`). For a caller with no graph in hand; the bundle hands its own view in."""
    from pipeline.common.io.serialize import read_artifact
    graph = read_artifact(str(Path(build_dir) / "graph.pkl"))
    attach_from(build_dir, graph, registry)
    return home_view(graph)


#: A section id naming a waterbody polygon — the SHAPE drawn (a river's polygon has it too).
LAKE_PREFIX = "lake:"


def in_region(graph, region: str, sections: set[str]) -> set[str]:
    """`sections` (one region's members) less the STREAM sections whose home is another region.

    T115'S HOME FILTER, read only with `STRADDLERS_IN_EVERY_REGION` off (the T183 mutation pin): the
    one membership is now raw (a straddling stream piece is in both regions, owner 2026-10-24). Kept
    so the pin can say what it reverses.

    A LAKE STRADDLING A REGION LINE IS IN BOTH (user ruling 2026-09-25, Ahbau Lake 51/49, Mara Lake
    61/39): a lake is one water that is never cut, and an angler on either shore is in either
    region, so it binds BOTH regions' zone rules and the MOST STRICT applies — decided per fish
    where the rules are read (`deliver.bundle.read.effective_rules`, step 6). A stream piece
    wandering across the line is a line with a length: it keeps its home region by length — and
    so does a river's own POLYGON (`attach`'s `both` is by the registry's WATER kind, never by the
    `lake:` prefix, which is only the shape drawn)."""
    home = getattr(graph, _ATTR, None) if graph is not None else None
    if not home:
        return sections
    both = getattr(graph, _ATTR_BOTH, frozenset())
    return {s for s in sections if s in both or home.get(s, region) == region}


# ---------------------------------------------------------------------------------------------------
# ONE REGION MEMBERSHIP (T115, owner 2026-10-22: one decision, one function). "Which sections are in
# region R?" is answered HERE and nowhere else. The zone rules (`extent.area_sections`), a per-region
# row's region (`reach.outside.region_limit`), the steelhead twin's `area_id`, an `area_kind: region`
# family, the regions a row's water lies in (`section_regions`) and the province's inside
# (`reach.outside.outside_bc`) all ask these functions. `test_one_region_membership.py` pins that no
# other module reads a region item's sections.
#
# ITS ANSWER IS RAW MEMBERSHIP (T183, owner 2026-10-24, REVERSING T115's "home region only"): "those
# straddle pieces are supposed to get both regions' regulations". A section is in EVERY region whose
# polygon holds more than `SLIVER_M` of it (`holds`, the atlas's membership pass): a stream piece drawn
# across a region line carries both regions' zone rules and both regions' per-region rows, exactly as a
# straddling lake always did, and the most strict applies per fish where the rules are read
# (`deliver.bundle.read.effective_rules` step 6). A TOUCH IS NOT MEMBERSHIP: a piece cut at the line
# meets the polygon at a point, and a piece reaching under `SLIVER_M` into it is positional error. The
# home region (`region_home.json`) stays a measured, displayed fact; it no longer filters anything.
# ---------------------------------------------------------------------------------------------------

#: Caches keyed by the registry's (and graph's) id — each value HOLDS the objects, so a recycled id
#: never serves another registry's answer.
_ITEMS: dict = {}
_SECTIONS: dict = {}
_MEMBER: dict = {}

#: POLICY (T183, reversing T115): a region's sections are EVERY section its polygon holds more than
#: `SLIVER_M` of, for every reader — a straddling stream piece is in both regions, as a lake is. False =
#: T115's home members (`in_region`: a straddling stream piece in its home region only) for every
#: reader — the mutation pin.
STRADDLERS_IN_EVERY_REGION = True


def holds(area_id: str, inside_m: float, otherwise_m: float) -> bool:
    """DOES THIS AREA'S POLYGON HOLD A LINE PIECE whose length inside it is `inside_m`? The atlas's
    membership pass asks it (`splits.border.mark_inside_areas`). A REGION holds a piece only when more
    than `SLIVER_M` of it lies inside (T183: a touch is not membership, and since a straddler now
    carries both regions' rules, a piece reaching a metre or two over the line must not); any other
    area keeps its own threshold (`otherwise_m`)."""
    return inside_m > (SLIVER_M if str(area_id).startswith(REGION_PREFIX) else otherwise_m)


def region_items(registry) -> list[str]:
    """Every `area:region:*` item id of the registry, sorted."""
    hit = _ITEMS.get(id(registry))
    if hit is not None and hit[0] is registry:
        return hit[1]
    got = sorted(str(k) for k in registry.keys() if str(k).startswith(REGION_PREFIX))
    if len(_ITEMS) > 16:
        _ITEMS.clear()
    _ITEMS[id(registry)] = (registry, got)
    return got


def zones_of(registry, region: str) -> dict[str, frozenset[str]]:
    """{"7a": MUs, "7b": MUs} for a region the registry holds as lettered zones; {} otherwise."""
    out: dict[str, frozenset[str]] = {}
    for k in region_items(registry):
        z = k[len(REGION_PREFIX):]
        if z.startswith(region) and z[len(region):].isalpha() and z != region:
            units = frozenset(getattr(registry[k], "mus", ()) or ())
            if units:
                out[z] = units
    return out


def region_members(registry, graph, key: str) -> set[str]:
    """ONE region item's sections (`area:region:5`, `area:region:7a`): every section its polygon
    holds more than `SLIVER_M` of (the atlas's membership pass, `holds`) — a straddler is in every
    region it lies in (T183). With the policy pin off, T115's home members (`in_region`). What
    `extent.area_sections` answers for the key."""
    got = set(registry[key].section_ids)
    if STRADDLERS_IN_EVERY_REGION:
        return got
    return in_region(graph, key[len(REGION_PREFIX):], got)


def _cache_key(registry, graph, *rest):
    """A cache key covering everything the answer reads: the registry, the graph AND the home map
    attached to it (`attach` may be called again on the same graph)."""
    home = getattr(graph, _ATTR, None) if graph is not None else None
    both = getattr(graph, _ATTR_BOTH, None) if graph is not None else None
    return (id(registry), id(graph), id(home), id(both), STRADDLERS_IN_EVERY_REGION, *rest), \
        (registry, graph, home, both)


def _cached(cache: dict, key, held):
    hit = cache.get(key)
    if hit is not None and all(a is b for a, b in zip(hit[0], held)):
        return hit[1]
    return None


def _store(cache: dict, key, held, value, cap: int):
    if len(cache) > cap:
        cache.clear()
    cache[key] = (held, value)
    return value


def region_sections(regions, registry, graph) -> frozenset[str] | None:
    """THE SECTIONS OF THE NAMED REGIONS — each region item's members (`region_members`: every
    section in its polygon, a straddler in both, T183), unioned. A region `7` is 7A and 7B together;
    a region the registry does not have contributes nothing. `None` when the registry has no regions
    at all (a fixture: nothing can be said). `graph` matters only with the policy pin off (T115's
    home members); `None` there is plain membership too."""
    items = region_items(registry)
    if not items:
        return None
    regions = tuple(regions)
    key, held = _cache_key(registry, graph, regions)
    hit = _cached(_SECTIONS, key, held)
    if hit is not None:
        return hit
    want = [k for k in items for r in regions
            if k == f"{REGION_PREFIX}{r}" or (k.startswith(f"{REGION_PREFIX}{r}")
                                              and k[len(REGION_PREFIX) + len(r):].isalpha())]
    s: set[str] = set()
    for k in want:
        s |= region_members(registry, graph, k)
    return _store(_SECTIONS, key, held, frozenset(s), 64)


def area_family_sections(registry, kind: str, graph=None) -> frozenset[str] | None:
    """Every section of every `area:<kind>:*` item (`Extent.area_kind` / `outside_area_kind`, the
    steelhead twin's `outside_area_kind`). The REGION family is `region_sections` of every region
    (the one membership). `None` when the family has no item."""
    if kind == "region":
        items = region_items(registry)
        return region_sections(tuple(k[len(REGION_PREFIX):] for k in items), registry, graph) \
            if items else None
    prefix = f"area:{kind}:"
    members = [it for k, it in registry.items() if str(k).startswith(prefix)]
    if not members:
        return None
    return frozenset(s for it in members for s in it.section_ids)


def section_regions(registry, graph=None) -> dict[str, frozenset[str]]:
    """`{section: regions}` — the regions (zones, where the registry holds them) each section is a
    member of, by the one membership (`region_members`: a straddler in each region it lies in)."""
    key, held = _cache_key(registry, graph)
    hit = _cached(_MEMBER, key, held)
    if hit is not None:
        return hit
    got: dict[str, set[str]] = {}
    for k in region_items(registry):
        for sec in region_members(registry, graph, k):
            got.setdefault(sec, set()).add(k[len(REGION_PREFIX):])
    return _store(_MEMBER, key, held, {sec: frozenset(r) for sec, r in got.items()}, 16)
