"""THE SHORE CUT (T660, rewritten by T665, owner 2026-11-11): a creek's line ends where it leaves the tidal water it enters.

FWA draws a creek on from its shore across a double-line river or an estuary to the receiving main flow, and some lines
lie wholly in the sea. Bound as freshwater, every rule on the creek coloured the estuary (the owner, on the T632 tidal
sheets: "collapsed to the boundary"). The DFO tidal-waters polygon (`tidal_boundary` in the build gpkg, the map's
`tidal_water`) says where the tidal water is; the owner's curated limits (`waters/tidal_limits.json`) say which rivers
are tidal and how far.

ONE PASS of the clean cutter (`clean_cut.decide_chain`, the parks' cutter: exact to the polygon edge, dips under
`clean_cut.DIP_M` ignored, a run that short at a line end taken as the end) gives each blue line its stretches inside the
polygon (`polygon_inside`). Two rules read them (`polygon_rule`):

  A. A LINE WHOLLY INSIDE THE POLYGON IS TIDAL WATER: not drawn, bound by nothing, kept in the graph so no id breaks
     and tributary walks still run on the full network. Named or not (owner ruling 2: all 206 of T660's list). The
     atlas lists them in `INSIDE` (`inside_tidal.json`); the reach gives their sections the owner `inside:<blk>`.
  B. A CREEK JOINING TIDAL WATER IS CUT ONCE, WHERE IT LEAVES THE POLYGON (owner ruling 1): its mouth is inside, its
     water followed down while inside reaches tidal water off the map (below a curated `split`/`cut`, a `whole` tidal
     water, or a rule-A line), and the cut sits at the top of the inside stretch that starts at the mouth (outside runs
     up to `BRIDGE_M` bridged: D3, the first real exit). The cut is made ONLY WHERE THE PIECE BELOW IT GOES TIDAL
     (owner 2026-11-11 on the dry run: "drop them"): every outlet tidal water off the map, a fixpoint over the creeks.
     A line whose exit is its top is rule A by this route (`EXIT_AT_TOP`). The reach still decides tidal for the piece
     below a shore cut (`reach.outside.tidal_crossing_owner`), by the same outlet test.

Never cut: a water that has its own `split`/`whole` limit (`owner_limited`: the owner's limit governs it), and the
rivers and creeks the owner has not ticked yet (`held_lines`). The owner's `mouth` rows no longer stop a cut (D4 withdrawn,
2026-11-11: "just do the 33 cuts and then remove tidal part"). `shore_cuts.json` lists every line the polygon reaches
whose mouth is inside or that joins tidal water: cut, or why not. The cut's id is `shore:<blk>:<int measure>`, its source
`shore`, its label `LABEL`.
"""
from __future__ import annotations

from typing import Mapping

from pipeline.common.models import AnchorType, NodeKind, SplitPoint, StreamGraph

#: the cut family (`SplitPoint.source`, `sectionizer.AUTO_SOURCES`) and its id prefix
SOURCE = "shore"
#: two FWA segments whose measures differ by less than this are one boundary (float slop between segment rows)
_JOIN_M = 0.01
#: the atlas sidecar: every line the rule looked at, cut or why not; its `rule` names the rule that made it
RECORD = "shore_cuts.json"
#: the rule `RECORD` carries. An atlas whose record names it also carries `INSIDE` (the reach refuses one without it)
RULE = "polygon/1"
#: the atlas sidecar listing the lines that are tidal water whole (rule A, and creeks whose exit is their top)
INSIDE = "inside_tidal.json"
#: every shore cut's label: where the creek's line leaves the tidal polygon
LABEL = "shore"
RECORD_ABOUT = ("T665: every blue line the DFO tidal polygon reaches whose mouth is inside it, or that joins tidal water. "
                "A row with `cut_m` was cut there (the polygon exit); the reach takes the piece below as tidal. A row with "
                "`not_cut` says why not. Written by the atlas build (`atlas.splits.shore`); read by people.")
INSIDE_ABOUT = ("T665: the blue lines that are tidal water along their whole length: wholly inside the DFO tidal polygon "
                "(`why: inside`), or a creek whose polygon exit is its top (`why: exit_at_top`). Written by the atlas build "
                "only (`atlas.sidecars` never changes a section, so it leaves this file as built); read by the reach run (`reach.outside.tidal_inside_owner`) and the tiles.")

#: POLICY (ticket ruling 2): every line wholly inside the polygon is tidal water. False is the mutation pin
A_LINE_WHOLLY_INSIDE_THE_POLYGON_IS_TIDAL = True
#: POLICY (ticket ruling 1): a creek joining tidal water is cut where it leaves the polygon. False is the mutation pin
A_CREEK_INTO_TIDAL_WATER_IS_CUT_AT_ITS_POLYGON_EXIT = True
#: POLICY (owner 2026-11-11, on the dry run): a cut whose piece below would stay freshwater is not made. False is the
#: mutation pin (173 such cuts in the first dry run)
A_CUT_THAT_MAKES_NOTHING_TIDAL_IS_NOT_MADE = True
#: POLICY (dry run 2026-11-11, a default the owner may still reverse): a line of a water the owner gave a `split` or
#: `whole` limit, or holding tidal water off the map already, is never cut by the polygon: its curated limit is where its
#: tidal water ends (unguarded, the rule cut the Pitt 3.4 km and the Nitinat 23.9 km above their limits). False is the pin
THE_OWNERS_LIMIT_GOVERNS_ITS_OWN_WATER = True
#: HELD FOR THE OWNER (2026-11-11): main rivers with no limit of their own that the rule would make tidal below their
#: polygon exit. Left as today (no cut) until the owner ticks each (`handoff/PLAN/app-integration/T665-main-rivers-map.html`);
#: a ticked one is removed from this list. By blue line.
HELD_FOR_OWNER: dict[str, str] = {
    "360218493": "Bella Coola River",
    "360222775": "Mamin River",
    "360229887": "Apple River",
    "360229888": "Apple River",
    "360229889": "Apple River",
    "360225716": "Kshwan River",
    "354088016": "Sarita River",
    "354088440": "Bedwell River",
}
#: HELD FOR THE OWNER, second group (2026-11-11, the owner's DFO-glossary question: "unless otherwise specified, a
#: straight line between the two most seaward points of land on either side of the mouth"): creeks whose polygon exit
#: lies more than 100 m up a channel under about 100 m wide (every point of the stretch within 50 m of the polygon edge,
#: measured down from the exit), so the polygon may reach past where the creek meets the shoreline (the Pitt case).
#: Measured by the dry run (`T665-dryrun.md`); same page, same tick. By blue line.
HELD_UP_THE_CHANNEL: dict[str, str] = {
    "354087624": "Nanaimo River (245 m up the channel)",
    "354087938": "Chemainus River (580 m up the channel)",
    "354087940": "Chemainus River (160 m up the channel)",
    "354100939": "unnamed (295 m up the channel)",
    "354118035": "unnamed (105 m up the channel)",
    "354119673": "unnamed (290 m up the channel)",
    "354145435": "Kitsucksus Creek (255 m up the channel)",
    "354147245": "Sydney River (300 m up the channel)",
    "354148021": "unnamed (670 m up the channel)",
    "354148132": "Trent River (325 m up the channel)",
    "354148274": "unnamed (260 m up the channel)",
    "354149928": "Nunns Creek (845 m up the channel)",
    "354150572": "Cayeghle Creek (480 m up the channel)",
    "354150804": "Franklin River (575 m up the channel)",
    "354150904": "Tsowwin River (570 m up the channel)",
    "354152039": "Bulson Creek (285 m up the channel)",
    "354154029": "Colonial Creek (460 m up the channel)",
    "354154428": "Tranquil Creek (300 m up the channel)",
    "354154484": "Kokish River (150 m up the channel)",
    "354154568": "Rogers Creek (1140 m up the channel)",
    "354155136": "Sarita River (465 m up the channel)",
    "356152742": "unnamed (130 m up the channel)",
    "356165068": "Tamboline Slough (165 m up the channel)",
    "356195562": "McLennan Creek (170 m up the channel)",
    "356227245": "unnamed (120 m up the channel)",
    "356232974": "unnamed (160 m up the channel)",
    "356237969": "Chester Creek (565 m up the channel)",
    "356249719": "unnamed (300 m up the channel)",
    "356286798": "Gundersen Slough (185 m up the channel)",
    "356310903": "Matsqui Slough (120 m up the channel)",
    "356321196": "unnamed (120 m up the channel)",
    "356330529": "unnamed (145 m up the channel)",
    "356339436": "Katzie Slough (125 m up the channel)",
    "356346058": "London Slough (120 m up the channel)",
    "360227039": "Kitlope River (125 m up the channel)",
    "360438268": "unnamed (205 m up the channel)",
    "360445710": "unnamed (185 m up the channel)",
    "360535529": "unnamed (250 m up the channel)",
    "360586479": "unnamed (105 m up the channel)",
    "360665787": "unnamed (105 m up the channel)",
    "360675720": "unnamed (710 m up the channel)",
    "360700529": "unnamed (110 m up the channel)",
    "360764607": "Carlisle Creek (105 m up the channel)",
    "360776500": "Oona River (900 m up the channel)",
    "360812872": "unnamed (115 m up the channel)",
    "360817979": "Scribner Creek (435 m up the channel)",
    "360832937": "unnamed (145 m up the channel)",
    "360840369": "unnamed (105 m up the channel)",
    "360852130": "Kitkiata Creek (295 m up the channel)",
    "360861076": "unnamed (215 m up the channel)",
    "360868424": "unnamed (115 m up the channel)",
    "360871265": "Pa-aat River (460 m up the channel)",
    "360880247": "unnamed (105 m up the channel)",
}
#: HELD FOR THE OWNER, up the channel, found after the dry run (next-rebuild 2a, 2026-11-11): cut only once the atlas
#: reads the reach's tidal set (the Fraser's side channels are tidal), and then more than 100 m up the channel by the
#: same measure. Two of the 33 mouth-tick creeks; the owner's dry-run tables showed both as "not cut". Held as build 27
#: has them (each keeps its T660 cut). Not yet on the owner's map page. By blue line.
HELD_UP_THE_CHANNEL_SINCE: dict[str, str] = {
    "356285962": "Cohilukthan Slough (880 m up the channel)",
    "356358883": "Deas Slough (225 m up the channel)",
}
#: HELD FOR THE OWNER, rivers and creeks the rule makes tidal that the owner's dry-run pages showed as "not cut: would
#: stay freshwater" (next-rebuild, 2026-11-11). The dry runs read each line's outlets off its lowest SECTION; on a line
#: not cut at its polygon exit that section is the whole lower river and carries every side channel leaving it, so one
#: fresh braid kept the river "fresh". Read by where each edge leaves the line (`outlet_edges`), these go tidal below
#: their polygon exit: the same case as the eight main rivers of `HELD_FOR_OWNER` (no limit of their own; 19 carry
#: the owner's `mouth` row), and 31 creeks into them follow. None has a cut in build 27 except the last two listed in
#: `HELD_KEEPS_ITS_CUT` (San Juan River, 356341234). Held as build 27 has them until the owner ticks. By blue line.
HELD_NOT_ON_THE_OWNERS_MAP: dict[str, str] = {
    "354087703": "San Juan River (would go tidal 695 m up)",
    "354087805": "Zeballos River (would go tidal 195 m up)",
    "354087807": "Zeballos River (would go tidal 474 m up)",
    "354088055": "Koksilah River (would go tidal 542 m up)",
    "354113387": "unnamed (would go tidal 12 m up)",
    "354133599": "Bonsall Creek (would go tidal 461 m up)",
    "354150843": "unnamed (would go tidal 127 m up)",
    "354154217": "Moyeha River (would go tidal 959 m up)",
    "356341234": "unnamed (would go tidal 264 m up)",
    "360215254": "Necleetsconnay River (would go tidal 1073 m up)",
    "360221844": "Mussel River (would go tidal 142 m up)",
    "360233262": "unnamed (would go tidal 172 m up)",
    "360242457": "Ksi X'anmas (would go tidal 310 m up)",
    "360856269": "Theodosia River (would go tidal 441 m up)",
    "360858825": "Kwatna River (would go tidal 3125 m up)",
    "360869552": "Nieumiamus Creek (would go tidal 120 m up)",
    "360870027": "Tom Browne Creek (would go tidal 855 m up)",
    "360875560": "Wathl Creek (would go tidal 928 m up)",
    "360877620": "Stagoo Creek (would go tidal 399 m up)",
    "360878779": "Khutze River (would go tidal 1068 m up)",
    "360882263": "Ksi X'anmas (would go tidal 1641 m up)",
    "360882453": "Brittain River (would go tidal 279 m up)",
    "360883402": "unnamed (would go tidal 1681 m up)",
    "360884170": "Mussel River (would go tidal 635 m up)",
    "360884575": "Necleetsconnay River (would go tidal 617 m up)",
    "360885253": "Bella Coola River (would go tidal 1320 m up)",
    "360885559": "Kemano River (would go tidal 813 m up)",
    "360885824": "Kitimat River (would go tidal 1251 m up)",
    "360886968": "Quaal River (would go tidal 2357 m up)",
}
#: HELD FOR THE OWNER, third group (next-rebuild 2c, 2026-11-11): the polygon exit falls where a side channel of the
#: creek's own water joins it on both sides, and the braid sweep cannot cut that channel on the cut's line ("line
#: misses"), so the build would stop (`build.cut_shores`: "a creek's own braid across its polygon exit is the owner's
#: call"). Not cut, as in build 27 (T660 skipped it: "a braid of its own water spans the shore"). By blue line.
HELD_A_BRAID_SPANS_THE_EXIT: dict[str, str] = {
    "360871314": "Kainet Creek (exit 16 m up; its side channel 360227727, itself wholly inside the polygon, joins on both sides)",
}
#: A HELD LINE STAYS EXACTLY AS BUILD 27 HAS IT (next-rebuild 2b, 2026-11-11). "Held" means no change until the owner
#: ticks; 26 of the 61 held lines carry a T660 shore cut in the live atlas (19 with a tidal piece below it), and the
#: polygon rule alone would have dropped those cuts and turned the 19 pieces back into freshwater (30 of 93 with the lines
#: next-rebuild added to the held groups). So each keeps its
#: cut where build 27 made it (the measure on the live graph's bound, same id `shore:<blk>:<int>`); the reach decides
#: tidal for the piece below as before. The other 35 held lines have no cut in build 27 and get none. A line leaves this
#: list with its tick (when it leaves the held lists). By blue line -> route measure.
HELD_KEEPS_ITS_CUT: dict[str, float] = {
    "354100939": 588.329692937512,
    "354145435": 174.134994065693,
    "354148021": 26.559677430487998,
    "354149928": 221.586454569785,
    "354154029": 25.4602836371018,
    "354154568": 62.8087872440429,
    "356152742": 124.86376793447201,
    "356165068": 678.448731074214,
    "356195562": 155.426769954466,
    "356227245": 227.945563434588,
    "356232974": 213.651559241231,
    "356237969": 228.875985561824,
    "356249719": 429.25548505503804,
    "356286798": 220.538577358928,
    "356310903": 320.30438,
    "356321196": 427.59326988837,
    "356330529": 362.06649872493205,
    "356339436": 295.53272215029506,
    "356346058": 574.990923530852,
    "360445710": 16.6025031034152,
    "360586479": 21.9787294285151,
    "360665787": 298.11859735134107,
    "360675720": 223.240657555566,
    "360764607": 3202.05711923718,
    "360812872": 37.326930338529294,
    "360861076": 15.6845316882484,
    "356285962": 27.4830560951735,
    "356358883": 2907.4250267973803,
    "354087703": 37.0329937742508,
    "356341234": 193.23053610635202,
}
#: owner D3: an outside run of at most this many metres between two inside stretches does not end the creek's tidal
#: stretch (the first REAL exit); a longer one does, and what lies inside higher up stays freshwater
BRIDGE_M = 25.0
#: the not-cut reasons (each lands in the record: none is silent)
EXIT_AT_TOP = "the exit is the line's top: the whole line is tidal"
LAKE_BEFORE_EXIT = "a lake on the line before the exit (lakes are never cut)"
MOUTH_OUTSIDE = "mouth outside the polygon (D6)"
FRESH_RECEIVER = "joins fresh water (D1: no shore cut)"
FRESH_BELOW = "the piece below the cut would stay freshwater (owner 2026-11-11: not cut)"
OWNER_LIMIT = "the owner's split/whole limit for this water governs it"
HELD = "held for the owner's tick (shore.held_lines: HELD_FOR_OWNER and the other HELD_ lists)"
#: on a row that is cut all the same: a held line keeps the cut build 27 made (`HELD_KEEPS_ITS_CUT`)
HELD_KEPT = "held for the owner's tick: keeps its build 27 cut (HELD_KEEPS_ITS_CUT)"
_EPS = _JOIN_M


def held_lines() -> dict[str, str]:
    """Every blue line held for the owner, all the groups together: {blk: why}."""
    return {**HELD_FOR_OWNER, **HELD_UP_THE_CHANNEL, **HELD_UP_THE_CHANNEL_SINCE, **HELD_A_BRAID_SPANS_THE_EXIT,
            **HELD_NOT_ON_THE_OWNERS_MAP}


def read_tidal_polygon(gpkg: str, bbox=None):
    """The DFO tidal-waters polygon (`tidal_boundary`, EPSG:3005), one geometry. A gpkg without it is refused (AGENTS
    37): a build that cut no shore would look healthy."""
    import pyogrio
    import shapely
    df = pyogrio.read_dataframe(gpkg, layer="tidal_boundary", bbox=bbox)
    geoms = [g for g in df.geometry if g is not None and not g.is_empty]
    if not geoms and bbox is None:
        raise SystemExit(f"shore: {gpkg} layer tidal_boundary holds no polygon")
    return shapely.union_all(geoms) if geoms else None


def _iv_contains(ivs, lo: float, hi: float) -> bool:
    return any(a <= lo + _EPS and b >= hi - _EPS for a, b in ivs)


def _iv_at(ivs, m: float) -> bool:
    return any(a <= m + _EPS and m < b for a, b in ivs)


def bridged(ivs, gap_m: float = BRIDGE_M) -> list[tuple[float, float]]:
    """Inside intervals (sorted, absolute) joined across every outside run of at most `gap_m`."""
    out: list[list[float]] = []
    for a, b in sorted(ivs):
        if out and a - out[-1][1] <= gap_m:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


def polygon_chunks(poly):
    """The polygon's edge in short runs (`border.outline_chunks`) behind an STRtree: an exact crossing test against a
    601,582-vertex coast is then local to each line."""
    from shapely.strtree import STRtree

    from pipeline.atlas.splits.border import outline_chunks
    chunks = outline_chunks(poly)
    return chunks, STRtree(chunks)


def chain_inside(chain, poly, chunks=None) -> list[tuple[float, float]]:
    """One blue line's stretches inside `poly`: `clean_cut.decide_chain` with dips (and outside runs) under
    `clean_cut.DIP_M` ignored. Absolute route measures; ±inf where a stretch reaches the line's own mouth or source, the
    middle of the lake run where it reaches a lake (`clean_cut.stretches`)."""
    from shapely.geometry import MultiLineString

    from pipeline.atlas.splits import clean_cut as _cc
    chunks = chunks or polygon_chunks(poly)
    parts, tree = chunks
    boundary = MultiLineString([parts[i] for i in sorted(tree.query(chain.geometry, predicate="intersects"))])
    _cuts, inside, _dips = _cc.decide_chain(chain, poly, boundary, 0.0, rejoin_m=_cc.DIP_M)
    return sorted(inside)


def polygon_inside(chains, poly) -> dict[str, list[tuple[float, float]]]:
    """`{blk: inside intervals}` for every chain meeting `poly` (`chain_inside`); a chain the polygon does not reach is
    absent. Chains are any objects with `blk`, `geometry` (mouth first), `mouth_measure`, `waterbody_runs`
    (`BlkChain`, or `graph_lines` for a built atlas). Deterministic: by blk."""
    import math

    import numpy as np
    import shapely
    shapely.prepare(poly)
    chunks = polygon_chunks(poly)
    keep = [c for c in chains if c.geometry is not None and not c.geometry.is_empty]
    if not keep:
        return {}
    arr = np.array([c.geometry for c in keep], dtype=object)
    meets = np.zeros(len(keep), dtype=bool)
    meets[np.unique(chunks[1].query(arr, predicate="intersects")[0])] = True
    first = shapely.get_point(arr, 0)
    meets |= shapely.contains_xy(poly, shapely.get_x(first), shapely.get_y(first))
    out: dict[str, list] = {}
    for i in sorted(np.flatnonzero(meets), key=lambda i: (keep[i].blk, keep[i].mouth_measure)):
        c = keep[i]
        ivs = chain_inside(c, poly, chunks)
        lo_end, hi_end = getattr(c, "lo_end", None), getattr(c, "hi_end", None)
        ivs = [(lo_end if (a == -math.inf and lo_end is not None) else a,
                hi_end if (b == math.inf and hi_end is not None) else b) for a, b in ivs]
        if ivs:
            out.setdefault(c.blk, []).extend(ivs)
    return {b: sorted(v) for b, v in out.items()}


def graph_lines(graph: StreamGraph, geoms: Mapping[str, object]) -> list:
    """A BUILT atlas's blue lines as chains for `polygon_inside` (the sidecar path and the dry run: the build itself
    has `BlkChain`s). The graph cuts a line at its lake runs, so each run of touching stream pieces is one chain; where
    it meets a lake its inside interval ends at the middle of the lake run (`lo_end` / `hi_end`), as
    `clean_cut.stretches` ends a `BlkChain`'s."""
    from types import SimpleNamespace

    import shapely
    from shapely.geometry import LineString
    by_blk: dict[str, list] = {}
    for nid in sorted(graph.nodes):
        n = graph.nodes[nid]
        if n.kind == NodeKind.stream and n.blk:
            by_blk.setdefault(n.blk, []).append(n)
    out = []
    for blk in sorted(by_blk):
        ns = sorted(by_blk[blk], key=lambda n: (n.down_m, n.up_m))
        groups: list[list] = []
        for n in ns:
            if groups and abs(n.down_m - groups[-1][-1].up_m) <= _JOIN_M:
                groups[-1].append(n)
            else:
                groups.append([n])
        for gi, grp in enumerate(groups):
            xy: list = []
            for n in grp:
                g = geoms.get(n.node_id)
                if g is None or g.is_empty:
                    continue
                c = list(shapely.force_2d(g).coords)
                xy.extend(c[1:] if xy and c and c[0] == xy[-1] else c)
            if len(xy) < 2:
                continue
            lo_end = None if gi == 0 else (groups[gi - 1][-1].up_m + grp[0].down_m) / 2.0
            hi_end = None if gi == len(groups) - 1 else (grp[-1].up_m + groups[gi + 1][0].down_m) / 2.0
            out.append(SimpleNamespace(blk=blk, geometry=LineString(xy), mouth_measure=grp[0].down_m,
                                       waterbody_runs=(), lo_end=lo_end, hi_end=hi_end))
    return out


def standing_boundaries(graph: StreamGraph, skip_prefix: str = f"split:{SOURCE}:") -> dict[str, list[float]]:
    """`{blk: measures}` of the boundaries already standing inside each blue line (curated cuts, areas, lengths, lake
    edges), for `clean_cut.onto_existing`. A boundary that is only a shore cut (`skip_prefix`) is not standing: the
    polygon rule replaces those."""
    out: dict[str, set] = {}
    for n in graph.nodes.values():
        if n.kind != NodeKind.stream or not n.blk:
            continue
        for b in (n.lower_bound, n.upper_bound):
            if b is None or b.route_measure is None:
                continue
            ids = [str(x) for x in (b.boundary_id, *b.aliases)]
            if ids and all(x.startswith(skip_prefix) for x in ids):
                continue
            if b.boundary_id in ("outlet", "headwaters"):
                continue
            out.setdefault(n.blk, set()).add(float(b.route_measure))
    return {k: sorted(v) for k, v in out.items()}


def atlas_tidal(graph: StreamGraph, limits, id_of: Mapping[str, str]) -> frozenset[str]:
    """THE TIDAL WATER OFF THE MAP AS THE ATLAS SEES IT, before the registry exists: for each `split`/`cut` row the
    stream pieces of its line below the cut, and every piece of the SAME WATER (same gnis; the line alone when it has
    none) connected to them, up or down, without passing the line above its limit (side channels, distributaries); for each
    `whole` row every piece of the item (`gnis:` / a lake `wbk:`). The reach's own set (`reach.outside.tidal_cut_owner`,
    `tidal_whole_owner`) is resolved on the registry and is the answer, and the build cuts by it (`reach_tidal`,
    `THE_ATLAS_CUTS_BY_THE_REACHS_TIDAL_SET`); this graph-only reading is kept as the mutation pin and for a caller with
    no registry. A split the graph does not carry is refused."""
    by_blk: dict[str, list] = {}
    by_gnis: dict[str, list] = {}
    for nid in sorted(graph.nodes):
        n = graph.nodes[nid]
        if n.kind == NodeKind.stream and n.blk:
            by_blk.setdefault(n.blk, []).append(n)
            if n.gnis_id:
                by_gnis.setdefault(str(n.gnis_id), []).append(n)
    out: set[str] = set()
    for r in sorted(limits, key=lambda r: (r.blk, r.limit)):
        if r.limit == "whole":
            kind, _, val = str(r.item or "").partition(":")
            if kind == "gnis":
                out |= {n.node_id for n in by_gnis.get(val, ())}
            elif kind == "wbk" and f"lake:{val}" in graph.nodes:
                out.add(f"lake:{val}")
            continue
        if r.limit not in ("split", "cut"):
            continue
        sid = f"split:{r.split if r.limit == 'split' else id_of[r.blk]}"
        ns = by_blk.get(r.blk) or []
        ms = [b.route_measure for n in ns for b in (n.lower_bound, n.upper_bound)
              if b is not None and b.route_measure is not None and sid in (b.boundary_id, *b.aliases)]
        if not ms:
            raise SystemExit(f"shore: tidal limit {sid} on {r.blk} ({r.name}) is not a boundary of its line in the graph")
        m = min(ms)
        gnis = next((str(n.gnis_id) for n in ns if n.gnis_id), "")
        water = {n.node_id for n in (by_gnis.get(gnis, ()) if gnis else ns)}
        above = {n.node_id for n in ns if n.up_m > m + _EPS}
        got = {n.node_id for n in ns if n.up_m <= m + _EPS}
        # THE WATER'S OTHER LINES CONNECTED TO IT BELOW THE CUT (side channels, distributaries to the sea): the
        # same-water pieces reached from the piece below without passing the line above its limit
        todo = sorted(got)
        while todo:
            d = todo.pop()
            nbrs = [graph.edges[i].to_node for i in graph.down_adj.get(d, ())] + \
                   [graph.edges[i].from_node for i in graph.up_adj.get(d, ())]
            for t in nbrs:
                if t in water and t not in got and t not in above:
                    got.add(t)
                    todo.append(t)
        out |= got
    return frozenset(out)


#: THE ATLAS CUTS BY THE REACH'S OWN TIDAL SET (next-rebuild 2a, 2026-11-11). `atlas_tidal` reads the limits off the graph
#: and misses the side channels with no gnis that the registry folds into their river (70 sections against the reach
#: run's 124 on build 27: 3 creeks cut for nothing, 7 not cut). The build makes the registry's waters once before the
#: shore cuts (`build.water_registry`) and asks the reach's own functions (`reach_tidal`); after the final registry it
#: asks again and stops if the two differ (`build.build_registry`). False is the mutation pin (the graph's own reading).
THE_ATLAS_CUTS_BY_THE_REACHS_TIDAL_SET = True


def reach_tidal(registry, graph: StreamGraph, limits, id_of: Mapping[str, str], *, strict: bool = True) -> frozenset[str]:
    """THE TIDAL WATER OFF THE MAP AS THE REACH RUN WILL SEE IT: the piece below every `split` / `cut` limit
    (`reach.outside.tidal_cut_owner`, the resolver a rule's extent uses) and every section of each `whole` water
    (`tidal_whole_owner`), on `registry`. The same two functions the reach run calls, so there is no second reading.
    `strict` (a full build): a limit no item carries, or a whole water with no section, is refused."""
    from pipeline.atlas.reach import outside as _outside
    # the ids as the tidal sidecar names them (`tidal.resolved_rows`), which is what the reach run reads
    cut_ids = sorted({str(r.split) for r in limits if r.limit == "split"} | {str(id_of[r.blk]) for r in limits if r.limit == "cut"})
    whole = sorted({str(r.item) for r in limits if r.limit == "whole"})
    out = set(_outside.tidal_cut_owner(registry, graph, cut_ids, strict=strict))
    out |= set(_outside.tidal_whole_owner(registry, whole, strict=strict))
    return frozenset(out)


def owner_limited(graph: StreamGraph, tidal, limit_rows, waters=()) -> frozenset:
    """The blks `polygon_rule` never cuts because the owner's own limit governs them: every line of a WATER (same gnis,
    or one registry water: `waters`, each the set of blks of one stream item) one of whose lines carries a `split` or
    `whole` row (`limit_rows`: `blk`, `limit`, as dicts or `tidal.Limit`) or holds a section of `tidal`. The registry
    waters are what joins an added line with no gnis to its water (T685: Port Moody's Turner Creek line is Noons Creek,
    which the owner's railway-bridge boundary governs; by gnis alone the coast rule cut it a second time)."""
    def val(r, k):
        return r.get(k) if isinstance(r, Mapping) else getattr(r, k, None)
    gnis_of: dict[str, str] = {}
    for n in graph.nodes.values():
        if n.kind == NodeKind.stream and n.blk and n.gnis_id:
            gnis_of.setdefault(n.blk, str(n.gnis_id))
    by_gnis: dict[str, set] = {}
    for b, g in gnis_of.items():
        by_gnis.setdefault(g, set()).add(b)
    lim = {str(val(r, "blk")) for r in limit_rows if val(r, "blk") and val(r, "limit") in ("split", "whole")}
    lim |= {graph.nodes[s].blk for s in tidal if s in graph.nodes and graph.nodes[s].blk}
    out = set(lim)
    for b in lim:
        if b in gnis_of:
            out |= by_gnis[gnis_of[b]]
    for w in waters:
        if not lim.isdisjoint(w):
            out |= set(w)
    return frozenset(out)


def polygon_rule(graph: StreamGraph, inside: Mapping[str, list], tidal, existing: Mapping[str, list] | None = None,
                 limited=frozenset(), held: Mapping[str, str] | None = None,
                 kept: Mapping[str, float] | None = None, leaves_at=None
                 ) -> tuple[list[SplitPoint], list[dict], list[dict]]:
    """THE T665 RULE over `inside` (`polygon_inside`). `tidal`: the section ids of tidal water OFF THE MAP under the
    curated limits (`atlas_tidal`). `existing`: `standing_boundaries`. `limited`: `owner_limited`. `held`: the blks held
    for the owner (default `held_lines()`: every held group). `kept`: the held blks that keep a cut, and where
    (default: the `HELD_KEEPS_ITS_CUT` rows still held, when `held` is the default; else none); a kept blk given here
    that is not held is refused. `leaves_at`: `edge_leave_measures` (the build passes it): where on its own line each
    edge leaves, so the outlets of the piece below a cut are judged on a line that is not cut yet (`outlet_edges`).

    Returns (cut points, whole lines, rows). Whole lines are rule A (`why: inside`) and the creeks whose exit is their
    top that end in tidal water (`why: exit_at_top`). Rows: one per line whose mouth is inside the polygon, or that meets
    it and joins tidal water — cut (`cut_m`) or why not (`not_cut`). All sorted by blk."""
    from pipeline.atlas.splits import clean_cut as _cc
    existing = existing or {}
    default_kept = kept is None and held is None
    held = held_lines() if held is None else held
    if default_kept:
        kept = {b: m for b, m in HELD_KEEPS_ITS_CUT.items() if b in held}     # a ticked line left the held lists
    kept = kept or {}
    loose = sorted(set(kept) - set(held))
    if loose:
        raise SystemExit(f"shore: {loose} keep a cut but are not held")
    tidal = frozenset(tidal)
    lines: dict[str, list] = {}
    for nid in sorted(graph.nodes):
        n = graph.nodes[nid]
        if n.kind == NodeKind.stream and n.blk:
            lines.setdefault(n.blk, []).append(n)
    for v in lines.values():
        v.sort(key=lambda n: (n.down_m, n.up_m))

    def name_of(ns):
        return next((n.display_name for n in ns if n.display_name), "")

    # RULE A: every stream piece of the line inside (a line already wholly tidal water is not counted again)
    rule_a: dict[str, dict] = {}
    if A_LINE_WHOLLY_INSIDE_THE_POLYGON_IS_TIDAL:
        for blk in sorted(inside):
            ns = lines.get(blk) or []
            if not ns or not all(_iv_contains(inside[blk], n.down_m, n.up_m) for n in ns):
                continue
            if all(n.node_id in tidal for n in ns):
                continue
            rule_a[blk] = {"blk": blk, "name": name_of(ns), "why": "inside",
                           "length_m": round(sum(n.length_m for n in ns), 1)}

    def first_stretch(blk):
        ns = lines[blk]
        ivs = bridged(inside.get(blk) or ())
        if not ivs or not _iv_at(ivs, ns[0].down_m):
            return None
        return ivs[0][1], len(ivs) > 1

    stretch = {blk: first_stretch(blk) for blk in sorted(inside) if blk in lines}

    def reaches_tidal(start) -> str | None:
        """The candidate filter: down the network from the outlets while still inside the polygon, to tidal water."""
        seen, todo = set(), sorted(start)
        while todo:
            d = todo.pop()
            if d in seen:
                continue
            seen.add(d)
            if d in tidal:
                return "curated"
            n = graph.nodes.get(d)
            if n is None:
                continue
            if n.blk and n.blk in rule_a:
                return "inside line"
            if n.kind != NodeKind.stream or not n.blk or not _iv_at(inside.get(n.blk) or (), n.down_m):
                continue
            todo.extend(sorted(graph.edges[i].to_node for i in graph.down_adj.get(d, ())))
        return None

    def outlets(n):
        return sorted(graph.edges[i].to_node for i in graph.down_adj.get(n.node_id, ()))

    def outlet_edges(blk, at) -> list[int]:
        """THE OUTLETS OF THE PIECE BELOW A CUT AT `at`, on a line that is NOT CUT YET (the build): the edges that
        leave the line below `at`. Before the cut the whole line may be one section carrying every edge that leaves
        it anywhere (a distributary 2 km up, the line's own loop), and reading that section's edges as the mouth's
        refused the cut ("would stay freshwater": McDonald Slough, Wahoo Creek, 6 lines of the first side build).
        With no geometry (`leaves_at` None) it is the lowest section's edges, as on a graph already cut."""
        ns = lines[blk]
        if leaves_at is None:
            return sorted(graph.down_adj.get(ns[0].node_id, ()))
        out = []
        for n in ns:
            if n.down_m >= at - _EPS:
                break
            for i in sorted(graph.down_adj.get(n.node_id, ())):
                t = graph.nodes.get(graph.edges[i].to_node)
                if t is not None and t.blk == blk:
                    continue                                   # the line's own next piece, or its own loop
                m = leaves_at(i)
                if m is not None and m >= at - _EPS:
                    continue                                   # leaves the line above the cut
                out.append(i)
        return out

    rows: dict[str, dict] = {}
    cand: dict[str, float] = {}            # blk -> cut measure (inf: the whole line, its exit is its top)
    kept_here = {b: float(m) for b, m in kept.items() if b in lines}
    for blk, at in sorted(kept_here.items()):
        ns = lines[blk]
        on_line = ns[0].down_m + _EPS < at < ns[-1].up_m - _EPS and any(n.down_m - _EPS <= at <= n.up_m + _EPS for n in ns)
        if blk in rule_a or not on_line:
            raise SystemExit(f"shore: held line {blk} cannot keep its cut at {at}: the line is wholly inside the polygon, "
                             "or the measure is not inside the line (HELD_KEEPS_ITS_CUT is build 27's; re-read it)")
    for blk in sorted(lines):
        ns = lines[blk]
        mouth = ns[0]
        if blk in kept_here:
            # a held line as build 27 has it: its cut where it was, whatever the polygon says of the line
            st = stretch.get(blk)
            rows[blk] = {"blk": blk, "name": name_of(ns), "section": mouth.node_id, "mouth_m": round(mouth.down_m, 2),
                         "top_m": round(ns[-1].up_m, 2), "outlets": outlets(mouth),
                         "joins": reaches_tidal(outlets(mouth)) if outlets(mouth) else None,
                         **({"exit_m": round(min(st[0], ns[-1].up_m), 2)} if st else {}), "held": HELD_KEPT}
            cand[blk] = kept_here[blk]
            continue
        if blk in rule_a or blk not in inside:
            continue
        outs = outlets(mouth)
        st = stretch.get(blk)
        via = reaches_tidal(outs) if outs else None
        top = ns[-1].up_m
        row = {"blk": blk, "name": name_of(ns), "section": mouth.node_id, "mouth_m": round(mouth.down_m, 2),
               "top_m": round(top, 2), "outlets": outs, "joins": via}
        if st is None:
            if via is not None:
                rows[blk] = {**row, "not_cut": MOUTH_OUTSIDE}
            continue
        exit_m, reenters = st
        lake_below = any(b.down_m - a.up_m > _EPS for a, b in zip(ns, ns[1:]) if b.down_m < exit_m)
        row.update(exit_m=round(min(exit_m, top), 2), inside_m=round(min(exit_m, top) - mouth.down_m, 1),
                   reenters=reenters, **({"lake_below_exit": True} if lake_below else {}))
        if via is None:
            rows[blk] = {**row, "not_cut": FRESH_RECEIVER}
            continue
        why = ""
        if THE_OWNERS_LIMIT_GOVERNS_ITS_OWN_WATER and blk in limited:
            why = OWNER_LIMIT
        elif blk in held:
            why = HELD
        elif not A_CREEK_INTO_TIDAL_WATER_IS_CUT_AT_ITS_POLYGON_EXIT:
            why = "the polygon rule is off"
        elif exit_m >= top - _cc.DIP_M:
            cand[blk] = float("inf")
        elif not any(n.down_m - _EPS <= exit_m <= n.up_m + _EPS for n in ns):
            why = LAKE_BEFORE_EXIT
        else:
            at = _cc.onto_existing(exit_m, existing.get(blk, ()), _cc.DIP_M)
            cand[blk] = at
            if at != exit_m:
                row["onto_existing"] = True
        rows[blk] = {**row, **({"not_cut": why} if why else {})}

    # WHAT GOES TIDAL: a candidate whose every outlet is tidal water off the map (curated, a rule-A line, or the piece
    # below another such candidate) — the least fixpoint, so two braids feeding each other with no tidal outlet stay
    # fresh. The reach answers the same question (`tidal_crossing_owner`) on the pieces the cuts make.
    ok: set[str] = set()

    def tidal_like(i) -> bool:
        """Does edge `i` end in tidal water off the map? Curated tidal water, a rule-A line, or the piece BELOW
        another candidate's cut: judged by where the edge joins that line (`at_measure`, on the receiver's blue
        line), since before the cuts the receiver's lowest section is still its whole line (a creek joining Gundersen
        Slough 1 km above its cut was read as entering its tidal piece: 10 cuts of the first side build)."""
        e = graph.edges[i]
        d = e.to_node
        if d in tidal:
            return True
        n = graph.nodes.get(d)
        if n is None or not n.blk:
            return False
        if n.blk in rule_a:
            return True
        if n.blk not in ok:
            return False
        m = getattr(e, "at_measure", None)
        if m is None or m != m:
            m = n.down_m
        return max(m, n.down_m) < cand[n.blk] - _EPS
    grew = True
    while grew:
        grew = False
        for blk in sorted(cand):
            if blk in ok:
                continue
            o = outlet_edges(blk, cand[blk])
            if o and all(tidal_like(i) for i in o):
                ok.add(blk)
                grew = True

    pts: list[SplitPoint] = []
    whole = dict(rule_a)
    for blk in sorted(cand):
        r = rows[blk]
        r["below_cut_tidal"] = blk in ok
        if blk not in ok and A_CUT_THAT_MAKES_NOTHING_TIDAL_IS_NOT_MADE and blk not in kept_here:
            r["not_cut"] = FRESH_BELOW
            continue
        if cand[blk] == float("inf"):
            r["not_cut"] = EXIT_AT_TOP
            if blk in ok:
                whole[blk] = {"blk": blk, "name": r["name"], "why": "exit_at_top",
                              "length_m": round(sum(n.length_m for n in lines[blk]), 1)}
            continue
        at = cand[blk]
        r["cut_m"] = round(at, 2)
        pts.append(SplitPoint(split_id=f"{SOURCE}:{blk}:{int(at)}", blk=blk, route_measure=at, fid="", label=LABEL,
                              anchor_type=AnchorType.point, source=SOURCE))
    return pts, [whole[b] for b in sorted(whole)], [rows[b] for b in sorted(rows)]


def edge_leave_measures(graph: StreamGraph, geoms, view=None):
    """`leaves_at` for `polygon_rule`: a function edge index -> the route measure, on the edge's FROM line, where it
    leaves that line (`braid_sweep.touch_measure`: the receiving line's point at the confluence, projected onto the
    from line), or None when it cannot be placed. `view`: a `braid_sweep.View` of this graph to reuse."""
    from pipeline.atlas.splits import braid_sweep as _bs
    view = view or _bs.View(graph, geoms)

    def leaves_at(i: int):
        e = graph.edges[i]
        f = graph.nodes.get(e.from_node)
        if f is None or f.kind != NodeKind.stream or not f.blk or e.to_node not in graph.nodes:
            return None
        m = _bs.touch_measure(view, e.to_node, i, e.from_node, f.blk)
        return None if m is None or m != m else float(m)
    return leaves_at


def write_inside(out_dir, whole: list[dict]) -> None:
    """Write `INSIDE` into an atlas directory (the build and `atlas.sidecars`): the lines that are tidal water whole."""
    import json
    from pathlib import Path
    (Path(out_dir) / INSIDE).write_text(json.dumps({"_about": INSIDE_ABOUT, "rule": RULE, "lines": list(whole)},
                                                   indent=1) + "\n", encoding="utf-8")


def read_inside(build_dir) -> tuple[str, ...]:
    """The blks an atlas lists as tidal water whole (`INSIDE`), sorted. An atlas whose `RECORD` names `RULE` must carry
    the file (refused otherwise: a missing file is not "no line inside", AGENTS 37); an atlas cut by the T660 rule
    (no `rule` in its record) has no such lines by construction, and says so by its record."""
    import json
    from pathlib import Path
    d = Path(build_dir)
    rec = d / RECORD
    rule = json.loads(rec.read_text(encoding="utf-8")).get("rule") if rec.exists() else None
    p = d / INSIDE
    if rule != RULE:
        if p.exists():
            raise SystemExit(f"shore: {p} exists but {rec} was not written by the polygon rule ({RULE})")
        return ()
    if not p.exists():
        raise SystemExit(f"shore: {d} was cut by the polygon rule but has no {INSIDE} — rebuild the atlas (only the "
                         "build writes it, with the cuts it belongs to; `pipeline.atlas.sidecars` does not)")
    return tuple(sorted(str(r["blk"]) for r in json.loads(p.read_text(encoding="utf-8"))["lines"]))
