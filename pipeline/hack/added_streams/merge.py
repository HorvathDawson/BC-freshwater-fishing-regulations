"""Merge input stream features into CHANNELS (the added-stream analogue of an FWA blue line).

OSM (and hand-drawn) data splits one stream into many ways. FWA merges linear features into one
blue line per BLK; we do the same: features that are connected AND share a name become one channel
with ONE minted BLK and a single stitched geometry. A tributary meeting a mainstem has a DIFFERENT
name, so it stays a separate channel (that join is handled as an edge in ``ingest``, not a merge).

Pure + source-neutral: consumes GeoJSON Feature dicts (lon/lat LineStrings) and is shared by
``fetch_osm`` (pre-grouping the candidate dump) and ``ingest`` (enforcing it regardless of source).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from pyproj import Transformer
from shapely.geometry import LineString, MultiLineString
from shapely.ops import linemerge

_ENDPOINT_DP = 7        # lon/lat rounding for endpoint identity (OSM shares exact nodes)
_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)

# property keys carried through per channel: minting overrides + municipal metadata (clean.py schema)
_OVERRIDE_KEYS = ("wsc", "stream_order", "stream_magnitude", "gnis_id", "edge_type",
                  "connect_to_hint", "ftype", "fish", "species", "trib_parent", "src_id")


@dataclass
class Channel:
    """One merged stream (= one blue line). ``geometry`` is a stitched lon/lat LineString whose
    orientation is not yet meaningful — ``ingest`` orients it mouth-first once the receiver is known."""
    blk: int                                   # minted negative id, shared by all member ways
    name: str
    source: str                                # "osm" | "manual"
    geometry: LineString                       # EPSG:4326 (lon/lat)
    connect_to: Optional[dict] = None
    overrides: dict = field(default_factory=dict)
    members: tuple = ()                        # osm_way_ids (or feature indices) for provenance


def _endpoints(coords: list) -> tuple[tuple, tuple]:
    a, b = coords[0], coords[-1]
    return (round(a[0], _ENDPOINT_DP), round(a[1], _ENDPOINT_DP)), \
           (round(b[0], _ENDPOINT_DP), round(b[1], _ENDPOINT_DP))


class _UF:
    def __init__(self, n: int):
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        self.p[self.find(a)] = self.find(b)


def _norm_name(props: dict) -> str:
    return str(props.get("name") or "").strip()


def _blk_of(props: dict) -> Optional[int]:
    b = props.get("blk")
    return int(b) if b not in (None, "") else None


def _way_id(props: dict) -> Optional[int]:
    w = props.get("osm_way_id")
    return int(w) if w not in (None, "") else None


_MINT_BASE = 2_000_000_000        # municipal channels with no id get blk = -(base + index)


def merge_channels(features: list[dict], mint_missing_blk: bool = False,
                   gap_tol_m: float = 0.0, group_by_name: bool = False) -> list[Channel]:
    """Group GeoJSON Feature dicts into ``Channel``s.

    Two features are unioned when they share an explicit ``blk`` (author asserts one channel), OR
    when they TOUCH at an endpoint AND carry the same non-empty ``name`` (the FWA-style merge). With
    ``gap_tol_m`` > 0, same-named features whose endpoints are within that many metres also merge —
    so a creek chopped into a few sections with small gaps becomes ONE channel (one blk, one
    connector) instead of each section connecting separately. With ``group_by_name`` (the municipal
    path) EVERY feature sharing a name joins one creek group regardless of connectivity — the group is
    then split into a mainstem + braids by geometry (see ``_mainstem_runs``). Each group's BLK is its
    shared explicit ``blk`` if any, else ``-min(osm_way_id)``; a group with neither raises unless
    ``mint_missing_blk`` (the municipal path), where such groups get a deterministic negative blk by
    stable group order."""
    feats = [f for f in features if (f.get("geometry") or {}).get("type") == "LineString"
             and len(f["geometry"].get("coordinates", [])) >= 2]
    n = len(feats)
    uf = _UF(n)

    # union by shared explicit blk
    by_blk: dict[int, list[int]] = {}
    for i, f in enumerate(feats):
        b = _blk_of(f.get("properties", {}))
        if b is not None:
            by_blk.setdefault(b, []).append(i)
    for members in by_blk.values():
        for j in members[1:]:
            uf.union(members[0], j)

    # union EVERY feature sharing a name into one creek group (municipal: names a whole drainage)
    if group_by_name:
        by_name: dict[str, list[int]] = {}
        for i, f in enumerate(feats):
            nm = _norm_name(f.get("properties", {}))
            if nm:
                by_name.setdefault(nm, []).append(i)
        for members in by_name.values():
            for j in members[1:]:
                uf.union(members[0], j)

    # union by shared endpoint + same name
    endpoint_users: dict[tuple, list[int]] = {}
    for i, f in enumerate(feats):
        for ep in _endpoints(f["geometry"]["coordinates"]):
            endpoint_users.setdefault(ep, []).append(i)
    for users in endpoint_users.values():
        for a in range(len(users)):
            for b in range(a + 1, len(users)):
                ia, ib = users[a], users[b]
                na = _norm_name(feats[ia].get("properties", {}))
                nb = _norm_name(feats[ib].get("properties", {}))
                if na and na == nb:
                    uf.union(ia, ib)

    # near-adjacent same-name merge: a creek split into sections with small GAPS -> one channel
    if gap_tol_m > 0:
        all_eps = [(_TO_ALBERS.transform(*f["geometry"]["coordinates"][0]),
                    _TO_ALBERS.transform(*f["geometry"]["coordinates"][-1])) for f in feats]

        def _d(p, q):
            return ((p[0] - q[0]) ** 2 + (p[1] - q[1]) ** 2) ** 0.5

        def _bridged(pa, pb, ia, ib):
            """A CONNECTOR feature already spans this gap (an end on pa AND an end on pb)? Then the gap-merge
            is redundant — it would stitch the far section on with a straight-line jump (Dallas Creek 298->299
            is bridged by connector 420). Leave the sections separate; the connector links them."""
            for j, (ja, jb) in enumerate(all_eps):
                if j == ia or j == ib:
                    continue
                if min(_d(pa, ja), _d(pa, jb)) <= gap_tol_m and min(_d(pb, ja), _d(pb, jb)) <= gap_tol_m:
                    return True
            return False

        by_name: dict[str, list[int]] = {}
        for i, f in enumerate(feats):
            nm = _norm_name(f.get("properties", {}))
            if nm:                                          # never proximity-merge unnamed features
                by_name.setdefault(nm, []).append(i)
        for members in by_name.values():
            for a in range(len(members)):
                for b in range(a + 1, len(members)):
                    ia, ib = members[a], members[b]
                    pa, pb = min(((x, y) for x in all_eps[ia] for y in all_eps[ib]), key=lambda t: _d(*t))
                    if _d(pa, pb) <= gap_tol_m and not _bridged(pa, pb, ia, ib):
                        uf.union(ia, ib)

    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(uf.find(i), []).append(i)

    channels: list[Channel] = []
    needs_mint: list[Channel] = []
    for members in groups.values():
        blks = {_blk_of(feats[i].get("properties", {})) for i in members}
        blks.discard(None)
        if len(blks) > 1:
            raise ValueError(f"merge_channels: features grouped as one channel have conflicting "
                             f"blk values {sorted(blks)}")
        # An explicit blk asserts ONE blue line -> stitch every member. Otherwise a same-name group may
        # be a branching NETWORK (municipal layers name a whole drainage the same), so decompose it into
        # its non-branching runs at the junctions — never chain a network into one zig-zagging line.
        if blks:
            runs = [(_stitch([LineString(feats[i]["geometry"]["coordinates"]) for i in members]),
                     list(members))]
        else:
            runs = _decompose(list(members), feats, gap_tol_m)
        for geom, run in runs:
            props = [feats[i].get("properties", {}) for i in run]
            way_ids = [w for w in (_way_id(p) for p in props) if w is not None]
            blk: Optional[int]
            if blks:
                blk = next(iter(blks))
            elif way_ids:
                blk = -min(way_ids)                 # deterministic, negative, non-colliding
            elif mint_missing_blk:
                blk = None                          # assigned below by stable order
            else:
                raise ValueError("merge_channels: a channel has neither an explicit negative 'blk' "
                                 "nor an 'osm_way_id' to mint one from")
            name = next((_norm_name(p) for p in props if _norm_name(p)), "")
            source = next((str(p["source"]) for p in props if p.get("source")),
                          "osm" if way_ids else "manual")
            connect_to = next((p["connect_to"] for p in props if p.get("connect_to")), None)
            overrides: dict = {}
            for k in _OVERRIDE_KEYS:
                for p in props:
                    if p.get(k) not in (None, ""):
                        overrides[k] = p[k]
                        break
            src_ids = tuple(p.get("src_id") for p in props if p.get("src_id") is not None)
            ch = Channel(blk=blk if blk is not None else 0, name=name, source=source, geometry=geom,
                         connect_to=connect_to, overrides=overrides,
                         members=tuple(way_ids) or src_ids or tuple(run))
            channels.append(ch)
            if blk is None:
                needs_mint.append(ch)
    for i, ch in enumerate(sorted(needs_mint,                     # stable order -> stable blks
                                  key=lambda c: (c.source, c.name, str(c.members[:1])))):
        ch.blk = -(_MINT_BASE + i)
    channels.sort(key=lambda c: c.blk)
    return channels


def _decompose(idxs: list[int], feats: list[dict],
               bridge_tol: float) -> list[tuple[LineString, list[int]]]:
    """Split a same-name group into its non-branching runs. ``linemerge`` breaks the group at every
    junction (a node where 3+ members meet), so a branching municipal drainage becomes several clean
    runs instead of one zig-zag line stitched across the whole network. Runs whose ends are within
    ``bridge_tol`` metres (a creek merely chopped into sections) are re-joined; each original member is
    assigned to the single run that carries it, so blk/props stay with the right run."""
    lines = [LineString(feats[i]["geometry"]["coordinates"]) for i in idxs]
    if len(lines) == 1:
        return [(lines[0], list(idxs))]
    merged = linemerge(MultiLineString(lines))
    parts = [merged] if merged.geom_type == "LineString" else \
        list(merged.geoms) if merged.geom_type == "MultiLineString" else list(lines)
    if bridge_tol > 0 and len(parts) > 1:
        parts = _bridge(parts, bridge_tol)
    assign: dict[int, list[int]] = {}
    for k, ln in enumerate(lines):
        mp = ln.interpolate(0.5, normalized=True)
        j = min(range(len(parts)), key=lambda j: parts[j].distance(mp))
        assign.setdefault(j, []).append(idxs[k])
    return [(parts[j], assign[j]) for j in sorted(assign)]


def _mainstem_runs(idxs: list[int], feats: list[dict],
                   bridge_tol: float) -> list[tuple[LineString, list[int]]]:
    """Decompose a same-name group into runs, then re-merge the longest through-path into ONE mainstem
    channel (one blk) and leave the rest as separate braids/forks (each its own blk). ``linemerge``
    splits a network at every junction, which would otherwise fragment a single creek into many blks
    with silly nested WSCs; the mainstem is the longest simple path through the run-graph."""
    runs = _decompose(idxs, feats, bridge_tol)
    if len(runs) <= 1 or len(runs) > _MAINSTEM_MAX_RUNS:
        return runs
    node = lambda c: (round(c[0], _ENDPOINT_DP), round(c[1], _ENDPOINT_DP))
    adj: dict[tuple, list[tuple[int, tuple]]] = {}
    for e, (g, _) in enumerate(runs):
        a, b = node(g.coords[0]), node(g.coords[-1])
        adj.setdefault(a, []).append((e, b))
        adj.setdefault(b, []).append((e, a))
    best: list[int] = []
    best_len = [-1.0]
    budget = [200_000]
    def dfs(n, used: set, path: list, length: float):
        if budget[0] <= 0:
            return
        budget[0] -= 1
        if length > best_len[0]:
            best_len[0] = length; best[:] = path
        for e, nxt in adj.get(n, ()):
            if e in used:
                continue
            used.add(e); path.append(e)
            dfs(nxt, used, path, length + runs[e][0].length)
            path.pop(); used.discard(e)
    for start in list(adj):
        dfs(start, set(), [], 0.0)
    if not best:
        return runs
    main = set(best)
    main_geoms = [runs[e][0] for e in best]
    main_geom = main_geoms[0] if len(main_geoms) == 1 else \
        _greedy_chain([list(g.coords) for g in main_geoms])
    out = [(main_geom, [m for e in best for m in runs[e][1]])]
    out += [runs[e] for e in range(len(runs)) if e not in main]     # braids/forks stay separate
    return out


_MAINSTEM_MAX_RUNS = 24         # above this the longest-path DFS is skipped (keep raw runs)


def _bridge(parts: list[LineString], tol_m: float) -> list[LineString]:
    """Chain parts separated by a genuine small GAP (a creek chopped into sections) into one run; parts
    farther than ``tol_m`` stay separate, and parts that TOUCH (gap ~0) are left alone — those are the
    branches ``linemerge`` split at a junction, and re-joining them would zig-zag the network again.

    A gap is bridged ONLY when it is a clean 1-1 continuation: the two endpoints being joined see each
    other and NOTHING else within ``tol_m``. Where 3+ endpoints converge (a Y-junction with small
    digitizing gaps, not an exact shared node), bridging would force ``_greedy_chain`` to linearise a
    fork and stitch a long straight jump across it (the Little Stawamus 4.2 km 'uphill' edge). Such
    junction endpoints have degree >= 2, so they are refused and the arms stay as separate runs."""
    ep = [(_TO_ALBERS.transform(*p.coords[0]), _TO_ALBERS.transform(*p.coords[-1])) for p in parts]
    endpts = [(i, xy) for i, ab in enumerate(ep) for xy in ab]        # 2 per part: [2i]=start,[2i+1]=end
    def d(p, q):
        return ((p[0] - q[0]) ** 2 + (p[1] - q[1]) ** 2) ** 0.5
    near = [[gj for gj, (pj, xyj) in enumerate(endpts) if pj != pi and d(xy, xyj) <= tol_m]
            for gi, (pi, xy) in enumerate(endpts)]                    # neighbouring endpoints (other parts)
    uf = _UF(len(parts))
    for gi, (pi, xy) in enumerate(endpts):
        for gj in near[gi]:
            gap = d(xy, endpts[gj][1])
            if 0.5 < gap <= tol_m and near[gi] == [gj] and near[gj] == [gi]:
                uf.union(pi, endpts[gj][0])                          # a clean 1-1 continuation only
    clusters: dict[int, list[int]] = {}
    for i in range(len(parts)):
        clusters.setdefault(uf.find(i), []).append(i)
    out = []
    for cluster in clusters.values():
        if len(cluster) == 1:
            out.append(parts[cluster[0]])
        else:
            out.append(_greedy_chain([list(parts[i].coords) for i in cluster]))
    return out


def _stitch(lines: list[LineString]) -> LineString:
    """Stitch a channel's member ways into one LineString (orientation resolved later in ingest).
    Touching members merge exactly; gapped members (a creek split into sections) are chained by
    nearest endpoint, bridging each gap with a straight segment, so no section is dropped."""
    if len(lines) == 1:
        return lines[0]
    merged = linemerge(MultiLineString(lines))
    if isinstance(merged, LineString):
        return merged
    parts = list(merged.geoms) if isinstance(merged, MultiLineString) else list(lines)
    return _greedy_chain([list(p.coords) for p in parts])


def _greedy_chain(segs: list[list]) -> LineString:
    """Order coordinate-runs into one path by nearest endpoint and concatenate (bridging gaps)."""
    def d(p, q):
        return ((p[0] - q[0]) ** 2 + (p[1] - q[1]) ** 2) ** 0.5

    chain = segs.pop(0)
    while segs:
        head, tail = chain[0], chain[-1]
        best = None                                          # (dist, idx, attach_end, flip)
        for i, s in enumerate(segs):
            for end, anchor in (("tail", tail), ("head", head)):
                for flip, pt in ((False, s[0]), (True, s[-1])):
                    dd = d(pt, anchor)
                    if best is None or dd < best[0]:
                        best = (dd, i, end, flip)
        _, i, end, flip = best
        s = segs.pop(i)
        if flip:
            s = s[::-1]
        if end == "tail":
            chain = chain + (s[1:] if chain[-1] == s[0] else s)
        else:
            chain = (s if chain[0] != s[-1] else s[:-1]) + chain
    return LineString(chain)
