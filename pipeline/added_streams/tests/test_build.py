"""build_dataset.resolve_and_mint + to_graph_inputs — classification, receiver resolution, WSC
propagation, under-lake wbk, and graph tie-in. Hermetic: synthetic FWA chains + lake/tidal geometry."""

from shapely.geometry import LineString, Polygon

from pipeline.added_streams.build_dataset import resolve_and_mint, to_graph_inputs, _is_name_tributary
from pipeline.added_streams.fwa_match import line_to_albers
from pipeline.added_streams.underlake import LakeIndex
from pipeline.graph.blk_chains import build_blk_chains
from pipeline.graph.graph import ancestors, build_stream_graph
from pipeline.models import BlkChain


def _chain(blk, wsc, coords, gnis_name="", gnis_id="", order=3, mag=5, mouth_measure=0.0):
    g = line_to_albers(coords)
    return BlkChain(blk=blk, fwa_watershed_code=wsc, fids=(), geometry=g, mouth_measure=mouth_measure,
                    length_m=g.length, name_tuples=(), gnis_id=gnis_id, gnis_name=gnis_name,
                    stream_order=order, stream_magnitude=mag)


def _feat(coords, name, **props):
    p = {"source": "burnaby", "src_id": name, "name": name, "connect_to_hint": "",
         "ftype": "stream", "fish": "", "species": "", "trib_parent": "", **props}
    return {"type": "Feature", "properties": p,
            "geometry": {"type": "LineString", "coordinates": coords}}


# FWA: a Fraser (100) mainstem + a far coastal (900) stream for tidal estimation
_BIG = _chain("1000", "100-100000", [(-123.0, 49.20), (-123.0, 49.24)], gnis_name="Big River", gnis_id="111")
_COAST = _chain("3000", "900-500000", [(-123.2, 49.30), (-123.2, 49.31)])
_FWA = [_BIG, _COAST]
_TIDAL = [line_to_albers([(-123.1004, 49.29), (-123.1004, 49.31)])]  # coastline (3005) near Coast Creek

_FEATS = [
    _feat([(-123.0, 49.205), (-123.0, 49.235)], "Big Creek"),          # duplicate of Big River (drop)
    _feat([(-123.0, 49.22), (-123.010, 49.22)], "Trib A"),             # novel -> joins Big River (FWA)
    _feat([(-123.005, 49.22), (-123.005, 49.226)], "Fork B"),          # novel -> joins Trib A (added)
    _feat([(-123.1, 49.30), (-123.105, 49.305)], "Coast Creek"),       # novel -> tidal (900)
]


def _mint():
    return resolve_and_mint(_FEATS, _FWA, LakeIndex([]), _TIDAL)


def test_classes_counts_and_drops_duplicate():
    streams, cands, report = _mint()
    assert report["counts"]["duplicate"] == 1
    assert report["counts"]["novel"] == 3 and report["minted"] == 3
    assert not report["unresolved"]


def test_wsc_propagation_inherits_primary():
    streams, _, _ = _mint()
    by = {s["name"]: s for s in streams}
    assert by["Trib A"]["wsc"].startswith("100-100000")               # inherits Fraser 100
    assert by["Fork B"]["wsc"].startswith(by["Trib A"]["wsc"])        # descends its added receiver
    assert by["Coast Creek"]["wsc"].startswith("900")                 # tidal root
    assert by["Coast Creek"]["receiver_kind"] == "tidal"


def test_wsc_on_partially_loaded_river_uses_true_blue_line_length():
    """Sanctuary Slough regression. Our FWA extract is regional, so a big river's blue line is only
    PARTIALLY loaded — its chain starts well up from the true mouth (mouth_measure > 0) and length_m
    understates the real length. Minting a tributary as proj/length_m then over-counts (Sanctuary got
    100-567200 where its real FWA neighbours sit near 100-0123xx). The fix recovers the true total from
    the receiver's already-coded FWA children (each child's local code = route-measure / TRUE length),
    so a novel mints the same small proportion its real neighbours do."""
    from pyproj import Transformer
    to_ll = Transformer.from_crs("EPSG:3005", "EPSG:4326", always_xy=True)
    # receiver: a big river whose loaded chain starts 9000 m up from its true (unloaded) mouth
    recv = _chain("1000", "100", [(-123.0, 49.20), (-123.0, 49.34)], gnis_name="Big River",
                  mouth_measure=9000.0)
    g = recv.geometry                                             # albers; ~13 km long

    def _touch_lonlat(proj_m):                                   # lon/lat of the point proj_m up recv
        p = g.interpolate(proj_m)
        return to_ll.transform(p.x, p.y)

    # two REAL FWA children touching recv at route measures 10000 and 20000 -> they imply total = 1e6 m
    def _child(blk, seg, route_m):
        lon, lat = _touch_lonlat(route_m - recv.mouth_measure)
        return _chain(blk, f"100-{seg:06d}", [(lon, lat), (lon + 0.002, lat + 0.002)])
    cA = _child("1100", 10000, 10000.0)                          # 100-010000
    cB = _child("1200", 20000, 20000.0)                          # 100-020000
    # a novel whose mouth touches recv midway between the children (route measure 15000)
    lon, lat = _touch_lonlat(6000.0)                             # 9000 + 6000 = route 15000
    novel = _feat([(lon, lat), (lon + 0.002, lat)], "Novel Slough")
    streams, _, _ = resolve_and_mint([novel], [recv, cA, cB], LakeIndex([]), [])
    s = next(x for x in streams if x["name"] == "Novel Slough")
    assert s["receiver_kind"] == "fwa" and s["receiver_blk"] == "1000"
    seg = int(s["wsc"].split("-")[-1])
    # route 15000 / true total 1e6 -> ~015000 (between its neighbours); NOT the naive proj/length_m (~045xxx)
    assert abs(seg - 15000) <= 20, f"expected ~100-015000 from the true blue-line length, got {s['wsc']}"


def test_channel_mouth_endpoint_scoped_to_own_reaches():
    """Stoney Creek reversal. dem_mouth picks which END of a merged channel is its mouth by nearest reach
    mouth. Scanning ALL reach mouths lets a FOREIGN creek whose mouth touches Stoney's HEADWATER flip the
    channel (Stoney got minted mouth-at-73.2 m + a big connector). Scoping to the channel's OWN reach mouths
    pins the mouth to the true low end; only a channel with none of its own reaches falls back to global."""
    from shapely.geometry import Point
    from pipeline.added_streams.build_dataset import _mouth_end
    low, high = Point(0.0, 0.0), Point(100.0, 0.0)
    own = [Point(2.0, 0.0)]                              # this channel's OWN mouth reach, near the LOW end
    foreign_at_high = lambda p: 0.0 if p.distance(high) < 1e-9 else p.distance(Point(2.0, 0.0))
    assert _mouth_end(low, high, own, foreign_at_high).equals(low), "own reaches pin the mouth to the low end"
    assert _mouth_end(low, high, [], foreign_at_high).equals(high), "no own reaches -> global fallback (old)"


def test_mouth_end_breaks_own_mouth_tie_by_elevation():
    """port_moody Dallas -280: a merged channel whose flow exits and re-enters through an external connector
    has an own reach-mouth at BOTH endpoints, so the distance test ties. The DOWNHILL end is the true mouth;
    picking the uphill end reverses the channel and routes its receiver into the cycle (-> unresolved)."""
    from shapely.geometry import Point
    from pipeline.added_streams.build_dataset import _mouth_end
    e0, e1 = Point(0.0, 0.0), Point(100.0, 0.0)         # e0 is UPHILL, e1 is DOWNHILL (the mouth)
    own = [e0, e1]                                       # both endpoints are own reach-mouths -> a tie
    elev = lambda p: 25.4 if p.equals(e0) else 22.5
    assert _mouth_end(e0, e1, own, lambda p: 1e9, elev=elev).equals(e1), "downhill end wins the tie"
    # only ONE end is an own-mouth -> not a tie -> distance decides, elevation ignored
    assert _mouth_end(e0, e1, [Point(2.0, 0.0)], lambda p: 1e9, elev=elev).equals(e0)


def test_prune_short_leaf_tributaries_cascades():
    """Short-tributary filter (Buena Vista Trib.3, 48 m). A stream that flows into ANOTHER added stream, is
    short, and has nothing (kept) flowing into it is dropped — iterated, so a short stream left with only
    pruned inflows becomes a leaf and goes too. Long tribs, streams with a surviving inflow, and non-added
    receivers are kept."""
    from types import SimpleNamespace
    from shapely.geometry import LineString
    from pipeline.added_streams.build_dataset import _prune_short_leaf_tribs
    def L(n):
        return LineString([(0.0, 0.0), (0.0, n)])
    minted = [SimpleNamespace(blk=b) for b in (-1, -2, -3, -4, -5, -6, -7)]
    receiver = {-1: ("fwa", "X"),        # mainstem -> FWA (never pruned even if short)
                -2: ("added", "-1"),     # long trib of main -> kept
                -3: ("added", "-1"),     # short leaf trib of main -> PRUNED
                -4: ("added", "-1"),     # short trib of main but has an inflow -> kept
                -5: ("added", "-4"),     # long inflow of -4 -> kept
                -6: ("added", "-1"),     # short trib, only inflow is short leaf -7 -> PRUNED (cascade)
                -7: ("added", "-6")}     # short leaf inflow of -6 -> PRUNED first
    geom = {-1: L(1000), -2: L(200), -3: L(30), -4: L(20), -5: L(300), -6: L(25), -7: L(15)}
    kept = {ch.blk for ch in _prune_short_leaf_tribs(minted, receiver, geom, max_len=50.0)}
    assert kept == {-1, -2, -4, -5}, f"kept={kept}"


def test_clip_receiver_overshoot_walks_mouth_back_to_crossing():
    """A municipal line drawn a few m PAST the river it drains into crosses the receiver then dangles beyond
    it, so the connector doubles BACK (the Brunette screenshot). Trim the mouth-side overshoot so the mouth
    lands ON the crossing. A real gap (no crossing), a mouth already touching, and an overshoot beyond the
    tol are all left unchanged."""
    from shapely.geometry import LineString, Point
    from pipeline.added_streams.build_dataset import _clip_receiver_overshoot, _OVERSHOOT_TOL
    river = LineString([(-100.0, 0.0), (100.0, 0.0)])          # E-W receiver through y=0
    # mouth-first line coming from the north, crossing the river and overshooting 10 m to the south
    over = LineString([(0.0, -10.0), (0.0, 50.0)])             # coords[0] = mouth at y=-10 (past the river)
    clipped = _clip_receiver_overshoot(over, river)
    assert clipped.distance(river) < 1e-6 and Point(clipped.coords[0]).distance(river) < 1e-6, "mouth on river"
    assert abs(clipped.length - 50.0) < 1e-6, "10 m overshoot trimmed"
    # a genuine gap (ends 10 m short of the river, never crosses) -> unchanged (a real connector)
    gap = LineString([(0.0, 10.0), (0.0, 60.0)])
    assert _clip_receiver_overshoot(gap, river) is gap
    # mouth already on the river -> unchanged
    onit = LineString([(0.0, 0.0), (0.0, 50.0)])
    assert _clip_receiver_overshoot(onit, river) is onit
    # overshoot longer than the tol -> left alone (never eat a real reach)
    big = LineString([(0.0, -(_OVERSHOOT_TOL + 20.0)), (0.0, 50.0)])
    assert _clip_receiver_overshoot(big, river) is big


def test_apply_receiver_overrides_forces_receiver_by_name():
    """Magnolia Trib 1 regression. When the DEM mis-bridges a piece's mouth to a neighbour, a curation
    override points it at the NEAREST channel of the named creek it should join."""
    from types import SimpleNamespace
    from shapely.geometry import LineString
    from pipeline.added_streams.build_dataset import _apply_receiver_overrides
    geom = {-1: LineString([(0, 0), (0, 10)]),        # the trib (mouth at 0,0)
            -2: LineString([(0, 0), (10, 0)]),        # Magnolia Creek (touches the trib's mouth)
            -3: LineString([(100, 0), (110, 0)])}     # Little Stawamus (far)
    ch = [SimpleNamespace(blk=-1, name="Magnolia Creek Trib 1", members=("594",)),
          SimpleNamespace(blk=-2, name="Magnolia Creek", members=("593",)),
          SimpleNamespace(blk=-3, name="Little Stawamus Creek", members=("635",))]
    receiver = {-1: ("added", "-3"), -2: ("added", "-3"), -3: ("tidal", "")}   # trib wrongly -> Little Stawamus
    _apply_receiver_overrides(ch, receiver, geom, {"594": "Magnolia Creek"})
    assert receiver[-1] == ("added", "-2"), "the trib is forced onto Magnolia Creek"
    _apply_receiver_overrides(ch, receiver, geom, {"999": "Magnolia Creek"})   # absent src_id -> no-op
    assert receiver[-1] == ("added", "-2")


def test_merge_by_name_does_not_fold_different_named_creeks():
    """port_moody Goulet/Hatchley regression. Two DIFFERENT named creeks connected THROUGH an unnamed
    connector must NOT fold into one blk — the upstream creek would become a non-trunk member and its whole
    path would be pruned (99% of Goulet Creek vanished). A named creek folds only along its OWN name; an
    unnamed connector continues the creek it belongs to, it does not bridge two creeks into one."""
    from types import SimpleNamespace
    from shapely.geometry import LineString
    from pipeline.added_streams.build_dataset import _merge_by_name
    geom = {-1: LineString([(0, 0), (0, 100)]),       # Alpha — downstream (rep, exits to tidal)
            -2: LineString([(0, 100), (0, 150)]),     # unnamed connector: -2 -> Alpha
            -3: LineString([(0, 150), (0, 250)])}     # Beta — upstream: -3 -> connector -> Alpha
    names = {-1: "Alpha Creek", -2: "", -3: "Beta Creek"}
    minted = [SimpleNamespace(blk=b, name=names[b], members=(b,)) for b in geom]
    ch_all = {b: SimpleNamespace(blk=b, name=names[b]) for b in geom}
    receiver = {-1: ("tidal", ""), -2: ("added", "-1"), -3: ("added", "-2")}
    keep, _, _, _ = _merge_by_name(minted, receiver, geom, ch_all)
    kept = {c.name for c in keep}
    assert "Alpha Creek" in kept and "Beta Creek" in kept, f"both creeks kept as separate streams; got {kept}"


def test_merge_by_name_drops_unnamed_side_branch_from_extras():
    """Dallas Creek regression. When a fragmented named creek folds into one blk, a same-name BRAID rides
    along as an extra segment. But an UNNAMED side-branch that folds in (off the longest trunk, via the
    unnamed-fragment rule) must NOT ride along — else it is drawn as a disconnected segment of the creek
    (the 'flows upstream then a straight line back' artifact). Only same-name braids are kept as extras."""
    from types import SimpleNamespace
    from shapely.geometry import LineString
    from pipeline.added_streams.build_dataset import _merge_by_name
    geom = {-1: LineString([(0, 0), (0, 100)]),        # R (Creek) — the rep (mouth at 0,0)
            -2: LineString([(0, 100), (0, 200)]),      # A (Creek) — long up-end continuation -> the trunk
            -3: LineString([(0, 100), (5, 150)]),      # Bd (Creek) — short same-name braid -> extra KEPT
            -4: LineString([(0, 100), (-5, 150)])}     # U (unnamed) — side-branch -> extra DROPPED
    names = {-1: "Creek", -2: "Creek", -3: "Creek", -4: ""}
    minted = [SimpleNamespace(blk=b, name=names[b], members=(b,)) for b in geom]
    ch_all = {b: SimpleNamespace(blk=b, name=names[b]) for b in geom}
    receiver = {-1: ("tidal", ""), -2: ("added", "-1"), -3: ("added", "-1"), -4: ("added", "-1")}
    keep, _, _, extra = _merge_by_name(minted, receiver, geom, ch_all)
    assert len(keep) == 1, "the same-name pieces merge into one Creek stream"
    rep = keep[0].blk
    assert len(extra.get(rep, [])) == 1, "same-name braid kept as an extra; the unnamed side-branch is dropped"


def test_export_carries_fwa_exclude_and_name_variants():
    """The build export must tell the consumer what to remove from FWA and how names survive: a municipal
    DUPLICATE of an FWA blue line becomes a name variant (alias) of that FWA blk."""
    _, _, report = _mint()
    assert report["fwa_exclude"] == []                            # nothing excluded in this fixture
    dup = [v for v in report["name_variants"] if v["kind"] == "duplicate"]
    assert any(v["name"] == "Big Creek" and v["target_blk"] == "1000" for v in dup), \
        "Big Creek (a duplicate of Big River / blk 1000) is exported as a name variant of that blk"


def test_excluded_fwa_name_becomes_a_variant_of_the_superseding_stream():
    """When an FWA reach is excluded, its own gnis name is kept as a variant of the municipal stream that
    superseded it — so the name is not lost with the FWA line. Coast Creek reaches tidal on its own, so it
    still mints once the overlapping FWA is excluded, and inherits that FWA's name."""
    from pipeline.added_streams.tests.test_build import _chain
    coast_fwa = _chain("5000", "100-500000", [(-123.1, 49.30), (-123.105, 49.305)],
                       gnis_name="Old Coast River", gnis_id="9")
    streams, _, report = resolve_and_mint(_FEATS, _FWA + [coast_fwa], LakeIndex([]), _TIDAL,
                                          exclude_wsc=("100-500000",))
    assert report["fwa_exclude"] == ["100-500000"]
    ev = [v for v in report["name_variants"] if v["kind"] == "excluded_fwa"]
    assert any(v["name"] == "Old Coast River" for v in ev), "excluded FWA name kept as a variant"


def test_nonlinear_blk_geometry_is_detected():
    """A blk whose geometry self-crosses is a loop the graph must never ingest — resolve_and_mint raises,
    and the detector flags it."""
    from pipeline.added_streams.build_dataset import _nonlinear_blks
    straight = {"blk": "-1", "segments": [{"coords3005": [(0, 0), (0, 1), (0, 2)], "wbk": ""}]}
    bowtie = {"blk": "-2", "segments": [{"coords3005": [(0, 0), (1, 1), (1, 0), (0, 1)], "wbk": ""}]}
    assert _nonlinear_blks([straight]) == []
    assert _nonlinear_blks([bowtie]) == ["-2"]


def test_name_conflict_candidate_emitted():
    _, cands, report = _mint()
    conflicts = [c for c in cands if c.conflict]
    assert any(c.name == "Big Creek" and c.target_blk == "1000" for c in conflicts)
    assert report["name_conflicts"]


def test_negative_blks_are_unique():
    streams, _, _ = _mint()
    blks = [s["blk"] for s in streams]
    assert all(b.startswith("-") for b in blks) and len(set(blks)) == len(blks)


class _StubSampler:
    """Elevation that decreases toward a given lon/lat point (that point is 'downhill')."""
    def __init__(self, low):
        self.low = low
    def elevation(self, lon, lat):
        return (lon - self.low[0]) ** 2 + (lat - self.low[1]) ** 2


def test_resolver_flow_direction_follows_dem_sampler():
    """With an injected sampler, the resolver takes flow DIRECTION from dem_flow: a creek touching an
    FWA outlet at EACH end is oriented (mouth, hence receiver) at the dem-downstream end. Flipping which
    end dem calls downhill flips which FWA it drains into — proving dem drives flow, not the geometry."""
    east = _chain("1000", "100-100000", [(-123.0, 49.20), (-123.0, 49.24)], gnis_name="East River")
    west = _chain("2000", "100-200000", [(-123.02, 49.20), (-123.02, 49.24)], gnis_name="West River")
    creek = _feat([(-123.02, 49.22), (-123.0, 49.22)], "Dem Creek")   # touches West River (W) and East River (E)
    e, _, _ = resolve_and_mint([creek], [east, west], LakeIndex([]), [],
                               orient_sampler=_StubSampler((-123.0, 49.22)))     # east end downhill
    assert e and e[0]["receiver_blk"] == "1000", "dem mouth at the east outlet -> drains into East River"
    w, _, _ = resolve_and_mint([creek], [east, west], LakeIndex([]), [],
                               orient_sampler=_StubSampler((-123.02, 49.22)))    # west end downhill
    assert w and w[0]["receiver_blk"] == "2000", "flip the downhill end -> drains into West River"


def test_dem_stranded_mouth_connects_to_fwa_within_dem_outlet_reach():
    """A novel that is its component's sink resolves via dem's OWN outlet connector: dem attaches a
    stranded sink to the nearest FWA/tidal within its reach (~800 m) and hands the resolver that outlet
    point, so the sink roots on the FWA. A creek beyond dem's reach has no outlet and stays unresolved."""
    river = _chain("1000", "100-100000", [(-123.0, 49.20), (-123.0, 49.24)], gnis_name="River")
    e_near = -123.0 - 300 / 72000.0      # mouth 300 m west of the FWA (within dem's outlet reach)
    near = _feat([(e_near, 49.22), (e_near - 200 / 72000.0, 49.22)], "Near Creek")
    r, _, _ = resolve_and_mint([near], [river], LakeIndex([]), [],
                               orient_sampler=_StubSampler((e_near, 49.22)))   # mouth (east) downhill
    assert r and r[0]["receiver_kind"] == "fwa", "a 300 m stranded sink connects via dem's outlet"
    e_far = -123.0 - 1000 / 72000.0      # mouth 1000 m from the FWA (beyond dem's outlet reach)
    far = _feat([(e_far, 49.22), (e_far - 200 / 72000.0, 49.22)], "Far Creek")
    r2, _, rep2 = resolve_and_mint([far], [river], LakeIndex([]), [],
                                   orient_sampler=_StubSampler((e_far, 49.22)))
    assert not r2 and rep2["unresolved"], "a mouth 1000 m from any outlet stays unresolved"


def test_lake_inflows_resolve_through_approved_lake():
    """Deer Lake regression: streams draining INTO an approved lake resolve — the lake is a node whose
    outflow reaches an FWA river, so inflows chain through it (inflow -> outflow -> river). The lake's
    under-lake FWA reach is excluded, so the ONLY way inflows reach the network is the lake node.
    Without the lake approved, the inflows strand as unresolved."""
    from pyproj import Transformer
    from shapely.geometry import Polygon
    ta = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)
    river = _chain("1000", "100-100000", [(-123.0, 49.20), (-123.0, 49.24)], gnis_name="Brunette")
    lake = Polygon([ta.transform(x, y) for x, y in                       # a lake box west of the river
                    [(-123.025, 49.215), (-123.015, 49.215), (-123.015, 49.225), (-123.025, 49.225)]])
    outflow = _feat([(-123.016, 49.22), (-123.001, 49.22)], "Lake Brook")     # lake -> river (flows east)
    in_a = _feat([(-123.05, 49.223), (-123.024, 49.223)], "North Inflow")     # -> lake (east end at lake)
    in_b = _feat([(-123.05, 49.217), (-123.024, 49.217)], "South Inflow")     # -> lake
    low = (-123.0, 49.22)                                                # east (the river) is downhill
    streams, _, rep = resolve_and_mint([outflow, in_a, in_b], [river], LakeIndex([]), [],
                                       approved_lakes=[lake], orient_sampler=_StubSampler(low))
    assert {s["name"] for s in streams} == {"Lake Brook", "North Inflow", "South Inflow"}, rep["unresolved"]
    assert not rep["unresolved"]
    assert next(s for s in streams if s["name"] == "Lake Brook")["receiver_kind"] == "fwa"
    s2, _, rep2 = resolve_and_mint([outflow, in_a, in_b], [river], LakeIndex([]), [],
                                   orient_sampler=_StubSampler(low))          # lake NOT approved
    assert rep2["unresolved"], "inflows need the approved lake node to resolve"


def test_tributary_connected_in_dem_resolves_into_its_novel_mainstem():
    """Little Stawamus regression: a tributary that flows INTO a novel mainstem and is connected to it in
    the dem graph (via a small confluence gap, not a shared vertex) must resolve to that mainstem — not
    strand as unresolved. The resolver mirrors the dem tree, so the trib's receiver is the mainstem blk."""
    main = _feat(_dense(-123.0, 49.22, -0.008, 0.0), "Main Creek")           # mouth at Big River (FWA, east)
    trib = _feat(_dense(-123.006, 49.22003, 0.0, 0.003), "Side Creek")       # joins Main's UPSTREAM half, ~3 m off
    streams, _, rep = resolve_and_mint([main, trib], _FWA, LakeIndex([]), _TIDAL,
                                       orient_sampler=_StubSampler((-123.0, 49.22)))   # Big River downhill
    assert not rep["unresolved"], rep["unresolved"]
    by = {s["name"]: s for s in streams}
    assert by["Side Creek"]["receiver_kind"] == "added"
    assert by["Side Creek"]["receiver_blk"] == by["Main Creek"]["blk"], "trib drains into its mainstem"


def test_to_graph_inputs_makes_tributaries():
    streams, _, _ = _mint()
    # rebuild fwa fids for Big River so the added fids can merge/attach
    from pipeline.graph.blk_chains import FidRow
    from pipeline.graph import cutting
    dn, up = cutting.blk_endpoints(_BIG.geometry)
    big_fid = FidRow(fid="F1", blk="1000", wsc="100-100000", edge_type="1000", wbk="",
                     gnis_id="111", gnis_name="Big River", stream_order=3, stream_magnitude=5,
                     down_m=0.0, up_m=_BIG.geometry.length, geometry=_BIG.geometry, down_node=dn, up_node=up)
    add_fids, specs = to_graph_inputs(streams)
    all_fids = [big_fid] + add_fids
    chains = build_blk_chains(all_fids, {})
    graph = build_stream_graph(chains, all_fids, {}, {})
    from pipeline.added_streams.ingest import attach_connectors
    attach_connectors(graph, {}, specs)
    big_node = [nid for nid, n in graph.nodes.items() if str(n.blk) == "1000"][0]
    anc = ancestors(graph, big_node)
    trib_a = [s["blk"] for s in streams if s["name"] == "Trib A"][0]
    fork_b = [s["blk"] for s in streams if s["name"] == "Fork B"][0]
    assert f"{trib_a}:0" in anc and f"{fork_b}:0" in anc            # both are tributaries of Big River


def _max_jump_m(coords):
    pts = [line_to_albers([c, c]).coords[0] for c in coords]         # lon/lat -> 3005 per vertex
    return max((((pts[i][0] - pts[i + 1][0]) ** 2 + (pts[i][1] - pts[i + 1][1]) ** 2) ** 0.5
                for i in range(len(pts) - 1)), default=0.0)


def test_same_name_branch_is_a_segment_not_a_straight_trunk_jump():
    # A same-name creek with a branch that joins the trunk MID-line (not at its up-end). Folding it into
    # one blk/wsc must NOT concatenate the branch onto the trunk's far end — that fabricates a long
    # straight 'uphill' edge (the Squamish artifact). The branch rides as a separate same-blk segment.
    # Arms are densely sampled (like real data) so a fabricated fork-jump dwarfs legit vertex spacing.
    def dense(lon0, lat0, dlon, dlat, n=40):
        return [(lon0 + dlon * i / n, lat0 + dlat * i / n) for i in range(n + 1)]
    feats = [
        _feat(dense(-123.0, 49.213, -0.010, 0.0), "Y Creek"),          # trunk: mouth at Big -> up (west)
        _feat(dense(-123.005, 49.2131, 0.0, 0.004), "Y Creek"),        # branch off mid-trunk, going north
    ]
    streams, _, report = resolve_and_mint(feats, _FWA, LakeIndex([]), _TIDAL)
    y = [s for s in streams if s["name"] == "Y Creek"]
    assert len({s["wsc"] for s in y}) == 1                          # same name -> ONE wsc (a braid shares it)
    assert len({s["blk"] for s in y}) == 2                          # ...but the branch keeps its OWN blk
    for d in report["diagnostics"]:
        if d["name"] == "Y Creek" and d["klass"] != "connector":
            assert _max_jump_m(d["coords"]) <= 20.0, \
                f"Y Creek drawn with a {_max_jump_m(d['coords']):.0f} m straight jump"


def _dense(lon0, lat0, dlon, dlat, n=30):
    return [(lon0 + dlon * i / n, lat0 + dlat * i / n) for i in range(n + 1)]


def test_immediate_tributary_is_exactly_one_level_deeper():
    # A tributary that flows straight into the mainstem must sit ONE wsc level below it — not several
    # (the bug was a trib minting off a deep braid code that later collapsed to the mainstem's).
    feats = [
        _feat(_dense(-123.0, 49.22, -0.010, 0.0), "Depth Creek"),          # mouth at Big River (FWA)
        _feat(_dense(-123.005, 49.2201, 0.0, 0.003), "Depth Creek Trib 1"),  # joins the mainstem, goes up
    ]
    streams, _, _ = resolve_and_mint(feats, _FWA, LakeIndex([]), _TIDAL)
    main = next(s for s in streams if s["name"] == "Depth Creek")
    trib = next(s for s in streams if s["name"] == "Depth Creek Trib 1")
    assert trib["wsc"].startswith(main["wsc"])                      # descends the mainstem
    assert trib["wsc"].count("-") == main["wsc"].count("-") + 1     # ...by EXACTLY one level


def test_mainstem_never_flows_into_its_own_tributary():
    # A same-name braid must not root THROUGH the stream's own tributary. Here a second "Fork Creek"
    # piece only touches "Fork Creek Trib 1" (which touches the mainstem) — topology would route the
    # braid -> Trib 1 -> mainstem; the name hierarchy must bypass the trib so no mainstem flows into it.
    feats = [
        _feat(_dense(-123.0, 49.22, -0.006, 0.0), "Fork Creek"),           # mainstem, mouth at Big River
        _feat(_dense(-123.006, 49.2201, 0.0, 0.003), "Fork Creek Trib 1"),  # trib off the mainstem up-end
        _feat(_dense(-123.006, 49.2231, -0.004, 0.0), "Fork Creek"),        # a braid touching only the trib
    ]
    streams, _, _ = resolve_and_mint(feats, _FWA, LakeIndex([]), _TIDAL)
    by = {str(s["blk"]): s for s in streams}
    for s in streams:
        r = by.get(str(s.get("receiver_blk")))
        if r:
            assert not _is_name_tributary(r["name"], s["name"]), \
                f"{s['name']} flows into its own tributary {r['name']}"
    assert len({s["wsc"] for s in streams if s["name"] == "Fork Creek"}) == 1   # braid shares mainstem wsc


def test_same_name_in_different_drainages_stays_distinct():
    # Two creeks that merely SHARE A NAME but drain to different primaries (a coastal 900 vs the Fraser
    # 100) must NOT be collapsed to one wsc — the primary drainage code is inherited, never overwritten.
    feats = [
        _feat([(-123.0, 49.22), (-123.010, 49.22)], "Twin Creek"),        # novel -> Big River (Fraser 100)
        _feat([(-123.1, 49.30), (-123.105, 49.305)], "Twin Creek"),       # novel -> tidal (coastal 900)
    ]
    streams, _, _ = resolve_and_mint(feats, _FWA, LakeIndex([]), _TIDAL)
    prims = {s["wsc"][:3] for s in streams if s["name"] == "Twin Creek"}
    assert prims == {"100", "900"}                                  # each keeps its own drainage primary


def test_under_lake_segment_tagged_and_noded():
    # a novel stream crossing a lake polygon -> under-lake fid tagged with the lake wbk
    line3005 = line_to_albers([(-123.0, 49.22), (-123.02, 49.22)])   # Trib A footprint (3005)
    mid = line3005.interpolate(0.5, normalized=True)
    lake = mid.buffer(60.0)                                          # a lake straddling the middle
    lake_index = LakeIndex([(lake, "LAKE7")])
    streams, _, _ = resolve_and_mint([_FEATS[1]], [_BIG], lake_index, _TIDAL)
    seg_wbks = [seg["wbk"] for s in streams for seg in s["segments"]]
    assert "LAKE7" in seg_wbks                                       # inside segment carries the wbk
    # and build_blk_chains turns it into a WaterbodyRun (ties into the lake node)
    add_fids, _ = to_graph_inputs(streams)
    chains = build_blk_chains(add_fids, {"LAKE7": "lake"})
    assert any(r.wbk == "LAKE7" for c in chains for r in c.waterbody_runs)
