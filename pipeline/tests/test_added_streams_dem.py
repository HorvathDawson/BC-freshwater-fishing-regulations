"""DEM flow connectivity rules (no network — a stub elevation sampler).

The user's rules, as tests:
  - pieces that physically touch (endpoint on another piece's line, ~0 m) are ONE component;
  - every tolerance-based join shows a VISIBLE bridge — nothing merges by tolerance without a connector;
  - a truly-coincident shared vertex merges silently (no bridge);
  - bridges only ever grow a component — never a within-component loop (a spanning forest).
"""

from pipeline.added_streams.dem import dem_flow, _metres

_LON, _LAT = -123.0, 49.0
_MDEG_LON = 1.0 / 72000.0     # ~1 m in lon degrees near lat 49
_MDEG_LAT = 1.0 / 111195.0    # ~1 m in lat degrees


def _e(m):    # metres east -> lon
    return _LON + m * _MDEG_LON


def _n(m):    # metres north -> lat
    return _LAT + m * _MDEG_LAT


def _feat(name, coords):
    return {"type": "Feature", "properties": {"name": name},
            "geometry": {"type": "LineString", "coordinates": coords}}


class _Sampler:
    """Deterministic fake elevation: rises to the east and north, so flow heads to the SW origin."""
    def elevation(self, lon, lat):
        return (lon - _LON) / _MDEG_LON + (lat - _LAT) / _MDEG_LAT


def _comps(out):
    from collections import defaultdict
    c = defaultdict(list)
    for i, v in out.items():
        c[v["comp"]].append(i)
    return c


def test_endpoint_on_line_join_is_one_component():
    """A tributary whose mouth sits ON a mainstem mid-span (gap ~0) must be the SAME component
    (the Hop Ranch/Dryden bug: a 0 m endpoint-on-line touch was dropped by the gap>1 guard)."""
    mainstem = _feat("Mainstem Creek", [[_e(0), _n(0)], [_e(100), _n(0)]])
    trib = _feat("Trib Creek", [[_e(50), _n(0)], [_e(50), _n(100)]])   # starts on the mainstem, mid-span
    out, bridges, _ = dem_flow([mainstem, trib], _Sampler())
    assert set(out) == {0, 1}
    assert out[0]["comp"] == out[1]["comp"], "endpoint-on-line touch must be one component"
    assert bridges == [], "a zero-gap endpoint-on-line touch must NOT draw a bridge"


def test_same_name_endpoint_near_line_with_a_real_gap_draws_a_bridge():
    """A SAME-NAME fragment whose end is OFF another fragment's body by a real gap (within max_bridge)
    is a spanning bridge and must be drawn — only true (~0 m) touches are silent."""
    body = _feat("Split Creek", [[_e(0), _n(0)], [_e(200), _n(0)]])
    arm = _feat("Split Creek", [[_e(100), _n(8)], [_e(100), _n(100)]])  # end 8 m off the body, mid-span
    out, bridges, _ = dem_flow([body, arm], _Sampler())
    assert out[0]["comp"] == out[1]["comp"], "an 8 m same-name endpoint-on-line gap still joins"
    assert len(bridges) == 1, "a real same-name gap to the body must show one bridge"


def test_endpoint_gap_becomes_a_visible_bridge():
    """Two collinear fragments of one creek with a small (8 m) gap: one component AND a visible bridge."""
    p1 = _feat("Gap Creek", [[_e(0), _n(0)], [_e(100), _n(0)]])
    p2 = _feat("Gap Creek", [[_e(108), _n(0)], [_e(208), _n(0)]])      # 8 m gap after p1
    out, bridges, _ = dem_flow([p1, p2], _Sampler())
    assert out[0]["comp"] == out[1]["comp"]
    assert len(bridges) == 1, "a tolerance gap must show exactly one bridge"
    b = bridges[0]
    assert 5.0 < _metres(b["a"], b["b"]) < 12.0
    s = _Sampler()                                                    # a->b must point DOWNSTREAM (downhill),
    assert s.elevation(*b["a"]) >= s.elevation(*b["b"]), \
        "the bridge arrow (a->b) must point downstream, toward the sink"


def test_gap_wider_than_max_bridge_is_not_bridged():
    """A gap beyond max_bridge (~50 m) must NOT be bridged, even for same-name fragments — the two
    stay separate components with no connector (bridges are capped short)."""
    p1 = _feat("Far Creek", [[_e(0), _n(0)], [_e(100), _n(0)]])
    p2 = _feat("Far Creek", [[_e(180), _n(0)], [_e(280), _n(0)]])      # 80 m gap: too far
    out, bridges, _ = dem_flow([p1, p2], _Sampler())
    assert out[0]["comp"] != out[1]["comp"], "a >10 m gap must not merge components"
    assert bridges == []


def test_different_named_fragments_are_not_gap_bridged():
    """A gap bridge may only join pieces of the SAME name. Two DIFFERENT-named creeks with a small
    gap (well within max_bridge) must stay separate components with no connector — cross-name gaps are
    coincidence, not one drainage (the Kaymar Trib.2 / Trib.2-1 spurious-bridge bug)."""
    a = _feat("Alpha Creek", [[_e(0), _n(0)], [_e(100), _n(0)]])
    b = _feat("Beta Creek", [[_e(108), _n(0)], [_e(208), _n(0)]])      # 8 m gap, but a different name
    out, bridges, _ = dem_flow([a, b], _Sampler())
    assert out[0]["comp"] != out[1]["comp"], "a cross-name gap must not merge components"
    assert bridges == [], "a cross-name gap must not draw a bridge"


def test_cross_name_confluence_touch_connects_within_touch_tol():
    """West Sundial Creek: a named creek whose MOUTH meets an unnamed ditch a couple of metres away is a
    real confluence and must join into ONE component (so the pair drains as one system to a single sink) —
    even across names. Only LARGER cross-name gaps stay separate (the Kaymar rule, 8 m test above)."""
    ws = _feat("West Sundial Creek", [[_e(0), _n(0)], [_e(100), _n(0)]])
    ditch = _feat("", [[_e(102), _n(0)], [_e(200), _n(0)]])            # unnamed ditch: end 2 m off WS's mouth,
    kyle = _feat("Kyle Creek", [[_e(200), _n(0)], [_e(200), _n(100)]])  # far end shares a vertex with Kyle
    out, bridges, _ = dem_flow([ws, ditch, kyle], _Sampler())
    assert set(out) == {0, 1, 2}, "the ditch is kept (it links two named creeks, not a dangling stub)"
    assert out[0]["comp"] == out[2]["comp"], "WS + ditch + Kyle are one drainage via the 2 m confluence"


def test_different_named_streams_touching_still_connect_silently():
    """A real confluence — a differently-named tributary whose mouth TOUCHES the mainstem (~0 m) —
    still joins into one component with no bridge. Only GAPPED cross-name joins are refused."""
    mainstem = _feat("Mainstem Creek", [[_e(0), _n(0)], [_e(100), _n(0)]])
    trib = _feat("Trib Creek", [[_e(50), _n(0)], [_e(50), _n(100)]])   # mouth exactly on the mainstem body
    out, bridges, _ = dem_flow([mainstem, trib], _Sampler())
    assert out[0]["comp"] == out[1]["comp"], "a zero-gap confluence still connects across names"
    assert bridges == [], "a touch draws no bridge"


def test_shared_vertex_merges_without_a_bridge():
    """An exact shared vertex is truly coincident — one component, and NO bridge is drawn."""
    p1 = _feat("Solid Creek", [[_e(0), _n(0)], [_e(100), _n(0)]])
    p2 = _feat("Solid Creek", [[_e(100), _n(0)], [_e(200), _n(0)]])    # shares the exact vertex
    out, bridges, _ = dem_flow([p1, p2], _Sampler())
    assert out[0]["comp"] == out[1]["comp"]
    assert bridges == [], "a coincident vertex must not draw a bridge"


def test_bridges_span_only_never_loop():
    """Three fragments meeting near a junction with ~5 m gaps form ONE component with a spanning
    set of bridges (2), never a third redundant bridge that would close a loop."""
    west = _feat("Star Creek", [[_e(-100), _n(0)], [_e(0), _n(0)]])    # inner end at origin
    east = _feat("Star Creek", [[_e(5), _n(0)], [_e(105), _n(0)]])     # inner end 5 m east
    north = _feat("Star Creek", [[_e(0), _n(5)], [_e(0), _n(105)]])    # inner end 5 m north
    out, bridges, _ = dem_flow([west, east, north], _Sampler())
    assert len({v["comp"] for v in out.values()}) == 1, "all three fragments are one component"
    assert len(bridges) == 2, "spanning tree of 3 nodes = 2 bridges, no loop-closing third"


class _DictSampler:
    """Elevation looked up by rounded lon/lat, so a test can place a spurious interior low. With a
    ``default`` set, an unlisted coord (e.g. a lake centroid) returns it instead of raising."""
    def __init__(self, table, default=None):
        self.table = {(round(lon, 7), round(lat, 7)): e for (lon, lat), e in table.items()}
        self.default = default
    def elevation(self, lon, lat):
        k = (round(lon, 7), round(lat, 7))
        return self.table.get(k, self.default) if self.default is not None else self.table[k]


def test_flow_converges_downhill_to_the_low_outlet():
    """Every kept piece is oriented so coords[0] (downstream) is its lower end, and the sink marker
    sits at the network's low outlet — not a mid-network point."""
    a = _feat("River", [[_e(0), _n(0)], [_e(100), _n(0)]])
    b = _feat("River", [[_e(100), _n(0)], [_e(200), _n(0)]])
    c = _feat("River", [[_e(100), _n(0)], [_e(100), _n(100)]])   # a trib joining at the junction
    s = _Sampler()                                                # rises east + north; origin is lowest
    out, _, markers = dem_flow([a, b, c], s)
    for i in out:
        cd = out[i]["coords"]
        assert s.elevation(*cd[0]) <= s.elevation(*cd[-1]) + 1e-9, "coords[0] must be the downhill end"
    sink = next(m for m in markers if m["kind"] == "sink")
    assert abs(sink["lonlat"][0] - _e(0)) < 1e-9 and abs(sink["lonlat"][1] - _n(0)) < 1e-9


def test_sink_prefers_a_low_leaf_over_an_interior_noise_pit():
    """On flat ground a DEM-noise dip at an interior junction must NOT be chosen as the sink — the
    true outlet is a network terminus (leaf). Sink = lowest leaf in the flat band."""
    O, J, E, N = (_e(0), _n(0)), (_e(100), _n(0)), (_e(200), _n(0)), (_e(100), _n(100))
    p1 = _feat("Flat Creek", [list(O), list(J)])
    p2 = _feat("Flat Creek", [list(J), list(E)])
    p3 = _feat("Flat Creek", [list(J), list(N)])
    sampler = _DictSampler({O: 0.0, J: -1.0, E: 5.0, N: 5.0})     # J is the interior noise pit
    out, _, markers = dem_flow([p1, p2, p3], sampler)
    sink = next(m for m in markers if m["kind"] == "sink")
    assert abs(sink["lonlat"][0] - O[0]) < 1e-9 and abs(sink["lonlat"][1] - O[1]) < 1e-9, \
        "sink must be the leaf outlet O, not the deeper interior junction J"


def test_trib_joined_by_endpoint_on_its_body_flows_to_the_mouth_not_the_far_end():
    """The Little Stawamus Trib 1 bug: a mainstem endpoint lands on a tributary's mid-body near the
    trib's MOUTH. That junction must not connect to BOTH trib ends equally (which ties their BFS depth
    and lets the elevation tiebreak flip the trib to drain out its far end). The trib must flow toward
    the mouth end that sits by the mainstem contact — even when the far end is the lower ground."""
    MOUTH, FAR = (_e(0), _n(0)), (_e(0), _n(100))
    trib = _feat("Trib Creek", [list(MOUTH), list(FAR)])              # vertical trib, mouth at origin
    P, MSEND = (_e(0), _n(10)), (_e(100), _n(10))                     # P sits ON the trib body (~0 m, fr~0.1)
    mainstem = _feat("Main Creek", [list(P), list(MSEND)])            # mainstem runs east to its outlet
    # far end is the LOWER ground, so a naive elevation tiebreak would wrongly drain the trib north:
    sampler = _DictSampler({MOUTH: 10.0, FAR: 8.0, P: 6.0, MSEND: 0.0})
    out, _, _ = dem_flow([trib, mainstem], sampler)
    assert out[0]["comp"] == out[1]["comp"], "the endpoint-on-body touch must join them"
    cd = out[0]["coords"]
    assert abs(cd[0][0] - MOUTH[0]) < 1e-9 and abs(cd[0][1] - MOUTH[1]) < 1e-9, \
        "the trib must flow to its mouth (by the mainstem), not out its lower far end"


def test_trust_source_orients_by_source_not_terrain():
    """When the municipal source direction is reliable (burnaby), flow follows the SOURCE vertex order,
    not the DEM. Source is drawn upstream-first, so the mouth-first output is reversed(source) — even
    when the terrain sampler would (wrongly) say otherwise."""
    up, down = (_e(0), _n(100)), (_e(0), _n(0))                       # source: coords[0]=upstream, [-1]=mouth
    creek = _feat("Trusted Creek", [list(up), list(down)])
    sampler = _DictSampler({up: 0.0, down: 50.0})                     # terrain LIES: says 'up' is lower
    out, _, _ = dem_flow([creek], sampler, trust_source=True)
    cd = out[0]["coords"]
    assert cd[0] == list(down) and cd[-1] == list(up), \
        "trust_source: downstream (coords[0]) must be the source's last vertex, ignoring terrain"


def test_burnaby_trust_source_matches_raw_everywhere():
    """Regression on real Burnaby geometry: with trust_source, every kept piece's dem orientation
    equals reversed(source) — i.e. dem-raw == raw exactly (Burnaby's source flow is authoritative)."""
    import pytest
    from pipeline.added_streams.clean import clean_source
    feats = [f for f in clean_source("burnaby")
             if not str(f.get("properties", {}).get("ftype", "")).lower().startswith("unconfirmed")]
    if not feats:
        pytest.skip("burnaby source data not available")

    class _Zero:
        def elevation(self, lon, lat):
            return 0.0
    out, _, _ = dem_flow(feats, _Zero(), trust_source=True)
    mismatched = [i for i, v in out.items()
                  if v["coords"] != list(feats[i]["geometry"]["coordinates"])[::-1]]
    assert mismatched == [], f"{len(mismatched)} burnaby pieces disagree with the trusted source direction"


def test_trust_source_sink_is_the_outflow_not_the_confluence():
    """Under trust_source the sink marker must follow the SOURCE flow: the outlet is the downstream
    terminus (a node with no outgoing piece), NOT a confluence that flow continues out of — even when
    DEM says the confluence is the lowest point (the Still/Ardley/Stickleback spurious-sink bug)."""
    A_up, B_up = (_e(0), _n(100)), (_e(100), _n(100))
    J, OUT = (_e(50), _n(50)), (_e(50), _n(0))                        # J = confluence, OUT = outflow end
    t1 = _feat("A Creek", [list(A_up), list(J)])                      # source upstream-first: [-1]=downstream
    t2 = _feat("B Creek", [list(B_up), list(J)])
    outflow = _feat("A Creek", [list(J), list(OUT)])                  # flow continues J -> OUT
    sampler = _DictSampler({A_up: 100.0, B_up: 100.0, J: 0.0, OUT: 50.0})  # DEM lies: J is the low
    out, _, markers = dem_flow([t1, t2, outflow], sampler, trust_source=True)
    sink = next(m for m in markers if m["kind"] == "sink")
    assert sink["lonlat"] == list(OUT), "sink must be the outflow terminus, not the confluence J"


def _lake(corners):
    """A shapely lake polygon in Albers (EPSG:3005), from lon/lat corners."""
    from shapely.geometry import Polygon
    from pipeline.added_streams.dem import _TO_ALBERS
    return Polygon([_TO_ALBERS.transform(x, y) for x, y in corners])


def _fwa_line(coords):
    from shapely.geometry import LineString as _LS
    from pipeline.added_streams.dem import _TO_ALBERS
    return _LS([_TO_ALBERS.transform(x, y) for x, y in coords])


def test_sink_is_the_outlet_even_when_a_trib_joins_there():
    """A tributary that joins the mainstem near its OUTLET makes that outlet node degree-2 (not a leaf).
    The sink must still be the outlet (the node at the FWA/tidal it drains to), not flip to a headwater
    leaf — otherwise the whole component reverses. Uses an FWA outlet by the mainstem's low end."""
    main = _feat("Main Creek", [[_e(0), _n(0)], [_e(200), _n(0)]])     # low end at origin (SW), the outlet
    trib = _feat("Side Creek", [[_e(20), _n(3)], [_e(20), _n(90)]])    # joins Main ~3 m off, near the outlet
    fwa = _fwa_line([[_e(-6), _n(-40)], [_e(-6), _n(40)]])             # FWA river just west of the outlet
    out, bridges, markers = dem_flow([main, trib], _Sampler(), fwa=[fwa])
    sink = next(m for m in markers if m["kind"] == "sink")
    assert abs(sink["lonlat"][0] - _e(0)) < 1e-6 and abs(sink["lonlat"][1] - _n(0)) < 1e-6, \
        "sink must be the outlet node, not a headwater leaf"
    cd = out[0]["coords"]                                              # Main flows toward the outlet (west)
    assert abs(cd[0][0] - _e(0)) < 1e-6, "Main's mouth (coords[0]) is the outlet end"


def test_lake_node_unifies_streams_draining_into_it():
    """Two differently-named creeks that both end at the same lake are ONE component (the lake is a
    shared confluence node), and with no outflow the single sink sits at the lake."""
    lake = _lake([(_e(180), _n(-20)), (_e(220), _n(-20)), (_e(220), _n(20)), (_e(180), _n(20))])
    a = _feat("A Creek", [[_e(100), _n(-10)], [_e(180), _n(-10)]])    # source upstream-first: mouth at lake
    b = _feat("B Creek", [[_e(100), _n(10)], [_e(180), _n(10)]])
    out, bridges, markers = dem_flow([a, b], _Sampler(), trust_source=True, lakes=[lake])
    assert out[0]["comp"] == out[1]["comp"], "streams touching the same lake are one component"
    sinks = [m for m in markers if m["kind"] == "sink"]
    assert len(sinks) == 1, "one lake-rooted component => one sink"


def test_lake_outflow_becomes_the_sink_not_the_lake():
    """When a stream flows OUT of the lake (its upstream end is at the lake), the component sink is that
    outflow's downstream terminus — the lake is a pass-through confluence, not the sink."""
    lake = _lake([(_e(180), _n(-20)), (_e(220), _n(-20)), (_e(220), _n(20)), (_e(180), _n(20))])
    a = _feat("A Creek", [[_e(100), _n(-10)], [_e(180), _n(-10)]])    # into the lake (mouth at lake)
    b = _feat("B Creek", [[_e(100), _n(10)], [_e(180), _n(10)]])      # into the lake
    outflow = _feat("Outlet Creek", [[_e(220), _n(0)], [_e(1500), _n(0)]])  # upstream at lake, runs far => outflow
    out, bridges, markers = dem_flow([a, b, outflow], _Sampler(), trust_source=True, lakes=[lake])
    assert len({v["comp"] for v in out.values()}) == 1, "all three join through the lake"
    sink = next(m for m in markers if m["kind"] == "sink")
    assert abs(sink["lonlat"][0] - _e(1500)) < 1e-6 and abs(sink["lonlat"][1] - _n(0)) < 1e-6, \
        "sink must be the outflow's downstream terminus, not the lake"


def _on_interior(pt, coords, tol_m=2.0):
    """True if lon/lat ``pt`` lies on the INTERIOR (not an endpoint) of the piece ``coords``."""
    from shapely.geometry import Point, LineString
    from pipeline.added_streams.dem import _TO_ALBERS
    g = LineString([_TO_ALBERS.transform(x, y) for x, y in coords])
    p = Point(_TO_ALBERS.transform(*pt))
    fr = g.project(p, normalized=True)
    return p.distance(g) <= tol_m and 0.05 < fr < 0.95


def test_duplicate_overlapping_pieces_are_deduped():
    """Municipal data often draws a reach TWICE (two features between the same endpoints). Two parallel
    pieces are a loop that doubles the arrows and leaves an unmarked pseudo-leaf. dem_flow must keep ONE
    piece per node-pair so the graph is a clean tree."""
    a = _feat("Dup Creek", [[_e(0), _n(0)], [_e(50), _n(20)], [_e(100), _n(0)]])
    b = _feat("Dup Creek", [[_e(0), _n(0)], [_e(100), _n(0)]])          # same endpoints, redundant path
    out, bridges, _ = dem_flow([a, b], _Sampler())
    assert len(out) == 1, "a duplicate piece between the same two nodes is dropped"


def test_overlapping_subsegment_piece_is_deduped():
    """The Kyle Creek artifact: a redundant piece drawn ON TOP of a longer one (a partial duplicate that
    does NOT share endpoints, so the node-pair dedup misses it) doubles the arrows and points a stub at a
    silent junction. A piece that lies almost entirely within a few metres of a LONGER piece is dropped."""
    long_ = _feat("Kyle Creek", [[_e(0), _n(0)], [_e(200), _n(0)]])
    over = _feat("Kyle Creek", [[_e(50), _n(0)], [_e(150), _n(0)]])   # lies exactly on long_'s middle
    out, bridges, _ = dem_flow([long_, over], _Sampler())
    assert len(out) == 1, "an overlapping sub-segment piece is dropped as redundant"


def test_dem_exposes_downstream_tree_link_per_piece():
    """dem_flow tags each piece with its DOWNSTREAM piece (``down``) — the piece its mouth flows into,
    following the sink-rooted spanning tree. The component sink piece has ``down`` None (it exits to an
    outlet). This is the tree the resolver builds receivers from, so resolved flow == dem-raw."""
    a = _feat("River", [[_e(0), _n(0)], [_e(100), _n(0)]])       # mouth at origin (the sink)
    b = _feat("River", [[_e(100), _n(0)], [_e(200), _n(0)]])     # upstream mainstem, flows into a
    c = _feat("Trib", [[_e(100), _n(0)], [_e(100), _n(100)]])    # trib joining at the (100,0) confluence
    out, _, _ = dem_flow([a, b, c], _Sampler())
    assert out[1]["down"] == 0, "upstream mainstem b flows into a"
    assert out[2]["down"] == 0, "the trib c flows into a at the confluence"
    assert out[0]["down"] is None, "the sink piece a has no downstream piece (it exits to an outlet)"


def test_a_loop_does_not_spawn_a_mid_network_source():
    """The port_moody Kyle Creek bug: a piece whose endpoint lands ON another piece that is ALREADY in
    the same component (closing a loop) had its endpoint-on-line link dropped entirely — leaving that
    endpoint a phantom degree-1 leaf, which got marked a SOURCE sitting mid-stream. A source must only be
    a true headwater leaf, never a point on another piece's body. The loop edge must still connect the
    endpoint (so it is not a leaf) even though it is not drawn as a spanning bridge."""
    p1 = _feat("Loop Creek", [[_e(0), _n(0)], [_e(100), _n(0)]])       # sink end at origin (SW low)
    p2 = _feat("Loop Creek", [[_e(0), _n(0)], [_e(50), _n(50)]])       # shares origin vertex with p1
    p3 = _feat("Loop Creek", [[_e(50), _n(50)], [_e(50), _n(0)]])      # p2 top -> lands ON p1's body: LOOP
    out, bridges, markers = dem_flow([p1, p2, p3], _Sampler())
    assert len({v["comp"] for v in out.values()}) == 1, "all three are one component"
    for m in markers:
        if m["kind"] == "source":
            for i in out:                                             # no source may sit on a piece's body
                assert not _on_interior(m["lonlat"], out[i]["coords"]), \
                    "a loop endpoint on another piece's body must NOT be a source"


def test_reach_is_a_lake_outflow_even_when_its_sink_drains_into_another_lake():
    """Deer Lake Brook: a reach is Deer Lake's OUTFLOW at its source end, but its mouth (sink) sits near a
    DIFFERENT lake (Burnaby Lake). The sink attaching into that other lake must NOT suppress the outflow
    attach — otherwise Deer Lake's tributaries never connect to the brook and the lake's own creek becomes
    a false sink. The reach must attach to BOTH lakes, merging Deer Lake's tributary into one component.
    (_Sampler rises east+north: LakeB at the low west, LakeA higher to the east.)"""
    lakeB = _lake([(_e(-20), _n(-20)), (_e(20), _n(-20)), (_e(20), _n(20)), (_e(-20), _n(20))])   # low (mouth)
    lakeA = _lake([(_e(380), _n(-20)), (_e(420), _n(-20)), (_e(420), _n(20)), (_e(380), _n(20))]) # high (source)
    brook = _feat("Brook Creek", [[_e(380), _n(0)], [_e(20), _n(0)]])   # source at LakeA -> mouth at LakeB
    trib = _feat("Trib Creek", [[_e(500), _n(0)], [_e(420), _n(0)]])    # drains INTO LakeA (its mouth at LakeA)
    out, _, _ = dem_flow([brook, trib], _Sampler(), lakes=[lakeA, lakeB])
    assert out[0]["comp"] == out[1]["comp"], \
        "the brook (LakeA outflow) and LakeA's tributary must be one component via LakeA"


def test_two_creeks_at_a_river_each_drain_to_their_own_mouth():
    """Rudolph / Ancient Grove / Trolley: several creeks whose mouths all reach the Brunette get merged into
    ONE component (their mouths touch near the river), but each must drain to the river at its OWN mouth, not
    be reversed uphill into a neighbour that shares the component. A river edge has MANY tributary mouths, so
    the flow BFS seeds from EVERY river contact. (_Sampler rises east+north; the river is the low south edge.)"""
    river = _fwa_line([[_e(0), _n(0)], [_e(300), _n(0)]])             # the Brunette, along the low south edge
    trolley = _feat("Trolley Creek", [[_e(100), _n(200)], [_e(100), _n(5)]])   # headwater -> mouth at river
    rudolph = _feat("Rudolph Creek", [[_e(100), _n(100)], [_e(150), _n(5)]])   # joins Trolley's body, own mouth
    out, _, markers = dem_flow([trolley, rudolph], _Sampler(), fwa=[river])
    assert out[0]["comp"] == out[1]["comp"], "the two creeks touch, so they are one component"
    cd = out[1]["coords"]                                            # Rudolph must flow to ITS OWN river mouth
    assert abs(cd[0][0] - _e(150)) < 1e-6 and abs(cd[0][1] - _n(5)) < 1e-6, \
        "Rudolph's mouth is its own river contact, not reversed up into Trolley Creek"
    assert sum(1 for m in markers if m["kind"] == "sink") >= 2, "each creek shows its own river outlet"


def test_lake_touching_a_river_is_the_basin_sink_over_a_dem_pit():
    """Burnaby Lake / Still Creek: an APPROVED lake that touches an FWA river drains OUT to it, so it is the
    basin terminus even when a DEM NOISE PIT elsewhere in the component is lower. Streams then flow TOWARD
    the lake (Still Creek stopped flowing away from Burnaby Lake) instead of past it toward the pit."""
    lake = _lake([(_e(-20), _n(180)), (_e(20), _n(180)), (_e(20), _n(220)), (_e(-20), _n(220))])
    fwa = _fwa_line([[_e(-60), _n(200)], [_e(-20), _n(200)]])          # river touching the lake's west edge
    creek = _feat("Still Creek", [[_e(0), _n(500)], [_e(0), _n(220)]]) # headwater -> mouth at the lake (north)
    spur = _feat("Still Creek", [[_e(0), _n(500)], [_e(300), _n(900)]])# shares the headwater; its far end is a PIT
    sampler = _DictSampler({(_e(0), _n(500)): 50.0, (_e(0), _n(220)): 20.0, (_e(300), _n(900)): 5.0},
                           default=100.0)                             # pit (5) is the lowest node in the component
    out, _, markers = dem_flow([creek, spur], sampler, lakes=[lake], fwa=[fwa])
    cd = out[0]["coords"]                                             # Still Creek's mouth (coords[0])
    assert abs(cd[0][0] - _e(0)) < 1e-6 and abs(cd[0][1] - _n(220)) < 1e-6, \
        "the creek must flow to its mouth AT the lake, not past it toward the lower DEM pit"
    sink = next(m for m in markers if m["kind"] == "sink" and m["comp"] == out[0]["comp"])
    assert _metres(sink["lonlat"], [_e(300), _n(900)]) > 100.0, "the pit must NOT be the sink; the lake is"


def test_lake_merges_streams_only_when_that_lake_is_passed():
    """Regression (squamish Little Stawamus / Valleycliffe merged through a lake): two unrelated creeks
    that both end near a lake merge into ONE drainage only if that lake is passed as a node. Sources
    pass ONLY approved lakes, so an un-approved estuary lake must NEVER connect separate creeks."""
    lake = _lake([(_e(180), _n(-20)), (_e(220), _n(-20)), (_e(220), _n(20)), (_e(180), _n(20))])
    a = _feat("A Creek", [[_e(100), _n(-10)], [_e(180), _n(-10)]])
    b = _feat("B Creek", [[_e(100), _n(10)], [_e(180), _n(10)]])
    merged, _, _ = dem_flow([a, b], _Sampler(), trust_source=True, lakes=[lake])
    assert merged[0]["comp"] == merged[1]["comp"], "the lake node merges them when the lake is passed"
    split, _, _ = dem_flow([a, b], _Sampler(), trust_source=True, lakes=None)
    assert split[0]["comp"] != split[1]["comp"], "without the lake, separate creeks must stay separate"


def test_unnamed_only_component_is_dropped():
    """A component with no named piece is noise and is dropped; a named one is kept."""
    named = _feat("Real Creek", [[_e(0), _n(0)], [_e(100), _n(0)]])
    orphan = _feat("", [[_e(0), _n(500)], [_e(100), _n(500)]])         # far, unnamed, isolated
    out, _, _ = dem_flow([named, orphan], _Sampler())
    assert 0 in out and 1 not in out


def test_close_headwaters_of_two_tribs_keep_their_source_markers():
    """Hutchinson Creek 205/206 (and Noble 174/175): two same-name tributaries join the SAME mainstem at
    DIFFERENT points, and their far HEADWATERS happen to sit within ``max_bridge`` of each other. A same-
    name loop-closing link between those two headwaters (already one component via the mainstem) makes both
    degree-2 — hiding their source markers and drawing a false Y between the tips. Because the two tips do
    NOT share a junction it is a 'diamond', not a triangle, so it is caught by elevation: a link between two
    HEADWATERS (each higher than its real neighbour) is dropped, while a genuine low-mouth-on-body link
    (test_a_loop_does_not_spawn_a_mid_network_source) is kept."""
    O = (_e(0), _n(0))                                                # low outlet (SW)
    main = _feat("H Creek", [list(O), [_e(0), _n(300)]])              # mainstem runs north from the outlet
    armA = _feat("H Creek", [[_e(60), _n(120)], [_e(0), _n(100)]])    # trib A: head -> joins main at n=100
    armB = _feat("H Creek", [[_e(55), _n(130)], [_e(0), _n(150)]])    # trib B: head -> joins main at n=150
    HA, HB = (_e(60), _n(120)), (_e(55), _n(130))                     # the two headwaters, ~11 m apart
    out, _, markers = dem_flow([main, armA, armB], _Sampler())
    assert len({v["comp"] for v in out.values()}) == 1, "all three are one drainage"
    srcs = [m for m in markers if m["kind"] == "source"]
    def _has(pt):
        return any(_metres(m["lonlat"], list(pt)) < 2.0 for m in srcs)
    assert _has(HA) and _has(HB), "both close headwater tips must keep a source marker (no headwater-link)"


def test_apex_draining_loop_splits_at_the_interior_touch():
    """Kyle Creek 204.1: a piece shaped like a lollipop — BOTH its endpoints are headwaters and the LOW
    outlet is at its interior, where an up-going stream's endpoint touches it. A single directed
    linestring cannot express 'both ends flow to the middle', so the piece must SPLIT at that interior
    touch into two reaches, each flowing to the apex (the T-junction becomes a real shared node — no
    far-endpoint jump). ``_Sampler`` rises east+north, so the SW origin is the low apex."""
    apex = [_e(0), _n(0)]                                              # low outlet (SW-most)
    # the lollipop: E-high headwater -> down to the apex (interior, frac ~0.5) -> up to N-high headwater
    loop = _feat("Kyle Creek", [[_e(100), _n(0)], apex, [_e(0), _n(100)]])
    # the up-going stream: its endpoint lands exactly on the apex (the loop's interior vertex)
    up = _feat("Kyle Creek", [[_e(-100), _n(-100)], apex])
    out, bridges, _ = dem_flow([loop, up], _Sampler())

    assert out[0]["comp"] == out[1]["comp"], "the loop and the up-stream are one drainage"
    reaches = out[0].get("reaches")
    assert reaches is not None and len(reaches) == 2, "the lollipop must split into two reaches at the apex"
    for r in reaches:                                                  # each reach flows TO the apex
        assert _metres(r["coords"][0], apex) < 2.0, "each reach's mouth (coords[0]) is the apex"
        assert _metres(r["coords"][-1], apex) > 50.0, "each reach's headwater end is far from the apex"
        assert r["down"] == 1, "each reach flows into the up-going stream (feat 1) at the apex"
    # the confluence is a real node AT the apex (a shared vertex) — no far-endpoint jump, no bridge drawn
    assert bridges == [], "a zero-gap T-junction is a coincident node, not a spanning bridge"
