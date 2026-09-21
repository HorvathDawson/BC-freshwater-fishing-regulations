"""Tributary-scoped area_boundary closure split (Garibaldi Park / Pitt River shape).

Synthetic — a box 'park' outline and streams that end-in / pass-through / sit inside / sit outside.
No gpkg. Mirrors test_border.py: the SAME cut machinery, but the INSIDE pieces are flagged (a
closure gate marker), not the outside ones, and the inside pieces get the "within {park}" identifier.

        park = box(0,0,100,100)
        TH (through):    (50,-50) ▶ (50,50) ▶ (50,150)   enters y=0 (m=50), exits y=100 (m=150)
        EN (ends-in):    (30,-30) ▶ (30,30)              enters y=0 (m=30); headwaters INSIDE
        IN (wholly in):  (70,20)  ▶ (70,80)              never crosses; whole piece inside
        OUT (wholly out):(150,20) ▶ (150,80)             in WSC scope, but entirely outside
        OOS (out-scope): (40,20)  ▶ (40,80)              inside, but a DIFFERENT WSC (not a Pitt trib)
"""

from shapely.geometry import LineString, box

from pipeline.atlas.graph import cutting
from pipeline.atlas.splits.anchors import _target_blks, resolve_split_defs
from pipeline.atlas.graph.blk_chains import FidRow, build_blk_chains
from pipeline.atlas.splits.border import mark_inside_area
from pipeline.atlas.graph.graph import build_section_geometries, build_stream_graph
from pipeline.common.models import AnchorType, SplitAnchor, SplitDef
from pipeline.atlas.splits.sectionizer import split_graph_at

PARK = box(0, 0, 100, 100)
PARK_WSC = "100-025956"          # "Pitt River" trunk; descendants share the prefix


def _fid(fid, blk, wsc, coords):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk="", gnis_id="",
                  gnis_name="", stream_order=1, stream_magnitude=1,
                  down_m=0.0, up_m=geom.length, geometry=geom, down_node=dn, up_node=un)


def _setup():
    fids = [
        _fid("TH1", "TH", PARK_WSC + "-1",   [(50, -50), (50, 50), (50, 150)]),
        _fid("EN1", "EN", PARK_WSC + "-2",   [(30, -30), (30, 30)]),
        _fid("IN1", "IN", PARK_WSC + "-3",   [(70, 20), (70, 80)]),
        _fid("OUT1", "OUT", PARK_WSC + "-4", [(150, 20), (150, 80)]),
        _fid("OOS1", "OOS", "200-999999",    [(40, 20), (40, 80)]),
    ]
    chains = build_blk_chains(fids, {})
    graph = build_stream_graph(chains, fids, {}, {})
    geoms = build_section_geometries(chains, fids, {})
    return chains, graph, geoms


def _split_def():
    return SplitDef(
        id="gari", wsc=PARK_WSC, stream_name="Pitt River", label="Garibaldi Park",
        anchor=SplitAnchor(type=AnchorType.area_boundary, area_layer="parks_bc",
                           area_name_field="PROTECTED_LANDS_NAME", area_name="Garibaldi Park",
                           wsc_descendants=True))


def _apply(chains, graph, geoms):
    sd = _split_def()
    pts = resolve_split_defs([sd], chains, area_polys={"Garibaldi Park": PARK})
    split_graph_at(graph, geoms, pts, proximity_pickup=False)
    blks = set(_target_blks(sd, chains, descendants=True))
    n = mark_inside_area(graph, geoms, PARK, "Garibaldi Park", blks=blks)
    return pts, blks, n


# ----------------------------------------------------------------- targeting / scope

def test_wsc_descendants_prefix_match():
    chains, _, _ = _setup()
    sd = _split_def()
    desc = set(_target_blks(sd, chains, descendants=True))
    assert desc == {"TH", "EN", "IN", "OUT"}          # all Pitt-WSC descendants
    exact = set(_target_blks(sd, chains, descendants=False))
    assert exact == set()                             # none have the EXACT trunk WSC (regression guard)


# ----------------------------------------------------------------- cutting

def test_area_split_points_at_every_crossing():
    chains, _, _ = _setup()
    pts = resolve_split_defs([_split_def()], chains, area_polys={"Garibaldi Park": PARK})
    by_blk = {}
    for p in pts:
        by_blk.setdefault(p.blk, []).append(round(p.route_measure))
        assert p.anchor_type == AnchorType.area_boundary
        assert p.label == "Garibaldi Park"
    assert sorted(by_blk["TH"]) == [50, 150]          # enter + exit
    assert sorted(by_blk["EN"]) == [30]               # single entry
    assert "IN" not in by_blk and "OUT" not in by_blk # no crossings
    assert "OOS" not in by_blk                        # out of WSC scope


def test_through_stream_yields_three_pieces_middle_inside():
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert {"TH:0", "TH:50", "TH:150"} <= set(graph.nodes)
    assert graph.nodes["TH:50"].in_areas == ("Garibaldi Park",)   # the INSIDE middle piece
    assert graph.nodes["TH:0"].in_areas == ()                     # outside ends not flagged
    assert graph.nodes["TH:150"].in_areas == ()
    # geometry KEPT on every piece (unlike under-lake)
    assert all(geoms[n] is not None and not geoms[n].is_empty for n in ("TH:0", "TH:50", "TH:150"))


# ----------------------------------------------------------------- identifiers (the crux)

def test_identifiers_through_stream():
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert graph.nodes["TH:0"].location_identifier == "downstream of Garibaldi Park"
    assert graph.nodes["TH:50"].location_identifier == "within Garibaldi Park"
    assert graph.nodes["TH:150"].location_identifier == "upstream of Garibaldi Park"


def test_identifier_ends_in_park_says_within_not_upstream():
    """The hard case: a piece from the boundary to the headwaters INSIDE the park must read
    'within', not the misleading 'upstream of' the bounds alone would give."""
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert {"EN:0", "EN:30"} <= set(graph.nodes)
    assert graph.nodes["EN:0"].location_identifier == "downstream of Garibaldi Park"
    assert graph.nodes["EN:30"].in_areas == ("Garibaldi Park",)
    assert graph.nodes["EN:30"].location_identifier == "within Garibaldi Park"


def test_wholly_inside_tributary_flagged_uncut():
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert "IN:0" in graph.nodes and "IN:50" not in graph.nodes   # never cut
    assert graph.nodes["IN:0"].in_areas == ("Garibaldi Park",)
    assert graph.nodes["IN:0"].location_identifier == "within Garibaldi Park"


# ----------------------------------------------------------------- scope negatives

def test_outside_tributary_untouched():
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert graph.nodes["OUT:0"].in_areas == ()                    # in WSC scope but outside geometry
    assert graph.nodes["OUT:0"].location_identifier is None


def test_out_of_scope_wsc_not_flagged_even_if_inside():
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert graph.nodes["OOS:0"].in_areas == ()                    # inside the box, wrong WSC -> ignored


# ----------------------------------------------------------------- determinism

def test_deterministic_across_two_builds():
    a = _setup(); _apply(*a)
    b = _setup(); _apply(*b)
    ga, gb = a[1], b[1]
    assert set(ga.nodes) == set(gb.nodes)
    assert {n: ga.nodes[n].in_areas for n in ga.nodes} == {n: gb.nodes[n].in_areas for n in gb.nodes}


class TestCuratedClosureZones:
    """Hand-drawn admin polygons — a closure a regulation states as an AREA, not a reach.

    The mechanism is deliberately the SAME one parks use. A curated file is just another
    selector for `load_area_polys`, so the cutter, the area catalog and the tile exporter never
    learn where the polygon came from — which is the whole reason it was done this way rather
    than by inventing a parallel path for hand-drawn shapes.
    """

    @staticmethod
    def _def():
        from pipeline.atlas.splits.area_splits import load_area_split_defs
        d = [a for a in load_area_split_defs() if a["id"] == "sign_zones"]
        assert d, "areas.json defines no `sign_zones`"
        return d[0]

    def test_a_curated_file_is_just_another_selector(self):
        """`file` instead of `layer`, and everything downstream is identical."""
        from pipeline.atlas.splits.area_splits import load_area_polys
        ad = self._def()
        assert ad.get("file") and not ad.get("layer"), "a curated area names a file, not a layer"
        polys = load_area_polys(None, ad)
        assert len(polys) == 2, f"expected the two closure rings, got {sorted(polys)}"
        for name, g in polys.items():
            assert g.is_valid and g.geom_type in ("Polygon", "MultiPolygon"), name

    def test_the_rings_are_reprojected_to_the_graphs_crs(self):
        """**The trap this walked past.** The file is lon/lat and every other area layer is BC
        Albers. Left in 4326 the ring is a shape about 0.005 units across, it intersects nothing,
        and the cut silently never happens — a closure that resolves to open water."""
        from pipeline.atlas.splits.area_splits import load_area_polys
        for name, g in load_area_polys(None, self._def()).items():
            assert 1e4 < g.area < 1e7, (
                f"{name}: area {g.area:,.0f} m² — not metres, so the file was not reprojected")

    def test_the_ids_are_minted_from_kind_and_name(self):
        """The area id is what a rule binds, so it is a function of `kind` + the feature's
        `name` — renaming a ring in the geojson silently rebinds every rule that named it."""
        from pipeline.atlas.splits.area_splits import load_area_polys
        from pipeline.atlas.splits.area_catalog import catalog_entries
        ad = self._def()
        ids = {e.area_id for e in catalog_entries([ad], {ad["id"]: load_area_polys(None, ad)})}
        assert ids == {"area:sign_zone:fraser_river_landstrom_bar_sign_zone",
                       "area:sign_zone:skeena_river_kispiox_confluence_sign_zone"}, sorted(ids)

    def test_each_ring_cuts_the_water_it_names(self):
        """A closure zone that crosses no stream cuts nothing and the rule binds nowhere."""
        import json
        from shapely.geometry import shape, LineString
        from pipeline.common.curated import CURATED

        feats = json.loads(CURATED.waters.added_areas.read_text())["features"]
        assert len(feats) == 2
        for f in feats:
            ring = shape(f["geometry"])
            assert ring.is_valid and ring.exterior.is_simple, f["properties"]["id"]
            assert f["properties"].get("cuts", "").startswith(("gnis:", "wbk:")), (
                f"{f['properties']['id']}: `cuts` must name the registry item the ring closes, "
                f"or the drawn polygon has no way back to the regulation")

    def test_a_carried_field_the_tile_layer_does_not_declare_is_a_build_failure(self):
        """`_writer` drops anything outside `spec.attrs`, silently. Six admin layers once
        shipped with no feature id at all that way, so `carry` is checked against the spec."""
        from pipeline.deliver.tiles.layers import BY_NAME
        ad = self._def()
        spec = BY_NAME[ad["tile_layer"]]
        for f in ad.get("carry") or ():
            assert f in spec.attrs, f"{f} carried but not declared by {ad['tile_layer']}"

    def test_the_zone_is_bound_with_within_area_not_area_id(self):
        """**Precedence, not geometry.** `_specificity` reads `area_id`/`area_kind` and files
        any rule carrying one as an AREA rule — a zone default that a water-specific rule
        outranks. That is backwards for a closure. `within_area` limits what the extent already
        selects, so the rule stays a section rule about the river and the polygon only says
        where."""
        import json, glob
        from pipeline.deliver.bundle.rules import _specificity

        want = {"area:sign_zone:fraser_river_landstrom_bar_sign_zone",
                "area:sign_zone:skeena_river_kispiox_confluence_sign_zone"}
        seen = set()
        for p in glob.glob("data/curated/regulations/entries/catalogue/region-*.json"):
            for e in json.load(open(p))["entries"]:
                for r in e.get("rules", []):
                    for x in r.get("extents") or []:
                        if x.get("within_area") in want:
                            seen.add(x["within_area"])
                            assert _specificity(r) == "section", (
                                f"{r['rule_id']} became an area rule — it would be outranked")
        for p in glob.glob("data/curated/regulations/entries/dfo_salmon/region-*.json"):
            for loc in json.load(open(p))["locations"]:
                for x in (loc.get("binding") or {}).get("extents") or []:
                    if x.get("within_area") in want:
                        seen.add(x["within_area"])
        assert seen == want, f"a closure zone nothing binds to: {sorted(want - seen)}"

    def test_the_superseded_splits_are_gone(self):
        """The ring replaces them. Leaving them behind means two cut-points sitting inside a
        polygon that already cuts there, and a rule that could bind either."""
        import json
        from pipeline.common.curated import CURATED
        d = json.loads(CURATED.waters.splits.read_text())
        ids = {s["id"] for w in d["waterbodies"] for s in w["splits"]}
        for gone in ("fraser_river__landstrom_bar", "fraser_river__croft_island_southern_end",
                     "skeena_river__kispiox_sign_zone_lower", "skeena_river__kispiox_sign_zone_upper"):
            assert gone not in ids, f"{gone} was superseded by a closure zone but still exists"


class TestSmallAreasAndScopedCuts:
    """Two defects the sign zones exposed, both invisible in the build log.

    The build printed `2 polygon(s), 6 transition cut(s)` and exited 0. Three of those six were
    on the wrong streams and the one river that mattered was never cut at all.
    """

    @staticmethod
    def _ring(w, h, cx=0.0, cy=0.0):
        from shapely.geometry import Polygon
        return Polygon([(cx - w / 2, cy - h / 2), (cx + w / 2, cy - h / 2),
                        (cx + w / 2, cy + h / 2), (cx - w / 2, cy + h / 2)])

    def test_a_polygon_crossed_between_two_vertices_is_still_cut(self):
        """**The Landstrom bug.** Containment was tested per VERTEX, which is exact for a park —
        kilometres wide, far wider than the spacing of points describing a river. It failed
        silently for an 11.5 ha ring: the Fraser's chain is 1,407 km long and has no vertex inside
        the 138 m the ring clips, so every vertex read as outside and the mainstem was never cut.

        The rule then bound the whole 10.6 km gauge-to-gauge section it sits in — a closure spread
        over ten times the water it covers, and WIDER than the two cut-points it replaced.
        """
        from shapely.geometry import LineString
        from pipeline.atlas.splits.anchors import _area_transition_measures
        line = LineString([(-1000, 0), (1000, 0)])       # one 2 km span, no vertex in the middle
        ring = self._ring(100, 100)                      # 100 m box the line crosses
        assert not any(ring.contains(__import__("shapely").geometry.Point(c)) for c in line.coords)
        ms = _area_transition_measures(line, ring, ring.boundary)
        assert len(ms) == 2, "a line crossing between vertices must still enter and exit"
        assert abs(ms[0] - 950) < 1 and abs(ms[1] - 1050) < 1, ms

    def test_the_fallback_cannot_move_or_remove_an_existing_cut(self):
        """Why the fix is safe to run province-wide: it lives in the branch that used to return
        `[]`. A polygon with a vertex inside never reaches it, so every cut that existed before
        is bit-for-bit the same and the only change is crossings that were being missed."""
        from shapely.geometry import LineString
        from pipeline.atlas.splits.anchors import _area_transition_measures
        line = LineString([(-1000, 0), (-10, 0), (0, 0), (10, 0), (1000, 0)])
        ring = self._ring(100, 100)
        ms = _area_transition_measures(line, ring, ring.boundary)
        assert len(ms) == 2 and abs(ms[0] - 950) < 1 and abs(ms[1] - 1050) < 1, ms

    def test_a_scoped_area_cuts_only_the_water_it_is_about(self):
        """**The Kispiox bug.** Blanket is right for a park — everything inside is closed, whatever
        stream it is. It is wrong for a ring drawn across a confluence: the zone spans the Kispiox
        mouth, so a blanket cut also chopped the Kispiox and an unnamed channel, while the
        regulation says "MAINSTEM waters within 3 white triangular fishing boundary signs".

        The scope is the `cuts` property the ring already carries, so the tile link and the cut
        scope cannot disagree. No scope = blanket, which is every area that existed before.
        """
        from shapely.geometry import LineString
        from pipeline.common.models import BlkChain
        from pipeline.atlas.splits.area_splits import resolve_area_splits

        def chain(blk, gnis):
            return BlkChain(blk=blk, fwa_watershed_code="", fids=(),
                            geometry=LineString([(-1000, 0), (1000, 0)]),
                            mouth_measure=0.0, length_m=2000.0, name_tuples=(), gnis_id=gnis)

        chains = [chain("main", "2936"), chain("trib", "2308"), chain("unnamed", "")]
        polys = {"zone": self._ring(100, 100)}
        assert len(resolve_area_splits(polys, chains)) == 6, "unscoped stays blanket"
        scoped = resolve_area_splits(polys, chains, scope={"zone": "gnis:2936"})
        assert {p.blk for p in scoped} == {"main"}, sorted(p.blk for p in scoped)
        assert len(scoped) == 2, "enter and exit on the one water the zone is about"

    def test_every_sign_zone_declares_the_water_it_cuts(self):
        """A ring with no `cuts` would silently fall back to blanket — which is exactly the bug
        it would be reintroducing."""
        import json
        from pipeline.common.curated import CURATED
        for f in json.loads(CURATED.waters.added_areas.read_text())["features"]:
            assert f["properties"].get("cuts"), f"{f['properties']['id']} declares no `cuts`"
