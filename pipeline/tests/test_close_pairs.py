"""T179 close pairs (owner 2026-10-25): park boundary ends, the one-entry guard, the border from the
region polygons (Moyie), and the close-pair report.

Synthetic, no gpkg.

        park = box(0, 0, 100, 100)
        BAK (named, Baker Creek):  (50,-50) ▶ (50,150)   leaves the park at m=50, enters it at m=150
        TRB (descendant):          (20,-20) ▶ (20,120)   leaves at m=20, enters at m=120
"""

import pytest
from shapely.geometry import LineString, box

from pipeline.atlas.graph import cutting
from pipeline.atlas.graph.blk_chains import FidRow, build_blk_chains
from pipeline.atlas.graph.graph import build_section_geometries, build_stream_graph
from pipeline.atlas.splits.anchors import resolve_split_defs, same_entry_conflicts
from pipeline.atlas.splits.sectionizer import split_graph_at
from pipeline.common.models import AnchorType, SplitAnchor, SplitDef, SplitPoint

PARK = box(0, 0, 100, 100)
WSC = "100-458476"


def _fid(fid, blk, wsc, coords, gnis_id=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk="", gnis_id=gnis_id,
                  gnis_name="", stream_order=1, stream_magnitude=1,
                  down_m=0.0, up_m=geom.length, geometry=geom, down_node=dn, up_node=un)


def _park_chains():
    fids = [_fid("BAK1", "BAK", WSC, [(50, -50), (50, 150)], gnis_id="7956"),
            _fid("TRB1", "TRB", WSC + "-1", [(20, -20), (20, 120)], gnis_id="9999")]
    return build_blk_chains(fids, {})


def _area(sid="up", entry="BAKER CREEK", **kw):
    return SplitDef(id=sid, wsc=WSC, label="Pinnacles Park upstream boundary", entry=entry,
                    anchor=SplitAnchor(type=AnchorType.area_boundary, area_layer="parks_bc",
                                       area_name_field="PROTECTED_LANDS_NAME", area_name="PINNACLES PARK",
                                       wsc_descendants=True, **kw))


def _at(pts):
    return sorted((p.blk, round(p.route_measure, 3)) for p in pts)


# ----------------------------------------------------------------- 1. park boundary ends

def test_an_area_split_without_side_cuts_both_ends():
    pts = resolve_split_defs([_area()], _park_chains(), area_polys={"PINNACLES PARK": PARK})
    assert _at(pts) == [("BAK", 50.0), ("BAK", 150.0), ("TRB", 20.0), ("TRB", 120.0)]


@pytest.mark.parametrize("side,want", [("upstream", 150.0), ("downstream", 50.0)])
def test_side_picks_one_end_on_the_named_line_and_tributaries_keep_both(side, want):
    """Baker Creek / Pinnacles: the "upstream boundary" split also cut the downstream end, 0.03 m from
    the entry's own "downstream boundary" point. `side` takes ONE end of the park stretch on the named
    line; a descendant keeps both so its inside flag stays closed at both."""
    pts = resolve_split_defs([_area(side=side)], _park_chains(), area_polys={"PINNACLES PARK": PARK})
    assert _at(pts) == [("BAK", want), ("TRB", 20.0), ("TRB", 120.0)]


def test_exclude_gnis_leaves_a_named_water_to_its_own_split():
    """Tweedsmuir: the Atnarko crosses the park once, at the entry's "eastern boundary" point."""
    pts = resolve_split_defs([_area(exclude_gnis=("7956",))], _park_chains(),
                             area_polys={"PINNACLES PARK": PARK})
    assert _at(pts) == [("TRB", 20.0), ("TRB", 120.0)]


def test_side_and_exclude_are_area_boundary_only():
    with pytest.raises(ValueError, match="side"):
        SplitAnchor.from_dict({"type": "point", "coord": [0, 0], "side": "upstream"})
    with pytest.raises(ValueError, match="side must be"):
        SplitAnchor.from_dict({"type": "area_boundary", "area_layer": "x", "area_name": "y",
                               "side": "up"})
    with pytest.raises(ValueError, match="exclude_gnis"):
        SplitAnchor.from_dict({"type": "point", "coord": [0, 0], "exclude_gnis": ["1"]})


# ----------------------------------------------------------------- 1b. one entry, one point = error

def _down_point(entry="BAKER CREEK"):
    # the Baker "downstream boundary" point, 0.03 m above the park's downstream crossing
    return SplitDef(id="down", gnis_id="7956", label="Pinnacles Park downstream boundary", entry=entry,
                    anchor=SplitAnchor(type=AnchorType.point, coord=(50.0, 0.03)))


def test_two_cuts_of_one_entry_on_one_point_is_an_error():
    with pytest.raises(ValueError, match=r"BAKER CREEK: down ~ up \(0\.03 m, blk BAK\)"):
        resolve_split_defs([_area(), _down_point()], _park_chains(), area_polys={"PINNACLES PARK": PARK})


def test_the_side_fix_clears_it():
    pts = resolve_split_defs([_area(side="upstream"), _down_point()], _park_chains(),
                             area_polys={"PINNACLES PARK": PARK})
    assert ("BAK", 50.03) in _at(pts) and ("BAK", 150.0) in _at(pts)


def test_two_entries_may_share_a_point():
    """The Thompson mouth is named by the Fraser entry AND the Thompson entry: not an error."""
    pts = resolve_split_defs([_area(), _down_point(entry="OTHER WATER")], _park_chains(),
                             area_polys={"PINNACLES PARK": PARK})
    assert same_entry_conflicts([_area(), _down_point(entry="OTHER WATER")], pts) == []


def test_load_split_defs_records_the_entry(tmp_path):
    import json
    from pipeline.atlas.splits.splits import load_split_defs
    p = tmp_path / "splits.json"
    p.write_text(json.dumps({"waterbodies": [{"name": "BAKER CREEK", "applies_to": {"gnis_id": "7956"},
                                              "splits": [{"id": "s", "anchor": {"type": "point",
                                                                                "coord": [0, 0]}}]}]}))
    assert load_split_defs(str(p))[0].entry == "BAKER CREEK"


# ----------------------------------------------------------------- 2. the border from the regions (Moyie)

def test_moyie_us_border_and_the_region_4_line_are_one_boundary():
    """B.C. = Region 4 = box(0,0,1000,1000) — the outline IS the regions' outer edge
    (`bc_boundary`: the exact union of the units they dissolve from). The Moyie runs north from the
    U.S. and crosses it at m=100. The curated "U.S. border" was a hand point 12 m inside (the live
    atlas: 42628.3 vs the line at 42640.55); as a `border` anchor it lands ON the border cut, and the
    region cut follows the border, so one boundary carries all three names."""
    from pipeline.atlas.splits.area_splits import resolve_area_splits
    from pipeline.atlas.splits.border import border_split_points

    region4 = box(0, 0, 1000, 1000)
    outline = region4
    fids = [_fid("MOY1", "MOY", "300-1", [(500, -100), (500, 900)], gnis_id="15783")]
    chains = build_blk_chains(fids, {})
    graph = build_stream_graph(chains, fids, {}, {})
    geoms = build_section_geometries(chains, fids, {})

    border = border_split_points(chains, outline)                       # the border pass, first
    assert _at(border) == [("MOY", 100.0)]
    split_graph_at(graph, geoms, border)
    border_cuts = {p.blk: [p.route_measure] for p in border}

    hand = SplitDef.from_dict({"id": "moyie_river__u_s_border", "gnis_id": "15783", "label": "U.S. border",
                               "anchor": {"type": "point", "coord": [500, 12]}})
    assert _at(resolve_split_defs([hand], chains)) == [("MOY", 112.0)], "the old hand point: 12 m off"

    sd = SplitDef.from_dict({"id": "moyie_river__u_s_border", "gnis_id": "15783", "label": "U.S. border",
                             "entry": "MOYIE RIVER", "anchor": {"type": "border", "coord": [500, 12]}})
    pts = resolve_split_defs([sd], chains, border_cuts=border_cuts)
    assert _at(pts) == [("MOY", 100.0)] and pts[0].source == "curated"
    split_graph_at(graph, geoms, pts)

    region = resolve_area_splits({"4": region4}, chains, term="Region",
                                 border=(outline.boundary, border_cuts))
    assert _at(region) == [("MOY", 100.0)], "the region line IS the border here"
    split_graph_at(graph, geoms, region)

    pieces = sorted((n for n in graph.nodes.values() if n.blk == "MOY"), key=lambda n: n.down_m)
    assert len(pieces) == 2, "one cut, not two 12 m apart"
    b = pieces[0].upper_bound
    assert b.boundary_id == "split:moyie_river__u_s_border" and b.label == "U.S. border"
    assert b.kind.value == "split", "a curated border cut is a named cut; `border` kind is the minted outline's"
    assert {"split:border:MOY:0", "split:area:4"} <= set(b.aliases)
    assert pieces[1].lower_bound == b


def test_a_border_anchor_needs_a_target_and_a_border_cut():
    with pytest.raises(ValueError, match="requires a target"):
        SplitDef.from_dict({"id": "b", "anchor": {"type": "border"}})
    sd = SplitDef.from_dict({"id": "b", "gnis_id": "7956", "anchor": {"type": "border"}})
    assert resolve_split_defs([sd], _park_chains(), border_cuts={}) == []


def test_a_border_anchor_with_a_coord_takes_the_nearest_crossing():
    """The Kootenay crosses twice: the coord picks one."""
    chains = _park_chains()
    sd = SplitDef.from_dict({"id": "b", "gnis_id": "7956", "anchor": {"type": "border", "coord": [50, 140]}})
    pts = resolve_split_defs([sd], chains, border_cuts={"BAK": [50.0, 150.0]})
    assert _at(pts) == [("BAK", 150.0)]
    sd = SplitDef.from_dict({"id": "b", "gnis_id": "7956", "anchor": {"type": "border"}})
    assert _at(resolve_split_defs([sd], chains, border_cuts={"BAK": [50.0, 150.0]})) == \
        [("BAK", 50.0), ("BAK", 150.0)]


# ----------------------------------------------------------------- 3. the close-pair report

def _line_graph():
    fids = [_fid("X1", "X", "100", [(0, 0), (300, 0)])]
    return build_stream_graph(build_blk_chains(fids, {}), fids)


def _cut(g, sid, m, source, anchor=AnchorType.point):
    split_graph_at(g, {}, [SplitPoint(sid, "X", m, "", sid, anchor, source=source)])


def test_close_pairs_reports_curated_cuts_5_to_25_m_from_any_boundary(tmp_path):
    """A curated cut 12 m from a border cut (the Moyie before), one 8 m from the line's own end, one
    20 m from another curated cut; a gauge 30 m off is out of the band, and only curated cuts report."""
    import json
    from pipeline.atlas.splits.close_pairs import close_pairs, report

    g = _line_graph()
    _cut(g, "border:X:0", 100.0, "border", AnchorType.border)
    _cut(g, "us_border", 112.0, "curated")
    _cut(g, "near_mouth", 8.0, "curated")
    _cut(g, "a", 200.0, "curated")
    _cut(g, "b", 220.0, "curated")
    _cut(g, "gauge__X", 250.0, "gauge", AnchorType.gauge)
    rows = {r["curated"]: r for r in close_pairs(g)}
    us = rows["split:us_border"]
    assert (us["other"], us["other_kind"], us["distance_m"]) == ("split:border:X:0", "border", 12.0)
    assert (rows["split:near_mouth"]["other_kind"], rows["split:near_mouth"]["distance_m"]) == ("line_end", 8.0)
    assert rows["split:a"]["other"] == "split:b" and rows["split:b"]["other"] == "split:a"
    assert set(rows) == {"split:us_border", "split:near_mouth", "split:a", "split:b"}
    report(g, tmp_path)
    assert len(json.loads((tmp_path / "close_pairs.json").read_text())) == 4


def test_a_merged_place_is_not_a_pair():
    """The Moyie after: the curated cut ON the border cut is one boundary, nothing to report."""
    from pipeline.atlas.splits.close_pairs import close_pairs

    g = _line_graph()
    _cut(g, "border:X:0", 100.0, "border", AnchorType.border)
    _cut(g, "us_border", 100.0, "curated", AnchorType.border)
    assert close_pairs(g) == []
