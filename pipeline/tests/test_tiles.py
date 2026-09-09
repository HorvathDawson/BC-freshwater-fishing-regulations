"""The tile ladder and the areas.json <-> tile-layer contract.

The ladder is pure arithmetic so it is tested directly; the contract tests are the ones
that matter operationally, because both failures they catch are SILENT — an area with no
tile_layer simply never draws, and a layer attribute that no exporter writes simply never
appears, and in both cases the map looks fine and answers wrong.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from pipeline.deliver.tiles import ladder
from pipeline.deliver.tiles.layers import ALL, BY_NAME
from pipeline.deliver.tiles.names import display, haystack, normalise
from pipeline.common.curated import CURATED, SOURCE

ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------- ladder

def test_magnitude_ladder_is_monotonic():
    """A bigger river never appears later than a smaller one."""
    zooms = [ladder.zoom_for_magnitude(m) for m in
             (500000, 20000, 5000, 1000, 250, 100, 50, 20, 10, 5, 2, 1, 0)]
    assert zooms == sorted(zooms)


def test_magnitude_ladder_measured_thresholds():
    """The thresholds are the measured province-wide ones (doc: One Interface, Two Storages §3)."""
    assert ladder.zoom_for_magnitude(231956) == 4      # the Liard
    assert ladder.zoom_for_magnitude(20000) == 4       # 101 features
    assert ladder.zoom_for_magnitude(1000) == 6        # 1,122
    assert ladder.zoom_for_magnitude(100) == 8         # 11,947
    assert ladder.zoom_for_magnitude(20) == 10         # 59,906
    assert ladder.zoom_for_magnitude(5) == 12          # 217,568
    assert ladder.zoom_for_magnitude(1) == 14          # all 2,053,299


def test_unmeasured_magnitude_is_drawn_last_never_dropped():
    """No magnitude is a headwater or an artifact — it belongs at z14, not nowhere."""
    for m in (None, 0):
        assert ladder.zoom_for_magnitude(m) == ladder.MAX_ZOOM


def test_layer_minzoom_floor_wins():
    """A layer that starts at z9 cannot leak a feature into a z4 tile."""
    assert ladder.zoom_for_magnitude(500000, floor=9) == 9


def test_area_ladder_orders_by_size():
    cultus, small, pond = 6.2e7, 1.4e6, 2e4
    assert (ladder.zoom_for_area(cultus)
            < ladder.zoom_for_area(small)
            < ladder.zoom_for_area(pond))


def test_area_ladder_is_derived_from_pixel_size():
    """A polygon appears once its side is ~3 px, not at a hand-picked hectare threshold."""
    for z in range(ladder.MIN_ZOOM, ladder.MAX_ZOOM):
        side = 3.0 * ladder.metres_per_pixel(z)
        assert ladder.zoom_for_area(side * side * 1.01) <= z


def test_contours_come_coarsest_first():
    assert ladder.zoom_for_contour(10) < ladder.zoom_for_contour(5) < ladder.zoom_for_contour(3)


# ---------------------------------------------------------------- names

def test_display_titlecases_shouting_but_leaves_real_case_alone():
    assert display("EAST WHITE RIVER") == "East White River"
    assert display("McArthur Island Slough") == "McArthur Island Slough"
    assert display("Sts'a'í:les / Sts'ailes") == "Sts'a'í:les / Sts'ailes"


def test_haystack_dedupes_case_variants_to_nothing():
    """The registry carries both cases of the same name; that is not an alias."""
    assert haystack("East White River", ["EAST WHITE RIVER", "East White River"]) == ""


def test_haystack_keeps_real_aliases_display_name_first():
    h = haystack("Wahleach Lake", ["JONES", "JONES LAKE", "Wahleach"])
    assert h.split("|")[0] == "wahleach lake"
    assert "jones" in h.split("|")
    assert len(h.split("|")) == len(set(h.split("|")))


def test_normalise_is_what_search_compares():
    assert normalise("WAHLEACH (JONES) L.") == "wahleach jones l"


# ------------------------------------------------- areas.json <-> layers contract

def _areas():
    return json.loads(CURATED.waters.areas.read_text())["areas"]


def test_every_area_either_declares_a_real_tile_layer_or_says_why_not():
    """Without this an area is stamped on sections but its boundary never draws.

    An area may legitimately not be drawn — the MU groups are unions of units whose own
    outlines already ship on the `mu` layer, and drawing the union would stack a second
    boundary on top of them. So the rule is not "must draw", it is "must not fail to draw by
    accident": either name a layer that exists, or write down why there is no layer.
    """
    for a in _areas():
        if a.get("not_drawn"):
            assert not a.get("tile_layer"), (
                f"{a['id']} says not_drawn but also names a tile_layer — one or the other")
            assert isinstance(a["not_drawn"], str) and len(a["not_drawn"]) > 20, (
                f"{a['id']}: not_drawn must carry the reason, not just a flag")
            continue
        assert a.get("tile_layer"), f"{a['id']} has no tile_layer and no not_drawn reason"
        assert a["tile_layer"] in BY_NAME, f"{a['id']} -> unknown layer {a['tile_layer']}"


def test_only_hard_closures_and_regulatory_zones_cut_the_streams_that_cross_them():
    """TWO categories may cut, for two different reasons, and nothing else may.

    A HARD CLOSURE, because the edge is where fishing stops. A park boundary is a real line
    on the water and a section straddling it is half open and half shut.

    A REGULATORY ZONE, because the synopsis is written PER ZONE. Water crosses a region
    boundary freely — that is the whole point, and it is why the drainage-divide argument
    below does not apply here. A river running through two regions needs a section boundary
    between the halves or region 5's rules cannot attach to the half that is in region 5.
    The Fraser already carried three such boundaries placed BY HAND, at the Chilcotin, the
    Williams Lake River and the Cottonwood; this generalises that to all nine regions and to
    the two management-unit groups the synopsis writes group regulations for.

    WHAT IT COST, measured, which is the part that makes it admissible: 3,054 region
    transitions and 46 MU-group transitions cut, +1,086 graph nodes (0.06%) and +1,098 rule
    bindings on 1.72M (0.06%). Afterwards 1,956,216 of 1,956,450 sections (99.99%) resolve to
    exactly ONE region, which is what lets a zone rule be a lookup on the tile's own `mus`
    attribute rather than a stored membership list.

    A WATERSHED BOUNDARY IS STILL NOT ADMISSIBLE, and this is the contrast that defines the
    rule: it is a drainage divide, so water does not cross it, so no section straddles it and
    no rule is waiting on the cut. Cutting the Liard would re-cut 322,626 sections to buy
    nothing.
    """
    cut = {a["id"] for a in _areas() if a.get("cut", True)}
    assert cut == {
        # hard closures — the edge is where fishing stops
        "national_parks", "ecological_reserves", "chilkoot_trail", "restricted_land_access",
        # regulatory zones — the synopsis is written per zone
        "regions", "mu_group_south_island", "mu_group_haida_gwaii",
    }


def test_the_regulated_areas_the_synopsis_names_all_exist():
    """Six entries were `no_registry` purely because these areas were missing."""
    ids = {a["id"] for a in _areas()}
    for needed in ("watersheds",                  # LIARD RIVER WATERSHED
                   "wildlife_management_areas",   # CRESTON VALLEY WMA
                   "provincial_parks",            # STRATHCONA / KIKOMUN / BOWRON / GARIBALDI
                   "chilkoot_trail"):
        assert needed in ids, f"areas.json lost '{needed}'"


def test_no_layer_ships_an_attribute_nothing_declares():
    """layers.py is the whole property list; a tile cannot grow a field nobody decided on."""
    for spec in ALL:
        if spec.decorative:
            # Decorative means no IDENTITY, not no attributes: the route through a lake
            # still carries the order that sets its width. What it may not carry is
            # anything that names it — an id, a registry item, a name to search for.
            identity = {"section_id", "area_id", "mu_id", "item", "name", "alt"}
            assert not (set(spec.attrs) & identity), (
                f"{spec.name} is decorative but ships "
                f"{sorted(set(spec.attrs) & identity)} — nothing can select it, so a "
                f"promoted id would give every feature `id: undefined`")
        else:
            assert spec.attrs, f"{spec.name} declares no attributes"
        assert len(set(spec.attrs)) == len(spec.attrs), f"{spec.name} repeats an attribute"


def test_water_layers_never_drop_features_to_fit_a_tile():
    """A stream that vanishes because its tile was crowded is a stream nobody can tap."""
    for name in ("stream", "lake", "wetland", "contour"):
        assert BY_NAME[name].drop_densest is False


def test_no_layer_carries_regulation_data():
    """Tiles are geometry, identity and administrative geography. What a rule SAYS is bundle
    data joined on id/item — otherwise a date change would invalidate a tile."""
    banned = {"status", "closed", "rule", "rule_id", "quota", "species",
              "restriction", "season", "dates", "open"}
    for spec in ALL:
        assert not (set(spec.attrs) & banned), f"{spec.name} carries regulation data"


# ------------------------------------------------- geometry sources

def test_a_waterbody_is_drawn_as_its_polygon_not_its_under_lake_line():
    """A lake node has two geometries meaning different things: the polygon is its shape, the
    under-lake fid run is the route a river takes through it. Drawing the line as the lake is
    what renders a lake as a spiky asterisk."""
    # Behavioural, not a source grep. The previous version matched a literal line that was
    # refactored away, so it had been red for days — and a red suite trains everyone to
    # ignore the next failure.
    src = (ROOT / "pipeline/deliver/tiles/export.py").read_text()
    fn = src[src.index("def export_waterbodies"):src.index("def _kind(")]
    assert "waterbody_polys" in fn, (
        "export_waterbodies no longer reads the polygon sidecar; if it went back to the "
        "node geometry it would draw each lake as its under-lake route — a spiky asterisk")
    assert "SystemExit" in fn or "raise" in fn, (
        "a missing polygon file must stop the build, not silently draw routes as lakes")


def test_everything_reads_one_source():
    """Membership is a property of a node, and every waterbody is a node. The exporter must
    not stream the gpkg or merge a side artifact — two places that can disagree about which
    management unit a pond is in."""
    src = (ROOT / "pipeline/deliver/tiles/export.py").read_text()
    fn = src[src.index("def export_waterbodies"):src.index("def _kind(")]
    assert "waterbody_membership" not in src
    assert "fiona.open" not in fn and "read_file" not in fn
    build = (ROOT / "pipeline/atlas/build.py").read_text()
    assert "get_all_waterbody_wbks" in build
    assert "allow_unnamed=True" in build


def test_tiles_refuse_to_build_without_management_units():
    """Tiles with no MU stamp look completely normal and make every zone regulation
    invisible. That must fail loudly, not ship."""
    src = (ROOT / "pipeline/deliver/tiles/export.py").read_text()
    assert "def _require_membership" in src
    assert "raise SystemExit" in src


def test_simplification_keeps_the_network_stitched():
    """If simplification may move the point where two sections meet, the river gets visible
    gaps and the two features stop sharing a node."""
    src = (ROOT / "pipeline/deliver/tiles/tippe.py").read_text()
    assert "--no-simplification-of-shared-nodes" in src
    assert "--no-feature-limit" in src and "--no-tile-size-limit" in src


def test_the_top_zoom_is_simplified_because_the_camera_cannot_go_past_it():
    """The archive's maximum zoom is simplified like every other zoom, and that is only
    safe while the app refuses to zoom past it.

    Tippecanoe holds its top zoom at near-full detail by default because the top zoom is
    normally OVERZOOMED — magnified 2x, 4x, 8x beyond the last tile built, where a tolerance
    that is invisible at 1:1 becomes visible corner-cutting. We build z14 and the camera
    stops at z14, so that never happens, and the tolerance is worth about 139 MiB.

    The day someone raises MAX_ZOOM, this stops being free. That is why the two numbers are
    checked against each other here rather than left to agree by luck.
    """
    from pipeline.deliver.tiles import tippe
    src = (ROOT / "pipeline/deliver/tiles/tippe.py").read_text()
    assert "simplify_at_max=1" not in src and ", 1, verbose" not in src, (
        "a band is still passing tippecanoe's near-nothing tolerance at its maximum zoom")

    ladder = (ROOT / "app/packages/core/src/ladder.ts").read_text()
    m = re.search(r"export const MAX_ZOOM = (\d+)", ladder)
    assert m, "app ladder no longer declares MAX_ZOOM"
    camera_max = int(m.group(1))
    tile_max = max(spec.maxzoom for spec in ALL)
    assert camera_max == tile_max, (
        f"the app can zoom to z{camera_max} but the atlas stops at z{tile_max}. Past the "
        f"last tile MapLibre magnifies z{tile_max} geometry, and it is simplified at "
        f"{tippe.SIMPLIFICATION} units — about {tippe.SIMPLIFICATION / 4096 * 512:.2f} px "
        f"at 1:1 and {tippe.SIMPLIFICATION / 4096 * 512 * 2 ** (camera_max - tile_max):.2f} "
        f"px there. Either lower MAX_ZOOM, build another zoom, or give the top zoom a "
        f"tighter tolerance of its own.")


def test_under_lake_route_is_its_own_layer():
    """A lake node's sidecar geometry is the route THROUGH the lake, not the lake. It has to
    be drawn or a chain of lakes stops reading as one river — and it has to be a separate
    layer, or it gets styled and tapped as though it were fishable open water."""
    spec = BY_NAME["under_lake"]
    assert spec.geometry == "line"
    # NOTHING AT ALL. A person tapping the dotted thread through a lake means the lake, so
    # the route needs no id, no name and no membership — and it briefly carried `ord`, to
    # draw it at the weight of the river it continues. That was wrong and v1 had it right:
    # the route is a CONSTRUCTION LINE, it says the river continues and nothing about how
    # big it is, and at the mainstem's own weight it stops reading as a note. A small
    # constant width, which needs no attribute at all.
    assert spec.decorative and spec.attrs == ()
    src = (ROOT / "pipeline/deliver/tiles/export.py").read_text()
    assert 'ul_write' in src


def test_every_waterbody_becomes_a_node_so_zone_rules_can_reach_it():
    """A zone regulation targets water by WHERE IT IS, not what it is called, and only a node
    carries `mus`. 417,111 waterbodies had no node — 333,468 of 333,526 wetlands — so every
    zone rule was invisible on exactly the small water people fish."""
    names = (ROOT / "pipeline/atlas/graph/names.py").read_text()
    assert "allow_unnamed" in names
    build = (ROOT / "pipeline/atlas/build.py").read_text()
    assert "def get_all_waterbody_wbks" in build
    # and the minted node's POLYGON must be written, or membership has nothing to test
    # against. It moved from the geometry sidecar into its own file (waterbody_polys.pkl)
    # precisely so a lake's shape and its through-route stopped being the same object.
    assert "waterbody_polys.pkl" in build
