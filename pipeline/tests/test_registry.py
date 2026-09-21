"""Unit tests for build_registry — synthetic StreamGraph, no FWA needed."""

from shapely.geometry import LineString, box

from pipeline.common.models import (
    BoundaryKind, NameSource, NameTuple, NodeKind, SectionBoundary, StreamGraph, StreamNode,
)
from pipeline.atlas.registry import add_mu_sets, add_waterbody_items, build_registry


def _node(nid, *, blk="", wsc="", gnis="", name="", tuples=(), in_areas=(),
          lo=None, hi=None, kind=NodeKind.stream, wbk=""):
    return StreamNode(node_id=nid, kind=kind, blk=blk, wbk=wbk, wsc=wsc, gnis_id=gnis,
                      display_name=name, name_tuples=tuples, in_areas=in_areas,
                      lower_bound=lo, upper_bound=hi)


def _reg():
    g = StreamGraph()
    falls = SectionBoundary("split:foo_falls", BoundaryKind.split, 100.0, "Foo Falls")
    lake_b = SectionBoundary("lake:W1", BoundaryKind.lake, 200.0, "Bar Lake")
    tx = (NameTuple("River X", NameSource.gazette, gnis_id="1"),)
    g.nodes["m1"] = _node("m1", blk="M", wsc="100", gnis="1", name="River X", tuples=tx, hi=falls)
    g.nodes["m2"] = _node("m2", blk="M", wsc="100", gnis="1", name="River X", tuples=tx,
                          lo=falls, hi=lake_b, in_areas=("GARIBALDI PARK",))
    # side channel: NO own gnis, inherits gnis via a side-channel name tuple -> must join gnis:1
    g.nodes["s1"] = _node("s1", blk="S", wsc="100", name="River X",
                          tuples=(NameTuple("River X", NameSource.side_channel, gnis_id="1"),))
    g.nodes["lakeW"] = _node("lakeW", name="Bar Lake", kind=NodeKind.lake, wbk="W1",
                             tuples=(NameTuple("Bar Lake", NameSource.gazette),))
    return build_registry(g)


def test_river_is_one_item_via_tuple_gnis():
    reg = _reg()
    assert "gnis:1" in reg
    item = reg["gnis:1"]
    assert item.name == "River X" and item.kind == "stream"
    assert set(item.section_ids) == {"m1", "m2", "s1"}     # side channel unified via tuple gnis


def test_readable_boundaries():
    reg = _reg()
    bids = {b.id: b for b in reg["gnis:1"].boundaries}
    # curated split keeps its (already-unique) id; auto lake is item-prefixed for global uniqueness
    assert "foo_falls" in bids and bids["foo_falls"].ref == "split:foo_falls"
    assert "river_x__bar_lake" in bids
    lk = bids["river_x__bar_lake"]
    assert lk.kind == "lake" and lk.wbk == "W1" and lk.label == "Bar Lake"


def test_lake_and_area_items():
    reg = _reg()
    assert reg["wbk:W1"].kind == "lake" and reg["wbk:W1"].name == "Bar Lake"
    assert "area:garibaldi_park" in reg
    area = reg["area:garibaldi_park"]
    assert area.kind == "area" and set(area.section_ids) == {"m2"}


def test_add_mu_sets():
    reg = _reg()
    geoms = {"m1": LineString([(0, 0), (10, 0)]), "m2": LineString([(10, 0), (30, 0)]),
             "s1": LineString([(0, 1), (5, 1)])}
    mu_polys = {"2-1": box(-1, -2, 15, 2), "2-2": box(15, -2, 40, 2)}
    add_mu_sets(reg, geoms, mu_polys)
    assert set(reg["gnis:1"].mus) == {"2-1", "2-2"}          # river spans both MUs


def test_add_mu_sets_isolated_lake_uses_own_polygon():
    # An isolated lake (no graph node -> no section geometry) still gets its MUs, computed from its
    # OWN wbk polygon rather than skipped. This is the source-level fix for no-MU lakes/wetlands.
    reg = _reg()
    add_waterbody_items(reg, {"329459820": (("Frazer Lake", "12484"),)}, "lake")
    assert reg["wbk:329459820"].section_ids == ()           # isolated: no section geometry
    mu_polys = {"5-4": box(0, 0, 10, 10), "5-5": box(10, 0, 20, 10)}
    wbk_polys = {"329459820": box(8, 2, 12, 6)}             # straddles both MUs
    add_mu_sets(reg, {}, mu_polys, wbk_polys)
    assert set(reg["wbk:329459820"].mus) == {"5-4", "5-5"}


def test_variants_exclude_foreign_side_channel_name():
    # A side channel (Blind Slough) carries the mainstem's name as a side-channel tuple whose gnis
    # (14589) is FOREIGN to this item (13674). That borrowed name must NOT become a searchable variant
    # (else 'Stave River' resolves to both items).
    g = StreamGraph()
    g.nodes["b1"] = _node("b1", blk="B", wsc="100", gnis="13674", name="Blind Slough",
                          tuples=(NameTuple("Blind Slough", NameSource.gazette, gnis_id="13674"),
                                  NameTuple("Stave River", NameSource.side_channel, gnis_id="14589")))
    reg = build_registry(g)
    item = reg["gnis:13674"]
    assert "Blind Slough" in item.variants
    assert "Stave River" not in item.variants            # foreign side-channel name excluded


def test_nameless_channel_keeps_inherited_name_searchable():
    # A channel with NO name of its OWN (only a foreign side-channel tuple) must keep that inherited
    # name as a searchable variant — else it has no search key at all and drops out of the registry.
    g = StreamGraph()
    g.nodes["x1"] = _node("x1", blk="X", wsc="100", gnis="99", name="",
                          tuples=(NameTuple("Stave River", NameSource.side_channel, gnis_id="14589"),))
    reg = build_registry(g)
    item = reg["gnis:99"]
    assert item.variants == ("Stave River",)             # inherited name kept (no own name to prefer)


def test_lake_ref_ids_exclude_river_codes():
    # A lake node carries the through-river's wsc/blk; those must NOT leak into the lake item's
    # ref_ids (a stream override's wsc/blk would otherwise false-match the lake).
    g = StreamGraph()
    g.nodes["lk"] = _node("lk", name="Ballon Lake", kind=NodeKind.lake, wbk="W9",
                          wsc="100-458399", blk="999",
                          tuples=(NameTuple("Ballon Lake", NameSource.gazette, gnis_id="18257"),))
    reg = build_registry(g)
    refs = set(reg["wbk:W9"].ref_ids)
    assert refs == {"wbk:W9", "gnis:18257"}                  # gnis + wbk ONLY, no wsc:/blk:


def test_add_waterbody_items_wetland():
    reg = _reg()
    wet = {
        "W1": (("Bar Marsh", "700"),),                        # collides with existing lake wbk -> skipped
        "329291857": (("Minnekhada Marsh", "5001"), ("", "")),
    }
    add_waterbody_items(reg, wet, "wetland")
    assert reg["wbk:W1"].kind == "lake"                       # not overwritten
    item = reg["wbk:329291857"]
    assert item.kind == "wetland" and item.name == "Minnekhada Marsh"
    assert set(item.ref_ids) == {"wbk:329291857", "gnis:5001"}


def test_add_curated_wbk_items_names_isolated_unnamed_lake():
    # A name_variants entry targets an isolated, FWA-unnamed wbk (no node, not added by
    # add_waterbody_items). add_curated_wbk_items mints an item so the curated name is matchable.
    reg = _reg()
    nv = [{"target": {"wbks": ["329343983", "329343806"]},
           "names": [{"name": "Redstart Lake", "source": "regulation", "display": True}]}]
    from pipeline.atlas.registry import add_curated_wbk_items
    add_curated_wbk_items(reg, nv)
    it = reg["wbk:329343983"]
    assert it.kind == "lake" and it.name == "Redstart Lake"
    assert set(it.ref_ids) == {"wbk:329343983"}          # wbk pin resolves a waterbody_keys override


def test_add_curated_wbk_items_skips_existing():
    reg = _reg()                                          # has wbk:W1 (Bar Lake)
    from pipeline.atlas.registry import add_curated_wbk_items
    add_curated_wbk_items(reg, [{"target": {"wbks": ["W1"]},
                                 "names": [{"name": "Renamed", "source": "regulation"}]}])
    assert reg["wbk:W1"].name == "Bar Lake"              # existing item not overwritten


def test_add_waterbody_items_isolated_lake():
    # An isolated named lake (no through-stream -> no graph node) is added from the layer as kind=lake,
    # carrying its gnis in ref_ids so a curated gnis override resolves to it (Frazer Lake case).
    reg = _reg()
    add_waterbody_items(reg, {"329459820": (("Frazer Lake", "12484"),)}, "lake")
    item = reg["wbk:329459820"]
    assert item.kind == "lake" and item.name == "Frazer Lake"
    assert set(item.ref_ids) == {"wbk:329459820", "gnis:12484"}


# --------------------------------------------------------------------------- #
# A NAMED side channel is its own water, not a reach of the mainstem
# (McArthur Island Slough was folded into the Thompson and took its name)
# --------------------------------------------------------------------------- #

def _named_channel_graph():
    """River X, plus a channel that has NO gnis of its own but a curated name, plus a gazetted reach
    of River X that curation renamed — the two cases `split_distinct_names` must tell apart."""
    g = StreamGraph()
    tx = (NameTuple("River X", NameSource.gazette, gnis_id="1"),)
    g.nodes["m1"] = _node("m1", blk="M", wsc="100", gnis="1", name="River X", tuples=tx)
    g.nodes["m2"] = _node("m2", blk="M", wsc="100", gnis="1", name="River X", tuples=tx)
    # inherits River X's gnis off a side-channel tuple, but curation gave it its OWN name
    g.nodes["c1"] = _node("c1", blk="C", wsc="100", name="Quiet Slough",
                          tuples=(NameTuple("River X", NameSource.side_channel, gnis_id="1"),
                                  NameTuple("Quiet Slough", NameSource.regulation)))
    # a gazetted reach of River X (owns gnis 1) that curation renamed — NOT a separate water
    g.nodes["r1"] = _node("r1", blk="M", wsc="100", gnis="1", name="The Narrows",
                          tuples=tx + (NameTuple("The Narrows", NameSource.regulation),))
    return build_registry(g)


def test_named_channel_without_own_gnis_becomes_its_own_item():
    reg = _named_channel_graph()
    assert "blk:C" in reg, "a curated-named channel must not be folded into the mainstem"
    ch = reg["blk:C"]
    assert ch.name == "Quiet Slough" and set(ch.section_ids) == {"c1"}
    assert "River X" not in ch.variants, "the borrowed mainstem name must not alias the channel"
    assert "c1" not in reg["gnis:1"].section_ids


def test_renamed_reach_that_owns_the_gnis_stays_on_the_mainstem():
    reg = _named_channel_graph()
    item = reg["gnis:1"]
    assert set(item.section_ids) == {"m1", "m2", "r1"}
    assert item.name == "River X", "the mainstem keeps its own name, not a longer member's"
    assert "The Narrows" in item.variants, "the renamed reach stays searchable on the river"


def test_item_name_is_not_the_longest_member_name():
    """The Fraser was displayed as 'Seabird Island North Side Channel' because the item name was the
    longest display name of any member node."""
    g = StreamGraph()
    tx = (NameTuple("River X", NameSource.gazette, gnis_id="1"),)
    g.nodes["m1"] = _node("m1", blk="M", wsc="100", gnis="1", name="River X", tuples=tx)
    g.nodes["r1"] = _node("r1", blk="M", wsc="100", gnis="1", name="A Very Long Reach Name", tuples=tx)
    assert build_registry(g)["gnis:1"].name == "River X"


def test_foreign_gnis_name_is_not_a_searchable_alias():
    """A reg name attached by a name_variants entry keyed to the MAINSTEM's gnis lands on every
    inheriting side channel too — 'FRASER RIVER' aliased all 15 Fraser channel items."""
    g = StreamGraph()
    g.nodes["m1"] = _node("m1", blk="M", wsc="100", gnis="1", name="River X",
                          tuples=(NameTuple("River X", NameSource.gazette, gnis_id="1"),
                                  NameTuple("RIVER X", NameSource.regulation, gnis_id="1")))
    # a side channel WITH its own gnis, carrying the mainstem's reg name off the same variant entry
    g.nodes["s1"] = _node("s1", blk="S", wsc="100", gnis="2", name="Side Channel",
                          tuples=(NameTuple("Side Channel", NameSource.gazette, gnis_id="2"),
                                  NameTuple("RIVER X", NameSource.regulation, gnis_id="1")))
    reg = build_registry(g)
    assert "RIVER X" in reg["gnis:1"].variants
    assert "RIVER X" not in reg["gnis:2"].variants


def test_a_lakes_other_gazetted_name_stays_searchable():
    """FWA gives a waterbody up to three gazetted names (GNIS_NAME_1/2/3), each with its own id —
    Nation Lakes also answers to 'Tsayta Lake'. Those are the lake's OWN names, not borrowed ones."""
    g = StreamGraph()
    g.nodes["l1"] = _node("l1", kind=NodeKind.lake, wbk="W9", name="Nation Lakes",
                          tuples=(NameTuple("Nation Lakes", NameSource.gazette, gnis_id="16576"),
                                  NameTuple("Tsayta Lake", NameSource.gazette, gnis_id="29218")))
    item = build_registry(g)["wbk:W9"]
    assert set(item.variants) == {"Nation Lakes", "Tsayta Lake"}
    assert "gnis:29218" in item.ref_ids


def test_boundary_aliases_survive_the_registry_round_trip(tmp_path):
    """The alias is minted in the graph but consumed by the review app through registry.json. Leaving
    it out of the serializer meant the split resolved in the build and was still dangling in the app."""
    from pipeline.common.models.registry import RegistryBoundary, RegistryItem
    from pipeline.atlas.registry.io import load_registry, write_registry

    item = RegistryItem(
        id="gnis:1", name="Duncan River", kind="stream",
        section_ids=("10:0",),
        boundaries=(
            RegistryBoundary(id="duncan_river__duncan_lake", label="Duncan Lake", kind="lake",
                             ref="lake:329120714", wbk="329120714",
                             aliases=("split:duncan_river__duncan_dam",)),
            RegistryBoundary(id="plain", label="Plain", kind="split", ref="split:plain"),
        ),
    )
    path = write_registry({"gnis:1": item}, tmp_path / "registry.json")
    back = load_registry(path)["gnis:1"]
    assert back.boundaries[0].aliases == ("split:duncan_river__duncan_dam",)
    assert back.boundaries[1].aliases == (), "a boundary with no alias round-trips as empty"

    import json
    raw = json.loads(path.read_text())["items"][0]["boundaries"]
    assert "aliases" not in raw[1], "empty aliases must not bloat the file"


def test_an_alias_on_either_edge_of_a_lake_reaches_the_registry():
    """A lake is the UPPER bound of the piece below it and the LOWER bound of the piece above, so it
    reaches build_registry twice — but the alias sits on only ONE of those two instances. Keeping the
    first and skipping the rest dropped the alias whenever the un-aliased edge came first, which is
    how Duncan Dam, the Mitchell dam and the Babine weir stayed dangling after being aliased."""
    from pipeline.common.models import BoundaryKind, NodeKind, SectionBoundary, StreamGraph, StreamNode
    from pipeline.atlas.registry.build import build_registry

    def graph_with_alias_on(which: str) -> StreamGraph:
        plain = SectionBoundary(boundary_id="lake:99", kind=BoundaryKind.lake, route_measure=1000.0,
                                label="Duncan Lake")
        aliased = SectionBoundary(boundary_id="lake:99", kind=BoundaryKind.lake, route_measure=5000.0,
                                  label="Duncan Lake", aliases=("split:duncan_river__duncan_dam",))
        lower, upper = (aliased, plain) if which == "lower" else (plain, aliased)
        below = StreamNode(node_id="10:0", kind=NodeKind.stream, blk="10", wsc="100", gnis_id="1",
                           display_name="Duncan River", down_m=0.0, up_m=1000.0, length_m=1000.0,
                           upper_bound=lower)
        above = StreamNode(node_id="10:5000", kind=NodeKind.stream, blk="10", wsc="100", gnis_id="1",
                           display_name="Duncan River", down_m=5000.0, up_m=6000.0, length_m=1000.0,
                           lower_bound=upper)
        g = StreamGraph()
        g.nodes = {n.node_id: n for n in (below, above)}
        g.edges, g.up_adj, g.down_adj = [], {}, {}
        return g

    for which in ("lower", "upper"):                 # the alias must survive from EITHER side
        reg = build_registry(graph_with_alias_on(which))
        item = next(it for it in reg.values() if it.kind == "stream")
        lake = next(b for b in item.boundaries if b.ref == "lake:99")
        assert lake.aliases == ("split:duncan_river__duncan_dam",), f"lost when alias was on {which}"


# --------------------------------------------------------------------------------------------
# A WATERBODY AN OVERRIDE NAMES BY KEY IS NOT NAMELESS.
#
# The registry keeps named waters only — right, or the 1.7M nameless stream pieces bloat it to
# ~458MB. But "nameless" was decided from FWA and the curated name file alone, and a curator
# writing waterbody keys into overrides.json is a third naming authority the check never asked.
#
# The Bluey Lake potholes: the book closes nine, FWA names none, two carry stocking names and so
# became items. The override listed all nine and could bind the two that existed, so a **No
# Fishing** closure covered two ninths of its water — and looked resolved, because two bound.

def _pothole_graph():
    g = StreamGraph()
    for i, w in enumerate(("W7", "W8", "W9")):
        g.nodes[f"p{i}"] = _node(f"p{i}", name="", kind=NodeKind.lake, wbk=w, tuples=())
    return g


def test_nameless_waterbody_is_dropped_without_a_pin():
    """The existing policy, unchanged: nothing names these, so they are not registry items."""
    reg = build_registry(_pothole_graph(), pinned={})
    assert [k for k in reg if k.startswith("wbk:")] == []


def test_an_override_pin_keeps_a_nameless_waterbody_and_names_it():
    reg = build_registry(_pothole_graph(),
                         pinned={"wbk:W7": "UNNAMED LAKES (north and south of Bluey Lake)",
                                 "wbk:W9": "UNNAMED LAKES (north and south of Bluey Lake)"})
    assert set(k for k in reg if k.startswith("wbk:")) == {"wbk:W7", "wbk:W9"}
    assert reg["wbk:W7"].name == "UNNAMED LAKES (north and south of Bluey Lake)"
    # W8 is not pinned and stays dropped — the pin is a whitelist, not a blanket "keep unnamed".
    assert "wbk:W8" not in reg


def test_several_polygons_under_one_heading_share_the_name():
    """They are ONE regulated water with several polygons, which is how the override describes
    them — so they take one name and the entry binds every piece."""
    nm = "UNNAMED LAKES (north and south of Bluey Lake)"
    reg = build_registry(_pothole_graph(), pinned={"wbk:W7": nm, "wbk:W8": nm, "wbk:W9": nm})
    assert {reg[k].name for k in ("wbk:W7", "wbk:W8", "wbk:W9")} == {nm}


def test_a_pin_never_overrides_a_real_name():
    g = StreamGraph()
    g.nodes["L"] = _node("L", name="Bar Lake", kind=NodeKind.lake, wbk="W1",
                         tuples=(NameTuple("Bar Lake", NameSource.gazette),))
    reg = build_registry(g, pinned={"wbk:W1": "SOMETHING ELSE"})
    assert reg["wbk:W1"].name == "Bar Lake"


def test_a_displaced_lake_edge_is_still_bindable_by_the_name_rules_use():
    """**The other half of the Sproat bug.** A pickup relabels the boundary it reuses and carries
    the displaced ids forward. For a split that is enough — `split:x` is `x` with a prefix and the
    lookup tries both spellings. For a lake it was not: the ref is `lake:W1`, a wbk nobody writes,
    while every rule binds `river_x__bar_lake` — the id THIS function derives from the label.

    So a kept lake edge came back unbindable. `_pickup` now sends the label along and `_expand`
    spells it here, where the readable-id rule already lives.
    """
    from pipeline.atlas.reach.classify import _cut_exists
    g = StreamGraph()
    tx = (NameTuple("River X", NameSource.gazette, gnis_id="1"),)
    # the dam won the pickup at the lake edge; the lake survives only as an alias
    dam = SectionBoundary("split:river_x__bar_lake_dam", BoundaryKind.split, 200.0, "Bar Lake dam",
                          aliases=("lake:W1", "label:Bar Lake"))
    g.nodes["m1"] = _node("m1", blk="M", wsc="100", gnis="1", name="River X", tuples=tx, hi=dam)
    reg = build_registry(g)

    aliases = {b.id: b.aliases for b in reg["gnis:1"].boundaries}["river_x__bar_lake_dam"]
    assert "river_x__bar_lake" in aliases, "the id a rule binds must survive the relabel"
    assert not [a for a in aliases if a.startswith("label:")], "label: is consumed, never shipped"
    assert _cut_exists("river_x__bar_lake", ["gnis:1"], reg, set()) is True
    assert _cut_exists("river_x__bar_lake_dam", ["gnis:1"], reg, set()) is True
