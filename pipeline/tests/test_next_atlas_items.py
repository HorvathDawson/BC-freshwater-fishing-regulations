"""The small items that ride the next atlas (2026-11-12): T606 item 2 (T547 §3 clash suffixes by place, §5 a lake's
river names), T560 item 3 (one owner of a shared watershed code), T560 item 5 (one "meets at" test, one area slug).
Synthetic graphs only, no FWA needed; each rule is mutation-pinned by its module flag or by the function it replaced."""
import pytest

from pipeline.atlas.registry import build as RB
from pipeline.atlas.registry import build_registry
from pipeline.common.models import BoundaryKind, NameSource, NameTuple, NodeKind, SectionBoundary, StreamGraph, StreamNode


@pytest.fixture(autouse=True)
def _no_pfma_layer_needed(monkeypatch):
    from pipeline.atlas.splits import pfma_drainage
    monkeypatch.setattr(pfma_drainage, "basin_areas", lambda *a, **kw: {})


def _node(nid, *, blk="", wsc="", gnis="", name="", tuples=(), lo=None, hi=None, kind=NodeKind.stream, wbk="",
          down=0.0, up=0.0, mag=None):
    return StreamNode(node_id=nid, kind=kind, blk=blk, wbk=wbk, wsc=wsc, gnis_id=gnis, display_name=name,
                      name_tuples=tuples, lower_bound=lo, upper_bound=hi, down_m=down, up_m=up,
                      stream_magnitude=mag)


def _lake(wbk, m):
    return SectionBoundary(f"lake:{wbk}", BoundaryKind.lake, m, "Nanaimo Lakes")


def _two_lake_river(order):
    """River X (gnis 1) runs through two lakes both labelled "Nanaimo Lakes": W_LOW at 100-200 m, W_HIGH at 400 m.
    `order` is the order the graph holds its pieces in."""
    tx = (NameTuple("River X", NameSource.gazette, gnis_id="1"),)
    pieces = {
        "p0": _node("p0", blk="M", wsc="100", gnis="1", name="River X", tuples=tx, hi=_lake("W_LOW", 100.0), up=100.0),
        "p1": _node("p1", blk="M", wsc="100", gnis="1", name="River X", tuples=tx, lo=_lake("W_LOW", 200.0),
                    hi=_lake("W_HIGH", 400.0), down=200.0, up=400.0),
        "p2": _node("p2", blk="M", wsc="100", gnis="1", name="River X", tuples=tx, lo=_lake("W_HIGH", 500.0),
                    down=500.0, up=900.0),
    }
    g = StreamGraph()
    for k in order:
        g.nodes[k] = pieces[k]
    return {b.id: b.ref for b in build_registry(g, pinned={})["gnis:1"].boundaries}


def test_a_lake_clash_suffix_follows_the_boundarys_place_not_the_node_order(monkeypatch):
    """T547 §3: `river_x__nanaimo_lakes_2` is the upper lake whatever order the graph holds its pieces in; a rule
    binding the `_2` id keeps meaning the same lake. MUTATION: numbering by node order (the old rule) swaps them."""
    want = {"river_x__nanaimo_lakes": "lake:W_LOW", "river_x__nanaimo_lakes_2": "lake:W_HIGH"}
    assert _two_lake_river(["p0", "p1", "p2"]) == want
    assert _two_lake_river(["p2", "p1", "p0"]) == want
    monkeypatch.setattr(RB, "CLASH_SUFFIX_BY_PLACE", False)
    assert _two_lake_river(["p2", "p1", "p0"]) == {"river_x__nanaimo_lakes": "lake:W_HIGH",
                                                   "river_x__nanaimo_lakes_2": "lake:W_LOW"}


def test_a_curated_split_keeps_its_id_before_any_lake_edge_is_numbered():
    """A curated split's id is the curated one: an auto lake id that spells the same never takes it."""
    tx = (NameTuple("River X", NameSource.gazette, gnis_id="1"),)
    lake = SectionBoundary("lake:W", BoundaryKind.lake, 100.0, "Bar Lake")
    split = SectionBoundary("split:river_x__bar_lake", BoundaryKind.split, 300.0, "Bar Lake outlet")
    g = StreamGraph()
    g.nodes["a"] = _node("a", blk="M", wsc="100", gnis="1", name="River X", tuples=tx, hi=lake, up=100.0)
    g.nodes["b"] = _node("b", blk="M", wsc="100", gnis="1", name="River X", tuples=tx, lo=lake, hi=split,
                         down=100.0, up=300.0)
    got = {b.id: b.ref for b in build_registry(g, pinned={})["gnis:1"].boundaries}
    assert got == {"river_x__bar_lake": "split:river_x__bar_lake", "river_x__bar_lake_2": "lake:W"}


def _blackwater(monkeypatch=None):
    """gnis 26104's primary is 'West Road (Blackwater) River'; FWA hangs 'BLACKWATER RIVER' / 'West Road River' (the
    LAKE's own gazette tuples) on Tsacha Lake, which the river threads."""
    g = StreamGraph()
    g.nodes["r"] = _node("r", blk="R", wsc="100", gnis="26104", name="West Road (Blackwater) River",
                         tuples=(NameTuple("West Road (Blackwater) River", NameSource.gazette, gnis_id="26104"),
                                 NameTuple("West Road River", NameSource.alias, gnis_id="26104"),
                                 NameTuple("BLACKWATER RIVER", NameSource.alias, gnis_id="26104")))
    g.nodes["lk"] = _node("lk", kind=NodeKind.lake, wbk="329126691", name="Tsacha Lake",
                          tuples=(NameTuple("Tsacha Lake", NameSource.gazette, gnis_id="900"),
                                  NameTuple("West Road River", NameSource.gazette, gnis_id="900"),
                                  NameTuple("BLACKWATER RIVER", NameSource.gazette, gnis_id="900")))
    return build_registry(g, pinned={})


def test_a_lake_does_not_answer_to_a_river_name_that_is_not_the_rivers_primary(monkeypatch):
    """T547 §5: a lake drops a flowing name any stream item owns, not only a stream's primary name.
    MUTATION: primaries only (the old check) leaves both river names on the lake."""
    reg = _blackwater()
    assert reg["wbk:329126691"].variants == ("Tsacha Lake",)
    assert "West Road River" in reg["gnis:26104"].variants              # the river keeps them
    monkeypatch.setattr(RB, "LAKE_CHECKS_EVERY_STREAM_NAME", False)
    assert set(_blackwater()["wbk:329126691"].variants) == {"Tsacha Lake", "West Road River", "BLACKWATER RIVER"}


def test_one_owner_of_a_shared_code_by_magnitude_then_lowest_blk():
    """T560 item 3: the registry's last-resort grouping asks the same rule the side-channel naming does — the biggest
    named line on the code, the lowest blk on a tie — never the first gnis met. MUTATION: the old `setdefault` picks
    the slough met first."""
    from pipeline.common.utils.wsc import code_owner
    g = StreamGraph()
    g.nodes["sl"] = _node("sl", blk="20", wsc="100", gnis="7", name="A Slough", mag=3)
    g.nodes["fr"] = _node("fr", blk="30", wsc="100", gnis="1", name="Big River", mag=900)
    g.nodes["tie"] = _node("tie", blk="25", wsc="200", gnis="8", name="B Creek", mag=5)
    g.nodes["tie2"] = _node("tie2", blk="24", wsc="200", gnis="9", name="C Creek", mag=5)
    assert RB._wsc_gnis_map(g) == {"100": "1", "200": "9"}
    old = {}
    for n in g.nodes.values():
        old.setdefault(n.wsc, n.gnis_id)
    assert old["100"] == "7"                                            # what first-met answered
    assert code_owner([], lambda x: x, lambda x: x) is None


def test_the_side_channel_naming_asks_the_same_owner():
    """`graph.names.resolve_names` takes its main channel from `code_owner` (no private `max`)."""
    import inspect

    from pipeline.atlas.graph import names
    src = inspect.getsource(names.resolve_names)
    assert "code_owner(siblings" in src and "max(siblings" not in src


def test_meets_at_is_float_identity_on_one_line():
    """T560 item 5: one "two pieces meet at this bound" test for the reach and the braid sweep."""
    from pipeline.common.tolerances import SAME_PLACE_M, meets_at
    cut = SectionBoundary("split:c", BoundaryKind.split, 100.0, "c")
    a = _node("a", blk="M", hi=cut, down=0.0, up=100.0)
    b = _node("b", blk="M", lo=cut, down=100.0, up=200.0)
    assert meets_at(a, b) is cut and meets_at(b, a) is cut
    assert meets_at(a, _node("c", blk="N", down=100.0, up=200.0)) is None          # another line
    assert meets_at(a, _node("d", blk="M", down=100.0 + 10 * SAME_PLACE_M, up=200.0)) is None
    assert meets_at(None, b) is None


def test_the_three_severed_tests_have_no_private_tolerance():
    import inspect

    from pipeline.atlas.reach import extent
    from pipeline.atlas.splits import braid_sweep
    for mod in (extent, braid_sweep):
        src = inspect.getsource(mod)
        assert "abs(na.up_m - nb.down_m)" not in src and "meets_at(" in src, mod.__name__


def test_one_area_slug():
    """T560 item 5: the catalog's `area:{kind}:{slug}`, the registry's ids and the review's `area_slug` are one
    function (they were three byte-identical copies)."""
    from pipeline.atlas.splits import area_catalog
    from pipeline.common.split_ids import readable_slug
    from pipeline.regs.review import split_use
    assert area_catalog._slug is readable_slug and RB._slug is readable_slug
    assert split_use.area_slug("GARIBALDI  PARK") == "garibaldi_park" == readable_slug("Garibaldi -- Park")
    assert area_catalog.area_id("watershed", "Liard River") == "area:watershed:liard_river"
