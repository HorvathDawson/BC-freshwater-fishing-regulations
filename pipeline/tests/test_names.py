"""(name, source) tuple tests (03 S2) — side-channel name inheritance."""

from pipeline.common.models import BlkChain, NameSource, NameTuple
from pipeline.atlas.graph.names import load_name_variants, resolve_names


def _chain(blk, wsc, gnis_id="", gnis_name="", order=1, mag=1):
    nts = (NameTuple(gnis_name, NameSource.gazette),) if gnis_name else ()
    return BlkChain(blk=blk, fwa_watershed_code=wsc, fids=(), geometry=None, mouth_measure=0.0,
                    length_m=1.0, name_tuples=nts, gnis_id=gnis_id, gnis_name=gnis_name,
                    stream_order=order, stream_magnitude=mag)


def _names(chain):
    return {(t.name, t.source.value) for t in chain.name_tuples}


def test_side_channel_inherits_main_name_directionally():
    # Same WSC: the SMALLER channel (Blind Slough) inherits the bigger mainstem's name; the mainstem
    # (Stave River) must NOT pick up the side channel's name — else both names become ambiguous.
    stave = _chain("S", "100-1", gnis_id="14589", gnis_name="Stave River", order=5, mag=100)
    blind = _chain("B", "100-1", gnis_id="13674", gnis_name="Blind Slough", order=2, mag=10)
    by_blk = {c.blk: c for c in resolve_names([stave, blind], [], curated_blks=())}

    assert ("Stave River", "side-channel") in _names(by_blk["B"])      # slough inherits mainstem
    assert ("Blind Slough", "side-channel") not in _names(by_blk["S"])  # mainstem does NOT inherit slough
    assert ("Stave River", "gazette") in _names(by_blk["S"])


def test_unnamed_side_channel_still_inherits():
    # An UNNAMED braided segment sharing the WSC still inherits the main channel's name (+gnis) so the
    # registry can group the whole river.
    main = _chain("M", "100-2", gnis_id="7551", gnis_name="Pitt River", order=6, mag=200)
    braid = _chain("X", "100-2", gnis_id="", gnis_name="", order=1, mag=3)
    by_blk = {c.blk: c for c in resolve_names([main, braid], [], curated_blks=())}
    tup = [t for t in by_blk["X"].name_tuples if t.source == NameSource.side_channel]
    assert tup and tup[0].name == "Pitt River" and tup[0].gnis_id == "7551"


# --- T497: follow connections (owner 2026-11-08) ------------------------------------------------------

def _fid(blk, wsc, down_node, up_node, down_m=0.0, up_m=1.0, gnis_id="", gnis_name=""):
    from pipeline.atlas.graph.blk_chains import FidRow
    return FidRow(fid=f"{blk}@{down_m}", blk=blk, wsc=wsc, edge_type="1000", wbk="", gnis_id=gnis_id,
                  gnis_name=gnis_name, stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m,
                  geometry=None, down_node=down_node, up_node=up_node)


def _side(chain):
    return [(t.name, t.gnis_id) for t in chain.name_tuples if t.source == NameSource.side_channel]


def _lower_fraser():
    """Fraser F (main) A-B-C-D; Nicomen Slough N off it B-E-G-C. Unnamed X1 E-P and X2 P-G: a CHAIN of two
    unnamed pieces whose outer ends both land on the slough. Unnamed Y A-Q and Z Q-E: a chain from the Fraser
    to the slough (a connector, T510). Unnamed W R-S: touches nothing named."""
    w = "100-0"
    chains = [_chain("F", w, gnis_id="39325", gnis_name="Fraser River", order=9, mag=9000),
              _chain("N", w, gnis_id="21105", gnis_name="Nicomen Slough", order=3, mag=40),
              *(_chain(b, w, mag=2) for b in ("X1", "X2", "Y", "Z", "W"))]
    fids = [_fid("F", w, "A", "B", 0, 1), _fid("F", w, "B", "C", 1, 2), _fid("F", w, "C", "D", 2, 3),
            _fid("N", w, "B", "E", 0, 1), _fid("N", w, "E", "G", 1, 2), _fid("N", w, "G", "C", 2, 3),
            _fid("X1", w, "E", "P"), _fid("X2", w, "P", "G"),
            _fid("Y", w, "A", "Q"), _fid("Z", w, "Q", "E"),
            _fid("W", w, "R", "S")]
    return chains, fids


def test_a_chain_of_unnamed_pieces_whose_ends_land_on_the_slough_belongs_to_the_slough():
    chains, fids = _lower_fraser()
    by_blk = {c.blk: c for c in resolve_names(chains, fids, curated_blks=())}
    assert _side(by_blk["X1"]) == [("Nicomen Slough", "21105")]
    assert _side(by_blk["X2"]) == [("Nicomen Slough", "21105")]


def test_nothing_named_keeps_the_magnitude_rule_and_a_named_water_is_untouched():
    chains, fids = _lower_fraser()
    by_blk = {c.blk: c for c in resolve_names(chains, fids, curated_blks=())}
    assert _side(by_blk["W"]) == [("Fraser River", "39325")]   # reaches nothing named
    assert _side(by_blk["N"]) == [("Fraser River", "39325")]   # a NAMED water is untouched by the rule


def test_a_connector_from_the_main_river_to_one_slough_is_the_sloughs():
    """T510 rule A (owner 2026-11-08): the chain Y-Z has one end ONLY on the Fraser and the other ONLY on
    Nicomen Slough, so both pieces are the slough's (Strawberry Slough's connector, blk 355991900)."""
    chains, fids = _lower_fraser()
    by_blk = {c.blk: c for c in resolve_names(chains, fids, curated_blks=())}
    assert _side(by_blk["Y"]) == [("Nicomen Slough", "21105")]
    assert _side(by_blk["Z"]) == [("Nicomen Slough", "21105")]


def test_a_channel_touching_both_waters_at_one_end_stays_with_the_main_river():
    """T510: K runs from B, where the Fraser and the slough meet, to the slough at G. Its B end reaches both, so
    it says nothing; one end on the slough alone is not a connector. The magnitude rule stays."""
    chains, fids = _lower_fraser()
    w = "100-0"
    chains.append(_chain("K", w, mag=2))
    fids.append(_fid("K", w, "B", "G"))
    by_blk = {c.blk: c for c in resolve_names(chains, fids, curated_blks=())}
    assert _side(by_blk["K"]) == [("Fraser River", "39325")]


def test_a_channel_on_the_main_river_at_both_ends_stays_with_it():
    chains, fids = _lower_fraser()
    w = "100-0"
    chains.append(_chain("L", w, mag=2))
    fids.append(_fid("L", w, "A", "D"))
    by_blk = {c.blk: c for c in resolve_names(chains, fids, curated_blks=())}
    assert _side(by_blk["L"]) == [("Fraser River", "39325")]


def test_a_connector_into_a_curated_water_is_left_to_curation():
    """T510: Jeperson Side Channel's connectors (blk 355994564, 355992188) are curation's call, not the rule's."""
    chains, fids = _lower_fraser()
    by_blk = {c.blk: c for c in resolve_names(chains, fids, curated_blks={"Z"})}
    assert _side(by_blk["Y"]) == [("Fraser River", "39325")]   # Fraser at one end, the curated Z at the other


def test_a_connector_whose_other_end_reaches_two_waters_stays_with_the_main_river():
    w = "100-0"
    chains = [_chain("F", w, gnis_id="1", gnis_name="Big River", mag=900),
              _chain("N", w, gnis_id="2", gnis_name="North Slough", mag=40),
              _chain("S", w, gnis_id="3", gnis_name="South Slough", mag=30),
              _chain("X", w, mag=2)]
    fids = [_fid("F", w, "a", "y"), _fid("N", w, "b", "n"), _fid("S", w, "b", "s"), _fid("X", w, "a", "b")]
    by_blk = {c.blk: c for c in resolve_names(chains, fids, curated_blks=())}
    assert _side(by_blk["X"]) == [("Big River", "1")]


def test_two_different_named_waters_reached_is_ambiguous_and_falls_back():
    w = "100-0"
    chains = [_chain("F", w, gnis_id="1", gnis_name="Big River", mag=900),
              _chain("N", w, gnis_id="2", gnis_name="North Slough", mag=40),
              _chain("S", w, gnis_id="3", gnis_name="South Slough", mag=30),
              _chain("X", w, mag=2)]
    fids = [_fid("N", w, "a", "b"), _fid("S", w, "c", "d"), _fid("X", w, "b", "c"), _fid("F", w, "y", "z")]
    by_blk = {c.blk: c for c in resolve_names(chains, fids, curated_blks=())}
    assert _side(by_blk["X"]) == [("Big River", "1")]


def test_a_branch_off_the_middle_does_not_move_a_channel_whose_ends_are_on_the_slough():
    """Baldy Channel (Columbia) 356364764: both ends on the channel, and an unnamed branch leaving its MIDDLE
    runs to the Columbia. The channel is judged by its own ends. The branch has one end on the main river
    only and the other (through C) on the slough only: a connector (T510 rule A), so it is the slough's."""
    chains, fids = _lower_fraser()
    w = "100-0"
    chains += [_chain("C", w, mag=2), _chain("BR", w, mag=2)]
    fids += [_fid("C", w, "E", "M", 0, 1), _fid("C", w, "M", "G", 1, 2), _fid("BR", w, "M", "D")]
    by_blk = {c.blk: c for c in resolve_names(chains, fids, curated_blks=())}
    assert _side(by_blk["C"]) == [("Nicomen Slough", "21105")]
    assert _side(by_blk["BR"]) == [("Nicomen Slough", "21105")]   # through C to the slough, and on the Fraser


def test_a_channel_curation_names_is_left_to_its_curated_water():
    """Jeperson Side Channel: name_variants names several blks as one water. The rule never renames a
    curated blk, and a piece reaching one keeps the magnitude rule — a curated water is never split."""
    chains, fids = _lower_fraser()
    by_blk = {c.blk: c for c in resolve_names(chains, fids, curated_blks={"X1"})}
    assert _side(by_blk["X1"]) == [("Fraser River", "39325")]
    assert _side(by_blk["X2"]) == [("Fraser River", "39325")]    # reaches the curated X1 and the slough


def test_follow_connections_ignores_input_order():
    chains, fids = _lower_fraser()
    a = {c.blk: c.name_tuples for c in resolve_names(chains, fids, curated_blks=())}
    b = {c.blk: c.name_tuples for c in resolve_names(chains[::-1], fids[::-1], curated_blks=())}
    assert a == b


# --- minting nodes for waterbodies the graph never nodes -------------------------------------------

def test_mint_waterbody_nodes_gives_an_unnoded_water_a_section():
    """A waterbody is only noded when stream fids run THROUGH it, so an isolated lake (nothing flows
    in or out) and an overlaid wetland (the fids record it in member_wbks, not as a node) both ended
    up with an EMPTY section_ids — `op=whole` then resolved against an empty universe and the rule
    silently bound nothing. Minting gives the item exactly one section to answer with."""
    from pipeline.atlas.graph.names import mint_waterbody_nodes
    from pipeline.common.models import NameSource, NodeKind, StreamGraph

    g = StreamGraph()
    n = mint_waterbody_nodes(g, {"111": (("Frazer Lake", "27804"),)}, NameSource.gazette)
    assert n == 1
    node = g.nodes["lake:111"]
    assert node.kind == NodeKind.lake and node.wbk == "111"
    assert node.display_name == "Frazer Lake"
    assert node.name_tuples[0].gnis_id == "27804"      # so the item's ref_ids keep the gnis
    assert not g.up_adj.get("lake:111") and not g.down_adj.get("lake:111")   # edgeless by design


def test_mint_waterbody_nodes_never_overwrites_a_real_node():
    from pipeline.atlas.graph.names import mint_waterbody_nodes
    from pipeline.common.models import NameSource, NodeKind, StreamGraph, StreamNode

    g = StreamGraph()
    g.nodes["lake:111"] = StreamNode(node_id="lake:111", kind=NodeKind.lake, wbk="111",
                                     display_name="Real Lake")
    assert mint_waterbody_nodes(g, {"111": (("Other Name", ""),)}, NameSource.gazette) == 0
    assert g.nodes["lake:111"].display_name == "Real Lake"


def test_mint_waterbody_nodes_skips_the_unnamed():
    """The name is the whole point — a nameless polygon can never be targeted by a regulation, and
    minting one would just add an unmatchable item."""
    from pipeline.atlas.graph.names import mint_waterbody_nodes
    from pipeline.common.models import NameSource, StreamGraph

    g = StreamGraph()
    assert mint_waterbody_nodes(g, {"222": (("", "999"),)}, NameSource.gazette) == 0
    assert not g.nodes


def test_mint_waterbody_nodes_takes_the_longest_gazetted_name():
    """Matches what add_waterbody_items put on the item, so no display name churns when the water
    becomes a node instead."""
    from pipeline.atlas.graph.names import mint_waterbody_nodes
    from pipeline.common.models import NameSource, StreamGraph

    g = StreamGraph()
    mint_waterbody_nodes(g, {"333": (("Tsayta", "1"), ("Nation Lakes", "2"))}, NameSource.gazette)
    assert g.nodes["lake:333"].display_name == "Nation Lakes"
    assert {t.name for t in g.nodes["lake:333"].name_tuples} == {"Tsayta", "Nation Lakes"}


# --- T497 pin on the real lower Fraser (needs the FWA gpkg) ----------------------------------------------

import pytest  # noqa: E402


@pytest.fixture(scope="module")
def lower_fraser(request):
    import pipeline.atlas.build as b
    from pipeline.atlas.graph.blk_chains import build_blk_chains, load_stream_fids
    from pipeline.tests.conftest import need, GPKG_HINT
    need(request, "source", b._DEFAULT_GPKG, GPKG_HINT)
    bbox = (1238000.0, 464000.0, 1316000.0, 490000.0)      # Widgeon Slough (Pitt) to Maria Slough
    fwa = b.FWADataAccessor(b._DEFAULT_GPKG)
    lake_kind = b.get_lake_wbk_kind(fwa, bbox)
    fids = load_stream_fids(b._DEFAULT_GPKG, bbox=bbox)
    from pipeline.atlas.graph.prune import nv_blks
    from pipeline.tests import under_test
    curated = nv_blks(load_name_variants(under_test.curated().waters.name_variants))     # as the build passes them
    return {c.blk: c for c in resolve_names(build_blk_chains(fids, lake_kind), fids, curated_blks=curated)}


@pytest.mark.needs_source
@pytest.mark.parametrize("blk, gnis, name", [("355991891", "21105", "Nicomen Slough"),
                                             ("355992357", "13499", "Maria Slough")])
def test_a_channel_with_both_ends_on_its_slough_is_the_sloughs(lower_fraser, blk, gnis, name):
    """T497: both channels share the Fraser's watershed code; both ends sit on their slough. The magnitude
    rule filed them under the Fraser (gnis 39325) and gave them its night and steelhead closures."""
    assert _side(lower_fraser[blk]) == [(name, gnis)]


@pytest.mark.needs_source
@pytest.mark.parametrize("blk, gnis, name", [("355991900", "16210", "Strawberry Slough"),
                                             ("355993053", "27696", "Widgeon Slough")])
def test_a_connector_from_the_main_river_is_its_sloughs(lower_fraser, blk, gnis, name):
    """T510 rule A: an unnamed channel on the Fraser's (Pitt's) watershed code, one end only on the main river,
    the other only on the slough. The magnitude rule filed it under the river (night + steelhead closures)."""
    assert _side(lower_fraser[blk]) == [(name, gnis)]
