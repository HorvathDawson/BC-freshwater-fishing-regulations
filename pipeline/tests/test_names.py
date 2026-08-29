"""(name, source) tuple tests (03 S2) — side-channel name inheritance."""

from pipeline.models import BlkChain, NameSource, NameTuple
from pipeline.graph.names import resolve_names


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
    by_blk = {c.blk: c for c in resolve_names([stave, blind])}

    assert ("Stave River", "side-channel") in _names(by_blk["B"])      # slough inherits mainstem
    assert ("Blind Slough", "side-channel") not in _names(by_blk["S"])  # mainstem does NOT inherit slough
    assert ("Stave River", "gazette") in _names(by_blk["S"])


def test_unnamed_side_channel_still_inherits():
    # An UNNAMED braided segment sharing the WSC still inherits the main channel's name (+gnis) so the
    # registry can group the whole river.
    main = _chain("M", "100-2", gnis_id="7551", gnis_name="Pitt River", order=6, mag=200)
    braid = _chain("X", "100-2", gnis_id="", gnis_name="", order=1, mag=3)
    by_blk = {c.blk: c for c in resolve_names([main, braid])}
    tup = [t for t in by_blk["X"].name_tuples if t.source == NameSource.side_channel]
    assert tup and tup[0].name == "Pitt River" and tup[0].gnis_id == "7551"


# --- minting nodes for waterbodies the graph never nodes -------------------------------------------

def test_mint_waterbody_nodes_gives_an_unnoded_water_a_section():
    """A waterbody is only noded when stream fids run THROUGH it, so an isolated lake (nothing flows
    in or out) and an overlaid wetland (the fids record it in member_wbks, not as a node) both ended
    up with an EMPTY section_ids — `op=whole` then resolved against an empty universe and the rule
    silently bound nothing. Minting gives the item exactly one section to answer with."""
    from pipeline.graph.names import mint_waterbody_nodes
    from pipeline.models import NameSource, NodeKind, StreamGraph

    g = StreamGraph()
    n = mint_waterbody_nodes(g, {"111": (("Frazer Lake", "27804"),)}, NameSource.gazette)
    assert n == 1
    node = g.nodes["lake:111"]
    assert node.kind == NodeKind.lake and node.wbk == "111"
    assert node.display_name == "Frazer Lake"
    assert node.name_tuples[0].gnis_id == "27804"      # so the item's ref_ids keep the gnis
    assert not g.up_adj.get("lake:111") and not g.down_adj.get("lake:111")   # edgeless by design


def test_mint_waterbody_nodes_never_overwrites_a_real_node():
    from pipeline.graph.names import mint_waterbody_nodes
    from pipeline.models import NameSource, NodeKind, StreamGraph, StreamNode

    g = StreamGraph()
    g.nodes["lake:111"] = StreamNode(node_id="lake:111", kind=NodeKind.lake, wbk="111",
                                     display_name="Real Lake")
    assert mint_waterbody_nodes(g, {"111": (("Other Name", ""),)}, NameSource.gazette) == 0
    assert g.nodes["lake:111"].display_name == "Real Lake"


def test_mint_waterbody_nodes_skips_the_unnamed():
    """The name is the whole point — a nameless polygon can never be targeted by a regulation, and
    minting one would just add an unmatchable item."""
    from pipeline.graph.names import mint_waterbody_nodes
    from pipeline.models import NameSource, StreamGraph

    g = StreamGraph()
    assert mint_waterbody_nodes(g, {"222": (("", "999"),)}, NameSource.gazette) == 0
    assert not g.nodes


def test_mint_waterbody_nodes_takes_the_longest_gazetted_name():
    """Matches what add_waterbody_items put on the item, so no display name churns when the water
    becomes a node instead."""
    from pipeline.graph.names import mint_waterbody_nodes
    from pipeline.models import NameSource, StreamGraph

    g = StreamGraph()
    mint_waterbody_nodes(g, {"333": (("Tsayta", "1"), ("Nation Lakes", "2"))}, NameSource.gazette)
    assert g.nodes["lake:333"].display_name == "Nation Lakes"
    assert {t.name for t in g.nodes["lake:333"].name_tuples} == {"Tsayta", "Nation Lakes"}
