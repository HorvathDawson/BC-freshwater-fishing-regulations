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
