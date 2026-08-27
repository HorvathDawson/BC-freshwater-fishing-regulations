"""validate_propagation — every added stream roots at a real FWA drainage or 900, WSC nesting intact."""

from pipeline.hack.added_streams.validate import validate_propagation


def _s(blk, wsc, kind, rblk="", rwsc="", klass="novel", name="x"):
    return {"blk": blk, "wsc": wsc, "receiver_kind": kind, "receiver_blk": rblk,
            "receiver_wsc": rwsc, "klass": klass, "name": name}


def test_valid_fraser_chain_and_tidal():
    streams = [
        _s("-1", "100-100000-500000", "fwa", "1000", "100-100000"),          # novel off FWA (100)
        _s("-2", "100-100000-500000-484300", "added", "-1", "100-100000-500000"),  # trib of -1
        _s("-3", "900-359486", "tidal"),                                     # tidal root
    ]
    assert validate_propagation(streams) == []


def test_primary_not_inherited_is_flagged():
    # a stream claiming 200 (Mackenzie) while its receiver is 100 (Fraser)
    bad = [_s("-1", "200-100000-500000", "fwa", "1000", "100-100000")]
    v = validate_propagation(bad)
    assert v and any("primary" in m for m in v)


def test_wsc_not_prefix_descending_is_flagged():
    bad = [_s("-1", "100-999999", "fwa", "1000", "100-100000")]
    assert validate_propagation(bad)


def test_cycle_is_flagged():
    streams = [
        _s("-1", "100-1-1", "added", "-2", "100-1"),
        _s("-2", "100-1", "added", "-1", "100-1-1"),
    ]
    assert any("cycle" in m for m in validate_propagation(streams))


def test_dangling_receiver_flagged():
    assert validate_propagation([_s("-1", "100-1-1", "added", "-99", "100-1")])


def test_extension_must_reuse_fwa_blk_and_wsc():
    good = [_s("356294414", "100-019698", "fwa", "356294414", "100-019698", klass="extension")]
    assert validate_propagation(good) == []
    bad = [_s("-5", "100-019698", "fwa", "356294414", "100-019698", klass="extension")]
    assert any("extension" in m for m in validate_propagation(bad))


def test_tidal_must_be_coastal():
    assert validate_propagation([_s("-1", "100-100000", "tidal")])   # 100 is not coastal -> violation
