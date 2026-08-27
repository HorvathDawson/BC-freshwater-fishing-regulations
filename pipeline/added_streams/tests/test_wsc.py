"""mint_wsc — the FWA proportional-distance watershed-code rule."""

from pipeline.added_streams.wsc import mint_wsc, proportion_segment
from pipeline.utils.wsc import trim_wsc


def test_spec_stream_b_example():
    # FWA/WSA spec: Stream B joins Stream A (100-115004) 528.8 m up a 2479.5 m stream -> 21.33% -> 213300
    assert proportion_segment(528.8, 2479.5) == "213300"
    assert mint_wsc("100-115004", 528.8, 2479.5) == "100-115004-213300"


def test_extends_and_prefix_descends_a_real_parent():
    parent = "100-019698-933296"
    code = mint_wsc(parent, 100.0, 1000.0)          # 10% -> 100000
    assert code == "100-019698-933296-100000"
    assert code.startswith(trim_wsc(parent))         # a proper prefix-descendant


def test_segment_is_six_digits_and_clamped():
    assert proportion_segment(0.0, 1000.0) == "000000"
    assert proportion_segment(1000.0, 1000.0) == "999999"      # source end clamps, never 7 digits
    assert proportion_segment(-5.0, 1000.0) == "000000"        # negative clamps to 0
    assert len(proportion_segment(123.4, 1000.0)) == 6


def test_degenerate_receiver_length():
    assert proportion_segment(50.0, 0.0) == "000000"
    assert mint_wsc("", 5.0, 10.0) == "500000"                 # no real parent -> bare segment
