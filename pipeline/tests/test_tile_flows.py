"""The stream tiles say which way their lines run, and the steps that shape a section keep it.

The app draws flow arrows on the Conditions map (`stream__arrows`, owner 2026-10-21) and turns
the glyph to point AGAINST the line, because the tile contract says stream lines run MOUTH TO
SOURCE (`LayerSpec.flows`). These pin the claim at the two steps that make a section's line out
of FWA's: the route-measure cut and the run-join. Either one reversing a line would turn its
arrows upstream with nothing failing anywhere else.
"""
from shapely.geometry import LineString

from pipeline.atlas.graph import cutting
from pipeline.deliver.tiles import layers as L

# A blue line digitised mouth first, as FWA ships it: the mouth at x=0, the source at x=300.
BLUE = LineString([(0.0, 0.0), (100.0, 10.0), (200.0, 0.0), (300.0, 5.0)])


def test_the_contract_says_streams_run_mouth_to_source_and_only_lines_say_anything():
    flows = {name: spec["flows"] for name, spec in L.contract()["layers"].items() if spec["flows"]}
    assert flows == {"stream": "mouth_to_source"}
    assert all(L.BY_NAME[n].geometry == "line" for n in flows)


def test_a_route_measure_cut_keeps_the_mouth_first():
    # A chain whose mouth sits at route measure 1000: the section [1050, 1250] of it.
    piece = cutting.substring_cut(BLUE, 1000.0, 1050.0, 1250.0)
    first, last = piece.coords[0], piece.coords[-1]
    assert (round(first[0]), round(last[0])) == (50, 249)


def test_joining_a_section_s_fid_runs_keeps_the_mouth_first():
    lower = LineString([(0.0, 0.0), (100.0, 10.0)])
    upper = LineString([(100.0, 10.0), (200.0, 0.0)])
    joined = cutting.merge_runs([lower, upper])
    assert list(joined.coords) == [(0.0, 0.0), (100.0, 10.0), (200.0, 0.0)]
