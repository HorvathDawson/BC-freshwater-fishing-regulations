"""T289: AN ADDED STREAM'S LAKE STUB MAKES A FALSE OUTLET. A city creek whose straight connector to its receiver
cuts the corner of a lake the receiver runs through, then leaves it by a short stub to the receiver BELOW the lake,
reads as creek -> lake -> stub -> receiver: a second lake outlet, so the lake-tributary walk skips it (Thomas Creek into
Burnaby Lake). `_lake_stub_confluence` re-joins such a connector to the receiver INSIDE the lake (a lake inlet, as
Lawson / Crayfish Creek join). No data: hand-made shapes."""
from shapely.geometry import LineString, Point, Polygon

from pipeline.atlas.waters.added_streams.build_dataset import _LAKE_IN_M, _lake_stub_confluence
from pipeline.atlas.waters.added_streams.underlake import LakeIndex

LAKE = Polygon([(0, 0), (1000, 0), (1000, 500), (0, 500)])
#: the receiver runs through the lake west -> east near its north shore and leaves it at x=0 (its outlet), on west
RIVER = LineString([(-300, 480), (0, 480), (1000, 480)])
IDX = LakeIndex([(LAKE, "lake")])


def test_a_connector_that_cuts_the_lake_and_stubs_out_to_the_outlet_joins_inside_the_lake():
    mouth = Point(60, 540)                          # the creek's mouth just north of the lake's NW corner
    confl = Point(-20, 480)                         # the receiver's nearest point: 20 m below its lake outlet
    # mouth -> confl cuts the lake's corner and leaves it 25 m before confl: Thomas Creek's shape
    got = _lake_stub_confluence(mouth, confl, RIVER, IDX)
    assert got is not None and LAKE.contains(got)
    assert RIVER.distance(got) < 1e-6 and got.x >= _LAKE_IN_M - 1e-6     # on the receiver, off its lake edge


def test_a_connector_already_inside_the_lake_is_left_alone():
    assert _lake_stub_confluence(Point(500, 600), Point(500, 480), RIVER, IDX) is None


def test_a_long_run_past_the_lake_is_a_real_connector():
    river = LineString([(-500, 480), (0, 480), (1000, 480)])
    assert _lake_stub_confluence(Point(60, 540), Point(-200, 300), river, IDX) is None    # far past the lake


def test_a_receiver_that_does_not_run_through_the_lake_is_left_alone():
    river = LineString([(-300, 600), (-20, 600), (-20, -200)])          # passes west of the lake, never in it
    assert _lake_stub_confluence(Point(60, 540), Point(-20, 480), river, IDX) is None
