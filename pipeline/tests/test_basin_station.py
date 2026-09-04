"""Which station speaks for each catchment, and how far the reading travelled to get there.

A gauge measures the water leaving its catchment. For the catchment it stands in that is a
measurement; for one upstream it is a claim about the same weather over a larger area, and
weaker the further it goes. The whole point of `levels_up` is that the map is obliged to be
able to say which it is looking at.
"""

import pytest

from pipeline.atlas.gauges.basin_station import MAX_LEVELS_UP, parents, resolve
from pipeline.deliver.tiles.basins import basin_id


def test_the_padding_comes_off_or_there_is_no_hierarchy():
    """A code runs to twenty-one segments and the trailing 000000s are padding, not levels.
    Left on, every code looks the same depth and nothing has a parent."""
    assert basin_id("300-432687-380566-000000-000000") == "300-432687-380566"
    assert basin_id("300-000000-000000") == "300"
    assert basin_id("300") == "300"


def test_parents_run_tightest_first():
    assert list(parents("300-432687-380566")) == [
        "300-432687-380566", "300-432687", "300"]


def test_a_gauge_in_the_catchment_itself_is_zero_levels_up():
    got = resolve({"300-432687-380566": "S"}, ["300-432687-380566"])
    assert got == {"300-432687-380566": ("S", 0)}


def test_a_catchment_inherits_from_the_one_it_drains_into():
    """The reading is real and the claim is weaker — the distance is reported, not hidden."""
    got = resolve({"300-432687": "S"}, ["300-432687-380566"])
    assert got == {"300-432687-380566": ("S", 1)}


def test_the_tightest_ancestor_wins():
    """A gauge two levels up must not speak over one that is one level up."""
    got = resolve({"300": "FAR", "300-432687": "NEAR"}, ["300-432687-380566"])
    assert got == {"300-432687-380566": ("NEAR", 1)}


def test_a_catchment_with_nothing_upstream_gets_no_row():
    """Absent, not zero and not a neighbour's number. 13% of the province, drawn as
    unmeasured, which is the honest answer there."""
    assert resolve({"400-111111": "S"}, ["300-432687-380566"]) == {}


def test_a_reading_that_has_travelled_too_far_is_refused():
    """Past MAX_LEVELS_UP, "the water here drains into the water measured there" has become
    "somewhere upstream of somewhere upstream of here"."""
    deep = "-".join(["300"] + [f"{i:06d}" for i in range(1, MAX_LEVELS_UP + 2)])
    assert resolve({"300": "S"}, [deep]) == {}
    # One level closer, and it is admitted.
    ok = "-".join(deep.split("-")[:-1])
    assert resolve({"300": "S"}, [ok])[ok] == ("S", MAX_LEVELS_UP)


def test_it_stops_at_the_first_ancestor_it_finds_even_if_that_is_too_far():
    """A nearer ancestor with no gauge does not let a further one through the cap: the walk
    reports the tightest TRUE answer, and refuses it if that is too far."""
    deep = "300-" + "-".join(f"{i:06d}" for i in range(1, 7))
    assert resolve({"300": "TOO FAR"}, [deep]) == {}


def test_every_basin_resolves_at_most_once():
    got = resolve({"300": "A", "300-000001": "B"},
                  ["300-000001-000002", "300-000001-000002"])
    assert len(got) == 1
    assert got["300-000001-000002"] == ("B", 1)
