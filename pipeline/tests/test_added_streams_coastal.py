"""coastal — tidal WSC-900 estimation from the nearest coastal FWA stream, unique per added stream."""

from shapely.geometry import Point

from pipeline.added_streams.coastal import estimate_coastal_wsc, is_coastal_wsc, level12


def test_level12_and_is_coastal():
    assert level12("900-359486-123456-000000") == ("900", "359486")
    assert is_coastal_wsc("900-359486") and is_coastal_wsc("920-1")
    assert not is_coastal_wsc("100-019698")               # Fraser is not coastal-rooted


def test_estimates_from_nearest_coastal_stream():
    coastal = [(Point(0, 0), "900-359486"), (Point(1000, 0), "900-400000")]
    used: set[str] = set()
    assert estimate_coastal_wsc(Point(10, 0), coastal, used) == "900-359486"   # nearest wins


def test_colliding_mouths_get_distinct_codes():
    coastal = [(Point(0, 0), "900-359486")]
    used: set[str] = set()
    a = estimate_coastal_wsc(Point(5, 0), coastal, used)
    b = estimate_coastal_wsc(Point(6, 0), coastal, used)   # same nearest -> must bump
    assert a == "900-359486" and b == "900-359487" and a != b


def test_no_neighbour_falls_back_to_900():
    used: set[str] = set()
    assert estimate_coastal_wsc(Point(0, 0), [], used) == "900-000000"
