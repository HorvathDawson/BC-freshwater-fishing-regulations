"""coastal — tidal WSC-900 estimation from the nearest coastal FWA stream, unique per added stream."""

from shapely.geometry import Point

from pipeline.atlas.waters.added_streams.coastal import estimate_coastal_wsc, fwa_level12_held, is_coastal_wsc, level12


def test_level12_and_is_coastal():
    assert level12("900-359486-123456-000000") == ("900", "359486")
    assert is_coastal_wsc("900-359486") and is_coastal_wsc("920-1")
    assert not is_coastal_wsc("100-019698")               # Fraser is not coastal-rooted


def test_estimates_from_nearest_coastal_stream():
    coastal = [(Point(0, 0), "900-359486"), (Point(1000, 0), "900-400000")]
    used: set[str] = set()
    assert estimate_coastal_wsc(Point(10, 0), coastal, used, set()) == "900-359486"   # nearest wins


def test_colliding_mouths_get_distinct_codes():
    coastal = [(Point(0, 0), "900-359486")]
    used: set[str] = set()
    a = estimate_coastal_wsc(Point(5, 0), coastal, used, set())
    b = estimate_coastal_wsc(Point(6, 0), coastal, used, set())   # same nearest -> must bump
    assert a == "900-359486" and b == "900-359487" and a != b


def test_no_neighbour_falls_back_to_900():
    used: set[str] = set()
    assert estimate_coastal_wsc(Point(0, 0), [], used, set()) == "900-000000"


def test_never_an_fwa_lines_code():
    """(T1246) Turner Creek took Noons Creek's own 900-057239: the nearest FWA stream's code is held by that stream (and
    any code an FWA line sits under), so the city creek is bumped past it, never filed as a part of that river."""
    fwa = ["900-057239", "900-057239-123456", "900-057240-000000-111111", "100-019698"]
    held = fwa_level12_held(fwa)
    assert held == {"900-057239", "900-057240"}
    coastal = [(Point(0, 0), "900-057239")]
    used: set[str] = set()
    assert estimate_coastal_wsc(Point(5, 0), coastal, used, held) == "900-057241"
