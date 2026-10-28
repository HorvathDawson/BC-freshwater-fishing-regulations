"""T173 — the fish obstacles tile layer: what is kept, what is one obstacle, and that the app knows every type."""

import re
from pathlib import Path

from shapely.geometry import LineString

from pipeline.deliver.tiles import obstacles as O
from pipeline.deliver.tiles.layers import BY_NAME

ROOT = Path(__file__).resolve().parents[2]


def test_types_by_name_not_code():
    # `PD` is both "Private Dam" and "Persistent debris": mapped by name, one kept and one dropped
    got = [O.fiss_type(n) for n in ("Falls", "Cascade or Chute", "Private Dam", "Persistent debris; present for several years",
                                    "BEAVER DAM", "Log jam", "Rocks", "Velocity Barrier", "Culvert", "Weir", None)]
    assert got == ["falls", "cascade", "dam", None, None, None, None, "velocity", "culvert", "dam", None]
    assert set(O.FISS_TYPES.values()) == set(O.OBSTACLE_TYPES)


def test_heights_drop_placeholders():
    assert [O.height(v) for v in (4, "1.55", 0, -1000, 9999, float("nan"), None, 440)] == \
        [4.0, 1.6, None, None, None, None, None, 440.0]


def test_years():
    import pandas as pd
    assert [O.year_of(v) for v in (pd.Timestamp("2002-10-14"), "1996-08-01Z", 1973.0, float("nan"), None)] == \
        [2002, 1996, 1973, None, None]


def _o(n, t, x, y, h=None, year=None):
    return {"obstacle_id": f"fiss:{n}", "type": t, "height_m": h, "year": year, "x": x, "y": y, "_n": n}


def test_dedupe_same_place_same_type_is_one():
    got = O.dedupe([_o(1, "falls", 0, 0, year=1996), _o(2, "falls", 3, 0, h=2.0, year=1990),
                    _o(3, "falls", 6, 0, year=2001),            # chained within 10 m of 2: the same falls
                    _o(4, "cascade", 1, 0),                     # another type at the same place: kept
                    _o(5, "falls", 50, 0)], [])                 # 44 m away: another falls
    # the one kept has a height (2), over a later survey without one (3)
    assert sorted(o["obstacle_id"] for o in got) == ["fiss:2", "fiss:4", "fiss:5"]


def test_dedupe_a_fiss_dam_on_a_wris_crest_is_that_dam():
    crest = LineString([(1000, 0), (1000, 100)])
    got = O.dedupe([_o(1, "dam", 1150, 50), _o(2, "dam", 1300, 50), _o(3, "falls", 1001, 50)], [crest])
    assert sorted(o["obstacle_id"] for o in got) == ["fiss:2", "fiss:3"]


def test_layer_spec_and_app_types():
    spec = BY_NAME["obstacle"]
    assert (spec.geometry, spec.attrs[0], spec.minzoom, spec.decorative, spec.mvt_id) == \
        ("point", "obstacle_id", 10, False, False)
    ts = (ROOT / "app/packages/core/src/obstacles.ts").read_text(encoding="utf-8")
    m = re.search(r"export const OBSTACLE_TYPES = \[([^\]]*)\]", ts)
    assert m and tuple(re.findall(r'"(\w+)"', m.group(1))) == O.OBSTACLE_TYPES
