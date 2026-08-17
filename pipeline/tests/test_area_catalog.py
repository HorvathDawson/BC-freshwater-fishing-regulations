"""Area catalog assembly (pipeline/splits/area_catalog.py) — pure, synthetic polygons."""

from shapely.geometry import box

from pipeline.splits.area_catalog import area_id, catalog_entries


def test_area_id_scheme():
    assert area_id("watershed", "Liard River") == "area:watershed:liard_river"
    assert area_id("park", "Wells Gray Park") == "area:park:wells_gray_park"


def test_catalog_entries_kind_cut_and_defaults():
    defs = [
        {"id": "provincial_parks", "kind": "park", "cut": False},
        {"id": "national_parks", "kind": "park", "cut": True},
    ]
    polys = {
        "provincial_parks": {"Wells Gray Park": box(0, 0, 1, 1)},
        "national_parks": {"Glacier": box(2, 2, 3, 3)},
    }
    entries = {e.area_id: e for e in catalog_entries(defs, polys)}
    assert entries["area:park:wells_gray_park"].cut is False
    assert entries["area:park:glacier"].cut is True


def test_cut_entry_wins_on_id_collision():
    # same kind+name from a membership def and a cut def -> the cut one is kept
    defs = [
        {"id": "membership", "kind": "reserve", "cut": False},
        {"id": "cutter", "kind": "reserve", "cut": True},
    ]
    polys = {
        "membership": {"Oak Bay Islands": box(0, 0, 1, 1)},
        "cutter": {"Oak Bay Islands": box(0, 0, 1, 1)},
    }
    entries = catalog_entries(defs, polys)
    assert len(entries) == 1 and entries[0].cut is True


def test_empty_polys_skipped():
    assert catalog_entries([{"id": "x", "kind": "park"}], {"x": {}}) == []
