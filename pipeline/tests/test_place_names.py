"""A rule's place, named from its extents in the book's words — never a gauge's, never an id."""

from __future__ import annotations

from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.deliver.bundle.place_names import PlaceNamer


def _b(bid, label, kind="split", aliases=()):
    return RegistryBoundary(id=bid, label=label, kind=kind, ref=f"split:{bid}", wbk="",
                            aliases=tuple(aliases))


REG = {
    "gnis:1": RegistryItem(id="gnis:1", name="Puntledge River", kind="stream", section_ids=("a",),
                           boundaries=(
        _b("puntledge__signs_75_m", "signs 75 m downstream of Puntledge River hatchery fence",
           aliases=("split:gauge__08HB006",)),
        _b("gauge__08HB999", "08HB999 · Puntledge River At Courtenay"),
        _b("length:1:20", "Browns River", "confluence"),
        _b("x__lake_329", "lake 329465442", "lake", aliases=("split:x__signs_400_m",)),
        _b("thorn__point", "point"),
        _b("cnr1", "CNR bridge"), _b("cnr2", "CNR bridge"),
        _b("comox_lake", "Comox Lake", "lake"),
        _b("m_u100", "Morrison Creek → Puntledge River (100 m upstream)", "confluence"),
        _b("m_d100", "Morrison Creek → Puntledge River (100 m downstream)", "confluence"))),
    "wbk:-20": RegistryItem(id="wbk:-20", name="Kootenay Lake — Main Body", kind="lake",
                            section_ids=("k",)),
    "area:park:tweedsmuir": RegistryItem(id="area:park:tweedsmuir", name="TWEEDSMUIR PARK",
                                         kind="area", section_ids=("a",)),
}
N = PlaceNamer(REG, {"x__signs_400_m": "fishing boundary signs 400 m upstream of its mouth"})


def test_a_gauge_id_is_named_by_the_curated_split_it_aliases():
    assert N([{"op": "upstream_of", "splits": ["gauge__08HB006"]}], ("gnis:1",)) == \
        "upstream of signs 75 m downstream of Puntledge River hatchery fence"


def test_a_gauge_with_no_curated_name_names_no_place_and_is_logged():
    assert N([{"op": "upstream_of", "splits": ["gauge__08HB999"]}], ("gnis:1",)) is None
    assert any(s == "gauge__08HB999" for s, _ in N.unnamed)


def test_an_unnamed_cut_point_uses_its_curated_alias_or_nothing():
    assert N([{"op": "downstream_of", "splits": ["x__lake_329"]}], ("gnis:1",)) == \
        "downstream of fishing boundary signs 400 m upstream of its mouth"
    assert N([{"op": "downstream_of", "splits": ["thorn__point"]}], ("gnis:1",)) is None


def test_a_confluence_and_a_between():
    assert N([{"op": "between", "splits": ["length:1:20", "comox_lake"]}], ("gnis:1",)) == \
        "between the Browns River confluence and Comox Lake"


def test_two_cut_points_under_one_name_name_nothing():
    """"between CNR bridge and CNR bridge" is true and useless; the book's words say it better."""
    assert N([{"op": "between", "splits": ["cnr1", "cnr2"]}], ("gnis:1",)) is None


def test_the_entry_s_own_water_is_no_place_but_another_water_is():
    assert N([{"op": "whole"}], ("gnis:1",)) is None
    assert N([{"op": "whole", "item_id": "gnis:1"}], ("gnis:1",)) is None
    assert N([{"op": "whole", "item_id": "wbk:-20"}], ()) == "Kootenay Lake, Main Body"


def test_areas_are_named_and_regions_are_not():
    assert N([{"op": "within", "area_id": "area:park:tweedsmuir"}], ()) == "within Tweedsmuir Park"
    assert N([{"op": "within", "area_id": "area:region:4"}], ()) is None
    assert N([{"op": "within", "area_kind": "national_parks"}], ()) == "in national parks"


def test_a_confluence_keeps_its_offset():
    assert N([{"op": "between", "splits": ["m_u100", "m_d100"]}], ("gnis:1",)) == (
        "from 100 m upstream to 100 m downstream of the Morrison Creek confluence")
    assert N([{"op": "upstream_of", "splits": ["m_d100"]}], ("gnis:1",)) == (
        "upstream of the Morrison Creek confluence (100 m downstream)")


SHORT = {"gnis:2": RegistryItem(id="gnis:2", name="Some River", kind="stream", section_ids=("b",),
                                boundaries=(
    _b("qfn", "boundary signs = QFN"), _b("koo", "Koocanusa Reservoir u/s end"),
    _b("che", "main logging road bridge ~2.4km d/s Chehalis Lk"),
    _b("rob", "old Robson Ferry landing ↔ south-bank sign"),
    _b("tah", "boundary signs 400 m upstream of Tahltan River Bridge and th (750 m upstream)"),
    _b("can", "top of lower canyon, (1300 m upstream)"), _b("wit", "within"),
    _b("wit100", "within (100 m downstream)"), _b("casc", "cascade falls"),
    _b("casc80", "the cascade falls (80 m downstream)"),
    _b("dick", "Dickson Falls"), _b("dick30", "Dickson Falls (30 m downstream)"),
    _b("cpr", "CPR")))}


def test_a_curator_s_note_is_not_a_name():
    """Labels written as working notes ("= QFN", "u/s", "~", "↔", a cut-off "th") are not the
    book's words: the place is not printed and the cut-point is logged, as for a gauge."""
    n = PlaceNamer(SHORT)
    for sid in ("qfn", "koo", "che", "rob", "tah", "can", "wit", "wit100"):
        assert n([{"op": "upstream_of", "splits": [sid]}], ("gnis:2",)) is None, sid
        assert any(s == sid for s, _ in n.unnamed)
    # mutation: the same place written as the book writes it IS named
    assert n([{"op": "upstream_of", "splits": ["dick"]}], ("gnis:2",)) == "upstream of Dickson Falls"


def test_two_offsets_from_one_place_say_the_place_once():
    n = PlaceNamer(SHORT)
    assert n([{"op": "between", "splits": ["dick", "dick30"]}], ("gnis:2",)) == \
        "from Dickson Falls to 30 m downstream"
    assert n([{"op": "between", "splits": ["casc", "casc80"]}], ("gnis:2",)) == \
        "from cascade falls to 80 m downstream"


def test_a_lone_acronym_is_not_title_cased():
    n = PlaceNamer(SHORT)
    assert n([{"op": "downstream_of", "splits": ["cpr"]}], ("gnis:2",)) == "downstream of CPR"


def test_an_area_whose_registry_name_is_its_id_is_named_from_the_atlas_catalogue():
    """A registry area's `name` is its own id, so "No Fishing within Garibaldi Park" (Pitt River
    r1, `whole` + `within_area`) read "No fishing" until the atlas's area names were handed in."""
    reg = {"gnis:7551": RegistryItem(id="gnis:7551", name="Pitt River", kind="stream", section_ids=("p",)),
           "area:park:garibaldi_park": RegistryItem(id="area:park:garibaldi_park",
                                                    name="area:park:garibaldi_park", kind="area",
                                                    section_ids=("p",)),
           "area:watershed:liard_river": RegistryItem(id="area:watershed:liard_river",
                                                      name="area:watershed:liard_river",
                                                      kind="area", section_ids=("l",)),
           "area:permit_land_access:ubc_forest_1": RegistryItem(
               id="area:permit_land_access:ubc_forest_1", name="area:permit_land_access:ubc_forest_1",
               kind="area", section_ids=("u",))}
    pitt = [{"op": "whole", "within_area": "area:park:garibaldi_park"}]
    assert PlaceNamer(reg)(pitt, ("gnis:7551",)) is None
    n = PlaceNamer(reg, area_names={"area:park:garibaldi_park": "GARIBALDI PARK",
                                    "area:watershed:liard_river": "Liard River",
                                    "area:permit_land_access:ubc_forest_1": "UBC Forest [1166294466]"})
    assert n(pitt, ("gnis:7551",)) == "within Garibaldi Park"
    assert n([{"op": "within", "area_id": "area:watershed:liard_river"}]) == \
        "within Liard River watershed"
    assert n([{"op": "within", "area_id": "area:permit_land_access:ubc_forest_1"}]) == \
        "within UBC Forest"
