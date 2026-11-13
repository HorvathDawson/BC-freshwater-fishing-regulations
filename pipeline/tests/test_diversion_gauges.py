"""T696 — a gauge on a diversion canal speaks for the river above its intake (owner 2026-11-12, RULINGS P54:
"Show 08GC005 above the canal"), labelled as measuring a diversion.

The fixture is the Theodosia's shape, small:

    H (headwater, mag 4) -> U (river above the intake, mag 8) -> N (the section the canal leaves, mag 10)
    N -> L (river below the intake, mag 12) -> M (mouth, mag 12)
    N -> C1 (canal top, its own blue line, mag 1) -> C0 (canal foot, mag 1)

The diversion gauge D stands on C1 (the canal); the bypass gauge B stands on N (as 08GC006 does on the live atlas).

MUTATION PINS — each mutation and the test it fails:
  * drop the in_place_of filter in the shed             -> test_the_diversion_takes_every_section_above_the_intake
  * walk the diversion down the river from its intake   -> test_the_bypass_keeps_the_river_below_the_intake_and_…
  * drop `up_only` in the panel walk                     -> test_the_panel_walk_only_climbs_from_the_intake
  * drop the panel displacement                          -> test_the_panel_above_the_intake_has_no_bypass_donor
  * drop the HYDAT name check                            -> test_a_row_whose_station_hydat_does_not_call_a_diversion_is_refused
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.common.models.enums import NodeKind
from pipeline.common.models.graph import FlowEdge, StreamGraph, StreamNode
from pipeline.gauges.consume import diversion as D
from pipeline.gauges.consume import panels as P
from pipeline.gauges.consume.shed import build_gauge_sheds
from pipeline.tests import under_test

RIVER, CANAL, WSC = "360856269", "360242203", "900-311867"
#: The bypass gauge sorts BEFORE the diversion, so a tie on ratio and liveness would go to it by id: the diversion
#: must win above its intake by the displacement, never by the id.
BYPASS = "08GC001"


def _graph() -> StreamGraph:
    g = StreamGraph()
    for nid, blk, mag in [("H", RIVER, 4), ("U", RIVER, 8), ("N", RIVER, 10), ("L", RIVER, 12), ("M", RIVER, 12),
                          ("C1", CANAL, 1), ("C0", CANAL, 1)]:
        g.nodes[nid] = StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, wsc=WSC, stream_magnitude=mag,
                                  length_m=100.0)
    for ix, (a, b) in enumerate([("H", "U"), ("U", "N"), ("N", "L"), ("L", "M"), ("N", "C1"), ("C1", "C0")]):
        g.edges.append(FlowEdge(from_node=a, to_node=b, at_measure=0.0))
        g.up_adj.setdefault(b, []).append(ix)
        g.down_adj.setdefault(a, []).append(ix)
    return g


STATIONS = [{"station": "08GC005"}, {"station": BYPASS}]
PLACED = {"08GC005": "C1", BYPASS: "N"}
ROW = {"river_blk": RIVER, "in_place_of": ["08GC006"], "owner": "\"Show 08GC005 above the canal\"", "why": "test"}


def _intakes(g):
    return D.intakes(g, {"08GC005": D.Diversion("08GC005", RIVER, (BYPASS,), "test")}, PLACED)


def _sheds(g, diversions):
    return {l.section_id: l for l in build_gauge_sheds(g, STATIONS, PLACED, diversions=diversions)}


# ------------------------------------------------------------------ the graph: where the canal leaves the river

def test_the_intake_is_the_river_section_that_feeds_the_canal():
    g = _graph()
    assert D.intake(g, "C1", RIVER) == ("N", 100.0)
    assert D.intake(g, "C0", RIVER) == ("N", 200.0)            # climbed the canal's own line to its top
    assert D.above(g, "N") == frozenset({"H", "U", "N"})


def test_a_canal_no_river_section_feeds_is_refused():
    with pytest.raises(ValueError, match="expected exactly one intake"):
        D.intake(_graph(), "C1", "999999999")


def test_a_station_on_the_river_itself_is_not_a_diversion():
    with pytest.raises(ValueError, match="not on a canal"):
        D.intake(_graph(), "N", RIVER)


def test_an_authored_station_not_on_this_graph_stops_the_build():
    with pytest.raises(ValueError, match="not placed"):
        D.intakes(_graph(), {"08GC005": D.Diversion("08GC005", RIVER, (BYPASS,), "x")}, {})


# ------------------------------------------------------------------ the shed (section_gauge)

def test_without_the_row_the_bypass_speaks_above_the_intake():
    got = _sheds(_graph(), None)
    assert {s: got[s].station for s in ("H", "U", "N")} == {"H": BYPASS, "U": BYPASS, "N": BYPASS}


def test_the_diversion_takes_every_section_above_the_intake():
    got = _sheds(_graph(), _intakes(_graph()))
    assert {s: (got[s].station, got[s].rel) for s in ("H", "U", "N")} == {
        "H": ("08GC005", "down"), "U": ("08GC005", "down"), "N": ("08GC005", "down")}
    assert got["N"].ratio == 1.0 and got["N"].trust == "good"      # rated against the intake, not the canal's mag 1
    assert got["N"].dist_m == 100                                   # the canal's metres down to the station


def test_the_diversion_never_speaks_below_its_intake():
    got = _sheds(_graph(), _intakes(_graph()))
    assert got["L"].station == BYPASS and got["M"].station == BYPASS


def test_the_bypass_keeps_the_river_below_the_intake_and_the_canal_keeps_its_gauge():
    got = _sheds(_graph(), _intakes(_graph()))
    assert got["L"].station == BYPASS
    assert got["C1"].station == "08GC005" and got["C0"].station == "08GC005"


def test_the_displaced_bypass_is_still_a_candidate():
    g, cands = _graph(), []
    build_gauge_sheds(g, STATIONS, PLACED, diversions=_intakes(g), candidates=cands)
    assert ("U", BYPASS) in {(c.section_id, c.station) for c in cands}


# ------------------------------------------------------------------ the donor panels (what the app shows)

class _Model:
    def area_km2(self, mag, _wsc):
        return float(mag) if mag else None


DONORS = [("08GC005", "N", 20, False), (BYPASS, "N", 20, False)]       # the diversion placed at its intake


def test_the_panel_walk_only_climbs_from_the_intake():
    g = _graph()
    cand = P.candidates(g, lambda n: float(n.stream_magnitude), DONORS, up_only=frozenset({"08GC005"}))
    assert {c[0] for c in cand["L"]} == {BYPASS}
    assert {c[0] for c in cand["U"]} == {"08GC005", BYPASS}


def test_the_panel_above_the_intake_has_no_bypass_donor():
    g = _graph()
    pans = P.build(g, _Model(), DONORS, diversions=_intakes(g))
    members = lambda s: {m.station for m in pans.members[pans.by_section[s][0]]}  # noqa: E731
    assert members("N") == members("U") == {"08GC005"}
    assert members("L") == {BYPASS}


def test_the_panel_refuses_a_diversion_not_placed_at_its_intake():
    g = _graph()
    with pytest.raises(ValueError, match="must stand at its intake"):
        P.build(g, _Model(), [("08GC005", "C1", 20, False)], diversions=_intakes(g))


# ------------------------------------------------------------------ the authored list, checked against HYDAT

def _file(tmp_path: Path, stations: dict) -> Path:
    p = tmp_path / "diversion_stations.json"
    p.write_text(json.dumps({"_about": "test", "stations": stations}), encoding="utf-8")
    return p


def _hydat(tmp_path: Path, names: dict) -> Path:
    p = tmp_path / "hydat.sqlite3"
    with sqlite3.connect(p) as con:
        con.execute("CREATE TABLE STATIONS (STATION_NUMBER TEXT, STATION_NAME TEXT)")
        con.executemany("INSERT INTO STATIONS VALUES (?,?)", sorted(names.items()))
    return p


THEODOSIA = {"08GC005": "THEODOSIA RIVER DIVERSION ABOVE OLSEN LAKE", "08GC006": "THEODOSIA RIVER DIVERSION BYPASS"}


def test_the_record_check_passes_the_theodosia(tmp_path):
    got = D.diversion_stations(_hydat(tmp_path, THEODOSIA), _file(tmp_path, {"08GC005": ROW}))
    assert got == {"08GC005": D.Diversion("08GC005", RIVER, ("08GC006",), "test")}


def test_a_row_whose_station_hydat_does_not_call_a_diversion_is_refused(tmp_path):
    hy = _hydat(tmp_path, {**THEODOSIA, "08GC005": "THEODOSIA RIVER ABOVE OLSEN LAKE"})
    with pytest.raises(ValueError, match="does not say it measures a diversion"):
        D.diversion_stations(hy, _file(tmp_path, {"08GC005": ROW}))


def test_a_station_it_speaks_instead_of_must_exist(tmp_path):
    with pytest.raises(ValueError, match="not in HYDAT"):
        D.diversion_stations(_hydat(tmp_path, {"08GC005": THEODOSIA["08GC005"]}), _file(tmp_path, {"08GC005": ROW}))


@pytest.mark.parametrize("bad", [
    {"8GC005": ROW},
    {"08GC005": {**ROW, "river_blk": "3608"}},
    {"08GC005": {**ROW, "in_place_of": []}},
    {"08GC005": {**ROW, "in_place_of": ["08GC005"]}},
    {"08GC005": {**ROW, "why": ""}},
    {"08GC005": {**ROW, "extra": "x"}},
])
def test_an_authored_row_must_say_what_it_claims(tmp_path, bad):
    with pytest.raises(ValueError):
        D.load_diversions(_file(tmp_path, bad))


def test_a_missing_list_or_hydat_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        D.load_diversions(tmp_path / "absent.json")
    with pytest.raises(FileNotFoundError):
        D.diversion_stations(tmp_path / "absent.sqlite3", _file(tmp_path, {}))


def test_the_list_is_declared_in_config_and_is_exactly_the_theodosia():
    path = under_test.curated().gauges.diversion_stations
    assert path is not None and path.name == "diversion_stations.json" and D.DIVERSION_FILE == path
    got = D.load_diversions(path)
    assert got == {"08GC005": D.Diversion("08GC005", RIVER, ("08GC006", "08GC007"), got["08GC005"].why)}


def test_the_bundle_gauge_table_carries_the_flag_last_and_the_release_contract_matches():
    from pipeline.deliver.release.contract import FILES
    schema = (Path(__file__).resolve().parents[1] / "deliver" / "bundle" / "schema.sql").read_text(encoding="utf-8")
    con = sqlite3.connect(":memory:")
    con.executescript(schema)
    cols = [(r[1], r[2], r[3], r[4]) for r in con.execute("PRAGMA table_info(gauge)")]
    assert [c[0] for c in cols] == FILES["conditions"]["tables"]["gauge"]
    assert cols[-1] == ("diversion", "INTEGER", 1, "0")
    assert FILES["conditions"]["format"] == "conditions/1"
