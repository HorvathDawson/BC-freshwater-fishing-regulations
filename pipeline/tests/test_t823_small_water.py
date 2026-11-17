"""T823 — "small next to its gauge": Python flags the led sections whose lead gauge drains far more than they do, and
writes the warning the app shows (owner 2026-11-16; pipeline/gauges/consume/small_water.py).

MUTATION PINS — each mutation and the test it fails:
  * `>=` -> `>` at the threshold                          -> test_the_threshold_is_inclusive
  * flag a section BIGGER than its gauge (abs ratio)      -> test_a_section_bigger_than_its_gauge_is_never_small
  * read the line's magnitude before the panel area       -> test_a_side_channel_is_judged_by_its_panel_area
  * read the gauge where it is matched, not where it reads -> test_a_diversion_gauge_is_sized_at_its_intake
  * a missing area read as small / not small              -> test_a_missing_area_is_refused
  * the switch ignored (one hard-coded ratio)             -> test_the_switch_moves_the_owners_creek
  * the column dropped from the contract or not last      -> test_the_conditions_contract_carries_small_last
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from pipeline.atlas.graph.drainage import AreaModel
from pipeline.deliver.bundle.build import SCHEMA
from pipeline.deliver.release import contract as C
from pipeline.gauges.consume import small_water as SW
from pipeline.gauges.consume.shed import GaugeLink

#: area_km2 == magnitude, so every fixture area is readable as it is written
UNIT = AreaModel(b=1.0, c_default=1.0, c_by_basin={})


def _graph(**mags: int):
    return SimpleNamespace(nodes={n: SimpleNamespace(stream_magnitude=m, wsc="100-000000") for n, m in mags.items()})


def _link(sec: str, station: str) -> GaugeLink:
    return GaugeLink(sec, station, "weak", 0.0, 1)


def test_the_sentence_is_the_one_the_app_shows_word_for_word():
    assert SW.SMALL_WATER_TEXT == ("This stream is much smaller than the river its gauge measures: "
                                   "the reading may not match it.")
    assert (SW.META_TEXT_KEY, SW.META_RATIO_KEY) == ("small_water_text", "small_water_ratio")


def test_the_default_is_the_recommended_value_pending_the_owner():
    assert SW.SMALL_WATER_RATIOS == {"x50": 50.0, "x100": 100.0, "x200": 200.0}
    assert SW.SMALL_WATER_POLICY == "x50"
    assert SW.ratio_for() == 50.0


def test_an_unknown_policy_is_refused_never_defaulted():
    with pytest.raises(ValueError, match="no policy 'x75'"):
        SW.ratio_for("x75")


@pytest.mark.parametrize("ratio", [50.0, 100.0, 200.0])
def test_the_threshold_is_inclusive(ratio):
    assert SW.is_small(1.0, ratio, ratio)                  # exactly at the line: small
    assert not SW.is_small(1.0, ratio - 0.001, ratio)      # just under: not
    assert SW.is_small(2.0, 2 * ratio + 0.001, ratio)


def test_a_section_bigger_than_its_gauge_is_never_small():
    # the lower Fraser's side channels led by Red Pass (08KA007): the gauge sees part of the water, the other question
    assert not SW.is_small(64_706.0, 1_121.0, 50.0)
    assert not SW.is_small(100.0, 100.0, 50.0)


def test_a_missing_area_is_refused():
    for s, g in ((None, 100.0), (0.0, 100.0), (1.0, None), (1.0, 0.0), (-1.0, 100.0)):
        with pytest.raises(ValueError, match="no drainage to compare"):
            SW.is_small(s, g, 50.0)
    with pytest.raises(ValueError, match="as big as its gauge"):
        SW.is_small(1.0, 100.0, 1.0)


def test_the_switch_moves_the_owners_creek():
    # the owner's 1.4 km² creek beside the Chilliwack (sid 63923 on the live bundle): led by Slesse Creek (08MH056),
    # modelled 186.075 km² against its 1.421 — 131x. Warned at x50 and x100, not at x200.
    assert SW.is_small(1.421, 186.075, SW.ratio_for("x50"))
    assert SW.is_small(1.421, 186.075, SW.ratio_for("x100"))
    assert not SW.is_small(1.421, 186.075, SW.ratio_for("x200"))


def test_small_sections_reads_the_model_at_both_ends():
    g = _graph(GAUGE=1000, CREEK=10, TRIB=30, MAIN=900)
    links = [_link(s, "08ZZ001") for s in ("CREEK", "TRIB", "MAIN")]
    # 1000 / 10 = 100x small; 1000 / 30 = 33x not; 1000 / 900 not
    assert SW.small_sections(links, g, UNIT, {"08ZZ001": "GAUGE"}, {}, 50.0) == {"CREEK"}
    assert SW.small_sections(links, g, UNIT, {"08ZZ001": "GAUGE"}, {}, 20.0) == {"CREEK", "TRIB"}


def test_a_side_channel_is_judged_by_its_panel_area():
    # a side channel's own line magnitude is 3; its panel adopted its river's 900 km²: not small next to a 1000 gauge
    g = _graph(GAUGE=1000, SIDE=3)
    links = [_link("SIDE", "08ZZ001")]
    assert SW.small_sections(links, g, UNIT, {"08ZZ001": "GAUGE"}, {"SIDE": 900.0}, 50.0) == set()
    # a panel with no area (None) falls to the model at its own magnitude: 3 against 1000 is small
    assert SW.small_sections(links, g, UNIT, {"08ZZ001": "GAUGE"}, {"SIDE": None}, 50.0) == {"SIDE"}


def test_a_diversion_gauge_is_sized_at_its_intake():
    # matched on its canal (mag 1) but reading the river at its intake (mag 5000): the creek of 40 is 125x the intake
    g = _graph(CANAL=1, INTAKE=5000, CREEK=40)
    links = [_link("CREEK", "08GC005")]
    assert SW.small_sections(links, g, UNIT, {"08GC005": "INTAKE"}, {}, 50.0) == {"CREEK"}
    assert SW.small_sections(links, g, UNIT, {"08GC005": "CANAL"}, {}, 50.0) == set()


def test_the_conditions_contract_carries_small_last():
    assert C.FILES["conditions"]["tables"]["section_gauge"] == ["sid", "station", "trust", "mag", "rel", "dist_b",
                                                                "small"]
    assert C.FILES["conditions"]["format"] == "conditions/1"          # additive: no format bump


def test_the_bundle_schema_defaults_small_to_zero():
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA.read_text())
    cols = {r[1]: r for r in db.execute("PRAGMA table_info(section_gauge)")}
    assert list(cols)[-1] == "small"
    assert cols["small"][3] == 1 and cols["small"][4] == "0"         # NOT NULL DEFAULT 0
    db.execute("INSERT INTO section_gauge (sid, station, trust, mag, rel, dist_b) VALUES (1, 'X', 'weak', 1, 0, 0)")
    assert db.execute("SELECT small FROM section_gauge").fetchone() == (0,)
