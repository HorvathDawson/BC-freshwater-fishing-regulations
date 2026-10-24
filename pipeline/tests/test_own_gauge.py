"""OWNER 2026-10-22 — a water's own gauge comes first.

"it can still use auxiliary gauges but own gauge should be main (unless far from click) low magnitude compared to
click spot." A water's OWN gauge (the station matched to that water) carries the main weight; the other donors keep a
smaller share; the own gauge stops leading when it is far along the water from the tapped section, or drains much
less than it. Never across quantities: a level-only own gauge leads the Level view and not Flow — which needs no rule
of its own here, because a station with no discharge reading is not a vote in Flow, so it cannot be the one leading.

Two halves, two places:
  · WHO MAY LEAD a section — the pipeline (`build_panels.build`, `panel.may_lead`): it alone knows the water, the
    along-water distance and both catchments. It ranks the section's own gauges; a member's `lead` is that rank.
  · WHAT LEADING MEANS — `panel.leader_of` and `panel.lead_shares`, mirrored by `leaderOf` / `leadShares` in
    app/packages/core/src/trust.ts and held equal by app/tools/one-formula.test.ts.
"""

from __future__ import annotations

import inspect
import textwrap

import pytest

from pipeline.gauges.consume import panel as P
from pipeline.gauges.consume.panels import build
from pipeline.gauges.consume.panel import (
    MEMBERS_PER_QUANTITY, OWN_AREA_TRUST, OWN_FAR_FRACTION, OWN_FAR_MIN_KM, OWN_LEAD_SHARE, OWN_LEADS_PER_QUANTITY,
    OWN_SMALLER_RATIO, area_scale, far_limit_km, lead_shares,
    leader_of, may_lead, river_length_km,
)
from pipeline.tests.test_build_panels import MODEL, E, N, g


# ------------------------------------------------------------------ the constants, said once

def test_the_defaults_are_the_ones_the_report_proposes():
    """handoff/PLAN/app-data/own-gauge/REPORT.md. Changing one is an owner decision, not a tidy-up."""
    assert (OWN_LEAD_SHARE, OWN_SMALLER_RATIO, OWN_FAR_FRACTION, OWN_FAR_MIN_KM, OWN_AREA_TRUST) == (
        0.7, 10.0, 0.2, 10.0, 10.0)


# ------------------------------------------------------------------ what leading means

def test_the_own_gauge_carries_the_main_weight():
    """Three donors of equal usual weight: a third each before, the own gauge 70% after."""
    assert lead_shares([1.0, 1.0, 1.0], 0) == pytest.approx([0.7, 0.15, 0.15])


def test_the_auxiliaries_keep_a_share_in_their_own_proportion():
    """The owner: "it can still use auxiliary gauges". They split what the lead leaves, 3:1 as before."""
    assert lead_shares([0.2, 0.75, 0.25], 0) == pytest.approx([0.7, 0.225, 0.075])


def test_an_own_gauge_already_above_the_share_is_not_pulled_down_to_it():
    """Leading is a floor, not a ceiling: a gauge carrying 90% of the answer on its own merits keeps 90%."""
    assert lead_shares([9.0, 1.0], 0) == pytest.approx([0.9, 0.1])


def test_with_no_lead_the_weights_are_the_usual_ones():
    assert lead_shares([2.0, 1.0, 1.0], None) == pytest.approx([0.5, 0.25, 0.25])


def test_an_own_gauge_alone_is_the_whole_answer():
    assert lead_shares([0.3], 0) == pytest.approx([1.0])
    assert lead_shares([0.3, 0.0], 0) == pytest.approx([1.0, 0.0])


def test_nothing_reporting_is_nothing():
    assert lead_shares([0.0, 0.0], None) == [0.0, 0.0]
    assert lead_shares([], None) == []


def test_the_leader_is_the_best_ranked_own_gauge_that_is_reporting():
    """Rank 1 is the nearest own gauge. A quiet one (weight 0) hands the lead to rank 2, not to nobody."""
    assert leader_of([0, 2, 1], [1.0, 1.0, 1.0]) == 2
    assert leader_of([0, 2, 1], [1.0, 1.0, 0.0]) == 1
    assert leader_of([0, 0, 0], [1.0, 1.0, 1.0]) is None
    assert leader_of([0, 2, 1], [1.0, 0.0, 0.0]) is None


def test_a_level_only_own_gauge_leads_level_and_does_not_touch_flow():
    """THE PITT. 08MH035 Pitt River near Port Coquitlam records level only. In Flow it reports nothing, so it is not
    a vote and cannot lead — the Flow answer is the usual one from North Alouette; in Level it leads.

    Weights are as `weight_of` gives them on the Pitt today (target 1,181 km²): 08MH035 at 1x / 8 years, North
    Alouette at 45x / 69 years, on another blue line."""
    own = P.weight_of(1.0, "down", 8, True)
    alouette = P.weight_of(26.4 / 1181.2, "up", 69, False)
    ranks = [1, 0]
    flow = [0.0, alouette]                      # 08MH035 has no discharge reading
    level = [own, alouette]
    assert leader_of(ranks, flow) is None
    assert lead_shares(flow, leader_of(ranks, flow)) == pytest.approx([0.0, 1.0])
    assert leader_of(ranks, level) == 0
    assert lead_shares(level, leader_of(ranks, level)) == pytest.approx([0.7, 0.3])


def test_combine_applies_the_lead():
    """The pipeline's own combiner agrees with the app's: led, the answer sits where the lead's share puts it."""
    led = combine_mid([(0.05, 1.0), (0.5, 1.0)], lead=0)
    usual = combine_mid([(0.05, 1.0), (0.5, 1.0)])
    assert led < usual
    assert led == pytest.approx(P._norm_cdf(0.7 * P._probit(0.05) + 0.3 * P._probit(0.5)), abs=1e-6)


def combine_mid(values, lead=None):
    got = P.combine(values, lead=lead)
    assert got is not None
    return got[0]


# ------------------------------------------------------------------ who may lead

def test_far_scales_with_the_river():
    """A fifth of the river's length at the larger of the two catchments (Hack's law), never under 10 km. At the
    area model's own figures (no published scale) the Fraser below Hope (63,113 km²) gives 193 km; on a 100 km²
    creek it is the 10 km floor. The published scale is `test_far_is_measured_at_the_published_catchment`."""
    assert round(river_length_km(60_356.3), 1) == 938.1
    assert round(far_limit_km(63_113.0, 60_356.3), 1) == 192.7
    assert far_limit_km(100.0, 80.0) == 10.0


def test_far_is_measured_at_the_published_catchment():
    """Hack's law speaks of real catchments; the area model runs 2-3.6x under them on big rivers. THE SIMILKAMEEN at
    sid 316754 (3,193 km² modelled): Hedley, 08NL038, 47.2 km up, is 2,084 km² modelled and 5,568 published. At the
    model's areas the limit is 32.2 km and Hedley is "far"; at the published scale (x2.671) it is 58.0 km and Hedley
    leads. The Fraser below Hope (216,600 km² published, 60,356 modelled): 414.8 km, not 192.7."""
    assert round(far_limit_km(3193.0, 2084.473), 1) == 32.2
    assert round(area_scale(2084.473, 5568.0), 3) == 2.671
    assert round(far_limit_km(3193.0, 2084.473, area_scale(2084.473, 5568.0)), 1) == 58.0
    assert not may_lead(3193.0, 2084.473, 47_217.0)
    assert may_lead(3193.0, 2084.473, 47_217.0, published_km2=5568.0)
    assert round(far_limit_km(63_113.0, 60_356.3, area_scale(60_356.3, 216_600.0)), 1) == 414.8


def test_a_published_catchment_below_the_model_shortens_far():
    """The scale runs both ways: 5,000 km² modelled reaches 42.1 km; published at 2,000 it reaches 24.3 km."""
    assert may_lead(5000.0, 5000.0, 40_000.0)
    assert not may_lead(5000.0, 5000.0, 40_000.0, published_km2=2000.0)


def test_a_published_catchment_past_the_trust_bound_is_not_believed():
    """08MH028, a Fraser gauge publishing 232,000 km², is matched to a 1.42 km² stub: x163,265. The model is used as
    it is (scale 1), so its reach is the 10 km floor, not the length of the Fraser."""
    assert area_scale(1.421, 232_000.0) == 1.0
    assert area_scale(100.0, 1000.0) == 10.0
    assert area_scale(100.0, 10.0) == pytest.approx(0.1)
    assert area_scale(100.0, 1001.0) == 1.0
    assert area_scale(100.0, None) == 1.0
    assert area_scale(100.0, 0.0) == 1.0
    assert not may_lead(1.5, 1.421, 50_000.0, published_km2=232_000.0)


def test_a_near_own_gauge_of_comparable_size_may_lead():
    assert may_lead(1181.2, 1181.2, 0.0)
    assert may_lead(63_113.0, 60_356.3, 47_500.0)          # lower Fraser, 47 km below Hope


def test_a_far_own_gauge_does_not_lead():
    """The Fraser at Hansard for the lower Fraser: 807 km up the valley."""
    assert not may_lead(63_113.0, 9_245.7, 807_100.0)
    assert not may_lead(500.0, 450.0, 30_000.0)               # 30 km on a 500 km² river is past a fifth of it


def test_a_much_smaller_own_gauge_does_not_lead():
    """Kicking Horse below Sherbrooke Creek: 61.8 km² at the gauge, 1,102 km² at the tapped section."""
    assert not may_lead(1102.0, 61.8, 0.0)
    assert may_lead(600.0, 61.0, 0.0)                         # just inside ten times


def test_a_bigger_own_gauge_downstream_still_leads_when_near():
    """The owner named only the smaller case. A gauge draining more than the tapped spot, and near it, leads."""
    assert may_lead(100.0, 400.0, 5_000.0)


def test_nothing_without_both_areas():
    assert not may_lead(None, 100.0, 0.0)
    assert not may_lead(100.0, 0.0, 0.0)


# ------------------------------------------------------------------ the build

def _river(n=4, mag=100, length=5_000.0, water="gnis:river"):
    """A chain s0 -> s1 -> ... on one water, each `length` metres, gauged at s0."""
    nodes = [N(f"s{i}", mag + i, length_m=length) for i in range(n)]
    return g(nodes, [E(f"s{i}", f"s{i+1}") for i in range(n - 1)]), {f"s{i}": water for i in range(n)}


def _leads(panels, sec):
    return {m.station: m.lead for m in panels.members[panels.by_section[sec][0]]}


def test_the_build_marks_the_own_gauge_as_leading():
    graph, water = _river()
    graph.nodes["trib"] = N("trib", 90, blk="T", length_m=5_000.0)
    graph.edges.append(E("trib", "s2"))
    water["trib"] = "gnis:trib"
    panels = build(graph, MODEL, [("OWN", "s0", 40, False), ("AUX", "trib", 40, False)], water_of=water)
    assert _leads(panels, "s2") == {"OWN": 1, "AUX": 0}
    assert _leads(panels, "s0") == {"OWN": 1}


def test_the_build_ranks_own_gauges_nearest_first():
    graph, water = _river(n=5)
    panels = build(graph, MODEL, [("TOP", "s0", 40, False), ("MID", "s2", 40, False)], water_of=water)
    assert _leads(panels, "s3") == {"MID": 1, "TOP": 2}
    assert _leads(panels, "s1") == {"TOP": 1, "MID": 2}


def test_a_far_own_gauge_gets_no_lead():
    """Ten 5 km sections of a ~100 km² river, gauged at the foot of s0: s1 starts at the gauge (0 km), s3 starts
    10 km below it — the floor, still near — and s4 at 15 km is far."""
    graph, water = _river(n=10)
    panels = build(graph, MODEL, [("OWN", "s0", 40, False)], water_of=water)
    assert [_leads(panels, f"s{i}")["OWN"] for i in range(10)] == [1, 1, 1, 1, 0, 0, 0, 0, 0, 0]


def test_the_build_measures_far_at_the_published_catchment():
    """The ten-section river again, its gauge publishing 9x the modelled 100 km²: the limit at s4 (104 km² modelled,
    936 real) is 15.4 km, so s4 at 15 km now leads and s5 at 20 km does not."""
    graph, water = _river(n=10)
    panels = build(graph, MODEL, [("OWN", "s0", 40, False)], water_of=water, published={"OWN": 900.0})
    assert [_leads(panels, f"s{i}")["OWN"] for i in range(10)] == [1, 1, 1, 1, 1, 0, 0, 0, 0, 0]


def test_own_gauges_tied_on_distance_and_size_go_to_the_one_the_model_agrees_with():
    """THE CHILLIWACK. 08MH016 (outlet of Chilliwack Lake, 335 km² published) and 08MH103 (above Slesse Creek, 650)
    are both matched to sid 1235783, both 644 km² in the model, both 0 km away. Station order made the lake outlet
    lead; the one whose published catchment agrees with the model at that section leads instead. Named here so
    station order would pick the wrong one."""
    graph, water = _river(n=2, mag=644)
    donors = [("A_LAKE", "s0", 40, False), ("B_SLESSE", "s0", 40, False)]
    panels = build(graph, MODEL, donors, water_of=water, published={"A_LAKE": 335.0, "B_SLESSE": 650.4})
    assert _leads(panels, "s0") == {"A_LAKE": 2, "B_SLESSE": 1}
    assert _leads(build(graph, MODEL, donors, water_of=water), "s0") == {"A_LAKE": 1, "B_SLESSE": 2}


def test_a_much_smaller_own_gauge_gets_no_lead():
    """A gauge on a 50 km² headwater; the tapped section below drains 600 km²."""
    graph = g([N("top", 50, length_m=1_000.0), N("low", 600, length_m=1_000.0)], [E("top", "low")])
    panels = build(graph, MODEL, [("OWN", "top", 40, False)], water_of={"top": "gnis:r", "low": "gnis:r"})
    assert _leads(panels, "low") == {"OWN": 0}
    assert _leads(panels, "top") == {"OWN": 1}


def test_a_gauge_on_another_water_never_leads():
    graph, water = _river()
    water["s0"] = "gnis:other"
    panels = build(graph, MODEL, [("OTHER", "s0", 40, False)], water_of=water)
    assert _leads(panels, "s1") == {"OTHER": 0}


def test_without_the_water_map_nothing_leads():
    """The synthetic callers that never had one keep the usual weighting."""
    graph, _water = _river()
    panels = build(graph, MODEL, [("OWN", "s0", 40, False)])
    assert _leads(panels, "s1") == {"OWN": 0}


def test_the_own_gauge_survives_the_member_cap():
    """MEMBERS_PER_QUANTITY keeps the heaviest; an own gauge that may lead is kept whatever its weight — a 3-year
    record counts 3/20 of a long one and would otherwise lose its seat to longer-lived neighbours. It rides ON TOP of
    the cap: every donor the panel held without the rule is still there (a level-only own gauge taking a seat cost
    125 real sections their Flow answer)."""
    graph, water = _river(n=2)
    donors = [("OWN", "s0", 3, False)]
    for i in range(MEMBERS_PER_QUANTITY):
        graph.nodes[f"t{i}"] = N(f"t{i}", 100, blk=f"T{i}", length_m=1_000.0)
        graph.edges.append(E(f"t{i}", "s1"))
        donors.append((f"AUX{i}", f"t{i}", 60, False))
    panels = build(graph, MODEL, donors, water_of=water)
    got = _leads(panels, "s1")
    assert got == {"OWN": 1, **{f"AUX{i}": 0 for i in range(MEMBERS_PER_QUANTITY)}}
    unled = build(graph, MODEL, donors)
    assert _leads(unled, "s1") == {f"AUX{i}": 0 for i in range(MEMBERS_PER_QUANTITY)}


def test_each_quantity_keeps_only_its_nearest_few_candidates():
    """On the lower Fraser at Mission 12 real-time own gauges are near enough and big enough, 11 with a level record
    and 4 with a discharge one; kept on top of the cap, every one would be a row in the donor table. Per quantity the nearest
    OWN_LEADS_PER_QUANTITY (4) are candidates — so Hope, the 4th discharge gauge down the list at Mission, still leads
    Flow there — and the rest are ordinary donors, kept or capped as before."""
    assert OWN_LEADS_PER_QUANTITY == 4
    graph, water = _river(n=7, length=1_000.0)
    donors = [(f"G{i}", f"s{i}", 40, False) for i in range(6)]
    measures = {**{f"G{i}": {"level"} for i in range(1, 6)}, "G0": {"discharge"}}
    panels = build(graph, MODEL, donors, water_of=water, measures=measures)
    # G5 is nearest s6 (0 km) ... G0 farthest (5 km). Level keeps G5..G2; discharge keeps G0. G1 is neither: an
    # ordinary donor, still in the panel (it is among Level's best MEMBERS_PER_QUANTITY).
    assert _leads(panels, "s6") == {"G5": 1, "G4": 2, "G3": 3, "G2": 4, "G1": 0, "G0": 5}


def test_a_station_with_no_record_in_any_quantity_never_leads():
    """Nor is it a member at all: the panel keeps each quantity's best (test_live_panels.py), and it has none."""
    graph, water = _river(n=2)
    panels = build(graph, MODEL, [("OWN", "s0", 40, False)], water_of=water, measures={})
    assert panels.by_section == {} and panels.members == {}
    beside = build(graph, MODEL, [("OWN", "s0", 40, False), ("REC", "s0", 40, False)], water_of=water,
                   measures={"REC": {"discharge"}})
    assert _leads(beside, "s1") == {"REC": 1}


def test_the_lead_is_part_of_the_interned_panel():
    """Two stretches with the same donors but a different leader must not share a panel."""
    graph, water = _river(n=10)
    panels = build(graph, MODEL, [("OWN", "s0", 40, False)], water_of=water)
    assert panels.by_section["s1"][0] != panels.by_section["s8"][0]
    assert panels.by_section["s1"][0] == panels.by_section["s2"][0]


def test_the_member_rows_carry_the_lead():
    graph, water = _river(n=2)
    panels = build(graph, MODEL, [("OWN", "s0", 40, False)], water_of=water)
    pid = panels.by_section["s1"][0]
    assert [r for r in panels.member_rows() if r[0] == pid] == [(pid, 0, "OWN", "up", 100.0, 40, 40, 0, 1, 1)]


def test_a_side_channel_takes_its_river_s_lead():
    graph, water = _river(n=3)
    graph.nodes["side"] = N("side", 2, edge_types=("1350",), length_m=500.0)
    graph.edges.append(E("s1", "side"))
    water["side"] = "gnis:river"
    panels = build(graph, MODEL, [("OWN", "s0", 40, False)], water_of=water)
    # `mainstem_of` adopts the LARGEST same-code reach in reach of the channel: s2
    assert panels.by_section["side"] == panels.by_section["s2"]
    assert _leads(panels, "side") == {"OWN": 1}


# ------------------------------------------------------------------ mutation pins

def _mutant(fn, old, new):
    src = textwrap.dedent(inspect.getsource(fn))
    assert src.count(old) == 1, f"{old!r} moved; update this pin"
    ns = dict(vars(P))
    exec(compile(src.replace(old, new), P.__file__, "exec"), ns)
    return ns[fn.__name__]


def test_the_far_condition_is_what_holds_the_far_tests():
    mutant = _mutant(may_lead, "distance_m <= far_limit_km(area_target, area_gauge, scale) * 1000.0", "True")
    assert not may_lead(63_113.0, 9_245.7, 807_100.0)
    assert mutant(63_113.0, 9_245.7, 807_100.0)


def test_the_published_scale_is_what_holds_the_published_far_test():
    mutant = _mutant(may_lead, "scale = area_scale(area_gauge, published_km2)", "scale = 1.0")
    assert may_lead(3193.0, 2084.473, 47_217.0, published_km2=5568.0)
    assert not mutant(3193.0, 2084.473, 47_217.0, published_km2=5568.0)


def test_the_trust_bound_is_what_holds_the_untrusted_test():
    mutant = _mutant(area_scale, "if 1.0 / OWN_AREA_TRUST <= k <= OWN_AREA_TRUST else 1.0", "")
    assert area_scale(1.421, 232_000.0) == 1.0
    assert mutant(1.421, 232_000.0) == pytest.approx(232_000.0 / 1.421)


def test_the_smaller_condition_is_what_holds_the_smaller_tests():
    mutant = _mutant(may_lead, "area_target > area_gauge * OWN_SMALLER_RATIO", "False")
    assert not may_lead(1102.0, 61.8, 0.0)
    assert mutant(1102.0, 61.8, 0.0)


def test_the_floor_is_what_holds_the_main_weight():
    mutant = _mutant(lead_shares, "max(own_share, norm[lead])", "norm[lead]")
    assert lead_shares([1.0, 1.0, 1.0], 0) == pytest.approx([0.7, 0.15, 0.15])
    assert mutant([1.0, 1.0, 1.0], 0) == pytest.approx([1 / 3, 1 / 3, 1 / 3])


def _build_mutant(old, new):
    import pipeline.gauges.consume.panels as bp
    src = textwrap.dedent(inspect.getsource(bp.build))
    assert src.count(old) == 1, f"{old!r} moved; update this pin"
    ns = dict(vars(bp))
    exec(compile(src.replace(old, new), bp.__file__, "exec"), ns)
    return ns["build"]


def test_the_per_quantity_limit_is_what_holds_the_candidate_test():
    mutant = _build_mutant("[:OWN_LEADS_PER_QUANTITY]", "")
    graph, water = _river(n=7, length=1_000.0)
    donors = [(f"G{i}", f"s{i}", 40, False) for i in range(6)]
    measures = {**{f"G{i}": {"level"} for i in range(1, 6)}, "G0": {"discharge"}}
    got = mutant(graph, MODEL, donors, water_of=water, measures=measures)
    assert _leads(got, "s6") == {"G5": 1, "G4": 2, "G3": 3, "G2": 4, "G1": 5, "G0": 6}


def test_the_published_tie_break_is_what_holds_the_chilliwack_test():
    mutant = _build_mutant("abs(math.log(area_scale(by_station[st], pub.get(st)))), ", "")
    graph, water = _river(n=2, mag=644)
    donors = [("A_LAKE", "s0", 40, False), ("B_SLESSE", "s0", 40, False)]
    got = mutant(graph, MODEL, donors, water_of=water, published={"A_LAKE": 335.0, "B_SLESSE": 650.4})
    assert _leads(got, "s0") == {"A_LAKE": 1, "B_SLESSE": 2}


def test_riding_on_top_of_the_cap_is_what_holds_the_cap_test():
    mutant = _mutant(P.panel_for, " or d.station in wanted]", "]")
    cand = [("OWN", "up", 100.0, 3, False, False, True)] + [
        (f"AUX{i}", "up", 100.0, 60, False, False, False) for i in range(MEMBERS_PER_QUANTITY)]
    assert "OWN" in {d.station for d in P.panel_for(cand, 101.0, keep=["OWN"])}
    assert "OWN" not in {d.station for d in mutant(cand, 101.0, keep=["OWN"])}
