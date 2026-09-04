"""The drainage staircase, and the two-exponent separation it rests on."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import pytest

from pipeline.atlas.graph.drainage import (
    AreaModel, basin_of, fit_area_model, magnitude_at, primary_edges, staircase,
)


@dataclass
class N:
    node_id: str
    stream_magnitude: int = 1
    down_m: float = 0.0
    up_m: float = 1000.0
    length_m: float = 1000.0
    wsc: str = "100-000000"


@dataclass
class E:
    from_node: str
    to_node: str
    at_measure: float | None = None


@dataclass
class G:
    nodes: dict = field(default_factory=dict)
    edges: list = field(default_factory=list)


def g(nodes, edges):
    return G(nodes={n.node_id: n for n in nodes}, edges=edges)


# ------------------------------------------------------------------ the staircase

def test_a_section_with_no_inner_junction_gets_no_row():
    """Most sections are a constant. Storing a staircase for them would be 1.2M rows of
    'nothing changes here'."""
    graph = g([N("main", 10), N("trib", 3)], [E("trib", "main", at_measure=0.0)])
    assert staircase(graph) == {}


def test_a_junction_in_the_middle_becomes_a_step():
    graph = g([N("main", 10, 0.0, 1000.0), N("trib", 4)],
              [E("trib", "main", at_measure=400.0)])
    assert staircase(graph) == {"main": [(400.0, 4)]}


def test_steps_come_out_in_order_up_the_section():
    graph = g([N("main", 20, 0.0, 1000.0), N("a", 3), N("b", 5), N("c", 2)],
              [E("a", "main", at_measure=700.0), E("b", "main", at_measure=200.0),
               E("c", "main", at_measure=450.0)])
    assert staircase(graph)["main"] == [(200.0, 5), (450.0, 2), (700.0, 3)]


def test_a_junction_at_either_end_is_not_inner():
    """An edge landing on the boundary is the section's own connection to its neighbour.
    Counting it as a step would subtract the whole upstream network from the top of the
    section, which is the section's magnitude, not a change within it."""
    graph = g([N("main", 10, 0.0, 1000.0), N("a", 3), N("b", 2)],
              [E("a", "main", at_measure=0.0), E("b", "main", at_measure=1000.0)])
    assert staircase(graph) == {}


def test_a_measure_of_none_is_skipped_not_treated_as_zero():
    graph = g([N("main", 10, 0.0, 1000.0), N("t", 3)], [E("t", "main", at_measure=None)])
    assert staircase(graph) == {}


# ------------------------------------------------------------------ reading it

def test_below_every_junction_you_have_the_whole_section():
    steps = [(200.0, 5), (700.0, 3)]
    assert magnitude_at(20, steps, 100.0) == 20


def test_above_a_junction_that_creek_is_no_longer_yours():
    steps = [(200.0, 5), (700.0, 3)]
    assert magnitude_at(20, steps, 400.0) == 15      # the 200 m creek is below you
    assert magnitude_at(20, steps, 900.0) == 12      # both are


def test_a_section_with_no_steps_is_flat():
    assert magnitude_at(7, None, 500.0) == 7
    assert magnitude_at(7, [], 500.0) == 7


def test_it_never_returns_a_catchment_smaller_than_one_headwater():
    """Where the braid tree has still double-counted, the honest degradation is a floor —
    'at least this much' rather than a negative catchment."""
    assert magnitude_at(3, [(100.0, 99)], 500.0) == 1


# ------------------------------------------------------------------ braids

def test_a_braid_sends_its_water_down_one_channel_only():
    """`up` splits into two channels that both reach `main`. Left alone it appears in the
    staircase twice — once behind each channel — and the section is credited with 16
    headwaters it does not have. The spanning tree cuts one of the two outflows, so `up`
    is delivered once.

    Note what this does NOT fix: `small` still carries its own FWA magnitude and still
    reports it on arrival. FWA assigns a braid's magnitude to both channels, so the count
    can still exceed the truth — which is why `magnitude_at` floors rather than trusting
    the arithmetic. The tree removes the doubling; the floor catches what is left."""
    graph = g([N("up", 8), N("big", 8), N("small", 8), N("main", 9, 0.0, 1000.0)],
              [E("up", "big", at_measure=0.0), E("up", "small", at_measure=0.0),
               E("big", "main", at_measure=500.0), E("small", "main", at_measure=600.0)])
    assert len(primary_edges(graph)) == 3       # `up` keeps only one of its two outflows

    # and the read is still sane above both arrivals, because the floor holds
    steps = staircase(graph)["main"]
    assert magnitude_at(9, steps, 900.0) >= 1


def test_the_channel_kept_is_the_mainstem():
    graph = g([N("up", 8), N("big", 8), N("small", 1)],
              [E("up", "small", at_measure=0.0), E("up", "big", at_measure=0.0)])
    kept = primary_edges(graph)
    assert [graph.edges[i].to_node for i in sorted(kept)] == ["big"]


# ------------------------------------------------------------------ the area model

def test_the_fit_recovers_a_relationship_it_was_given():
    pts = [(1.5 * m ** 0.85, m, "100-000000") for m in (1, 4, 20, 90, 400, 3000, 20000)]
    fit = fit_area_model(pts)
    assert fit.b == pytest.approx(0.85, abs=0.01)
    assert fit.c_default == pytest.approx(1.5, rel=0.02)


def test_a_basin_with_enough_points_gets_its_own_constant():
    """A wet coastal basin maps more headwater links per square kilometre than a dry
    plateau, so one province-wide constant is a bias that varies with where you are."""
    wet = [(3.0 * m ** 0.85, m, "100-000000") for m in range(2, 60)]
    dry = [(1.0 * m ** 0.85, m, "200-000000") for m in range(2, 60)]
    fit = fit_area_model(wet + dry)
    assert fit.c_by_basin["100"] == pytest.approx(3.0, rel=0.03)
    assert fit.c_by_basin["200"] == pytest.approx(1.0, rel=0.03)
    assert fit.area_km2(100, "100-x") > fit.area_km2(100, "200-x")


def test_a_basin_with_too_few_points_falls_back_rather_than_overfitting():
    many = [(2.0 * m ** 0.9, m, "100-000000") for m in range(2, 80)]
    few = [(9.9 * m ** 0.9, m, "900-000000") for m in range(2, 6)]
    fit = fit_area_model(many + few)
    assert "900" not in fit.c_by_basin
    assert fit.area_km2(50, "900-x") == pytest.approx(fit.c_default * 50 ** fit.b)


def test_no_magnitude_means_no_area_rather_than_a_small_one():
    fit = AreaModel(b=0.85, c_default=1.5, c_by_basin={})
    assert fit.area_km2(None, "100-x") is None
    assert fit.area_km2(0, "100-x") is None


def test_the_basin_key_is_the_head_of_the_watershed_code():
    assert basin_of("100-000000-000000") == "100"
    assert basin_of("") == ""
    assert basin_of(None) == ""


def test_the_ratio_is_what_this_is_for_and_the_constant_cancels_inside_a_basin():
    """The exponent may be attenuated by ordinary least squares, so it is only trusted
    inside a ratio between two points that share a basin — where `c` divides out."""
    fit = AreaModel(b=0.85, c_default=1.5, c_by_basin={"100": 4.0})
    a, b = fit.area_km2(400, "100-x"), fit.area_km2(50, "100-y")
    assert a / b == pytest.approx((400 / 50) ** 0.85, rel=1e-9)
