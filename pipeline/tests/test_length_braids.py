"""THE LENGTH CUTS AVOID BRAIDS (T67, owner 2026-10-24, Q2 refined): `length_splits.junction_cuts` never takes
a junction a braid spans — the braid sweep's own gate (`braid_sweep.spans_a_braid`) decides — and takes the
next eligible junction by its usual rules; the sweep then finds nothing to cut at the length cuts
(`braid_sweep.length_cut_failures`, the build check). Mutation: without the gate the braid-spanned junction
is taken and the check fires."""

from __future__ import annotations

import pytest

from pipeline.atlas.splits import braid_sweep as BS
from pipeline.atlas.splits.sectionizer import split_graph_at
from pipeline.tests.test_braid_sweep import _G, _apply, _cut, _one_braid, _pieces


def _long_river_with_a_braid():
    """A 30 km host, unnamed tributaries joining at 6,000 and 9,000 m, and a braid spanning 9,000 (it
    rejoins at 8,500 and leaves at 9,500). Cap 10 km: the stage would take 9,000 (the last junction inside
    the cap) — and that cut would straddle the braid."""
    G = _G(host=((0, 0), (30000, 0)))
    for x in (6000, 9000):
        t = G.add(f"T{x}", [(x, 0), (x, -500)], wsc=f"100-{x}")
        G.join(t, "H:0", x, xy=(x, 0))
    G.braid("B", [(8500, 0), (8600, 120), (9400, 120), (9500, 0)], 8500, 9500)
    return G.done()


def test_length_cuts_never_take_a_junction_a_braid_spans():
    from pipeline.atlas.splits.length_splits import junction_cuts
    g, geoms = _long_river_with_a_braid()
    v = BS.View(g, geoms)
    skipped: list = []
    cuts = junction_cuts(g, cap_m=10_000, spanned=lambda b, m: BS.spans_a_braid(v, b, m), skipped=skipped)
    first = sorted(c.route_measure for c in cuts)[0]
    assert first == 8500.0                                  # the next eligible junction: the braid's mouth
    assert [s["m"] for s in skipped] == [9000.0]
    sw = BS.sweep(g, geoms, cuts, view=v)
    assert sw.points == [] and BS.length_cut_failures(sw) == [], "the sweep finds nothing to cut there"


def test_length_mutation_without_the_gate_the_braid_spanned_junction_is_taken():
    from pipeline.atlas.splits.length_splits import junction_cuts
    g, geoms = _long_river_with_a_braid()
    cuts = junction_cuts(g, cap_m=10_000)
    assert 9000.0 in {c.route_measure for c in cuts}
    # ... and the build check would fire: the sweep would have to cut the braid at that length cut
    sw = BS.sweep(g, geoms, cuts)
    assert sw.points and BS.length_cut_failures(sw)
