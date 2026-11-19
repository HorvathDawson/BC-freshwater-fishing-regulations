"""(T864) The app's "near B.C." outline is what `pipeline.deliver.tiles.near_bc` makes from the committed boundary.

A changed boundary, margin or simplification must be regenerated (`python -m pipeline.deliver.tiles.near_bc`) and
committed with it, or the app's location check would answer for an outline nobody can rebuild."""
from __future__ import annotations

from pipeline.common.curated import SOURCE
from pipeline.deliver.tiles import near_bc


def test_the_committed_outline_is_what_the_generator_makes(tmp_path):
    out = near_bc.build(SOURCE / "bc_boundary.geojson", tmp_path / "nearBcOutline.ts")
    assert out.read_text() == near_bc.OUT.read_text(), "regenerate: python -m pipeline.deliver.tiles.near_bc"
