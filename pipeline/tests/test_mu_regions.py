"""Which region administers each management unit — and why that is not where a SECTION's
region comes from.

THE MU NUMBER IS NOT THE REGION. Splitting "6-13" at the dash gives 6, and Haida Gwaii is
administered from Region 1. That inference is what dropped all 25 Region 1 rules reaching the
Yakoun River — the bait ban, the barbless-hook rule, and Haida Gwaii's own quota table — and
left a screen with no quotas from either region on it.

The source layer does not say so either: MUs 6-12 and 6-13 still carry
``REGION_RESPONSIBLE_ID = 6``, and the move is curated, in the `remap` beside the `regions`
area in `areas.json`. `mu_region_table()` reads BOTH, so the fact has one home.

These need the source geopackage (10 GB, a fetch away) and skip without it.
"""

from __future__ import annotations

import pytest

pytest.importorskip("geopandas")

from pipeline.atlas.splits.area_splits import _GPKG, mu_region_table  # noqa: E402
from pipeline.tests.conftest import ATLAS_HINT, GPKG_HINT, need  # noqa: E402


@pytest.fixture(scope="module")
def table(request) -> dict[str, str]:
    need(request, "source", _GPKG, GPKG_HINT)
    return mu_region_table()


@pytest.mark.needs_source
@pytest.mark.slow
def test_every_management_unit_has_exactly_one_region(table):
    """A unit belongs to one region: administration is a partition, not an overlap."""
    assert len(table) == 225, f"expected 225 management units, got {len(table)}"
    assert all(v for v in table.values()), "a unit with no region"
    assert sorted(set(table.values())) == ["1", "2", "3", "4", "5", "6", "7A", "7B", "8"]


@pytest.mark.needs_source
@pytest.mark.slow
def test_haida_gwaii_is_region_1(table):
    """The whole defect in two assertions. 7A and 7B are kept apart in the same breath —
    they share the name "Omineca" and are two regions in the book with different rules."""
    assert table["6-12"] == "1"
    assert table["6-13"] == "1"
    assert table["1-4"] == "1", "an ordinary Region 1 unit still reads Region 1"
    assert table["6-1"] == "6", "the remap moved only Haida Gwaii"
    assert table["7-12"] == "7A" and table["7-33"] == "7B"


@pytest.mark.needs_source
@pytest.mark.slow
@pytest.mark.needs_atlas
def test_the_table_never_contradicts_the_region_polygons():
    """THE TABLE IS NOT WHERE A SECTION'S REGION COMES FROM, and this is the guard on both.

    `node.in_areas` carries the region polygon a section was found inside; the table answers a
    different question, about a unit rather than a piece of water. Measured over all 1,957,890
    sections they agree on 1,955,747, the table loses nothing the polygons found, and it
    recovers a region on 394 they missed.

    Where they differ the table is WIDER, never contradictory: `mus` is every unit a piece
    TOUCHES, so a section cut at a region line sits wholly in one region while still touching a
    unit across it. That is why the polygons decide — a section given two regions picks up both
    rulebooks, which is the defect the cutter exists to prevent. This test allows wider and
    fails on disjoint: a section whose polygon region is NOT among its units' regions would mean
    the two sources disagree about the province, and one of them is then wrong.
    """
    import pathlib

    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact

    need(None, "source", _GPKG, GPKG_HINT)
    graph_path = need(None, "atlas", GENERATED.atlas.builds / GENERATED.atlas.default_build
                      / "graph.pkl", ATLAS_HINT)

    tbl = mu_region_table()
    graph = read_artifact(str(graph_path))
    disjoint = []
    for nid, n in graph.nodes.items():
        poly = {a.split(":")[-1].upper() for a in (getattr(n, "in_areas", ()) or ())
                if a.startswith("area:region:")}
        units = {tbl[m] for m in (getattr(n, "mus", ()) or ()) if m in tbl}
        if poly and units and not (poly & units):
            disjoint.append((nid, sorted(poly), sorted(units)))
            if len(disjoint) > 5:
                break
    assert not disjoint, (
        "sections whose region polygon shares nothing with their units' regions — the "
        f"polygons and the unit table disagree about the province: {disjoint}")
