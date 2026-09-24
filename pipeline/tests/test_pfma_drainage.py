"""PFMA drainage (pipeline/atlas/splits/pfma_drainage.py) — against the real layers.

Needs the fetched `pfma_areas` layer and the FWA `streams` layer, so most of these skip where
those are absent rather than failing a clean checkout.
"""

import sqlite3
from pathlib import Path

import pytest

from project_config import get_config
from pipeline.atlas.splits import pfma_drainage as pd_


def _has(layer: str) -> bool:
    try:
        con = sqlite3.connect(str(get_config().fwa_data_gpkg))
        return bool(con.execute(
            "SELECT 1 FROM gpkg_contents WHERE table_name = ?", (layer,)).fetchone())
    except Exception:
        return False


needs_layers = pytest.mark.skipif(
    not (_has(pd_.LAYER) and _has("streams")),
    reason=f"needs the {pd_.LAYER} and streams layers "
           f"(fetch: python -m data.fetch_data --layers pfma_areas)")


def test_basin_is_a_prefix_of_the_watershed_code():
    """**The whole design in one function.** The FWA code is a path from the ocean inward, so a
    basin is a string prefix: `400-` is the whole Skeena, `910-665062-` the whole Kitimat. A major
    watershed is the first group; a `9XX` coastal stream is the first two."""
    assert pd_.basin_of("400-000000-000000-000000") == "400-"         # Skeena mainstem
    assert pd_.basin_of("400-179139-522840-000000") == "400-"         # deep in the Skeena
    assert pd_.basin_of("500-000000-000000-000000") == "500-"         # Nass
    assert pd_.basin_of("910-665062-000000-000000") == "910-665062-"  # Kitimat
    assert pd_.basin_of("915-669295-093269-000000") == "915-669295-"  # on a coastline trunk
    assert pd_.basin_of("999-999999-999999-999999") is None           # FWA's unknown
    assert pd_.basin_of("") is None and pd_.basin_of(None) is None


def test_area_id_names_the_drainage_not_the_marine_area():
    """`area:pfma:5` would name the salt water. A rule bound to that selects the estuary slivers
    touching it and nothing else — the streams the regulation means are all UPSTREAM."""
    assert pd_.area_id(5) == "area:drains_to:pfma_5"


def test_area_of_is_a_lookup():
    basins = {"400-": 4, "910-665062-": 6}
    assert pd_.area_of("400-179139-522840-000000", basins) == 4
    assert pd_.area_of("910-665062-441122-000000", basins) == 6
    assert pd_.area_of("915-669295-093269-000000", basins) is None
    assert pd_.area_of("999-999999-999999-999999", basins) is None


@needs_layers
def test_missing_area_is_an_error_not_an_empty_answer():
    """A silently empty area is indistinguishable from a water with no regulation."""
    with pytest.raises(ValueError, match="carries no polygon"):
        pd_.basin_areas(areas=(9999,))


@needs_layers
def test_asking_for_one_area_gives_the_same_answer_as_asking_for_four():
    """**The search is province-wide; `areas` filters the RESULT only.**

    Restricting earlier bites twice. Ask only about Area 5 and the Skeena's mouth (Chatham Sound,
    Area 4) is out of view — so its tributaries down in the Ecstall get filed under Area 5, and
    section E's Area 5 rule reaches the whole Skeena system. A neighbouring Area that was not
    offered also cannot win.
    """
    four = pd_.basin_areas((3, 4, 5, 6))
    five = pd_.basin_areas((5,))
    assert five == {b: a for b, a in four.items() if a == 5}
    assert "400-" not in five, "the Skeena drains to Area 4 and must not appear under Area 5"


def test_the_coastal_group_table_is_complete():
    """FWA's 9XX major-watershed groups. Three of them are NOT British Columbia, and a DFO Pacific
    Region rule does not reach those."""
    assert len(pd_.COASTAL_GROUPS) == 15
    assert pd_.COASTAL_GROUPS["910"].startswith("North Coast Rivers")
    assert pd_.COASTAL_GROUPS["990"].startswith("Alsek")
    assert pd_.OUT_OF_PROVINCE_GROUPS == {"960", "970", "990"}
    assert pd_.OUT_OF_PROVINCE_GROUPS <= set(pd_.COASTAL_GROUPS)


@needs_layers
def test_out_of_province_basins_never_get_an_area():
    """**Geometry alone cannot exclude these.** Hyder, Alaska sits at the head of Portland Canal,
    so a `960` stream can be metres from Area 3 — nearer than plenty of legitimate BC creeks.

    The Alsek is the visible case: 237 `990-` basins, including Tough Creek, cross into Alaska and
    reach the sea in the Gulf of Alaska, 647 km from Area 3. The Tatshenshini is a NAMED DFO
    Region 6 water, so this is not hypothetical — section E must not reach that drainage.
    """
    basins = pd_.basin_areas()
    leaked = [b for b in basins if b.split("-")[0] in pd_.OUT_OF_PROVINCE_GROUPS]
    assert not leaked, f"out-of-province basins were assigned an Area: {leaked[:5]}"


@needs_layers
def test_areas_3_to_6_draw_only_from_the_north_coast():
    """Areas 3-6 are the coast north of Cape Caution, so the only coastal groups that should
    appear are 910 and 915 — plus the two major rivers that empty into them, the Skeena and the
    Nass. A south-coast or Vancouver Island group turning up means the mouth search has drifted."""
    groups = {b.split("-")[0] for b in pd_.basin_areas()}
    assert groups == {"910", "915", "400", "500"}, f"unexpected groups: {sorted(groups)}"


@needs_layers
def test_basins_that_never_reach_tidewater_are_dropped():
    """The distance guard catches what the CODE table cannot: inland MAJOR watersheds, which carry
    no 9XX group. Exactly four reach this far — Nazcha (`800-`), Wolverine (`700-`), Hoy (`200-`)
    and Olatine (`600-`) — draining to the Arctic, the Peace or the Liard rather than the Pacific.
    The data leaves a wide gap: p97 is 0 m, 15,464 of 15,720 are within 10 km, the rest jump to
    hundreds of kilometres."""
    assert pd_.MAX_MOUTH_OFFSET_M == 10_000
    basins = pd_.basin_areas()
    assert 1_000 < len(basins) < 6_000


@needs_layers
def test_the_big_rivers_drain_to_the_right_area():
    """**The check that makes the assignment trustworthy.** These Areas are known independently of
    this code: the Skeena enters at Chatham Sound (4), the Nass at Portland Inlet (3), the Kitimat
    down Douglas Channel (6)."""
    basins = pd_.basin_areas()
    assert basins.get("400-") == 4
    assert basins.get("500-") == 3
    assert basins.get("910-665062-") == 6


@needs_layers
def test_a_rivers_braids_do_not_become_extra_basins():
    """618 blue lines carry the Skeena's `400-000000-000000-…`, one per side channel, each with its
    own local DOWNSTREAM_ROUTE_MEASURE 0 — one of them 110 km inland near Babine. They all share
    the prefix, so they are ONE basin, and the mouth is whichever candidate is nearest the sea. If
    this regresses, inland braids become their own basins and get their own Areas."""
    basins = pd_.basin_areas()
    skeena = [b for b in basins if b.startswith("400")]
    assert skeena == ["400-"], f"the Skeena split into {len(skeena)} basins: {skeena[:5]}"


@needs_layers
def test_the_basin_count_is_in_range():
    """Nearest-Area always returns something, so the counts are the sanity check. An
    order-of-magnitude move means the basin or mouth rule drifted."""
    basins = pd_.basin_areas()
    assert 1_000 < len(basins) < 6_000, (
        f"{len(basins):,} basins draining to Areas 3-6 — far off means the basin rule broke")
    assert set(basins.values()) <= set(pd_.SECTION_E_AREAS)


@needs_layers
def test_the_prefix_agrees_with_the_tributary_walk():
    """**The cross-validation the design rests on.** If a basin really is a prefix, the sections
    `build_reach` reaches by walking the Skeena's tributaries must all carry `400`. Measured:
    84,617 of 84,624 — 99.99%. The handful that differ are tidal channels at the mouth, where the
    Skeena meets the coastal system.
    """
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    from pipeline.atlas.reach.build import build_reach
    from pipeline.regs.dfo_salmon.splitwork import _registry

    graph_path = Path(GENERATED.build()) / "graph.pkl"
    if not graph_path.exists():
        pytest.skip("no built atlas on this machine")
    reg, g = _registry(), read_artifact(graph_path)

    entry = {"entry_id": "t", "matched": [], "includes_tributaries": True}
    rule = {"rule_id": "r1", "extents": [{"op": "whole", "item_id": "gnis:2936"}],
            "includes_tributaries": True, "tributaries_only": False, "tributary_excludes": []}
    walked, _ = build_reach(entry, rule, reg, g)
    sections = list(walked.sections or [])
    assert len(sections) > 50_000, "the Skeena walk collapsed"

    in_basin = sum(1 for s in sections
                   if pd_.basin_of(getattr(g.nodes.get(s), "wsc", "") or "") == "400-")
    share = in_basin / len(sections)
    assert share > 0.99, (
        f"only {share:.2%} of the walked Skeena carries basin prefix '400-' — the prefix and the "
        f"walk disagree, so a basin is not a prefix after all")


@needs_layers
def test_the_remaining_uncertainty_is_recorded_not_hidden():
    """Keeps the one open question visible: nearest-Area always returns something, and ~39 mouths
    sit beyond 100 m. That must reach a curator before this is minted into the registry."""
    assert "nearest-Area" in pd_.LIMITATION
    assert "curator" in pd_.LIMITATION
