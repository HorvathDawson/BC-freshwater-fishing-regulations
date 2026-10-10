"""BOUNDARIES ON A BUILT ATLAS (BOUND round, 2026-10-06) — `-m slow`.

The user's rulings: slivers are prevented at the SOURCE (no snap, no merge, no metre floor; the
build gate only asserts — `sliver_gate`); the B.C. outline is the exact union of the region
polygons, so no strip inside B.C. reads as outside; ecological reserves and parks are cut clean, so
a stream grazing one is not a member of it. Each test below reads the artifacts a build wrote
(`ATLAS_BUILD`, else the promoted atlas; `UI_EXPORT_BUNDLE`, else the shipped bundle) and names
the real waters the rulings were made about.

Before this round the live atlas held 108 cut pieces under 1 m, 270 outside-B.C. sections lying
inside the exact outline (Fishtrap, Pepin, Bertrand creeks and the Sumas River on the U.S. line …)
and 7 zero-length ones in no region (the West Road, the Cottonwood, Ahbau Creek), and three streams
carried an ecological reserve's closure along kilometres of water that touched the reserve by
1.5-4.7 m.
"""
from __future__ import annotations

import collections
import os
import sqlite3
from functools import lru_cache
from pathlib import Path

import pytest
from pipeline.tests.conftest import need, ATLAS_HINT, BUNDLE_HINT, GPKG_HINT

pytestmark = pytest.mark.slow


def _atlas() -> Path:
    from pipeline.common.curated import GENERATED
    return Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())


def _bundle() -> Path:
    from pipeline.common.curated import GENERATED
    return Path(os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite")


@lru_cache(maxsize=1)
def _graph():
    from pipeline.common.io.serialize import read_artifact
    p = need(None, "atlas", _atlas() / "graph.pkl", ATLAS_HINT)
    return read_artifact(str(p))


@lru_cache(maxsize=1)
def _geoms():
    from pipeline.common.io.serialize import read_artifact
    return read_artifact(str(need(None, "atlas", _atlas() / "geometries.pkl", ATLAS_HINT)))


@lru_cache(maxsize=1)
def _inner():
    """The exact B.C. outline, 1 m in."""
    from pipeline.atlas.splits.bc_boundary import load_outline
    from project_config import get_config
    return load_outline(need(None, "source", get_config().fwa_data_gpkg, GPKG_HINT)).buffer(-1.0)


def _pieces(blk: str):
    g = _graph()
    return sorted((n for n in g.nodes.values()
                   if n.blk == blk and getattr(n.kind, "value", n.kind) == "stream"),
                  key=lambda n: n.down_m)


def _ids(b) -> set[str]:
    return set() if b is None else {str(b.boundary_id), *map(str, b.aliases or ())}


@pytest.mark.needs_atlas
def test_no_cut_left_a_sliver():
    """THE GATE, re-asked of the finished graph: no stream piece under `SLIVER_M` has a CUT at
    either end (a `split:` boundary). FWA's own short pieces lie between natural ends (a mouth, a
    source, a lake edge) and are not the cutter's."""
    from pipeline.atlas.splits.sliver_gate import SLIVER_M
    bad = []
    for nid, n in _graph().nodes.items():
        if getattr(n.kind, "value", n.kind) != "stream" or n.up_m - n.down_m >= SLIVER_M:
            continue
        ends = _ids(n.lower_bound) | _ids(n.upper_bound)
        if any(e.startswith("split:") for e in ends):
            bad.append((nid, round(n.up_m - n.down_m, 3), sorted(ends)[:3]))
    assert bad == [], f"{len(bad)} sliver(s) at a cut, e.g. {bad[:5]}"


@pytest.mark.needs_source
@pytest.mark.needs_bundle
@pytest.mark.needs_atlas
def test_no_strip_inside_bc_reads_as_outside():
    """An outside-B.C. section whose line lies more than 1 m inside the exact outline was the
    region/outline mismatch (270 in the live atlas) or a zero-length piece in no region (7). The
    26 flagged by `border._mark_measure_beyond_the_data` (a piece whose route measure runs on far
    past its geometry — Saxon and McKercher creeks on Vancouver Island) are a separate, pre-existing
    rule and are listed apart."""
    from pipeline.common.section_handles import read as read_handles
    need(None, "bundle", _bundle(), BUNDLE_HINT)
    need(None, "atlas", _atlas() / "section_handles.txt", ATLAS_HINT)
    _, handles = read_handles(_atlas())
    node_of = {s: n for n, s in handles.items()}
    db = sqlite3.connect(f"file:{_bundle()}?mode=ro", uri=True)
    try:
        osids = [r[0] for r in db.execute("SELECT sid FROM outside_bc")]
    finally:
        db.close()
    inner, geoms, g = _inner(), _geoms(), _graph()
    bad, measure_rule = [], []
    for sid in osids:
        nid = node_of.get(sid)
        line = geoms.get(nid)
        if line is None or line.is_empty or not inner.covers(line.interpolate(0.5, normalized=True)):
            continue
        n = g.nodes[nid]
        if n.out_of_bc and n.up_m - n.down_m >= 250.0 and line.length < 250.0:
            measure_rule.append(nid)
            continue
        bad.append((nid, n.display_name, round(line.length, 1)))
    assert bad == [], f"{len(bad)} strip(s) inside B.C. read as outside, e.g. {bad[:8]}"


@pytest.mark.needs_source
@pytest.mark.needs_atlas
@pytest.mark.needs_bundle
@pytest.mark.parametrize("name", ["Fishtrap Creek", "Pepin Creek", "Bertrand Creek", "Sumas River",
                                  "Cottonwood River", "Ahbau Creek", "West Road (Blackwater) River"])
def test_named_waters_that_had_strips_read_as_outside(name):
    """Waters that shipped a stretch inside B.C. as "outside B.C.": every part of them inside the
    outline now has a region. Read off the bundle's sections of the named item."""
    from pipeline.common.section_handles import read as read_handles
    need(None, "bundle", _bundle(), BUNDLE_HINT)
    db = sqlite3.connect(f"file:{_bundle()}?mode=ro", uri=True)
    try:
        sids = {r[0] for r in db.execute(
            "SELECT s.sid FROM item i JOIN item_section s ON s.ord = i.ord WHERE i.name = ?",
            (name,))}
        outside = {r[0] for r in db.execute("SELECT sid FROM outside_bc")}
    finally:
        db.close()
    assert sids, f"{name} has no sections in the bundle"
    _, handles = read_handles(_atlas())
    node_of = {s: n for n, s in handles.items()}
    inner, geoms = _inner(), _geoms()
    bad = [node_of[s] for s in sids & outside
           if geoms.get(node_of[s]) is not None and not geoms[node_of[s]].is_empty
           and inner.covers(geoms[node_of[s]].interpolate(0.5, normalized=True))]
    assert bad == [], (name, bad)


# ---- clean cut on the real reserves -----------------------------------------------------------------
GRAZES = [  # (blk, measure range of the piece the reserve's closure used to cover, reserve)
    ("356570499", (4466.7, 10734.0), "trout_creek_ecological_reserve"),            # Trout Creek, 4.18 m in
    ("360876178", (45434.1, 52883.6), "gladys_lake_ecological_reserve"),           # Eaglenest Creek, 1.49 m
    ("359549195", (441.1, 4364.0), "fort_nelson_river_ecological_reserve"),        # unnamed, 4.66 m
]


@pytest.mark.needs_atlas
@pytest.mark.parametrize("blk,rng,reserve", GRAZES)
def test_a_stream_grazing_a_reserve_is_not_in_it(blk, rng, reserve):
    """Each of these touched the reserve by a few metres, and the overlap test put the whole piece
    — kilometres of creek — under the reserve's closure. A graze is no cut and no membership."""
    want = f"area:ecological_reserves:{reserve}"
    # under the closure rule (rejoin_m) the straddle along the reserve's edge is inside, and it may
    # end a few metres into the old piece's range; the km-long piece beyond it must not be a member
    hit = [n.node_id for n in _pieces(blk)
           if n.down_m >= rng[0] - 5.0 and n.up_m <= rng[1] + 1.0 and want in n.in_areas]
    assert hit == [], hit
    assert any(n.down_m >= rng[0] - 5.0 for n in _pieces(blk)), "the old piece's range is still drawn"


@pytest.mark.needs_atlas
def test_ospika_river_is_not_widened():
    """The Ospika's mouth lies 1.9 m inside the Ospika Cones reserve's edge. Cut there, it left a
    1.9 m sliver; merged (the rejected repair), it handed the reserve's closure to the 329 m piece
    above. A crossing inside the crossing zone of a line's end is no cut: nothing of the mouth
    stretch is in the reserve."""
    ps = [n for n in _pieces("359020636") if n.down_m < 328.9]
    assert ps, "the Ospika's mouth stretch"
    assert all(not any("ecological_reserves" in a for a in n.in_areas) for n in ps), \
        [(n.node_id, n.in_areas) for n in ps]
    assert all(n.up_m - n.down_m >= 5.0 for n in ps)


@pytest.mark.needs_atlas
def test_a_real_dip_into_a_reserve_is_cut_on_entry_and_exit():
    """Trematon Creek crosses into Lasqueti Island Ecological Reserve for 80 m and out again —
    longer than the reserve's 50 m crossing zone, so it is two cuts, and the stretch between them,
    and only it, is in the reserve."""
    want = "area:ecological_reserves:lasqueti_island_ecological_reserve"
    ps = _pieces("354127144")
    inside = [n for n in ps if want in n.in_areas]
    assert len(inside) == 1, [(n.node_id, n.down_m, n.up_m, n.in_areas) for n in ps]
    n = inside[0]
    cut = lambda b: any(i.startswith("split:area:") for i in _ids(b))
    assert cut(n.lower_bound) and cut(n.upper_bound)
    assert 50.0 <= n.up_m - n.down_m <= 120.0, (n.down_m, n.up_m)


@pytest.mark.needs_atlas
def test_border_and_region_line_are_one_cut():
    """Where a river leaves B.C. through a region's edge the two are one geometry, so one boundary
    carries both names with the border canonical (`bc_border` token)."""
    from pipeline.common.models import BoundaryKind
    both = border_only = 0
    for n in _graph().nodes.values():
        b = n.upper_bound
        if b is None or b.kind != BoundaryKind.border:
            continue
        border_only += 1
        if any(a.startswith("split:area:") for a in (b.aliases or ())):
            both += 1
        assert not str(b.boundary_id).startswith("split:area:"), b
    assert border_only and both, (border_only, both)


@pytest.mark.needs_atlas
def test_two_builds_of_identical_inputs_are_identical():
    """DETERMINISM (BOUND round, 2026-10-06): `ATLAS_BUILD_TWIN` names a second build of the same
    code and inputs (run under another PYTHONHASHSEED). Every artifact a reader consumes must be
    byte-identical — the graph (node order included: the registry's section lists follow it), the
    registry, the handles, the region homes, the resolved splits and the geometry (the graph by
    content, see below). Two builds once
    differed on ~50 braided rivers (the braid prune's set order) and on 482 area items' section
    order (unnamed waterbodies minted from a set). `summary.txt` and `*.meta.json` carry the build's
    wall-clock timings and are not compared."""
    twin = os.environ.get("ATLAS_BUILD_TWIN")
    if not twin:
        pytest.skip("ATLAS_BUILD_TWIN not set")
    import hashlib

    from pipeline.common.io.serialize import read_artifact
    for f in ("registry.json", "section_handles.txt", "region_home.json", "splits.resolved.json",
              "geometries.pkl", "blk_chains.pkl", "waterbody_polys.pkl", "item_points.json",
              "merge_report.json"):
        a = hashlib.sha256((_atlas() / f).read_bytes()).hexdigest()
        b = hashlib.sha256((Path(twin) / f).read_bytes()).hexdigest()
        assert a == b, f"{f} differs between {_atlas()} and {twin}"
    # THE GRAPH BY CONTENT: pickle's memo shares an equal object or string in one run and writes it
    # twice in another, so the bytes of graph.pkl can differ while every node (in order), every
    # attribute, every edge and both adjacency maps are identical — which is what a reader sees.
    ga, gb = _graph(), read_artifact(str(Path(twin) / "graph.pkl"))
    assert list(ga.nodes) == list(gb.nodes), "node order"
    assert all(ga.nodes[k] == gb.nodes[k] for k in ga.nodes), "node content"
    assert ga.edges == gb.edges and ga.up_adj == gb.up_adj and ga.down_adj == gb.down_adj


# ---- the closure rule on real water: a straddle is inside (rejoin_m = 1,000 m) ----------------------
def _member_runs(name: str, area: str):
    """The member pieces of `area` along every blue line named `name`, grouped into runs of
    consecutive member pieces: [(blk, first piece down_m, last piece up_m), …]."""
    runs = []
    blks = sorted({n.blk for n in _graph().nodes.values() if n.display_name == name and
                   getattr(n.kind, "value", n.kind) == "stream"})
    for blk in blks:
        cur = None
        for n in _pieces(blk):
            if area in n.in_areas:
                cur = [blk, n.down_m, n.up_m] if cur is None else [blk, cur[1], n.up_m]
            elif cur is not None:
                runs.append(tuple(cur)); cur = None
        if cur is not None:
            runs.append(tuple(cur))
    return runs


@pytest.mark.needs_atlas
def test_krajina_creek_is_inside_from_its_entry():
    """Vladimir J. Krajina (Port Chanal) ER on 360832863: enters at 207 m, out 413-493 m (80 m, a
    straddle), in again. One member run starting at 207 m."""
    area = "area:ecological_reserves:vladimir_j_krajina_port_chanal_ecological_reser"   # name truncated in the source
    ps = [n for n in _pieces("360832863") if area in n.in_areas]
    assert ps and abs(min(n.down_m for n in ps) - 207.4) < 1.0, [(n.down_m, n.up_m) for n in ps]
    lo, hi = min(n.down_m for n in ps), max(n.up_m for n in ps)
    gaps = [n for n in _pieces("360832863") if lo < n.down_m < hi and area not in n.in_areas]
    assert gaps == [], "the 80 m out is straddle, inside"


@pytest.mark.needs_atlas
@pytest.mark.parametrize("name", ["Kicking Horse River", "Beaverfoot River"])
def test_yoho_straddlers_are_one_inside_run(name):
    """The Kicking Horse (longest run out 135 m) and the Beaverfoot (968 m) wander along Yoho's
    edge: under rejoin_m = 1,000 m every straddled stretch is inside — one member run per line."""
    runs = _member_runs(name, "area:national_parks:yoho_national_park_of_canada")
    assert runs, name
    per_blk = collections.Counter(blk for blk, *_ in runs)
    assert all(v == 1 for v in per_blk.values()), runs


@pytest.mark.needs_atlas
@pytest.mark.parametrize("name,area", [
    ("Canyon Creek", "area:ecological_reserves:burnt_cabin_bog_ecological_reserve"),
    ("Carmanah Creek", "area:national_parks:pacific_rim_national_park_reserve_of_canada"),
    ("Cormier Creek", "area:ecological_reserves:blue_dease_rivers_ecological_reserve"),
    ("Hoodas Creek", "area:ecological_reserves:kingcome_river_atlatzi_river_ecological_reserve")])
def test_a_line_out_longer_than_the_rejoin_distance_exits_and_reenters(name, area):
    """At rejoin_m = 1,000 m these four leave their area for more than a kilometre and come back:
    an exit cut and a re-entry cut, so two member runs on one blue line."""
    runs = _member_runs(name, area)
    per_blk = collections.Counter(blk for blk, *_ in runs)
    assert any(v >= 2 for v in per_blk.values()), runs
