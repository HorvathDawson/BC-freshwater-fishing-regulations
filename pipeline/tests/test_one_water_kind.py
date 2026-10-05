"""ONE WATER KIND, decided in the registry and read everywhere (user rulings 2026-10-03; FREV/sloughs,
dataflow DF-1/DF-2). The corpus half of `test_registry_flowing.py`: against the built atlas, reach
run, bundle, export and tile layers (`UI_EXPORT_BUNDLE`, `UI_EXPORT_REACHES`, `UI_EXPORT_TILES` — the
tile LAYER directory holding `stream.geojsonl` / `lake.geojsonl` / `wetland.geojsonl`).

The grep gate is fast; everything else is `slow`. Each test names the mutation that turns it red.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

import pytest

from pipeline.common.curated import GENERATED, REPO_ROOT

PIPELINE = REPO_ROOT / "pipeline"
#: The modules allowed to ask whether a NAME flows: the registry pass that decides the kind, the
#: registry build (a river's borrowed name on a lake), and the separate steelhead-list generator.
FLOWS_READERS = {
    "pipeline/common/water_kind.py", "pipeline/atlas/registry/flowing.py",
    "pipeline/atlas/registry/build.py", "pipeline/regs/steelhead/known_waters.py",
}


def test_flows_is_decided_in_the_registry_only():
    """Nothing after the registry recomputes the water kind from a name. MUTATION: re-adding
    `from pipeline.common.water_kind import flows` to the export (or `FLOWING` anywhere) is red."""
    bad = []
    for p in sorted(list(PIPELINE.rglob("*.py")) + list((REPO_ROOT / "curation-review").rglob("*.py"))):
        rel = str(p.relative_to(REPO_ROOT))
        if rel.startswith("pipeline/tests/") or "/node_modules/" in rel:
            continue
        text = p.read_text(encoding="utf-8")
        if re.search(r"water_kind import[^\n]*\b(flows|FLOWING|head_noun)\b"
                     r"|\bFLOW_RE\b|flowing_lake_sections|from pipeline.common.water_kind import",
                     text) and rel not in FLOWS_READERS:
            bad.append(rel)
    assert bad == [], bad
    text = (PIPELINE / "tools/export_ui_rules.py").read_text()
    assert "_flows" not in text and "drawn_as" not in text and "drawn_kind" not in text
    assert "flowing_lake_sections" not in (PIPELINE / "atlas/reach/water_kind.py").read_text()
    assert "FLOW_RE" not in (PIPELINE / "atlas/registry/build.py").read_text()


# --------------------------------------------------------------------------- the built artifacts

def _bundle() -> Path:
    b = Path(os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite")
    if not b.exists():
        pytest.skip(f"no bundle at {b}")
    db = sqlite3.connect(f"file:{b}?mode=ro", uri=True)
    cols = {r[1] for r in db.execute("PRAGMA table_info(section_span)")}
    db.close()
    if "shape" not in cols:
        pytest.skip(f"{b} predates the one water kind (no section_span.shape)")
    return b


@pytest.fixture(scope="module")
def db():
    con = sqlite3.connect(f"file:{_bundle()}?mode=ro", uri=True)
    yield con
    con.close()


@pytest.fixture(scope="module")
def build(db) -> Path:
    return Path(dict(db.execute("SELECT k, v FROM meta"))["build"])


@pytest.fixture(scope="module")
def registry(build):
    from pipeline.atlas.registry import load_registry
    return load_registry(build / "registry.json")


@pytest.fixture(scope="module")
def graph(build):
    from pipeline.common.io.serialize import read_artifact
    return read_artifact(str(build / "graph.pkl"))


@pytest.fixture(scope="module")
def handles(build):
    from pipeline.common.section_handles import read as read_handles
    return read_handles(build)[1]


@pytest.fixture(scope="module")
def doc(db):
    from pipeline.tools import export_ui_rules as X
    return X.build(_bundle())


def _tiles() -> Path:
    d = Path(os.environ.get("UI_EXPORT_TILES") or GENERATED.tiles / "layers")
    if not (d / "lake.geojsonl").exists():
        pytest.skip(f"no tile layers at {d}")
    return d


_SID = re.compile(r'"section_id":(\d+)')
_WATER = re.compile(r'"water":"([a-z]+)"')
_MINZ = re.compile(r'"minzoom":(\d+)')


def _layer(path: Path) -> dict[int, tuple[str | None, int]]:
    """{section_id: (water, minzoom)} for every feature of a tile layer file, read line by line."""
    out: dict[int, tuple[str | None, int]] = {}
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            m = _SID.search(line)
            if not m:
                continue
            w = _WATER.search(line)
            z = _MINZ.search(line)
            out[int(m.group(1))] = (w.group(1) if w else None, int(z.group(1)) if z else -1)
    return out


@pytest.fixture(scope="module")
def tiles():
    d = _tiles()
    return {name: _layer(d / f"{name}.geojsonl") for name in ("stream", "lake", "wetland")}


def _owner(db) -> dict[int, tuple[str, str]]:
    """{sid: (item_id, kind)} from the bundle."""
    return {s: (i, k) for i, k, s in db.execute(
        "SELECT i.item_id, i.kind, s.sid FROM item i JOIN item_section s ON s.ord = i.ord")}


@pytest.mark.slow
def test_one_water_kind(db, registry, graph, handles, doc, tiles):
    """For EVERY section: the bundle owner's `item.kind` == the reach's `kind_of` == the export's
    `waters[owner].kind` == the tile's kind (the `stream` layer, or a `lake`/`wetland` feature
    with `water: stream` => stream; else its layer). Every lake/wetland feature carries `water`; a
    stream's polygon is in BOTH the `stream` and its polygon layer. MUTATIONS: recompute `flows`
    in the export; drop `water` from the LayerSpec; write a stream's route to `under_lake`."""
    from pipeline.atlas.reach.water_kind import kind_of
    owner = _owner(db)
    node_of = {s: n for n, s in handles.items()}
    tile_kind: dict[int, str] = {}
    for layer in ("lake", "wetland"):
        for sid, (water, _) in tiles[layer].items():
            assert water in ("stream", layer), (layer, sid, water)
            tile_kind[sid] = water
    for sid, (water, _) in tiles["stream"].items():
        assert water is None
        if sid in tile_kind:
            assert tile_kind[sid] == "stream", sid          # a polygon in the stream layer is a stream's
        tile_kind.setdefault(sid, "stream")
    bad = []
    for sid, (item, kind) in owner.items():
        reach = kind_of(graph, registry, node_of[sid])
        export = doc["waters"][item]["kind"]
        tile = tile_kind.get(sid)
        if not (kind == reach == export) or (tile is not None and tile != kind):
            bad.append((sid, item, kind, reach, export, tile))
    assert bad == [], bad[:20]
    polys = {s for s, (i, k) in owner.items() if k == "stream" and node_of[s].startswith("lake:")}
    assert polys <= set(tiles["lake"]) | set(tiles["wetland"])
    # every LAKE node of a stream has a route (a wetland node has none: nothing to draw as a line)
    routed = {s for s in polys if graph.nodes[node_of[s]].kind.value == "lake"}
    assert routed and routed <= set(tiles["stream"]), "a stream's polygon draws its route as stream"


@pytest.mark.slow
def test_flowing_polygons_belong_to_their_water(registry):
    """The merge, pinned on the province (MUTATION: any change to `registry.flowing`'s rules)."""
    from pipeline.common.water_kind import flows
    water = {k: v for k, v in registry.items() if v.kind in ("stream", "lake", "wetland")}
    assert not [k for k, v in water.items() if v.kind != "stream" and flows(v.kind, v.name)]
    secs = lambda i: set(registry[i].section_ids)           # noqa: E731
    assert len(secs("gnis:11267")) == 138                    # Rancheria: the 42 on the Little Rancheria
    assert len(secs("wsc:915-679587-924307")) == 13          # East Gribbell
    assert len(secs("gnis:7836")) == 9 and "lake:329148363" in secs("gnis:7836")   # Stellako
    assert registry["gnis:6438"].name == "Six Mile Slough" and len(secs("gnis:6438")) == 12
    assert {"lake:328970802", "lake:328970811", "lake:329177996", "lake:329178131"} <= secs("gnis:21105")
    assert "lake:329707189" in secs("gnis:3062") and "wbk:329707189" not in registry   # Vedder Canal
    assert "wbk:329707189" in registry["gnis:3062"].aliases
    assert {"lake:329587077", "lake:329587644"} <= secs("gnis:14431")                # Hansen: its own
    assert registry["gnis:14431"].kind == "stream" and "lake:329587077" not in secs("gnis:26482")
    assert {"lake:329177998", "lake:329178163"} <= secs("gnis:15743")                # Mountain Slough
    assert registry["wbk:329524002"].kind == "stream"                                 # Lewis (Kootenay)
    for name in ("Bear Creek Reservoir", "River Lakes"):
        assert {v.kind for v in water.values() if v.name == name} == {"lake"}, name
    assert all(v.kind != "stream" for v in water.values() if "OXBOWS" in v.name.upper())
    assert sum(len(v.aliases) for v in water.values()) == 116


@pytest.mark.slow
def test_no_item_has_a_lake_boundary_on_its_own_section_and_no_section_is_owned_twice(registry):
    own: dict[str, str] = {}
    twice, bad = [], []
    for k, v in registry.items():
        if v.kind not in ("stream", "lake", "wetland"):
            continue
        polys = {s.split(":", 1)[1] for s in v.section_ids if s.startswith("lake:")}
        bad += [(k, b.id) for b in v.boundaries if b.kind == "lake" and b.wbk in polys]
        for s in v.section_ids:
            if s in own:
                twice.append((s, own[s], k))
            own[s] = k
    assert bad == [] and twice == []


@pytest.mark.slow
def test_a_split_lake_parent_owns_nothing(db, registry, handles, doc, tiles):
    """Kootenay, Williston, Shannon: no section in the registry or the bundle, listed in the export
    with `divided_into` and its parts' entries, not drawn as a whole in the tiles."""
    parents = {v.part_of for v in registry.values() if v.part_of}
    assert parents == {"wbk:328974235", "wbk:328961697", "wbk:329459193"}
    for p in parents:
        assert registry[p].section_ids == ()
        assert db.execute("SELECT COUNT(*) FROM item i JOIN item_section s ON s.ord = i.ord "
                          "WHERE i.item_id = ?", (p,)).fetchone()[0] == 0
        w = doc["waters"][p]
        assert w["sections"] == 0 and w["parts"] == [] and len(w["divided_into"]) >= 2
        assert set(w["entries"]) >= {e for c in w["divided_into"] for e in doc["waters"][c]["entries"]}
        ghost = handles[f"lake:{p.split(':', 1)[1]}"]
        assert ghost not in tiles["lake"], p
    assert "Kootenay Lake — Main Body" in {doc["waters"][c]["name"] for c in doc["waters"]["wbk:328974235"]["divided_into"]}


@pytest.mark.slow
def test_a_river_polygon_is_inside_its_reach(db, handles):
    """The Stellako's wide reach carries the row's rules its line carries; East Gribbell's six
    polygons, Nicomen Slough's four unnamed ones, the Rancheria's 42 likewise (the walk over
    `rancheria_river_s_tributaries` collects them). MUTATION: `_by_measure` ignoring the window."""
    def rules_on(sid: int) -> set[str]:
        return {f"{e}::{r}" for e, r in db.execute(
            "SELECT r.entry_id, r.rule_id FROM section_ruleset s JOIN ruleset r ON r.set_id = s.set_id "
            "WHERE s.sid = ?", (sid,))}
    stellako = {r for r in rules_on(handles["lake:329148363"]) if r.startswith("r6:stellako_river")}
    assert stellako and stellako == {r for r in rules_on(handles["356362768:2125"])
                                     if r.startswith("r6:stellako_river")}
    for s in ("lake:328970802", "lake:328970811", "lake:329177996", "lake:329178131"):
        assert any(r.startswith("r2:nicomen_slough") for r in rules_on(handles[s])), s
    for s in ("lake:329222284", "lake:329222359", "lake:329222367", "lake:329222376"):
        assert any(r.startswith("r6:east_gribbell_creek") for r in rules_on(handles[s])), s
    assert any(r.startswith("r6:rancheria_river") for r in rules_on(handles["lake:328979188"]))


@pytest.mark.slow
def test_run_through_own_polygon_is_one_run(doc):
    """The Stellako's part holding its polygon is one run across it, with no `lake_*` end naming
    the river itself (a lake still splits runs: `test_part_runs.py`)."""
    w = doc["waters"]["gnis:7836"]
    assert w["kind"] == "stream" and "drawn_as" not in w
    ends = [r[k] for p in w["parts"] for r in p["runs"] for k in ("from", "to")]
    assert not any(e.endswith(":gnis:7836") for e in ends), ends
    assert not any(e == "polygon" for e in ends), "the polygon lies inside a run, not at an end"
    assert max(r["km_from"] for p in w["parts"] for r in p["runs"]) > 7
    # the polygon (km 2.62-3.00 of the stem) lies INSIDE one run — the run is not split by it
    across = [r for p in w["parts"] for r in p["runs"]
              if r["km_from"] is not None and r["km_from"] >= 3.0 and r["km_to"] <= 2.6]
    assert len(across) == 1 and across[0]["km_from"] == 7.95 and across[0]["km_to"] == 2.12
    # a polygon-only stream: one polygon run
    assert doc["waters"]["wbk:329103932"]["parts"][0]["runs"] == [
        {"from": None, "to": None, "km_from": None, "km_to": None, "polygon": "whole"}]


@pytest.mark.slow
def test_absorbed_id_resolves_once(db, doc):
    """No bundle row names an absorbed id; `item_alias` holds every one; the export's `absorbed`
    lists them on the water; the folded polygon's rows reach the river's `entries`."""
    aliases = dict(db.execute("SELECT alias, item_id FROM item_alias"))
    assert len(aliases) == 116
    assert aliases["wbk:329148363"] == "gnis:7836" and aliases["wbk:329707189"] == "gnis:3062"
    for (matched,) in db.execute("SELECT matched FROM entry"):
        assert not set(json.loads(matched)) & set(aliases)
    assert set(doc["waters"]["gnis:3062"]["absorbed"]) == {"wbk:329707189"}
    assert all(w.get("absorbed") is None or a in aliases
               for w in doc["waters"].values() for a in w.get("absorbed") or [])
    assert any(e.startswith("r2:vedder_river") for e in doc["waters"]["gnis:3062"]["entries"])
    assert doc["about"]["unresolved_references"] == []


@pytest.mark.slow
def test_tile_stream_polygon_draws_with_its_river(db, handles, graph, tiles):
    """A stream's polygon route is in the `stream` layer no later than the stem pieces beside it
    (FREV/sloughs F4: 65 of 141 used to appear 1-7 zooms later)."""
    owner = _owner(db)
    node_of = {s: n for n, s in handles.items()}
    late = []
    for sid, (item, kind) in owner.items():
        nid = node_of[sid]
        if kind != "stream" or not nid.startswith("lake:") or graph.nodes[nid].kind.value != "lake":
            continue                                   # a wetland node has no route to draw
        assert sid in tiles["stream"], (item, nid)
        z = tiles["stream"][sid][1]
        nbrs = [graph.edges[e].to_node for e in graph.down_adj.get(nid, ())] + \
               [graph.edges[e].from_node for e in graph.up_adj.get(nid, ())]
        zs = [tiles["stream"][handles[n]][1] for n in nbrs
              if n in handles and handles[n] in tiles["stream"] and owner.get(handles[n], ("",))[0] == item]
        if zs and z > min(zs):
            late.append((item, nid, z, min(zs)))
    assert late == [], late[:10]


@pytest.mark.slow
def test_placement_moves_only_on_folded_sections(db):
    """Against the shipped bundle of the SAME atlas (handle digest): rule and licensing sets differ
    only on the sections the merge moved (the folded polygons, the unnamed polygons a slough took,
    the connector pieces) and on sections whose set is a renumbering of the same members.

    It measures THE MERGE, so it compares only a shipped bundle from BEFORE the one water kind
    with one after it (the Phase 1 -> Phase 2 pair). Once the shipped bundle holds the merge
    there is no merge between the two, and any difference is a later phase's rule change (Phase
    3 moved 1,183 sections through 6 rules, Kennedy Lake one of them) — never widen `allowed`
    for that; a later phase diffs its own rules against the shipped bundle."""
    live = GENERATED.bundle / "bundle.sqlite"
    if not live.exists() or live.resolve() == _bundle().resolve():
        pytest.skip("no second bundle to compare")
    old = sqlite3.connect(f"file:{live}?mode=ro", uri=True)
    if "shape" in {r[1] for r in old.execute("PRAGMA table_info(section_span)")}:
        old.close()
        pytest.skip("the shipped bundle already holds the one water kind — no merge to measure")
    meta_old = dict(old.execute("SELECT k, v FROM meta"))
    meta_new = dict(db.execute("SELECT k, v FROM meta"))
    if meta_old["section_handles"] != meta_new["section_handles"]:
        pytest.skip("the shipped bundle is another atlas")

    def members(con):
        m: dict[int, frozenset] = defaultdict(frozenset)
        by_set = defaultdict(set)
        for s, e, r in con.execute("SELECT set_id, entry_id, rule_id FROM ruleset"):
            by_set[s].add((e, r))
        return {sid: frozenset(by_set[k]) for sid, k in con.execute("SELECT sid, set_id FROM section_ruleset")}
    a, b = members(old), members(db)
    moved = {s for s in set(a) | set(b) if a.get(s) != b.get(s)}
    allowed = {s for (s,) in db.execute("SELECT s.sid FROM item i JOIN item_section s ON s.ord = i.ord "
                                        "WHERE i.kind = 'stream'")}
    # every moved section is a stream's (its polygon or a connector), never a lake's
    assert moved <= allowed, sorted(moved - allowed)[:20]
    assert moved, "the merge moved placements (the Stellako's polygon takes its row)"
