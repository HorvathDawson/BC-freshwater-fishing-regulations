"""T76 — a water's own gauge across a CONTINUATION (audit N19, owner call 10; T81 Q8: `water_continue` only).

The Vedder (sid 313838) had nothing leading: its own gauge, 08MH047, is retired, and the Chilliwack's gauges belong to
another water item — though `water_continue` says the Chilliwack goes on as the Vedder. The owner: a river that changes
its name is ONE system (87ba267c). So a gauge on a `water_continue` predecessor or successor counts as the section's
OWN gauge — same quantity, same far and area gates (`panel.may_lead`), the distance measured along the water across
the name change. A tributary, a side channel or the river a water flows into is other water: only `water_continue`.
"""

from __future__ import annotations

import inspect
import json
import sqlite3
import textwrap

import pytest

import pipeline.atlas.gauges.build_panels as bp
from pipeline.atlas.gauges.build_panels import build, own_distances
from pipeline.deliver.bundle import continuation as C
from pipeline.tests.test_build_panels import MODEL, E, N, g

CHW, VED, FRA = "gnis:chilliwack", "gnis:vedder", "gnis:fraser"


def _system(km=1.0):
    """The Chilliwack c0 -> c1 flows into the Vedder v0 -> v1, which flows into the Fraser f0 (no continuation).
    Sections `km` long. 08MH001 stands at the foot of c1 (Vedder Crossing)."""
    L = km * 1000.0
    nodes = [N("c0", 600, length_m=L), N("c1", 620, length_m=L),
             N("v0", 640, length_m=L), N("v1", 660, length_m=L), N("f0", 700, length_m=L)]
    graph = g(nodes, [E("c0", "c1"), E("c1", "v0"), E("v0", "v1"), E("v1", "f0")])
    water = {"c0": CHW, "c1": CHW, "v0": VED, "v1": VED, "f0": FRA}
    return graph, water


def _leads(panels, sec):
    return {m.station: m.lead for m in panels.members[panels.by_section[sec][0]]}


def _all_leads(panels):
    return {sec: _leads(panels, sec) for sec in sorted(panels.by_section)}


def test_the_vedder_is_led_by_the_chilliwacks_gauge():
    """The acceptance: across the continuation the Chilliwack's 08MH001 leads the Vedder; the Fraser below, which the
    Vedder only flows into, is other water."""
    graph, water = _system()
    panels = build(graph, MODEL, [("08MH001", "c1", 40, False)], water_of=water, continues=[(CHW, VED)])
    assert _all_leads(panels) == {"c0": {"08MH001": 1}, "c1": {"08MH001": 1},
                                  "v0": {"08MH001": 1}, "v1": {"08MH001": 1}, "f0": {"08MH001": 0}}


def test_without_the_continuation_the_vedder_is_other_water():
    """The same graph with no `water_continue` row: as before T76, nothing leads the Vedder."""
    graph, water = _system()
    panels = build(graph, MODEL, [("08MH001", "c1", 40, False)], water_of=water)
    assert _all_leads(panels) == {"c0": {"08MH001": 1}, "c1": {"08MH001": 1},
                                  "v0": {"08MH001": 0}, "v1": {"08MH001": 0}, "f0": {"08MH001": 0}}


def test_the_successors_gauge_leads_the_predecessor_too():
    """One system both ways: a gauge on the Vedder leads the Chilliwack above it."""
    graph, water = _system()
    panels = build(graph, MODEL, [("VEDDER_G", "v0", 40, False)], water_of=water, continues=[(CHW, VED)])
    assert _all_leads(panels) == {"c0": {"VEDDER_G": 1}, "c1": {"VEDDER_G": 1},
                                  "v0": {"VEDDER_G": 1}, "v1": {"VEDDER_G": 1}, "f0": {"VEDDER_G": 0}}


def test_the_distance_runs_along_the_water_across_the_name_change():
    """The far gate is the same gate, measured through the link: 08MH001 at the foot of c1; v0 begins at it (0 km),
    v1 one 5 km section below; c0 one section above."""
    graph, water = _system(km=5.0)
    got = own_distances(graph, water, [("08MH001", "c1", 40, False)], continues=[(CHW, VED)])
    assert {sec: dict(d) for sec, d in sorted(got.items())} == {
        "c0": {"08MH001": 5000.0}, "c1": {"08MH001": 0.0}, "v0": {"08MH001": 0.0}, "v1": {"08MH001": 5000.0}}


def test_the_far_gate_still_holds_across_the_continuation():
    """Twelve 5 km sections, the name changing after the sixth; the ~100 km² river's limit is the 10 km floor. The
    gauge at the foot of c0 leads c0..c3 (0, 0, 5, 10 km) and not past them, whichever water the section is on."""
    nodes = [N(f"c{i}", 100 + i, length_m=5_000.0) for i in range(6)] + [
        N(f"v{i}", 106 + i, length_m=5_000.0) for i in range(6)]
    order = [f"c{i}" for i in range(6)] + [f"v{i}" for i in range(6)]
    graph = g(nodes, [E(a, b) for a, b in zip(order, order[1:])])
    water = {s: (CHW if s[0] == "c" else VED) for s in order}
    panels = build(graph, MODEL, [("OWN", "c0", 40, False)], water_of=water, continues=[(CHW, VED)])
    assert [_leads(panels, s)["OWN"] for s in order] == [1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0]
    # and the gate is what stops it: with the gauge at the foot of c5, v0 (0 km) and v1, v2 lead, v3 (15 km) does not
    panels = build(graph, MODEL, [("OWN", "c5", 40, False)], water_of=water, continues=[(CHW, VED)])
    assert [_leads(panels, s)["OWN"] for s in order[5:]] == [1, 1, 1, 1, 0, 0, 0]


def test_a_chain_of_continuations_is_one_system():
    """A goes on as B, B as C: a gauge on A is C's own (within the gates), as the app's ▼ walks A → B → C."""
    graph, water = _system()
    water["f0"] = "gnis:third"
    panels = build(graph, MODEL, [("G", "c1", 40, False)], water_of=water,
                   continues=[(CHW, VED), (VED, "gnis:third")])
    assert _leads(panels, "f0") == {"G": 1}


def test_a_lake_between_still_ends_the_walk():
    """A continuation never carries an own gauge through a lake (another item, and storage): c1 -> lake -> v0."""
    graph, water = _system()
    graph.nodes["lk"] = N("lk", 630, kind="lake")
    graph.edges = [E("c0", "c1"), E("c1", "lk"), E("lk", "v0"), E("v0", "v1"), E("v1", "f0")]
    water["lk"] = "gnis:lake"
    got = own_distances(graph, water, [("08MH001", "c1", 40, False)], continues=[(CHW, VED)])
    assert sorted(got) == ["c0", "c1"]


# ------------------------------------------------------------------ the mutation pin: the system, not the item

def _mutant(fn, old, new):
    src = textwrap.dedent(inspect.getsource(fn))
    assert src.count(old) == 1, f"{old!r} moved; update this pin"
    ns = dict(vars(bp))
    exec(compile(src.replace(old, new), bp.__file__, "exec"), ns)
    return ns[fn.__name__]


def test_the_system_is_what_holds_the_vedder_test():
    """Walking the gauge's own ITEM again (the rule before T76) loses the Vedder's lead."""
    mutant = _mutant(own_distances, "system_of(water_of.get(nxt)) != system", "water_of.get(nxt) != water")
    graph, water = _system()
    got = mutant(graph, water, [("08MH001", "c1", 40, False)], continues=[(CHW, VED)])
    assert sorted(got) == ["c0", "c1"]


# ------------------------------------------------------------------ the bundle: which continuations the panels get

def _db():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, name TEXT, kind TEXT);
        CREATE TABLE entry (entry_id TEXT PRIMARY KEY, full_name TEXT, matched TEXT NOT NULL DEFAULT '[]');
        INSERT INTO item VALUES (1, 'gnis:1', 'Atnarko River', 'stream'), (2, 'gnis:2', 'Bella Coola River', 'stream'),
                                (3, 'gnis:3', 'Talchako River', 'stream');
    """)
    return db


HEADS = [("r5:atn@5-1", "ATNARKO/BELLA COOLA RIVERS [Includes Tributaries]", ["gnis:1", "gnis:2"]),
         ("z5:x", "A/B RIVERS", ["gnis:1", "gnis:3"]),
         ("r5:tal@5-2", "TALCHAKO RIVER", ["gnis:3"])]


def test_the_catalogue_pairs_what_the_entry_table_pairs():
    """The panels are built before `entry` is written, so the bundle reads the pairings from the catalogue then: one
    rule (`pairs_of`) over the same rows, whichever side they are read from."""
    db = _db()
    db.executemany("INSERT INTO entry VALUES (?,?,?)", [(e, h, json.dumps(m)) for e, h, m in HEADS])
    assert C.pairs_of(db, HEADS) == C.book_pairs(db) == {(1, 2), (2, 1)}


def test_the_catalogue_heads_are_read_from_every_entry_source(tmp_path):
    (tmp_path / "synopsis").mkdir()
    (tmp_path / "synopsis" / "region-5.json").write_text(json.dumps({"entries": [
        {"entry_id": e, "name": h, "matched": m} for e, h, m in HEADS]}))
    (tmp_path / "dfo").mkdir()
    (tmp_path / "dfo" / "region-5.json").write_text(json.dumps({"waters": []}))
    assert sorted(C.catalogue_heads(tmp_path)) == sorted(HEADS)


def test_the_settled_links_name_their_items():
    db = _db()
    assert C.item_pairs(db, [(1, 11, 2, 21)]) == [("gnis:1", "gnis:2")]


def test_the_table_must_be_what_the_panels_were_built_with():
    """`write` settles again from the `entry` table; a pairing the catalogue read did not see stops the build — the
    panels would disagree with the ▼ the app shows."""
    db = _db()
    db.executescript("""
        CREATE TABLE part_section (ord INTEGER NOT NULL, sid INTEGER NOT NULL, part_ix INTEGER NOT NULL);
        CREATE TABLE water_continue (ord INTEGER, part INTEGER, next_ord INTEGER, next_part INTEGER);
        INSERT INTO part_section VALUES (1, 11, 0), (2, 21, 0);
    """)
    db.executemany("INSERT INTO entry VALUES (?,?,?)", [(e, h, json.dumps(m)) for e, h, m in HEADS])

    class Cov:
        def filled(self, *_):
            pass

    found = [(1, 11, 2, 21, True)]
    with pytest.raises(C.ContinuationError, match="panels were built"):
        C.write(db, found, Cov(), built_with=[])
    assert C.write(db, found, Cov(), built_with=[(1, 11, 2, 21)]) == 1


# ------------------------------------------------------------------ the acceptance, on the real bundle

def _bundle():
    import os
    from pathlib import Path

    from pipeline.deliver.bundle import read as R
    return Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE)


VEDDER_LEADS = {
    # the real graph, before -> after (decisions.md "CLAUDE 2026-10-22 (T76 …)"): before, 08MH047 alone on both
    313838: {"08MH047": 1, "08MH001": 2, "08MH055": 3},
    313839: {"08MH001": 1, "08MH047": 2, "08MH055": 3},
}


@pytest.mark.needs_bundle
def test_the_vedder_313838_is_led_by_08mh001_in_the_bundle():
    """THE ACCEPTANCE on shipped data, not a toy graph: the Vedder's sections carry the Chilliwack's gauges as own
    (`panel_member.lead`), and with its own 08MH047 retired (no reading: weight 0) `leader_of` — the rule trust.ts
    mirrors — hands the lead to 08MH001."""
    from pipeline.atlas.gauges.panel import leader_of
    from pipeline.tests.conftest import predates
    db = sqlite3.connect(f"file:{_bundle()}?mode=ro", uri=True)
    got = {sid: {st: lead for st, lead in db.execute(
        "SELECT m.station, m.lead FROM section_panel p JOIN panel_member m USING (panel_id) "
        "WHERE p.sid = ? AND m.lead > 0", (sid,))} for sid in VEDDER_LEADS}
    if got == {313838: {"08MH047": 1}, 313839: {"08MH047": 1}}:
        predates(f"{_bundle()} predates T76 (the Vedder takes the Chilliwack's gauges as own) — rerun pipeline.deliver")
    assert got == VEDDER_LEADS
    members = db.execute("SELECT m.station, m.lead FROM section_panel p JOIN panel_member m USING (panel_id) "
                         "WHERE p.sid = 313838 ORDER BY m.station").fetchall()
    weights = [0.0 if st == "08MH047" else 1.0 for st, _ in members]
    assert members[leader_of([lead for _, lead in members], weights)][0] == "08MH001"


def test_the_catalogue_is_read_before_the_graph_is_loaded():
    """The early catalogue read parses registry.json again (~1 GB resident). Done while graph.pkl and geometries.pkl
    are in memory it raises the bundle build's peak — the build that once took the Mac down — so it runs before
    either is unpickled, when only the `item` table it pairs against is needed."""
    import ast
    from pathlib import Path
    src = Path(C.__file__).with_name("build.py").read_text(encoding="utf-8")
    fn = next(n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.FunctionDef) and n.name == "build")
    lines = {}
    for c in ast.walk(fn):
        if isinstance(c, ast.Call):
            name = getattr(c.func, "attr", None) or getattr(c.func, "id", None)
            lines.setdefault(name, []).append(c.lineno)
    assert len(lines["catalogue_heads"]) == 1
    assert lines["catalogue_heads"][0] < min(lines["load"])  # pickle.load: graph.pkl, then geometries.pkl
