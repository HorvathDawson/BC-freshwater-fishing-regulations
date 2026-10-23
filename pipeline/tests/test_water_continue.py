"""THE CONTINUATION LINK (owner 2026-10-21: "it should be able to navigate up if one stream comes directly from another,
like vedder from chilliwack"): `water_continue` in the release's water file — water A CONTINUES AS water B when A's
mouth is B's top on the same watercourse, and nothing else comes in there. Tributaries never do.

No data: the rule on synthetic topology, in the ATLAS GRAPH's terms (every inflow, with the measure on B's line where it
joins). The bundle writes it (`links` while the graph is loaded, `write` once the parts exist) and the release
copies it; the real Chilliwack / Vedder is checked on the bundle below and, with the Cowichan, the Somass and the
Thompson, on the release in `test_release.py` (`needs_bundle`).

REVIEW 2026-10-21 (why the graph): the first cut read the bundle's `section_down` for "the only inflow at B's top". That
table holds GAUGE-SHED members only, so it hides most inflows: it shipped 92 rows of which 89 were tributaries or
confluences (Oliver Creek "continued as" the Cowichan, the lake outlet missing; the Sproat as the Somass, the Stamp
missing; the South Thompson as the Thompson, the North Thompson missing) and it missed every continuation outside a
shed.
"""
from __future__ import annotations

import pytest

from pipeline.deliver.bundle import continuation as C


def _line(**ids):
    """{sid: (blue line, measure)} from `sid=(blk, m)` keywords: `s1=("X", 0)`."""
    return {int(k[1:]): v for k, v in ids.items()}


#: A Chilliwack / Vedder shape. Water 1 (main line X, a braid piece Z) starts at measure 0 where it flows into water 2's
#: main line Y at its top piece s12 (Y 12723 .. 12950): it joins 25 m below the piece's top (the atlas's snapping), and
#: nothing else joins s12. Water 3 (line T) is a tributary joining s11 mid-piece. Water 2 also owns a side piece W.
LINE = _line(s1=("X", 0), s2=("X", 4000), s3=("X", 9000), s4=("Z", 0),
             s10=("Y", 0), s11=("Y", 4738), s12=("Y", 12723), s13=("W", 0),
             s20=("T", 0), s21=("T", 800))
SECS = {1: {1, 2, 3, 4}, 2: {10, 11, 12, 13}, 3: {20, 21}}
TOP = {12: 12950.0, 11: 12723.0, 10: 4738.0, 13: 300.0}
#: (from, to, at — the measure on `to`'s line where `from` joins)
EDGES = [(1, 12, 12925.0), (2, 1, 0.0), (3, 2, 0.0), (4, 3, 0.0), (12, 11, 12723.0), (11, 10, 4738.0),
         (13, 10, 2000.0), (20, 11, 8000.0), (21, 20, 0.0)]


def _cont(*, secs=SECS, edges=EDGES, line=LINE, top=TOP, streams=(1, 2, 3), item_down=((1, 2), (3, 2))):
    owners = {s: {o for o, ss in secs.items() if s in ss} for ss in secs.values() for s in ss}
    return C.continuations(set(streams), secs, list(item_down), line.get, C.Flow(edges, top, owners))


def test_a_stream_whose_mouth_is_the_top_of_anothers_line_continues_as_it():
    assert _cont() == [(1, 1, 2, 12)]


def test_a_tributary_joining_mid_river_does_not():
    # water 3 joins Y's piece s11 at 8000, far below that piece's top — and s11 is not water 2's top anyway
    assert [r for r in _cont() if r[0] == 3] == []
    # nor when it joins the TOP piece, but well below its top: water 2's own line goes on above it (a stub > 100 m)
    assert _cont(edges=[e for e in EDGES if e[0] != 1] + [(1, 12, 12500.0)]) == []


def test_a_confluence_is_no_continuation_whichever_river_is_bigger():
    # a second river joins the top piece 64 m below the first (the Stamp beside the Sproat, forming the Somass)
    assert _cont(edges=EDGES + [(30, 12, 12861.0)]) == []
    # 145 m apart is still one confluence (the North Thompson beside the South Thompson, forming the Thompson)
    assert _cont(edges=EDGES + [(30, 12, 12780.0)]) == []
    # a lake's outlet at the top, the creek just below it (Cowichan Lake, then Oliver Creek): the lake is the source
    assert _cont(edges=EDGES + [(31, 12, 12950.0)]) == []
    # a creek joining well down the top piece is just a tributary of B, and changes nothing
    assert _cont(edges=EDGES + [(30, 12, 12500.0)]) == [(1, 1, 2, 12)]


def test_a_water_whose_own_name_comes_in_lower_down_goes_on_as_itself():
    # the upper Cottonwood joins the Cottonwood's top piece 448 m below the Swift: the Cottonwood goes on as itself
    secs = {**SECS, 2: SECS[2] | {40}}
    assert _cont(secs=secs, edges=EDGES + [(40, 12, 12477.0)]) == []


def test_the_same_blue_line_is_the_same_watercourse():
    # A's mouth piece lies on B's own line: the FWA main stem, whatever else joins there
    line = {**LINE, 1: ("Y", 12950)}
    assert _cont(line=line, secs={**SECS, 1: {1}}, edges=EDGES + [(30, 12, 12940.0)]) == [(1, 1, 2, 12)]


def test_only_the_main_lines_ends_count():
    # A's braid piece (Z) flows into B's top, not A's main line: no continuation through a braid
    assert _cont(edges=[(1, 2, 0.0) if e[0] == 1 else e for e in EDGES] + [(4, 12, 12925.0)]) == []
    # A's main line flows into B's side piece (W), not B's main line
    assert _cont(edges=[(1, 13, 300.0) if e[0] == 1 else e for e in EDGES]) == []
    # A's main line does not START there (its lowest section on X is s1 at 0; s2 is above it)
    assert _cont(edges=[(1, 2, 0.0) if e[0] == 1 else e for e in EDGES] + [(2, 12, 12925.0)]) == []


def test_a_lake_never_continues_or_is_continued():
    assert _cont(streams=(2, 3)) == []
    assert _cont(streams=(1, 3)) == []


def test_only_waters_the_graph_already_says_flow_into_each_other():
    assert _cont(item_down=((3, 2),)) == []


def test_a_section_off_any_blue_line_is_no_end():
    assert C.line_of("380887781:0") == ("380887781", 0)
    assert C.line_of("lake:329707189") is None
    assert C.line_of("-2000000226:0") == ("-2000000226", 0)


def test_a_water_continued_twice_or_continuing_twice_is_refused():
    def refused(**kw):
        try:
            C.continuations(**kw)
        except SystemExit as e:
            return str(e)
        raise AssertionError("must be refused")
    # two waters whose mouths both sit on B's own line at its top: the data is wrong, and nothing is guessed
    secs = {**SECS, 1: {1}, 4: {30}}
    line = {**LINE, 1: ("Y", 13000), 30: ("Y", 13000)}
    owners = {s: {o for o, ss in secs.items() if s in ss} for ss in secs.values() for s in ss}
    flow = C.Flow(EDGES + [(30, 12, 12950.0)], TOP, owners)
    assert "continued by two waters" in refused(streams={1, 2, 3, 4}, secs=secs, item_down=[(1, 2), (4, 2)],
                                                line=line.get, flow=flow)
    # one mouth that is the top of two waters (a distributary): which one is not ours to guess
    secs = {**SECS, 5: {50}}
    line = {**LINE, 50: ("V", 100)}
    owners = {s: {o for o, ss in secs.items() if s in ss} for ss in secs.values() for s in ss}
    flow = C.Flow(EDGES + [(1, 50, 190.0)], {**TOP, 50: 200.0}, owners)
    assert "continues as two waters" in refused(streams={1, 2, 3, 5}, secs=secs, item_down=[(1, 2), (1, 5)],
                                                line=line.get, flow=flow)


# ---- REVERSED LINKS (owner 2026-10-21 late: "the issue seems to be eagle trib 5-3 turns into 5? we could just trim out
# those bad ones?"). The added-streams data says Eagle Trib.5 (wsc 100-019698-794100-757900) flows into Eagle Trib.5-3
# (…-757900-384000), whose code says it is a TRIBUTARY of Trib.5: the flow is reversed, and the link would send ▲ down.

def _coded(code_a, code_b, *, line=LINE, secs=SECS):
    """`continuations` on the Chilliwack / Vedder shape with FWA watershed codes on A's mouth piece (s1) and B's top
    piece (s12)."""
    owners = {s: {o for o, ss in secs.items() if s in ss} for ss in secs.values() for s in ss}
    flow = C.Flow(EDGES, TOP, owners, wsc={1: code_a, 12: code_b})
    return C.continuations({1, 2, 3}, secs, [(1, 2), (3, 2)], line.get, flow)


def test_a_link_into_a_water_the_codes_call_a_tributary_of_it_is_refused():
    # Eagle Trib.5 -> Eagle Trib.5-3: B's code extends A's, so B drains into A — the data's flow is backwards
    assert _coded("100-019698-794100-757900", "100-019698-794100-757900-384000") == []
    # padded codes are the same codes (trim_wsc): the FWA's -000000 padding changes nothing
    assert _coded("100-019698-794100-757900-000000-000000", "100-019698-794100-757900-384000-000000") == []
    # deeper down the tree is still B draining into A
    assert _coded("100-019698", "100-019698-794100-757900-384000") == []


def test_a_link_the_codes_agree_with_or_do_not_order_is_kept():
    # A is B's tributary by code (Aliceville Trib.1-1 -> Trib.1; Rochester Creek's 999999 at the top of the Holmes)
    assert _coded("900-059194-358500-866100", "900-059194-358500") == [(1, 1, 2, 12)]
    assert _coded("100-019698-429300-999999", "100-019698-429300") == [(1, 1, 2, 12)]
    # one code (one watercourse), sibling codes, a code that only SHARES leading digits with the other
    assert _coded("100-064535", "100-064535") == [(1, 1, 2, 12)]
    assert _coded("100-019698-794100-757900-138900", "100-019698-794100-757900-384000") == [(1, 1, 2, 12)]
    assert _coded("100-01969", "100-019698-794100") == [(1, 1, 2, 12)]
    # no code on either end (a lake handle, an unnamed inflow) is no evidence either way
    assert _coded("", "100-019698-794100-757900-384000") == [(1, 1, 2, 12)]
    assert _coded("100-019698-794100-757900", "") == [(1, 1, 2, 12)]


def test_a_reversed_link_is_refused_on_the_same_blue_line_too():
    assert _coded("100-1", "100-1-2", line={**LINE, 1: ("Y", 12950)}, secs={**SECS, 1: {1}}) == []
    assert _coded("100-1-2", "100-1", line={**LINE, 1: ("Y", 12950)}, secs={**SECS, 1: {1}}) == [(1, 1, 2, 12)]


def test_the_graph_flow_carries_each_pieces_code():
    class N:
        def __init__(self, up_m, wsc):
            self.up_m, self.wsc = up_m, wsc

    class E:
        def __init__(self, f, t, at):
            self.from_node, self.to_node, self.at_measure = f, t, at

    class G:
        nodes = {"Y:12723": N(12950.0, "100-1"), "X:0": N(4000.0, "100-1-2")}
        edges = [E("X:0", "Y:12723", 12925.0)]
        up_adj = {"Y:12723": [0]}

    flow = C.graph_flow(G(), ["X:0", "Y:12723"], {}, [2])
    assert (flow.into(2), flow.top, flow.wsc) == ([(1, 12925.0)], {2: 12950.0}, {2: "100-1", 1: "100-1-2"})


def test_the_graph_flow_keeps_lakes_apart_and_marks_a_creek_that_ends_in_one():
    """T108: a LAKE draining into B's top is not a river meeting A (`Flow.lakes`), and a piece that also flows into a
    lake ends there (`Flow.into_lake`) — the span words' old "it ends at the lake" now lives in `goes_on`."""
    class N:
        def __init__(self, up_m, kind="stream"):
            self.up_m, self.wsc, self.kind = up_m, "", kind

    class E:
        def __init__(self, f, t, at, kind="confluence"):
            self.from_node, self.to_node, self.at_measure, self.kind = f, t, at, kind

    class G:
        nodes = {"Y:0": N(500.0), "X:0": N(40.0), "lake:9": N(0.0, "lake")}
        edges = [E("X:0", "Y:0", 480.0, "connector"), E("lake:9", "Y:0", 500.0, "lake_out"),
                 E("X:0", "lake:9", 0.0, "lake_in")]
        up_adj = {"Y:0": [0, 1], "lake:9": [2]}
        down_adj = {"X:0": [0, 2], "lake:9": [1]}

    flow = C.graph_flow(G(), ["X:0", "Y:0", "lake:9"], {}, [2])
    assert (flow.into(2), dict(flow.lakes), flow.into_lake) == ([(1, 480.0)], {2: [500.0]}, {1})
    assert C.goes_on(1, 1, 2, 2, False, flow) is C.NO
    lakes, flow.lakes = flow.lakes, {}
    assert C.goes_on(1, 1, 2, 2, False, flow) is C.NO                       # it ends at the lake
    flow.lakes, flow.into_lake = lakes, set()
    assert C.goes_on(1, 1, 2, 2, False, flow) is C.NO                       # B begins at the lake's outlet
    flow.lakes = {}
    assert C.goes_on(1, 1, 2, 2, False, flow) == C.YES                      # with neither, it goes on


def test_no_water_goes_on_as_another_of_its_own_name():
    owners = {s: {o for o, ss in SECS.items() if s in ss} for ss in SECS.values() for s in ss}
    flow = C.Flow(EDGES, TOP, owners, names={1: "Jeperson Side Channel", 2: "Jeperson Side  Channel"})
    assert C.continuations({1, 2, 3}, SECS, [(1, 2), (3, 2)], LINE.get, flow) == []
    flow = C.Flow(EDGES, TOP, owners, names={1: "Jeperson Side Channel", 2: "Jeperson Creek"})
    assert C.continuations({1, 2, 3}, SECS, [(1, 2), (3, 2)], LINE.get, flow) == [(1, 1, 2, 12)]


def test_two_rivers_meeting_just_below_the_top_is_a_confluence_past_the_stub():
    """T108, the one constant set: A 154 m below B's top (past `STUB_M`) with another river 65 m below it is the
    Inklin and the Nakina making the Taku — a confluence, which only the book can make a link of; 250 m below it is a
    tributary whatever the book says."""
    owners = {s: {o for o, ss in SECS.items() if s in ss} for ss in SECS.values() for s in ss}
    edges = [e for e in EDGES if e[0] != 1] + [(1, 12, 12950.0 - 154.0), (30, 12, 12950.0 - 65.0)]
    flow = C.Flow(edges, TOP, owners)
    assert C.goes_on(1, 1, 2, 12, False, flow) == C.CONFLUENCE
    assert C.continuations({1, 2, 3}, SECS, [(1, 2), (3, 2)], LINE.get, flow, paired={(1, 2)}) == [(1, 1, 2, 12)]
    edges = [e for e in EDGES if e[0] != 1] + [(1, 12, 12950.0 - 250.0), (30, 12, 12950.0 - 65.0)]
    assert C.goes_on(1, 1, 2, 12, False, C.Flow(edges, TOP, owners)) is C.NO
    # without the river near the top, 154 m below is a tributary
    edges = [e for e in EDGES if e[0] != 1] + [(1, 12, 12950.0 - 154.0)]
    assert C.goes_on(1, 1, 2, 12, False, C.Flow(edges, TOP, owners)) is C.NO


def test_the_span_end_words_are_the_links():
    """`lower_ends` (T108) turns `goes_on`'s answers into the span end words, deciding nothing again: a settled link
    is `continues:`, an unsettled confluence names the comparable river meeting there (largest magnitude), a creek of
    no comparable rank names nothing (Sixtyone Creek, order 3, beside Fiftynine Creek, order 5)."""
    item = {1: "a", 2: "b", 3: "c", 4: "d"}
    found = [(1, 10, 2, 20, True), (3, 30, 2, 20, True), (4, 40, 2, 20, True)]
    rank = {10: (5, 700), 30: (6, 1600), 40: (6, 900), 20: (6, 2400)}
    assert C.lower_ends(found, [], item, rank) == {10: "confluence:c", 30: "confluence:a", 40: "confluence:a"}
    assert C.lower_ends(found, [(1, 10, 2, 20)], item, rank) == {10: "continues:b", 30: "confluence:a",
                                                                 40: "confluence:a"}
    # no comparable rank (3 beside 5 cannot make 5... nor 6): nothing named; no rank known: nothing named
    assert C.lower_ends(found[:2], [], item, {10: (5, 253), 30: (3, 14), 20: (5, 288)}) == {}
    assert C.lower_ends(found[:2], [], item, {}) == {}
    # a YES link is settled by construction: always `continues:`
    assert C.lower_ends([(1, 10, 2, 20, False)], [(1, 10, 2, 20)], item, {}) == {10: "continues:b"}


# ---- the bundle writes it (audit F07, owner 2026-10-22 call 6): the release no longer opens graph.pkl -------------

class _N:
    def __init__(self, up_m, wsc=""):
        self.up_m, self.wsc = up_m, wsc


class _E:
    def __init__(self, f, t, at):
        self.from_node, self.to_node, self.at_measure = f, t, at


class _G:
    """The atlas graph's shape `graph_flow` reads: water 1 (line X) flows into the top piece of water 2's line Y."""
    nodes = {"X:0": _N(4000.0), "Y:0": _N(12723.0), "Y:12723": _N(12950.0)}
    edges = [_E("X:0", "Y:12723", 12925.0), _E("Y:12723", "Y:0", 12723.0)]
    up_adj = {"Y:12723": [0], "Y:0": [1]}


def _bundle_tables():
    """The bundle tables `links` and `write` read, at the point of the build where each is written, and the
    `water_continue` DDL exactly as schema.sql declares it."""
    import re
    import sqlite3
    from pipeline.deliver.bundle.build import SCHEMA
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, name TEXT, kind TEXT);
        CREATE TABLE item_section (ord INTEGER NOT NULL, sid INTEGER NOT NULL);
        CREATE TABLE item_down (ord INTEGER NOT NULL, down_ord INTEGER NOT NULL);
        CREATE TABLE part_section (ord INTEGER NOT NULL, sid INTEGER NOT NULL, part_ix INTEGER NOT NULL);
        CREATE TABLE entry (entry_id TEXT PRIMARY KEY, full_name TEXT, matched TEXT NOT NULL DEFAULT '[]');
        INSERT INTO item VALUES (1, 'gnis:1', 'Chilliwack River', 'stream'), (2, 'gnis:2', 'Vedder River', 'stream');
        INSERT INTO item_section VALUES (1, 1), (2, 2), (2, 3);
        INSERT INTO item_down VALUES (1, 2);
    """)
    ddl = re.search(r"CREATE TABLE water_continue .*?;", SCHEMA.read_text(), re.S)
    assert ddl, "schema.sql declares water_continue"
    db.execute(ddl.group(0))
    return db


class _Cov:
    def __init__(self):
        self.rows = {}

    def filled(self, t, n):
        self.rows[t] = n


def test_the_bundle_links_while_the_graph_is_loaded_and_writes_parts_once_they_exist():
    """Two halves, because the build drops the graph before the parts exist: `links` reads the graph (section ids),
    `write` turns them into parts after `derived.write`."""
    db = _bundle_tables()
    ids = ["X:0", "Y:0", "Y:12723"]                          # handles 1, 2, 3
    links = C.links(db, ids, _G())
    assert links == [(1, 1, 2, 3, False)]
    db.executescript("INSERT INTO part_section VALUES (1, 1, 1), (2, 2, 1), (2, 3, 0);")
    cov = _Cov()
    C.write(db, links, cov)
    assert db.execute("SELECT ord, part, next_ord, next_part FROM water_continue").fetchall() == [(1, 1, 2, 0)]
    assert cov.rows == {"water_continue": 1}


class _G2(_G):
    """`_G` with a second river (line Q, water 3) beginning 64 m from water 1's mouth: a confluence."""
    nodes = {**_G.nodes, "Q:0": _N(3000.0)}
    edges = _G.edges + [_E("Q:0", "Y:12723", 12861.0)]
    up_adj = {"Y:12723": [0, 2], "Y:0": [1]}


def _confluence_bundle(heading, matched):
    db = _bundle_tables()
    db.executescript("""
        INSERT INTO item VALUES (3, 'gnis:3', 'Talchako River', 'stream');
        INSERT INTO item_section VALUES (3, 4);
        INSERT INTO item_down VALUES (3, 2);
        INSERT INTO part_section VALUES (1, 1, 1), (2, 2, 1), (2, 3, 0), (3, 4, 0);
    """)
    import json
    db.execute("INSERT INTO entry VALUES ('r5:x@5-1', ?, ?)", (heading, json.dumps(matched)))
    return db


def test_the_bundle_keeps_a_confluence_link_only_where_an_entry_heading_pairs_the_two():
    """`links` (graph loaded) marks a link made at a confluence; `write` (entries written by then) keeps it only when
    one entry's heading names A and B together and matched both."""
    ids = ["X:0", "Y:0", "Y:12723", "Q:0"]                   # handles 1..4
    db = _confluence_bundle("CHILLIWACK/VEDDER RIVERS", ["gnis:1", "gnis:2"])
    links = C.links(db, ids, _G2())
    assert links == [(1, 1, 2, 3, True), (3, 4, 2, 3, True)]
    C.write(db, links, _Cov())
    assert db.execute("SELECT ord, part, next_ord, next_part FROM water_continue").fetchall() == [(1, 1, 2, 0)]
    # the heading names them, but the entry did not match the Chilliwack: no pairing
    db = _confluence_bundle("CHILLIWACK/VEDDER RIVERS", ["gnis:2"])
    C.write(db, C.links(db, ids, _G2()), _Cov())
    assert db.execute("SELECT COUNT(*) FROM water_continue").fetchone() == (0,)
    # a lone heading names no pair
    db = _confluence_bundle("VEDDER RIVER", ["gnis:1", "gnis:2"])
    C.write(db, C.links(db, ids, _G2()), _Cov())
    assert db.execute("SELECT COUNT(*) FROM water_continue").fetchone() == (0,)


def test_a_link_whose_section_has_no_part_stops_the_build():
    db = _bundle_tables()
    db.execute("INSERT INTO part_section VALUES (1, 1, 1)")
    with pytest.raises(C.ContinuationError, match="has no part"):
        C.write(db, [(1, 1, 2, 3, False)], _Cov())


@pytest.mark.needs_bundle
def test_the_chilliwack_continues_as_the_vedder_in_the_bundle(request):
    """On the real bundle: the link the owner asked for (the Chilliwack's mouth part into the Vedder's part 0), and the
    18 reviewed links (T59: the Atnarko as the Bella Coola) — the bundle is where `water_continue` is made now; the
    release copies it (`test_release.py`)."""
    import os
    import sqlite3
    from pathlib import Path
    from pipeline.deliver.bundle import read as R
    from pipeline.tests.conftest import BUNDLE_HINT, need
    bundle = need(request, "bundle", Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE), BUNDLE_HINT)
    b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    if not b.execute("SELECT 1 FROM sqlite_master WHERE name = 'water_continue'").fetchone():
        pytest.fail(f"{bundle} predates water_continue in the bundle — rebuild it: python -m pipeline.deliver bundle")
    ordof = dict(b.execute("SELECT item_id, ord FROM item"))
    named = lambda n: {o for (o,) in b.execute("SELECT ord FROM item WHERE name = ?", (n,))}  # noqa: E731
    rows = b.execute("SELECT ord, part, next_ord, next_part FROM water_continue ORDER BY ord").fetchall()
    chw, ved = ordof["gnis:8634"], ordof["gnis:3062"]
    assert [r for r in rows if ved in (r[0], r[2]) or chw in (r[0], r[2])] == [(chw, 1, ved, 0)]
    preds = {n for (n,) in b.execute("SELECT i.name FROM item_down d JOIN item i ON i.ord = d.ord WHERE d.down_ord = ?",
                                     (ved,))}
    assert preds == {"Chilliwack River", "Stewart Slough", "Street Creek", "McGillivray Slough", "Lewis Slough"}
    # REVIEW 2026-10-21: no tributary, confluence or lake outlet is linked — the first cut (read from the gauge-shed-only
    # `section_down`) linked Oliver Creek to the Cowichan, the Sproat to the Somass, the South Thompson to the Thompson,
    # Chase Creek to the South Thompson, the Puntledge to the Courtenay: 89 of its 92 rows
    linked = {r[0] for r in rows} | {r[2] for r in rows}
    for trib in ("Slesse Creek", "North Thompson River", "Stewart Slough", "Lewis Slough", "Street Creek",
                 "Oliver Creek", "Sproat River", "Stamp River", "South Thompson River", "Chase Creek", "Puntledge River",
                 "Tsolum River", "Fairmont Creek", "Poquette Creek", "Swift River", "Talchako River"):
        assert named(trib) and not named(trib) & linked, trib
    # both ends keyed; every part named is real
    assert len({r[0] for r in rows}) == len({r[2] for r in rows}) == len(rows) > 0
    parts = set(b.execute("SELECT ord, part_ix FROM part"))
    assert all((a, p) in parts and (n, q) in parts for a, p, n, q in rows)
    # OWNER 2026-10-21 (late): the 18 rows reviewed in handoff/PLAN/app-data/p15/continuations.md, less the one the FWA
    # codes call reversed (Eagle Trib.5 "flows into" Eagle Trib.5-3); the 10 municipal continuations stay
    name = dict(b.execute("SELECT ord, name FROM item"))
    assert sorted({(name[a], name[n]) for a, _, n, _ in rows}) == sorted({
        ("Chilliwack River", "Vedder River"), ("Hatzis Slough", "Lower Hatzic Slough"),
        ("Chilqua Creek", "Chilqua Slough"), ("Page Creek", "Matsqui Slough"), ("Marblehill Creek", "Semmihault Creek"),
        ("Davidson Creek", "Tomat Creek"), ("Pass Creek", "Screw Creek"),
        # T108: ("Jeperson Side Channel", "Jeperson Side Channel") is gone — no water continues into itself
        ("Aliceville Trib.1-1", "Aliceville Trib.1"), ("Beecher Trib.4", "Beecher Creek"),
        ("Crayfish Creek Trib.3-1", "Crayfish Creek Trib.3"), ("Eagle Trib.4-1", "Eagle Trib.4"), ("Jr 4-1-2", "Jr 4-1"),
        ("Noble Creek Trib.1 Trib.1", "Noble Creek Trib.1"), ("Rochester Creek", "Holmes Creek"),
        ("Westway Avenue Drainage Ditch", "Magnolia Creek Trib 1"),
        ("Finch Connector (Unnamed)", "Logger's Lane Connector"),
        # T59 (owner 2026-10-22): two rivers meet, and the book's "ATNARKO/BELLA COOLA RIVERS" names the one that goes
        # on; the Talchako stays a tributary (above). The only row the pairing adds, reviewed on the live bundle + graph
        ("Atnarko River", "Bella Coola River")})
    assert len(rows) == 17
    atn, bc = ordof["gnis:17209"], ordof["gnis:11611"]
    assert [r for r in rows if atn in (r[0], r[2]) or bc in (r[0], r[2])] == [(atn, 3, bc, 0)]


@pytest.mark.needs_bundle
def test_every_continuation_is_a_continues_end_and_every_continues_end_a_continuation(request):
    """THE BUNDLE PIN (T108): "goes on as" is decided once (`continuation.goes_on`), so the ▲ button
    (`water_continue`) and the part's end words (`section_span` lower end `continues:<B>`) are the same set: every
    link's A part holds a section ending `continues:<B>`, every `continues:` end belongs to a linked A part, and no
    water continues into itself."""
    import os
    import sqlite3
    from pathlib import Path
    from pipeline.deliver.bundle import read as R
    from pipeline.tests.conftest import BUNDLE_HINT, need
    bundle = need(request, "bundle", Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE), BUNDLE_HINT)
    b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    item = dict(b.execute("SELECT ord, item_id FROM item"))
    links = {(a, p, item[n]) for a, p, n, _q in b.execute("SELECT ord, part, next_ord, next_part FROM water_continue")}
    ends = {(o, p, t.split(":", 1)[1]) for o, p, t in b.execute(
        "SELECT ps.ord, ps.part_ix, e.token FROM section_span s JOIN span_end e ON e.eid = s.lo "
        "JOIN part_section ps ON ps.sid = s.sid WHERE e.token LIKE 'continues:%'")}
    assert links and links == ends
    assert not [r for r in links if item[r[0]] == r[2]]
    name = dict(b.execute("SELECT ord, name FROM item"))
    nord = {i: o for o, i in item.items()}
    assert not [r for r in links if name[r[0]].lower() == name[nord[r[2]]].lower()]


def test_no_other_bundle_module_decides_continuation():
    """THE GREP PIN (T108, with T61 2.10): the continuation tolerances and the decision live in `continuation.py`
    alone — `spans.py` had its own (`_BEGINS_M` 100, `_MEET_M` 250, `lower_end_into`, `_strahler`) and disagreed on 11
    of the 17 links. Mutation: re-adding `_MEET_M = 250.0` to spans.py fails here."""
    import re
    from pathlib import Path
    here = Path(C.__file__).parent
    bad = re.compile(r"^\s*_?(?:BEGINS|MEET|STUB|CONFLUENCE|OWN)_M\s*=|^\s*def\s+_?(?:lower_end_into|goes_on|strahler)\b",
                     re.M)
    hits = sorted(f.name for f in here.rglob("*.py") if f.name != "continuation.py" and bad.search(f.read_text()))
    assert hits == []
    assert bad.search(Path(C.__file__).read_text())        # the pattern still finds the one home


# ---- TWO RIVERS MEET, AND THE BOOK NAMES ONE OF THEM WITH THE THIRD (T59; owner 2026-10-22: "think of things like this
# as one system; bella coola atnarko is another example"). The Bella Coola begins where the Atnarko and the Talchako
# meet; FWA codes both as its tributaries, and the confluence gate refuses both. The synopsis prints ONE row for
# "ATNARKO/BELLA COOLA RIVERS": that names the Atnarko as the river that goes on as the Bella Coola. The Talchako stays
# a tributary. Every other gate still holds.

#: The Atnarko / Talchako / Bella Coola shape on the Chilliwack / Vedder one: water 4 (line Q, the Talchako) begins 64 m
#: from water 1's mouth, at the top of water 2's top piece s12.
CONF_SECS = {**SECS, 4: {30, 31}}
CONF_LINE = {**LINE, 30: ("Q", 0), 31: ("Q", 5000)}
CONF_EDGES = EDGES + [(30, 12, 12861.0), (31, 30, 0.0)]


def _conf(paired=(), *, edges=CONF_EDGES, secs=CONF_SECS, item_down=((1, 2), (3, 2), (4, 2)), wsc=None):
    owners = {s: {o for o, ss in secs.items() if s in ss} for ss in secs.values() for s in ss}
    return C.continuations({1, 2, 3, 4}, secs, list(item_down), CONF_LINE.get, C.Flow(edges, TOP, owners, wsc),
                           paired=set(paired))


def test_at_a_confluence_the_river_the_book_names_with_the_receiver_goes_on_as_it():
    assert _conf() == []                                     # no pairing: two rivers make a third, as before
    assert _conf(paired={(1, 2)}) == [(1, 1, 2, 12)]         # "ATNARKO/BELLA COOLA": the Atnarko goes on
    assert _conf(paired={(4, 2)}) == [(4, 30, 2, 12)]        # the pairing, not which river is listed first, decides
    # the book pairing BOTH rivers with the receiver names no one of them: nothing is guessed
    assert _conf(paired={(1, 2), (4, 2)}) == []
    # a pairing of two waters that do not meet there is no evidence for a third
    assert _conf(paired={(1, 3)}) == []


def test_a_pairing_never_lifts_the_other_gates():
    # the stub gate: B's line goes on 450 m above A's mouth — A is a tributary, whatever the book says
    # (and with A 450 m down, the other river alone joins at B's top: it goes on, by the old rule, unpaired)
    edges = [e for e in CONF_EDGES if e[0] != 1] + [(1, 12, 12500.0)]
    assert _conf(paired={(1, 2)}, edges=edges) == [(4, 30, 2, 12)]
    # B's own piece comes in lower down its top piece (the Cottonwood): B goes on as itself
    secs = {**CONF_SECS, 2: CONF_SECS[2] | {40}}
    assert _conf(paired={(1, 2)}, secs=secs, edges=CONF_EDGES + [(40, 12, 12477.0)]) == []
    # item_down must already say A flows into B
    assert _conf(paired={(1, 2)}, item_down=((3, 2), (4, 2))) == []


def test_b_rejoined_by_its_own_side_channel_far_down_its_top_piece_needs_the_book():
    # the Bella Coola's braids rejoin its 14 km top piece 7 km below the Atnarko: no longer "B goes on as itself" (that
    # is within OWN_M, the Cottonwood), but the atlas alone cannot say A goes on either — only a pairing does
    secs = {**SECS, 2: SECS[2] | {40}}
    edges = EDGES + [(40, 12, 12925.0 - 7000.0)]
    assert _cont(secs=secs, edges=edges) == []
    owners = {s: {o for o, ss in secs.items() if s in ss} for ss in secs.values() for s in ss}
    assert C.continuations({1, 2, 3}, secs, [(1, 2), (3, 2)], LINE.get, C.Flow(edges, TOP, owners),
                           paired={(2, 1)}) == [(1, 1, 2, 12)]
    # 999 m below is still the Cottonwood, paired or not
    edges = EDGES + [(40, 12, 12925.0 - 999.0)]
    assert C.continuations({1, 2, 3}, secs, [(1, 2), (3, 2)], LINE.get, C.Flow(edges, TOP, owners),
                           paired={(1, 2)}) == []
    # the FWA codes saying B drains into A
    assert _conf(paired={(1, 2)}, wsc={1: "100-1", 12: "100-1-2"}) == []


def test_the_book_pairs_the_waters_one_heading_names_together():
    names = {"gnis:17209": "Atnarko River", "gnis:11611": "Bella Coola River", "gnis:29411": "South Atnarko River"}
    assert C.heading_pairs("ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCEPT: Burnt Bridge Creek upstream of "
                           "Sitkatapa Creek, Hunlen Creek upstream of Hunlen Falls", names) == {
        frozenset({"gnis:17209", "gnis:11611"})}
    assert C.heading_pairs("CHILLIWACK / VEDDER RIVERS (does not include Sumas River) (see map on page 24)",
                           {"gnis:8634": "Chilliwack River", "gnis:3062": "Vedder River"}) == {
        frozenset({"gnis:8634", "gnis:3062"})}
    # one name, or a slash that names one matched water and a place (a confluence, not two waters)
    assert C.heading_pairs("VEDDER RIVER", {"gnis:3062": "Vedder River"}) == set()
    assert C.heading_pairs("SKEENA RIVER/KISPIOX RIVER CONFLUENCE", {"gnis:2936": "Skeena River"}) == set()
    # a matched water the heading does not name exactly is not paired ("South Atnarko" is not "Atnarko")
    assert C.heading_pairs("ATNARKO/BELLA COOLA RIVERS", {"gnis:29411": "South Atnarko River",
                                                          "gnis:11611": "Bella Coola River"}) == set()
