"""ONE MAIN LINE (T118, owner 2026-10-22: "unify checks that decide the same thing"). "Which sections are this water's
main line?" is answered by `pipeline.deliver.bundle.lines` alone: the FWA blue line carrying the most of the water's
LENGTH, ties to the lowest line key. Spans used length; continuation and the exit walk (`topology.mouth_downs`) used
SECTION COUNT, and on a water like the Ecstall (37 of its 42 sections off the length stem) they named different lines.

No data. Mutations each test catches (run by hand, T118):
  * `main_line` weighing every piece 1 (count, the old `continuation._main`):
    `test_the_main_line_is_the_one_carrying_the_most_length_not_the_most_sections`,
    `test_spans_continuation_and_the_exit_walk_read_one_line`.
  * `main_line` breaking a tie to the HIGHEST key (the old `_main`'s `max`): `test_a_tie_goes_to_the_lowest_line_key`.
  * `piece` counting a lake / polygon node: `test_only_a_stream_node_is_a_piece_of_a_line`.
  * `topology.mouth_downs` (or `continuation.links`) picking its own line again by count:
    `test_spans_continuation_and_the_exit_walk_read_one_line` (its exit / link half).
  * a stem picker re-added to another bundle module (`def main_stem` in spans.py, `def _main` in continuation.py):
    `test_no_other_bundle_module_picks_a_main_line`.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from types import SimpleNamespace as NS

from pipeline.deliver.bundle import continuation as C
from pipeline.deliver.bundle import lines as L
from pipeline.deliver.bundle import spans as SP
from pipeline.deliver.bundle import topology as T


def test_the_main_line_is_the_one_carrying_the_most_length_not_the_most_sections():
    # the Ecstall's shape: one long piece on its stem, many short ones on a side line
    assert L.main_line([("S", 5000.0), ("B", 100.0), ("B", 100.0), ("B", 100.0)]) == "S"
    assert L.main_line([("2", 10.0), ("1", 4.0), ("1", 4.0), ("3", 1.0)]) == "2"
    assert L.main_line([("1", 4.0), ("1", 4.0), ("2", 7.9)]) == "1"         # summed per line
    assert L.main_line([]) is None


def test_a_tie_goes_to_the_lowest_line_key():
    assert L.main_line([("9", 5.0), ("4", 5.0)]) == "4"
    assert L.main_line([("K", 100.0), ("B", 100.0), ("D", 100.0)]) == "B"
    assert L.main_line([("9", 5.0004), ("4", 5.0)]) == "4"                  # equal to the millimetre is a tie


def test_only_a_stream_node_is_a_piece_of_a_line():
    assert L.piece(NS(kind="stream", blk=356363467, length_m=12.5)) == ("356363467", 12.5)
    assert L.piece(NS(kind=NS(value="stream"), blk="X", length_m=3)) == ("X", 3.0)
    assert [L.piece(NS(kind=k, blk="X", length_m=1.0)) for k in ("lake", "wetland", "polygon")] == [None] * 3
    assert L.piece(None) is None


def test_line_ends_are_the_lowest_and_highest_measure_on_the_main_line():
    line = {1: ("S", 0), 2: ("S", 2500), 3: ("B", 0), 4: ("B", 100), 5: ("B", 200), 6: None}.get
    length = {1: 2500.0, 2: 2500.0, 3: 100.0, 4: 100.0, 5: 100.0}
    assert L.line_ends({5, 1, 3, 2, 4, 6}, line, length) == ("S", 1, 2)
    # a handle with no length is no piece (a polygon on a blue-line id): it never makes a line, nor an end
    assert L.line_ends({3, 4, 5, 7}, {**{3: ("B", 0), 4: ("B", 100), 5: ("B", 200)}, 7: ("Z", 0)}.get, length) == (
        "B", 3, 5)
    assert L.line_ends({6}, line, length) is None                          # a lake: no line
    nodes = {"S:0": NS(kind="stream", blk="S", length_m=2500.0), "lake:9": NS(kind="lake", blk="", length_m=0.0)}
    assert L.handle_lengths(nodes, ["S:0", "lake:9", "gone:0"], [1, 2, 3, 4]) == {1: 2500.0}


# ---- the three readers, on one water where length and count disagree ----------------------------------------------

def _piece(nid, blk, lo, hi):
    return NS(node_id=nid, kind="stream", blk=blk, down_m=lo, up_m=hi, length_m=hi - lo, wsc="",
              stream_order=1, stream_magnitude=1, lower_bound=None, upper_bound=None)


def _e(a, b, at, kind="confluence"):
    return NS(from_node=a, to_node=b, at_measure=float(at), kind=kind)


#: Water 1 (the Ecstall's shape): stem S, one 5 km piece whose mouth joins the TOP of water 2's line R (10 m below its
#: end); a side line B of three 100 m pieces, whose own lower end flows into water 3 (line T). By count B is the main
#: line, by length S.
NODES = {"S:0": _piece("S:0", "S", 0, 5000), "B:0": _piece("B:0", "B", 0, 100), "B:100": _piece("B:100", "B", 100, 200),
         "B:200": _piece("B:200", "B", 200, 300), "R:0": _piece("R:0", "R", 0, 1000), "T:0": _piece("T:0", "T", 0, 2000)}
EDGES = [_e("S:0", "R:0", 990), _e("B:200", "B:100", 100), _e("B:100", "B:0", 0), _e("B:0", "T:0", 1500)]
WATERS = [("eco", ["S:0", "B:0", "B:100", "B:200"]), ("rcv", ["R:0"]), ("oth", ["T:0"])]


def _graph():
    up, down = {}, {}
    for ix, e in enumerate(EDGES):
        up.setdefault(e.to_node, []).append(ix)
        down.setdefault(e.from_node, []).append(ix)
    return NS(nodes=NODES, edges=EDGES, up_adj=up, down_adj=down)


def test_spans_continuation_and_the_exit_walk_read_one_line():
    ids = sorted(NODES)
    h = {n: i + 1 for i, n in enumerate(ids)}
    items = [(o + 1, w, "stream", [h[n] for n in ns]) for o, (w, ns) in enumerate(WATERS)]
    # spans: the stem the measures run along, and every B piece off it
    stems: dict = {}
    rows = SP.compute(NODES, EDGES, h, items, {}, stems=stems)
    assert stems == {"eco": "S", "rcv": "R", "oth": "T"}
    assert {s: off for s, _lo, _hi, _a, _b, off, _sh in rows if s in {h[n] for n in WATERS[0][1]}} == {
        h["S:0"]: 0, h["B:0"]: 1, h["B:100"]: 1, h["B:200"]: 1}
    # the exit walk: from the mouth of S, into water 2 (by count it walked from B:0 into water 3)
    secs = {o: set(ss) for o, _w, _k, ss in items}
    assert T.mouth_downs(_graph(), ids, secs) == {1: {2}, 2: set(), 3: set()}
    # continuation: S's mouth is water 2's top, so water 1 goes on as water 2 (by count its "mouth" was B:0)
    db = sqlite3.connect(":memory:")
    db.executescript("CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, name TEXT, kind TEXT);"
                     "CREATE TABLE item_section (ord INTEGER, sid INTEGER);"
                     "CREATE TABLE item_down (ord INTEGER, down_ord INTEGER);")
    db.executemany("INSERT INTO item VALUES (?,?,?,?)", [(o, w, w, k) for o, w, k, _s in items])
    db.executemany("INSERT INTO item_section VALUES (?,?)", [(o, s) for o, _w, _k, ss in items for s in ss])
    db.execute("INSERT INTO item_down VALUES (1, 2)")
    assert C.links(db, ids, _graph()) == [(1, h["S:0"], 2, h["R:0"], False)]


def test_a_flow_without_lengths_is_refused_not_linkless():
    import pytest
    with pytest.raises(C.ContinuationError, match="no piece lengths"):
        C.candidates({1, 2}, {1: {1}, 2: {2}}, [(1, 2)], {1: ("X", 0), 2: ("Y", 0)}.get, C.Flow([], {}, {}))


def test_no_other_bundle_module_picks_a_main_line():
    """THE GREP PIN: no module of `deliver/bundle/` but `lines.py` defines a main-line picker or tallies sections per
    blue line; the three readers import theirs from `lines`. Mutation: re-adding `def main_stem(` to spans.py or
    `def _main(` to continuation.py fails here."""
    here = Path(L.__file__).parent
    bad = re.compile(r"^\s*def\s+(?:_main|_?main_stem|_?main_line\w*|_?stem_of|_?pick_stem)\s*\(|Counter\(\)\s*\n(?:.*\n){0,4}"
                     r".*\[ln\[0\]\]\s*\+=", re.M)
    hits = sorted(f.name for f in here.rglob("*.py") if f.name != "lines.py" and bad.search(f.read_text()))
    assert hits == []
    assert bad.search(Path(L.__file__).read_text())            # the pattern still finds the one home
    for mod in ("spans.py", "continuation.py", "topology.py"):
        assert "from pipeline.deliver.bundle.lines import" in (here / mod).read_text(), mod
