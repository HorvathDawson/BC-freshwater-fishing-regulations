"""(T369) The `lines` release file: a link's PLACE (stream key + measure) resolves to a section of the release held.

A tiny graph's `section_measure` rows (bundle/section_measure.py) packed into blocks (release/lines.py) and read back
with the same layout; the PARITY fixture (app/packages/data/dev/section-line-parity.json) is the block the app's
`encodeLineBlock` writes too (`app/tools/section-line-parity.test.ts`), byte for byte. No data.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from pipeline.deliver.bundle import section_measure as SM
from pipeline.deliver.release import build as B
from pipeline.deliver.release import contract as C
from pipeline.deliver.release import lines as L

PARITY = Path(__file__).resolve().parents[2] / "app" / "packages" / "data" / "dev" / "section-line-parity.json"


@dataclass(frozen=True)
class _Node:
    kind: str
    down_m: float
    length_m: float


def _graph():
    ids = ["-2300000000:0", "356364584:0", "356364584:312", "356364584:7400", "356364590:250", "lake:7", "lake:9"]
    nodes = {"-2300000000:0": _Node("stream", 0.0, 10.4),
             "356364584:0": _Node("stream", 0.0, 312.7),          # ends at up_m 312.7: the next piece's int(down_m)
             "356364584:312": _Node("stream", 312.7, 6277.3 - 1e-9),  # up_m 6590 a hair under: still 6590
             "356364584:7400": _Node("stream", 7400.2, 1700.0),   # a lake 6590..7400 below it
             "356364590:250": _Node("stream", 250.9, 80.0),
             "lake:7": _Node("lake", 0.0, 0.0), "lake:9": _Node("lake", 0.0, 0.0)}
    return ids, nodes


def test_section_measure_reads_the_graph_and_skips_lakes():
    ids, nodes = _graph()
    assert SM.rows(ids, nodes) == [(1, -2300000000, 0, 10), (2, 356364584, 0, 312), (3, 356364584, 312, 6590),
                                   (4, 356364584, 7400, 9100), (5, 356364590, 250, 330)]


def test_section_measure_refuses_a_stream_handle_the_graph_lacks():
    ids, nodes = _graph()
    del nodes["356364584:312"]
    with pytest.raises(SystemExit, match="different atlas runs"):
        SM.rows(ids, nodes)


def test_a_tiny_graph_packs_and_reads_back():
    ids, nodes = _graph()
    con = sqlite3.connect(":memory:")
    assert L.write(con, SM.rows(ids, nodes)) == 5
    rows = con.execute("SELECT block, n, blk0, m0, b FROM section_line").fetchall()
    assert [r[:4] for r in rows] == [(0, 6, -2300000000, 0)]           # handle 0's filler + 5 stream handles
    got = L.decode_block(rows[0][4], 6)
    assert got == [(-2300000000, 0, 0),                    # the filler: handle 1's key and start, spans nothing
                   (-2300000000, 0, L.OPEN),               # its stream's one section: open (the filler's run)
                   (356364584, 0, 312), (356364584, 312, 6590), (356364584, 7400, 9100),
                   (356364590, 250, L.OPEN)]               # its stream's one section: open, no length shipped
    assert [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'index'")] == ["section_line_head"]


def test_blocks_of_4096_and_a_stream_cut_across_two_is_closed_in_both():
    rows = [(h, 1000 + h, 0, 50) for h in range(1, 4095)] + [(4095, 9000, 0, 100), (4096, 9000, 100, 250),
                                                              (4097, 9001, 5, 60)]
    bs = L.blocks(rows)
    assert [(k, n, b0, m0) for k, n, b0, m0, _ in bs] == [(0, 4096, 1001, 0), (1, 2, 9000, 100)]
    assert L.decode_block(bs[0][4], 4096)[-2:] == [(5094, 0, L.OPEN), (9000, 0, 100)]
    assert L.decode_block(bs[1][4], 2) == [(9000, 100, 250), (9001, 5, L.OPEN)]


def test_blocks_refuse_a_gap_or_a_disorder():
    with pytest.raises(ValueError, match="contiguous"):
        L.blocks([(1, 5, 0, 10), (3, 6, 0, 10)])
    with pytest.raises(ValueError, match="not after"):
        L.blocks([(1, 5, 10, 20), (2, 5, 0, 10)])


def test_parity_with_the_app_writer():
    """The block `encodeLineBlock` writes (app/tools/section-line-parity.test.ts holds it to the same hex): byte for byte."""
    fx = json.loads(PARITY.read_text())
    lines = [tuple(x) for x in fx["lines"]]
    assert L.encode_block(lines).hex() == fx["hex"]
    assert L.decode_block(bytes.fromhex(fx["hex"]), len(lines)) == lines


def test_the_release_ships_lines_as_its_own_file_outside_every_web_moment():
    assert C.FILES["lines"]["tables"]["section_line"] == L.COLUMNS and B.FORMATS["lines"] == L.FORMAT
    assert "lines" in B.SIZE_GATES
    assert all("lines" not in m["files"] for m in C.WEB_BUDGET.values())


def test_a_bundle_without_section_measure_is_refused():
    with pytest.raises(B.ReleaseError, match="section_measure"):
        B.bundle_section_measure(sqlite3.connect(":memory:"))


def test_lines_is_held_by_a_phone_too(tmp_path):
    """(T386, owner 2026-11-04) `lines` is no longer web-only (it was, T369): a phone holds it so an unnamed reach is
    carried across a data update and a link resolves the same way on both — it counts in the offline total."""
    assert C.WEB_ONLY == frozenset() and "lines" in C.FILES

    def offline(files):
        (tmp_path / "manifest.json").write_text(json.dumps({"files": files, "tiles": {"mask": {"bytes": 1}}}))
        return [p for p in B.gate_problems(tmp_path) if p.startswith("offline total")]
    files = {k: {"bytes": 1, "gz": 1} for k in C.FILES}
    files["water"]["gz"] = B.OFFLINE_GATE - len(C.FILES)          # the phone's set at its gate, one byte each else
    assert offline(files) == []
    files["lines"]["gz"] = B.SIZE_GATES["lines"]                    # `lines` at its size: over (it is the phone's now)
    assert len(offline(files)) == 1


def test_ship_marks_a_web_only_file_and_no_other(tmp_path, monkeypatch):
    """The mechanism, kept for a file that is the web's alone: `ship` marks a `WEB_ONLY` name `web_only`, nothing else;
    with none listed (today) no entry carries it."""
    def shipped(out: str, rid: str) -> dict:
        work = tmp_path / f"w-{out}"
        work.mkdir()
        files = {}
        for name in ("lines", "near"):
            p = work / f"{name}.sqlite"
            p.write_bytes(name.encode())
            files[name] = p
        d = B.ship(work, files, "h" * 16, tmp_path / out, data_release=rid)
        return json.loads((d / "manifest.json").read_text())["files"]
    m = shipped("out", "r-000000000000")
    assert (m["lines"].get("web_only"), m["near"].get("web_only")) == (None, None)
    monkeypatch.setattr(C, "WEB_ONLY", frozenset({"lines"}))
    m = shipped("out2", "r-000000000001")
    assert (m["lines"].get("web_only"), m["near"].get("web_only")) == (True, None)
