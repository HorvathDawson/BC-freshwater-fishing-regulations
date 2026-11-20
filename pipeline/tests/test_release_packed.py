"""(T686) THE THREE PACKED RELEASE TABLES — near/2, down/2, clim/3 (pipeline/deliver/release/packed.py).

The codec round trip (AGENTS 58: a new shape is taught to its codec, which refuses what it does not know): the PARITY
fixture (app/packages/data/dev/packed-parity.json) is the bytes both writers write and both readers read back — the
app's half is `app/tools/packed-parity.test.ts`. Then each `extra` packs what copy_file copied and decodes back to every
row; the contract names the packed formats; and an app that reads only the old formats is held back by the release's
app gate (it keeps its data and is asked to update), never handed a file it cannot read. THE SWITCH (owner 2026-11-20
09:59, "Ship readers now, formats later"): `packed.SHIP_PACKED_FORMATS` off writes the old forms with today's gates,
on writes the packed ones with theirs; both are held here. No data.
"""
from __future__ import annotations

import json
import sqlite3
import struct
from pathlib import Path

import pytest

from pipeline.deliver.release import app_gate as G
from pipeline.deliver.release import build as B
from pipeline.deliver.release import contract as C
from pipeline.deliver.release import packed as P

PARITY = Path(__file__).resolve().parents[2] / "app" / "packages" / "data" / "dev" / "packed-parity.json"


@pytest.mark.smoke  # T667 D6: a guard beside the file's first test
def test_parity_with_the_app_codec():
    """Byte for byte with the fixture the app's packed.ts is held to (`app/tools/packed-parity.test.ts`)."""
    fx = json.loads(PARITY.read_text())
    near = [tuple(r) for r in fx["near"]["rows"]]
    assert P.encode_near(near).hex() == fx["near"]["hex"]
    assert P.decode_near(bytes.fromhex(fx["near"]["hex"])) == [tuple(r) for r in fx["near"]["sorted"]] == sorted(near)
    rows = [tuple(r) for r in fx["down"]["rows"]]
    assert P.encode_down_block(fx["down"]["block"], rows).hex() == fx["down"]["hex"]
    assert P.decode_down_block(fx["down"]["block"], bytes.fromhex(fx["down"]["hex"])) == rows
    clim2 = bytes.fromhex(fx["clim"]["clim2"])
    assert P.clim3(clim2).hex() == fx["clim"]["clim3"]
    assert P.unclim3(bytes.fromhex(fx["clim"]["clim3"])) == clim2


def test_each_decoder_refuses_a_blob_that_does_not_parse():
    """W46: a cut or padded blob is an error naming the format, never a shorter answer."""
    fx = json.loads(PARITY.read_text())
    near, down, clim = (bytes.fromhex(x) for x in (fx["near"]["hex"], fx["down"]["hex"], fx["clim"]["clim3"]))
    for bad in (near[:-1], near + b"\0", bytes([0x80])):
        with pytest.raises(P.PackedError, match="near/2"):
            P.decode_near(bad)
    for bad in (down[:-1], down + b"\0"):
        with pytest.raises(P.PackedError, match="down/2"):
            P.decode_down_block(fx["down"]["block"], bad)
    with pytest.raises(P.PackedError, match="past the block"):          # one sid, its gap 4096: the next block's
        P.decode_down_block(0, bytes([1, 0x80, 0x20, 0, 0, 0]))
    for bad in (clim[:-1], clim + b"\0", b""):
        with pytest.raises(P.PackedError, match="clim/3"):
            P.unclim3(bad)
    # a ckm past 2500 written by hand into the high plane
    with pytest.raises(P.PackedError, match="ckm"):
        P.decode_near(bytes([1, 0, 0xFF, 0xFF]))


def test_each_encoder_refuses_what_the_old_table_could_not_hold():
    with pytest.raises(P.PackedError, match="twice"):
        P.encode_near([(10, 3), (20, 3)])
    with pytest.raises(P.PackedError, match="ckm"):
        P.encode_near([(2501, 3)])
    with pytest.raises(P.PackedError, match="lake"):
        P.encode_down_block(0, [(1, 2, 3, 0)])
    with pytest.raises(P.PackedError, match="not after"):
        P.encode_down_block(0, [(5, 2, 3, None), (5, 2, 3, None)])
    with pytest.raises(P.PackedError, match="in block"):
        P.encode_down_block(0, [(4096, 2, 3, None)])
    with pytest.raises(P.PackedError, match="not whole records"):
        P.clim3(b"\0" * 12)


def test_each_extra_packs_every_row_and_decodes_back_to_it():
    """The release's `extra` on what copy_file copied: every near/1 and down/1 row (NULLs included) comes back, and
    the climatology's clim/3 is the clim/2 the old release shipped, byte for byte."""
    con = sqlite3.connect(":memory:")
    near = [(p, c, o) for p in (3, 9, 70) for c, o in ((0, p), (2500, p + 1), (130, 20000 + p), (7, 1))]
    con.execute("CREATE TABLE place_water (place_id INTEGER NOT NULL, ckm INTEGER NOT NULL, ord INTEGER NOT NULL, "
                "PRIMARY KEY (place_id, ckm, ord)) WITHOUT ROWID")
    con.executemany("INSERT INTO place_water VALUES (?, ?, ?)", near)
    P.pack_near(con)
    assert [r[1] for r in con.execute("PRAGMA table_info(place_water)")] == C.FILES["near"]["tables"]["place_water"]
    got = sorted((p, c, o) for p, b in con.execute("SELECT place_id, b FROM place_water") for c, o in P.decode_near(b))
    assert got == sorted(near)

    down = [(1, 2, None, None), (2, 9000, 0, 1), (4095, 4096, 12, None), (4096, 4095, 2630, 1), (1958478, 3, 7, None)]
    con.execute("CREATE TABLE section_down (sid INTEGER PRIMARY KEY, down_sid INTEGER NOT NULL, len INTEGER, lake INTEGER)")
    con.executemany("INSERT INTO section_down VALUES (?, ?, ?, ?)", down)
    P.pack_down(con)
    assert [r[1] for r in con.execute("PRAGMA table_info(section_down)")] == C.FILES["down"]["tables"]["section_down"]
    blocks = con.execute("SELECT block, b FROM section_down ORDER BY block").fetchall()
    assert [k for k, _ in blocks] == [0, 1, 478]
    assert [r for k, b in blocks for r in P.decode_down_block(k, b)] == down

    f32 = b"".join(bytes([p]) + struct.pack("<5f", *v) for p, v in ((0, (1.0, 2.5, 3.0, 4.0, 900.0)),
                                                                    (5, (0.5, 2.0, 3.25, 4.5, 800.0))))
    con.execute("CREATE TABLE gauge_clim (station TEXT NOT NULL, parameter TEXT NOT NULL, bands BLOB NOT NULL)")
    con.execute("INSERT INTO gauge_clim VALUES ('08MH001', 'discharge', ?)", (f32,))
    B._climate_extra(con)
    (shipped,) = con.execute("SELECT bands FROM gauge_clim").fetchone()
    assert P.unclim3(shipped) == B.clim_f16(f32)


def test_the_contract_names_the_packed_formats_and_their_layout():
    assert (C.FILES["near"]["format"], C.FILES["down"]["format"], C.FILES["climate"]["format"]) == \
        (P.NEAR_FORMAT, P.DOWN_FORMAT, P.CLIMATE_FORMAT) == ("near/2", "down/2", "climate/2")
    assert C.FILES["near"]["tables"]["place_water"] == ["place_id", "b"]
    assert C.FILES["down"]["tables"]["section_down"] == ["block", "b"]
    assert C.UNITS["climate.gauge_clim.bands"].startswith(P.CLIM_CODEC)
    assert {"near.place_water.b", "down.section_down.b"} <= set(C.UNITS)


def test_an_app_that_reads_only_the_old_formats_is_held_back_never_broken():
    """OLD-APP COMPATIBILITY. A release carrying near/2, down/2, climate/2 needs the app version recorded reading them
    (app_reads.json): the app gate derives that floor, refuses a stamp below it, and an older app given the release
    (`releaseAllowed` on `minAppVersion`, and `readsRelease` on each file's format) keeps the data it holds and asks
    to be updated. A release cut before T686 (near/1, …) is still read by the new app (its ALSO_READS)."""
    apps = {"0.8.3": {"near": ["near/1"], "down": ["down/1"], "climate": ["climate/1"]},
            "0.8.4": {"near": ["near/2", "near/1"], "down": ["down/2", "down/1"], "climate": ["climate/2", "climate/1"]}}
    new = {"files": {"near": {"format": "near/2"}, "down": {"format": "down/2"}, "climate": {"format": "climate/2"}}}
    old = {"files": {"near": {"format": "near/1"}, "down": {"format": "down/1"}, "climate": {"format": "climate/1"}}}
    assert G.oldest_reader(new, apps) == "0.8.4"
    assert G.oldest_reader(old, apps) == "0.8.3"                       # the new app still reads the old release
    assert G.problems(new, "0.8.3", apps) and not G.problems(new, "0.8.4", apps)
    # the committed table: the recorded current app reads both forms of each
    from pipeline.tools.emit.app_reads import current_reads
    reads = current_reads()
    for name, fmt in (("near", "near/1"), ("down", "down/1"), ("climate", "climate/1")):
        assert reads[name][0] == C.FILES[name]["format"] and fmt in reads[name]


def test_the_switch_is_off_until_084_is_in_the_production_stores():
    """Owner 2026-11-20 09:59: this release's data (staging and production) stays near/1, down/1, climate/1. Flipping
    it is a reviewed one-line change (packed.py), made in the first data release after app 0.8.4 is in production."""
    assert P.SHIP_PACKED_FORMATS is False


@pytest.mark.parametrize("on", [False, True])
def test_each_switch_position_writes_its_forms_and_gates(monkeypatch, tmp_path, on):
    """OFF: the old formats, columns, climatology codec and gates, and the copied rows untouched (the release is the one
    before T686). ON: the packed ones. Both are read when the release runs, not at import."""
    monkeypatch.setattr(P, "SHIP_PACKED_FORMATS", on)
    files = C.written_files()
    want = {"near": ("near/2", ["place_id", "b"]), "down": ("down/2", ["block", "b"]),
            "climate": ("climate/2", ["station", "parameter", "bands"])} if on else \
        {"near": ("near/1", ["place_id", "ckm", "ord"]), "down": ("down/1", ["sid", "down_sid", "len", "lake"]),
         "climate": ("climate/1", ["station", "parameter", "bands"])}
    for name, (fmt, cols) in want.items():
        t = {"near": "place_water", "down": "section_down", "climate": "gauge_clim"}[name]
        assert (files[name]["format"], files[name]["tables"][t], B.formats()[name]) == (fmt, cols, fmt)
    assert C.FILES["near"]["format"] == "near/2"            # the app's contract is the packed one either way
    gates = B.size_gates()
    assert {k: gates[k] for k in ("near", "down", "climate")} == (B.PACKED_GATES if on else B.UNPACKED_GATES)
    # the bundle half's choice on a tiny bundle: what near and climate hold
    b = tmp_path / "b.sqlite"
    con = sqlite3.connect(b)
    con.executescript("""
        CREATE TABLE place_water (place_id INTEGER NOT NULL, ckm INTEGER NOT NULL, ord INTEGER NOT NULL,
                                  PRIMARY KEY (place_id, ckm, ord)) WITHOUT ROWID;
        INSERT INTO place_water VALUES (1, 10, 3), (1, 2500, 4), (2, 0, 3);
        CREATE TABLE gauge_clim (station TEXT NOT NULL, parameter TEXT NOT NULL, bands BLOB NOT NULL);
    """)
    f32 = bytes([3]) + struct.pack("<5f", 1.0, 2.0, 3.0, 4.0, 5.0)
    con.execute("INSERT INTO gauge_clim VALUES ('08MH001', 'discharge', ?)", (f32,))
    con.commit()
    con.close()
    near = B.copy_file(tmp_path / "near.sqlite", {"b": b}, [("b", "place_water", None)], {"format": B.formats()["near"]},
                       extra=P.pack_near if on else None)
    rows = sqlite3.connect(near).execute("SELECT * FROM place_water ORDER BY 1").fetchall()
    if on:
        assert [(p, c, o) for p, bl in rows for c, o in P.decode_near(bl)] == [(1, 10, 3), (1, 2500, 4), (2, 0, 3)]
    else:
        assert sorted(rows) == [(1, 10, 3), (1, 2500, 4), (2, 0, 3)]
    clim = B.copy_file(tmp_path / "clim.sqlite", {"b": b}, [("b", "gauge_clim", None)], {},
                       extra=B._climate_extra if on else B._climate_f16)
    (bands,) = sqlite3.connect(clim).execute("SELECT bands FROM gauge_clim").fetchone()
    assert (P.unclim3(bands) if on else bands) == B.clim_f16(f32)


def test_off_no_minappversion_is_derived_from_the_three_files():
    """With the switch off the manifest names near/1, down/1, climate/1: the oldest recorded app reads them, so the
    three files ask nothing of the app gate (the release's floor is what it was)."""
    m = {"files": {n: {"format": C.UNPACKED[n]["format"]} for n in ("near", "down", "climate")}}
    assert G.oldest_reader(m) == min(G.table(), key=G.ver)
    # (T679 #16) paint/5 too: no app floor moves while the switch is off (paint/5 is read since 0.3.0)
    assert G.needs({"files": {"paint": {"format": C.UNPACKED["paint"]["format"]}}}, G.table())["paint/5"] != "0.8.4"
    # ON: paint/6 and the packed three need 0.8.4
    on = {"files": {n: {"format": C.FILES[n]["format"]} for n in ("paint", "near", "down", "climate")}}
    assert G.oldest_reader(on) == "0.8.4"


@pytest.mark.parametrize("on", [False, True])
def test_t679_16_paint_follows_the_same_switch(monkeypatch, on):
    """(T679 #16, owner 2026-11-20 10:00: every left-over on this release) paint/6 (the one access flag) is written with
    the packed formats and not before; off, paint/5 exactly as before (no `mostly` column)."""
    from pipeline.deliver.release import paint as PAINT
    from pipeline.tests.test_release import _tiny_bundle
    from pipeline.deliver import status_index as SI
    from pipeline.tests.test_status_index import _tiny
    monkeypatch.setattr(P, "SHIP_PACKED_FORMATS", on)
    assert B.formats()["paint"] == ("paint/6" if on else "paint/5") == f"paint/{PAINT.written_version()}"
    db = _tiny_bundle()
    db.execute("INSERT INTO section_no_access VALUES (3, 1000), (4, 600), (5, 20)")
    status = SI.encode(_tiny())
    b = PAINT.encode(status, db)
    assert b[4] == (6 if on else 5)
    d = PAINT.decode(b)
    assert d.get("mostly") == ([3, 4] if on else None)
    assert PAINT.encode(status, db, version=5) == (PAINT.encode(status, db) if not on else b[:4] + bytes([5]) + b[5:-len(
        PAINT.mostly_column(db))])                      # paint/6 is paint/5 + the column, nothing else moves


def test_t679_8_flow_own_is_built_only_behind_its_switch_and_refuses_a_stray_row():
    """(T679 #8) `flow-own` ships (`packed.SHIP_OWN_GAUGE_FLAG` on: the owner saw the list, "Ship it" 2026-11-20 10:45); optional in
    the contract, so a release without it is read as before. Its writer refuses an own row on a section with no panel."""
    from pipeline.deliver.release import paint as PAINT
    from pipeline.tests.test_release import _tiny_bundle
    assert P.SHIP_OWN_GAUGE_FLAG is True and "flow-own" in C.OPTIONAL
    db = _tiny_bundle()
    with pytest.raises(ValueError, match="section_own"):
        PAINT.encode_own(db)
    db.executescript("CREATE TABLE section_own (sid INTEGER PRIMARY KEY, bits INTEGER NOT NULL);"
                     "INSERT INTO section_own VALUES (3, 1), (9, 1);")
    assert PAINT.decode_own(PAINT.encode_own(db), 4) == [1, 0, 0, 1]
    db.execute("INSERT INTO section_own VALUES (77, 1)")
    with pytest.raises(ValueError, match="no panel"):
        PAINT.encode_own(db)
