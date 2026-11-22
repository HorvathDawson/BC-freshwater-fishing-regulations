"""(T686) THE THREE PACKED RELEASE TABLES — near/2, down/2, clim/3 (pipeline/deliver/release/packed.py).

The codec round trip (AGENTS 58: a new shape is taught to its codec, which refuses what it does not know): the PARITY
fixture (app/packages/data/dev/packed-parity.json) is the bytes both writers write and both readers read back — the
app's half is `app/tools/packed-parity.test.ts`. Then each `extra` packs what copy_file copied and decodes back to every
row; the contract names the packed formats; and an app that reads only the old formats is held back by the release's
app gate (it keeps its data and is asked to update), never handed a file it cannot read. (T975) THE SWITCH that wrote
the old forms (T686, owner 2026-11-20 09:59, "Ship readers now, formats later") is gone with them: a release writes
the packed forms only, and a guard holds that no reader or writer of a replaced format remains. No data.
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
    to be updated. (T975) The app in this tree reads the packed forms ONLY: a release cut before T686 is refused by
    its `readsRelease`, never read."""
    apps = {"0.8.3": {"near": ["near/1"], "down": ["down/1"], "climate": ["climate/1"]},
            "0.8.4": {"near": ["near/2", "near/1"], "down": ["down/2", "down/1"], "climate": ["climate/2", "climate/1"]},
            "0.8.5": {"near": ["near/2"], "down": ["down/2"], "climate": ["climate/2"]}}
    new = {"files": {"near": {"format": "near/2"}, "down": {"format": "down/2"}, "climate": {"format": "climate/2"}}}
    assert G.oldest_reader(new, apps) == "0.8.4"
    assert G.problems(new, "0.8.3", apps) and not G.problems(new, "0.8.4", apps)
    # the committed table and the code: the current app reads each packed file in its contract format alone
    from pipeline.tools.emit.app_reads import current_reads
    reads = current_reads()
    for name in ("near", "down", "climate"):
        assert reads[name] == [C.FILES[name]["format"]], (name, reads[name])
    assert reads["paint"] == [C.FILES["paint"]["format"], "paint/4"]       # paint/4: the frozen app fixture releases


def test_t975_the_switch_and_the_old_writers_are_gone():
    """(T975) The switch that wrote near/1, down/1, climate/1 and paint/5 (T686, T679 #16), its old-form contract, its
    gates and its writers are removed: no code path can write a replaced format."""
    from pipeline.deliver.release import paint as PAINT
    assert not hasattr(P, "SHIP_PACKED_FORMATS")
    assert not any(hasattr(C, n) for n in ("UNPACKED", "written_files"))
    assert not any(hasattr(B, n) for n in ("UNPACKED_GATES", "PACKED_GATES", "size_gates", "formats", "_climate_f16"))
    assert not any(hasattr(PAINT, n) for n in ("MOSTLY_VERSION", "MOSTLY_FORMAT", "written_version"))
    assert (PAINT.VERSION, PAINT.FORMAT) == (6, "paint/6")


def test_t975_a_release_writes_only_the_packed_formats():
    """(T975) What a release writes: the contract's packed formats and their gates, nothing else (B.FORMATS is the
    manifest's `format` of every file), and the mini release the REAL pipeline built (committed,
    app/packages/data/dev/mini-release, `python -m pipeline.tools.mini_release --check`) carries exactly them: near and
    down in their packed tables, the climatology clim/3, paint/6. No data."""
    import gzip
    import tempfile
    want = {"near": "near/2", "down": "down/2", "climate": "climate/2", "paint": "paint/6"}
    assert {n: B.FORMATS[n] for n in want} == {n: C.FILES[n]["format"] for n in want} == want
    assert {n: B.SIZE_GATES[n] for n in ("near", "down", "climate")} == \
        {"near": 1_000_000, "down": 700_000, "climate": 1_100_000}
    from pipeline.tools.mini_release import COMMITTED
    (rel,) = [d for d in COMMITTED.iterdir() if d.is_dir()]
    m = json.loads((rel / "manifest.json").read_text())
    assert {n: m["files"][n]["format"] for n in want} == want
    tables = {"near": ("place_water", ["place_id", "b"]), "down": ("section_down", ["block", "b"]),
              "climate": ("gauge_clim", ["station", "parameter", "bands"])}
    with tempfile.TemporaryDirectory() as tmp:
        for name, (table, cols) in tables.items():
            db = Path(tmp) / f"{name}.sqlite"
            db.write_bytes(gzip.decompress((COMMITTED / m["files"][name]["url"]).read_bytes()))
            con = sqlite3.connect(db)
            assert [r[1] for r in con.execute(f"PRAGMA table_info({table})")] == cols, name
            meta = dict(con.execute("SELECT k, v FROM meta"))
            assert meta["format"] == want[name]
            if name == "climate":
                assert meta["clim"] == P.CLIM_CODEC
                for (bands,) in con.execute("SELECT bands FROM gauge_clim"):
                    P.unclim3(bands)                                       # every blob parses as clim/3
            if name == "near":
                for _p, b in con.execute("SELECT place_id, b FROM place_water"):
                    P.decode_near(b)
            if name == "down":
                for k, b in con.execute("SELECT block, b FROM section_down"):
                    P.decode_down_block(k, b)
            con.close()
    paint = gzip.decompress((COMMITTED / m["files"]["paint"]["url"]).read_bytes())
    assert paint[:5] == b"BCPT" + bytes([6])


#: (T975) the formats the packed ones replaced: no product code may name one as a string
REPLACED = ("near/1", "down/1", "climate/1", "paint/5")


@pytest.mark.smoke  # a guard
def test_t975_no_reader_or_writer_for_a_replaced_format_remains():
    """(T975) GUARD: no product code — the pipeline's release and app-reads modules, the app's packages, the mobile
    app, the feeds worker — names near/1, down/1, climate/1 or paint/5 as a string (a reader compares one, a writer
    stamps one), nor the T686 old-format constants. Python: a string TOKEN that is exactly the format (a docstring
    telling the history is not one); TypeScript: a quoted literal outside comments. Tests, fixtures and generated
    goldens are not product code; app_reads.json (the record of what older apps read) is not scanned."""
    import io
    import os
    import re
    import tokenize
    root = Path(__file__).resolve().parents[2]
    lit = re.compile("[\"'`](" + "|".join(re.escape(f) for f in REPLACED) + ")[\"'`]")
    names = re.compile(r"\b(SHIP_PACKED_FORMATS|NEAR_ALSO_READS|DOWN_ALSO_READS|CLIMATE_ALSO_READS|UNPACKED_GATES|"
                       r"written_files|size_gates)\b")
    bad = []
    py = [root / "pipeline" / "deliver", root / "pipeline" / "tools" / "emit", root / "scripts"]
    ts = [root / "app" / "packages", root / "app" / "apps" / "mobile" / "src", root / "app" / "workers" / "feeds" / "src"]
    skip = {"node_modules", "__pycache__", "tests", "dist", "fixtures", "dev", "__snapshots__"}
    for top, ext in [(d, ".py") for d in py] + [(d, x) for d in ts for x in (".ts", ".tsx")]:
        for folder, dirs, files in os.walk(top):
            dirs[:] = sorted(d for d in dirs if d not in skip and not d.startswith("."))
            for f in sorted(files):
                if not f.endswith(ext) or ".test." in f or f.startswith("test_") or ".fixture." in f or f.endswith(".d.ts"):
                    continue
                path = Path(folder) / f
                rel = path.relative_to(root).as_posix()
                text = path.read_text(encoding="utf-8")
                if ext == ".py":
                    toks = list(tokenize.generate_tokens(io.StringIO(text).readline))
                    bad += [f"{rel}:{x.start[0]}: {x.string}" for x in toks
                            if x.type == tokenize.STRING and x.string.strip("\"'") in REPLACED]
                    bad += [f"{rel}:{x.start[0]}: {x.string}" for x in toks
                            if x.type == tokenize.NAME and names.fullmatch(x.string)]
                else:
                    for n, line in enumerate(text.splitlines(), 1):
                        code = re.sub(r"(^|\s)//.*$|^\s*\*.*$|/\*.*?\*/", "", line)
                        bad += [f"{rel}:{n}: {m.group(0)}" for m in lit.finditer(code)]
                        bad += [f"{rel}:{n}: {m.group(0)}" for m in names.finditer(code)]
    assert bad == [], "a replaced format (or T686's switch) is still named:\n" + "\n".join(bad)


def test_t975_paint_writes_paint6_only():
    """(T679 #16, T975) paint/6 (the one access flag) is the only paint a release writes: no version to choose."""
    from pipeline.deliver.release import paint as PAINT
    from pipeline.tests.test_release import _tiny_bundle
    from pipeline.deliver import status_index as SI
    from pipeline.tests.test_status_index import _tiny
    db = _tiny_bundle()
    db.execute("INSERT INTO section_no_access VALUES (3, 1000), (4, 600), (5, 20)")
    b = PAINT.encode(SI.encode(_tiny()), db)
    assert b[4] == 6 and PAINT.decode(b)["mostly"] == [3, 4]
    with pytest.raises(ValueError, match="not a paint/6 file"):
        PAINT.decode(b[:4] + bytes([5]) + b[5:])


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


def test_t973_cw_through_is_optional_and_carries_each_section_one_note(tmp_path):
    """(T973) `cw-through/1`: OPTIONAL in the contract (an app before 0.8.4 knows no such file, so the app gate asks
    nothing of it and it reads nothing new), written into every release, and its writer is the bundle's
    `designation_carried` joined to the designation: one note per (unit, class, period, through), one row per section.
    A bundle without the table is refused."""
    from pipeline.deliver.bundle.build import SCHEMA
    assert "cw-through" in C.OPTIONAL and B.FORMATS["cw-through"] == C.FILES["cw-through"]["format"]
    assert G.needs({"files": {"cw-through": {"format": "cw-through/1"}}},
                   {"0.8.3": {"core": ["core/1"]}, "0.8.4": {"cw-through": ["cw-through/1"]}}) == {"cw-through/1": "0.8.3"}
    bundle = tmp_path / "bundle.sqlite"
    db = sqlite3.connect(bundle)
    db.executescript(SCHEMA.read_text())
    rec = lambda when: json.dumps({"kind": "designation", "id": "d", "classified": "II", **({"when": when} if when else {})})
    db.executemany("INSERT INTO designation (entry_id, designation_id, classified, unit, unit_name, placement, label, "
                   "parts, verbatim, record) VALUES (?, 'd', 'II', ?, ?, 'sections', '', '{}', '', ?)",
                   [("r6:stellako@6-4", "stellako_river", "Stellako River", rec(None)),
                    ("r6:morice@6-9", "morice_river", "Morice River",
                     rec({"dates": [{"from_day": 1, "from_month": 9, "to_day": 31, "to_month": 10}]}))])
    endako = '[["gnis:1","Endako River"]]'
    db.executemany("INSERT INTO designation_carried VALUES (?, ?, 'd', ?)",
                   [(5, "r6:stellako@6-4", endako), (7, "r6:stellako@6-4", endako),
                    (9, "r6:morice@6-9", '[["gnis:2","Gosnell Creek"]]')])
    # each designation's own section (the source the carried answer is read at); 5 and 7 share a licensing set
    db.executemany("INSERT INTO licensing_set VALUES (?, 'designation', ?, 'd', 'reach')",
                   [(0, "r6:stellako@6-4"), (1, "r6:morice@6-9")])
    db.executemany("INSERT INTO section_licensing VALUES (?, ?)", [(100, 0), (200, 1), (5, 2), (7, 2)])
    db.commit()
    db.close()
    asked = []

    def year_of(sid, source_sid, designation, set_id):     # stands in for answers.licence.carried_year
        asked.append((sid, source_sid, designation, set_id))
        return {1: {"holds": {"designations": [designation]}}, 200: {"holds": {"designations": []}}}
    out = B.copy_file(tmp_path / "cw.sqlite", {"b": bundle}, [], {"format": "cw-through/1"},
                      extra=lambda con: B.cw_through(con, year_of=year_of))
    got = sqlite3.connect(out)
    assert asked == [(5, 100, "r6:stellako@6-4#d", 2), (9, 200, "r6:morice@6-9#d", None)]   # once per (set, unit)
    assert got.execute("SELECT sid, ix, yx FROM section_cw ORDER BY sid").fetchall() == [(5, 0, 0), (7, 0, 0), (9, 1, 1)]
    assert json.loads(got.execute("SELECT j FROM cw_year WHERE yx = 1").fetchone()[0]) == [
        [1, {"holds": {"designations": ["r6:morice@6-9#d"]}}], [200, {"holds": {"designations": []}}]]
    assert got.execute("SELECT * FROM cw_note ORDER BY ix").fetchall() == [
        (0, "Stellako River", "II", None, endako),
        (1, "Morice River", "II", '{"dates":[{"from_day":1,"from_month":9,"to_day":31,"to_month":10}]}',
         '[["gnis:2","Gosnell Creek"]]')]
    assert {t: [r[1] for r in got.execute(f"PRAGMA table_info({t})")] for t in ("cw_note", "cw_year", "section_cw")} == {
        t: cols for t, cols in C.FILES["cw-through"]["tables"].items() if t != "meta"}
    bare = tmp_path / "bare.sqlite"
    sqlite3.connect(bare).close()
    with pytest.raises(ValueError, match="designation_carried"):
        B.copy_file(tmp_path / "cw2.sqlite", {"b": bare}, [], {"format": "cw-through/1"}, extra=B.cw_through)


def test_t973_review_carried_sections_share_a_year_only_under_one_full_licence_key(tmp_path):
    """(T973 review, LF) `cw_through` computes one licence year per (LICENCE KEY, unit), the key `part_licence_key` gives:
    licensing set, province exceptions, tidal, water kind, a keyed rule set. Sections 5 (a stream), 7 (a lake) and 8
    (tidal) share licensing set 2 and the Stellako's unit; each is its own key, so each is asked for its own year.
    MUTATION: grouping by (licensing set, unit) alone gives 7 and 8 the stream's year (asked once)."""
    from pipeline.deliver.bundle.build import SCHEMA
    bundle = tmp_path / "bundle.sqlite"
    db = sqlite3.connect(bundle)
    db.executescript(SCHEMA.read_text())
    db.execute("INSERT INTO designation (entry_id, designation_id, classified, unit, unit_name, placement, label, parts, "
               "verbatim, record) VALUES ('r6:stellako@6-4', 'd', 'II', 'stellako_river', 'Stellako River', 'sections', "
               "'', '{}', '', ?)", (json.dumps({"kind": "designation", "id": "d", "classified": "II"}),))
    db.executemany("INSERT INTO designation_carried VALUES (?, 'r6:stellako@6-4', 'd', '[]')", [(5,), (6,), (7,), (8,)])
    db.execute("INSERT INTO licensing_set VALUES (0, 'designation', 'r6:stellako@6-4', 'd', 'reach')")
    db.executemany("INSERT INTO section_licensing VALUES (?, ?)", [(100, 0), (5, 2), (6, 2), (7, 2), (8, 2)])
    db.executemany("INSERT INTO item (ord, item_id, name, kind) VALUES (?, ?, ?, ?)",
                   [(0, "gnis:1", "Endako River", "stream"), (1, "wbk:1", "Endako Lake", "lake")])
    db.executemany("INSERT INTO item_section VALUES (?, ?)", [(0, 5), (0, 6), (1, 7), (0, 8)])
    db.execute("INSERT INTO tidal VALUES (8, 'zp:tidal')")
    db.commit()
    db.close()
    asked = []

    def year_of(sid, source_sid, designation, set_id):
        asked.append(sid)
        return {1: {"holds": {"at": sid}}}
    out = B.copy_file(tmp_path / "cw.sqlite", {"b": bundle}, [], {"format": "cw-through/1"},
                      extra=lambda con: B.cw_through(con, year_of=year_of))
    assert asked == [5, 7, 8]                    # 6 is 5's key: one year for both
    got = sqlite3.connect(out)
    assert got.execute("SELECT sid, yx FROM section_cw ORDER BY sid").fetchall() == [(5, 0), (6, 0), (7, 1), (8, 2)]


def test_t973_the_carried_answer_is_the_sections_own_with_the_units_on_designation_rows_added():
    """(owner 2026-11-20 13:22) `carried_holds`: on a day the unit is in force at its own water, the section's answer
    gains the designation and what holds BECAUSE of it there (`on_designation`: the Classified Waters Licence), nothing
    else of the source's; on a day it is not, the section's own answer as it is."""
    from pipeline.deliver.answers import licence as LA
    reqs = {"zp:basic#basic": {"_placement": "province"}, "zp:cwl#cwl": {"_placement": "on_designation"},
            "r6:x#steelhead": {"_placement": "sections"}}
    C = LA.Corpus(records={}, index={}, requirements=reqs, exemptions=[], alternatives=[], terms=[])
    here = {"holds": ["zp:basic#basic"], "rows": ["zp:basic#basic"], "displaced": {}, "designations": [], "_desig": [],
            "stamp_period": False}
    unit = {"entry_id": "r6:stellako", "id": "d", "classified": "II"}
    src = {"holds": ["r6:x#steelhead", "zp:basic#basic", "zp:cwl#cwl"], "rows": ["r6:x#steelhead", "zp:basic#basic", "zp:cwl#cwl"],
           "displaced": {}, "designations": ["r6:stellako#d"], "_desig": [unit], "stamp_period": False, "_waived_by": {}}
    got = LA.carried_holds(C, here, src, "r6:stellako#d", (7, 15))
    assert got["rows"] == got["holds"] == ["zp:basic#basic", "zp:cwl#cwl"] and got["designations"] == ["r6:stellako#d"]
    assert got["_desig"] == [unit]
    assert LA.carried_holds(C, here, {**src, "designations": []}, "r6:stellako#d", (11, 20)) is here
    # the designation's own stamp waiver goes with it: the steelhead stamp the section holds comes off, as the box says
    here2 = {**here, "holds": here["holds"] + ["r6:x#steelhead"], "rows": here["rows"] + ["r6:x#steelhead"], "waived": []}
    got = LA.carried_holds(C, here2, {**src, "waived": ["r6:x#steelhead"], "_waived_by": {"r6:x#steelhead": ["r6:stellako#d"]}},
                           "r6:stellako#d", (7, 15))
    assert "r6:x#steelhead" not in got["rows"] and got["waived"] == ["r6:x#steelhead"]
    # (T973 review, LF) TWO DESIGNATIONS AT THE SOURCE: the waiver another one prints there is not the carried unit's,
    # so the section keeps its steelhead stamp. MUTATION: copying the source's whole `waived` takes it off
    two = {**src, "designations": ["r6:other#w", "r6:stellako#d"], "waived": ["r6:x#steelhead"],
           "_waived_by": {"r6:x#steelhead": ["r6:other#w"]}}
    got = LA.carried_holds(C, here2, two, "r6:stellako#d", (7, 15))
    assert "r6:x#steelhead" in got["rows"] and "r6:x#steelhead" in got["holds"] and got["waived"] == []
