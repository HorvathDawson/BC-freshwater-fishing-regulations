"""P08: THE RELEASE — only shipped files, every file paired to one atlas, paint and regs parity, determinism, sizes.

No data: paint and the climatology conversion round-trip on synthetic inputs, and the manifest's shape. On a bundle
(`needs_bundle`): a release is built into a temp dir from `UI_EXPORT_BUNDLE` (a bundle with the P09 tables; point it at
a side bundle until the live one is rebuilt), the live regs store, status index and card-gate stamp. Query parity and
the perf gates are stage 2 (`test_release_perf.py`, with `emit_pack`).
"""
from __future__ import annotations

import gzip
import json
import os
import sqlite3
import struct
from pathlib import Path

import pytest

from pipeline.deliver.release import build as B
from pipeline.deliver.release import paint as P
from pipeline.deliver.release import shards as SH


# ---- no data -----------------------------------------------------------------------------------------------------

def _tiny_bundle() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE gauge (station TEXT PRIMARY KEY);
        CREATE TABLE section_panel (sid INTEGER PRIMARY KEY, panel_id INTEGER, area_km2 REAL);
        CREATE TABLE panel_member (panel_id INTEGER, ord INTEGER, station TEXT, role TEXT, area_km2 REAL,
                                   years INTEGER, regulated INTEGER, same_river INTEGER);
        CREATE TABLE basin_member (basin_id TEXT, station TEXT, area_km2 REAL, years INTEGER);
        INSERT INTO gauge VALUES ('08MH001'), ('08MH016');
        INSERT INTO section_panel VALUES (3, 7, 1.044), (4, 7, 1.883), (5, 7, 2.659), (9, 2, 64000.5);
        INSERT INTO panel_member VALUES (2, 0, '08MH016', 'down', 4840.015, 66, 0, 1), (7, 0, '08MH001', 'up', 12.5, 3, 1, 0);
        INSERT INTO basin_member VALUES ('ADMS', '08MH001', 70.41000366210938, 33), ('BABL', '08MH016', NULL, 4);
    """)
    return db


def test_paint_and_flow_area_round_trip():
    db = _tiny_bundle()
    status = b"BCSI" + bytes(range(20))
    d = P.decode(P.encode(status, db))
    assert d["status"] == status
    assert d["section_panel"] == [(3, 7), (4, 7), (5, 7), (9, 2)]
    assert d["panel_member"] == [(2, 0, "08MH016", "down", 4840.015, 66, 0, 1), (7, 0, "08MH001", "up", 12.5, 3, 1, 0)]
    assert d["basin_member"] == [("ADMS", "08MH001", 70.41000366210938, 33), ("BABL", "08MH016", None, 4)]
    assert P.decode_area(P.encode_area(db), 4) == [1.044, 1.883, 2.659, 64000.5]
    with pytest.raises(ValueError):
        P.decode_area(P.encode_area(db), 3)          # paired with a paint of other sections: refused


def test_climatology_converts_to_float16_and_refuses_what_does_not_fit():
    rec = bytes([5]) + struct.pack("<5f", 1.0, 2.5, 10.25, 300.0, 1500.0)
    out = B.clim_f16(rec)
    assert len(out) == 11 and out[0] == 5 and struct.unpack("<5e", out[1:]) == (1.0, 2.5, 10.25, 300.0, 1500.0)
    with pytest.raises(SystemExit):
        B.clim_f16(bytes([1]) + struct.pack("<5f", 1, 2, 3, 4, 70000.0))


def test_the_manifest_names_the_sid_space_not_a_tile_build():
    from pipeline.deliver.release.manifest import manifest
    m = manifest("r-x", {}, "d768d6e135e62b99")
    assert m["format"] == "app-manifest/2" and m["tiles"]["compatible"] == ["d768d6e135e62b99.*"]
    assert m["tiles"]["current"] is None


# ---- on a bundle -------------------------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def rel(request, tmp_path_factory):
    from pipeline.common.curated import GENERATED
    from pipeline.deliver.bundle import read as R
    from pipeline.deliver.store import OUT as STORE
    from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT
    from pipeline.tools.export_ui_rules import OUT as EXPORT
    bundle = need(request, "bundle", Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE), BUNDLE_HINT)
    store = need(request, "bundle", STORE, BUNDLE_HINT)
    si = need(request, "bundle", GENERATED.bundle / "status_index.bin", BUNDLE_HINT)
    exp = need(request, "bundle", EXPORT.parent / "ui-rules-answers.json", EXPORT_HINT).parent
    if not sqlite3.connect(f"file:{bundle}?mode=ro", uri=True).execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'part_geo'").fetchone():
        pytest.fail(f"{bundle} predates P09: point UI_EXPORT_BUNDLE at a bundle with part_geo")
    out = tmp_path_factory.mktemp("release")
    kw = dict(bundle=bundle, store=store, status_index=si, export_dir=exp)
    d = B.build(**kw, out_root=out / "a")
    d2 = B.build(**kw, out_root=out / "b")
    return d, d2, bundle, store, si


def _open(d: Path, name: str, tmp: Path) -> sqlite3.Connection:
    m = json.loads((d / "manifest.json").read_text())
    raw = gzip.decompress((d.parent / m["files"][name]["url"]).read_bytes())
    p = tmp / f"{name}.sqlite"
    p.write_bytes(raw)
    return sqlite3.connect(p)


@pytest.mark.needs_bundle
def test_the_release_holds_only_the_manifests_files_and_every_hash_is_true(rel):
    d = rel[0]
    m = json.loads((d / "manifest.json").read_text())
    listed = {Path(e["url"]).name for e in m["files"].values()}
    assert {p.name for p in d.iterdir()} == listed | {"manifest.json"}
    import hashlib
    for name, e in m["files"].items():
        raw = gzip.decompress((d.parent / e["url"]).read_bytes())
        assert (len(raw), hashlib.sha256(raw).hexdigest()) == (e["bytes"], e["sha256"]), name
        assert e["url"].startswith(m["data_release"] + "/") and "data/generated" not in e["url"]


@pytest.mark.needs_bundle
def test_every_file_is_paired_to_the_bundles_atlas_and_the_card_gate(rel, tmp_path):
    d, _, bundle, store, _ = rel
    handles = dict(sqlite3.connect(f"file:{bundle}?mode=ro", uri=True).execute("SELECT k, v FROM meta"))[
        "section_handles"]
    m = json.loads((d / "manifest.json").read_text())
    for name, e in m["files"].items():
        assert e["section_handles"] == handles, name
        if e["url"].endswith(".sqlite.gz"):
            meta = dict(_open(d, name, tmp_path).execute("SELECT k, v FROM meta"))
            assert (meta["section_handles"], meta["format"]) == (handles, e["format"]), name
    stamp = json.loads(dict(_open(d, "water", tmp_path).execute("SELECT k, v FROM meta"))["card_check"])
    assert stamp["problems"] == 0 and stamp["parts_checked"] > 0
    core, water = _open(d, "core", tmp_path), _open(d, "water", tmp_path)
    by_core = dict(core.execute("SELECT item_id, ord FROM item"))
    strs = dict(water.execute("SELECT id, text FROM str"))
    by_water = {strs[i]: o for o, i in water.execute("SELECT item_ord, item_id FROM item")}
    assert by_water and all(by_core[k] == o for k, o in by_water.items())


@pytest.mark.needs_bundle
def test_paint_reproduces_the_status_index_and_the_flow_tables(rel):
    d, _, bundle, _, si = rel
    m = json.loads((d / "manifest.json").read_text())
    paint = P.decode(gzip.decompress((d.parent / m["files"]["paint"]["url"]).read_bytes()))
    b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    assert paint["status"] == si.read_bytes()
    sp = b.execute("SELECT sid, panel_id, area_km2 FROM section_panel ORDER BY sid").fetchall()
    area = P.decode_area(gzip.decompress((d.parent / m["files"]["flow-area"]["url"]).read_bytes()), len(sp))
    assert [(s, p, a) for (s, p), a in zip(paint["section_panel"], area)] == sp
    assert paint["panel_member"] == [tuple(r[:7]) + (int(r[7] or 0),) for r in b.execute(
        "SELECT panel_id, ord, station, role, area_km2, years, regulated, same_river FROM panel_member "
        "ORDER BY panel_id, ord")]
    assert paint["basin_member"] == b.execute(
        "SELECT basin_id, station, area_km2, years FROM basin_member ORDER BY basin_id, station").fetchall()


@pytest.mark.needs_bundle
def test_water_regions_and_sid_files_reproduce_the_regs_store(rel, tmp_path):
    d, _, _, store, _ = rel
    st = sqlite3.connect(f"file:{store}?mode=ro", uri=True)
    files = {n: _open(d, n, tmp_path) for n in ["water", "regs-sid"] + [f"regs-{r}" for r in SH.REGIONS]}
    tables = [t for (t,) in st.execute("SELECT name FROM sqlite_master WHERE type='table' AND name <> 'meta'")]
    for t in tables:
        want = set(st.execute(f"SELECT * FROM {t}"))
        got = set()
        for f in files.values():
            if f.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone():
                got |= set(f.execute(f"SELECT * FROM {t}"))
        assert got == want, f"{t}: {len(want - got)} store rows missing, {len(got - want)} extra"
    regions = SH.akey_regions(st)
    for r in SH.REGIONS:
        have = {a for (a,) in files[f"regs-{r}"].execute("SELECT DISTINCT akey FROM cell")}
        assert {a for a, rs in regions.items() if r in rs} == have, r


@pytest.mark.needs_bundle
def test_two_builds_write_the_same_bytes_and_the_sizes_hold(rel):
    d, d2 = rel[0], rel[1]
    assert (d / "manifest.json").read_text() == (d2 / "manifest.json").read_text()
    assert B.gate_problems(d) == [], B.sizes(d)
