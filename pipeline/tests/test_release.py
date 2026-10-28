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
                                   discharge_years INTEGER, level_years INTEGER, regulated INTEGER,
                                   same_river INTEGER, lead INTEGER);
        CREATE TABLE basin_member (basin_id TEXT, station TEXT, area_km2 REAL, discharge_years INTEGER,
                                   level_years INTEGER);
        INSERT INTO gauge VALUES ('08MH001'), ('08MH016');
        INSERT INTO section_panel VALUES (3, 7, 1.044), (4, 7, 1.883), (5, 7, 2.659), (9, 2, 64000.5);
        INSERT INTO panel_member VALUES (2, 0, '08MH016', 'down', 4840.015, 66, 12, 0, 1, 1), (7, 0, '08MH001', 'up', 12.5, 3, 0, 1, 0, 0);
        INSERT INTO basin_member VALUES ('ADMS', '08MH001', 70.41000366210938, 33, 7), ('BABL', '08MH016', NULL, 0, 4);
    """)
    # (T54) section access, as pipeline/deliver/bundle/access.py writes it
    from pipeline.deliver.bundle.access import SCHEMA as ACCESS_SCHEMA
    db.executescript(ACCESS_SCHEMA + """
        INSERT INTO access_area VALUES (0, 'no_access', 'Coquitlam Watershed [13480215]', 'Coquitlam Watershed'),
                                       (1, 'no_access', 'Seymour Watershed [678689186]', 'Seymour Watershed'),
                                       (2, 'permit', 'UBC Malcolm Knapp Research Forest [1166294466]',
                                        'UBC Malcolm Knapp Research Forest'),
                                       (3, 'reserve', 'Capilano 5', 'Capilano 5'), (4, 'private', 'parcel', '');
        INSERT INTO section_access VALUES (3, 0, 1000), (4, 0, 400), (4, 1, 300), (4, 4, 1000), (5, 3, 200),
                                          (5, 4, 12), (300, 1, 1000), (301, 2, 999);
    """)
    return db


def test_paint_and_flow_area_round_trip():
    db = _tiny_bundle()
    status = b"BCSI" + bytes(range(20))
    d = P.decode(P.encode(status, db))
    assert d["status"] == status
    assert d["section_panel"] == [(3, 7), (4, 7), (5, 7), (9, 2)]
    # `lead` (paint/2): 08MH016 is its section's own gauge and leads; 08MH001 does not. (T110, paint/4) the record per
    # quantity, discharge then level — distinct values, so a swapped or dropped column fails here
    assert d["panel_member"] == [(2, 0, "08MH016", "down", 4840.015, 66, 12, 0, 1, 1),
                                 (7, 0, "08MH001", "up", 12.5, 3, 0, 1, 0, 0)]
    assert d["basin_member"] == [("ADMS", "08MH001", 70.41000366210938, 33, 7), ("BABL", "08MH016", None, 0, 4)]
    # (T54, paint/3) the no-access rows only, their areas renumbered; the gentler tiers are the `access` file's
    assert d["access"] == {"areas": [("no_access", "Coquitlam Watershed"), ("no_access", "Seymour Watershed")],
                           "rows": [(3, 0, 1000), (4, 0, 400), (4, 1, 300), (300, 1, 1000)]}
    a = P.decode_access(P.encode_access(db, handles="147b20dce7d8576c"))
    assert a == {"handles": "147b20dce7d8576c",
                 "areas": [("permit", "UBC Malcolm Knapp Research Forest"), ("reserve", "Capilano 5"), ("private", "")],
                 "rows": [(4, 2, 1000), (5, 1, 200), (5, 2, 12), (301, 0, 999)]}
    with pytest.raises(ValueError, match="not an access/1 file"):
        P.decode_access(b"BCAX\x02" + bytes(8))
    assert P.decode_area(P.encode_area(db), 4) == [1.044, 1.883, 2.659, 64000.5]
    with pytest.raises(ValueError):
        P.decode_area(P.encode_area(db), 3)          # paired with a paint of other sections: refused


def test_climatology_converts_to_float16_and_refuses_what_does_not_fit():
    rec = bytes([5]) + struct.pack("<5f", 1.0, 2.5, 10.25, 300.0, 1500.0)
    out = B.clim_f16(rec)
    assert len(out) == 11 and out[0] == 5 and struct.unpack("<5e", out[1:]) == (1.0, 2.5, 10.25, 300.0, 1500.0)
    with pytest.raises(SystemExit):
        B.clim_f16(bytes([1]) + struct.pack("<5f", 1, 2, 3, 4, 70000.0))


def test_the_release_copies_water_continue_from_the_bundle_and_refuses_a_bundle_without_it():
    """Audit F07 (owner 2026-10-22 call 6): the continuation links are the bundle's; the release no longer opens the
    atlas graph to make them."""
    db = sqlite3.connect(":memory:")
    with pytest.raises(B.ReleaseError, match="predates the continuation links"):
        B.bundle_continues(db)
    db.execute("CREATE TABLE water_continue (ord INTEGER PRIMARY KEY, part INTEGER NOT NULL, "
               "next_ord INTEGER NOT NULL UNIQUE, next_part INTEGER NOT NULL)")
    db.executemany("INSERT INTO water_continue VALUES (?,?,?,?)", [(9, 0, 4, 2), (3, 1, 7, 0)])
    assert B.bundle_continues(db) == [(3, 1, 7, 0), (9, 0, 4, 2)]


def test_the_release_reads_nothing_past_the_bundle():
    """No atlas path, no graph: the release module neither takes an atlas nor imports the continuation rule."""
    import inspect
    src = inspect.getsource(B)
    assert "graph.pkl" not in src and "pickle" not in src and "continuation as" not in src
    assert "atlas" not in inspect.signature(B.build).parameters


def test_the_manifest_names_the_sid_space_not_a_tile_build():
    from pipeline.deliver.release.manifest import manifest
    m = manifest("r-x", {}, "d768d6e135e62b99")
    assert m["format"] == "app-manifest/2" and m["tiles"]["compatible"] == ["d768d6e135e62b99.*"]
    assert m["tiles"]["current"] is None


# ---- on a bundle -------------------------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def rel(request, tmp_path_factory, live_store_of):
    from pipeline.common.curated import GENERATED
    from pipeline.deliver.bundle import read as R
    from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT
    from pipeline.deliver.export.contract import OUT as EXPORT
    bundle = need(request, "bundle", Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE), BUNDLE_HINT)
    si = need(request, "bundle", GENERATED.bundle / "status_index.bin", BUNDLE_HINT)
    exp = need(request, "bundle", EXPORT.parent / "ui-rules-answers.json", EXPORT_HINT).parent
    if not sqlite3.connect(f"file:{bundle}?mode=ro", uri=True).execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'part_geo'").fetchone():
        pytest.fail(f"{bundle} predates P09: point UI_EXPORT_BUNDLE at a bundle with part_geo")
    out = tmp_path_factory.mktemp("release")
    # no regs.sqlite is kept (P20): the store is made here from the answers + export pair, as `deliver all` does
    store = live_store_of(exp, bundle)
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
    # (T54) paint's access half is exactly the bundle's no-access rows; the access file exactly the gentler tiers
    def rows_of(tiers, got):
        names = [(t, n) for t, n in got["areas"]]
        return [(s, names[a], pm) for s, a, pm in got["rows"]] == [
            (s, (t, n), pm) for s, t, n, pm in b.execute(
                f"SELECT s.sid, a.tier, a.name, s.permille FROM section_access s JOIN access_area a ON a.ix = s.area "
                f"WHERE a.tier IN ({','.join('?' * len(tiers))}) ORDER BY s.sid, s.area", tiers)]
    assert rows_of(P.PAINT_TIERS, paint["access"])
    acc = P.decode_access(gzip.decompress((d.parent / m["files"]["access"]["url"]).read_bytes()))
    assert acc["handles"] == m["files"]["access"]["section_handles"] and rows_of(P.FILE_TIERS, acc)
    assert paint["panel_member"] == [tuple(r[:8]) + (int(r[8] or 0), int(r[9] or 0)) for r in b.execute(
        "SELECT panel_id, ord, station, role, area_km2, discharge_years, level_years, regulated, same_river, lead "
        "FROM panel_member ORDER BY panel_id, ord")]
    assert paint["basin_member"] == b.execute(
        "SELECT basin_id, station, area_km2, discharge_years, level_years FROM basin_member "
        "ORDER BY basin_id, station").fetchall()


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
def test_the_release_is_the_regs_store_put_back_together(rel):
    """P20 (T61 2.4): no reader needs regs.sqlite. `province_store(release)` reassembles the store from water + the nine
    shards + regs-sid, row for row and with its own meta (`store_meta`), so it is held to the store's `store_digest`;
    and one dropped row is refused."""
    from pipeline.deliver.store.common import StoreError, store_digest
    from pipeline.deliver.store.decode import Store
    from pipeline.deliver.store.release import province_store
    d, _, _, store, _ = rel
    want = Store(store)
    got = province_store(d)
    assert got.meta == want.meta and store_digest(got.con) == want.meta["store_digest"]
    assert got.answers_bytes() == want.answers_bytes()
    # mutation: a water file that lost a row (its sha256 re-stated, so only the digest can tell) is refused
    import shutil
    bad = d.parent.parent / "bad" / d.name
    shutil.copytree(d, bad)
    m = json.loads((bad / "manifest.json").read_text())
    f = m["files"]["water"]
    p = bad.parent / f["url"]
    con = sqlite3.connect(":memory:")
    con.deserialize(gzip.decompress(p.read_bytes()))
    assert con.execute("DELETE FROM str WHERE id = (SELECT max(id) FROM str)").rowcount == 1
    con.commit()
    raw = con.serialize()
    import hashlib
    p.write_bytes(gzip.compress(raw))
    f["sha256"] = hashlib.sha256(raw).hexdigest()
    (bad / "manifest.json").write_text(json.dumps(m))
    with pytest.raises(StoreError, match="reassembled"):
        province_store(bad)


@pytest.mark.needs_bundle
def test_two_builds_write_the_same_bytes_and_the_sizes_hold(rel):
    d, d2 = rel[0], rel[1]
    assert (d / "manifest.json").read_text() == (d2 / "manifest.json").read_text()
    assert B.gate_problems(d) == [], B.sizes(d)


@pytest.mark.needs_bundle
def test_a_built_release_is_exactly_the_contract(rel, tmp_path):
    from pipeline.deliver.release.contract import FILES, OPTIONAL
    d = rel[0]
    m = json.loads((d / "manifest.json").read_text())
    # every file but the optional ones; `lakes` is prepared, not fed (owner 2026-10-21), so this release has none
    assert set(m["files"]) == set(FILES) - OPTIONAL
    for name, spec in ((n, s) for n, s in FILES.items() if n in m["files"]):
        assert m["files"][name]["format"] == spec["format"], name
        if spec["kind"] == "sqlite":
            con = _open(d, name, tmp_path)
            got = {t: [r[1] for r in con.execute(f"PRAGMA table_info({t})")]
                   for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            assert got == spec["tables"], name


@pytest.mark.needs_bundle
def test_the_release_ships_the_bundles_continuation_links_as_they_are(rel, tmp_path):
    """`water_continue` is MADE by the bundle (`bundle/continuation.py`; the Chilliwack / Vedder and the reviewed list
    are pinned on the bundle in `test_water_continue.py`) and COPIED here: the release reads nothing past the bundle
    (audit F07 — it used to unpickle the atlas graph for this)."""
    d, _, bundle, _, _ = rel
    w = _open(d, "water", tmp_path)
    b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    q = "SELECT ord, part, next_ord, next_part FROM water_continue ORDER BY ord"
    assert w.execute(q).fetchall() == b.execute(q).fetchall() != []


@pytest.mark.needs_bundle
def test_every_cores_effective_key_is_the_search_key(rel, tmp_path):
    """T114, keys stored thin (`stored_key`): over EVERY item and alias row of the built core, what the app's SEARCH
    reads — `COALESCE(name_key, lower(name))` (`@app/data` `NAME_KEY` / `ALIAS_KEY`) — is `search_key` of the text,
    the twin of `@app/core` `searchKey`. One rule, however the key is stored."""
    from pipeline.common.search_key import search_key
    con = _open(rel[0], "core", tmp_path)
    for table, text, key in (("item", "name", "name_key"), ("alias", "alias", "alias_key")):
        rows = con.execute(f"SELECT {text}, COALESCE({key}, lower({text})), {key} IS NULL FROM {table}").fetchall()
        assert rows, table
        bad = [(t, k) for t, k, _ in rows if k != search_key(t)]
        assert not bad, f"{table}: {len(bad)} rows read a key that is not search_key: {bad[:5]}"
        # the thinning is what keeps core under its gate: most rows store none
        assert sum(n for *_, n in rows) > len(rows) // 2, table


def test_the_apps_pack_contract_is_current():
    from pipeline.tools.emit import pack as emit_pack
    assert emit_pack.OUT.read_text() == emit_pack.render(), "run: python -m pipeline.tools.emit.pack"


def test_the_web_budget_prices_each_moment_and_fails_a_crossed_gate(tmp_path):
    """(T129) Each moment is priced from the manifest alone — gzip bytes, the manifest's own size, the mask as sent, the
    LARGEST region for `regs-{region}` — and a release whose moment crosses its gate fails the release step. Mutation:
    one region file grown past the tap gate's headroom fails `first_tap` (and the unnamed tap), nothing else."""
    from pipeline.deliver.release import contract as C
    names = {f for spec in C.WEB_BUDGET.values() for f in spec["files"]} - {"manifest", "mask", "regs-{region}"}
    files = {n: {"bytes": 1, "gz": 1000} for n in names} | {f"regs-{r}": {"bytes": 1, "gz": 100 + i}
                                                            for i, r in enumerate(SH.REGIONS)}
    m = {"files": files, "tiles": {"mask": {"bytes": 62}}}
    (tmp_path / "manifest.json").write_text(json.dumps(m))
    size = (tmp_path / "manifest.json").stat().st_size
    got = B.web_moments(tmp_path)
    assert got["first_paint"] == (size + 1000 + 62, ["manifest", "paint", "mask"])
    assert got["first_tap"][1][-1] == "regs-8" and got["first_tap"][0] == 4 * 1000 + 108
    assert not [p for p in B.gate_problems(tmp_path) if p.startswith("web ")]
    files["regs-3"]["gz"] = C.WEB_BUDGET["first_tap_unnamed"]["gate"]
    (tmp_path / "manifest.json").write_text(json.dumps(m))
    assert sorted(p.split(":")[0] for p in B.gate_problems(tmp_path) if p.startswith("web ")) == \
        ["web cold_load", "web first_tap", "web first_tap_unnamed"]


def test_t182_every_file_has_its_own_gate_and_the_total_is_the_phones(tmp_path):
    """(T182) The web fetches each file lazily, so every file the contract names carries its own gzip gate (a new file
    states its size when it is added); a manifest naming an ungated file fails the release. The release total is the
    offline backstop (what a phone holds), not the web's cap — that is `WEB_BUDGET["cold_load"]` (first paint + the idle
    prefetch). Mutations: an ungated file, a file over its gate, and the total over the offline gate each fail alone."""
    from pipeline.deliver.release import contract as C
    assert set(C.FILES) <= set(B.SIZE_GATES), sorted(set(C.FILES) - set(B.SIZE_GATES))
    assert C.WEB_BUDGET["cold_load"]["files"] == C.WEB_BUDGET["first_paint"]["files"] + \
        ["core", "regs-{region}", "salmon", "method"]
    assert C.WEB_BUDGET["cold_load"]["gate"] < B.OFFLINE_GATE

    def problems(files):
        (tmp_path / "manifest.json").write_text(json.dumps({"files": files, "tiles": {"mask": {"bytes": 1}}}))
        return [p for p in B.gate_problems(tmp_path) if not p.startswith("web ")]
    ok = {k: {"bytes": 1, "gz": 1} for k in C.FILES}
    assert problems(ok) == []
    assert problems(ok | {"runs": {"bytes": 1, "gz": 1}}) == ["runs: no size gate (build.SIZE_GATES)"]
    assert problems(ok | {"water": {"bytes": 1, "gz": B.SIZE_GATES["water"] + 1}}) == \
        [f"water: {B.SIZE_GATES['water'] + 1:,} B gzip > {B.SIZE_GATES['water']:,}"]
    # the total alone: gated files each under their gate, together over the phone's
    big = {k: {"bytes": 1, "gz": B.SIZE_GATES[k]} for k in C.FILES}
    assert sum(v["gz"] for v in big.values()) > B.OFFLINE_GATE
    assert [p.split(":")[0] for p in problems(big)] == ["offline total"]


def test_t182_each_moment_fetches_only_the_files_holding_what_it_reads():
    """(T182) The tables T129 found riding in a file the moment did not otherwise need have files of their own: an
    unnamed tap reads `down` (section_down), not conditions; Conditions opens conditions + flow-area (the lake road is
    conditions' own `lake_section`), not core and not the climatology; the first search reads core alone. Pinned on the
    contract, so a table moved back fails here before a release is cut."""
    from pipeline.deliver.release import contract as C
    where = {t: f for f, spec in C.FILES.items() for t in spec["tables"] if t != "meta"}
    assert (where["section_down"], where["gauge_clim"], where["part_geo"], where["lake_section"], where["item_near"]) \
        == ("down", "climate", "geo", "conditions", "core")
    assert "conditions" not in C.WEB_BUDGET["first_tap_unnamed"]["files"]
    assert C.WEB_BUDGET["conditions_open"]["files"] == ["flow-area", "conditions"]
    assert C.WEB_BUDGET["first_search"]["files"] == ["core"]
    assert set(B.FORMATS) == set(C.FILES)


def _rings(con, rows, places):
    con.executescript("CREATE TABLE b.place_water (place_id INTEGER, ckm INTEGER, ord INTEGER);"
                      "CREATE TABLE main.place (place_id INTEGER PRIMARY KEY, osm TEXT, name TEXT, kind TEXT, pop INTEGER,"
                      " lat REAL, lon REAL);")
    con.executemany("INSERT INTO b.place_water VALUES (?, ?, ?)", rows)
    con.executemany("INSERT INTO main.place VALUES (?, '', ?, ?, NULL, ?, 0)", places)


def test_t182_item_near_ships_the_town_the_search_row_names_and_its_exact_ties():
    """`core_near`: per water, the ring towns at the smallest distance + kind handicap, in exact hundredths of a km —
    the one(s) `describeBy` can name. Fernie (a city 4.4 km off) beats the locality on the bank (0.1 km + 6); two
    localities both on the water tie and both ship (the app breaks the tie by name); a town with no position is never
    one; an unknown kind weighs the unknown handicap."""
    con = sqlite3.connect(":memory:")
    con.execute("ATTACH ':memory:' AS b")
    _rings(con, [(1, 440, 10), (2, 10, 10),            # water 10: Fernie (city) 4.4 km, Mosquito Flats (locality) 0.1
                 (3, 0, 20), (4, 0, 20), (1, 900, 20),  # water 20: two localities ON it, Fernie 9 km (9 > 0 + 6? no: 9)
                 (5, 0, 30), (6, 700, 30),              # water 30: the nearest has no position; a hamlet 7 km
                 (7, 100, 40), (2, 650, 40)],           # water 40: an unknown kind 1 km (+6 = 7) vs a locality 6.5 (12.5)
           [(1, "Fernie", "city", 49.5), (2, "Mosquito Flats", "locality", 49.5), (3, "Bravo", "locality", 50.0),
            (4, "Alpha", "locality", 50.0), (5, "Nowhere", "town", None), (6, "Elko", "hamlet", 49.3),
            (7, "Odd", "camp", 49.1)])
    B.core_near(con)
    assert con.execute("SELECT ord, place_id, ckm FROM item_near ORDER BY ord, place_id").fetchall() == [
        (10, 1, 440), (20, 3, 0), (20, 4, 0), (30, 6, 700), (40, 7, 100)]
    # MUTATION: the nearest town alone (no handicap) names Mosquito Flats for the Elk — the pin above tells them apart
    assert con.execute("SELECT place_id FROM b.place_water WHERE ord = 10 ORDER BY ckm LIMIT 1").fetchone() == (2,)
    from pipeline.deliver.release import contract as C
    assert max(C.NEAR_HANDICAP_KM.values()) <= C.NEAR_HANDICAP_UNKNOWN_KM


def test_t182_lake_section_is_the_lowest_station_over_every_lake_holding_the_section():
    con = sqlite3.connect(":memory:")
    con.execute("ATTACH ':memory:' AS b")
    con.executescript("""
        CREATE TABLE b.item (ord INTEGER PRIMARY KEY, item_id TEXT);
        CREATE TABLE b.item_section (ord INTEGER, sid INTEGER);
        CREATE TABLE b.lake_gauge (item_id TEXT, station TEXT);
        INSERT INTO b.item VALUES (1, 'wbk:1'), (2, 'wbk:2'), (3, 'gnis:3');
        INSERT INTO b.item_section VALUES (1, 100), (1, 101), (2, 101), (3, 102);
        INSERT INTO b.lake_gauge VALUES ('wbk:1', '08MH103'), ('wbk:1', '08MH090'), ('wbk:2', '08MH001');
    """)
    B.lake_section(con)
    # 100: lake 1's lowest; 101: in lakes 1 and 2, the lowest of both; 102: no gauged lake, no row
    assert con.execute("SELECT sid, station FROM lake_section ORDER BY sid").fetchall() == [
        (100, "08MH090"), (101, "08MH001")]


def test_t182_geo_is_part_geo_in_integer_microdegrees():
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE part_geo (ord INTEGER, part_ix INTEGER, rank INTEGER, mid_sid INTEGER, mid_lon REAL, "
                "mid_lat REAL, w REAL, s REAL, e REAL, n REAL)")
    con.execute("INSERT INTO part_geo VALUES (11837, 0, 0, 7, -129.984551, 53.344782, -130.043061, 53.333981, "
                "-129.902673, 53.361987)")
    B.geo_e6(con)
    assert con.execute("SELECT * FROM part_geo").fetchall() == [
        (11837, 0, 0, 7, -129984551, 53344782, -130043061, 53333981, -129902673, 53361987)]


def test_t204_geo_carries_part_stretch_in_integer_microdegrees():
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE part_geo (ord INTEGER, part_ix INTEGER, rank INTEGER, mid_sid INTEGER, mid_lon REAL, "
                "mid_lat REAL, w REAL, s REAL, e REAL, n REAL)")
    con.execute("CREATE TABLE part_stretch (ord INTEGER, part_ix INTEGER, stretch_ix INTEGER, rank INTEGER, "
                "mid_sid INTEGER, mid_lon REAL, mid_lat REAL, w REAL, s REAL, e REAL, n REAL, sids TEXT)")
    con.execute("INSERT INTO part_stretch VALUES (455, 0, 1, 2, 9, -122.5, 49.2, -122.6, 49.1, -122.4, 49.3, '[9, 10]')")
    B.geo_e6(con)
    assert con.execute("SELECT * FROM part_stretch").fetchall() == [
        (455, 0, 1, 2, 9, -122500000, 49200000, -122600000, 49100000, -122400000, 49300000, "[9, 10]")]


# ---- the app's paint decoder reads what this encoder writes (core/src/paint.ts) --------------------------------------
APP_PAINT_FIXTURE = Path(__file__).resolve().parents[2] / "app/packages/core/src/paint.fixture.ts"
_PAINT_HEAD = "// GENERATED by pipeline/tests/test_release.py — do not edit.\nexport const PAINT_FIXTURE = "


def app_paint_fixture() -> dict:
    """The tiny paint (with a real status index) + flow-area, and what the PYTHON decoders say they hold."""
    import base64
    from pipeline.deliver import status_index as SI
    from pipeline.tests.test_status_index import _tiny
    db = _tiny_bundle()
    status = SI.encode(_tiny())
    paint, area = P.encode(status, db), P.encode_area(db)
    access = P.encode_access(db, handles=_tiny()["handles"])
    d, a = P.decode(paint), P.decode_access(access)
    return {"paint": base64.b64encode(paint).decode(), "area": base64.b64encode(area).decode(),
            "handles": _tiny()["handles"], "reach": _tiny()["reach_digest"],
            "stations": d["stations"], "section_panel": [list(x) for x in d["section_panel"]],
            "areas": P.decode_area(area, len(d["section_panel"])),
            "panel_member": [list(x) for x in d["panel_member"]], "basin_member": [list(x) for x in d["basin_member"]],
            # (T54) the access halves: paint's (no access) and the `access` file's (the gentler tiers)
            "access_areas": [list(x) for x in d["access"]["areas"]],
            "access_rows": [list(x) for x in d["access"]["rows"]],
            "access_file": base64.b64encode(access).decode(),
            "file_areas": [list(x) for x in a["areas"]], "file_rows": [list(x) for x in a["rows"]]}


def test_the_apps_paint_fixture_is_current():
    want = app_paint_fixture()
    if os.environ.get("UPDATE_APP_FIXTURE"):
        APP_PAINT_FIXTURE.write_text(_PAINT_HEAD + json.dumps(want) + ";\n")
    text = APP_PAINT_FIXTURE.read_text()
    assert text.startswith(_PAINT_HEAD) and json.loads(text[len(_PAINT_HEAD):].rstrip().rstrip(";")) == want, \
        "run UPDATE_APP_FIXTURE=1 pytest pipeline/tests/test_release.py -k paint_fixture"
