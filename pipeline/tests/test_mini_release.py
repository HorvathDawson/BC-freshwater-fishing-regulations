"""T667 slice 8: the mini release's INPUTS (an invented FWA-shaped GeoPackage + coast pair, from the committed
`pipeline/tests/fixtures/mini/world.json`) and the atlas build's input flags it needs (`--gpkg` reaching the area
layers and the drainage areas, `--coast-gpkg`). No live data: every gpkg here is written into `tmp_path`."""
from __future__ import annotations

import argparse
import json

import pyogrio
import pytest

from pipeline.tools import mini_release as M


@pytest.fixture(scope="module")
def inputs(tmp_path_factory):
    return M.write_inputs(tmp_path_factory.mktemp("mini-inputs"))


def test_the_world_is_invented_and_its_ids_sit_in_their_own_ranges():
    w = M.load_world()
    assert w["format"] == M.FORMAT
    assert all(s["blk"] >= 900_000_001 for s in w["streams"])
    assert all(b["wbk"] >= 800_000_001 for b in w["waterbodies"])
    gnis = [x["gnis_id"] for x in w["streams"] + w["waterbodies"] if x.get("gnis_id")]
    assert gnis and all(g >= 990_001 for g in gnis)


def test_a_break_outside_its_line_is_refused(tmp_path):
    w = json.loads(M.WORLD.read_text(encoding="utf-8"))
    w["streams"][0]["breaks"] = [5000, 99_000]
    p = tmp_path / "world.json"
    p.write_text(json.dumps(w))
    with pytest.raises(ValueError, match="breaks must rise strictly inside"):
        M.load_world(p)
    w["streams"][0]["breaks"] = [5000, 10000]                      # the lake run's ends are no longer breaks
    p.write_text(json.dumps(w))
    with pytest.raises(ValueError, match="must start and end on a break"):
        M.load_world(p)


def test_codes_are_fwa_width():
    w = M.load_world()
    assert M.full_wsc(w, "").startswith("920-123456-000000") and len(M.full_wsc(w, "").split("-")) == 21
    assert M.full_wsc(w, "222222-500000").split("-")[:4] == ["920", "123456", "222222", "500000"]
    assert M.full_wsc(w, None).startswith("999-999999")
    # (T1250) a stream's own root in another drainage: Northward Creek is the Mackenzie's (200), not the world's
    assert M.full_wsc(w, "", "200-990006").split("-")[:3] == ["200", "990006", "000000"]
    assert M.full_wsc(w, "500000", "200-990006").split("-")[:3] == ["200", "990006", "500000"]
    assert len(M.full_wsc(w, "", "200-990006").split("-")) == 21
    north = next(s for s in w["streams"] if s["name"] == "Northward Creek")
    assert M.full_wsc(w, north["wsc"], north.get("wsc_root")).startswith("200-990006")


def test_the_gpkg_carries_every_layer_and_column_the_atlas_reads(inputs):
    from pipeline.atlas.graph.blk_chains import _STREAM_COLUMNS
    fwa, coast = inputs
    layers = {n for n, _ in pyogrio.list_layers(fwa)}
    assert {"streams", "lakes", "wetlands", "manmade", "watersheds", "wmu", "tidal_boundary", "pfma_areas"} <= layers
    assert set(_STREAM_COLUMNS) <= set(pyogrio.read_info(fwa, layer="streams")["fields"])
    assert {"WILDLIFE_MGMT_UNIT_ID", "REGION_RESPONSIBLE_ID"} <= set(pyogrio.read_info(fwa, layer="wmu")["fields"])
    assert {n for n, _ in pyogrio.list_layers(coast)} == {"land", "rivers"}
    assert "BLUE_LINE_KEY" in pyogrio.read_info(coast, layer="rivers")["fields"]
    assert fwa.stat().st_size < 1_000_000 and coast.stat().st_size < 1_000_000


def test_the_streams_read_back_as_fwa_features_mouth_first(inputs):
    from pipeline.atlas.graph.blk_chains import load_stream_fids
    rows = load_stream_fids(str(inputs[0]))
    river = sorted((r for r in rows if r.blk == "900000001"), key=lambda r: r.down_m)
    assert [round(r.down_m) for r in river] == [0, 3000, 5000, 6000, 11000, 16000, 19000, 25000, 26000, 27000, 31000,
                                                33000, 37000]       # 3000 / 5000: the creek-less braid's ends (T1242)
    assert river[-1].up_m == pytest.approx(46000)                      # its mouth 1 km out in the sea (the tidal mouth)
    assert [r.wbk for r in river if r.wbk] == ["800000001", "800000006", "800000006"]   # the two lake runs
    mock = next(r for r in rows if r.blk == "900000002")
    assert mock.down_node == river[4].down_node                        # the confluence is a shared vertex
    assert mock.wsc == "920-123456-222222"


def test_area_polygons_come_from_the_builds_own_gpkg(inputs, tmp_path):
    """`load_area_polys` read the CONFIGURED gpkg whatever the build was given: the mini atlas got the province's
    regions. Now it reads the accessor's file — and clips to THAT file's outline (the mini world sits inside the real
    province, so only the mini outline cuts this watershed)."""
    from pipeline.atlas.fwa import FWADataAccessor
    from pipeline.atlas.splits.area_splits import load_area_polys
    w = json.loads(M.WORLD.read_text(encoding="utf-8"))
    w["watersheds"][0]["box"] = [0, -10000, 60000, 10000]              # 10 km past the mini outline (x = 50 km)
    fwa, _ = M.write_inputs(tmp_path / "wide", w)
    acc = FWADataAccessor(fwa)
    regions = load_area_polys(acc, {"id": "regions", "layer": "wmu", "name_field": "REGION_RESPONSIBLE_ID",
                                    "which": "all"})
    assert sorted(regions) == ["1", "2"]
    ws = load_area_polys(acc, {"id": "ws", "layer": "watersheds", "name_field": "GNIS_NAME", "which": "all"})
    minx, _, maxx, _ = ws["Fixture River"].bounds
    assert (maxx - minx) == pytest.approx(50000)                       # clipped to the mini outline, not the live one


def test_the_drainage_areas_read_the_gpkg_the_registry_is_given(monkeypatch, inputs):
    from pipeline.atlas.registry import build_registry
    from pipeline.atlas.splits import pfma_drainage
    from pipeline.common.models import StreamGraph
    seen = []
    monkeypatch.setattr(pfma_drainage, "basin_areas", lambda areas, gpkg, **kw: seen.append(gpkg) or {})
    build_registry(StreamGraph(), pinned={}, gpkg=str(inputs[0]))
    build_registry(StreamGraph(), pinned={})
    assert seen == [str(inputs[0]), pfma_drainage._GPKG]               # the default is the configured file, as before


def test_the_mini_basin_drains_to_its_invented_area(inputs):
    from pipeline.atlas.splits import pfma_drainage
    assert pfma_drainage.basin_areas(pfma_drainage.MINTED_AREAS, str(inputs[0]), city_mouths=()) == {"920-123456-": 29}


def test_the_atlas_input_flags_default_to_the_configured_files():
    from pipeline.atlas import build
    from pipeline.common.curated import SOURCE
    a = build._parse_args(["--full"])
    assert a.gpkg == str(SOURCE.fwa_gpkg) and a.coast_gpkg == str(SOURCE.coast_gpkg)
    b = build._parse_args(["--full", "--gpkg", "/x/fwa.gpkg", "--coast-gpkg", "/x/coast.gpkg"])
    assert (b.gpkg, b.coast_gpkg) == ("/x/fwa.gpkg", "/x/coast.gpkg")
    assert isinstance(b, argparse.Namespace)


def test_a_digest_ignores_the_clock_and_the_scratch_path(tmp_path):
    a = {"build_ts": "2026-11-14T10:00:00", "input_hashes": {"source:fwa_gpkg": "stamp:1:2"}, "n": 1}
    b = {"build_ts": "2026-11-14T11:00:00", "input_hashes": {"source:fwa_gpkg": "stamp:1:9"}, "n": 1}
    na = M._normalised("atlas/graph.pkl.meta.json", json.dumps(a).encode(), tmp_path)
    nb = M._normalised("atlas/graph.pkl.meta.json", json.dumps(b).encode(), tmp_path)
    assert na == nb
    nc = M._normalised("atlas/graph.pkl.meta.json", json.dumps(dict(b, n=2)).encode(), tmp_path)
    assert nc != nb                                                    # a real difference still shows
    # (T1223) the source files by content: the scratch gpkg's bytes move each run; a curated digest still shows
    c = {"input_hashes": {"source:fwa_gpkg": "sha256:" + "1" * 64, "source:fwa_coast": "sha256:" + "2" * 64,
                          "curated:waters.areas": "sha256:" + "3" * 64}}
    d = {"input_hashes": {"source:fwa_gpkg": "sha256:" + "4" * 64, "source:fwa_coast": "sha256:" + "5" * 64,
                          "curated:waters.areas": "sha256:" + "3" * 64}}
    nd = M._normalised("atlas/graph.pkl.meta.json", json.dumps(d).encode(), tmp_path)
    assert M._normalised("atlas/graph.pkl.meta.json", json.dumps(c).encode(), tmp_path) == nd
    d["input_hashes"]["curated:waters.areas"] = "sha256:" + "6" * 64
    assert M._normalised("atlas/graph.pkl.meta.json", json.dumps(d).encode(), tmp_path) != nd


def test_the_build_refuses_a_directory_inside_the_fixture_or_the_data_tree():
    with pytest.raises(SystemExit, match="scratch only"):
        M.build(M.FIXTURE / "out")


def test_a_gpkg_is_compared_by_its_features_not_its_pages(tmp_path):
    a, _ = M.write_inputs(tmp_path / "a")
    b, _ = M.write_inputs(tmp_path / "b")
    assert M._gpkg_content(a) == M._gpkg_content(b)
    w = json.loads(M.WORLD.read_text(encoding="utf-8"))
    w["streams"][0]["z"] = [2, 401]                                    # one vertex height moved
    c, _ = M.write_inputs(tmp_path / "c", w)
    assert M._gpkg_content(c) != M._gpkg_content(a)


def test_compare_names_the_file_that_differs(capsys):
    assert M._compare({"x": "1", "y": "2"}, {"x": "1", "y": "2"}) == 0
    assert M._compare({"x": "1", "y": "2"}, {"x": "1", "y": "3"}) == 1
    assert "y: 2 vs 3" in capsys.readouterr().out


def test_the_fed_inputs_are_written_from_the_world_alone(tmp_path):
    """`write_source`: the roster, places, outline, synopsis, HYDAT and the feeds come from `world.json`; the one gauge
    stands where the mini tree's frozen match says it does, and the synopsis states its period."""
    from pipeline.common.curated import SOURCE
    from pipeline.common.synopsis_pages import synopsis_period
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    from pipeline.regs.dfo_salmon.typed import FORMAT
    w = M.load_world()
    fwa, _ = M.write_inputs(tmp_path / "in")
    got = M.write_source(tmp_path / "fed", fwa)
    roster = json.loads((got["source"] / "bc_hydrometric_stations.json").read_text())
    match = json.loads((M.CURATED_TREE / "gauges" / "matches.json").read_text())["stations"]
    assert [s["station"] for s in roster] == sorted(match) == ["99MA001"]
    assert (roster[0]["lon"], roster[0]["lat"]) == (match["99MA001"]["lon"], match["99MA001"]["lat"]) \
        == M.lonlat(w, *w["gauges"][0]["at"])
    assert tuple(synopsis_period(SOURCE.under(got["source"]).synopsis_pdf)) == ("2025-04-01", "2027-03-31")
    clim = json.loads(got["clim"].read_text())
    assert set(clim["stations"]) == set(clim["stats"]) == {"99MA001"}
    typed = sorted(p.name for p in got["dfo_typed"].glob("region-*.json"))
    assert typed == sorted(f"region-{s}.json" for s in ALL_SLUGS if not PAGES[s].is_stub)
    assert all(json.loads(p.read_text())["typed_format"] == FORMAT for p in got["dfo_typed"].glob("*.json"))


def test_a_build_that_writes_a_live_path_is_refused(tmp_path, monkeypatch):
    """The watch `build` keeps on the live directories a deliver step writes by default: a stage that touches one
    stops the build, naming the path."""
    watched = tmp_path / "live"
    watched.mkdir()
    monkeypatch.setattr(M, "watched_dirs", lambda: [watched])
    monkeypatch.setattr(M, "write_inputs", lambda d: (d / "fwa.gpkg", d / "coast.gpkg"))
    monkeypatch.setattr(M, "write_source", lambda d, fwa: {})
    import sys
    touch = [sys.executable, "-c", f"open({str(watched / 'x')!r}, 'w').write('1')"]
    monkeypatch.setattr(M, "stage_commands", lambda out, fwa, coast: [("toucher", touch)])
    with pytest.raises(SystemExit, match=r"changed 1 live path"):
        M.build(tmp_path / "out")
    monkeypatch.setattr(M, "stage_commands", lambda out, fwa, coast: [("quiet", [sys.executable, "-c", "1"])])
    assert set(M.build(tmp_path / "out2")) == {"inputs", "quiet"}


@pytest.fixture(scope="module")
def chain(tmp_path_factory):
    """The mini chain built twice (a, b), once for this file."""
    root = tmp_path_factory.mktemp("mini-chain")
    return root, M.build(root / "a"), M.build(root / "b")


def test_the_mini_chain_builds_twice_alike_on_invented_inputs_only(chain):
    """THE CHAIN on the mini world (atlas -> reach -> tile layers -> bundle ... -> release), twice, into
    tmp: every stage's own command, every input invented (on CI there is no live tree at all, so a stage that reached
    one fails here), no live directory changed (`build` refuses), and the two runs' digests equal (AGENTS 19)."""
    root, a, _ = chain
    assert list(a) == ["inputs"] + [n for n, _ in M.stage_commands(root, root, root)]
    da, db = M.digests(root / "a"), M.digests(root / "b")
    assert da == db
    assert {"deliver/bundle.sqlite", "deliver/verdicts.sqlite", "deliver/status_index.bin",
            "tiles/atlas.meta.json", "tiles/layers/stream.geojsonl", "reaches/report.json"} <= set(da)
    assert {"deliver/ui-rules-export.json", "deliver/ui-rules-answers.json", "release/LATEST"} <= set(da)
    rel = [k for k in da if k.startswith("release/r-") and k.endswith("/manifest.json")]
    assert len(rel) == 1                                                  # one release, the same id both runs


def test_the_export_holds_the_mini_corpus_to_its_own_claims(chain, tmp_path, monkeypatch):
    """The mini claims bite as the book's do: a mechanism the corpus has no case of, or an example the reader
    contradicts, refuses the export (the shipped claims pass: the chain above built)."""
    from pipeline.deliver.export import __main__ as X
    from pipeline.regs.inseason import matches as _M
    # (T978) in process, the export reads the mini tree's notices as the chain's own export does (CURATED_ROOT), never
    # the machine's (whose generated resolution CI does not have)
    monkeypatch.setattr(_M, "default_path", lambda: M.CURATED_TREE / "inseason" / "notice_waters.json")
    out = chain[0] / "a"
    claims = json.loads((out / "fed" / "claims.json").read_text())
    for change, said in ((lambda c: c["mechanisms"].append("two_regions_closure"), "no real case of two_regions_closure"),
                         (lambda c: c["examples"][0].update(silent=c["examples"][0]["speaks"]), "is not silent"),
                         (lambda c: c["duties"].remove("protected_fish"), None)):
        c = json.loads(json.dumps(claims))
        change(c)
        p = out / "fed" / "claims-planted.json"
        p.write_text(json.dumps(c))
        rc = X.main(["--bundle", str(out / "deliver" / "bundle.sqlite"), "--out", str(tmp_path / "e.json"),
                     "--claims", str(p)])
        assert rc == (0 if said is None else 1), change


def test_check_and_write_compare_and_replace_the_copy_whole(chain, tmp_path, monkeypatch, capsys):
    built = chain[0] / "a" / "release"
    monkeypatch.setattr(M, "_fresh_release", lambda scratch: built)
    dest = tmp_path / "mini-release"
    assert M.write(dest, tmp_path / "s") == 0 and M.check(dest, tmp_path / "s") == 0
    manifest = next(dest.glob("r-*/manifest.json"))
    text = manifest.read_text()
    manifest.write_text(text + " ")                                   # bytes only: the same content
    assert M.check(dest, tmp_path / "s") == 0
    manifest.write_text(text.replace('"valid_until": "2027-03-31"', '"valid_until": "2027-04-01"'))
    assert M.check(dest, tmp_path / "s") == 1 and "manifest.json" in capsys.readouterr().out
    manifest.write_text(text)
    import gzip
    core = next(dest.glob("r-*/core-*.sqlite.gz"))                    # one row of a data file changed
    db = tmp_path / "core.sqlite"
    db.write_bytes(gzip.decompress(core.read_bytes()))
    import sqlite3
    con = sqlite3.connect(db)
    t = next(n for (n,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
             if con.execute(f'SELECT COUNT(*) FROM "{n}"').fetchone()[0])
    first = con.execute(f'SELECT * FROM "{t}" LIMIT 1').fetchone()                    # WITHOUT ROWID tables too
    cols = [r[1] for r in con.execute(f'PRAGMA table_info("{t}")')]
    con.execute(f'DELETE FROM "{t}" WHERE ' + " AND ".join(f'"{c}" IS ?' for c in cols), first)
    con.commit()
    con.close()
    core.write_bytes(gzip.compress(db.read_bytes(), mtime=0))
    assert M.check(dest, tmp_path / "s") == 1 and core.name.split("-")[0] in capsys.readouterr().out
    (dest / "r-000000000001").mkdir()
    with pytest.raises(SystemExit, match="beside the release"):          # a folder with no manifest
        M.check(dest, tmp_path / "s")


def test_a_column_the_writer_adds_without_a_re_cut_fails_the_check_naming_the_re_cut(chain, tmp_path, monkeypatch,
                                                                                      capsys):
    """(T768 / T993 item 10; 0.8.5 stop 1, 0.8.4 stop 4) A lane whose code adds a column to a release file (T985's
    `water_exit` lon/lat, additive within water/3) and commits without re-cutting the mini release: the rebuild holds the
    column, the committed copy does not, and `--check` — fast-fail's mini chain on any pipeline source change, and
    pipeline-ci's step, which `scripts/before_push.py` runs before a push — fails naming the file and the re-cut
    command. A column with no value in any row is still a difference: on a table with rows every row grows a NULL, and
    on an empty table (`water_continue` in the mini world) only the schema says so, which the content digest compares."""
    import gzip
    import shutil
    import sqlite3
    built = chain[0] / "a" / "release"
    dest = tmp_path / "committed"
    monkeypatch.setattr(M, "_fresh_release", lambda scratch: built)
    assert M.write(dest, tmp_path / "s") == 0
    capsys.readouterr()

    def grown_by(table: str, rows: bool) -> Path:
        grown = tmp_path / f"grown-{table}"
        shutil.copytree(built, grown)
        man = json.loads(next(grown.glob("r-*/manifest.json")).read_text())
        water = grown / man["files"]["water"]["url"]                   # the whole province's water file
        db = tmp_path / f"{table}.sqlite"
        db.write_bytes(gzip.decompress(water.read_bytes()))
        con = sqlite3.connect(db)
        assert bool(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) is rows, table
        con.execute(f"ALTER TABLE {table} ADD COLUMN planted INTEGER")
        con.commit()
        con.close()
        water.write_bytes(gzip.compress(db.read_bytes(), mtime=0))
        return grown

    # (review) the schema alone: an empty table's new column changes no row
    monkeypatch.setattr(M, "_fresh_release", lambda scratch: grown_by("water_continue", rows=False))
    assert M.check(dest, tmp_path / "s") == 1
    assert "\n  water: committed " in capsys.readouterr().out
    grown = grown_by("water_exit", rows=True)
    monkeypatch.setattr(M, "_fresh_release", lambda scratch: grown)
    assert M.check(dest, tmp_path / "s") == 1
    out = capsys.readouterr().out
    assert "is STALE — 1 file(s) differ" in out and "\n  water: committed " in out
    assert f"re-cut it: python -m pipeline.tools.mini_release --write --release-dir {dest}" in out
    # the committed copy itself (no --release-dir): the bare command, the one pipeline-ci's annotation names
    monkeypatch.setattr(M, "COMMITTED", dest)
    assert M.check(dest, tmp_path / "s") == 1
    assert capsys.readouterr().out.rstrip().endswith("re-cut it: python -m pipeline.tools.mini_release --write")


def test_the_mini_world_holds_the_plans_inventory(chain):
    """THE INVENTORY (ticket T667 "Slice 8 plan"), each mechanism read off what the chain BUILT (never off the inputs),
    by name, so the world cannot quietly shrink. The plan's "a lake cut into parts" has no live counterpart (every lake
    of the live bundle is one part; lakes are never cut) and is not here: Twofold Lake is the two-region lake."""
    import sqlite3
    out = chain[0] / "a"
    reg = {i["id"]: i for i in json.loads((out / "atlas" / "registry.json").read_text())["items"]}
    db = sqlite3.connect(f"file:{out / 'deliver' / 'bundle.sqlite'}?mode=ro", uri=True)
    q = lambda sql, *a: db.execute(sql, a).fetchall()                         # noqa: E731
    secs = lambda item: set(reg[item]["section_ids"])                           # noqa: E731
    in_region = lambda r: secs(f"area:region:{r}")                              # noqa: E731

    def bound(entry_id, rule_id):
        return {s for (s,) in q("SELECT DISTINCT sr.sid FROM section_ruleset sr JOIN ruleset r ON r.set_id = sr.set_id "
                                "WHERE r.entry_id = ? AND r.rule_id = ?", entry_id, rule_id)}

    def parts(item):
        return q("SELECT p.part_ix, p.set_id FROM part p JOIN item i ON i.ord = p.ord WHERE i.item_id = ?", item)

    def in_set(set_id, entry_id, rule_id):
        return bool(q("SELECT 1 FROM ruleset WHERE set_id = ? AND entry_id = ? AND rule_id = ?", set_id, entry_id,
                      rule_id))
    river, mock, stub = "gnis:990001", "gnis:990002", "gnis:990004"
    # the waters: a river with a lake on it, two tributaries (one with a row), an unnamed trib, an added stream
    assert reg["wbk:800000001"]["name"] == "Fixture Lake" and "lake:800000001" not in secs(river)
    assert "Fixture Lake  through=['Fixture River']" in (out / "atlas" / "summary.txt").read_text()
    assert {reg[mock]["name"], reg[stub]["name"]} == {"Mock Creek", "Stub Creek"}
    assert not [i for i in reg.values() if "900000003:0" in i.get("section_ids", []) and not i["id"].startswith("area")]
    assert reg["wsc:920-123456-666666-500000"]["name"] == "Mockup Creek"
    # a braid: the river holds a section of a second blue line (900000005, kept BY NAME in the mini tree's
    # waters/loop_exemptions.json); its second braid 900000007 no creek flows into is removed (T1242, ruling E31)
    assert {s.split(":")[0] for s in secs(river)} == {"900000001", "900000005"}
    loops = json.loads((out / "atlas" / "small_loops.json").read_text())       # missing with the pass off
    assert loops["ran"] and loops["max_offset_m"] == float("inf")
    assert [lp["blks"] for lp in loops["removed"]] == [["900000007"]]
    assert loops["keep_by_kind"].get("named") == 1
    assert json.loads((out / "atlas" / "graph.pkl.meta.json").read_text())["build"]["small_loops"] == {
        "ran": True, "max_offset_m": "inf"}
    # the found names not in the atlas are NAMED in its record, never only counted (T1242 review)
    mined = json.loads((out / "atlas" / "mined_names_report.json").read_text())
    assert mined["absent"] == len(mined["absent_named"])
    # a tidal mouth: the river's piece below the coast is tidal in the bundle
    assert q("SELECT entry_id FROM tidal") == [("crossing:900000001:0",)]
    # two regions: the river straddles the line, and Twofold Lake lies in both
    assert secs(river) & in_region(1) and secs(river) & in_region(2)
    assert "lake:800000006" in in_region(1) & in_region(2)
    # a part closed all year: Mock Creek above its falls, the closure with no `when`
    closed = [p for p in parts(mock) if in_set(p[1], "r1:mock_creek@1-1", "mock_creek.r2")]
    assert len(parts(mock)) == 2 and len(closed) == 1
    assert q("SELECT when_, take FROM rule WHERE rule_id = 'mock_creek.r2'") == [(None, 0)]
    # a weekday rule and an hours window, bound, and read by the reader as moments of their keys
    assert bound("r1:fixture_lake@1-1", "fixture_lake.r2") and bound("r1:fixture_river@1-1", "fixture_river.r3")
    v = sqlite3.connect(f"file:{out / 'deliver' / 'verdicts.sqlite'}?mode=ro", uri=True)
    assert v.execute("SELECT COUNT(*) FROM moment WHERE weekdays IS NOT NULL AND weekdays != 127").fetchone()[0]
    assert v.execute("SELECT COUNT(*) FROM moment WHERE hours IS NOT NULL").fetchone()[0]
    # zone vs water, a lift, a moot size clause: Stub Creek's own release beside the zone's 50 cm clause, and its row
    # exempting the zone's summer closure
    stub_sets = {p[1] for p in parts(stub)}
    assert all(in_set(x, "r2:stub_creek@2-1", "stub_creek.r1") and in_set(x, "z2:daily_quotas", "trout_char_quota.r2")
               for x in stub_sets)
    assert bound("r2:stub_creek@2-1", "stub_creek.r2") and \
        "summer_stream_closure" in (q("SELECT exempts FROM rule WHERE rule_id = 'stub_creek.r2'")[0][0] or "")
    # closures: the river's spring closure and a water closed whole (Mockup Creek)
    assert bound("r1:fixture_river@1-1", "fixture_river.r1") and bound("r2:mockup_creek@2-1", "mockup_creek.r1")
    # the steelhead twin (book-known waters: none, so it stays unresolved) and the stamp
    twins = q("SELECT rule_id, unresolved FROM rule WHERE entry_id = 'zp:steelhead' AND rule_id LIKE '%b'")
    assert sorted(r for r, _ in twins) == ["steelhead.r1b", "steelhead.r2b", "steelhead.r4b"] and all(u for _, u in twins)
    assert bound("zp:steelhead", "steelhead.r1")
    assert q("SELECT COUNT(*) FROM section_licensing")[0][0] > 0
    # the land a section crosses (T54), a gauge, and the corpus size
    assert {t for (t,) in q("SELECT tier FROM access_area")} == {"no_access", "reserve", "private"}
    assert q("SELECT station, item_id FROM gauge") == [("99MA001", river)]
    # (T981) a local authority's closure: Bylaw Pond, closed whole by the second entry source, citing its page
    assert len(bound("r2:bylaw_pond_local@2-1", "bylaw_pond_local.r1")) == len(secs("wbk:800000007")) == 1
    assert json.loads(q("SELECT authority FROM entry WHERE entry_id = 'r2:bylaw_pond_local@2-1'")[0][0])["url"] \
        == "https://example.org/mini-east/parks-bylaw"
    # 18 entries of the mini's own (one of them the local closure) and 10 of the book's provincial rows the answers'
    # glossary needs (copied)
    assert q("SELECT COUNT(*) FROM entry")[0][0] == 28


def test_under_test_mini_is_the_committed_release_and_ci_no_build_hides_none_of_it():
    """`under_test.mini()` is the one way a pipeline test reaches the mini release: the committed copy, alone, the
    release its LATEST names — and a run under CI's view (`-p pipeline.tests.ci_no_build`, every local build hidden)
    still has it."""
    import subprocess
    import sys
    from pipeline.tests import under_test
    d = under_test.mini()
    assert d.parent == M.COMMITTED and (d / "manifest.json").is_file()
    code = ("from pipeline.tests import under_test; d = under_test.mini(); "
            "import json; print(json.loads((d / 'manifest.json').read_text())['format'])")
    out = subprocess.run([sys.executable, "-c", "import pipeline.tests.ci_no_build; " + code], cwd=M.REPO_ROOT,
                         capture_output=True, text=True, env={**__import__("os").environ, "PYTHONPATH": str(M.REPO_ROOT)})
    assert out.returncode == 0 and out.stdout.strip() == "app-manifest/2", out.stderr[-500:]


def test_the_barrier_list_holds_its_two_kinds_of_row_on_the_built_world(chain):
    """(T1250, RULINGS S14) The mini barrier list on the BUILT world: its `basin` row holds Northward Creek (the
    Mackenzie, FWA 200) and its `upstream_of` row Mock Creek above its falls (blue line 900000002 from 4,000 m up,
    with the unnamed trib above); both stay "possible", while Mock Creek below the point and the Fixture River are
    known Pacific streams."""
    out = chain[0] / "a"
    rep = json.loads((out / "reaches" / "report.json").read_text())["steelhead"]
    assert {b["id"]: b["kind"] for b in rep["barriers"]} == {"mackenzie": "basin",
                                                             "mock_creek_above_its_falls": "upstream_of"}
    assert all(b["sections"] > 0 for b in rep["barriers"])
    pres = {json.loads(l)["section_id"]: json.loads(l) for l in (out / "reaches" / "steelhead_presence.jsonl")
            .read_text().splitlines() if l.strip()}
    north = [s for s in pres if s.startswith("900000006:")]
    above = [s for s in pres if s.startswith("900000002:") and float(s.split(":")[1]) >= 4000]
    below = [s for s in pres if s.startswith("900000002:") and float(s.split(":")[1]) < 4000]
    assert north and above and below
    assert {pres[s]["steelhead"] for s in north + above} == {"possible"}
    assert {(pres[s]["steelhead"], pres[s]["pacific"]) for s in below} == {("known", True)}
    assert rep["possible_held"]["mackenzie"] == len(north)
