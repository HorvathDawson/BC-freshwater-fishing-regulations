"""THE DATE EACH SOURCE WAS LAST UPDATED, in the manifest (`sources`), for the app's "Data sources & dates" screen.

Owner 2026-10-21: "we will also want the date each source of info was updated". The app never states a date itself; it
reads this block. Every date here is a fact OF THE INPUT (the HYDAT release the climatology was built from, the day the
FWA streams layer was fetched into the GeoPackage, the "Date modified" DFO prints on its region pages, the OpenStreetMap
replication the basemap was cut from), never the clock of the run: two runs on the same inputs still write the same
bytes (AGENTS 19). A source whose date the inputs do not record says null, and the app says so.
"""
from __future__ import annotations

import gzip
import json
import sqlite3
import struct

from pipeline.deliver.release.manifest import SOURCE_KEYS, manifest, sources_block

TITLE = "2025–2027 BC Freshwater Fishing Regulations Synopsis"


def _gpkg(path, rows):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE gpkg_contents (table_name TEXT PRIMARY KEY, data_type TEXT, last_change TEXT)")
    con.executemany("INSERT INTO gpkg_contents VALUES (?, 'features', ?)", rows)
    con.commit()
    con.close()


def _pmtiles(path, meta: dict, compression: int = 2):
    """A PMTiles v3 file holding only its header and its metadata (all `sources_block` reads of it)."""
    raw = json.dumps(meta).encode()
    body = gzip.compress(raw, mtime=0) if compression == 2 else raw
    h = bytearray(127)
    h[0:7] = b"PMTiles"
    h[7] = 3
    h[24:40] = struct.pack("<QQ", 127, len(body))
    h[97] = compression
    path.write_bytes(bytes(h) + body)


def _dfo(rules, dates):
    rules.mkdir(parents=True)
    for slug, d in dates.items():
        (rules / f"region-{slug}.json").write_text(json.dumps({"region": slug, "date_modified": d, "rules": []}))


def _inputs(tmp_path):
    (tmp_path / "hydat.release").write_text("20260717\n")
    _gpkg(tmp_path / "bc_fisheries_data.gpkg",
          [("lakes", "2026-07-14T23:18:32.347Z"), ("streams", "2026-07-15T23:21:10.921Z"),
           ("pfma_areas", "2026-09-15T17:59:31.190Z")])
    _pmtiles(tmp_path / "bc.pmtiles", {"name": "Protomaps Basemap", "planetiler:buildtime": "2025-05-06T00:22:16.609Z",
                                       "planetiler:osm:osmosisreplicationtime": "2026-03-05T04:00:00Z"})
    _dfo(tmp_path / "dfo" / "rules", {"1": "2026-08-25", "4": "2025-04-01", "6": "2026-08-28", "5b": None})
    return tmp_path / "dfo" / "rules"


def test_every_date_is_read_from_the_inputs(tmp_path):
    dfo = _inputs(tmp_path)
    assert sources_block(tmp_path, synopsis_title=TITLE, dfo_rules=dfo) == {
        "synopsis": {"title": TITLE, "date": None},
        # the newest of the region pages' own "Date modified" (a page that states none is skipped, never "today")
        "dfo": {"date": "2026-08-28"},
        # the streams layer is the atlas: a later fetch of another layer (pfma_areas) does not date it
        "fwa": {"date": "2026-07-15"},
        # the OpenStreetMap data the Protomaps build was cut from, not the day planetiler ran
        "basemap": {"date": "2026-03-05"},
        "hydat": {"date": "2026-07-17"},
    }


def test_an_input_that_records_no_date_says_null_never_today(tmp_path):
    # no hydat.release, no GeoPackage, no basemap source, no DFO scrape (a fixture release, CI)
    assert sources_block(tmp_path, synopsis_title=TITLE, dfo_rules=tmp_path / "nope") == {
        "synopsis": {"title": TITLE, "date": None}, "dfo": {"date": None}, "fwa": {"date": None},
        "basemap": {"date": None}, "hydat": {"date": None}}


def test_a_malformed_marker_is_not_a_date(tmp_path):
    (tmp_path / "hydat.release").write_text("latest\n")
    _gpkg(tmp_path / "bc_fisheries_data.gpkg", [("streams", "")])
    _pmtiles(tmp_path / "bc.pmtiles", {"name": "Protomaps Basemap"}, compression=1)
    _dfo(tmp_path / "rules", {"1": "soon", "2": None})
    assert sources_block(tmp_path, synopsis_title=TITLE, dfo_rules=tmp_path / "rules") == {
        "synopsis": {"title": TITLE, "date": None}, "dfo": {"date": None}, "fwa": {"date": None},
        "basemap": {"date": None}, "hydat": {"date": None}}


def test_a_file_that_is_not_a_pmtiles_archive_is_no_date(tmp_path):
    (tmp_path / "bc.pmtiles").write_bytes(b"not an archive")
    assert sources_block(tmp_path, synopsis_title=TITLE, dfo_rules=tmp_path)["basemap"] == {"date": None}


def test_the_keys_are_the_ones_the_app_reads_in_its_order():
    assert SOURCE_KEYS == ("synopsis", "dfo", "fwa", "basemap", "hydat")


def test_the_manifest_carries_the_block_only_when_given():
    # a fixture release (regs_fixture_release, fixture_release) passes none: its committed manifest stays as it is
    assert "sources" not in manifest("r-x", {}, "d768d6e135e62b99")
    s = {"synopsis": {"title": TITLE, "date": None}, "fwa": {"date": "2026-07-15"}, "hydat": {"date": None}}
    assert manifest("r-x", {}, "d768d6e135e62b99", sources=s)["sources"] == s


def test_ship_writes_the_block_into_the_manifest(tmp_path):
    from pipeline.deliver.release.build import ship
    work = tmp_path / "work"
    work.mkdir()
    raw = work / "core.sqlite"
    raw.write_bytes(b"core")
    s = {"synopsis": {"title": TITLE, "date": None}, "fwa": {"date": "2026-07-15"}, "hydat": {"date": "2026-07-17"}}
    d = ship(work, {"core": raw}, "d768d6e135e62b99", tmp_path / "out", data_release="r-t", sources=s)
    assert json.loads((d / "manifest.json").read_text())["sources"] == s


def test_the_release_cli_dates_the_sources_from_the_fetched_data_dir():
    # the CLI's one call: the fetched inputs (SOURCE), the book's own title (answers.common.BOOK_TITLE) and the DFO scrape
    import inspect

    from pipeline.deliver.release import __main__ as CLI
    assert ('sources=sources_block(SOURCE, synopsis_title=BOOK_TITLE, dfo_rules=GENERATED.regs.dfo_salmon / "rules")'
            in inspect.getsource(CLI.main))
