"""T841: every classified water the book prints ships classified, with its Classified Waters Licence (owner 2026-11-17:
"make sure the classified water symbols are maintained properly … capture all classified waters whether labeled by
symbol or by tributary of something"). `pipeline.regs.classified_waters` holds the book's rows (`BOOK`) and checks the
corpus against them (fast-fail, release preflight) and the built bundle (the release's checks stage).

The corpus tests stand on a corpus made from the BOOK table itself (anchor a), each mutation named by its printed page;
the live corpus is in git (`under_test.curated()`); the bundle test reads the bundle under test (`needs_bundle`)."""
from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from pipeline.regs import classified_waters as CW
from pipeline.tests import under_test


def _entry(r: CW.Row, n: int) -> dict:
    region = r.region
    e = {"entry_id": f"r{region}:row_{n}@{region}-1", "name": r.name, "region": region, "source_pages": [r.page],
         "symbols": ["Classified"] if r.glyph else [], "matched": [f"gnis:{n}"], "regs_verbatim": "",
         "licensing": []}
    if r.tributaries is True:
        e["includes_tributaries"] = True
    for i, c in enumerate(r.classes):
        d = {"kind": "designation", "id": f"d{i}", "classified": c, "unit": f"u{n}_{i}", "unit_name": r.name,
             "verbatim": f"Class {c} water"}
        if r.tributaries == "only":
            d["tributaries_only"] = True
        e["licensing"].append(d)
        e["regs_verbatim"] += f"Class {c} water\n"
    return e


def _corpus() -> dict[str, dict]:
    out = {}
    for n, r in enumerate(CW.BOOK):
        e = _entry(r, n)
        out[e["entry_id"]] = e
    for v in (r for r in CW.BOOK if r.via):          # a pointer holds the water its row names (Kwinamass) or is walked
        via = next(e for e in out.values() if (e["region"], e["name"]) == v.via)
        via["includes_tributaries"] = True
    for k, (region, page, name) in enumerate(CW.NOT_CLASSIFIED):
        out[f"r{region}:nc_{k}@{region}-1"] = {
            "entry_id": f"r{region}:nc_{k}@{region}-1", "name": name, "region": region, "source_pages": [page],
            "symbols": [], "matched": [f"gnis:nc{k}"], "regs_verbatim": "Part described is NOT a Classified Water",
            "licensing": [{"kind": "not_classified", "id": "not_classified", "verbatim": "NOT a Classified Water"}]}
    return out


def _by_name(c: dict, name: str, region: str | None = None) -> dict:
    return next(e for e in c.values() if e["name"] == name and (region is None or e["region"] == region))


def test_a_corpus_as_the_book_prints_it_passes():
    assert CW.corpus_problems(_corpus()) == []


def test_the_book_table_counts_the_2025_27_rows():
    """72 rows printed with the CW glyph (3 of them classified through another row) and the West Road's text-only
    Class II (RULINGS Li10): 73 rows, 74 printed classes (the Dean three, the Skeena and the Zymoetz two each)."""
    assert len(CW.BOOK) == 73 and sum(r.glyph for r in CW.BOOK) == 72
    assert sorted(r.name for r in CW.BOOK if r.via) == ["BIGHORN (Ram) CREEK",
                                                         "FORDING RIVER (upstream of Josephine Falls)",
                                                         "KWINAMASS RIVER"]
    assert sum(len(r.classes) for r in CW.BOOK) == 74
    assert [r.name for r in CW.BOOK if not r.glyph] == ['WEST ROAD ("Blackwater") RIVER']


def test_a_dropped_designation_is_named_with_its_page():
    c = _corpus()
    _by_name(c, "TELKWA RIVER")["licensing"] = []
    assert CW.corpus_problems(c) == [
        f"p.54 TELKWA RIVER (Region 6): the book prints Class II; {_by_name(c, 'TELKWA RIVER')['entry_id']} carries "
        f"no designation"]


def test_one_of_two_printed_classes_lost_is_named():
    c = _corpus()
    e = _by_name(c, "SKEENA RIVER (mainstem only)")
    e["licensing"] = e["licensing"][:1]
    [p] = CW.corpus_problems(c)
    assert p.startswith("p.53 SKEENA RIVER (mainstem only) (Region 6): the book prints Class II, Class II;")


def test_a_wrong_class_is_named():
    c = _corpus()
    _by_name(c, "LAKELSE RIVER")["licensing"][0]["classified"] = "II"
    [p] = CW.corpus_problems(c)
    assert "p.52 LAKELSE RIVER" in p and "the book prints Class I;" in p and "carries Class II" in p


def test_tributaries_the_book_prints_must_be_reached_and_never_more():
    c = _corpus()
    del _by_name(c, "BULKLEY RIVER")["includes_tributaries"]                   # the row's ✱ lost
    _by_name(c, "DEENA CREEK")["licensing"][0]["includes_tributaries"] = True  # Deena prints none
    ps = CW.corpus_problems(c)
    assert any("p.50 BULKLEY RIVER" in p and "does not reach the tributaries; the book: reaches the tributaries" in p
               for p in ps)
    assert any("p.15 DEENA CREEK" in p and "reaches the tributaries; the book: does not reach" in p for p in ps)
    assert len(ps) == 2


def test_the_elk_rivers_tributaries_cover_only_the_tributaries():
    c = _corpus()
    del _by_name(c, "ELK RIVER'S TRIBUTARIES (see exceptions)")["licensing"][0]["tributaries_only"]
    [p] = CW.corpus_problems(c)
    assert "p.36 ELK RIVER'S TRIBUTARIES" in p and "the book: covers only the tributaries" in p


def test_a_lost_glyph_is_named():
    c = _corpus()
    _by_name(c, "ECSTALL RIVER")["symbols"] = []
    [p] = CW.corpus_problems(c)
    assert p.startswith("p.51 ECSTALL RIVER (Region 6): the book prints the CW glyph;")


def test_a_pointer_row_is_classified_through_the_row_it_names():
    c = _corpus()
    via = _by_name(c, "WIGWAM RIVER (downstream of the access road adjacent to km 42 on the Bighorn (Ram) Forest "
                      "Service Road)")
    del via["includes_tributaries"]
    via["licensing"][0]["includes_tributaries"] = False
    ps = CW.corpus_problems(c)                 # the Wigwam row itself, and the pointer that leans on it
    assert len(ps) == 2 and "p.40 WIGWAM RIVER (downstream" in ps[1]          # in book order
    assert ps[0].startswith("p.35 BIGHORN (Ram) CREEK (Region 4) is classified through WIGWAM RIVER") \
        and ps[0].endswith("neither holds the same water nor reaches its tributaries")
    # the same water is enough, tributaries or not (an old name's pointer: Kwinamass -> Ksi X'anmas)
    book = (CW.Row("6", 52, "A RIVER", ("II",), False), CW.Row("6", 52, "OLD A RIVER", via=("6", "A RIVER")))
    c = {e["entry_id"]: e for e in (_entry(book[0], 1), _entry(book[1], 2))}
    _by_name(c, "OLD A RIVER")["matched"] = ["gnis:1"]
    assert CW.corpus_problems(c, book=book, not_classified=()) == []
    _by_name(c, "OLD A RIVER")["matched"] = ["gnis:2"]
    assert CW.corpus_problems(c, book=book, not_classified=()) == [
        "p.52 OLD A RIVER (Region 6) is classified through A RIVER (), but that row neither holds the same water nor "
        "reaches its tributaries"]


def test_a_classified_entry_the_book_table_does_not_list_is_named():
    c = _corpus()
    c["r3:new_creek@3-1"] = {"entry_id": "r3:new_creek@3-1", "name": "NEW CREEK", "region": "3", "source_pages": [30],
                             "symbols": [], "regs_verbatim": "Class II water Sept 1-Oct 31", "licensing": []}
    c["r3:glyph_creek@3-1"] = {"entry_id": "r3:glyph_creek@3-1", "name": "GLYPH CREEK", "region": "3",
                               "source_pages": [30], "symbols": ["Classified"], "regs_verbatim": "Bait ban",
                               "licensing": []}
    c["r3:not_creek@3-1"] = {"entry_id": "r3:not_creek@3-1", "name": "NOT CREEK", "region": "3", "source_pages": [30],
                             "symbols": [], "regs_verbatim": "This tributary is not a Classified Water",
                             "licensing": []}
    assert CW.corpus_problems(c) == [
        "r3:glyph_creek@3-1 (p.30 GLYPH CREEK) has the CW glyph but is not a BOOK row",
        "r3:new_creek@3-1 (p.30 NEW CREEK) has a printed \"Class I/II water\" but is not a BOOK row"]


def test_a_missing_row_or_page_is_named():
    c = _corpus()
    del c[_by_name(c, "SUSTUT RIVER")["entry_id"]]
    _by_name(c, "KITWANGA RIVER")["source_pages"] = [51]
    ps = CW.corpus_problems(c)
    assert "book row p.53 SUSTUT RIVER (Region 6): 0 corpus entries carry this printed name — want exactly 1" in ps
    assert any(p.startswith("book row p.52 KITWANGA RIVER: its entry") and "says pages [51]" in p for p in ps)


def test_not_a_classified_water_is_held_both_ways():
    c = _corpus()
    _by_name(c, "JOSEPH CREEK")["licensing"] = []
    _by_name(c, "DEENA CREEK")["licensing"].append({"kind": "not_classified", "id": "nc", "verbatim": "x"})
    deena = _by_name(c, "DEENA CREEK")["entry_id"]
    ps = CW.corpus_problems(c)
    assert len(ps) == 2 and ps[0].startswith("p.37 JOSEPH CREEK: prints \"NOT a Classified Water\"")
    assert ps[1] == f"{deena} carries a not_classified record but is not a NOT_CLASSIFIED row"


def test_the_extraction_cross_check_reads_glyphs_and_printed_classes():
    rows = [{"water": r.name, "region": f"REGION {r.region}", "page": r.page,
             "symbols": ["Classified"] if r.glyph else [],
             "raw_regs": " ".join(f"Class {c} water" for c in r.classes)} for r in CW.BOOK]
    rows += [{"water": n, "region": f"REGION {g}", "page": p, "symbols": [], "raw_regs": "NOT a Classified Water"}
             for g, p, n in CW.NOT_CLASSIFIED]
    assert CW.extraction_problems(rows) == []
    rows.append({"water": "NEW RIVER", "region": "REGION 6 - Skeena", "page": 54, "symbols": ["Classified"],
                 "raw_regs": "Bait ban"})
    assert CW.extraction_problems(rows) == ["extraction p.54 NEW RIVER: the CW glyph, but not a BOOK row"]


def _write(tmp_path: Path, c: dict) -> Path:
    by = {}
    for e in c.values():
        by.setdefault(e["region"], []).append(e)
    for region, es in by.items():
        (tmp_path / f"region-{region}.json").write_text(json.dumps({"region": region, "entries": es}),
                                                        encoding="utf-8")
    return tmp_path


def test_the_command_exits_0_1_2(tmp_path, capsys):
    (tmp_path / "good").mkdir()
    good = _write(tmp_path / "good", _corpus())
    assert CW.main(["--check", "--corpus", str(good)]) == 0
    assert "73 book rows, 74 designations" in capsys.readouterr().out
    bad = _corpus()
    _by_name(bad, "DEAN RIVER")["licensing"].pop()
    (tmp_path / "bad").mkdir()
    assert CW.main(["--check", "--corpus", str(_write(tmp_path / "bad", bad))]) == 1
    out = capsys.readouterr().out
    assert "p.44 DEAN RIVER (Region 5): the book prints Class I, Class I, Class II" in out and "re-runs the parser" in out
    (tmp_path / "none").mkdir()
    assert CW.main(["--check", "--corpus", str(tmp_path / "none")]) == 2
    assert "could not run" in capsys.readouterr().err


def test_the_live_corpus_is_the_book():
    """The corpus in git agrees with the book's classified rows (the gate fast-fail and the preflight run)."""
    assert CW.corpus_problems(CW.load_corpus(Path(under_test.curated().regulations.synopsis))) == []


@pytest.mark.needs_bundle
def test_the_bundle_under_test_ships_every_classified_water_with_its_licence(request):
    """Every designation of the corpus is placed, and on every section it binds the B.C. resident adult's Licence tab
    asks for the Classified Waters Licence in its period (the Atnarko's own sleep aside), and nowhere outside it."""
    entries = CW.load_corpus(Path(under_test.curated().regulations.synopsis))
    assert CW.bundle_problems(under_test.bundle(request), entries) == []


@pytest.mark.slow
@pytest.mark.needs_bundle
def test_mutation_a_licence_requirement_without_its_period_and_a_water_losing_sections_fail(request, tmp_path):
    """The mutation: the province's Classified Waters Licence requirement losing `on: classified_period` holds outside
    every period, and a whole-water designation losing three of its own sections ships the water half classified;
    both are named."""
    b = tmp_path / "bundle.sqlite"
    shutil.copy(under_test.bundle(request), b)
    db = sqlite3.connect(b)
    with db:
        db.execute("UPDATE requirement SET record = json_remove(record, '$.on') WHERE entry_id = "
                   "'zp:classified_waters_licence' AND req_id = 'classified_waters_licence'")
        db.execute("DELETE FROM section_licensing WHERE sid IN (SELECT sid FROM designation_section WHERE entry_id = "
                   "'r6:telkwa_river@6-9' AND via = 'reach' LIMIT 3)")
    db.close()
    ps = CW.bundle_problems(b, CW.load_corpus(Path(under_test.curated().regulations.synopsis)))
    assert any(p.startswith("requirement zp:classified_waters_licence#classified_waters_licence asks for the "
                            "Classified Waters Licence without `on: classified_period`") for p in ps)
    assert any("TELKWA RIVER" in p and "3 section(s) of its own water" in p for p in ps)
    assert any("outside its period, with no designation in force" in p for p in ps)
