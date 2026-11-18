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


_MON = {m: i for i, m in enumerate(("", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
                                     "Dec")) if m}


def _when(period: str):
    """The BOOK's "Apr 1-Oct 31, …" back as a `When`."""
    if not period:
        return None
    out = []
    for rng in period.split(", "):
        a, b = rng.split("-")
        (fm, fd), (tm, td) = a.split(" "), b.split(" ")
        out.append({"from_month": _MON[fm], "from_day": int(fd), "to_month": _MON[tm], "to_day": int(td)})
    return {"dates": out}


def _extents(words: str):
    """The BOOK's "op [item] cut,cut [walk_past]; …" back as extents."""
    out = []
    for part in [w for w in words.split("; ") if w]:
        toks = part.split(" ")
        x = {"op": toks[0]}
        for t in toks[1:]:
            if t == "walk_past":
                x["walk_past"] = True
            elif ":" in t:
                x["item_id"] = t
            else:
                x["splits"] = t.split(",")
        out.append(x)
    return out


def _entry(r: CW.Row, n: int) -> dict:
    region = r.region
    e = {"entry_id": f"r{region}:row_{n}@{region}-1", "name": r.name, "region": region, "source_pages": [r.page],
         "symbols": ["Classified"] if r.glyph else [], "matched": [f"gnis:{n}"], "regs_verbatim": "",
         "licensing": []}
    if r.tributaries is True:
        e["includes_tributaries"] = True
    for i, u in enumerate(r.units):
        d = {"kind": "designation", "id": f"d{i}", "classified": u.cls, "unit": f"u{n}_{i}", "unit_name": r.name,
             "verbatim": f"Class {u.cls} water"}
        if u.period:
            d["when"] = _when(u.period)
        own, _, clip = u.place.partition(" & ")       # the designation's own place, then the row's clip
        if own:
            d["extents"] = _extents(own)
        if clip:
            e["extents"] = _extents(clip)
        if u.excludes:
            d["tributary_excludes"] = _extents(u.excludes)
        if u.sleeps:
            d["suspended_while"] = [{"rule_id": "r1", "verbatim": "not required until reopened"}]
        if r.tributaries == "only":
            d["tributaries_only"] = True
        e["licensing"].append(d)
        e["regs_verbatim"] += f"Class {u.cls} water\n"
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


def test_a_period_moved_off_the_printed_dates_is_named():
    """The reviewer's mutation: the Dean's Class II starting Jun 20, not the printed Jun 16."""
    c = _corpus()
    _by_name(c, "DEAN RIVER")["licensing"][2]["when"] = _when("Jun 20-Oct 31")
    assert CW.corpus_problems(c) == ["p.44 DEAN RIVER (Region 5): designation d2 is classified 'Jun 20-Oct 31'; the "
                                     "book: 'Jun 16-Oct 31'"]


def test_a_narrowed_stretch_is_named():
    """The reviewer's mutation: Skeena River 2 ending at another cut."""
    c = _corpus()
    _by_name(c, "SKEENA RIVER (mainstem only)")["licensing"][0]["extents"][0]["splits"][1] = "skeena_river__lower_cut"
    [p] = CW.corpus_problems(c)
    assert p.startswith("p.53 SKEENA RIVER (mainstem only) (Region 6): designation d0 lies 'between "
                        "skeena_river__exchamsiks_river_into_skeena_river,skeena_river__lower_cut'")


def test_the_walk_past_of_li5a_is_held():
    """The reviewer's mutation: the Elk's carve-out of lower Coal Creek losing `walk_past` (Li5a: the walk goes on above
    the bridge)."""
    c = _corpus()
    for name in ("ELK RIVER (upstream of Elko Dam)", "ELK RIVER'S TRIBUTARIES (see exceptions)"):
        del _by_name(c, name)["licensing"][0]["tributary_excludes"][0]["walk_past"]
    ps = CW.corpus_problems(c)
    assert len(ps) == 2 and all("carves out 'downstream_of gnis:17550 "
                                "elk_river_s_tributaries__old_mf_m_railway_bridge_coal_ck'" in p for p in ps)


def test_the_atnarkos_sleep_is_held():
    c = _corpus()
    del _by_name(c, "BURNT BRIDGE CREEK (upstream of Sitkatapa Creek)")["licensing"][0]["suspended_while"]
    assert CW.corpus_problems(c) == ["p.43 BURNT BRIDGE CREEK (upstream of Sitkatapa Creek) (Region 5): designation d0 "
                                     "sleeps while its closure binds: False; the book: True"]


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
    book = (CW.Row("6", 52, "A RIVER", (CW.U("II"),), False), CW.Row("6", 52, "OLD A RIVER", via=("6", "A RIVER")))
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


def _mini_bundle(sections, bound=(), not_classified=()) -> sqlite3.Connection:
    """The tables the named cases and the pins read: `sections` [(sid, item, m)] (item None: an unnamed section, its
    length a route measure span), `bound` [(sid, eid, did, unit, via)], `not_classified` [(sid, eid)]."""
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT); CREATE TABLE item_section (ord INTEGER, sid INTEGER);
        CREATE TABLE item_alias (alias TEXT, item_id TEXT); CREATE TABLE tidal (sid INTEGER);
        CREATE TABLE outside_bc (sid INTEGER); CREATE TABLE section_length (sid INTEGER, m INTEGER);
        CREATE TABLE section_measure (sid INTEGER, blk INTEGER, from_m INTEGER, to_m INTEGER);
        CREATE TABLE designation (entry_id TEXT, designation_id TEXT, unit TEXT);
        CREATE TABLE designation_section (sid INTEGER, entry_id TEXT, designation_id TEXT, via TEXT);
        CREATE TABLE not_classified_section (sid INTEGER, entry_id TEXT);""")
    items = {}
    for sid, item, m in sections:
        if item is None:
            db.execute("INSERT INTO section_measure VALUES (?, 1, 1000, ?)", (sid, 1000 + m))
            continue
        o = items.setdefault(item, len(items) + 1)
        db.execute("INSERT OR IGNORE INTO item VALUES (?, ?)", (o, item))
        db.execute("INSERT INTO item_section VALUES (?, ?)", (o, sid))
        db.execute("INSERT INTO section_length VALUES (?, ?)", (sid, m))
    for sid, eid, did, unit, via in bound:
        db.execute("INSERT INTO designation SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM designation WHERE "
                   "entry_id = ? AND designation_id = ?)", (eid, did, unit, eid, did))
        db.execute("INSERT INTO designation_section VALUES (?, ?, ?, ?)", (sid, eid, did, via))
    db.executemany("INSERT INTO not_classified_section VALUES (?, ?)", not_classified)
    return db


_ELK = "r4:elk@4-2"
_COAL_ENTRY = {"entry_id": "r4:coal@4-23", "region": "4", "name": CW._COAL[1], "matched": ["gnis:17550"]}


def test_the_named_case_coal_creek_above_the_bridge_is_the_elks():
    """Li5a: the part its own row does not un-classify is the Elk's Class II; the part it does carries nothing."""
    cases = [c for c in CW.NAMED if c.row == CW._COAL]
    secs = [(1, "gnis:17550", 280), (2, "gnis:17550", 6588), (3, "gnis:17550", 15780)]
    nc = [(1, "r4:coal@4-23"), (2, "r4:coal@4-23")]
    good = _mini_bundle(secs, [(3, _ELK, "elk_river", "elk_river", "trib")], nc)
    assert CW.named_case_problems(good, {"r4:coal@4-23": _COAL_ENTRY}, cases) == []
    # the shipped r-46e4355a7277: the walk stopped at the bridge (its reach run predates Li5a's code)
    bad = _mini_bundle(secs, [], nc)
    assert CW.named_case_problems(bad, {"r4:coal@4-23": _COAL_ENTRY}, cases) == [
        "Li5a: Coal Creek above the old MF&M Railway bridge is the Elk River's Class II — 1 of its 1 section(s) carry "
        "no designation (e.g. section 3)"]
    over = _mini_bundle(secs, [(2, _ELK, "elk_river", "elk_river", "trib"), (3, _ELK, "elk_river", "elk_river", "trib")],
                        nc)
    assert CW.named_case_problems(over, {"r4:coal@4-23": _COAL_ENTRY}, cases) == [
        "Li5a: Coal Creek below the bridge is not a Classified Water (its row) — 1 of its 2 section(s) carry "
        "['elk_river'] (e.g. section 2)"]


def test_the_named_cases_unclassified_rows_and_the_nanika():
    entries = {f"r{g}:{n}": {"entry_id": f"r{g}:{n}@1", "region": g, "name": n, "matched": [i]}
               for g, n, i in (("4", "LINE CREEK", "gnis:10373"), ("4", "GRAVE CREEK", "gnis:19452"),
                               ("6", "GOSNELL CREEK", "gnis:18182"), ("6", "ENDAKO RIVER", "gnis:17338"))}
    cases = [c for c in CW.NAMED if c.row != CW._COAL]
    secs = [(10, "gnis:10373", 9), (11, "gnis:19452", 9), (12, "gnis:18182", 9), (13, "gnis:17338", 9),
            (14, "gnis:16517", 9)]
    morice = (14, "r6:morice@6-9", "morice_river", "morice_river", "trib")
    assert CW.named_case_problems(_mini_bundle(secs, [morice]), entries, cases) == []
    ps = CW.named_case_problems(_mini_bundle(secs, [(12, "r6:morice@6-9", "morice_river", "morice_river", "trib"),
                                                    (14, "r6:bulkley@6-9", "bulkley_river", "bulkley_river", "trib")]),
                                entries, cases)
    assert ps == ["Li5: Gosnell Creek (its own row, no class) is not classified (owner 2026-11-17: keep) — 1 of its 1 "
                  "section(s) carry ['morice_river'] (e.g. section 12)",
                  "Li5: the Nanika River above Morice Lake (no row) is the Morice River's Class II — 1 of its 1 "
                  "section(s) carry ['bulkley_river'] (e.g. section 14)"]
    # a water the bundle no longer has is a problem, never a pass
    assert CW.named_case_problems(_mini_bundle(secs[:4]), entries, cases)[-1].endswith("the bundle has no section of it")


def test_the_pins_hold_each_walk_and_each_stretch():
    """A walk that loses its unnamed creeks (the reviewer's 4,846 deleted Bulkley sections), a named water or part of
    one, a stretch that narrows (Skeena River 2), a water gained, a designation unpinned: each named."""
    secs = [(1, "gnis:1", 5000), (2, "gnis:1", 5000), (3, "gnis:2", 3000), (4, "gnis:3", 2000), (5, "gnis:3", 2000)]
    secs += [(10 + i, None, 1000) for i in range(20)]                  # twenty unnamed creeks, 1 km each
    full = [(1, "r6:s@6-10", "s2", "s2", "reach"), (2, "r6:s@6-10", "s2", "s2", "reach")] + [
        (sid, "r6:s@6-10", "s2", "s2", "trib") for sid in [3, 4, 5] + list(range(10, 30))]
    pins = {"designations": CW.measure_pins(_mini_bundle(secs, full))}
    assert pins["designations"] == {"r6:s@6-10#s2": {
        "own": {"n": 2, "m": 10000, "waters": {"gnis:1": 10000}},
        "walk": {"n": 23, "m": 27000, "waters": {"gnis:2": 3000, "gnis:3": 4000}}}}
    assert CW.pin_problems(_mini_bundle(secs, full), {}, pins) == []
    named_only = [x for x in full if x[0] < 10]
    assert CW.pin_problems(_mini_bundle(secs, named_only), {}, pins) == [
        "r6:s@6-10#s2: its tributary walk binds 3 section(s); pinned 23",
        "r6:s@6-10#s2: its tributary walk is 7.0 km; pinned 27.0 km"]
    no_trib = full[:2]
    assert CW.pin_problems(_mini_bundle(secs, no_trib), {}, pins)[-1] == \
        "r6:s@6-10#s2: its tributary walk no longer reaches 2 pinned water(s): gnis:2, gnis:3"
    half_a_water = [x for x in full if x[0] != 5]                      # gnis:3 loses 2 of its 4 km
    assert CW.pin_problems(_mini_bundle(secs, half_a_water), {}, pins) == [
        "r6:s@6-10#s2: its tributary walk is 25.0 km; pinned 27.0 km",
        "r6:s@6-10#s2: its tributary walk moved on 1 named water(s): gnis:3 2.0 km (pinned 4.0)"]
    narrowed = CW.pin_problems(_mini_bundle(secs, full[1:]), {}, pins)
    assert narrowed == ["r6:s@6-10#s2: its own water is 5.0 km; pinned 10.0 km",
                        "r6:s@6-10#s2: its own water moved on 1 named water(s): gnis:1 5.0 km (pinned 10.0)"]
    more = CW.pin_problems(_mini_bundle(secs + [(6, "gnis:9", 50)], full + [(6, "r6:s@6-10", "s2", "s2", "trib"),
                                                                             (6, "r6:x@6-1", "x", "x", "reach")]),
                           {}, pins)
    assert more == ["r6:x@6-1#x: no pin — read where it lies, then re-cut the pins (--write-pins)",
                    "r6:s@6-10#s2: its tributary walk now reaches 1 water(s) not pinned: gnis:9"]


def test_write_pins_says_what_moved(tmp_path, capsys):
    """A re-cut prints what moved since the last cut, so it is read, not rubber-stamped."""
    secs = [(1, "gnis:1", 5000), (2, "gnis:1", 5000)]
    b = tmp_path / "b.sqlite"
    src = _mini_bundle(secs, [(1, "r6:s@6-10", "s2", "s2", "reach"), (2, "r6:s@6-10", "s2", "s2", "reach")])
    src.execute("CREATE TABLE meta (k TEXT, v TEXT)")
    src.commit()
    src.execute("VACUUM INTO ?", (str(b),))
    pins = tmp_path / "pins.json"
    assert CW.write_pins(b, pins)["moved"] == ["(no earlier pins)"]
    db = sqlite3.connect(b)
    with db:
        db.execute("DELETE FROM designation_section WHERE sid = 2")
    db.close()
    assert CW.write_pins(b, pins)["moved"] == [                      # (a count within 3 sections is noise)
        "r6:s@6-10#s2: its own water is 5.0 km; pinned 10.0 km",
        "r6:s@6-10#s2: its own water moved on 1 named water(s): gnis:1 5.0 km (pinned 10.0)"]


def test_the_committed_pins_name_every_corpus_designation():
    """The pins file is cut for the corpus in git: every designation pinned, nothing else."""
    pins = CW.load_pins()["designations"]
    corpus = CW.load_corpus(Path(under_test.curated().regulations.synopsis))
    assert sorted(pins) == sorted(f"{e['entry_id']}#{d['id']}" for e in corpus.values() for d in CW.designations(e))


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


def _copy_with(request, tmp_path, *sql) -> Path:
    b = tmp_path / "bundle.sqlite"
    shutil.copy(under_test.bundle(request), b)
    db = sqlite3.connect(b)
    with db:
        for q in sql:
            db.execute(q)
    db.close()
    return b


def _new_problems(request, tmp_path, *sql, monkeypatch=None) -> list[str]:
    """What a mutation of the bundle under test adds to the gate's list (pins measured on the unmutated copy, so only
    the mutation moves them)."""
    from pipeline.deliver.answers.common import connect
    entries = CW.load_corpus(Path(under_test.curated().regulations.synopsis))
    db = connect(str(under_test.bundle(request)))
    pins = {"designations": CW.measure_pins(db)}
    db.close()
    before = set(CW.bundle_problems(under_test.bundle(request), entries, pins=pins))
    after = CW.bundle_problems(_copy_with(request, tmp_path, *sql), entries, pins=pins)
    return [x for x in after if x not in before]


_DESIG = "SELECT sid FROM designation_section WHERE entry_id = '{}' AND via = '{}'"


@pytest.mark.slow
@pytest.mark.needs_bundle
def test_mutation_the_elks_walk_deleted_is_named(request, tmp_path):
    """The reviewer's mutation: every Elk (upstream of Elko Dam) tributary section unbound."""
    got = _new_problems(request, tmp_path, "DELETE FROM section_licensing WHERE sid IN (" + _DESIG.format(
        "r4:elk_river_upstream_of_elko_dam@4-2+4-23", "trib") + ")")
    assert any("r4:elk_river_upstream_of_elko_dam@4-2+4-23#elk_river: its tributary walk is" in p for p in got)
    assert any("its tributary walk no longer reaches" in p for p in got)


@pytest.mark.slow
@pytest.mark.needs_bundle
def test_mutation_the_bulkleys_unnamed_creeks_deleted_are_named(request, tmp_path):
    """The re-review's mutation: every UNNAMED section of the Bulkley's tributary walk unbound (94 % of walked sections
    are unnamed — "tributary of something")."""
    got = _new_problems(request, tmp_path, "DELETE FROM section_licensing WHERE sid IN (SELECT d.sid FROM "
                        "designation_section d WHERE d.entry_id = 'r6:bulkley_river@6-9' AND d.via = 'trib' AND NOT "
                        "EXISTS (SELECT 1 FROM item_section s WHERE s.sid = d.sid))")
    assert any(p.startswith("r6:bulkley_river@6-9#bulkley_river: its tributary walk binds") for p in got)
    assert any(p.startswith("r6:bulkley_river@6-9#bulkley_river: its tributary walk is") for p in got)


@pytest.mark.slow
@pytest.mark.needs_bundle
def test_mutation_a_narrowed_stretch_is_named(request, tmp_path):
    """Skeena River 2 losing its upper half."""
    got = _new_problems(request, tmp_path, "DELETE FROM section_licensing WHERE sid IN (SELECT d.sid FROM "
                        "designation_section d JOIN section_measure m ON m.sid = d.sid WHERE d.entry_id = "
                        "'r6:skeena_river_mainstem_only@6-10' AND d.designation_id = 'skeena_river_2' ORDER BY m.from_m "
                        "DESC LIMIT 30)")
    assert any(p.startswith("r6:skeena_river_mainstem_only@6-10#skeena_river_2: its own water is") for p in got)


@pytest.mark.slow
@pytest.mark.needs_bundle
def test_mutation_a_sleep_whose_closure_no_longer_binds_is_named(request, tmp_path):
    """The Atnarko's "No Fishing for steelhead" moved to June: the designation wakes on Mar 1 although the book says it
    sleeps until reopened — the gate names the sleep that binds nowhere."""
    got = _new_problems(request, tmp_path, "UPDATE rule SET when_ = '{\"dates\":[{\"from_month\":6,\"from_day\":1,"
                        "\"to_month\":6,\"to_day\":30}]}' WHERE rule_id = 'atnarko_bella_coola_rivers.r13'")
    assert any("atnarko_bella_coola_rivers (p.43" in p and "binds none of its" in p for p in got)


@pytest.mark.slow
@pytest.mark.needs_bundle
def test_mutation_a_frame_that_drops_a_designation_from_considered_is_named(request, tmp_path, monkeypatch):
    """The card's Class badge reads `holds.considered`: a reader that left the Dean's designations out of it is named."""
    from pipeline.deliver.answers.common import connect
    from pipeline.deliver.bundle.read import licensing as RL
    b, entries = under_test.bundle(request), CW.load_corpus(Path(under_test.curated().regulations.synopsis))
    db = connect(str(b))
    pins = {"designations": CW.measure_pins(db)}
    db.close()
    said = lambda ps: {p.split(" (p.")[0] for p in ps if "does not list it in `considered`" in p}  # noqa: E731
    assert said(CW.bundle_problems(b, entries, pins=pins)) == set()
    real = RL.considered_records
    monkeypatch.setattr(RL, "considered_records",
                        lambda *a, **k: [x for x in real(*a, **k) if not x.startswith("r5:dean_river@5-9#")])
    assert said(CW.bundle_problems(b, entries, pins=pins)) == {
        "r5:dean_river@5-9#dean_river_class_i_lower", "r5:dean_river@5-9#dean_river_class_i_main",
        "r5:dean_river@5-9#dean_river_class_ii_upper"}


@pytest.mark.slow
@pytest.mark.needs_bundle
def test_mutation_coal_creek_above_the_bridge_unbound_is_named(request, tmp_path):
    """Li5a by name: the Elk's designation taken off Coal Creek above the bridge (as r-46e4355a7277 shipped it)."""
    got = _new_problems(request, tmp_path, "DELETE FROM section_licensing WHERE sid IN (SELECT s.sid FROM item i JOIN "
                        "item_section s ON s.ord = i.ord WHERE i.item_id = 'gnis:17550' AND s.sid NOT IN (SELECT sid FROM "
                        "not_classified_section))")
    assert any(p.startswith("Li5a: Coal Creek above the old MF&M Railway bridge is the Elk River's Class II") for p in got)


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
