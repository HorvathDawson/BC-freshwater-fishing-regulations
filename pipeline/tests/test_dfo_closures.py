"""T573: a DFO row that names closed areas inside its words becomes two entries — entry B (the closure) is derived
from the stored rows, with no fetch and nothing written to the places files or the feed."""
from __future__ import annotations

from pipeline.regs.dfo_salmon import closures as K
from pipeline.regs.review import dfo as D

LIVE = "3:thompson-river:from-kamloops-lake-outlet-downstre~2"
OLD = "3:thompson-river:from-kamloops-lake-outlet-downstre"


def _stored():
    return [l for _s, l, _w in D.locations(D.default_dir()).values()]


def test_the_thompson_row_gives_a_closure_entry_with_dfos_words_for_each_area():
    got = K.derive(_stored())
    b = got[LIVE + "#closed"]
    assert b["derived_from"] == LIVE and b["kind"] == "closure" and b["water_id"] == "3:thompson-river"
    assert b["rule"]["take"] == 0 and b["rule"]["may_target"] is False and b["rule"]["verbatim"] == "No fishing, year-round"
    assert b["rule"]["when"]["dates"] == [{"from_month": 1, "from_day": 1, "to_month": 12, "to_day": 31}]
    assert [a["name"] for a in b["areas"]] == ["Deadman", "Juniper", "Ashcroft"]
    assert b["areas"][0]["words"].startswith("from the fishing boundary signs approximately 1 km downstream of the Hwy No. 1 Bridge at Savona")
    assert "50°46.893' N" in b["areas"][1]["words"] and b["areas"][2]["words"].endswith("Hwy 97C bridge in Ashcroft.")
    # the older wording only says "except:" — it takes the closure from the live wording's identical list
    old = got[OLD + "#closed"]
    assert old["said_by"] == LIVE and [a["name"] for a in old["areas"]] == ["Deadman", "Juniper", "Ashcroft"]


def test_only_rows_whose_words_say_no_fishing_are_derived_and_no_place_id_changes():
    stored = _stored()
    got = K.derive(stored)
    assert sorted(got) == [OLD + "#closed", LIVE + "#closed"]          # Babine's 400 m radius exclusions are not closures
    ids = {l["location_id"] for l in stored}
    assert not (set(got) & ids) and all(e["derived_from"] in ids for e in got.values())   # new ids only, each from a real row
    babine = [l for l in stored if "babine-lake" in l["location_id"] and K._excludes(l)]
    assert babine and not any(K.says_closed(l) for l in babine)


def test_an_exclusion_with_no_name_keeps_its_whole_text():
    loc = {"location_id": "1:a:b", "water": "A River", "source_text": {
        "specific_area": "the river. Closed areas:", "excludes": ["the pool below the falls", "Upper - above the dam"]}}
    assert K.areas(loc) == [{"name": "area 1", "words": "the pool below the falls"}, {"name": "Upper", "words": "above the dam"}]
    e = K.closure_entry(loc)
    assert e["rule"]["verbatim"] == "No fishing" and "when" not in e["rule"]      # no "year-round" in the words: no season claimed


def test_the_review_app_serves_entry_b_and_says_when_it_is_not_placed(tmp_path, monkeypatch):
    """`dfo.closure_view`: entry B of a place from the derived file; with no proposal placing its areas each area keeps
    DFO's words and has no ends (never invented); a file that cannot be read is said"""
    import json
    import sys
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "curation-review" / "backend"))
    sys.modules.pop("dfo", None)
    import dfo as BD
    sys.modules.pop("dfo", None)
    f = tmp_path / "closures.json"
    f.write_text(json.dumps({"entries": K.derive(_stored())}))
    monkeypatch.setenv("CURATION_DFO_CLOSURES", str(f))
    monkeypatch.setattr(BD, "fix_proposal", lambda lid: None)
    got = BD.closure_view(LIVE)
    assert got["id"] == LIVE + "#closed" and got["proposed"] is False and got["rule"]["verbatim"] == "No fishing, year-round"
    assert [(a["name"], a["ends"], a["metres"]) for a in got["areas"]] == [("Deadman", [], None), ("Juniper", [], None), ("Ashcroft", [], None)]
    assert BD.closure_view("1:nanaimo-river:whole") is None
    f.write_text("{not json")
    assert "cannot be read" in BD.closure_view(LIVE)["error"]
