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
    # the two Thompson wordings (named stretches) and the two Babine Lake wordings (mouths with a radius): nothing else
    assert sorted(got) == sorted(x + "#closed" for x in (OLD, LIVE, "6:babine-lake:babine-lake-excluding-tributaries~2",
                                                         "6:babine-lake:babine-lake-excluding-tributaries~3"))
    ids = {l["location_id"] for l in stored}
    assert not (set(got) & ids) and all(e["derived_from"] in ids for e in got.values())   # new ids only, each from a real row
    # a list with no closed / no-fishing words, or of neither shape, derives nothing
    plain = {"location_id": "1:a:b", "water_id": "1:a", "source_text": {"specific_area": "the river, including:", "excludes": ["Side Creek"]}}
    assert K.derive([plain]) == {}


def test_an_exclusion_with_no_name_keeps_its_whole_text():
    loc = {"location_id": "1:a:b", "water": "A River", "source_text": {
        "specific_area": "the river. Closed areas:", "excludes": ["the pool below the falls", "Upper - above the dam"]}}
    got = K.areas(loc)
    assert [(a["kind"], a["name"], a["words"]) for a in got] == [("unknown", "area 1", "the pool below the falls"),
                                                                   ("stretch", "Upper", "above the dam")]
    assert got[1]["key"] == K.piece_key("Upper - above the dam") == K.piece_key("  upper -  ABOVE the dam ")   # case and spacing aside
    assert K.piece_key("Upper - above the weir") != got[1]["key"]                                                # a rewording is another key
    e = K.closure_entry(loc)
    assert e["rule"]["verbatim"] == "Closed" and "when" not in e["rule"]          # the words say "closed", name no season: none is claimed


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
    monkeypatch.setenv("CURATION_DFO_CLOSED_GEOMETRY", str(tmp_path / "none.json"))
    got = BD.closure_view(LIVE)
    assert got["id"] == LIVE + "#closed" and got["proposed"] is False and got["rule"]["verbatim"] == "No fishing, year-round"
    assert [(a["name"], a["state"], a["ends"], a["metres"]) for a in got["areas"]] == [
        ("Deadman", "unplaced", [], None), ("Juniper", "unplaced", [], None), ("Ashcroft", "unplaced", [], None)]
    # a circle on file is served with its ring at true radius — and ONLY while the live page names its item
    live = BD.closure_view("6:babine-lake:babine-lake-excluding-tributaries~3")
    assert all(a["state"] == "unplaced" and "ring" not in a for a in live["areas"])
    morrison = next(a for a in live["areas"] if a["name"] == "Morrison Creek")
    pinkut_key = next(a["key"] for a in BD.closure_view("6:babine-lake:babine-lake-excluding-tributaries~2")["areas"] if a["name"] == "Pinkut Creek")
    g = tmp_path / "geo.json"
    rec = lambda key, name: {"key": key, "water_id": "6:babine-lake", "name": name, "item": name, "kind": "circle",    # noqa: E731
                             "on_item": "wbk:329026676", "centre": [-126.3, 55.1], "radius_m": 400}
    g.write_text(json.dumps({"pieces": [rec(morrison["key"], "Morrison Creek"), rec(pinkut_key, "Pinkut Creek")]}))
    monkeypatch.setenv("CURATION_DFO_CLOSED_GEOMETRY", str(g))
    old = {a["name"]: a for a in BD.closure_view("6:babine-lake:babine-lake-excluding-tributaries~2")["areas"]}
    assert old["Morrison Creek"]["state"] == "placed" and len(old["Morrison Creek"]["ring"]) == 73
    assert old["Pinkut Creek"]["state"] == "not live" and "ring" not in old["Pinkut Creek"]      # DFO's live page dropped Pinkut Creek
    g.write_text(json.dumps({"pieces": [{**rec(morrison["key"], "Morrison Creek"), "radius_m": -1}]}))
    assert "not the schema" in BD.closure_view("6:babine-lake:babine-lake-excluding-tributaries~3")["error"]
    assert BD.closure_view("1:nanaimo-river:whole") is None
    f.write_text("{not json")
    assert "cannot be read" in BD.closure_view(LIVE)["error"]


# ---- one model, two shapes; never free-standing (owner 2026-11-10)

BABINE_LIVE = "6:babine-lake:babine-lake-excluding-tributaries~3"
BABINE_OLD = "6:babine-lake:babine-lake-excluding-tributaries~2"


def test_babine_lakes_mouths_are_circles_of_dfos_radius_and_its_rule_is_what_the_words_say():
    got = K.derive(_stored())
    b = got[BABINE_LIVE + "#closed"]
    circles = [a for a in b["areas"] if a["kind"] == "circle"]
    assert len(circles) == 12 and {a["radius_m"] for a in circles} == {400.0}
    assert circles[0]["name"] == "Morrison Creek" and circles[0]["words"] == "within a 400 m radius of the mouth of Morrison Creek"
    assert [a["kind"] for a in b["areas"] if a["kind"] != "circle"] == ["line"]          # "closed east of a line from Gullwing Creek…"
    # DFO's words exclude these waters and call them closed; they name no season and do not say "no fishing"
    assert b["rule"]["verbatim"] == "Closed" and "when" not in b["rule"]
    assert len([a for a in got[BABINE_OLD + "#closed"]["areas"] if a["kind"] == "circle"]) == 13     # the older wording also named Pinkut Creek


def test_a_circle_is_drawn_at_true_ground_radius():
    """within 1 % at Babine Lake's latitude, in every direction (a degree buffer is 43 % short east-west there)"""
    from pyproj import Geod
    g = Geod(ellps="WGS84")
    centre = (-126.0, 55.0)
    ring = K.circle_polygon(centre, 400.0)
    assert ring[0] == ring[-1] and len(ring) == 73
    ds = [g.inv(centre[0], centre[1], x, y)[2] for x, y in ring[:-1]]
    assert all(abs(d - 400.0) <= 4.0 for d in ds), (min(ds), max(ds))
    assert max(abs(x - centre[0]) for x, _y in ring) > 1.5 * max(abs(y - centre[1]) for _x, y in ring)   # wider in degrees east-west


def test_the_stored_shape_of_a_piece_is_one_schema_with_a_kind():
    import pytest
    base = {"key": "0123456789ab", "water_id": "6:babine-lake", "name": "Morrison Creek", "item": "Morrison Creek", "on_item": "wbk:1"}
    K.PieceGeometry.model_validate({**base, "kind": "circle", "centre": [-126.4, 55.2], "radius_m": 400})
    K.PieceGeometry.model_validate({**base, "kind": "stretch", "points": [{"kind": "split", "split_id": "a__b"}, {"kind": "new", "split_id": "n1"}]})
    for bad in ({"kind": "circle", "centre": [-126.4, 55.2]},                                   # no radius
                {"kind": "circle", "centre": [55.2, -126.4], "radius_m": 400},                  # lat, lon swapped
                {"kind": "circle", "centre": [-126.4, 55.2], "radius_m": 400, "points": [{"kind": "split", "split_id": "a"}]},
                {"kind": "stretch", "points": [{"kind": "split", "split_id": "a"}]},            # one end
                {"kind": "stretch", "centre": [-126.4, 55.2], "radius_m": 400},
                {"kind": "polygon", "centre": [-126.4, 55.2], "radius_m": 400}):
        with pytest.raises(ValueError):
            K.PieceGeometry.model_validate({**base, **bad})


def _row(items, *, lid="6:x:row", status="active", radius=400):
    return {"location_id": lid, "water": "X Lake", "water_id": "6:x", "status": status, "source_text": {
        "specific_area": f"X Lake excluding those waters within a {radius} m radius of the following tributary streams: Also closed "
                         "east of a line.", "excludes": list(items)}}


def test_entry_b_is_never_free_standing_item_removed_clause_removed_restored_reworded():
    """owner 2026-11-10: "circles should only show up when that entry is in the dfo page too (if they remove it circles
    should go away)" — per item; the geometry stays on file, never drawn while not live"""
    first = K.closure_entry(_row(["A Creek", "B Creek"]))
    geo = [{"key": a["key"], "water_id": "6:x", "name": a["name"], "item": a["item"], "kind": "circle", "on_item": "wbk:1",
            "centre": [-126.0, 55.0], "radius_m": 400.0} for a in first["areas"] if a["kind"] == "circle"]
    state = lambda entry, **kw: {a["name"]: a["state"] for a in K.attach(entry, geo, **kw)}          # noqa: E731
    assert state(first) == {"A Creek": "placed", "B Creek": "placed", "east of a line": "unplaced"}
    # ITEM REMOVED: DFO drops one creek — only that circle goes; its geometry stays on file, not live
    one = K.closure_entry(_row(["A Creek"]))
    got = K.attach(one, geo)
    assert {a["name"]: a["state"] for a in got}["B Creek"] == "not live" and {a["name"]: a["state"] for a in got}["A Creek"] == "placed"
    assert next(a for a in got if a["name"] == "B Creek")["geometry"]["centre"] == [-126.0, 55.0]      # kept, to come back
    # CLAUSE REMOVED: the live page has no such list at all — an older wording's pieces are all not live
    assert set(state(first, live=set()).values()) == {"not live"}
    assert K.derive([{**_row([]), "source_text": {"specific_area": "X Lake.", "excludes": []}}]) == {}   # and no entry B is derived
    # ITEM RESTORED: the words are back — placed again, nothing re-curated
    assert state(K.closure_entry(_row(["A Creek", "B Creek"])))["B Creek"] == "placed"
    # an older wording shows a piece only while a LIVE row names it
    old = K.closure_entry(_row(["A Creek", "B Creek"], lid="6:x:old", status="dormant"))
    live = K.live_keys({"l#closed": one, "o#closed": old}, "6:x")
    assert state(old, live=live) == {"A Creek": "placed", "B Creek": "not live", "east of a line": "placed" if False else "unplaced"}
    # ITEM REWORDED: 400 m becomes 500 m — "changed, check again"-kind flags, never silently kept
    wider = K.attach(K.closure_entry(_row(["A Creek", "B Creek"], radius=500)), geo)
    a = next(x for x in wider if x["name"] == "A Creek")
    assert a["state"] == "radius" and "500 m" in a["note"] and "400 m" in a["note"]
    renamed = K.closure_entry({"location_id": "3:t:r", "water": "T River", "water_id": "3:t", "source_text": {
        "specific_area": "no fishing in the following areas:", "excludes": ["Deadman - from the signs to the NEW rail bridge"]}})
    geo_t = [{"key": K.piece_key("Deadman - from the signs to the rail bridge"), "water_id": "3:t", "name": "Deadman",
              "item": "Deadman - from the signs to the rail bridge", "kind": "stretch", "on_item": "gnis:1",
              "points": [{"kind": "split", "split_id": "a__b"}, {"kind": "split", "split_id": "a__c"}]}]
    ch = K.attach(renamed, geo_t)[0]
    assert ch["state"] == "changed" and "NEW rail bridge" in ch["note"]
