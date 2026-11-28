"""T573: a DFO row that names closed areas inside its words becomes two entries — entry B (the closure) is derived
from the stored rows, with no fetch and nothing written to the places files or the feed."""
from __future__ import annotations

from pipeline.regs.dfo_salmon import closures as K
from pipeline.regs.review import dfo as D

LIVE = "3:thompson-river:from-kamloops-lake-outlet-downstre~2"
OLD = "3:thompson-river:from-kamloops-lake-outlet-downstre"
SHUSWAP_LIVE = "8:shuswap-river:middle-between-shuswap-falls-and-m~2"
SHUSWAP_OLD = "8:shuswap-river:middle-between-shuswap-falls-and-m"


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
    # the two Thompson wordings (named stretches), the two Babine Lake wordings (mouths with a radius) and the two middle
    # Shuswap wordings (an inline "except closed from … to …", T743): nothing else
    assert sorted(got) == sorted(x + "#closed" for x in (OLD, LIVE, "6:babine-lake:babine-lake-excluding-tributaries~2",
                                                         "6:babine-lake:babine-lake-excluding-tributaries~3",
                                                         SHUSWAP_OLD, SHUSWAP_LIVE))
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
    from pipeline.tests.conftest import curated_root, generated_root
    # a curated tree this test builds, with a review that has nothing in it: "not placed" is proven with no confirmed
    # closed area on file, never on the live `closed_areas.json` (which the owner's accepts fill: Thompson, 2026-11-15)
    curated_root(monkeypatch, tmp_path / "curated")
    generated_root(monkeypatch, tmp_path / "generated")      # T1192: the derived files are this test's, in a whole copy
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "tools" / "curation-review" / "backend"))
    sys.modules.pop("dfo", None)
    import dfo as BD
    sys.modules.pop("dfo", None)
    f = BD._closures_path()
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps({"entries": K.derive(_stored())}))
    assert not BD._closed_geometry_path().exists()           # no geometry on file yet
    got = BD.closure_view(LIVE)
    assert got["id"] == LIVE + "#closed" and got["proposed"] is False and got["rule"]["verbatim"] == "No fishing, year-round"
    assert [(a["name"], a["state"], a["ends"], a["metres"]) for a in got["areas"]] == [
        ("Deadman", "unplaced", [], None), ("Juniper", "unplaced", [], None), ("Ashcroft", "unplaced", [], None)]
    # a circle on file is served with its ring at true radius — and ONLY while the live page names its item
    live = BD.closure_view("6:babine-lake:babine-lake-excluding-tributaries~3")
    assert all(a["state"] == "unplaced" and "ring" not in a for a in live["areas"] if a["live"])
    # GENERAL RULE (owner 2026-11-10, Pinkut Creek): an item only an OLDER wording names is listed on the live row too —
    # not live, reviewable, with nothing on file yet
    [kept] = [a for a in live["areas"] if not a["live"]]
    assert (kept["name"], kept["state"], kept["held"], kept["target"]) == (
        "Pinkut Creek", "not live", "unplaced", "6:babine-lake#" + kept["key"])
    morrison = next(a for a in live["areas"] if a["name"] == "Morrison Creek")
    pinkut_key = next(a["key"] for a in BD.closure_view("6:babine-lake:babine-lake-excluding-tributaries~2")["areas"] if a["name"] == "Pinkut Creek")
    g = BD._closed_geometry_path()
    rec = lambda key, name: {"key": key, "water_id": "6:babine-lake", "name": name, "item": name, "kind": "circle",    # noqa: E731
                             "on_item": "wbk:329026676", "centre": [-126.3, 55.1], "radius_m": 400}
    g.write_text(json.dumps({"pieces": [rec(morrison["key"], "Morrison Creek"), rec(pinkut_key, "Pinkut Creek")]}))
    old = {a["name"]: a for a in BD.closure_view("6:babine-lake:babine-lake-excluding-tributaries~2")["areas"]}
    assert old["Morrison Creek"]["state"] == "placed" and len(old["Morrison Creek"]["ring"]) == 73
    # DFO's live page dropped Pinkut Creek: NOT LIVE (never bound), yet its circle is served — flagged `live: false` — so
    # the review app can draw it muted and the owner can check it ("both mapped out in case they go back")
    pk = old["Pinkut Creek"]
    assert pk["state"] == "not live" and pk["live"] is False and pk["held"] == "placed" and len(pk["ring"]) == 73
    on_live = {a["name"]: a for a in BD.closure_view("6:babine-lake:babine-lake-excluding-tributaries~3")["areas"]}
    assert on_live["Pinkut Creek"]["live"] is False and on_live["Pinkut Creek"]["piece"]["centre"] == [-126.3, 55.1]
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
    # … so the derivation alone reads "Closed"; what it MEANS is the owner's reading (2026-11-10: "babine is closed in the
    # circles (no fishing salmon)"), recorded as his and shown beside DFO's own word — never passed off as DFO's
    assert b["rule"]["dfo_words"] == "Closed" and "when" not in b["rule"]
    assert b["rule"]["verbatim"] == "No fishing for salmon" and "owner's reading, 2026-11-10" in b["rule"]["reading"]
    thompson = next(e for k, e in got.items() if k.startswith("3:thompson-river"))
    assert "reading" not in thompson["rule"] and thompson["rule"]["verbatim"] == "No fishing, year-round"
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


def test_an_item_only_an_older_wording_names_is_reviewable_not_live_and_live_again_with_no_recuration():
    """owner 2026-11-10 (Pinkut Creek): "we should have both mapped out in case they go back to old wording" """
    old, new = _row(["A Creek", "B Creek"]), _row(["A Creek"])
    entries = {"old#closed": K.closure_entry({**old, "location_id": "x:old", "status": "dormant"}),
               "new#closed": K.closure_entry({**new, "location_id": "x:new"})}
    wid = entries["new#closed"]["water_id"]
    b_key = next(a["key"] for a in entries["old#closed"]["areas"] if a["name"] == "B Creek")
    geo = [{"key": b_key, "water_id": wid, "name": "B Creek", "item": "B Creek", "kind": "circle", "on_item": "wbk:1",
            "centre": [-126.0, 55.0], "radius_m": 400}]
    others = K.water_areas(entries, wid)
    # with nothing on file: listed on the LIVE row, not live, to be placed
    b = next(a for a in K.attach(entries["new#closed"], [], live=K.live_keys(entries, wid), others=others) if a["name"] == "B Creek")
    assert (b["state"], b["held"], b["live"], b["geometry"]) == ("not live", "unplaced", False, None)
    # curated while not live: its geometry is offered, still not live
    b = next(a for a in K.attach(entries["new#closed"], geo, live=K.live_keys(entries, wid), others=others) if a["name"] == "B Creek")
    assert (b["state"], b["held"], b["live"]) == ("not live", "placed", False) and b["geometry"]["centre"] == [-126.0, 55.0]
    # DFO goes back to the old words: live and placed from the same record, nothing re-curated
    back = {"new#closed": K.closure_entry({**old, "location_id": "x:new"})}
    b = next(a for a in K.attach(back["new#closed"], geo, live=K.live_keys(back, wid), others=K.water_areas(back, wid)) if a["name"] == "B Creek")
    assert (b["state"], b["live"]) == ("placed", True)


def test_the_centre_is_where_the_creeks_own_channel_meets_the_shore_never_the_in_lake_connector():
    """owner 2026-11-10: "where stream meets lake (not including the connecting stream)" """
    from shapely.geometry import LineString, Polygon
    from pipeline.regs.dfo_salmon import closure_mouths as CM
    lake = Polygon([(0, 0), (1000, 0), (1000, 1000), (0, 1000)])
    other = Polygon([(3000, 0), (3500, 0), (3500, 500), (3000, 500)])
    segs = [({"GNIS_NAME": "A Creek", "WATERBODY_KEY": None, "BLUE_LINE_KEY": 1, "STREAM_ORDER": 2}, LineString([(-800, 500), (0, 500)])),
            ({"GNIS_NAME": "A Creek", "WATERBODY_KEY": 9, "BLUE_LINE_KEY": 1, "STREAM_ORDER": 2}, LineString([(0, 500), (600, 500)])),   # in-lake connector
            ({"GNIS_NAME": "Far Creek", "WATERBODY_KEY": None, "BLUE_LINE_KEY": 2, "STREAM_ORDER": 1}, LineString([(3250, 900), (3250, 500)])),
            ({"GNIS_NAME": None, "WATERBODY_KEY": None, "BLUE_LINE_KEY": 3, "STREAM_ORDER": 3}, LineString([(500, 1400), (500, 1000)])),
            ({"GNIS_NAME": "Cross Creek", "WATERBODY_KEY": None, "BLUE_LINE_KEY": 4, "STREAM_ORDER": 5}, LineString([(600, 1400), (600, 1000)]))]
    a = CM.shore_mouth(lake, 9, segs, "A Creek")
    assert a["confidence"] == "high" and (round(a["centre"].x), round(a["centre"].y)) == (0, 500)       # the shore, not (600, 500)
    far = CM.shore_mouth(lake, 9, segs, "Far Creek", [("Other Lake", other)])
    assert far["confidence"] == "medium" and round(far["centre"].y) == 500 and "Other Lake" in far["why"]
    assert "looked" in CM.shore_mouth(lake, 9, segs, "Nowhere Creek")
    hint = CM.shore_mouth(lake, 9, segs, "Bay Creek", hint={"near": [550, 1000], "why": "a bay of that name"}, to_albers=lambda x, y: (x, y))
    assert hint["confidence"] == "low" and hint["stream"] == 3, "the unnamed stream at the hint, never another named creek"


def test_a_creek_dfo_names_differently_is_placed_only_by_a_hint_naming_the_stream_or_its_watershed_code():
    """T796 (2026-11-16): DFO's Six Mile Creek is the network's Gullwing Creek (`as`); its Sockeye Creek is an unnamed
    stream found by DFO's census site's FWA code (`fwa_code`) — the stream itself, never one of its tributaries"""
    from shapely.geometry import LineString, Polygon
    from pipeline.regs.dfo_salmon import closure_mouths as CM
    lake = Polygon([(0, 0), (1000, 0), (1000, 1000), (0, 1000)])
    segs = [({"GNIS_NAME": "Gullwing Creek", "WATERBODY_KEY": None, "BLUE_LINE_KEY": 5, "STREAM_ORDER": 4,
              "FWA_WATERSHED_CODE": "400-1-2-000000"}, LineString([(-500, 200), (0, 200)])),
            ({"GNIS_NAME": None, "WATERBODY_KEY": None, "BLUE_LINE_KEY": 6, "STREAM_ORDER": 5,
              "FWA_WATERSHED_CODE": "400-1-3-000000-000000"}, LineString([(1500, 700), (1000, 700)])),
            ({"GNIS_NAME": None, "WATERBODY_KEY": None, "BLUE_LINE_KEY": 7, "STREAM_ORDER": 2,      # its tributary
              "FWA_WATERSHED_CODE": "400-1-3-123456-000000"}, LineString([(1500, 300), (1000, 300)]))]
    six = CM.shore_mouth(lake, 9, segs, "Six Mile Creek", hint={"as": "Gullwing Creek", "confidence": "medium", "why": "w"})
    assert six["confidence"] == "medium" and six["stream"] == 5 and round(six["centre"].y) == 200
    sock = CM.shore_mouth(lake, 9, segs, "Sockeye Creek", hint={"fwa_code": "400-1-3", "confidence": "high", "why": "w"})
    assert sock["stream"] == 6 and round(sock["centre"].y) == 700, "the coded stream itself, never its tributary"
    assert "looked" in CM.shore_mouth(lake, 9, segs, "Nowhere Creek", hint={"fwa_code": "400-9-9", "why": "w"})


# --------------------------------------------------------------------------- T743: an inline "except closed from A to B"

def test_the_middle_shuswaps_inline_closed_stretch_is_entry_b_with_dfos_points_as_printed():
    """owner 2026-11-15: "(middle) between Shuswap Falls and Mabel Lake except closed from the water survey station
    (located at 50°19.105' N …) … to WTFB signs (located at 50°18.181' N …) one km down stream" — the scrape stored no
    exclusion (the clause is not a bullet); it is read from the words as one stretch, both ends with their coordinates"""
    got = K.derive(_stored())
    live, old = got[SHUSWAP_LIVE + "#closed"], got[SHUSWAP_OLD + "#closed"]
    assert live["status"] == "active" and old["status"] == "dormant" and live["water_id"] == old["water_id"] == "8:shuswap-river"
    # the words say "closed" and name no season: the rule claims none (as Babine Lake's)
    assert live["rule"]["verbatim"] == "Closed" and "when" not in live["rule"] and live["rule"]["take"] == 0
    assert live["title"] == "Shuswap River: closed in 1 named area"
    [a] = live["areas"]
    assert a["kind"] == "stretch" and a["inline"] is True and a["name"] == "the water survey station to WTFB signs"
    assert a["item"].startswith("closed from the water survey station (located at 50°19.105' N")
    assert a["item"].endswith("to WTFB signs (located at 50°18.181' N and 118°49.105' W) one km down stream")
    first, second = a["ends"]
    assert first["words"].endswith("approximately 200 m down stream of Shuswap Falls Hatchery")
    assert second["words"] == "WTFB signs (located at 50°18.181' N and 118°49.105' W) one km down stream"
    # degrees + decimal minutes, exactly, as printed (never reordered or corrected: the mismatch is the owner's call)
    assert first["coords"] == [{"printed": "50°19.105' N 118°48.857' W", "lat": 50.3184167, "lon": -118.8142833}]
    assert second["coords"] == [{"printed": "50°18.181' N 118°49.105' W", "lat": 50.3030167, "lon": -118.8184167}]
    # the dormant wording's typing slips ("( located", "200m") do not make it another piece: one key across both
    assert old["areas"][0]["key"] == a["key"] and old["areas"][0]["item"] != a["item"]
    assert a["key"] in K.live_keys(got, "8:shuswap-river")


def test_only_an_except_closed_or_no_fishing_from_to_clause_is_read_inline():
    def row(words, excludes=()):
        return {"location_id": "1:a:b", "water_id": "1:a", "water": "A River",
                "source_text": {"specific_area": words, "excludes": list(excludes)}}
    # pointers to other rows, a "with exception of" note, and the Stave's spawning channels (rows of their own) derive nothing
    for words in ("including tributaries, except in the area described below.",
                  "downstream of Parker Creek, except in the areas and times described below.",
                  "from the confluence with Crag Creek downstream to the tidal boundary, except for the canyon as listed below.",
                  "upstream of CNR bridge at Terrace to a point above the confluence with the Babine River, excluding the areas below",
                  "from a line … downstream, with exception of “no fishing for salmon” closure area identified above.",
                  "downstream of B.C. Hydro Dam to the CPR Railway Bridge; except you shall not fish for salmon in that portion "
                  "on the east side of the Stave River, known as the Ruskin Spawning Channel from the inlet near the dam, "
                  "downstream to the boat ramp crossing"):
        assert K.inline_items(row(words)) == [] and K.derive([row(words)]) == {}, words
    # "no fishing" reads as no fishing; "downstream to" and a trailing Note are handled; an end without a point has none
    got = K.derive([row("the river except no fishing from the dam downstream to the old bridge. Note: barbless.")])
    [a] = got["1:a:b#closed"]["areas"]
    assert got["1:a:b#closed"]["rule"]["verbatim"] == "No fishing"
    assert a["item"] == "no fishing from the dam downstream to the old bridge"
    assert [e["words"] for e in a["ends"]] == ["the dam", "the old bridge"] and all(e["coords"] == [] for e in a["ends"])
    assert a["name"] == "the dam to the old bridge"
    # a bulleted list is the row's list: an inline clause beside it is not read a second time
    assert K.inline_items(row("the river except closed from A to B, and no fishing in the following areas:",
                              ["Upper - above the dam"])) == []


def test_printed_coordinates_convert_exactly():
    assert K.coords_in("at 50°19.105' N and 118°48.857' W") == [
        {"printed": "50°19.105' N 118°48.857' W", "lat": 50.3184167, "lon": -118.8142833}]
    assert K.coords_in("49°41'30\" N 123°58'27\" W") == [
        {"printed": "49°41'30\" N 123°58'27\" W", "lat": round(49 + 41 / 60 + 30 / 3600, 7), "lon": round(-(123 + 58 / 60 + 27 / 3600), 7)}]
    assert K.coords_in("50°46.893' N only") == [{"printed": "50°46.893' N", "lat": 50.78155, "lon": None}]
    assert K.coords_in("no point here") == []


def test_rows_with_a_bulleted_list_derive_what_they_did_before():
    """T743 adds inline stretches only: the Thompson's and Babine Lake's pieces carry no `inline` and keep their keys"""
    got = K.derive(_stored())
    for eid, e in got.items():
        if not eid.startswith("8:shuswap-river"):
            assert not any(a.get("inline") for a in e["areas"]), eid
    live = next(l for l in _stored() if l["location_id"] == LIVE)
    assert [a["key"] for a in got[LIVE + "#closed"]["areas"]] == [K.piece_key(x) for x in K._excludes(live)]
