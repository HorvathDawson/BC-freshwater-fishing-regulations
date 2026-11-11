"""T573 (owner 2026-11-10): ENTRY B IS SAVEABLE — one mark per closed area (scope `closed_area`), written through to a
curated file of its own (`dfo_salmon/closed_areas.json`).

  the mark      a confirm carries the placement it confirms (a stretch, a circle or a line; its water and key are the
                mark's target); "something's wrong" carries none; no other mark may carry one
  the prints    DFO's words for the item, from any stored wording (an older-only item is reviewable; live or not is
                not in the prints, so a mark survives the item leaving DFO's page and returning)
  the file      only confirmed areas; a later "wrong" takes its area out; a moved circle / another radius is what he saved
  nothing else  no places file changes by a byte, and no other mark's state moves
  the app       POST /api/marks writes the file in the request; the place's view then shows it confirmed; a new
                cut placed by hand is resolved on the water and stamped
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from pipeline.regs.dfo_salmon import closures as K
from pipeline.regs.review import apply as AP
from pipeline.regs.review import closed_areas as CA
from pipeline.regs.review import export as E
from pipeline.regs.review import marks as M
from pipeline.tests.test_review_write_through import wt  # noqa: F401  (the app over a sandbox, write-through on)
from pipeline.tests import under_test

SHA = "0" * 64
BABINE = "6:babine-lake"
ROW = "6:babine-lake:babine-lake-excluding-tributaries~3"
THOMPSON_ROW = "3:thompson-river:from-kamloops-lake-outlet-downstre~2"
CIRCLE = {"key": "aaaaaaaaaaa1", "water_id": BABINE, "name": "Morrison Creek", "item": "Morrison Creek", "kind": "circle",
          "on_item": "wbk:329026676", "centre": [-126.286131, 55.152225], "radius_m": 400.0}


def _mark(piece=None, verdict="verified", target=f"{BABINE}#aaaaaaaaaaa1", **kw) -> dict:
    return {"scope": "closed_area", "target": target, "verdict": verdict,
            "answers": {"P": "y" if verdict == "verified" else "n"}, "reviewer": "dawson", "date": "2026-11-10",
            "pdf_sha256": SHA, "piece": piece, "note": "" if verdict == "verified" else "the mouth is further east",
            "problem": None if verdict == "verified" else "wrong place",
            "prints": {"text": None, "records": [{"key": f"closed_area:{target}", "meaning": "Morrison Creek",
                                                  "binding": f"{BABINE} · circle · 400 m"}]}, **kw}


# --------------------------------------------------------------------------- the mark

def test_a_confirm_carries_its_placement_and_a_wrong_carries_none():
    assert M.Mark.model_validate(_mark(CIRCLE)).piece["radius_m"] == 400.0
    assert M.Mark.model_validate(_mark(None, "wrong")).piece is None
    with pytest.raises(ValueError, match="carries the placement it confirms"):
        M.Mark.model_validate(_mark(None))


def test_a_placement_of_another_area_or_on_another_scope_is_refused():
    with pytest.raises(ValueError, match="another area's"):
        M.Mark.model_validate(_mark({**CIRCLE, "key": "aaaaaaaaaaa2"}))
    with pytest.raises(ValueError, match="only a closed-area mark"):
        M.Mark.model_validate({**_mark(CIRCLE), "scope": "q154", "target": "1", "call": "pool",
                               "prints": {"text": None, "records": [{"key": "q154:1", "meaning": "m", "binding": "b"}]}})


def test_the_three_shapes_and_what_each_refuses():
    line = {**CIRCLE, "kind": "line", "centre": None, "radius_m": None, "line": [[-126.0, 55.0], [-126.01, 55.01]]}
    assert K.PieceGeometry.model_validate({k: v for k, v in line.items() if v is not None}).kind == "line"
    stretch = {"key": "aaaaaaaaaaa1", "water_id": BABINE, "name": "n", "item": "i", "kind": "stretch", "on_item": "gnis:1",
               "points": [{"kind": "split", "split_id": "a"}, {"kind": "new", "split_id": "n1"}],
               "new_splits": [{"ref": "n1", "anchor": "coord", "on_item": "gnis:1", "coord": [-126.0, 55.0]}]}
    assert K.PieceGeometry.model_validate(stretch).new_splits[0]["ref"] == "n1"
    with pytest.raises(ValueError, match="not among its new cuts"):
        K.PieceGeometry.model_validate({**stretch, "new_splits": []})
    with pytest.raises(ValueError, match="British Columbia"):
        K.PieceGeometry.model_validate({**CIRCLE, "centre": [55.15, -126.28]})
    with pytest.raises(ValueError):                                  # a new end is a real NewSplit in the mark
        M.Mark.model_validate(_mark({**stretch, "new_splits": [{"ref": "n1", "anchor": "nowhere"}]}))


# --------------------------------------------------------------------------- the prints: live rows only

def _loc(status, items):
    return {"location_id": f"{BABINE}:x-{status}", "water_id": BABINE, "water": "Babine Lake", "status": status,
            "text": "Babine Lake", "source_text": {"excludes": items}}


def test_prints_are_dfos_words_from_any_stored_wording_and_do_not_move_with_liveness():
    """owner 2026-11-10: an item only an older wording names is curated too; live or not is no part of the prints, so
    a mark survives the item leaving DFO's page and coming back"""
    real = CA.items()
    live = [t for t, r in real.items() if r["live"]]
    dead = [t for t, r in real.items() if not r["live"]]
    assert live and dead, "the stored rows hold a live closed area and one DFO dropped (Pinkut Creek)"
    assert CA.prints_of(live[0])["records"][0]["key"] == f"closed_area:{live[0]}"
    assert CA.prints_of(dead[0])["records"][0]["meaning"] == "Pinkut Creek" and CA.prints_of(f"{BABINE}#nope") is None
    old = {"location_id": "x:old", "water_id": "x", "water": "X Lake", "status": "dormant",
           "source_text": {"specific_area": "X Lake, excluding the waters within a 400 m radius of the following streams. Closed.",
                           "excludes": ["A Creek", "B Creek"]}}
    new = {**old, "location_id": "x:new", "status": "active", "source_text": {**old["source_text"], "excludes": ["A Creek"]}}
    b = next(t for t, r in CA.items([old, new]).items() if r["name"] == "B Creek")
    assert CA.items([old, new])[b]["live"] is False
    back = {**new, "source_text": old["source_text"]}
    assert CA.items([old, back])[b]["live"] is True and CA.prints_of(b, [old, new]) == CA.prints_of(b, [old, back])


# --------------------------------------------------------------------------- the file

def _decided(tmp_path, marks):
    p = tmp_path / "marks.jsonl"
    p.write_text("".join(M.line(M.Mark.model_validate(m)) for m in marks))
    return E.load(p)


def test_the_file_holds_only_what_is_confirmed_and_a_later_wrong_takes_it_out(tmp_path):
    rows = CA.build(_decided(tmp_path, [_mark(CIRCLE)]))
    assert [(r["key"], r["kind"], r["radius_m"], r["confirmed_by"]) for r in rows] == [("aaaaaaaaaaa1", "circle", 400.0, "dawson")]
    doc = json.loads(CA.document(rows))
    assert list(doc) == ["_about", "pieces"] and AP.rows_in(CA.document(rows)) == 1
    assert f"{BABINE}#aaaaaaaaaaa1" in AP.row_keys(CA.document(rows))
    assert CA.build(_decided(tmp_path, [_mark(CIRCLE), _mark(None, "wrong")])) == []
    moved = {**CIRCLE, "centre": [-126.28, 55.15], "radius_m": 250.0}
    [r] = CA.build(_decided(tmp_path, [_mark(CIRCLE), _mark(moved)]))
    assert (r["centre"], r["radius_m"]) == ([-126.28, 55.15], 250.0)


def test_the_writer_writes_its_own_file_and_nothing_else(tmp_path):
    assert E.DOMAINS["closed_areas"] == ("closed_area",) and "closed_areas" in AP.WRITERS
    out = AP.WRITERS["closed_areas"](_decided(tmp_path, [_mark(CIRCLE)]))
    assert list(out) == [CA.path()] and CA.path().name == "closed_areas.json"
    assert CA.path().parent == Path(under_test.curated().dfo_salmon.places).parent
    # the file is born with his first confirm: nothing confirmed and no file = nothing to write (the landed check
    # must not read a missing file as "marks not landed")
    assert not CA.path().exists() and AP.WRITERS["closed_areas"](_decided(tmp_path, [])) == {}
    assert not CA.path().is_relative_to(Path(under_test.curated().dfo_salmon.places)), "never inside the places files"


# --------------------------------------------------------------------------- the app (sandbox, write-through on)

def _tree(d: Path) -> dict:
    return {str(p.relative_to(d)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(d.rglob("*")) if p.is_file()}


def _area(client, row, pick):
    c = client.get(f"/api/dfo/locations/{row}").json()["closure"]
    assert not c.get("error"), c
    return next(a for a in c["areas"] if pick(a))


@pytest.mark.needs_atlas
def test_confirming_a_circle_writes_the_file_in_the_request_and_no_places_file(wt):  # noqa: F811
    client, places = wt["client"], Path(under_test.curated().dfo_salmon.places)
    before = _tree(places)
    a = _area(client, ROW, lambda a: a["kind"] == "circle" and a["state"] == "placed")
    assert a["confirmed"] is False and a["mark"] is None and a["piece"]["kind"] == "circle"
    r = client.post("/api/marks", json={"scope": "closed_area", "target": a["target"], "answers": {"P": "y"},
                                               "piece": a["piece"], "reviewer": "test"})
    assert r.status_code == 200, r.text
    w = r.json()["write_through"]
    assert w["domain"] == "closed_areas" and w["state"] == "done", w
    [row] = json.loads(CA.path().read_text())["pieces"]
    assert (row["key"], row["centre"], row["radius_m"]) == (a["key"], a["piece"]["centre"], a["piece"]["radius_m"])
    assert _tree(places) == before, "a closed-area save changes no places file"
    now = _area(client, ROW, lambda x: x["key"] == a["key"])
    assert now["confirmed"] is True and now["mark"]["verdict"] == "verified" and now["ring"]
    # he moves the centre and sets another radius: what he saved is what the file and the map hold
    moved = {**a["piece"], "centre": [a["piece"]["centre"][0] + 0.002, a["piece"]["centre"][1]], "radius_m": 250}
    assert client.post("/api/marks", json={"scope": "closed_area", "target": a["target"], "answers": {"P": "y"},
                                                  "piece": moved, "reviewer": "test"}).status_code == 200
    now = _area(client, ROW, lambda x: x["key"] == a["key"])
    assert now["radius_m"] == 250 and now["state"] == "radius", "the words say 400 m: his other radius stays flagged"
    # something's wrong: the area leaves the file, the proposal is shown again, his mark stays on it
    r = client.post("/api/marks", json={"scope": "closed_area", "target": a["target"], "answers": {"P": "n"},
                                               "problem": "wrong place", "note": "the mouth is further east",
                                        "reviewer": "test"})
    assert r.status_code == 200, r.text
    assert json.loads(CA.path().read_text())["pieces"] == []
    now = _area(client, ROW, lambda x: x["key"] == a["key"])
    assert now["confirmed"] is False and now["mark"]["verdict"] == "wrong" and now["radius_m"] == 400.0
    assert _tree(places) == before


@pytest.mark.needs_atlas
def test_placing_an_unplaced_mouth_by_hand_and_an_area_dfo_dropped(wt):  # noqa: F811
    client = wt["client"]
    a = _area(client, ROW, lambda a: a["kind"] == "circle" and a["state"] == "unplaced")
    assert a["piece"] is None
    on = client.get(f"/api/dfo/locations/{ROW}").json()["closure"]["on_item"]
    piece = {"key": a["key"], "water_id": BABINE, "name": a["name"], "item": a["item"], "kind": "circle",
             "on_item": on, "centre": [-126.0, 54.9], "radius_m": a["radius_m"]}
    pv = client.get(f"/api/dfo/locations/{ROW}/closed-area-preview", params={"piece": json.dumps(piece)})
    assert pv.status_code == 200 and pv.json()["ok"] and len(pv.json()["ring"]) > 30, pv.text
    assert client.get(f"/api/dfo/locations/{ROW}/closed-area-preview",
                      params={"piece": json.dumps({**piece, "radius_m": -1})}).status_code == 422
    r = client.post("/api/marks", json={"scope": "closed_area", "target": a["target"], "answers": {"P": "y"},
                                               "piece": piece, "reviewer": "test"})
    assert r.status_code == 200, r.text
    assert _area(client, ROW, lambda x: x["key"] == a["key"])["state"] == "placed"
    # an item only the OLDER wording names (Pinkut Creek) is confirmed like any other — and stays not live
    dead = _area(client, ROW, lambda x: not x["live"])
    assert dead["name"] == "Pinkut Creek" and dead["piece"]["kind"] == "circle"
    r = client.post("/api/marks", json={"scope": "closed_area", "target": dead["target"], "answers": {"P": "y"},
                                        "piece": dead["piece"], "reviewer": "test"})
    assert r.status_code == 200, r.text
    now = _area(client, ROW, lambda x: x["key"] == dead["key"])
    assert now["confirmed"] is True and now["live"] is False and now["state"] == "not live"
    r = client.post("/api/marks", json={"scope": "closed_area", "target": f"{BABINE}#aaaaaaaaaaa1", "answers": {"P": "n"},
                                        "problem": "wrong place", "note": "x", "reviewer": "test"})
    assert r.status_code == 422 and "no stored DFO wording names" in r.text
    r = client.post("/api/marks", json={"scope": "slough", "target": "1", "answers": {"P": "y"},
                                               "piece": piece, "reviewer": "test"})
    assert r.status_code == 422 and "only a closed-area mark" in r.text


@pytest.mark.needs_atlas
def test_a_stretch_end_placed_by_hand_is_resolved_on_the_water_and_stamped(wt):  # noqa: F811
    client = wt["client"]
    a = _area(client, THOMPSON_ROW, lambda a: a["name"] == "Deadman")
    assert a["piece"]["new_splits"], "a proposal's new end travels with the piece"
    at = a["ends"][0]["coord"]
    piece = {**a["piece"], "points": [{"kind": "new", "split_id": "hand-1"}, a["piece"]["points"][1]],
             "new_splits": [{"ref": "hand-1", "anchor": "coord", "on_item": a["piece"]["on_item"], "coord": at}]}
    pv = client.get(f"/api/dfo/locations/{THOMPSON_ROW}/closed-area-preview", params={"piece": json.dumps(piece)}).json()
    assert pv["ok"] and pv["metres"] and pv["ends"][0]["new"] and pv["ends"][1]["new"] is False
    # owner 2026-11-10 (T590): every new cut says its nearest existing cut on the same line, and whether it is near
    near = pv["ends"][0]["nearest"]
    assert near["split_id"] and near["metres"] > 200 and near["near"] is False, near
    hit = client.get(f"/api/dfo/locations/{THOMPSON_ROW}/anchor", params={"split": json.dumps(
        {"ref": "x", "anchor": "coord", "on_item": a["piece"]["on_item"], "coord": a["ends"][1]["coord"]})}).json()
    assert hit["ok"] and hit["nearest"]["near"] is True and hit["nearest"]["metres"] <= 200, \
        "a new cut clicked on an existing one (the CNR bridge) is told to use it"
    off = {**piece, "new_splits": [{**piece["new_splits"][0], "coord": [-120.0, 50.0]}]}
    assert client.get(f"/api/dfo/locations/{THOMPSON_ROW}/closed-area-preview", params={"piece": json.dumps(off)}).json()["ok"] is False
    r = client.post("/api/marks", json={"scope": "closed_area", "target": a["target"], "answers": {"P": "y"},
                                               "piece": off, "reviewer": "test"})
    assert r.status_code == 422 and "does not land on the water" in r.text
    r = client.post("/api/marks", json={"scope": "closed_area", "target": a["target"], "answers": {"P": "y"},
                                               "piece": piece, "reviewer": "test"})
    assert r.status_code == 200, r.text
    [row] = json.loads(CA.path().read_text())["pieces"]
    [n] = row["new_splits"]
    assert n["blk"] and n["measure"] is not None and row["points"][0] == {"kind": "new", "split_id": "hand-1"}


def test_a_places_words_are_shown_whole_with_the_excluded_items_put_back():
    """owner 2026-11-10: the Thompson's words stopped at "except:" """
    from pipeline.regs.review import checklist as CL
    assert CL.full_words("From the outlet to the signs, except:", ["Deadman - from A to B.", "Juniper - from C to D."]) == \
        "From the outlet to the signs, except: Deadman - from A to B; Juniper - from C to D."
    assert CL.full_words("X Lake within 400 m of the following streams: Also closed east of a line.", ["A Creek", "B Creek"]) == \
        "X Lake within 400 m of the following streams: A Creek; B Creek. Also closed east of a line."
    assert CL.full_words("The whole river", ["the canal"]) == "The whole river. Except: the canal."
    assert CL.full_words("The whole river.", []) == "The whole river."
    from pipeline.regs.review import dfo as D
    for _s, loc, _w in D.locations(D.default_dir()).values():       # every stored row: no item is lost, none doubled
        st = loc.get("source_text") or {}
        got = CL.full_words(st.get("specific_area") or "", st.get("excludes") or ())
        for e in st.get("excludes") or ():
            assert got.count(e.strip().rstrip(".")) >= 1, (loc["location_id"], e)
