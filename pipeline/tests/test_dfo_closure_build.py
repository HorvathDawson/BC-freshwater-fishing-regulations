"""T573 BUILD STEP: a closed area inside a DFO place (entry B) reaches the reach run, the bundle and the live feed.

Toy data only (the synthetic X River of `test_registry_flowing._river`: r0..r3, cuts at Bar Lake / lake 1 / lake 2),
the REAL `build_reach`, `closures.build_pieces` and `deliver.salmon.build`. Each test names its mutation.
"""
from __future__ import annotations

import pytest

from pipeline.atlas.reach import dfo_salmon as D
from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import closures as K
from pipeline.regs.dfo_salmon import entries as E
from pipeline.regs.parsing.entry_models import Extent
from pipeline.tests.test_registry_flowing import _item, _river

DEADMAN = "Deadman - from the Bar Lake signs to the lake 1 signs"
THOMPSON = "Whole river. Please note that no fishing is allowed in the following areas, year-round:"
OPEN = [{"rule_id": "a.r0", "type": "retention_limit", "verbatim": "2 per day", "species": ["SA"], "when": {},
         "take": 2}]
SID = {"r0": 10, "r1": 11, "r2": 12, "r3": 13, "lake:9": 20, "lake:77": 21}
CH, JUL20 = S.SPECIES.index("CH"), S.DAYS.index(720)


def _place(excludes=(DEADMAN,), text=THOMPSON, water_id="3:x-river", lid="3:x-river:whole", status="active"):
    return E.EntryLocation(location_id=lid, region="3", region_number=3, kind="water", precedence=3, water="X River",
                           water_id=water_id, fingerprints=["fa"], status=status,
                           source_text={"specific_area": text, "excludes": list(excludes)},
                           binding=E.Binding(extents=[Extent(op="whole")]))


def _ef(*locs, water_id="3:x-river", items=("gnis:1",)):
    w = E.WaterBinding(water_id=water_id, region="3", region_number=3, name="X River", item_ids=list(items))
    return E.EntryFile(region="3", region_number=3, region_name="Thompson", waters=[w], locations=list(locs))


def _stretch(item=DEADMAN, ends=("x_river__bar_lake", "x_river__lake_1")):
    return {"key": K.piece_key(item), "water_id": "3:x-river", "name": "Deadman", "item": item, "kind": "stretch",
            "on_item": "gnis:1", "points": [{"kind": "split", "split_id": e} for e in ends]}


def _reach(monkeypatch, ef, confirmed, graph=None, registry=None):
    g, reg = _river()
    monkeypatch.setattr(D, "_slugs", lambda: ["3"])
    monkeypatch.setattr(E, "load", lambda *a, **k: ef)
    return D.resolve_all(registry or reg, graph or g, outside=frozenset(), closed_areas=confirmed)


def _bundle(ef, confirmed, placed):
    feeds = {"3": {"date_modified": "2026-11-15", "locators": [{"fingerprint": "fa", "rules": OPEN}]}}
    return S.build({"3": K.with_pieces(ef, confirmed)}, feeds, placed, SID, {"gnis:1": [10, 11, 12, 13]},
                   section_regions=None)


def _chinook_jul20(payload, sections, sid):
    c = payload["chains"][dict(sections)[sid]]
    from pipeline.regs.dfo_salmon.live import _at
    return c, payload["verdicts"][_at(c["sp"][CH], JUL20)]["s"]


PIECE = K.piece_id("3:x-river", K.piece_key(DEADMAN))


def test_an_accepted_stretch_closes_its_sections_and_only_those(monkeypatch):
    """Mutation: drop `resolve_closures` from `resolve_all`, or `with_pieces` from the bundle — r1 reads OPEN (chinook
    2 per day) where DFO says no fishing year-round."""
    ef = _ef(_place())
    rows = {r["location_id"]: r for r in _reach(monkeypatch, ef, [_stretch()])}
    assert rows[PIECE]["sections"] == ["r1"] and rows[PIECE]["mapped"] and not rows[PIECE].get("carried")
    assert rows[PIECE]["closure"] == {"of": "3:x-river:whole", "key": K.piece_key(DEADMAN), "kind": "stretch",
                                      "state": "placed"}
    assert rows["3:x-river:whole"]["sections"] == ["r0", "r1", "r2", "r3"]
    payload, sections = _bundle(ef, [_stretch()], list(rows.values()))
    c, v = _chinook_jul20(payload, sections, 11)
    assert c["l"] == [PIECE, "3:x-river:whole"] and v == "closed"             # narrowest first: same tier, 1 < 4
    assert "No fishing, year-round — " + DEADMAN in payload["verdicts"][c["sp"][CH][1]]["t"]   # DFO's own words
    # THE PLACE'S OWN RULES ARE UNCHANGED OUTSIDE THE PIECE: r0, r2, r3 exactly as with no closed area at all
    p0, s0 = _bundle(ef, [], [rows["3:x-river:whole"]])
    for sid in (10, 12, 13):
        assert payload["chains"][dict(sections)[sid]]["sp"] == p0["chains"][dict(s0)[sid]]["sp"]
        assert _chinook_jul20(payload, sections, sid)[1] == "open"


def test_removing_the_new_step_makes_the_closure_read_open(monkeypatch):
    """The mutation check itself: with the reach step or the bundle step taken out, r1 is open again — so the test
    above is the one that holds the step."""
    ef = _ef(_place())
    real = D.resolve_closures
    monkeypatch.setattr(D, "resolve_closures", lambda *a, **k: [])
    rows = _reach(monkeypatch, ef, [_stretch()])
    assert _chinook_jul20(*_bundle(ef, [_stretch()], rows), 11)[1] == "open"
    monkeypatch.setattr(D, "resolve_closures", real)
    rows = _reach(monkeypatch, ef, [_stretch()])
    monkeypatch.setattr(K, "with_pieces", lambda ef, confirmed: ef)
    assert _chinook_jul20(*_bundle(ef, [_stretch()], rows), 11)[1] == "open"


def test_an_unaccepted_proposal_binds_nothing_and_is_carried_on_its_water(monkeypatch):
    """Mutation: bind a piece with no confirmed placement (a proposal) — or drop it: the caveat vanishes and the
    angler reads the water as plainly open."""
    ef = _ef(_place())
    rows = {r["location_id"]: r for r in _reach(monkeypatch, ef, [])}
    p = rows[PIECE]
    assert (p["mapped"], p["sections"], p["carried"]) == (False, [], True) and "not confirmed" in p["reason"]
    payload, sections = _bundle(ef, [], list(rows.values()))
    c, v = _chinook_jul20(payload, sections, 11)
    assert c["l"] == ["3:x-river:whole"] and c["u"] == [PIECE] and v == "open"    # the drawn answer + the caveat
    assert [u["w"] for u in payload["unmapped"]] == ["X River — " + DEADMAN]


def test_a_stretch_whose_new_cut_is_not_built_is_carried_not_dropped(monkeypatch):
    ef = _ef(_place())
    g = _stretch()
    g["points"][1] = {"kind": "new", "split_id": "x_juniper_signs"}
    (p,) = [r for r in _reach(monkeypatch, ef, [g]) if r.get("closure")]
    assert not p["mapped"] and p["carried"] and "x_juniper_signs" in p["reason"] and "cut_not_found" in p["reason"]


def test_an_item_dropped_from_the_live_row_unbinds_and_comes_back(monkeypatch):
    """Mutation: bind every stored geometry (ignore `attach`'s live rule) — the circle/stretch DFO removed stays
    closed. The geometry stays on file: the words returning bind it again with nothing re-curated."""
    dropped = _ef(_place(excludes=("Juniper - from lake 1 to lake 2",)))
    rows = _reach(monkeypatch, dropped, [_stretch()])
    assert PIECE not in {r["location_id"] for r in rows}
    payload, sections = _bundle(dropped, [_stretch()], rows)
    assert PIECE not in {l for c in payload["chains"] for l in (*c["l"], *(c.get("u") or ()))}
    # a dormant wording that still names it builds nothing either (only a live row does)
    dormant = _ef(_place(excludes=("Juniper - from lake 1 to lake 2",)),
                  _place(lid="3:x-river:old", status="dormant"))
    assert PIECE not in {r["location_id"] for r in _reach(monkeypatch, dormant, [_stretch()])}
    back = {r["location_id"]: r for r in _reach(monkeypatch, _ef(_place()), [_stretch()])}
    assert back[PIECE]["sections"] == ["r1"]


def test_the_live_feed_carries_a_piece_only_while_todays_row_names_it():
    """Mutation: emit the release's pieces whatever the page says — DFO drops the item and it stays closed in the
    app; or emit none — the feed's chain skips the piece and r1 reads the place's opening."""
    from types import SimpleNamespace as NS
    ef = _ef(_place())
    row = lambda ex: NS(fingerprint="fa", specific_area=THOMPSON, excludes=list(ex))   # noqa: E731
    got = K.live_piece_rules(ef, [row([DEADMAN])])
    assert list(got) == [PIECE] and got[PIECE][0]["take"] == 0 and got[PIECE][0]["species"] == ["SA"]
    assert K.live_piece_rules(ef, [row(["Juniper - from lake 1 to lake 2"])]).keys() == {
        K.piece_id("3:x-river", K.piece_key("Juniper - from lake 1 to lake 2"))}
    assert K.live_piece_rules(ef, [row([])]) == {}
    assert K.live_piece_rules(ef, [NS(fingerprint="other", specific_area=THOMPSON, excludes=[DEADMAN])]) == {}


BABINE = "Babine Lake, excluding those waters within a 400 m radius of the following tributary streams, closed:"


def _lake(monkeypatch, outline):
    """Babine-shaped: a circle at Morrison Creek's mouth on lake:9 (`wbk:9`), geometry attached the way the real run
    attaches it (`reach.position`), EPSG:3005 metres."""
    from pyproj import Transformer
    from shapely.geometry import LineString, Point
    g, reg = _river()
    reg["wbk:77"] = _item("wbk:77", "Pond", "lake", ["lake:77"])
    centre = (-126.285975, 55.152191)
    x, y = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True).transform(*centre)
    outlines = {"lake:9": Point(x, y).buffer(outline), "lake:77": Point(x + 50, y).buffer(30)}
    lines = {s: LineString([(x + 5000, y), (x + 6000, y)]) for s in ("r0", "r1", "r2", "r3")}
    g._position_geoms = (outlines, lines)
    ef = _ef(_place(excludes=("Morrison Creek", "Pond Creek"), text=BABINE, water_id="6:babine-lake"),
             water_id="6:babine-lake", items=("wbk:9",))
    circ = lambda name, on: {"key": K.piece_key(f"400 m radius: {name}"), "water_id": "6:babine-lake",  # noqa: E731
                             "name": name, "item": name, "kind": "circle", "on_item": on, "centre": list(centre),
                             "radius_m": 400.0}
    conf = [circ("Morrison Creek", "wbk:9"), circ("Pond Creek", "wbk:77")]
    return _reach(monkeypatch, ef, conf, graph=g, registry=reg), ef, conf


def test_a_circle_on_a_lake_marks_the_lake_never_closes_it_whole(monkeypatch):
    """Mutation: bind every section a circle touches — the 400 m circle closes the whole of Babine Lake (widened).
    A section wholly inside a circle (the pond) is bound; the lake it only touches is marked and carried."""
    rows, ef, conf = _lake(monkeypatch, outline=5000)
    by = {r["closure"]["key"]: r for r in rows if r.get("closure")}
    lake, pond = by[conf[0]["key"]], by[conf[1]["key"]]
    assert (lake["mapped"], lake["sections"], lake["marked"], lake["carried"]) == (False, [], ["lake:9"], True)
    assert (pond["mapped"], pond["sections"], pond.get("carried")) == (True, ["lake:77"], None)
    payload, sections = _bundle(ef, conf, rows)
    c20 = payload["chains"][dict(sections)[20]]
    # on the lake: the place answers (drawn), the circle is its caveat — never a closure of the whole lake
    assert c20["u"] == [lake["location_id"]] and c20["l"] == ["3:x-river:whole"]
    assert _chinook_jul20(payload, sections, 20)[1] == "open"
    assert _chinook_jul20(payload, sections, 21)[0]["l"] == [pond["location_id"]]
    assert _chinook_jul20(payload, sections, 21)[1] == "closed"


def test_a_lake_line_is_reported_deferred_never_dropped(monkeypatch):
    text = "Babine Lake. Also closed east of a line from Gullwing Creek to south shore of Babine Lake."
    ef = _ef(_place(excludes=(), text=text + " Excluding waters within a 400 m radius of: X", water_id="6:babine-lake"),
             water_id="6:babine-lake", items=("wbk:9",))
    ef.locations[0].source_text["excludes"] = ["X"]
    rows = [r for r in _reach(monkeypatch, ef, []) if r.get("closure")]
    line = [r for r in rows if r["closure"]["kind"] == "line"]
    assert len(line) == 1 and line[0]["carried"] and not line[0]["mapped"]
    line_geo = {"key": line[0]["closure"]["key"], "water_id": "6:babine-lake", "name": "Gullwing", "kind": "line",
                "item": "east of a line from Gullwing Creek to south shore of Babine Lake", "on_item": "wbk:9",
                "line": [[-126.0, 55.0], [-126.1, 55.1]]}
    (again,) = [r for r in _reach(monkeypatch, ef, [line_geo]) if r.get("closure", {}).get("kind") == "line"]
    assert "deferred" in again["reason"] and again["carried"] and not again["mapped"]


def test_pieces_are_never_written_into_a_places_file():
    ef = _ef(_place())
    out = K.with_pieces(ef, [_stretch()])
    assert [l.location_id for l in ef.locations] == ["3:x-river:whole"]          # the loaded file is untouched
    assert [l.location_id for l in out.locations] == ["3:x-river:whole", PIECE]
    assert isinstance(out.locations[1], K.ClosureLocation) and out.locations[1].fingerprints == []


@pytest.mark.parametrize("state", ["changed", "radius"])
def test_a_changed_piece_is_carried(state):
    p = K._piece_location(_place(), K.derive([_place().to_dict()])["3:x-river:whole#closed"],
                          {"key": "a" * 12, "item": DEADMAN, "words": DEADMAN, "kind": "stretch", "state": state,
                           "geometry": _stretch()})
    assert p.why and not p.binding.extents


# --------------------------------------------------------------------------- a confirmed stretch's new cuts are proposed

def _cut_src(tmp_path, resolved=()):
    import json
    from pipeline.regs.review import proposals as P
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "cat").mkdir()
    (tmp_path / "dfo").mkdir()
    (tmp_path / "build").mkdir()
    (tmp_path / "build" / "registry.json").write_text(json.dumps({"items": []}))
    (tmp_path / "build" / "splits.resolved.json").write_text(json.dumps(list(resolved)))
    (tmp_path / "splits.json").write_text(json.dumps({"waterbodies": [{
        "name": "THOMPSON RIVER", "applies_to": {"gnis_id": "39492"}, "splits": [{
            "id": "thompson_river__cnr_bridge", "label": "CNR bridge", "kind": "point", "note": "",
            "anchor": {"type": "point", "coord": [-121.3, 50.6], "is_lonlat": True}}]}]}))
    return P.Sources(entries_dir=tmp_path / "cat", dfo_dir=tmp_path / "dfo", splits_path=tmp_path / "splits.json",
                     build=tmp_path / "build")


KEY = "00673ca0afe5"
TARGET = f"3:thompson-river#{KEY}"


def _end(ref, lon, m):
    return {"ref": ref, "anchor": "bridge", "name": f"{ref} signs", "on_item": "gnis:39492", "coord": [lon, 50.7],
            "blk": "356", "measure": m}


def _closed_mark(verdict="verified", ends=(("x_upper", -120.9, 5000.0), ("x_lower", -121.0, 9000.0))):
    from pipeline.regs.review import marks as M
    piece = {"key": KEY, "water_id": "3:thompson-river", "name": "Deadman", "item": "Deadman - …", "kind": "stretch",
             "on_item": "gnis:39492", "points": [{"kind": "new", "split_id": r} for r, _l, _m in ends],
             "new_splits": [_end(*e) for e in ends]}
    return M.Mark.model_validate({
        "scope": "closed_area", "target": TARGET, "verdict": verdict,
        "answers": {"P": "y" if verdict == "verified" else "n"}, "reviewer": "dawson", "date": "2026-11-15",
        "pdf_sha256": "0" * 64, "piece": piece if verdict == "verified" else None,
        "note": "" if verdict == "verified" else "the signs are elsewhere",
        "problem": None if verdict == "verified" else "wrong place",
        "prints": {"text": None, "records": [{"key": f"closed_area:{TARGET}", "meaning": "Deadman - …",
                                              "binding": "3:thompson-river · stretch"}]}})


def _new_splits(marks, src):
    from pipeline.regs.review import export as X
    from pipeline.regs.review import proposals as P
    return [p for p in P.build(X.decide(marks), src) if p["kind"] == "new_split"]


def test_a_confirmed_stretch_with_two_new_cuts_proposes_both(tmp_path):
    """Mutation: drop `cut_proposals_of` from `proposals.build` / `marks.proposal_groups` — the confirmed piece's cuts
    never reach the cuts file and the stretch is carried `cut_not_found` for ever."""
    got = _new_splits([_closed_mark()], _cut_src(tmp_path))
    assert len(got) == 2 and all(p["applicable"] and not p["problems"] for p in got)
    assert all(p["marks"][0]["scope"] == "closed_area" and p["marks"][0]["target"] == TARGET for p in got)
    assert {p["edits"][0]["split"]["anchor"]["type"] for p in got} == {"point"}


def test_an_unconfirmed_piece_proposes_no_cut(tmp_path):
    assert _new_splits([_closed_mark("wrong")], _cut_src(tmp_path)) == []


def test_an_existing_cut_within_25_m_is_reused_and_a_cut_on_the_lines_end_is_refused(tmp_path):
    src = _cut_src(tmp_path, resolved=[{"split_id": "thompson_river__cnr_bridge", "blk": "356",
                                        "route_measure": 9010.0}])
    got = _new_splits([_closed_mark()], src)
    assert len(got) == 1 and "x_upper" in got[0]["id"]                 # x_lower IS the CNR bridge cut (10 m)
    from pipeline.regs.review import marks as M
    from pipeline.regs.review import proposals as P
    assert P.landed_cut(M.NewSplit.model_validate(_end("x_lower", -121.0, 9000.0)), src) == \
        "thompson_river__cnr_bridge"
    assert P.landed_cut(M.NewSplit.model_validate(_end("x_upper", -120.9, 5000.0)), src) is None
    got = _new_splits([_closed_mark(ends=(("x_mouth", -120.9, 1.0), ("x_upper", -120.95, 5000.0)))],
                      _cut_src(tmp_path / "b"))
    (mouth,) = [p for p in got if "x_mouth" in p["id"]]
    assert not mouth["applicable"] and "line's end" in mouth["problems"][0]


def test_a_landed_new_end_binds_the_stretch(monkeypatch):
    """Mutation: read a new end by its ref (ignore `landed`) — the stretch stays carried after its cut landed."""
    g = _stretch()
    g["points"][1] = {"kind": "new", "split_id": "x_lower"}
    g["new_splits"] = [_end("x_lower", -121.0, 9000.0)]
    g["new_splits"][0]["on_item"] = "gnis:1"
    gr, reg = _river()
    monkeypatch.setattr(D, "_slugs", lambda: ["3"])
    monkeypatch.setattr(E, "load", lambda *a, **k: _ef(_place()))
    rows = {r["location_id"]: r for r in D.resolve_all(reg, gr, outside=frozenset(), closed_areas=[g],
                                                       landed=lambda n: "x_river__lake_1")}
    assert rows[PIECE]["sections"] == ["r1"] and not rows[PIECE].get("carried")
    rows = {r["location_id"]: r for r in D.resolve_all(reg, gr, outside=frozenset(), closed_areas=[g],
                                                       landed=lambda n: None)}
    assert rows[PIECE]["carried"] and "x_lower" in rows[PIECE]["reason"] and "not landed" in rows[PIECE]["reason"]


# --------------------------------------------------------------------------- the app's marker, the id, the gate, the oracle

def test_a_bound_piece_is_marked_in_the_payload_and_a_place_is_not(monkeypatch):
    """Mutation: drop the `piece: 1` marker — the app cannot tell a bound closed area the feed lacks from a dormant
    place, and falls through to the place's opening."""
    ef = _ef(_place())
    rows = _reach(monkeypatch, ef, [_stretch()])
    payload, _ = _bundle(ef, [_stretch()], rows)
    assert payload["p"][PIECE] == {"r": "3", "water": "X River", "piece": 1, "bound": 1}
    assert "piece" not in payload["p"]["3:x-river:whole"]
    from pipeline.deliver.release.pieces_gate import bound_pieces
    assert bound_pieces(payload) == [PIECE]
    # a carried (unconfirmed) piece binds nothing: marked a piece (its own unmapped chain), not bound, nothing to gate
    carried = _bundle(ef, [], _reach(monkeypatch, ef, []))[0]
    assert carried["p"][PIECE].get("piece") == 1 and "bound" not in carried["p"][PIECE] and bound_pieces(carried) == []


@pytest.mark.parametrize("water,key,ok", [
    ("3:thompson-river", "00673ca0afe5", True), ("5a:quesnel-river", "0123456789ab", True),
    ("3:thompson river", "00673ca0afe5", False), ("3:thompson-river", "00673CA0AFE5", False),
    ("3:thompson-river", "00673ca0af", False), ("3:thompsön-river", "00673ca0afe5", False),
    ("3:thompson#river", "00673ca0afe5", False)])
def test_the_piece_id_grammar(water, key, ok):
    """`<region>:<slug>#closed:<12 hex>` — nothing to escape; anything else is refused at the source."""
    if ok:
        lid = K.piece_id(water, key)
        m = K.PIECE_ID_RE.match(lid)
        assert (m["water"], m["key"]) == (water, key) and K.is_piece_id(lid)
    else:
        with pytest.raises(ValueError, match="not a piece id"):
            K.piece_id(water, key)
    assert len(K.piece_key("Deadman - ÉÀ from #a: to b")) == 12        # any words give 12 hex


def test_every_curated_water_id_fits_the_piece_grammar():
    import json
    from pipeline.common.curated import CURATED
    from pathlib import Path
    ids = [w["water_id"] for p in sorted(Path(CURATED.dfo_salmon.places).glob("region-*.json"))
           for w in json.loads(p.read_text(encoding="utf-8"))["waters"]]
    assert ids and all(K.PIECE_ID_RE.match(K.piece_id(w, "0" * 12)) for w in ids)


GATE_PAYLOAD = {"p": {"3:x-river:whole": {"r": "3"}, PIECE: {"r": "3", "piece": 1, "bound": 1}}}


@pytest.mark.parametrize("status,refused,why", [
    ({"format": "feeds-status/1", "dfo": {"pieces": 1}}, False, ""),
    ({"format": "feeds-status/1", "dfo": {"pieces": 2}}, False, ""),
    ({"format": "feeds-status/1", "feeds": {}}, True, "does not emit them"),
    ({"format": "feeds-status/1", "dfo": {"pieces": 0}}, True, "does not emit them"),
    (None, True, "cannot be asked")])
def test_the_release_gate_refuses_bound_pieces_to_a_worker_that_does_not_emit_them(status, refused, why):
    """Mutation: let the gate pass a worker without `dfo.pieces` (or an unreachable one) — the release ships, the
    feed's chains skip the pieces and the Thompson's closed areas read open. No network: the status is a stub."""
    from pipeline.deliver.release import pieces_gate as G
    asked = []
    got = G.problems(GATE_PAYLOAD, "https://w.example", lambda url: asked.append(url) or status)
    assert asked == ["https://w.example"] and bool(got) is refused
    if refused:
        assert why in got[0] and "T757" in got[0] and "upload again" in got[0] and PIECE in got[0]


def test_the_release_gate_asks_nothing_without_bound_pieces_and_refuses_with_no_worker_named():
    from pipeline.deliver.release import pieces_gate as G
    boom = lambda url: (_ for _ in ()).throw(AssertionError("asked"))     # noqa: E731
    assert G.problems({"p": {"3:x-river:whole": {"r": "3"}, PIECE: {"r": "3", "piece": 1}}}, None, boom) == []
    (only,) = G.problems(GATE_PAYLOAD, None, boom)
    assert "no feeds worker was named" in only
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    for script in ("deploy/staging/upload.sh", "deploy/prod/upload.sh"):          # both uploads ask it, before any copy
        body = (root / script).read_text()
        import re
        first_copy = re.search(r'^\s*(?:\S+=\S+; )?cpr? "', body, re.M) or re.search(r'cp_ "', body)
        assert body.index("pipeline.deliver.release.pieces_gate") < first_copy.start()


def test_the_worker_oracle_is_what_the_python_feed_emits():
    """`T573-files/closure-oracle.json` is regenerated by `python -m pipeline.tools.emit.closure_oracle`: stale, it
    would hold the worker's port to answers the Python no longer gives."""
    from pathlib import Path
    from pipeline.tools.emit import closure_oracle as O
    root = Path(__file__).resolve().parents[2]
    assert (root / O.OUT / O.FILE).read_text(encoding="utf-8") == O.text()
    cases = {c["name"]: c["expect"]["locs"] for c in __import__("json").loads(O.text())["cases"]}
    dead = K.piece_id("3:thompson-river", K.piece_key(O.DEADMAN))
    assert dead in cases["a_confirmed_on_todays_row"] and dead in cases["b_unconfirmed_on_todays_row"]
    assert dead not in cases["c_confirmed_dropped_today"] and len(cases["c_confirmed_dropped_today"]) == 1
