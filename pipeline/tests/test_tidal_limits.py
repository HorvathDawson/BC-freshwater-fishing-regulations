"""T326: THE ONE TIDAL FILE — `waters/tidal_limits.json`, written from the Tidal-tab marks (`tools.tidal_limits`, the tidal
write-through) and read by the atlas only (`atlas.splits.tidal`), which resolves it into `tidal.resolved.json`.

  mouth      Place y at the FWA mouth (or the atlas line's own lower end) -> limit `mouth`: no cut, ever; the atlas
             aliases `<water>__tidal_boundary` onto the boundary standing at the mouth, or names a natural mouth
             on the registry (`<item>__mouth`) — 0 new sections
  split      Place y naming a splits.json cut, or a point ON one -> limit `split` (the cut is the tidal limit)
  cut        Place y on a located words point -> limit `cut` with its point; the atlas mints the id from it
  follow-up  wrong / unsure / polygon / off point -> nothing written, listed with why
  owner      rows with no mark are the owner's own calls: kept, unless a LATER mark decides the same water
  guard      a tidal cut on its line's lower end stops the atlas; a mouth never becomes a cut
  retired    tidal_waters.json, splits.json `tidal` flags and handoff owner-decisions.json are gone / unread

Temp files only (a tiny splits.json and candidates file); the live marks are never read.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.atlas.splits import tidal as TA
from pipeline.regs.review import export as E
from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P
from pipeline.regs.review import tidal as T
from pipeline.tools import tidal_limits as TL
from pipeline.tests import under_test

NANAIMO, TLELL, COPPER, BEAR, CAPILANO = ("tidal:354154938", "tidal:360872960", "tidal:360881072",
                                          "tidal:360868849", "tidal:360875983")


def _cand(cid, water, gnis, measure, coord, label, conf, reuse=None):
    pr = {"measure": measure, "coord": coord, "split_label": label,
          "split_name": f"{water}: {label}"}
    if reuse:
        pr["reuse"] = {"split_id": reuse}
    return {"id": cid, "water": water, "blk": cid.split(":")[1], "items": [f"gnis:{gnis}"],
            "proposed": pr, "polygon": {"crossing": "clean"}, "sources": [], "confidence": conf,
            # the located words (owner 2026-10-28: a cut stands only at words or the mouth); Bear has none
            **({"words_point": {"measure": measure}} if "mouth" not in label else {})}


CANDS = [
    _cand(NANAIMO, "Nanaimo River", 16505, 2979.5, [-123.880448, 49.116249], "tidal boundary", "high"),
    _cand(TLELL, "Tlell River", 2, 5750.3, [-131.95, 53.6], "tidal sign", "medium",
          reuse="tlell_river__tidal_sign"),
    _cand(COPPER, "Copper Creek", 33991, 0.0, [-131.800331, 53.161347], "mouth (tidal boundary)",
          "medium"),
    _cand(BEAR, "Bear River", 4943, 7342.9, [-129.97, 55.95], "mouth (tidal boundary)", "low"),
    _cand(CAPILANO, "Capilano River", 5922, 36.9, [-123.139433, 49.321832],
          "CN Railway Bridge (tidal boundary)", "medium"),
]
#: Nelson Creek (Port Coquitlam): its FWA line is replaced by the municipal added streams, so no registry item
NELSON = "tidal:356013125"
CANDS.append({**_cand(NELSON, "Nelson Creek", 37595, 38.2, [-122.876534, 49.229439], "mouth (tidal boundary)",
                      "medium"), "items": [], "mouth": {"measure": 0.0, "coord": [-122.877015, 49.229572]}})


@pytest.fixture()
def src(tmp_path, monkeypatch) -> P.Sources:
    cands = tmp_path / "tidal-candidates.json"
    cands.write_text(json.dumps({"candidates": CANDS}))
    monkeypatch.setenv("CURATION_TIDAL_CANDIDATES", str(cands))
    (tmp_path / "cat").mkdir()
    (tmp_path / "dfo").mkdir()
    sp = tmp_path / "curated" / "splits.json"
    sp.parent.mkdir()
    sp.write_text(json.dumps({"waterbodies": [{"name": "TLELL RIVER", "applies_to": {"gnis_id": "2"},
                                               "splits": [{"id": "tlell_river__tidal_sign",
                                                           "label": "tidal sign", "kind": "point",
                                                           "anchor": {"type": "point",
                                                                      "coord": [-131.95, 53.6],
                                                                      "is_lonlat": True}}]}]},
                             indent=1) + "\n")
    return P.Sources(entries_dir=tmp_path / "cat", dfo_dir=tmp_path / "dfo", splits_path=sp)


def _tidal(cid, verdict="verified", note="", **ns):
    row = T.candidate(cid)
    pr = row["proposed"]
    n = {"ref": cid.replace(":", "-"), "on_item": (row["items"] or [None])[0], "name": pr["split_name"],
         **({"anchor": "split", "split_id": pr["reuse"]["split_id"]} if pr.get("reuse") else
            {"anchor": "coord", "coord": pr["coord"], "blk": row["blk"], "measure": pr["measure"]}),
         **ns}
    return M.Mark.model_validate({
        "scope": "tidal", "target": cid, "verdict": verdict,
        "answers": {"P": {"verified": "y", "unsure": "?"}.get(verdict, "n")}, "reviewer": "dawson",
        "date": "2026-10-28", "pdf_sha256": "0" * 64, "prints": T.prints(row),
        "note": note or ("moved" if verdict == "wrong" else ""),
        "problem": "split misplaced" if verdict == "wrong" else None, "new_splits": [n]})


def _rows(marks, src, existing=()):
    rows, skipped = TL.build(E.decide(marks), src, existing=list(existing))
    return {r.blk: r for r in rows}, skipped


def _blk(cid):
    return cid.split(":")[1]


# ------------------------------------------------------------------------------------------- the writer

def test_a_mouth_mark_is_a_mouth_row_and_no_split_proposal(src):
    rows, _ = _rows([_tidal(COPPER)], src)
    [r] = rows.values()
    assert r.limit == "mouth" and r.blk == _blk(COPPER) and r.split is None and r.at is None and len(r.mark) == 12
    assert P.build([_tidal(COPPER), _tidal(NANAIMO), _tidal(TLELL)], src) == []     # tidal makes no proposal now


def test_a_mouth_on_a_water_the_registry_lacks_is_still_its_mouth_row(src):
    rows, _ = _rows([_tidal(NELSON, on_item=None, measure=0.0, coord=[-122.877015, 49.229572])], src)
    assert rows[_blk(NELSON)].limit == "mouth"


def test_a_mark_naming_an_existing_cut_is_a_split_row(src):
    rows, _ = _rows([_tidal(TLELL)], src)
    assert rows[_blk(TLELL)].limit == "split" and rows[_blk(TLELL)].split == "tlell_river__tidal_sign"
    assert '"tidal"' not in src.splits_path.read_text()        # splits.json is never flagged


def test_a_point_on_a_cut_already_there_is_that_cut(src):
    """Same anchor as a splits.json cut: the row names that cut, never a second cut beside it."""
    doc = json.loads(src.splits_path.read_text())
    doc["waterbodies"].append({"name": "NANAIMO RIVER", "applies_to": {"gnis_id": "16505"}, "splits": [
        {"id": "nanaimo_river__cedar_road_bridge", "label": "Cedar Road bridge", "kind": "point",
         "anchor": {"type": "point", "coord": [-123.880448, 49.116249], "is_lonlat": True}}]})
    src.splits_path.write_text(json.dumps(doc))
    rows, _ = _rows([_tidal(NANAIMO)], src)
    assert rows[_blk(NANAIMO)].limit == "split" and rows[_blk(NANAIMO)].split == "nanaimo_river__cedar_road_bridge"


def test_a_new_cut_keeps_its_point_and_the_atlas_mints_a_real_id(src):
    rows, _ = _rows([_tidal(NANAIMO)], src)
    r = rows[_blk(NANAIMO)]
    assert r.limit == "cut" and r.at["coord"] == [-123.880448, 49.116249] and r.at["label"] == "tidal boundary"
    assert r.split is None                                      # no id is stored
    ids = TA.ids([r], taken={"tlell_river__tidal_sign"})
    assert ids == {r.blk: "nanaimo_river__tidal_boundary"}
    [d] = TA.cut_defs([r], ids)
    assert d.id == "nanaimo_river__tidal_boundary" and d.blk == r.blk and d.label == "tidal boundary"
    assert d.anchor.type.value == "point"
    # a clash with splits.json is numbered, the same way every time
    assert TA.ids([r], taken={"nanaimo_river__tidal_boundary"}) == {r.blk: "nanaimo_river__tidal_boundary_2"}


def test_follow_ups_write_nothing_and_say_why(src):
    def with_point(note):
        m = _tidal(NANAIMO)
        return m.model_copy(update={"new_splits": [m.new_splits[0].model_copy(update={"note": note})]})
    rows, skipped = _rows([_tidal(BEAR, verdict="wrong", note="look more online", measure=4387.3),
                           with_point("chosen point: polygon (the proposal) — polygon crossing")], src)
    assert rows == {}
    assert {s["target"] for s in skipped} == {BEAR, NANAIMO}


def test_a_point_off_the_words_and_the_mouth_is_not_written(src, tmp_path):
    def at(m):
        x = _tidal(NANAIMO)
        return x.model_copy(update={"new_splits": [x.new_splits[0].model_copy(update={"measure": m})]})
    assert _rows([at(2600.0)], src)[0] == {}
    assert _rows([at(0.5)], src)[0][_blk(NANAIMO)].limit == "mouth"
    build = tmp_path / "b"
    build.mkdir()
    (build / "section_handles.txt").write_text("354154938:430\n354154938:2000\n")
    s2 = P.Sources(src.entries_dir, src.dfo_dir, src.splits_path, build=build)
    r = _rows([at(429.8)], s2)[0][_blk(NANAIMO)]
    assert r.limit == "mouth" and "both halves" in r.why     # the line's own lower end is the mouth, never a cut


def test_the_owners_own_rows_are_kept_unless_a_later_mark_decides(src):
    own = TA.Limit(blk=_blk(COPPER), limit="whole", name="Copper Creek", item="gnis:33991", decided="2026-10-29",
                   why="owner: the whole creek is tidal")
    rows, _ = _rows([_tidal(COPPER)], src, [own])               # the mark is 10-28: the owner's 10-29 call stands
    assert rows[_blk(COPPER)] == own
    later = _tidal(COPPER).model_copy(update={"date": "2026-10-30"})
    assert _rows([later], src, [own])[0][_blk(COPPER)].limit == "mouth"
    # a mark-made row is never kept once its mark is gone
    stale = TA.Limit(blk="1", limit="mouth", name="X", decided="2026-10-01", mark="abcdefabcdef")
    assert "1" not in _rows([], src, [stale])[0]


def _whole(cid, date="2026-11-03", **kw):
    """T359: the owner's "the whole water is tidal" — Place y, call whole, no cut."""
    return M.Mark.model_validate({
        "scope": "tidal", "target": cid, "verdict": "verified", "answers": {"P": "y"}, "reviewer": "dawson",
        "date": date, "pdf_sha256": "0" * 64, "prints": T.prints(T.candidate(cid)), "call": "whole",
        "note": "this entire channel is tidal", **kw})


def test_a_whole_water_mark_is_a_whole_row_on_its_one_item_and_no_follow_up(src):
    rows, skipped = _rows([_whole(COPPER)], src)
    assert {b: r.to_dict() for b, r in rows.items()} == {_blk(COPPER): {
        "blk": _blk(COPPER), "name": "Copper Creek", "limit": "whole", "item": "gnis:33991",
        "decided": "2026-11-03", "mark": rows[_blk(COPPER)].mark, "why": "the owner: the whole water is tidal"}}
    assert len(rows[_blk(COPPER)].mark) == 12 and skipped == []
    # it supersedes the earlier unsure on the same candidate, and a later owner row (no mark) still wins over it
    unsure = _tidal(COPPER, verdict="unsure")
    assert _rows([unsure, _whole(COPPER)], src)[0][_blk(COPPER)].limit == "whole"
    own = TA.Limit(blk=_blk(COPPER), limit="mouth", name="Copper Creek", decided="2026-11-04", why="owner")
    assert _rows([_whole(COPPER)], src, [own])[0][_blk(COPPER)] == own
    # a candidate with no registry item cannot be a whole row: listed, with why
    rows, skipped = _rows([_whole(NELSON)], src)
    assert rows == {} and skipped == [{"target": NELSON, "why": "the whole water is tidal, but the candidate has "
                                                                "0 registry items — one item is needed"}]
    assert TA.parse(json.loads(TA.document(_rows([_whole(COPPER)], src)[0].values())))[0].item == "gnis:33991"


def test_a_whole_water_mark_is_place_y_with_no_cut_and_only_on_tidal():
    base = {"scope": "tidal", "target": COPPER, "reviewer": "d", "date": "2026-11-03", "pdf_sha256": "0" * 64,
            "prints": {"text": None, "records": [{"key": f"split:{COPPER}", "meaning": "m", "binding": "b"}]}}
    assert M.Mark.model_validate({**base, "verdict": "verified", "answers": {"P": "y"}, "call": "whole"}).call == "whole"
    for bad, why in [({"verdict": "unsure", "answers": {"P": "?"}}, "Place y mark with no cut"),
                     ({"verdict": "verified", "answers": {"P": "y"}, "new_splits": [
                         {"ref": "r", "anchor": "coord", "coord": [-123.0, 49.2]}]}, "Place y mark with no cut"),
                     ({"verdict": "verified", "answers": {"P": "y"}, "call": "stream"}, "a tidal call is whole")]:
        with pytest.raises(ValueError, match=why):
            M.Mark.model_validate({**base, "call": "whole", **bad})
    # with no call, an accepted tidal mark still needs its one cut
    with pytest.raises(ValueError, match="exactly its one cut"):
        M.Mark.model_validate({**base, "verdict": "verified", "answers": {"P": "y"}})


def test_a_dem_point_and_an_owner_row_say_so_in_the_follow_up(src):
    """T359: the follow-up's why — a DEM point asks the owner's open question (no cut either way), and an unsure
    on a water the owner's own row already decides says that row is there."""
    x = _tidal(NANAIMO)
    dem = x.model_copy(update={"new_splits": [x.new_splits[0].model_copy(update={
        "measure": 1925.0, "note": "chosen point: dem — DEM tidal limit: DEM tidal limit (pre-verify)"})]})
    unsure = _tidal(COPPER, verdict="unsure", note="this entire channel is tidal")
    own = TA.Limit(blk=_blk(COPPER), limit="whole", name="Copper Creek", item="gnis:33991", decided="2026-10-29")
    rows, skipped = _rows([dem, unsure], src, [own])
    assert list(rows) == [_blk(COPPER)] and rows[_blk(COPPER)] == own
    assert skipped == [
        {"target": NANAIMO, "why": f"a DEM point (1925.0 m above the mouth): {TL.DEM_QUESTION}"},
        {"target": COPPER, "why": "a follow-up (unsure): no tidal limit until a Place y mark — tidal.json already "
                                  "holds the owner's own `whole` row (2026-10-29); re-mark to confirm it",
         "followup": True}]        # T545: a follow-up decides nothing (the gate counts it); the DEM point WAITS


def test_the_file_round_trips_and_refuses_what_it_cannot_mean(src):
    rows, _ = _rows([_tidal(COPPER), _tidal(TLELL), _tidal(NANAIMO)], src)
    text = TA.document(rows.values())
    assert TA.parse(json.loads(text)) == sorted(rows.values(), key=lambda r: int(r.blk))
    base = {"format": TA.FORMAT}
    for bad, why in [({"waters": [{"blk": "1", "limit": "mouth"}, {"blk": "1", "limit": "mouth"}]}, "two rows"),
                     ({"waters": [{"blk": "1", "limit": "estuary"}]}, "not one of"),
                     ({"waters": [{"blk": "1", "limit": "split"}]}, "names no split"),
                     ({"waters": [{"blk": "1", "limit": "whole"}]}, "names no item"),
                     ({"waters": [{"blk": "1", "limit": "mouth", "split": "x"}]}, "carries")]:
        with pytest.raises(ValueError, match=why):
            TA.parse({**base, **bad})
    with pytest.raises(ValueError, match="format"):
        TA.parse({"waters": []})


# ------------------------------------------------------------------------------------------- the atlas

def _graph(lower=None):
    from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
    from pipeline.common.models.enums import BoundaryKind
    from pipeline.common.models.sections import SectionBoundary
    falls = SectionBoundary(boundary_id="split:r__falls", kind=BoundaryKind("split"), route_measure=1000.0,
                            label="falls")
    g = StreamGraph()
    g.nodes = {"R:0": StreamNode(node_id="R:0", kind=NodeKind.stream, blk="7", down_m=0.0, up_m=1000.0,
                                 length_m=1000.0, lower_bound=lower, upper_bound=falls),
               "R:1000": StreamNode(node_id="R:1000", kind=NodeKind.stream, blk="7", down_m=1000.0, up_m=3000.0,
                                    length_m=2000.0, lower_bound=falls)}
    g.edges = [FlowEdge(from_node="R:1000", to_node="R:0", kind="continuation", at_measure=1000.0)]
    return g


def _registry():
    from pipeline.common.models.registry import RegistryBoundary, RegistryItem
    return {"gnis:7": RegistryItem(id="gnis:7", name="River", kind="stream", section_ids=("R:0", "R:1000"),
                                   boundaries=(RegistryBoundary(id="r__falls", label="falls", kind="split",
                                                                ref="split:r__falls"),)),
            # an area holding the mouth section is not the water: it gains no mouth boundary
            "area:basin:9": RegistryItem(id="area:basin:9", name="Basin", kind="area", section_ids=("R:0",))}


MOUTH = TA.Limit(blk="7", limit="mouth", name="River")


def test_a_natural_mouth_gains_a_mouth_boundary_whose_alias_is_tidal_and_no_section():
    g = _graph()
    ids = TA.ids([MOUTH], taken=())
    n0, bounds0 = len(g.nodes), {k: (n.lower_bound, n.upper_bound) for k, n in g.nodes.items()}
    placed = TA.alias_mouths(g, [MOUTH], ids)
    assert len(g.nodes) == n0 and {k: (n.lower_bound, n.upper_bound) for k, n in g.nodes.items()} == bounds0
    assert placed["7"] == {"id": "river__tidal_boundary", "nid": "R:0", "measure": 0.0, "on": ""}
    reg = TA.add_mouth_boundaries(_registry(), placed)
    b = reg["gnis:7"].boundaries[-1]
    assert (b.id, b.kind, b.ref, b.label) == ("river__mouth", "outlet", "mouth:7", "mouth")
    assert b.answers_to() == ("river__mouth", "river__tidal_boundary") and b.names == ("tidal boundary",)
    assert placed["7"]["items"] == ["gnis:7"] and reg["area:basin:9"].boundaries == ()
    rows = TA.resolved_rows([MOUTH], ids, placed)
    assert rows == [{"blk": "7", "name": "River", "limit": "mouth", "alias": "river__tidal_boundary", "on": "",
                     "measure": 0.0, "items": ["gnis:7"]}]


def test_a_mouth_on_a_standing_boundary_is_its_alias_never_a_cut():
    from pipeline.common.models.enums import BoundaryKind
    from pipeline.common.models.sections import SectionBoundary
    lake = SectionBoundary(boundary_id="lake:55", kind=BoundaryKind("lake"), route_measure=0.0, label="Big Lake")
    g = _graph(lower=lake)
    placed = TA.alias_mouths(g, [MOUTH], TA.ids([MOUTH], taken=()))
    assert placed["7"]["on"] == "lake:55" and len(g.nodes) == 2
    lb = g.nodes["R:0"].lower_bound
    assert lb.boundary_id == "lake:55" and "split:river__tidal_boundary" in lb.aliases
    reg = TA.add_mouth_boundaries(_registry(), placed)
    assert not any(b.ref.startswith("mouth:") for b in reg["gnis:7"].boundaries)    # the graph carries it


def test_a_mouth_the_atlas_lacks_is_reported_not_invented():
    placed = TA.alias_mouths(_graph(), [TA.Limit(blk="99", limit="mouth", name="Gone")],
                             {"99": "gone__tidal_boundary"})
    assert "absent" in placed["99"]


def test_the_guard_a_tidal_cut_on_a_line_end_stops_the_atlas():
    from pipeline.common.models import AnchorType, SplitPoint
    g = _graph()
    at_end = SplitPoint(split_id="river__tidal_boundary", blk="7", route_measure=TA.END_M - 1, fid="",
                        label="tidal boundary", anchor_type=AnchorType.point)
    with pytest.raises(SystemExit, match="lower end"):
        TA.refuse_end_cuts([at_end], g, ["river__tidal_boundary"])
    above = SplitPoint(split_id="river__tidal_boundary", blk="7", route_measure=500.0, fid="",
                       label="tidal boundary", anchor_type=AnchorType.point)
    TA.refuse_end_cuts([above], g, ["river__tidal_boundary"])           # above the end: fine


def test_the_atlas_build_never_hands_a_mouth_row_to_the_cutter():
    """Structural: only `cut` rows become SplitDefs (`cut_defs`); `alias_mouths` is the only thing a mouth reaches."""
    assert TA.cut_defs([MOUTH, TA.Limit(blk="8", limit="split", split="x")], {"7": "river__tidal_boundary"}) == []


# ------------------------------------------------------------------------------------------- retired

def test_the_old_tidal_records_are_gone_and_unread():
    from pipeline.common.curated import REPO_ROOT
    assert not (under_test.curated().base / "waters" / "tidal_waters.json").exists()
    assert Path(under_test.curated().waters.tidal).name == "tidal_limits.json" and not hasattr(under_test.curated().waters, "tidal_waters")
    assert '"tidal"' not in Path(under_test.curated().waters.cuts).read_text()
    src = "\n".join(p.read_text() for p in (REPO_ROOT / "pipeline").rglob("*.py") if "tests" not in p.parts)
    src += "\n".join(p.read_text() for p in (REPO_ROOT / "curation-review/backend").glob("*.py"))
    for gone in ("tidal_waters.json", "owner-decisions.json", "TIDAL_DECISIONS =", "under_test.curated().waters.tidal_waters"):
        assert gone not in src, gone
    rows = TA.load()
    assert {r.limit for r in rows} <= set(TA.LIMITS)


def test_words_measures_never_read_the_polygon():
    cand = {"words_point": {"measure": 10.0}, "alternates": [{"kind": "words", "measure": 20.0},
            {"kind": "polygon", "measure": 30.0}, {"kind": "mouth", "measure": 0.0}],
            "sources": [{"kind": "synopsis_box", "measure": 40.0}, {"kind": "polygon", "measure": 50.0}]}
    assert sorted(P.words_measures(cand)) == [10.0, 20.0, 40.0]


def test_a_dfo_area_description_is_no_cut_label():
    """A tidal point whose words are a DFO subarea description (cut to 60 chars by the mark) is labelled by the
    subarea, never by the fragment — splits.json refuses a label that names no place (2026-10-29, Toquart)."""
    from types import SimpleNamespace
    from pipeline.regs.review.proposals import _tidal_label
    from pipeline.atlas.splits.splits import label_problem
    for words, want in [
            ("TOQUART RIVER: That portion of Toquart Bay in Subarea 23-10 bounded on the ",
             "tidal boundary (DFO Subarea 23-10)"),
            ("The waters and intertidal foreshore of the Fraser River, lyi", "tidal boundary"),
            ("CN Railway Bridge", "CN Railway Bridge (tidal boundary)")]:
        got = _tidal_label(SimpleNamespace(name=words, measure=900.0))
        assert got == want and not label_problem(got)


def test_a_tidal_point_on_a_cut_already_there_flags_that_cut(tmp_path):
    """A tidal point within TIDAL_REUSE_M of a resolved splits.json cut on its line IS that cut (Nanaimo: tidal
    boundary on the Cedar Road bridge, 0.06 m) — the atlas refuses a second cut on one point (2026-10-29)."""
    import json as _json
    from types import SimpleNamespace
    from pipeline.regs.review.proposals import _cut_already_there
    (tmp_path / "splits.resolved.json").write_text(_json.dumps([
        {"split_id": "nanaimo_river__cedar_road_bridge", "blk": "354154938", "route_measure": 2980.0},
        {"split_id": "gauge__x", "blk": "354154938", "route_measure": 2981.0}]))
    src = SimpleNamespace(build=tmp_path, text=lambda p: p.read_text())
    have = {"nanaimo_river__cedar_road_bridge": {}}
    assert _cut_already_there(SimpleNamespace(blk="354154938", measure=2980.06), have, src) \
        == "nanaimo_river__cedar_road_bridge"
    assert _cut_already_there(SimpleNamespace(blk="354154938", measure=3100.0), have, src) is None


def test_a_rule_may_bind_the_natural_mouth_alias_and_it_is_the_line_end():
    """The mouth variant is bindable: `upstream_of <water>__tidal_boundary` is the whole water, `downstream_of` it
    is nothing — the line's open lower end, never a cut."""
    from pipeline.atlas.reach import extent as X
    g = _graph()
    placed = TA.alias_mouths(g, [MOUTH], TA.ids([MOUTH], taken=()))
    reg = TA.add_mouth_boundaries(_registry(), placed)
    up = X.resolve_extent(reg, g, [], {"op": "upstream_of", "splits": ["river__tidal_boundary"],
                                       "item_ids": ["gnis:7"]})
    assert up is not None and set(up["sections"]) == {"R:0", "R:1000"}
    down = X.resolve_extent(reg, g, [], {"op": "downstream_of", "splits": ["river__tidal_boundary"],
                                         "item_ids": ["gnis:7"]})
    assert down is None or not down["sections"]
