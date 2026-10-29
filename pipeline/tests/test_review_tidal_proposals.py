"""T166 / T212: THE OWNER'S TIDAL MARKS -> curated tidal boundaries (`proposals`, T185's path).

  cut        Place y on a candidate above the mouth -> ONE new split flagged `tidal: true`, labelled as
             its water's cuts are ("tidal boundary", never "<water>: ..."; a cut above the mouth is
             no "mouth"), the candidate's confidence unless the mark set one
  reuse      Place y naming an existing split -> `tidal_flag` (the cut stays, the flag is added)
  mouth      at the FWA mouth -> listed, no edit, nothing tidal (a cut on a line's end breaks both
             halves)
  follow-up  a wrong / unsure tidal mark is listed and never applied (Bear River: "look more online")
  earlier    the owner's calls before the Tidal tab (owner-decisions.json: Capilano, Cowichan) are
             marks too; a later mark on the same candidate wins
  side       every tidal proposal patched into ONE side copy (`side tidal`), never under data/curated
  apply      a tidal flag / cut waits on its side rebuild; a mouth or follow-up is never "applied"

Temp files only (a tiny splits.json and candidates file); the live marks are never read.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.regs.review import apply_proposals as A
from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P
from pipeline.regs.review import tidal as T

NANAIMO, TLELL, COPPER, BEAR, CAPILANO = ("tidal:354154938", "tidal:360872960", "tidal:360881072",
                                          "tidal:360868849", "tidal:360875983")


def _cand(cid, water, gnis, measure, coord, label, conf, reuse=None):
    pr = {"measure": measure, "coord": coord, "split_label": label,
          "split_name": f"{water}: {label}"}
    if reuse:
        pr["reuse"] = {"split_id": reuse}
    return {"id": cid, "water": water, "blk": cid.split(":")[1], "items": [f"gnis:{gnis}"],
            "proposed": pr, "polygon": {"crossing": "clean"}, "sources": [], "confidence": conf}


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
    n = {"ref": cid.replace(":", "-"), "on_item": row["items"][0], "name": pr["split_name"],
         **({"anchor": "split", "split_id": pr["reuse"]["split_id"]} if pr.get("reuse") else
            {"anchor": "coord", "coord": pr["coord"], "blk": row["blk"], "measure": pr["measure"]}),
         **ns}
    return M.Mark.model_validate({
        "scope": "tidal", "target": cid, "verdict": verdict,
        "answers": {"P": "y" if verdict == "verified" else "n"}, "reviewer": "dawson",
        "date": "2026-10-28", "pdf_sha256": "0" * 64, "prints": T.prints(row),
        "note": note or ("moved" if verdict == "wrong" else ""),
        "problem": "split misplaced" if verdict == "wrong" else None, "new_splits": [n]})


def _by_kind(props):
    out: dict = {}
    for p in props:
        out.setdefault(p["kind"], []).append(p)
    return out


def test_an_accepted_tidal_boundary_is_one_new_split_flagged_tidal(src):
    props = P.build([_tidal(NANAIMO)], src)
    [p] = props
    assert p["kind"] == "new_split" and p["applicable"] and p["title"].startswith("TIDAL BOUNDARY")
    [ed] = p["edits"]
    sp = ed["split"]
    assert sp["id"] == "nanaimo_river__tidal_boundary" and sp["label"] == "tidal boundary"
    assert sp["tidal"] is True and sp["anchor"]["coord"] == [-123.880448, 49.116249]
    assert "confidence" not in sp                       # the candidate's grade: high (absent)
    assert ed["waterbody"]["applies_to"] == {"gnis_id": "16505"}
    assert P.tidal_props(props) == props


def test_a_mark_set_confidence_wins_over_the_candidates(src):
    [p] = P.build([_tidal(NANAIMO, confidence="low")], src)
    assert p["edits"][0]["split"]["confidence"] == "low"


def test_a_cut_above_the_mouth_is_no_mouth(src):
    m = _tidal(BEAR, name="Bear River: mouth (tidal boundary)", measure=500.0)
    [p] = P.build([m.model_copy(update={"verdict": "verified", "answers": {"P": "y"}})], src)
    assert p["edits"][0]["split"]["label"] == "tidal boundary"
    assert p["edits"][0]["split"]["confidence"] == "low"


def test_a_reused_cut_is_flagged_not_cut_again(src):
    props = P.build([_tidal(TLELL)], src)
    [p] = props
    assert p["kind"] == "tidal_flag" and p["applicable"]
    assert p["edits"] == [{"file": p["edits"][0]["file"], "kind": "split_flag",
                           "split_id": "tlell_river__tidal_sign", "field": "tidal",
                           "before": P._MISSING, "after": True}]
    text = P.materialize(props, src)[src.splits_path]
    assert json.loads(text)["waterbodies"][0]["splits"][0]["tidal"] is True
    # already flagged: nothing to do
    src.splits_path.write_text(text)
    assert P.build([_tidal(TLELL)], P.Sources(src.entries_dir, src.dfo_dir, src.splits_path)) == []


def test_at_the_mouth_there_is_no_cut_and_a_wrong_mark_is_only_listed(src):
    props = P.build([_tidal(COPPER), _tidal(BEAR, verdict="wrong", note="look more online",
                                            measure=4387.3)], src)
    k = _by_kind(props)
    assert set(k) == {"tidal_mouth", "tidal_followup"}
    assert all(not p["edits"] for p in props)
    assert "look more online" in k["tidal_followup"][0]["notes"][0]
    nxt, blocked = A.plan(props, set())
    assert nxt is None and blocked == []                # never applied, never "blocked"


def test_the_owners_earlier_calls_are_marks_and_a_later_mark_wins(src, tmp_path):
    dec = tmp_path / "owner-decisions.json"
    dec.write_text(json.dumps({"decisions": [
        {"id": CAPILANO, "water": "Capilano River", "where": "CN Railway Bridge", "decided": "2026-10-27"},
        {"id": COPPER, "water": "Copper Creek", "decided": "2026-10-27"}]}))
    s2 = P.Sources(src.entries_dir, src.dfo_dir, src.splits_path, tidal_decisions=dec)
    k = _by_kind(P.build([], s2))
    assert [p["edits"][0]["split"]["label"] for p in k["new_split"]] == [
        "CN Railway Bridge (tidal boundary)"]
    assert [p["target"] for p in k["tidal_mouth"]] == ["gnis:33991"]
    # a later wrong mark on Capilano supersedes the call: no cut
    k2 = _by_kind(P.build([_tidal(CAPILANO, verdict="wrong")], s2))
    assert "new_split" not in k2 and k2["tidal_followup"][0]["target"] == CAPILANO
    dec.write_text(json.dumps({"decisions": [{"id": "tidal:1", "water": "Nowhere"}]}))
    with pytest.raises(ValueError, match="no such candidate"):
        P.build([], s2)


def test_one_side_copy_holds_every_tidal_boundary_and_never_curated(src, tmp_path):
    props = P.build([_tidal(NANAIMO), _tidal(TLELL), _tidal(COPPER)], src)
    tid = P.tidal_props(props)
    assert {p["kind"] for p in tid} == {"new_split", "tidal_flag"}
    out = tmp_path / "side"
    P.write_side_many(tid, src, out)
    doc = json.loads((out / "splits.json").read_text())
    flagged = {sp["id"] for wb in doc["waterbodies"] for sp in wb["splits"] if sp.get("tidal")}
    assert flagged == {"nanaimo_river__tidal_boundary", "tlell_river__tidal_sign"}
    assert "\"tidal\": true" not in src.splits_path.read_text()  # the source file is untouched
    from pipeline.common.curated import CURATED
    with pytest.raises(ValueError, match="curated"):
        P.write_side_many(tid, src, Path(CURATED.waters.splits).parent / "x")


def test_a_tidal_cut_or_flag_waits_on_its_side_rebuild(src):
    props = P.build([_tidal(NANAIMO), _tidal(TLELL)], src)
    for p in props:
        assert P.changes_cuts(p, props)
    nxt, blocked = A.plan(props, set())
    assert nxt is None and len(blocked) == 2 and all("side rebuild" in w for _, w in blocked)
    nxt, _ = A.plan(props, set(), side_checked=[p["id"] for p in props])
    assert nxt is not None
    P.check(props, src)
    assert all(not p["check"]["cheap"] for p in props)


def test_a_polygon_point_is_never_a_cut(src):
    """Owner 2026-10-28: tidal splits come only from written words or the DFO mouth default."""
    def with_point(note):
        m = _tidal(NANAIMO)
        return m.model_copy(update={"new_splits": [m.new_splits[0].model_copy(update={"note": note})]})
    poly = with_point("chosen point: polygon (the proposal) — polygon crossing (uppermost)")
    [p] = P.build([poly], src)
    assert p["kind"] == "tidal_followup" and not p["edits"] and "polygon" in p["title"]
    words = with_point("chosen point: words (the proposal) — the words: Cedar Bridge")
    assert P.build([poly, words], src)[0]["kind"] == "new_split"     # a later words mark wins


def test_an_owner_call_may_name_the_place_itself(src, tmp_path):
    """Nitinat (owner 2026-10-28): the lake and the river below it are tidal — the cut stands at the
    river's entry to the lake, not at the polygon's crossing."""
    dec = tmp_path / "owner-decisions.json"
    dec.write_text(json.dumps({"decisions": [{"id": NANAIMO, "water": "Nanaimo River", "decided": "2026-10-28",
        "anchor": {"anchor": "lake_inlet", "item_id": "wbk:9", "on_item": "gnis:16505",
                   "name": "Nanaimo River: Lake inlet (tidal boundary)"}}]}))
    [p] = P.build([], P.Sources(src.entries_dir, src.dfo_dir, src.splits_path, tidal_decisions=dec))
    sp = p["edits"][0]["split"]
    assert sp["tidal"] is True and sp["label"] == "Lake inlet (tidal boundary)"
    assert sp["anchor"] in ({"type": "lake", "wbk": "9", "edge": "inlet"},
                            {"type": "point", "coord": sp["anchor"].get("coord"), "is_lonlat": True})
