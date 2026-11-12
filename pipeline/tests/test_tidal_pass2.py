"""T212 pass 2: DFO area descriptions -> tidal-boundary evidence (pipeline/tools/audit/tidal_pass2.py) and
its additive merge into the review's tidal candidates (pipeline/regs/review/tidal.py merge_pass2)."""
import copy
import json
from pathlib import Path

import pytest

from pipeline.tools.audit import tidal_pass2 as P
from pipeline.regs.review import tidal as T


def test_coords_forms():
    g = P.geometry_of("from 52°03.86' north latitude and 127°52.05' west longitude, thence true east to "
                      "52°03.86' north latitude and 127°50.88' west longitude. [NAD83]")
    assert g["lines"] == [[[-127.8675, 52.064333], [-127.848, 52.064333]]]
    g = P.geometry_of("upstream of a line from the old Eurocan dock at 53 degrees 59.600 minutes N 128 degrees "
                      "40.733 minutes W to the kiosk at 53 minutes 58.841 minutes N 128 degrees 39.144 minutes W.")
    assert len(g["lines"]) == 1 and len(g["lines"][0]) == 2
    g = P.geometry_of("lying southerly of a line that begins at 50°23.831′N 125°57.647′W [near Kelsey Bay] then to "
                      "50°23.438′N 125°56.614′W [Graveyard Point] and northerly of the parallel passing through "
                      "50°21.679′N near the first bridge")
    assert g["parallels"] and g["parallels"][0]["lat"] == pytest.approx(50.361317, abs=1e-5)


def test_nad27_converted():
    g = P.geometry_of("at 52°03.86' north latitude and 127°52.05' west longitude to 52°03.86' north latitude and "
                      "127°50.88' west longitude [NAD27]")
    assert g["datum"].startswith("NAD27") and g["points"][0][0] < -127.8675   # shifted ~100 m west


def test_line_breaks_on_shoreline_and_new_clause():
    g = P.geometry_of("begins at 52°16.065′N 127°47.100′W [Boscowitz Point] then to 52°14.759′N 127°45.956′W "
                      "[Rattenbury Point] and southwesterly of a line that begins at 52°27.297′N 127°17.586′W "
                      "[north of Eucott Bay] then to 52°26.354′N 127°16.415′W [Edward Point].")
    assert len(g["lines"]) == 2
    g = P.geometry_of("at 50°59.137′N 126°32.131′W [west] then southerly following the shoreline to "
                      "50°58.000′N 126°31.000′W [east]")
    assert g["lines"] == []


@pytest.mark.parametrize("text,ptype,want", [
    ("Tidal waters of the Tlell River downstream of a white triangular fishing boundary sign located approximately "
     "1 km upstream of the Highway #16 bridge.", "tidal_area", "is"),
    ("then to the tidal water boundary signs approximately 100 m upstream of the Highway 19 bridge.", "tidal_area", "is"),
    ("The intertidal foreshore of the Qualicum River Estuary from a point", "tidal_area", "confirms_mouth"),
    ("from the white triangle boundary sign located in Sandy Pool Regional Park, downstream to the tidal boundary.",
     "freshwater", "at_or_below"),
    ("upstream of white triangular fishing boundary signs located near mouth of river.", "freshwater", "at_or_below"),
    ("Unless otherwise specified, the tidal boundary is a straight line drawn between the two most seaward points",
     "rec_other", "context"),
])
def test_implies(text, ptype, want):
    assert P.implies_of(text, ptype, P.geometry_of(text)) == want


def test_landmarks():
    assert P.landmark_of("approximately 100 m upstream of the Highway 19 bridge")["offset_m"] == 100
    lm = P.landmark_of("approximately 1 km upstream of the Highway #16 bridge")
    assert (lm["match"], lm["offset_m"]) == ("16", 1000)
    assert P.landmark_of("The tidal waters of Gingolx (Kincolith) River from the Gingolx Highway Bridge downstream"
                         )["match"] == "Gingolx Highway"
    assert P.landmark_of("downstream of the 6 mile Bridge.") is None      # which bridge is not said
    assert P.landmark_of("upstream of the bridge located near the mouth of Pallant Creek.") is not None


def test_names():
    assert P.names_in("The tidal waters of Gingolx (Kincolith) River") == ["Gingolx River", "Kincolith River"]
    assert P.names_in("waters of Rivers Inlet near the Namu River") == ["Namu River"]


@pytest.mark.parametrize("implies,p,ref,want", [
    ("is", 1000, 1100, "agrees"), ("is", 1000, 2000, "moves"),
    ("at_or_above", 1000, 3000, "consistent"), ("at_or_above", 3000, 1000, "moves"),
    ("at_or_below", 3000, 1000, "consistent"), ("at_or_below", 1000, 3000, "moves"),
])
def test_relation(implies, p, ref, want):
    assert P.relation(implies, p, ref) == want


def _cand(cid="tidal:1", measure=500.0):
    return {"id": cid, "water": "Test River", "blk": cid.split(":")[1], "enters": "ocean", "items": ["gnis:1"],
            "proposed": {"measure": measure, "coord": [-125.0, 50.0], "above_mouth_m": measure,
                         "split_label": "mouth (tidal boundary)", "split_name": "Test River: mouth", "aliases": []},
            "polygon": {"crossing": "clean", "measure": measure}, "mouth": {"measure": 0.0, "coord": [-125.0, 50.0]},
            "sources": [{"kind": "synopsis_row", "source": "x", "measure": measure}],
            "confidence": "medium", "agreement": "agree",
            "triage": {"class": "no claim", "web": [], "why": "w"}, "alternates": []}


def _ev(effective="is", measure=1500.0, vs="moves"):
    return {"entry": "dfo2:abc", "page_type": "tidal_area", "area": ["8"], "label": "Test River", "implies": effective,
            "effective": effective, "text": "Tidal waters of the Test River downstream of ...", "url": "https://x",
            "confidence": "high", "vs_t212": vs, "vs_mark": None,
            "located": {"measure": measure, "above_mouth_m": measure, "coord": [-125.01, 50.01], "located_by": "line"}}


def test_merge_keeps_prints_and_flags_move():
    c = _cand()
    by_id, order = {c["id"]: c}, [c["id"]]
    before = T.prints(c)
    pr = {"measure": 1500.0, "above_mouth_m": 1500.0, "coord": [-125.01, 50.01], "located_by": "line",
          "implies": "is", "confidence": "high", "source_text": "t", "url": "https://x"}
    doc2 = {"proposals": [{"id": c["id"], "water": "Test River", "blk": "1", "status": "moves", "mark_conflict": False,
                           "proposed": pr, "evidence": [_ev()], "items": ["gnis:1"]}]}
    snap = copy.deepcopy(c)
    by2, order2 = T.merge_pass2(by_id, order, doc2)
    assert c == snap                                   # the cached row is not mutated
    r = by2[c["id"]]
    assert T.prints(r) == before                       # no mark goes stale
    assert r["triage"]["class"] == "missed" and r["triage"]["class_before_pass2"] == "no claim"
    assert r["triage"]["web"][0]["kind"] == "dfo_pass2" and r["triage"]["web"][0]["located"]["measure"] == 1500.0
    assert r["alternates"][-1]["kind"] == "words" and r["alternates"][-1]["measure"] == 1500.0
    assert order2 == order


def test_merge_agreeing_keeps_triage_and_adds_new_stream():
    c = _cand()
    pr = {"measure": 520.0, "above_mouth_m": 520.0, "coord": [-125.0, 50.0], "located_by": "line",
          "implies": "at_or_above", "confidence": "medium", "source_text": "t", "url": "https://x"}
    new_pr = {**pr, "measure": 900.0, "above_mouth_m": 900.0}
    doc2 = {"proposals": [
        {"id": c["id"], "water": "Test River", "blk": "1", "status": "agrees", "mark_conflict": False,
         "proposed": pr, "evidence": [_ev("at_or_above", 520.0, "agrees")], "items": ["gnis:1"]},
        {"id": "tidal:2", "water": "New Channel", "blk": "2", "status": "new", "mark_conflict": False,
         "proposed": new_pr, "evidence": [_ev("at_or_above", 900.0, None)], "items": ["gnis:2"],
         "mouth": {"measure": 0.0, "coord": [-125.1, 50.1]}},
        {"id": "tidal:3", "water": "No Items", "blk": "3", "status": "new", "proposed": new_pr, "evidence": [],
         "items": []}]}
    by2, order2 = T.merge_pass2({c["id"]: c}, [c["id"]], doc2)
    assert by2[c["id"]]["triage"]["class"] == "no claim"
    assert order2 == [c["id"], "tidal:2"]
    n = by2["tidal:2"]
    assert n["pass2_new"] and n["proposed"]["measure"] == 900.0 and n["triage"]["class"] == "missed"
    assert T.prints(n)["records"][0]["key"] == "split:tidal:2"


def test_pass2_off_for_a_test_copy(monkeypatch, tmp_path):
    monkeypatch.delenv("CURATION_TIDAL_PASS2", raising=False)
    monkeypatch.setenv("CURATION_TIDAL_CANDIDATES", str(tmp_path / "c.json"))
    assert T.pass2_path() is None
    monkeypatch.setenv("CURATION_TIDAL_PASS2", "")
    assert T.pass2_path() is None


def test_mouth_names_and_class():
    t = ("The waters of the mouth of Namu River shoreward of a line from a fishing boundary sign located 180 m "
         "from a bridge at the mouth of Namu Creek on the west shore to a fishing boundary sign")
    assert "Namu River" in P.mouth_names("Namu River", t)
    assert P._entry("x", t, "tidal_area", {"area": ["8"]}, "Namu River", "s")["implies"] == "confirms_mouth"
    t = "In Subarea 14-14, Courtenay River Mouth - the waters of Comox Harbour inside a line from a sign near Trent River"
    assert P.mouth_names("Comox Harbour", t) == ["Courtenay River"]          # not Trent: a sign near it
    assert P.mouth_names("Koeye Inlet", "east of a line starting Koeye Point. This closes Koeye River estuary.") == ["Koeye River"]
    assert P.relation("confirms_mouth", 0.0, 120.0, P.MOUTH_TOL_M) == "agrees"
    assert P.relation("confirms_mouth", 0.0, 800.0, P.MOUTH_TOL_M) == "conflict"


def test_mouth_verdict():
    c = _cand(measure=40.0)
    ev = {"entry": "dfo2:m", "effective": "confirms_mouth", "located": {"above_mouth_m": 0.0}}
    assert P.mouth_verdict(c, 0.0, 40.0, None, [], [ev])["verdict"] == "upgrade"
    c2 = _cand(measure=40.0)
    c2["sources"] = [{"kind": "synopsis_row", "words": "sign 1 km up", "measure": 1000.0}]
    assert P.mouth_verdict(c2, 0.0, 40.0, None, [], [ev])["verdict"] == "conflict"   # a source names upstream
    assert P.mouth_verdict(_cand(measure=900.0), 0.0, 900.0, 900.0, [], [ev])["mark_upstream"] is True
    assert P.mouth_verdict(None, 0.0, None, None, [], [ev])["verdict"] == "new"


def test_merge_dfo_mouth_upgrade_keeps_prints():
    c = _cand(measure=40.0)
    before = T.prints(c)
    pr = {"measure": 0.0, "above_mouth_m": 0.0, "coord": [-125.0, 50.0], "located_by": "mouth",
          "implies": "confirms_mouth", "confidence": "high", "source_text": "t", "url": "https://x"}
    doc2 = {"proposals": [{"id": c["id"], "water": "Test River", "blk": "1", "status": "agrees", "proposed": pr,
                           "evidence": [_ev("confirms_mouth", 0.0, "agrees")], "items": ["gnis:1"],
                           "dfo_mouth": {"verdict": "upgrade", "why": "DFO confirms the mouth"}}]}
    r = T.merge_pass2({c["id"]: c}, [c["id"]], doc2)[0][c["id"]]
    assert r["confidence"] == "high" and r["dfo_mouth"] == "upgrade" and T.prints(r) == before
    # the mouth is the proposal's own point: no "dfo2" alternate is added (a candidate with no
    # alternates of its own carries no `alternates` key at all — merge_pass2 adds it only for a point)
    assert all(a["id"] != "dfo2" for a in r.get("alternates") or [])


# ---------------------------------------------------------------- DFO default: the mouth (owner 2026-10-28)

def _cand(**kw):
    base = {"id": "tidal:1", "water": "X River", "mouth": {"measure": 0.0, "coord": [-125.0, 50.0]},
            "proposed": {"measure": 20.0, "above_mouth_m": 20.0}, "polygon": {"crossing": "at_mouth", "measure": 20.0},
            "sources": [], "triage": {"class": "no claim", "web": []}, "confidence": "medium"}
    return {**base, **kw}


def test_default_high_when_nothing_specifies():
    from pipeline.regs.review import tidal as T
    out = T.apply_default({"tidal:1": _cand()}, None)["tidal:1"]
    assert out["dfo_default"] == "high" and out["specified"] == []
    assert out["confidence"] == "medium"               # a non-mouth cut keeps the candidate's grade (proposals)
    assert out["triage"]["web"][0]["url"] == T.DEFAULT_URL
    assert out["proposed"]["measure"] == 0.0 and out["proposed_t212"]["measure"] == 20.0   # the mouth; file's kept


def test_polygon_is_ignored_for_cuts():
    from pipeline.regs.review import tidal as T
    r = _cand(polygon={"crossing": "blobs", "measure": 2971.6},
              proposed={"measure": 2971.6, "above_mouth_m": 2971.6, "split_label": "mouth (tidal boundary)"})
    before = T.prints(r)
    out = T.apply_default({"tidal:1": r}, None)["tidal:1"]
    assert out["dfo_default"] == "high" and out["proposed"]["above_mouth_m"] == 0.0
    assert T.prints(out) == before                       # held against the file's proposal: no mark goes stale


def test_owner_stated_nitinat_is_words_not_default():
    from pipeline.regs.review import tidal as T
    out = T.apply_default({"tidal:354155007": _cand(id="tidal:354155007", water="Nitinat River")}, None)["tidal:354155007"]
    assert out["dfo_default"] is None and out["specified"][0].startswith("owner 2026-10-28")
    assert out["proposed"]["measure"] == 23306.5


def test_located_source_or_pass2_is_specifies_so_no_default():
    from pipeline.regs.review import tidal as T
    r = _cand(sources=[{"kind": "synopsis_box", "source": "box", "measure": 900.0}])
    assert T.apply_default({"tidal:1": r}, None)["tidal:1"]["dfo_default"] is None
    doc2 = {"proposals": [{"id": "tidal:1", "evidence": [{"effective": "is", "located": {"measure": 500.0}}]}]}
    assert T.apply_default({"tidal:1": _cand()}, doc2)["tidal:1"]["dfo_default"] is None
    doc2 = {"proposals": [{"id": "tidal:1", "evidence": [{"effective": "at_or_above", "located": {"measure": 800.0, "above_mouth_m": 800.0}}]}]}
    assert T.apply_default({"tidal:1": _cand()}, doc2)["tidal:1"]["dfo_default"] == "conflict"


def test_boundary_words_never_fall_to_the_default():
    """Serpentine (owner 2026-10-28): words name a boundary -> never the mouth default; located -> words name it."""
    from pipeline.regs.review import tidal as T
    r = _cand(sources=[{"kind": "synopsis_box", "source": "box", "words": "X River: Burlington Northern Railway Bridge"}])
    out = T.apply_default({"tidal:1": r}, None)["tidal:1"]
    assert out["dfo_default"] is None and out["words_status"] == "locate"
    r = _cand(sources=[{"kind": "synopsis_box", "source": "box", "words": "X River: BN Bridge"}],
              words_point={"measure": 0.0, "located_by": "rail seaward of the mouth", "words": "X River: BN Bridge"})
    out = T.apply_default({"tidal:1": r}, None)["tidal:1"]
    assert out["dfo_default"] is None and out["words_status"] == "located"
    doc2 = {"proposals": [{"id": "tidal:1", "evidence": [{"implies": "at_or_below", "effective": "at_or_below", "page_type": "freshwater",
            "text": "upstream of white triangular fishing boundary signs located near mouth of river",
            "located": {"measure": 0.0, "above_mouth_m": 0.0, "coord": [-125.0, 50.0], "located_by": "near the mouth"}}]}]}
    out = T.apply_default({"tidal:1": _cand()}, doc2)["tidal:1"]
    assert out["words_status"] == "located" and out["dfo_default"] is None


def test_relative_mentions_are_not_boundary_words():
    from pipeline.regs.review import tidal as T
    r = _cand(id="tidal:360873432", sources=[{"kind": "synopsis_row", "source": "row", "words": "third bridge ~10 km upstream of the tidal boundary"}])
    out = T.apply_default({"tidal:360873432": r}, None)["tidal:360873432"]
    assert out["dfo_default"] == "high" and out["words_status"] is None


# --------------------------------------------------------------------------- T678: the re-scrape, the missed texts,
# and a decided water staying decided

#: DFO Area 29's own list, verbatim from data/source/dfo_tidal_areas/2026-10-28 (and 2026-11-11) a-s29-eng.html
A29_LIST = """<details><summary>Area map</summary><h2 id="map">Area map</h2>
<h3 class="h4">Tidal water boundaries in streams and rivers</h3>
<p>In streams and rivers, tidal waters and non-tidal waters meet. In Area 29 these boundaries are marked by bridges and rail lines. On the side closest to the ocean, the waters are considered tidal. On the side farthest from the ocean, the waters are considered non-tidal.</p>
<ul class="lst-spcd">
<li>Brunette River: Burlington Northern Railway Bridge</li>
<li>Kanaka Creek: CPR Bridge</li>
<li>Nathan Creek: CNR Bridge</li>
<li>Pitt River: CPR Bridge</li>
<li>Stave River: CPR Bridge</li>
<li>West Creek: CN Railway Bridge</li>
<li>York Creek: CP Rail line</li>
</ul></details>"""
#: DFO Area 17's new area description (live page 2026-11-11), verbatim
A17_CEDAR = """<h3 id="s-adesc">Area descriptions</h3><table><thead><tr><th>Area</th><th>Map</th>
<th>Area description</th></tr></thead><tbody><tr id="_8821">
<td>Cedar Bridge</td>
<td></td>
<td>In Subarea 17-14, that portion of the Nanaimo River from the Cedar Bridge to the white square boundary signs approximately 400 m downstream of the Cedar Bridge.</td>
</tr></tbody></table>"""


def _cache(root, date, pages):
    d = root / date
    man = {}
    for path, body in pages.items():
        f = d / path.lstrip("/")
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(f"<html><head><title>BC tidal area</title></head><body>{body}</body></html>")
        man[path] = {"status": 200, "snapshot": None, "archive_url": None, "fetched": date}
    (d / "manifest.json").write_text(json.dumps(man))
    return d


def test_the_area_29_list_items_are_boundaries(tmp_path):
    """The 5 short items ("Kanaka Creek: CPR Bridge") were dropped by the length floor and the rest read as context;
    West Creek lost its name to the direction filter. Each item now names its stream's tidal boundary ("is")."""
    ents = {e["primary"]: e for e in P.extract_pages(_cache(tmp_path, "2026-10-28",
                                                            {"/fm-gp/rec/tidal-maree/a-s29-eng.html": A29_LIST}))}
    for nm in ("Brunette River", "Kanaka Creek", "Nathan Creek", "Pitt River", "Stave River", "West Creek", "York Creek"):
        assert ents[nm]["implies"] == "is" and ents[nm]["boundary_list"].endswith("Tidal water boundaries in streams and rivers"), nm
    # the paragraph above the list is not an item
    assert all(not e.get("boundary_list") for e in ents.values() if e["text"].startswith("In streams and rivers"))


def test_rail_landmarks_of_the_list():
    assert P.landmark_of("Kanaka Creek: CPR Bridge")["kind"] == "rail"
    assert P.landmark_of("York Creek: CP Rail line")["kind"] == "rail"
    cn = P.landmark_of("West Creek: CN Railway Bridge")
    assert (cn["kind"], cn["match"]) == ("rail", None)          # a rail bridge, not a road named "CN Railway"
    bn = P.landmark_of("Brunette River: Burlington Northern Railway Bridge")
    assert (bn["kind"], bn["match"]) == ("rail", "BNSF")         # OSM names the BN line BNSF, or it is not located
    assert P.landmark_of("Coquitlam River: Mary Hill Bypass Bridge")["kind"] == "road"


def test_cedar_bridge_is_a_lower_bound_located_by_its_bridge(tmp_path):
    """Nanaimo: the stretch "from the Cedar Bridge ... downstream" is in tidal Subarea 17-14, so tidal water reaches the
    bridge — at_or_above, placed by the bridge (was filed as context)."""
    (e,) = P.extract_pages(_cache(tmp_path, "2026-11-11", {"/fm-gp/rec/tidal-maree/a-s17-eng.html": A17_CEDAR}))
    assert (e["primary"], e["implies"], e.get("reach_landmark")) == ("Nanaimo River", "at_or_above", True)
    assert P.landmark_of(e["text"])["match"] == "Cedar"


def test_wording_history_dates_new_and_gone_words(tmp_path):
    old = {"/fm-gp/rec/tidal-maree/a-s29-eng.html": A29_LIST}
    new = {"/fm-gp/rec/tidal-maree/a-s29-eng.html": A29_LIST.replace("<li>Stave River: CPR Bridge</li>", ""),
           "/fm-gp/rec/tidal-maree/a-s17-eng.html": A17_CEDAR}
    _cache(tmp_path, "2026-10-28", old)
    doc = P.extract(_cache(tmp_path, "2026-11-11", new))
    by = {e["primary"]: e for e in doc["entries"]}
    assert by["Kanaka Creek"]["first_seen"] == "2026-10-28"     # the extractor fix is not new wording
    assert by["Nanaimo River"]["first_seen"] == "2026-11-11"
    assert [(g["primary"], g["gone"]) for g in doc["gone"]] == [("Stave River", "2026-11-11")]
    # the oldest copy has no history of its own
    first = P.extract(tmp_path / "2026-10-28")
    assert {e["first_seen"] for e in first["entries"]} == {"2026-10-28"} and first["gone"] == []


def test_bears_on_tidal():
    assert P.bears_on_tidal("is", "Area map / Tidal water boundaries in streams and rivers")
    assert not P.bears_on_tidal("context", "Salmon / Restrictions")
    assert not P.bears_on_tidal("confirms_mouth", "Bivalve shellfish and snails / Bivalve shellfish contamination closures")


def test_a_decided_water_is_not_reopened_by_a_move():
    """T678: once the owner has marked a water, pass 2 moving the proposal does not put it back in the queue; words
    newer than the mark do (`mark_conflict`), and the wording rides on the row for "DFO wording changed"."""
    c = _cand()
    pr = {"measure": 1500.0, "above_mouth_m": 1500.0, "coord": [-125.01, 50.01], "located_by": "line",
          "implies": "is", "confidence": "high", "source_text": "t", "url": "https://x"}
    w = [{"entry": "dfo2:new", "change": "new", "date": "2026-11-11", "implies": "is", "label": "L", "text": "t", "url": "u"}]
    doc2 = {"proposals": [{"id": c["id"], "water": "Test River", "blk": "1", "status": "moves", "mark_conflict": False,
                           "owner_mark": {"measure": 500.0, "date": "2026-10-28"}, "proposed": pr, "evidence": [_ev()],
                           "items": ["gnis:1"], "wording": w}]}
    r = T.merge_pass2({c["id"]: c}, [c["id"]], doc2)[0][c["id"]]
    assert r["triage"]["class"] == "no claim" and r["pass2"]["wording"] == w
    assert T.wording_changed(r, "2026-10-28") == w               # marked before the words: reopened
    assert T.wording_changed(r, "2026-11-11") == []              # marked on or after them: still decided
    assert T.wording_changed(r, None) == []


def test_the_review_reopens_a_mark_older_than_its_words(monkeypatch):
    """The Tidal tab's status: "DFO wording changed" only when the water's own words are newer than the decision —
    the mark's date, or the owner's own later row in tidal_limits.json (San Juan, 2026-11-11)."""
    import sys
    from types import SimpleNamespace
    from pipeline.regs.review import marks as M
    backend = str(Path(__file__).resolve().parents[2] / "curation-review" / "backend")
    monkeypatch.syspath_prepend(backend)
    sys.modules.pop("tidal", None)
    import tidal as B
    monkeypatch.setattr(M, "status", lambda m, now: {"status": "verified", "verdict": "verified", "reasons": []})
    monkeypatch.setattr(B.T, "prints", lambda row: {"text": None, "records": []})
    row = {"blk": "1", "pass2": {"wording": [{"date": "2026-11-11", "change": "new", "entry": "e"}]}}
    monkeypatch.setattr(B, "_owner_calls", lambda: {})
    v = B._verify(SimpleNamespace(date="2026-10-28"), row)
    assert (v["status"], v["reasons"]) == ("stale", [T.WORDING_CHANGED])
    assert B._verify(SimpleNamespace(date="2026-11-12"), row)["status"] == "verified"
    monkeypatch.setattr(B, "_owner_calls", lambda: {"1": "2026-11-11"})
    assert B._verify(SimpleNamespace(date="2026-10-28"), row)["status"] == "verified"
    sys.modules.pop("tidal", None)
