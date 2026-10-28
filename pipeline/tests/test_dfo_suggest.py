"""DFO extent SUGGESTIONS (`pipeline.regs.dfo_salmon.suggest`, T164 / T210 follow-up): the matcher on
real locations, and the review app's "accept suggestion" -> extent-fix editor pre-fill.

The locations are the curated DFO files' own (read only); the cuts on their water are the served
registry's boundaries for those items, copied here (a slice), so the matcher runs with no atlas. A
NEW split is pinned by an injected lookup (the FWA one reads the GeoPackage; not here).
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from pipeline.regs.dfo_salmon import suggest as S
from pipeline.regs.review import dfo as D
from pipeline.regs.review import marks as M

REPO = Path(__file__).resolve().parents[2]

#: the served registry's boundaries on these items (atlas/full/registry.json, 2026-10-27), as
#: `suggest.cuts_on` reads them — label, kind, the other names at the place
REGISTRY = {
    "gnis:39532": {"id": "gnis:39532", "name": "Campbell River", "kind": "stream",
                   "ref_ids": ["blk:354154635", "gnis:39532", "wsc:920-627155"], "boundaries": [
        {"id": "campbell_river__cement_block_boundary_sign", "label": "cement block boundary sign", "kind": "split"},
        {"id": "campbell_river__john_hart_lake", "label": "John Hart Lake", "kind": "lake"},
        {"id": "campbell_river__ladore_dam", "label": "Ladore Dam", "kind": "lake", "names": ["Campbell Lake"]},
        {"id": "campbell_river__strathcona_dam_100_m_downstream", "label": "Strathcona Dam (100 m downstream)", "kind": "split"},
        {"id": "campbell_river__maple_street_boundary_sign", "label": "Maple Street boundary sign", "kind": "split",
         "names": ["08HD022 · Campbell River At Campbell River Lodge"]},
        {"id": "gauge__08HD035", "label": "08HD035 · Campbell River Near Campbell River Cableway", "kind": "split"},
        {"id": "campbell_river__power_line_200m_up_of_quinsam", "label": "power line ~200m up of Quinsam", "kind": "split"},
        {"id": "campbell_river__elk_falls", "label": "Elk Falls", "kind": "split"},
        {"id": "campbell_river__strathcona_dam", "label": "Strathcona Dam", "kind": "split"}]},
    "gnis:12227": {"id": "gnis:12227", "name": "Cowichan River", "kind": "stream",
                   "ref_ids": ["blk:354155148", "gnis:12227", "wsc:920-252823"], "boundaries": [
        {"id": "cowichan_river__lake_329101968", "label": "lake 329101968", "kind": "lake"},
        {"id": "cowichan_river__holt_creek_into_cowichan_river", "label": "Holt Creek → Cowichan River", "kind": "confluence"},
        {"id": "cowichan_river__cnr_trestle", "label": "CNR Mile 66 Trestle", "kind": "split"},
        {"id": "cowichan_river__cnr_bridge", "label": "CNR Bridge (mile 70.2)", "kind": "split"},
        {"id": "cowichan_river__greendale_trestle", "label": "Greendale Trestle", "kind": "split"},
        {"id": "cowichan_river__cowichan_lake", "label": "Cowichan Lake", "kind": "lake"}]},
    "gnis:16075": {"id": "gnis:16075", "name": "Dean River", "kind": "stream",
                   "ref_ids": ["gnis:16075", "wsc:910-302877"], "boundaries": [
        {"id": "dean_river__signs_100_m_below_the_canyon_100_m_downstream",
         "label": "signs ~100 m below the canyon (100 m downstream)", "kind": "split"},
        {"id": "dean_river__signs_0_5_km_above_the_canyon_500_m_upstream",
         "label": "signs ~0.5 km above the canyon (500 m upstream)", "kind": "split"},
        {"id": "length:360883982:33156", "label": "Kalone Creek", "kind": "confluence"},
        {"id": "dean_river__crag_creek_into_dean_river", "label": "Crag Creek → Dean River", "kind": "confluence"},
        {"id": "dean_river__iltasyuko_river_into_dean_river", "label": "Iltasyuko River → Dean River", "kind": "confluence"}]},
    "gnis:2936": {"id": "gnis:2936", "name": "Skeena River", "kind": "stream",
                  "ref_ids": ["gnis:2936", "wsc:400"], "boundaries": [
        {"id": "skeena_river__cnr_railway_bridge_terrace", "label": "CNR Railway Bridge at Terrace", "kind": "split"},
        {"id": "skeena_river__above_babine_river", "label": "above Babine River (WSC gauge 08EB005)", "kind": "split"},
        {"id": "skeena_river__highway_37_bridge", "label": "Highway 37 Bridge", "kind": "split"}]},
}
#: the owner's proposals (`marks.proposals` shape) — the Quinsam confluence and the Sandy Pool sign
PROPOSALS = [
    {"id": "n2", "split": {"ref": "n2", "anchor": "confluence", "name": "Quinsam River", "item_id": "gnis:27883",
                           "on_item": "gnis:39532", "coord": [-125.294097, 50.037899], "blk": "354154635",
                           "measure": 4199.5}},
    {"id": "n1", "split": {"ref": "n1", "anchor": "coord",
                           "name": "white triangle boundary sign located in Sandy Pool Regional Park",
                           "on_item": "gnis:12227", "coord": [-123.835989, 48.757157], "blk": "354155148",
                           "measure": 20036.9}},
]


def _loc(location_id: str) -> dict:
    got = D.find(location_id)
    if got is None:
        pytest.fail(f"{location_id} is not in the curated DFO files any more")
    return got[1]


def _suggest(location_id: str, items: list[str], look=None, proposals=PROPOSALS) -> dict:
    cuts = S.cuts_on(items, REGISTRY, {}, proposals)
    return S.suggest_one(_loc(location_id), S.water_of(items, REGISTRY), cuts, look)


def _fix_ok(r: dict) -> M.ExtentFix:
    s = r["suggestion"]
    for n in s["new_splits"]:
        M.NewSplit.model_validate(n)
    return M.ExtentFix.model_validate({"scope": "entry", "op": s["op"], "points": s["points"],
                                       "items": s["items"], "tributaries": s["tributaries"]})


def test_campbell_matches_its_two_signs_and_the_owners_quinsam_proposal():
    """"from a fishing boundary sign at the end of Maple Street downstream to the fishing boundary
    sign at the cement block" -> between the two curated signs; "downstream of the confluence with
    the Quinsam River to the ... sign at the end of Maple Street" -> the owner's proposed Quinsam
    confluence (a `kind: new` point naming the proposal) and the Maple Street sign. Gauge cuts are
    never offered."""
    r = _suggest("1:campbell-river:from-a-fishing-boundary-sign-at-th", ["gnis:39532"])
    assert r["matched"] == "existing" and r["confidence"] == "high"
    assert r["suggestion"]["op"] == "between"
    assert r["suggestion"]["points"] == [
        {"kind": "split", "split_id": "campbell_river__maple_street_boundary_sign"},
        {"kind": "split", "split_id": "campbell_river__cement_block_boundary_sign"}]
    assert _fix_ok(r).items == ["gnis:39532"]

    q = _suggest("1:campbell-river:downstream-of-the-confluence-with~2", ["gnis:39532"])
    assert q["matched"] == "existing" and q["suggestion"]["op"] == "between"
    assert q["suggestion"]["points"] == [{"kind": "new", "split_id": "n2"},
                                         {"kind": "split", "split_id": "campbell_river__maple_street_boundary_sign"}]
    assert q["suggestion"]["new_splits"] == []          # the proposal rides in from the store
    _fix_ok(q)
    # without the owner's proposal there is nothing on the water at the Quinsam confluence: the
    # power line ~200 m up of it is NOT it
    bare = _suggest("1:campbell-river:downstream-of-the-confluence-with", ["gnis:39532"], proposals=[])
    assert all(p.get("split_id") != "campbell_river__power_line_200m_up_of_quinsam"
               for p in (bare["suggestion"] or {}).get("points", []))


def test_cowichan_matches_the_lake_the_trestle_the_proposal_and_reads_the_tidal_boundary_as_the_mouth():
    """"downstream of the weir on Cowichan Lake to 66 Mile Trestle" -> the lake cut and "CNR Mile
    66 Trestle"; "from the white triangle boundary sign located in Sandy Pool Regional Park,
    downstream to the tidal boundary" -> downstream of the owner's proposed sign (the tidal
    boundary is the water's own end); "from CNR Trestle at mile 70.2 to Skutz Falls" -> the CNR
    bridge (mile 70.2), and a NEW split at the falls when the data pins it."""
    r = _suggest("1:cowichan-river:downstream-of-the-weir-on-cowichan", ["gnis:12227"])
    assert (r["matched"], r["confidence"], r["suggestion"]["op"]) == ("existing", "high", "between")
    assert [p["split_id"] for p in r["suggestion"]["points"]] == ["cowichan_river__cowichan_lake",
                                                                  "cowichan_river__cnr_trestle"]
    _fix_ok(r)

    t = _suggest("1:cowichan-river:from-the-white-triangle-boundary-s", ["gnis:12227"])
    assert t["suggestion"]["op"] == "downstream_of"
    assert t["suggestion"]["points"] == [{"kind": "new", "split_id": "n1"}]
    assert "mouth" in t["reason"]

    class Falls(S.NoLookup):
        def falls(self, name, on_items):
            assert name == "Skutz Falls"
            return {"coord": [-123.955626, 48.783139], "item_id": "gnis:12227", "blk": "354155148",
                    "measure": 34212.6, "distance_m": 0.0}
    s = _suggest("1:cowichan-river:from-cnr-trestle-at-mile-70-2-to-s", ["gnis:12227"], look=Falls())
    assert s["matched"] == "new" and s["confidence"] == "medium"
    pts = s["suggestion"]["points"]
    assert pts[0] == {"kind": "split", "split_id": "cowichan_river__cnr_bridge"}
    assert pts[1]["kind"] == "new"
    ns = s["suggestion"]["new_splits"][0]
    assert ns["ref"] == pts[1]["split_id"] and ns["name"] == "Skutz Falls" and ns["coord"]
    _fix_ok(s)
    # with nothing to pin the falls, one end stays open: a partial suggestion, never a guess
    open_ = _suggest("1:cowichan-river:from-cnr-trestle-at-mile-70-2-to-s", ["gnis:12227"])
    assert open_["matched"] == "partial" and open_["confidence"] == "none"


def test_a_confluence_matches_the_curated_cut_or_proposes_a_new_one_from_the_watershed_code():
    """Dean River "upstream of the confluence with Crag Creek" -> the curated confluence cut (an
    auto `length:` cut is never offered). Skeena "from the confluence with the Babine River
    downstream to the CNR Railway Bridge at Terrace": no cut at the Babine mouth, but the FWA code
    says the Babine flows straight into the Skeena -> a NEW confluence split, high."""
    d = _suggest("5b:dean-river:upstream-of-the-confluence-with-cr", ["gnis:16075"])
    assert (d["matched"], d["confidence"]) == ("existing", "high")
    assert d["suggestion"]["op"] == "upstream_of"
    assert d["suggestion"]["points"] == [{"kind": "split", "split_id": "dean_river__crag_creek_into_dean_river"}]

    class Fwa(S.NoLookup):
        def stream(self, name):
            return [{"id": "gnis:17687", "name": "Babine River", "kind": "stream", "wsc": ["400-536025"]}] \
                if name == "Babine River" else []

        def mouth(self, item_id, on_items):
            return {"blk": "360887278", "measure": 335399.8, "coord": [-127.694583, 55.698129],
                    "distance_m": 12.0, "item_id": on_items[0]}
    k = _suggest("6:skeena-river:from-the-confluence-with-the-babin", ["gnis:2936"], look=Fwa())
    assert (k["matched"], k["confidence"], k["suggestion"]["op"]) == ("new", "high", "between")
    first, second = k["suggestion"]["points"]
    assert second == {"kind": "split", "split_id": "skeena_river__cnr_railway_bridge_terrace"}
    ns = k["suggestion"]["new_splits"][0]
    assert first == {"kind": "new", "split_id": ns["ref"]}
    assert (ns["anchor"], ns["item_id"], ns["on_item"], ns["blk"]) == ("confluence", "gnis:17687", "gnis:2936",
                                                                       "360887278")
    _fix_ok(k)
    # a river this water flows INTO is its mouth, not a confluence to cut
    assert S._is_receiving(["400"], S.water_of(["gnis:2936"], REGISTRY)) is False
    assert S._is_receiving(["400"], S.Water(["x"], {"x": "Babine River"}, {"x": ["400-536025"]})) is True


def test_the_words():
    """Endpoint phrases, offsets and the notice's coordinates."""
    assert [p.text for p in S.endpoints("between fishing boundary signs located 100m upstream and downstream "
                                        "of the confluence with Morrison Creek.")] == [
        "100m upstream of the confluence with Morrison Creek", "100m downstream of the confluence with Morrison Creek"]
    assert S._offset("fishing boundary signs approx. 300m downstream of Hwy 4") == -300
    assert S.coords_in("Bridge (52° 58.373'N; 122° 29.349'W)") == [(-122.48915, 52.972883)]
    assert S.coords_in("at 50°15'23.0\"N 121°30'55.0\"W") == [(-121.515278, 50.256389)]
    assert S.direction("upstream of Hwy #16 bridge.") == "upstream_of"


def test_the_written_suggestions_are_valid_extent_fixes():
    """Every suggestion in the handoff file is one the mark accepts as it is: a complete one is a
    valid `ExtentFix`, every NEW split a valid `NewSplit` its points name by ref; none is applied
    (the file is read by the review app only)."""
    p = REPO / "handoff/PLAN/curation-review/dfo-suggestions.json"
    doc = json.loads(p.read_text())
    assert doc["counts"] and doc["suggestions"]
    for lid, r in doc["suggestions"].items():
        s = r.get("suggestion")
        if not s:
            assert r["confidence"] == "none", lid
            continue
        refs = {n["ref"] for n in s["new_splits"]}
        for n in s["new_splits"]:
            M.NewSplit.model_validate(n)
        assert {pt["split_id"] for pt in s["points"] if pt["kind"] == "new"} >= refs, lid
        if r["matched"] != "partial":
            _fix_ok(r)


# --------------------------------------------------------------------------- the review app

DFO_TSX = REPO / "curation-review/frontend/src/components/DfoReview.tsx"


def _ts_function(src: str, name: str) -> str:
    """The source of one exported top-level function (to the first line that is just "}")."""
    start = src.index(f"export function {name}(")
    end = src.index("\n}\n", start) + 3
    return src[start:end]


def test_accept_suggestion_prefills_the_editor_and_writes_nothing():
    """"accept suggestion" sets the extent-fix editor's value (`setFix`) and the new splits it
    names (`setSplits`) from `suggestionFix`, and posts nothing — the owner's Save extent is the
    one write. Source pins (no JS runner in the repo) plus, when node + typescript are at hand, the
    pure `suggestionFix` run on a real suggestion."""
    src = DFO_TSX.read_text()
    accept = src[src.index("function acceptSuggestion()"):src.index("\n  }\n", src.index("function acceptSuggestion()"))]
    assert "suggestionFix(sugg" in accept and "setFix(got.fix)" in accept and "setSplits(" in accept
    assert "api." not in accept and "submit(" not in accept          # accepting never writes
    assert 'data-testid="dfo-accept-suggestion"' in src and "onClick={acceptSuggestion}" in src
    assert "value={fix ?? undefined}" in src                         # the editor shows what was accepted
    assert "suggested={suggestedFc}" in src and "detail.suggestion" in src
    mp = (DFO_TSX.parent / "MapPanel.tsx").read_text()
    assert '"suggested-lines"' in mp and "suggested = null" in mp

    tsc = REPO / "curation-review/frontend/node_modules/typescript/lib/typescript.js"
    node = shutil.which("node")
    if not (node and tsc.exists()):
        pytest.skip("node / the frontend's typescript not installed: source pins only")
    fn = _ts_function(src, "suggestionFix")
    sugg = {"location_id": "x", "matched": "new", "confidence": "high", "reason": "r",
            "suggestion": {"op": "between", "items": ["gnis:2936"], "tributaries": None,
                           "points": [{"kind": "new", "split_id": "sg1"},
                                      {"kind": "split", "split_id": "skeena_river__cnr_railway_bridge_terrace"}],
                           "new_splits": [{"ref": "sg1", "anchor": "confluence", "name": "Babine River",
                                           "item_id": "gnis:17687", "on_item": "gnis:2936"}]}}
    js = (f"const ts = require({json.dumps(str(tsc))});\n"
          f"const out = ts.transpileModule({json.dumps(fn)}, {{compilerOptions: {{module: ts.ModuleKind.CommonJS, "
          f"target: ts.ScriptTarget.ES2020}}}}).outputText;\n"
          "const m = {exports: {}}; new Function('module', 'exports', out)(m, m.exports);\n"
          f"const s = {json.dumps(sugg)};\n"
          "console.log(JSON.stringify([m.exports.suggestionFix(s, []), m.exports.suggestionFix(s, ['sg1']),"
          " m.exports.suggestionFix({...s, suggestion: null}, [])]));\n")
    got = json.loads(subprocess.run([node, "-e", js], capture_output=True, text=True, check=True).stdout)
    plain, clash, none = got
    assert plain["fix"] == {"scope": "entry", "op": "between", "items": ["gnis:2936"], "tributaries": None,
                            "points": sugg["suggestion"]["points"]}
    assert plain["newSplits"] == sugg["suggestion"]["new_splits"]
    # a ref already taken in the mark is renamed, and the point follows it
    assert clash["newSplits"][0]["ref"] == "sg1-2" and clash["fix"]["points"][0]["split_id"] == "sg1-2"
    assert clash["fix"]["points"][1]["split_id"] == "skeena_river__cnr_railway_bridge_terrace"
    assert none is None


# --------------------------------------------------------------------------- the split hunt

class _HuntLook(S.NoLookup):
    """A lookup that snaps every point onto one blk (no atlas)."""
    def snap(self, lonlat, on_items):
        return {"blk": "1", "measure": 1000.0, "distance_m": 12.0, "coord": [lonlat[0], lonlat[1]],
                "item_id": on_items[0]}

    def at_measure(self, blk, measure):
        return [-128.0, 54.0 + measure / 1e6]


def test_the_split_hunt_turns_research_into_a_valid_suggestion_and_keeps_the_evidence():
    """A hunted cut becomes a NEW split the mark accepts (`marks.NewSplit`), snapped, its research
    (confidence, description, sources, notes) on the row and in the split's note; an offset is
    baked into the point (so two cuts off one anchor never dedupe into one); a curated cut + an
    offset keeps `offset_m`; `not_found` keeps the row unplaced with its reason."""
    water = S.Water(["gnis:2936"], {"gnis:2936": "Skeena River"}, {"gnis:2936": ["400"]}, {"gnis:2936": "stream"})
    row = {"location_id": "6:x", "matched": "none", "confidence": "none", "suggestion": None,
           "reason": "none", "endpoints": []}
    cuts = {"b": {"anchor": "bridge", "name": "Hwy 16 bridge", "lat": 54.5, "lon": -128.5, "confidence": "high",
                  "description": "the bridge", "sources": ["https://example.org/b"], "notes": "MoTI structure"},
            "b_d75": {"anchor": "sign", "name": "signs 75 m below", "lat": 54.5, "lon": -128.5, "offset_m": -75,
                      "confidence": "medium", "description": "signs", "sources": [], "notes": ""},
            "g": {"anchor": "split", "split_id": "skeena_river__highway_37_bridge", "name": "23 m below",
                  "lat": 54.5, "lon": -128.5, "offset_m": -23, "confidence": "low", "description": "d",
                  "sources": [], "notes": ""}}
    out = S.apply_hunt(row, {"op": "between", "ends": [{"cut": "b"}, {"cut": "b_d75"}], "notes": "read"},
                       cuts, water, _HuntLook(), page="https://dfo/page")
    assert out["matched"] == "new" and out["confidence"] == "medium"
    s = out["suggestion"]
    assert [p["split_id"] for p in s["points"]] == ["b", "b_d75"]
    a, b = (M.NewSplit.model_validate(n) for n in s["new_splits"])
    assert not M.same_anchor(a, b)                                 # the offset is baked in
    assert b.measure == 925.0 and b.offset_m is None and "https://example.org/b" in a.note
    pr = out["hunt"]["proposals"]
    assert pr[0]["confidence"] == "high" and pr[0]["sources"] == ["https://example.org/b"]
    assert out["hunt"]["sources"] == ["https://dfo/page"] and out["hunt"]["matcher"]["matched"] == "none"
    up = S.apply_hunt(row, {"op": "upstream_of", "ends": [{"cut": "g"}]}, cuts, water, _HuntLook())
    g = M.NewSplit.model_validate(up["suggestion"]["new_splits"][0])
    assert g.anchor == "split" and g.offset_m == -23 and g.side == "upstream" and up["confidence"] == "low"
    nf = S.apply_hunt(row, {"not_found": "a lake area"}, cuts, water, _HuntLook())
    assert nf["suggestion"] is None and nf["hunt"] == {"status": "not_found", "why": "a lake area", "sources": []}


def test_the_hunt_file_covers_its_targets_and_names_real_cuts():
    """Every location the hunt names is in the suggestions and carries the hunt; every `cut` end
    names a hunted cut with a confidence, a description and a source."""
    hunt = json.loads((REPO / S.HUNT_PATH).read_text())
    doc = json.loads((REPO / "handoff/PLAN/curation-review/dfo-suggestions.json").read_text())["suggestions"]
    for k, c in hunt["cuts"].items():
        assert c["confidence"] in ("high", "medium", "low") and c["description"] and c["sources"], k
        assert len(k) <= 40, k
    for lid, e in hunt["locations"].items():
        assert lid in doc and doc[lid].get("hunt"), lid
        for end in e.get("ends") or []:
            assert "cut" not in end or end["cut"] in hunt["cuts"], (lid, end)


def test_the_review_app_shows_the_hunt_and_accepting_still_only_prefills():
    src = DFO_TSX.read_text()
    assert "function HuntProposals(" in src and 'data-testid="dfo-hunt-proposal"' in src
    assert "<HuntProposals hunt={sugg.hunt} />" in src and 'data-testid="dfo-hunt-not-found"' in src
    hp = src[src.index("function HuntProposals("):src.index("\n}\n", src.index("function HuntProposals("))]
    assert "api." not in hp and "fetch(" not in hp                 # display only


def test_coverage_clusters_rewordings_inherits_a_placed_extent_and_keeps_the_two_sides_of_a_cut_apart():
    """owner 2026-10-27: a rewording of a placed location inherits its curated extent; two wordings of
    one unplaced place share the surer confidence; 'upstream of X' and 'downstream of X' never cluster."""
    from pipeline.regs.dfo_salmon import coverage as C

    def loc(lid, text, ext=None, dup=None):
        return ("r", {"location_id": lid, "water_id": "w", "duplicate_of": dup, "status": "active",
                      "source_text": {"specific_area": text},
                      "binding": {"extents": ext or [], "tributaries": None}}, {"item_ids": ["gnis:1"]})

    def row(lid, conf, split):
        return {"location_id": lid, "items": ["gnis:1"], "confidence": conf, "matched": "existing", "reason": "",
                "suggestion": {"op": "downstream_of", "points": [{"kind": "split", "split_id": split}],
                               "items": ["gnis:1"], "tributaries": None, "new_splits": []}}

    locs = dict([
        ("w:placed", loc("w:placed", "upstream of CNR bridge at Terrace to a point above the Babine confluence",
                         [{"op": "between", "splits": ["cnr", "babine"]}])),
        ("w:old", loc("w:old", "upstream of CNR bridge at Terrace to a point above the Babine confluence, excluding",
                      dup="w:placed")),
        ("w:a", loc("w:a", "downstream of 168th Street at Bothwell Park")),
        ("w:b", loc("w:b", "downstream of 168th Street.")),
        ("w:up", loc("w:up", "upstream of Highway 37 bridge.")),
        ("w:down", loc("w:down", "downstream of Highway 37 bridge.")),
    ])
    rows = [row("w:old", "low", "x"), row("w:a", "high", "s168"), row("w:b", "low", "s168"),
            {**row("w:up", "high", "h37"), "suggestion": {**row("w:up", "high", "h37")["suggestion"], "op": "upstream_of"}},
            row("w:down", "high", "h37")]
    C.annotate(rows, locs)
    by = {r["location_id"]: r for r in rows}
    assert by["w:old"]["cluster"]["role"] == "inherits" and by["w:old"]["why"] == "variant_of_placed"
    assert [p["split_id"] for p in by["w:old"]["suggestion"]["points"]] == ["cnr", "babine"]
    assert by["w:old"]["before"]["confidence"] == "low"
    assert by["w:b"]["cluster"]["role"] == "agrees" and by["w:b"]["confidence"] == "high"
    assert "cluster" not in by["w:up"] and "cluster" not in by["w:down"]
