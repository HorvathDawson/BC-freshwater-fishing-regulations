"""MISSING-SPLIT SUGGESTIONS FOR CATALOGUE ENTRIES (owner 2026-10-27, curation-review cleanup) — the entry-
side twin of `pipeline.regs.dfo_salmon.suggest`: for every rule the pre-checks say cannot be placed, or that
binds through an ambiguous cut, propose WHERE (an existing split or a new one) with a confidence, the evidence
and an independent second check. Never applied: the review app shows each item; the owner's mark is the only
write. Nothing here writes splits.json, the catalogue or the marks store.

    .venv/bin/python -m pipeline.regs.review.split_suggest            # -> handoff/PLAN/curation-review/
                                                                      #    entry-split-suggestions.json

Per finding (`precheck`): undrawn_part, unresolved_locator, unresolved, unplaced_pieces, ambiguous_cut; plus
`landmark_unbound` — hand-researched rules the pre-checks miss (a landmark the rule names that its extent
does not bind correctly).

  * ambiguous_cut — a lake split matched both ends of the lake on its line; the builder takes one by E16
    ("toward" the rule's other endpoint). The SECOND method reads the side from the rule's own WORDS
    ("downstream of X Lake", "from X Lake upstream to", "outlet") and, where the split is curated, its
    anchor's edge (outlet = the lower measure, inlet = the upper) or point; then the other endpoint's measure
    must lie on that side. Both agreeing = pre-verified.
  * a lake part ("within 100 m of the outlet", "north of a line between signs") is a lake area, deferred by
    owner rule (lake-splits-deferred): confidence none, the reason says so, no cut is invented.
  * `RESEARCH` holds the hand-checked items (OSM via Overpass, FWA snaps, web) with their sources; the
    measures were snapped to the FWA lines of `data/source/bc_fisheries_data.gpkg`.

Duplicates (the same place named by several rules, or by a DFO suggestion / tidal candidate) point at one
canonical item with `duplicate_of` ("dfo:<location_id>" / "tidal:<blk>" for the other queues).
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "handoff/PLAN/curation-review/entry-split-suggestions.json"
DFO = ROOT / "handoff/PLAN/curation-review/dfo-suggestions.json"
TIDAL = ROOT / "handoff/PLAN/curation-review/tidal/tidal-candidates.json"
KINDS = {"undrawn_part": "undrawn_part", "unresolved_locator": "unresolved_locator",
         "unresolved": "unresolved", "unplaced_pieces": "unplaced", "ambiguous_cut": "ambiguous_cut"}
#: the tolerance (m) two methods must agree within
TOL_POINT, TOL_CONFL = 150.0, 300.0

OVERPASS = "https://overpass-api.de/api/interpreter (OSM, queried 2026-10-27)"


def _pv(status, method, first, delta=None, tol=None, why=""):
    return {"status": status, "method": method, "first": first,
            "delta_m": None if delta is None else round(float(delta), 1),
            "tolerance_m": tol, "why": why}


def _sugg(op, points, items, new_splits=(), words="", tributaries=None):
    return {"op": op, "points": list(points), "items": list(items), "tributaries": tributaries,
            "new_splits": list(new_splits), "words": words}


# --------------------------------------------------------------------------- hand-researched items
#: (entry_id, rule_id) -> fields that override the generic item (kind/suggestion/confidence/...).
RESEARCH: dict[tuple[str, str], dict] = {}

_JH_TAIL = {"ref": "sg1", "anchor": "coord", "name": "John Hart Generating Station (tailrace)",
            "coord": [-125.310574, 50.044315], "on_item": "gnis:39532", "blk": "354154635",
            "measure": 5644.0, "quote": "John Hart Dam Power Station", "confidence": "medium"}
_JH_EVID = [f"{OVERPASS}: node 13523318514 power=generator (hydro) at 50.04401,-125.31113 -> Campbell River "
            "measure 5644 m (52 m off the line); node 10208287925 'John Hart Generating Station' (underground) "
            "-> 6334 m",
            "curated campbell_river__elk_falls at 7479 m; curated campbell_river__john_hart_dam_power_station "
            "at 8504 m = the dam face (owner's own note: 'is just at outlet of lake i think'; its auto-proposal "
            "said 'decide dam-face vs tailrace')",
            "https://www.renewableenergyworld.com/hydro-power/dams-civil-structures/a-global-power-energy-"
            "elite-project-canadas-first-p3-hydropower-project/ (1.8 km underground conveyance from the dam "
            "to the powerhouse)",
            "https://www.bchydro.com/community/recreation_areas/vancouver-island/john-hart.html"]
for _rid, _other, _op_words in (("campbell_river.r1", "campbell_river__elk_falls", "between Elk Falls and the station"),
                                ("campbell_river.r7", "campbell_river__elk_falls", "between Elk Falls and the station")):
    RESEARCH[("r1:campbell_river@1-10", _rid)] = {
        "suggestion": _sugg("between", [{"kind": "split", "split_id": _other}, {"kind": "new", "split_id": "sg1"}],
                            ["gnis:39532"], [_JH_TAIL], _op_words),
        "confidence": "medium",
        "reason": "the power station is BELOW Elk Falls (OSM generator at 5644 m, falls at 7479 m), not at the "
                  "dam face (8504 m): the rule's no-fishing canyon runs from the falls DOWN to the tailrace",
        "evidence": _JH_EVID,
        "preverify": _pv("disagree", "OSM power plant/generator snapped to the FWA line",
                         "curated point at the dam face", 8504 - 5644, TOL_POINT,
                         "the curated station point and OSM's station are 2.9 km apart, on opposite sides of "
                         "Elk Falls"),
        "pick": {"builder": 8504.0, "proposed": 5644.0, "agrees": False},
    }
RESEARCH[("r1:campbell_river@1-10", "campbell_river.r5")] = {
    "suggestion": _sugg("between", [{"kind": "new", "split_id": "sg1"},
                                    {"kind": "split", "split_id": "campbell_river__power_line_200m_up_of_quinsam"}],
                        ["gnis:39532"], [_JH_TAIL], "from the station tailrace down to the power line"),
    "confidence": "low",
    "reason": "with the station at the tailrace (5644 m) the curated power-line split (5534 m) is only 110 m "
              "below it, and OSM has no power line 200 m above the Quinsam (its mouth is at 4200 m): the "
              "power-line split needs the owner's eye too",
    "evidence": _JH_EVID + [f"{OVERPASS}: the only power lines crossing the Campbell are 1L104 at 5437 m and "
                            "1L103 at 5584 m (both at the switchyard); Quinsam mouth snaps to 4200 m"],
    "preverify": _pv("disagree", "OSM power-line crossings vs the book's '200 m upstream of Quinsam'",
                     "curated power-line split at 5534 m", 5534 - 4400, TOL_POINT,
                     "the book's power line (~4400 m) and the curated one (5534 m) are 1.1 km apart"),
    "pick": {"builder": 8504.0, "proposed": 5644.0, "agrees": False},
}
RESEARCH[("r4:caribou_creek@4-15", "caribou_creek.r1")] = {
    "confidence": "high",
    "reason": "the Hwy 6 bridge is at the small lake's OUTLET (976 m); the builder took the inlet (1354 m) — "
              "no visible effect: the 976–1354 m stretch is the lake's own connector and the rule binds the "
              "lake (wbk:329262655) whole",
    "evidence": [f"{OVERPASS}: way 66745479 'Vernon-Slocan Highway' (BC 6) bridge at 49.98135,-117.88905 -> "
                 "Caribou Creek measure 965 m (1 m off the line)",
                 "splits.json caribou_creek__hwy_6_bridge anchor: lake 329262655 edge 'outlet' (T179 owner-accepted)"],
    "preverify": _pv("pre-verified", "OSM Hwy 6 bridge snapped to the FWA line + the curated anchor edge",
                     "curated anchor: lake outlet", 976 - 965, TOL_POINT,
                     "both put the bridge at the lower end (976 m); the builder's pick (1354 m) is the other end"),
    "pick": {"builder": 1354.0, "proposed": 976.0, "agrees": False},
}
RESEARCH[("r7:gagnon_creek@7-30", "gagnon_creek.r1")] = {
    "suggestion": _sugg("downstream_of", [{"kind": "split", "split_id": "gagnon_creek__signs_400_m_above_williston_lake"}],
                        [], [], "downstream of fishing boundary signs near its mouth"),
    "confidence": "medium",
    "reason": "the rule says 'downstream of fishing boundary signs near its mouth'; a curated split says exactly "
              "that (2161 m) but the rule binds through the lake split (1835 m) instead",
    "evidence": ["splits.resolved.json gagnon_creek__signs_400_m_above_williston_lake: label 'fishing boundary "
                 "signs near the mouth (400 m above Williston Lake)', 2161 m"],
    "preverify": _pv("pre-verified", "the curated split's label = the rule's words; its offset from the lake end",
                     "geometry: 2161 m is 326 m above the lake end at 1835 m (book: 400 m)", 400 - 326, TOL_POINT,
                     "label and offset agree"),
    "pick": {"builder": 1835.0, "proposed": 2161.3, "agrees": False},
}
RESEARCH[("r4:kootenay_lake_s_tributaries@4-19+4-7", "kootenay_lake_tributaries.r2")] = {
    "suggestion": _sugg("between", [{"kind": "new", "split_id": "sg1"},
                                    {"kind": "split", "split_id": "kootenay_river__kootenay_idaho_border"}],
                        ["gnis:14097"],
                        [{"ref": "sg1", "anchor": "lake_inlet", "name": "Kootenay Lake", "item_id": "wbk:328974235",
                          "on_item": "gnis:14097", "blk": "356570348", "measure": 122699.1,
                          "coord": None, "quote": "upstream from Kootenay Lake", "confidence": "high"}],
                        "the Kootenay River from Kootenay Lake to the U.S. border"),
    "confidence": "high",
    "reason": "the row EXCLUDES this reach ('does not include'): it is the Kootenay River between the lake's "
              "inlet (122,699 m) and the curated Idaho-border split (168,731 m) — bind it as r1's exception",
    "evidence": ["FWA streams: blk 356570348 segments with WATERBODY_KEY 328974235 span 25,860–122,699 m",
                 "FWA lakes polygon 328974235 intersected with the line: 25,860–122,699 m",
                 "splits.resolved.json kootenay_river__kootenay_idaho_border at 168,731 m"],
    "preverify": _pv("pre-verified", "lake polygon ∩ FWA line", "the line's WATERBODY_KEY span", 0.0, TOL_POINT,
                     "the inlet measure agrees to 0.1 m"),
}
RESEARCH[("r4:kinbasket_mcnaughton_lake_s_tributaries@4-36", "kinbasket_lake_tributaries.r2")] = {
    "suggestion": _sugg("upstream_of", [{"kind": "new", "split_id": "sg1"}], ["gnis:37414"],
                        [{"ref": "sg1", "anchor": "lake_inlet", "name": "Kinbasket Lake", "item_id": "wbk:328961767",
                          "on_item": "gnis:37414", "blk": "356570372", "measure": 1724961.5, "coord": None,
                          "quote": "Columbia River upstream of Kinbasket Reservoir", "confidence": "high"}],
                        "the Columbia River upstream of Kinbasket Reservoir"),
    "confidence": "high",
    "reason": "the row EXCLUDES the Columbia upstream of the reservoir: upstream of where the Columbia enters "
              "Kinbasket (1,724,962 m; the curated 'Donald' split sits 15 km above it) — bind as r1's exception",
    "evidence": ["FWA streams: blk 356570372 segments with WATERBODY_KEY 328961767 end at 1,724,962 m",
                 "FWA lakes polygon 328961767 ∩ the line: upstream end 1,724,962 m",
                 "splits.resolved.json columbia_river__donald at 1,740,058 m (upstream, as expected)"],
    "preverify": _pv("pre-verified", "lake polygon ∩ FWA line", "the line's WATERBODY_KEY span", 0.0, TOL_POINT,
                     "the inlet measure agrees to 0.1 m"),
}
_ROSE = {
    "suggestion": _sugg("whole", [], ["wbk:329518151"], [], "Rosemond Lake (all of it lies south of the bridge)"),
    "confidence": "high",
    "reason": "the CPR (now rail-trail) bridge crosses the narrows at Rosemond Lake's NORTH tip: the whole lake "
              "is south of it — the whole-lake binding is already right; the undrawn part can be dropped",
    "evidence": [f"{OVERPASS}: way 145006909 abandoned CPR branch 'Shuswap North Okanagan Rail Trail' bridge at "
                 "50.72269,-119.05274",
                 "FWA lakes: the bridge is 29 m off Rosemond Lake's polygon (north side) and 339 m off Mara Lake's"],
    "preverify": _pv("pre-verified", "OSM bridge position vs the FWA lake polygon",
                     "the lake's north edge (FWA bbox 50.7220)", 28.8, TOL_POINT,
                     "the bridge sits on the lake's north shore line"),
}
RESEARCH[("r8:rosemond_lake@8-26", "rosemond_lake.r1")] = _ROSE
RESEARCH[("r8:mara_lake@8-26", "mara_lake.r2")] = {
    "suggestion": _sugg("whole", [], ["wbk:329518146"], [], "Mara Lake"),
    "confidence": "medium",
    "reason": "two CPR bridges: the Shuswap Subdivision main line at Sicamous (north of Mara Lake) — then ALL of "
              "Mara is south of it and the whole-lake binding is right; or the old branch bridge at Mara's south "
              "end (Rosemond's bridge) — then the rule is Rosemond's and holds on none of Mara. The owner picks",
    "evidence": [f"{OVERPASS}: ways 89451290/366966237 'Shuswap Subdivision' (CPR main) bridges at 50.8363,-118.9947, "
                 "Mara Lake's north end is 50.8256",
                 f"{OVERPASS}: way 145006909 old CPR branch bridge at 50.72269,-119.05274, 339 m south of Mara Lake"],
    "preverify": _pv("disagree", "OSM: which CPR bridge", "the row's own words", None, None,
                     "two bridges answer to 'the CPR bridge'; the readings differ by the whole lake"),
}
RESEARCH[("z7b:peace_river_navigation", "peace_river_navigation.r1")] = {
    "suggestion": _sugg("between", [{"kind": "new", "split_id": "sg1"}, {"kind": "new", "split_id": "sg2"}],
                        ["gnis:14619"],
                        [{"ref": "sg1", "anchor": "split", "split_id": "peace_river__site_c_dam", "offset_m": 2000.0,
                          "on_item": "gnis:14619", "blk": "359572348", "measure": 1598981.5, "coord": None,
                          "name": "2 km upstream of the Site C dam", "quote": "2 km upstream of the dam site",
                          "confidence": "medium"},
                         {"ref": "sg2", "anchor": "bridge", "name": "Site C downstream construction bridge",
                          "coord": [-120.897689, 56.191332], "on_item": "gnis:14619", "blk": "359572348",
                          "measure": 1596038.3, "quote": "the downstream construction bridge",
                          "confidence": "medium"}],
                        "between 2 km upstream of the Site C dam site and the downstream construction bridge"),
    "confidence": "medium",
    "reason": "an advisory (avoid boat travel), bound to the whole Peace today: the dam split + 2 km and the OSM "
              "service bridge 943 m below the dam bound it",
    "evidence": [f"{OVERPASS}: way 1061495811 highway=service bridge over the Peace at 56.19133,-120.89769 -> "
                 "1,596,038 m (dam split 1,596,981 m)",
                 "splits.resolved.json peace_river__boundary_signs_1200_m_downstream_of_site_c_dam at 1,595,781 m",
                 "https://www.sitecproject.com/boating"],
    "preverify": _pv("unchecked", "OSM bridge", "", None, None,
                     "one source for the bridge; that it is 'the downstream construction bridge' is not "
                     "confirmed by a second"),
}
_STAVE = {"confidence": "none",
          "reason": "a spawning channel FWA does not draw: it needs an added stream (data/curated/waters/"
                    "added_streams.json), not a split",
          "evidence": ["the parser's note on the rule"]}
RESEARCH[("r2:stave_river@2-8", "stave_river.r1")] = _STAVE
RESEARCH[("r2:stave_river@2-8", "stave_river.r2")] = _STAVE


# --------------------------------------------------------------------------- generic classification

LAKE_WORDS = re.compile(r"\b(shore|bay|arm|buoy|buoyed|swimming|radius|within \d+ ?m|line (drawn|between|bearing)|"
                        r"north end|south end|half|lagoon|pier|outlet bay|wetlands|narrows|drawdown|"
                        r"head of the lake|western tip|of the lake|of lake|in the waters)\b", re.I)


def _is_lake(entry: dict, registry_kind) -> bool:
    ids = list(entry.get("matched") or [])
    return bool(ids) and all(registry_kind(i) == "lake" for i in ids)


_NOT_PROPER = {"The", "A", "In", "At", "On", "Of", "From", "To", "North", "South", "East", "West", "Lake",
               "River", "Creek", "Bay", "Arm", "BC", "Hwy", "No", "Fishing", "Within", "Between", "Map", "See",
               "Spawning", "Channel", "Signs", "Sign", "Boundary", "Point", "Road", "Bridge"}


def _proper(text: str, entry: dict) -> set:
    """The text's distinctive proper names (Horsefly, Pinkut, Gullwing), less the water's own name."""
    own = set(re.findall(r"[A-Z][a-z]+", entry.get("name", "").title()))
    return {w for w in re.findall(r"\b[A-Z][a-z]{3,}\b", text or "")} - _NOT_PROPER - own


def _phrase(f: dict) -> str:
    w = f["what"]
    return w.split(": ", 1)[1].split(" — the parser")[0].strip() if ": " in w else w


def side_from_words(verbatim: str, lake_words: str) -> Optional[str]:
    """'low' (the lake's outlet end), 'high' (its inlet end) or None, from the rule's WORDS alone."""
    v = " ".join((verbatim or "").split()).lower()
    lw = re.escape(lake_words.lower())
    if re.search(rf"(upstream of|above)\s+(the\s+)?{lw}", v) or re.search(rf"from\s+{lw}\w*\s+upstream", v) \
            or re.search(rf"(inlet|head)\s+of\s+{lw}", v):
        return "high"
    if re.search(rf"(downstream of|below)\s+(the\s+)?{lw}", v) or re.search(rf"from\s+{lw}\w*\s+(downstream|down)", v) \
            or re.search(rf"outlet\s+of\s+{lw}", v) or re.search(rf"{lw}\w*\s+outlet", v):
        return "low"
    return None


#: a lake split that is not in splits.resolved.json, its span on the line read from FWA (streams.WATERBODY_KEY)
FWA_SPANS = {"babine_river__babine_lake": [111116.3, 263742.7]}   # blk 360886970, Babine Lake 329026676


def ambiguous_item(eid: str, rid: str, rule: dict, pay: dict, resolved: dict, idx: dict,
                   siblings: list = ()) -> dict:
    sid, cands, used, toward = pay["split_id"], sorted(pay["candidates"]), pay["used"], pay.get("toward")
    lo, hi = cands[0], cands[-1]
    sp = idx.get(sid) or {}
    anchor = sp.get("anchor") or {}
    lake_words = (sp.get("label") or sid.split("__", 1)[-1].replace("_", " ")).split("(")[0].strip()
    lake_words = re.sub(r"\s+\d+$", "", lake_words)
    ways, why = [], []
    w = side_from_words(rule.get("verbatim") or "", lake_words.split(" lake")[0] + " lake"
                        if " lake" in lake_words.lower() else lake_words)
    if w:
        ways.append(w)
        why.append(f"the rule's words put the cut at the lake's {'outlet' if w == 'low' else 'inlet'} end")
    if anchor.get("edge") in ("outlet", "inlet"):
        e = "low" if anchor["edge"] == "outlet" else "high"
        ways.append(e)
        why.append(f"the curated anchor is the lake's {anchor['edge']}")
    other, forced = None, False
    if toward in ("mouth", "source"):
        other = 0.0 if toward == "mouth" else float("inf")
    elif toward and toward.split("__")[-1].startswith(sid.split("__")[0] + "_into_"):
        other = 0.0                       # this water's own mouth (its confluence into the next)
    elif toward:
        ms = [r["route_measure"] for r in resolved.get(toward, [])]
        ms += FWA_SPANS.get(toward, [])
        ms += [c for s in siblings if s.get("split_id") == toward for c in s.get("candidates") or []]
        if ms:
            other = min(ms, key=lambda m: min(abs(m - lo), abs(m - hi)))
    if other is not None:
        o = "low" if other <= lo else ("high" if other >= hi else None)
        if o:
            forced = True
            ways.append(o)
            why.append("the rule's other end lies " + ("below" if o == "low" else "above") + " the lake")
    want = None
    if ways and len(set(ways)) == 1:
        want = ways[0]
    proposed = (lo if want == "low" else hi) if want else None
    agrees = None if proposed is None else abs(proposed - used) < 1
    words_backed = bool(w) or anchor.get("edge") in ("outlet", "inlet")
    if proposed is None:
        status = "disagree" if len(set(ways)) > 1 else "unchecked"
        pv = _pv(status, "rule words / anchor edge / endpoint order", "the builder's E16 pick", None, None,
                 "; ".join(why) or "nothing independent says which end")
        conf = "low"
    elif agrees and (words_backed or forced):
        pv = _pv("pre-verified", ("rule words / anchor edge" if words_backed else "")
                 + (" + " if words_backed and forced else "")
                 + ("the other endpoint lies outside the lake's span: only one end can bound the reach"
                    if forced else ""), "the builder's E16 pick", 0.0, 1.0, "; ".join(why))
        conf = "high"
    elif agrees:
        pv = _pv("unchecked", "endpoint order only", "the builder's E16 pick", 0.0, 1.0,
                 "; ".join(why) + " — the same order E16 reads, so not independent")
        conf = "medium"
    else:
        pv = _pv("disagree", "rule words + anchor edge + endpoint order", "the builder's E16 pick",
                 abs(proposed - used), 1.0, "; ".join(why))
        conf = "medium"
    return {"kind": "ambiguous_cut", "phrase": lake_words,
            "suggestion": _sugg("(as authored)", [{"kind": "split", "split_id": sid}], [], [],
                                f"{sid} at {proposed if proposed is not None else used:,.0f} m"),
            "confidence": conf,
            "reason": (f"{sid} matched both lake ends ({lo:,.0f} / {hi:,.0f} m); builder took {used:,.0f} m"
                       + ("" if proposed is None else ("; agrees" if agrees else f"; the words say {proposed:,.0f} m"))),
            "evidence": [f"{pay.get('_src')}: ambiguous_cut payload (candidates, used, toward {toward})",
                         "data/curated/waters/splits.json anchor: " + (json.dumps(anchor) if anchor else
                                                                       "generated lake split (no curated anchor)")],
            "preverify": pv,
            "pick": {"builder": used, "proposed": proposed, "agrees": agrees},
            "_key": ("amb", sid, used, proposed)}


def run(out: Path = OUT) -> dict:
    from pipeline.common.curated import CURATED, GENERATED
    from pipeline.regs.review.precheck import _corpus, corpus_findings
    from pipeline.regs.review.runfacts import DIAGNOSTIC, RunFacts
    from pipeline.deliver.bundle.read import BUNDLE
    from pipeline.regs.review import marks as M
    from pipeline.regs.review.fingerprint import Context
    from pipeline.regs.dfo_salmon.coverage import similarity

    build = GENERATED.require_build()
    reach_dir = GENERATED.reaches / build.name
    run_ = RunFacts.read(reach_dir)
    ctx = Context.on_disk(CURATED.waters.splits, CURATED.regulations.overrides, build / "registry.json", BUNDLE)
    status = {k: v["status"] for k, v in M.split_states(M.load(CURATED.regulations.review.marks), ctx).items()}
    entries = _corpus(CURATED.regulations.entries.catalogue, ctx.registry)
    findings = corpus_findings(entries, registry_ids=ctx.registry, run=run_, splits=dict(ctx.splits),
                               split_status=status)
    by_eid = {e["entry_id"]: e for e in entries}
    rules = {(e["entry_id"], r["rule_id"]): r for e in entries for r in e.get("rules") or []}
    reg = json.loads((build / "registry.json").read_text())
    reg = reg.get("items", reg) if isinstance(reg, dict) else {i["id"]: i for i in reg}
    kind_of = lambda i: ((reg.get(i) or {}).get("kind") if isinstance(reg, dict) else None) or \
        ("lake" if str(i).startswith("wbk:") else "stream")
    resolved: dict[str, list] = {}
    for r in json.loads((build / "splits.resolved.json").read_text()):
        resolved.setdefault(r["split_id"], []).append(r)
    idx = {sp["id"]: sp for w in json.loads(Path(CURATED.waters.splits).read_text())["waterbodies"]
           for sp in w["splits"]}
    # the run's ambiguous cuts through the ONE run reader (`RunFacts`), in precheck's order: the
    # n-th `ambiguous_cut` finding of a rule is its n-th OPEN cut (curated picks are `cut_picked`)
    src = f"{reach_dir.relative_to(ROOT)}/{DIAGNOSTIC}.jsonl"
    diag = {(eid, rid): [{**p, "_src": src} for p in cuts]
            for eid in findings for rid, cuts in run_.open_ambiguous_of(eid).items()}
    dfo = json.loads(DFO.read_text())["suggestions"] if DFO.exists() else {}
    tidal = json.loads(TIDAL.read_text())["candidates"] if TIDAL.exists() else []

    items: dict[str, list] = {}
    seen: dict[tuple, str] = {}
    for eid, fs in findings.items():
        entry = by_eid.get(eid) or {}
        amb_n: Counter = Counter()
        done_rules: set = set()
        for f in fs:
            if f["code"] not in KINDS:
                continue
            rid = f["rule_id"]
            rule = rules.get((eid, rid)) or {}
            if f["code"] == "ambiguous_cut":
                pays = diag.get((eid, rid)) or []
                pay = pays[amb_n[rid]] if amb_n[rid] < len(pays) else None
                amb_n[rid] += 1
                if not pay:
                    continue
                it = ambiguous_item(eid, rid, rule, pay, resolved, idx, pays)
            else:
                if (rid, "place") in done_rules:       # undrawn_part + unresolved_locator on one rule: one item
                    continue
                done_rules.add((rid, "place"))
                phrase = _phrase(f)
                it = {"kind": KINDS[f["code"]], "phrase": phrase, "suggestion": None, "confidence": "none",
                      "reason": "", "evidence": [f"precheck {f['code']}: {f['what'][:300]}"],
                      "preverify": _pv("unchecked", "", "", None, None, "no proposal to check"), "pick": None,
                      "_key": (f["code"], eid, phrase.lower())}
                why = f["what"]
                if f["code"] == "unresolved" and "no_sections_for_items" in why:
                    it["reason"] = ("no registry water: the row's lake is not in this build (a same-name lake "
                                    "exists only in other MUs) — a matching problem, not a split")
                elif f["code"] == "unresolved" and "no_sections (" in why:
                    it["reason"] = "intended: the siblings bind every water in its area — nothing to cut"
                elif f["code"] == "unresolved" and "no_extents" in why:
                    it["reason"] = "no extent authored"
                elif f["code"] == "unplaced_pieces":
                    blks = {c["id"]: c for c in tidal if set(c.get("items") or []) & set(entry.get("matched") or [])}
                    if blks:
                        cid = sorted(blks)[0]
                        it["reason"] = ("pieces that flow to the sea unjoined: the tidal boundary decides them — "
                                        f"see the Tidal tab's {cid}")
                        it["duplicate_of"] = cid
                    else:
                        it["reason"] = "pieces that flow to the sea unjoined — reported, never guessed"
                elif _is_lake(entry, kind_of) or LAKE_WORDS.search(phrase):
                    it["reason"] = ("a part of a lake (a line / radius / bay), not a cut on a stream line — lake "
                                    "splits are deferred (owner rule lake-splits-deferred)")
                else:
                    it["reason"] = "no automatic proposal — see the evidence"
                # the same place in the DFO queue
                for lid, s in dfo.items():
                    if set(s.get("items") or []) & set(entry.get("matched") or []) and \
                            (similarity(s.get("text") or "", phrase) >= 0.35
                             or _proper(phrase, entry) & _proper(s.get("text") or "", entry)):
                        it["duplicate_of"] = f"dfo:{lid}"
                        it["evidence"].append(f"same place as DFO location {lid} ({s.get('outcome')})")
                        break
            r = RESEARCH.get((eid, rid))
            if r and (eid, rid, "R") not in done_rules:
                done_rules.add((eid, rid, "R"))
                it = {**it, **r, "_key": ("R", eid, rid, it.get("_key"))}
            elif r:
                continue
            it["rule_id"] = rid
            items.setdefault(eid, []).append(it)
    # ---- ids, duplicates within this file
    for eid, lst in items.items():
        for n, it in enumerate(lst, 1):
            it["id"] = f"{eid}#{it['rule_id']}#{n}"
    for eid, lst in items.items():
        for it in lst:
            k = it.pop("_key", None)
            if "duplicate_of" in it and it["duplicate_of"]:
                continue
            dk = k if k and k[0] == "amb" else (k[0], k[1], k[2]) if k and k[0] != "R" else None
            if dk and dk in seen:
                it["duplicate_of"] = seen[dk]
            else:
                it["duplicate_of"] = None
                if dk:
                    seen[dk] = it["id"]
    order = ["id", "rule_id", "kind", "phrase", "suggestion", "confidence", "reason", "evidence", "preverify",
             "duplicate_of", "pick"]
    items = {e: [{k: it.get(k) for k in order} for it in lst] for e, lst in sorted(items.items())}
    flat = [it for lst in items.values() for it in lst]
    counts = {"items": len(flat), "entries": len(items),
              "by_kind_confidence": dict(Counter(f"{i['kind']}:{i['confidence']}" for i in flat)),
              "pre_verified": sum(1 for i in flat if i["preverify"]["status"] == "pre-verified"),
              "disagree": sum(1 for i in flat if i["preverify"]["status"] == "disagree"),
              "duplicates": sum(1 for i in flat if i["duplicate_of"]),
              "with_suggestion": sum(1 for i in flat if i["suggestion"]),
              "ambiguous_pick_disagrees": sum(1 for i in flat if i["pick"] and i["pick"]["agrees"] is False)}
    doc = {"_about": (__doc__ or "").split("\n\n")[0].replace("\n", " ") +
           " Regenerate: .venv/bin/python -m pipeline.regs.review.split_suggest",
           "generated": "2026-10-27", "counts": counts, "items": items}
    out.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    return doc


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.split_suggest")
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args(argv)
    doc = run(a.out)
    print(json.dumps(doc["counts"], indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
