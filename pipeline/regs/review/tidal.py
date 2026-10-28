"""T212: THE TIDAL CANDIDATES as a review target — a mark of scope "tidal" is held against these.

The candidates are a proposal file (`handoff/PLAN/curation-review/tidal/tidal-candidates.json`, written by
`pipeline.tools.audit.tidal_candidates`): one per stream, where its tidal boundary would be cut. The
curation review shows them (its Tidal mode); ACCEPTING one is a mark (scope "tidal", answer P) that carries
the proposed cut as a `NewSplit` — never a splits.json edit (T185 turns marks into reviewed changes).

A tidal mark's prints are its candidate's: `meaning` the proposal (where the cut goes), `binding` what it
stands on (the polygon's crossing and the sources). A regenerated file that moves the cut makes the mark
stale ("rule changed"); one that only adds a source makes it stale as "binding moved".
"""
from __future__ import annotations

import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[3]
#: the candidates file; `CURATION_TIDAL_CANDIDATES` points it at a copy (tests)
DEFAULT_PATH = ROOT / "handoff/PLAN/curation-review/tidal/tidal-candidates.json"


def path() -> Path:
    env = os.environ.get("CURATION_TIDAL_CANDIDATES")
    return Path(env) if env else DEFAULT_PATH


#: T212 PASS 2 (`pipeline.tools.audit.tidal_pass2`): what DFO's own area / closure / PFMA / freshwater
#: descriptions imply, per stream — merged in as EVIDENCE only; `CURATION_TIDAL_PASS2` points it at a
#: copy ("" = none). Read alongside the default candidates file only (a test's copy gets none).
PASS2_PATH = ROOT / "handoff/PLAN/curation-review/tidal/pass2/proposals.json"


def pass2_path() -> Optional[Path]:
    env = os.environ.get("CURATION_TIDAL_PASS2")
    if env is not None:
        return Path(env) if env else None
    return None if os.environ.get("CURATION_TIDAL_CANDIDATES") else PASS2_PATH


IMPLIES_WORDS = {"is": "names the boundary here", "confirms_mouth": "DFO confirms the mouth (tidal water at the mouth)", "at_or_above": "tidal at least to here (boundary at or above)",
                 "at_or_below": "freshwater down to here (boundary at or below)", "context": "names the stream"}


def _web(ev: dict) -> dict:
    """One pass-2 evidence as a `triage.web` row (the Tidal tab's evidence list shows these)."""
    lo = ev.get("located") or {}
    loc = ({"measure": lo["measure"], "coord": lo.get("coord"), "located_by": lo.get("located_by")}
           if lo.get("measure") is not None and ev.get("effective") != "context" else None)
    page = ev.get("page_type", "").replace("_", " ")
    return {"id": ev.get("entry"), "marker": f"DFO pass 2 · {IMPLIES_WORDS.get(ev.get('effective') or 'context')}"
                                              f"{' · ' + ev['label'] if ev.get('label') else ''}",
            "kind": "dfo_pass2", "quote": ev.get("text"), "url": ev.get("url"),
            "site": f"DFO {page}{' Area ' + '/'.join(ev.get('area') or []) if ev.get('area') else ''} (pass 2, via web.archive.org)",
            "confidence": ev.get("confidence") or "low", "located": loc,
            "agrees": {"agrees": True, "consistent": True, "moves": False}.get(ev.get("vs_t212") or "")}


def _alt(pr: dict, mouth_m: float) -> dict:
    return {"id": "dfo2", "kind": "words", "label": f"DFO pass 2: {IMPLIES_WORDS.get(pr.get('implies'), '')}",
            "measure": pr["measure"], "above_mouth_m": round(float(pr["measure"]) - mouth_m, 1),
            "coord": pr.get("coord"), "words": f"DFO pass 2: {(pr.get('source_text') or '')[:200]}",
            "source": pr.get("url"), "located_by": pr.get("located_by"), "confidence": pr.get("confidence"),
            "supported": pr.get("implies") == "is", "proposed": False}


def merge_pass2(by_id: dict, order: list, doc2: dict) -> tuple[dict, list]:
    """THE PASS-2 EVIDENCE MERGED IN, additively: on a listed candidate (a copy) its evidence joins
    `triage.web`, its located point joins `alternates`, `pass2` says how it bears on the proposal, and a
    candidate pass 2 MOVES or whose mark it contradicts is put back in the "missed" triage (the review
    queue). `proposed`, `polygon` and `sources` are never touched, so no mark goes stale. A stream with
    no candidate becomes one (`pass2_new`), its proposal the pass-2 point."""
    import copy
    by_id, order = dict(by_id), list(order)
    for p in doc2.get("proposals") or []:
        evs = [e for e in p.get("evidence") or []]
        evs.sort(key=lambda e: (e.get("effective") == "context", {"high": 0, "medium": 1}.get(e.get("confidence"), 2)))
        web = [_web(e) for e in evs[:8]]
        pr = p.get("proposed")
        dm = p.get("dfo_mouth")
        info = {"status": p["status"], "mark_conflict": p.get("mark_conflict"), "proposed": pr,
                "n_entries": len(evs), "dfo_mouth": dm}
        r0 = by_id.get(p["id"])
        if r0 is not None:
            r = copy.deepcopy(r0)
            t = r.setdefault("triage", {"class": None, "web": []})
            t["web"] = [*(t.get("web") or []), *web]
            if pr and pr.get("coord") and pr.get("implies") not in ("context", "confirms_mouth"):
                r["alternates"] = [*(r.get("alternates") or []), _alt(pr, float(r["mouth"]["measure"]))]
            if dm:
                # DFO CONFIRMS THE MOUTH (owner 2026-10-28): at the mouth with nothing upstream -> high; else listed
                r["dfo_mouth"] = dm["verdict"]
                r["dfo_mouth_why"] = dm["why"]
                if dm["verdict"] == "upgrade":
                    r["confidence_before_pass2"] = r.get("confidence")
                    r["confidence"] = "high"
                    r["why"] = f"DFO confirms the mouth; {r.get('why') or ''}".strip("; ")
            if p["status"] == "moves" or p.get("mark_conflict"):
                t["class_before_pass2"] = t.get("class")
                t["class"] = "missed"
                t["why"] = (f"DFO pass 2 {'contradicts the mark' if p.get('mark_conflict') else 'moves the proposal'}: "
                            f"{IMPLIES_WORDS.get((pr or {}).get('implies'), '')} at "
                            f"{(pr or {}).get('above_mouth_m', 0):.0f} m ({(pr or {}).get('located_by')})"
                            + (f"; was: {t.get('why')}" if t.get("why") else ""))
            r["pass2"] = info
            by_id[p["id"]] = r
            continue
        if not pr or not pr.get("coord") or not p.get("items"):
            continue
        mouth = p.get("mouth") or {}
        m0 = float(mouth.get("measure") or 0.0)
        label = ("mouth (tidal boundary)" if pr.get("implies") == "confirms_mouth" else "tidal boundary (DFO pass 2)")
        by_id[p["id"]] = {
            "id": p["id"], "water": p["water"], "blk": p["blk"], "wsc": p.get("wsc"), "gnis_id": p.get("gnis_id"),
            "enters": "tidal_water", "items": p["items"], "item_names": [p["water"]], "zones": [],
            "proposed": {"measure": pr["measure"], "coord": pr["coord"], "above_mouth_m": pr["above_mouth_m"],
                         "basis": f"DFO pass 2: {pr.get('basis')}", "split_label": label,
                         "split_name": f"{p['water']}: {label}", "aliases": ["tidal boundary"], "reuse": None},
            "polygon": {"crossing": "pass2 only", "measure": None, "coord": None, "cuts": [], "n_cuts": 0,
                        "inside_pieces": 0, "outside_below_m": 0.0, "gaps_filled": [], "ignored_pieces": [],
                        "multi_crossing": False, "mouth_gap_m": 0.0},
            "mouth": {"measure": m0, "coord": mouth.get("coord")},
            "sources": [], "confidence": pr.get("confidence") or "low", "why": pr.get("basis"),
            "words_disagree": False, "agreement": "pass 2 only",
            "triage": {"class": "missed", "edited": None, "edit_sites": [], "web": web,
                       "why": "not a T212 candidate: a DFO line in tidal water crosses this stream (pass 2)"},
            "alternates": [{**_alt(pr, m0), "proposed": True},
                           *([{"id": "mouth", "kind": "mouth", "label": "FWA mouth", "measure": m0, "above_mouth_m": 0.0,
                               "coord": mouth["coord"]}] if mouth.get("coord") else [])],
            "pass2": {**info, "new": True}, "pass2_new": True,
            **({"dfo_mouth": "new", "dfo_mouth_why": dm["why"]} if dm else {})}
        order.append(p["id"])
    return by_id, order


#: DFO'S DEFAULT RULE (owner 2026-10-28) — DFO Pacific's recreational-fishing definitions page, cached with the
#: other DFO pages (data/source/dfo_tidal_areas/2026-10-28/fm-gp/rec/definitions-eng.html, snapshot 20260421173623)
DEFAULT_URL = "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/definitions-eng.html"
DEFAULT_CACHE = "data/source/dfo_tidal_areas/2026-10-28/fm-gp/rec/definitions-eng.html"
DEFAULT_QUOTE = ("Tidal water boundary: Unless otherwise specified, the tidal boundary is a straight line drawn between "
                 "the two most seaward points of land located on either side of the mouth of a river or stream.")
DEFAULT_REASON = "DFO default (unless otherwise specified): the mouth"
#: the polygon's top crossing this close (m) to the FWA mouth IS the mouth (`tidal_candidates.AT_MOUTH_M`)
DEFAULT_POLY_M = 75.0
#: a DFO line in tidal water this close (m) above the mouth is at the mouth (`tidal_pass2.MOUTH_TOL_M`)
DEFAULT_LINE_M = 150.0


def specified_by(r: dict, evs: list) -> list[str]:
    """WHAT NAMES THIS STREAM'S TIDAL BOUNDARY ITSELF, located on the line: a word source located there
    (synopsis box / row, DFO tidal-area page or notice, a curated split), a web finding located there (a
    sign, bridge, road, rail, line — not "other"), or a DFO pass-2 entry whose words name the boundary ("is"). Words
    that could not be located, a polygon crossing, a guess, a gauge — none of these specify it."""
    out = []
    for s in r.get("sources") or []:
        if s.get("kind") != "tidal_gauge" and s.get("measure") is not None:
            out.append(f"{s['kind']}: {s.get('source')}")
    for w in (r.get("triage") or {}).get("web") or []:
        # "other" is a finding that is not a boundary landmark (Marble River: a habitat survey's tidal reach)
        if w.get("kind") not in ("dfo_pass2", "dfo_default", "other") and (w.get("located") or {}).get("measure") is not None:
            out.append(f"web ({w.get('kind')}): {w.get('marker')}")
    for e in evs:
        if e.get("effective") == "is" and (e.get("located") or {}).get("measure") is not None:
            out.append(f"DFO pass 2 names it: {e.get('label') or e.get('entry')}")
    return out


def default_verdict(r: dict, evs: list) -> Optional[dict]:
    """DFO'S DEFAULT (owner 2026-10-28): a stream nothing specifies (`specified_by` empty) takes the boundary
    at its FWA mouth — "high", or "conflict" where the tidal polygon's top crossing sits above the mouth, a
    DFO line in tidal water crosses it above the mouth, or pass 2 found an upstream source (listed, not
    moved). None: something specifies the boundary."""
    if specified_by(r, evs):
        return None
    m0 = float((r.get("mouth") or {}).get("measure") or 0.0)
    against = []
    poly = r.get("polygon") or {}
    if poly.get("crossing") in ("clean", "blobs", "at_mouth") and poly.get("measure") is not None:
        up = float(poly["measure"]) - m0
        if up > DEFAULT_POLY_M:
            against.append(f"the tidal polygon crosses {up:.0f} m above the mouth"
                           + (" (owner-edited polygon)" if (r.get("triage") or {}).get("edited") else ""))
    for e in evs:
        lo = e.get("located") or {}
        if e.get("effective") == "at_or_above" and lo.get("above_mouth_m") is not None and lo["above_mouth_m"] > DEFAULT_LINE_M:
            against.append(f"a DFO line in tidal water crosses {lo['above_mouth_m']:.0f} m above the mouth ({e.get('label') or e.get('entry')})")
    if r.get("dfo_mouth") == "conflict":
        against.append(f"pass 2: {r.get('dfo_mouth_why')}")
    unlocated = [s for s in r.get("sources") or [] if s.get("kind") != "tidal_gauge" and s.get("measure") is None]
    note = (f"; {len(unlocated)} source(s) mention a boundary but locate nothing (e.g. {unlocated[0].get('source')})"
            if unlocated else "; no source specifies a boundary")
    if against:
        return {"verdict": "conflict", "why": f"{DEFAULT_REASON}{note} — but {'; '.join(against)}"}
    return {"verdict": "high", "why": f"{DEFAULT_REASON}{note}"}


def apply_default(by_id: dict, doc2: dict | None) -> dict:
    """THE DFO DEFAULT on every candidate, additively (`proposed`, `polygon`, `sources` untouched — no mark
    goes stale): `specified` (what names the boundary), `dfo_default` high | conflict | None and its `why`;
    a default row gains the definitions page as evidence. The default's own grade is `dfo_default` ("high"):
    `confidence` stays the candidate's, since `proposals` grades a non-mouth cut by it (a mouth cut has none)."""
    import copy
    evs_by = {p["id"]: p.get("evidence") or [] for p in (doc2 or {}).get("proposals") or []}
    out = {}
    for cid, r0 in by_id.items():
        r = copy.deepcopy(r0)
        evs = evs_by.get(cid, [])
        r["specified"] = specified_by(r, evs)
        dv = default_verdict(r, evs)
        r["dfo_default"] = dv["verdict"] if dv else None
        r["dfo_default_why"] = dv["why"] if dv else None
        if dv:
            mouth = r.get("mouth") or {}
            t = r.setdefault("triage", {"class": None, "web": []})
            t["web"] = [{"id": "dfo-default", "marker": DEFAULT_REASON, "kind": "dfo_default", "quote": DEFAULT_QUOTE,
                         "url": DEFAULT_URL, "site": f"DFO Pacific recreational fishing definitions (cached: {DEFAULT_CACHE})",
                         "confidence": "high" if dv["verdict"] == "high" else "medium",
                         "located": ({"measure": mouth.get("measure"), "coord": mouth.get("coord"), "located_by": "the FWA mouth"}
                                     if mouth.get("coord") else None),
                         "agrees": dv["verdict"] == "high"}, *(t.get("web") or [])]
        out[cid] = r
    return out


@lru_cache(maxsize=2)
def _load(p: str, _mtime: float, p2: str | None = None, _mtime2: float | None = None) -> dict:
    doc = json.loads(Path(p).read_text())
    by_id = {r["id"]: r for r in doc.get("candidates") or []}
    order = [r["id"] for r in doc.get("candidates") or []]
    pass2 = doc2 = None
    if p2:
        doc2 = json.loads(Path(p2).read_text())
        by_id, order = merge_pass2(by_id, order, doc2)
        pass2 = doc2.get("counts")
    by_id = apply_default(by_id, doc2)
    return {"coverage": {**(doc.get("coverage") or {}), **({"pass2": pass2} if pass2 else {})},
            "about": doc.get("_about") or "", "by_id": by_id, "order": order}


def load(p: Path | None = None) -> dict:
    """{coverage, about, by_id, order} — empty when the file is absent (no proposals yet); the pass-2
    evidence merged in when its file is there (`merge_pass2`)."""
    p = Path(p or path())
    try:
        mt = p.stat().st_mtime
    except OSError:
        return {"coverage": {}, "about": "", "by_id": {}, "order": []}
    p2 = pass2_path()
    try:
        mt2 = p2.stat().st_mtime if p2 is not None else None
    except OSError:
        p2, mt2 = None, None
    return _load(str(p), mt, str(p2) if p2 else None, mt2)


def candidate(cid: str, p: Path | None = None) -> Optional[dict]:
    return load(p)["by_id"].get(cid)


def _h(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def prints(row: dict) -> dict:
    """THE PRINTS of a candidate (`marks.Prints` shape): no book text; one `split:<id>` record."""
    pr = row.get("proposed") or {}
    meaning = {"water": row.get("water"), "blk": row.get("blk"), "measure": pr.get("measure"),
               "label": pr.get("split_label")}
    binding = {"polygon": row.get("polygon"),
               "sources": sorted(json.dumps([s.get("kind"), s.get("source"), s.get("measure")])
                                 for s in row.get("sources") or [])}
    return {"text": None, "records": [{"key": f"split:{row['id']}", "meaning": _h(meaning),
                                       "binding": _h(binding)}]}


def prints_of(cid: str, p: Path | None = None) -> Optional[dict]:
    row = candidate(cid, p)
    return prints(row) if row is not None else None
