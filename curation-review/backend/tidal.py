"""THE TIDAL REVIEW MODE's reads (T212): every proposed tidal boundary, listed and on the map.

    queue(...)       one row per candidate (stream, where the cut would go, confidence, agreement, mark)
    progress()       marked / total, by confidence, and the coverage the proposal file states
    detail(id)       one candidate: its sources verbatim, the polygon's crossing, the proposed cut,
                     the mouth, labelled map points, the polygon's edge near it, the mark in force
    mark_for(body)   the `Mark` a tidal payload makes (each new split snapped to the stream and stamped)
                     — `reuse.post_mark` commits it, the ONE write

READ ONLY. The candidates file (`handoff/PLAN/curation-review/tidal/tidal-candidates.json`, from
`pipeline.tools.audit.tidal_candidates`) and splits.json are never written here: accepting a candidate
is a mark (scope "tidal", Place y) carrying the cut as a `NewSplit`; T185 turns marks into reviewed
changes.
"""
from __future__ import annotations

import json
import os
from collections import Counter
from functools import lru_cache
from pathlib import Path

import preverify
import reuse
from pipeline.regs.review import tidal as T

#: the build gpkg the polygon's edge is drawn from (read only); `CURATION_TIDAL_GPKG` overrides
GPKG = (Path(os.environ["CURATION_TIDAL_GPKG"]) if os.environ.get("CURATION_TIDAL_GPKG")
        else reuse._ROOT / "data/source/bc_fisheries_data.gpkg")
#: how far (m) an accepted point may sit from the stream it cuts
SNAP_MAX_M = 500.0
#: the polygon's edge is drawn within this box (m) around the candidate
EDGE_BOX_M = 4000.0
CONFIDENCES = ("high", "medium", "low")
VERIFIES = ("unmarked", "verified", "wrong", "unsure", "deferred", "stale", "todo")
#: the triage classes (`pipeline.tools.audit.tidal_triage`), and "review" = missed + sanity +
#: multi-crossing (owner, T212 follow-up 2: every stream crossing the polygon in/out/in is shown)
TRIAGE_CLASSES = ("missed", "sanity", "multi-crossing", "owner-edited", "book agrees", "no claim", "owner-decided")
REVIEW = ("missed", "sanity", "multi-crossing")
TRIAGES = (*TRIAGE_CLASSES, "review")
#: the note of an owner's un-accept (a new deferred mark superseding an accepted one; history kept)
UNACCEPT_NOTE = "owner 2026-10-28: unaccepted"


def _marked(r: dict) -> bool:
    return r["verify"] not in ("unmarked", "stale")


def groups(rows: list[dict]) -> dict:
    """THE TIDAL TAB'S PROGRESS GROUPS (owner 2026-10-28): marked / total per group — words name it (a
    source specifies the boundary), DFO confirms mouth, DFO default: mouth, conflicts — and the unmarked
    count; `default_acceptable` is what the default bulk accept would take (high, unmarked or un-accepted — never a
    row whose words name a boundary: those have no default)."""
    sel = {"words": [r for r in rows if r["words_status"] == "located"],
           "words_locate": [r for r in rows if r["words_status"] == "locate"],
           "dfo_mouth": [r for r in rows if r["dfo_mouth"] in ("upgrade", "new")],
           "dfo_default": [r for r in rows if r["dfo_default"] == "high"],
           # only WORDS make a conflict (owner 2026-10-28: the polygon is ignored for cuts)
           "conflict": [r for r in rows if r["words_conflict"]]}
    out = {k: {"total": len(v), "marked": sum(1 for r in v if _marked(r))} for k, v in sel.items()}
    out["unmarked"] = {"total": len(rows), "marked": sum(1 for r in rows if _marked(r)),
                       "unmarked": sum(1 for r in rows if r["verify"] == "unmarked")}
    out["default_acceptable"] = sum(1 for r in sel["dfo_default"] if (r["verify"] == "unmarked" or r["unaccepted"])
                                    and not r["boundary_words"])
    return out


def _states() -> dict:
    from pipeline.regs.review import marks as M
    return {m.target: m for m in M.latest(M.load(reuse.MARKS_PATH)).values() if m.scope == "tidal"}


def _verify(m, row) -> dict:
    from pipeline.regs.review import marks as M
    if m is None:
        return {"status": "unmarked", "verdict": None, "reasons": []}
    return M.status(m, M.Prints.model_validate(T.prints(row)))


def _row(r: dict, m) -> dict:
    v = _verify(m, r)
    words = [s for s in r.get("sources") or [] if s.get("kind") != "tidal_gauge"]
    t = r.get("triage") or {}
    return {"id": r["id"], "water": r["water"], "blk": r["blk"], "enters": r["enters"],
            "zones": r.get("zones") or [], "items": r.get("items") or [],
            "confidence": r["confidence"], "agreement": r["agreement"],
            "crossing": (r.get("polygon") or {}).get("crossing"),
            "above_mouth_m": (r.get("proposed") or {}).get("above_mouth_m"),
            "n_words": len(words), "words_disagree": bool(r.get("words_disagree")),
            "verify": v["status"], "reasons": v["reasons"],
            "triage": t.get("class"), "edited": t.get("edited"), "n_web": len(t.get("web") or []),
            "triage_why": t.get("why"),
            "multi_crossing": bool((r.get("polygon") or {}).get("multi_crossing")),
            "n_cuts": (r.get("polygon") or {}).get("n_cuts") or 0,
            "pv": preverify.tidal(r["id"]),
            # T212 pass 2 (owner 2026-10-28): "DFO confirms the mouth" — upgrade | conflict | new | None
            "dfo_mouth": r.get("dfo_mouth"), "dfo_mouth_why": r.get("dfo_mouth_why"),
            "pass2_new": bool(r.get("pass2_new")),
            # DFO DEFAULT (owner 2026-10-28): nothing specifies the boundary -> the mouth; high | conflict | None
            "dfo_default": r.get("dfo_default"), "dfo_default_why": r.get("dfo_default_why"),
            "specified": list(r.get("specified") or []),
            "words_conflict": list(r.get("words_conflict") or []),
            # owner 2026-10-28: words that name the boundary — "located" (words name it) | "locate" (not located yet)
            "words_status": r.get("words_status"), "boundary_words": list(r.get("boundary_words") or []),
            "words_confidence": r.get("words_confidence"),
            # un-accepted by the owner (a deferred mark superseding an accept): the default bulk accept may take it
            "unaccepted": bool(m is not None and m.verdict == "deferred" and m.note.startswith(UNACCEPT_NOTE))}


def queue(confidence: str | None = None, verify: str | None = None, enters: str | None = None,
          words: bool = False, q: str | None = None, triage: str | None = None) -> list[dict]:
    """THE TIDAL QUEUE, in the proposal file's order (words first, then confidence), filtered;
    `triage` "review" keeps missed + sanity + multi-crossing (what the owner still has to look at)."""
    doc = T.load()
    states = _states()
    rows = [_row(doc["by_id"][i], states.get(i)) for i in doc["order"]]
    if confidence:
        rows = [r for r in rows if r["confidence"] == confidence]
    if enters:
        rows = [r for r in rows if r["enters"] == enters]
    if words:
        rows = [r for r in rows if r["n_words"]]
    if triage:
        want_t = REVIEW if triage == "review" else (triage,)
        rows = [r for r in rows if r["triage"] in want_t]
    if verify:
        rows = [r for r in rows if (r["verify"] in ("unmarked", "stale") if verify == "todo"
                                    else r["verify"] == verify)]
    if q and q.strip():
        want = q.strip().casefold()
        rows = [r for r in rows if want in r["water"].casefold() or want in r["id"].casefold()]
    return rows


def progress() -> dict:
    rows = queue()
    return {"total": len(rows), "done": sum(1 for r in rows if r["verify"] not in ("unmarked", "stale")),
            "by_verify": dict(Counter(r["verify"] for r in rows)),
            "by_confidence": dict(Counter(r["confidence"] for r in rows)),
            "with_words": sum(1 for r in rows if r["n_words"]),
            "by_triage": {k: sum(1 for r in rows if r["triage"] == k) for k in TRIAGE_CLASSES},
            "by_dfo_mouth": dict(Counter(r["dfo_mouth"] for r in rows if r.get("dfo_mouth"))),
            "by_dfo_default": dict(Counter(r["dfo_default"] for r in rows if r.get("dfo_default"))),
            "groups": groups(rows),
            "coverage": T.load()["coverage"]}


# --------------------------------------------------------------------------- the detail

def _pt(coord, sid: str, label: str, *, to_verify: bool, item_id: str | None, kind: str) -> dict:
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": coord},
            "properties": {"split_id": sid, "label": label, "water": "", "name": label, "rules": [],
                           "parts": [], "to_verify": to_verify, "grade": None, "item_id": item_id,
                           "kind": kind}}


#: the short map label of each alternate kind
ALT_WORD = {"words": "words", "polygon": "polygon", "mouth": "FWA mouth", "dem": "DEM limit", "proposed": "proposal"}


def alternates(r: dict) -> list[dict]:
    """EVERY POINT THE OWNER CAN SWITCH TO (owner 2026-10-27, "in the app I can't switch points"): the
    candidate's `alternates` (`pipeline.tools.audit.tidal_words`: the words' landmark(s), each polygon
    crossing with the uppermost marked, the FWA mouth, the DEM limit), the DEM one refreshed from the
    live pre-verify file. [] for a file written before tidal_words ran."""
    alts = [dict(a) for a in r.get("alternates") or []]
    pv = preverify._items("tidal").get(r["id"]) or {}
    if pv.get("dem_limit_m") is not None and pv.get("dem_coord"):
        m0 = float(r["mouth"]["measure"])
        dem = {"id": "dem0", "kind": "dem", "label": f"DEM tidal limit (pre-verify: streambed above {pv.get('threshold_m', 4)} m)",
               "measure": round(m0 + float(pv["dem_limit_m"]), 1), "above_mouth_m": round(float(pv["dem_limit_m"]), 1),
               "coord": pv["dem_coord"], "status": pv.get("status"), "why": pv.get("why"),
               "proposed": abs(m0 + float(pv["dem_limit_m"]) - float(r["proposed"]["measure"])) < 1}
        alts = [a for a in alts if a["kind"] != "dem"] + [dem]
    return alts


def points(r: dict) -> dict:
    """The candidate's labelled map points: every ALTERNATE (split_id `alt:<id>`, clicking one switches
    the point; the proposal amber), or — a file without alternates — the PROPOSED cut, the polygon's
    crossing, the FWA mouth, and every word source located on the line."""
    item = (r.get("items") or [None])[0]
    alts = alternates(r)
    if alts:
        return {"type": "FeatureCollection", "unplaced": [], "features": [
            _pt(a["coord"], f"alt:{a['id']}",
                f"{'PROPOSED · ' if a.get('proposed') else ''}{ALT_WORD.get(a['kind'], a['kind'])}"
                f"{' (uppermost)' if a.get('uppermost') else ''} · {a['above_mouth_m']:.0f} m",
                to_verify=bool(a.get("proposed")), item_id=item, kind=a["kind"])
            for a in alts if a.get("coord")]}
    pr, poly, mouth = r["proposed"], r["polygon"], r["mouth"]
    feats = [_pt(pr["coord"], f"{r['id']}:proposed", f"PROPOSED {pr['split_name']}", to_verify=True,
                 item_id=item, kind="proposed")]
    if poly.get("coord") and poly.get("measure") != pr.get("measure"):
        feats.append(_pt(poly["coord"], f"{r['id']}:polygon", f"polygon edge ({poly['crossing']})",
                         to_verify=False, item_id=item, kind="polygon"))
    if mouth.get("coord") and mouth.get("measure") != pr.get("measure"):
        feats.append(_pt(mouth["coord"], f"{r['id']}:mouth", "FWA mouth", to_verify=False,
                         item_id=item, kind="mouth"))
    for i, s in enumerate(r.get("sources") or []):
        if s.get("coord"):
            feats.append(_pt(s["coord"], f"{r['id']}:src{i}", f"{s['kind']}: {s['words'][:60]}",
                             to_verify=False, item_id=item, kind="source"))
    for i, w in enumerate((r.get("triage") or {}).get("web") or []):
        loc = w.get("located") or {}
        if loc.get("coord"):
            feats.append(_pt(loc["coord"], f"{r['id']}:web{i}", f"web: {w['marker'][:60]}",
                             to_verify=False, item_id=item, kind="source"))
    return {"type": "FeatureCollection", "features": feats, "unplaced": []}


@lru_cache(maxsize=1)
def _polygon():
    """The tidal polygon (EPSG:3005), read once from the build gpkg; None when it is not there."""
    import sqlite3
    import shapely
    if not GPKG.exists():
        return None
    con = sqlite3.connect(f"file:{GPKG}?mode=ro", uri=True)
    try:
        (b,) = con.execute("select geom from tidal_boundary").fetchone()
    finally:
        con.close()
    flags = b[3]
    g = shapely.from_wkb(bytes(b[8 + {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[(flags >> 1) & 7]:]))
    shapely.prepare(g)
    return g


def polygon_edge(r: dict) -> dict:
    """The polygon's EDGE within `EDGE_BOX_M` of the candidate, as lon/lat lines (drawn violet,
    dashed) — so the reviewer sees where the polygon actually cuts."""
    import shapely
    from shapely.ops import transform
    empty = {"type": "FeatureCollection", "features": []}
    g = _polygon()
    c = (r.get("polygon") or {}).get("coord") or r["proposed"]["coord"]
    if g is None or not c:
        return empty
    from dfo import _to_albers
    x, y = _to_albers()(*c)
    box = shapely.box(x - EDGE_BOX_M, y - EDGE_BOX_M, x + EDGE_BOX_M, y + EDGE_BOX_M)
    edge = shapely.clip_by_rect(g.boundary, *box.bounds)
    if edge.is_empty:
        return empty
    ll = transform(reuse._to_lonlat(), edge)
    return {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": json.loads(shapely.to_geojson(ll)),
         "properties": {"label": "tidal polygon edge"}}]}


def detail(cid: str) -> dict | None:
    r = T.candidate(cid)
    if r is None:
        return None
    m = _states().get(cid)
    v = _verify(m, r)
    items = []
    for iid in r.get("items") or []:
        it = reuse._registry().get(iid)
        items.append({"id": iid, "name": it.name if it else iid, "kind": it.kind if it else None,
                      "mus": list(it.mus)[:4] if it else []})
    return reuse._json_safe({
        "row": _row(r, m), "candidate": r, "items": items, "points": points(r), "alternates": alternates(r),
        "mark": ({**json.loads(m.model_dump_json()), **v} if m is not None else None),
    })


def edge(cid: str) -> dict | None:
    r = T.candidate(cid)
    return None if r is None else polygon_edge(r)


# --------------------------------------------------------------------------- the mark

def _snap(r: dict, coord) -> dict:
    """A (lon, lat) point snapped to the candidate's stream: {blk, measure, distance_m, coord}.
    ValueError when the stream has no line or the point is not on it."""
    from shapely.geometry import Point
    import dfo
    items = list(r.get("items") or [])
    if not items:
        raise ValueError(f"{r['water']} is not in the atlas registry: nothing to cut")
    pt = Point(*dfo._to_albers()(*coord))
    s = dfo._snap(pt, items)
    if s is None:
        raise ValueError(f"{r['water']} has no line section to cut")
    if s["distance_m"] > SNAP_MAX_M:
        raise ValueError(f"the point is {s['distance_m']:.0f} m from {r['water']}: click on the stream")
    return {"blk": s["blk"], "measure": s["measure"], "distance_m": s["distance_m"],
            "coord": tuple(dfo._lonlat(s["point"])), "on_item": dfo._item_of(s["node"], items)}


def snap(cid: str, coord) -> dict | None:
    """A clicked (lon, lat) snapped to the candidate's stream, for the click-to-place point (read
    only; `mark_for` snaps again when the mark is recorded). None: no such candidate."""
    r = T.candidate(cid)
    if r is None:
        return None
    try:
        s = _snap(r, coord)
    except ValueError as e:
        return {"error": str(e)}
    above = (round(s["measure"] - float(r["mouth"]["measure"]), 1)
             if str(s["blk"]) == str(r["blk"]) else None)
    return {"coord": list(s["coord"]), "blk": s["blk"], "measure": s["measure"], "above_mouth_m": above,
            "distance_m": s["distance_m"], "on_item": s["on_item"]}


def mark_for(body: dict, *, date: str, pdf_sha256: str):
    """THE `Mark` A TIDAL PAYLOAD MAKES: the candidate's prints NOW; each new split snapped to the
    stream (refused, ValueError, when it is not on it) and its resolved point stamped; one already
    proposed by another mark reuses that proposal's id (`dfo._dedupe_against_store`). KeyError: no
    such candidate. Writes nothing — `reuse.post_mark` commits the mark."""
    from pipeline.regs.review import marks as M
    import dfo
    r = T.candidate(str(body.get("target") or ""))
    if r is None:
        raise KeyError(body.get("target"))
    stamped = []
    for raw in body.get("new_splits") or []:
        ns = M.NewSplit.model_validate(raw)
        if ns.coord is None:
            raise ValueError(f"new split {ns.ref}: a tidal boundary is a point on the stream")
        s = _snap(r, ns.coord)
        stamped.append(ns.model_copy(update={"blk": s["blk"], "measure": s["measure"],
                                             "on_item": ns.on_item or s["on_item"],
                                             "coord": s["coord"]}).model_dump(mode="json"))
    stamped, _rename, reused = dfo._dedupe_against_store(stamped)
    now = T.prints(r)
    data = {**body, "rule_id": None, "new_splits": stamped, "date": date, "pdf_sha256": pdf_sha256,
            "pages": [], "prints": now}
    return M.Mark.model_validate(data), M.Prints.model_validate(now), reused
