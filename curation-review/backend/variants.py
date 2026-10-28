"""THE NAMES REVIEW MODE's reads (T224, owner 2026-10-28): every mined name-variant candidate (a city layer, OSM,
an ECCC station, DFO, the synopsis) for a water, with its sources and evidence. The 181 high-confidence rows are
AUTO-ACCEPTED (the owner spot-checks a sample of them here); every other row waits for the owner's mark (scope
"name", `Mark.call` accept / reject). "Accept / reject all visible" posts one mark per row in ONE commit.

    queue(...)          one row per candidate
    progress()          counts by group and by call
    detail(id)          one candidate: its row, every source with its evidence, a map point
    marks_for(body, …)  the `Mark`s a name payload makes (`targets` for a batch); writes nothing

READ ONLY: the candidates file (`pipeline.regs.review.names`) is never written here.
"""
from __future__ import annotations

import json
from collections import Counter

import reuse
from pipeline.regs.review import names as N

VERIFIES = ("unmarked", "verified", "unsure", "deferred", "stale", "todo")
CONFIDENCES = ("high", "medium", "low")


def _states() -> dict:
    from pipeline.regs.review import marks as M
    return {m.target: m for m in M.latest(M.load(reuse.MARKS_PATH)).values() if m.scope == "name"}


def _verify(i: str, c: dict, m) -> dict:
    from pipeline.regs.review import marks as M
    if m is None:
        return {"status": "unmarked", "verdict": None, "reasons": []}
    return M.status(m, M.Prints.model_validate(N.prints(i, c)))


def _point(c: dict):
    for s in c["sources"]:
        ll = (s.get("evidence") or {}).get("lonlat")
        if ll:
            return ll
    return None


def _row(i: str, c: dict, m, spot: set) -> dict:
    v = _verify(i, c, m)
    g = N.group_of(i, c)
    call = getattr(m, "call", None) if m is not None and v["status"] == "verified" else None
    owner = N.OWNER.get(i)
    # what will happen to it: accepted (auto / owner / your mark), rejected (your mark), or waiting
    if call == "reject":
        fate = "rejected"
    elif call == "accept" or owner or g == "auto":
        fate = "accepted"
    else:
        fate = "waiting"
    t = c["target"]
    ll = _point(c)
    return {"id": i, "proposed": (owner or {}).get("name") or c["proposed"], "group": g, "spot": i in spot,
            "confidence": c["confidence"], "relation": c["relation"],
            "target": t, "target_label": N.tkey(t), "current_names": c.get("current_names") or [],
            "sources": sorted({s["source_label"] for s in c["sources"]}),
            "rival_names": c.get("rival_names") or [], "corroborated_by": c.get("corroborated_by") or [],
            "display": bool((owner or {}).get("display")) or (fate == "accepted" and c["relation"] == "unnamed"),
            "owner_decision": (owner or {}).get("why"),
            "lon": ll[0] if ll else None, "lat": ll[1] if ll else None,
            "verify": v["status"], "reasons": v["reasons"], "call": call, "fate": fate,
            "reviewer": m.reviewer if m is not None else None}


def queue(group: str | None = None, confidence: str | None = None, source: str | None = None,
          verify: str | None = None, q: str | None = None, spot: bool = False) -> list[dict]:
    """THE NAMES QUEUE (owner, auto spot-check sample, rest of auto, then review by confidence), filtered:
    `group` auto | owner | review, `confidence`, `source` a substring of a source label, `verify` the mark
    (todo = unmarked or stale), `q` a substring of the name / current names / id, `spot` the spot-check sample."""
    doc = N.load()
    states = _states()
    rows = [_row(i, doc["by_id"][i], states.get(i), doc["spot"]) for i in doc["order"]]
    if group:
        rows = [r for r in rows if r["group"] == group]
    if spot:
        rows = [r for r in rows if r["spot"]]
    if confidence:
        rows = [r for r in rows if r["confidence"] == confidence]
    if source:
        want = source.casefold()
        rows = [r for r in rows if any(want in s.casefold() for s in r["sources"])]
    if verify:
        rows = [r for r in rows if (r["verify"] in ("unmarked", "stale") if verify == "todo"
                                    else r["verify"] == verify)]
    if q and q.strip():
        want = q.strip().casefold()
        rows = [r for r in rows if want in f"{r['proposed']} {' '.join(r['current_names'])} {r['id']}".casefold()]
    return rows


def progress() -> dict:
    rows = queue()
    spot = [r for r in rows if r["spot"]]
    review = [r for r in rows if r["group"] == "review"]
    return {"total": len(rows), "by_group": dict(Counter(r["group"] for r in rows)),
            "by_fate": dict(Counter(r["fate"] for r in rows)),
            "spot": {"total": len(spot), "done": sum(1 for r in spot if r["verify"] not in ("unmarked", "stale"))},
            "review": {"total": len(review), "done": sum(1 for r in review if r["verify"] not in ("unmarked", "stale")),
                       "by_confidence": dict(Counter(r["confidence"] for r in review))},
            "by_call": dict(Counter(r["call"] for r in rows if r["call"]))}


def detail(i: str) -> dict | None:
    c = N.candidate(i)
    if c is None:
        return None
    doc = N.load()
    m = _states().get(i)
    row = _row(i, c, m, doc["spot"])
    feats = []
    if row["lon"] is not None:
        feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [row["lon"], row["lat"]]},
                      "properties": {"label": row["proposed"]}})
    return reuse._json_safe({
        "row": row,
        "sources": [{"source": s["source"], "label": s["source_label"], "ref": s.get("source_ref"),
                     "official": bool(s.get("official")), "confidence": s["confidence"], "flags": s.get("flags") or [],
                     "evidence": s.get("evidence") or {}} for s in c["sources"]],
        "map": {"type": "FeatureCollection", "features": feats},
        "mark": ({**json.loads(m.model_dump_json()), **_verify(i, c, m)} if m is not None else None),
    })


def marks_for(body: dict, *, date: str, pdf_sha256: str):
    """THE `Mark`s A NAME PAYLOAD MAKES — one per target (`targets`, else `target`), each held against its
    candidate's prints NOW. KeyError: an unknown candidate. Writes nothing."""
    from pipeline.regs.review import marks as M
    targets = [str(t) for t in (body.get("targets") or [body.get("target")]) if t]
    if not targets:
        raise ValueError("a name mark names its candidate")
    if len(set(targets)) != len(targets):
        raise ValueError("a candidate is named twice")
    base = {k: v for k, v in body.items() if k not in ("targets", "target")}
    out = []
    for t in targets:
        c = N.candidate(t)
        if c is None:
            raise KeyError(t)
        out.append(M.Mark.model_validate({**base, "target": t, "rule_id": None, "date": date,
                                          "pdf_sha256": pdf_sha256, "pages": [], "prints": N.prints(t, c)}))
    return out
