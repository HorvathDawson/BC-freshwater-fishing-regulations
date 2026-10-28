"""Q154 SMALL PONDS — the Sloughs tab's "Small ponds (Q154)" view (owner 2026-10-28): the sample of small
unnamed lake polygons a river runs through, each with the cutoff's prediction (caught = takes the river's
rules). The owner marks each "pool" (river rules) / "real lake" / unsure: a mark of scope "q154", through
`reuse.post_mark` — the one write. The tally compares the owner's calls with the cutoff's prediction.

    queue(...)          one row per sample pond (measures, prediction, mark)
    tally()             the owner's calls vs the cutoff, overall and by stratum
    detail(id)          one pond: the row and its map (outline + river lines)
    marks_for(body, …)  the `Mark` a q154 payload makes; writes nothing

READ ONLY: the sample file (`pipeline.regs.review.q154`) is never written here.
"""
from __future__ import annotations

import json
from collections import Counter

import reuse
from pipeline.regs.review import q154 as Q

VERIFIES = ("todo", "decided", "unsure", "deferred", "stale")
ROW_KEYS = ("id", "set", "n", "stratum", "why", "region", "river", "order", "ha", "avg_width_m", "length_m",
            "elongation", "river_width_m", "ratio", "man_made", "caught", "md_read", "lat", "lon")


def _states() -> dict:
    from pipeline.regs.review import marks as M
    return {m.target: m for m in M.latest(M.load(reuse.MARKS_PATH)).values() if m.scope == "q154"}


def _verify(m, row) -> dict:
    from pipeline.regs.review import marks as M
    if m is None:
        return {"status": "unmarked", "verdict": None, "reasons": []}
    return M.status(m, M.Prints.model_validate(Q.prints(row)))


def _row(r: dict, m) -> dict:
    v = _verify(m, r)
    call = getattr(m, "call", None) if m is not None and v["status"] == "verified" else None
    return {**{k: r.get(k) for k in ROW_KEYS}, "verify": v["status"], "reasons": v["reasons"], "call": call,
            "agrees": None if call is None else (call == "pool") == bool(r.get("caught")),
            "reviewer": m.reviewer if m is not None else None}


def queue(set_: str | None = None, verify: str | None = None, caught: str | None = None) -> list[dict]:
    """The sample in its file order (the 20 md examples first, then the mixed sample), filtered."""
    states = _states()
    rows = [_row(r, states.get(r["id"])) for r in Q.load()["rows"]]
    if set_:
        rows = [r for r in rows if r["set"] == set_]
    if caught in ("yes", "no"):
        rows = [r for r in rows if r["caught"] == (caught == "yes")]
    if verify == "todo":
        rows = [r for r in rows if r["verify"] in ("unmarked", "stale")]
    elif verify == "decided":
        rows = [r for r in rows if r["verify"] == "verified"]
    elif verify:
        rows = [r for r in rows if r["verify"] == verify]
    return rows


def tally() -> dict:
    """THE OWNER'S CALLS vs THE CUTOFF: per prediction (caught / not caught) how many the owner called pool,
    real lake, unsure; the agreement; and the same by stratum."""
    rows = queue()

    def word(r):
        if r["verify"] == "verified":
            return r["call"]
        return "unsure" if r["verify"] == "unsure" else "todo"
    by_pred = {p: dict(Counter(word(r) for r in rows if r["caught"] == (p == "caught")))
               for p in ("caught", "not caught")}
    by_stratum: dict[str, dict] = {}
    for r in rows:
        k = f"{r['set']}: {r['stratum']}"
        by_stratum.setdefault(k, Counter())[word(r)] += 1
    called = [r for r in rows if r["agrees"] is not None]
    cut = Q.load()["cutoff"]
    return {"total": len(rows), "done": sum(1 for r in rows if r["verify"] in ("verified", "unsure", "deferred")),
            "called": len(called), "agree": sum(1 for r in called if r["agrees"]),
            # the two ways the cutoff can be wrong
            "caught_but_lake": sum(1 for r in called if r["caught"] and r["call"] == "real lake"),
            "missed_pool": sum(1 for r in called if not r["caught"] and r["call"] == "pool"),
            "by_prediction": by_pred, "by_stratum": {k: dict(v) for k, v in by_stratum.items()},
            "cutoff": cut}


def _map(r: dict) -> dict:
    """The pond's outline (violet) and the stream lines (blue: through it; pale: the others nearby)."""
    feats = [{"type": "Feature", "geometry": {"type": "LineString", "coordinates": ring},
              "properties": {"color": "#e11d48", "width": 2.5}} for ring in r.get("rings") or []]
    feats += [{"type": "Feature", "geometry": {"type": "LineString", "coordinates": ln["coords"]},
               "properties": {"color": "#22d3ee" if ln.get("through") else "#7dd3fc",
                              "width": 2.5 if ln.get("through") else 1.2}} for ln in r.get("lines") or []]
    return {"type": "FeatureCollection", "features": feats}


def _bounds(r: dict) -> list[float] | None:
    pts = [p for ring in r.get("rings") or [] for p in ring]
    if not pts:
        return None
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    return [min(xs), min(ys), max(xs), max(ys)]


def detail(pid: str) -> dict | None:
    r = Q.pond(pid)
    if r is None:
        return None
    m = _states().get(pid)
    return reuse._json_safe({
        "row": _row(r, m), "map": _map(r), "bounds": _bounds(r),
        "mark": ({**json.loads(m.model_dump_json()), **_verify(m, r)} if m is not None else None),
    })


def marks_for(body: dict, *, date: str, pdf_sha256: str):
    """THE `Mark` A Q154 PAYLOAD MAKES, held against its pond's prints NOW (a one-item list, like the slough
    batch). KeyError: an unknown pond. Writes nothing."""
    from pipeline.regs.review import marks as M
    t = str(body.get("target") or "")
    r = Q.pond(t)
    if r is None:
        raise KeyError(t)
    base = {k: v for k, v in body.items() if k not in ("targets", "target")}
    return [M.Mark.model_validate({**base, "target": t, "rule_id": None, "date": date, "pdf_sha256": pdf_sha256,
                                   "pages": [], "prints": Q.prints(r)})]
