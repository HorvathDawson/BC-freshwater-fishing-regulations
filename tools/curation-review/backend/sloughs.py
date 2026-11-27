"""THE SLOUGH REVIEW MODE's reads (T55, owner 2026-10-28): every slough / canal candidate still drawn as a
lake, with its PRE-MARK — a proposed answer ("stream", because it is narrow or a wide marsh) the owner
confirms or changes. A pre-mark is never a mark: the owner's mark (scope "slough", `Mark.call`) is the
one write, through `reuse.post_mark`; "accept all visible" posts one mark per candidate in ONE commit.

    queue(...)          one row per candidate (stream, FWA type, width, area, group, pre-mark, mark)
    progress()          counts by group and by mark
    detail(id)          one candidate: the row, the finder's evidence, its outline on the map
    marks_for(body, …)  the `Mark`s a slough payload makes (`targets` for a batch); writes nothing

READ ONLY: the automark and finder files (`pipeline.regs.review.slough`) are never written here.
"""
from __future__ import annotations

import json
from collections import Counter

import reuse
from pipeline.regs.review import slough as S

VERIFIES = ("unmarked", "verified", "unsure", "deferred", "stale", "todo")
CALLS = ("stream", "not stream")


def _states() -> dict:
    from pipeline.regs.review import marks as M
    return {m.target: m for m in M.latest(M.load(reuse.MARKS_PATH)).values() if m.scope == "slough"}


def _verify(m, row) -> dict:
    from pipeline.regs.review import marks as M
    if m is None:
        return {"status": "unmarked", "verdict": None, "reasons": []}
    return M.status(m, M.Prints.model_validate(S.prints(row)))


def _row(r: dict, m) -> dict:
    v = _verify(m, r)
    pm = S.premark(r)
    return {"id": r["id"], "name": r.get("name"), "stream": r.get("stream"), "region": r.get("region"),
            "fwa": r.get("fwa"), "width_m": r.get("width_m"), "area_ha": r.get("area_ha"),
            "rule_class": r.get("rule_class"), "group": S.group(r), "intermittent": S.intermittent(r),
            "wide_wetland": bool(r.get("wide_wetland")), "complex": r.get("complex"),
            "img_water": r.get("img_water"), "img_veg": r.get("img_veg"),
            "lat": r.get("lat"), "lon": r.get("lon"),
            # the review page's verdict (before this app), kept as the owner's
            "page_verdict": r.get("verdict") if r.get("by") == "owner" else None,
            "page_note": S.page_note(r["id"]),
            "premark": pm, "why": r.get("why"), "spot_check": bool(r.get("spot_check")),
            "verify": v["status"], "reasons": v["reasons"],
            "call": getattr(m, "call", None) if m is not None else None,
            "reviewer": m.reviewer if m is not None else None}


def queue(group: str | None = None, basis: str | None = None, verify: str | None = None,
          q: str | None = None, region: str | None = None) -> list[dict]:
    """THE SLOUGH QUEUE in review order (pre-marked, your list, review-page marks, default), filtered:
    `group` one of `slough.GROUPS`, `basis` narrow | wide marsh | intermittent, `verify` the mark
    (todo = unmarked or stale), `q` a substring of the stream / name / id, `region`."""
    doc = S.load()
    states = _states()
    rows = [_row(doc["by_id"][i], states.get(i)) for i in doc["order"]]
    if group:
        rows = [r for r in rows if r["group"] == group]
    if basis == "intermittent":
        rows = [r for r in rows if r["intermittent"]]
    elif basis:
        rows = [r for r in rows if (r["premark"] or {}).get("basis") == basis]
    if region:
        rows = [r for r in rows if r["region"] == region]
    if verify:
        rows = [r for r in rows if (r["verify"] in ("unmarked", "stale") if verify == "todo"
                                    else r["verify"] == verify)]
    if q and q.strip():
        want = q.strip().casefold()
        rows = [r for r in rows if want in f"{r['stream'] or ''} {r['name'] or ''} {r['id']}".casefold()]
    return rows


def progress() -> dict:
    rows = queue()
    review = [r for r in rows if r["group"] in ("premark", "owner list")]
    return {"total": len(review), "done": sum(1 for r in review if r["verify"] not in ("unmarked", "stale")),
            "by_group": dict(Counter(r["group"] for r in rows)),
            "by_basis": dict(Counter((r["premark"] or {}).get("basis") for r in rows if r["premark"])),
            "intermittent": sum(1 for r in rows if r["intermittent"] and r["group"] == "owner list"),
            "by_verify": dict(Counter(r["verify"] for r in review)),
            "by_call": dict(Counter(r["call"] for r in rows if r["call"]))}


def outline(cid: str) -> dict:
    """The candidate's polygon outline (lon/lat) as a line FeatureCollection, for the map."""
    fin = S.finder_row(cid) or {}
    ring = fin.get("outline")
    feats = []
    if ring:
        feats.append({"type": "Feature", "geometry": {"type": "LineString", "coordinates": ring},
                      "properties": {"label": ""}})
    if fin.get("lon") is not None and fin.get("lat") is not None:   # a small piece is a dot at low zoom
        feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [fin["lon"], fin["lat"]]},
                      "properties": {"label": "this piece"}})
    return {"type": "FeatureCollection", "features": feats}


def detail(cid: str) -> dict | None:
    r = S.candidate(cid)
    if r is None:
        return None
    m = _states().get(cid)
    fin = S.finder_row(cid) or {}
    return reuse._json_safe({
        "row": _row(r, m),
        "finder": {k: fin.get(k) for k in ("poly_layer", "fwa_feature_code", "mu", "area_m2", "elongation",
                                           "mean_width_m", "stream_name", "stream_item", "stream_item_kind",
                                           "current_item", "class", "class_reason", "evidence",
                                           "entries_missing_on_polygon")},
        "outline": outline(cid),
        "mark": ({**json.loads(m.model_dump_json()), **_verify(m, r)} if m is not None else None),
    })


def marks_for(body: dict, *, date: str, pdf_sha256: str):
    """THE `Mark`s A SLOUGH PAYLOAD MAKES — one per target (`targets`, else `target`), each held
    against its candidate's prints NOW. KeyError: an unknown candidate. Writes nothing."""
    from pipeline.regs.review import marks as M
    targets = [str(t) for t in (body.get("targets") or [body.get("target")]) if t]
    if not targets:
        raise ValueError("a slough mark names its candidate")
    if len(set(targets)) != len(targets):
        raise ValueError("a candidate is named twice")
    base = {k: v for k, v in body.items() if k not in ("targets", "target")}
    out = []
    for t in targets:
        r = S.candidate(t)
        if r is None:
            raise KeyError(t)
        out.append(M.Mark.model_validate({**base, "target": t, "rule_id": None, "date": date,
                                          "pdf_sha256": pdf_sha256, "pages": [],
                                          "prints": S.prints(r)}))
    return out
