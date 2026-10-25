"""THE REVIEW SCREEN's reads (T134, PLAN §4 phase 3): what a reviewer is shown to answer T, R, P, A.

    review_view(entry_id)   evidence first (where it is printed, a legible crop, the coverage
                            strip), the tier and its risk factors, each rule's impact, and the
                            answer: the app's own page by its start link
    split_queue()           tier S0 — the split points still to verify, each as a card
    progress()              done / to do per tier and overall, for the progress bar and the ETA
    tickets()               T135: the wrong and unsure marks in force, as ticket text

Read only. The one thing the review screen writes is a mark (`POST /api/marks`, `reuse.post_mark`).
Every decision is someone else's function: the tier and the queue's order (`tiers`), the pre-checks
(`precheck`), the strip (`coverage`), whether the entry is built (`vintage`), the bundle's counts
(`fingerprint.rule_section_counts`), the part to open (`answer.app_part`), a ticket (`tickets`).
"""
from __future__ import annotations

import datetime as dt

import answer
import evidence
import reuse

#: what a mark's verdict counts as on the progress bar
DONE = ("verified", "wrong", "unsure", "deferred")


def impact_line(n: int | None, region: str) -> str:
    """One line on what a zone rule binds (PLAN: "binds N sections in region R")."""
    where = "province-wide" if region in ("provincial", "p", "") else f"in region {region}"
    if n is None:
        return "awaiting build — the bundle's count is another vintage's"
    if n == 0:
        return f"binds no section {where}"
    return f"binds {n:,} section{'s' if n != 1 else ''} {where}"


def _evidence(e: dict, image_key: str | None) -> dict:
    eid = e["entry_id"]
    try:
        where = evidence.locate(e, image_key)
    except evidence.Missing as ex:
        where = {"kind": "missing", "why": str(ex),
                 "printed": next(iter(e.get("source_pages") or []), None)}
    out = {k: v for k, v in where.items() if k != "strips"}
    if where["kind"] in ("row", "passage"):
        out["crop_url"] = f"/api/entries/{eid}/evidence/crop.png"
        out["page_url"] = f"/api/entries/{eid}/evidence/page.png"
    # the extractor's own crop, offered (labelled) beside the legible one — never instead of it
    out["row_image"] = f"/api/row-image/{image_key}" if image_key else None
    return out


def located(entry_id: str) -> dict:
    """`evidence.locate` for one entry (the crop and page endpoints). KeyError: no such entry."""
    found = reuse._find(entry_id)
    if found is None:
        raise KeyError(entry_id)
    e = found[1]
    return evidence.locate(e, reuse.entry_source_image(e) if reuse.entry_kind(e) == "water"
                           else None)


def review_view(entry_id: str, day: str | None = None) -> dict | None:
    """Everything the checklist is answered from, for one entry. None: no such entry."""
    from pipeline.deliver.bundle import read as R
    from pipeline.regs.review import tiers as T
    from pipeline.regs.review.coverage import entry_coverage
    from pipeline.regs.review.fingerprint import rule_section_counts
    from pipeline.regs.review.vintage import CURRENT
    found = reuse._find(entry_id)
    if found is None:
        return None
    region, e = found
    item = reuse._item_for_entry(e)
    zone = reuse.entry_kind(e) == "zone"
    image_key = None if zone else reuse.entry_source_image(e)
    vin = reuse.entry_vintage(e)
    current = vin["state"] == CURRENT
    run = reuse.run_facts()                       # the reach run's facts (T135); None: no run
    day = day or dt.date.today().isoformat()

    counts: dict[str, int] = {}
    if current:
        db = answer._db()
        try:
            counts = rule_section_counts(db, entry_id)
        finally:
            db.close()

    def link(rule_id=None):
        if not current:
            return {"url": None, "why": "awaiting build — the app's page is another vintage's"}
        got = answer.app_part(entry_id, e.get("matched") or [], rule_id)
        if got is None:
            return {"url": None, "why": "the bundle binds this nowhere — there is no page to open"}
        return {**got, "url": answer.app_link(got["item"], got["part"], day)}

    rules = []
    for r in e.get("rules") or []:
        rid = r.get("rule_id")
        row = {"rule_id": rid}
        if zone:
            row["impact"] = impact_line(counts.get(rid, 0) if current else None, region)
            row["answer"] = link(rid)
        rules.append(row)
    return {
        "entry_id": entry_id,
        "name": e.get("name", ""),                    # THE RAW NAME: the heading (PLAN §4)
        "display_name": e.get("display_name"),
        "kind": "zone" if zone else "water",
        "region": region,
        "tier": T.entry_tier(e, binds=item is not None, run=run),
        "risks": T.risks(e, binds=item is not None, run=run),
        # T135: what a machine says before the reviewer reads (the gate included), the cuts
        # the builder chose between (ruling E16; an extent's `cut_pick` overrides), and the
        # second-pass agent's findings on the parse (none exist yet)
        "prechecks": reuse.entry_prechecks(e, run=run, gate=True),
        "ambiguous_cuts": {rid: list(cuts) for rid, cuts in
                           (run.ambiguous_of(entry_id) if run is not None else {}).items()},
        "parse_findings": reuse.parse_review_findings().get(entry_id, []),
        "run": reuse.queue_facts(),
        "evidence": _evidence(e, image_key),
        "coverage": entry_coverage(e),
        "vintage": vin,
        "answer": link(),
        "date": day,
        "rules": rules,
        "bundle": str(R.BUNDLE).rsplit("/", 1)[-1],
    }


def split_queue(only_to_verify: bool = True) -> list[dict]:
    """The split cards: every split point (tier S0 alone by default — an open auto-proposal, a
    "Verify:" note, or a split mark gone stale), its note and grade, its water, its mark status and whether the atlas holds it
    as authored. Order: not yet done (unmarked, stale) first, then grade L, M, H, ungraded, then
    the waterbody's name — PLAN §3 "L grade first"."""
    from pipeline.regs.review import tiers as T
    data = reuse._load_splits()
    states = reuse.split_mark_states()
    out = []
    for wb in data.get("waterbodies", []):
        for sp in wb.get("splits", []):
            sid = sp.get("id")
            if not sid:
                continue
            st = states.get(sid) or {"status": "unmarked", "verdict": None, "reasons": [],
                                     "vintage": None}
            s0 = T.split_needs_verify(sp, st["status"])
            if only_to_verify and not s0:
                continue
            out.append({"split_id": sid, "label": sp.get("label"), "kind": sp.get("kind"),
                        "waterbody": wb.get("name"), "grade": T.split_grade(sp),
                        "note": str(sp.get("_note") or ""), "s0": s0,
                        "item_id": reuse.split_item(sp, wb),
                        "status": st["status"], "verdict": st.get("verdict"),
                        "reasons": st.get("reasons") or [], "vintage": st.get("vintage"),
                        # the one queue's order (`tiers.split_rank`); a split not in S0 after
                        "_rank": T.split_rank(sp, st["status"]) if s0 else (len(T.TIERS), 9, 0)})
    out.sort(key=lambda r: (r["status"] in DONE, r["_rank"], str(r["waterbody"]), r["split_id"]))
    for r in out:
        del r["_rank"]
    return out


def progress() -> dict:
    """Done / to do per tier and overall. `done` is a mark in force (verified, wrong, unsure,
    deferred); `stale` a mark that no longer holds (it counts as to do); S0 counts the split
    points still to verify."""
    from pipeline.regs.review import tiers as T
    tiers = {t: {"tier": t, "total": 0, "done": 0, "stale": 0} for t in T.TIERS}
    for r in reuse.queue():
        t = tiers[r["tier"]]
        t["total"] += 1
        t["done"] += r["verify"] in DONE
        t["stale"] += r["verify"] == "stale"
    for s in split_queue():
        t = tiers["S0"]
        t["total"] += 1
        t["done"] += s["status"] in DONE
        t["stale"] += s["status"] == "stale"
    rows = list(tiers.values())
    for t in rows:
        t["todo"] = t["total"] - t["done"]
    total = {k: sum(t[k] for t in rows) for k in ("total", "done", "stale", "todo")}
    return {"tiers": rows, "overall": {"tier": "all", **total}, "facts": reuse.queue_facts()}


def tickets() -> list[dict]:
    """THE TICKET EXPORT (T135, PLAN §4 phase 4): every wrong and unsure mark in force as ticket text
    (`tickets.export`), each with what changed since it was made. Read only: the owner files them."""
    from pipeline.regs.review import marks as M
    from pipeline.regs.review import tickets as TK
    marks = M.load(reuse.MARKS_PATH)
    entries = {e["entry_id"]: e for _, e in reuse._all_entries()}
    got = TK.export(marks, entries=entries, splits=TK.split_index(reuse._load_splits()),
                    statuses=TK.statuses_now(marks, entries=entries, ctx=reuse._review_context()))
    return [{"name": n, "text": t} for n, t in got]
