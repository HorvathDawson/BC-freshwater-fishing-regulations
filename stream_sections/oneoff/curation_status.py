"""Curation cockpit for docs/14-locators-to-curate.json — progress + a work queue.

Shows how far POINT curation has come per anchor_kind, and hands you the next batch of
un-curated locators to work (default 10 at a time). Items you looked at but could not resolve
can be parked with ``defer`` so they sort to the BACK of the queue and stop crowding the front.

    # progress table + the next 10 to do
    .venv/bin/python -m stream_sections.oneoff.curation_status

    # next 15, only confluence rows
    .venv/bin/python -m stream_sections.oneoff.curation_status next 15 --kind confluence_tributary

    # park one you couldn't solve (moves to the end of the queue, with a reason)
    .venv/bin/python -m stream_sections.oneoff.curation_status defer chapman-creek-6d4cfd-b --reason "falls point unknown"
    .venv/bin/python -m stream_sections.oneoff.curation_status undefer chapman-creek-6d4cfd-b

    # audit a whole status bucket with reg text + notes (e.g. sanity-check the not_applicable set-regs)
    .venv/bin/python -m stream_sections.oneoff.curation_status review not_applicable --kind confluence_tributary
    .venv/bin/python -m stream_sections.oneoff.curation_status review deferred

    # full detail for one row
    .venv/bin/python -m stream_sections.oneoff.curation_status show chapman-creek-6d4cfd-b

Statuses: todo | curated | manual | not_applicable | deferred.
  DONE  = curated + manual + not_applicable   (no more work expected)
  OPEN  = todo (fresh) then deferred (looked at, unsolved — shown last)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

_DOC = Path("stream_sections/docs/14-locators-to-curate.json")

DONE = ("curated", "manual", "not_applicable")
# queue order: fresh todo first, deferred last (0 sorts before 1)
_QUEUE_RANK = {"todo": 0, "deferred": 1}


def _load() -> dict:
    return json.loads(_DOC.read_text())


def _save(doc: dict) -> None:
    _DOC.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")


def _find(doc: dict, locator_id: str) -> dict:
    for x in doc["locators"]:
        if x["id"] == locator_id:
            return x
    raise SystemExit(f"no locator with id {locator_id!r}")


# ---------------------------------------------------------------- summary

def summary(doc: dict) -> None:
    locs = doc["locators"]
    kinds = sorted({x["anchor_kind"] for x in locs})
    statuses = ("curated", "manual", "not_applicable", "todo", "deferred")
    hdr = f"{'anchor_kind':26s} {'tot':>4} {'cur':>4} {'man':>4} {'n/a':>4} {'todo':>5} {'defer':>6} {'done%':>6}"
    print(hdr)
    print("-" * len(hdr))
    tot = {s: 0 for s in statuses}
    tot_all = 0
    for k in kinds:
        rows = [x for x in locs if x["anchor_kind"] == k]
        c = {s: sum(1 for x in rows if x["status"] == s) for s in statuses}
        for s in statuses:
            tot[s] += c[s]
        tot_all += len(rows)
        done = sum(c[s] for s in DONE)
        pct = 100 * done / len(rows) if rows else 0
        print(f"{k:26s} {len(rows):4d} {c['curated']:4d} {c['manual']:4d} "
              f"{c['not_applicable']:4d} {c['todo']:5d} {c['deferred']:6d} {pct:5.0f}%")
    print("-" * len(hdr))
    done_all = sum(tot[s] for s in DONE)
    pct = 100 * done_all / tot_all if tot_all else 0
    print(f"{'TOTAL':26s} {tot_all:4d} {tot['curated']:4d} {tot['manual']:4d} "
          f"{tot['not_applicable']:4d} {tot['todo']:5d} {tot['deferred']:6d} {pct:5.0f}%")


# ---------------------------------------------------------------- queue

def _open_queue(doc: dict, kind: str | None) -> list[dict]:
    q = [x for x in doc["locators"] if x["status"] in _QUEUE_RANK]
    if kind:
        q = [x for x in q if x["anchor_kind"] == kind]
    # fresh todo first, deferred last; stable within a rank by original order
    return sorted(q, key=lambda x: _QUEUE_RANK[x["status"]])


def next_batch(doc: dict, n: int, kind: str | None) -> None:
    q = _open_queue(doc, kind)
    todo = sum(1 for x in q if x["status"] == "todo")
    defer = len(q) - todo
    scope = f" [kind={kind}]" if kind else ""
    print(f"OPEN{scope}: {len(q)} ({todo} todo, {defer} deferred). Showing up to {n}:\n")
    for i, x in enumerate(q[:n], 1):
        tag = "  (deferred)" if x["status"] == "deferred" else ""
        mus = ",".join(x.get("mus") or [])
        lt = " ".join((x.get("locator_text") or "").split())
        if len(lt) > 96:
            lt = lt[:95] + "…"
        print(f"{i:2d}. {x['id']}{tag}")
        print(f"    {x['anchor_kind']} | {x['name_verbatim'][:34]} | MU {mus}")
        print(f"    “{lt}”")
    if not q:
        print("nothing open — all done \U0001f389")


def show(doc: dict, locator_id: str) -> None:
    print(json.dumps(_find(doc, locator_id), indent=2, ensure_ascii=False))


def review(doc: dict, status: str, kind: str | None, limit: int) -> None:
    """List items of a given status with full context (reg text + notes) for auditing —
    e.g. sanity-check the not_applicable set-regs/exclusions, or eyeball the curated points."""
    rows = [x for x in doc["locators"] if x["status"] == status]
    if kind:
        rows = [x for x in rows if x["anchor_kind"] == kind]
    scope = f" kind={kind}" if kind else ""
    print(f"REVIEW status={status}{scope}: {len(rows)} item(s)"
          + (f" (showing {limit})" if len(rows) > limit else "") + "\n")
    for x in rows[:limit]:
        mus = ",".join(x.get("mus") or [])
        print(f"• {x['id']}   [{x['anchor_kind']}]  MU {mus}")
        print(f"    name : {x['name_verbatim']}")
        if x.get("locator_text"):
            print(f"    loc  : {' '.join(x['locator_text'].split())[:110]}")
        reg = " ".join((x.get("full_regulation") or "").split())
        if reg:
            print(f"    reg  : {reg[:160]}{'…' if len(reg) > 160 else ''}")
        if x.get("coord"):
            print(f"    coord: {x['coord']}")
        if x.get("split_id"):
            print(f"    split: {x['split_id']}")
        if x.get("notes"):
            print(f"    note : {' '.join(x['notes'].split())[:200]}")
        print()


# ---------------------------------------------------------------- mutations

def defer(doc: dict, locator_id: str, reason: str | None) -> None:
    x = _find(doc, locator_id)
    if x["status"] in DONE:
        raise SystemExit(f"{locator_id} is already {x['status']} — nothing to defer")
    x["status"] = "deferred"
    if reason:
        note = f"[deferred] {reason}"
        x["notes"] = (x["notes"] + " — " if x.get("notes") else "") + note
    _save(doc)
    print(f"deferred {locator_id}" + (f": {reason}" if reason else "") + " (moved to back of queue)")


def undefer(doc: dict, locator_id: str) -> None:
    x = _find(doc, locator_id)
    if x["status"] != "deferred":
        raise SystemExit(f"{locator_id} is {x['status']}, not deferred")
    x["status"] = "todo"
    _save(doc)
    print(f"undeferred {locator_id} (back to todo)")


# ---------------------------------------------------------------- cli

def main() -> None:
    p = argparse.ArgumentParser(description="Curation progress + work queue for locators-to-curate.")
    sub = p.add_subparsers(dest="cmd")

    sub.add_parser("summary", help="progress table only")

    pn = sub.add_parser("next", help="show the next N open items (default 10)")
    pn.add_argument("n", nargs="?", type=int, default=10)
    pn.add_argument("--kind", help="filter by anchor_kind")

    ps = sub.add_parser("show", help="dump one locator as JSON")
    ps.add_argument("id")

    pr = sub.add_parser("review", help="audit items of a status with reg text + notes")
    pr.add_argument("status", nargs="?", default="not_applicable",
                    help="todo|curated|manual|not_applicable|deferred (default not_applicable)")
    pr.add_argument("--kind", help="filter by anchor_kind")
    pr.add_argument("-n", type=int, default=1000, help="max items to show")

    pd = sub.add_parser("defer", help="park an unsolved item at the back of the queue")
    pd.add_argument("id")
    pd.add_argument("--reason", help="why it's parked (appended to notes)")

    pu = sub.add_parser("undefer", help="return a deferred item to todo")
    pu.add_argument("id")

    args = p.parse_args()
    doc = _load()

    if args.cmd == "summary":
        summary(doc)
    elif args.cmd == "show":
        show(doc, args.id)
    elif args.cmd == "review":
        review(doc, args.status, args.kind, args.n)
    elif args.cmd == "defer":
        defer(doc, args.id, args.reason)
    elif args.cmd == "undefer":
        undefer(doc, args.id)
    elif args.cmd == "next":
        next_batch(doc, args.n, args.kind)
    else:  # default: summary + next 10
        summary(doc)
        print()
        next_batch(doc, 10, None)


if __name__ == "__main__":
    main()
