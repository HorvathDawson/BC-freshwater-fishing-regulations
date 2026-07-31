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

    # INTERACTIVE review loop for QGIS work — show each item + map links, skip or update inline
    .venv/bin/python -m stream_sections.oneoff.curation_status label --hint dam_weir_fence
    #   per item:  y=accept candidate · c <lon,lat>=set coord · d/x/m=defer/not-a-split/manual
    #              t wbk=..|blk=..=set target · n <note> · a <for-agent> · enter=skip · q=quit
    #   every action saves immediately + logs to docs/review_log.jsonl (agent-consumable)

    # consume a decisions.json exported by the offline HTML labeller (build_review_html.py)
    .venv/bin/python -m stream_sections.oneoff.curation_status apply decisions.json

Statuses: todo | curated | manual | not_applicable | deferred.
  DONE  = curated + manual + not_applicable   (no more work expected)
  OPEN  = todo (fresh) then deferred (looked at, unsolved — shown last)
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path

_DOC = Path("stream_sections/docs/14-locators-to-curate.json")
_LOG = Path("stream_sections/docs/review_log.jsonl")   # append-only audit trail (agent-consumable)
_CAND = re.compile(r"Candidate coord\s*\[\s*(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)\s*\]")

DONE = ("curated", "manual", "not_applicable", "auto")
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
    statuses = ("curated", "manual", "not_applicable", "auto", "todo", "deferred")
    hdr = f"{'anchor_kind':26s} {'tot':>4} {'cur':>4} {'man':>4} {'n/a':>4} {'auto':>5} {'todo':>5} {'defer':>6} {'done%':>6}"
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
              f"{c['not_applicable']:4d} {c['auto']:5d} {c['todo']:5d} {c['deferred']:6d} {pct:5.0f}%")
    print("-" * len(hdr))
    done_all = sum(tot[s] for s in DONE)
    pct = 100 * done_all / tot_all if tot_all else 0
    print(f"{'TOTAL':26s} {tot_all:4d} {tot['curated']:4d} {tot['manual']:4d} "
          f"{tot['not_applicable']:4d} {tot['auto']:5d} {tot['todo']:5d} {tot['deferred']:6d} {pct:5.0f}%")


# ---------------------------------------------------------------- queue

def _open_queue(doc: dict, kind: str | None, hint: str | None) -> list[dict]:
    q = [x for x in doc["locators"] if x["status"] in _QUEUE_RANK]
    if kind:
        q = [x for x in q if x["anchor_kind"] == kind]
    if hint:
        q = [x for x in q if x.get("resolver_hint") == hint]
    # fresh todo first, deferred last; stable within a rank by original order
    return sorted(q, key=lambda x: _QUEUE_RANK[x["status"]])


def next_batch(doc: dict, n: int, kind: str | None, hint: str | None) -> None:
    q = _open_queue(doc, kind, hint)
    todo = sum(1 for x in q if x["status"] == "todo")
    defer = len(q) - todo
    scope = "".join(f" [{k}={v}]" for k, v in (("kind", kind), ("hint", hint)) if v)
    print(f"OPEN{scope}: {len(q)} ({todo} todo, {defer} deferred). Showing up to {n}:\n")
    for i, x in enumerate(q[:n], 1):
        tag = "  (deferred)" if x["status"] == "deferred" else ""
        mus = ",".join(x.get("mus") or [])
        lt = " ".join((x.get("locator_text") or "").split())
        if len(lt) > 96:
            lt = lt[:95] + "…"
        hint = x.get("resolver_hint") or ""
        kind = x["anchor_kind"] + (f"/{hint}" if hint else "")
        print(f"{i:2d}. {x['id']}{tag}")
        print(f"    {kind} | {x['name_verbatim'][:34]} | MU {mus}")
        print(f"    “{lt}”")
    if not q:
        print("nothing open — all done \U0001f389")


def show(doc: dict, locator_id: str) -> None:
    print(json.dumps(_find(doc, locator_id), indent=2, ensure_ascii=False))


def review(doc: dict, status: str, kind: str | None, hint: str | None, limit: int) -> None:
    """List items of a given status with full context (reg text + notes) for auditing —
    e.g. sanity-check the not_applicable set-regs/exclusions, or eyeball the curated points."""
    rows = [x for x in doc["locators"] if x["status"] == status]
    if kind:
        rows = [x for x in rows if x["anchor_kind"] == kind]
    if hint:
        rows = [x for x in rows if x.get("resolver_hint") == hint]
    scope = "".join(f" {k}={v}" for k, v in (("kind", kind), ("hint", hint)) if v)
    print(f"REVIEW status={status}{scope}: {len(rows)} item(s)"
          + (f" (showing {limit})" if len(rows) > limit else "") + "\n")
    for x in rows[:limit]:
        mus = ",".join(x.get("mus") or [])
        hint = x.get("resolver_hint") or ""
        kind = x["anchor_kind"] + (f"/{hint}" if hint else "")
        print(f"• {x['id']}   [{kind}]  MU {mus}")
        print(f"    name : {x['name_verbatim']}")
        if x.get("locator_text"):
            print(f"    loc  : {' '.join(x['locator_text'].split())[:110]}")
        reg = " ".join((x.get("full_regulation") or "").split())
        if reg:
            print(f"    reg  : {reg[:160]}{'…' if len(reg) > 160 else ''}")
        if x.get("coord"):
            print(f"    coord: {x['coord']}")
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


_VALID_STATUS = ("todo", "curated", "manual", "not_applicable", "deferred", "auto")


def annotate(doc: dict, locator_id: str, *, status: str | None, wbk: str | None,
             blk: str | None, coord: str | None, label: str | None,
             note: str | None) -> None:
    """Write ONE row back to disk immediately (incremental persistence for batch work).

    Sets any provided field and appends `note` to notes. Saving after every call means a
    killed batch loses nothing already annotated. coord is "lon,lat".
    """
    x = _find(doc, locator_id)
    if status:
        if status not in _VALID_STATUS:
            raise SystemExit(f"bad status {status!r}; pick {'/'.join(_VALID_STATUS)}")
        x["status"] = status
    if wbk or blk:
        tgt = dict(x["target"]) if isinstance(x.get("target"), dict) else {}
        if wbk:
            tgt["wbk"] = wbk
        if blk:
            tgt["blk"] = blk
        x["target"] = tgt
    if coord:
        lon, lat = (float(v) for v in coord.split(","))
        x["coord"] = [lon, lat]
    if label is not None:
        x["label"] = label
    if note:
        x["notes"] = (x["notes"] + " — " if x.get("notes") else "") + note
    _save(doc)
    print(f"annotated {locator_id}: status={x['status']} target={x.get('target')!r} coord={x.get('coord')}")


# ---------------------------------------------------------------- interactive review + apply

def _candidate(x: dict):
    """The candidate [lon,lat] for a row: parsed from a note's 'Candidate coord [..]', else coord."""
    m = _CAND.search(x.get("notes") or "")
    if m:
        return [float(m.group(1)), float(m.group(2))]
    return x["coord"] if isinstance(x.get("coord"), list) else None


def _links(c) -> str:
    lon, lat = c
    return (f"    OSM  https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map=15/{lat}/{lon}\n"
            f"    Ggl  https://www.google.com/maps/search/?api=1&query={lat},{lon}\n"
            f"    Sat  https://www.google.com/maps/@{lat},{lon},15z/data=!3m1!1e3")


def _log(entry: dict) -> None:
    with _LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _apply(doc: dict, x: dict, *, status=None, coord=None, target=None, note=None, action="") -> None:
    """Mutate one row, save the doc NOW (incremental), and log the decision."""
    if status:
        x["status"] = status
    if coord:
        x["coord"] = coord
    if target:
        x["target"] = target
    if note:
        x["notes"] = (x["notes"] + " — " if x.get("notes") else "") + note
    _save(doc)
    _log({"ts": datetime.now().isoformat(timespec="seconds"), "id": x["id"], "action": action,
          "status": x.get("status"), "coord": x.get("coord"), "target": x.get("target"), "note": note})
    print(f"  ✓ {action}: status={x.get('status')} coord={x.get('coord')}")


_LABEL_HELP = ("[enter]/s=skip  y=accept candidate  c lon,lat=set coord  d[ reason]=defer  "
               "x[ reason]=not_a_split  m[ reason]=manual  t key=val..=set target(wbk/blk/wsc)  "
               "n <note>=add note  a <text>=flag for agent  q=quit")


def label(doc: dict, kind: str | None, hint: str | None) -> None:
    """Interactive review loop for QGIS work: shows each open item (what to find + candidate +
    map links), you skip or update; every action is saved immediately and logged to review_log.jsonl."""
    q = _open_queue(doc, kind, hint)
    if not q:
        print("nothing open in that scope."); return
    scope = "".join(f" [{k}={v}]" for k, v in (("kind", kind), ("hint", hint)) if v)
    print(f"REVIEW{scope}: {len(q)} open. Actions:\n  {_LABEL_HELP}\n")
    i = 0
    while i < len(q):
        x = q[i]; cand = _candidate(x); mus = ",".join(x.get("mus") or [])
        khint = x["anchor_kind"] + (f"/{x['resolver_hint']}" if x.get("resolver_hint") else "")
        print("=" * 92)
        print(f"[{i+1}/{len(q)}] {khint}  ·  {x['id']}  ·  {x['region']} {mus}  ·  status={x['status']}")
        print(f"  {x['name_verbatim']}")
        if x.get("locator_text"):
            print("  loc: " + " ".join(x["locator_text"].split())[:170])
        reg = " ".join((x.get("full_regulation") or "").split())
        if reg:
            print("  reg: " + reg[:300] + ("…" if len(reg) > 300 else ""))
        if isinstance(x.get("target"), dict) and x["target"]:
            print(f"  target: {x['target']}   (locate in QGIS/FWA)")
        if cand:
            print(f"  candidate: [{cand[0]}, {cand[1]}]")
            print(_links(cand))
        if x.get("notes"):
            print("  notes: " + " ".join(x["notes"].split())[:220])
        try:
            cmd = input("  > ").strip()
        except (EOFError, KeyboardInterrupt):
            print(); break
        if cmd in ("", "s"):
            i += 1; continue
        if cmd == "q":
            break
        if cmd == "?":
            print("  " + _LABEL_HELP); continue
        op, rest = cmd[0], cmd[1:].strip()
        if op == "y" and cand:
            _apply(doc, x, status="curated", coord=cand, action="accept-candidate"); i += 1
        elif op == "c":
            m = re.search(r"(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)", rest)
            if m:
                _apply(doc, x, status="curated", coord=[float(m.group(1)), float(m.group(2))],
                       action="set-coord"); i += 1
            else:
                print("  ! use 'c lon,lat'")
        elif op == "d":
            _apply(doc, x, status="deferred", note=f"[deferred] {rest}" if rest else None, action="defer"); i += 1
        elif op == "x":
            _apply(doc, x, status="not_applicable", note=f"[n/a] {rest}" if rest else None, action="not_a_split"); i += 1
        elif op == "m":
            _apply(doc, x, status="manual", note=f"[manual] {rest}" if rest else None, action="manual"); i += 1
        elif op == "n":
            _apply(doc, x, note=rest, action="note")           # stay on this item
        elif op == "a":
            _apply(doc, x, note=f"[for-agent] {rest}", action="for-agent"); i += 1
        elif op == "t":
            tgt = dict(x["target"]) if isinstance(x.get("target"), dict) else {}
            for kv in rest.split():
                if "=" in kv:
                    k, v = kv.split("=", 1); tgt[k] = v
            _apply(doc, x, target=tgt, action="set-target")    # stay on this item
        else:
            print("  ? unknown — '?' for help")
    print(f"\nsaved. log: {_LOG}")


def apply_decisions(doc: dict, path: str) -> None:
    """Consume a decisions.json exported by the offline HTML labeller and write it into the doc.
    Correct -> curated (+coord); wrong+coord -> curated; not_a_split -> not_applicable;
    defer -> deferred; skip -> left as-is. Tricky/`for-agent` rows are reported for hand-finishing."""
    dec = json.loads(Path(path).read_text())
    applied = 0; flagged = []
    for lid, v in dec.items():
        if not v.get("verdict"):
            continue
        try:
            x = _find(doc, lid)
        except SystemExit:
            print(f"  ? no row {lid}"); continue
        verdict, coord = v["verdict"], v.get("coord")
        if verdict == "correct":
            if coord:
                x["coord"] = coord
            x["status"] = "curated"
        elif verdict == "wrong":
            if coord:
                x["coord"] = coord; x["status"] = "curated"
            else:
                flagged.append(lid)              # wrong but no replacement coord -> needs an agent
        elif verdict == "not_a_split":
            x["status"] = "not_applicable"
        elif verdict == "defer":
            x["status"] = "deferred"
        # verdict == "skip": leave untouched
        if v.get("note"):
            x["notes"] = (x["notes"] + " — " if x.get("notes") else "") + "[review] " + v["note"]
        applied += 1
    _save(doc)
    print(f"applied {applied} decisions.")
    if flagged:
        print(f"NEEDS AGENT ({len(flagged)} marked 'wrong' with no coord): {', '.join(flagged)}")


# ---------------------------------------------------------------- cli

def main() -> None:
    p = argparse.ArgumentParser(description="Curation progress + work queue for locators-to-curate.")
    sub = p.add_subparsers(dest="cmd")

    sub.add_parser("summary", help="progress table only")

    pn = sub.add_parser("next", help="show the next N open items (default 10)")
    pn.add_argument("n", nargs="?", type=int, default=10)
    pn.add_argument("--kind", help="filter by anchor_kind (split type)")
    pn.add_argument("--hint", help="filter by resolver_hint (falls_obstacle/dam_weir_fence/...)")

    ps = sub.add_parser("show", help="dump one locator as JSON")
    ps.add_argument("id")

    pr = sub.add_parser("review", help="audit items of a status with reg text + notes")
    pr.add_argument("status", nargs="?", default="not_applicable",
                    help="todo|curated|manual|not_applicable|deferred (default not_applicable)")
    pr.add_argument("--kind", help="filter by anchor_kind (split type)")
    pr.add_argument("--hint", help="filter by resolver_hint")
    pr.add_argument("-n", type=int, default=1000, help="max items to show")

    pd = sub.add_parser("defer", help="park an unsolved item at the back of the queue")
    pd.add_argument("id")
    pd.add_argument("--reason", help="why it's parked (appended to notes)")

    pu = sub.add_parser("undefer", help="return a deferred item to todo")
    pu.add_argument("id")

    pa = sub.add_parser("annotate", help="write ONE row back to disk now (incremental batch work)")
    pa.add_argument("id")
    pa.add_argument("--status", help="todo|curated|manual|not_applicable|deferred")
    pa.add_argument("--wbk", help="lake waterbody key -> target.wbk")
    pa.add_argument("--blk", help="blue-line key -> target.blk")
    pa.add_argument("--coord", help="'lon,lat'")
    pa.add_argument("--label", help="section label")
    pa.add_argument("--note", help="appended to notes")

    pl = sub.add_parser("label", help="INTERACTIVE review loop (QGIS-friendly): show → skip/update → note")
    pl.add_argument("--kind", help="filter by anchor_kind (split type)")
    pl.add_argument("--hint", help="filter by resolver_hint (falls_obstacle/dam_weir_fence/...)")

    pp = sub.add_parser("apply", help="consume a decisions.json (from the offline HTML labeller)")
    pp.add_argument("decisions", help="path to decisions.json")

    args = p.parse_args()
    doc = _load()

    if args.cmd == "summary":
        summary(doc)
    elif args.cmd == "show":
        show(doc, args.id)
    elif args.cmd == "review":
        review(doc, args.status, args.kind, args.hint, args.n)
    elif args.cmd == "defer":
        defer(doc, args.id, args.reason)
    elif args.cmd == "undefer":
        undefer(doc, args.id)
    elif args.cmd == "annotate":
        annotate(doc, args.id, status=args.status, wbk=args.wbk, blk=args.blk,
                 coord=args.coord, label=args.label, note=args.note)
    elif args.cmd == "label":
        label(doc, args.kind, args.hint)
    elif args.cmd == "apply":
        apply_decisions(doc, args.decisions)
    elif args.cmd == "next":
        next_batch(doc, args.n, args.kind, args.hint)
    else:  # default: summary + next 10
        summary(doc)
        print()
        next_batch(doc, 10, None, None)


if __name__ == "__main__":
    main()
