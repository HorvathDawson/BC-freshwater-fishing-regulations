"""Id resync of the in-season matches file against a candidate registry (next-rebuild.md step 4).

    python handoff/PLAN/app-integration/T664-pending/resync_notice.py <notice_waters.json> <registry.json> [--at ISO]

Every match extent naming an item the registry ABSORBED into another (the id is an alias of the survivor) is renamed to
the survivor; duplicates collapse to one extent. The match's resolved `waters` keep only the survivor (marked whole when
every renamed extent was whole); they and the stamp are re-resolved by the release upload. A changed match gets a `set`
history row. Run it on a COPY of the curated tree (`CURATED_ROOT`); it refuses the live file unless `--live`.
Idempotent. Prints what it changed.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("matches", type=Path)
    ap.add_argument("registry", type=Path)
    ap.add_argument("--at", default="2026-11-11T23:59:00Z", help="the history row's time (fixed, so a rerun is identical)")
    ap.add_argument("--live", action="store_true", help="allow writing the live curated file (after a backup)")
    a = ap.parse_args(argv)
    from pipeline.common import curated as C
    from pipeline.regs.inseason import matches as M
    live = Path(C.load_repo().inseason.notice_waters).resolve()
    if a.matches.resolve() == live and not a.live:
        raise SystemExit(f"resync_notice: {a.matches} is the LIVE file — run on the side copy, or back it up and pass --live")
    items = json.loads(a.registry.read_text(encoding="utf-8"))["items"]
    ids = {it["id"] for it in items}
    alias = {al: it["id"] for it in items for al in it.get("aliases") or ()}
    raw = a.matches.read_text(encoding="utf-8")
    doc = M.parse(raw)
    if M.dumps(doc) != raw:
        raise SystemExit(f"resync_notice: {a.matches} does not round-trip through its own writer; not touching it")
    changed = 0
    for rid in sorted(doc.matches):
        m = doc.matches[rid]
        dead = sorted({e.item_id for e in m.extents if e.item_id and e.item_id not in ids and e.item_id not in alias})
        if dead:
            raise SystemExit(f"resync_notice: {rid} names item(s) the registry does not have at all: {dead}")
        if not any(e.item_id in alias and e.item_id not in ids for e in m.extents if e.item_id):
            continue
        seen, extents, took = set(), [], {}
        for e in m.extents:
            new = alias.get(e.item_id, e.item_id) if e.item_id not in ids else e.item_id
            if new != e.item_id:
                took.setdefault(new, []).append(e)
                e = e.model_copy(update={"item_id": new})
            k = json.dumps(e.model_dump(exclude_none=True), sort_keys=True)
            if k not in seen:
                seen.add(k)
                extents.append(e)
        survivors = sorted(took)
        waters = {k: w for k, w in m.waters.items() if k in ids and k not in survivors}
        for s in survivors:
            old = m.waters.get(s)
            waters[s] = old if old is not None else M.Water(all=True)
        n_abs = sum(len(v) for v in took.values())
        note = (f"Claude {a.at[:10]} (id resync, next-rebuild step 4): {n_abs} extent(s) named item(s) the rebuilt atlas "
                f"folds into {', '.join(survivors)}; renamed to the survivor ({len(m.extents)} -> {len(extents)} extent(s)). "
                f"Waters + stamp re-resolve on the release built from that atlas.")
        new_m = m.model_copy(update={"extents": extents, "waters": waters})
        doc = M.set_match(doc, rid, new_m, at=a.at, by="claude", note=note)
        changed += 1
        print(f"  {rid} ({m.water}): {len(m.extents)} -> {len(extents)} extent(s); survivors {survivors}")
    if changed:
        a.matches.write_text(M.dumps(doc), encoding="utf-8")
    print(f"resync_notice: {changed} match(es) changed in {a.matches}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
