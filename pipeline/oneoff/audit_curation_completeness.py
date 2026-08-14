"""Curation-completeness audit: did every curated *physical cut* survive into splits.json?

This is the gate before deleting the curation (waterbody-splits.json). It does NOT care about
fields that intentionally change or drop — statuses, n/a & deferred rows, and relabelled/renamed
ids/labels are all fine. It DOES care that every kept row (status curated|manual) is accounted for:
it either became a split, merged into one (same physical cut / dedup), or is a listed no-anchor row.
Anything else is a silent loss and fails the audit.

Run:  python -m pipeline.oneoff.audit_curation_completeness
"""

from __future__ import annotations

from collections import Counter

from pipeline.oneoff.waterbody_splits import load_curation
from pipeline.oneoff.build_splits import build_waterbodies, DROP_ROWS, KEEP_STATUSES
from pipeline.splits.splits import load_split_defs


def main() -> None:
    rows = load_curation()
    waterbodies, stats = build_waterbodies(rows)
    prov = stats["provenance"]
    accounted = {rid for ids in prov.values() for rid in ids}
    no_anchor_ids = {rid for _, rid in stats["no_anchor_rows"]}

    status_counts: Counter = Counter()
    became: list = []; merged: list = []; noanchor: list = []; dropped_status: list = []
    dropped_rows: list = []; LOST: list = []
    for r in rows:
        rid = r.get("id"); st = r.get("status")
        status_counts[st] += 1
        if st not in KEEP_STATUSES:
            dropped_status.append(rid); continue           # intentional: n/a, deferred, todo, error
        if rid in DROP_ROWS:
            dropped_rows.append(rid); continue             # intentional: explicit drop (redundant dup)
        if rid in accounted:
            (merged if any(rid in ids[1:] for ids in prov.values()) else became).append(rid)
        elif rid in no_anchor_ids:
            noanchor.append(rid)
        else:
            LOST.append(rid)                               # <-- unexpected silent loss

    # a merged row is one that is NOT the first (representative) id of its split
    firsts = {ids[0] for ids in prov.values()}
    merged = [rid for rid in accounted if rid not in firsts]
    became = [rid for rid in accounted if rid in firsts]

    defs = load_split_defs("pipeline/splits.json")
    load_loss = stats["splits"] - len(defs)

    print("=== Curation completeness audit ===")
    print(f"curation rows: {len(rows)}  | status breakdown: {dict(status_counts)}")
    print(f"KEEP statuses: {sorted(KEEP_STATUSES)}  (everything else is intentionally dropped)")
    print()
    print(f"kept rows accounted for:")
    print(f"  -> became a split : {len(became)}")
    print(f"  -> merged (dedup) : {len(merged)}   (same physical cut as another row)")
    print(f"  -> no-anchor drop : {len(noanchor)}   (row had no resolvable anchor)")
    print(f"  intentional drops : status={len(dropped_status)}  DROP_ROWS={len(dropped_rows)}")
    print()
    print(f"splits built: {stats['splits']}  | load_split_defs: {len(defs)}  | load-loss: {load_loss}")
    print()
    if noanchor:
        print(f"NO-ANCHOR rows ({len(noanchor)}) — confirm these carry no physical cut:")
        for rid in noanchor:
            r = next(x for x in rows if x.get('id') == rid)
            print(f"   {rid}  | {r.get('name_verbatim')}  | kind={r.get('anchor_kind')} | {(r.get('locator_text') or '')[:60]}")
    if LOST:
        print(f"\n!!! LOST ({len(LOST)}) — kept curated/manual rows that vanished with NO split/merge/no-anchor:")
        for rid in LOST:
            r = next(x for x in rows if x.get('id') == rid)
            print(f"   {rid}  | {r.get('name_verbatim')}  | {r.get('anchor_kind')}")
    verdict = "PASS — no silent loss" if (not LOST and load_loss == 0) else "FAIL — investigate above"
    print(f"\nVERDICT: {verdict}")


if __name__ == "__main__":
    main()
