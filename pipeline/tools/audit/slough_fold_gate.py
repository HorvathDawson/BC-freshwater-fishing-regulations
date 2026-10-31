"""T322 PROMOTE GATE (owner 2026-10-30): the slough fold's binding diff is MECHANICAL, not an owner review. The only
waters allowed to change binding are the folded slough polygons, the re-kinded items (the oxbows) and the streams
they join — plus a creek whose path to that stream now passes THROUGH a folded / re-kinded polygon (it really is the
stream's tributary now: `via_fold`, allowed and listed). ANY other section that gains, loses or rescopes a rule (or a
licensing record), and any rule that becomes or stops being unresolved, STOPS the promote (exit 1).

    python -m pipeline.tools.audit.slough_fold_gate BEFORE_REACHES AFTER_REACHES SIDE_ATLAS [--json OUT]

`SIDE_ATLAS` is the side build's atlas (its `slough_fold_report.json` + `registry.json` name the allowed sections, its
`graph.pkl` the creeks that drain into a folded polygon). A heavy job: run it through scripts/heavy.sh.
Streams both runs side by side (`reach.binding_diff._rows`), so a 19 GB run fits.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Iterator

#: how many offending rows the report keeps (the count is always exact)
KEEP = 50


def folded_polygons(report: dict) -> set[str]:
    return {f"lake:{r['wbk']}" for r in (report.get("rows") or []) if r.get("outcome") == "folded"}


def allowed_sections(report: dict, registry: dict) -> set[str]:
    """Every section of a folded polygon, of each stream that took one, and of each item re-kinded in place."""
    out = folded_polygons(report)
    for iid in [*(report.get("streams") or {}), *(report.get("own_streams") or {})]:
        it = registry.get(iid)
        if it is not None:
            out.update(it.section_ids)
    return out


def via_fold_sections(graph, polygons: set[str], allowed: set[str]) -> set[str]:
    """Every section that drains into a folded / re-kinded polygon WITHOUT first reaching the stream it joined: walk
    upstream from each polygon (`up_adj`), through other folded polygons, never into an allowed (stream / item)
    section — those were the stream or its tributaries already."""
    stop = allowed - polygons
    out: set[str] = set()
    q = [p for p in polygons if p in graph.nodes]
    seen = set(q)
    while q:
        s = q.pop()
        for ei in graph.up_adj.get(s, ()):
            o = graph.edges[ei].from_node
            if o in seen or o in stop:
                continue
            seen.add(o)
            if o not in polygons:
                out.add(o)
            q.append(o)
    return out


def violations(before: Iterator, after: Iterator, allowed: set[str], via: set[str] = frozenset(),
               via_fold: list | None = None) -> tuple[int, list[dict]]:
    """(count, the first KEEP) of the rows that differ between two SORTED ((entry, owner, section), scope) streams on
    a section outside `allowed` and outside `via` (a creek that reaches its stream through a folded polygon: those rows
    are appended to `via_fold` instead, all of them)."""
    n, kept = 0, []

    def hit(key, what):
        nonlocal n
        if key[2] in allowed:
            return
        if key[2] in via:
            if via_fold is not None:
                via_fold.append({"entry": key[0], "rule": key[1], "section": key[2], "change": what})
            return
        n += 1
        if len(kept) < KEEP:
            kept.append({"entry": key[0], "rule": key[1], "section": key[2], "change": what})
    ra, rb = next(before, None), next(after, None)
    while ra is not None or rb is not None:
        if rb is None or (ra is not None and ra[0] < rb[0]):
            hit(ra[0], "lost")
            ra = next(before, None)
        elif ra is None or rb[0] < ra[0]:
            hit(rb[0], "gained")
            rb = next(after, None)
        else:
            if ra[1] != rb[1]:
                hit(ra[0], "rescoped")
            ra, rb = next(before, None), next(after, None)
    return n, kept


def fold_gate(before: Path, after: Path, side_atlas: Path) -> dict:
    """Named `fold_gate`, not `gate`: `validate_catalogue.gate` is THE acceptance gate, and
    test_one_acceptance_gate lists every caller of a `gate` by name."""
    from pipeline.atlas.reach import binding_diff as B
    from pipeline.atlas.reach.io import rule_section_file
    from pipeline.atlas.registry import load_registry
    from pipeline.common.io.serialize import read_artifact
    rp = Path(side_atlas) / "slough_fold_report.json"
    if not rp.exists():
        raise FileNotFoundError(f"no {rp} — the side atlas was built with FOLD_CONFIRMED off")
    report = json.loads(rp.read_text(encoding="utf-8"))
    allowed = allowed_sections(report, load_registry(Path(side_atlas) / "registry.json"))
    graph = read_artifact(str(Path(side_atlas) / "graph.pkl"))
    via = via_fold_sections(graph, folded_polygons(report), allowed)
    del graph
    via_fold: list = []
    n, kept = violations(B._rows(rule_section_file(before), "rule_id"),
                         B._rows(rule_section_file(after), "rule_id"), allowed, via, via_fold)
    ln, lkept = 0, []
    lb, la = Path(before) / "licensing_section.jsonl", Path(after) / "licensing_section.jsonl"
    if lb.exists() and la.exists():
        ln, lkept = violations(B._rows(lb, "record_id"), B._rows(la, "record_id"), allowed, via, via_fold)
    ub, ua = B._unresolved(Path(before) / "rule_unresolved.jsonl"), B._unresolved(Path(after) / "rule_unresolved.jsonl")
    unres = sorted(f"{e}::{r}: {ub.get((e, r))} -> {ua.get((e, r))}" for (e, r) in set(ub) | set(ua)
                   if ub.get((e, r)) != ua.get((e, r)))
    return {"allowed_sections": len(allowed), "via_fold_sections": len(via), "via_fold_rows": len(via_fold),
            "via_fold": via_fold, "rule_rows_outside": n, "rule_sample": kept,
            "licensing_rows_outside": ln, "licensing_sample": lkept, "unresolved_changed": unres,
            "ok": not (n or ln or unres)}


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.tools.audit.slough_fold_gate", description=__doc__.split("\n")[0])
    ap.add_argument("before")
    ap.add_argument("after")
    ap.add_argument("side_atlas")
    ap.add_argument("--json", type=Path, default=None)
    a = ap.parse_args(list(argv) if argv is not None else None)
    rep = fold_gate(Path(a.before), Path(a.after), Path(a.side_atlas))
    if a.json:
        a.json.write_text(json.dumps(rep, indent=1) + "\n", encoding="utf-8")
    print(f"T322 gate: {rep['allowed_sections']:,} allowed sections; {rep['via_fold_rows']:,} row(s) via a folded "
          f"polygon (allowed, listed); outside them: {rep['rule_rows_outside']:,} rule "
          f"row(s), {rep['licensing_rows_outside']:,} licensing row(s), {len(rep['unresolved_changed'])} unresolved "
          f"change(s) -> {'PASS' if rep['ok'] else 'STOP: do not promote'}")
    for r in rep["rule_sample"][:20]:
        print(f"  {r['change']:8} {r['section']}  {r['entry']}::{r['rule']}")
    for u in rep["unresolved_changed"][:20]:
        print(f"  unresolved {u}")
    return 0 if rep["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
