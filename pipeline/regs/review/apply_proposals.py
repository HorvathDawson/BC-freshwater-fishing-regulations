"""T185: APPLY THE OWNER'S MARKS — Claude runs this; one commit per proposal.

    python -m pipeline.regs.review.apply_proposals --dry-run     # what would land, as diffs
    python -m pipeline.regs.review.apply_proposals [--only ID]   # land each, `git commit -- <paths>`

THE MARKS ARE THE DECISIONS (owner 2026-10-27): there is no accept step. Every applicable proposal
the marks in force make is landed, rebuilt from the marks and the curated files NOW
(`proposals.build`), never read from the written patches. To change or stop one, the owner changes
the mark. New splits land first; a proposal naming a new split lands only once that split is in
splits.json (applied in this run, or before). After each commit the proposals are rebuilt, so the
next one is made on the files as they now are.

Before each lands:
  * THE BINDING CHECK — the in-memory before / after binding of every rule (`proposals.check`);
    it is printed, carried in the commit message, and a check that fails refuses the proposal;
  * A CUT THAT CHANGES (a new split, or one the proposal names that landed in this run) needs its
    SIDE REBUILD first: the side atlas at `proposals.side_dir` (named by the proposal's digest),
    made by the printed `proposals.side_commands`, with its binding diff read. Without it the
    proposal waits and its commands are printed.

Refused per proposal (and said why): not applicable, a new split not applied, the side rebuild not
made, a failed binding check, a file with uncommitted changes (a commit would sweep them in), a
failed commit (the files are put back). Run the normal rebuild afterwards (`GET /api/build` lists
it); each changed entry re-enters the review queue as a re-check (its fingerprint moved, T97).
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Mapping

from pipeline.regs.review import proposals as P


def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=P.REPO, capture_output=True, text=True)


def _dirty(paths: list[Path]) -> list[str]:
    got = _git("status", "--porcelain", "--", *[str(p) for p in paths])
    return [ln for ln in got.stdout.splitlines() if ln.strip()]


def check_words(c: Mapping | None) -> list[str]:
    """The binding check in lines (the commit message and the console say the same)."""
    if not c:
        return ["binding check: none"]
    if not c.get("cheap"):
        return [f"binding check: {c.get('why')}"]
    return [f"{rid}: {r['n_before']} -> {r['n_after']} sections (+{r['gained']} -{r['lost']}), "
            f"{r['km_before']} -> {r['km_after']} km, {r['outcome_before']} -> {r['outcome_after']}"
            for rid, r in (c.get("rules") or {}).items()] or ["binding check: no rule moved"]


def message(p: Mapping) -> str:
    marks = "\n".join(f"- {m['target']}{' / ' + m['rule_id'] if m.get('rule_id') else ''}: "
                      f"{m['reviewer']}, {m['date']}"
                      + (f" — {m['note']}" if m.get("note") else "") for m in p["marks"])
    conf = f"Confidence: {p['confidence']}.\n" if p.get("confidence") else ""
    side = (f"Side rebuild: {P.side_dir(p)}.\n" if p.get("_side") else "")
    return (f"T185 apply {p['id']}: {p['title']}"[:200] + "\n\n"
            f"From the owner's marks (the marks are the decision, proposal digest {p['digest']}):\n"
            f"{marks}\n{conf}{side}" + "\n".join(check_words(p.get("check"))) + "\n")


def needs_side(p: Mapping, props: Iterable[Mapping], landed_sids: set[str]) -> bool:
    """Does `p` add or move a cut — its own split edit, a new split it waits on, or a split it
    names that landed in THIS run (the live atlas has not got it yet)?"""
    if P.changes_cuts(p, props):
        return True
    return any(sid in landed_sids for ed in p["edits"] if ed["kind"] != "split"
               for ex in (ed["after"] if ed["field"] == "extents" and isinstance(ed["after"], list)
                          else []) if isinstance(ex, Mapping) for sid in ex.get("splits") or ())


def plan(props: list[dict], tried: set[str], landed_sids: set[str] = frozenset(),
         side_checked: Iterable[str] = ()) -> tuple[dict | None, list]:
    """The next proposal to land, and the ones that cannot now (id, why)."""
    by_id = {p["id"]: p for p in props}
    blocked = []
    for p in props:
        if p["id"] in tried or p["kind"] == "lake_split_later":
            continue
        if not p["applicable"]:
            blocked.append((p["id"], "not applicable: " + "; ".join(p["problems"] or ["no edit"])))
            continue
        waiting = [x for x in p["depends_on"] if x in by_id]
        if waiting:
            blocked.append((p["id"], "waits on new split(s) not applied: " + ", ".join(waiting)))
            continue
        if needs_side(p, props, landed_sids) and not (P.side_ready(p) or p["id"] in side_checked):
            blocked.append((p["id"], "a cut changes: make the side rebuild and read its binding "
                                     "diff first —\n      " + "\n      ".join(P.side_commands(p, props))))
            continue
        return p, blocked
    return None, blocked


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import CURATED
    from pipeline.regs.parsing.io import atomic_write
    from pipeline.regs.review import marks as M
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.apply_proposals",
                                 description="apply the owner's marks (T185 proposals), one commit "
                                             "each, after the binding check")
    ap.add_argument("--marks", type=Path, default=CURATED.regulations.review.marks)
    ap.add_argument("--build", type=Path, help="the atlas whose registry names new anchors")
    ap.add_argument("--only", action="append", default=[], metavar="ID",
                    help="land only these proposals (repeatable)")
    ap.add_argument("--side-checked", action="append", default=[], metavar="ID",
                    help="the side rebuild of this cut-changing proposal was made and its binding "
                         "diff read elsewhere (its side atlas is not at the default path)")
    ap.add_argument("--dry-run", action="store_true", help="print the diffs and checks, write nothing")
    a = ap.parse_args(argv)
    tried: set[str] = set()
    landed_sids: set[str] = set()
    landed, rc, blocked = [], 0, []
    while True:
        src = P.Sources.live(a.build)                      # the files as they are now
        props = P.build(M.load(a.marks), src)
        if a.only:
            props_pick = [p for p in props if p["id"] in a.only or any(
                p["id"] in q["depends_on"] for q in props if q["id"] in a.only)]
            tried |= {p["id"] for p in props if p not in props_pick}
        p, blocked = plan(props, tried, landed_sids, a.side_checked)
        if p is None:
            break
        tried.add(p["id"])
        p["_side"] = needs_side(p, props, landed_sids)
        if not p["_side"]:
            P.check([p], src)                              # THE BINDING CHECK, in memory
            c = p.get("check") or {}
            if not c.get("cheap"):
                print(f"!! {p['id']}: the binding check failed — {c.get('why')}; not applied")
                rc = 1
                continue
        texts = P.materialize([p], src)
        paths = sorted(texts)
        if a.dry_run:
            print(f"== would apply {p['id']}: {p['title']}")
            for ln in check_words(p.get("check")) if not p["_side"] else [
                    f"side rebuild: {P.side_dir(p)}"]:
                print(f"   {ln}")
            for path in paths:
                sys.stdout.write(P.unified(P._rel(path), src.text(path), texts[path]))
            # a dry run lands nothing, so a proposal that waits on a new split stays waiting
            continue
        dirty = _dirty(paths)
        if dirty:
            print(f"!! {p['id']}: uncommitted changes in {', '.join(dirty)} — commit or stash "
                  "them first; not applied")
            rc = 1
            continue
        before = {path: src.text(path) for path in paths}
        for path in paths:
            atomic_write(path, texts[path])
        got = _git("commit", "-m", message(p), "--", *[str(x) for x in paths])
        if got.returncode != 0:
            for path in paths:
                atomic_write(path, before[path])
            print(f"!! {p['id']}: git commit failed, files put back:\n{got.stderr or got.stdout}")
            rc = 1
            continue
        if p.get("split_id"):
            landed_sids.add(p["split_id"])
        sha = _git("rev-parse", "--short", "HEAD").stdout.strip()
        landed.append((p["id"], sha))
        print(f"applied {p['id']} -> {sha}")
    for pid, why in blocked:
        print(f"-- {pid}: {why}")
    if landed:
        print(f"{len(landed)} commit(s). Next: the rebuild the review app lists (GET /api/build); "
              "a new split needs the atlas.")
    elif not blocked and not a.dry_run:
        print("nothing to apply")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
