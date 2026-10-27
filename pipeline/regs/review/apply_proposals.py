"""T185: APPLY THE ACCEPTED PROPOSALS — the owner runs this; one commit per proposal.

    python -m pipeline.regs.review.apply_proposals --dry-run     # what would land, as diffs
    python -m pipeline.regs.review.apply_proposals               # land each, `git commit -- <paths>`

The proposals are REBUILT from the marks and the curated files NOW (`proposals.build`), never read
from the written patches: a decision applies only while its digest is the proposal's now (the
edits, before and after, are what the owner accepted). New splits land first; a proposal naming a
new split lands only once that split is in splits.json (accepted and applied in this run, or
before). After each commit the proposals are rebuilt, so the next one is made on the files as they
now are.

Refused per proposal (and said why): a stale decision (the proposal changed since), a dependency
not applied, a file with uncommitted changes (a commit would sweep them in), a failed commit (the
files are put back). Run the normal rebuild afterwards (`GET /api/build` lists it); each changed
entry re-enters the review queue as a re-check (its fingerprint moved, T97).
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from pipeline.regs.review import proposals as P


def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=P.REPO, capture_output=True, text=True)


def _dirty(paths: list[Path]) -> list[str]:
    got = _git("status", "--porcelain", "--", *[str(p) for p in paths])
    return [ln for ln in got.stdout.splitlines() if ln.strip()]


def message(p: dict, d: dict) -> str:
    who = ", ".join(sorted({m["target"] for m in p["marks"]}))
    return (f"T185 apply {p['id']}: {p['title']}"[:200] + "\n\n"
            f"Accepted by {d['reviewer']} on {d['date']} (proposal digest {p['digest']}).\n"
            f"From the review marks on: {who}.\n"
            + (f"Note: {d['note']}\n" if d.get("note") else ""))


def plan(props: list[dict], decided: dict[str, dict], done: set[str]) -> tuple[dict | None, list]:
    """The next proposal to land, and the accepted ones that cannot (id, why)."""
    by_id = {p["id"]: p for p in props}
    blocked = []
    for p in props:
        d = decided.get(p["id"])
        if d is None or d["decision"] != "accept" or p["id"] in done:
            continue
        if not p["applicable"]:
            blocked.append((p["id"], "not applicable: " + "; ".join(p["problems"] or ["no edit"])))
            continue
        if d["digest"] != p["digest"]:
            blocked.append((p["id"], "STALE: the proposal changed since it was accepted — "
                                     "decide it again"))
            continue
        waiting = [x for x in p["depends_on"] if x in by_id]
        if waiting:
            blocked.append((p["id"], "waits on new split(s) not applied: " + ", ".join(waiting)))
            continue
        return p, blocked
    return None, blocked


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import CURATED
    from pipeline.regs.parsing.io import atomic_write
    from pipeline.regs.review import marks as M
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.apply_proposals",
                                 description="apply the ACCEPTED T185 proposals, one commit each")
    ap.add_argument("--marks", type=Path, default=CURATED.regulations.review.marks)
    ap.add_argument("--build", type=Path, help="the atlas whose registry names new anchors")
    ap.add_argument("--dry-run", action="store_true", help="print the diffs, write nothing")
    a = ap.parse_args(argv)
    decided = M.decisions(M.load(a.marks))
    if not any(d["decision"] == "accept" for d in decided.values()):
        print("no accepted proposal")
        return 0
    done: set[str] = set()
    landed, rc = [], 0
    while True:
        src = P.Sources.live(a.build)                      # the files as they are now
        props = P.build(M.load(a.marks), src)
        p, blocked = plan(props, decided, done)
        if p is None:
            break
        done.add(p["id"])
        texts = P.materialize([p], src)
        paths = sorted(texts)
        if a.dry_run:
            print(f"== would apply {p['id']}: {p['title']}")
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
        got = _git("commit", "-m", message(p, decided[p["id"]]), "--", *[str(x) for x in paths])
        if got.returncode != 0:
            for path in paths:
                atomic_write(path, before[path])
            print(f"!! {p['id']}: git commit failed, files put back:\n{got.stderr or got.stdout}")
            rc = 1
            continue
        sha = _git("rev-parse", "--short", "HEAD").stdout.strip()
        landed.append((p["id"], sha))
        print(f"applied {p['id']} -> {sha}")
    for pid, why in blocked:
        print(f"-- {pid}: {why}")
    for pid, d in decided.items():
        if d["decision"] == "accept" and pid not in done and pid not in {b for b, _ in blocked}:
            print(f"-- {pid}: accepted, but no such proposal now (applied before, or its mark "
                  "changed)")
    if landed:
        print(f"{len(landed)} commit(s). Next: the rebuild the review app lists (GET /api/build); "
              "a proposal with a new split needs the atlas.")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
