"""T325: LOCK A FINISHED REVIEW DOMAIN — its marks -> its final curated file, then the domain is CLOSED.

    python -m pipeline.regs.review.lock <domain>            # refuse while open; else write, print the proof, close
    python -m pipeline.regs.review.lock <domain> --dry-run  # what is open / what would be written; changes nothing
    python -m pipeline.regs.review.lock --reopen <domain>   # remove it from the closed list (the marks were never touched)
    python -m pipeline.regs.review.lock --status            # the closed list

Locking only closes a FINISHED domain; a domain still under review keeps the one everyday path
(`python -m pipeline.regs.review.apply <domain>`), which this tool never changes or slows.

In order (data-flow.md §4, owner option (a) 2026-10-30 — marks.jsonl is NEVER edited):

  1. REFUSE while the domain has open review items: a mark in force that is unsure / wrong; queue items not marked
     (the review app's own count for that tab); decided data behind the marks (`review.export --check`); a proposal
     of the domain's scopes still waiting in `apply_proposals`. Each is listed.
  2. WRITE the final file through the domain's writer (`review.apply.WRITERS`, the same one `apply` runs) — the shape
     the build reads, no review fields, at its `CURATED` path. Proposal domains (dfo / entries / splits) already
     live in their final files once nothing waits (1). Ponds have no final file (T329: nothing reads a pond call).
     Tidal has no writer until T326 (`waters/tidal.json`): refused.
  3. PRINT the side-build proof — a heavy job, never run here: `scripts/heavy.sh` + a sandboxed `pipeline build
     --side` with marks.jsonl and decided/ unreadable, whose release must be byte-identical to the live one.
  4. CLOSE: add the domain to the closed list (`review.closed`) and remove its `decided/<domain>.json` (the
     exporter no longer writes it). The review app shows the tab "locked"; the exporter and `apply` skip it.

Reopen = remove the entry (`--reopen`): the next export writes `decided/<domain>.json` again from the untouched marks.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import sys
from pathlib import Path
from typing import Callable, Optional

from pipeline.regs.review import closed as C
from pipeline.regs.review import export as E

#: domain -> its final curated file (what the build reads), or None: no file (owner ruling)
FINAL: dict[str, Optional[str]] = {
    "ponds": None,
    "sloughs": "waters/slough_calls.json",
    "names": "waters/mined_names.json",
    "dfo": "regulations/entries/dfo_salmon/region-*.json",
    "entries": "regulations/entries/catalogue/region-*.json",
    "splits": "waters/splits.json",
}
#: domains that cannot be locked yet, and why
NOT_YET = {"tidal": "no final-file writer yet — T326 makes waters/tidal.json (and folds owner-decisions.json)"}
OPEN_VERDICTS = ("unsure", "wrong")


# --------------------------------------------------------------------------- the review app's own queue counts

def _backend(marks: Optional[Path]):
    """The review app's backend modules, over `marks` (its queue counts are the truth of what is left to mark)."""
    if marks is not None:
        os.environ["CURATION_MARKS"] = str(marks)
    backend = str(Path(__file__).resolve().parents[3] / "curation-review" / "backend")
    if backend not in sys.path:
        sys.path.insert(0, backend)


def _progress_left(module: str, key: Optional[str] = None) -> Callable[[Optional[Path]], int]:
    def left(marks: Optional[Path]) -> int:
        _backend(marks)
        p = __import__(module).progress()
        p = p[key] if key else p
        return int(p["total"]) - int(p["done"])
    return left


def _ponds_left(marks: Optional[Path]) -> int:
    _backend(marks)
    import ponds
    return sum(1 for r in ponds.queue() if r["verify"] in ("unmarked", "stale"))


#: domain -> how many queue items are not yet marked (the review app's count). A domain with no entry has no queue
#: count here: the owner says the tab is finished (`--queue-checked`).
QUEUES: dict[str, Callable[[Optional[Path]], int]] = {
    "ponds": _ponds_left,
    "sloughs": _progress_left("sloughs"),
    "tidal": _progress_left("tidal"),
    "dfo": _progress_left("dfo", "all"),
}


# --------------------------------------------------------------------------- 1. what is still open

def open_items(domain: str, decided: E.Decided, *, decided_dir: Path, marks: Optional[Path],
               queue_left: Optional[int], waiting: list[str], queue_checked: bool = False) -> list[str]:
    """Why `domain` is not finished ([] = finished). `queue_left`: the app's unmarked count (None = no count);
    `waiting`: the domain's proposals `apply_proposals` would still land."""
    why = []
    held = [m for m in decided.scope(*E.DOMAINS[domain]) if m.verdict in OPEN_VERDICTS]
    if held:
        why.append(f"{len(held)} mark(s) in force are unsure / wrong: "
                   + ", ".join(f"{m.target}{'/' + m.rule_id if m.rule_id else ''} ({m.verdict})" for m in held[:12])
                   + (" …" if len(held) > 12 else ""))
    if queue_left:
        why.append(f"{queue_left} queue item(s) not marked yet (the review app's {domain} tab)")
    elif queue_left is None and not queue_checked:
        why.append(f"no queue count for {domain} here — check the review app's tab is finished, then pass "
                   "--queue-checked")
    behind = [b for b in E.stale(decided_dir, marks) if b == f"{domain}.json"]      # this domain's file only
    if behind:
        why.append(f"decided data is behind the marks ({', '.join(behind)}) — "
                   f"python -m pipeline.regs.review.apply {domain}")
    if waiting:
        why.append(f"{len(waiting)} proposal(s) still waiting to land: {', '.join(waiting[:8])}"
                   + (" …" if len(waiting) > 8 else "") + f" — python -m pipeline.regs.review.apply {domain}")
    return why


def waiting_proposals(domain: str, decided: E.Decided, build: Optional[Path] = None) -> list[str]:
    """The domain's proposals `apply_proposals` would still land (an edit not yet in the files)."""
    from pipeline.regs.review import apply as AP
    if domain not in AP.PROPOSAL_DOMAINS:
        return []
    from pipeline.regs.review import apply_proposals as A
    from pipeline.regs.review import proposals as P
    props = P.build(decided, P.Sources.live(build))
    return [p["id"] for p in props if A.of_scopes(p, E.DOMAINS[domain]) and p.get("edits")]


# --------------------------------------------------------------------------- 3. the proof (printed, never run)

def proof_command(domain: str, *, marks: Path, decided_dir: Path, date: str) -> str:
    side = f"data/generated/side/lock-{domain}-{date}"
    deny = (f'(deny file-read-data (literal "{Path(marks).resolve()}") (literal "{C.path(marks).resolve()}") '
            f'(subpath "{Path(decided_dir).resolve()}"))')
    return (f"scripts/heavy.sh lock-{domain} 70 -- sandbox-exec -p '(version 1)(allow default){deny}' \\\n"
            f"  .venv/bin/python -m pipeline build --side {side} --atlas --jobs 2\n"
            f"# then: every release file byte-identical to the live one (tile_version aside, T305):\n"
            f"diff -rq data/release/$(cat data/release/LATEST) {side}/deliver/release/$(ls {side}/deliver/release | "
            f"grep '^r-' | head -1)")


# --------------------------------------------------------------------------- main

def reopen(domain: str, closed_path: Path) -> int:
    got = C.entries(closed_path)
    if domain not in got:
        print(f"{domain} is not closed")
        return 1
    del got[domain]
    C.write(got, closed_path)
    print(f"{domain}: REOPENED (removed from {E._rel(closed_path)}). Next: python -m pipeline.regs.review.apply "
          f"{domain}  (re-exports decided/{domain}.json from the untouched marks)")
    return 0


def lock(domain: str, *, marks: Optional[Path] = None, decided_dir: Optional[Path] = None, build: Optional[Path] = None,
         why: str = "", by: str = "owner", dry_run: bool = False, queue_checked: bool = False,
         queue_left: Optional[Callable[[Optional[Path]], int]] = None,
         waiting: Optional[Callable[[str, E.Decided], list[str]]] = None, today: Optional[str] = None) -> int:
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import apply as AP
    from pipeline.regs.review import marks as M
    from pipeline.regs.parsing.io import atomic_write
    if domain not in E.DOMAINS:
        print(f"unknown domain {domain!r} — one of {', '.join(E.DOMAINS)}", file=sys.stderr)
        return 2
    closed_path = C.path(marks)
    shut = C.entries(closed_path)
    if domain in shut:
        print(f"{domain} is already closed ({shut[domain].get('date')}). Reopen: "
              f"python -m pipeline.regs.review.lock --reopen {domain}")
        return 1
    if domain in NOT_YET:
        print(f"REFUSED: {domain} cannot be locked yet — {NOT_YET[domain]}")
        return 1
    marks_path = Path(marks or CURATED.regulations.review.marks)
    decided_dir = Path(decided_dir or E.default_dir())
    decided = E.decide(M.load(marks_path))
    counter = queue_left if queue_left is not None else QUEUES.get(domain)
    left = counter(marks) if counter is not None else None
    wait = (waiting or (lambda d, dd: waiting_proposals(d, dd, build)))(domain, decided)
    why_open = open_items(domain, decided, decided_dir=decided_dir, marks=marks, queue_left=left, waiting=wait,
                          queue_checked=queue_checked)
    if why_open:
        print(f"REFUSED: the {domain} review is still open —")
        for w in why_open:
            print(f"  - {w}")
        return 1
    # 2. the final file
    files = AP.WRITERS[domain](decided) if domain in AP.WRITERS else {}
    for path, text in files.items():
        same = path.exists() and path.read_text(encoding="utf-8") == text
        print(f"final file {E._rel(path)}: {'current' if same else 'would write' if dry_run else 'written'}")
        if not same and not dry_run:
            atomic_write(path, text)
    if not files:
        print(f"final file: {FINAL.get(domain) or 'none (owner: nothing reads these calls)'}")
    date = today or _dt.date.today().isoformat()
    print("\nPROOF (a heavy job — run it yourself, one at a time):\n"
          + proof_command(domain, marks=marks_path, decided_dir=decided_dir, date=date) + "\n")
    if dry_run:
        print(f"dry run: {domain} would be CLOSED in {E._rel(closed_path)}")
        return 0
    # 4. close + re-export (removes decided/<domain>.json)
    shut[domain] = {"domain": domain, "scopes": list(E.DOMAINS[domain]), "date": date, "by": by,
                    "why": why or "review finished", "final": FINAL.get(domain)}
    C.write(shut, closed_path)
    left_over = decided_dir / f"{domain}.json"          # what the exporter would remove; no other file is touched
    if left_over.exists():
        left_over.unlink()
    paths = [E._rel(closed_path), E._rel(decided_dir)] + [E._rel(p) for p in files]
    print(f"{domain}: CLOSED. Commit: git commit -- {' '.join(paths)}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.lock", description=__doc__.split("\n")[0])
    ap.add_argument("domain", nargs="?", help=f"one of {', '.join(E.DOMAINS)}")
    ap.add_argument("--reopen", metavar="DOMAIN", help="remove DOMAIN from the closed list")
    ap.add_argument("--status", action="store_true", help="print the closed list")
    ap.add_argument("--dry-run", action="store_true", help="say what is open / would be written; change nothing")
    ap.add_argument("--why", default="", help="the reason recorded on the closed entry")
    ap.add_argument("--by", default="owner", help="who closed it (recorded)")
    ap.add_argument("--queue-checked", action="store_true",
                    help="the review app's tab is finished (for a domain with no queue count here)")
    ap.add_argument("--marks", type=Path, default=None, help="the marks store (default: CURATED's)")
    ap.add_argument("--decided", type=Path, default=None, help="the decided-data dir (default: CURATED.decided)")
    ap.add_argument("--build", type=Path, default=None, help="the atlas the waiting proposals are built against")
    a = ap.parse_args(argv)
    if a.status:
        got = C.entries(C.path(a.marks))
        for d, e in sorted(got.items()):
            print(f"{d}: closed {e.get('date')} by {e.get('by')} — {e.get('why')} (final: {e.get('final')})")
        print("" if got else "nothing closed")
        return 0
    if a.reopen:
        return reopen(a.reopen, C.path(a.marks))
    if not a.domain:
        ap.error("name a domain (or --reopen DOMAIN / --status)")
    return lock(a.domain, marks=a.marks, decided_dir=a.decided, build=a.build, why=a.why, by=a.by,
                dry_run=a.dry_run, queue_checked=a.queue_checked)


if __name__ == "__main__":
    raise SystemExit(main())
