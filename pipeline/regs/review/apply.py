"""T325: THE ONE COMMAND that lands finished review marks in their final curated files — the same for every open
domain (owner 2026-10-30: domains still under review must keep working easily, DFO extents especially).

    python -m pipeline.regs.review.apply                 # every open domain
    python -m pipeline.regs.review.apply dfo             # one domain (or several: dfo tidal)
    python -m pipeline.regs.review.apply dfo --dry-run   # what would land; writes nothing

Marking is unchanged: one click in the review app appends a mark to marks.jsonl. This command then

  1. EXPORTS the marks (`review.export` -> data/curated/decided/; a closed domain is skipped), and
  2. runs the domain's WRITER, which puts what the marks decide into the domain's final curated file:

     domain    writer                                        final file
     tidal     apply_proposals --scope tidal                 waters/splits.json (tidal cuts; T326 adds waters/tidal.json)
     dfo       apply_proposals --scope dfo                   regulations/entries/dfo_salmon/region-*.json (+ splits.json)
     entries   apply_proposals --scope entry --scope rule    regulations/entries/catalogue/region-*.json (+ splits.json)
     splits    apply_proposals --scope split                 waters/splits.json
     sloughs   tools.slough_calls                            waters/slough_calls.json
     names     tools.mined_names                             waters/mined_names.json
     ponds     — (closed, T329: nothing reads a pond call)

`apply_proposals` keeps every guard it had: the binding check, one commit per proposal (`git commit -- <paths>`), a
cut that changes waits for its side rebuild (the commands are printed). The file writers write their file; commit
it yourself. `--dry-run` exports into a temp dir and prints what would change.

A CLOSED domain (`review.closed`, `review.lock`) is skipped with a line saying so: its marks no longer move anything.
"""
from __future__ import annotations

import argparse
import difflib
import sys
import tempfile
from pathlib import Path
from typing import Callable, Optional

from pipeline.regs.review import export as E

#: the domains whose writer is `apply_proposals` (one commit per proposal, binding check, side rebuilds)
PROPOSAL_DOMAINS = ("tidal", "dfo", "entries", "splits")


def _sloughs(decided: E.Decided) -> dict[Path, str]:
    from pipeline.common.curated import CURATED
    from pipeline.tools import slough_calls as S
    rows, skipped = S.build(decided)
    for s in skipped:
        print(f"  sloughs: skipped {s['wbk']}: {s['why']}")
    return {Path(CURATED.waters.slough_calls): S.document(rows)}


def _names(decided: E.Decided) -> dict[Path, str]:
    import json
    from pipeline.common.curated import CURATED
    from pipeline.tools import mined_names as N
    return {Path(CURATED.waters.mined_names): json.dumps(N.document(N.build(decided)), indent=1, ensure_ascii=False)
            + "\n"}


#: domain -> its FILE writer: the decided data -> {final curated file: its text}. Shared with `review.lock`.
WRITERS: dict[str, Callable[[E.Decided], dict[Path, str]]] = {"sloughs": _sloughs, "names": _names}


def _rel(p: Path) -> str:
    return E._rel(p)


def _diff(path: Path, new: str) -> str:
    old = path.read_text(encoding="utf-8") if path.exists() else ""
    return "".join(difflib.unified_diff(old.splitlines(True), new.splitlines(True),
                                        f"a/{_rel(path)}", f"b/{_rel(path)}", n=1))


def write_files(domain: str, decided: E.Decided, *, dry_run: bool) -> list[Path]:
    """Run a file domain's writer; write (or, dry, print) what changed. Returns the files that differ."""
    changed = []
    for path, text in WRITERS[domain](decided).items():
        if path.exists() and path.read_text(encoding="utf-8") == text:
            print(f"{domain}: {_rel(path)} is current")
            continue
        changed.append(path)
        if dry_run:
            print(f"== {domain}: would write {_rel(path)}")
            sys.stdout.write(_diff(path, text)[:20000])
        else:
            from pipeline.regs.parsing.io import atomic_write
            atomic_write(path, text)
            print(f"{domain}: wrote {_rel(path)} — commit it: git commit -- {_rel(path)}")
    return changed


def main(argv: Optional[list[str]] = None) -> int:
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import apply_proposals as A
    from pipeline.regs.review import closed as C
    from pipeline.regs.review import marks as M
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.apply", description=__doc__.split("\n")[0])
    ap.add_argument("domains", nargs="*", metavar="DOMAIN", help=f"one of {', '.join(E.DOMAINS)} (default: every open one)")
    ap.add_argument("--dry-run", action="store_true", help="export into a temp dir and print what would land; write nothing")
    ap.add_argument("--marks", type=Path, default=None, help="the marks store (default: CURATED's)")
    ap.add_argument("--decided", type=Path, default=None, help="the decided-data dir (default: CURATED.decided)")
    ap.add_argument("--build", type=Path, help="apply_proposals: the atlas whose registry names new anchors")
    ap.add_argument("--side-checked", action="append", default=[], metavar="ID",
                    help="apply_proposals: this cut-changing proposal's side rebuild was made and read elsewhere")
    a = ap.parse_args(argv)
    bad = [d for d in a.domains if d not in E.DOMAINS]
    if bad:
        ap.error(f"unknown domain(s) {', '.join(bad)} — one of {', '.join(E.DOMAINS)}")
    shut = C.entries(C.path(a.marks))
    want = list(dict.fromkeys(a.domains or E.DOMAINS))
    for d in [d for d in want if d in shut]:
        e = shut[d]
        print(f"{d}: CLOSED ({e.get('date')}, {e.get('why') or 'locked'}) — skipped; reopen: "
              f"python -m pipeline.regs.review.lock --reopen {d}")
    want = [d for d in want if d not in shut]
    if not want:
        return 0

    # 1. export (dry: into a temp dir, so nothing is written)
    tmp = tempfile.TemporaryDirectory(prefix="review-apply-") if a.dry_run else None
    out = Path(tmp.name) if tmp else Path(a.decided or E.default_dir())
    decided_mem = E.decide(M.load(a.marks or CURATED.regulations.review.marks))
    got = E.write(decided_mem, out, set(shut))
    if not a.dry_run:
        print(f"export: {len(got)} decided file(s) changed in {_rel(out)}"
              + (f" — commit them: git commit -- {_rel(out)}" if got else ""))
    decided = E.load(out, closed=set(shut))

    rc = 0
    # 2a. the file writers
    for d in want:
        if d in WRITERS:
            write_files(d, decided, dry_run=a.dry_run)
    # 2b. the proposal writer, once, over every proposal domain asked for
    scopes = [s for d in want if d in PROPOSAL_DOMAINS for s in E.DOMAINS[d]]
    if scopes:
        args = ["--decided", str(out)] + (["--marks", str(a.marks)] if a.marks else [])
        args += [x for s in scopes for x in ("--scope", s)]
        args += (["--build", str(a.build)] if a.build else []) + [x for i in a.side_checked for x in ("--side-checked", i)]
        args += ["--dry-run"] if a.dry_run else []
        print(f"proposals ({', '.join(d for d in want if d in PROPOSAL_DOMAINS)}): apply_proposals {' '.join(args)}")
        rc = A.main(args)
    if tmp:
        tmp.cleanup()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
