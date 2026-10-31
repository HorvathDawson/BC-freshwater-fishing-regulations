"""T325: lands review marks in their final curated files — the same for every open domain.

WRITE-THROUGH (owner 2026-10-30: "it should just work naturally from marks"): the review app runs `run()` for the
mark's domain right after each save (`curation-review/backend/write_through.py`), so the owner runs nothing and
locks nothing. This command is the same apply by hand — a catch-up after marks were made with the server off:

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

A CLOSED domain (`review.closed`: one that feeds nothing — ponds, T329) is skipped with a line saying so.
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


#: domain -> its FILE writer: the decided data -> {final curated file: its text} (fast: the write-through runs these
#: in the request).
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


def domain_of(scope: str) -> Optional[str]:
    """The review domain a mark scope belongs to (`export.DOMAINS`), or None."""
    return next((d for d, sc in E.DOMAINS.items() if scope in sc), None)


def run(domains: Optional[list[str]] = None, *, marks: Optional[Path] = None, decided_dir: Optional[Path] = None,
        build: Optional[Path] = None, side_checked: tuple = (), dry_run: bool = False, fresh: bool = False) -> dict:
    """Export, then each asked domain's writer (every open one when `domains` is empty). Returns
    {rc, domains, skipped: {domain: why}, files: [changed final files], events: [...]}, where an event is one
    outcome on the marks behind it — {domain, status: applied | written | refused | waiting | would, why,
    marks: [{scope, target, rule_id}]} — the review app's write-through shows each on its item.

    `fresh` (the write-through): skip apply_proposals' "decided data behind the marks" check — this run has just
    exported them; a mark appended meanwhile triggers its own run. A failed export (conflicting decisions) writes
    nothing and is one refused event."""
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import apply_proposals as A
    from pipeline.regs.review import closed as C
    from pipeline.regs.review import marks as M
    shut = C.entries(C.path(marks))
    want = list(dict.fromkeys(domains or E.DOMAINS))
    out = {"rc": 0, "domains": [], "skipped": {}, "files": [], "events": []}
    for d in [d for d in want if d in shut]:
        e = shut[d]
        out["skipped"][d] = f"closed ({e.get('date')}, {e.get('why') or 'closed'})"
        print(f"{d}: CLOSED ({e.get('date')}, {e.get('why') or 'closed'}) — skipped")
    want = [d for d in want if d not in shut]
    out["domains"] = want
    if not want:
        return out

    # 1. export (dry: into a temp dir, so nothing is written)
    tmp = tempfile.TemporaryDirectory(prefix="review-apply-") if dry_run else None
    try:
        dest = Path(tmp.name) if tmp else Path(decided_dir or E.default_dir())
        try:
            decided_mem = E.decide(M.load(marks or CURATED.regulations.review.marks))
        except ValueError as ex:                                   # conflicting decisions: nothing exported
            print(str(ex))
            out["rc"] = 2
            out["events"].append({"domain": "export", "status": "refused", "why": str(ex), "marks": []})
            return out
        got = E.write(decided_mem, dest, set(shut))
        if not dry_run:
            print(f"export: {len(got)} decided file(s) changed in {_rel(dest)}"
                  + (f" — commit them: git commit -- {_rel(dest)}" if got else ""))
        decided = E.load(dest, closed=set(shut))

        # 2a. the file writers
        for d in want:
            if d in WRITERS:
                if d == "sloughs":
                    from pipeline.tools import slough_calls as S
                    for s in S.build(decided)[1]:
                        out["events"].append({"domain": d, "status": "refused", "why": s["why"],
                                              "marks": [{"scope": "slough", "target": s["wbk"], "rule_id": None}]})
                changed = write_files(d, decided, dry_run=dry_run)
                out["files"] += [str(p) for p in changed]
                out["events"].append({"domain": d, "status": "would" if dry_run else "written",
                                      "why": ", ".join(_rel(p) for p in changed) or "current", "marks": []})
        # 2b. the proposal writer, once, over every proposal domain asked for
        scopes = [s for d in want if d in PROPOSAL_DOMAINS for s in E.DOMAINS[d]]
        if scopes:
            args = ["--decided", str(dest)] + (["--marks", str(marks)] if marks else [])
            args += [x for s in scopes for x in ("--scope", s)]
            args += (["--build", str(build)] if build else []) + [x for i in side_checked for x in ("--side-checked", i)]
            args += ["--dry-run"] if dry_run else []
            print(f"proposals ({', '.join(d for d in want if d in PROPOSAL_DOMAINS)}): apply_proposals {' '.join(args)}")
            rc, events = A.run(args, fresh=fresh)
            out["rc"] = rc
            for e in events:
                sc = {m.get("scope") for m in e.get("marks") or ()}
                e["domain"] = next((domain_of(x) for x in sc if domain_of(x)), None)
            out["events"] += events
    finally:
        if tmp:
            tmp.cleanup()
    return out


def main(argv: Optional[list[str]] = None) -> int:
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
    return run(a.domains, marks=a.marks, decided_dir=a.decided, build=a.build, side_checked=tuple(a.side_checked),
               dry_run=a.dry_run)["rc"]


if __name__ == "__main__":
    raise SystemExit(main())
