"""T325: lands review marks in their final curated files — the same for every open domain.

WRITE-THROUGH (owner 2026-10-30: "it should just work naturally from marks"): the review app runs `run()` for the
mark's domain right after each save (`curation-review/backend/write_through.py`), so the owner runs nothing and
locks nothing. This command is the same apply by hand — a catch-up after marks were made with the server off:

    python -m pipeline.regs.review.apply                 # every open domain
    python -m pipeline.regs.review.apply dfo             # one domain (or several: dfo tidal)
    python -m pipeline.regs.review.apply dfo --dry-run   # what would land; writes nothing
    python -m pipeline.regs.review.apply --check         # T545: every mark in force has landed? exit 1 + the list

Marking is unchanged: one click in the review app appends a mark to marks.jsonl. This command then reads what the
marks decide IN MEMORY (`review.export.load`: the marks in force, a closed domain's left out — no intermediate file,
T408) and runs each domain's WRITER, which puts it into the domain's final curated file:

     domain    writer                                        final file
     tidal     tools.tidal_limits                            waters/tidal_limits.json (T326: mouth | split | cut | whole)
     dfo       apply_proposals --scope dfo                   dfo_salmon/places/region-*.json (+ waters/cuts.json)
     entries   apply_proposals --scope entry --scope rule    regulations/synopsis/region-*.json (+ waters/cuts.json)
     splits    apply_proposals --scope split                 waters/cuts.json
     sloughs   tools.slough_calls                            waters/slough_calls.json
     names     tools.mined_names                             waters/found_names.json
     ponds     — (closed, T329: nothing reads a pond call)

`apply_proposals` keeps every guard it had: the binding check, one commit per proposal (`git commit -- <paths>`), a
cut that changes waits for its side rebuild (the commands are printed). The file writers write their file; commit
it yourself. `--dry-run` prints what would change and writes nothing.

A CLOSED domain (`review.closed`: one that feeds nothing — ponds, T329) is skipped with a line saying so.

THE GATE (T545): `--check` (`check`) is read-only — the dry run of every open domain, as a list of what the marks in
force decide that the curated files do not say: a file a writer would change, a proposal that would land, one that is
refused, one that waits. `scripts/fast-fail.sh` and `pipeline release` preflight run it, so no release is built from
files that lag the owner's marks unsaid. A mark that is KNOWN to wait (an owner decision, a side rebuild not made
yet) passes only while it is on `NOT_LANDED_OK`, by key, dated, with its reason.
"""
from __future__ import annotations

import argparse
import difflib
import sys
from pathlib import Path
from typing import Callable, Optional

from pipeline.regs.review import export as E

#: the domains whose writer is `apply_proposals` (one commit per proposal, binding check, side rebuilds)
PROPOSAL_DOMAINS = ("dfo", "entries")


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
    return {Path(CURATED.waters.found_names): json.dumps(N.document(N.build(decided)), indent=1, ensure_ascii=False)
            + "\n"}


def _tidal(decided: E.Decided) -> dict[Path, str]:
    from pipeline.common.curated import CURATED
    from pipeline.tools import tidal_limits as T
    rows, skipped = T.build(decided)
    for s in skipped:
        print(f"  tidal: not written {s['target']}: {s['why']}")
    return {Path(CURATED.waters.tidal): T.document(rows)}


#: domain -> its FILE writer: the decided data -> {final curated file: its text} (fast: the write-through runs these
#: in the request).
WRITERS: dict[str, Callable[[E.Decided], dict[Path, str]]] = {"sloughs": _sloughs, "names": _names, "tidal": _tidal}


def _rel(p: Path) -> str:
    return E._rel(p)


def _diff(path: Path, new: str) -> str:
    old = path.read_text(encoding="utf-8") if path.exists() else ""
    return "".join(difflib.unified_diff(old.splitlines(True), new.splitlines(True),
                                        f"a/{_rel(path)}", f"b/{_rel(path)}", n=1))


#: a write may drop this many rows at most without `--accept-shrink` (T429): an un-mark drops a few; a missing or
#: moved input drops them all
SHRINK_LIMIT = 10


def rows_in(text: str) -> int:
    """The rows of a file domain's document: `entries` (found names) or `waters` (sloughs, tidal)."""
    import json
    doc = json.loads(text)
    return len(doc.get("entries") or doc.get("waters") or ())


def shrink_refusal(path: Path, text: str) -> Optional[str]:
    """Why writing `text` over `path` is refused (T429): it drops more than `SHRINK_LIMIT` rows; None when it may
    land."""
    if not path.exists():
        return None
    old, new = rows_in(path.read_text(encoding="utf-8")), rows_in(text)
    if old - new > SHRINK_LIMIT:
        return (f"{_rel(path)}: would drop {old - new} of {old} rows (to {new}) — refused; a review input missing or "
                f"moved? If meant: python -m pipeline.regs.review.apply --accept-shrink")
    return None


def write_files(domain: str, decided: E.Decided, *, dry_run: bool, accept_shrink: bool = False) -> list[Path]:
    """Run a file domain's writer; write (or, dry, print) what changed. Returns the files that differ. A write that
    drops more than `SHRINK_LIMIT` rows RAISES unless `accept_shrink` (T429)."""
    changed = []
    for path, text in WRITERS[domain](decided).items():
        if path.exists() and path.read_text(encoding="utf-8") == text:
            print(f"{domain}: {_rel(path)} is current")
            continue
        why = None if accept_shrink else shrink_refusal(path, text)
        if why:
            raise ValueError(why)
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


def run(domains: Optional[list[str]] = None, *, marks: Optional[Path] = None,
        build: Optional[Path] = None, side_checked: tuple = (), dry_run: bool = False,
        accept_shrink: bool = False) -> dict:
    """The marks' decisions (in memory), then each asked domain's writer (every open one when `domains` is empty). Returns
    {rc, domains, skipped: {domain: why}, files: [changed final files], events: [...]}, where an event is one
    outcome on the marks behind it — {domain, status: applied | written | refused | waiting | would, why,
    marks: [{scope, target, rule_id}]} — the review app's write-through shows each on its item.

    Conflicting decisions (`export.refusals`) write nothing and are one refused event."""
    from pipeline.regs.review import apply_proposals as A
    from pipeline.regs.review import closed as C
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

    try:
        decided = E.load(marks, closed=set(shut))
    except ValueError as ex:                                       # conflicting decisions: nothing written
        print(str(ex))
        out["rc"] = 2
        out["events"].append({"domain": "export", "status": "refused", "why": str(ex), "marks": []})
        return out

    # the file writers
    for d in want:
        if d in WRITERS:
            if d == "sloughs":
                from pipeline.tools import slough_calls as S
                for s in S.build(decided)[1]:
                    out["events"].append({"domain": d, "status": "refused", "why": s["why"],
                                          "marks": [{"scope": "slough", "target": s["wbk"], "rule_id": None}]})
            if d == "tidal":                  # T326: a mark that sets no limit says why, on its item
                from pipeline.tools import tidal_limits as TL
                for s in TL.build(decided)[1]:
                    out["events"].append({"domain": d, "status": "waiting", "why": s["why"],
                                          **({"followup": True} if s.get("followup") else {}),
                                          "marks": [{"scope": "tidal", "target": s["target"], "rule_id": None}]})
            try:
                changed = write_files(d, decided, dry_run=dry_run, accept_shrink=accept_shrink)
            except ValueError as ex:                               # T429: a shrink refused, said on the domain
                print(str(ex))
                out["rc"] = 2
                out["events"].append({"domain": d, "status": "refused", "why": str(ex), "marks": []})
                continue
            out["files"] += [str(p) for p in changed]
            out["events"].append({"domain": d, "status": "would" if dry_run else "written",
                                  "why": ", ".join(_rel(p) for p in changed) or "current", "marks": [],
                                  "files": [_rel(p) for p in changed]})
    # the proposal writer, once, over every proposal domain asked for
    scopes = [s for d in want if d in PROPOSAL_DOMAINS for s in E.DOMAINS[d]]
    if scopes:
        args = ["--marks", str(marks)] if marks else []
        args += [x for s in scopes for x in ("--scope", s)]
        args += (["--build", str(build)] if build else []) + [x for i in side_checked for x in ("--side-checked", i)]
        args += ["--dry-run"] if dry_run else []
        print(f"proposals ({', '.join(d for d in want if d in PROPOSAL_DOMAINS)}): apply_proposals {' '.join(args)}")
        rc, events = A.run(args)
        out["rc"] = rc
        for e in events:
            sc = {m.get("scope") for m in e.get("marks") or ()}
            e["domain"] = next((domain_of(x) for x in sc if domain_of(x)), None)
        out["events"] += events
    return out


#: T545 — THE MARKS KNOWN NOT TO HAVE LANDED, which `check` lets through: mark key (`scope|target|rule_id`) ->
#: {date, why}. Explicit and dated: a row is added by hand with its reason (an owner decision that is open, a side
#: rebuild that is queued) and taken out when the mark lands (`check` names a row that is no longer needed).
NOT_LANDED_OK: dict[str, dict] = {
    "tidal|tidal:354151091|": {"date": "2026-11-09", "why": "T359: does a confirmed DEM point become a cut? (owner)"},
    "tidal|tidal:360848786|": {"date": "2026-11-09", "why": "T359: does a confirmed DEM point become a cut? (owner)"},
}


def mark_key(m: dict) -> str:
    """A mark's key as the write-through and `NOT_LANDED_OK` spell it."""
    return f"{m.get('scope')}|{m.get('target')}|{m.get('rule_id') or ''}"


def check(*, marks: Optional[Path] = None, build: Optional[Path] = None,
          allow: Optional[dict[str, dict]] = None) -> dict:
    """T545, READ-ONLY: what the marks in force decide that the curated files do not say NOW. Returns
    {ok, not_landed: [row], allowed: [row], followups: n, unused_allow: [key]}; a row is {domain, status, why,
    marks: [key], id?}:

      stale     a file domain's writer would change its file (tidal / sloughs / names)
      would     a proposal that an apply would land now
      refused   a proposal / a mark the apply refuses (not applicable, an id two proposals share, a stale slough
                mark, conflicting decisions, a shrink)
      waiting   a proposal / a tidal mark that waits (a side rebuild, an owner decision)

    `ok` is False when any row is not allowed. A row is allowed only when EVERY mark behind it is in `allow`
    (default `NOT_LANDED_OK` for the curated store, nothing for any other store); a row with no mark (a stale file,
    a whole-domain refusal) is never allowed. A tidal FOLLOW-UP (a mark that is not Place y: it decides nothing, so
    nothing of it can land) is counted, not listed."""
    import contextlib
    import io
    from pipeline.common.curated import CURATED
    if allow is None:
        own = marks is None or Path(marks).resolve() == Path(CURATED.review.marks).resolve()
        allow = NOT_LANDED_OK if own else {}
    said = io.StringIO()
    with contextlib.redirect_stdout(said):               # the dry run prints every diff; the gate prints its list
        out = run(None, marks=marks, build=build, dry_run=True)
    rows: list[dict] = []
    followups = 0
    for e in out["events"]:
        st = e.get("status")
        if e.get("files"):                               # a file writer's own event: it would change its file
            rows.append({"domain": e.get("domain"), "status": "stale", "marks": [],
                         "why": f"{', '.join(e['files'])} is not what the marks say — "
                                f"python -m pipeline.regs.review.apply {e.get('domain')}"})
        elif e.get("followup"):
            followups += 1
        elif st in ("refused", "waiting") or (st == "would" and e.get("id")):
            rows.append({"domain": e.get("domain"), "status": st, "why": e.get("why") or "",
                         "marks": [mark_key(m) for m in e.get("marks") or ()],
                         **({"id": e["id"]} if e.get("id") else {})})
    allowed = [r for r in rows if r["marks"] and all(k in allow for k in r["marks"])]
    bad = [r for r in rows if r not in allowed]
    used = {k for r in allowed for k in r["marks"]}
    return {"ok": not bad, "not_landed": bad, "allowed": allowed, "followups": followups,
            "skipped": out["skipped"], "unused_allow": sorted(k for k in allow if k not in used),
            "allow": dict(allow), "log": said.getvalue()}


def print_check(got: dict) -> int:
    """`check`, said: one line per mark that has not landed; 0 when every one has (or is allowed), else 1."""
    def line(r: dict) -> str:
        who = ", ".join(r["marks"]) or r.get("id") or r.get("domain") or "?"
        return f"  {r['status']:8} {r.get('domain') or '-':8} {who}: {str(r['why']).splitlines()[0][:300]}"
    if got["skipped"]:
        print(f"review marks: closed domain(s), not checked: {', '.join(got['skipped'])}")
    for r in got["allowed"]:
        k = got["allow"].get(r["marks"][0]) or {}
        print(line(r) + f"\n           allowed ({k.get('date')}: {k.get('why')})")
    for k in got["unused_allow"]:
        print(f"  note: NOT_LANDED_OK lists {k}, which is not waiting any more — take the row out "
              "(pipeline/regs/review/apply.py)")
    if got["ok"]:
        print(f"review marks: every mark in force has landed ({len(got['allowed'])} allowed to wait, "
              f"{got['followups']} tidal follow-up(s) decide nothing)")
        return 0
    print(f"review marks: {len(got['not_landed'])} NOT LANDED in the curated files:")
    for r in got["not_landed"]:
        print(line(r))
    print("  land them: python -m pipeline.regs.review.apply   (--dry-run shows the diffs); a mark that must wait "
          "goes on NOT_LANDED_OK in pipeline/regs/review/apply.py, dated, with its reason")
    return 1


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.apply", description=__doc__.split("\n")[0])
    ap.add_argument("domains", nargs="*", metavar="DOMAIN", help=f"one of {', '.join(E.DOMAINS)} (default: every open one)")
    ap.add_argument("--dry-run", action="store_true", help="print what would land; write nothing")
    ap.add_argument("--marks", type=Path, default=None, help="the marks store (default: CURATED's)")
    ap.add_argument("--build", type=Path, help="apply_proposals: the atlas whose registry names new anchors")
    ap.add_argument("--side-checked", action="append", default=[], metavar="ID",
                    help="apply_proposals: this cut-changing proposal's side rebuild was made and read elsewhere")
    ap.add_argument("--accept-shrink", action="store_true",
                    help=f"write a file even when it drops more than {SHRINK_LIMIT} rows (T429)")
    ap.add_argument("--check", action="store_true",
                    help="T545, read-only: list every mark in force that has not landed; exit 1 when there is one "
                         "that is not on NOT_LANDED_OK")
    a = ap.parse_args(argv)
    bad = [d for d in a.domains if d not in E.DOMAINS]
    if bad:
        ap.error(f"unknown domain(s) {', '.join(bad)} — one of {', '.join(E.DOMAINS)}")
    if a.check:
        if a.domains or a.dry_run or a.accept_shrink:
            ap.error("--check takes no domain and no other mode: it checks every open domain")
        return print_check(check(marks=a.marks, build=a.build))
    return run(a.domains, marks=a.marks, build=a.build, side_checked=tuple(a.side_checked),
               dry_run=a.dry_run, accept_shrink=a.accept_shrink)["rc"]


if __name__ == "__main__":
    raise SystemExit(main())
