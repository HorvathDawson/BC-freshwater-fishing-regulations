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
cut that changes waits for its side rebuild (the commands are printed). The file writers write their file and COMMIT
it the same way (T541: `git commit -- <file>`; refused when the file already has uncommitted changes, put back when
the commit fails; `--no-commit` leaves it to you). `--dry-run` prints what would change and writes nothing. A file write NAMES every row it drops, and is
refused when it would drop a row no mark in force explains (`write_files`, T545).

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
def _closed_areas(decided: E.Decided) -> dict[Path, str]:
    """T573: the closed areas the owner confirmed (`closed_areas.build`). The file is BORN with his first confirm:
    with nothing confirmed and no file there is nothing to write (an empty file would read as "marks not landed")."""
    from pipeline.regs.review import closed_areas as CA
    rows = CA.build(decided)
    if not rows and not CA.path().exists():
        return {}
    return {CA.path(): CA.document(rows)}


WRITERS: dict[str, Callable[[E.Decided], dict[Path, str]]] = {"sloughs": _sloughs, "names": _names, "tidal": _tidal,
                                                               "closed_areas": _closed_areas}


def _ever(decided: E.Decided, scope: str) -> set[str]:
    return {t for s, t in decided.ever if s == scope}


def _names_spoken(decided: E.Decided) -> set[str]:
    from pipeline.regs.review import names as N
    return _ever(decided, "name") | set(N.owner_calls())


#: T545 — domain -> THE ROWS THE OWNER HAS SPOKEN ABOUT (`row_keys` keys): the target of every mark in the store, a
#: cleared one too (`Decided.ever`), and, for names, the owner's single-row calls. A row that leaves its file is
#: explained only when it is one of these (an un-mark, a reject, a mark gone stale — each said on its item); any other
#: row that would vanish is a review input that moved (candidates regenerated, an auto-accept flag flipped), and the
#: write is refused.
SPOKEN: dict[str, Callable[[E.Decided], set[str]]] = {
    "sloughs": lambda d: _ever(d, "slough"),
    "closed_areas": lambda d: _ever(d, "closed_area"),
    "names": _names_spoken,
    "tidal": lambda d: {t.partition(":")[2] for t in _ever(d, "tidal")}
    | {str(n.blk) for m in d.scope("tidal") for n in m.new_splits if n.blk},
}


#: T545 item 6 — the domains whose file is FROZEN against rows nobody accepted: found_names.json takes most of its
#: rows from the generated candidates' auto-accept flag (no mark), so a re-mine could add names on the next save. A
#: row that is not in the file and that the owner never spoke about (`SPOKEN`) is refused; `--accept-new-names` is the
#: explicit act that takes a re-mine's new auto-accepted names in.
FROZEN = ("names",)


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
    """The rows of a file domain's document: `entries` (found names), `waters` (sloughs, tidal) or `pieces` (closed
    areas)."""
    import json
    doc = json.loads(text)
    return len(doc.get("entries") or doc.get("waters") or doc.get("pieces") or ())


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


def row_keys(text: str) -> dict[str, str]:
    """A file domain's rows, key -> how the row is NAMED: a found name by its candidate id, a slough by its wbk, a
    tidal limit by its blue line."""
    import json
    doc = json.loads(text)
    out: dict[str, str] = {}
    for r in doc.get("entries") or ():
        names = [n for n in r.get("names") or () if n.get("candidate")]
        for n in names:
            out[str(n["candidate"])] = f"{n.get('name')} ({n['candidate']})"
        if not names:
            k = json.dumps(r, sort_keys=True, ensure_ascii=False)
            out[k] = k[:120]
    for r in doc.get("waters") or ():
        k = str(r.get("wbk") or r.get("blk"))
        out[k] = f"{k} {r.get('name') or ''}".strip()
    for r in doc.get("pieces") or ():            # T573: a closed area, by its mark target (`<water_id>#<piece key>`)
        k = f"{r.get('water_id')}#{r.get('key')}"
        out[k] = f"{k} {r.get('name') or ''}".strip()
    return out


def dropped_rows(path: Path, text: str) -> dict[str, str]:
    """The rows `path` holds that `text` lacks (T545): key -> name."""
    if not path.exists():
        return {}
    new = row_keys(text)
    return {k: w for k, w in row_keys(path.read_text(encoding="utf-8")).items() if k not in new}


def _git(*args: str):
    import subprocess
    from pipeline.regs.review import proposals as P
    return subprocess.run(["git", *args], cwd=P.REPO, capture_output=True, text=True)


def not_committable(path: Path) -> Optional[str]:
    """Why a file domain's file cannot be committed (T541): it is outside the repository (a sandbox `CURATED_ROOT`, a
    test's temp file) or git ignores it; None for a file of the repository."""
    from pipeline.regs.review import proposals as P
    try:
        Path(path).resolve().relative_to(Path(P.REPO).resolve())
    except ValueError:
        return "outside the repository"
    return "ignored by git" if _git("check-ignore", "-q", "--", str(path)).returncode == 0 else None


def _commit(domain: str, path: Path, old: Optional[str], gone: list[str]) -> str:
    """Commit ONE written file by its path (`git commit -- <path>`, as `apply_proposals` does); its short sha. A commit
    that fails puts the file back and RAISES."""
    from pipeline.regs.parsing.io import atomic_write
    from pipeline.regs.review import proposals as P
    new_file = _git("ls-files", "--error-unmatch", "--", str(path)).returncode != 0
    name = Path(path).resolve().relative_to(Path(P.REPO).resolve()).as_posix()      # in the repository: `not_committable`
    msg = (f"T325 apply {domain}: {name} from the review marks\n\n"
           f"Written by the {domain} writer (`python -m pipeline.regs.review.apply {domain}`, the review app's "
           "write-through) from the marks in force.\n"
           + "".join(f"Dropped: {w}\n" for w in gone))
    got = _git("add", "--", str(path)) if new_file else None
    if got is None or got.returncode == 0:
        got = _git("commit", "-m", msg, "--", str(path))
    if got.returncode != 0:
        if old is None:
            _git("reset", "-q", "--", str(path))
            Path(path).unlink(missing_ok=True)
        else:
            atomic_write(path, old)
        raise ValueError(f"{_rel(path)}: git commit failed, the file is put back — "
                         f"{(got.stderr or got.stdout).strip()[:300]}")
    return _git("rev-parse", "--short", "HEAD").stdout.strip()


def write_files(domain: str, decided: E.Decided, *, dry_run: bool, accept_shrink: bool = False,
                report: Optional[dict] = None, commit: bool = False, accept_new: bool = False) -> list[Path]:
    """Run a file domain's writer; write (or, dry, print) what changed. Returns the files that differ. THE ONE WRITE
    PATH of the file domains (the review app's write-through, `review.apply`, and each tool's own command): atomic,
    utf-8, and guarded —

      * a write that drops more than `SHRINK_LIMIT` rows RAISES (T429);
      * a write that drops ANY row the owner has not spoken about (`SPOKEN`) RAISES, naming the rows (T545);
      * every row a write drops is NAMED (printed, and in `report["dropped"]`).

    `accept_shrink` lets both through (a drop the OWNER asked for).

      * a FROZEN domain's write (names) that ADDS a row the owner never spoke about RAISES, naming the rows (T545
        item 6: a re-mined candidates file must not add shipped names on the next save); `accept_new` lets it through.

    `commit` (T541, what `run` asks for): the written file is COMMITTED by its path, as a proposal is — refused
    before anything is written when the file already has uncommitted changes (the commit would sweep them in), put
    back when the commit fails. A file that is not the repository's (`not_committable`) is written and said to be
    uncommitted. `report["sha"]` is {file: sha}, `report["uncommitted"]` [(file, why)]."""
    changed = []
    for path, text in WRITERS[domain](decided).items():
        if path.exists() and path.read_text(encoding="utf-8") == text:
            print(f"{domain}: {_rel(path)} is current")
            continue
        why = None if accept_shrink else shrink_refusal(path, text)
        if why:
            raise ValueError(why)
        gone = dropped_rows(path, text)
        spoken = SPOKEN[domain](decided) if gone else set()
        loose = [w for k, w in gone.items() if k not in spoken]
        if loose and not accept_shrink:
            raise ValueError(
                f"{_rel(path)}: would drop {len(loose)} row(s) that no mark in force explains — "
                + "; ".join(loose[:10]) + (f"; … {len(loose) - 10} more" if len(loose) > 10 else "")
                + f" — refused; a review input regenerated or moved? If meant: python -m pipeline.regs.review.apply "
                  f"{domain} --accept-shrink")
        loose_file = not_committable(path) if commit and not dry_run else None
        if commit and not dry_run and loose_file is None:
            dirty = [ln for ln in _git("status", "--porcelain", "--", str(path)).stdout.splitlines() if ln.strip()]
            if dirty:
                raise ValueError(f"{_rel(path)} has uncommitted changes — commit or restore it first (the save's "
                                 "commit would sweep them in); nothing written")
        if domain in FROZEN and path.exists() and not accept_new:
            have = row_keys(path.read_text(encoding="utf-8"))
            said = SPOKEN[domain](decided)
            new = [w for k, w in row_keys(text).items() if k not in have and k not in said]
            if new:
                raise ValueError(
                    f"{_rel(path)}: would add {len(new)} row(s) nobody accepted (no mark, no owner call: the "
                    "candidates were regenerated?) — " + "; ".join(new[:10])
                    + (f"; … {len(new) - 10} more" if len(new) > 10 else "")
                    + f" — refused. If meant: python -m pipeline.regs.review.apply {domain} --accept-new-names")
        changed.append(path)
        report = {} if report is None else report
        report.setdefault("dropped", []).extend(gone.values())
        if dry_run:
            print(f"== {domain}: would write {_rel(path)}")
            sys.stdout.write(_diff(path, text)[:20000])
        else:
            from pipeline.regs.parsing.io import atomic_write
            old = path.read_text(encoding="utf-8") if path.exists() else None
            atomic_write(path, text)
            if not commit:
                print(f"{domain}: wrote {_rel(path)} — commit it: git commit -- {_rel(path)}")
            elif loose_file:
                report.setdefault("uncommitted", []).append((_rel(path), loose_file))
                print(f"{domain}: wrote {_rel(path)} — NOT committed ({loose_file})")
            else:
                sha = _commit(domain, path, old, list(gone.values()))
                report.setdefault("sha", {})[_rel(path)] = sha
                print(f"{domain}: wrote {_rel(path)} -> {sha}")
        for w in gone.values():
            print(f"{domain}: {'would drop' if dry_run else 'DROPPED'} {w}")
    return changed


def domain_of(scope: str) -> Optional[str]:
    """The review domain a mark scope belongs to (`export.DOMAINS`), or None."""
    return next((d for d, sc in E.DOMAINS.items() if scope in sc), None)


def run(domains: Optional[list[str]] = None, *, marks: Optional[Path] = None,
        build: Optional[Path] = None, side_checked: tuple = (), dry_run: bool = False,
        accept_shrink: bool = False, commit: bool = True, accept_new: bool = False) -> dict:
    """The marks' decisions (in memory), then each asked domain's writer (every open one when `domains` is empty). Returns
    {rc, domains, skipped: {domain: why}, files: [changed final files], events: [...]}, where an event is one
    outcome on the marks behind it — {domain, status: applied | written | refused | waiting | would, why,
    marks: [{scope, target, rule_id}]} — the review app's write-through shows each on its item.

    Conflicting decisions (`export.refusals`) write nothing and are one refused event.

    `commit` (T541): a file domain's written file is committed by its path, as each proposal is (`write_files`); its
    `written` event carries the `sha`."""
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
            try:
                said: list[dict] = []
                if d == "sloughs":
                    from pipeline.tools import slough_calls as S
                    for s in S.build(decided)[1]:
                        said.append({"domain": d, "status": "refused", "why": s["why"],
                                     "marks": [{"scope": "slough", "target": s["wbk"], "rule_id": None}]})
                if d == "names":              # T545: a name mark gone stale says so on its item (as a slough's does)
                    from pipeline.tools import mined_names as MN
                    for s in MN.stale(decided):
                        said.append({"domain": d, "status": "refused", "why": s["why"],
                                     "marks": [{"scope": "name", "target": s["target"], "rule_id": None}]})
                if d == "tidal":              # T326: a mark that sets no limit says why, on its item
                    from pipeline.tools import tidal_limits as TL
                    for s in TL.build(decided)[1]:
                        said.append({"domain": d, "status": "waiting", "why": s["why"],
                                     **({"followup": True} if s.get("followup") else {}),
                                     "marks": [{"scope": "tidal", "target": s["target"], "rule_id": None}]})
                report: dict = {}
                changed = write_files(d, decided, dry_run=dry_run, accept_shrink=accept_shrink, report=report,
                                      commit=commit, accept_new=accept_new)
            except ValueError as ex:                               # T429 / T545: a refusal, said on the domain
                print(str(ex))
                out["rc"] = 2
                out["events"].append({"domain": d, "status": "refused", "why": str(ex), "marks": []})
                continue
            out["events"] += said
            out["files"] += [str(p) for p in changed]
            gone = report.get("dropped") or []
            shas = sorted(set((report.get("sha") or {}).values()))
            out["events"].append({"domain": d, "status": "would" if dry_run else "written",
                                  "why": (", ".join(_rel(p) for p in changed) or "current")
                                  + (f"; dropped: {'; '.join(gone)}" if gone else "")
                                  + "".join(f"; {f} NOT committed ({why})" for f, why in report.get("uncommitted") or ()),
                                  "marks": [], "files": [_rel(p) for p in changed], "dropped": gone,
                                  **({"sha": ", ".join(shas)} if shas else {})})
    # the proposal writer, once, over every proposal domain asked for
    scopes = [s for d in want if d in PROPOSAL_DOMAINS for s in E.DOMAINS[d]]
    if scopes:
        args = ["--marks", str(marks)] if marks else []
        args += [x for s in scopes for x in ("--scope", s)]
        args += (["--build", str(build)] if build else []) + [x for i in side_checked for x in ("--side-checked", i)]
        args += ["--dry-run"] if dry_run else []
        print(f"proposals ({', '.join(d for d in want if d in PROPOSAL_DOMAINS)}): apply_proposals {' '.join(args)}")
        rc, events = A.run(args)
        out["rc"] = max(out["rc"], rc)                     # a file domain's refusal (2) is not lost to a clean apply
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
    # owner 2026-11-11 (chat 11): build 27 ships without these nine; each waits on a new cut and the next atlas rebuild
    "dfo|2:harrison-river:from-the-highway-7-bridge-downstre|": {"date": "2026-11-11", "why": "needs a new cut: lands with the next atlas rebuild (owner 2026-11-11: release build 27 without these)"},
    "dfo|2:harrison-river:from-the-outlet-of-harrison-lake-d|": {"date": "2026-11-11", "why": "needs a new cut: lands with the next atlas rebuild (owner 2026-11-11: release build 27 without these)"},
    "dfo|2:nicomekl-river:within-23-m-downstream-of-the-floo|": {"date": "2026-11-11", "why": "needs a new cut: lands with the next atlas rebuild (owner 2026-11-11: release build 27 without these)"},
    "dfo|2:nicomen-including-dewdney-slough:from-the-confluence-of-siddle-bell|": {"date": "2026-11-11", "why": "needs a new cut: lands with the next atlas rebuild (owner 2026-11-11: release build 27 without these)"},
    "dfo|2:nicomen-including-dewdney-slough:from-the-highway-7-bridge-at-dewdn|": {"date": "2026-11-11", "why": "needs a new cut: lands with the next atlas rebuild (owner 2026-11-11: release build 27 without these)"},
    "dfo|2:serpentine-river:within-23-m-downstream-of-the-floo|": {"date": "2026-11-11", "why": "needs a new cut: lands with the next atlas rebuild (owner 2026-11-11: release build 27 without these)"},
    "dfo|3:thompson-river:from-kamloops-lake-outlet-downstre~2|": {"date": "2026-11-11", "why": "needs a new cut: lands with the next atlas rebuild (owner 2026-11-11: release build 27 without these)"},
    "dfo|3:thompson-river:from-the-white-triangle-fishing-bo|": {"date": "2026-11-11", "why": "needs a new cut: lands with the next atlas rebuild (owner 2026-11-11: release build 27 without these)"},
    "dfo|6:nakina-river:that-portion-of-the-nakina-river-f|": {"date": "2026-11-11", "why": "needs a new cut: lands with the next atlas rebuild (owner 2026-11-11: release build 27 without these)"},
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


def print_check(got: dict, *, waiting_ok: bool = False) -> int:
    """`check`, said: one line per mark that has not landed; 0 when every one has (or is allowed), else 1.
    `waiting_ok` (fast-fail): a mark that only WAITS (a side rebuild, an owner decision) is listed but does not
    fail, so the owner's labelling cannot turn every lane's fast-fail red; release preflight stays strict."""
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
    held = [r for r in got["not_landed"] if r["status"] == "waiting"] if waiting_ok else []
    if held:
        print(f"review marks: {len(held)} WAITING (listed, not failing here; a release stops on them):")
        for r in held:
            print(line(r))
        got = {**got, "not_landed": [r for r in got["not_landed"] if r not in held]}
        got["ok"] = not got["not_landed"]
        if got["ok"]:
            return 0
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
    ap.add_argument("--accept-new-names", action="store_true",
                    help="names: take in names the regenerated candidates auto-accept that nobody marked (T545)")
    ap.add_argument("--no-commit", action="store_true",
                    help="file domains (tidal / sloughs / names): write the file, do not commit it")
    ap.add_argument("--check", action="store_true",
                    help="T545, read-only: list every mark in force that has not landed; exit 1 when there is one "
                         "that is not on NOT_LANDED_OK")
    ap.add_argument("--waiting-ok", action="store_true",
                    help="with --check: list marks that only wait (a side rebuild, an owner decision) without "
                         "failing; refused, stale and would-land rows still fail")
    a = ap.parse_args(argv)
    bad = [d for d in a.domains if d not in E.DOMAINS]
    if bad:
        ap.error(f"unknown domain(s) {', '.join(bad)} — one of {', '.join(E.DOMAINS)}")
    if a.check:
        if a.domains or a.dry_run or a.accept_shrink or a.accept_new_names:
            ap.error("--check takes no domain and no other mode: it checks every open domain")
        return print_check(check(marks=a.marks, build=a.build), waiting_ok=a.waiting_ok)
    return run(a.domains, marks=a.marks, build=a.build, side_checked=tuple(a.side_checked),
               dry_run=a.dry_run, accept_shrink=a.accept_shrink, commit=not a.no_commit,
               accept_new=a.accept_new_names)["rc"]


if __name__ == "__main__":
    raise SystemExit(main())
