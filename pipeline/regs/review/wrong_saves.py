"""T786: NO PLACE SHIPS ON A "WRONG" SAVE — a release gate (owner 2026-11-17 15:21: "The safety net should be a build
warning I think then we fix it and do not ship "wrong" places..").

    python -m pipeline.regs.review.wrong_saves --check     # exit 0: none listed; 1: listed; 2: could not run

THE LIST: every review target whose LATEST save in the marks store (`CURATED.review.marks`) has verdict "wrong" or
"unsure" and carries no extent fix. Live and dormant places alike: the store is read, not the places, so a dormant
place cannot drop out of it. "unsure" is listed with "wrong" because the marks audit treats them as one
(`audit.old_extent_in_force`, section B): either way the owner's last word does not stand behind what is drawn.

THE SCOPES (`GATE_SCOPES`): every scope that draws an extent — a DFO place (`dfo`), a synopsis entry (`entry`) and one
of its rules (`rule`), and a DFO row's closed area (`closed_area`). A cut / tidal / slough / pond / name mark is a call
on a cut or a candidate, not an extent: `apply --check` holds those. What a fix is (`has_fix`):

  dfo, entry, rule   the mark's `extent_fix` (the corrected extent it proposes; `apply --check` holds it to having landed)
  closed_area        none: a closed area lands only from a VERIFIED mark (`closed_areas.build`), so a wrong or unsure
                     one is fixed only by a re-save

A target whose last save is `cleared` has no mark in force and is not listed (`marks.latest`).

THE ONLY WAY PAST is a row on `WRONG_SAVE_OK`: mark key (`scope|target|rule_id`, as `apply.mark_key`) -> {date, line,
why}, added by hand, dated, with its reason, and naming the store LINE it excuses — a newer wrong save on the same
place is not covered by an old row. A row that excuses nothing listed is named so it is taken out. A row without a
date, a line or a reason is refused (exit 2), never read as an excuse.

`scripts/fast-fail.sh` and the release preflight (`pipeline/release.py` `check_wrong_saves`) run it; both refuse while
the list holds a place that is not allowed. READ-ONLY.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Mapping, Optional

#: the scopes that draw an extent (see the module doc)
GATE_SCOPES = ("dfo", "entry", "rule", "closed_area")
#: the verdicts the gate lists when no fix rides them (`audit.old_extent_in_force` reads the same two)
GATE_VERDICTS = ("wrong", "unsure")
#: the words every refusal ends with
FIX_WORDS = "fix and re-save it"

#: T786 — THE WRONG SAVES KNOWN TO SHIP, which the gate lets through: mark key -> {date, line, why}. Explicit and
#: dated, by hand, naming the store line it excuses. Empty: no place ships on a wrong save.
WRONG_SAVE_OK: dict[str, dict] = {
    # all four are DORMANT: DFO no longer lists the place, so nothing ships from them (they bind only if DFO lists them again)
    "dfo|3:thompson-river:from-kamloops-lake-outlet-downstre|": {
        "date": "2026-11-17", "line": 2521,
        "why": "dormant; owner 2026-11-16 (T806): answered by the three Thompson closed areas"},
    "dfo|6:morice-river:from-fishing-boundary-signs-locate|": {
        "date": "2026-11-17", "line": 2596,
        "why": "dormant; owner 2026-11-16 (T806): retired, fine by his own note"},
    "dfo|2:alouette-river-and-tributaries:downstream-of-a-line-between-two-f|": {
        "date": "2026-11-17", "line": 2846,
        "why": "dormant; owner 2026-11-17 17:01 OK'd its new drawing (T807, side streams in by DFO's heading); his P '?' of 11-09 kept as given"},
    "dfo|1:cowichan-river:from-cnr-trestle-at-mile-70-2-to-s|": {
        "date": "2026-11-17", "line": 2848,
        "why": "dormant; owner 2026-11-17 'dfo words' (T807, river only by DFO's words); his P '?' of 11-15 kept as given"},
}

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def mark_key(m: Mapping) -> str:
    """A mark's key as `apply.NOT_LANDED_OK` and `WRONG_SAVE_OK` spell it."""
    return f"{m.get('scope')}|{m.get('target')}|{m.get('rule_id') or ''}"


def has_fix(m: Mapping) -> bool:
    """Does mark `m` carry an extent fix (see the module doc: a closed area's never does)?"""
    if m.get("scope") == "closed_area":
        return False
    return bool(m.get("extent_fix") or [])


def in_force(lines: list[tuple[int, dict]]) -> dict[str, tuple[int, dict]]:
    """mark key -> (line, mark) of the LAST save per target (`marks.latest`): a target whose last line is `cleared` has
    none in force and is left out."""
    last: dict[str, tuple[int, dict]] = {}
    for n, m in lines:
        last[mark_key(m)] = (n, m)
    return {k: v for k, v in last.items() if v[1].get("verdict") != "cleared"}


def listed(lines: list[tuple[int, dict]]) -> list[dict]:
    """Every target in a gated scope whose save in force is wrong / unsure with no fix, in store order:
    {key, line, scope, target, rule_id, verdict, date, reviewer, note}. Pure."""
    out = []
    for k, (n, m) in in_force(lines).items():
        if m.get("scope") in GATE_SCOPES and m.get("verdict") in GATE_VERDICTS and not has_fix(m):
            out.append({"key": k, "line": n, "scope": m.get("scope"), "target": m.get("target"),
                        "rule_id": m.get("rule_id"), "verdict": m.get("verdict"), "date": m.get("date") or "",
                        "reviewer": m.get("reviewer") or "", "note": (m.get("note") or "").strip()})
    return sorted(out, key=lambda r: r["line"])


def _checked_allow(allow: Mapping[str, Mapping]) -> dict[str, dict]:
    """The allowance, each row whole — a row with no date / line / reason raises (it is never read as an excuse)."""
    for k, row in allow.items():
        if k.count("|") != 2:
            raise ValueError(f"WRONG_SAVE_OK key {k!r} is not scope|target|rule_id")
        if not _DATE.match(str(row.get("date") or "")):
            raise ValueError(f"WRONG_SAVE_OK[{k!r}] has no ISO date (YYYY-MM-DD)")
        if not isinstance(row.get("line"), int) or isinstance(row.get("line"), bool):
            raise ValueError(f"WRONG_SAVE_OK[{k!r}] names no store line (an int)")
        if not str(row.get("why") or "").strip():
            raise ValueError(f"WRONG_SAVE_OK[{k!r}] gives no reason")
    return {k: dict(v) for k, v in allow.items()}


def check(*, marks: Optional[Path] = None, allow: Optional[Mapping[str, Mapping]] = None) -> dict:
    """READ-ONLY: {ok, wrong: [row], allowed: [row], unused_allow: [key], allow, store}. `allow` defaults to
    `WRONG_SAVE_OK` for the curated store and to nothing for any other store. Raises when the store cannot be read
    (a missing file, a line that is not JSON) or an allowance row is incomplete: never a pass."""
    from pipeline.common.curated import CURATED
    from pipeline.regs.review.audit import read_marks
    store = Path(marks or CURATED.review.marks)
    if allow is None:
        allow = WRONG_SAVE_OK if store.resolve() == Path(CURATED.review.marks).resolve() else {}
    allow = _checked_allow(allow)
    rows = listed(read_marks(store))
    allowed = [r for r in rows if r["key"] in allow and allow[r["key"]]["line"] == r["line"]]
    bad = [r for r in rows if r not in allowed]
    used = {r["key"] for r in allowed}
    return {"ok": not bad, "wrong": bad, "allowed": allowed, "unused_allow": sorted(k for k in allow if k not in used),
            "allow": allow, "store": str(store)}


def line_of(r: Mapping) -> str:
    """One refusal: the place, its mark line, its verdict, and what to do."""
    who = r["target"] + (f" (rule {r['rule_id']})" if r.get("rule_id") else "")
    return (f"  {r['scope']} {who}: mark line {r['line']}, verdict {r['verdict']}, no extent fix "
            f"({r['date']}, {r['reviewer']}: {r['note'][:120]}) — {FIX_WORDS}")


def say(got: Mapping) -> tuple[int, str]:
    """(exit code, what to print): 0 when nothing is listed but what is allowed, else 1."""
    out = []
    for r in got["allowed"]:
        a = got["allow"][r["key"]]
        out.append(line_of(r) + f"\n      allowed ({a['date']}, line {a['line']}: {a['why']})")
    for k in got["unused_allow"]:
        out.append(f"  note: WRONG_SAVE_OK lists {k}, which excuses no wrong save in force any more — take the row out "
                   "(pipeline/regs/review/wrong_saves.py)")
    if got["ok"]:
        out.append(f"wrong saves: no place ships on a wrong or unsure save with no fix ({len(got['allowed'])} allowed)")
        return 0, "\n".join(out)
    out.append(f"wrong saves: {len(got['wrong'])} place(s) whose latest save is wrong or unsure with no extent fix "
               "(T786: no release ships while one is listed):")
    out += [line_of(r) for r in got["wrong"]]
    out.append(f"  {FIX_WORDS} in the review app; a save that must ship as it is goes on WRONG_SAVE_OK in "
               "pipeline/regs/review/wrong_saves.py, dated, with its line and reason")
    return 1, "\n".join(out)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.wrong_saves", description=__doc__.split("\n")[0])
    ap.add_argument("--check", action="store_true", required=True,
                    help="list every place whose latest save is wrong / unsure with no extent fix; exit 1 when one is "
                         "not on WRONG_SAVE_OK")
    ap.add_argument("--marks", type=Path, default=None, help="the marks store (default: CURATED's)")
    a = ap.parse_args(argv)
    try:
        got = check(marks=a.marks)
    except Exception as ex:  # noqa: BLE001 — said and exit 2: a gate that could not read is never a pass
        print(f"wrong saves: could not run: {type(ex).__name__}: {ex}", file=sys.stderr)
        return 2
    rc, said = say(got)
    print(said)
    return rc


if __name__ == "__main__":
    sys.exit(main())
