"""THE GATE BEFORE THE FULL REVIEW (curation-review PLAN §4 phase 5, T136): the measurements a
person's weeks of review wait on.

    python -m pipeline.regs.review.parity …            AGENTS 16: the app binds every rule as the run does
    python -m pipeline.regs.review.gate stale …        THE FALSE-STALE RATE: which marks a change staled, and
                                                       whether an edit or only a rebuild moved them
    python -m pipeline.regs.review.gate pilot …        THE PILOT LIST: 30 targets across the tiers, from the queue
    python -m pipeline.regs.review.gate pilot-report … THE PILOT'S MEASURE: time per entry, defects, tool blockers

FALSE STALE. A mark goes stale when what it answered for moved (`marks.moved`). The parts a curated
file decides (text, meaning, binding) move only when someone edits; the parts a BUILD writes (the
bundle's `reach` and `answer`, `marks.BUILT_PARTS`) also move when nothing was edited but the atlas
was rebuilt — and the atlas is not deterministic. A mark stale with ONLY built parts moved, across a
rebuild whose inputs are the same, staled a reviewer's work for nothing: a false stale. The gate is
0 (`stale_report`'s `build_only`), measured across a real rebuild (the T136 end-to-end).

THE PILOT. A person other than the builder reviews 30 ENTRIES spread across the tiers (`pilot_pick`)
in the review screen — entries only (T184): a split point is checked by proxy on the map of each entry
whose rules name it, never as a target of its own; every mark records the screen's clock (`Mark.seconds`) and the verdict, so the
report is read off the marks store (`pilot_report`): time per entry by tier, the defects found (wrong /
unsure marks and their problem), and the tool blockers (a mark whose note starts "TOOL:", the
reviewer's word for "the tool stopped me" — the how-to says so). Accepted when no tool blocker.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence

#: How many pilot targets each tier gives (PLAN §4 phase 5: 30 across the tiers) — every one an
#: ENTRY (T184: the S0 split-point tier is gone; a zone entry, tier Z, is marked rule by rule).
PILOT_QUOTAS: dict[str, int] = {"P1": 7, "Z": 5, "P2": 4, "P3": 5, "P4": 7, "sweep": 2}
#: a mark whose note starts with this is the reviewer reporting that the TOOL blocked them
TOOL_BLOCKER = "TOOL:"


# --------------------------------------------------------------------------------------------
# False stale
# --------------------------------------------------------------------------------------------

def stale_report(marks: Iterable, *, entries: Mapping[str, Mapping], ctx) -> dict:
    """Every mark IN FORCE against its target NOW (`ctx`, a `fingerprint.Context` of the vintage on
    disk): how many hold, and every stale one with its reasons and the parts that moved
    (`marks.moved`) — split into `edited` (a curated part moved: an edit did it, the reviewer must
    look again) and `build_only` (only `marks.BUILT_PARTS` moved: the rebuild alone did it — across
    a rebuild of unchanged inputs, a FALSE STALE)."""
    from pipeline.regs.review.marks import (BUILT_PARTS, MOVED_REASONS, STALE_REASONS, latest, moved,
                                            target_prints)
    held, edited, build_only = 0, [], []
    for key, m in latest(marks).items():
        parts = moved(m, target_prints(m, entries, ctx))
        if not parts:
            held += 1
            continue
        got = {MOVED_REASONS[p] for p in parts}
        row = {"scope": m.scope, "target": m.target, "rule_id": m.rule_id, "verdict": m.verdict,
               "reasons": [r.value for r in STALE_REASONS if r in got], "moved": sorted(parts)}
        (build_only if parts <= BUILT_PARTS else edited).append(row)
    n = held + len(edited) + len(build_only)
    return {"marks": n, "held": held, "stale": len(edited) + len(build_only),
            "edited": edited, "build_only": build_only,
            "false_stale_rate": round(len(build_only) / n, 4) if n else None}


# --------------------------------------------------------------------------------------------
# The pilot
# --------------------------------------------------------------------------------------------

def _spread(items: Sequence, q: int) -> list:
    """`q` items spread evenly through `items` (its own order): the middle of each of q equal runs."""
    n = len(items)
    if n <= q:
        return list(items)
    return [items[int((k + 0.5) * n / q)] for k in range(q)]


def pilot_pick(rows: Sequence[Mapping], quotas: Mapping[str, int] = PILOT_QUOTAS) -> dict:
    """THE PILOT LIST: per tier, `quotas[tier]` ENTRIES spread evenly through the one queue's order
    (`rows`: the review queue, risk order) among those a reviewer can finish now — unmarked and
    BUILT (an entry awaiting its build can only be half answered). Returns {targets: [...], short:
    {tier: how many the tier could not give}}."""
    out, short = [], {}
    for tier, q in quotas.items():
        # a zone entry is marked rule by rule: one with no rules (licensing records only) can
        # never be finished, so it is no pilot target (T136 adversarial review)
        pool = [r for r in rows if r.get("tier") == tier and r.get("verify") == "unmarked"
                and r.get("vintage") == "current"
                and not (r.get("kind") == "zone" and not r.get("n_rules"))]
        got = [{"tier": tier, "kind": "entry", "id": r["entry_id"], "name": r.get("name"),
                "region": r.get("region"), "hash": r["entry_id"]} for r in _spread(pool, q)]
        out += got
        if len(got) < q:
            short[tier] = q - len(got)
    return {"targets": out, "short": short}


def _finished(ms: Sequence, rules: Optional[Sequence[str]]) -> bool:
    """Is a target DONE by its marks `ms`: a water entry, any mark; an entry marked rule by rule
    (no entry mark — a zone entry), a mark on EVERY one of its `rules` (a zone entry with one rule of
    ten marked is not done). `rules` None: not known, any mark counts. (A pilot list made before
    T184 may still name a split target: any mark counts for it.)"""
    if not ms:
        return False
    if rules is None or any(m.scope != "rule" for m in ms):
        return True
    return set(rules) <= {m.rule_id for m in ms}


def pilot_report(targets: Sequence[Mapping], marks: Iterable,
                 rules_of: Optional[Mapping[str, Sequence[str]]] = None) -> dict:
    """THE PILOT'S MEASURE, read off the marks store: per target, the reviewer's time (the sum of
    its marks' `seconds` — a zone entry is marked rule by rule), its verdicts and problems, and any
    tool blocker; per tier and overall, how many were marked (`_finished`: for an entry marked rule
    by rule, every rule of `rules_of[entry]`) and the median / mean time per target.
    A copy a migration carried (`carried_by`) is not the reviewer's work and is not counted."""
    mine: dict[str, list] = {}
    for m in marks:
        if m.carried_by is None:
            mine.setdefault(m.target, []).append(m)
    rows = []
    for t in targets:
        ms = mine.get(t["id"], [])
        secs = [m.seconds for m in ms if m.seconds is not None]
        rules = rules_of.get(t["id"]) if rules_of is not None and t["kind"] == "entry" else None
        rows.append({"tier": t["tier"], "kind": t["kind"], "id": t["id"], "marked": _finished(ms, rules),
                     "seconds": round(sum(secs), 1) if secs else None,
                     "verdicts": [m.verdict for m in ms],
                     "defects": [{"verdict": m.verdict, "problem": m.problem, "note": m.note}
                                 for m in ms if m.verdict in ("wrong", "unsure")],
                     "tool_blockers": [m.note for m in ms
                                       if m.note.strip().upper().startswith(TOOL_BLOCKER)],
                     "reviewers": sorted({m.reviewer for m in ms})})

    def stats(rs: list[dict]) -> dict:
        s = [r["seconds"] for r in rs if r["seconds"] is not None]
        return {"targets": len(rs), "marked": sum(r["marked"] for r in rs), "timed": len(s),
                "median_s": round(statistics.median(s), 1) if s else None,
                "mean_s": round(statistics.fmean(s), 1) if s else None,
                "defects": sum(len(r["defects"]) for r in rs),
                "tool_blockers": sum(len(r["tool_blockers"]) for r in rs)}
    tiers = {}
    for r in rows:
        tiers.setdefault(r["tier"], []).append(r)
    overall = stats(rows)
    return {"overall": overall, "tiers": {t: stats(rs) for t, rs in tiers.items()},
            "targets": rows, "passes": overall["marked"] == len(rows) and overall["tool_blockers"] == 0}


def pilot_markdown(picked: Mapping, *, url: str = "http://127.0.0.1:5173/") -> str:
    """The pilot list as the how-to's table: tier, what, and the link that opens it in the screen."""
    from urllib.parse import quote
    lines = ["| # | tier | target | open |", "|---|---|---|---|"]
    for i, t in enumerate(picked["targets"], 1):
        lines.append(f"| {i} | {t['tier']} | {t['name']} (`{t['id']}`) | {url}#{quote(t['hash'], safe='')} |")
    if picked.get("short"):
        lines.append("")
        lines.append("Short (the tier had too few unmarked, built targets): "
                     + ", ".join(f"{k} {v}" for k, v in picked["short"].items()))
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------------

def _app(a):
    """The review app's data layer over the paths given (as the parity gate loads it)."""
    import os
    for env, v in (("CURATION_BUILD", a.build), ("CURATION_ENTRIES_DIR", a.entries),
                   ("CURATION_SPLITS_JSON", a.splits), ("CURATION_MARKS", a.marks),
                   ("CURATION_REACH_DIR", a.reach), ("CURATION_BUNDLE", a.bundle),
                   ("CURATION_RELEASE_ROOT", a.release)):
        if v is not None:
            os.environ[env] = str(v)
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "curation-review" / "backend"))
    import reuse                                               # noqa: E402
    return reuse


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.gate",
                                 description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, what in (("stale", "every mark in force: held, stale by an edit, stale by a rebuild alone"),
                       ("pilot", "the 30-entry pilot list from the review queue"),
                       ("pilot-report", "the pilot's time per entry, defects and tool blockers")):
        p = sub.add_parser(name, help=what)
        p.add_argument("--marks", type=Path, default=None, help="default: the curated store")
        p.add_argument("--entries", type=Path, default=None)
        p.add_argument("--splits", type=Path, default=None)
        p.add_argument("--build", type=Path, default=None)
        p.add_argument("--reach", type=Path, default=None)
        p.add_argument("--bundle", type=Path, default=None)
        p.add_argument("--release", type=Path, default=None)
        p.add_argument("--json", type=Path, help="write the whole result here")
        if name == "pilot":
            p.add_argument("--md", type=Path, help="write the how-to's table here")
            p.add_argument("--url", default="http://127.0.0.1:5173/")
        if name == "pilot-report":
            p.add_argument("--pilot", type=Path, required=True, help="the list `gate pilot --json` wrote")
    a = ap.parse_args(argv)
    reuse = _app(a)
    from pipeline.regs.review import marks as M
    if a.cmd == "stale":
        entries = {e["entry_id"]: e for _, e in reuse._all_entries()}
        got = stale_report(M.load(reuse.MARKS_PATH), entries=entries, ctx=reuse._review_context())
        print(json.dumps({k: (v if k not in ("edited", "build_only") else len(v))
                          for k, v in got.items()}, indent=1))
        for r in got["build_only"][:40]:
            print(f"  BUILD-ONLY STALE {r['scope']} {r['target']}{'#' + r['rule_id'] if r['rule_id'] else ''}: "
                  f"{r['reasons']} moved {r['moved']}")
    elif a.cmd == "pilot":
        got = pilot_pick(reuse.queue(order="risk"))
        got["vintage"] = reuse.built().summary()
        print(pilot_markdown(got, url=a.url))
        if a.md:
            a.md.write_text(pilot_markdown(got, url=a.url), encoding="utf-8")
    else:
        targets = json.loads(a.pilot.read_text(encoding="utf-8"))["targets"]
        rules_of = {e["entry_id"]: [r["rule_id"] for r in e.get("rules") or []]
                    for _, e in reuse._all_entries()}
        got = pilot_report(targets, M.load(reuse.MARKS_PATH), rules_of)
        print(json.dumps({k: v for k, v in got.items() if k != "targets"}, indent=1))
    if a.json:
        a.json.write_text(json.dumps(got, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    return 0 if a.cmd != "stale" or not got["build_only"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
