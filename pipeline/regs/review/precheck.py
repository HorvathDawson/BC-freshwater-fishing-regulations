"""PRE-CHECKS (T135, curation-review PLAN §4 phase 4): what a machine can say about an entry BEFORE a
reviewer reads it, so the reviewer starts where the risk is and knows what to look at.

Each finding is `{code, severity, rule_id, what}`. Severity:

    A   a FACT read from the data — the reach run, the entry's own fields, splits.json, the save
        gate. It is true of the entry as served; the reviewer decides whether it is acceptable.
    B   a HEURISTIC — text matching (the coverage strip) or a decision already taken by hand. It
        may be nothing; it is shown, never counted toward the queue's order.

PLAN §4 named the audit's codes A1–A13 ("A6, A9 and A13 stay at severity B until measured"). That
audit list was never committed, so the codes here are named for what they check, and each is
measured over the corpus in `test_review_queue.py` (`test_the_prechecks_measured_over_the_corpus`).

`CHECKS` is the one table of codes; `prechecks` the one function that finds them. The review queue
orders by the count of severity-A findings (`tiers.queue_rank`); the review screen lists them.

CLI (reads the catalogue, splits.json, the marks store and the reach run; writes nothing):

    python -m pipeline.regs.review.precheck                       # counts per code
    python -m pipeline.regs.review.precheck --list [--severity A] [--code ambiguous_cut]
    python -m pipeline.regs.review.precheck --entry r1:ash_river@1-7
    python -m pipeline.regs.review.precheck --json > prechecks.json

The save gate (`gate`) is the review tool's own (`model_api.check`), run when an entry is opened;
the CLI does not run it (`validate_catalogue` is the gate at ingest).
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Iterable, Mapping, Sequence

#: code -> (severity, what it means). THE ONE TABLE.
CHECKS: dict[str, tuple[str, str]] = {
    "gate": ("A", "fails the save gate — it cannot be marked until it is fixed"),
    "binds_nothing": ("A", "the water row's `matched` names no item of this build — it binds nothing"),
    "unresolved": ("A", "the reach run could not bind this rule"),
    "unplaced_pieces": ("A", "the reach run left pieces of this rule's water unplaced (reported, "
                             "never guessed)"),
    "ambiguous_cut": ("A", "a cut matched several places on its line; the builder took one by "
                           "ruling E16 — check the pick"),
    "split_to_verify": ("A", "binds through a split point still to verify (tier S0)"),
    "parser_uncertain": ("A", "the parser flagged it (`review_reason`)"),
    "unresolved_locator": ("A", "names a place nothing draws (`unresolved_locators`)"),
    "undrawn_part": ("A", "applies to a part of its water nothing draws (`undrawn_part`)"),
    "cut_picked": ("B", "an ambiguous cut decided by hand (`cut_pick`) — the pick stands unless "
                        "it is wrong"),
    "quote_not_in_row": ("B", "quotes text the row does not contain (invented, or reworded)"),
    "unquoted_text": ("B", "more than 15 characters of the row no rule quotes"),
}
SEVERITIES = ("A", "B")
#: `unquoted_text`: how many characters of the row no rule quotes before it is a finding (PLAN §3)
UNQUOTED_OVER = 15


def finding(code: str, rule_id: str | None, detail: str = "") -> dict:
    sev, what = CHECKS[code]
    return {"code": code, "severity": sev, "rule_id": rule_id,
            "what": what + (f": {detail}" if detail else "")}


def _splits_of(rule: dict) -> list[str]:
    return [s for x in rule.get("extents") or [] if isinstance(x, dict)
            for s in x.get("splits") or []]


def _fmt_m(v) -> str:
    return f"{float(v):,.0f} m"


def prechecks(entry: dict, *, binds: bool, run=None, s0: Iterable[str] = (),
              gate_errors: Sequence | None = None) -> list[dict]:
    """Every finding on one entry, in `CHECKS` order, then rule order.

    `binds`: whether its `matched` names an item of the served build (zone entries name none and
    are never `binds_nothing`); `run`: the reach run's facts (`runfacts.RunFacts`; None: the
    run-based codes are not claimed); `s0`: the ids of the split points still to verify;
    `gate_errors`: the save gate's errors for the entry, when the caller ran it."""
    from pipeline.regs.review.coverage import entry_coverage
    eid = str(entry.get("entry_id", ""))
    zone = eid.startswith("z")
    rules = [r for r in entry.get("rules") or [] if isinstance(r, dict)]
    s0 = set(s0)
    got: dict[str, list[dict]] = {k: [] for k in CHECKS}
    if gate_errors:
        msgs = [e.get("msg", str(e)) if isinstance(e, Mapping) else str(e) for e in gate_errors]
        got["gate"].append(finding("gate", None, "; ".join(msgs[:3])
                                   + (f" (+{len(msgs) - 3} more)" if len(msgs) > 3 else "")))
    if not zone and rules and not binds:
        got["binds_nothing"].append(finding("binds_nothing", None,
                                            ", ".join(entry.get("matched") or []) or "matched is empty"))
    if run is not None:
        for rid, u in sorted(run.unresolved_of(eid).items()):
            got["unresolved"].append(finding("unresolved", rid,
                                             f"{u.get('reason')} ({u.get('detail')})"
                                             if u.get("detail") else str(u.get("reason"))))
        for rid, p in sorted(run.unclassified_of(eid).items()):
            why = sorted(set((p.get("reasons") or {}).values()))
            got["unplaced_pieces"].append(finding(
                "unplaced_pieces", rid, f"{len(p.get('pieces') or [])} piece(s)"
                + (f" — {', '.join(why)}" if why else "")))
        for rid, cuts in sorted(run.ambiguous_of(eid).items()):
            for p in cuts:
                code = "cut_picked" if p.get("toward") == "curated" else "ambiguous_cut"
                cands = ", ".join(_fmt_m(c) for c in p.get("candidates") or [])
                got[code].append(finding(code, rid, f"{p.get('split_id')} at {cands}; took "
                                                    f"{_fmt_m(p.get('used'))} (toward {p.get('toward')})"))
    for r in rules:
        rid = r.get("rule_id")
        held = sorted(set(_splits_of(r)) & s0)
        if held:
            got["split_to_verify"].append(finding("split_to_verify", rid, ", ".join(held)))
        if r.get("review_reason"):
            got["parser_uncertain"].append(finding("parser_uncertain", rid, str(r["review_reason"])))
        if r.get("unresolved_locators"):
            got["unresolved_locator"].append(finding(
                "unresolved_locator", rid, "; ".join(map(str, r["unresolved_locators"]))))
        if r.get("undrawn_part"):
            got["undrawn_part"].append(finding("undrawn_part", rid, str(r["undrawn_part"])))
    for x in entry.get("licensing") or []:
        if isinstance(x, dict) and x.get("review_reason"):
            got["parser_uncertain"].append(finding("parser_uncertain", x.get("id"),
                                                   str(x["review_reason"])))
    if entry.get("regs_verbatim"):
        cov = entry_coverage(entry)
        for rid in cov["missing"]:
            got["quote_not_in_row"].append(finding("quote_not_in_row", rid))
        if cov["unquoted_chars"] > UNQUOTED_OVER:
            got["unquoted_text"].append(finding("unquoted_text", None,
                                                f"{cov['unquoted_chars']} characters"))
    return [f for k in CHECKS for f in got[k]]


def severe(findings: Iterable[dict]) -> int:
    """How many severity-A findings — what the queue orders by (`tiers.queue_rank`)."""
    return sum(1 for f in findings if f["severity"] == "A")


# --------------------------------------------------------------------------------------- CLI

def _corpus(entries_dir: Path) -> list[dict]:
    out: list[dict] = []
    for p in sorted(Path(entries_dir).glob("region-*.json")):
        out += json.loads(p.read_text(encoding="utf-8"))["entries"]
    return out


def corpus_findings(entries: Sequence[dict], *, registry_ids, run, splits: dict,
                    split_status: Mapping[str, str] | None = None) -> dict[str, list[dict]]:
    """entry_id -> its findings, over a corpus (the CLI and the tests)."""
    from pipeline.atlas.reach.covered import covered_ids
    from pipeline.regs.review.tiers import split_needs_verify
    split_status = split_status or {}
    s0 = {sp["id"] for wb in splits.get("waterbodies", []) for sp in wb.get("splits", [])
          if sp.get("id") and split_needs_verify(sp, split_status.get(sp["id"]))}
    return {e["entry_id"]: prechecks(e, binds=bool(covered_ids(e, registry_ids)), run=run, s0=s0)
            for e in entries}


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import CURATED, GENERATED
    from pipeline.regs.review.runfacts import RunFacts
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.precheck",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("--reach", type=Path, default=None,
                    help="the reach run (default: the served build's, data/generated/reaches/<build>)")
    ap.add_argument("--entries", type=Path, default=CURATED.regulations.entries.catalogue)
    ap.add_argument("--entry", action="append", default=[], help="only this entry (repeatable)")
    ap.add_argument("--code", action="append", default=[], help="only this code (repeatable)")
    ap.add_argument("--severity", choices=SEVERITIES)
    ap.add_argument("--list", action="store_true", help="every finding, one per line")
    ap.add_argument("--json", action="store_true", help="entry_id -> findings, as JSON")
    a = ap.parse_args(argv)
    for c in a.code:
        if c not in CHECKS:
            ap.error(f"--code {c}: one of {', '.join(CHECKS)}")
    build = GENERATED.require_build()
    run = RunFacts.read(a.reach or GENERATED.reaches / build.name)
    from pipeline.atlas.registry import load_registry
    registry = load_registry(build / "registry.json")
    splits = json.loads(CURATED.waters.splits.read_text(encoding="utf-8"))
    entries = [e for e in _corpus(a.entries) if not a.entry or e["entry_id"] in a.entry]
    got = corpus_findings(entries, registry_ids=registry, run=run, splits=splits)
    got = {eid: [f for f in fs if (not a.code or f["code"] in a.code)
                 and (not a.severity or f["severity"] == a.severity)]
           for eid, fs in got.items()}
    if a.json:
        json.dump({k: v for k, v in got.items() if v}, sys.stdout, indent=1, ensure_ascii=False)
        print()
        return 0
    if a.list or a.entry:
        for eid, fs in got.items():
            for f in fs:
                print(f"{f['severity']} {f['code']:<20} {eid}{' ' + f['rule_id'] if f['rule_id'] else ''}"
                      f" — {f['what']}")
        return 0
    n = Counter(f["code"] for fs in got.values() for f in fs)
    ents = Counter(f["code"] for fs in got.values() for f in {x["code"]: x for x in fs}.values())
    print(f"reach run {run.digest}  ·  {len(entries)} entries")
    for code, (sev, what) in CHECKS.items():
        if a.code and code not in a.code or a.severity and sev != a.severity:
            continue
        if code == "gate":
            print(f"  {sev} {code:<20}   (not run here: the review tool's save gate runs on the open entry)")
            continue
        print(f"  {sev} {code:<20} {n[code]:>5} finding(s) on {ents[code]:>4} entries — {what}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
