"""Stamp `Rule.exempts_from` — the machine-readable form of "Exempt from the spring closure".

A regional closure applies to every water in a region UNLESS that water is exempted, so the question
"is this river open on 15 May?" cannot be answered from an entry's own closure rules: the answer lives
in an exemption that was, until now, only a sentence in `details`. This reads those sentences once and
records what each one actually lifts.

Wordings that name ANOTHER WATER's closure ("Exempt from Columbia Lake's tributaries closure") are
left empty and reported — they are a cross-reference to a different entry, not one of the standing
regional restrictions, and guessing an id for them would be worse than leaving it explicit.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.backfill_exemptions --dry-run
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from pipeline.parsing import io

# normalized id -> what the synopsis writes. The regional closures are also written as their dates.
VOCAB: list[tuple[str, str]] = [
    ("spring_closure", r"spring\s+closure|apr(?:il)?\s*1\s*-\s*jun(?:e)?\s*14(?:\s+stream)?\s+closure"),
    ("summer_closure", r"summer\s+closure"),
    ("trout_char_release", r"trout\s*/\s*char\s+(?:catch[- ]and[- ])?release"),
    ("bull_trout_release", r"bull\s+trout\s+catch\s+and\s+release"),
    ("bait_ban", r"bait\s+ban"),
    ("single_barbless_hook", r"single\s+barbless\s+hooks?"),
    ("kokanee_stream_quota", r"kokanee[^.;]*quota"),
]


def classify(details: str) -> tuple[list[str], bool]:
    """(normalized ids, saw_exempt). Empty ids with saw_exempt=True -> a named-water cross-reference."""
    if not re.search(r"\bexempt", details or "", re.I):
        return [], False
    tail = re.split(r"\bexempt(?:ion)?\s+from\s+", details, flags=re.I)
    text = tail[-1] if len(tail) > 1 else details
    return [vid for vid, pat in VOCAB if re.search(pat, text, re.I)], True


def run(entries_dir: Path, dry_run: bool = False) -> dict:
    rep: dict[str, list] = {"stamped": [], "unmatched": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        changed = False
        for eid, e in by_id.items():
            for r in e.get("rules") or []:
                ids, saw = classify(r.get("details", ""))
                if not saw:
                    continue
                if not ids:
                    rep["unmatched"].append((region, eid, r["rule_id"], r.get("details", "")[:72]))
                    continue
                if r.get("exempts_from") != ids:
                    r["exempts_from"] = ids
                    changed = True
                    rep["stamped"].append((region, eid, r["rule_id"], ids, r.get("details", "")[:56]))
        if changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())
    return rep


def main() -> None:
    ap = argparse.ArgumentParser(description="Stamp Rule.exempts_from from the exemption wording.")
    ap.add_argument("--entries-dir")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    rep = run(Path(a.entries_dir) if a.entries_dir else io.entries_dir(), dry_run=a.dry_run)
    from collections import Counter
    print(f"{'[dry-run] would stamp' if a.dry_run else 'stamped'} {len(rep['stamped'])} rule(s)")
    print("  by target:", dict(Counter(i for r in rep["stamped"] for i in r[3])))
    if rep["unmatched"]:
        print(f"  left empty — exempts from a NAMED water's closure ({len(rep['unmatched'])}):")
        for region, eid, rid, d in rep["unmatched"]:
            print(f"    [{region}] {eid} {rid}: {d!r}")


if __name__ == "__main__":
    main()
