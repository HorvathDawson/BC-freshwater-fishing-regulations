"""ONE-OFF REPAIR: "on parts" is not an undrawn part (owner 2026-10-26).

Where the book says a rule holds "on parts" of a water and never says which, the rule holds on the
WHOLE water for now — the words are ambiguous, and a note nobody can act on is not a part. Hatzic
Lake's "speed restriction on parts (8 and 50 km/h)" is the example the owner gave.

So this drops, from every rule whose `undrawn_part` is ONLY such wording:
  - `undrawn_part` itself (the rule then colours what its extents draw — already `whole`);
  - the `review_reason` the undrawn-part normaliser wrote for it (`precheck.UNDRAWN_REASON` …);
  - an `unresolved_locators` entry that is the same words.
A rule that still needs a reason for something else keeps one (a speed restriction with no km/h).

ONLY THE PURE CASES. A part that names a place ("the area at the outlet of François Lake described
on map 1", "within 100 m of outlet") stays undrawn; so does wording that names a kind of place
("in swimming areas", "various locations (as buoyed and signed)") — those are listed, not applied.

NOT WIRED INTO run_parse.sh (memory: normalisation runs after ingest). Run by hand, dry-run first:

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.drop_on_parts --dry-run \\
        --report handoff/PLAN/curation-review/on-parts.md
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.drop_on_parts

A reach + deliver rebuild picks the change up.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

from pipeline.regs.parsing import io
from pipeline.regs.review.precheck import UNDRAWN_REASON

#: THE PURE "ON PARTS" WORDING — says a part, names none. Anything else is left alone.
ON_PARTS = re.compile(
    r"^\s*(?:on\s+)?parts?(?:\s+of\s+the\s+(?:lake|river|water|slough))?"
    r"(?:\s*\(not specified in the book\))?\s*$", re.I)

#: wording that names no single place but a KIND of place, or several signed ones — close, but
#: not "on parts": listed for the owner, never applied
NEAR = re.compile(r"various locations|swimming areas|\bin wetlands\b|small bays", re.I)


def is_on_parts(part: str) -> bool:
    return bool(ON_PARTS.match(part or ""))


def repair_rule(rule: dict) -> dict | None:
    """Drop the "on parts" undrawn part from one rule dict, in place. Returns {field: old value}
    of what changed, or None when the rule is not a pure "on parts" case."""
    part = rule.get("undrawn_part") or ""
    if not is_on_parts(part):
        return None
    was: dict = {"undrawn_part": rule.pop("undrawn_part")}
    reason = str(rule.get("review_reason") or "")
    if reason.startswith(UNDRAWN_REASON):
        was["review_reason"] = rule.pop("review_reason")
    locs = rule.get("unresolved_locators") or []
    keep = [x for x in locs if not is_on_parts(str(x))]
    if len(keep) != len(locs):
        was["unresolved_locators"] = locs
        if keep:
            rule["unresolved_locators"] = keep
        else:
            rule.pop("unresolved_locators")
    # A REASON THAT STILL HAS A JOB stays (the model requires it): a speed restriction the book
    # prints with no km/h, or a locator still unresolved.
    if not rule.get("review_reason"):
        if rule.get("type") == "vessel_rule" and rule.get("aspect") == "speed" \
                and rule.get("max_kmh") is None:
            rule["review_reason"] = "the book prints this speed restriction with no km/h"
        elif rule.get("unresolved_locators"):
            rule["review_reason"] = "a locator nothing draws: " + "; ".join(
                map(str, rule["unresolved_locators"]))
    return was


def run(entries_dir: Path, dry_run: bool) -> dict:
    report: dict[str, list] = {"changed": [], "near": [], "named": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        changed = False
        for eid, e in by_id.items():
            for r in e.get("rules") or []:
                if not isinstance(r, dict) or not r.get("undrawn_part"):
                    continue
                part = r["undrawn_part"]
                was = repair_rule(r)
                if was is not None:
                    changed = True
                    report["changed"].append((eid, r, was))
                elif NEAR.search(part):
                    report["near"].append((eid, r))
                else:
                    report["named"].append((eid, r))
        if changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())    # atomic; the file is validated
    return report


def _cell(s) -> str:
    return str(s).replace("|", "\\|").replace("\n", " ")


def markdown(report: dict, dry_run: bool) -> str:
    ch, near, named = report["changed"], report["near"], report["named"]
    out = [
        "# \"On parts\" rules — held on the whole water (owner 2026-10-26)",
        "",
        "Made by `python -m pipeline.regs.parsing.drop_on_parts` "
        + ("(dry run — nothing written)." if dry_run else "(applied)."),
        "Where the book says a rule holds \"on parts\" of a water and never says which, the rule "
        "holds on the WHOLE water for now. The tool drops `undrawn_part`, the undrawn-part "
        "`review_reason` and a matching `unresolved_locators` entry. Needs a reach + deliver "
        "rebuild.",
        "",
        f"## Applied: {len(ch)} rules",
        "",
        "| entry | rule | verbatim | undrawn part (dropped) | reason left |",
        "|---|---|---|---|---|",
    ]
    for eid, r, was in ch:
        out.append(f"| `{eid}` | `{r.get('rule_id')}` | {_cell(r.get('verbatim', ''))} | "
                   f"{_cell(was['undrawn_part'])} | {_cell(r.get('review_reason', '') or '—')} |")
    out += ["", "Several waters now carry two limits of one kind on the whole water (Prospect, "
            "Shawnigan, Hatzic: \"8 and 60/65/50 km/h\") — the book does not say where each holds.",
            "", f"## Kept undrawn, close wording (names a kind of place, not a part): {len(near)}",
            "", "Not applied — the owner's call.", "",
            "| entry | rule | undrawn part |", "|---|---|---|"]
    for eid, r in near:
        out.append(f"| `{eid}` | `{r.get('rule_id')}` | {_cell(r['undrawn_part'])} |")
    out += ["", f"## Kept undrawn, names a place: {len(named)}", "",
            "| entry | rule | undrawn part |", "|---|---|---|"]
    for eid, r in named:
        out.append(f"| `{eid}` | `{r.get('rule_id')}` | {_cell(r['undrawn_part'])} |")
    return "\n".join(out) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description="Drop 'on parts' undrawn parts (owner 2026-10-26).")
    ap.add_argument("--entries-dir", help="entries dir (default: the catalogue).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    ap.add_argument("--report", help="write the list as markdown here.")
    args = ap.parse_args()
    entries_dir = Path(args.entries_dir) if args.entries_dir else io.entries_dir()
    report = run(entries_dir, args.dry_run)
    verb = "[dry-run] would drop" if args.dry_run else "dropped"
    print(f"{verb} 'on parts' from {len(report['changed'])} rule(s); kept "
          f"{len(report['near'])} close-wording and {len(report['named'])} named parts")
    for eid, r, _ in report["changed"]:
        print(f"    {eid}  {r.get('rule_id')}")
    if args.report:
        Path(args.report).write_text(markdown(report, args.dry_run), encoding="utf-8")
        print(f"wrote {args.report}")


if __name__ == "__main__":
    main()
