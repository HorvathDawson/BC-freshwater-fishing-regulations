"""ONE-OFF PASS (T209, owner 2026-10-28): store the number on "Electric motor only - max 7.5 kW".

The catalogue's 162 `vessel_rule` rules at `level: electric_only` print their cap in the words
("Electric motor only - max 7.5 kW") but carried it in no field, so the app could not read it.
This sets `max_power_kw` from the number the rule's OWN words print. Nothing else changes: the
verbatim is untouched and the export label already reads "Electric motor only (max 7.5 kW)" with
or without the field.

Deterministic and conservative:
  - only `vessel_rule` + `level: electric_only` + no `max_power_kw` yet;
  - the number comes from the words ("max 7.5 kW", "max. 7.5 kW", "(max 7.5 kW)"); a rule whose
    words print no kW figure, or two different ones, is listed and left alone — never defaulted.

NOT WIRED INTO run_parse.sh (memory: normalisation runs after ingest). Run by hand, dry-run first:

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.electric_kw --dry-run
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.electric_kw

A deliver rebuild picks the change up (no reach change: the field moves no extent).
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

from pipeline.regs.parsing import io

#: "max 7.5 kW", "max. 7.5 kW", "maximum 7.5 kW" — the cap the rule's own words print
KW = re.compile(r"\bmax(?:imum|\.)?\s*(\d+(?:\.\d+)?)\s*kW\b", re.I)


def kw_in_words(verbatim: str) -> float | None:
    """The one kW cap the words print, or None when they print none or disagree."""
    found = {float(m) for m in KW.findall(verbatim or "")}
    return found.pop() if len(found) == 1 else None


def fill_rule(rule: dict) -> float | None:
    """Set `max_power_kw` on one electric_only rule dict, in place. Returns the value set, or
    None when the rule is not a candidate or its words print no single cap."""
    if rule.get("type") != "vessel_rule" or rule.get("level") != "electric_only":
        return None
    if rule.get("max_power_kw") is not None:
        return None
    kw = kw_in_words(str(rule.get("verbatim") or ""))
    if kw is None:
        return None
    rule["max_power_kw"] = kw
    return kw


def run(entries_dir: Path, dry_run: bool) -> dict:
    report: dict[str, list] = {"filled": [], "skipped": [], "had": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        changed = False
        for eid, e in by_id.items():
            for r in e.get("rules") or []:
                if not isinstance(r, dict) or r.get("type") != "vessel_rule" \
                        or r.get("level") != "electric_only":
                    continue
                if r.get("max_power_kw") is not None:
                    report["had"].append((eid, r))
                    continue
                kw = fill_rule(r)
                if kw is None:
                    report["skipped"].append((eid, r))
                else:
                    changed = True
                    report["filled"].append((eid, r, kw))
        if changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())    # atomic; the file is validated
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Fill max_power_kw on electric_only rules (T209).")
    ap.add_argument("--entries-dir", help="entries dir (default: the catalogue).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    args = ap.parse_args()
    entries_dir = Path(args.entries_dir) if args.entries_dir else io.entries_dir()
    rep = run(entries_dir, args.dry_run)
    verb = "[dry-run] would set" if args.dry_run else "set"
    by_kw: dict[float, int] = {}
    for *_, kw in rep["filled"]:
        by_kw[kw] = by_kw.get(kw, 0) + 1
    print(f"{verb} max_power_kw on {len(rep['filled'])} electric_only rule(s) "
          f"{dict(sorted(by_kw.items()))}; already set {len(rep['had'])}; "
          f"skipped (no single kW in the words) {len(rep['skipped'])}")
    for eid, r in rep["skipped"]:
        print(f"    skipped {eid}  {r.get('rule_id')}  {r.get('verbatim')!r}")


if __name__ == "__main__":
    main()
