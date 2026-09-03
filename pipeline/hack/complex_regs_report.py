"""TEMPORARY report — list regulations that are "complex" (need curation / splits / trib logic).

Scans the curated overrides (named) and the parsed synopsis, and emits a Markdown report:
  - NAME/LOCATION complexity: names or rules with section language (upstream/downstream/
    between/confluence/side channel/...) -> candidates for curated splits.
  - TRIBUTARY scope: tributary_only / includes_tributaries.
  - MULTI-RULE / EXCEPTION: >1 rule, or a rule with an exception.

Run: .venv/bin/python -m pipeline.complex_regs_report
Writes output/v2/complex_regulations.md
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from pipeline.curated import CURATED, SOURCE

_OVERRIDES = str(CURATED.regulations.overrides)
_PARSED = "output/pipeline/parsing/synopsis_parsed.json"
_OUT = "output/v2/complex_regulations.md"

# Section/location language that implies a stream needs splitting or careful matching.
# Strong terms only (avoids false positives on names that merely contain "bridge"/"reach").
_LOC = re.compile(
    r"\b(upstream|downstream|u/s|d/s|above|below|between|confluence|portion|"
    r"side ?channel|slough|outlet|inlet|except|excluding)\b", re.I)


def _load(path):
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else None


def build_report() -> str:
    overrides = _load(_OVERRIDES) or []
    parsed = _load(_PARSED) or []
    out: list[str] = ["# Complex regulations report", ""]
    out.append(f"_Sources: `{_OVERRIDES}` ({len(overrides)} entries), "
               f"`{_PARSED}` ({len(parsed)} entries)._\n")

    # 1. Curated overrides whose NAME carries location/section language (named -> most useful).
    named_loc = []
    for e in overrides:
        if e.get("skip"):
            continue
        crit = e.get("criteria", {}) or {}
        name = crit.get("name_verbatim", "") or ""
        if _LOC.search(name):
            named_loc.append((name, crit.get("region", ""), ",".join(crit.get("mus", []) or [])))
    named_loc.sort()
    out.append(f"## 1. Curated overrides with section/location language in the NAME  ({len(named_loc)})")
    out.append("_These are named waterbodies that already encode a split ('upstream of', "
               "'between', etc.) — prime candidates for curated `splits.json` entries._\n")
    out.append("| name_verbatim | region | mus |")
    out.append("|---|---|---|")
    for name, region, mus in named_loc:
        out.append(f"| {name} | {region} | {mus} |")
    out.append("")

    # 2. Parsed synopsis complexity (no waterbody name in this file -> identify by location + rule).
    loc_entries, trib_entries, multi_entries = [], [], []
    for i, e in enumerate(parsed):
        rules = e.get("rules") or []
        entry_loc = e.get("entry_location_text", "") or ""
        rule_locs = [r.get("location_text", "") for r in rules if r.get("location_text")]
        loc = entry_loc or (rule_locs[0] if rule_locs else "")
        snippet = (e.get("regs_verbatim", "") or "").replace("\n", " ")[:70]
        if loc or _LOC.search(e.get("regs_verbatim", "") or ""):
            loc_entries.append((i, loc, snippet))
        if e.get("tributary_only") or e.get("includes_tributaries"):
            kind = "tributary_only" if e.get("tributary_only") else "incl. tribs"
            trib_entries.append((i, kind, snippet))
        has_exc = any(r.get("exception") for r in rules)
        if len(rules) > 1 or has_exc:
            multi_entries.append((i, len(rules), "exc" if has_exc else "", snippet))

    out.append(f"## 2. Parsed synopsis — LOCATION / section language  ({len(loc_entries)})")
    out.append("_Entries with `entry_location_text`, a rule `location_text`, or section words "
               "in the rules. (Parsed file has no waterbody name — identified by index + text.)_\n")
    out.append("| # | location_text | rules snippet |")
    out.append("|---|---|---|")
    for i, loc, snip in loc_entries:
        out.append(f"| {i} | {loc} | {snip} |")
    out.append("")

    out.append(f"## 3. Parsed synopsis — TRIBUTARY-scoped  ({len(trib_entries)})")
    out.append("| # | scope | rules snippet |")
    out.append("|---|---|---|")
    for i, kind, snip in trib_entries:
        out.append(f"| {i} | {kind} | {snip} |")
    out.append("")

    out.append(f"## 4. Parsed synopsis — MULTI-RULE / EXCEPTION  ({len(multi_entries)})")
    out.append("| # | #rules | exception? | rules snippet |")
    out.append("|---|---|---|---|")
    for i, n, exc, snip in multi_entries:
        out.append(f"| {i} | {n} | {exc} | {snip} |")
    out.append("")

    out.append("## Summary")
    out.append(f"- overrides with location-language names: **{len(named_loc)}**")
    out.append(f"- parsed with location/section language: **{len(loc_entries)}**")
    out.append(f"- parsed tributary-scoped: **{len(trib_entries)}**")
    out.append(f"- parsed multi-rule/exception: **{len(multi_entries)}**")
    return "\n".join(out)


def main() -> None:
    report = build_report()
    p = Path(_OUT)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(report)
    print(f"wrote {_OUT}")
    print("\n".join(l for l in report.splitlines() if l.startswith("- ") or l.startswith("## ")))


if __name__ == "__main__":
    main()
