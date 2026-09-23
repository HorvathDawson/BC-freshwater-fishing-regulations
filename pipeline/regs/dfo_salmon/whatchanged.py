"""whatchanged — what a re-scrape did to the rules, read against the curated locators.

`entries reconcile` answers the locator question: did the geography move? This answers the other
half — **the rules turned over, so which ones, and on what water?** Together they are the review
step the scheduled run opens when a page changes.

Rules carry no identity (they are replaced wholesale every run), so they cannot be diffed by id.
They are compared on **structure, never on prose**: everything the catalogue types, minus
`rule_id`, minus `verbatim`, minus the fishery notice. That is deliberate and it is the whole
point of the catalogue — DFO rewrites the wording of rules that have not changed
("Sockeye, Pink & Chum" -> "Sockeye, pink and chum"), and a diff keyed on text reports those as
churn. A diff keyed on structure reports nothing, correctly.

The comparison runs against a real archived snapshot, so this is exercised on history rather than
on a synthetic edit:

    .venv/bin/python -m pipeline.regs.dfo_salmon.whatchanged --region 2 --since 2023
    .venv/bin/python -m pipeline.regs.dfo_salmon.whatchanged --region 6 --since 2024 --water Kispiox
    .venv/bin/python -m pipeline.regs.dfo_salmon.whatchanged --region 6 --list
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pipeline.regs.dfo_salmon import entries as E
from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, DEFAULT_CACHE, PAGES, normalize_slug
from pipeline.regs.dfo_salmon.locations import extract
from pipeline.regs.dfo_salmon.parse import parse_cached, parse_region
from pipeline.regs.dfo_salmon.typed import to_rules
from pipeline.regs.dfo_salmon.untangle import untangle
from pipeline.regs.parsing.catalogue import CatalogueRule, label

logger = logging.getLogger(__name__)

HISTORY = Path(DEFAULT_CACHE) / "history"

#: Fields that are NOT the rule. `rule_id` is positional, `verbatim` and `reason` are prose that
#: drifts without the regulation changing — 2017's "Sockeye, Pink & Chum" is 2026's
#: "Sockeye, pink and chum", and an in-season notice number changes every time one is issued.
_NOT_THE_RULE = ("rule_id", "verbatim", "reason")


def signature(rule: CatalogueRule) -> Tuple:
    """What makes two rules the same rule. Structure only."""
    d = rule.model_dump(mode="json", exclude_defaults=True, by_alias=True)
    for k in _NOT_THE_RULE:
        d.pop(k, None)
    return tuple(sorted((k, str(v)) for k, v in d.items()))


def snapshots(slug: str) -> List[Tuple[str, Path]]:
    """Archived versions for a region, oldest first."""
    out = []
    for p in sorted(HISTORY.glob(f"region{slug}_*.html")):
        m = re.match(rf"region{re.escape(slug)}_(\d{{14}})\.html$", p.name)
        if m:
            out.append((m.group(1), p))
    return out


def rules_by_fingerprint(slug: str, path: Optional[Path]) -> Dict[str, List[CatalogueRule]]:
    """Typed rules from one version, keyed by the locator fingerprint. `None` = the live cache."""
    if path is None:
        parsed = parse_cached(slug)
    else:
        parsed = parse_region(path.read_text(encoding="utf-8", errors="ignore"), slug)
    _locs, recs, _sig = extract(untangle(parsed))

    out: Dict[str, List[CatalogueRule]] = defaultdict(list)
    per_fp: Dict[str, int] = defaultdict(int)
    for rec in recs:
        fp = rec.fingerprint
        out[fp].extend(to_rules(rec.to_dict(), fp[:8], per_fp[fp]))
        per_fp[fp] += 1
    return dict(out)


def compare(slug: str, old: Optional[Path], new: Optional[Path] = None) -> dict:
    """Diff two versions of a region's rules, named through the curated locators."""
    ef = E.load(slug)
    idx = ef.by_fingerprint()
    before, after = rules_by_fingerprint(slug, old), rules_by_fingerprint(slug, new)

    rows = []
    for fp in sorted(set(before) | set(after)):
        b = {signature(r): r for r in before.get(fp, [])}
        a = {signature(r): r for r in after.get(fp, [])}
        added, removed = [a[k] for k in a if k not in b], [b[k] for k in b if k not in a]
        loc = idx.get(fp)
        if not (added or removed):
            continue
        rows.append({
            "fingerprint": fp,
            "location_id": loc.location_id if loc else None,
            "water": (loc.water if loc else "") or "(cascade default)",
            "section": loc.section if loc else None,
            "precedence": loc.precedence if loc else None,
            "scope": ((loc.source_text or {}).get("specific_area") if loc else "") or "",
            "bound": bool(loc and loc.binding.extents),
            "state": ("both" if fp in before and fp in after
                      else "appeared" if fp in after else "vanished"),
            "added": added,
            "removed": removed,
        })
    return {
        "region": slug,
        "locators_before": len(before), "locators_after": len(after),
        "rules_before": sum(len(v) for v in before.values()),
        "rules_after": sum(len(v) for v in after.values()),
        "unchanged_locators": len(set(before) & set(after)) - len(
            [r for r in rows if r["state"] == "both"]),
        "rows": rows,
    }


def render(rep: dict, water: Optional[str] = None) -> None:
    slug = rep["region"]
    print(f"region {slug} — {PAGES[slug].name}")
    print(f"  locators {rep['locators_before']} -> {rep['locators_after']}"
          f"   rules {rep['rules_before']} -> {rep['rules_after']}")
    print(f"  {rep['unchanged_locators']} locator(s) carry exactly the same rules as before")

    rows = rep["rows"]
    if water:
        rows = [r for r in rows if water.lower() in (r["water"] or "").lower()]
    if not rows:
        print("  nothing changed" if not water else f"  nothing changed on {water!r}")
        return

    print(f"  {len(rows)} locator(s) whose rules moved:\n")
    for r in rows:
        head = r["water"]
        if r["section"]:
            head += f"  [section {r['section']}, precedence {r['precedence']}]"
        flag = "" if r["bound"] else "   ** no extent **"
        print(f"  {head}{flag}")
        if r["scope"]:
            print(f"    scope: {r['scope'][:96]}")
        if r["state"] != "both":
            print(f"    locator {r['state'].upper()} between these versions")
        for rule in r["removed"]:
            print(f"      - {label(rule)}")
        for rule in r["added"]:
            print(f"      + {label(rule)}")
        print()


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--region", required=True, choices=ALL_SLUGS)
    ap.add_argument("--since", help="archived version to compare against (a YYYY or full stamp); "
                                    "default is the oldest one cached")
    ap.add_argument("--water", help="only show this water")
    ap.add_argument("--list", action="store_true", help="list the archived versions and stop")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    slug = normalize_slug(args.region)
    snaps = snapshots(slug)
    if args.list:
        print(f"region {slug}: {len(snaps)} archived version(s)")
        for ts, p in snaps:
            print(f"  {ts[:4]}-{ts[4:6]}-{ts[6:8]}   {p.name}")
        return 0
    if not snaps:
        print(f"region {slug}: no archived versions cached", file=sys.stderr)
        return 1

    chosen = snaps[0]
    if args.since:
        match = [s for s in snaps if s[0].startswith(args.since)]
        if not match:
            print(f"no archived version matching {args.since!r}; try --list", file=sys.stderr)
            return 1
        chosen = match[0]

    ts = chosen[0]
    print(f"comparing {ts[:4]}-{ts[4:6]}-{ts[6:8]} against the current scrape\n")
    render(compare(slug, chosen[1]), water=args.water)
    return 0


if __name__ == "__main__":
    sys.exit(main())
