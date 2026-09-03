"""validate — assert the parse is sound, on every page and every archived version.

Each check below exists because the corresponding defect actually shipped, was silent,
and was found by accident rather than by failing:

* a `<td colspan="4">` note read as a rule, whose species, dates AND limits were all
  one sentence of prose (Tlell River, live page);
* an unmarked continuation row filing "Chinook" as a **waterbody** (Region 5a, 2025-03);
* an unterminated `<!--` hiding a whole table (Regions 4, 7, 5a);
* 179 of 202 rows nested inside each other by an unclosed cell tag (Region 6, 2020).

Every one produced plausible-looking output. That is the point: the parser cannot tell
you it failed, so the output has to be checked against what it must always be true of.

Severity:
    ERROR  the output is wrong; do not publish
    WARN   suspicious, worth a look, not necessarily wrong

CLI
---
    .venv/bin/python -m pipeline.regs.dfo_salmon.validate            # live pages
    .venv/bin/python -m pipeline.regs.dfo_salmon.validate --history  # + every archived version
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES, load_cached, normalize_slug
from pipeline.regs.dfo_salmon.locations import extract
from pipeline.regs.dfo_salmon.parse import N_COLS, ParsedRegion, parse_region
from pipeline.regs.dfo_salmon.untangle import Untangled, untangle, verify

logger = logging.getLogger(__name__)

#: The species vocabulary across every live page and archived version. A new value is
#: not necessarily wrong, but it should be looked at before it ships.
KNOWN_SPECIES = {
    "all", "chinook", "coho", "sockeye", "pink", "chum",
    "sockeye, pink and chum", "sockeye, pink, chum", "sockeye, pink & chum",
    "chinook, sockeye, pink, chum",
}

#: Date strings that legitimately carry no calendar window at all.
NO_WINDOW_DATES = {"to be determined", ""}

#: A waters cell is a NAME. Anything much longer is prose that leaked into the column.
MAX_WATER_NAME = 120
#: A species cell holds a species, not a sentence.
MAX_SPECIES = 60


@dataclass
class Finding:
    severity: str      # ERROR | WARN
    check: str
    region: str
    detail: str

    def __str__(self) -> str:
        return f"{self.severity:<5} [{self.region}] {self.check}: {self.detail}"


def _species_looks_like_prose(text: str) -> bool:
    return len(text) > MAX_SPECIES or text.count(" ") > 6


def check_region(slug: str, parsed: ParsedRegion, u: Untangled,
                 label: Optional[str] = None) -> List[Finding]:
    """Every invariant the parse output must satisfy."""
    where = label or slug
    out: List[Finding] = []

    def err(check, detail):
        out.append(Finding("ERROR", check, where, detail))

    def warn(check, detail):
        out.append(Finding("WARN", check, where, detail))

    # --- the table exists at all ------------------------------------------
    if not parsed.table_found:
        if not PAGES.get(slug) or not PAGES[slug].is_stub:
            warn("no_table", "no regulation table on the page")
        return out
    if not parsed.rows:
        err("empty_table", "a table was found but produced zero rules")
        return out

    # --- nothing lost between parse and untangle --------------------------
    try:
        verify(parsed, u)
    except AssertionError as exc:
        err("rule_conservation", str(exc))

    # --- the phantom-rule shape -------------------------------------------
    for r in parsed.rows:
        if r.species and r.species == r.dates == r.limits_gear:
            err("note_read_as_rule",
                f"species == dates == limits: {r.species[:70]!r}")
        if r.species and _species_looks_like_prose(r.species):
            err("prose_in_species", f"{r.species[:70]!r}")

    # --- the waters column holds names, not species or prose --------------
    for w in u.waters:
        low = w.name.strip().lower()
        if low in KNOWN_SPECIES - {"all"}:
            err("species_as_waterbody",
                f"{w.name!r} is a species, not a waterbody — "
                "an unmarked continuation row was left-aligned")
        if len(w.name) > MAX_WATER_NAME:
            err("prose_in_waters", f"{w.name[:80]!r} ({len(w.name)} chars)")
        if not w.name.strip():
            err("empty_water_name", "a water location has no name")
        if not w.reaches:
            err("water_without_reaches", w.name)

    # --- species and dates -------------------------------------------------
    for r in parsed.rows:
        sp = (r.species or "").strip().lower()
        if sp and sp not in KNOWN_SPECIES:
            warn("unknown_species", f"{r.species!r}")

    from pipeline.regs.dfo_salmon.locations import interpret_dates

    for r in parsed.rows:
        d = (r.dates or "").strip()
        if not interpret_dates(d)["parsed"] and d.lower() not in NO_WINDOW_DATES:
            # Left as WARN, not ERROR: every one so far has been a source typo
            # ("Aprl 1 to Jun 15", "Nov 01- to Dec 31"), which a human must read.
            warn("unparsed_dates", f"{d[:60]!r}")

    # --- locations and rules line up --------------------------------------
    locs, rules, _sig = extract(u)
    known = {l.fingerprint for l in locs}
    orphans = [r for r in rules if r.fingerprint not in known]
    if orphans:
        err("orphan_rules", f"{len(orphans)} rules reference no location")
    seen = set()
    for l in locs:
        if l.fingerprint in seen:
            err("duplicate_fingerprint", f"{l.water!r} / {l.specific_area[:50]!r}")
        seen.add(l.fingerprint)

    # --- the cascade ------------------------------------------------------
    from pipeline.regs.dfo_salmon.cascade import build_scopes, resolution_chain

    scopes = build_scopes(u)
    ids = {s.scope_id for s in scopes}
    for s in scopes:
        if s.parent and s.parent not in ids:
            err("dangling_scope_parent", f"{s.scope_id} -> {s.parent}")
        chain = resolution_chain(scopes, s.scope_id)
        if len(chain) != len(set(chain)):
            err("scope_cycle", f"{s.scope_id}: {chain}")
    for w in u.waters:
        if w.section and scopes and w.section not in ids:
            err("water_in_unknown_section", f"{w.name!r} claims section {w.section!r}")

    # --- rules should mostly say something --------------------------------
    blank = [r for r in parsed.rows if not (r.species or r.dates or r.limits_gear)]
    if blank:
        err("blank_rules", f"{len(blank)} rules carry no species, dates or limits")

    return out


def validate_slug(slug: str, cache_dir: Optional[Path] = None) -> List[Finding]:
    html = load_cached(slug) if cache_dir is None else \
        (Path(cache_dir) / "raw" / f"region{slug}-eng.html").read_text(encoding="utf-8")
    parsed = parse_region(html, slug)
    return check_region(slug, parsed, untangle(parsed))


def validate_history(history_dir: Path) -> List[Finding]:
    """Re-parse every archived version. A parser change that only works on today's page
    is a regression, and the archive is the only place that shows it."""
    out: List[Finding] = []
    for f in sorted(Path(history_dir).glob("region*_*.html")):
        slug = f.name.split("_")[0].replace("region", "")
        if slug not in PAGES:
            continue
        stamp = f.name.split("_")[1][:8]
        try:
            parsed = parse_region(f.read_text(encoding="utf-8", errors="replace"), slug)
            out += check_region(slug, parsed, untangle(parsed), label=f"{slug}@{stamp}")
        except Exception as exc:
            out.append(Finding("ERROR", "parse_raised", f"{slug}@{stamp}",
                               f"{type(exc).__name__}: {exc}"))
    return out


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--regions", nargs="+", choices=ALL_SLUGS)
    ap.add_argument("--history", action="store_true", help="also replay every archived version")
    ap.add_argument("--history-dir", type=Path, default=Path("cache/dfo_salmon/history"))
    ap.add_argument("--warnings", action="store_true", help="show WARN findings too")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    findings: List[Finding] = []
    checked = 0
    for slug in (args.regions or [s for s in ALL_SLUGS if not PAGES[s].is_stub]):
        try:
            findings += validate_slug(normalize_slug(slug))
            checked += 1
        except FileNotFoundError:
            logger.warning("region %s: no snapshot", slug)
    if args.history:
        before = len(findings)
        findings += validate_history(args.history_dir)
        n = len(list(Path(args.history_dir).glob("region*_*.html")))
        print(f"replayed {n} archived versions ({len(findings) - before} findings)")

    errors = [f for f in findings if f.severity == "ERROR"]
    warns = [f for f in findings if f.severity == "WARN"]
    for f in errors:
        print(f)
    if args.warnings:
        for f in warns:
            print(f)

    print(f"\n{checked} live regions checked · {len(errors)} errors · {len(warns)} warnings"
          + ("" if args.warnings else " (use --warnings to list)"))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
