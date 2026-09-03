"""Bring every rule's `details` and `restriction_type` to the corpus standard.

The standard is `pipeline/regs/parsing/prompts/RULE_STANDARDS.md`; this module is its executable half.
One regulation must read the same way everywhere: `details` is the line a curator and the app read
on its own, and it is what `split_bundled_gear` pattern-matches, so a rule written five ways is five
things to everything downstream.

Measured before this ran (3,047 rules): 80 distinct quota grammars, three spellings of one motor
limit, three of one engine limit, and 6 statements (128 rules) filed under two different
`restriction_type`s.

CONSERVATIVE: every transform is a rewrite of PUNCTUATION, CASE or TYPE. None changes the content of
a rule — no number, species, size, season or subject is added, removed or reinterpreted. Anything a
rule says that a transform does not recognise is left exactly as it is. Run --dry-run and read the
diff; --report groups what changed so it can be scanned.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.normalize_details --dry-run
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.normalize_details
"""

from __future__ import annotations

import argparse
import re
from collections import Counter
from pathlib import Path

from pipeline.regs.parsing import io

# ── details rewrites ───────────────────────────────────────────────────────────────────────────
# (name, pattern, replacement). Applied in order, each at most once per rule.
DETAIL_RULES: list[tuple[str, re.Pattern, str]] = [
    # "daily quota 2" / "quota = 2" / "quota 2"  ->  "daily quota = 2"
    ("quota: insert '='",
     re.compile(r"\b(daily|annual|possession)\s+quotas?\s+(?=\d|unlimited)", re.I), r"\1 quota = "),
    ("quota: singular + '='",
     re.compile(r"\b(daily|annual|possession)\s+quotas\s*=\s*", re.I), r"\1 quota = "),
    # a bare "quota" with no period word is a DAILY quota — that is what the synopsis means
    ("quota: name the period",
     re.compile(r"(?<!daily )(?<!annual )(?<!possession )\bquotas?\s*=?\s*(?=\d|unlimited)", re.I),
     "daily quota = "),
    # ", none under 30 cm" / "- max 7.5 kW"  ->  parenthesised qualifier
    ("qualifier: comma -> parens",
     re.compile(r",\s*(none (?:under|over)[^,;()]*?|any size|no minimum size|max[^,;()]*?)\s*$", re.I),
     r" (\1)"),
    ("qualifier: dash -> parens",
     re.compile(r"\s+-\s+(max\s+[^,;()]+?)\s*$", re.I), r" (\1)"),
    # "Engine power restriction - 7.5 kW (10 hp)" -> "Engine power restriction (7.5 kW / 10 hp)"
    ("engine power: one parenthetical",
     re.compile(r"^engine power restriction\s*[-,]?\s*([\d.]+)\s*kW\s*\(\s*([\d.]+)\s*hp\s*\)\s*$", re.I),
     r"Engine power restriction (\1 kW / \2 hp)"),
    ("electric motor: parens",
     re.compile(r"^electric motor only\s*[-,]?\s*\(?\s*max\.?\s*([\d.]+)\s*kW\s*\)?\s*$", re.I),
     r"Electric motor only (max \1 kW)"),
    ("speed restriction: parens",
     re.compile(r"^speed restriction\s*[-,]?\s*\(?\s*([\d.]+)\s*km/h\s*\)?", re.I),
     r"Speed restriction (\1 km/h)"),
    # "catch-and-release" -> "catch and release". MUST run before the bare-"release" rule below:
    # a lookbehind for "catch and " does not see the hyphenated form, so the bare rule rewrote
    # "catch-and-release" into "catch-and-catch and release".
    ("catch-and-release: unhyphenate",
     re.compile(r"\bcatch[-\s]and[-\s]release\b", re.I), "catch and release"),
    # A bare "…release" where the synopsis means catch-and-release ("Rainbow trout and char release").
    ("release -> catch and release",
     re.compile(r"(?<!catch and )\brelease\b(?!d)", re.I), "catch and release"),
]

# ── restriction_type corrections ───────────────────────────────────────────────────────────────
# A statement's type is decided by what the rule DOES. Each of these was filed BOTH ways in the
# corpus; see RULE_STANDARDS.md §3 for the reasoning behind each.
TYPE_RULES: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"^no ice fishing\b", re.I), "closure",
     "it prohibits fishing in a season — not a tackle rule"),
    (re.compile(r"^no powered boats?\b", re.I), "vessel_restriction",
     "restricts the vessel, not the tackle"),
    (re.compile(r"^no vessels?\b", re.I), "vessel_restriction", "restricts the vessel"),
    (re.compile(r"^no towing\b", re.I), "vessel_restriction", "restricts the vessel"),
    (re.compile(r"^class (?:i|ii|1|2) water\b", re.I), "licensing", "a licence classification"),
    # Access, not licensing: it says who may be brought along on the water, not what licence anyone
    # holds. The corpus already said so — 7 of 8 were `note` before this table briefly disagreed.
    (re.compile(r"^youth/disabled accompanied water\b", re.I), "note", "an access provision, not a licence"),
    (re.compile(r"^angling prohibited\b", re.I), "closure", "prohibits fishing"),
    (re.compile(r"^exempt from\b", re.I), "note", "removes a restriction rather than imposing one"),
]


def normalize_details(details: str) -> tuple[str, list[str]]:
    """The standard spelling of `details`, and which transforms fired."""
    out, fired = (details or "").strip(), []
    for name, pat, repl in DETAIL_RULES:
        # count=0 (all occurrences): a rule can carry TWO quota clauses — "Bass daily quota = 8,
        # yellow perch quota 20" — and rewriting only the first left the rule off-standard and
        # unsplittable. Every transform here is anchored or punctuation-only, so repeating is safe.
        new = pat.sub(repl, out)
        if new != out:
            out, _ = new, fired.append(name)
    out = re.sub(r"\s{2,}", " ", out).strip(" ,;")
    if out and out[0].islower():                       # sentence case, leaving the rest alone
        out, _ = out[0].upper() + out[1:], fired.append("sentence case")
    return out, fired


def normalize_type(details: str, current: str) -> tuple[str, str]:
    """The type this statement should carry, and why. ('', '') when nothing applies."""
    for pat, want, why in TYPE_RULES:
        if pat.match((details or "").strip()) and current != want:
            return want, why
    return "", ""


def run(entries_dir: Path, dry_run: bool = False) -> dict:
    rep: dict = {"details": [], "types": [], "fired": Counter()}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        changed = False
        for eid, e in by_id.items():
            for r in e.get("rules") or []:
                before = (r.get("details") or "").strip()
                after, fired = normalize_details(before)
                if after != before:
                    r["details"] = after
                    rep["details"].append((eid, r.get("rule_id"), before, after, fired))
                    rep["fired"].update(fired)
                    changed = True
                want, why = normalize_type(after, r.get("restriction_type") or "")
                if want:
                    rep["types"].append((eid, r.get("rule_id"), after,
                                         r.get("restriction_type"), want, why))
                    r["restriction_type"] = want
                    changed = True
        if changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())      # atomic, via the model
    return rep


def conformance(entries_dir: Path) -> dict:
    """How much of the corpus is already at the standard — run before and after."""
    total = off_details = off_type = 0
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        for e in io.read_entryfile(path).values():
            for r in e.get("rules") or []:
                total += 1
                d = (r.get("details") or "").strip()
                after, _ = normalize_details(d)
                if after != d:
                    off_details += 1
                if normalize_type(after, r.get("restriction_type") or "")[0]:
                    off_type += 1
    return {"rules": total, "off_standard_details": off_details, "off_standard_type": off_type}


def main() -> None:
    ap = argparse.ArgumentParser(description="Normalise rule `details` and `restriction_type`.")
    ap.add_argument("--entries-dir", help="EntryFiles dir (default: pipeline/regs/parsing/entries).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    ap.add_argument("--report", action="store_true", help="group the changes instead of listing them.")
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()
    entries_dir = Path(args.entries_dir) if args.entries_dir else io.entries_dir()

    print("before:", conformance(entries_dir))
    rep = run(entries_dir, dry_run=args.dry_run)
    verb = "[dry-run] would rewrite" if args.dry_run else "rewrote"
    print(f"\n{verb} `details` on {len(rep['details'])} rule(s), by transform:")
    for name, n in rep["fired"].most_common():
        print(f"    {n:5d}  {name}")
    if not args.report:
        for eid, rid, b, a, _f in rep["details"][: args.limit]:
            print(f"    {eid} [{rid}]\n        {b!r}\n     -> {a!r}")
        if len(rep["details"]) > args.limit:
            print(f"    … and {len(rep['details']) - args.limit} more")
    print(f"\n{verb} `restriction_type` on {len(rep['types'])} rule(s):")
    grouped = Counter((t[3], t[4], t[5]) for t in rep["types"])
    for (was, now, why), n in grouped.most_common():
        print(f"    {n:5d}  {was} -> {now}   ({why})")
    if not args.dry_run:
        print("\nafter:", conformance(entries_dir))


if __name__ == "__main__":
    main()
