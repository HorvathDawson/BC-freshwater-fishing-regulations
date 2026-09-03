"""Put the subject back into a rule's `details` when the parser dropped it.

`details` is what a curator and the app read on its own, with no sentence around it. The parser
normalised "Trout/char catch and release" down to "Catch and release" — which fish is gone, and the
line is unusable standalone. 190 rules corpus-wide, across every region.

It also broke `split_bundled_gear`, which refuses to split a clause whose subject it does not
recognise ("a subject we do not know is a refusal to split, never a guess"). So the dropped subject
silently blocked normalisation downstream: MAHOOD LAKE stayed at 4 rules where it should be 8.

The subject is recovered from `rule_text`, which is verbatim synopsis text and was never rewritten —
so this restores what the book says rather than guessing. A rule is touched only when ALL of:

  * `details` STARTS with a bare "catch and release" / "daily quota" / "release", and
  * `details` names no fish anywhere, and
  * `rule_text` has a fish noun immediately before that same verb phrase.

Anything else is left alone and reported.

Where `species` is also EMPTY the subject was not merely unreadable, it was LOST — an empty species
list means "all species", so "Bull trout catch and release" was being applied to every fish in the
water. Those are filled from the recovered subject, using the same SUBJECTS table
`split_bundled_gear` uses, and only when it resolves; an unrecognised subject is reported, never
guessed. 8 rules corpus-wide.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_rule_subjects --dry-run
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_rule_subjects
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from pipeline.regs.parsing import io
from pipeline.regs.parsing.species import resolve_species_phrase
from pipeline.regs.parsing.split_bundled_gear import _subject_species


def species_of(subject: str) -> list[str]:
    """Species codes a subject names, or [] if neither resolver recognises it.

    `_subject_species` owns the COMPOUND subjects the synopsis uses ("Trout/char" -> the seven
    trout+char codes); `resolve_species_phrase` owns the single names, including the bare group
    "Trout". Neither covers the other, and a subject no resolver knows is left for a human.
    """
    codes = _subject_species(subject)
    if codes:
        return list(codes)
    return list(resolve_species_phrase(subject) or [])

FISH = (r"(?:trout|char|salmon|kokanee|bass|pike|whitefish|sturgeon|burbot|walleye|perch|"
        r"steelhead|cutthroat|rainbow|brook|dolly)")
VERB = r"(?:catch\s*and\s*release|daily\s+quota|annual\s+quota|release)"
_BARE = re.compile(r"^(?:catch\s*and\s*release|daily\s+quota|release)\b", re.I)
_HAS_FISH = re.compile(FISH, re.I)
_SUBJ = re.compile(r"([A-Za-z/()' ]*?" + FISH + r"[A-Za-z/()' ]*?)\s*" + VERB, re.I)


def subject_of(rule_text: str) -> str:
    """The fish subject sitting immediately before the verb phrase, or ''.

    Cut at the last clause boundary so "Upstream of Goodwin Falls: Trout/char catch and release"
    yields "Trout/char", not the whole locator.
    """
    m = _SUBJ.search(rule_text or "")
    if not m:
        return ""
    return re.split(r"[,;:]", m.group(1))[-1].strip()


def needs_subject(rule: dict) -> str:
    """The subject to prepend, or '' if this rule is fine / not safely repairable."""
    d = (rule.get("details") or "").strip()
    t = rule.get("rule_text") or ""
    if not d or not _BARE.match(d) or _HAS_FISH.search(d) or not _HAS_FISH.search(t):
        return ""
    return subject_of(t)


def repair(entries_dir: Path, dry_run: bool = False) -> dict:
    rep: dict[str, list] = {"details": [], "species": [], "skipped": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        changed = False
        for eid, e in by_id.items():
            for r in e.get("rules") or []:
                subj = needs_subject(r)
                if not subj:
                    d = (r.get("details") or "").strip()
                    if d and _BARE.match(d) and not _HAS_FISH.search(d):
                        rep["skipped"].append((eid, r.get("rule_id"), d, (r.get("rule_text") or "")[:60]))
                    continue
                before = (r.get("details") or "").strip()
                r["details"] = f"{subj[0].upper()}{subj[1:]} {before[0].lower()}{before[1:]}"
                rep["details"].append((eid, r.get("rule_id"), before, r["details"]))
                changed = True
                if not r.get("species"):
                    codes = species_of(subj)
                    if codes:
                        r["species"] = list(codes)
                        rep["species"].append((eid, r.get("rule_id"), subj, list(codes)))
                    else:
                        rep["skipped"].append((eid, r.get("rule_id"),
                                               f"species EMPTY and subject {subj!r} unrecognised", ""))
        if changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())      # atomic, via the model
    return rep


def main() -> None:
    ap = argparse.ArgumentParser(description="Restore the dropped subject in rule `details`.")
    ap.add_argument("--entries-dir", help="EntryFiles dir (default: pipeline/regs/parsing/entries).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    ap.add_argument("--limit", type=int, default=15, help="rows to print per section (default 15)")
    args = ap.parse_args()

    rep = repair(Path(args.entries_dir) if args.entries_dir else io.entries_dir(), dry_run=args.dry_run)
    verb = "[dry-run] would fix" if args.dry_run else "fixed"
    print(f"{verb} `details` on {len(rep['details'])} rule(s)")
    for eid, rid, before, after in rep["details"][: args.limit]:
        print(f"    {eid} [{rid}]\n        {before!r}  ->  {after!r}")
    if len(rep["details"]) > args.limit:
        print(f"    … and {len(rep['details']) - args.limit} more")
    print(f"\n{verb} `species` on {len(rep['species'])} rule(s) that had NONE (= 'all species'):")
    for eid, rid, subj, codes in rep["species"]:
        print(f"    {eid} [{rid}]  {subj!r} -> {codes}")
    if rep["skipped"]:
        print(f"\nleft alone ({len(rep['skipped'])}) — could not recover a subject safely:")
        for row in rep["skipped"][: args.limit]:
            print(f"    {row}")


if __name__ == "__main__":
    main()
