"""Split a gear_restriction rule that bundles several restrictions into one rule each.

The synopsis writes them as a single phrase — "Bait ban, single barbless hook" — and the parser keeps
that shape, so one rule carries two independent restrictions. They have to be separate rules: they are
separately searchable, separately displayable, and a curator editing one should not have to edit a
sentence to do it.

CONSERVATIVE BY CONSTRUCTION: a rule is split only when EVERY comma/semicolon/"and"-separated segment
of `details` is a recognised restriction and there are at least two. Anything with a segment we do not
recognise is left alone and reported, so a phrase like "bait ban except when fishing for sturgeon"
is never chopped into nonsense.

Everything else on the rule is preserved verbatim on each half — extents, dates, species,
tributaries, `rule_text` (both halves quote the same source sentence, which is still a contiguous
substring of `regs_verbatim`), location_text, exception, display_location. Only `details` differs, and
`rule_id` gains a suffix.

Locked entries ARE included: this is a faithful re-expression of what the curator already confirmed,
not a reinterpretation of it. Run --dry-run first and read the diff.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.split_bundled_gear --dry-run
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.split_bundled_gear
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from pipeline.parsing import io

# (canonical name, pattern, restriction_type). `{1}` is replaced by the pattern's first capture
# group (hook sizes and closure seasons vary). A segment may carry a DIFFERENT type than the rule it
# came from: "Bait ban; exempt from spring closure" is a gear restriction plus a note, and the 75
# standalone exemptions already on file are all `note`.
RESTRICTIONS: list[tuple[str, str, str]] = [
    ("Exempt from {1} closure", r"^exempt\s+from\s+(?:the\s+)?(\w+)\s+closure$", "note"),
    ("Single barbless hook", r"^single\s+barbless\s+hooks?(?:\s+only)?$", "gear_restriction"),
    ("Single hook", r"^single\s+hooks?(?:\s+only)?$", "gear_restriction"),
    ("Barbless hook", r"^barbless\s+hooks?(?:\s+only)?$", "gear_restriction"),
    ("Max hook size {1} mm", r"^(?:no\s+hooks?\s+(?:greater|larger)\s+than\s*|max(?:imum)?\s+hook\s+size\s*)"
                             r"(\d+)\s*mm(?:\s+(?:from\s+)?point\s+to\s+shank)?$", "gear_restriction"),
    ("Bait ban", r"^bait\s+ban$", "gear_restriction"),
    ("Fly fishing only", r"^fly[\s-]*fishing\s+only$", "gear_restriction"),
    ("Artificial fly only", r"^artificial\s+fl(?:y|ies)(?:\s+only)?$", "gear_restriction"),
    ("Artificial lure only", r"^artificial\s+lures?(?:\s+only)?$", "gear_restriction"),
    ("No powered boats", r"^no\s+powered\s+boats?$", "gear_restriction"),
    ("Electric motor only", r"^electric\s+motors?\s+only(?:\s*-?\s*max.*)?$", "gear_restriction"),
]

# "catch and release" / "point to shank" are FIXED PHRASES — splitting on the "and" inside them turns
# one restriction into two fragments and is how a naive scan reports 411 bundled rules instead of 7.
_PROTECT = [(re.compile(r"catch\s*(?:and|&)\s*release", re.I), "\x00CR\x00"),
            (re.compile(r"point\s+to\s+shank", re.I), "\x00PTS\x00")]
_UNPROTECT = [("\x00CR\x00", "catch and release"), ("\x00PTS\x00", "point to shank")]

# Top-level clauses are separated by , and ; only. A bare "and" is NOT a top-level separator: it also
# joins two subjects of ONE clause ("hatchery rainbow trout ... and hatchery cutthroat catch and
# release"). Splitting on it first tore that clause in half and made the rule unrecognisable, so "and"
# is tried per-clause, as a fallback, once the clause has failed to match as a whole.
_SEP = re.compile(r"\s*[,;]\s*")
_AND = re.compile(r"\s+and\s+", re.I)

# A release/quota clause carries its OWN subject, so it is a separate rule from the gear restrictions
# beside it — and a DIFFERENT restriction_type, with its own species. `subject -> species codes`;
# a subject we do not know is a refusal to split, never a guess.
_TROUT_CHAR = ["RB", "CT", "WCT", "CCT", "GB", "GT", "SLV"]
SUBJECTS: list[tuple[str, list[str]]] = [
    (r"trout\s*/\s*char", _TROUT_CHAR),
    (r"(?:all\s+)?hatchery\s+rainbow\s+trout", ["RB"]),
    (r"(?:all\s+)?hatchery\s+cutthroat(?:\s+trout)?", ["CT"]),
    (r"(?:all\s+)?rainbow\s+trout", ["RB"]),
    (r"(?:all\s+)?cutthroat(?:\s+trout)?", ["CT"]),
    (r"(?:all\s+)?bull\s+trout", ["BT"]),
    (r"(?:all\s+)?kokanee", ["KO"]),
    (r"(?:all\s+)?bass", ["BS"]),
    (r"(?:all\s+)?yellow\s+perch", ["YP"]),
    (r"(?:all\s+)?steelhead", ["ST"]),
]

# "<subject> [(qualifier)] catch and release"  ->  one harvest rule per subject
_RELEASE = re.compile(r"^(?P<subj>.+?)\s*(?P<qual>\([^)]*\))?\s*\x00CR\x00$", re.I)
# "<subject> daily quota = N"  ->  one harvest rule per subject
_QUOTA = re.compile(r"^(?P<subj>.+?)\s+daily\s+quota\s*=?\s*(?P<n>\d+|unlimited)$", re.I)


def _subject_species(subj: str) -> list[str] | None:
    """The species codes a clause's own subject names, or None if we do not recognise it."""
    t = subj.strip().strip(" .").lower()
    for pat, codes in SUBJECTS:
        if re.fullmatch(pat, t, re.I):
            return codes
    return None


def _split_segments(details: str) -> list[str]:
    """`details` cut into candidate clauses, with fixed phrases protected from the "and" split."""
    d = details or ""
    for pat, tok in _PROTECT:
        d = pat.sub(tok, d)
    return [x.strip(" .") for x in _SEP.split(d) if x.strip(" .")]


def _restore(s: str) -> str:
    for tok, txt in _UNPROTECT:
        s = s.replace(tok, txt)
    return s


def classify(details: str) -> list[tuple[str, str, list[str] | None]] | None:
    """The (canonical restriction, restriction_type, species | None) triples in `details`, or None if
    ANY clause is unrecognised — an unrecognised clause is always a refusal to split the whole rule,
    never a guess.

    `species is None` means "leave the parent rule's species alone" (a gear restriction applies to
    whoever is fishing). A list means the clause named its own subject and the new rule gets exactly
    those codes — "Bass daily quota 8; yellow perch daily quota 20" is two rules over two species, and
    inheriting the parent's combined list onto both would say bass have a quota of 20."""
    segs = _split_segments(details)
    out: list[tuple[str, str, list[str] | None]] = []
    for seg in segs:
        got = _one_clause(seg)
        if got is None:                                             # try "and" as a separator here
            parts = [x.strip(" .") for x in _AND.split(seg) if x.strip(" .")]
            if len(parts) < 2:
                return None
            got = []
            for part in parts:
                sub = _one_clause(part)
                if sub is None:
                    return None              # an unrecognised clause -> do not touch this rule
                got += sub
        out += got
    return out if len({n for n, _, _ in out}) > 1 else None


def _one_clause(seg: str) -> list[tuple[str, str, list[str] | None]] | None:
    """One clause as rule(s): a gear restriction, or a release/quota clause. None if unrecognised."""
    for canon, pat, rtype in RESTRICTIONS:
        m = re.match(pat, _restore(seg), re.I)
        if m:
            name = canon.replace("{1}", m.group(1).lower()) if "{1}" in canon else canon
            return [(name, rtype, None)]
    return _release_or_quota(seg)


def _release_or_quota(seg: str) -> list[tuple[str, str, list[str]]] | None:
    """One release/quota clause as harvest rule(s), or None if its subject is not recognised.

    A single clause can carry SEVERAL subjects sharing one verb — "hatchery rainbow trout (50 cm or
    less) and hatchery cutthroat catch and release" is two rules over two species, and the size
    qualifier belongs only to the subject it follows. Each subject is resolved independently, so an
    unknown one refuses the whole rule rather than silently dropping a species."""
    m = _RELEASE.match(seg)
    if m:
        subjects = re.split(r"\s+and\s+", m.group("subj"))
        tail_qual = (m.group("qual") or "").strip()
        parsed: list[tuple[str, str, list[str]]] = []
        for i, raw in enumerate(subjects):
            raw = raw.strip(" .")
            q = ""
            qm = re.search(r"\(([^)]*)\)\s*$", raw)               # this subject's own qualifier
            if qm:
                q, raw = f"({qm.group(1)})", raw[:qm.start()].strip()
            elif i == len(subjects) - 1 and tail_qual:
                q = tail_qual
            codes = _subject_species(raw)
            if codes is None:
                return None
            parsed.append((raw.strip(), q, codes))
        # Several subjects under ONE restriction with the SAME qualifier is one rule over several
        # species — that is what `species` is for, and splitting it would produce two identical rules
        # ("Cutthroat trout and bull trout catch and release" is not two restrictions). Split only
        # when the qualifiers differ, because then no single rule can state them: on the Chilliwack
        # the 50 cm limit binds the rainbow and not the cutthroat.
        if len({q for _r, q, _c in parsed}) == 1:
            q = parsed[0][1]
            subj = " and ".join(r for r, _q, _c in parsed)
            codes = list(dict.fromkeys(c for _r, _q, cs in parsed for c in cs))
            label = f"{subj.capitalize()} {q}".strip()
            return [(f"{label}: catch and release", "harvest", codes)]
        return [(f"{f'{r.capitalize()} {q}'.strip()}: catch and release", "harvest", c)
                for r, q, c in parsed]
    m = _QUOTA.match(seg)
    if m:
        codes = _subject_species(m.group("subj"))
        if codes is None:
            return None
        return [(f"{m.group('subj').strip().capitalize()} daily quota = {m.group('n')}",
                 "harvest", codes)]
    return None


def _suffixed(rule_id: str, i: int, taken: set[str]) -> str:
    base = f"{rule_id}{chr(ord('a') + i)}"
    rid, n = base, 2
    while rid in taken:
        rid, n = f"{base}{n}", n + 1
    taken.add(rid)
    return rid


def _is_pure_reexpression(rule: dict, parts) -> bool:
    """True when every part keeps the parent's restriction_type and species — the same statement, just
    written as separate rules. Anything else (a gear rule yielding a harvest rule, a clause carrying
    its own species) is a REINTERPRETATION, and those are not applied to a locked entry."""
    return all(t == rule.get("restriction_type") and sp is None for _n, t, sp in parts)


def split_entry(entry: dict, include_locked: bool = False) -> tuple[list[str], list[str], list[str]]:
    """Split this entry's bundled rules in place.

    Returns (applied notes, skipped-locked notes, rule ids whose entry was UNLOCKED for re-review)."""
    notes: list[str] = []
    held: list[str] = []
    unlocked: list[str] = []
    taken = {r["rule_id"] for r in entry.get("rules") or []}
    locked = bool(entry.get("locked"))
    out: list[dict] = []
    for r in entry.get("rules") or []:
        parts = (classify(r.get("details", ""))
                 if r.get("restriction_type") in ("gear_restriction", "harvest") else None)
        if not parts:
            out.append(r)
            continue
        reinterprets = not _is_pure_reexpression(r, parts)
        if locked and reinterprets and not include_locked:
            held.append(f"{r['rule_id']}: {r.get('details','')!r} -> "
                        f"{[(n, t, sp) for n, t, sp in parts]}")
            out.append(r)                    # a locked curator decision: report, do not rewrite
            continue
        # Rewriting a CONFIRMED rule into rules with a different restriction_type and species is a
        # reinterpretation, so the entry stops being confirmed: it is unlocked and each new rule is
        # flagged, putting it back in the needs_review queue for the curator to re-confirm. Silently
        # keeping the lock would leave a "confirmed" stamp on content nobody has actually read.
        unlock_this = locked and reinterprets
        for i, (canon, rtype, species) in enumerate(parts):
            copy = dict(r)
            copy["details"] = canon
            copy["restriction_type"] = rtype
            if species is not None:
                copy["species"] = list(species)
            copy["rule_id"] = _suffixed(r["rule_id"], i, taken)
            if unlock_this:
                copy["needs_review"] = True
                copy["review_reason"] = (
                    f"split out of {r['rule_id']} ({r.get('details','')!r}) — confirm the "
                    f"restriction, its species and its extent")
            out.append(copy)
        if unlock_this:
            entry["locked"] = False
            entry.setdefault("audit_log", []).append(
                f"unlocked: {r['rule_id']} bundled several restrictions and was split into "
                f"{len(parts)}; re-confirm")
            unlocked.append(r["rule_id"])
        notes.append(f"{r['rule_id']}: {r.get('details','')!r} -> "
                     f"{[(n, t, sp) for n, t, sp in parts]}")
    entry["rules"] = out
    return notes, held, unlocked


def run(entries_dir: Path, dry_run: bool = False, include_locked: bool = False) -> dict:
    report: dict[str, list] = {"split": [], "skipped_unrecognised": [], "locked_held": [],
                               "unlocked": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        changed = False
        for eid, e in by_id.items():
            for r in e.get("rules") or []:                     # report-only pass for the near-misses
                if (r.get("restriction_type") in ("gear_restriction", "harvest")
                        and not classify(r.get("details", ""))):
                    d = r.get("details", "")
                    if len(_SEP.split(d)) > 1:
                        report["skipped_unrecognised"].append((region, eid, r["rule_id"], d[:70]))
            notes, held, unlocked = split_entry(e, include_locked=include_locked)
            for rid in unlocked:
                report["unlocked"].append((region, eid, rid))
            for note in notes:
                report["split"].append((region, eid, bool(e.get("locked")), note))
                changed = True
            for h in held:
                report["locked_held"].append((region, eid, h))
        if changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())    # atomic, via the model
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Split bundled gear_restriction rules into one rule each.")
    ap.add_argument("--entries-dir", help="EntryFiles dir (default: pipeline/parsing/entries).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    ap.add_argument("--include-locked", action="store_true",
                    help="also split LOCKED entries where the split changes restriction_type or "
                         "species. Such an entry is UNLOCKED and its new rules flagged needs_review, "
                         "so a reinterpreted rule is re-confirmed rather than left stamped as read.")
    args = ap.parse_args()
    entries_dir = Path(args.entries_dir) if args.entries_dir else io.entries_dir()
    rep = run(entries_dir, dry_run=args.dry_run, include_locked=args.include_locked)

    verb = "[dry-run] would split" if args.dry_run else "split"
    print(f"{verb} {len(rep['split'])} bundled rule(s)  "
          f"(locked: {sum(1 for r in rep['split'] if r[2])})")
    for region, eid, locked, note in rep["split"][:40]:
        print(f"    [{region}] {eid}{' (LOCKED)' if locked else ''}\n        {note}")
    if len(rep["split"]) > 40:
        print(f"    … and {len(rep['split']) - 40} more")
    if rep["unlocked"]:
        print(f"\n  UNLOCKED for re-review ({len(rep['unlocked'])}) — the split reinterpreted a "
              f"confirmed rule, so the entry goes back in the queue:")
        for region, eid, rid in rep["unlocked"]:
            print(f"    [{region}] {eid}  ({rid})")
    if rep["locked_held"]:
        print(f"\n  LOCKED — split changes restriction_type/species, NOT applied "
              f"({len(rep['locked_held'])}); re-run with --include-locked to apply:")
        for region, eid, note in rep["locked_held"]:
            print(f"    [{region}] {eid}\n        {note}")
    if rep["skipped_unrecognised"]:
        print(f"\n  LEFT ALONE — a segment was not recognised ({len(rep['skipped_unrecognised'])}):")
        for region, eid, rid, d in rep["skipped_unrecognised"][:15]:
            print(f"    [{region}] {eid} {rid}: {d!r}")


if __name__ == "__main__":
    main()
