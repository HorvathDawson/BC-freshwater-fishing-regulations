"""Snapshot what the resolver answers for every rule, so a refactor can be proven behaviour-free.

    .venv/bin/python -m pipeline.tools.resolver_snapshot --out before.json
    # ... refactor ...
    .venv/bin/python -m pipeline.tools.resolver_snapshot --out after.json --compare before.json

`resolve_extent` is the most-tested logic in the project and the thing every downstream artifact will
be built on. Lifting it out of the review app must not change a single answer, and "the tests still
pass" does not prove that: the suite covers the shapes, not the 3,038 real rules. This records the
actual answer for each one — sections, straddlers, ambiguous cuts, waters — and diffs them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


def _load_reuse():
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "curation-review" / "backend"))
    import reuse  # noqa: E402
    return reuse


def snapshot() -> dict:
    reuse = _load_reuse()
    out: dict = {}
    for _region, e in reuse._all_entries():
        got = reuse.entry_reaches(e["entry_id"])
        rules = {}
        for rid, exts in got["rules"].items():
            rules[rid] = [
                None if x is None else {
                    "sections": sorted(x["sections"]),
                    "unclassified": sorted(x["unclassified"]),
                    "ambiguous_cut": sorted(json.dumps(a, sort_keys=True) for a in x["ambiguous_cut"]),
                    "waters": x["waters"],
                }
                for x in exts
            ]
        out[e["entry_id"]] = {
            "covered": got["covered"],
            "scope_sections": got.get("scope_sections", []),
            "rules": rules,
        }
    return out


def digest(snap: dict) -> str:
    return hashlib.sha256(json.dumps(snap, sort_keys=True).encode()).hexdigest()[:16]


def compare(a: dict, b: dict) -> int:
    """Print every difference. Returns the number of rules that differ."""
    diffs = 0
    for eid in sorted(set(a) | set(b)):
        if eid not in a or eid not in b:
            print(f"  entry only in {'before' if eid in a else 'after'}: {eid}")
            diffs += 1
            continue
        ra, rb = a[eid]["rules"], b[eid]["rules"]
        for rid in sorted(set(ra) | set(rb)):
            if ra.get(rid) != rb.get(rid):
                diffs += 1
                if diffs <= 20:
                    xa, xb = ra.get(rid), rb.get(rid)
                    na = [len(x["sections"]) if x else None for x in (xa or [])]
                    nb = [len(x["sections"]) if x else None for x in (xb or [])]
                    print(f"  {eid} :: {rid}   sections {na} -> {nb}")
        if a[eid].get("scope_sections") != b[eid].get("scope_sections"):
            diffs += 1
            print(f"  {eid} :: entry scope changed")
    return diffs


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True)
    ap.add_argument("--compare", help="an earlier snapshot to diff against")
    args = ap.parse_args()

    snap = snapshot()
    Path(args.out).write_text(json.dumps(snap, sort_keys=True))
    n_rules = sum(len(v["rules"]) for v in snap.values())
    print(f"{len(snap)} entries, {n_rules} rules  ->  {args.out}   digest {digest(snap)}")

    if args.compare:
        before = json.loads(Path(args.compare).read_text())
        print(f"\nbaseline digest {digest(before)}")
        d = compare(before, snap)
        print(f"\n{'IDENTICAL — the refactor changed no answer' if d == 0 else f'{d} RULE(S) DIFFER'}")
        sys.exit(0 if d == 0 else 1)


if __name__ == "__main__":
    main()
