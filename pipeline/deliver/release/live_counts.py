"""LIVE COUNTS IN THE RELEASE DIFF (T667 slice 6, owner D3 2026-11-11: "live counts stop being tests and are printed in
the release diff").

    python -m pipeline.deliver.release.live_counts data/release/<before> data/release/<after> [--reach DIR]

A count measured on one build (the Rancheria's 137 sections, 117 absorbed ids, 95 ambiguous cuts, P4 = 747 entries) is
not a rule: every approved change moved one and a test had to be re-pinned by hand. The rules those tests stood for stay
tests (`test_one_water_kind.py`, `test_review_queue.py`: invariants over the whole build). The counts are printed here
instead, before -> after, at the release check (`pipeline.release.stage_checks`), where a jump is read by a person.
It LISTS and never refuses.

Two groups:
  * RELEASE counts, read from a release folder's core file (both releases are on disk): items, absorbed ids, and the
    sections of the waters the flowing-polygon fold is watched on;
  * REVIEW counts, from a reach run + the corpus + the registry (the candidate's, at the release; the live release's
    come from its ledger record, where the release before stored them): the severity-A pre-check codes and the
    review tiers, as the curation review computes them (`regs.review.precheck` / `tiers`, one function each).
"""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
import tempfile
from collections import Counter
from pathlib import Path

#: the waters whose section count the flowing-polygon fold moves (AGENTS 55; were pins in test_one_water_kind.py)
WATCHED: dict[str, str] = {
    "gnis:11267": "Rancheria River",
    "wsc:915-679587-924307": "East Gribbell Creek",
    "gnis:7836": "Stellako River",
    "gnis:6438": "Six Mile Slough",
}


def _core(release_dir: Path) -> Path:
    release_dir = Path(release_dir)
    mpath = release_dir / "manifest.json"
    if not mpath.is_file():
        raise FileNotFoundError(f"{mpath} is missing: {release_dir} is not a release folder")
    core = json.loads(mpath.read_text(encoding="utf-8"))["files"].get("core")
    if not core:
        raise ValueError(f"{mpath} names no core file")
    gz = release_dir.parent / core["url"]
    if not gz.is_file():
        raise FileNotFoundError(f"{gz} is missing (named by {mpath})")
    return gz


def release_counts(release_dir: Path) -> dict[str, int]:
    """Counts read from a release folder's core file."""
    gz = _core(release_dir)
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "core.sqlite"
        db.write_bytes(gzip.decompress(gz.read_bytes()))
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            out = {"items": con.execute("SELECT COUNT(*) FROM item").fetchone()[0],
                   "absorbed ids (item_alias)": con.execute("SELECT COUNT(*) FROM item_alias").fetchone()[0]}
            for item, label in WATCHED.items():
                n = con.execute("SELECT COUNT(*) FROM item i JOIN item_section s ON s.ord = i.ord WHERE i.item_id = ?",
                                (item,)).fetchone()[0]
                out[f"sections: {label} ({item})"] = n
        finally:
            con.close()
    return out


def review_counts(reach_dir: Path, *, entries_dir: Path, cuts: Path, registry_json: Path) -> dict[str, int]:
    """The pre-check codes and the review tiers over the corpus, with the run's facts (`test_review_queue` measured these)."""
    from pipeline.atlas.reach.covered import covered_ids
    from pipeline.deliver.bundle.rules import closure_grade
    from pipeline.regs.review import precheck as P
    from pipeline.regs.review import tiers as T
    from pipeline.regs.review.runfacts import RunFacts
    run = RunFacts.read(Path(reach_dir))
    ids = {i["id"] for i in json.loads(Path(registry_json).read_text(encoding="utf-8"))["items"]}
    entries = P._corpus(Path(entries_dir))
    splits = json.loads(Path(cuts).read_text(encoding="utf-8"))
    codes = Counter(f["code"] for fs in P.corpus_findings(entries, registry_ids=ids, run=run, splits=splits).values()
                    for f in fs)
    tiers = Counter(T.entry_tier(e, binds=bool(covered_ids(e, ids)), run=run) for e in entries)
    closures = [(e, r) for e in entries for r in e.get("rules") or [] if closure_grade(r)]
    out = {f"precheck: {c}": codes.get(c, 0) for c in P.CHECKS}
    out |= {f"tier: {t}": tiers.get(t, 0) for t in T.TIERS}
    out["closures with an undrawn part"] = sum(1 for _, r in closures if r.get("undrawn_part")
                                               and not T.unidentified_part(r))
    out["closures with an ambiguous cut"] = sum(1 for e, r in closures if r["rule_id"] in run.ambiguous_of(e["entry_id"]))
    return out


def diff_text(before: dict[str, int] | None, after: dict[str, int], before_id: str, after_id: str) -> str:
    """One line per count, `before -> after (±d)`; a count the release before did not record says so."""
    lines = [f"live counts, {before_id} -> {after_id} (listed, never a stop; T667 D3):"]
    for k in sorted(after):
        a = after[k]
        if before is None or k not in before:
            lines.append(f"  {k}: {a} (not recorded for {before_id})")
            continue
        b = before[k]
        lines.append(f"  {k}: {b} -> {a}" + (f" ({a - b:+d})" if a != b else ""))
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("before", type=Path)
    ap.add_argument("after", type=Path)
    ap.add_argument("--reach", type=Path, help="a reach run: the review counts too (the candidate's, beside 'after')")
    a = ap.parse_args(argv)
    after = release_counts(a.after)
    if a.reach:
        from pipeline.common.curated import CURATED, GENERATED
        after |= review_counts(a.reach, entries_dir=CURATED.regulations.synopsis, cuts=CURATED.waters.cuts,
                               registry_json=GENERATED.registry())
    print(diff_text(release_counts(a.before), after, a.before.name, a.after.name))
    return 0


if __name__ == "__main__":
    sys.exit(main())
