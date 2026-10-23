"""WHAT THE REACH RUN SAYS ABOUT A RULE that the entry alone cannot (T135, curation-review PLAN §4
phase 4): the ambiguous cuts it chose between, the rules it could not bind, and the pieces it left
unplaced. The review queue's risk factors (`tiers.risks`) and the pre-checks (`precheck`) read them
from here, one reader:

    rule_diagnostic.jsonl   kind `ambiguous_cut` -> every candidate measure, the one used, and
                            `toward` (the ruling E16 side, or `curated` when the extent's
                            `cut_pick` decided it); kind `unclassified` -> pieces left unplaced
                            (one payload PER EXTENT: a rule with two cut extents can report two;
                            a payload with `included: true` was placed, so it is not unplaced)
    rule_unresolved.jsonl   the rules that bind nothing, with their typed reason

A run is read only when it is COMPLETE: `report.json` is the last file a reach run writes, so a run
directory without it is a run still being written and is refused, never read as "no findings".
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

#: the run's tables read here (`pipeline.atlas.reach.io` names them)
DIAGNOSTIC, UNRESOLVED = "rule_diagnostic", "rule_unresolved"
REACH_HINT = "python -m pipeline build --no-tiles   (the reach step writes the run)"


def is_curated_pick(cut: Mapping) -> bool:
    """Whether an ambiguous cut was DECIDED BY HAND — the extent's `cut_pick` (the builder writes
    `toward: "curated"`) — rather than taken by ruling E16. The one test of it on the review side."""
    return cut.get("toward") == "curated"


@dataclass(frozen=True)
class RunFacts:
    """Per entry, per rule: the ambiguous cuts (each a payload: `split_id`, `extent`, `used`,
    `candidates`, `toward`), the unresolved reason, and the unplaced pieces (each extent's
    `unclassified` payload: `extent`, `pieces`, `reasons`)."""
    ambiguous: Mapping[str, Mapping[str, tuple[dict, ...]]] = field(default_factory=dict)
    unresolved: Mapping[str, Mapping[str, dict]] = field(default_factory=dict)
    unclassified: Mapping[str, Mapping[str, tuple[dict, ...]]] = field(default_factory=dict)
    #: the run's own digest (`report.json` `digest`), so a reader can say which run it read
    digest: str | None = None

    def ambiguous_of(self, entry_id: str) -> Mapping[str, tuple[dict, ...]]:
        """Every ambiguous cut of the entry's rules, a curated pick's included."""
        return self.ambiguous.get(entry_id) or {}

    def open_ambiguous_of(self, entry_id: str) -> Mapping[str, tuple[dict, ...]]:
        """The ambiguous cuts NOBODY HAS DECIDED (`is_curated_pick` false): the builder took one by
        ruling E16. A rule whose every ambiguous cut is a curated `cut_pick` is not listed — the
        risk factor (`tiers.risks`) and the severity-A pre-check (`precheck`) both read this."""
        return {rid: open_ for rid, cuts in self.ambiguous_of(entry_id).items()
                if (open_ := tuple(p for p in cuts if not is_curated_pick(p)))}

    def unresolved_of(self, entry_id: str) -> Mapping[str, dict]:
        return self.unresolved.get(entry_id) or {}

    def unclassified_of(self, entry_id: str) -> Mapping[str, tuple[dict, ...]]:
        return self.unclassified.get(entry_id) or {}

    @classmethod
    def from_rows(cls, diagnostics: list[dict], unresolved: list[dict],
                  digest: str | None = None) -> "RunFacts":
        amb: dict[str, dict[str, list[dict]]] = {}
        unc: dict[str, dict[str, list[dict]]] = {}
        for r in diagnostics:
            if r["kind"] not in ("ambiguous_cut", "unclassified"):
                continue
            p = json.loads(r["payload"]) if isinstance(r["payload"], str) else dict(r["payload"])
            if r["kind"] == "ambiguous_cut":
                amb.setdefault(r["entry_id"], {}).setdefault(r["rule_id"], []).append(p)
            elif not p.get("included"):         # included pieces were placed: not unplaced
                unc.setdefault(r["entry_id"], {}).setdefault(r["rule_id"], []).append(p)
        unr: dict[str, dict[str, dict]] = {}
        for r in unresolved:
            unr.setdefault(r["entry_id"], {})[r["rule_id"]] = {
                "reason": r.get("reason"), "detail": r.get("detail") or ""}
        def by_extent(rows: dict[str, dict[str, list[dict]]]):
            return {e: {k: tuple(sorted(v, key=lambda p: (p.get("extent", 0),
                                                         str(p.get("split_id")))))
                        for k, v in rules.items()} for e, rules in rows.items()}
        return cls(ambiguous=by_extent(amb), unresolved=unr, unclassified=by_extent(unc),
                   digest=digest)

    @classmethod
    def read(cls, run_dir: str | Path) -> "RunFacts":
        """The facts of the COMPLETE run in `run_dir`. Raises FileNotFoundError naming the file and
        the command that makes it: no run, or a run still being written (no `report.json`)."""
        from pipeline.atlas.reach.io import read_table
        d = Path(run_dir)
        report = d / "report.json"
        if not report.exists():
            raise FileNotFoundError(f"{report} is missing — no complete reach run in {d} (a run "
                                    f"writes report.json last). Make one: {REACH_HINT}")
        for name in (DIAGNOSTIC, UNRESOLVED):
            if not (d / f"{name}.jsonl").exists():
                raise FileNotFoundError(f"{d / (name + '.jsonl')} is missing. Re-run: {REACH_HINT}")
        digest = json.loads(report.read_text(encoding="utf-8")).get("digest")
        return cls.from_rows(read_table(d, DIAGNOSTIC), read_table(d, UNRESOLVED), digest)
