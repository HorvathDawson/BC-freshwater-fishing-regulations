"""THE PARITY GATE (AGENTS 16; curation-review PLAN §4 phase 5, T136): does the review app bind every
rule exactly as the reach run that ships did?

    python -m pipeline.regs.review.parity --reach data/generated/reaches/full [--json out.json]
        [--build data/generated/atlas/full] [--entries data/curated/regulations/entries/catalogue]

A reviewer signs off on what the review app shows (`reuse.entry_reaches` -> `reach.build.build_reach`,
one entry at a time); the bundle ships what the reach run bound (`reach.build.build_reaches`, the
whole corpus at once). AGENTS 16 says they are one implementation; this measures it, rule by rule:
the outcome, the reason, and the SET of sections — over every rule of the corpus.

A difference is EXPLAINED only by a typed cause the code states (`EXPLAINED`): today one — a rule
whose extent is the book-known steelhead waters is a fact of the whole corpus, so one entry alone
answers `needs_corpus` by design (`build_reach`; AGENTS 54). Anything else is unexplained and fails the
gate (exit 1). The run must be of THIS corpus on THIS atlas (`check_vintage`), or the comparison
would be measuring a rebuild, not the two implementations: the CLI refuses otherwise.

The run's `rule_section.jsonl` is ~20 GB (one row per rule x section): it is STREAMED in its own
sorted order, one rule's sections in memory at a time, beside the app's answer for that rule's entry.
Heavy (the app loads the graph; reading the run is IO): run it through `scripts/heavy.sh`.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Callable, Iterable, Iterator, Mapping, Optional

BOUND = "bound"
UNRESOLVED = "unresolved"

#: THE ONLY DIFFERENCES THAT ARE NOT DEFECTS, each a cause the code states, by the app's reason.
EXPLAINED: dict[str, str] = {
    "needs_corpus": "the book-known steelhead waters (`Op.STEELHEAD_WATERS`) are a fact of the whole "
                    "corpus: one entry alone (`build_reach`, the app) answers needs_corpus by design; "
                    "only the full run (`build_reaches`) resolves them (AGENTS 54)",
}


@dataclass(frozen=True)
class Binding:
    """One rule's outcome as either side reports it: bound with its sections, or unresolved with a
    reason."""
    outcome: str
    reason: Optional[str] = None
    sections: frozenset = frozenset()


def compare(app: Optional[Binding], run: Optional[Binding]) -> Optional[dict]:
    """THE ONE COMPARISON: None when the two bindings are identical, else what differs —
    `{outcome: [app, run], reason: [app, run], gained, lost, sample_gained, sample_lost, explained}`
    (`gained` = sections the RUN binds and the app does not; `lost` = the app's that the run lacks).
    `explained` is the `EXPLAINED` key that accounts for it, or None."""
    a = app or Binding("absent")
    r = run or Binding("absent")
    if (a.outcome, a.reason, a.sections) == (r.outcome, r.reason, r.sections):
        return None
    gained, lost = r.sections - a.sections, a.sections - r.sections
    return {"outcome": [a.outcome, r.outcome], "reason": [a.reason, r.reason],
            "gained": len(gained), "lost": len(lost),
            "sample_gained": sorted(gained)[:5], "sample_lost": sorted(lost)[:5],
            "explained": explain(a, r)}


def explain(app: Binding, run: Binding) -> Optional[str]:
    """The `EXPLAINED` cause of a difference, or None. Only the app's `needs_corpus` today — and
    only when the run did answer (bound, or unresolved for a reason of its own)."""
    if app.outcome == UNRESOLVED and app.reason in EXPLAINED and run.outcome != "absent":
        return app.reason
    return None


def report(diffs: Mapping[tuple[str, str], dict], n_rules: int) -> dict:
    """The gate's summary: rules compared, identical, explained (by cause), unexplained (the keys)."""
    by: dict[str, int] = {}
    bad: list[str] = []
    for (eid, rid), d in sorted(diffs.items()):
        if d["explained"]:
            by[d["explained"]] = by.get(d["explained"], 0) + 1
        else:
            bad.append(f"{eid}#{rid}")
    return {"rules": n_rules, "identical": n_rules - len(diffs), "explained": by,
            "unexplained": len(bad), "unexplained_rules": bad,
            "parity": round(100.0 * (n_rules - len(diffs)) / n_rules, 3) if n_rules else None,
            "passes": not bad}


# --------------------------------------------------------------------------------------------
# Reading the run (streamed) and the app
# --------------------------------------------------------------------------------------------

def _row(line: str) -> tuple[str, str, str]:
    """(entry_id, rule_id, section_id) of a `rule_section.jsonl` line. The writer's own form
    (`io._write_jsonl`: sorted keys, compact) is split on its quotes — 20 GB through `json.loads`
    is most of the run's time; any other line is parsed as JSON."""
    p = line.split('"')
    if (len(p) == 17 and p[1] == "entry_id" and p[5] == "rule_id" and p[13] == "section_id"
            and "\\" not in line):
        return p[3], p[7], p[15]
    r = json.loads(line)
    return r["entry_id"], r["rule_id"], r["section_id"]


def run_sections(path: Path) -> Iterator[tuple[str, Iterator[tuple[str, frozenset]]]]:
    """The run's bound rules, entry by entry: (entry_id, iterator of (rule_id, sections)), in the
    file's sorted order (a file out of order is refused, never misread)."""
    def rows():
        last = None
        with Path(path).open(encoding="utf-8") as fh:
            for n, line in enumerate(fh, 1):
                if not line.strip():
                    continue
                k = _row(line)
                if last is not None and k <= last:
                    raise ValueError(f"{path}:{n}: rows out of order ({last} then {k})")
                last = k
                yield k
    for eid, er in groupby(rows(), key=lambda k: k[0]):
        yield eid, ((rid, frozenset(s for _, _, s in rr))
                    for rid, rr in groupby(er, key=lambda k: k[1]))


def run_unresolved(facts) -> dict[tuple[str, str], Binding]:
    """The run's unresolved rules as `Binding`s — read by the review side's one reader of the run's
    findings (`runfacts.RunFacts`), never a second one."""
    return {(eid, rid): Binding(UNRESOLVED, u.get("reason"))
            for eid, rules in facts.unresolved.items() for rid, u in rules.items()}


def compare_all(rules: Mapping[str, list[str]],
                app_of: Callable[[str], Mapping[str, Binding]],
                bound: Iterable[tuple[str, Iterator[tuple[str, frozenset]]]],
                unresolved: Mapping[tuple[str, str], Binding],
                progress: Callable[[int, int], None] | None = None) -> tuple[dict, int]:
    """Every rule of `rules` (entry_id -> its rule ids) compared: the app's binding (`app_of(entry)`,
    asked once per entry) against the run's (`bound`, the streamed sections; `unresolved`). A run
    row of an entry or rule the corpus does not have is a difference too (the app has nothing).
    Returns ({(entry, rule): difference}, rules compared)."""
    diffs: dict[tuple[str, str], dict] = {}
    seen: set[str] = set()
    # the run's unresolved rules by entry: one the corpus' entry does not have is still compared
    # (T136 adversarial review: only the run's BOUND rules were added to the corpus' own)
    unresolved_of: dict[str, set[str]] = {}
    for (e, r) in unresolved:
        unresolved_of.setdefault(e, set()).add(r)
    n = 0
    stream = iter(bound)
    pending = next(stream, None)
    for i, eid in enumerate(sorted(rules)):
        # the run's entries sorted before this one are entries the corpus does not have
        while pending is not None and pending[0] < eid:
            for rid, secs in pending[1]:
                diffs[(pending[0], rid)] = compare(None, Binding(BOUND, None, secs))
            pending = next(stream, None)
        run_b: dict[str, Binding] = {}
        if pending is not None and pending[0] == eid:
            run_b = {rid: Binding(BOUND, None, secs) for rid, secs in pending[1]}
            pending = next(stream, None)
        app = app_of(eid)
        for rid in sorted(set(rules[eid]) | set(run_b) | unresolved_of.get(eid, set())):
            n += 1
            got = compare(app.get(rid), run_b.get(rid) or unresolved.get((eid, rid)))
            if got is not None:
                diffs[(eid, rid)] = got
        seen.add(eid)
        if progress:
            progress(i + 1, len(rules))
    while pending is not None:
        for rid, secs in pending[1]:
            diffs[(pending[0], rid)] = compare(None, Binding(BOUND, None, secs))
        pending = next(stream, None)
    for (eid, rid), b in unresolved.items():
        if eid not in seen:
            diffs[(eid, rid)] = compare(None, b)
    return diffs, n


def app_binding(verdict: Mapping) -> Binding:
    """The review app's verdict for one rule (`reuse.entry_reaches(...)["verdict"][rule]`) as a
    `Binding`."""
    if verdict.get("outcome") == BOUND:
        return Binding(BOUND, None, frozenset(verdict.get("sections") or ()))
    return Binding(verdict.get("outcome") or "absent", verdict.get("reason"))


def check_vintage(report_json: Mapping, corpus_digest: str, handles: str, registry: str) -> list[str]:
    """Why a run cannot be compared with the app on this corpus and atlas ([] = it can)."""
    out = []
    if report_json.get("entries_digest") != corpus_digest:
        out.append(f"the run's corpus {report_json.get('entries_digest')} is not the app's "
                   f"{corpus_digest} (rerun reach, or point --entries at the corpus it read)")
    if (report_json.get("handles"), report_json.get("registry_digest")) != (handles, registry):
        out.append("the run was made on another atlas than --build")
    return out


def _load_app(build: Path, entries: Path):
    """The review app's data layer (`curation-review/backend/reuse.py`) over `build` and `entries` —
    the code a reviewer's screen runs, imported as the app imports it."""
    os.environ["CURATION_BUILD"] = str(build)
    os.environ["CURATION_ENTRIES_DIR"] = str(entries)
    os.environ.setdefault("CURATION_FIX", "0")
    backend = Path(__file__).resolve().parents[3] / "curation-review" / "backend"
    sys.path.insert(0, str(backend))
    import reuse                                               # noqa: E402
    # The corpus does not change during the run: read it once (the app re-reads it per request).
    once = reuse._all_entries()
    reuse._all_entries = lambda: once
    return reuse


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import CURATED, GENERATED
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.parity",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("--build", type=Path, default=GENERATED.build(), help="the atlas (default: the live one)")
    ap.add_argument("--reach", type=Path, default=None,
                    help="the reach run made on it (default: data/generated/reaches/<build name>)")
    ap.add_argument("--entries", type=Path, default=CURATED.regulations.entries.catalogue)
    ap.add_argument("--json", type=Path, help="write every difference and the summary here")
    ap.add_argument("--only", nargs="*", help="these entry ids only (a spot check)")
    a = ap.parse_args(argv)
    reach_dir = a.reach or (GENERATED.reaches / a.build.name)
    rep = json.loads((reach_dir / "report.json").read_text(encoding="utf-8"))
    reuse = _load_app(a.build, a.entries)
    from pipeline.common.section_handles import digest_for, registry_digest_for
    from pipeline.regs.parsing import io
    corpus = {e["entry_id"]: e for _, e in reuse._all_entries()}
    why = check_vintage(rep, io.corpus_digest(corpus), digest_for(a.build), registry_digest_for(a.build))
    if why:
        print("parity: refused —\n  " + "\n  ".join(why), file=sys.stderr)
        return 2
    rules = {eid: [r["rule_id"] for r in e.get("rules") or []] for eid, e in corpus.items()
             if not a.only or eid in a.only}
    t0 = time.time()

    def app_of(eid: str) -> dict[str, Binding]:
        got = reuse.entry_reaches(eid).get("verdict") or {}
        return {rid: app_binding(v) for rid, v in got.items()}

    def progress(i: int, n: int) -> None:
        if i % 100 == 0 or i == n:
            print(f"  {i:,}/{n:,} entries  {time.time() - t0:,.0f}s", flush=True)

    bound = ((eid, rr) for eid, rr in run_sections(reach_dir / "rule_section.jsonl")
             if not a.only or eid in a.only)
    from pipeline.regs.review.runfacts import RunFacts
    unresolved = {k: v for k, v in run_unresolved(RunFacts.read(reach_dir)).items()
                  if not a.only or k[0] in a.only}
    diffs, n = compare_all(rules, app_of, bound, unresolved, progress)
    summary = report(diffs, n)
    summary.update(seconds=round(time.time() - t0), reach=str(reach_dir), build=str(a.build),
                   run_digest=rep.get("digest"), entries_digest=rep.get("entries_digest"))
    print(json.dumps({k: v for k, v in summary.items() if k != "unexplained_rules"}, indent=1))
    for key in summary["unexplained_rules"][:40]:
        eid, rid = key.split("#", 1)
        print(f"  UNEXPLAINED {key}: {json.dumps(diffs[(eid, rid)])}")
    if a.json:
        a.json.write_text(json.dumps({"summary": summary, "differences": {
            f"{e}#{r}": d for (e, r), d in sorted(diffs.items())}}, indent=1) + "\n", encoding="utf-8")
    return 0 if summary["passes"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
