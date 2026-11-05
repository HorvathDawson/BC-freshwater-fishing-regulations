"""T266 / T408: WHAT THE REVIEW MARKS DECIDE — computed in memory from marks.jsonl, never written to a file.

    python -m pipeline.regs.review.export            # print the decisions in force, per domain

`marks.jsonl` is REVIEW STATE (who checked what, when, the verdicts, the history). What the marks DECIDE is
`load()`: the mark IN FORCE on each key (`marks.latest`: the last one, a CLEARED key has none), each with its
`provenance` — {mark: sha256 of its line (12 hex), line: its line number, reviewer, date, source} — and every
distinct cut an uncleared mark proposed (`marks.proposal_groups`, the shared-cut history). The domains:

    tidal       scope tidal: the tidal boundaries          -> tools.tidal_limits    -> waters/tidal.json
    dfo         scope dfo: DFO extent fixes and new cuts   -> apply_proposals       -> entries/dfo_salmon (+ splits.json)
    entries     scopes entry / rule                        -> apply_proposals       -> entries/catalogue (+ splits.json)
    splits      scope split: calls on existing cuts        -> apply_proposals       -> waters/splits.json
    sloughs     scope slough: stream / not stream (T55)    -> tools.slough_calls    -> waters/slough_calls.json
    names       scope name: accept / reject (T224)         -> tools.mined_names     -> waters/mined_names.json
    ponds       scope q154 — CLOSED 2026-10-30 (T329)
    proposals   scope proposal (the retired accept step) — CLOSED (T331)

THE WRITERS (`review.apply`, run by the review app's write-through) take `load()` and write ONLY their final curated
file. There is no intermediate decided-data directory (owner 2026-11-05, T408: `data/curated/decided/` dropped). The
BUILDS (atlas, reach, tiles, deliver) read neither the marks nor this: they read the curated files those land in.

CLOSED DOMAINS (T325, `review.closed`): `load` leaves a closed domain's marks out of `inforce`. Its marks stay in
marks.jsonl (never edited) and still count in `marks.latest` order, so closing or reopening one domain changes no
other domain's decisions.

Deterministic. REFUSED (ValueError) on a conflict: two tidal decisions in force that cut the same water at
different points.
"""
from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence

from pipeline.regs.review import marks as M

#: domain -> the mark scopes it holds
DOMAINS: dict[str, tuple[str, ...]] = {
    "tidal": ("tidal",), "dfo": ("dfo",), "entries": ("entry", "rule"), "splits": ("split",),
    "sloughs": ("slough",), "ponds": ("q154",), "names": ("name",),
    # T331: the retired accept step's marks (scope "proposal", 7, 2026-10-27) — a domain only so it can be CLOSED
    "proposals": ("proposal",),
}

def _rel(p: Path) -> str:
    from pipeline.common.curated import REPO_ROOT
    try:
        return str(Path(p).resolve().relative_to(REPO_ROOT.resolve()))
    except ValueError:
        return str(p)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


@dataclass(frozen=True)
class Decided:
    """What the marks decide: the marks in force (in `marks.latest` order) and the shared proposed
    cuts (`marks.proposal_groups`), with each in-force mark's provenance."""
    inforce: tuple                                   # Mark, in latest() order
    groups: tuple                                    # proposal_groups rows
    provenance: Mapping = field(default_factory=dict)  # mark key -> provenance dict
    #: the owner's pre-Tab tidal calls file folded in first (repo-relative), or None
    tidal_decisions: Optional[str] = None

    def latest(self) -> dict:
        return {m.key: m for m in self.inforce}

    def scope(self, *scopes: str) -> list:
        return [m for m in self.inforce if m.scope in scopes]

    def shared(self, overrides: Sequence = ()) -> list[dict]:
        """The proposed splits with their confidence (`marks.proposals`)."""
        return M.with_confidence(self.groups, overrides)


def decide(marks: Iterable[M.Mark], *, tidal_decisions: Optional[Path] = None,
           source: str = "marks.jsonl", candidates: Optional[Path] = None) -> Decided:
    """export(marks) in memory. `tidal_decisions`: a file of tidal calls made before the Tidal tab, folded in
    before the marks (None, the default since T326: the owner's calls live in `waters/tidal.json` as rows with no
    mark, and the three pre-tab calls are all superseded by marks)."""
    from pipeline.regs.review import proposals as P
    early = P.tidal_decision_marks(tidal_decisions, candidates) if tidal_decisions else []
    store = list(marks)
    history = early + store
    prov: dict[tuple, dict] = {}
    for m in early:
        prov[m.key] = {"mark": _sha(M.line(m)), "line": None, "reviewer": m.reviewer, "date": m.date,
                       "source": _rel(Path(tidal_decisions))}
    for i, m in enumerate(store, 1):
        prov[m.key] = {"mark": _sha(M.line(m)), "line": i, "reviewer": m.reviewer, "date": m.date,
                       "source": source}
    inforce = tuple(M.latest(history).values())
    d = Decided(inforce=inforce, groups=tuple(M.proposal_groups(history)),
                provenance={m.key: prov[m.key] for m in inforce},
                tidal_decisions=_rel(Path(tidal_decisions)) if tidal_decisions else None)
    conflicts = refusals(d)
    if conflicts:
        raise ValueError("review.export: conflicting decisions — nothing exported:\n  " + "\n  ".join(conflicts))
    return d


def refusals(d: Decided) -> list[str]:
    """Conflicts the export refuses: two tidal DECISIONS (verified, one cut) in force on one water whose
    cuts are not the same cut (`marks.same_anchor`)."""
    from pipeline.regs.review import proposals as P
    by_water: dict[str, list] = {}
    for m in d.inforce:
        if P._tidal_decides(m) and m.new_splits[0].on_item:
            by_water.setdefault(m.new_splits[0].on_item, []).append(m)
    out = []
    for water, ms in sorted(by_water.items()):
        for a, b in zip(ms, ms[1:]):
            if not M.same_anchor(a.new_splits[0], b.new_splits[0]):
                out.append(f"{water}: tidal {a.target} ({a.date}) and {b.target} ({b.date}) cut it at different points")
    return out


def load(marks: Optional[Path] = None, closed: Optional[Iterable[str]] = None) -> Decided:
    """The decisions as the writers take them: `decide(marks)` (default the curated store) with a CLOSED domain's
    marks left out (`closed`; default the closed list beside the store, T325) — nothing applies a closed domain."""
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import closed as C
    shut = C.domains(C.path(marks)) if closed is None else frozenset(closed)
    d = decide(M.load(marks or CURATED.regulations.review.marks))
    gone = {s for dom in shut for s in DOMAINS.get(dom, ())}
    if not gone:
        return d
    keep = tuple(m for m in d.inforce if m.scope not in gone)
    return Decided(inforce=keep, groups=d.groups, provenance={m.key: d.provenance[m.key] for m in keep},
                   tidal_decisions=d.tidal_decisions)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.export", description=__doc__.split("\n")[0])
    ap.add_argument("--marks", type=Path, default=None, help="the marks store (default: CURATED's)")
    a = ap.parse_args(argv)
    from pipeline.regs.review import closed as C
    shut = C.domains(C.path(a.marks))
    d = load(a.marks, closed=shut)
    n = {dom: len(d.scope(*sc)) for dom, sc in DOMAINS.items() if dom not in shut}
    print(f"review.export: {sum(n.values())} decisions in force ({', '.join(f'{k} {v}' for k, v in n.items())}), "
          f"{len(d.groups)} proposed cuts" + (f"; closed: {', '.join(sorted(shut))}" if shut else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
