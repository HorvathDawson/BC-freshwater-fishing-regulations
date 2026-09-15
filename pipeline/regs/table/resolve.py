"""PROTOTYPE 3 — THE FOLD: a ruleset becomes a TABLE, in the pipeline, once.

WHAT IT HANDLES
  Today the browser is handed ~88 raw rules per section and works out the table itself, with
  three independent "who wins" ladders (`kindBeaten`, `authBeaten`, `tribBeaten`) plus a fourth
  override (`shutAll`) applied straight to the keep cell outside any of them. Four mechanisms,
  four sets of hand-written guards, four places for a defect.

  Here there is ONE mechanism, and it is not a mechanism so much as a sort:

      candidates = every rule whose Subject COVERS the subject being asked about
      order by  (authority rank, then Outcome strictness)
      winner    = the first
      chain     = the rest, in that same order, each with why it is not the winner

  The chain of custody is not computed separately — it IS the sorted list. That is the point
  of doing this in the pipeline: the page cannot disagree with the chain, because the page
  never derives either one.

  CLOSURE IS ABSOLUTE, and says so in one line rather than in a keep-cell rewrite. A `Closed`
  from any authority wins outright unless something lifts it; that is the book's own shape
  ("No fishing" is not outranked by a regional quota) and it is now a property of the fold.

  MERGING is the last step, and it is safe BECAUSE outcomes are comparable: two rows merge only
  when their subjects join on one axis AND their outcomes are equal. "Release · wild only" plus
  "release · hatchery only" becomes one row saying "release · wild and hatchery". Two rows that
  merely look alike never merge, because `Outcome` equality is a real test.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional

from dataclasses import replace
from pipeline.regs.table.subject import Subject, Water
from pipeline.regs.table.outcome import Outcome
from pipeline.regs.table.applies import Applies, ALWAYS
from pipeline.regs.table.clauses import SubLimit


@dataclass(frozen=True)
class Rung:
    """One rule's claim on a subject, and what became of it."""
    rule_id: str
    authority: str          # "Provincial" | "Region 5" | "this water" ...
    rank: int               # 3 provincial, 2 zone, 1 inherited from downstream, 0 this water
    subject: Subject
    outcome: Outcome
    verbatim: str           # the synopsis sentence, exactly — present on 3,422 of 3,422 rules
    applies: Applies = ALWAYS
    status: str = ""        # filled by the fold


@dataclass
class Row:
    subject: Subject
    outcome: Outcome
    chain: List[Rung]
    caveats: List[Rung] = None      # true here, but not the answer: a window or an undrawable spot
    limits: List[SubLimit] = None   # what the allowance may be MADE OF (`within`)

    @property
    def governs(self) -> Rung: return self.chain[0]


def resolve(rungs: List[Rung], subject: Subject,
            kids=None, lifted=frozenset()) -> Optional[Row]:
    """Everything that speaks to `subject`, ordered, with the winner first."""
    cand = [r for r in rungs if r.subject.covers(subject)]
    if not cand:
        return None
    # A LIFTED RULE LEAVES THE RUNNING BUT NOT THE PAGE. The reader still needs to see that the
    # province bans spearing, and that burbot here are the exception to it.
    gone = [r for r in cand if r.rule_id in lifted]
    cand = [r for r in cand if r.rule_id not in lifted]
    if not cand:
        return None
    # Only an unconditional rule can BE the answer. A closure that bites in a season, or 23 m
    # below a fishway nobody can place, is carried beside the answer instead of replacing it.
    caveats = [r for r in cand if not r.applies.can_win]
    cand = [r for r in cand if r.applies.can_win]
    if not cand:
        return None

    shut = [r for r in cand if r.outcome.kind == "closed"]
    cand.sort(key=lambda r: (r.rank, r.outcome.rank))
    win = shut[0] if shut else cand[0]
    if shut:
        cand.remove(win); cand.insert(0, win)

    out, chain = win.outcome, []
    for r in cand:
        if r is win:
            st = "governs"
        elif r.outcome == out:
            st = "says the same thing"
        elif win.outcome.kind == "closed" and r.rank <= win.rank:
            st = "suspended while the water is closed"
        elif r.rank > win.rank:
            st = f"wider rule ({r.authority}), replaced by one closer to this water"
        else:
            st = "not the strictest here"
        chain.append(Rung(r.rule_id, r.authority, r.rank, r.subject, r.outcome,
                          r.verbatim, r.applies, st))
    for r in gone:
        chain.append(Rung(r.rule_id, r.authority, r.rank, r.subject, r.outcome,
                          r.verbatim, r.applies, "lifted here — does not apply"))
    return Row(subject, out, chain, caveats, list((kids or {}).get(win.rule_id, [])))


def _dedup(limits):
    """One clause, once. Two rows that merge usually hang off the SAME allowance, so their
    sub-limits are the same sub-limits — printed twice, they read as eight constraints on a
    quota of four."""
    out, seen = [], set()
    for l in limits:
        k = (l.subject, l.n, l.pooled)
        if k in seen: continue
        seen.add(k); out.append(l)
    return out


def applies_here(r: Rung, water_kind: str) -> bool:
    """CONTEXT IS NOT A TABLE ROW. "Kokanee — 5 per day" and "none from streams" are two rows
    only if you do not know which you are standing in. You do: the section is a stream or a
    lake. Resolving that axis away here is why the reader is not asked to do it, and why the
    page showed a kokanee limit of 5 beside a kokanee release on the same river."""
    return r.subject.water.value in ("any", water_kind)


def table(rungs: List[Rung], water_kind: str = "stream",
          kids=None, lifted=frozenset()) -> List[Row]:
    """Every subject anybody wrote about, resolved, then merged where it is safe."""
    rungs = [r for r in rungs if applies_here(r, water_kind)]
    # ONCE THE CONTEXT HAS DECIDED AN AXIS, IT IS NOT PART OF THE SUBJECT ANY MORE.
    #
    # "Kokanee — 5 per day" and "Kokanee — none from streams" are two subjects only while you
    # do not know which you are standing in. On a river they are one subject with two claims on
    # it, and leaving the axis in place produced two rows — a limit of 5 printed directly above
    # a release, for the same fish, on the same water. Erasing it here is also what stops "in
    # streams" being printed as a qualifier on a river, where it qualifies nothing.
    rungs = [replace(r, subject=replace(r.subject, water=Water.any)) for r in rungs]
    seen, subjects = set(), []
    for r in rungs:
        if r.subject not in seen:
            seen.add(r.subject); subjects.append(r.subject)

    rows = [row for s in subjects if (row := resolve(rungs, s, kids, lifted))]

    merged: List[Row] = []
    for row in rows:
        for m in merged:
            if m.outcome == row.outcome and (j := m.subject.join(row.subject)) is not None:  # noqa
                m.subject = j
                m.chain = m.chain + [replace(c, status="says the same thing"
                                             if c.status == "governs" else c.status)
                                     for c in row.chain if c.rule_id not in
                                     {x.rule_id for x in m.chain}]
                # The caveats come too. Leaving them behind here leaked 22 rules that were only
                # ever conditional — a seasonal closure on the merged fish, gone without trace.
                m.caveats = (m.caveats or []) + [c for c in (row.caveats or [])
                                                 if c.rule_id not in
                                                 {x.rule_id for x in (m.caveats or [])}]
                m.limits = _dedup((m.limits or []) + list(row.limits or []))
                break
        else:
            merged.append(row)
    # A BROAD ROW ABSORBS A NARROW ONE THAT AGREES WITH IT. "All game fish — 0" and "Kokanee —
    # 0" are not two answers; the second is the first, restated. Printing both is what made the
    # quota table look like it had more to say than it did.
    #
    # ABSORBING IS NOT DELETING. The first version dropped the narrow row outright and took its
    # whole chain with it — every rule that appeared only there stopped existing, silently, which
    # is the one failure this table cannot have. The compliance check found 861 of them. A row
    # that is absorbed hands its chain, its caveats and its sub-limits to the row that absorbed
    # it, so the reader can still find the rule and see why it is not the headline.
    # ABSORPTION IS TRANSITIVE. A absorbs into B, B into C — and the first pass handed A's chain
    # to B after B had already emptied into C, so A's rules were lost on a row nobody prints.
    # Follow the host chain to whoever actually survives.
    host_of = {}
    for row in merged:
        host_of[id(row)] = next((m for m in merged
                                 if m is not row and m.outcome == row.outcome
                                 and m.subject.covers(row.subject)
                                 and not row.subject.covers(m.subject)), None)
    def final(row):
        seen = set()
        while host_of.get(id(row)) is not None and id(row) not in seen:
            seen.add(id(row)); row = host_of[id(row)]
        return row
    out: List[Row] = []
    for row in merged:
        host = final(row)
        if host is row:
            out.append(row); continue
        have = {c.rule_id for c in host.chain}
        # ONE ROW, ONE WINNER. An absorbed row brings its own `governs` rung, and a page that
        # renders "governs" as "follow this one" then prints it twice for the same fish.
        # Whatever governed the narrower row governed a restatement of this one.
        host.chain += [replace(c, status="says the same thing" if c.status == "governs"
                               else c.status)
                       for c in row.chain if c.rule_id not in have]
        hc = {c.rule_id for c in (host.caveats or [])}
        host.caveats = (host.caveats or []) + [c for c in (row.caveats or []) if c.rule_id not in hc]
        host.limits = _dedup((host.limits or []) + list(row.limits or []))
    out.sort(key=lambda r: (r.outcome.rank, sorted(r.subject.fish)))
    return out
