"""PROTOTYPE 3 — THE FOLD: a ruleset becomes a TABLE, in the pipeline, once.

WHAT IT HANDLES
  Today the browser is handed ~88 raw rules per section and works out the table itself, with
  three independent "who wins" ladders (`kindBeaten`, `authBeaten`, `tribBeaten`) plus a fourth
  override (`shutAll`) applied straight to the keep cell outside any of them. Four mechanisms,
  four sets of hand-written guards, four places for a defect.

  Here there is ONE rule, the domain owner's: "Regional always overrides provincial (except full
  closure), and this water overrides regional always (except closures unless they are lifted in
  this water's regs)." Authority wins, with one exception. For the subject being asked about:

      candidates  every rule whose Subject COVERS it
      (1)         group them by their own subject; in a group the closest authority, then the
                  strictest
      (2)         among the group-winners a take of zero — closed or release — stands unless
                  something lifted it; closed over release, the one on the water first
      (3)         otherwise the closest authority; at equal authority the narrower subject;
                  then the strictest
      chain       the answer first, then the rest, each with why it is not the answer

  The chain of custody is not computed separately from the answer — the answer is `chain[0]`
  by construction. That is the point of doing this in the pipeline: the page cannot disagree
  with the chain, because the page never derives either one.

  A LIFT is the only thing that moves a take of zero. "except burbot, which may also be speared
  in Regions 3, 5, 6, 7 and 8" is a subtraction from the ban's subject where it bites; a
  regional table that merely says "release" under a federal closure does not lift it, and the
  closure stands (see `lifts.contradicted_closures` for the one case in the corpus).

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


#: The statuses that take a rung OUT OF THE RUNNING, as opposed to recording why it lost. A
#: rung with one of these is shown, and never re-weighed — see `competes`.
LIFTED = "lifted here — does not apply"
REPLACED_BY_CLAUSE = "replaced here by its own clause for this kind of water"


def competes(rung: "Rung") -> bool:
    """Could this rung be the answer on SOME day? A lifted rule, a parent its own clause
    replaced and a demoted ceiling cannot — they ride in the chain so the reader can see them,
    and that is all. The date-aware pass re-weighed every live rung in a chain, and Region 3's
    spring closure — lifted on the Fraser by the Fraser's own "Exempt from spring closure" —
    took the whole river back for six months of the year."""
    st = rung.status
    return st != LIFTED and st != REPLACED_BY_CLAUSE and not st.startswith("a weaker ")


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
    ceilings: List[Rung] = None     # other periods that bind AT THE SAME TIME (annual, possession)
    duties: List[Rung] = None       # what you must DO on keeping one
    quals: List = None              # possession multiples (see qualifiers.py)
    gates: List = None              # size bounds, accumulated from every rule (see gates.py)
    exemptions: List = None         # an exception whose place cannot be drawn — see lifts.py
    dormant: List = None            # clauses of a rule in the chain that is NOT the answer

    @property
    def governs(self) -> Rung:
        """The answer's own rule — first in the chain, by construction (see `resolve`)."""
        return self.chain[0]


def resolve(rungs: List[Rung], subject: Subject,
            kids=None, lifted=frozenset(), stranded: bool = False) -> Optional[Row]:
    """Everything that speaks to `subject`, ordered, with the winner first.

    `stranded` is for a subject NOTHING ELSE CAN CARRY: only seasonal rules speak to it and
    no row covers it. White sturgeon on the Fraser in Region 5, below Williams Lake River,
    has one rule here — "No Fishing for sturgeon Sept 15 – July 15" — and once the protected
    list stopped covering the fish, that rule was in no chain, no caveat list, nowhere, and
    the check failed on it. Its season heads the row, and the head carries its window so a
    reader is never shown a bare "0" for a closure that lifts in July."""
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

    # A DAILY QUOTA AND AN ANNUAL ONE ARE NOT COMPETITORS. They bind at the same time: you may
    # keep 4 trout today AND no more than 10 hatchery steelhead this licence year. Ranked
    # against each other one of them loses, and `rank` put "1 per day" ahead of "5 per licence
    # year" — so the annual province-wide steelhead quota, the most recognisable number in BC
    # angling regulation, was demoted into the chain on 2,716 rows under the words "wider rule,
    # replaced by one closer to this water". It is not wider and it is not replaced.
    #
    # So each period is resolved in its own partition. The daily winner is the row's answer;
    # the others ride on the row as ceilings, the same shape `SubLimit` already has — a
    # constraint on the allowance, never a competitor and never a loser in the chain.
    other = [r for r in cand
             if r.outcome.kind == "quota" and (r.outcome.period or "daily") != "daily"]
    cand = [r for r in cand if r not in other]
    if not cand:
        cand, other = other, []
    # Every one of them rides — strictest first within its period. The first pass kept one
    # winner per period and dropped the rest, and lost 102 rules that way; the second carried
    # the losers into the chain as "a weaker annual ceiling", which measured empty across the
    # whole corpus. A ceiling that another ceiling beats is still a ceiling; it needs no
    # separate bucket, and the reader reads them in order.
    ceilings = sorted(other, key=lambda r: (r.outcome.period, r.rank, r.outcome.rank))

    # THE ORDER, in the domain owner's words: "Regional always overrides provincial (except full
    # closure), and this water overrides regional always (except closures unless they are lifted
    # in this water's regs)." AUTHORITY WINS, with one exception, and that exception is why the
    # first version of this — "group by subject, take the strictest group-winner" — was wrong for
    # everything but the one case it was written for:
    #
    #   "All wild steelhead must be released" is provincial and has no exception anywhere in the
    #   book. Region 4's "Trout/char: 5" does not say a wild steelhead may be among them. The
    #   release stands — a take of zero from ANY authority stands unless something lifts it.
    #
    #   But Kootenay Lake's own "rainbow trout daily quota = 10" over Region 4's "Trout/char: 5"
    #   is not a closure, and the strictest-wins rule handed the lake's reader a 5. The closest
    #   authority wins. And Region 8's "20 brook trout from streams" beside its own "4 from
    #   streams" is one authority speaking twice, once about all trout and once about this one:
    #   the narrower statement is the one about the fish, and the reader was told 4.
    #
    # So, in three steps. (1) Group the candidates by their own subject; within a group the
    # closest authority replaces the wider one, and at equal authority the stricter stands. (2)
    # Among the group-winners a take of zero — closed or release — stands, closed over release.
    # (3) Otherwise the closest authority wins; at equal authority the narrower subject; then
    # the stricter. A water that writes "trout and char: 10" over a regional 5 replaces it,
    # upward, by step (1) alone — the two are about the same fish.
    best = {}
    for r in cand:
        if not r.applies.always:
            continue
        cur = best.get(r.subject)
        if cur is None or (r.rank, r.outcome.rank) < (cur.rank, cur.outcome.rank):
            best[r.subject] = r
    head = head_of(list(best.values()))
    if head is None and stranded:
        for r in cand:
            cur = best.get(r.subject)
            if cur is None or (r.rank, r.outcome.rank) < (cur.rank, cur.outcome.rank):
                best[r.subject] = r
        head = head_of(list(best.values()))
    if head is None:
        # NO UNCONDITIONAL ANSWER HERE. Falling back to the first seasonal rung printed a
        # closure as the year-round answer with its dates stripped — the Cowichan read "you may
        # not fish for it" off a rule that closes it for eleven days in July, and 36 other rows
        # did the same. A subject whose only rules are seasonal has no year-round row; the
        # seasonal rules are still carried, and the client shows them on the days they bite.
        # (Unless nothing can carry them — see `stranded`.)
        return None
    out, chain = head.outcome, []
    # An annual ceiling is a ceiling on a NUMBER. Where the row has none it constrains nothing,
    # and it rode on 19 closed and 16 release rows. Demoted before the chain is built, so the
    # rungs land in it rather than being dropped.
    demoted = []
    if out.kind not in ("quota", "unlimited"):
        demoted, ceilings = ceilings, []
    # THE CHAIN STARTS WITH ITS ANSWER. It used to be sorted by (authority, strictness) and the
    # answer found by a second algorithm, so `chain[0]` was not the head on 370 of 846 rows and
    # `Row.governs` returned the wrong rung. The answer goes first; behind it, a closure on the
    # water — a season's "No Fishing" is what a reader looks for — then authority, then
    # strictness.
    cand.sort(key=lambda r: (r is not head,
                             not (r.outcome.kind == "closed" and _shuts_the_water(r.subject)),
                             r.rank, r.outcome.rank))
    for r in cand:
        if r is head:
            st = "governs"
        elif not r.applies.always:
            st = f"instead, {r.applies.detail}"
        elif r.outcome == out:
            st = "says the same thing"
        elif head.rank < 0 and r.outcome.kind not in ("closed",):
            # Under a superior authority nothing is "closer to this water". The regional table's
            # "White Sturgeon: CATCH AND RELEASE ONLY" beneath the federal closure was printed
            # as "set wider (Region 2), replaced by one closer" — a federal closure is not closer
            # to the Fraser than the Fraser's own region is. It is simply not something a lower
            # table can open (see `lifts.contradicted_closures`).
            st = "does not open what a superior authority closed"
        elif r.rank > head.rank:
            st = f"set wider ({r.authority}), replaced by one closer to this water"
        elif r.subject == head.subject:
            # Same fish, and no closer than the answer: it lost on strictness alone.
            st = "not the strictest here"
        elif head.outcome.kind == "closed" and _shuts_the_water(head.subject):
            # AGAINST THE ANSWER, NOT AGAINST `win`. The two differ exactly when the strictest
            # closure is seasonal — and then 38 rows told a reader a rule was "suspended while
            # the water is closed" on a row whose own answer was release or a number.
            st = "suspended while the water is closed"
        elif head.outcome.kind in ("closed", "release"):
            # Step (2): a closer or equal authority wrote a number for a broader group of fish,
            # and a number does not lift a take of zero on one of them.
            st = "a take of zero stands unless something lifts it here"
        else:
            # Step (3), equal authority: the same table wrote a narrower rule for this fish.
            st = f"{r.authority} also wrote a narrower rule for this fish, which speaks first"
        chain.append(Rung(r.rule_id, r.authority, r.rank, r.subject, r.outcome,
                          r.verbatim, r.applies, st))
    for r in gone:
        # `lifted` may carry a REASON. A rule can leave the running for more than one cause —
        # an exemption disapplies it, or its own clause replaced it on this kind of water — and
        # a reader who is shown the rule needs to know which.
        why = (lifted.get(r.rule_id) if isinstance(lifted, dict) else None)
        chain.append(Rung(r.rule_id, r.authority, r.rank, r.subject, r.outcome,
                          r.verbatim, r.applies, why or LIFTED))
    for r in demoted:
        chain.append(Rung(r.rule_id, r.authority, r.rank, r.subject, r.outcome, r.verbatim,
                          r.applies, f"a weaker {r.outcome.period} ceiling"))
    # A CLAUSE OF A RULE THAT LOST IS STILL A RULE. Sub-limits attached to the head alone, so
    # every clause whose parent was beaten vanished — 142 of them, while the check that exists
    # to catch exactly that printed COMPLIES because it counted the input instead of the output.
    #
    # They do not constrain the answer, so they cannot sit in `limits` where a reader would take
    # them as conditions on the number above. They ride separately, attributed to the parent
    # they belong to, which is also what makes them checkable.
    kids = kids or {}
    dormant = [l for c in chain if c.rule_id != head.rule_id
               for l in kids.get(c.rule_id, [])]
    # BY NAME. Positionally, `dormant` landed in `exemptions` — the ninth field — and `build`
    # then overwrote `exemptions` with the unresolved lifts, so the dormant clauses computed
    # here were thrown away every time and only the orphan sweep in `build` found them again,
    # on whichever row it tried first rather than the row whose chain holds their parent.
    return Row(subject, out, chain, caveats, limits=list(kids.get(head.rule_id, [])),
               ceilings=ceilings, duties=[], dormant=dormant)


#: "No Fishing" — a closure on the water itself, as opposed to a quota of zero for one fish.
_ALL_GAME = Subject(frozenset({"ALL_GAME_FISH"}))

def _shuts_the_water(s) -> bool:
    return s.method is None and s.covers(_ALL_GAME)


def head_of(winners: List[Rung]) -> Optional[Rung]:
    """The answer among ONE RULE PER SUBJECT — steps (2) and (3) of the order in `resolve`.

    A take of zero stands, closed over release, then the closest. Otherwise the closest
    authority; at equal authority the subject fewer of the others cover — the narrower one —
    and then the stricter. Narrowness is read off `covers` rather than off a species count, so
    "wild steelhead" is narrower than "steelhead" the same way "brook trout" is narrower than
    "trout and char".
    """
    if not winners:
        return None
    zero = [w for w in winners if w.outcome.kind in ("closed", "release")]
    if zero:
        # Among closures, the one on the WATER speaks first: "No Fishing" is the statement a
        # reader needs, not "Bass: 0", even where both are true.
        return min(zero, key=lambda r: (r.outcome.rank, not _shuts_the_water(r.subject), r.rank))
    def covered_by(r):
        return sum(1 for o in winners if o is not r and o.subject.covers(r.subject))
    return min(winners, key=lambda r: (r.rank, -covered_by(r), r.outcome.rank))


def _restates(row: Row, host: Row) -> bool:
    """Does `row` say what `host` says on EVERY date, not only year-round?

    Absorption asked one question — is the narrow row's answer the host's answer — and the
    answer it compared was the year-round one. The Chilliwack's "hatchery rainbow trout: 4,
    Jul 1 – Apr 30" sat in a rainbow row whose year-round answer was Region 2's 2, the same
    2 the "Trout and char · hatchery only" row had, so the rainbow row was absorbed and its
    season went with it: the group row then read 4 on the day, for every hatchery trout on
    the river. Shuswap's "Lake trout — release, Oct 15 – Jan 31" did the same to "Trout and
    char · 5", which read release for every trout in November.

    A row with a season of its own — a windowed rung that is not the host's and would beat
    the host's answer on the days it is live — is its own row. Beating is decided by the
    order itself: a windowed release under a host that is CLOSED changes nothing on any day,
    and the "Trout and char · 0" row it would otherwise print beside "All game fish · 0" is
    the restatement absorption exists to remove.
    """
    have = {c.rule_id for c in host.chain}
    g = host.governs
    for c in row.chain:
        if c.rule_id in have or not competes(c) or not c.applies.can_win or c.applies.always:
            continue
        best = {}
        for r in (g, c):
            cur = best.get(r.subject)
            if cur is None or (r.rank, r.outcome.rank) < (cur.rank, cur.outcome.rank):
                best[r.subject] = r
        won = head_of(list(best.values()))
        # THE SAME ANSWER, NOT THE SAME RUNG. A whole-water "No Fishing" in September beats a
        # species closure in `head_of` by being the statement a reader looks for — and both
        # are 0. Region 4's "White Sturgeon: CLOSED" restates the protected list either way.
        if won is not g and not won.outcome.same_answer(g.outcome):
            return False
    return True


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

    # MEASURED AND KEPT. On the corpus today every wild/hatchery pair also has a broader
    # release row to absorb into, so switching this step off changes no row and no chain
    # membership on any of the 102 sections — six differ in chain order only. It stays because
    # the row it produces, "release · wild and hatchery", is the honest heading where NO broader
    # row exists, and absorption cannot produce it.
    merged: List[Row] = []
    for row in rows:
        for m in merged:
            # ONLY WHERE THE OUTCOME HAS NO NUMBER. "release · wild" + "release · hatchery" is
            # one statement said twice. "2 · wild" + "2 · hatchery" is FOUR FISH, and merging it
            # into "2 · wild and hatchery" reads as two. All 3,264 joins in the corpus are
            # releases today; the guard is for the day one is not.
            if (m.outcome == row.outcome and m.outcome.kind in ("release", "closed")
                    and (j := m.subject.join(row.subject)) is not None
                    and _restates(row, m) and _restates(m, row)):
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
                m.dormant = _dedup((m.dormant or []) + list(row.dormant or []))
                mk = {c.rule_id for c in (m.ceilings or [])}
                m.ceilings = (m.ceilings or []) + [c for c in (row.ceilings or [])
                                                   if c.rule_id not in mk]
                md = {c.rule_id for c in (m.duties or [])}
                m.duties = (m.duties or []) + [c for c in (row.duties or [])
                                               if c.rule_id not in md]
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
    # THE GUARD THAT BLOCKS MUTUAL COVER ALSO BLOCKS EQUALITY, and `join` manufactures equality
    # after the merge pass has run: (ST, wild) + (ST, hatchery) becomes (ST, both), which is the
    # row already sitting there. Neither could absorb the other, so both printed — 684 identical
    # rows, sorted adjacent. Ordering by POSITION keeps the relation acyclic and lets the earlier
    # row win, without letting a genuinely mutual pair collapse the wrong way.
    host_of = {}
    for i, row in enumerate(merged):
        host_of[id(row)] = next((m for j, m in enumerate(merged)
                                 if m is not row and m.outcome == row.outcome
                                 and m.subject.covers(row.subject)
                                 and (not row.subject.covers(m.subject) or j < i)
                                 and _restates(row, m)), None)
    def final(row):
        seen = set()
        while host_of.get(id(row)) is not None:
            if id(row) in seen:
                # Unreachable while the ordering above stays acyclic — and if it ever is
                # reachable, the row silently leaves the table with its whole chain, which is
                # the one failure `comply` exists to catch and this path would escape.
                raise AssertionError("absorption cycle: a row would vanish with its chain")
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
        hk = {c.rule_id for c in (host.ceilings or [])}
        host.ceilings = (host.ceilings or []) + [c for c in (row.ceilings or [])
                                                 if c.rule_id not in hk]
        hd = {c.rule_id for c in (host.duties or [])}
        host.duties = (host.duties or []) + [c for c in (row.duties or [])
                                             if c.rule_id not in hd]
        host.dormant = _dedup((host.dormant or []) + list(row.dormant or []))
    out.sort(key=lambda r: (r.outcome.rank, sorted(r.subject.fish)))
    return out
