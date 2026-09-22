"""THE TABLE, DERIVED: one row per kind of fish that the ledger treats alike.

A row is not a unit of the model any more — the ledger is. A row is a VIEW: every species the
same set of counters reaches, of the same origin, printed once, with every counter that binds
it ON the row. "Trout and char · 5 between them · of which 1 rainbow or cutthroat over 50 cm ·
of which 1 bull trout or Dolly Varden · none under 30 cm · 10 in possession" is one row, and
every one of those is a live constraint on the headline number, so none of them is folded away.

Where the ledger treats a species differently — Kootenay Lake's rainbow trout has its own 10
where the region's 5 covers the rest — that species is its own row, and the group row says
"other than rainbow trout" so the two cannot be read as overlapping.

Nothing here decides anything. Every line on a row is a counter the ledger already settled;
the page computes nothing, and the oracle (`oracle.py`) reads the same counters, so what it
decides by is what the row shows — that is the totality guarantee, by construction.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Tuple

from pipeline.regs.parsing.catalogue import SPECIES_GROUPS
from pipeline.regs.table.authority import Authority
from pipeline.regs.table.ledger import Allowance, Ledger
from pipeline.regs.table.subject import Origin, expand


#: Every (month, day) of a year, Feb 29 included — the calendar a row's answer is walked over.
DAYS = [(m, d) for m, n in enumerate((31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31), 1)
        for d in range(1, n + 1)]


@dataclass
class Row:
    ledger: Ledger
    fish: FrozenSet[str]             # leaf species, all treated alike
    origin: Origin                   # wild | hatchery | both
    counters: List[Allowance]        # every counter that reaches these fish on some date
    behind: List[Tuple[Allowance, str]] = field(default_factory=list)   # not binding, and why
    #: NAMED BY THE PROVINCE ALONE. A province-wide rule names this fish by name, and no
    #: regional table and no water rule does — and province-wide rules bind everywhere,
    #: whether or not the fish swims here. Region 4's printed table has no steelhead line, because the Columbia above its
    #: dams has no steelhead; the province's "all wild steelhead must be released" reaches
    #: Kootenay Lake all the same. The row is kept — a protection is never dropped — and the
    #: page sets it apart and says why, instead of splitting the trout block around it.
    province_only: bool = False

    @property
    def species(self) -> str:
        """A representative — every member of the row has the same counters."""
        return min(self.fish)

    def live(self, on: Optional[Tuple[int, int]] = None) -> List[Allowance]:
        """The counters binding on a date (`None`: the standing answer, no seasons)."""
        sp, o = self.species, self.origin if self.origin is not Origin.both else Origin.wild
        return [a for a in self.counters if self.ledger.binds(a, sp, o, None, on)]

    def moot(self, a: Allowance, on: Optional[Tuple[int, int]] = None) -> bool:
        """A number, or a cap, or a bound, under a headline of zero: true, and irrelevant —
        nothing may be kept, so there is nothing to count or to measure.

        A SIZE GATE IS MOOT TOO. The old guard ended `and not a.is_zero`, and a gate IS an
        allowance of zero — so "must be at least 60 cm" went on printing beside "Put it back",
        which reads as permission to keep a 61 cm fish. Nothing may be kept, so nothing can be
        measured."""
        h = self.headline(on)
        return h is not None and h.is_zero and a is not h

    def headline(self, on: Optional[Tuple[int, int]] = None) -> Optional[Allowance]:
        """THE number: the strictest daily counter on the whole fish, not a cap inside it and
        not a bound on a size class. Zero beats a number; among equals the closer speaks."""
        heads = [a for a in self.live(on) if a.period == "daily" and a.kind != "gate"
                 and not a.within and a.scope.size.is_any]
        if not heads:
            return None
        return min(heads, key=lambda a: (a.outcome.rank, a.rank, a.rule_id))

    def calendar(self) -> List[dict]:
        """The year, as the headline changes — every stretch of days with its answer."""
        segs, cur = [], None
        for day in DAYS:
            h = self.headline(day)
            key = (h.word(), h.rule_id) if h is not None else (None, None)
            if cur is not None and cur[0] == key:
                cur[2] = day
            else:
                cur = [key, day, day]; segs.append(cur)
        if len(segs) > 1 and segs[0][0] == segs[-1][0]:
            segs[0][1] = segs[-1][1]; segs.pop()          # Dec 31 wraps into Jan 1
        return [{"from": list(a), "to": list(b), "keep": k[0], "rule": k[1]}
                for k, a, b in segs]

    def heading(self, name) -> str:
        return heading(self.fish, name)

    def qualifier(self) -> str:
        return "" if self.origin is Origin.both else f"{self.origin.value} only"

    def size(self, on: Optional[Tuple[int, int]] = None, name=None) -> List[dict]:
        """THE SIZE STATEMENT, ALWAYS POPULATED. An angler holding a fish asks two things, in
        this order: may I keep this species, and is this one big enough or too big. Every
        bound and every size-class cap that binds today, in one place — and where nothing
        restricts the size, the row says so, because silence is not an answer.

        Each item: {"says", "kind" (bound|cap|any), "rule", "source", "n", "shared"}."""
        name = name or (lambda c: c)
        out = []
        per = {"annual": " this licence year", "possession": " in possession"}
        for a in self.live(on):
            if a.scope.size.is_any or a.derived_from is not None:
                continue                     # a possession multiple restates its daily cap
            if self.moot(a, on):
                continue                     # nothing may be kept, so nothing can be measured
            who, _ = a.scope.words(name)
            if a.kind == "gate":
                narrow = a.scope.fish and not a.scope.effective() >= self.fish
                out.append({"says": a.scope.size.words() + (f" ({who.lower()})" if narrow else ""),
                            "plain": a.scope.size.plain() + (f" ({who.lower()})" if narrow else ""),
                            "kind": "bound", "rule": a.rule_id, "source": a.source, "n": 0,
                            "shared": ""})
            else:
                cls = a.scope.size.words().replace("counting those ", "")
                # WHO ACTUALLY SPENDS IT, ON THIS DATE. Not `reaches`, which cannot see a
                # seasonal release; not one origin's view, which drops hatchery steelhead from
                # the family pool; and never a hidden anadromous form, whose name appears in no
                # heading on any table.
                o = None if self.origin is Origin.both else self.origin
                spend = self.ledger.spenders(a, o, on) - HIDDEN
                others = spend - self.fish
                shared = ", ".join(sorted(name(c).lower() for c in others)) if a.pooled and others else ""
                # "BETWEEN THEM" IS ABOUT THE POOL, NOT ABOUT WHO IS OFF THIS ROW. Region 4's
                # "1 rainbow trout or cutthroat trout over 50 cm" is pooled over exactly the two
                # fish on its row, so `others` is empty and the phrase vanished — and a reader
                # kept a 55 cm rainbow AND a 55 cm cutthroat where the book allows one.
                many = a.pooled and len(spend) > 1
                when = per.get(a.period, "")
                out.append({"says": f"no more than {a.n} {cls}{when}" + (f" — shared with {shared}" if shared else ""),
                            "plain": f"only {a.n} {a.scope.size.plain()}" + (" between them" if many else "") + when,
                            "kind": "cap", "rule": a.rule_id, "source": a.source, "n": a.n,
                            "shared": shared,
                            "spenders": sorted(name(c) for c in spend)})
        if not out:
            h = self.headline(on)
            if h is None or not h.is_zero:
                out.append({"says": "any size", "plain": "any size", "kind": "any", "rule": "",
                            "source": None, "n": None, "shared": ""})
        return out


#: Not shown to a reader. The anadromous forms of brook trout and Dolly Varden are in the
#: group codes and in no rule of their own; the one anadromous form that matters is steelhead,
#: which has its own name. Hidden on the page only — the ledger still carries them.
HIDDEN = frozenset({"ADV", "AEB"})


def heading(fish: FrozenSet[str], name, hide: FrozenSet[str] = HIDDEN) -> str:
    """The name a reader finds a set of species under: the group it is, or the members
    themselves. NEVER A GROUP NAMED BY EXCLUSION — "any other trout" tells the reader what a
    thing is not. Up to eight fish are named; beyond that the count and the group, with the
    members listed beneath."""
    shown = frozenset(fish) - hide or frozenset(fish)
    if len(shown) == 1:
        return name(next(iter(shown)))
    groups = [(len(expand(frozenset({g}))), g) for g in SPECIES_GROUPS
              if expand(frozenset({g})) and expand(frozenset({g})) >= fish]
    g = min(groups)[1] if groups else None
    if g and not (expand(frozenset({g})) - fish - hide):
        return name(g)
    names = sorted(name(f) for f in shown)
    if len(names) <= 8:
        return ", ".join(names[:-1]) + " or " + names[-1]
    return f"{len(names)} kinds of " + (name(g).lower() if g else "fish")


def rows(ledger: Ledger, name=None) -> List[Row]:
    """Every species the ledger names, grouped by the counters that reach it."""
    classes: Dict[Tuple[FrozenSet[Allowance], Origin], set] = {}
    for sp in sorted(ledger.universe()):
        keys = {}
        for o in (Origin.wild, Origin.hatchery):
            keys[o] = frozenset(a for a in ledger.allowances if ledger.reaches(a, sp, o))
        if keys[Origin.wild] == keys[Origin.hatchery]:
            if keys[Origin.wild]:
                classes.setdefault((keys[Origin.wild], Origin.both), set()).add(sp)
        else:
            for o in (Origin.wild, Origin.hatchery):
                if keys[o]:
                    classes.setdefault((keys[o], o), set()).add(sp)
    out = []
    for (key, origin), fish in classes.items():
        fish = frozenset(fish)
        counters = sorted(key, key=_order)
        row = Row(ledger, fish, origin, counters)
        sp, o = row.species, (origin if origin is not Origin.both else Origin.wild)
        # Named explicitly by a province-wide rule, and explicitly by nothing closer: a group
        # name ("trout and char") is not naming the fish.
        named = lambda a: any(f in a.scope.fish for f in fish)
        row.province_only = (
            any(a.source.authority is Authority.province and named(a) for a in ledger.allowances)
            and not any(a.source.authority is not Authority.province and named(a)
                        for a in ledger.allowances))
        # WHAT ELSE NAMES THESE FISH, AND WHY IT DOES NOT BIND. Replaced, lifted, only
        # somewhere — or in force for other fish and carved away for these.
        # OVER EVERY FISH IN THE CLASS, not a representative: "any other game fish" under a
        # whole-water closure holds whitefish and crayfish beside the char, and the replaced
        # whitefish 15 must be listed beneath it or it is a rule the reader never sees.
        for a in ledger.allowances:
            if a in key:
                continue
            f = next((f for f in sorted(fish) if a.contains(f, o)), None)
            if f is None:
                continue
            st = ledger.status[a]
            if not st and not a.applies.can_bind:
                st = ledger.status.get(a) or "only somewhere in here"
            if not st:
                c = ledger.carved_out(a, f, o, None, None)
                st = f"replaced for these fish by {c.source.words()}" if c else ""
            if st:
                row.behind.append((a, st))
        out.append(row)
    out.sort(key=lambda r: ((r.headline().outcome.rank if r.headline() else (9, 0, 0)),
                            sorted(r.fish), r.origin.value))
    return out


def _order(a: Allowance):
    """Headline first — a zero before a number — then caps inside it, then gates, then the
    other clocks."""
    cap = bool(a.within) or (not a.scope.size.is_any and a.kind != "gate")
    return ({"daily": 0, "annual": 4, "possession": 6}.get(a.period, 8)
            + (2 if a.kind == "gate" else 1 if cap else 0),
            not (a.applies.always or a.applies.unless),       # the standing rule before a season's
            a.outcome.rank, a.rank, a.rule_id)


#: "Jan", "Feb", … — a segment names its own dates, and a reader picks a table by them.
MONTHS = ("", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def schedule(rs: List[Row], also=()) -> List[dict]:
    """THE YEAR AS THE WHOLE TABLE CHANGES — every stretch of days over which one table is
    the answer.

    `Row.calendar` is this for one row and one number. A reader does not pick a row, they pick
    a DAY: "Aug 1 – Oct 31" is a whole table, not a footnote under one fish, and a season that
    releases the lake trout also empties the number the char shared with it. So the signature
    is every counter IN FORCE on the day, on every row — not just the headline, because a cap
    or a size bound coming into force changes the table without changing a single number.

    Wraps at the year's end the way `Row.calendar` does: Region 3's lake trout is released
    Oct 15 – Jan 31, which is ONE stretch of the reader's year, not a December one and a
    January one.
    """
    # `also` IS ANYTHING ELSE THE DAY CHANGES. The quota rows are not the whole table: Haida
    # Gwaii's bait ban runs Nov 1 – Apr 30 and touches no number at all, so a schedule built
    # from the rows alone gives that region one stretch and the bait ban is never shown in
    # force. Anything with `applies` and a `rule_id` can be passed in — gear terms are.
    def sig(day):
        return (tuple(tuple(sorted(a.rule_id for a in r.live(day))) for r in rs),
                tuple(sorted(t.rule_id for t in also if t.applies.live(*day))))

    segs = []
    for day in DAYS:
        k = sig(day)
        if segs and segs[-1][0] == k:
            segs[-1][2] = day
        else:
            segs.append([k, day, day])
    if len(segs) > 1 and segs[0][0] == segs[-1][0]:
        segs[0][1] = segs[-1][1]; segs.pop()
    out = []
    for _, a, b in segs:
        days = sum(1 for d in DAYS if _between(d, a, b))
        out.append({"from": list(a), "to": list(b), "days": days,
                    "label": f"{MONTHS[a[0]]} {a[1]} – {MONTHS[b[0]]} {b[1]}",
                    "whole_year": len(segs) == 1})
    return out


def _between(day, a, b) -> bool:
    return a <= day <= b if a <= b else (day >= a or day <= b)
