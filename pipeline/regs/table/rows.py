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
        nothing may be kept, so there is nothing to count or to measure."""
        h = self.headline(on)
        return h is not None and h.is_zero and a is not h and not a.is_zero

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
        for a in self.live(on):
            if a.period != "daily" or a.scope.size.is_any:
                continue
            who, _ = a.scope.words(name)
            if a.kind == "gate":
                narrow = a.scope.fish and not a.scope.effective() >= self.fish
                out.append({"says": a.scope.size.words() + (f" ({who.lower()})" if narrow else ""),
                            "kind": "bound", "rule": a.rule_id, "source": a.source, "n": 0,
                            "shared": ""})
            else:
                cls = a.scope.size.words().replace("counting those ", "")
                # Shared with the fish the cap STILL reaches — not one a closer rule took out
                # of it (Kootenay Lake's rainbow has its own any-size 10).
                o = self.origin if self.origin is not Origin.both else Origin.wild
                others = [c for c in a.scope.effective() - self.fish if self.ledger.reaches(a, c, o)]
                shared = ", ".join(sorted(name(c).lower() for c in others)) if a.pooled and others else ""
                out.append({"says": f"no more than {a.n} {cls}" + (f" — shared with {shared}" if shared else ""),
                            "kind": "cap", "rule": a.rule_id, "source": a.source, "n": a.n,
                            "shared": shared})
        if not out:
            h = self.headline(on)
            if h is None or not h.is_zero:
                out.append({"says": "any size", "kind": "any", "rule": "", "source": None,
                            "n": None, "shared": ""})
        return out


def heading(fish: FrozenSet[str], name) -> str:
    """The shortest honest name for a set of species: the group it is, the group it is all
    but a few of, or the members themselves."""
    if len(fish) == 1:
        return name(next(iter(fish)))
    groups = [(len(expand(frozenset({g}))), g) for g in SPECIES_GROUPS
              if expand(frozenset({g})) and expand(frozenset({g})) >= fish]
    g = min(groups)[1] if groups else None
    missing = (expand(frozenset({g})) - fish) if g else None
    if g and not missing:
        return name(g)
    if len(fish) <= 3 or (g and len(missing) >= len(fish)) or not g:
        if len(fish) <= 6:
            return ", ".join(sorted(name(f) for f in fish))
    # THE REST OF A GROUP IS NAMED AS THE REST, not by listing what it is not. Every fish
    # missing from it has a row of its own (that is why it is missing), so "Other trout and
    # char" beside "Rainbow trout" and "Bull trout" is what a reader expects to find.
    if g:
        return f"Other {name(g).lower()}"
    return f"{len(fish)} kinds of fish"


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
        for a in ledger.allowances:
            if a in key or not a.contains(sp, o):
                continue
            st = ledger.status[a]
            if not st and not a.applies.can_bind:
                st = ledger.status.get(a) or "only somewhere in here"
            if not st:
                c = ledger.carved_out(a, sp, o, None, None)
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
