"""HOW YOU MAY FISH — gear, in gear's own words, on the quota table's two stages.

WHAT A GEAR RULE IS
  Three different kinds of statement answer "may I fish this way here", and they are not
  competitors for one cell:

      STANDING    may you use the method at all — a PERMIT or a BAN on the method itself.
                  "Spear fishing is permitted"; "No spear fishing of any kind in Regions 1, 2
                  and 4"; a lake's "No ice fishing". One of these governs, by the ladder.

      RIG         how you must rig it — a CONDITION on a method you may use. "Single barbless
                  hook", "Bait ban", "Artificial fly only", "1 line per angler". They do not
                  compete for a verdict; they ACCUMULATE, and the ladder only decides which of
                  two statements about the same thing is the one to print.

      KEEP        what you may keep by it — "Only non-game fish may be speared", "any game fish
                  other than burbot taken on a set line must be released". These ARE quota
                  counters, restricted to a method, and they settle in a `Ledger` exactly as the
                  quota table's do. That is the one abstraction gear and quotas share.

  A LIFT is a subtraction, not a rule of its own: "except burbot, which may also be speared in
  Regions 3, 5, 6, 7 and 8" takes one fish out of the spear ban where it bites; "EXEMPT from
  single barbless hooks" takes one rig rule off one river. A lift is shown on the row it
  changes, citing the sentence that made it.

THE DEFAULT, STATED
  Angling is what a licence is for: it is permitted unless something here bans it. Every other
  method is a listed exception to "angling only" — the book says "You may: ice fish ...; fish
  with a spear ...; trap crayfish ...; fish with a set line in lakes of Region 6 and 7A" — so it
  is NOT permitted unless something here permits it. The old table said "nothing forbids it, so
  it is allowed" of every method, which put set lining open on every lake in the province.

THE LADDER
  "Regional always overrides provincial (except full closure), and this water overrides
  regional always (except closures unless they are lifted in this water's regs)." The same
  `Source` the quota table carries, on the same two axes — who wrote it, what it binds to — and
  the same rank. A "No Fishing" closure on the water is not a gear rule at all: it comes from
  the quota ledger, already settled, so a water the quota table says is shut reads as shut here
  and the two cannot disagree.

TWO STAGES
  The BASE is the region's standing gear table for one kind of water — every region-wide rule
  (`Source.is_base`), settled on its own, once per (region, kind), and checkable against the
  printed synopsis. A section's OVERRIDES — its own rules, area rules, inherited ones, and the
  lifts among them — are laid on top. A water-scoped rule cannot enter a base, by type.
"""
from __future__ import annotations
import re
from dataclasses import dataclass, field, replace
from typing import Dict, FrozenSet, Iterable, List, Optional, Tuple

from pipeline.regs.table.applies import Applies, ALWAYS
from pipeline.regs.table.authority import Source, Authority, Scope, region_words
from pipeline.regs.table.ledger import Allowance, Ledger, LIFTED, SAME, ONLY_SOMEWHERE
from pipeline.regs.table.subject import Origin


#: Every way of fishing the book names, plus angling — which the book names only by omission.
METHODS = ["angling", "ice_fishing", "set_lining", "spear_fishing", "crayfish_trapping",
           "netting", "snagging", "chumming", "other"]
#: Plain words for the person at the water. Not the schema's identifiers.
NAMES = {"angling": "Rod and line", "ice_fishing": "Ice fishing", "set_lining": "Set line (left unattended)",
         "spear_fishing": "Spear or bow", "crayfish_trapping": "Crayfish traps", "netting": "Nets",
         "snagging": "Snagging", "chumming": "Chumming", "other": "Other"}
#: Rigging rules name no method because they do not have to — you rig a rod to angle. Ice
#: fishing and set lining are angling through ice and angling unattended, and the same hook,
#: bait and line rules reach them. A spear has no hook.
HOOK_AND_LINE = frozenset({"angling", "ice_fishing", "set_lining"})
#: Permitted unless banned. Everything not here is banned unless permitted (see the docstring).
BY_LICENCE = frozenset({"angling"})

RIG_TYPES = frozenset({"bait_restriction", "tackle_restriction"})
RIG_TOPIC = {"bait": "Bait", "barbless": "Hooks", "hook_count": "Hooks", "max_gap_mm": "Hooks",
             "min_gap_cm": "Hooks", "lure": "Lures and flies", "max_lines": "Lines",
             "max_flies": "Lines", "max_weight_kg": "Lines", "unspecified": "Also"}
RIG_ORDER = ["Bait", "Hooks", "Lures and flies", "Lines", "Also"]

#: What a class of bait is inside of. A ban on a wider class covers a rule about a narrower one.
BAIT_WITHIN = {"dead_fin_fish": ("fin_fish", "any"), "fin_fish": ("any",),
               "invertebrate": ("any",), "roe": ("any",), "any": ()}

COVERED = "covered by a wider rule here"
MOOT = "moot — a ban here already covers it"
REPLACED = "replaced here by a closer rule"
OPENED = "opened here by a closer rule"
CLOSED_BY = "closed here by a stricter rule"
EXCEPTION = "an exception carved into a wider rule — printed on that rule's line"
CONDITION = "a condition of the permit — printed with it"
HOURS = "at certain hours or days only"
#: Folded, not disapplied: still true here, printed inside another line.
FOLDED = frozenset({SAME, COVERED, EXCEPTION, CONDITION})


def bait_covers(wide: str, narrow: str) -> bool:
    return wide == narrow or wide in BAIT_WITHIN.get(narrow, ())


@dataclass(frozen=True)
class Term:
    """One statement about a way of fishing, with its provenance on both axes."""
    method: str                      # "" = every hook-and-line method
    kind: str                        # permit | ban | rig | keep | lift | while_closed
    source: Source
    applies: Applies = ALWAYS
    regions: FrozenSet[str] = frozenset()   # the book limits it to these regions; empty = wherever it lands
    text: str = ""                   # the plain words — the curated label
    topic: str = ""                  # rig: Bait | Hooks | Lures and flies | Lines | Also
    key: str = ""                    # rig: what it is about — bait:<class>, barbless, hook_count, lure, max_lines...
    says: Tuple[Tuple[str, str], ...] = ()   # rig: the content it sets, as (field, value) pairs
    allows: bool = False             # rig: "may be used" — an allowance, not a restriction
    only_when: str = ""              # a condition the book attaches: "when fishing for white sturgeon"
    keep: Optional[Allowance] = None # keep: the counter
    lifts: FrozenSet[str] = frozenset()      # rule ids this term lifts (entry::rule)
    lifts_fish: FrozenSet[str] = frozenset() # ... for these fish only; empty = the whole rule
    prose_extent: str = ""           # a condition the catalogue filed as a place — reported, not applied

    @property
    def rule_id(self) -> str: return self.source.rule_id
    @property
    def rank(self) -> int: return self.source.rank
    @property
    def is_base(self) -> bool: return self.source.is_base
    @property
    def is_default(self) -> bool: return not self.source.rule_id
    @property
    def bait_class(self) -> str:
        return self.key.split(":", 1)[1] if self.key.startswith("bait:") else ""

    def bites_in(self, here: FrozenSet[str]) -> bool:
        """A region contains its sub-regions: "Regions 3, 5, 6, 7 and 8" names 7A and 7B."""
        if not self.regions:
            return True
        return any(h == r or h.startswith(r) for h in here for r in self.regions)

    def stricter_than(self, o: "Term") -> bool:
        """Between two standing terms at one rank: a ban before a permit."""
        return self.kind == "ban" and o.kind == "permit"

    def covers_rig(self, o: "Term") -> bool:
        """Does this restriction say everything `o` says about the same part of the tackle?"""
        if self.topic != o.topic or self.allows:
            return False
        if self.key.startswith("bait:") and o.key.startswith("bait:"):
            return bait_covers(self.bait_class, o.bait_class)
        if self.key != o.key and not (set(dict(self.says)) >= set(dict(o.says))):
            return False
        a, b = dict(self.says), dict(o.says)
        if not b:
            return False
        for k, v in b.items():
            if k not in a:
                return False
            if k.startswith("max_") and a[k] and v:
                if float(a[k]) > float(v): return False
            elif k.startswith("min_") and a[k] and v:
                if float(a[k]) < float(v): return False
            elif a[k] != v:
                return False
        return True

    def plain(self) -> str:
        """The words a reader sees for this term. The label is already plain; the ones the
        page must not let anyone miss are said in the shortest possible form."""
        if self.kind == "rig" and self.key == "bait:any" and not self.allows:
            return "No bait" + _when(self)
        return self.text + _when(self)


def _when(t: Term) -> str:
    if not t.only_when or t.only_when.lower() in t.text.lower():
        return ""
    return f" — {t.only_when}"


def default_term(method: str, elsewhere: Iterable[Term] = ()) -> Term:
    """What holds when nothing on this water speaks about a method — said explicitly, with
    its reason, so a reader sees WHY rather than an empty cell."""
    if method in BY_LICENCE:
        src = Source(Authority.province, Scope.region, "", "", frozenset(), "",
                     "Angling is what a licence is for. Nothing here says otherwise.")
        return Term(method, "permit", src, text="Allowed — nothing here says otherwise")
    where = sorted({w for t in elsewhere for w in _permit_places(t)})
    note = (" The book allows it only " + " and ".join(where) + ".") if where else ""
    src = Source(Authority.province, Scope.region, "", "", frozenset(), "",
                 "You may fish only by angling, except as the book allows." + note)
    return Term(method, "ban", src, text="Not allowed here — nothing in the book allows it here")


def _permit_places(t: Term) -> List[str]:
    """"in lakes of Regions 6 and 7A" from a permit the book limits to some regions."""
    if t.kind != "permit" or not t.regions:
        return []
    kind = dict(t.says).get("water", "")
    return [(f"in {kind}s of " if kind else "in ") + region_words(t.regions)]


# ----------------------------------------------------------------------------------------
class MethodTable:
    """Every gear term on a section, settled. Immutable after construction.

    Settling is the ladder applied pairwise within each method's row:

        STANDING   the closest live term governs; a ban beats a permit at one rank. A permit
                   behind a closer ban is "closed here by"; a ban behind a closer permit is
                   "opened here by" — this water overrides regional, regional overrides
                   provincial, and a superior authority's ban is opened by nothing. A permit at
                   the governor's own rank that says something more is a CONDITION of it.

        RIG        restrictions accumulate. Where two say the same thing the closer prints and
                   the wider is folded behind it; where a wider rule is COVERED by a stricter
                   closer one ("fin fish may not be used" under "No bait") it is folded too —
                   still true, not worth the reader's eyes. A closer rule about the SAME thing
                   with a different answer replaces the wider one ("unlimited rods" over "one
                   line"). An allowance lifts what it names; a narrower allowance is an
                   EXCEPTION carved into the wider ban and printed on the ban's own line ("dead
                   fin fish when fishing for sturgeon" inside the fin fish ban); an allowance
                   under a ban that already covers it is moot. Only a YEAR-ROUND rule folds or
                   lifts another; a seasonal one rides beside the standing rule with its dates,
                   the way the ledger's carves do.

                   Rig terms are settled PER METHOD: a rule that names no method reaches every
                   hook-and-line method, but a lift that names one ("dead fin fish may be used
                   when set lining") lifts the ban under that method alone.

        KEEP       a `Ledger` per method — the quota table's own settlement.
    """

    def __init__(self, terms: Iterable[Term], *, water_kind: str = "", here: FrozenSet[str] = frozenset(),
                 label: str = "", closures: Iterable[Allowance] = (), lifted: Dict[str, str] = None,
                 keep_ledgers: Dict[str, Ledger] = None, elsewhere: Dict[str, List[Term]] = None,
                 lifted_fish: Dict[str, List[Tuple[FrozenSet[str], Term]]] = None):
        self.water_kind, self.here, self.label = water_kind, frozenset(here), label
        self.closures = tuple(closures)          # "No Fishing" on the water, from the quota ledger
        self.elsewhere = dict(elsewhere or {})   # permits the book writes for other places
        self.keep_ledgers = dict(keep_ledgers or {})
        self.lifted_fish = dict(lifted_fish or {})
        self.terms: Tuple[Term, ...] = tuple(sorted(
            terms, key=lambda t: (t.method, t.kind, t.rank, t.topic, t.key, t.rule_id)))
        #: method -> term -> why it does not print (empty = it prints). Rig terms are settled
        #: once per method; a method's own terms once, under that method.
        self.status: Dict[str, Dict[Term, str]] = {m: {} for m in METHODS}
        self.behind: Dict[str, Dict[Term, Term]] = {m: {} for m in METHODS}
        self.carves: Dict[str, Dict[Term, List[Term]]] = {m: {} for m in METHODS}
        pre = dict(lifted or {})
        for m in METHODS:
            for t in self._all(m):
                st = pre.get(t.rule_id, "")
                if t.applies.kind == "somewhere":
                    st = ONLY_SOMEWHERE
                self.status[m][t] = st
                self.carves[m][t] = []
            self._settle_standing(m)
            self._settle_rig(m)

    # -- construction -----------------------------------------------------------------
    def _for(self, method: str, kind: str) -> List[Term]:
        if kind == "rig":
            return [t for t in self.terms if t.kind == "rig"
                    and (t.method == method or (not t.method and method in HOOK_AND_LINE))]
        return [t for t in self.terms if t.kind == kind and t.method == method]

    def _all(self, method: str) -> List[Term]:
        return [t for t in self.terms if t.method == method
                or (not t.method and method in HOOK_AND_LINE and t.kind in ("rig", "lift"))]

    def status_of(self, method: str, t: Term) -> str:
        return self.status.get(method, {}).get(t, "")

    def _year_round(self, t: Term) -> bool:
        return t.applies.can_bind and not t.applies.within_day and (t.applies.always or t.applies.unless)

    def _settle_standing(self, method: str) -> None:
        st = self.status[method]
        cands = [t for t in self._for(method, "permit") + self._for(method, "ban")
                 if not st[t] and self._year_round(t)]
        if not cands:
            return
        gov = min(cands, key=lambda t: (t.rank, t.kind != "ban", t.rule_id))
        for t in cands:
            if t is gov:
                continue
            if t.kind == gov.kind:
                st[t] = CONDITION if (t.rank == gov.rank and t.kind == "permit") else SAME
            else:
                st[t] = OPENED if t.kind == "ban" else CLOSED_BY
            self.behind[method][t] = gov

    def _settle_rig(self, method: str) -> None:
        st, behind, carves = self.status[method], self.behind[method], self.carves[method]
        rig = self._for(method, "rig")
        lifts = [t for t in self.terms if t.kind == "lift" and not st.get(t)
                 and (t.method == method or (not t.method and method in HOOK_AND_LINE))]
        # 1. LIFTS. A lift names its target and removes it here, whole, for the method it
        #    speaks about — or, with a condition, carves an exception into it.
        for L in lifts:
            for t in rig:
                if t.rule_id in L.lifts and not L.lifts_fish and not st[t]:
                    if L.only_when:
                        carves[t].append(L)
                    else:
                        st[t] = LIFTED; behind[t] = L
        live = [t for t in rig if not st[t] and t.applies.can_bind and not t.applies.within_day]
        # 2. ALLOWANCES against restrictions.
        for A in [t for t in live if t.allows]:
            for R in [t for t in live if not t.allows and t.topic == A.topic]:
                if st[R] or st[A]:
                    continue
                related = R.covers_rig(A) or A.key == R.key
                if not related:
                    continue
                if A.rank < R.rank and self._year_round(A):
                    if A.key == R.key and not A.only_when:
                        st[R] = REPLACED; behind[R] = A
                    else:
                        carves[R].append(A); st[A] = EXCEPTION; behind[A] = R
                elif A.rank >= R.rank and R.covers_rig(A) and self._year_round(R):
                    # An allowance the ban itself names as its exception is carved in at any
                    # rank; one the ban does not name is simply moot under it.
                    if A.rule_id in R.lifts or R.rule_id in A.lifts:
                        carves[R].append(A); st[A] = EXCEPTION; behind[A] = R
                    else:
                        st[A] = MOOT; behind[A] = R
        # 3. RESTRICTIONS against restrictions.
        rest = [t for t in live if not t.allows and not st[t]]
        for A in rest:
            for B in rest:
                if A is B or st[A] or st[B] or B.rank > A.rank or not self._year_round(B):
                    continue
                if B.covers_rig(A) and (B.rank < A.rank or len(B.says) > len(A.says)
                                        or (len(B.says) == len(A.says) and B.rule_id < A.rule_id)):
                    st[A] = SAME if (B.says == A.says and B.key == A.key) else COVERED
                    behind[A] = B
                elif (B.rank < A.rank and B.key == A.key and not B.key.startswith("bait:")
                      and B.says != A.says and not A.covers_rig(B)):
                    st[A] = REPLACED; behind[A] = B

    # -- reading ----------------------------------------------------------------------
    def in_force(self, method: str, t: Term, on: Optional[Tuple[int, int]] = None) -> bool:
        """Binding on this date. Folded terms (same / covered / a condition / an exception)
        are still in force; lifted, moot, replaced, opened, closed-by and somewhere are not."""
        s = self.status_of(method, t)
        if s and s not in FOLDED:
            return False
        if not t.applies.can_bind or t.applies.within_day:
            return False
        return t.applies.live(*on) if on is not None else (t.applies.always or t.applies.unless)

    def shut(self, on: Optional[Tuple[int, int]] = None) -> Optional[Allowance]:
        """The closure that shuts the water on this date, if any — from the quota ledger."""
        live = [a for a in self.closures
                if (a.applies.live(*on) if on is not None else (a.applies.always or a.applies.unless))]
        return min(live, key=lambda a: (a.rank, a.rule_id)) if live else None

    def standing(self, method: str, on: Optional[Tuple[int, int]] = None) -> Term:
        """The permit or ban that governs, on this date — or the default, said explicitly."""
        cands = [t for t in self._for(method, "permit") + self._for(method, "ban")
                 if self.status_of(method, t) not in (LIFTED, ONLY_SOMEWHERE)
                 and t.applies.can_bind and not t.applies.within_day
                 and (t.applies.live(*on) if on is not None else (t.applies.always or t.applies.unless))]
        if not cands:
            return default_term(method, self.elsewhere.get(method, ()))
        return min(cands, key=lambda t: (t.rank, t.kind != "ban", t.rule_id))

    def conditions(self, method: str, on: Optional[Tuple[int, int]] = None) -> List[Term]:
        """The permit's own conditions: what the governing permit says, and every permit at
        its rank that adds a duty ("warn others of your ice hole")."""
        gov = self.standing(method, on)
        if gov.kind != "permit":
            return []
        out = [gov] if not gov.is_default else []
        out += [t for t in self._for(method, "permit")
                if self.status_of(method, t) == CONDITION and self.behind[method].get(t) is gov]
        return out

    def calendar(self, method: str) -> List[dict]:
        from pipeline.regs.table.rows import DAYS
        segs, cur = [], None
        for day in DAYS:
            c = self.shut(day)
            t = self.standing(method, day)
            key = ("closed", c.rule_id) if c else (t.kind, t.rule_id)
            if cur is not None and cur[0] == key:
                cur[2] = day
            else:
                cur = [key, day, day]; segs.append(cur)
        if len(segs) > 1 and segs[0][0] == segs[-1][0]:
            segs[0][1] = segs[-1][1]; segs.pop()
        return [{"from": list(a), "to": list(b), "verdict": k[0], "rule": k[1]} for k, a, b in segs]

    def rig(self, method: str, on: Optional[Tuple[int, int]] = None) -> Dict[str, List[Term]]:
        """The conditions to print, by topic: in force and not folded — and, with no date,
        every seasonal one beside the standing ones, each carrying its dates."""
        out: Dict[str, List[Term]] = {}
        for t in self._for(method, "rig"):
            if self.status_of(method, t):
                continue
            if not (t.applies.can_bind and not t.applies.within_day):
                continue
            if on is not None and not self.in_force(method, t, on):
                continue
            out.setdefault(t.topic or "Also", []).append(t)
        return {k: sorted(out[k], key=lambda t: (not (t.applies.always or t.applies.unless), t.rank, t.rule_id))
                for k in RIG_ORDER if k in out}

    def folded(self, method: str) -> List[Tuple[Term, str]]:
        """Every term of this row that is not printed as a line of its own, and why."""
        out = []
        for t in self._for(method, "rig") + self._for(method, "permit") + self._for(method, "ban"):
            s = self.status_of(method, t)
            if s or t.applies.within_day or not t.applies.can_bind:
                out.append((t, s or (ONLY_SOMEWHERE if not t.applies.can_bind else HOURS)))
        return out

    def within_day(self, method: str) -> List[Term]:
        return [t for t in self._for(method, "rig") + self._for(method, "ban") + self._for(method, "permit")
                if t.applies.within_day and not self.status_of(method, t)]

    def keep(self, method: str) -> Optional[Ledger]:
        return self.keep_ledgers.get(method)

    def rows(self) -> List["MethodRow"]:
        return [MethodRow(self, m) for m in METHODS if self.speaks_about(m)]

    def speaks_about(self, method: str) -> bool:
        """Every method the book names gets a row — a reader asks about set lines on a lake
        that has no set-line rule, and the answer is the default, said. `other` only where
        something is filed under it."""
        if method == "other":
            return any(t.method == method for t in self.terms)
        return True

    def universe(self) -> FrozenSet[str]:
        return frozenset(t.rule_id for t in self.terms if t.rule_id)

    def overlay(self, overrides: Iterable[Term], **kw) -> "MethodTable":
        """Stage 2: this base, with a section's own terms laid on top."""
        kw.setdefault("water_kind", self.water_kind)
        kw.setdefault("elsewhere", self.elsewhere)
        return MethodTable(list(self.terms) + list(overrides), **kw)


@dataclass
class MethodRow:
    """One way of fishing, as the page prints it — a VIEW of the settled table."""
    table: MethodTable
    method: str

    @property
    def name(self) -> str: return NAMES.get(self.method, self.method.replace("_", " ").title())
    def standing(self, on=None) -> Term: return self.table.standing(self.method, on)
    def conditions(self, on=None) -> List[Term]: return self.table.conditions(self.method, on)
    def shut(self, on=None): return self.table.shut(on)
    def rig(self, on=None): return self.table.rig(self.method, on)
    def carves(self, t: Term) -> List[Term]: return self.table.carves[self.method].get(t, [])
    def status_of(self, t: Term) -> str: return self.table.status_of(self.method, t)
    def folded(self): return self.table.folded(self.method)
    def calendar(self): return self.table.calendar(self.method)
    def keep(self): return self.table.keep(self.method)
    def candidates(self) -> List[Term]:
        return self.table._for(self.method, "permit") + self.table._for(self.method, "ban")

    def verdict_word(self, on=None) -> str:
        """The three words that must be unmissable, in the order a reader needs them."""
        if self.shut(on) is not None:
            return "closed"
        return "allowed" if self.standing(on).kind == "permit" else "not allowed"

    def uses_hook(self) -> bool:
        return self.method in HOOK_AND_LINE

    def visible_ids(self) -> FrozenSet[str]:
        """Every rule a reader can reach from this row — the totality check's target."""
        ids = set()
        for t in self.candidates():
            ids.add(t.rule_id)
        for ts in self.rig().values():
            for t in ts:
                ids.add(t.rule_id)
                ids |= {c.rule_id for c in self.carves(t)}
        for t, _ in self.folded():
            ids.add(t.rule_id)
        for t in self.table.within_day(self.method):
            ids.add(t.rule_id)
        for t in self.table._for(self.method, "rig"):
            ids |= {c.rule_id for c in self.carves(t)}
        L = self.keep()
        if L is not None:
            ids |= {a.rule_id for a in L.allowances}
        for fish, lifter in self.table.lifted_fish.get(self.method, []):
            ids.add(lifter.rule_id)
        ids |= {a.rule_id for a in self.table.closures}
        ids |= {t.rule_id for t in self.table.terms if t.kind == "while_closed"}
        ids.discard("")
        return frozenset(ids)


def resolve_method(method: str, terms, here=frozenset()):
    """Kept importable from here because `test_regs_table.py` imports it from here; the
    entry point is `method_build.table`."""
    from pipeline.regs.table.method_build import resolve_method as _r
    return _r(method, terms, here)
