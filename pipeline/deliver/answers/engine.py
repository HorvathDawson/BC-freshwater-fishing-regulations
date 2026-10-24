"""THE KEEPING ENGINE (audit F13, plan 2.20): baskets of fish against one Part's rules and the reader's verdicts.

Moved out of the card gate (`card_check.py`) so the two modules that use it — the gate, which proves every number
on a card, and `caps.py`, which words a card's caps — import it from a neutral home instead of one reaching into
the other's privates. The method is described in `card_check`'s docstring: `Engine.can_keep` (may this fish be
kept after these?) and `Engine.most` (the counter-state search for the most a basket can hold). Unchanged code.
"""
from __future__ import annotations

import math
import sys
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver.bundle import read
from pipeline.common import entry_id as EID

INF = math.inf
PLACES = ("here", "stream", "lake")
#: "no daily limit" is proved by keeping this many
CAP = 40
#: the kinds of rule that limit keeping (`display.kind_of`)
COUNTING = ("pool", "subcap", "sizecap", "size", "gate")

Fish = Tuple[str, str, float, str]            # (species, origin, length cm, place)


def _covers(r, S: str) -> bool:
    return S in r.species and S not in r.except_


def bands_of(r) -> List[Tuple[float, float, Optional[float]]]:
    from pipeline.deliver.answers.display import bands
    return [(a, INF if b is None else b, t) for a, b, t in bands(r.x)]


class Engine:
    """Baskets of fish against one Part's rules and verdicts. `total` is the group's pool (its key):
    only the kinds it holds are asked about, but every rule covering them takes part."""

    def __init__(self, P, total: str, unlift: frozenset = frozenset()):
        """`unlift`: rules whose lifts are set aside (the headline: the share with no lift)."""
        self.P, self.T = P, P.by_key[total]
        self.unlift = frozenset(unlift)
        self.rules = [r for r in P.cands if r.k in COUNTING and r.type == "retention_limit"
                      and r.dimension != "lift" and not r.f.get("standing")]
        self.st_cm = read._steelhead_min_cm()
        # elsewhere: the zone's (and B.C.'s) rules in force today, by kind of water
        self.zone = [r for r in self.rules if EID.is_table(r.entry_id)
                     and read.in_force(r.f.get("when"), P.md, P.at) != "no"]
        self._zone = {id(r) for r in self.zone}
        # rows decision F17 (owner ruling 2026-10-21): the tally a water's steelhead quota raises a lifted size
        # clause to — every fish over the size, the steelhead included, toward the quota's number (a water's
        # own: it counts the fish kept elsewhere today). Not where the clause's lifts are set aside (`unlift`:
        # the steelhead are the clause's own fish again)
        from pipeline.deliver.answers import rows as RW
        here = {id(r) for r in self.rules}
        self.rules += [x for x in RW.raises(P) if id(x.clause) in here and x.clause.key not in self.unlift]
        self._state: Dict[Tuple[str, str, str], str] = {}
        self._bands_of: Dict[int, list] = {}
        self._needs: Dict[Fish, Optional[frozenset]] = {}
        self._incs: Dict[Fish, frozenset] = {}
        self._clip: Dict[tuple, int] = {}

    # -- what a rule is to a fish ------------------------------------------------------------------
    def kind_of_place(self, place: str) -> str:
        return self.P.kind if place == "here" else place

    def state(self, r, S: str, o: str) -> Optional[str]:
        """The reader's state of `r` for this fish and origin here (speaks / lifted / …)."""
        k = (id(r), S, o)
        if k not in self._state:
            clause = getattr(r, "clause", None)
            if clause is not None:
                # a raised tally (rows F17): it speaks for the fish the clause was lifted for, else as the clause
                st = "speaks" if (S, o) in r.lifted else self.state(clause, S, o)
            else:
                v = (self.P.ladder.get(S) or {}).get(o) or {}
                st = (v.get(r.key) or [None])[0]
                if st == "lifted" and r.key in self.unlift:
                    st = "speaks"
            self._state[k] = st
        return self._state[k]

    def _applies(self, r, f: Fish) -> bool:
        S, o, L, place = f
        if not _covers(r, S) or (r.f.get("origin") and r.f["origin"] != o):
            return False
        return not (r.f.get("water") and r.f["water"] != self.kind_of_place(place))

    def checks(self, r, f: Fish) -> bool:
        """`r` decides whether `f` may be kept."""
        if not self._applies(r, f):
            return False
        if f[3] == "here":
            return self.state(r, f[0], f[1]) == "speaks"
        return id(r) in self._zone

    def counts(self, r, g: Fish, f: Optional[Fish] = None) -> bool:
        """`r` counts `g` (a fish already kept) toward its take when `f` is asked about — RULINGS
        Q23b (refined 2026-10-12): a fish a water's rule lifted out of `r` counts toward `r` for a fish
        kept on ANOTHER water (the stream tally counts every trout/char from any stream), never for one
        kept on its own water (there the lifted kind and the rest are kept in any order)."""
        if not self._applies(r, g):
            return False
        if g[3] == "here":
            st = self.state(r, g[0], g[1])
            if f is None or f[3] == "here":
                return st == "speaks"
            # for a fish kept on ANOTHER water: a daily quota counts every fish kept today (p.80;
            # rows F9) — one this water's own number decided too (the zone's pool DISPLACED by a
            # water's smaller one sits beside it, RULINGS Q4: Deer Lake's 2, then Region 2's 4 leaves
            # 2 for another lake) — save a fish a water's rule LIFTED out of the total itself: it is
            # counted apart (rows F7, Ross Lake's brook trout 5). One lifted out of a sub-cap still
            # counts toward it (RULINGS Q23b, the stream tally).
            if st == "lifted" and r.k == "pool":
                return False
            return st in ("speaks", "lifted", "displaced")
        # elsewhere: the zone's rules for that water, and this water's own counts (rows F9)
        return id(r) in self._zone or (not EID.is_table(r.entry_id)
                                  and self.state(r, g[0], g[1]) in ("speaks", "lifted"))

    def _band(self, r, L: float):
        """(band index, take) of the length, or None where no band of the rule holds it."""
        bs = self._bands_of.get(id(r))
        if bs is None:
            bs = self._bands_of[id(r)] = bands_of(r)
        if not bs:
            return 0, (None if r.f.get("unlimited") else r.f.get("take"))
        for i, (a, b, t) in enumerate(bs):
            if a <= L <= b:
                return i, t
        return None

    def can_keep(self, kept: Sequence[Fish], f: Fish) -> bool:
        S, o, L, _place = f
        for r in self.rules:
            if not self.checks(r, f):
                continue
            hit = self._band(r, L)
            if hit is None:
                continue
            bi, take = hit
            if r.k == "gate" or take == 0:
                return False
            if take is None:                       # a clause's uncovered band, or unlimited
                continue
            n = sum(1 for g in kept if self.counts(r, g, f) and (self._band(r, g[2]) or (None,))[0] == bi)
            if n + 1 > take:
                return False
        return True

    def legal(self, seq: Sequence[Fish]) -> bool:
        return all(self.can_keep(seq[:i], f) for i, f in enumerate(seq))

    # -- the compiled fish (ROUND16 1): a basket read through the counters, as `most` searches them -------
    def _need(self, f: Fish):
        """The counters (rule, band, mode) a fish needs below their take, or None (never kept)."""
        if f in self._needs:
            return self._needs[f]
        mode = "here" if f[3] == "here" else "away"
        need = set()
        for ri, r in enumerate(self.rules):
            if not self.checks(r, f):
                continue
            hit = self._band(r, f[2])
            if hit is None:
                continue
            bi, take = hit
            if r.k == "gate" or take == 0:
                need = None
                break
            if take is None:
                continue
            self._clip[(ri, bi, mode)] = int(math.floor(take))
            need.add((ri, bi, mode))
        self._needs[f] = None if need is None else frozenset(need)
        return self._needs[f]

    def _inc(self, g: Fish) -> frozenset:
        """The counters a kept fish moves: for a fish asked here, and for one asked elsewhere."""
        got = self._incs.get(g)
        if got is None:
            out = set()
            for mode, f in (("here", ("", "", 0.0, "here")), ("away", ("", "", 0.0, "stream"))):
                for ri, r in enumerate(self.rules):
                    if self.counts(r, g, f):
                        hit = self._band(r, g[2])
                        if hit is not None:
                            out.add((ri, hit[0], mode))
            got = self._incs[g] = frozenset(out)
        return got

    def accepts(self, seq: Sequence[Fish]) -> bool:
        """`legal`, read through the counters (the solver's compiled fish): each fish needs every counter
        it is checked by below its take, and moves the counters that count it. Equal to `legal` (the
        no-data tests hold them so); fast enough for the cap-system proof's random baskets."""
        state: Dict[tuple, int] = {}
        for f in seq:
            need = self._need(f)
            if need is None or any(state.get(c, 0) >= self._clip[c] for c in need):
                return False
            for c in self._inc(f):
                state[c] = state.get(c, 0) + 1
        return True

    # -- the fish to try ---------------------------------------------------------------------------
    def lengths(self) -> List[float]:
        """One length inside every band any rule draws (cut points from every rule's lengths)."""
        cuts = {0.0}
        for r in self.rules:
            for a, b, _t in (self._bands_of.get(id(r)) or bands_of(r)):
                cuts.add(float(a))
                if b != INF:
                    cuts.add(float(b))
        cuts.add(float(self.st_cm))
        pts = sorted(cuts)
        return [(a + b) / 2 for a, b in zip(pts, pts[1:])] + [pts[-1] + 5]

    def fish(self, species: Iterable[str], places: Iterable[str] = ("here",),
             origins: Iterable[str] = ("hatchery", "wild")) -> List[Fish]:
        out = []
        for S in species:
            for o in origins:
                for L in self.lengths():
                    # a rainbow over 50 cm IS a steelhead where the definition holds (p.80)
                    if self.P.steelhead_water and ((S == "RB" and L > self.st_cm)
                                                   or (S == "ST" and L < self.st_cm)):
                        continue
                    for p in places:
                        f = (S, o, L, p)
                        if self.can_keep((), f):
                            out.append(f)
        return out

    def most(self, cands: Sequence[Fish], base: Sequence[Fish] = (), cap: int = CAP) -> int:
        """The most fish of `cands` (any number of each) that can be kept after `base`, in the best
        order, up to `cap` (ask for one more than a claim to prove it) — searched over the baskets'
        counter states (the module docstring); equal to `most_brute`, which the tests hold it to."""
        base = tuple(base)
        if not self.legal(base):
            raise ValueError(f"card_check: the base basket {base} is not legal")
        modes = {"here": ("", "", 0.0, "here"), "away": ("", "", 0.0, "stream")}
        idx: Dict[tuple, int] = {}             # (rule, band, mode) -> counter
        clip: List[int] = []
        meta: List[tuple] = []                  # counter -> (rule, band, the fish asked about)
        need_of: List[Optional[tuple]] = []
        for f in cands:
            mode = "here" if f[3] == "here" else "away"
            need, blocked = [], False
            for ri, r in enumerate(self.rules):
                if not self.checks(r, f):
                    continue
                hit = self._band(r, f[2])
                if hit is None:
                    continue
                bi, take = hit
                if r.k == "gate" or take == 0:
                    blocked = True
                    break
                if take is None:
                    continue
                c = idx.get((ri, bi, mode))
                if c is None:
                    c = idx[(ri, bi, mode)] = len(clip)
                    # refused once n + 1 > take: every n from floor(take) on reads alike
                    clip.append(int(math.floor(take)))
                    meta.append((r, bi, modes[mode]))
                need.append(c)
            need_of.append(None if blocked else tuple(sorted(set(need))))

        def inc(g: Fish) -> tuple:
            return tuple(c for c, (r, bi, f) in enumerate(meta)
                         if self.counts(r, g, f) and (self._band(r, g[2]) or (None,))[0] == bi)

        state0 = [0] * len(clip)
        for g in base:
            for c in inc(g):
                state0[c] = min(clip[c], state0[c] + 1)
        classes = sorted({(n, inc(f)) for f, n in zip(cands, need_of) if n is not None})
        memo: Dict[tuple, float] = {}

        def best(st: tuple) -> float:
            got = memo.get(st)
            if got is not None:
                return got
            top: float = 0
            for need, add in classes:
                if any(st[c] >= clip[c] for c in need):
                    continue
                nxt = list(st)
                for c in add:
                    nxt[c] = min(clip[c], nxt[c] + 1)
                nxt = tuple(nxt)
                if nxt == st:                  # keepable, and keeping it changes nothing: no limit
                    top = INF
                    break
                top = max(top, 1 + best(nxt))
            memo[st] = top
            return top

        depth = sum(clip) + 50
        if depth > sys.getrecursionlimit():
            sys.setrecursionlimit(depth + 1000)
        got = best(tuple(state0))
        return cap if got >= cap else int(got)

    def most_brute(self, cands: Sequence[Fish], base: Sequence[Fish] = (), cap: int = CAP) -> int:
        """THE REFERENCE for `most`: the same maximum by keeping fish one at a time over count
        vectors of classes, up to `cap` (the search stops there). Fish that every rule treats alike
        are one class (their count is what matters). Exponential in the claim — tests only."""
        base = tuple(base)
        if not self.legal(base):
            raise ValueError(f"card_check: the base basket {base} is not legal")
        # classes: fish every rule checks and counts alike, in the same band
        sig: Dict[tuple, Fish] = {}
        for f in cands:
            away = ("", "", 0.0, "stream")
            key = tuple((self.checks(r, f), self.counts(r, f), self.counts(r, f, away),
                         (self._band(r, f[2]) or (None,))[0]) for r in self.rules)
            sig.setdefault(key, f)
        reps = list(sig.values())

        @lru_cache(maxsize=None)
        def best(counts: Tuple[int, ...]) -> int:
            seq = list(base) + [f for f, c in zip(reps, counts) for _ in range(c)]
            if sum(counts) >= cap:
                return 0
            top, room = 0, cap - sum(counts)
            for i, f in enumerate(reps):
                if self.can_keep(seq, f):
                    nxt = counts[:i] + (counts[i] + 1,) + counts[i + 1:]
                    top = max(top, 1 + best(nxt))
                    if top >= room:
                        break
            return top
        return best(tuple(0 for _ in reps))
