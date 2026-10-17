"""THE CARD GATE (answers 2.8, ROUND9 A5/R6) — every number on a quota card, proved by keeping fish.

The card (`card.py`, on `rows[].group`) is WORDED from the rows' arrangement (the decided answer per fish,
the row items, the group's sub-caps). This module never reads that arrangement: it keeps baskets of fish
against the RULES and the reader's VERDICT per fish and origin (`Part.verdict`: which rule speaks, which a
water's rule lifted, which was displaced) and asks, by search, how many fish a basket can hold. The gate
then holds every claim of the card against those maxima:

  each kind row's number   = the most of that kind alone, here (and of each of its species alone);
  each bucket's cap        = the most of its kinds together, here;
  the headline             = the most of every kind together, here, WITH NO LIFT (lifted rules checked);
  the total                = a zone's (B.C.'s): the most of every kind together, here, on other
                             streams and on lakes; a water's (an area's) own: the most here, and one
                             fewer after a fish kept elsewhere (rows F9) — past it the zone's rules
                             govern the other waters;
  each worked count        = what is left, here / on another stream / on a lake, after keeping that many
                             of the lifted kind here (RULINGS Q23b);
  no two card lines state the same cap;
  each fish the card says nobody may keep here (`not_kept`, ROUND13 S): none may be kept.

HOW A FISH IS KEPT (`Engine.can_keep`): a fish (species, origin, length, place) — place "here", or
"stream" / "lake" for another water of that kind in the region — may be kept after the fish already in
the basket when every rule that CHECKS it allows one more:
  * here, the rules the reader says SPEAK for that fish and origin; a rule a water's own rule LIFTED for
    it does not check it (RULINGS Q23b: "a kind the water lifts may go past it ON THAT WATER");
  * elsewhere, the zone's and B.C.'s rules for that kind of water (no water's lift reaches another water);
  * a gate (take 0) or a size band of take 0 at the fish's length: never; a count (a pool, a sub-cap, a
    size clause's band): the fish it COUNTS already in the basket must be fewer than its take.
A rule COUNTS every fish it covers (species, origin, water kind, band) that it speaks for, and, for a
fish kept on ANOTHER water, every fish a water's rule lifted out of it too — RULINGS Q23b (refined by the
owner 2026-10-12): on the Vedder up to 2 non-lifted trout/char from streams AND up to 4 hatchery rainbows,
4 in all, in any order; on every other stream the tally counts every trout/char kept from any stream,
the Vedder's rainbows included; lakes face only the total — and, for a water's own count, the same fish
kept elsewhere too (rows F9). Across waters order matters (rainbows kept here first use the tally another
stream needs), so a basket is a SEQUENCE; the maxima search every order.

HOW THE MAXIMA ARE FOUND (`Engine.most`): `can_keep` reads a basket only through its COUNTERS — for each
rule, band and kind of place asked about (here / elsewhere), how many kept fish that rule counts — and only
asks whether a counter is below the rule's take. So the search runs over counter states, each counter
clipped at its take (past it, a counter refuses the same fish whatever its value): every fish of a class
needs some counters below their take and adds one to others. A keepable fish that moves no counter can be
kept for ever (the claim of no limit); otherwise every fish kept moves the state up, so the states are a
finite DAG searched once each, memoised. Every order is still searched — the state forgets only what no
rule can read. `most_brute` is the old search over count vectors of fish (the reference; the no-data
tests hold the two equal on the fixture and on random cap systems).
"""
from __future__ import annotations

import math
import sys
import time
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver.bundle import read

INF = math.inf
PLACES = ("here", "stream", "lake")
#: "no daily limit" is proved by keeping this many
CAP = 40
#: the kinds of rule that limit keeping (`display.kind_of`)
COUNTING = ("pool", "subcap", "sizecap", "size", "gate")

Fish = Tuple[str, str, float, str]            # (species, origin, length cm, place)


def _covers(r, S: str) -> bool:
    return S in r.species and S not in r.except_


def _bands(r) -> List[Tuple[float, float, Optional[float]]]:
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
        self.zone = [r for r in self.rules if r.entry_id.startswith("z")
                     and read.in_force(r.f.get("when"), P.md, P.at) != "no"]
        self._zone = {id(r) for r in self.zone}
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
        k = (r.key, S, o)
        if k not in self._state:
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
        return id(r) in self._zone or (not r.entry_id.startswith("z")
                                  and self.state(r, g[0], g[1]) in ("speaks", "lifted"))

    def _band(self, r, L: float):
        """(band index, take) of the length, or None where no band of the rule holds it."""
        bs = self._bands_of.get(id(r))
        if bs is None:
            bs = self._bands_of[id(r)] = _bands(r)
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
            for a, b, _t in (self._bands_of.get(id(r)) or _bands(r)):
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


# --------------------------------------------------------------------------------------------
# The gate
# --------------------------------------------------------------------------------------------

def _claim(v) -> Optional[float]:
    return None if v in ("unlimited", None) else v


def check_group(P, g: dict) -> List[str]:
    """Every claim of one group's card, against the engine. Returns the problems (empty: proved)."""
    probs: List[str] = []
    where = f"total {g['pool']}"
    E = Engine(P, g["pool"])
    kinds = list(g["kinds"])

    def proved(claim, cands, eng=E, base=()) -> Tuple[bool, int]:
        """(the most keepable equals the claim, the most found up to claim + 1); a claim of no
        limit (None) is proved by keeping CAP."""
        c = _claim(claim)
        got = eng.most(cands, base, cap=CAP if c is None else int(c) + 1)
        return (got >= CAP if c is None else got == c), got

    for b in g.get("buckets") or []:
        rows_ = list(b["kinds"]) + [k for s in b.get("groups") or [] for k in s["kinds"]]
        sp = [S for k in rows_ for S in k["species"]]
        ok, got = proved(b["cap"], E.fish(sp))
        if not ok:
            probs.append(f"{where}: bucket {b['title']!r} says {b['cap']}, its kinds together keep {got}")
        # ROUND14 3: a nested group — its kinds together keep exactly its number
        for s in b.get("groups") or []:
            ok, got = proved(s["n"], E.fish([S for k in s["kinds"] for S in k["species"]]))
            if not ok:
                probs.append(f"{where}: bucket {b['title']!r} group {s['says']!r} says {s['n']}, its kinds "
                             f"together keep {got}")
        for k in rows_:
            ok, got = proved(k["n"], E.fish(k["species"]))
            if not ok:
                probs.append(f"{where}: kind {k['species']} says {k['n']}, alone it keeps {got}")
            for S in k["species"]:
                ok, one = proved(k["n"], E.fish([S]))
                if not ok:
                    probs.append(f"{where}: kind {k['species']} says {k['n']}, {S} alone keeps {one}")
        # ROUND11 A1: a bucket's size note — at most `n` of its kinds over the size together, and each
        # kind it says does not count still keepable over the size after those `n`
        # ROUND12 3: a count note (over_cm 0: any length), and each nested cap of a note (`then`)
        for nt in b.get("notes") or []:
            cm = nt["over_cm"]
            over = [f for f in E.fish(nt["kinds"]) if f[2] > cm]
            ok, got = proved(nt["n"], over)
            if not ok:
                probs.append(f"{where}: bucket {b['title']!r} note says {nt['n']} over {cm} cm, its kinds "
                             f"keep {got} over it")
            for t in nt.get("then") or []:
                ok, got = proved(t["n"], [f for f in E.fish(t["kinds"]) if f[2] > t["over_cm"]])
                if not ok:
                    probs.append(f"{where}: bucket {b['title']!r} note says {t['n']} over {t['over_cm']} cm "
                                 f"(nested), its kinds keep {got} over it")
            if nt["not_counting"]:
                base: List[Fish] = []
                for f in over:
                    while len(base) < nt["n"] and E.can_keep(base, f):
                        base.append(f)
                aside = [f for f in E.fish(nt["not_counting"]) if f[2] > cm]
                if len(base) < nt["n"] or not aside or E.most(aside, base, cap=1) < 1:
                    probs.append(f"{where}: bucket {b['title']!r} note says {nt['not_counting']} do not "
                                 f"count toward its {nt['n']} over {cm} cm, but none may be kept after them")
        # ROUND15 1: a size group — at most `n` of its kinds over the size together (each nested cap too),
        # EXACTLY the kinds it binds: each it names may be kept over the size here, and no other kind of
        # the card the cap's rule decides over the size goes unnamed; the kinds it says do not count
        # still keepable over the size after those `n`
        for sz in b.get("sizes") or []:
            cm = sz["over_cm"]
            over = [f for f in E.fish(sz["species"]) if f[2] > cm]
            ok, got = proved(sz["n"], over)
            if not ok:
                probs.append(f"{where}: size group {sz['says']!r} says {sz['n']} over {cm} cm, its kinds keep {got}")
            for t in sz.get("then") or []:
                ok, got = proved(t["n"], [f for f in E.fish(t["species"]) if f[2] > t["over_cm"]])
                if not ok:
                    probs.append(f"{where}: size group {sz['says']!r} says {t['n']} over {t['over_cm']} cm "
                                 f"(nested), its kinds keep {got}")
            for S in sz["species"]:
                if not [f for f in over if f[0] == S]:
                    probs.append(f"{where}: size group {sz['says']!r} names {S}, which cannot be kept over {cm} cm here")
            r = E.P.by_key.get(sz["by"])
            if r is not None:
                for S in kinds:
                    if S in sz["species"] or S in sz["not_counting"]:
                        continue
                    if any(E.checks(r, f) and ((E._band(r, f[2]) or (0, None))[1] or 0) > 0
                           for f in E.fish([S]) if f[2] > cm):
                        probs.append(f"{where}: size group {sz['says']!r} binds {S} over {cm} cm but does not name it")
            if sz["not_counting"]:
                base = []
                for f in over:
                    while len(base) < sz["n"] and E.can_keep(base, f):
                        base.append(f)
                aside = [f for f in E.fish(sz["not_counting"]) if f[2] > cm]
                if len(base) < sz["n"] or not aside or E.most(aside, base, cap=1) < 1:
                    probs.append(f"{where}: size group {sz['says']!r} says {sz['not_counting']} do not count, "
                                 f"but none may be kept after its {sz['n']}")
    # ROUND13 S: a fish the card says nobody may keep here (closed or released) — none may be kept
    for x in g.get("not_kept") or []:
        got = E.most(E.fish(x["species"]), cap=1)
        if got:
            probs.append(f"{where}: not_kept says {x['species']} {x['status']}, but {got} may be kept here")
    head = g.get("headline")
    if head is not None:
        no_lift = Engine(P, g["pool"], unlift=frozenset(s["r"] for s in g["subcaps"] if s["lifted"]))
        ok, got = proved(head["n"], no_lift.fish(kinds), no_lift)
        if not ok:
            probs.append(f"{where}: headline says {head['n']}, with no lift {got} may be kept here")
    if E.T.entry_id.startswith("z"):
        # the zone's (or B.C.'s) total holds on every water of the region: here and elsewhere together
        ok, got = proved(g["total"], E.fish(kinds, PLACES))
        if not ok:
            probs.append(f"{where}: the total says {g['total']}, {got} may be kept here and elsewhere")
    else:
        # a water's (an area's) own number limits what is kept HERE; the fish kept elsewhere today
        # count toward it (rows F9), but past it the region's rules govern the other waters — keep 2
        # on a lake whose quota is 2, and the region's 5 still allows 3 on another lake
        ok, got = proved(g["total"], E.fish(kinds))
        if not ok:
            probs.append(f"{where}: the total says {g['total']}, {got} may be kept here")
        c = _claim(g["total"])
        # a fish kept elsewhere that the total counts, in a band of it that has a number
        away = next((f for f in E.fish(kinds, ("lake", "stream"))
                     if E.counts(E.T, f, f[:3] + ("here",))
                     and ((E._band(E.T, f[2]) or (0, None))[1] or 0) > 0), None) if c else None
        if away is not None:
            after = E.most(E.fish(kinds), (away,), cap=int(c) + 1)
            if after > c - 1:
                probs.append(f"{where}: the total says {g['total']}, but after 1 kept elsewhere "
                             f"{after} more may be kept here (fish kept elsewhere count toward it)")
    other = "stream" if P.kind == "stream" else "lake"
    away = "lake" if P.kind == "stream" else "stream"
    for w in g.get("worked") or []:
        if w.get("at", "here") != "here":
            probs += _check_elsewhere_line(E, g, w, where, kinds, other, away)
            continue
        share = P.by_key[w["share"]]
        lifted = [f for f in E.fish(w["species"], origins=(w["origin"],))
                  if E.state(share, f[0], f[1]) == "lifted"]
        if not lifted:
            probs.append(f"{where}: worked count names {w['species']}, which no lift lets past the share")
            continue
        base = [lifted[0]] * w["kept"]
        if not E.legal(base):
            probs.append(f"{where}: worked count keeps {w['kept']} {w['species']} here, which is not legal")
            continue
        for place, key in (("here", "here"), (other, "other"), (away, "away")):
            ok, got = proved(w[key], E.fish(kinds, (place,)), base=base)
            if not ok:
                probs.append(f"{where}: worked count {w['kept']}: says {w[key]} more ({key}), "
                             f"the rules allow {got}")
    if g.get("caps") is not None:
        probs += check_caps(P, g)
    if g.get("tree") is not None:
        probs += check_tree(P, g)
    seen: Dict[str, str] = {}
    for s in g.get("states") or []:
        if s["r"] in seen:
            probs.append(f"{where}: the cap {s['r']} is stated twice ({seen[s['r']]}; {s['line']})")
        seen[s["r"]] = s["line"]
    return probs


def _numbers(text: str, strip: Sequence[str] = ()) -> List[float]:
    """The numbers a sentence states (a water's or region's name taken out first: "Region 2")."""
    import re
    for s in strip:
        if s:
            text = text.replace(s, " ")
    text = re.sub(r"\b(Region|Zone|MUs?|Management Unit)\s+[0-9A-Z][-0-9A-Z]*", " ", text)
    return [float(x) for x in re.findall(r"\d+(?:\.\d+)?", text)]


def check_tree(P, g: dict) -> List[str]:
    """ROUND18 1 — the card's TREE (`group.tree`, `tree.py`) against the engine, never against the buckets it was
    read from:
      * every kind of the group sits in the tree exactly once, under tiers only;
      * the total tier states the group's total (proved by `check_group`) and names its pool; every other tier's
        number is the most of the kinds under it together, here; a size tier's is the most of its cap's kinds
        over its size (each nested cap too), every kind under it is kept only over that size, and its `extra`
        (RULINGS Q16) — after its number, exactly that many more of the extra kinds over the size;
      * every kind's number is the most of it alone (each species too), its `sizes` exactly the lengths it may
        be kept at here (each range holds a keepable length, each keepable length is in a range);
      * each `also_counts`: a size cap — at most its number of the kind over its size, and none after the cap's
        number of the other kinds it names over the size (where that many may be kept); a count — at most its
        number of the kind and the other kinds together;
      * every number in a tier's `says`, a kind's `says` / `sentence` and the header is one those fields prove
        (or a size in inches)."""
    from pipeline.deliver.answers import common
    from pipeline.deliver.answers.tree import walk
    probs: List[str] = []
    where = f"total {g['pool']} tree"
    tree = g.get("tree")
    if not tree:
        return [f"{where}: no tree"]
    E = Engine(P, g["pool"])
    names = [P.by_key[r].x.get("entry_name") for r in {g["pool"], *(s["r"] for s in g["subcaps"])}
             if r in P.by_key]
    seen: List[str] = []

    def kinds_under(t: dict) -> List[str]:
        return [S for x, _p in walk(t["children"]) if x["node"] == "kind" for S in x["species"]]

    def most(fish, claim) -> int:
        return E.most(fish, cap=CAP if claim is None else int(claim) + 1)

    def eq(got, claim) -> bool:
        return got >= CAP if claim is None else got == claim

    def size_words(cms) -> set:
        out = set()
        for v in cms:
            if v is not None:
                out |= {float(v), float(common.inches(v))}
        return out

    def words_ok(text: str, allowed: set, what: str, also: Sequence[str] = ()) -> None:
        bad = [x for x in _numbers(text, [*also, *names]) if x not in allowed]
        if bad:
            probs.append(f"{where}: {what} {text!r} states {bad}, which nothing proves here")

    if len(tree) != 1 or tree[0]["node"] != "tier" or tree[0]["tier"] != "total":
        probs.append(f"{where}: the tree is one total tier")
    for x, parent in walk(tree):
        if x["node"] == "kind":
            if parent is None:
                probs.append(f"{where}: kind {x['species']} sits outside every tier")
            seen += x["species"]
            ok_n = eq(most(E.fish(x["species"]), x["n"]), x["n"])
            if not ok_n or any(not eq(most(E.fish([S]), x["n"]), x["n"]) for S in x["species"]):
                probs.append(f"{where}: kind {x['species']} says {x['n']}, which is not the most of it alone here")
            rng = [(a or 0, INF if b is None else b) for a, b in x["sizes"]["cm"]]
            if [[common.inches(a), None if b == INF else common.inches(b)] for a, b in rng] != \
                    [list(v) for v in x["sizes"]["in"]]:
                probs.append(f"{where}: kind {x['species']} sizes in inches are not its cm")
            for S in x["species"]:
                Ls = sorted({f[2] for f in E.fish([S])})
                if any(not any(a <= L <= b for a, b in rng) for L in Ls) or \
                        any(not any(a <= L <= b for L in Ls) for a, b in rng):
                    probs.append(f"{where}: kind {x['species']} sizes {x['sizes']['cm']} are not the lengths {S} "
                                 f"may be kept at here")
            allowed = {float(v) for v in (x["n"], parent["n"] if parent else None) if v is not None}
            allowed |= size_words([v for r in x["sizes"]["cm"] for v in r])
            if parent is not None and parent.get("over_cm") is not None:
                allowed |= size_words([parent["over_cm"]])
            for ac in x["also_counts"]:
                cm = ac["over_cm"] or 0
                others = [S for S in ac["species"] if S not in x["species"]]
                mine = [S for S in x["species"] if S in ac["species"]]
                allowed |= {float(ac["n"])} | size_words([cm] if cm else []) | \
                    {float(t["n"]) for t in ac["then"]} | size_words([t["over_cm"] for t in ac["then"]])
                kn = min([INF if x["n"] is None else x["n"]]
                         + [a["n"] for a in x["also_counts"] if a is not ac and a["over_cm"] and cm
                            and a["over_cm"] <= cm and (a["over_cm"], a["n"]) < (cm, ac["n"])])
                if cm:
                    over = [f for f in E.fish(mine) if f[2] > cm]
                    got = most(over, ac["n"])
                    if got > ac["n"] or (kn > ac["n"] and got != ac["n"]):
                        probs.append(f"{where}: kind {x['species']} says {ac['n']} over {cm} cm, {got} may be "
                                     f"kept over it")
                    if others and ac["n"] > 1 and \
                            most([f for f in E.fish(mine + others) if f[2] > cm],
                                 ac["n"]) > ac["n"]:
                        probs.append(f"{where}: kind {x['species']} says {ac['n']} over {cm} cm with {others}, but "
                                     f"more may be kept over it together")
                    for t in ac["then"]:
                        if most([f for f in over if f[2] > t["over_cm"]], t["n"]) > t["n"]:
                            probs.append(f"{where}: kind {x['species']} also counts toward {t['n']} over "
                                         f"{t['over_cm']} cm, but more may be kept over it")
                    base: List[Fish] = []
                    for f in [f for f in E.fish(others) if f[2] > cm]:
                        while len(base) < ac["n"] and E.can_keep(base, f):
                            base.append(f)
                    if others and ac["n"] == 1 and (len(base) < 1 or E.most(over, base, cap=1) > 0):
                        probs.append(f"{where}: kind {x['species']} says none over {cm} cm after one of "
                                     f"{others} over it, which the rules do not hold")
                else:
                    if most(E.fish(mine + others), ac["n"]) > ac["n"]:
                        probs.append(f"{where}: kind {x['species']} also counts toward {ac['n']} with {others}, "
                                     f"but more may be kept together")
            for key in ("says", "sentence"):
                words_ok(x[key], allowed, f"kind {x['species']} {key}")
            continue
        under = kinds_under(x)
        allowed = {float(x["n"])} if x["n"] is not None else set()
        if x["tier"] == "total":
            if x["n"] != _claim(g["total"]) or x["by"] != g["pool"]:
                probs.append(f"{where}: the total tier says {x['n']} by {x['by']}, the group's total is "
                             f"{g['total']} by {g['pool']}")
        elif x["tier"] == "size":
            cm = x["over_cm"]
            allowed |= size_words([cm] + [t["over_cm"] for t in x["then"]]) | {float(t["n"]) for t in x["then"]}
            extra = x.get("extra")
            capped = [S for S in dict.fromkeys(x["species"] + under) if not extra or S not in extra["species"]]
            over = [f for f in E.fish(capped) if f[2] > cm]
            if not eq(most(over, x["n"]), x["n"]):
                probs.append(f"{where}: size tier {x['says']!r} says {x['n']}, its kinds keep "
                             f"{most(over, x['n'])} over {cm} cm")
            for t in x["then"]:
                if not eq(most([f for f in over if f[2] > t["over_cm"]], t["n"]), t["n"]):
                    probs.append(f"{where}: size tier {x['says']!r} says {t['n']} over {t['over_cm']} cm (nested)")
            if any(f[2] <= cm for f in E.fish(under)):
                probs.append(f"{where}: size tier {x['says']!r} holds a kind that may be kept at {cm} cm or less")
            if extra:
                allowed.add(float(extra["n"]))
                base = []
                for f in over:
                    while len(base) < x["n"] and E.can_keep(base, f):
                        base.append(f)
                aside = [f for f in E.fish(extra["species"]) if f[2] > cm]
                if len(base) < x["n"] or E.most(aside, base, cap=int(extra["n"]) + 1) != extra["n"] \
                        or not eq(most(over + aside, x["n"] + extra["n"]), x["n"] + extra["n"]):
                    probs.append(f"{where}: size tier {x['says']!r} says {extra['n']} more may be "
                                 f"{extra['species']}, which the rules do not allow")
        elif not eq(most(E.fish(under), x["n"]), x["n"]):
            probs.append(f"{where}: {x['tier']} tier {x['says']!r} says {x['n']}, its kinds together keep "
                         f"{most(E.fish(under), x['n'])} here")
        words_ok(x["says"], allowed, f"{x['tier']} tier", [x.get("whose") or "",
                                                          (P.by_key[x["by"]].x.get("entry_name") or "")
                                                          if x["by"] in P.by_key else ""])
    if sorted(seen) != sorted(g["kinds"]):
        probs.append(f"{where}: the tree holds {sorted(seen)}, the group's kinds are {sorted(g['kinds'])}")
    h = g.get("header") or {}
    head = g.get("headline") or {}
    words_ok(h.get("says") or "", {float(v) for v in (head.get("n"), _claim(g["total"])) if v is not None},
             "header")
    return probs


#: ROUND16 1: random baskets per group the cap-system proof keeps (and two per cap at its edge)
BASKETS = 48
SEED = 20261016


def check_caps(P, g: dict, baskets: int = BASKETS) -> List[str]:
    """ROUND16 1 — the group's TABLE and CAP SYSTEM against the engine, never this module's reading of them:
      * every table cell = the engine's most of that fish alone at that size here (each origin where the
        cell names one), a cell of 0 says why, the bands cover every length once;
      * the caps accept EXACTLY the baskets the engine accepts — seeded random baskets of the kinds kept
        here, on another stream and on another lake today (an origin-free kind as either origin, at a random
        length inside its band), and for each cap a basket filling it to its take and one past it. The
        engine keeps the fish kept elsewhere first (the table answers what may still be kept HERE). Each cap
        is tried at its edge three ways: its kinds kept here only, and twice anywhere it counts."""
    import random
    from pipeline.deliver.answers import caps as CP
    probs: List[str] = []
    where = f"total {g['pool']} caps"
    E = Engine(P, g["pool"])
    table, kinds, caps = g["table"], g["cap_kinds"], g["caps"]
    Ls = E.lengths()
    for S, bands in table["bands"].items():
        at = 0
        for bd, cell in zip(bands, table["cells"][S]):
            a, b = bd["from"] or 0, INF if bd["to"] is None else bd["to"]
            if a != at:
                probs.append(f"{where}: {S}'s bands leave a gap or overlap at {at} cm")
            at = b
            T = cell.get("as") or S

            def most(origins=("hatchery", "wild"), claim=None):
                cands = [f for f in E.fish([T], origins=origins) if a < f[2] < b]
                return E.most(cands, cap=CAP if claim is None else int(claim) + 1)
            got = most(claim=cell["n"])
            if (got < CAP if cell["n"] is None else got != cell["n"]):
                probs.append(f"{where}: {S} {bd['says']} says {cell['n']}, alone {got} may be kept here")
            if cell["n"] == 0 and not cell.get("why"):
                probs.append(f"{where}: {S} {bd['says']} keeps none and says no why")
            if cell.get("origin"):
                other = "wild" if cell["origin"] == "hatchery" else "hatchery"
                if most((other,), 0) or not most((cell["origin"],), 0):
                    probs.append(f"{where}: {S} {bd['says']} says {cell['origin']} only")
            for o, n in (cell.get("by_origin") or {}).items():
                got = most((o,), n)
                if (got < CAP if n is None else got != n):
                    probs.append(f"{where}: {S} {bd['says']} says {n} {o}, alone {got}")
        if at != INF:
            probs.append(f"{where}: {S}'s bands stop at {at} cm")
    rng = random.Random(f"{SEED}:{g['pool']}:{len(kinds)}")
    by_id = {k["id"]: k for k in kinds}

    def fish_of(kid: str, place: str) -> Fish:
        k = by_id[kid]
        a, b = k["from"] or 0, INF if k["to"] is None else k["to"]
        inside = [L for L in Ls if a < L < b] or [CP.rep(E, k, place)[2]]
        return (k["species"], k["origin"] or rng.choice(("hatchery", "wild")), rng.choice(inside), place)

    def run(basket: Dict[str, Dict[str, int]]) -> None:
        seq = [fish_of(k, p) for p in ("stream", "lake", "here") for k, n in sorted(basket.get(p, {}).items())
               for _ in range(n)]
        a, b = CP.accepts(caps, basket), E.accepts(seq)
        if a != b:
            probs.append(f"{where}: the caps {'accept' if a else 'refuse'} the basket {basket}, the engine "
                         f"{'accepts' if b else 'refuses'} it")
    ids = [k["id"] for k in kinds]
    if not ids:
        return probs
    # most kinds keep none somewhere (wild fish on a stream, a size band): draw mostly from the kind and place
    # pairs where one fish alone may be kept, so the baskets reach the counting caps
    alone = [(p, k) for p in PLACES for k in ids if E.accepts([fish_of(k, p)])] or \
        [(p, k) for p in PLACES for k in ids]
    for _ in range(baskets):
        bk: Dict[str, Dict[str, int]] = {}
        for _j in range(rng.randint(1, 5)):
            p, k = rng.choice(alone) if rng.random() < 0.9 else (rng.choice(PLACES), rng.choice(ids))
            bk.setdefault(p, {})[k] = bk.get(p, {}).get(k, 0) + 1
        run(bk)
    # the engine's own edges, never read from the caps: each rule band with a take, at its take and one past
    # it, drawn from the kinds it checks here and the kinds it counts (a missing cap is found here)
    for ri, r in enumerate(E.rules):
        bands_ = {}
        for p in PLACES:
            for k in ids:
                f = CP.rep(E, by_id[k], p)
                hit = E._band(r, f[2])
                if hit is None or hit[1] is None or r.k == "gate" or hit[1] == 0:
                    continue
                if E.checks(r, f) or E.counts(r, f, ("", "", 0.0, "here")) or E.counts(r, f, ("", "", 0.0, "stream")):
                    bands_.setdefault((hit[0], int(math.floor(hit[1]))), []).append((p, k, E.checks(r, f)))
        for (bi, t), slots in bands_.items():
            checked = [(p, k) for p, k, c in slots if c]
            if not checked:
                continue
            for n in (t, t + 1):
                for _rep in range(2):
                    bk = {}
                    p, k = rng.choice(checked)
                    bk.setdefault(p, {})[k] = 1
                    for _j in range(n - 1):
                        p, k, _c = rng.choice(slots)
                        bk.setdefault(p, {})[k] = bk.get(p, {}).get(k, 0) + 1
                    run(bk)
    for c in caps:
        every = [(p, k) for p in PLACES for k in c["kinds"][p]]
        here = [("here", k) for k in c["kinds"]["here"]]
        for slots, n in ((s_, n) for s_ in (here, every, every) if s_ for n in (c["take"], c["take"] + 1)):
            bk = {}
            for _j in range(n):
                p, k = rng.choice(slots)
                bk.setdefault(p, {})[k] = bk.get(p, {}).get(k, 0) + 1
            if c["if_kept_here"] and c["kinds"]["here"] and not any(bk.get("here", {}).values()):
                k = rng.choice(c["kinds"]["here"])
                bk.setdefault("here", {})[k] = bk.get("here", {}).get(k, 0) + 1
            run(bk)
    return probs


def _check_elsewhere_line(E: Engine, g: dict, w: dict, where: str, kinds: Sequence[str], other: str,
                          away: str) -> List[str]:
    """ROUND12 4: a line of the one list that starts from fish kept ON ANOTHER WATER (`at`: a stream or
    lake of the region; `over_cm`: fish over that size, and only those counted after): keep `kept` of
    the line's kind there, then the most of the card's kinds here (another water of this kind, the
    other kind) is `here` (`other`, `away`) — of the kinds `of` (None: every kind of the card) —
    exactly, or at most (`most`: "at_most", a ceiling the line states: "at most 2 over 50 cm a day in
    all")."""
    probs: List[str] = []
    cm = w.get("over_cm")
    if all(w.get(k) is None for k in ("here", "other", "away")):
        return []
    # the basket: `kept` fish of the line's kinds kept at that place ("elsewhere": another water of
    # this kind, else of the other), taken greedily in the line's order (its first kind first). Where
    # no such basket may be kept today (the region's streams closed on the date, Region 3 in January)
    # the example cannot happen and there is nothing to prove.
    base = None
    for p in ((other, away) if w["at"] == "elsewhere" else (w["at"],)):
        got: List[Fish] = []
        for f in [f for S in w["species"] for o in ("hatchery", "wild") for f in E.fish([S], (p,), (o,))
                  if cm is None or f[2] > cm]:
            while len(got) < w["kept"] and E.can_keep(got, f):
                got.append(f)
        if len(got) == w["kept"]:
            base = got
            break
    if base is None:
        return []
    for key, place in (("here", "here"), ("other", other), ("away", away)):
        claim = w.get(key)
        if claim is None:
            continue
        fish = [f for f in E.fish(w.get("of") or kinds, (place,)) if cm is None or f[2] > cm]
        got = E.most(fish, base, cap=int(claim) + 1)
        ok = got <= claim if w.get("most") == "at_most" else got == claim
        if not ok:
            probs.append(f"{where}: worked line {w['q']!r}: says {'at most ' if w.get('most') == 'at_most' else ''}"
                         f"{claim} more ({key}), the rules allow {got}")
    return probs


def check_card(P, out: dict) -> List[str]:
    """Every group of one card (`rows.produce`'s output, rules by id)."""
    probs = []
    for row in out["rows"]:
        g = row.get("group")
        if g is not None:
            probs += check_group(P, g)
    return probs


# --------------------------------------------------------------------------------------------
# Every section: the built rows frames, at sample dates (the CLI and the slow test)
# --------------------------------------------------------------------------------------------

#: the sample dates: winter, the Vedder's May fly-only month, June's closures, July's lifts, October
DATES = ((1, 15), (5, 15), (6, 15), (7, 15), (10, 10))


def _unref(v, ids):
    """A built group (rules by export index) with every rule named by its id again."""
    g = dict(v)
    r = ids.__getitem__
    g["pool"] = r(g["pool"])
    g["subcaps"] = [{**s, "r": r(s["r"]), "lifted": [{**L, "by": r(L["by"])} for L in s["lifted"]]}
                    for s in g["subcaps"]]
    g["lifts"] = [{**L, "by": r(L["by"]), "each": [{**e, "by": r(e["by"])} for e in L["each"]]}
                  for L in g["lifts"]]
    g["buckets"] = [{**b, "by": r(b["by"]),
                     "replaces": None if b.get("replaces") is None else
                     {**b["replaces"], "rules": [r(k) for k in b["replaces"]["rules"]]}} for b in g["buckets"]]
    g["not_kept"] = [{**x, "by": r(x["by"]), "rules": [r(k) for k in x.get("rules") or []]}
                     for x in g.get("not_kept") or []]
    g["buckets"] = [{**b, "groups": [{**s, "by": r(s["by"])} for s in b.get("groups") or []],
                     "sizes": [{**s, "by": r(s["by"])} for s in b.get("sizes") or []]}
                    for b in g["buckets"]]
    g["worked"] = [{**w, "share": None if w.get("share") is None else r(w["share"])} for w in g["worked"]]
    g["states"] = [{**x, "r": r(x["r"])} for x in g["states"]]
    if g.get("tree") is not None:
        from pipeline.deliver.answers.tree import map_rules
        g["tree"] = map_rules(g["tree"], r)
    if g.get("caps") is not None:
        g["caps"] = [{**c, "by": r(c["by"])} for c in g["caps"]]
    return g


def check_built(bundle: str, export_dir, items=None, dates=DATES, log=print) -> dict:
    """Build the rows for every part (or `items`) and prove every group of every card at `dates`:
    {checked, groups, problems [str]} — one check per distinct (rule key, kind, steelhead, date, the
    reader's verdicts, the card): rows are keyed by content and many sections share a key, so each
    distinct card is proved once on each sample date (the date picks the zone rules in force elsewhere)."""
    from pathlib import Path
    from pipeline.deliver.answers import answers as A, common as C, encode as E, rows as RW
    from pipeline.deliver.calendar import day_of
    data, guide = C.load_export(Path(export_dir))
    model = A.build(bundle, Path(export_dir), workers=1, items=items, sections=["rows"],
                    export=(data, guide), log=lambda *_: None)
    B = C.load(bundle)
    ids = data["rule_ids"]
    seen, probs, groups = set(), [], 0
    lad_json: Dict[int, str] = {}
    t0 = time.monotonic()
    for k, key in enumerate(model.keys):
        if k and k % 2000 == 0:
            log(f"card_check: {k}/{len(model.keys)} keys, {len(seen)} distinct cards, {groups} groups, "
                f"{len(probs)} problems, {time.monotonic() - t0:.0f} s")
        rk, kd = C.rule_key(key), C.key_dict(key)
        for md in dates:
            s = E.segment_index(model.segments[k], day_of(md))
            frame = model.sections["rows"][k][s]
            built = [r["group"] for r in frame["rows"] if r.get("group")]
            if not built:
                continue
            lad = model.sections["ladder"][k][s]
            lj = lad_json.get(id(lad))         # the model holds every frame: ids are stable here
            if lj is None:
                lj = lad_json[id(lad)] = json_key(lad)
            sig = (rk, kd["kind"], kd["steelhead"], md, lj, json_key(built))
            if sig in seen:
                continue
            seen.add(sig)
            # the engine reads no open-subject gate (protected species, salmon): no open states
            P = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"],
                        kd["steelhead"], lad, md, {})
            for g in built:
                groups += 1
                for p in check_group(P, _unref(g, ids)):
                    probs.append(f"key {k} ({md[0]:02d}-{md[1]:02d}): {p}")
    log(f"card_check: {groups} groups on {len(seen)} distinct cards, {len(probs)} problems "
        f"({time.monotonic() - t0:.0f} s)")
    return {"checked": len(seen), "groups": groups, "problems": probs}


def json_key(v) -> str:
    import json
    return json.dumps(v, sort_keys=True, default=str)


def main(argv=None) -> int:
    import argparse
    from pathlib import Path
    from pipeline.deliver.bundle import read as RD
    ap = argparse.ArgumentParser(description="answers 2.8: prove every number on every quota card")
    ap.add_argument("--bundle", default=RD.BUNDLE)
    ap.add_argument("--export-dir", default=str(Path(RD.BUNDLE).parent.parent / "regs"))
    ap.add_argument("--items", default=None, help="comma-separated item ids (default: every part)")
    a = ap.parse_args(argv)
    got = check_built(a.bundle, a.export_dir, a.items.split(",") if a.items else None)
    for p in got["problems"][:200]:
        print(p)
    return 1 if got["problems"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
