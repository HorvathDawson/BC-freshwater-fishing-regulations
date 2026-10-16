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
  no two card lines state the same cap.

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
        sp = [S for k in b["kinds"] for S in k["species"]]
        ok, got = proved(b["cap"], E.fish(sp))
        if not ok:
            probs.append(f"{where}: bucket {b['title']!r} says {b['cap']}, its kinds together keep {got}")
        for k in b["kinds"]:
            ok, got = proved(k["n"], E.fish(k["species"]))
            if not ok:
                probs.append(f"{where}: kind {k['species']} says {k['n']}, alone it keeps {got}")
            for S in k["species"]:
                ok, one = proved(k["n"], E.fish([S]))
                if not ok:
                    probs.append(f"{where}: kind {k['species']} says {k['n']}, {S} alone keeps {one}")
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
    seen: Dict[str, str] = {}
    for s in g.get("states") or []:
        if s["r"] in seen:
            probs.append(f"{where}: the cap {s['r']} is stated twice ({seen[s['r']]}; {s['line']})")
        seen[s["r"]] = s["line"]
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
    g["buckets"] = [{**b, "by": r(b["by"])} for b in g["buckets"]]
    g["worked"] = [{**w, "share": r(w["share"])} for w in g["worked"]]
    g["states"] = [{**x, "r": r(x["r"])} for x in g["states"]]
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
