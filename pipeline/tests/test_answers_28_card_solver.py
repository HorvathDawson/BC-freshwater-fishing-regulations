"""ANSWERS 2.8 — the card gate's fast maxima (`Engine.most`, over counter states) equal the reference
search (`Engine.most_brute`, keeping fish one at a time over count vectors).

NO DATA: the fixture's cards (every query the gate asks, its mutations included) and random small cap
systems (fake rules and verdicts — pools, sub-caps, size bands, gates, origins, kinds of water, lifted and
displaced rules, zone and water rules — with a fixed seed), plus a 15-a-day pool the old search took
seconds on (the whitefish group that made the full gate too slow).
"""
from __future__ import annotations

import copy
import random
import time
from types import SimpleNamespace

from pipeline.deliver.answers import card_check as CC
from pipeline.tests import answers28_fixture as F

DATES = ((10, 10), (7, 15), (5, 15), (6, 15))


def _both(monkeypatch, calls):
    fast = CC.Engine.most

    def most(self, cands, base=(), cap=CC.CAP):
        a = fast(self, cands, base, cap)
        b = self.most_brute(cands, base, cap)
        calls.append((a, b, len(cands), len(base), cap))
        return a
    monkeypatch.setattr(CC.Engine, "most", most)


def test_fast_equals_brute_on_every_fixture_query(monkeypatch):
    calls: list = []
    _both(monkeypatch, calls)
    for w in (F.VEDDER, F.STREAM, F.LAKE):
        for md in DATES:
            out, P = F.card(w, md)
            assert CC.check_card(P, out) == [], (w, md)
    # the mutations' queries too (their verdicts differ: the fast search must find the same breaks)
    out, P = F.card(F.VEDDER, (10, 10))
    lad = copy.deepcopy(P.ladder)
    for o in ("none", "hatchery"):
        lad["RB"][o][F.SHARE] = ["speaks", None, None, None]
    assert CC.check_card(F.part(F.VEDDER, (10, 10), lad), out)
    assert len(calls) > 100
    bad = [c for c in calls if c[0] != c[1]]
    assert not bad, bad[:5]


def test_fast_equals_brute_uncapped_on_the_fixture():
    """Past the claims: the true maxima (cap well above every take) on every place mix and base."""
    for w in (F.VEDDER, F.STREAM, F.LAKE):
        out, P = F.card(w, (10, 10))
        g = F.trout_group(out)
        E = CC.Engine(P, g["pool"])
        for places in (("here",), ("stream",), ("lake",), CC.PLACES):
            for S in [*g["kinds"], None]:
                cands = E.fish(g["kinds"] if S is None else [S], places)
                assert E.most(cands, cap=7) == E.most_brute(cands, cap=7), (w, places, S)


# -- random cap systems ----------------------------------------------------------------------------

SPECIES = ("AA", "BB", "CC")
KINDS = ("pool", "pool", "subcap", "sizecap", "size", "gate")
STATES = ("speaks", "speaks", "speaks", "lifted", "lifted", "displaced", None)


def _rule(rng, i):
    k = rng.choice(KINDS)
    x = {"take": rng.choice((0, 1, 1, 2, 2, 3)) if k != "gate" else 0}
    if k == "pool" and rng.random() < 0.15:
        x["unlimited"] = True
    if k in ("sizecap", "size") or rng.random() < 0.2:
        x["lengths"] = rng.choice(([{"min_cm": 50}], [{"max_cm": 30, "take": 0}],
                                   [{"min_cm": 50, "take": 1}], [{"max_cm": 40}],
                                   [{"min_cm": 30, "max_cm": 50, "take": 2}],
                                   [{"max_cm": 30}, {"min_cm": 30, "take": 1}],
                                   [{"max_cm": 50, "take": 3}, {"min_cm": 50, "take": 1}]))
        if rng.random() < 0.2:
            x["period"] = "annual"             # a band no range covers then takes nothing
    if rng.random() < 0.3:
        x["origin"] = rng.choice(("hatchery", "wild"))
    if rng.random() < 0.35:
        x["water"] = rng.choice(("stream", "lake"))
    sp = set(rng.sample(SPECIES, rng.randint(1, len(SPECIES))))
    zone = rng.random() < 0.65
    return SimpleNamespace(key=f"r{i}", x=x, f=x, k=k, type="retention_limit", dimension="daily",
                           entry_id=("z1:t" if zone else "w9:t") + str(i), species=sp, except_=set())


def _system(rng):
    rules = [_rule(rng, i) for i in range(rng.randint(2, 5))]
    ladder = {S: {o: {r.key: [rng.choice(STATES)] for r in rules} for o in ("hatchery", "wild")}
              for S in SPECIES}
    return SimpleNamespace(cands=rules, by_key={r.key: r for r in rules}, md=(6, 15), at=None,
                           ladder=ladder, kind=rng.choice(("stream", "lake")), steelhead_water=False)


def test_fast_equals_brute_on_random_cap_systems():
    rng = random.Random(20261013)
    asked = nonzero = unlimited = 0
    for _ in range(400):
        P = _system(rng)
        E = CC.Engine(P, P.cands[0].key)
        unlift = frozenset(r.key for r in P.cands if rng.random() < 0.3)
        for eng in (E, CC.Engine(P, P.cands[0].key, unlift)):
            sp = rng.sample(SPECIES, rng.randint(1, 2))
            places = rng.choice((("here",), ("here", "stream"), ("stream", "lake"), CC.PLACES))
            cands = eng.fish(sp, places)
            # a legal base basket, kept one fish at a time
            base: list = []
            pool = eng.fish(SPECIES, CC.PLACES)
            for _k in range(rng.randint(0, 3)):
                ok = [f for f in pool if eng.can_keep(base, f)]
                if ok:
                    base.append(rng.choice(ok))
            cap = rng.randint(1, 6)
            a, b = eng.most(cands, base, cap), eng.most_brute(cands, base, cap)
            assert a == b, (a, b, [(r.k, r.f, r.species, r.entry_id) for r in P.cands], P.ladder,
                            P.kind, cands, base, cap)
            asked += 1
            nonzero += a > 0
            unlimited += a == cap
    # the systems exercise the solver: some keep nothing, some reach the cap, most lie between
    assert asked == 800 and nonzero > 200 and unlimited > 50, (asked, nonzero, unlimited)


def test_a_15_a_day_pool_is_fast():
    """The whitefish shape: 15 a day over two species, a wild sub-cap, a size band, a stream share one
    origin is lifted out of, and a water's own 8. Here and elsewhere together the reference search
    took 303 s for this (15, found by both); the counter states take well under a second."""
    specs = (("pool", {"take": 15}, "z3:a"), ("subcap", {"take": 10, "origin": "wild"}, "z3:b"),
             ("sizecap", {"take": 15, "lengths": [{"max_cm": 30}, {"min_cm": 30, "take": 5}]}, "z3:c"),
             ("subcap", {"take": 4, "water": "stream"}, "z3:d"), ("pool", {"take": 8}, "w9:e"))
    rules = [SimpleNamespace(key=f"r{i}", x=x, f=x, k=k, type="retention_limit", dimension="daily",
                             entry_id=e, species={"MW", "LW"}, except_=set())
             for i, (k, x, e) in enumerate(specs)]
    lad = {S: {o: {r.key: ["speaks"] for r in rules} for o in ("hatchery", "wild")} for S in ("MW", "LW")}
    lad["MW"]["hatchery"]["r3"] = ["lifted"]
    P = SimpleNamespace(cands=rules, by_key={r.key: r for r in rules}, md=(6, 15), at=None, ladder=lad,
                        kind="stream", steelhead_water=False)
    E = CC.Engine(P, "r0")
    t = time.perf_counter()
    assert E.most(E.fish(["MW", "LW"], CC.PLACES), cap=16) == 15
    assert E.most(E.fish(["MW", "LW"], CC.PLACES), cap=CC.CAP) == 15
    here = E.fish(["MW", "LW"])
    assert E.most(here, cap=16) == E.most_brute(here, cap=16) == 8
    assert time.perf_counter() - t < 2.0
