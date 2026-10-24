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
import time
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.common import entry_id as EID

# THE KEEPING ENGINE lives in `engine.py` (plan 2.20); its names are this gate's vocabulary too
from pipeline.deliver.answers.engine import CAP, INF, PLACES, Engine, Fish


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
    if EID.is_table(E.T.entry_id):
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
        # UR2 2: every number the line's words state ("If you keep 3 hatchery rainbow trout: 1 more fish of any
        # kind, …") is one the counts above prove — how many kept, and how many more here / elsewhere
        said = {float(v) for v in (w["kept"], w["here"], w["other"], w["away"]) if v is not None}
        bad = [x for x in _numbers(f"{w['q']}: {w['a']}") if x not in said]
        if bad:
            probs.append(f"{where}: worked line {w['q']!r}: {w['a']!r} states {bad}, which nothing proves here")
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
      * ROUND19 3 — THE PLACEMENT of every size tier: each kind under it may be kept over its size here; each
        of its caps (`by`, and the `extra`'s `by`) binds no kind OUTSIDE it over the size (the reader's verdict:
        the rule checks such a fish with a number); a kind sits under no tier holding fewer (its nearest COUNT
        tier — a size tier limits only what is over its size);
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
    par = {id(n): p for n, p in walk(tree) if p is not None}

    def above(n: dict) -> List[dict]:
        out = []
        while id(n) in par:
            n = par[id(n)]
            out.append(n)
        return out

    def counting(n: dict) -> Optional[dict]:
        """The nearest tier above `n` that counts (a size tier limits only what is over its size)."""
        return next((t for t in above(n) if t["tier"] != "size"), None)

    # ROUND19 3: every size tier's placement
    for t, _p in walk(tree):
        if t["node"] != "tier" or t["tier"] != "size":
            continue
        cm = t["over_cm"]
        inside = {id(y) for y, _q in walk(t["children"])}
        for y, _q in walk(t["children"]):
            if y["node"] == "kind" and any(not [f for f in E.fish([S]) if f[2] > cm] for S in y["species"]):
                probs.append(f"{where}: size tier {t['says']!r} holds {y['species']}, which cannot be kept over "
                             f"{cm} cm here")
        caps_ = [P.by_key[k] for k in (t["by"], (t.get("extra") or {}).get("by")) if k in P.by_key]
        for y, _q in walk(tree):
            if y["node"] != "kind" or id(y) in inside:
                continue
            for S in y["species"]:
                if any(E.checks(r, f) and ((E._band(r, f[2]) or (0, None))[1] or 0) > 0
                       for r in caps_ for f in E.fish([S]) if f[2] > cm):
                    probs.append(f"{where}: size tier {t['says']!r} caps {S} over {cm} cm, but {S} sits outside it")
    # ROUND19 3: a size cap said on the kinds (no tier) over more than one kind row must be one no tier can hold — the
    # rows it binds include part of a row, or the tightest tier around them holds, beside them, a kind that cannot be
    # kept over the size
    def passes(y: dict, cm) -> bool:
        return any(f[2] > cm for f in E.fish(y["species"]))
    kinds_all = [y for y, _q in walk(tree) if y["node"] == "kind"]
    said: set = set()
    for y in kinds_all:
        for ac in y["also_counts"]:
            cm = ac["over_cm"]
            if not cm or ac["tier"] is not None or (ac["by"], cm) in said:
                continue
            said.add((ac["by"], cm))
            rows = [z for z in kinds_all if set(z["species"]) & set(ac["species"]) and passes(z, cm)]
            if len(rows) < 2 or any(not set(z["species"]) <= set(ac["species"]) for z in rows):
                continue
            chains = [above(z)[::-1] for z in rows]
            box = None
            for level in zip(*chains):
                if any(t is not level[0] for t in level):
                    break
                box = level[0]
            ids = {id(z) for z in rows}
            spoilt = False
            for z in box["children"]:
                if z["node"] == "tier":
                    under = [q for q, _r in walk(z["children"]) if q["node"] == "kind"]
                    if any(id(q) in ids for q in under) and not all(id(q) in ids for q in under):
                        spoilt = True
            if not spoilt:
                probs.append(f"{where}: the cap {ac['by']} over {cm} cm is said on the kinds {[z['species'] for z in rows]}, "
                             f"but a size tier could hold them")
    # every size cap the card states (a bucket's size group) is in the tree — a size tier, or said on a kind — unless
    # a cap the tree says implies it (no larger size or number, every fish of it), or it binds one kind row whose own
    # number (proved below) is no more than its number: then it says nothing more
    said_caps = [(t["by"], t["over_cm"], t["n"], set(t["species"])) for t, _q in walk(tree)
                 if t["node"] == "tier" and t["tier"] == "size"] + \
        [(ac["by"], ac["over_cm"], ac["n"], set(ac["species"])) for y in kinds_all for ac in y["also_counts"]
         if ac["over_cm"]]
    for b in g.get("buckets") or []:
        for sz in b.get("sizes") or []:
            if any(k == sz["by"] for k, *_x in said_caps):
                continue
            if any(cm <= sz["over_cm"] and n <= sz["n"] and set(sz["species"]) <= sp for _k, cm, n, sp in said_caps):
                continue
            rows = [z for z in kinds_all if set(z["species"]) & set(sz["species"]) and passes(z, sz["over_cm"])]
            if len(rows) == 1 and rows[0]["n"] is not None and rows[0]["n"] <= sz["n"]:
                continue
            probs.append(f"{where}: the size cap {sz['says']!r} is in no size tier and on no kind")
    for x, parent in walk(tree):
        if x["node"] == "kind":
            up = counting(x)
            if parent is None or up is None:
                probs.append(f"{where}: kind {x['species']} sits outside every tier")
            # ROUND18 fix: a kind never keeps more than the count tier it sits in ("Of the 1 …: up to 2" contradicts
            # itself)
            elif up["n"] is not None and (x["n"] is None or x["n"] > up["n"]):
                probs.append(f"{where}: kind {x['species']} says {x['n']} under the tier {up['says']!r} "
                             f"({up['n']})")
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
            allowed = {float(v) for v in (x["n"], up["n"] if up else None) if v is not None}
            allowed |= size_words([v for r in x["sizes"]["cm"] for v in r])
            for t in above(x):
                if t["tier"] == "size":
                    ex = (t.get("extra") or {}).get("n") or 0
                    allowed |= size_words([t["over_cm"]] + [h["over_cm"] for h in t["then"]]) | \
                        {float(t["n"]), float(t["n"] + ex)} | {float(h["n"]) for h in t["then"]}
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
            # T128: a kind's line names the total it is part of ("Of your 2 trout and char a day in streams, …") — a
            # number of a total, share or own tier the kind sits in (each proved below); an own tier's name may print one
            allowed |= {float(t["n"]) for t in above(x) if t["tier"] in ("total", "share", "own") and t["n"] is not None}
            for key in ("says", "sentence"):
                whose = [t.get("whose") or "" for t in above(x)]
                words_ok(x[key], allowed, f"kind {x['species']} {key}", whose + [w[:1].lower() + w[1:] for w in whose])
            continue
        under = kinds_under(x)
        allowed = {float(x["n"])} if x["n"] is not None else set()
        up = counting(x)
        if up is not None and up["n"] is not None:
            allowed.add(float(up["n"]))                    # ROUND19 6: "(part of the 4)"
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
            if extra:
                allowed |= {float(extra["n"]), float(x["n"] + extra["n"])}
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
    # ROUND20 1: the header IS the root tier — it names the root and says its words, nothing else
    root = g["tree"][0] if g.get("tree") else None
    if root is not None and h.get("tier") is not None:
        if h["tier"] != root["id"]:
            probs.append(f"{where}: the header names tier {h['tier']!r}, the tree's root is {root['id']!r}")
        elif h.get("says") != root["says"]:
            probs.append(f"{where}: the header says {h.get('says')!r}, its tier {root['says']!r}")
    # ROUND20h 3: the possession example multiplies the group's TOTAL by its possession rule's own multiplier
    ps = g.get("possession")
    if ps is not None:
        R_ = P.by_key.get(ps["by"])
        if R_ is None or R_.f.get("per_daily") != ps["per_daily"]:
            probs.append(f"{where}: possession says ×{ps['per_daily']}, its rule {ps['by']} says otherwise")
        if _claim(g["total"]) != ps["of"] or ps["n"] != ps["of"] * ps["per_daily"]:
            probs.append(f"{where}: possession {ps['n']} is not {ps['per_daily']} × the total {g['total']}")
        words_ok(ps["example"], {float(ps["of"]), float(ps["n"]), float(ps["per_daily"]), float(ps["per_daily"] + 1)},
                 "possession")
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
    from pipeline.deliver.answers import common
    for S, bands in table["bands"].items():
        at, held = 0, None
        for bd, cell in zip(bands, table["cells"][S]):
            a, b = bd["from"] or 0, INF if bd["to"] is None else bd["to"]
            if a != at:
                probs.append(f"{where}: {S}'s bands leave a gap or overlap at {at} cm")
            # ROUND19 5, THE BOUNDARY STANDARD: the cut belongs to exactly one of the bands meeting at it, and each
            # band's words say the ends it holds ("X or less" / "X or longer" hold X; "under X" / "over X" do not)
            if held is not None and held == bd["from_in"]:
                probs.append(f"{where}: {S}'s cut at {at} cm belongs to {'both' if held else 'neither'} of its bands")
            if bd["says"] != common.band_words(a, b, bd["from_in"], bd["to_in"]):
                probs.append(f"{where}: {S} band {bd['says']!r} does not say the ends it holds")
            at, held = b, bd["to_in"]
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
    if g.get("possession") is not None:
        g["possession"] = {**g["possession"], "by": r(g["possession"]["by"])}
    return g


def check_built(bundle: str, export_dir, items=None, dates=DATES, log=print) -> dict:
    """Build the rows for every part (or `items`) and prove every group of every card at `dates`:
    {checked, groups, problems [str]} — one check per distinct (rule key, kind, steelhead, date, the
    reader's verdicts, the card): rows are keyed by content and many sections share a key, so each
    distinct card is proved once on each sample date (the date picks the zone rules in force elsewhere).
    This REBUILDS the rows: it proves the code, not a file. The shipped file is `check_shipped`'s."""
    from pathlib import Path
    from pipeline.deliver.answers import answers as A, common as C
    data, guide = C.load_export(Path(export_dir))
    model = A.build(bundle, Path(export_dir), workers=1, items=items, sections=["rows"],
                    export=(data, guide), log=lambda *_: None)
    return check_model(model, bundle, data, dates=dates, log=log)


def check_shipped(bundle: str, export_dir, answers=None, dates=DATES, keys=None, log=print) -> dict:
    """THE GATE ON THE SHIPPED FILE (P05): decode `ui-rules-answers.json` exactly as written (default: in
    `export_dir`) and prove every group of every card it holds — every key, or only `keys`. A number
    changed in the file is a problem here, because the cards come from the file's bytes and the maxima
    from the bundle's rules. The result also has `parts_checked`, `keys_checked` and
    `answers_sha256`, the digest of the bytes that were proved."""
    import hashlib
    import json
    from pathlib import Path
    from pipeline.deliver.answers import common as C, encode as E
    answers = Path(answers) if answers else Path(export_dir) / "ui-rules-answers.json"
    raw = answers.read_bytes()
    wire = json.loads(raw)
    data, guide = C.load_export(Path(export_dir))
    E.check_pair(wire, data, guide)
    model = E.decode(wire, data)
    got = check_model(model, bundle, data, dates=dates, keys=keys, log=log)
    got["keys_checked"] = len(model.keys) if keys is None else len(set(keys))
    got["parts_checked"] = sum(len(v) for v in model.parts.values()) if keys is None else None
    got["answers_sha256"] = hashlib.sha256(raw).hexdigest()
    return got


def check_model(model, bundle: str, data: dict, dates=DATES, keys=None, log=print) -> dict:
    """Prove every rows group of `model` (built or decoded: the same `answers.Model`) at `dates`,
    over every key or only `keys`."""
    from pipeline.deliver.answers import common as C, encode as E, rows as RW
    from pipeline.deliver.calendar import day_of
    B = C.load(bundle)
    ids = data["rule_ids"]
    seen, probs, groups = set(), [], 0
    lad_json: Dict[int, str] = {}
    t0 = time.monotonic()
    only = None if keys is None else set(keys)
    for k, key in enumerate(model.keys):
        if only is not None and k not in only:
            continue
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


# --------------------------------------------------------------------------------------------
# The stamp (P05): what `deliver all` writes after the gate, and what the store refuses without
# --------------------------------------------------------------------------------------------

#: written beside the answers file it proves; `pipeline.deliver.store.build` reads it
STAMP_NAME = "card-check.json"


def code_sha() -> str:
    """The digest of the gate's code and the engine it holds the cards against (every module of
    `pipeline.deliver.answers`, its subpackages `rows/`, `display/` and `model/` included), so a stamp says
    WHICH gate passed."""
    import hashlib
    from pathlib import Path
    h = hashlib.sha256()
    root = Path(__file__).parent
    for f in sorted(root.rglob("*.py")):
        h.update(f.relative_to(root).as_posix().encode() + b"\0" + f.read_bytes() + b"\0")
    return h.hexdigest()[:16]


def stamp_of(got: dict, dates=DATES) -> dict:
    """The stamp for a whole-file `check_shipped` result (no `keys`)."""
    return {"card_check": {"problems": len(got["problems"]), "parts_checked": got["parts_checked"],
                           "keys_checked": got["keys_checked"], "cards_checked": got["checked"],
                           "groups_checked": got["groups"], "dates": [list(d) for d in dates],
                           "code_sha": code_sha(), "answers_sha256": got["answers_sha256"]},
            "problems": got["problems"]}


def stamp_problems(answers, raw: bytes) -> List[str]:
    """Why the answers bytes `raw` (of the file `answers`) may NOT be packaged: no stamp beside them,
    a stamp with problems, or a stamp for other bytes. Empty means proved."""
    import hashlib
    import json
    from pathlib import Path
    p = Path(answers).with_name(STAMP_NAME)
    if not p.exists():
        return [f"no card-gate stamp {p} — run `python -m pipeline.deliver.answers.card_check --all`"]
    try:
        st = json.loads(p.read_text())["card_check"]
    except (ValueError, KeyError, TypeError) as e:
        return [f"{p} is not a card-gate stamp ({e})"]
    out = []
    if st.get("problems") != 0:
        out.append(f"{p}: the card gate found {st.get('problems')} problems")
    if st.get("answers_sha256") != hashlib.sha256(raw).hexdigest():
        out.append(f"{p} proves other answers bytes (sha {str(st.get('answers_sha256'))[:12]}…), not {Path(answers).name}")
    if not st.get("parts_checked"):
        out.append(f"{p} was not a whole-file run (parts_checked {st.get('parts_checked')!r})")
    return out


def json_key(v) -> str:
    import json
    return json.dumps(v, sort_keys=True, default=str)


def main(argv=None) -> int:
    import argparse
    import json
    from pathlib import Path
    from pipeline.deliver.bundle import read as RD
    ap = argparse.ArgumentParser(description="answers 2.8: prove every number on every quota card")
    ap.add_argument("--bundle", default=RD.BUNDLE)
    ap.add_argument("--export-dir", default=str(Path(RD.BUNDLE).parent.parent / "regs"))
    ap.add_argument("--items", default=None, help="comma-separated item ids (default: every part)")
    ap.add_argument("--all", action="store_true",
                    help="prove the SHIPPED answers file (every key, decoded from its bytes) and write the "
                         f"stamp + JSON report ({STAMP_NAME} beside it); `deliver all` runs this")
    ap.add_argument("--answers", default=None, help="with --all: default ui-rules-answers.json in --export-dir")
    a = ap.parse_args(argv)
    if a.all:
        if a.items:
            ap.error("--all proves the whole file; --items is for the rebuild check")
        answers = Path(a.answers) if a.answers else Path(a.export_dir) / "ui-rules-answers.json"
        out = answers.with_name(STAMP_NAME)
        out.unlink(missing_ok=True)              # a stale stamp never outlives a failed run
        got = check_shipped(a.bundle, a.export_dir, answers)
        st = stamp_of(got)
        tmp = out.with_name(out.name + ".tmp")
        tmp.write_text(json.dumps(st, indent=1) + "\n")
        tmp.replace(out)
        print(json.dumps(st["card_check"]))
    else:
        got = check_built(a.bundle, a.export_dir, a.items.split(",") if a.items else None)
    for p in got["problems"][:200]:
        print(p)
    return 1 if got["problems"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
