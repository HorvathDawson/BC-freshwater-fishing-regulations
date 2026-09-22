"""THE TABLE A READER SEES — built from the ledger, procedurally, with no region named.

`rows.py` derives rows; this derives the whole table. It exists because a row is not enough:
the thing a reader has to understand is that several fish SHARE one number, and that fact lives
between rows, not on one.

THE ONE RULE EVERYTHING ELSE FOLLOWS FROM

    EVERY COUNTER THAT BINDS IS DRAWN SOMEWHERE.

Three of the first version's defects were one defect wearing three hats. It drew a headline (a
plain daily number) and a shared budget (a pooled number with two or more spenders), and every
counter that was neither simply vanished:

    · a pooled cap narrowed to ONE fish. Region 3's char share a 1. Bull trout and Dolly Varden
      go back in the autumn, so on 20 August the only fish left spending that 1 is the lake
      trout — and the budget was dropped for having fewer than two spenders, so the table said
      keep 4. "The cap is only for the remaining fish" had been built as "the cap is gone".
    · a NON-POOLED sized cap. "2 hatchery steelhead over 50 cm" is not pooled and is not plain,
      so Region 1's lakes printed the family's 4 where the truth is 2.
    · a clause. `within` was read only for nesting.

So there is no "headline plus extras" here. A fish's counters are PARTITIONED — exactly one is
the answer, the shared ones become budgets, and everything left is drawn on the fish as its own
limit. `_check` asserts the partition is total; a counter that fits nowhere is a crash, not a
silent omission, because a dropped counter always reads as MORE fish than the book allows.

The rest, each forced by a measured defect (see `pipeline/docs/05-table-generation.md` Part 7):

    A BUDGET IS DRAWN ONCE.        A shared number printed again on each member row is the same
                                   figure two or three times; it appeared twice on 53 of 88
                                   pooled rows. The block owns it; a member row says what it
                                   SPENDS.
    A MEMBER MAY HAVE ITS OWN.     Region 6's streams give trout 1 a day inside a family of 5. A
                                   layout where a member can only add to the group puts a reader
                                   five fish over.
    A RELEASED FISH LEAVES.        It is not drawn inside a live shared number and its name is in
                                   no sharer list that day.
    A SEASONAL COUNTER IS DRAWN    ...only on its days. The year view printed "1 trout ·
    ON ITS DAYS.                   July 1 – Oct 31" beside the trout answering 5.
    A SIZE HAS TWO ENDS.           A floor was the only bound read, so R7A's "30 to 50 cm" slot
                                   rendered as no size at all — 76 of them.
    NOTHING KEPT ⇒ NO SIZE.        "at least 60 cm" beside "Put it back" reads as permission.
    NAMES, NEVER A COUNT.          "9 kinds of trout and char" tells a reader with a bull trout
                                   nothing. Every member is named.
    NO BLANK ANSWER.               A row with no rule says so in words; a blank reads as "no
                                   limit", the most permissive failure there is.

Nothing here resolves anything. The ledger has already settled every counter; this only decides
what a reader is shown and in what order.
"""
from __future__ import annotations

import json
from typing import Dict, List, Optional, Tuple

from pipeline.regs.parsing.catalogue import DEFINITIONAL_SIZE, SPECIES_GROUPS
from pipeline.regs.table.build import name as fish_name
from pipeline.regs.table.ledger import Allowance, Ledger
from pipeline.regs.table.rows import heading as group_heading
from pipeline.regs.table.subject import expand
from pipeline.regs.table.subject import Origin

ORIGINS = (Origin.wild, Origin.hatchery)


# ----------------------------------------------------------------------------------------
# the pieces
# ----------------------------------------------------------------------------------------
def _named(fish) -> List[str]:
    return sorted(fish_name(c) for c in fish)


def _src(a: Allowance) -> dict:
    return {"rule": a.rule_id, "who": a.source.who, "verbatim": a.source.verbatim.strip(),
            "season": "" if a.applies.always else (a.applies.detail or "")}


def _daily(L: Ledger, sp: str, o: Origin, on) -> List[Allowance]:
    """Every daily counter binding this fish today — gates excluded, they are the size plan;
    possession multiples excluded, they are derived from one of these."""
    return [a for a in L.allowances
            if a.derived_from is None and a.period == "daily" and a.kind != "gate"
            and L.binds(a, sp, o, None, on)]


def _headline(L: Ledger, sp: str, o: Origin, on) -> Optional[Allowance]:
    """The answer: the strictest counter about the WHOLE fish. A sized counter ("1 over 50 cm")
    and a clause are not answers — they are limits inside one."""
    heads = [a for a in _daily(L, sp, o, on) if not a.within and a.scope.size.is_any]
    return min(heads, key=lambda a: (a.outcome.rank, a.rank, a.rule_id)) if heads else None


def sizes(L: Ledger, sp: str, o: Origin, on) -> List[dict]:
    """THE SIZE PLAN — one statement per bound, because a reader parses "at least 30 cm · only
    1 over 50 cm" as one muddled sentence and two clear rows as two facts.

    A gate is a take of zero on a size class, and `Size.kind` says which end it shuts. Reading
    only `none_under` lost every ceiling and every slot: R7A's rainbow may be kept at 30 to
    50 cm and the table showed no size at all, 76 times over.

    The definitional floor is the word itself, not a rule. A steelhead IS a rainbow over 50 cm,
    so a regional floor of 30 cm can never bite on one and printing 30 is simply wrong."""
    out, by_bound = [], {}
    for a in L.allowances:
        if a.kind != "gate" or not L.binds(a, sp, o, None, on):
            continue
        k, lo, hi = a.scope.size.kind, a.scope.size.lo, a.scope.size.hi
        if k == "none_under":
            got = [("floor", hi)]
        elif k == "none_over":
            got = [("ceiling", lo)]
        elif k == "slot":                       # keep only hi–lo: both ends at once
            got = [("floor", hi), ("ceiling", lo)]
        else:                                   # band: a hole in the middle, never merged
            out.append({"bound": "band", "lo": hi, "hi": lo, "cm": None,
                        "words": a.scope.size.plain(), "source": _src(a), "definition": False})
            continue
        for bound, cm in got:
            if cm is None:
                continue
            keep = (bound == "floor" and cm > by_bound.get(bound, (-1, None))[0]) or \
                   (bound == "ceiling" and cm < by_bound.get(bound, (10 ** 6, None))[0])
            if bound not in by_bound or keep:
                by_bound[bound] = (cm, a)

    d = (DEFINITIONAL_SIZE.get(sp) or {}).get("min_cm")
    if d is not None and ("floor" not in by_bound or d > by_bound["floor"][0]):
        by_bound["floor"] = (d, None)

    for bound, (cm, a) in by_bound.items():
        out.append({"bound": bound, "cm": cm, "lo": None, "hi": None,
                    "words": (f"must be at least {cm} cm" if bound == "floor"
                              else f"must be {cm} cm or shorter"),
                    "source": _src(a) if a is not None else None, "definition": a is None})
    order = {"floor": 0, "ceiling": 1, "band": 2}
    return sorted(out, key=lambda s: (order[s["bound"]], s["cm"] or 0))


def _spend(L: Ledger, a: Allowance, o: Origin, on) -> frozenset:
    """Who actually spends this number today. `binds` on both ends rather than `reaches`, so a
    fish carved out of the pool for the season leaves it, and `keepable` so a fish released by
    some OTHER rule leaves it too — a release does not carve the number it can no longer
    spend, which is why both tests are needed."""
    return frozenset(sp for sp in a.scope.effective()
                     for oo in ((Origin.wild, Origin.hatchery) if o in (None, Origin.both)
                                else (o,))
                     if L.binds(a, sp, oo, None, on) and L.keepable(sp, oo, on))


def pools(L: Ledger, on, o: Origin = None) -> List[dict]:
    """Every shared number in force: a pooled daily quota MORE THAN ONE keepable fish spends.

    A pooled number with one spender left is not a shared budget — but it is still a limit, and
    `view` puts it on that fish (see `_check`). Dropping it here without catching it there is
    what printed 4 lake trout under a cap of 1.

    `a.within` is not consulted: a clause ("1 char", inside the trout-and-char 5) is exactly the
    inner budget a reader needs, and excluding clauses is what loses the nesting."""
    seen, out = {}, []
    for a in L.allowances:
        if a.derived_from is not None or a.period != "daily":
            continue
        if a.outcome.kind != "quota" or not a.pooled:
            continue
        # THE ORIGIN OF THE VIEW, not both. Region 2's wild trout and char are all released, so
        # the family's 2 has no wild spender at all — unioning the origins kept a live pool of
        # 2 on a view where every fish in it must go back.
        spend = _spend(L, a, o, on)
        if len(spend) < 2:
            continue
        key = (a.rule_id, a.scope.size.words(), a.within)
        if key in seen:
            seen[key]["same"].append(a)      # ONE number the book states once; see `_same`
            continue
        seen[key] = {"same": [a]}
        out.append({"id": "|".join(str(x) for x in key), "n": a.n, "spends": spend,
                    "same": seen[key]["same"],
                    "size": a.scope.size.words(), "sized": not a.scope.size.is_any,
                    "clause_of": a.within or None, "allowance": a, "source": _src(a)})
    return out


def _nest(ps: List[dict]) -> None:
    """Parent: the rule the book itself says this is a clause of, and only failing that the
    narrowest budget that contains this one.

    Two things were wrong before. `Allowance.within` held the answer and was never read, which
    left 52 clause pairs unparented. And the fallback used STRICT containment on the spender
    sets, which cannot see the commonest clause of all: "1 over 50 cm" inside "4 trout and
    char" reaches exactly the same fish — it narrows by SIZE, not by species — so the sets are
    equal and neither was the other's parent.

    So the fallback orders budgets by how narrow they are, not by their fish alone: a clause is
    narrower than a plain number, and a sized one narrower than an unsized one. A parent must
    contain this budget's fish and be strictly wider on that order, which also makes a cycle
    impossible."""
    def narrowness(q):
        return (1 if q["clause_of"] else 0) + (1 if q["sized"] else 0)

    by_rule: Dict[str, List[dict]] = {}
    for p in ps:
        by_rule.setdefault(p["allowance"].rule_id, []).append(p)
    for p in ps:
        stated = [q for q in by_rule.get(p["clause_of"] or "", []) if q is not p]
        if stated:
            p["parent"] = max(stated, key=lambda q: len(q["spends"]))["id"]
            continue
        wider = [q for q in ps if q is not p and p["spends"] <= q["spends"]
                 and narrowness(q) < narrowness(p)]
        p["parent"] = (min(wider, key=lambda q: (len(q["spends"]), narrowness(q), q["id"]))["id"]
                       if wider else None)


# ----------------------------------------------------------------------------------------
# one view — one origin, one date
# ----------------------------------------------------------------------------------------
def _whole_fish(a: Allowance, fish) -> bool:
    """Does this SIZED counter in fact bind every one of these fish?

    A steelhead is by definition a rainbow trout longer than 50 cm, so "1 over 50 cm" is not a
    limit on the big ones — on a steelhead row it is the limit, full stop. The table was already
    printing "must be at least 50 cm" on that row from the same definition, and then printing 5
    beside it: two halves of one fact, and the half that reached the number was missing. Region
    6's hatchery steelhead read 5 where the book allows 1.

    This needs no per-water fact. `DEFINITIONAL_SIZE` is withheld from the oracle because
    deciding whether a RAINBOW is a steelhead means knowing whether steelhead run here; a row
    already labelled steelhead has that answer in its name."""
    k, lo, hi = a.scope.size.kind, a.scope.size.lo, a.scope.size.hi
    cut = lo if k == "counts_over" else hi if k == "counts_under" else None
    if cut is None:
        return False
    mins = [(DEFINITIONAL_SIZE.get(f) or {}).get("min_cm") for f in fish]
    return bool(mins) and all(m is not None and m >= cut for m in mins)


def _whole_fish_any(L: Ledger, sp: str, o: Origin, on) -> bool:
    """Is any SIZED counter on this fish one its own definition puts it entirely inside? Asked
    by the tests, so that the fish for which a sized number really is the number can be told
    apart from the fish merely sharing that number."""
    return any(not a.scope.size.is_any and _whole_fish(a, (sp,))
               for a in _daily(L, sp, o, on))


def _own(L: Ledger, a: Allowance, sp: str, o: Origin, on, answer: Optional[Allowance]) -> dict:
    """A counter that is this fish's alone today. Mostly a pooled cap the season has narrowed
    to one survivor — Region 3's "1 bull trout, Dolly Varden or lake trout" on 20 August, when
    the first two are going back. It is still a limit of one, and it must not say "BETWEEN
    THEM": there is nothing left to share it with, and the phrase is what makes a reader read a
    shared number as somebody else's problem."""
    with_me = sorted(_spend(L, a, o, on) | {sp})
    others = [fish_name(c) for c in with_me if c != sp]
    whole = not a.scope.size.is_any and _whole_fish(a, (sp,))
    if whole:
        words = f"at most {a.n} a day — every {fish_name(sp).lower()} is over " \
                f"{a.scope.size.lo or a.scope.size.hi} cm"
    elif not a.scope.size.is_any:
        words = f"at most {a.n} over {a.scope.size.lo or a.scope.size.hi} cm"
    elif a.is_zero:
        words = a.outcome.sentence()
    elif others:
        words = f"at most {a.n} a day, shared with {', '.join(sorted(others)).lower()}"
    else:
        words = f"at most {a.n} a day"
    # A SECOND AUTHORITY SAYING THE SAME THING IS NOT A SECOND LIMIT. Region 5 releases
    # steelhead itself, and the province releases the wild ones again; drawing both prints "put
    # it back" twice and, worse, made the wild and hatchery views differ over a restatement, so
    # every table in the province claimed an origin split it does not have.
    dull = bool(answer is not None and a.scope.size.is_any and answer.scope.size.is_any
                and ((a.is_zero and answer.is_zero)
                     or (a.n is not None and answer.n is not None and a.n >= answer.n)))
    return {"n": a.n, "size": a.scope.size.words(), "words": words,
            "sized": not a.scope.size.is_any and not whole, "by_definition": whole,
            "shared_with": sorted(others), "redundant": dull, "allowance": a, "source": _src(a)}


def _check(sp: str, o: Origin, mine: List[Allowance], answer, budgets, own) -> None:
    """THE PARTITION. Every counter binding this fish is the answer, or a budget it spends, or
    a limit of its own. A counter in none of the three is a number the reader never sees, and a
    number the reader never sees always reads as PERMISSION — so this raises."""
    placed = ({id(answer)} if answer is not None else set())
    placed |= {id(b["allowance"]) for b in budgets} | {id(c["allowance"]) for c in own}
    lost = [a for a in mine if id(a) not in placed]
    assert not lost, (sp, o.value, [(a.rule_id, a.n, a.scope.size.words()) for a in lost])


def view(L: Ledger, o: Origin, on=None) -> dict:
    universe = sorted(L.universe())
    answer = {sp: _headline(L, sp, o, on) for sp in universe}
    plan = {sp: (sizes(L, sp, o, on)
                 if answer[sp] is not None and not answer[sp].is_zero else [])
            for sp in universe}

    ps = pools(L, on, o)
    _nest(ps)
    by_id = {p["id"]: p for p in ps}
    # EVERY allowance the budget was built from, not just the one that got there first.
    # `pools` collapses a number the book states once but the ledger carries several times;
    # keying the map on the survivor alone sent its twins into `own`, so the reader saw the
    # same 1 twice — once as the shared budget and once as the fish's own limit.
    pool_of = {id(a): p for p in ps for a in p["same"]}

    # -- what each fish spends, and what it is capped by on its own ------------------------
    spent: Dict[str, List[str]] = {}
    alone: Dict[str, List[dict]] = {}
    for sp in universe:
        mine = _daily(L, sp, o, on)
        budgets = [pool_of[id(a)] for a in mine if id(a) in pool_of]
        own = [_own(L, a, sp, o, on, answer[sp]) for a in mine
               if a is not answer[sp] and id(a) not in pool_of]
        _check(sp, o, mine, answer[sp], budgets, own)
        spent[sp] = [p["id"] for p in sorted(budgets, key=lambda p: len(p["spends"]))]
        alone[sp] = own

    # -- leaves: fish a reader cannot tell apart ------------------------------------------
    def leafkey(sp):
        h = answer[sp]
        extra = tuple(sorted((a.rule_id, a.n, a.scope.size.words()) for a in L.allowances
                             if L.binds(a, sp, o, None, on) and a.period != "daily"
                             and a.derived_from is None))
        return (h.word() if h else None, h.rule_id if h else None,
                tuple((s["bound"], s["cm"], s["lo"], s["hi"]) for s in plan[sp]),
                tuple(spent[sp]), tuple(sorted(c["words"] for c in alone[sp])), extra)

    classes: Dict[tuple, List[str]] = {}
    for sp in universe:
        classes.setdefault(leafkey(sp), []).append(sp)

    leaves, rep_of = [], {}
    for _, fish in classes.items():
        fish, rep = frozenset(fish), min(fish)
        h = answer[rep]
        annual = [a for a in L.allowances
                  if L.binds(a, rep, o, None, on) and a.period == "annual"
                  and a.derived_from is None]
        # POSSESSION IS A COLUMN ON THIS TABLE, so it has to be on this leaf. It is the one
        # period `_daily` deliberately skips — a possession counter is N times a daily one and
        # would otherwise be drawn as a second, larger limit on the same fish.
        poss = [a for a in L.allowances
                if L.binds(a, rep, o, None, on) and a.period == "possession"
                and not a.within and a.scope.size.is_any]
        poss = min(poss, key=lambda a: (a.outcome.rank, a.rank, a.rule_id)) if poss else None
        leaves.append({
            "handle": group_heading(fish, fish_name),
            "members": _named(fish), "fish": sorted(fish),
            "answer": h.word() if h is not None else None,
            "zero": bool(h is not None and h.is_zero),
            "unwritten": h is None,
            "sizes": plan[rep],
            "spends": spent[rep],
            "own": [{k: v for k, v in c.items() if k != "allowance"}
                    for c in alone[rep] if not c["redundant"]],
            "own_number": None,            # filled below where it beats its innermost pool
            "capped_to": None,             # ...and where a limit of its own beats the headline
            "most": None,                  # ...and THE NUMBER: see below
            "annual": ({"n": annual[0].n, "source": _src(annual[0])} if annual else None),
            "possession": ({"n": poss.n, "word": poss.word(), "times": poss.multiplier or None,
                            "of": poss.derived_from.n if poss.derived_from is not None else None,
                            "source": _src(poss)} if poss is not None else None),
            "may_have": None,              # filled below, beside `most`
            "source": _src(h) if h is not None else None,
        })
        rep_of[leaves[-1]["handle"]] = rep

    # -- a member whose OWN number is smaller than the pool it sits in ---------------------
    # Region 6's streams give trout 1 a day inside a family of 5. Without this the row can only
    # add to the group and a reader is five fish over.
    for lf in leaves:
        if lf["zero"] or lf["unwritten"]:
            continue
        try:
            mine = int(str(lf["answer"]))
        except (TypeError, ValueError):
            continue
        if lf["spends"]:
            inner = by_id[lf["spends"][0]]
            if inner["n"] is not None and mine < inner["n"]:
                lf["own_number"] = mine
        # ...and a cap of its own that is SMALLER than the headline replaces the headline. The
        # headline is the widest counter about the whole fish; a narrower one that survives the
        # season is the number this reader may actually keep, and printing the 4 above it with
        # the 1 below reads as four.
        tighter = [c["n"] for c in alone[rep_of[lf["handle"]]]
                   if not c["sized"] and not c["redundant"] and c["n"] is not None and c["n"] < mine]
        tighter += [by_id[pid]["n"] for pid in lf["spends"]
                    if by_id[pid]["sized"] and by_id[pid]["n"] is not None
                    and by_id[pid]["n"] < mine
                    and _whole_fish(by_id[pid]["allowance"], lf["fish"])]
        if tighter:
            lf["capped_to"] = min(tighter)

    # ...and the flag the page reads says WHICH fish, never just "some fish".
    for p in ps:
        p["by_definition"] = sorted(
            f for lf in leaves if p["id"] in lf["spends"] and p["sized"]
            and _whole_fish(p["allowance"], lf["fish"]) for f in lf["fish"])
    # -- THE NUMBER A READER MAY ACTUALLY KEEP ---------------------------------------------
    # Haida Gwaii's lakes give bull trout 5 and share 3 across the char. Both are true and the
    # book prints both; the fish you may put in the boat is 3. Leaving that arithmetic to
    # whatever draws the table is how 431 species-slots came to show a number larger than the
    # ledger allows — every one of them a row sitting inside a smaller budget. `answer` stays
    # as the book's own words, with its provenance; `most` is what it comes to here.
    for lf in leaves:
        if lf["zero"]:
            # NOTHING KEPT MEANS NOTHING IN THE COOLER EITHER. Region 3's streams are shut to
            # every game fish until 1 July, and the possession counter is a family number that
            # knows nothing about the closure — so the column offered ten beside twelve rows
            # reading "No fishing".
            lf["most"], lf["may_have"] = 0, 0
            continue
        if lf["unwritten"]:
            continue
        try:
            n = int(str(lf["answer"]))
        except (TypeError, ValueError):
            continue
        if lf["capped_to"] is not None:
            n = min(n, lf["capped_to"])
        for pid in lf["spends"]:
            p = by_id[pid]
            # A SIZED BUDGET CAPS ONLY THE FISH THE SIZE CLASS SWALLOWS WHOLE — and that is a
            # question about THIS fish, not about the budget. Region 6's streams share "1 over
            # 50 cm" between steelhead and nine others; a steelhead is over 50 cm by definition
            # so its 1 is a 1, and asking the question of the budget instead of the leaf put
            # that 1 on the Arctic char too, which may have five.
            if p["n"] is None:
                continue
            if not p["sized"] or _whole_fish(p["allowance"], lf["fish"]):
                n = min(n, p["n"])
        lf["most"] = n
        # AND THE COOLER FOLLOWS THE DAY. A possession counter is N times a DAILY one, and the
        # daily one it multiplies is the family's, not this fish's. Region 6's hatchery
        # steelhead may keep one a day and the row offered ten in the cooler, explaining itself
        # as "2 x the daily 5" beside its own answer of 1.
        p = lf["possession"]
        if p is not None and p["n"] is not None:
            lf["may_have"] = (min(p["n"], p["times"] * n) if p["times"] else p["n"])

    out_pools = [{k: v for k, v in p.items() if k not in ("allowance", "same")}
                 | {"spends": sorted(p["spends"])} for p in ps]
    return {"origin": o.value, "pools": out_pools,
            "by_id": {p["id"]: p for p in out_pools},
            "leaves": sorted(leaves, key=lambda x: x["fish"])}


def _shape(v: dict) -> str:
    """What a reader would see, for comparing two origins. EVERYTHING they would see: the first
    version left out the annual limit and the fish's own caps, so Regions 3 and 5 collapsed two
    different tables into one and steelhead's ten-a-year vanished off the page."""
    import json
    return json.dumps(
        [[lf["fish"], lf["answer"], lf["sizes"], lf["spends"], lf["own"], lf["own_number"],
          lf["annual"] and lf["annual"]["n"], lf["zero"], lf["unwritten"], lf["capped_to"],
          lf["most"], lf["may_have"]]
         for lf in v["leaves"]] +
        [[p["spends"], p["n"], p["size"], p["parent"]]
         for p in sorted(v["pools"], key=lambda p: p["spends"])],
        sort_keys=True, default=str)


def table(L: Ledger, kind: str = "", on=None, label: str = "") -> dict:
    """The whole table. `origin_mode`:

        none   the wild and hatchery views are identical — no origin control anywhere
        split  they differ only on some fish — one table, those lines carry an origin
        hoist  a shared NUMBER itself differs — two tables, and the reader must choose

    Keyed on the pool's IDENTITY (its rule, its size class, the clause it sits in), not on its
    spender set. Keying on the set made this the wrong way round — 16 hoists and 2 splits where
    the truth is 2 and 16 — because a wild release empties a pool of its spenders without
    changing the number the book prints."""
    vs = {o: view(L, o, on) for o in ORIGINS}
    same = _shape(vs[Origin.wild]) == _shape(vs[Origin.hatchery])

    hoist = False
    if not same:
        w = {p["id"]: p["n"] for p in vs[Origin.wild]["pools"]}
        h = {p["id"]: p["n"] for p in vs[Origin.hatchery]["pools"]}
        hoist = any(w[k] != h[k] for k in set(w) & set(h))

    # THE NAMES, ONCE. `leaf["fish"]` is sorted by code and `leaf["members"]` by name, so
    # zipping the two — which is the obvious thing for a caller to do, and what the page did —
    # hands back the wrong name for almost every fish: Region 6's shared five named a Dolly
    # Varden that is in fact released. A map cannot be zipped wrong.
    names = {c: fish_name(c) for v in vs.values() for lf in v["leaves"] for c in lf["fish"]}
    return {"label": label, "kind": kind, "on": list(on) if on else None, "names": names,
            "merged": merged(L, on),
            "origin_mode": "none" if same else "hoist" if hoist else "split",
            "views": ({"both": vs[Origin.wild]} if same
                      else {o.value: vs[o] for o in ORIGINS})}


# ----------------------------------------------------------------------------------------
# ONE TABLE — both origins, and the complex fish apart from the simple ones
# ----------------------------------------------------------------------------------------
#: The fish whose regulation is a structure rather than a number: they share budgets, the
#: budgets have clauses, the clauses have size classes, and half of them go back. Everything
#: else is one fish and one figure. Printing the two kinds in one list makes the simple fish
#: look complicated and buries the structure the complicated ones need.
COMPLEX = frozenset(SPECIES_GROUPS["TROUT_CHAR"]) | frozenset(SPECIES_GROUPS["SALMON"])

#: What a reader can see on a fish's line. Two origins agreeing on all of it is ONE line.
SEEN = ("answer", "most", "sizes", "own", "own_number", "capped_to", "annual", "possession",
        "may_have", "zero", "unwritten", "spends")


def _handle(fish) -> str:
    """The name a reader finds this line under — and NEVER A COUNT.

    `rows.heading` gives up past eight fish and returns "9 kinds of all game fish". Two things
    are wrong with that on a table. A count is not a name: a reader holding a burbot cannot tell
    whether they are in it. And the umbrella is one the set does not fill — nine of the twenty-
    eight game fish are not "all game fish", they are nine fish that happen to share an answer.

    So: the group's name only where the set IS the group, and otherwise the fish, all of them,
    however many. A long cell is a solvable layout problem; a wrong name is not."""
    fish = frozenset(fish)
    if len(fish) == 1:
        return fish_name(next(iter(fish)))
    exact = [g for g in SPECIES_GROUPS
             if expand(frozenset({g})) and expand(frozenset({g})) == fish]
    if exact:
        return fish_name(min(exact, key=lambda g: len(g)))
    names = sorted(fish_name(f) for f in fish)
    return ", ".join(names[:-1]) + " or " + names[-1]


def merged(L: Ledger, on=None) -> dict:
    """BOTH ORIGINS, ONE TABLE.

    Drawing wild and hatchery as two whole tables was a misreading of what the split is. In
    every region the wild difference is the SAME small fact — the wild trout go back — and
    printing it as a second table of fourteen rows makes a reader compare two pages to find
    one sentence. Worse, the wild page has no budgets on it at all (nothing may be kept, so
    nothing shares a number), so the two tables do not even have the same shape: Region 1's
    streams showed a bare list beside a nested one and nothing said they were the same water.

    So the table is one table, and the origin lives on the LINE that differs. A fish both
    origins treat alike is one row with no origin on it at all — which is most of them.

    Leaves are grouped by BOTH origins' answers at once, not by one origin's and then matched
    up: the grouping itself differs between origins (wild releases every trout, so they are one
    leaf; hatchery keeps six of them apart), and pairing two different groupings after the fact
    cannot be done without guessing."""
    vs = {o: view(L, o, on) for o in ORIGINS}
    per = {o: {sp: lf for lf in v["leaves"] for sp in lf["fish"]} for o, v in vs.items()}
    pool = {o: v["by_id"] for o, v in vs.items()}

    def words(sp, o):
        lf = per[o][sp]
        return tuple(json.dumps(lf[k], sort_keys=True, default=str) for k in SEEN)

    def rule(sp, o):
        return (per[o][sp]["source"] or {}).get("rule", "")

    # GROUPED BY WHAT IS SAID **AND WHO SAID IT**. Grouping on the words alone put the kokanee
    # and the white sturgeon in with the char, because all three read "Put it back" — three
    # different rules collapsed under one of their names, and the line could then cite only one
    # of them. `same` below still compares the words only, so two origins saying the same thing
    # are one line even where different rules got them there.
    # ...AND A LINE NEVER STRADDLES THE TWO TABLES. On the province's own tables every fish has
    # the same answer, so all twenty-eight fall into one class — which would be drawn under
    # "Trout, char and salmon" with the burbot and the crayfish inside it. Which table a fish
    # belongs to is part of what tells two lines apart, so it belongs in the key.
    classes: Dict[tuple, List[str]] = {}
    for sp in sorted(L.universe()):
        key = (tuple(words(sp, o) for o in ORIGINS) + tuple(rule(sp, o) for o in ORIGINS)
               + (sp in COMPLEX,))
        classes.setdefault(key, []).append(sp)

    leaves = []
    for _, fish in classes.items():
        rep = min(fish)
        sides = {o.value: {k: per[o][rep][k] for k in SEEN} for o in ORIGINS}
        same = sides["wild"] == sides["hatchery"]
        leaves.append({
            "fish": sorted(fish), "members": _named(frozenset(fish)),
            "handle": _handle(fish),
            "complex": all(f in COMPLEX for f in fish),
            #: one line, or two — and when two, WHICH half is the exception. Every region's
            #: wild trout go back, so "wild" is the word that carries the information.
            "same": same,
            "both": sides["wild"] if same else None,
            "sides": None if same else sides,
            "source": per[Origin.hatchery][rep]["source"] or per[Origin.wild][rep]["source"],
        })

    # -- the budgets, said once, with the origins they are live for ------------------------
    ids = {p for o in ORIGINS for p in pool[o]}
    out = []
    for pid in sorted(ids):
        live = [o.value for o in ORIGINS if pid in pool[o]]
        any_ = pool[Origin.hatchery].get(pid) or pool[Origin.wild].get(pid)
        out.append(dict(any_, origins=live,
                        spends_by={o.value: pool[o][pid]["spends"] for o in ORIGINS
                                   if pid in pool[o]},
                        only=None if len(live) == 2 else live[0]))
    order = {p["id"]: i for i, p in enumerate(vs[Origin.hatchery]["pools"])}
    out.sort(key=lambda p: (order.get(p["id"], 99), -len(p["spends"])))
    return {"pools": out, "by_id": {p["id"]: p for p in out},
            "leaves": sorted(leaves, key=lambda x: (not x["complex"], x["fish"]))}
