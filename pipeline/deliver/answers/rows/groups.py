"""Group totals and the kept-elsewhere line (decision F14, answers 2.5): a shared limit's group
(`group_of`), its total, what is kept elsewhere (`elsewhere_of`, `over_elsewhere`), and which row holds a row
(`parent_of`)."""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence
from pipeline.deliver.answers import common
from pipeline.deliver.answers.rows.rule import INF, R, _bands, _covers, _js, _keep_range, _keepish
from pipeline.deliver.answers.rows.evaluate import counts_toward, main_res
from pipeline.deliver.answers.rows.card_model import _icu_key
from pipeline.deliver.answers.rows.conds import _raise_of
from pipeline.deliver.answers.rows.wire import _num, badge_of


def _kept(model: dict, S: str) -> Optional[dict]:
    """The fish's decided answer as the card reads it (the main origin's), when it keeps."""
    R_ = model["R"].get(S) or {}
    m = main_res(R_.get("H"), R_.get("W"))
    return m if m and _keepish(m["status"]) else None


def total_of(P: "Part", model: dict, row: dict) -> R:
    """The TOTAL a keep row's fish count toward (decisions F4, F14): the outer pool every member's
    answer is contained by (Vedder r9's hatchery rainbow: Region 2's 4), else the row's pool."""
    outs: Optional[List[str]] = None
    for S in row["members"]:
        m = _kept(model, S)
        ks = [k for k in m["roles"] if k != m["win"].key and counts_toward(m, k)] if m else []
        outs = ks if outs is None else [k for k in outs if k in ks]
    return P.by_key[outs[0]] if outs else row["pool"]


def _names(codes: Sequence[str], conj: str = " or ") -> str:
    g = common.group_for(codes)
    if g:
        return common.lc(g)
    return common.join([common.lc(common.sp_name(c)) for c in codes], conj)


def _names_short(codes: Sequence[str]) -> str:
    """RULINGS P26: the fish named shortly — a closed group by its name, one missing a member or two as the
    group "other than" them ("trout and char other than steelhead"), else each fish."""
    g = common.group_for(codes)
    if g:
        return common.lc(g)
    s = set(codes)
    for name, members in common._closed_groups():
        rest = sorted(members - s)
        if len(s) >= 3 and s < members and len(rest) <= 2:
            from pipeline.deliver.answers.card import book_order
            return (f"{common.lc(common.sp_name(name))} other than "
                    f"{common.join([common.lc(common.sp_name(x)) for x in book_order(rest)], ' or ')}")
    return common.join([common.lc(common.sp_name(c)) for c in codes], " or ")


def _range_words(a, b) -> str:
    """A keep range in the book's words (bounds of a keep band are inclusive: "50 cm or less"), in cm AND inches
    (RULINGS P34: "50 cm (20 in) or less", "over 50 cm (20 in)", "20–30 cm (8–12 in)")."""
    if not a and (b is None or b == INF):
        return ""
    if not a:
        return f"{common.cm_in(b)} or less"
    if b is None or b == INF:
        return f"over {common.cm_in(a)}"
    return common.range_cm_in(a, b)


def _clauses(P: "Part", T: R) -> List[R]:
    """Every count or size clause of a total (`within` it, nested), in the part's rule order."""
    ids, out, grew = {T.rule_id}, [], True
    while grew:
        grew = False
        for r in P.cands:
            if r.f.get("within") and r.entry_id == T.entry_id and r.f["within"] in ids \
                    and r not in out:
                out.append(r)
                ids.add(r.rule_id)
                grew = True
    return [r for r in out if r.k in ("subcap", "sizecap")]


def _clause_take(c: R):
    """A clause's number: its own, or its band's ("[{min_cm: 50, take: 1}]")."""
    if c.f.get("take") is not None:
        return c.f["take"]
    return next((t for _, _, t in _bands(c) if t), None)


def _lift_words(P: "Part", by: str, fish: List[str], sized: bool = True) -> str:
    """The fish a water's rule lifts out of a sub-cap, said as that rule keeps them: "hatchery
    rainbow trout 50 cm or less" (Vedder r9), "rainbow trout" (Duncan r4). Out of a SIZE clause
    the fish alone (`sized` False): its own range is no part of an "over 50 cm" cap."""
    L = P.by_key[by]
    a, b = _keep_range(L)
    return " ".join(x for x in (L.f.get("origin") or "", _names(fish),
                                _range_words(a, b) if sized else "") if x)


def group_of(P: "Part", model: dict, T: R, own_row: bool = False) -> Optional[dict]:
    """One total on today's card (decision F14): `pool` the total's rule, `total` its number
    ("unlimited" for none), `kinds` every fish of the card counting toward it — the total's own
    row and every fish with its own row contained by it (F4; steelhead included, RULINGS Q23),
    `subcaps` each count or size clause of the total holding some of those kinds here
    ({r, take, water, over_cm, under_cm, of, lifted [{members, by}]} — `of` the kinds it holds,
    `lifted` those a water's own rule lifts out of it). None for a total of one kind with no
    sub-cap, unless that kind is a trout or char and the total is its row's own pool (`own_row`)
    on every kind of water (`A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP`).
    THE DECISION ONLY: how the card SAYS it is `card.card_of` (answers 2.8 — the lead sentence
    and the items of 2.6/2.7 are gone: they restated the kind rows)."""
    kinds = [S for S in model["spp"] if counts_toward(_kept(model, S), T.key)]          # T127: the one predicate
    kinds.sort(key=lambda S: _icu_key(P.sp_name(S)))
    if not kinds:
        return None
    # the stream (or lake) share first, then the count clauses, then the size clauses
    chain = sorted(_clauses(P, T), key=lambda c: (0 if c.f.get("water") else 1 if c.k == "subcap"
                                                  else 2, -sum(_covers(c, S) for S in kinds),
                                                  _clause_take(c) or 0))
    in_chain = {c.key for c in chain} | {T.key}
    unlimited = bool(T.f.get("unlimited"))
    total = "unlimited" if unlimited else _num(T.f.get("take"))
    subcaps = []
    for c in chain:
        held, lifted = [], {}
        for S in kinds:
            m = _kept(model, S)
            v = m["roles"].get(c.key) or {}
            # a clause the card's lines fold into an equal-sized one of the same total (Region 2's
            # "2 over 50 cm" beside "1 over 50 cm, not steelhead": Q16) still holds the fish
            if m.get("narrow") is c or v.get("role") in ("narrows", "limit") \
                    or (v.get("role") == "replaced" and v.get("by") in in_chain):
                held.append(S)
            elif v.get("role") in ("lifted", "replaced") and v.get("by") \
                    and v["by"] not in in_chain and v["by"] in P.by_key:
                lifted.setdefault(v["by"], []).append(S)
        if not held:
            continue
        hit = next(((a, b, t) for a, b, t in _bands(c) if t), None) if c.k == "sizecap" else None
        band = hit[:2] if hit else None
        take = _clause_take(c)
        if not take:
            # a slot ("none between 36 cm and 44 cm", Teslin) keeps no number: a release band the
            # row's lines and the item's bands show (F5a), never a sub-cap of the total
            continue
        sc = {"r": c.key, "take": _num(take), "water": c.f.get("water"),
              "over_cm": _num(band[0]) if band and band[0] else None,
              "under_cm": _num(band[1]) if band and band[1] != INF else None,
              "of": held,
              "lifted": [{"members": fish, "by": by} for by, fish in lifted.items()]}
        # a size clause another of the total makes redundant on this card — the same band, holding
        # every kind it holds, with no larger number (Region 1's "2 over 50 cm" beside "1 over 50
        # cm" on a lake with no steelhead) — is not said twice
        if band and any(o["over_cm"] == sc["over_cm"] and o["under_cm"] == sc["under_cm"]
                        and set(held) <= set(o["of"]) and o["take"] <= sc["take"]
                        for o in subcaps):
            continue
        subcaps.append(sc)
        # decision F17: the tally a water's steelhead quota raises the clause to, over every fish the clause
        # holds AND the steelhead it lifted ("+1 for steelhead"; the Vedder's r2b beside its r2), as that
        # quota's — a sub-cap the card says with the clause (`raise_of`), never on its own
        if band and band[1] == INF:
            for x in _raise_of(P, c):
                fish = [S for S in kinds if S in held or (S in {F for F, _o in x.lifted}
                                                          and S in lifted.get(x.lifter.key, []))]
                if set(fish) - set(held):
                    subcaps.append({"r": x.lifter.key, "take": _num(x.f["take"]), "water": None,
                                    "over_cm": sc["over_cm"], "under_cm": None, "of": fish, "lifted": []})
    if len(kinds) < 2 and not subcaps \
            and not (A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP and own_row and not T.f.get("water")
                     and set(kinds) <= _trout_char()):
        return None
    return {"pool": T.key, "total": total, "kinds": kinds, "subcaps": subcaps}


#: D19 (a), T80 3 (owner call 11, Q11: the backend answers, the app only reads): a TROUT/CHAR total of ONE kind with
#: no sub-cap is still a group WHERE THE TOTAL IS ITS ROW'S OWN POOL — its row's headline is what the app's Keep tab
#: draws a pooled row by (core `keep.ts`: a group, else a plain row with no pool). Without it the row was drawn nowhere:
#: Kootenay Lake — Main Body's rainbow 10 a day (klmb_1010), Tranquille Lake's rainbow, the Region 7 brook trout
#: lakes, Ross Lake's and 14 other waters' own brook trout quotas. Other fish rows keep no group: the Other fish list
#: draws them by its own compact path. A one-kind row whose total is ANOTHER rule's (Duncan River's Dolly Varden 2,
#: inside Region 4's 5) is not covered: a group of that total would say "5" over one kind that can never reach it (the
#: card gate refuses it); nor is a total limited to a kind of water (Region 8's "20 brook trout from streams", z8 trout_char_quota.r5): the card gate proves a zone
#: total over fish kept on lakes too, and its worked "kept elsewhere" line is off by one there. Both stay drawn nowhere
#: until T80's follow-up (the gate's places for a water-limited total; a contained one-kind row).
A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP = True


def _trout_char() -> frozenset:
    return common.trout_char()                          # the model (model.py) accepts by the same predicate


def raise_of(grp: dict, s: dict) -> Optional[dict]:
    """Decision F17: the size sub-cap of `grp` that sub-cap `s` RAISES — the clause at the same size whose
    fish `s`'s rule lifted out of it (`s` the tally over them all) — or None where `s` raises none."""
    return next((o for o in grp["subcaps"] if o is not s and o["over_cm"] is not None
                 and o["over_cm"] == s["over_cm"] and o["under_cm"] == s["under_cm"]
                 and any(L["by"] == s["r"] and set(L["members"]) <= set(s["of"]) for L in o["lifted"])
                 and set(o["of"]) < set(s["of"])), None)


def written_name_of(r: R) -> str:
    from pipeline.deliver.answers.display import written_name
    return written_name(r.x)


def _scope_name(sc: dict, pool: R) -> Optional[str]:
    """Whose count a badge says (the page's `scopeOf`): B.C., the region's or area's entry name."""
    if sc["of"] == "bc":
        return "B.C."
    if sc["of"] in ("region", "area"):
        return pool.x.get("entry_name") or ("the area" if sc["of"] == "area" else "the region")
    return None


def _kept_origin(model: dict, S: str) -> Optional[str]:
    m = _kept(model, S)
    if m is None:
        return None
    R_ = model["R"].get(S) or {}
    return "hatchery" if R_.get("H") is m else "wild"


def _line(q: str, a: str, model: dict, kinds: Sequence[str], kept, at: str, here,
          over_cm=None, most: str = "exactly", kind: str = "other_kind", of=None) -> dict:
    """One line of the card's one list (RULINGS P9): the question and answer, and the basket it starts
    from — `kept` fish of one kind of `kinds` (the gate takes the first it may keep there) kept at a
    `at` (another stream or lake, or "elsewhere": either; over `over_cm`) — and
    `here`, the most more here (the gate proves it: exactly, or `most` "at_most" for a ceiling the
    answer states; `of` the kinds that number counts, None every kind of the card). `kind`: the line's
    example — "stream_tally" (the share used up on another stream: a worked keep-N count already says
    it), "other_kind" (some kept on the other kind of water), "tally" (k kept anywhere, the total's
    number less k here), "over" (one over the size kept elsewhere)."""
    sp = [x for x in kinds if _kept_origin(model, x) is not None]
    return {"q": q, "a": a, "kept": kept, "at": at, "species": sp,
            "origin": _kept_origin(model, sp[0]) if sp else None, "over_cm": over_cm,
            "here": here if sp else None, "most": most, "kind": kind,
            "of": None if of is None else list(of)}


def elsewhere_of(P: "Part", model: dict, row: dict, sc: dict, T: R,
                 grp: Optional[dict]) -> dict:
    """The kept-elsewhere line of a keep row (rows F9, F14) — `says` one sentence, `ask`
    [[question, answer]] worked examples — qualified by what the total's sub-caps lift: on the
    Vedder, Oct 10, "Kept 2 on another stream? Keep 0 here, except hatchery rainbow trout 50 cm or
    less: up to 4 in all" (it was "Keep 0 here": p.22 lifts the stream share for them). `lines` the
    same examples with the basket each starts from and the number it states (`_line`, RULINGS P9: the
    card folds them into `group.worked`, its one list)."""
    n, pool, nw = row["daily"], row["pool"], row["narrow"]
    s = _scope_name(sc, pool)
    lines: List[dict] = []
    if sc["share"] and nw is not None and all(_covers(nw, S) for S in row["members"]):
        wk = nw.f["water"]
        other = "lake" if wk == "stream" else "stream"
        mine = next((x for x in (grp or {}).get("subcaps", []) if x["r"] == nw.key), None)
        tot = "unlimited" if T.f.get("unlimited") else T.f.get("take")
        says = f"Your own limit, counted across every {s} {wk} you fish today."   # RULINGS P44
        # RULINGS Q23b: the stream tally counts every trout/char kept from ANY
        # stream today; a kind THIS water lifts may still go past it here, to its own quota and the
        # total — after n kept on another stream, up to min(its own, total - n) more of it here
        ex = []
        for L in (mine or {}).get("lifted", []):
            own = P.by_key[L["by"]].f.get("take")
            more = (tot - n) if own is None or tot == "unlimited" else min(own, tot - n) \
                if tot != "unlimited" else own
            ex.append((_lift_words(P, L["by"], L["members"]), more))
        # RULINGS P24: no bare "0" in an answer (it reads as a count): "None more here"
        a1 = "None more here"
        if ex and tot != "unlimited":
            a1 += ", except " + common.join([f"{w}: up to {_js(m)} more" for w, m in ex], "; ") \
                + f" ({_js(tot)} in all)"
        elif ex:
            a1 += ", except " + common.join([w for w, _m in ex], " or ")
        held = [S for S in row["members"] if S in (mine or {}).get("of", row["members"])]
        lifted_own = [P.by_key[L["by"]].f.get("take") for L in (mine or {}).get("lifted", [])]
        unl = tot == "unlimited" or any(x is None for x in lifted_own)
        here1 = None if unl else min(tot - n, sum(m for _w, m in ex)) if ex else 0
        here2 = None if unl else min(tot - 1, n + sum(lifted_own))
        # RULINGS P41: the lake line says how many are left here after 1 kept there (the gate
        # proves `here`), then why: "Keep 2 more here. Fish from lakes count toward the 4 a day, not
        # toward the 2 from streams."
        if tot == "unlimited":
            a2 = f"They don’t count toward the {_js(n)} from {wk}s"
        elif here2 is None:
            a2 = f"They count toward the {_js(tot)} a day, not toward the {_js(n)} from {wk}s"
        else:
            a2 = (f"Keep {_js(here2)} more here. Fish from {other}s count toward the {_js(tot)} a day, not "
                  f"toward the {_js(n)} from {wk}s.")
        lines = [_line(f"Kept {_js(n)} on another {wk}?", a1, model, held, n, wk, here1, kind="stream_tally"),
                 _line(f"Kept 1 at a {other}?" if here2 is not None else f"Kept some at a {other}?", a2,
                       model, row["members"], 1, other, here2)]
    elif s:
        says = f"Your own limit, counted across every water in {s} you fish today" + \
            (", lakes and streams alike." if not pool.f.get("water") else ".")
        if row["kind"] == "keep" and n is not None and n > 1:
            k = 2 if n > 2 else 1
            a = f"Keep {_js(n - k)} more here"
            # a stream (lake) share of the total holding some of the row's kinds here still binds
            # them (Region 6's "1 trout from streams", Kitimat's tributaries): said, with its lifts
            sh = next((x for x in (grp or {}).get("subcaps", []) if x["water"] == P.kind
                       and x["take"] < n and set(x["of"]) & set(row["members"])), None) \
                if grp is not None and grp["pool"] == T.key else None
            if sh is not None:
                of = "" if set(row["members"]) <= set(sh["of"]) else f" {_names(sh['of'])}"
                a += f", but no more than {_js(sh['take'])}{of} from {sh['water']}s in all"
                ex = common.join([_lift_words(P, L["by"], L["members"]) for L in sh["lifted"]])
                if ex:
                    # RULINGS Q23b: lifted fish count toward the tally; they may go past it here
                    a += f" ({ex} may go past it here)"
            lines = [_line(f"Kept {k} elsewhere today?", a, model, (grp or {}).get("kinds") or row["members"],
                           k, "elsewhere", None if sh is not None else n - k, kind="tally")]
    else:
        # RULINGS P26: SPECIFIC — whose limit, its number, and which fish kept elsewhere today count
        # toward it (the row's kinds: Ape Creek's char released on Oct 10 are not among them): "The Bella
        # Coola River’s rule: 1 a day here. Brown trout, cutthroat trout or rainbow trout you kept anywhere
        # else today count toward that 1." (was "This river’s own limit. Fish you kept elsewhere today count
        # toward it too." on a creek the river's rule reached by the walk)
        from pipeline.deliver.answers.card import _whose
        whose = common.cap(_whose(P, pool)[1])
        fish = _names_short(row["members"])
        if n is None:
            says = f"{whose}: no daily limit here."
        else:
            that = "that 1" if n == 1 else f"those {_js(n)}"
            says = (f"{whose}: {_js(n)} a day here. {common.cap(fish)} you kept anywhere else today count "
                    f"toward {that}.")
        if sc["apart"]:
            says += " These fish are counted apart from the region’s total: they don’t use it up."
        if T is not pool:
            whose = T.x.get("entry_name") if badge_of(T) != "water" else None
            whose = "B.C.’s" if badge_of(T) == "bc" else f"{whose}’s" if whose else "the"
            says += (f" They also count toward {whose} {_js(T.f.get('take'))} {written_name_of(T)} a "
                     f"day." if not T.f.get("unlimited") else
                     f" They also count toward {whose} {written_name_of(T)}, which has no daily "
                     f"limit.")
    lines += over_elsewhere(P, model, row, grp)
    return {"says": says, "ask": [[x["q"], x["a"]] for x in lines], "lines": lines}


def over_elsewhere(P: "Part", model: dict, row: dict, grp: Optional[dict]) -> List[dict]:
    """RULINGS Q16 across waters (answers 2.8, A2): a size clause of the total holding the row's kinds
    counts fish kept anywhere today — said in the kept-elsewhere sheet, never on the card: "Kept a fish
    over 50 cm elsewhere today?" — "It counts here too: at most 2 over 50 cm a day in all — only 1 of
    them may be anything other than a hatchery steelhead." The line's basket: one of the kinds the
    smaller clause holds (not a steelhead) kept over the size on another water of this kind; at most
    the larger clause's number less one more over the size here."""
    if grp is None:
        return []
    sizes = [x for x in grp["subcaps"] if x["over_cm"] is not None and not x["water"]
             and set(x["of"]) & set(row["members"])]
    if not sizes:
        return []
    by_band: Dict[tuple, List[dict]] = {}
    for x in sizes:
        by_band.setdefault((x["over_cm"], x["under_cm"]), []).append(x)
    out = []
    for (a, b), xs in sorted(by_band.items(), key=lambda kv: (kv[0][0], kv[0][1] or 0)):
        xs.sort(key=lambda x: -x["take"])
        band = _range_words(a, INF if b is None else b)
        big = xs[0]
        say = f"It counts here too: at most {_js(big['take'])} {band} a day in all"
        small = big
        if len(xs) > 1 and set(xs[1]["of"]) < set(big["of"]):
            small = xs[1]
            rest = [S for S in big["of"] if S not in xs[1]["of"]]
            who = " ".join(w for w in ("hatchery" if "ST" in rest and P.steelhead_water else "",
                                       _names(rest)) if w)
            say += (f" — only {_js(xs[1]['take'])} of them may be anything other than "
                    f"{'a ' if len(rest) == 1 else ''}{who}")
        from pipeline.deliver.answers.display import written_name
        # ROUND UR5 (D1): the fish the clause holds HERE — Region 4's "rainbow trout or cutthroat trout" clause reaches the
        # Kootenay Lake main body's trout and char only for cutthroat (rainbow has its own row there); naming rainbow read as a
        # daily cap on big rainbows. The rule's own words where the clause holds every fish it names.
        # decision F17: a tally a water's quota raises a clause to is said in the clause's words (its fish)
        rx = P.by_key[(small if raise_of(grp, big) is small else big)["r"]].x
        rsp = list(rx.get("species") or [])
        narrower = rsp and all(c not in common._species().SPECIES_GROUPS for c in rsp) and set(rsp) - set(big["of"]) and set(big["of"]) <= set(rsp)
        who = _names([c for c in rsp if c in big["of"]]) if narrower else written_name(rx, " or ").replace(" and ", " or ")
        kinds = [S for S in small["of"] if S != "ST"] or list(small["of"])
        out.append(_line(f"Kept {who} {band} elsewhere today?", say + ".", model, kinds, 1, "elsewhere",
                         max(0, big["take"] - 1), over_cm=a if b is None else None, most="at_most",
                         kind="over", of=big["of"]))
        if b is not None:
            out[-1]["here"] = None
    return out


def parent_of(rows: Sequence[dict], i: int) -> Optional[int]:
    """RULINGS P46 (page v36 parentRow): THE POOL ROW THAT HOLDS A POOLED ROW — of the stored rows
    (`produce`'s, in card order), the first other pooled row holding every fish of row `i` (among its `all_members`,
    or in one of its fact groups) whose facts cross-reference one of them (an `xref` fact: the other pool counts
    them, decision F4); else (T139, `A_ROW_HELD_BY_ITS_TOTALS_GROUP`) the row that carries the group of row `i`'s
    total where that total is ANOTHER rule's (`total`: `total_of`'s key; `draws`: the totals whose group a row
    carries, `produce`'s `home`) — that group's kinds are row `i`'s fish too, and its article says them. The card
    draws no article for such a row: its fish are said inside that pool's; None for every other row (and every row
    with no pool)."""
    o = rows[i]
    if not o.get("pool"):
        return None
    M = o["members"]
    for j, r in enumerate(rows):
        if j == i or not r.get("pool"):
            continue
        holds = all(S in r["all_members"] or any(S in g["members"] for g in r["groups"]) for S in M)
        if holds and any(l["t"] == "xref" and any(S in g["members"] for S in M)
                         for g in r["groups"] for l in g["facts"]):
            return j
    T = o.get("total")
    if A_ROW_HELD_BY_ITS_TOTALS_GROUP and T is not None and T != o["pool"]:
        return next((j for j, r in enumerate(rows) if j != i and r.get("pool") and T in (r.get("draws") or ())),
                    None)
    return None


#: T139: a pooled row whose total is ANOTHER rule's, carried as a group on another row, is held by that
#: row. Beattie Lake (r7, in the Liard River watershed, wbk:329665706): the reader lets the Liard's "bull trout 1
#: (30-50 cm)" displace the lake's "trout 1" for bull trout, and both count toward Zone B's 5 (`contains`), whose group
#: sits on the lake's trout row and already says the bull trout (the bucket "LIARD RIVER WATERSHED's rule: Dolly
#: Varden/bull trout, 30-50 cm, 1, counts toward the 5 a day"). Until T127 the bull trout row was held through the lake
#: row's cross-reference of a pool the reader DISPLACED; T127 rightly removed that cross-reference, and the row was
#: left pooled, groupless and unheld — drawn nowhere (core `keep.ts`), a duplicate of the group's bucket. False only in
#: the mutation pin (test_answers_one_kind_group.py).
A_ROW_HELD_BY_ITS_TOTALS_GROUP = True
