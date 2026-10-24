"""The `display` section of the answers file: its scope, the per-segment facts (`section_prepare`), what a
water inherits here, and the statics (`section_static`: per rule and per named water)."""
from __future__ import annotations

import json
import re
from typing import Dict, List, Optional
from pipeline.deliver.answers.common import AnswersError, cap, join
from pipeline.deliver.answers.display.builders import (
    build_rules, closers_of, produce_parts, unless_opened_here
)
from pipeline.deliver.answers.display.subsets import add_subsets
from pipeline.deliver.answers.display.strip import strip_legend
from pipeline.deliver.answers.display.decided import (
    _unless_words, add_sources, inherited_kind, inherited_line, licensing_books, reasons_table
)


def section_scope(key: tuple, B):
    """The part's status per day reads its rule key (`status_index.set_profile`) — or, on tidal
    water, is the documented tidal state (`common.TIDAL_SCOPE`)."""
    from pipeline.deliver.answers.common import TIDAL_SCOPE, is_tidal, rule_key
    return TIDAL_SCOPE if is_tidal(key) else rule_key(key)


def section_prepare(scope, ctx):
    """Per slot (day x moment, `answers.slots`): the status index's code for the rule key at that
    moment — a projection of the verdicts (closed on a closed reading, else the key's floor):
    `base` (the tables' rules only), `own` (the water's own rules too), `closed` (every game fish
    under a speaking full closure) — and `closing` (gap G1, answers 2.1): every full closure that
    SPEAKS, not partly lifted, for some game fish, with the fish it closes
    (`verdicts.project.closing`, the one predicate `closed` is made of), rule by export index. A
    night closure's hours and a weekday rule's days are their own moments, so their status and
    closures are theirs. On tidal water, all year, `{"status": "tidal", **common.TIDAL_STATE}`:
    no freshwater status at all (FIX D12)."""
    from pipeline.deliver.answers.answers import slots
    from pipeline.deliver.answers.common import TIDAL_SCOPE, TIDAL_STATE
    from pipeline.deliver.calendar import DAYS
    from pipeline.deliver.verdicts import project
    if scope == TIDAL_SCOPE:
        return [0] * DAYS, [{"status": "tidal", **TIDAL_STATE}]
    st = ctx.store
    k = ctx.B.key_ix[scope]
    floor = "own" if st.own(k) else "base"
    none = "none"
    # RULINGS P12: the lifting rules this key holds by the tributary walk (a ✱ row's, T13 / T3a)
    walked = {f"{e}::{r}" for e, r, via in ctx.B.sets[scope.set_id]
              if via == "trib" and ctx.B.rules[(e, r)].get("exempts")}
    by_reading = []
    for rd in st.readings(k):
        closing = project.closing(lambda f, r=rd.ix: st.rows(st.verdict_id(k, r, f, none)),
                                  st.grade)
        v = {"status": "closed" if rd.closed else floor,
             "closing": [[ctx.rule_index[st.rule_ids[r]], fs] for r, fs in sorted(
                 closing.items(), key=lambda p: ctx.rule_index[st.rule_ids[p[0]]])]}
        if rd.closed:
            # display 18: the closures that close the water, decided (page v36 closersAt), never re-chosen by the app
            v["closers"] = closers_of(v["closing"])
        # Z12/Q41: the closing rules printed "unless opened" whose proviso is the answer here
        words = {st.rule_ids[r]: _unless_words(ctx, st.rule_ids[r]) for r in closing}
        sup = [st.rule_ids[r] for r in closing
               if ctx.by_id[st.rule_ids[r]].get("authority") == "superior"]
        unless = unless_opened_here({st.rule_ids[r]: fs for r, fs in closing.items()}, words, sup)
        if unless:
            v["unless_opened"] = sorted(ctx.rule_index[r] for r in unless)
        got = inherited_here(ctx, k, rd.ix, walked, ctx.B.sets[scope.set_id])
        if got:
            v["inherited"] = got
        by_reading.append(v)
    values: List[dict] = []
    seen: Dict[str, int] = {}
    ix = []
    for v in by_reading:
        key = json.dumps(v, sort_keys=True)
        if key not in seen:
            seen[key] = len(values)
            values.append(v)
        ix.append(seen[key])
    return [ix[r] for r in slots(st, k)], values


def inherited_here(ctx, k: int, reading: int, walked: set, bound=None) -> List[dict]:
    """RULINGS P12: the lifts the tributary walk carries here that THIS reading relies on — every rule
    the stored verdicts (origin none, every fish asked) say a walked rule `lifted` — each said by
    `inherited_line`: [{says, by, lifts, caution}] (rules by export index), in export order. A quota lifted
    (a T3a lift) is left to the card, which names it. RULINGS T15: `caution` where a
    NEARER water row binding this stream prints no such exemption (`nearer.silent_nearer`) — the lift still
    holds; the caution names both rows (`inherited_caution`), else None."""
    if not walked:
        return []
    from pipeline.deliver import types as TY
    st = ctx.store
    got: Dict[str, set] = {}
    for f in st.fish(k):
        for r, _s, rs, by, _lift in st.rows(st.verdict_id(k, reading, f, "none")):
            if rs is None or by is None or TY.by_code(TY.LossReason, rs).value != "lifted":
                continue
            L = st.rule_ids[by]
            if L in walked and inherited_kind(ctx.by_id[st.rule_ids[r]]) is not None:
                got.setdefault(L, set()).add(st.rule_ids[r])
    out = []
    ix = ctx.rule_index
    for L in sorted(got, key=ix.__getitem__):
        ts = sorted(got[L], key=ix.__getitem__)
        out.append({"says": inherited_line(L, ts, {"rules": ctx.by_id}), "by": ix[L],
                    "lifts": [ix[t] for t in ts],
                    "caution": inherited_caution(ctx.B, bound, L) if bound is not None else None})
    return out


def inherited_caution(B, bound, lifter: str) -> Optional[str]:
    """RULINGS T15: a farther ✱ row's exemption is applied on every one of its
    tributaries, the streams of a nearer row printing none included — and said: "Trout Lake’s row, nearer this
    stream, prints no such exemption; the Duncan River’s is applied here because its row covers all its
    tributaries." (Glenn Creek); the stream's own row: "Wilkie Creek’s own row prints no such exemption; …".
    None where every nearer row prints it too."""
    from pipeline.deliver.answers.common import the_name
    from pipeline.deliver.bundle import nearer as NR, read
    silent = NR.silent_nearer(bound, B.rules, read._nearer_of(B.path), lifter)
    if not silent:
        return None
    names = {e: x.get("entry_name") or e for (e, _r), x in B.rules.items()}

    def nm(e: str) -> str:
        n = re.sub(r"^the ", "", names.get(e) or e, flags=re.I)
        if n.isupper():                                  # "WHITETAIL LAKE'S INLET & OUTLET STREAMS"
            n = " ".join(w[:1].upper() + w[1:] for w in n.lower().split())
        return the_name(n)

    def poss(n: str) -> str:
        return f"{n}’" if n.endswith("s") else f"{n}’s"
    owner = nm(lifter.split("::", 1)[0])
    rows = [f"{poss(nm(e))} {'own ' if how == 'own_row' else ''}row" for e, how in silent]
    near = join(rows)
    if all(h == "own_row" for _e, h in silent):
        first = f"{cap(near)} prints no such exemption"
    else:
        first = f"{cap(near)}, nearer this stream, print{'s' if len(rows) == 1 else ''} no such exemption"
    return f"{first}; {poss(owner)} is applied here because its row covers all its tributaries."


def section_static(ctx, data: dict, guide: dict, keys, parts) -> dict:
    """The facts that are not keyed by (part key, segment): per export rule (kind, closure, bands,
    plain sentence) and per named water (each export part's names and picker facts)."""
    doc = ctx.doc
    if doc is None:
        raise AnswersError("display: the statics need the decoded export (`Context.doc`)")
    lic_ix = {k: i for i, k in enumerate(data.get("licensing_ids") or [])}
    rules = build_rules(ctx.B)
    # G3: the rows' frames, as built (`answers.build` hands every section's values over)
    rows = (ctx.cache.get("sections_out") or {}).get("rows")
    if rows is not None:
        order = sorted(ctx.B.index, key=ctx.B.index.__getitem__)
        add_subsets(rules, [ctx.B.rules[k] for k in order], (v for per in rows for v in per))
    order = sorted(ctx.B.index, key=ctx.B.index.__getitem__)
    add_sources(rules, doc, [f"{e}::{r}" for e, r in order])
    return {"rules": rules,
            "waters": produce_parts(ctx.B, doc, keys, parts, rule_ref=ctx.rule_index.__getitem__,
                                    lic_ref=lic_ix.__getitem__, log=lambda *_: None,
                                    store=ctx.store),
            "strip": strip_legend(), "reasons": reasons_table(),
            "licensing": licensing_books(doc, data.get("licensing_ids") or [])}
