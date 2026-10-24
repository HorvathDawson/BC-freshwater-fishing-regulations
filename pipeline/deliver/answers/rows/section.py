"""The `rows` section of the answers file: per rule key and day, the card's rows derived from the ladder
(`section_derive`), their open states and references, and the gate that refuses a row with no answer."""
from __future__ import annotations

import json
from typing import Dict, List
from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.calendar import month_day
from pipeline.common import entry_id as EID
from pipeline.deliver.answers.rows.part import Part, open_fish
from pipeline.deliver.answers.rows.assemble import produce


def section_scope(key: tuple, B):
    """The card reads the rule key, the water's kind (which rules apply, a stream share holding
    here) and the steelhead presence (whether ST is asked about, the presence line)."""
    k = common.key_dict(key)
    return (common.rule_key(key), k["kind"], k["steelhead"])


def open_states(ctx, rk, md, at=None) -> dict:
    """Decision R5: the reader's state of every open-subject gate rule of the set, asked about the
    subject's first named fish: {rule id: (state, lifted_in_part_by)} — the STORED verdicts
    (`verdicts.sqlite`, origin not known; the verdicts ask every fish a member rule names)."""
    from pipeline.deliver.calendar import day_of
    from pipeline.regs.parsing.catalogue import OPEN_SUBJECTS, PROTECTED_FISH
    ck = ("rows_open", rk, md, at)
    got = ctx.cache.get(ck)
    if got is not None:
        return got
    bound = ctx.sets[rk.set_id]
    by_fish: Dict[str, List[str]] = {}
    for e, r, _ in bound:
        x = ctx.rules[(e, r)]
        sp = x.get("species") or []
        if x.get("type") == "retention_limit" and sp and all(
                (c in OPEN_SUBJECTS and c != "ALL_FIN_FISH") or c in PROTECTED_FISH for c in sp):
            by_fish.setdefault(open_fish(x), []).append(f"{e}::{r}")
    out = {}
    key = ctx.B.key_ix[rk]
    ms = ctx.store.moments(key)
    reading = ctx.store.reading_of(key, day_of(md), None if len(ms) == 1 else ms.index(at))
    for f, ks in sorted(by_fish.items()):
        ans = ctx.ladder_dict(ctx.store.verdict_id(key, reading, f, "none"))
        for k in ks:
            y = ans.get(k)
            if y is None:
                raise AnswersError(f"rows: the verdict does not hold {k} asked about {f}")
            out[k] = (y[0], y[1])
    ctx.cache[ck] = out
    return out


def refs(out: dict, ix) -> dict:
    """`produce`'s rows with every rule named by its export index (`ix("entry::rule")`) — FIELD
    BY FIELD, the fields that hold a rule (L1: a blind scan turned any string holding "::" into a
    rule, a caution's text included)."""
    def res(d):
        if d is None:
            return None
        return {**d, "win": ix(d["win"]), "narrow": None if d["narrow"] is None else ix(d["narrow"]),
                "lines": [line(l) for l in d["lines"]],
                "roles": [[ix(k), role, None if by is None else ix(by)] for k, role, by in d["roles"]],
                "lift_notes": [[ix(by), q] for by, q in d["lift_notes"]]}

    def line(l):
        l = {**l, "r": ix(l["r"])}
        if "outer" in l:
            l["outer"] = ix(l["outer"])
        if "rules" in l:
            l["rules"] = [ix(r) for r in l["rules"]]
        return l

    from pipeline.deliver.answers.tree import map_rules

    def row(r):
        o = {**r, "pool": None if r["pool"] is None else ix(r["pool"]),
             "win": None if r["win"] is None else ix(r["win"]),
             "narrow": None if r["narrow"] is None else ix(r["narrow"]),
             "everyone": [line(l) for l in r["everyone"]],
             "groups": [{**g, "facts": [line(l) for l in g["facts"]]} for g in r["groups"]],
             "wins": None if r["wins"] is None else [ix(w) for w in r["wins"]],
             "lift_notes": [[ix(by), q] for by, q in r["lift_notes"]]}
        if r.get("restates"):
            o["restates"] = [{**x, "r": ix(x["r"]), "of": ix(x["of"])} for x in r["restates"]]
        if "conds" in r:
            o["conds"] = [{**c, "r": None if c.get("r") is None else ix(c["r"])} for c in r["conds"]]
        if r.get("group") is not None:
            g = r["group"]
            o["group"] = {**g, "pool": ix(g["pool"]),
                          "subcaps": [{**s, "r": ix(s["r"]),
                                       "lifted": [{**L, "by": ix(L["by"])} for L in s["lifted"]]}
                                      for s in g["subcaps"]],
                          "lifts": [{**L, "by": ix(L["by"]),
                                     "each": [{**e, "by": ix(e["by"])} for e in L["each"]]}
                                    for L in g["lifts"]],
                          "buckets": [{**b, "by": ix(b["by"]),
                                       "groups": [{**s, "by": ix(s["by"])} for s in b["groups"]],
                                       "sizes": [{**s, "by": ix(s["by"])} for s in b.get("sizes") or []],
                                       "replaces": None if b["replaces"] is None else
                                       {**b["replaces"], "rules": [ix(k) for k in b["replaces"]["rules"]]}}
                                      for b in g["buckets"]],
                          "not_kept": [{**x, "by": ix(x["by"]), "rules": [ix(k) for k in x["rules"]]}
                                       for x in g["not_kept"]],
                          "worked": [{**w, "share": None if w["share"] is None else ix(w["share"])}
                                     for w in g["worked"]],
                          "states": [{**x, "r": ix(x["r"])} for x in g["states"]],
                          "caps": [{**c, "by": ix(c["by"])} for c in g["caps"]],
                          "tree": map_rules(g["tree"], ix),
                          "possession": None if g.get("possession") is None else
                          {**g["possession"], "by": ix(g["possession"]["by"])}}
        return o
    return {**out, "fish": {S: {"hatchery": res(v["hatchery"]), "wild": res(v["wild"]), "lead": v["lead"]}
                            for S, v in out["fish"].items()},
            "rows": [row(r) for r in out["rows"]]}


def section_derive(ladder_value: dict, scope, ctx, first_day: int, at=None) -> dict:
    """The card's rows for one part scope on one reading (the ladder's: its first day and the
    moment it is asked at), rules by export index."""
    rk, kind, presence = scope
    md = month_day(first_day)
    P = Part(ctx.B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kind, presence,
             ladder_value, md, open_states(ctx, rk, md, at), at=at)
    out = produce(P)
    no_answer_gate(out, ladder_value, ctx.by_id, f"rule set {rk.set_id}, {md}")
    return refs(json.loads(json.dumps(out)), ctx.rule_index.__getitem__)


def no_answer_gate(out: dict, ladder_value: dict, by_id: Dict[str, dict], where: str) -> None:
    """EVERY FISH THE ZONE HAS A RULE FOR HAS AN ANSWER (answers 2.6). A decided answer is null
    only where no rule in scope speaks; it is REFUSED where a zone or provincial table's quota or
    release for the fish (`retention_limit` of a `z…` entry) took part in the competition — it
    speaks, or was displaced — and still nothing decides. That is a reader that lost every rule (a
    cycle: Clearwater Creek gnis:7628, Arctic grayling, before `read._step4_competition` broke it
    at the own row), never an answer."""
    for S, v in out["fish"].items():
        for o in ("hatchery", "wild"):
            if v[o] is not None:
                continue
            lad = (ladder_value.get(S) or {}).get(o) or {}
            took = sorted(r for r, (st, *_rest) in lad.items()
                          if st in ("speaks", "displaced") and EID.is_table(r)
                          and (by_id.get(r) or {}).get("type") == "retention_limit")
            if took:
                raise AnswersError(f"rows: {S} ({o}) has no decided answer at {where}, but the "
                                   f"zone's {took[:3]} took part — a reader that lost every rule")
