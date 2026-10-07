"""THE PORT, ISOLATED: `rows.py` run on the PAGE's own ladder (its golden ladder states) against
the page's own card (its golden answers and rows).

What differs here is the arrangement, never the ladder. v1 matched the page on every card (12,818
cards, 376,518 answers, 89,463 rows). The rows fixes (rows.DECISIONS F1-F8) correct the page where
it contradicts the book, so the port now differs from the page EXACTLY where a documented page bug
shows; `classify_answer` / `classify_row` attribute every difference to one, and anything else is
`unexplained` (a port error).

  python -m pipeline.deliver.answers.reference.port BUNDLE EXPORT_DIR GOLDEN_DIR [N|all]

BUNDLE and EXPORT_DIR must be the ones the golden outputs were made from (regenerate.sh)."""
from __future__ import annotations

import collections
import copy
import glob
import gzip
import json
import random
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

from pipeline.deliver.answers.reference import compare as CMP

#: the page bugs the card's rows correct (rows.DECISIONS), as the port sees them
PAGE_BUGS = {
    "F1": "items from fact groups: a fish in two items, one range for fish with different ones "
          "(the real daily limit then reads overlapping items or skips the row)",
    "F2": "no origin2 line when the other origin differs only in its keep range "
          "('No wild trout over 50 cm')",
    "F3": "a line for as many OTHER fish as the row has read as everyone's (counts, not sets)",
    "F4": "a fish lifted out of the row's narrowing clause counted against the narrowed number",
    "F5": "keep bands without slots, outer size caps or the steelhead 50 cm; only the first shared "
          "cap",
    "F6": "a possession limit with a number read as a yearly limit",
}


def _norm_answer(a: Optional[dict]) -> Optional[dict]:
    """Our decided answer in the page's words for F6 (possession_cap -> annual / season)."""
    if a is None:
        return None
    a = copy.deepcopy(a)
    for l in a["lines"]:
        if l.get("t") == "possession_cap":
            l["t"] = "annual"
    for k, v in a["roles"].items():
        if v[0] == "possession_cap":
            v[0] = "season"
    return a


def classify_answer(page: Optional[dict], ours: Optional[dict]) -> Set[str]:
    """The documented classes that explain a differing decided answer (empty: identical)."""
    if page == ours:
        return set()
    if _norm_answer(ours) == page:
        return {"F6"}
    return {"unexplained"}


def _norm_row(page: dict, ours: dict) -> dict:
    """Our row projection with the F2 and F6 differences taken back to the page's form."""
    o = copy.deepcopy(ours)
    pfacts = {(l[0], l[1]) for l in page.get("everyone") or []} | {
        (t, r) for g in page.get("groups") or [] for t, r in g[1]}
    for l in o["everyone"]:
        if l[0] == "possession_cap":
            l[0] = "annual"
    o["everyone"] = [l for l in o["everyone"] if not (l[0] == "origin2"
                                                       and (l[0], l[1]) not in pfacts)]
    groups = []
    for ms, facts in o["groups"]:
        facts = [["annual" if t == "possession_cap" else t, r] for t, r in facts]
        facts = [[t, r] for t, r in facts if not (t == "origin2" and (t, r) not in pfacts)]
        if facts:
            groups.append([ms, facts])
    o["groups"] = groups
    return o


def classify_row(page: dict, ours: dict, full: dict) -> Set[str]:
    """The documented classes that explain a differing row. `full` is our row as shipped (items,
    conds, real_daily) — each class needs its own feature on the row, so a difference without
    one stays `unexplained`."""
    if page == ours:
        return set()
    why: Set[str] = set()
    o = _norm_row(page, ours)
    if o["everyone"] != ours["everyone"] or o["groups"] != ours["groups"]:
        if any(l[0] == "possession_cap" for l in ours["everyone"]) or any(
                t == "possession_cap" for _, fs in ours["groups"] for t, _ in fs):
            why.add("F6")
        if json.dumps(o) != json.dumps(ours) and (
                any(l[0] == "origin2" for l in ours["everyone"])
                or any(t == "origin2" for _, fs in ours["groups"] for t, _ in fs)):
            why.add("F2")
    if o["real_daily"] != page["real_daily"]:
        rd = full.get("real_daily") or {}
        feats = set()
        conds = full.get("conds") or []
        groups = [set(g["members"]) for g in full.get("groups") or []]
        # the page's items: one per fact group, one per condition naming other fish
        page_items = groups + [set(c["who"]) for c in conds
                               if c.get("who") and set(c["who"]) not in groups]
        overlap = any(a & b for i, a in enumerate(page_items) for b in page_items[i + 1:])
        if overlap or any(c.get("except") == ["RB"] for c in conds):
            feats.add("F1")
        ms = set(full["members"])
        if any(c.get("who") and len(c["who"]) == len(ms) and set(c["who"]) != ms for c in conds):
            feats.add("F3")
        if any(it.get("against") not in (None, full.get("daily")) for it in full.get("items") or []):
            feats.add("F4")
        if len(rd.get("shared_cap") or []) > 1 or any(
                x[2] == 0 for it in full.get("items") or [] for x in it.get("bands") or []) or any(
                l["t"] == "outersize" for l in full.get("everyone") or []) or any(
                "RB" in it["members"] and (it.get("bands") or [[0, None]])[-1][1] == 50
                for it in full.get("items") or []):
            feats.add("F5")
        why |= feats or {"unexplained"}
        o["real_daily"] = page["real_daily"]
    if o != page:
        why.add("unexplained")
    return why


def run(bundle: str, export_dir: str, golden: str, n: Optional[str] = "all",
        only_water: Optional[str] = None) -> dict:
    """Every golden card (or a seeded sample of `n`) through rows.py on the page's own ladder."""
    from pipeline.deliver.answers import cli, common
    from pipeline.deliver.answers import rows as RW
    B = common.load(bundle)
    data, guide = common.load_export(Path(export_dir))
    rule_index = common.check_export(B, data, guide)
    keys, parts = common.part_keys(B, data)
    pk2pi = {}
    for item, w in data["waters"].items():
        for pi in range(len(w["parts"])):
            pk2pi.setdefault((item, cli.part_key_string(data, item, pi)), pi)
    lad = collections.defaultdict(list)
    for f in glob.glob(golden + "/ladder*.jsonl.gz"):
        for line in gzip.open(f, "rt"):
            r = json.loads(line)
            lad[(r["w"], r["pk"])].append(r)
    recs = [json.loads(line) for f in sorted(glob.glob(golden + "/card*.jsonl.gz"))
            for line in gzip.open(f, "rt")]
    if only_water:
        recs = [r for r in recs if r["w"] == only_water]
    if n not in (None, "all"):
        random.Random(5).shuffle(recs)
        recs = recs[:int(n)]

    class Ctx:
        pass
    ctx = Ctx()
    ctx.B, ctx.sets, ctx.rules, ctx.bundle, ctx.cache = B, B.sets, B.rules, B.path, {}

    def page_ladder(rec):
        md = rec["md"]
        out = {}
        for x in lad[(rec["w"], rec["pk"])]:
            seg = int(x["seg_from"].replace("-", ""))
            if seg <= md:
                cur = out.get((x["fish"], x["origin"]))
                if cur is None or seg >= cur[0]:
                    out[(x["fish"], x["origin"])] = (seg, x["states"])
        v: Dict[str, dict] = collections.defaultdict(dict)
        for (S, o), (_, st) in out.items():
            v[S][o] = {k: [s[0], ["?"] if s[1] else None, None, s[2]] for k, s in st.items()}
        return v

    stats: collections.Counter = collections.Counter()
    classes: collections.Counter = collections.Counter()
    examples: Dict[str, List] = collections.defaultdict(list)
    for rec in recs:
        pi = pk2pi[(rec["w"], rec["pk"])]
        key = keys[parts[rec["w"]][pi]]
        rk, kd = common.rule_key(key), common.key_dict(key)
        md = (rec["md"] // 100, rec["md"] % 100)
        ladder = page_ladder(rec)
        for S in rec["spp"]:
            ladder.setdefault(S, {"hatchery": {}, "wild": {}})
        P = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"],
                    kd["steelhead"], ladder, md, RW.open_states(ctx, rk, md))
        out = RW.to_refs(json.loads(json.dumps(RW.produce(P))), rule_index.__getitem__)
        stats["cards"] += 1
        if out["spp"] != rec["spp"]:
            stats["spp_differ"] += 1
        for S, a in rec["answers"].items():
            for o in ("hatchery", "wild"):
                want = CMP._answer(a[o])
                got = cli.decided_projection(out["fish"][S][o], data["rule_ids"])
                stats["answers"] += 1
                why = classify_answer(want, got)
                for c in why:
                    classes["answer " + c] += 1
                    if len(examples["answer " + c]) < 5:
                        examples["answer " + c].append((rec["w"], rec["date"], S, o))
                if why:
                    stats["answers_differ"] += 1
        gv = {}
        for row in rec["rows"]:
            rd = row.get("real_daily")
            gv[(row["kind"], row.get("pool") or row.get("win"))] = {
                "kind": row["kind"], "pool": row.get("pool"), "win": row.get("win"),
                "members": row.get("members"), "all_members": row.get("allMembers"),
                "daily": CMP._num(row.get("daily")) if row.get("pool") else None,
                "real_daily": None if not rd else [rd["n"], rd["all"], rd.get("sum"),
                                                   rd["cappedSum"], rd["rb"]],
                "everyone": [[l["t"], l["r"], sorted(l["members"])] for l in row.get("everyone") or []],
                "groups": [[sorted(g["members"]), [[l["t"], l["r"]] for l in g["facts"]]]
                           for g in row.get("groups") or []]}
        ov, full = {}, {}
        for r in out["rows"]:
            k = (r["kind"], data["rule_ids"][r["pool"] if r["pool"] is not None else r["win"]])
            ov[k] = cli.row_projection(r, data["rule_ids"])
            full[k] = r
        stats["rows"] += len(gv)
        if list(gv) != list(ov):
            stats["row_order_or_set_differ"] += 1
            classes["row unexplained"] += 1
            examples["row unexplained"].append((rec["w"], rec["date"], "order/set"))
        for k in gv:
            if k not in ov:
                continue
            why = classify_row(gv[k], ov[k], full[k])
            if why:
                stats["rows_differ"] += 1
            for c in why:
                classes["row " + c] += 1
                if len(examples["row " + c]) < 5:
                    examples["row " + c].append((rec["w"], rec["pk"], rec["date"], list(k)))
    return {"stats": dict(stats), "classes": dict(classes), "examples": dict(examples)}


def main(argv=None):
    a = (argv or sys.argv[1:])
    rep = run(a[0], a[1], a[2], a[3] if len(a) > 3 else "all", a[4] if len(a) > 4 else None)
    print(json.dumps(rep, indent=1, default=str))


if __name__ == "__main__":
    main()
