"""THE PORT, ISOLATED: `rows.py` run on the PAGE's own ladder (its golden ladder states) against
the page's own card (its golden answers and rows).

What differs here is the arrangement, never the ladder. v1 matched the page on every card (12,818
cards, 376,518 answers, 89,463 rows). The rows fixes (rows.DECISIONS F1-F8) correct the page where
it contradicts the book, so the port now differs from the page EXACTLY where a documented page bug
shows; `classify_answer` / `classify_row` attribute every difference to one, and anything else is
`unexplained` (a port error, or a finding about the answers layer).

The rows read the STORED verdicts (`rows.open_states`), so BUNDLE needs its `verdicts.sqlite` beside
it. `pipeline/tests/test_answers_reference.py` pins this at zero unexplained (slow, needs_bundle).

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
          "(the real daily limit then reads overlapping items or skips the row); every fish no "
          "group or condition names lumped into one `rest` item",
    "F2": "no origin2 line when the other origin differs only in its keep range "
          "('No wild trout over 50 cm')",
    "F3": "a line for as many OTHER fish as the row has read as everyone's (counts, not sets)",
    "F4": "a fish lifted out of the row's narrowing clause counted against the narrowed number",
    "F5": "keep bands without slots, outer size caps or the steelhead 50 cm; only the first shared "
          "cap",
    "F6": "a possession limit with a number read as a yearly limit",
    "F11": "a count limit shared by several kinds ('3 Dolly Varden and lake trout combined') "
           "summed per kind in the real daily limit, not given to them together",
    "F16": "an orphan count clause (another table's, still speaking) not read as a cap on its fish "
           "(rows.DECISIONS F16, RULINGS Q2)",
    "R15.3": "a rule of the winner's own row saying the very same thing shown as a second "
             "('also') line, not as the same statement agreeing with the winner (rows.py, "
             "ROUND15 3, owner)",
    "B9.4": "a deciding record-it rule (`record_retention`: Kootenay Lake's 'Write each rainbow trout over "
            "50 cm on your licence', Shuswap's two stamp records) shown under its deciding role (`floor`), "
            "where the answers store it as `duty` as page v36 does (rows.stored_role, rows 31, owner "
            "2026-10-21)",
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


def restated(page: Optional[dict], ours: Optional[dict]) -> Set[str]:
    """ROUND15 3: the rules our answer has AGREE with its winner where the page says `also`."""
    if not page or not ours:
        return set()
    return {r for r, v in ours["roles"].items()
            if v == ["agrees", ours["winner"]] and page["roles"].get(r) == ["also", None]}


def recorded(page: Optional[dict], ours: Optional[dict], record_rules: Set[str]) -> Set[str]:
    """B9.4 (rows.stored_role, rows 31): the record-it rules (`record_rules`, ids of the rules with
    `record_retention`) our answer stores as `duty` where the page gives them a deciding role other
    than duty (display WINS: v35 ranks them, v36 lists them under "Other rules"), held by the same
    rule (`by`) — the computation kept its role, only the stored answer says duty."""
    if not page or not ours or not record_rules:
        return set()
    from pipeline.deliver.answers.display import WINS
    return {r for r, v in ours["roles"].items()
            if r in record_rules and v[0] == "duty" and r in page["roles"]
            and page["roles"][r][0] in WINS and page["roles"][r][0] != "duty"
            and page["roles"][r][1] == v[1]}


def classify_answer(page: Optional[dict], ours: Optional[dict],
                    record_rules: Set[str] = frozenset()) -> Set[str]:
    """The documented classes that explain a differing decided answer (empty: identical).
    `record_rules`: the ids of the rules with `record_retention` (B9.4)."""
    if page == ours:
        return set()
    rec = recorded(page, ours, record_rules)
    if rec:
        back = {**ours, "roles": {**ours["roles"], **{r: list(page["roles"][r]) for r in rec}}}
        why = classify_answer(page, back)
        return {"unexplained"} if "unexplained" in why else why | {"B9.4"}
    n = _norm_answer(ours)
    if n == page:
        return {"F6"}
    rs = restated(page, ours)
    if rs:
        for r in rs:
            n["roles"][r] = ["also", None]
        if n == {**page, "lines": [l for l in page["lines"]
                                   if not (l.get("t") == "also" and l.get("r") in rs)]}:
            return {"R15.3"} | ({"F6"} if _norm_answer(ours) != ours else set())
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


def _drop_also(page: dict, rules: Set[str]) -> dict:
    """The page's row without its `also` facts for `rules` (ROUND15 3: said once, by the winner)."""
    keep = lambda t, r: not (t == "also" and r in rules)
    return {**page, "everyone": [l for l in page.get("everyone") or [] if keep(l[0], l[1])],
            "groups": [[ms, fs] for ms, fs in ([ms, [f for f in fs if keep(f[0], f[1])]]
                                               for ms, fs in page.get("groups") or []) if fs]}


def _rest_differs(full: dict) -> bool:
    """F1, the page's `rest` item: page_v35.js `speciesItems` puts every fish of the row that no
    fact group or condition names into ONE trailing item, and `quotaLines` gives that item one range
    (a stream share only when it covers every member). Ours gives each kind its own: the feature is
    rest fish to which our items give DIFFERENT bands (Kitimat River, Jul 1-Oct 31: brown and
    cutthroat 1 under '1 trout from streams', brook trout, a char, 5)."""
    named = {S for g in full.get("groups") or [] for S in g["members"]} | {
        S for c in full.get("conds") or [] if c.get("who") for S in c["who"]}
    bands = {S: json.dumps(it.get("bands")) for it in full.get("items") or [] for S in it["members"]}
    rest = [S for S in full.get("all_members") or full["members"] if S not in named and S in bands]
    return len({bands[S] for S in rest}) > 1


def _orphan_capped(full: dict) -> bool:
    """F16: a fish the real daily limit caps is capped BY an orphan fact — the count clause our
    `rows.fish_bands` caps it with and the page's `quotaLines` never reads. The orphan must bind
    the fish (`binds`, fish_bands' own gate: `orphan_takes`) and its take must be the fish's cap
    (the top of its item's bands): an orphan above a cap something else sets explains nothing."""
    capped = set((full.get("real_daily") or {}).get("capped") or [])
    top = {S: max(x[2] for x in it["bands"]) for it in full.get("items") or []
           if it.get("bands") for S in it["members"]}
    facts = list(full.get("everyone") or []) + [l for g in full.get("groups") or []
                                                for l in g["facts"]]
    return any(l["t"] == "orphan" and l.get("take") is not None
               and top.get(S) == l["take"]
               for l in facts for S in capped & set(l.get("binds") or ())
               & set(l.get("members") or full["members"]))


def orphan_takes(out: dict, P) -> dict:
    """Each orphan fact of `produce`'s rows given, in place, its clause's take (`P.by_key[rule]`)
    and the fish it `binds`: those of the fact (else the row) for which `rows.fish_bands` lets the
    orphan cap at all — its water None or the part's kind, its dates holding, and it covering the
    fish. A fact ships neither (the rule holds them), and F16 needs both. Port tooling only — the
    row projection compared with the page does not read them."""
    from pipeline.deliver.answers.rows import _covers
    for row in out["rows"]:
        for l in list(row.get("everyone") or []) + [l for g in row.get("groups") or []
                                                     for l in g["facts"]]:
            if l["t"] == "orphan":
                r = P.by_key[l["r"]]
                l["take"] = r.f.get("take")
                live = r.f.get("water") in (None, P.kind) and P.in_dates(r)
                l["binds"] = [S for S in (l.get("members") or row.get("members") or [])
                              if live and _covers(r, S)]
    return out


def classify_row(page: dict, ours: dict, full: dict, restated_rules: Set[str] = frozenset()
                 ) -> Set[str]:
    """The documented classes that explain a differing row. `full` is our row as shipped (items,
    conds, real_daily); `restated_rules` the card's ROUND15 3 rules (`restated`, read on its
    answers) — each class needs its own feature on the row, so a difference without one stays
    `unexplained`."""
    if page == ours:
        return set()
    why: Set[str] = set()
    if restated_rules:
        dropped = _drop_also(page, set(restated_rules))
        if dropped != page:
            why.add("R15.3")
            page = dropped
            if page == ours:
                return why
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
        if overlap or any(c.get("except") == ["RB"] for c in conds) or _rest_differs(full):
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
        if rd.get("shared_count"):
            feats.add("F11")
        if _orphan_capped(full):
            feats.add("F16")
        why |= feats or {"unexplained"}
        o["real_daily"] = page["real_daily"]
    if o != page:
        why.add("unexplained")
    return why


def row_key(r: dict, rule_ids: list) -> tuple:
    """Our row's key in the page's words: (kind, pool or winner rule id); the page says "nolimit"
    where we say "no_limit" (as `cli.page_view` keys it)."""
    kind = "nolimit" if r["kind"] == "no_limit" else r["kind"]
    return (kind, rule_ids[r["pool"] if r["pool"] is not None else r["win"]])


def _page_moment(moments: list):
    """The moment the page's card is read at. The page knew no moments: a key that reads by weekday
    or hour (answers 2.1) is read on a Monday, outside any hours window — as `cli.page_view` does;
    a key with one moment is read at none (None)."""
    if len(moments) == 1:
        return None
    for m in moments:
        if m.weekdays & 1 and not m.inside:            # Monday = bit 0 (calendar.weekday_mask)
            return m
    raise ValueError(f"port: no moment holds a Monday outside the hours window: {moments}")


def run(bundle: str, export_dir: str, golden: str, n: Optional[str] = "all",
        only_water: Optional[str] = None) -> dict:
    """Every golden card (or a seeded sample of `n`) through rows.py on the page's own ladder."""
    from pipeline.deliver.answers import cli, common
    from pipeline.deliver.answers import rows as RW
    from pipeline.deliver.answers.answers import Context
    data, guide = common.load_export(Path(export_dir))
    # the rows read the STORED verdicts (open_states: `ctx.store`), as the answers build does
    ctx = Context(bundle, {}, verdicts=str(Path(bundle).with_name("verdicts.sqlite")))
    B = ctx.B
    rule_index = ctx.rule_index = common.check_export(B, data, guide)
    # B9.4: the rules a record-it duty is stored for (rows.stored_role)
    record_rules = {f"{e}::{r}" for (e, r), x in B.rules.items() if x.get("record_retention")}
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
        at = _page_moment(ctx.store.moments(B.key_ix[rk]))
        P = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"],
                    kd["steelhead"], ladder, md, RW.open_states(ctx, rk, md, at), at=at)
        out = RW.refs(orphan_takes(json.loads(json.dumps(RW.produce(P))), P),
                      rule_index.__getitem__)
        stats["cards"] += 1
        if out["spp"] != rec["spp"]:
            stats["spp_differ"] += 1
        card_restated: Set[str] = set()
        for S, a in rec["answers"].items():
            for o in ("hatchery", "wild"):
                want = CMP._answer(a[o])
                got = cli.decided_projection(out["fish"][S][o], data["rule_ids"])
                stats["answers"] += 1
                why = classify_answer(want, got, record_rules)
                card_restated |= restated(want, got)
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
            k = row_key(r, data["rule_ids"])
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
            why = classify_row(gv[k], ov[k], full[k], card_restated)
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
