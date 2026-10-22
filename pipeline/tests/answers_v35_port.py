"""THE PORT CHECK AGAINST THE CONSUMER PAGE v35, AS A GOLDEN FIXTURE (audit F12, owner 2026-10-22).

The page v35 harness (page_v35.js run in node, `regenerate.sh`, `port.py`) is archived in
`archive/pipeline/answers-reference-v35/`. What it proved is kept here without it: `rows.py`, run on the PAGE's
own ladder for a card, reproduces the PAGE's own card — every decided answer and every row — except where a
documented page bug shows (`PAGE_BUGS`, rows.DECISIONS). `fixtures/answers_v35_port.json.gz` freezes, for a
chosen set of the page's golden cards, everything `rows.produce` reads (each bound rule's bundle record, the
part's kind and steelhead facts, the page's ladder on that date, the stored open-subject states, the moment)
beside what the page showed. `test_answers_v35_port.py` re-runs rows.py on it: no bundle, no node, no harness.

The classifier (`classify_answer`, `classify_row` and their helpers) and the page-word projections
(`decided_projection`, `row_projection`, `part_key_string`) are the harness's own code, moved here verbatim from
`reference/port.py` and `answers/cli.py` (where only the archived `page-view` command used them).

MAKE (once, from a golden made by the archived harness for the LIVE export; ~1-2 min, one process, reads the
bundle + verdicts; no node, no credits):

    python -m pipeline.tests.answers_v35_port BUNDLE EXPORT_DIR GOLDEN_DIR \
        pipeline/tests/fixtures/answers_v35_port.json.gz [N_CLEAN=60]

It runs the port over EVERY golden card first and refuses to write when any difference is unexplained, then
verifies the fixture: read back from its own JSON (a stub bundle), every chosen card gives the very output and
classes of the live run.
"""
from __future__ import annotations

import collections
import copy
import glob
import gzip
import json
import random
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List, Optional, Set

FIXTURE = Path(__file__).parent / "fixtures" / "answers_v35_port.json.gz"
#: the stub bundle's path: `read._nearer_of` answers it from `read._NEARER_BY_PATH` (the fixture's pairs)
STUB_PATH = "fixture:answers_v35_port"
#: cards chosen per combination of documented classes (every combination the live run saw is kept)
PER_CLASS = 3
#: a committed fixture stays small: the maker refuses a bigger one (lower N_CLEAN)
MAX_BYTES = 1_500_000


# ------------------------------------------------------------------------------------------------
# the page's golden records, in the meeting point's words (archived reference/compare.py, verbatim)
# ------------------------------------------------------------------------------------------------
def page_answer(res: dict | None) -> dict | None:
    if res is None:
        return None
    return {"status": res["status"], "daily": res.get("daily"), "winner": res["win"],
            "narrow": res.get("narrow"),
            "lines": [{k: v for k, v in l.items() if k in ("t", "r", "a", "b", "take", "o", "status", "daily", "outer", "only")}
                      for l in res.get("lines", [])],
            "roles": {k: [v["role"], v.get("by")] for k, v in res.get("roles", [])}}


def page_num(v):
    return "Infinity" if v in ("Infinity", float("inf")) else v


# ------------------------------------------------------------------------------------------------
# our rows in the page's words (moved verbatim from answers/cli.py)
# ------------------------------------------------------------------------------------------------
def part_key_string(data: dict, item: str, pi: int) -> str:
    """The harness's `partKey` of an export part (golden.js, archived): how a golden card names its part."""
    arr = data["waters"][item]["parts"][pi]
    f = arr[4] if len(arr) > 4 else {}
    return "|".join(["" if arr[0] is None else str(arr[0]), "" if arr[1] is None else str(arr[1]),
                     "sw" if f.get("anadromous_rainbow") else "",
                     f.get("steelhead") or "", "sr0" if f.get("steelhead_rules") is False else "",
                     "+".join(f.get("province_except") or []), ",".join(f.get("home_region") or [])])


_LINE_KEYS = ("t", "r", "a", "b", "take", "o", "status", "daily", "outer", "only")


def _inf(v):
    return "Infinity" if v is None else v


def decided_projection(d, rules: list) -> dict | None:
    """Our rows section's decided answer for one fish and origin, as the page's evalSp states it."""
    if d is None:
        return None
    lines = []
    for l in d["lines"]:
        x = {}
        for k in _LINE_KEYS:
            if k not in l:
                continue
            v = l[k]
            if k in ("r", "outer"):
                v = rules[v]
            elif k == "b":
                v = _inf(v)
            elif k == "daily" and l["t"] in ("xref", "origin2"):
                v = _inf(v)
            x[k] = v
        lines.append(x)
    # the page's words: its "nolimit" is answers/2's `no_limit`
    page_status = "nolimit" if d["status"] == "no_limit" else d["status"]
    return {"status": page_status, "daily": "Infinity" if d["status"] == "no_limit" else d["daily"],
            "winner": rules[d["win"]], "narrow": None if d["narrow"] is None else rules[d["narrow"]],
            "lines": lines,
            "roles": {rules[k]: [role, None if by is None else rules[by]] for k, role, by in d["roles"]}}


def row_projection(r: dict, rules: list) -> dict:
    rd = r.get("real_daily")
    return {"kind": "nolimit" if r["kind"] == "no_limit" else r["kind"],
            "pool": None if r["pool"] is None else rules[r["pool"]],
            "win": None if r["win"] is None else rules[r["win"]],
            "members": r["members"], "all_members": r["all_members"],
            "daily": (("Infinity" if r["kind"] == "no_limit" else r["daily"])
                      if r["pool"] is not None else None),
            "real_daily": None if not rd else [rd["n"], rd["all"], rd["sum"], rd["capped_sum"],
                                               rd["rb"]],
            "everyone": [[l["t"], rules[l["r"]], sorted(l["members"])] for l in r["everyone"]],
            "groups": [[sorted(g["members"]), [[l["t"], rules[l["r"]]] for l in g["facts"]]]
                       for g in r["groups"]]}


# ------------------------------------------------------------------------------------------------
# the classifier (moved verbatim from the archived reference/port.py)
# ------------------------------------------------------------------------------------------------
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
    where we say "no_limit" (as the archived `cli page-view` keyed it)."""
    kind = "nolimit" if r["kind"] == "no_limit" else r["kind"]
    return (kind, rule_ids[r["pool"] if r["pool"] is not None else r["win"]])


def page_moment(moments: list):
    """The moment the page's card is read at. The page knew no moments: a key that reads by weekday
    or hour (answers 2.1) is read on a Monday, outside any hours window — as the archived `cli page-view` did;
    a key with one moment is read at none (None)."""
    if len(moments) == 1:
        return None
    for m in moments:
        if m.weekdays & 1 and not m.inside:            # Monday = bit 0 (calendar.weekday_mask)
            return m
    raise ValueError(f"port: no moment holds a Monday outside the hours window: {moments}")


# ------------------------------------------------------------------------------------------------
# one card: the frozen inputs, rows.py on them, the classes
# ------------------------------------------------------------------------------------------------
def page_ladder(lad_recs: list, md: int, spp: list) -> dict:
    """The page's own ladder for a card on day `md` (MMDD): each fish and origin's latest golden segment
    starting on or before it (archived port.run `page_ladder`)."""
    out = {}
    for x in lad_recs:
        seg = int(x["seg_from"].replace("-", ""))
        if seg <= md:
            cur = out.get((x["fish"], x["origin"]))
            if cur is None or seg >= cur[0]:
                out[(x["fish"], x["origin"])] = (seg, x["states"])
    v: Dict[str, dict] = collections.defaultdict(dict)
    for (S, o), (_, st) in out.items():
        v[S][o] = {k: [s[0], ["?"] if s[1] else None, None, s[2]] for k, s in st.items()}
    for S in spp:
        v.setdefault(S, {"hatchery": {}, "wild": {}})
    return dict(v)


def page_rows(rec: dict) -> dict:
    """The page's rows of a golden card, keyed (kind, pool or winner), in the meeting point's words."""
    gv = {}
    for row in rec["rows"]:
        rd = row.get("real_daily")
        gv[(row["kind"], row.get("pool") or row.get("win"))] = {
            "kind": row["kind"], "pool": row.get("pool"), "win": row.get("win"),
            "members": row.get("members"), "all_members": row.get("allMembers"),
            "daily": page_num(row.get("daily")) if row.get("pool") else None,
            "real_daily": None if not rd else [rd["n"], rd["all"], rd.get("sum"), rd["cappedSum"], rd["rb"]],
            "everyone": [[l["t"], l["r"], sorted(l["members"])] for l in row.get("everyone") or []],
            "groups": [[sorted(g["members"]), [[l["t"], l["r"]] for l in g["facts"]]]
                       for g in row.get("groups") or []]}
    return gv


def run_card(B, inp: dict, rule_index: dict, all_rules: Optional[Set[str]] = None) -> dict:
    """`rows.produce` on one card's inputs (`card_inputs`' shape), rule-referenced as the answers ship them."""
    from pipeline.deliver.answers import rows as RW
    from pipeline.deliver.calendar import Moment
    B.sets = {0: [tuple(t) for t in inp["set"]]}
    at = None if inp["at"] is None else Moment(*inp["at"])
    P = RW.Part(B, 0, inp["sw"], inp["sr"], inp["kind"], inp["presence"], inp["ladder"], tuple(inp["md"]),
                {k: tuple(v) for k, v in inp["open"].items()}, at=at)
    if all_rules is not None:
        P.all_rules = all_rules
    return RW.refs(orphan_takes(json.loads(json.dumps(RW.produce(P))), P), rule_index.__getitem__)


def classify_card(page: dict, out: dict, rule_ids: list, record_rules: Set[str]):
    """(classes, stats) of one card: our `out` against the page's golden card `page` (archived port.run)."""
    classes: collections.Counter = collections.Counter()
    st: collections.Counter = collections.Counter(cards=1)
    if out["spp"] != page["spp"]:
        st["spp_differ"] += 1
    card_restated: Set[str] = set()
    for S, a in page["answers"].items():
        for o in ("hatchery", "wild"):
            want = page_answer(a[o])
            got = decided_projection(out["fish"][S][o], rule_ids)
            st["answers"] += 1
            why = classify_answer(want, got, record_rules)
            card_restated |= restated(want, got)
            for c in why:
                classes["answer " + c] += 1
            if why:
                st["answers_differ"] += 1
    gv = page_rows(page)
    ov, full = {}, {}
    for r in out["rows"]:
        k = row_key(r, rule_ids)
        ov[k] = row_projection(r, rule_ids)
        full[k] = r
    st["rows"] += len(gv)
    if list(gv) != list(ov):
        st["row_order_or_set_differ"] += 1
        classes["row unexplained"] += 1
    for k in gv:
        if k not in ov:
            continue
        why = classify_row(gv[k], ov[k], full[k], card_restated)
        if why:
            st["rows_differ"] += 1
        for c in why:
            classes["row " + c] += 1
    return classes, st


def stub(fx: dict):
    """The fixture's stub bundle: its rules (keyed (entry, rule), as `rows.Part` reads `B.rules`) and its
    `trib_nearer` pairs, seeded where `read._nearer_of` looks for a path that is no file."""
    from pipeline.deliver.bundle import read
    read._NEARER_BY_PATH[STUB_PATH] = frozenset(tuple(p) for p in fx["nearer"])
    return SimpleNamespace(rules={tuple(k.split("::", 1)): v for k, v in fx["rules"].items()}, sets={},
                           path=STUB_PATH)


def load() -> dict:
    return json.loads(gzip.decompress(FIXTURE.read_bytes()))


# ------------------------------------------------------------------------------------------------
# MAKE: from a golden of the live export (the archived harness made it), with the live bundle
# ------------------------------------------------------------------------------------------------
def card_inputs(ctx, B, keys, parts, pk2pi, lad, rec) -> dict:
    """Everything `rows.produce` reads for one golden card, as JSON."""
    from pipeline.deliver.answers import common
    from pipeline.deliver.answers import rows as RW
    pi = pk2pi[(rec["w"], rec["pk"])]
    key = keys[parts[rec["w"]][pi]]
    rk, kd = common.rule_key(key), common.key_dict(key)
    md = (rec["md"] // 100, rec["md"] % 100)
    at = page_moment(ctx.store.moments(B.key_ix[rk]))
    opens = RW.open_states(ctx, rk, md, at)
    return {"set": [list(t) for t in B.sets[rk.set_id]], "sw": rk.steelhead_water, "sr": rk.steelhead_rules,
            "kind": kd["kind"], "presence": kd["steelhead"],
            "ladder": page_ladder(lad[(rec["w"], rec["pk"])], rec["md"], rec["spp"]), "md": list(md),
            "at": None if at is None else list(at), "open": {k: list(v) for k, v in opens.items()}}


def make(bundle: str, export_dir: str, golden: str, out_path: str, n_clean: int = 60) -> dict:
    from pipeline.deliver.answers import common
    from pipeline.deliver.answers.answers import Context
    from pipeline.deliver.bundle import read
    data, guide = common.load_export(Path(export_dir))
    ctx = Context(bundle, {}, verdicts=str(Path(bundle).with_name("verdicts.sqlite")))
    B = ctx.B
    rule_index = ctx.rule_index = common.check_export(B, data, guide)
    rule_ids = data["rule_ids"]
    record_rules = {f"{e}::{r}" for (e, r), x in B.rules.items() if x.get("record_retention")}
    keys, parts = common.part_keys(B, data)
    pk2pi = {}
    for item, w in data["waters"].items():
        for pi in range(len(w["parts"])):
            pk2pi.setdefault((item, part_key_string(data, item, pi)), pi)
    lad = collections.defaultdict(list)
    for f in sorted(glob.glob(golden + "/ladder*.jsonl.gz")):
        for line in gzip.open(f, "rt"):
            r = json.loads(line)
            lad[(r["w"], r["pk"])].append(r)
    recs = [json.loads(line) for f in sorted(glob.glob(golden + "/card*.jsonl.gz")) for line in gzip.open(f, "rt")]
    manifest = json.loads((Path(golden) / "manifest.json").read_text())
    live = SimpleNamespace(rules=B.rules, sets={}, path=B.path)

    # 1. the port over EVERY golden card, as the archived port.run counted it
    total, stats, by_combo, clean, inputs = collections.Counter(), collections.Counter(), {}, [], {}
    for i, rec in enumerate(recs):
        inp = card_inputs(ctx, B, keys, parts, pk2pi, lad, rec)
        classes, st = classify_card(rec, run_card(live, inp, rule_index), rule_ids, record_rules)
        total.update(classes)
        stats.update(st)
        combo = tuple(sorted(classes))
        (by_combo.setdefault(combo, []) if combo else clean).append(i)
    print("port over every golden card:", dict(stats), dict(total), file=sys.stderr)
    if stats["cards"] != manifest["counts"]["card"] or stats.get("spp_differ") or \
            any("unexplained" in k for k in total):
        raise SystemExit(f"answers_v35_port: the live port is not clean — nothing written: {dict(stats)} {dict(total)}")

    # 2. the chosen cards: every class combination (PER_CLASS each) + a seeded sample that agrees
    rnd = random.Random(5)
    chosen = sorted({i for ix in by_combo.values() for i in rnd.sample(ix, min(PER_CLASS, len(ix)))}
                    | set(rnd.sample(clean, min(n_clean, len(clean)))))
    cards, used = [], set()
    for i in chosen:
        rec = recs[i]
        inp = card_inputs(ctx, B, keys, parts, pk2pi, lad, rec)
        used |= {(e, r) for e, r, _ in inp["set"]}
        cards.append({"w": rec["w"], "pk": rec["pk"], "date": rec["date"], **inp,
                      "page": {"spp": rec["spp"], "answers": rec["answers"], "rows": rec["rows"]}})
    entries = {e for e, _ in used}
    fx = {"about": {"golden": Path(golden).parent.name, "export_digest": B.digest,
                    "made_by": "python -m pipeline.tests.answers_v35_port (golden by the archived v35 harness)",
                    "live_port": {"stats": dict(stats), "classes": dict(total)}},
          "rule_ids": rule_ids, "all_rules": sorted(f"{e}::{r}" for e, r in B.rules),
          "record_rules": sorted(record_rules),
          "nearer": sorted([a, b] for a, b in read._nearer_of(B.path) if a in entries or b in entries),
          "rules": {f"{e}::{r}": B.rules[(e, r)] for e, r in sorted(used)}, "cards": cards}
    raw = json.dumps(fx, separators=(",", ":"), sort_keys=True, default=str).encode()

    # 3. VERIFY before writing: read back from its own JSON, every chosen card is the live run
    back = json.loads(raw)
    S = stub(back)
    ri = {r: i for i, r in enumerate(back["rule_ids"])}
    for c, i in zip(back["cards"], chosen):
        want = run_card(live, card_inputs(ctx, B, keys, parts, pk2pi, lad, recs[i]), rule_index)
        got = run_card(S, c, ri, set(back["all_rules"]))
        if json.dumps(got, sort_keys=True) != json.dumps(want, sort_keys=True):
            raise SystemExit(f"answers_v35_port: card {c['w']} {c['date']} does not replay from the fixture")
    gz = gzip.compress(raw, 9, mtime=0)
    if len(gz) > MAX_BYTES:
        raise SystemExit(f"answers_v35_port: {len(gz):,} bytes gzipped > {MAX_BYTES:,} — nothing written; lower N_CLEAN")
    Path(out_path).write_bytes(gz)
    print(f"wrote {out_path}: {len(cards)} cards, {len(fx['rules'])} rules, {len(raw):,} bytes raw", file=sys.stderr)
    return fx


if __name__ == "__main__":
    a = sys.argv[1:]
    make(a[0], a[1], a[2], a[3], int(a[4]) if len(a) > 4 else 60)
