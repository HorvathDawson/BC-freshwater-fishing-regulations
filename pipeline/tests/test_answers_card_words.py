"""THE CARD'S WORDS, ONE TABLE (T866): every sentence the answers' wording rounds pinned on a no-data card, once.

NO DATA: the cards are `answers28_fixture`'s (the real reader and the real rows over rule records in git) and
`test_answers_28_gate_classes`'s (its own Region 5 fixture); the gear sentences are `gear.resolve` over the real
clauses (`test_answers_r11_wording._rules`, and the Lines row's clause set `_lines`). Before T866 the same sentence was
pinned in three or four round files (`test_answers_r10…r20i`, `ur1…ur7`, `28_card`), so one wording change failed
them all and was edited in each. Now:

  CARDS   (water, day[, moment]) -> {field: what the card says}: one row per card, each sentence typed once
  FIELDS  field -> how it is read off the card (`out`, what `rows.produce` writes); `grp` is the card's trout group
  GATE    the same for 28_gate_classes' cards (GATE_FIELDS)
  GEAR    a terminal-gear answer (GEAR_BUILD: the clauses) -> {field (GFIELDS): its words}

One test per row compares every field at once, so a failure lists each field that moved. The rounds keep what is not
a sentence (numbers, rule ids, the gate's mutations, `check_card` refusals, the composers on made-up records); a
sentence read off a card or a gear answer belongs here. A field is the exact projection a round pinned, never a wider
one (a fold, not new pins); the same (row, field) pinned in two rounds is one entry. The live twin (the bundle's
cards) is `test_answers_card_words_live.py`, which names the sentences it shares with these from here.
"""
from __future__ import annotations

from functools import lru_cache

import pytest

from pipeline.deliver import calendar as K
from pipeline.deliver.answers import gear as G
from pipeline.deliver.answers.tree import walk
from pipeline.tests import answers28_fixture as F
from pipeline.tests import test_answers_r11_wording as R11

OCT10, MAY1, MAY15, JUL15 = (10, 10), (5, 1), (5, 15), (7, 15)
MOMENTS = {"sat": K.Moment(K.weekday_mask(["Saturday", "Sunday"])),
           "week": K.Moment(K.weekday_mask(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]))}
TROUT = {"RB", "CT", "EB", "GB", "DV", "LT"}


@lru_cache(maxsize=None)
def _card(key):
    water, md, *at = key
    out, _P = F.card(water, md, at=MOMENTS[at[0]] if at else None)
    return out


# ---------------------------------------------------------------------------------------------- reading a card
def one(xs):
    """The one item (the rounds' `(x,) = …` unpacking)."""
    (x,) = xs
    return x


def nth(xs, i, n):
    """Item i of exactly n (the rounds' `a, b = …` unpacking)."""
    assert len(xs) == n, xs
    return xs[i]


def grp(out: dict) -> dict:
    """The card's trout group: the one row group holding a trout kind (the region's total where it has one)."""
    return one([r["group"] for r in out["rows"] if r.get("group") and "pool" in r["group"]
                and set(r["group"]["kinds"]) & TROUT])


def bucket_of(g: dict, S: str) -> dict:
    """The bucket holding species S as a kind."""
    return one([b for b in g["buckets"] if any(S in k["species"] for k in b["kinds"])])


def kind_of(g: dict, S: str) -> dict:
    return one([k for b in g["buckets"] for k in b["kinds"] if S in k["species"]])


def kinds_by_name(g: dict) -> dict:
    return {k["name"]: k for b in g["buckets"] for k in b["kinds"]}


def bucket_by(g: dict, rule: str) -> dict:
    """The one bucket a rule decides."""
    return one([b for b in g["buckets"] if b["by"] == rule])


def trout_row(o: dict) -> dict:
    g = grp(o)
    return one([r for r in o["rows"] if r.get("group") is g])


def water_row(o: dict) -> dict:
    """The row of the water's own limit (its `elsewhere` says whose it is)."""
    return one([r for r in o["rows"] if r.get("elsewhere") and r["scope"]["of"] == "water"])


def placements(o: dict) -> list:
    """Where the steelhead presence line sits: a row, a kind row, a not-kept line."""
    got = []
    for r in o["rows"]:
        if r.get("presence"):
            got.append(("row", r["all_members"], r["presence"]))
        g = r.get("group") or {}
        for b in g.get("buckets") or []:
            for k in list(b["kinds"]) + [k for s in b.get("groups") or [] for k in s["kinds"]]:
                if k.get("presence"):
                    got.append(("kind", k["species"], k["presence"]))
        for x in g.get("not_kept") or []:
            if x.get("presence"):
                got.append(("not_kept", x["species"], x["presence"]))
    return got


def cells(g: dict, S: str) -> list:
    return [(b["says"], c["n"], c["why"] or c["origin"]) for b, c in zip(g["table"]["bands"][S], g["table"]["cells"][S])]


def tier(o: dict, name: str) -> dict:
    """The card tree's one tier of that kind (share, own, size, count)."""
    return one([n for n, _p in walk(grp(o)["tree"]) if n["node"] == "tier" and n["tier"] == name])


def root(o: dict) -> dict:
    return one(grp(o)["tree"])


def tree_kind(o: dict, *species: str) -> dict:
    """The tree's one kind row holding exactly these species."""
    return one([n for n, _p in walk(grp(o)["tree"]) if n["node"] == "kind" and n["species"] == list(species)])


def tree_kind_with(o: dict, S: str) -> dict:
    """The first kind row of the tree holding species S."""
    return next(n for n, _p in walk(grp(o)["tree"]) if n["node"] == "kind" and S in n["species"])


# ---------------------------------------------------------------------------------------------- the sentences
# A sentence two rows say alike (or a row here and one in the live table) is named once.
R2_TOTAL_SAYS = "4 a day per angler across Region 2, lakes included"
HEADLINE_R2_STREAM = "2 hatchery trout/char a day from streams"
HEADLINE_R2_LAKE = "4 a day per angler"
TOTAL_LINE_R2 = "No more than 4 trout and char a day per angler across Region 2, lakes included."
HEADLINE_ALSO_RB4 = "4 if they’re hatchery rainbow trout 50 cm (20 in) or less"
LIFT_RB4 = ("On this river, hatchery rainbow trout 50 cm (20 in) or less: up to 4 a day — this river’s rule "
            "replaces Region 2’s 2-from-streams limit for them.")
RB_LIFT_FISH = "hatchery rainbow trout 50 cm (20 in) or less"
SHARE_TITLE_R2 = "Up to 2 a day between them — all Region 2 streams combined"
SHARE_OF_4 = "Up to 2 of the 4 from streams"
OWN_RELATION_R2 = "Not part of the 2 from streams; part of the 4 a day across Region 2"
THIS_RIVERS_RULE = "This river’s rule"
THIS_LAKES_RULE = "This lake’s rule"
REST_OF_4 = "The rest of the 4 a day"
COUNTS_TOWARD_4 = "counts toward the 4 a day"
WILD_GO_BACK = "From streams, keep hatchery fish only — wild trout and char go back."
BY_THE_WALK = "(its row covers its tributaries, so it applies here)"
OVER50_NOT_RB = ("Cutthroat trout, brown trout, Dolly Varden/bull trout, lake trout or brook trout: only 1 over 50 cm "
                 "between them")
OVER50_ALL = ("Rainbow trout, cutthroat trout, brown trout, Dolly Varden/bull trout, lake trout or brook trout: only 1 "
              "over 50 cm between them")
OVER50_ONE = "Only 1 of all your trout and char may be over 50 cm (20 in) — not 1 per species."
OVER50_ONE_ST = OVER50_ONE + " 2 if one of them is a hatchery steelhead."
COUNT_CT_RB_OF_2 = "Up to 1 cutthroat or rainbow trout combined (part of the 2)"
CT_RB_ST_ONE = "Cutthroat trout, rainbow trout or steelhead: 1 between them"
HATCHERY_30_50 = "hatchery, 30–50 cm"
SLOT_DETAIL = "60 cm or less, or 90 cm or longer"
ST_DETAIL = "hatchery rainbow trout over 50 cm"
RB_ANY_LENGTH_50 = "hatchery, any length, 50 cm or less"
DV_LT_ONE_IN_ALL = "Only 1 Dolly Varden/bull trout or lake trout a day in all"
ST_DUTIES_VEDDER = ["10 a year", "stop fishing this river after 2 a day", "record each on your licence right away"]
DV_LT_COMBINED = "Up to 1 Dolly Varden/bull trout or lake trout combined"
ST_HATCHERY_2 = "Up to 2 hatchery steelhead — wild steelhead go back"
STEELHEAD_KNOWN = {"line": "known_with_rules", "says": "A rainbow trout over 50 cm (20 in) here counts as a steelhead."}
ST_CLOSED = "Steelhead: closed, no catch and release"
CT_RELEASE = "Cutthroat trout: release every one"
STREAM_ELSEWHERE_R2 = "Your own limit, counted across every Region 2 stream you fish today."
KEEP4_RB = "If you keep 4 hatchery rainbow trout here"
KEEP2_RB = "If you keep 2 hatchery rainbow trout here"
YOUR_DAY = "that’s your trout and char for the day, everywhere."
KEEP2_RB_SAYS = ("they don’t use up your 2 from streams here, but on other streams they count as them: none from other "
                 "streams today — up to 2 more of any kind, from here or from lakes.")
KEPT1_LAKE_Q = "Kept 1 at a lake?"
VEDDER_LAKE_KEEP3 = "Keep 3 more here. Fish from lakes count toward the 4 a day, not toward the 2 from streams."
KEPT2_STREAM_Q, NONE_MORE_HERE = "Kept 2 on another stream?", "None more here"
LAKE_KEEP2 = "Keep 2 more here. Fish from lakes count toward the 4 a day, not toward the 2 from streams."
OVER50_ELSEWHERE_Q = "Kept trout or char over 50 cm (20 in) elsewhere today?"
OVER50_ELSEWHERE_A = ("It counts here too: at most 2 over 50 cm (20 in) a day in all — only 1 of them may be anything "
                      "other than a hatchery steelhead.")
# the gear sentences (`gear.summary.says`, the Lines row)
GEAR_VEDDER = "1 line with a single barbless hook, no trebles (one hook, lure or fly). Weight on the line up to 1 kg (2.2 lb)."
GEAR_OKANAGAN = "1 line (2 while alone in a boat) with one hook, lure or fly. Weight on the line up to 1 kg (2.2 lb)."
GEAR_KOOTENAY = ("1 line from shore; any number from a boat, each with one hook, lure or fly. "
                 "Weight on the line up to 1 kg (2.2 lb).")
GEAR_COWICHAN = ("Fly fishing only: 1 line with a single barbless hook, no trebles (one fly). No float or strike indicator, "
                 "no sinker or split shot, no lure, spinner or other attracting device.")
LINES_ONE = "1 line (rod)"
LINES_LAKE = "1 line (rod); 2 while alone in a boat"
LINES_KOOTENAY = "1 line (rod) from shore; no limit from a boat"

# ---------------------------------------------------------------------------------------------- the fields
FIELDS = {
    # the top: headline, header, origin, lifts, total
    "headline (no info)": lambda o: {k: v for k, v in grp(o)["headline"].items() if k != "info"},
    "headline: says": lambda o: grp(o)["headline"]["says"],
    "headline: also": lambda o: grp(o)["headline"]["also"],
    "header": lambda o: grp(o)["header"],
    "origin": lambda o: grp(o)["origin"],
    "lifts: says": lambda o: [L["says"] for L in grp(o)["lifts"]],
    "lifts[0]: each": lambda o: grp(o)["lifts"][0]["each"],
    "total_line": lambda o: grp(o)["total_line"],
    # the buckets and their kinds
    "buckets: title": lambda o: [b["title"] for b in grp(o)["buckets"]],
    "buckets[0]: notes": lambda o: grp(o)["buckets"][0]["notes"],
    "buckets[0]: sizes says": lambda o: [n["says"] for n in grp(o)["buckets"][0]["sizes"]],
    "buckets[0]: sizes says, species": lambda o: [(x["says"], x["species"]) for x in grp(o)["buckets"][0]["sizes"]],
    "buckets[0]: size (one) says": lambda o: one(grp(o)["buckets"][0]["sizes"])["says"],
    "buckets[0]: group (one) says, n, kinds' species": lambda o: (lambda s: (s["says"], s["n"],
                                                                             [k["species"] for k in s["kinds"]]))(
        one(grp(o)["buckets"][0]["groups"])),
    "buckets[0]: replaces": lambda o: grp(o)["buckets"][0]["replaces"],
    "buckets[0]: replaces says": lambda o: grp(o)["buckets"][0]["replaces"]["says"],
    "buckets[1]: sizes": lambda o: grp(o)["buckets"][1]["sizes"],
    "bucket notes and sizes: title, says": lambda o: [(b["title"], n["says"]) for b in grp(o)["buckets"]
                                                       for n in b["notes"] + b["sizes"]],
    "the bucket holding ST: title, cap, by": lambda o: (lambda b: (b["title"], b["cap"], b["by"]))(bucket_of(grp(o), "ST")),
    "the bucket holding CT: sizes says": lambda o: [n["says"] for n in bucket_of(grp(o), "CT")["sizes"]],
    "the kind ST: n, name, detail": lambda o: (lambda k: (k["n"], k["name"], k["detail"]))(kind_of(grp(o), "ST")),
    "kind notes": lambda o: [t for b in grp(o)["buckets"] for k in b["kinds"] for t in k["notes"]],
    "kind Rainbow trout: detail": lambda o: kinds_by_name(grp(o))["Rainbow trout"]["detail"],
    "kind Brook trout, brown trout and cutthroat trout: detail":
        lambda o: kinds_by_name(grp(o))["Brook trout, brown trout and cutthroat trout"]["detail"],
    "kind Dolly Varden/bull trout and lake trout: detail":
        lambda o: kinds_by_name(grp(o))["Dolly Varden/bull trout and lake trout"]["detail"],
    "bucket by BELLA_R5: title, cap, why": lambda o: (lambda b: (b["title"], b["cap"], b["why"]))(bucket_by(grp(o), F.BELLA_R5)),
    "bucket by BELLA_R5: between": lambda o: bucket_by(grp(o), F.BELLA_R5)["between"],
    "bucket by BELLA_R5: size (one) says": lambda o: one(bucket_by(grp(o), F.BELLA_R5)["sizes"])["says"],
    "bucket by BELLA_R5: group (one) says, n, by": lambda o: (lambda s: (s["says"], s["n"], s["by"]))(
        one(bucket_by(grp(o), F.BELLA_R5)["groups"])),
    "bucket by BELLA_R5: group (one) kinds' name, detail, n": lambda o: [(k["name"], k["detail"], k["n"]) for k in
                                                                         one(bucket_by(grp(o), F.BELLA_R5)["groups"])["kinds"]],
    "the own-rule bucket (one): between": lambda o: one([b for b in grp(o)["buckets"] if b["why"] == "rule"])["between"],
    "cap by BELLA_R5 of 2: says": lambda o: next(c for c in grp(o)["caps"] if c["by"] == F.BELLA_R5 and c["take"] == 2)["says"],
    # not kept, presence
    "not_kept": lambda o: grp(o)["not_kept"],
    "not_kept other than CT": lambda o: [x for x in grp(o)["not_kept"] if x["species"] != ["CT"]],
    "not_kept CT (one): status, says": lambda o: (lambda x: (x["status"], x["says"]))(
        one([x for x in grp(o)["not_kept"] if x["species"] == ["CT"]])),
    "not_kept: species, says": lambda o: [(x["species"], x["says"]) for x in grp(o)["not_kept"]],
    "not_kept whose: char, steelhead": lambda o: (lambda w: (w[("DV", "LT", "EB")], w[("ST",)]))(
        {tuple(x["species"]): x["whose"] for x in grp(o)["not_kept"]}),
    "presence placements": placements,
    # the worked lines and the kept-elsewhere line
    "worked: kept, q, a": lambda o: [{k: w[k] for k in ("kept", "q", "a")} for w in grp(o)["worked"]],
    "worked: q, a": lambda o: [(w["q"], w["a"]) for w in grp(o)["worked"]],
    "worked[0]: q, a, here": lambda o: (lambda w: [w["q"], w["a"], w["here"]])(grp(o)["worked"][0]),
    "worked here: q, a": lambda o: [(w["q"], w["a"]) for w in grp(o)["worked"] if w["at"] == "here"],
    "worked here with a share, kept 2 and 4: q, a": lambda o: (lambda w: {k: (w[k]["q"], w[k]["a"]) for k in (2, 4)})(
        {x["kept"]: x for x in grp(o)["worked"] if x["at"] == "here" and x["share"]}),
    "worked at lake: q, a, kept, here": lambda o: (lambda w: (w["q"], w["a"], w["kept"], w["here"]))(
        next(w for w in grp(o)["worked"] if w["at"] == "lake")),
    "worked at lake: a": lambda o: next(w for w in grp(o)["worked"] if w["at"] == "lake")["a"],
    "the trout row: elsewhere says": lambda o: trout_row(o)["elsewhere"]["says"],
    "the water's own row: elsewhere says": lambda o: water_row(o)["elsewhere"]["says"],
    "kokanee row: restates": lambda o: one([r for r in o["rows"] if r["all_members"] == ["KO"]])["restates"],
    # the tree (ROUND18 on): its root and tiers, its kind rows, the quota table's bands
    "the root tier: tier, n, says": lambda o: (lambda t: (t["tier"], t["n"], t["says"]))(root(o)),
    "the root tier: says": lambda o: root(o)["says"],
    "the root's first of 2 children: says": lambda o: nth(root(o)["children"], 0, 2)["says"],
    "the root's first child's one child: says": lambda o: one(root(o)["children"][0]["children"])["says"],
    "the root's one child: children[0] says": lambda o: one(root(o)["children"])["children"][0]["says"],
    "the root's one child: children[1] says": lambda o: one(root(o)["children"])["children"][1]["says"],
    "the root's one child: children[1].children[0] sentence":
        lambda o: one(root(o)["children"])["children"][1]["children"][0]["sentence"],
    "tree[0] children: origins": lambda o: [t["origin"] for t in grp(o)["tree"][0]["children"] if t.get("origin")],
    "the share tier: says": lambda o: tier(o, "share")["says"],
    "the share tier: origin": lambda o: tier(o, "share")["origin"],
    "the share tier: its kinds' says": lambda o: [k["says"] for k in tier(o, "share")["children"] if k["node"] == "kind"],
    "the own tier: tier, n, says": lambda o: (lambda t: (t["tier"], t["n"], t["says"]))(tier(o, "own")),
    "the own tier: says": lambda o: tier(o, "own")["says"],
    "the own tier: line": lambda o: tier(o, "own")["line"],
    "the own tier: why_here": lambda o: tier(o, "own")["why_here"],
    "the own tier: replaces": lambda o: tier(o, "own")["replaces"],
    "the own tier: relation": lambda o: tier(o, "own")["relation"],
    "the own tier: children says": lambda o: [k["says"] for k in tier(o, "own")["children"]],
    "the size tier: says": lambda o: tier(o, "size")["says"],
    "the size tier: children says": lambda o: [k["says"] for k in tier(o, "size")["children"]],
    "the count tier: says": lambda o: tier(o, "count")["says"],
    "the count tier: its kinds' sentences": lambda o: [k["sentence"] for k in tier(o, "count")["children"]],
    "the tree kind RB: says, detail": lambda o: (lambda k: (k["says"], k["detail"]))(tree_kind(o, "RB")),
    "the tree kind RB: sentence": lambda o: tree_kind(o, "RB")["sentence"],
    "the tree kind EB,GB,CT: sentence": lambda o: tree_kind(o, "EB", "GB", "CT")["sentence"],
    "the tree kind DV,LT: sentence": lambda o: tree_kind(o, "DV", "LT")["sentence"],
    "the tree kind ST: sentence": lambda o: tree_kind(o, "ST")["sentence"],
    "the tree kind ST: duties": lambda o: tree_kind(o, "ST")["duties"],
    "the tree kind ST: presence says": lambda o: tree_kind(o, "ST")["presence"]["says"],
    "the tree kind RB: also_counts says": lambda o: [a["says"] for a in tree_kind(o, "RB")["also_counts"]],
    "the tree kind ST: also_counts says": lambda o: [a["says"] for a in tree_kind(o, "ST")["also_counts"]],
    "the tree kind EB,GB,CT: size_rule": lambda o: tree_kind(o, "EB", "GB", "CT")["size_rule"],
    "the tree kind DV,LT: size_rule": lambda o: tree_kind(o, "DV", "LT")["size_rule"],
    "the tree kind RB: size_rule": lambda o: tree_kind(o, "RB")["size_rule"],
    "the tree kind CT: size_rule": lambda o: tree_kind(o, "CT")["size_rule"],
    "the tree kind holding DV: says": lambda o: tree_kind_with(o, "DV")["says"],
    "the tree kind holding ST: says": lambda o: tree_kind_with(o, "ST")["says"],
    "table bands RB: says, from_in, to_in": lambda o: [(b["says"], b["from_in"], b["to_in"])
                                                       for b in grp(o)["table"]["bands"]["RB"]],
    "table bands RB: says": lambda o: [b["says"] for b in grp(o)["table"]["bands"]["RB"]],
    "table bands CT: says": lambda o: [b["says"] for b in grp(o)["table"]["bands"]["CT"]],
    "table bands DV: says": lambda o: [b["says"] for b in grp(o)["table"]["bands"]["DV"]],
    "table cells CT": lambda o: cells(grp(o), "CT"),
    "table cells DV": lambda o: cells(grp(o), "DV"),
    "table RB last band: says, n, as, why": lambda o: (lambda g: (g["table"]["bands"]["RB"][-1]["says"],
                                                                  g["table"]["cells"]["RB"][-1]["n"],
                                                                  g["table"]["cells"]["RB"][-1]["as"],
                                                                  g["table"]["cells"]["RB"][-1]["why"]))(grp(o)),
}

# ---------------------------------------------------------------------------------------------- the cards
CARDS = {
    (F.VEDDER, OCT10): {
        "headline (no info)": {"n": 2, "unit": "hatchery trout/char a day from streams",
                               "says": HEADLINE_R2_STREAM, "origin": "hatchery",
                               "fish": "trout/char", "also": [{"n": 4, "says": HEADLINE_ALSO_RB4}]},
        "header": {"says": R2_TOTAL_SAYS, "tier": "t1", "tag": None},
        "lifts: says": [LIFT_RB4],
        "lifts[0]: each": [{"kinds": ["RB"], "n": 4, "by": F.LIFT, "fish": RB_LIFT_FISH}],
        "total_line": TOTAL_LINE_R2,
        "buckets: title": [THIS_RIVERS_RULE, SHARE_TITLE_R2],
        "buckets[0]: replaces": {
            "says": "Replaces Region 2’s 2-from-streams limit and 30 cm minimum size for hatchery rainbow trout",
            "rules": [F.SHARE, "z2:daily_quotas::trout_char_quota.r8"]},
        "buckets[1]: sizes": [{"says": OVER50_NOT_RB, "n": 1, "over_cm": 50, "by": "z2:daily_quotas::trout_char_quota.r2",
                               "species": ["CT", "GB", "DV", "LT", "EB"], "not_counting": ["ST"], "then": []}],
        "the bucket holding ST: title, cap, by": (SHARE_TITLE_R2, 2, F.SHARE),
        "the bucket holding CT: sizes says": [OVER50_NOT_RB],
        "the kind ST: n, name, detail": (2, "Steelhead", ST_DETAIL),
        "kind notes": [COUNTS_TOWARD_4],
        "kind Rainbow trout: detail": RB_ANY_LENGTH_50,
        "worked: kept, q, a": [
            {"kept": 4, "q": KEEP4_RB, "a": YOUR_DAY},
            {"kept": 2, "q": KEEP2_RB, "a": KEEP2_RB_SAYS},
            {"kept": 1, "q": KEPT1_LAKE_Q, "a": VEDDER_LAKE_KEEP3},
            {"kept": 1, "q": OVER50_ELSEWHERE_Q, "a": OVER50_ELSEWHERE_A}],
        "worked here: q, a": [(KEEP4_RB, YOUR_DAY), (KEEP2_RB, KEEP2_RB_SAYS)],
        "worked here with a share, kept 2 and 4: q, a": {2: (KEEP2_RB, KEEP2_RB_SAYS), 4: (KEEP4_RB, YOUR_DAY)},
        "worked at lake: a": VEDDER_LAKE_KEEP3,                      # r13 P (was its startswith)
        "the trout row: elsewhere says": STREAM_ELSEWHERE_R2,
        "the root tier: tier, n, says": ("total", 4, R2_TOTAL_SAYS),
        "the root's first of 2 children: says": SHARE_OF_4,
        "the root's first child's one child: says": OVER50_ONE_ST,
        "the share tier: says": SHARE_OF_4,
        "the share tier: origin": WILD_GO_BACK,
        "the own tier: tier, n, says": ("own", 4, "This river’s rule: up to 4 hatchery rainbow trout a day"),
        "the own tier: line": THIS_RIVERS_RULE,
        "the own tier: replaces": ("Replaces Region 2’s 2-from-streams limit and 30 cm (12 in) minimum size for "
                                   "hatchery rainbow trout"),
        "the own tier: relation": OWN_RELATION_R2,
        "the size tier: says": OVER50_ONE_ST,
        "the tree kind RB: says, detail": ("Up to 4 hatchery rainbow trout", "any length, 50 cm (20 in) or less"),
        "the tree kind RB: sentence": "Up to 4 hatchery rainbow trout, any length, 50 cm (20 in) or less",
        "the tree kind EB,GB,CT: sentence": "Up to 2 brook, brown or cutthroat trout combined, 30 cm (12 in) or longer",
        "the tree kind DV,LT: sentence": DV_LT_COMBINED + ", 60 cm (24 in) or longer",
        "the tree kind ST: sentence": "Up to 2 hatchery steelhead, rainbow trout over 50 cm (20 in) — wild steelhead go back",
        "the tree kind ST: duties": ST_DUTIES_VEDDER,
        "the tree kind ST: presence says": STEELHEAD_KNOWN["says"],
        "the tree kind EB,GB,CT: size_rule": "must be 30 cm (12 in) or longer; release any shorter",
        "the tree kind DV,LT: size_rule": "must be 60 cm (24 in) or longer; release any shorter",
        "the tree kind holding DV: says": DV_LT_COMBINED,
        "the tree kind holding ST: says": ST_HATCHERY_2,
        "table bands RB: says, from_in, to_in": [("50 cm (20 in) or less", True, True),
                                                 ("over 50 cm (20 in)", False, False)],
        "table bands CT: says": ["under 30 cm (12 in)", "30–50 cm (12–20 in)", "over 50 cm (20 in)"],
        "table bands DV: says": ["under 60 cm (24 in)", "60 cm (24 in) or longer"],
    },
    (F.TWO, OCT10): {
        "lifts: says": [
            "On this river, its own rule replaces Region 2’s 2-from-streams limit for hatchery rainbow "
            "trout 50 cm (20 in) or less (up to 4 a day) and hatchery cutthroat trout (up to 3 a day)."],
        "lifts[0]: each": [{"kinds": ["RB"], "n": 4, "by": F.LIFT, "fish": RB_LIFT_FISH},
                           {"kinds": ["CT"], "n": 3, "by": F.LIFT2, "fish": "hatchery cutthroat trout"}],
        "headline: also": [{"n": 4, "says": HEADLINE_ALSO_RB4}, {"n": 3, "says": "3 if they’re hatchery cutthroat trout"}],
        "worked here: q, a": [(KEEP4_RB, YOUR_DAY), (KEEP2_RB, KEEP2_RB_SAYS)],
        "buckets: title": [THIS_RIVERS_RULE, SHARE_TITLE_R2],
        "buckets[0]: replaces says": ("Replaces Region 2’s 2-from-streams limit for hatchery cutthroat trout and "
                                      "rainbow trout, and its 30 cm minimum size for hatchery rainbow trout"),
        "the own-rule bucket (one): between": "Up to 4 a day between them",
        "the share tier: says": SHARE_OF_4,
        "the share tier: its kinds' says": ["Up to 2 brook or brown trout combined", DV_LT_COMBINED, ST_HATCHERY_2],
        "the own tier: relation": OWN_RELATION_R2,
        "the own tier: children says": ["Up to 3 hatchery cutthroat trout", "Up to 4 hatchery rainbow trout"],
    },
    (F.STREAM, OCT10): {
        "worked[0]: q, a, here": [KEPT2_STREAM_Q, NONE_MORE_HERE, 0],
        "worked: q, a": [(KEPT2_STREAM_Q, NONE_MORE_HERE), (KEPT1_LAKE_Q, LAKE_KEEP2),
                         (OVER50_ELSEWHERE_Q, OVER50_ELSEWHERE_A)],
        "worked at lake: q, a, kept, here": (KEPT1_LAKE_Q, LAKE_KEEP2, 1, 2),
        "kind Rainbow trout: detail": HATCHERY_30_50,
        "kind Brook trout, brown trout and cutthroat trout: detail": "hatchery, 30 cm or longer",
        "kind Dolly Varden/bull trout and lake trout: detail": "hatchery, 60 cm or longer",
        "presence placements": [("kind", ["ST"], STEELHEAD_KNOWN)],
        "the share tier: says": SHARE_OF_4,
        "the share tier: origin": WILD_GO_BACK,
        "the share tier: its kinds' says": ["Up to 2 rainbow trout"],
        "the size tier: children says": ["Up to 2 brook, brown or cutthroat trout combined", DV_LT_COMBINED,
                                         ST_HATCHERY_2],
        "the tree kind RB: size_rule": "must be 30–50 cm (12–20 in); release any shorter",
    },
    (F.LAKE, OCT10): {
        "headline (no info)": {"n": 4, "unit": "a day per angler", "says": HEADLINE_R2_LAKE},
        "header": {"says": "4 a day per angler across Region 2", "tier": "t1", "tag": None},
        "buckets[0]: sizes says": [OVER50_ALL],
        "worked[0]: q, a, here": ["Kept 2 elsewhere today?", "Keep 2 more here", 2],
        "the trout row: elsewhere says": ("Your own limit, counted across every water in Region 2 you fish today, "
                                          "lakes and streams alike."),
    },
    (F.CLAKE, OCT10): {
        "buckets: title": [THIS_LAKES_RULE, REST_OF_4],
        "buckets[0]: notes": [{"says": DV_LT_ONE_IN_ALL, "n": 1, "over_cm": 0,
                               "kinds": ["DV", "LT"], "not_counting": [], "then": []}],
        "buckets[0]: sizes says, species": [(OVER50_ALL, ["RB", "CT", "GB", "DV", "LT", "EB"])],
        "kind notes": [COUNTS_TOWARD_4],
        "the size tier: says": OVER50_ONE,
    },
    (F.SLOT, OCT10): {
        "kind Rainbow trout: detail": SLOT_DETAIL,
        "buckets: title": [THIS_LAKES_RULE, REST_OF_4],
        "bucket notes and sizes: title, says": [(THIS_LAKES_RULE, "Rainbow trout: only 1 over 90 cm"),
                                                (REST_OF_4, OVER50_NOT_RB)],
        "table bands RB: says": ["60 cm (24 in) or less", "between 60 cm (24 in) and 90 cm (35 in)",
                                 "90 cm (35 in) or longer"],
    },
    (F.COUNT, OCT10): {
        "bucket notes and sizes: title, says": [(None, OVER50_NOT_RB)],
        "buckets[0]: group (one) says, n, kinds' species": (CT_RB_ST_ONE, 1, [["CT"], ["RB"], ["ST"]]),
        "origin": "Keep hatchery fish only — wild trout and char go back.",
        "the root tier: says": "This creek’s rule: up to 2 a day per angler",
        "the count tier: says": "Up to 1 cutthroat trout, rainbow trout or steelhead combined",
        "the tree kind RB: also_counts says": ["no more than 1 counting any steelhead or cutthroat trout you keep"],
        "the tree kind ST: also_counts says": ["no more than 1 counting any rainbow or cutthroat trout you keep"],
    },
    (F.NESTED, OCT10): {
        "buckets[0]: size (one) says": ("Rainbow trout, cutthroat trout, brown trout, Dolly Varden/bull trout, lake "
                                        "trout or brook trout: only 2 over 30 cm between them, and only 1 over 50 cm"),
    },
    (F.WAHLEACH, OCT10): {
        "bucket notes and sizes: title, says": [
            (None, "Rainbow trout, cutthroat trout, brown trout, Dolly Varden/bull trout, lake trout or brook trout: "
                   "only 1 over 40 cm between them")],
        "the water's own row: elsewhere says": ("This lake’s rule: 4 a day here. Trout and char other than steelhead you "
                                                "kept anywhere else today count toward those 4."),
    },
    (F.BELLA, OCT10): {
        "not_kept CT (one): status, says": ("release", CT_RELEASE),
        "not_kept other than CT": [{"species": ["ST"], "status": "closed", "says": ST_CLOSED, "by": F.BELLA_ST,
                                    "rules": [F.BELLA_ST], "whose": "this river’s rule", "term": "no_fishing_for",
                                    "presence": STEELHEAD_KNOWN}],
        "kind Rainbow trout: detail": HATCHERY_30_50,
        "presence placements": [("not_kept", ["ST"], STEELHEAD_KNOWN)],
        "the root tier: says": R2_TOTAL_SAYS,
        "the own tier: says": "This river’s rule: up to 1 a day (part of the 4)",
    },
    (F.FRASER, OCT10): {
        "not_kept": [{"species": ["ST"], "status": "closed", "says": ST_CLOSED, "by": F.FRASER_ST,
                      "rules": [F.FRASER_ST], "whose": "this river’s rule", "term": "no_fishing_for",
                      "presence": STEELHEAD_KNOWN}],
    },
    # T1250, RULINGS S15 (owner 2026-11-27, Buckley Creek: "Release: the row's cap holds (Recommended)"): the river's
    # "Rainbow trout (none over 50 cm)" (r8) holds for its steelhead, so a hatchery steelhead (a rainbow over 50 cm) is
    # released and leaves the "1 between them" group (was: "Cutthroat trout, rainbow trout or steelhead", with
    # "Steelhead, hatchery rainbow trout over 50 cm, 1"). The fixture puts the Bella Coola's rows on a Region 2 entry;
    # the real river (Region 5) releases every steelhead and prints "No fishing for steelhead" (r13): no angler change.
    (F.BELLA_MAY, MAY1): {
        "bucket by BELLA_R5: title, cap, why": (THIS_RIVERS_RULE, 2, "rule"),
        "bucket by BELLA_R5: group (one) says, n, by": ("Cutthroat trout or rainbow trout: 1 between them", 1,
                                                        F.BELLA_R6),
        "bucket by BELLA_R5: group (one) kinds' name, detail, n": [("Cutthroat trout", "hatchery, 30–33 cm", 1),
                                                                   ("Rainbow trout", HATCHERY_30_50, 1)],
    },
    (F.BELLA_MAY_ST, MAY1): {
        "bucket by BELLA_R5: size (one) says": ("Brown trout, Dolly Varden/bull trout, lake trout or brook trout: "
                                                "only 1 over 50 cm between them"),
        "bucket by BELLA_R5: between": "Up to 2 a day between them",
        "cap by BELLA_R5 of 2: says": "Up to 2 trout and char 25 cm (10 in) or longer here, counting fish kept elsewhere today.",
        "table cells CT": [("under 30 cm (12 in)", 0, "too small"), ("30–33 cm (12–13 in)", 1, "hatchery"),
                           ("over 33 cm (13 in)", 0, "too big")],
        "table cells DV": [("under 60 cm (24 in)", 0, "too small"), ("60 cm (24 in) or longer", 1, "hatchery")],
        "table RB last band: says, n, as, why": ("over 50 cm (20 in)", 0, "ST", "closed"),
        "the root's one child: children[0] says": OVER50_ONE,
        "the root's one child: children[1] says": COUNT_CT_RB_OF_2,
        "the root's one child: children[1].children[0] sentence": "Up to 1 hatchery cutthroat trout, 30–33 cm (12–13 in)",
        "the own tier: says": "This river’s rule: up to 2 a day (part of the 4)",
        "the size tier: says": OVER50_ONE,
        "the count tier: says": COUNT_CT_RB_OF_2,
        "the count tier: its kinds' sentences": ["Up to 1 hatchery cutthroat trout, 30–33 cm (12–13 in)",
                                                 "Up to 1 hatchery rainbow trout, 30–50 cm (12–20 in)"],
        "the tree kind CT: size_rule": "must be 30–33 cm (12–13 in); release any other size",
    },
    (F.VEDDER_MAY, MAY15): {
        "not_kept": [{"species": ["RB", "CT"], "status": "release",
                      "says": "Rainbow trout and cutthroat trout: release every one",
                      "by": F.VEDDER_R6, "rules": [F.VEDDER_R6, F.VEDDER_R7], "whose": "this river’s rule"}],
        "tree[0] children: origins": [WILD_GO_BACK],
    },
    (F.VEDDER_TRIB, OCT10): {
        "the share tier: says": SHARE_OF_4,
        "the own tier: why_here": "its row covers its tributaries, so it applies here",
        "the own tier: line": f"The Chilliwack River’s rule {BY_THE_WALK}",
        "the own tier: relation": OWN_RELATION_R2,
    },
    (F.BELLA_TRIB, OCT10): {
        "not_kept: species, says": [(["ST"], ST_CLOSED), (["CT"], CT_RELEASE),
                                    (["DV", "LT", "EB"],
                                     "Char (Dolly Varden/bull trout, lake trout and brook trout): release every one")],
        "not_kept whose: char, steelhead": ("the Bella Coola River’s rule for its tributaries", "the Bella Coola River’s rule"),
        "the water's own row: elsewhere says": ("The Bella Coola River’s rule: 1 a day here. Brown trout or rainbow trout "
                                                "you kept anywhere else today count toward that 1. They also count toward "
                                                "Region 2’s 4 trout and char a day."),
        "the own tier: says": "The Bella Coola River’s rule: up to 1 a day (part of the 4)",
        "the own tier: line": f"The Bella Coola River’s rule {BY_THE_WALK}",
    },
    (F.LWA, OCT10): {
        "headline: says": HEADLINE_R2_LAKE,
        "the tree kind holding DV: says": DV_LT_COMBINED,
    },
    (F.LWA, (10, 17), "sat"): {
        "kokanee row: restates": [{"r": F.LWA_R5, "of": F.LWA_R3,
                                   "says": "Kokanee from the Lower and Upper West Arms together: no more than 5 a day."}],
    },
}

# ---------------------------------------------------------------------------------------------- 28_gate_classes' cards
Z5 = "z5:daily_quotas::trout_char_quota.r1"


@lru_cache(maxsize=None)
def _gate_card(key):
    from pipeline.tests import test_answers_28_gate_classes as GC
    water, md = key
    out, _P = GC.card(water, md)
    return out


def _gate_group(out, pool=Z5):
    return one([r["group"] for r in out["rows"] if r.get("group") and r["group"]["pool"] == pool])


GATE_FIELDS = {
    "Z5 group headline: says": lambda o: _gate_group(o)["headline"]["says"],
    "Z5 group buckets: title, cap": lambda o: [(b["title"], b["cap"]) for b in _gate_group(o)["buckets"]],
    "Z5 group total_line": lambda o: _gate_group(o)["total_line"],
}
GATE = {
    ("abbott", JUL15): {
        "Z5 group headline: says": "1 a day per angler",
        "Z5 group buckets: title, cap": [(THIS_LAKES_RULE, 1)],
        "Z5 group total_line": "No more than 5 trout and char a day per angler across Region 5, streams included.",
    },
    ("dean", JUL15): {
        "Z5 group headline: says": "1 trout/char a day from streams",
    },
}


# ---------------------------------------------------------------------------------------------- the gear answers
def _lines(kind, kootenay=False):
    """r10 W4's clause set for the Lines row (the province's 2-alone-in-a-boat clause, the ice-fishing 1, Kootenay
    Lake's no limit in a boat): `gear.resolve`'s whole answer."""
    tackle = {"family": "gear_and_method", "type": "tackle_restriction"}
    rules = [G.Rule(R11.PROVINCE, 4, {**tackle, "gear": [
        {"max": 2.0, "slot": "lines_per_angler", "when": {"angler": "alone_in_boat", "water": "lake"}},
        {"max": 1.0, "slot": "lines_per_angler"}]}),
        G.Rule("zp:allowable_methods::allowable_methods.r2b", 4, {**tackle, "while": ["ice_fishing"],
               "gear": [{"max": 1.0, "slot": "lines_per_angler"}]})]
    if kootenay:
        rules.append(G.Rule(R11.KOOTENAY, 0, {**tackle, "gear": [
            {"slot": "lines_per_angler", "unlimited": True, "when": {"angler": "in_boat"}}]}))
    return G.resolve(rules, kind, ["angling"])


GEAR_BUILD = {
    "vedder": lambda: R11._rules("stream", hook=R11.Z2_HOOK),
    "okanagan": lambda: R11._rules("lake"),
    "kootenay": lambda: R11._rules("lake", kootenay=True),
    "cowichan": lambda: R11._rules("stream", hook=R11.Z1_HOOK, fly=R11.COWICHAN_FLY),
    "lines: stream": lambda: _lines("stream"),
    "lines: lake": lambda: _lines("lake"),
    "lines: wetland": lambda: _lines("wetland"),
    "lines: kootenay": lambda: _lines("lake", kootenay=True),
}
GFIELDS = {
    "summary: says": lambda a: a["summary"]["says"],
    "lines: says": lambda a: a["counts"]["lines_per_angler"]["says"],
    "lines: says, cites": lambda a: (a["counts"]["lines_per_angler"]["says"], a["counts"]["lines_per_angler"]["cites"]),
}
GEAR = {
    "vedder": {"summary: says": GEAR_VEDDER, "lines: says": LINES_ONE},
    "okanagan": {"summary: says": GEAR_OKANAGAN, "lines: says": LINES_LAKE},
    "kootenay": {"summary: says": GEAR_KOOTENAY, "lines: says": LINES_KOOTENAY},
    "cowichan": {"summary: says": GEAR_COWICHAN},
    "lines: stream": {"lines: says, cites": (LINES_ONE, [R11.PROVINCE])},
    "lines: lake": {"lines: says, cites": (LINES_LAKE, [R11.PROVINCE])},
    # T1249: every key has a kind (the bundle's); a wetland is no lake, as every named wetland reads
    "lines: wetland": {"lines: says, cites": (LINES_ONE, [R11.PROVINCE])},
    "lines: kootenay": {"lines: says, cites": (LINES_KOOTENAY, [R11.PROVINCE, R11.KOOTENAY])},
}


# ---------------------------------------------------------------------------------------------- the tests
def _id(key) -> str:
    return "-".join(str(x) if not isinstance(x, tuple) else "%02d%02d" % x for x in key)


@pytest.mark.parametrize("key", list(CARDS), ids=_id)
def test_the_card_says(key):
    out = _card(key)
    want = CARDS[key]
    assert {f: FIELDS[f](out) for f in want} == want


@pytest.mark.parametrize("key", list(GATE), ids=_id)
def test_the_gate_fixtures_card_says(key):
    out = _gate_card(key)
    want = GATE[key]
    assert {f: GATE_FIELDS[f](out) for f in want} == want


@pytest.mark.parametrize("key", list(GEAR))
def test_the_gear_answer_says(key):
    a = GEAR_BUILD[key]()
    want = GEAR[key]
    assert {f: GFIELDS[f](a) for f in want} == want


def test_every_field_is_read_by_a_row():
    """A field no row reads is a pin that went missing in an edit (or dead code)."""
    for fields, table in ((FIELDS, CARDS), (GATE_FIELDS, GATE), (GFIELDS, GEAR)):
        assert set(fields) - {f for row in table.values() for f in row} == set()
    assert set(GEAR_BUILD) == set(GEAR)
