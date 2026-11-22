"""THE CARD'S WORDS ON THE REAL WATERS, ONE TABLE (T866): every sentence the live answers rounds (`test_answers_25`,
`26`, `27`, `28_live`) pinned on a built card, once.

LIVE DATA: the answers built for the pinned waters from the bundle and export under test (`UI_EXPORT_BUNDLE` /
`ANSWERS_EXPORT_DIR` point the suite at a side pair); `needs_bundle`, so the no-data tier never collects a row. The
no-data twin is `test_answers_card_words.py` (the fixture's cards); a sentence both say is typed there once and named
here.

  LIVE         (water item, part, day) -> {field: what the card says}; a part is its index or the endpoint labels
               `conftest.part_at` finds it by (never a position of one build)
  FIELDS_LIVE  field -> how it is read off the card's trout row (the row whose group holds cutthroat or Dolly Varden)

One test per row compares every field at once. The rounds keep what is not a sentence (numbers, rule ids, the
mutations, structure, the corpus-wide sweeps). A field is the exact projection a round pinned, never a wider one; the
same (row, field) pinned in two rounds is one entry.
"""
from __future__ import annotations

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.tests.conftest import need, part_at, BUNDLE_HINT, EXPORT_HINT
from pipeline.tests import under_test
from pipeline.tests.test_answers_card_words import (
    COUNTS_TOWARD_4, DV_LT_ONE_IN_ALL, HEADLINE_R2_LAKE, HEADLINE_R2_STREAM, KEEP2_RB, KEEP2_RB_SAYS, KEEP4_RB,
    KEPT1_LAKE_Q, KEPT2_STREAM_Q, LIFT_RB4, NONE_MORE_HERE, OVER50_ALL, OVER50_ELSEWHERE_A, OVER50_ELSEWHERE_Q,
    OVER50_NOT_RB, RB_ANY_LENGTH_50, REST_OF_4, SHARE_TITLE_R2, ST_DUTIES_VEDDER, STREAM_ELSEWHERE_R2, THIS_LAKES_RULE,
    THIS_RIVERS_RULE, TOTAL_LINE_R2, VEDDER_LAKE_KEEP3, WILD_GO_BACK, YOUR_DAY)

BUNDLE = under_test.path("bundle")
EXPORT_DIR = under_test.path("export_dir")

VEDDER, DUNCAN, LARDEAU_CREEK = "gnis:3062", "gnis:22255", "gnis:16357"
KOOTENAY_MAIN = "wbk:-20"
R2_LAKE, R2_STREAM = "wbk:328961745", "gnis:10014"          # Judson Lake, Anderson Creek: zone rules only
CHILLIWACK_LAKE = "wbk:329083341"                           # Region 2 lake: no steelhead, the Q16 pair is one clause
WATERS = (VEDDER, DUNCAN, LARDEAU_CREEK, KOOTENAY_MAIN, R2_LAKE, R2_STREAM, CHILLIWACK_LAKE)
NAMES = {VEDDER: "vedder", DUNCAN: "duncan", LARDEAU_CREEK: "lardeau_creek", KOOTENAY_MAIN: "kootenay_lake",
         R2_LAKE: "judson_lake", R2_STREAM: "anderson_creek", CHILLIWACK_LAKE: "chilliwack_lake"}
MOUTH = ("mouth",)
DUNCAN_LOW = ("mouth", "duncan_river__lardeau_river_into_duncan_river")
OCT10, JAN15 = (10, 10), (1, 15)


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    missing = [w for w in WATERS if w not in data["waters"]]
    if missing:
        pytest.fail(f"pinned waters missing from this export: {missing}")
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=WATERS, sections=["display"],
                    export=(data, guide), log=lambda *_: None)
    return data, model


def trout_row(built, item, part, md) -> dict:
    """The card's trout row on that part and day: the row whose group holds cutthroat or Dolly Varden."""
    data, model = built
    if isinstance(part, tuple):
        part = part_at(data, item, *part)
    k = model.parts[item][part]
    v = model.sections["rows"][k][E.segment_index(model.segments[k], CAL.day_of(md))]
    return next(r for r in v["rows"] if r.get("group") and {"CT", "DV"} & set(r["group"]["kinds"]))


def text(row) -> list:
    """Every line the card shows for the group, a kind row as `<species> <n>: <notes · duties>` (test_answers_28_live)."""
    g = row["group"]
    out = [g["headline"]["says"], g["origin"], *[L["says"] for L in g["lifts"]], g["total_line"]]
    for b in g["buckets"]:
        out.append(b["title"])
        out += [n["says"] for n in b.get("notes") or []] + [n["says"] for n in b.get("sizes") or []]
        out += [f"{'/'.join(k['species'])} {k['n']}: " + " · ".join(k["notes"] + k["duties"])
                for k in b["kinds"]]
    return [t for t in out if t]


def kind(row, species: str) -> dict:
    """A kind row by its species joined with "/" (test_answers_27's `_kinds`)."""
    return {"/".join(k["species"]): k for b in row["group"]["buckets"] for k in b["kinds"]}[species]


# ---------------------------------------------------------------------------------------------- the sentences
LIFT_RB5_R4 = ("On this river, rainbow trout: up to 5 a day — this river’s rule replaces Region 4’s 2-from-streams "
               "limit for them.")
LIFT_RB5_DUNCAN_TRIB = ("On the Duncan River and its tributaries, rainbow trout: up to 5 a day — the Duncan River’s "
                        "rule replaces Region 4’s 2-from-streams limit for them.")
LAKE_ONE_R2 = [KEPT1_LAKE_Q, VEDDER_LAKE_KEEP3]

# ---------------------------------------------------------------------------------------------- the fields
FIELDS_LIVE = {
    "text": text,
    "text[:2]": lambda r: text(r)[:2],
    "text[1]": lambda r: text(r)[1],
    "text[0], and the over-50 group among the lines": lambda r: (text(r)[0], OVER50_ALL in text(r)),
    "headline says": lambda r: r["group"]["headline"]["says"],
    "headline also": lambda r: r["group"]["headline"]["also"],
    "headline says, lifts, total_line": lambda r: (r["group"]["headline"]["says"], r["group"]["lifts"],
                                                   r["group"]["total_line"]),
    "lifts: says": lambda r: [L["says"] for L in r["group"]["lifts"]],
    "total_line": lambda r: r["group"]["total_line"],
    "buckets: title": lambda r: [b["title"] for b in r["group"]["buckets"]],
    "buckets: notes says": lambda r: [[n["says"] for n in b["notes"]] for b in r["group"]["buckets"]],
    "buckets: sizes says": lambda r: [[n["says"] for n in b["sizes"]] for b in r["group"]["buckets"]],
    "buckets[1]: sizes says": lambda r: [n["says"] for n in r["group"]["buckets"][1]["sizes"]],
    "kind RB: n, notes": lambda r: (kind(r, "RB")["n"], kind(r, "RB")["notes"]),
    "kind RB: detail": lambda r: next(k for b in r["group"]["buckets"] for k in b["kinds"]
                                      if k["species"] == ["RB"])["detail"],
    "kind ST: name, n": lambda r: (kind(r, "ST")["name"], kind(r, "ST")["n"]),
    "kind DV: notes": lambda r: kind(r, "DV")["notes"],
    "the share tier's origin": lambda r: next(t for t in r["group"]["tree"][0]["children"]
                                              if t["tier"] == "share")["origin"],
    "possession: example, doubles": lambda r: (r["group"]["possession"]["example"],
                                               r["group"]["possession"]["doubles"]),
    "worked: q → a": lambda r: [w["q"] + " → " + w["a"] for w in r["group"]["worked"]],
    "worked away from here[:1]: q, a": lambda r: [[w["q"], w["a"]] for w in r["group"]["worked"]
                                                  if w["at"] != "here"][:1],
    "worked[0]: q, a, here": lambda r: [r["group"]["worked"][0][k] for k in ("q", "a", "here")],
    "elsewhere says": lambda r: r["elsewhere"]["says"],
}

# ---------------------------------------------------------------------------------------------- the cards
_VEDDER_DAY = {                                       # the Vedder's mouth part, Oct 10 and Jan 15 alike
    "headline says": HEADLINE_R2_STREAM,
    "lifts: says": [LIFT_RB4],
    "total_line": TOTAL_LINE_R2,
    "elsewhere says": STREAM_ELSEWHERE_R2,
    "worked away from here[:1]: q, a": [LAKE_ONE_R2],
}
LIVE = {
    (VEDDER, MOUTH, OCT10): {
        **_VEDDER_DAY,
        "text": [
            HEADLINE_R2_STREAM,                                     # T475 ask 1
            # ROUND20g 1: the origin is the stream share's (its tier says it), not the group's
            LIFT_RB4, TOTAL_LINE_R2,
            THIS_RIVERS_RULE,                                       # ROUND12 2: the river's own bucket first
            "RB 4: " + COUNTS_TOWARD_4,
            SHARE_TITLE_R2, OVER50_NOT_RB,
            "EB/GB/CT 2: ",
            "DV/LT 1: ",
            "ST 2: " + " · ".join(ST_DUTIES_VEDDER)],                # ROUND20g 2
        "the share tier's origin": WILD_GO_BACK,                  # ROUND20g 1
        # ROUND20h 3: possession on the group's TOTAL (p.21 "Possession quotas = 2 daily quotas")
        "possession: example, doubles": ("With 4 trout and char a day you may have 8: 2 days’ fish.",
                                         "Possession doubles the 4 a day, not the 2 from streams."),
        "kind RB: detail": RB_ANY_LENGTH_50,                       # ROUND12 1: p.22 "of any length 50 cm or less"
        "kind RB: n, notes": (4, [COUNTS_TOWARD_4]),
        "kind ST: name, n": ("Steelhead", 2),
        # ROUND11 A1 / ROUND15 1: the share's over-50 cap is its bucket's size group (the river's own bucket first)
        "buckets[1]: sizes says": [OVER50_NOT_RB],
        # ROUND12 4: ONE list — the keep-N count, then the examples it does not repeat
        "worked: q → a": [f"{KEEP4_RB} → {YOUR_DAY}", f"{KEEP2_RB} → {KEEP2_RB_SAYS}", " → ".join(LAKE_ONE_R2),
                          f"{OVER50_ELSEWHERE_Q} → {OVER50_ELSEWHERE_A}"],
    },
    (VEDDER, MOUTH, JAN15): dict(_VEDDER_DAY),
    (DUNCAN, DUNCAN_LOW, OCT10): {
        # ROUND10 W1: one lift line. T834 (RULINGS T17, T3a revised): the bull trout's own 2 equals the 2 from streams
        # and lifts nothing, so the line names rainbow alone
        "text[:2]": ["2 trout/char a day from streams", LIFT_RB5_R4],   # T475: Region 4 keeps wild fish too
        "lifts: says": [LIFT_RB5_R4],
        # ROUND12 2 / T834: the bull trout's own 2 a bucket inside the share, the rainbow's 5 the lifted one
        "buckets: title": [THIS_RIVERS_RULE, THIS_RIVERS_RULE,
                           "Up to 2 a day between them — all Region 4 streams combined"],
        "headline also": [{"n": 5, "says": "5 if they’re rainbow trout"}],
    },
    (LARDEAU_CREEK, 0, OCT10): {
        "text[1]": LIFT_RB5_DUNCAN_TRIB,
        "lifts: says": [LIFT_RB5_DUNCAN_TRIB],
    },
    (KOOTENAY_MAIN, 0, OCT10): {
        # T834 (box head "Lead with 5"): the lake's rainbow 10 is said as the rainbow's exception to the region's 5
        "text[:2]": ["5 trout/char a day",
                     "On this lake, rainbow trout up to 10 a day — they count toward the 5; past 5, rainbow trout only."],
    },
    (R2_LAKE, 0, OCT10): {
        "text[0], and the over-50 group among the lines": (HEADLINE_R2_LAKE, True),
    },
    (R2_STREAM, 0, OCT10): {
        "headline says": HEADLINE_R2_STREAM,
        "worked[0]: q, a, here": [KEPT2_STREAM_Q, NONE_MORE_HERE, 0],   # ROUND12 4: the one list
    },
    (CHILLIWACK_LAKE, 0, OCT10): {
        "headline says, lifts, total_line": (HEADLINE_R2_LAKE, [], None),
        # round 9 gate / ROUND11 A1: the lake's own Dolly Varden 1 is a bucket of its own
        "buckets: title": [THIS_LAKES_RULE, REST_OF_4],
        # ROUND12 3: Region 2's "Dolly Varden and lake trout 1" spans both buckets, said once on the first
        "buckets: notes says": [[DV_LT_ONE_IN_ALL], []],
        # ROUND15 1: the over-50 cap spanning both buckets is ONE size group, on the first
        "buckets: sizes says": [[OVER50_ALL], []],
        "kind DV: notes": [COUNTS_TOWARD_4],
    },
}


def _id(key) -> str:
    item, part, md = key
    where = "-".join(part) if isinstance(part, tuple) else f"part{part}"
    return f"{NAMES[item]}-{where}-{md[0]:02d}{md[1]:02d}"


@pytest.mark.needs_bundle
@pytest.mark.parametrize("key", list(LIVE), ids=_id)
def test_the_live_card_says(built, key):
    row = trout_row(built, *key)
    want = LIVE[key]
    assert {f: FIELDS_LIVE[f](row) for f in want} == want


def test_every_live_field_is_read_by_a_row():
    assert set(FIELDS_LIVE) - {f for row in LIVE.values() for f in row} == set()
