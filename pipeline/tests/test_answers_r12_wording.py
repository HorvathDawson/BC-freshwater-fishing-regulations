"""ROUND 12 WORDING (owner review of v13, 2026-10-16) — no data, CI's no-data tier.

  W1  a kind row's size says what is kept AND where a usual limit is lifted: the Vedder's hatchery rainbow
      "any length, 50 cm or less" (p.22 "of any length 50 cm or less" lifts Region 2's 30 cm minimum);
      a slot reads as its two ranges ("60 cm or less, or 90 cm or longer"); other details unchanged.
  W2  a water's own rule bucket(s) come FIRST, then the region's share, then "the rest".
  W3  every size (or count) cap inside a group is said once, in the bucket it governs: a kind's own cap
      ("Only 1 rainbow trout may be over 90 cm"), a count cap over two kind rows ("Only 1 of these may be
      cutthroat trout or rainbow trout"), nested caps in order on one note ("Only 2 of these may be over
      30 cm, and only 1 over 50 cm"); a cap another one implies is not said (Wahleach's 1 over 40 cm
      leaves Region 2's over-50 caps nothing to add). The gate proves every one, `then` included.
  W4  "How it adds up" and "Fished elsewhere today?" are ONE list, `group.worked`: on a lifted water the
      keep-N lines and only the elsewhere lines they do not repeat (a lake, over 50 cm across waters); on
      a water with no lift the elsewhere lines; the card row's `elsewhere.ask` is empty (folded). Every
      line carries the basket it starts from and the numbers the gate proves.
  W5  `gear.summary.says` has no em dash: "1 line with a single barbless hook, no trebles (one hook, lure or fly).
      Weight up to 1 kg."

W1-W4 run on `answers28_fixture` (the real reader and rows); W5 on `gear.resolve` with the real clauses.
"""
from __future__ import annotations

import copy

from pipeline.deliver.answers import card_check as CC
from pipeline.tests import answers28_fixture as F
from pipeline.tests import test_answers_r11_wording as R11

OCT10 = (10, 10)
SHARE_TITLE = "Up to 2 a day between them — all Region 2 streams combined"


def _card(water, md=OCT10):
    out, P = F.card(water, md)
    rows = [r for r in out["rows"] if r.get("group") and r["group"]["pool"] in
            (F.TOTAL, "r2:wahleach_jones_lake@2-3::wahleach_jones_lake.r1",
             "r2:count_creek@2-3::count_creek.r1", "r2:nested_lake@2-3::nested_lake.r1")]
    assert len(rows) == 1, [r.get("group", {}).get("pool") for r in out["rows"]]
    return rows[0], out, P


def _kinds(g):
    return {k["name"]: k for b in g["buckets"] for k in b["kinds"]}


def _notes(g):
    # ROUND15 1: the size caps are size groups now (`sizes`); `notes` keep the counts spanning buckets
    return [(b["title"], n["says"]) for b in g["buckets"] for n in b["notes"] + b["sizes"]]


# W1 --------------------------------------------------------------------------------------------
def test_w1_vedder_rainbow_any_length_50_cm_or_less():
    row, _o, _P = _card(F.VEDDER)
    # ROUND15 8: each kept kind says its origin
    assert _kinds(row["group"])["Rainbow trout"]["detail"] == "hatchery, any length, 50 cm or less"


def test_w1_other_details_stay_exact():
    row, _o, _P = _card(F.STREAM)
    k = _kinds(row["group"])
    assert k["Rainbow trout"]["detail"] == "hatchery, 30–50 cm"
    assert k["Brook trout, brown trout and cutthroat trout"]["detail"] == "hatchery, 30 cm or longer"
    assert k["Dolly Varden/bull trout and lake trout"]["detail"] == "hatchery, 60 cm or longer"


def test_w1_a_slot_reads_as_its_two_ranges():
    row, _o, _P = _card(F.SLOT)
    assert _kinds(row["group"])["Rainbow trout"]["detail"] == "60 cm or less, or 90 cm or longer"


# W2 --------------------------------------------------------------------------------------------
def test_w2_the_waters_own_bucket_first():
    row, _o, _P = _card(F.VEDDER)
    assert [b["title"] for b in row["group"]["buckets"]] == ["This river’s rule", SHARE_TITLE]
    row, _o, _P = _card(F.TWO)
    assert [b["title"] for b in row["group"]["buckets"]] == ["This river’s rule", SHARE_TITLE]
    row, _o, _P = _card(F.SLOT)
    assert [b["title"] for b in row["group"]["buckets"]] == ["This lake’s rule", "The rest of the 4 a day"]


# W3 --------------------------------------------------------------------------------------------
def test_w3_a_kinds_own_cap_on_its_bucket():
    row, _o, _P = _card(F.SLOT)
    assert _notes(row["group"]) == [("This lake’s rule", "Rainbow trout: only 1 over 90 cm"),
                                    ("The rest of the 4 a day", "Cutthroat trout, brown trout, Dolly Varden/bull trout, lake trout or brook trout: only 1 over 50 cm between them")]


def test_w3_a_count_cap_over_two_kind_rows():
    """ROUND14 3 (owner): the count over two of the bucket's kind rows is a NESTED GROUP now, not a note."""
    row, _o, _P = _card(F.COUNT)
    assert _notes(row["group"]) == [(None, "Cutthroat trout, brown trout, Dolly Varden/bull trout, lake trout or brook trout: only 1 over 50 cm between them")]
    (sub,) = row["group"]["buckets"][0]["groups"]
    assert (sub["says"], sub["n"], [k["species"] for k in sub["kinds"]]) == (
        "Cutthroat trout or rainbow trout: 1 between them", 1, [["CT"], ["RB"]])


def test_w3_nested_caps_in_order_on_one_note():
    row, _o, _P = _card(F.NESTED)
    (note,) = row["group"]["buckets"][0]["sizes"]
    assert note["says"] == "Rainbow trout, cutthroat trout, brown trout, Dolly Varden/bull trout, lake trout or brook trout: only 2 over 30 cm between them, and only 1 over 50 cm"
    assert (note["n"], note["over_cm"]) == (2, 30)
    assert [(t["n"], t["over_cm"]) for t in note["then"]] == [(1, 50)]
    states = [s["r"] for s in row["group"]["states"]]
    assert "z2:trout_char_quota::trout_char_quota.r2" in states and len(states) == len(set(states))


def test_w3_an_implied_cap_is_not_said():
    row, _o, _P = _card(F.WAHLEACH)
    assert _notes(row["group"]) == [(None, "Rainbow trout, cutthroat trout, brown trout, Dolly Varden/bull trout, lake trout or brook trout: only 1 over 40 cm between them")]


def test_w3_the_gate_fails_a_wrong_nested_or_count_note():
    """MUTATION: the nested 1 over 50 cm said as 2; the count note said as 2."""
    row, out, P = _card(F.NESTED)
    bad = copy.deepcopy(out)
    next(r for r in bad["rows"] if r.get("group") is not None and r["group"]["pool"] == row["group"]["pool"]) \
        ["group"]["buckets"][0]["sizes"][0]["then"][0]["n"] = 2
    assert any("nested" in p for p in CC.check_card(P, bad))
    row, out, P = _card(F.COUNT)
    bad = copy.deepcopy(out)
    next(r for r in bad["rows"] if r.get("group") is not None and r["group"]["pool"] == row["group"]["pool"]) \
        ["group"]["buckets"][0]["groups"][0]["n"] = 2
    assert any("group" in p for p in CC.check_card(P, bad))


# W4 --------------------------------------------------------------------------------------------
def test_w4_vedder_one_list_keep_n_then_what_it_does_not_repeat():
    row, _o, _P = _card(F.VEDDER)
    w = row["group"]["worked"]
    assert [x["q"] for x in w] == ["If you keep 4 hatchery rainbow trout here", "If you keep 2 hatchery rainbow trout here",   # ROUND20b C4: the ends
                                   "Kept 1 at a lake?", "Kept trout or char over 50 cm (20 in) elsewhere today?"]
    assert row["elsewhere"]["ask"] == []
    assert row["elsewhere"]["says"] == "Your own limit, counted across every Region 2 stream you fish today."
    lake, over = w[2], w[3]                      # ROUND20b C4: two keep-N lines
    assert (lake["kept"], lake["at"], lake["here"], lake["most"]) == (1, "lake", 3, "exactly")
    assert (over["kept"], over["at"], over["over_cm"], over["here"], over["most"]) == \
        (1, "elsewhere", 50, 1, "at_most")


def test_w4_no_lift_the_elsewhere_lines_are_the_list():
    row, _o, _P = _card(F.STREAM)
    w = row["group"]["worked"]
    assert [(x["q"], x["a"]) for x in w] == [
        ("Kept 2 on another stream?", "None more here"),
        ("Kept 1 at a lake?", "Keep 2 more here. Fish from lakes count toward the 4 a day, not toward the 2 from streams."),
        ("Kept trout or char over 50 cm (20 in) elsewhere today?",
         "It counts here too: at most 2 over 50 cm (20 in) a day in all — only 1 of them may be anything other "
         "than a hatchery steelhead.")]
    assert [x["here"] for x in w] == [0, 2, 1]
    assert row["elsewhere"]["ask"] == []
    row, _o, _P = _card(F.LAKE)
    assert [(x["q"], x["a"], x["here"]) for x in row["group"]["worked"]][0] == \
        ("Kept 2 elsewhere today?", "Keep 2 more here", 2)


def test_w4_the_gate_fails_a_wrong_line():
    """MUTATION: the lake line says 2 more here (the rules allow 3)."""
    row, out, P = _card(F.VEDDER)
    bad = copy.deepcopy(out)
    g = next(r for r in bad["rows"] if r.get("group") is not None and r["group"]["pool"] == F.TOTAL)["group"]
    g["worked"][2]["here"] = 2
    assert any("worked" in p for p in CC.check_card(P, bad))


# W5 --------------------------------------------------------------------------------------------
def test_w5_gear_sentences_have_no_em_dash():
    vedder = R11._rules("stream", hook=R11.Z2_HOOK)["summary"]["says"]
    okanagan = R11._rules("lake")["summary"]["says"]
    kootenay = R11._rules("lake", kootenay=True)["summary"]["says"]
    cowichan = R11._rules("stream", hook=R11.Z1_HOOK, fly=R11.COWICHAN_FLY)["summary"]["says"]
    # ROUND13 G/P: no "any hook", "weight on the line" (test_answers_r13_wording.py)
    assert vedder == "1 line with a single barbless hook, no trebles (one hook, lure or fly). Weight on the line up to 1 kg (2.2 lb)."
    assert okanagan == "1 line (2 while alone in a boat) with one hook, lure or fly. Weight on the line up to 1 kg (2.2 lb)."
    assert kootenay == ("1 line from shore; any number from a boat, each with one hook, lure or fly. "
                        "Weight on the line up to 1 kg (2.2 lb).")              # UR1 G
    assert cowichan == ("Fly fishing only: 1 line with a single barbless hook, no trebles (one fly). No float or strike indicator, no sinker or "
                        "split shot, no lure, spinner or other attracting device.")
    assert not any("—" in s for s in (vedder, okanagan, kootenay, cowichan))
