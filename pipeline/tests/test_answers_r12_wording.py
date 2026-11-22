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


def _card(water, md=OCT10):
    out, P = F.card(water, md)
    rows = [r for r in out["rows"] if r.get("group") and r["group"]["pool"] in
            (F.TOTAL, "r2:wahleach_jones_lake@2-3::wahleach_jones_lake.r1",
             "r2:count_creek@2-3::count_creek.r1", "r2:nested_lake@2-3::nested_lake.r1")]
    assert len(rows) == 1, [r.get("group", {}).get("pool") for r in out["rows"]]
    return rows[0], out, P


# W1 / W2 --------------------------------------------------------------------------------------
# (T866) Each kind row's detail (the Vedder's "hatchery, any length, 50 cm or less", the stream's, the slot's two
# ranges) and the buckets' order (the water's own bucket first): test_answers_card_words.py CARDS.


# W3 --------------------------------------------------------------------------------------------
# (T866) A kind's own cap on its bucket (the slot), the count cap over two kind rows as a nested group (ROUND14 3;
# "only one may be a trout (cutthroat or rainbow)" names rainbow with no size, so it holds steelhead too: T623 F12,
# RULINGS S12), the nested caps in order on one note and the implied cap not said (Wahleach): word for word in
# test_answers_card_words.py CARDS.
def test_w3_nested_caps_in_order_on_one_note():
    row, _o, _P = _card(F.NESTED)
    (note,) = row["group"]["buckets"][0]["sizes"]
    assert (note["n"], note["over_cm"]) == (2, 30)
    assert [(t["n"], t["over_cm"]) for t in note["then"]] == [(1, 50)]
    states = [s["r"] for s in row["group"]["states"]]
    assert "z2:daily_quotas::trout_char_quota.r2" in states and len(states) == len(set(states))


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
    # (T866) the lines word for word (ROUND20b C4: the ends) and the row's `elsewhere` sentence: card_words CARDS
    assert row["elsewhere"]["ask"] == []
    lake, over = w[2], w[3]                      # ROUND20b C4: two keep-N lines
    assert (lake["kept"], lake["at"], lake["here"], lake["most"]) == (1, "lake", 3, "exactly")
    assert (over["kept"], over["at"], over["over_cm"], over["here"], over["most"]) == \
        (1, "elsewhere", 50, 1, "at_most")


def test_w4_no_lift_the_elsewhere_lines_are_the_list():
    row, _o, _P = _card(F.STREAM)
    w = row["group"]["worked"]
    # (T866) the stream's lines and the lake's first, word for word: test_answers_card_words.py CARDS
    assert [x["here"] for x in w] == [0, 2, 1]
    assert row["elsewhere"]["ask"] == []


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
    # ROUND13 G/P: no "any hook", "weight on the line"; UR1 G. (T866) The four sentences word for word:
    # test_answers_card_words.py GEAR vedder / okanagan / kootenay / cowichan
    assert not any("—" in s for s in (vedder, okanagan, kootenay, cowichan))
    assert not any("any hook" in s for s in (vedder, okanagan, kootenay, cowichan))   # ROUND13 G (T866 D4: from r13)


def test_t623_f12_the_size_group_gate_asks_some_n_not_the_first_n():
    """Count Creek on Oct 10: Region 2's "1 over 50 cm (2 hatchery steelhead allowed)" says steelhead do not count
    (RULINGS Q16), and "only one may be a trout (cutthroat or rainbow)" now holds steelhead too (S12). After a
    cutthroat over 50 cm no steelhead may be kept (the count cap's doing); after a brown trout one may. The gate asks
    whether SOME 1 of the group's kinds over 50 cm leaves a steelhead keepable: it passes. MUTATION: the group says a
    kind it counts (cutthroat) does not count — no base of 1 leaves a cutthroat over 50 cm keepable: refused."""
    row, out, P = _card(F.COUNT)
    (sz,) = row["group"]["buckets"][0]["sizes"]
    assert (sz["not_counting"], sz["n"], sz["over_cm"]) == (["ST"], 1, 50)
    assert CC.check_card(P, out) == []
    bad = copy.deepcopy(out)
    next(r for r in bad["rows"] if r.get("group") is not None and r["group"]["pool"] == row["group"]["pool"]) \
        ["group"]["buckets"][0]["sizes"][0]["not_counting"] = ["CT"]
    assert any("do not count" in p for p in CC.check_card(P, bad))
