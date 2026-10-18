"""ROUND 14 WORDING (owner review of v15, 2026-10-16) — no data, CI's no-data tier.

  3  A count cap over a SUBSET of a bucket's kinds is a NESTED GROUP, not a note (size caps stay notes): the
     Bella Coola mainstem, Apr 1-May 31 ("trout/char daily quota = 2, of which only one may be a trout
     (cutthroat or rainbow)") has a sub-group "Cutthroat trout or rainbow trout: 1 between them" holding the
     two kinds with their own sizes. And the 2 is the river's OWN rule: its bucket is "This river’s rule",
     never "all Region 2 streams combined" though the region's share says the same number.
  4  Where steelhead are known (or steelhead rules apply), the presence line is INSIDE the steelhead section
     — the card's steelhead kind row, its not-kept steelhead line, or the steelhead's own row — exactly once.
  5  A weekday window is said as weekdays (`rows[].week`): Kootenay Lake's Lower West Arm kokanee "Keep up to
     5 on Saturdays and Sundays; release Monday to Friday", on both moments' frames.
  6  Not-kept lines with the same status from the same row are ONE line, in the book's species order (p.80):
     the Vedder on May 15 "Rainbow trout and cutthroat trout: release every one"; a group is said with its
     members: "Char (Dolly Varden/bull trout, lake trout and brook trout): release every one".

All run on `answers28_fixture` (the real reader and rows; the card gate where it applies).
"""
from __future__ import annotations

from pipeline.deliver import calendar as K
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import rows as RW
from pipeline.tests import answers28_fixture as F

OCT10, MAY1, MAY15 = (10, 10), (5, 1), (5, 15)
SAT = K.Moment(K.weekday_mask(["Saturday", "Sunday"]))
WEEK = K.Moment(K.weekday_mask(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]))
KNOWN = {"line": "known_with_rules", "says": "Steelhead are known to be in this water."}


def _group(water, md=OCT10):
    out, P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and "pool" in r["group"]
            and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]
    return g, out, P


# 3 ---------------------------------------------------------------------------------------------
def test_3_a_count_over_some_of_a_buckets_kinds_is_a_nested_group():
    g, _o, _P = _group(F.BELLA_MAY, MAY1)
    (b,) = [b for b in g["buckets"] if b["by"] == F.BELLA_R5]
    assert [n["says"] for n in b["notes"] if n["over_cm"] == 0] == []          # no count note
    (sub,) = b["groups"]
    assert (sub["says"], sub["n"], sub["by"]) == ("Cutthroat trout or rainbow trout: 1 between them", 1, F.BELLA_R6)
    assert [(k["name"], k["detail"], k["n"]) for k in sub["kinds"]] == [("Cutthroat trout", "hatchery, 30–33 cm", 1),
                                                                         ("Rainbow trout", "hatchery, 30–50 cm", 1)]
    assert not {S for k in b["kinds"] for S in k["species"]} & {"CT", "RB"}


def test_3_the_rivers_own_two_is_its_own_bucket_not_the_regions_share():
    g, _o, _P = _group(F.BELLA_MAY, MAY1)
    (b,) = [b for b in g["buckets"] if b["by"] == F.BELLA_R5]
    assert (b["title"], b["cap"], b["why"]) == ("This river’s rule", 2, "rule")
    held = {S for k in b["kinds"] for S in k["species"]} | {S for s in b["groups"] for k in s["kinds"]
                                                            for S in k["species"]}
    assert {"EB", "GB", "CT", "RB", "DV", "LT"} <= held
    assert not [x for x in g["buckets"] if "streams combined" in (x["title"] or "")
                and {S for k in x["kinds"] for S in k["species"]} & held]


def test_3_the_gate_proves_the_nested_group():
    out, P = F.card(F.BELLA_MAY, MAY1)
    assert CC.check_card(P, out) == []


def test_3_a_size_cap_is_no_nested_count_group():
    """ROUND15 1 revises "a size cap stays a note": it is a size group (`sizes`), never a count group."""
    g, _o, _P = _group(F.NESTED)
    assert all(not b.get("groups") for b in g["buckets"])
    assert any("over 30 cm" in n["says"] for b in g["buckets"] for n in b["sizes"])


# 4 ---------------------------------------------------------------------------------------------
def _placements(out):
    got = []
    for r in out["rows"]:
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


def test_4_the_presence_line_is_on_the_steelhead_kind_row():
    out, _P = F.card(F.STREAM, OCT10)
    assert _placements(out) == [("kind", ["ST"], KNOWN)]


def test_4_the_presence_line_is_on_the_closed_steelhead_line():
    out, _P = F.card(F.BELLA, OCT10)
    assert _placements(out) == [("not_kept", ["ST"], KNOWN)]


def test_4_no_presence_line_where_steelhead_are_not_known():
    out, _P = F.card(F.LAKE, OCT10)
    assert _placements(out) == [] and out["steelhead_line"] is None


# 5 ---------------------------------------------------------------------------------------------
def test_5_a_weekday_window_is_said_as_weekdays_on_every_moment():
    sat, _P = F.card(F.LWA, (10, 17), at=SAT)
    mon, _P = F.card(F.LWA, (10, 19), at=WEEK)
    got = RW.say_weekdays([sat, mon], [SAT.as_json(), WEEK.as_json()])
    says = "Keep up to 5 on Saturdays and Sundays; release Monday to Friday"
    for frame in got:
        (ko,) = [r for r in frame["rows"] if r["all_members"] == ["KO"]]
        assert (ko["week"]["species"], ko["week"]["says"]) == (["KO"], says)
        assert not [r for r in frame["rows"] if r["all_members"] != ["KO"] and r.get("week")]
    assert "week" not in [r for r in sat["rows"] if r["all_members"] == ["KO"]][0]   # inputs untouched


def test_5_weekdays_in_words():
    W = RW.weekdays_words
    assert W(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]) == "Monday to Friday"
    assert W(["Saturday", "Sunday"]) == "on Saturdays and Sundays"
    assert W(["Friday", "Saturday", "Sunday"]) == "Friday to Sunday"
    assert W(["Monday", "Wednesday"]) == "on Mondays and Wednesdays"


# 6 ---------------------------------------------------------------------------------------------
def test_6_same_status_same_row_is_one_line_in_the_books_order():
    g, _o, _P = _group(F.VEDDER_MAY, MAY15)
    assert g["not_kept"] == [{"species": ["RB", "CT"], "status": "release",
                              "says": "Rainbow trout and cutthroat trout: release every one",
                              "by": F.VEDDER_R6, "rules": [F.VEDDER_R6, F.VEDDER_R7],
                              "whose": "this river’s rule"}]


def test_6_a_group_is_said_with_its_members():
    g, _o, _P = _group(F.BELLA_TRIB, OCT10)
    assert [(x["species"], x["says"]) for x in g["not_kept"]] == [
        (["ST"], "Steelhead: no fishing for them, not even catch and release — release any you hook by accident"),
        (["DV", "LT", "EB"], "Char (Dolly Varden/bull trout, lake trout and brook trout): release every one")]


def test_6_the_gate_proves_the_merged_lines():
    for w, md in ((F.VEDDER_MAY, MAY15), (F.BELLA_TRIB, OCT10)):
        out, P = F.card(w, md)
        assert CC.check_card(P, out) == [], w
