"""ROUND 11 WORDING (owner review of v12, 2026-10-16) — no data, CI's no-data tier.

  A1  a size cap a bucket's kinds share is said ONCE, as the bucket's note ("Only 1 of these may be over
      50 cm, not counting steelhead"); kind rows carry only their own size and origin — never "only 1
      over 50 cm" / "uses your 1 over 50 cm" / "shares the 1 over 50 cm above". A cap spanning buckets
      (Chilliwack Lake: its own Dolly Varden 1 and Region 2's lake trout) is said once, on the first
      bucket it touches, naming the day's fish. The gate proves the note's number.
  A2  `gear.summary`: ONE sentence of the day's terminal gear, made from the count / hook / barb / lure /
      weight answers ("1 line, a single barbless hook — one hook, lure or fly — and up to 1 kg of
      weight."), its `terms` (glossary keys) and `cites`, and `changes` — the other days of the year
      that read differently ({when: "May 1–31", says: "fly fishing only"}). A stream's Lines row says
      "1 line (rod)" (no ", in a boat too"); a lake keeps its boat clause.
  A3  a section relying on a lift INHERITED by the tributary walk (RULINGS T13 / T3a) carries a line
      naming whose rule it is, what it lifts and when (`display` frame `inherited`); a quota lift the
      card already names is not said twice.

A1 runs on `answers28_fixture` (the real reader and rows); A2 on `gear.resolve` with the real clauses;
A3 on `display.inherited_line` / `display.compose` with the export's own records.
"""
from __future__ import annotations

import copy

from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import display as D
from pipeline.deliver.answers import gear as G
from pipeline.tests import answers28_fixture as F

OCT10 = (10, 10)
SIZE_WORDS = ("only 1 over", "uses your", "shares the", "over 50 cm")


def _g(water, md=OCT10):
    out, _P = F.card(water, md)
    return F.trout_group(out)


def _row_notes(g):
    return [t for b in g["buckets"] for k in b["kinds"] for t in k["notes"]]


# A1 --------------------------------------------------------------------------------------------
def test_a1_the_share_bucket_says_the_over_50_cap_once_steelhead_aside():
    g = _g(F.VEDDER)
    share = g["buckets"][0]
    assert share["title"] == "Up to 2 a day between them — all Region 2 streams combined"
    assert share["notes"] == [{"says": "Only 1 of these may be over 50 cm, not counting steelhead",
                               "n": 1, "over_cm": 50, "kinds": ["EB", "GB", "CT", "DV", "LT"],
                               "not_counting": ["ST"]}]
    assert g["buckets"][1]["notes"] == []                       # this river's rule: rainbows ≤ 50 cm
    assert not any(w in t for t in _row_notes(g) for w in SIZE_WORDS), _row_notes(g)
    assert _row_notes(g) == ["counts toward the 4 a day"]


def test_a1_a_lake_says_it_on_its_only_bucket():
    g = _g(F.LAKE)
    assert [b["title"] for b in g["buckets"]] == [None]
    assert [n["says"] for n in g["buckets"][0]["notes"]] == ["Only 1 of these may be over 50 cm"]
    assert _row_notes(g) == []


def test_a1_a_cap_spanning_buckets_is_said_once_where_the_reader_meets_it():
    """Chilliwack Lake: its own row's Dolly Varden 1 (over 60 cm) and Region 2's lake trout and other
    trout share Region 2's 1 over 50 cm — said on the first bucket, naming the day's fish."""
    g = _g(F.CLAKE)
    assert [b["title"] for b in g["buckets"]] == ["This lake’s rule", "The rest of the 4 a day"]
    assert g["buckets"][0]["notes"] == [{"says": "Only 1 of the day’s trout and char may be over 50 cm",
                                         "n": 1, "over_cm": 50,
                                         "kinds": ["EB", "GB", "CT", "DV", "LT", "RB"],
                                         "not_counting": []}]
    assert g["buckets"][1]["notes"] == []
    assert _row_notes(g) == ["counts toward the 4 a day"]
    states = [s["r"] for s in g["states"]]
    assert len(states) == len(set(states))


def test_a1_the_gate_proves_the_bucket_note():
    for w in (F.VEDDER, F.STREAM, F.LAKE, F.CLAKE, F.TWO):
        out, P = F.card(w, OCT10)
        assert CC.check_card(P, out) == [], w


def test_a1_the_gate_fails_a_wrong_bucket_note():
    """MUTATION: the note says 2 over 50 cm; then: steelhead said to count."""
    out, P = F.card(F.VEDDER, OCT10)
    bad = copy.deepcopy(out)
    F.trout_group(bad)["buckets"][0]["notes"][0]["n"] = 2
    assert any("note" in p for p in CC.check_card(P, bad))
    bad = copy.deepcopy(out)
    n = F.trout_group(bad)["buckets"][0]["notes"][0]
    n["kinds"], n["not_counting"] = n["kinds"] + ["ST"], []
    assert any("note" in p for p in CC.check_card(P, bad))


# A2 --------------------------------------------------------------------------------------------
PROVINCE = "zp:terminal_tackle::terminal_tackle.r1"
ATTACH = "zp:terminal_tackle::terminal_tackle.r6"
WEIGHT = "zp:terminal_tackle::terminal_tackle.r4"
FLIES = "zp:terminal_tackle::terminal_tackle.r3"
Z2_HOOK = "z2:single_barbless_hook::single_barbless_hook.r1"
Z1_HOOK = "z1:single_barbless_hook::single_barbless_hook.r1"
KOOTENAY = "r4:kootenay_lake_main_body::kootenay_lake_main_body.r1"
VEDDER_FLY = "r2:chilliwack_vedder_rivers::chilliwack_vedder_rivers.r4"
COWICHAN_FLY = "r1:cowichan_river::cowichan_river.r7"
TACKLE = {"family": "gear_and_method", "type": "tackle_restriction"}


def _rules(kind, hook=None, kootenay=False, fly=None):
    rules = [
        G.Rule(PROVINCE, 4, {**TACKLE, "gear": [
            {"max": 2.0, "slot": "lines_per_angler", "when": {"angler": "alone_in_boat", "water": "lake"}},
            {"max": 1.0, "slot": "lines_per_angler"}]}),
        G.Rule(FLIES, 4, {**TACKLE, "gear": [{"max": 1.0, "slot": "flies_per_line"}]}),
        G.Rule(WEIGHT, 4, {**TACKLE, "gear": [{"max": 1.0, "slot": "weight_per_line_kg",
                                               "unless": [{"gear_in_use": "downrigger_weight"}]}]}),
        G.Rule(ATTACH, 4, {**TACKLE, "gear": [{"max": 1.0, "slot": "terminal_attachments_per_line",
                                               "members": ["hook", "artificial_lure", "artificial_fly"]}]})]
    if hook:
        rules.append(G.Rule(hook, 3, {**TACKLE, "water": "stream", "gear": [
            {"only": ["barbless"], "slot": "barb"}, {"max": 1.0, "slot": "points_per_hook"}]}))
    if kootenay:
        rules.append(G.Rule(KOOTENAY, 0, {**TACKLE, "gear": [
            {"slot": "lines_per_angler", "unlimited": True, "when": {"angler": "in_boat"}}]}))
    if fly:
        rules.append(G.Rule(fly, 0, {**TACKLE, "type": "method_restriction",
                                     "gear": [{"only": ["fly_fishing"], "slot": "method"}]}))
    return G.resolve(rules, kind, ["angling"])


def test_a2_vedder_one_sentence_of_terminal_gear():
    s = _rules("stream", hook=Z2_HOOK)["summary"]
    assert s["says"] == "1 line, a single barbless hook — one hook, lure or fly — and up to 1 kg of weight."
    assert s["terms"] == ["single_barbless_hook"]
    assert s["cites"] == [PROVINCE, Z2_HOOK, ATTACH, WEIGHT]
    assert s["changes"] == []


def test_a2_a_stream_says_one_line_a_lake_keeps_its_boat_clause():
    assert _rules("stream", hook=Z2_HOOK)["counts"]["lines_per_angler"]["says"] == "1 line (rod)"
    okanagan = _rules("lake")
    assert okanagan["counts"]["lines_per_angler"]["says"] == "1 line (rod); 2 while alone in a boat"
    assert okanagan["summary"]["says"] == ("1 line (2 while alone in a boat), any hook — one hook, lure or "
                                           "fly — and up to 1 kg of weight.")
    assert okanagan["summary"]["terms"] == []


def test_a2_kootenay_main_body_no_limit_in_a_boat():
    s = _rules("lake", kootenay=True)["summary"]
    assert s["says"] == ("1 line (no limit while in a boat), any hook — one hook, lure or fly — and up to "
                         "1 kg of weight.")
    assert s["cites"] == [PROVINCE, KOOTENAY, ATTACH, WEIGHT]


def test_a2_cowichan_fly_only_part():
    s = _rules("stream", hook=Z1_HOOK, fly=COWICHAN_FLY)["summary"]
    assert s["says"] == ("Fly fishing only: 1 line, a single barbless hook — one fly — with no float, "
                         "sinker or attracting device.")
    assert s["terms"] == ["fly_fishing", "single_barbless_hook"]
    assert s["cites"] == [COWICHAN_FLY, PROVINCE, Z1_HOOK, ATTACH]


def test_a2_dated_changes_name_the_other_days():
    """The Vedder: fly fishing only May 1-31 (its r4), the rest of the year the plain sentence."""
    plain, may = _rules("stream", hook=Z2_HOOK), _rules("stream", hook=Z2_HOOK, fly=VEDDER_FLY)
    year = [plain, may]
    G.add_changes(year, [(1, 0), (122, 1), (153, 0)])           # Jan 1, May 1, Jun 1 (leap calendar)
    assert plain["summary"]["changes"] == [{"when": "May 1–31", "says": "fly fishing only",
                                            "cites": [VEDDER_FLY]}]
    assert plain["summary"]["terms"] == ["single_barbless_hook", "fly_fishing"]
    assert may["summary"]["changes"] == [{"when": "Jun 1–Apr 30",
                                          "says": "one hook, lure or fly and up to 1 kg of weight",
                                          "cites": [ATTACH, WEIGHT]}]


def test_a2_a_year_with_one_reading_has_no_changes():
    one = _rules("lake")
    G.add_changes([one], [(1, 0)])
    assert one["summary"]["changes"] == []


# A3 --------------------------------------------------------------------------------------------
QUINSAM = "r1:quinsam_river@1-6::quinsam_river.r3"
SUMMER = "z1:summer_stream_closure::summer_stream_closure.r1"
DUNCAN = "r4:duncan_river@4-19::duncan_river.r2"
WINTER = "z4:trout_char_winter_release::trout_char_winter_release.r1"
DOC = {
    "entries": {
        "r1:quinsam_river@1-6": {"kind": "water", "name": "Quinsam River", "pages": [18]},
        "z1:summer_stream_closure": {"kind": "area", "name": "Streams of MUs 1-1 to 1-6",
                                     "full_name": "Summer stream closure", "pages": [13]},
        "r4:duncan_river@4-19": {"kind": "water", "name": "Duncan River", "pages": [36]},
        "z4:trout_char_winter_release": {"kind": "zone", "name": "Every stream in Region 4",
                                         "full_name": "Trout and char winter release", "pages": [33]}},
    "rules": {
        QUINSAM: {"id": QUINSAM, "entry_id": "r1:quinsam_river@1-6", "label": "Summer stream closure lifted",
                  "parts": {"what": "Summer stream closure lifted"}, "provenance": {"rank": 0},
                  "fields": {"species": ["ALL_GAME_FISH"], "exempts": [
                      {"entry_id": "z1:summer_stream_closure", "rule_id": "summer_stream_closure.r1"}]}},
        SUMMER: {"id": SUMMER, "entry_id": "z1:summer_stream_closure", "type": "retention_limit",
                 "label": "No fishing in streams, Jul 15-Aug 31 — within Management Units 1-1 to 1-6",
                 "parts": {"what": "No fishing in streams", "when": "Jul 15-Aug 31",
                           "where": "within Management Units 1-1 to 1-6"},
                 "provenance": {"rank": 2},
                 "fields": {"species": ["ALL_GAME_FISH"], "take": 0, "may_target": False, "water": "stream",
                            "when": {"dates": [{"from_day": 15, "from_month": 7, "to_day": 31,
                                                "to_month": 8}]}}},
        DUNCAN: {"id": DUNCAN, "entry_id": "r4:duncan_river@4-19",
                 "label": "Trout and char winter release lifted for dolly varden/bull trout — downstream of "
                          "Duncan Lake", "provenance": {"rank": 0},
                 "fields": {"species": ["DV"], "exempts": [
                     {"entry_id": "z4:trout_char_winter_release", "rule_id": "trout_char_winter_release.r1"}]}},
        WINTER: {"id": WINTER, "entry_id": "z4:trout_char_winter_release", "type": "retention_limit",
                 "label": "Trout and char — release all, from streams, Nov 1-Mar 31",
                 "parts": {"what": "Trout and char — release all", "when": "Nov 1-Mar 31"},
                 "provenance": {"rank": 3},
                 "fields": {"species": ["TROUT_CHAR"], "take": 0, "may_target": True, "water": "stream",
                            "when": {"dates": [{"from_day": 1, "from_month": 11, "to_day": 31,
                                                "to_month": 3}]}}}},
    "waters": {}}


def test_a3_a_quinsam_tributary_says_whose_exemption_opens_it():
    assert D.inherited_line(QUINSAM, [SUMMER], DOC) == (
        "Open Jul 15–Aug 31: the Quinsam River’s exemption from the summer stream closure (MUs 1-1 to 1-6) "
        "covers its tributaries.")


def test_a3_a_release_lift_says_which_fish_may_be_kept():
    assert D.inherited_line(DUNCAN, [WINTER], DOC) == (
        "Dolly Varden/bull trout may be kept Nov 1–Mar 31: the Duncan River’s exemption from the trout and "
        "char winter release covers its tributaries.")


def test_a3_the_line_names_the_row_applies_trib_names():
    trib = D.applies_here(DOC["rules"][QUINSAM], DOC)[1]
    assert trib.startswith("Inherited from Quinsam River:")
    assert "the Quinsam River’s" in D.inherited_line(QUINSAM, [SUMMER], DOC)


def test_a3_a_lift_the_card_names_is_not_said_again():
    frame = {"status": "own", "closing": [],
             "inherited": [{"says": "Open …", "by": 7, "lifts": [3]}, {"says": "Up to 5 …", "by": 9, "lifts": [4]}]}
    rows = {"rows": [{"group": {"lifts": [{"each": [{"by": 9}]}]}}], "fish": {}}
    got = D.compose(frame, {"rows": rows, "gear": {}})
    assert got["inherited"] == [{"says": "Open …", "by": 7, "lifts": [3]}]
    got = D.compose({**frame, "inherited": frame["inherited"][1:]}, {"rows": rows, "gear": {}})
    assert "inherited" not in got


def test_a2_every_term_the_sentence_names_is_a_glossary_term():
    from pipeline.deliver.answers import glossary as GL
    book = GL.Book()
    ids = {GL.t_fly_fishing(book)["id"], GL.t_artificial_fly(book)["id"], "single_barbless_hook"}
    named = {t for _lead, t in G.FLY_MODES.values()} | {"single_barbless_hook"}
    assert named <= ids
    assert {g.__name__ for g in GL.GENERATORS} >= {"t_fly_fishing", "t_artificial_fly", "t_hook"}
