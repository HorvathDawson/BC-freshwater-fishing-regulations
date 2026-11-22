"""ROUND20i (user panel UR4's data words; rows 26, gear 12, glossary 5):

1. A kind's size says the RULE (`size_rule`): "must be 30 cm (12 in) or longer; release any shorter" — never where the range's end
   is the steelhead line (a rainbow over 50 cm on a steelhead water is a steelhead, said in its own row): "must be 30 cm
   (12 in) or longer; release any shorter"; none for a kind kept at any size, a steelhead, or a kind kept at none.
2. The worked keep-share line says what is still allowed HERE: "none from other streams today — up to 2 more of any kind,
   from here or from lakes." (Rob, UR4 R3: rainbows kept here do not block a cutthroat here).
3. A gear change's dates leave out the days the water is closed ("NEXT Jun 1–Apr 30" on a water closed Jun 1–30 → "Jul 1–
   Apr 30").
4. Fly-fishing-only and hook words name what the book's p.80 definitions exclude: "fly fishing" (floats, sinkers, attracting
   devices may not be attached), "artificial fly" (a single-pointed hook), "single hook" (one point; a treble has three).
5. The steelhead stop duty names the water as the book's "stop fishing that water" does: "stop fishing this river after 2 a
   day" (the water's own row's noun; "this stream" with none).
6c. "Shared with every Region 2 stream you fish today" (read as shared with Dad) → "Your own limit, counted across every
   Region 2 stream you fish today."
"""
from __future__ import annotations

from pipeline.deliver.answers import gear as GE
from pipeline.deliver.answers import glossary as G
from pipeline.deliver.bundle.glossary_source import rows_from_book
from pipeline.deliver.answers.tree import walk
from pipeline.deliver.calendar import SPEC
from pipeline.tests import answers28_fixture as F

OCT10 = (10, 10)


def _kinds(water, md=OCT10):
    out, P = F.card(water, md)
    return {tuple(n["species"]): n for r in out["rows"] if r.get("group")
            for n, _p in walk(r["group"]["tree"]) if n["node"] == "kind"}, out


# 1 -----------------------------------------------------------------------------------------------------------------
def test_a_size_says_the_rule():
    # (T866) the size rules said (the Vedder's brook/brown/cutthroat and char; a plain stream's rainbow, T503: the band,
    # over 50 a steelhead; the Bella Coola May cutthroat), word for word: card_words CARDS
    k, _o = _kinds(F.VEDDER)
    assert k[("ST",)]["size_rule"] is None                   # the steelhead row: over 50 cm IS what it is
    assert k[("RB",)]["size_rule"] is None                   # any length up to the steelhead line
    k, _o = _kinds(F.LAKE)
    assert k[("EB", "GB", "CT", "RB")]["size_rule"] is None  # any size


def test_a_slot_says_the_books_between():
    """owner 2026-10-28 (Q62): a kind kept up to A and from B is released BETWEEN them, in the book's word."""
    from pipeline.deliver.answers.tree import size_rule
    kn = {"n": 1, "species": ["LT"], "sizes": {"cm": [[0, 60], [90, None]]}}
    assert size_rule(kn, False, 50) == "release any between 60 cm (24 in) and 90 cm (35 in)"
    kn = {"n": 1, "species": ["LT"], "sizes": {"cm": [[30, 60], [90, None]]}}          # not a pure slot: both ranges
    assert size_rule(kn, False, 50) == "must be 30–60 cm (12–24 in), or 90 cm (35 in) or longer; release any other size"


# 2: the worked keep-share line saying what is still allowed here is the Vedder's `worked` row in
# test_answers_card_words.py CARDS (T866)


# 3 -----------------------------------------------------------------------------------------------------------------
def _ans(**parts):
    ps = [{"part": k, "says": v, "cites": [], "terms": []} for k, v in parts.items()]
    return {"summary": GE.summary_of(ps)}


def test_a_change_leaves_out_the_closed_days():
    d = SPEC.day_index
    a = [_ans(lines="1 line", attach="one hook, lure or fly"),            # 0: the year
         _ans(mode="Fly fishing only", lines="1 line", attach="one fly"),  # 1: May
         _ans(lines="1 line", attach="one hook, lure or fly")]             # 2: June, closed
    runs = [(1, 0), (d(5, 1), 1), (d(6, 1), 2), (d(7, 1), 0)]
    GE.add_changes(a, runs, closed={2})
    assert [c["when"] for c in a[1]["summary"]["changes"]] == ["Jul 1–Apr 30"]
    GE.add_changes(a, runs)                                               # unchanged without closed days
    assert [c["when"] for c in a[1]["summary"]["changes"]] == ["Jun 1–Apr 30"]


# 4 -----------------------------------------------------------------------------------------------------------------
def test_fly_and_hook_words_name_what_the_book_excludes():
    assert GE.FLY_ONLY_WEIGHT == ("no float or strike indicator, no sinker or split shot, no lure, spinner or other "
                                  "attracting device")
    assert GE.HOOK_WORDS["single_barbless"] == "a single barbless hook, no trebles"
    assert GE.HOOK_WORDS["single"] == "a single hook, no trebles"
    assert GE.summary_sentence({"lines": "1 line", "hook": GE.HOOK_WORDS["single_barbless"],
                                "attach": "one hook, lure or fly"}) == \
        "1 line with a single barbless hook, no trebles (one hook, lure or fly)."
    t = G.t_fly_fishing(G.Book(rows_from_book()))
    assert t["says"].startswith("Only an artificial fly on the line (one single-pointed hook) — no bait or lure, no float "
                                "or strike indicator, no sinker or split shot, no spinner or other attracting device.")
    assert G.VERSION == 5


# 5 -----------------------------------------------------------------------------------------------------------------
def test_the_stop_duty_names_the_water():
    # (T866) the Vedder's "this river", word for word: card_words CARDS
    k, _o = _kinds(F.STREAM)
    assert "stop fishing this stream after 2 a day" in k[("ST",)]["duties"]


# 6c: "Your own limit, counted across …" on a stream and on a lake is the Vedder's and the lake's `elsewhere` row in
# test_answers_card_words.py CARDS (T866)
