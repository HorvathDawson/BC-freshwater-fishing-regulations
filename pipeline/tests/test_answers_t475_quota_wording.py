"""T475 (owner 2026-11-07): the quota block — one clear headline, sub-limits as parts of it. Four asks:

  1  the stream share in one line, "2 hatchery trout/char a day from streams": the hatchery-only rule FOLDED IN from a
     field (`headline.origin`, the share tier's `origin_kind` — the resolved rules, never the sentence), and an ⓘ
     (`headline.info`) on the regional total the share is part of, with examples made from this card's numbers (each one
     proved by the card gate);
  2  the steelhead line "Steelhead: closed, no catch and release", what an accidental hook means in its ⓘ (`term`);
  3  every sub-limit "up to N", never "only";
  4  sub-limits read as parts of the headline total ("Up to 2 brook, brown or cutthroat trout combined"), not "Of your 2
     trout and char a day in streams, …" on every line (T128's named total is gone).

NOTHING DROPPED (`dropped`): over the real kinds of waters — a hatchery-only stream, a wild-allowed stream, a lake, a water
with no regional total, combined sub-limits, a single species, size conditions, the steelhead line — every rule is still on
the card: the card gate proves every number, every kind once and every size cap (`card_check.check_card`); every fish the
row holds is a kind or a not-kept line; every kind's line states its number and its fish; every kind keeping one origin
says it — on its own line, by the headline over the share it sits in, or by the group's origin line. Each MUTATION below
removes one rule and `dropped` names it.

No data: Region 2's real table (`answers28_fixture`) and the gate-class waters (`test_answers_28_gate_classes`: the Dean,
a Region 5 stream keeping wild fish), through the real reader and rows.
"""
from __future__ import annotations

import copy
import re
from typing import List

import pytest

from pipeline.deliver.answers import card as CA
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import tree as TR
from pipeline.deliver.answers.rows import _keepish, main_res
from pipeline.deliver.answers.tree import walk
from pipeline.tests import answers28_fixture as F
from pipeline.tests import test_answers_28_gate_classes as GC

OCT10, JAN15, MAY1, MAY15, JUL15 = (10, 10), (1, 15), (5, 1), (5, 15), (7, 15)

#: (water, date, what it is) — every shape the ticket names
FIXTURE_WATERS = [
    (F.STREAM, OCT10, "hatchery-only stream, a steelhead kind, a size cap with a steelhead extra"),
    (F.FRASER, OCT10, "hatchery-only stream, steelhead closed (the steelhead line)"),
    (F.FRASER, JUL15, "the same stream outside the steelhead closure"),
    (F.VEDDER, OCT10, "a water's own lift out of the share (a single species, a size range)"),
    (F.TWO, OCT10, "two lifts from one row"),
    (F.LAKE, OCT10, "a lake: the regional total, no share"),
    (F.CLAKE, OCT10, "a lake with its own Dolly Varden rule inside the region's size cap"),
    (F.SLOT, OCT10, "a slot size on a lake"),
    (F.NESTED, OCT10, "two nested size caps"),
    (F.WAHLEACH, OCT10, "a lake's own total and size cap"),
    (F.BELLA, OCT10, "no regional total (the river's own 1), steelhead closed, cutthroat released"),
    (F.COUNT, OCT10, "no regional total, a count over two kinds"),
    (F.BELLA_MAY_ST, MAY1, "an own rule under the region's total, a count tier, steelhead closed"),
    (F.BELLA_TRIB, OCT10, "a group released (char)"),
    (F.VEDDER_MAY, MAY15, "a release line of two fish"),
]
GATE_WATERS = [("dean", JUL15, "a wild-allowed stream (Region 5), the river's own 1 under the region's 5"),
               ("lonzo", JUL15, "a Region 2 stream whose char go back, the share folded into one kind"),
               ("coquitlam", JUL15, "a steelhead stream whose own rule prints the share's number"),
               ("polley", JUL15, "a lake's own total with orphan count clauses")]


def _card(water, md):
    return GC.card(water, md) if water in GC.WATERS else F.card(water, md)


def _origins(out: dict, S: str) -> List[str]:
    f = out["fish"].get(S) or {}
    return [o for o in ("hatchery", "wild") if f.get(o) and _keepish(f[o]["status"])]


def dropped(out: dict, P) -> List[str]:
    """Every rule of the card that is NOT on it (empty: nothing dropped)."""
    probs = list(CC.check_card(P, out))
    for row in out["rows"]:
        g = row.get("group")
        if not g:
            continue
        where = g["pool"]
        # every fish the row holds is a kind of the group or a not-kept line (closed / released here)
        nk = {S for x in g["not_kept"] for S in x["species"]}
        for S in row["all_members"]:
            m = main_res(out["fish"][S]["hatchery"], out["fish"][S]["wild"]) if S in out["fish"] else None
            if S not in g["kinds"] and m and m["status"] in ("closed", "release") and S not in nk:
                probs.append(f"{where}: {S} is {m['status']} here and the card does not say it")
        for x in g["not_kept"]:
            if x["status"] == "closed" and x.get("term") != CA.NO_FISHING_TERM:
                probs.append(f"{where}: {x['says']!r} has no ⓘ for what an accidental hook means")
        h = g["headline"]
        share = next((t for t, _p in walk(g["tree"]) if t["node"] == "tier" and t["tier"] == "share"), None)
        under_share = {id(x) for x, _p in walk(share["children"])} if share is not None else set()
        for k, _p in walk(g["tree"]):
            if k["node"] != "kind":
                continue
            names = TR.kind_names(k["species"], k["name"] == "Steelhead")
            if names not in k["says"]:
                probs.append(f"{where}: kind {k['species']} says {k['says']!r}, not its fish ({names})")
            if k["n"] not in (0, None) and not k["says"].startswith(f"Up to {k['n']} "):
                probs.append(f"{where}: kind {k['species']} says {k['says']!r}, not its number {k['n']}")
            os_ = {tuple(_origins(out, S)) for S in k["species"]}
            if k["n"] != 0 and len(os_) == 1 and len(next(iter(os_))) == 1:
                o = next(iter(os_))[0]
                said = f"{o} " in k["says"] \
                    or (id(k) in under_share and share.get("origin_kind") == o and h.get("origin") == o
                        and f" {o} " in f" {h['says']} ") \
                    or (g["origin"] or "").startswith(f"Keep {o} fish only")
                if not said:
                    probs.append(f"{where}: kind {k['species']} keeps {o} fish only and nothing on the card says it")
    return probs


def _waters():
    return [(w, md) for w, md, _ in FIXTURE_WATERS + GATE_WATERS]


@pytest.mark.parametrize("water,md", _waters())
def test_nothing_is_dropped_on_every_kind_of_water(water, md):
    out, P = _card(water, md)
    assert any(r.get("group") for r in out["rows"]), water
    assert dropped(out, P) == []


@pytest.mark.parametrize("water,md", _waters())
def test_every_sub_limit_says_up_to_never_only_and_no_line_repeats_the_total(water, md):
    """Asks 3-4: no tier, kind or note says "only" (the origin sentence "keep hatchery fish only" is no sub-limit), and no
    line says T128's "Of your N … a day"."""
    out, _P = _card(water, md)
    for r in out["rows"]:
        g = r.get("group")
        if not g:
            continue
        for x, _p in walk(g["tree"]):
            lines = [x["says"]] + ([x["sentence"]] + [a["says"] for a in x["also_counts"]] if x["node"] == "kind" else [])
            for s in lines:
                assert not re.search(r"\bonly\b", s), s
                assert "Of your " not in s and "Of these, " not in s and "Of the " not in s, s


def _g(water, md=OCT10):
    out, P = _card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and set(r["group"]["kinds"]) & {"RB", "EB", "GB", "CT"}]
    return g, out, P


def _kinds(g):
    return [x["says"] for x, _p in walk(g["tree"]) if x["node"] == "kind"]


# 1 -- the headline ----------------------------------------------------------------------------------------------------

def test_1_a_hatchery_only_stream_folds_hatchery_into_the_headline_from_the_rules():
    g, _o, _P = _g(F.STREAM)
    h = g["headline"]
    assert (h["says"], h["origin"], h["fish"]) == ("2 hatchery trout/char a day from streams", "hatchery", "trout/char")
    share = g["tree"][0]["children"][0]
    assert (share["tier"], share["origin_kind"]) == ("share", "hatchery")
    # ask 4: the kinds under the share do not say "hatchery" again (a steelhead keeps its own line)
    assert _kinds(g) == ["Up to 2 brook, brown or cutthroat trout combined",
                         "Up to 1 Dolly Varden/bull trout or lake trout combined",
                         "Up to 2 hatchery steelhead — wild steelhead go back",
                         "Up to 2 rainbow trout"]


def test_1_a_wild_allowed_stream_says_no_origin():
    g, _o, _P = _g("dean", JUL15)
    assert (g["headline"]["says"], g["headline"]["origin"]) == ("1 trout/char a day from streams", None)
    assert all("hatchery" not in k and "wild" not in k for k in _kinds(g))


def test_1_the_info_explains_the_regional_total_with_the_cards_numbers():
    g, _o, P = _g(F.STREAM)
    info = g["headline"]["info"]
    assert info["title"] == "Your 2 from streams and the 4 across Region 2"
    assert info["says"] == ("The 2 from streams are part of your 4 trout and char a day across Region 2, lakes included. "
                            "Every one you keep that day, at any stream or lake, counts toward the 4; no more than 2 of "
                            "them may come from streams.")
    assert info["by"] == F.SHARE
    assert [(w["q"], w["a"]) for w in info["examples"]] == [
        ("Kept 2 on another stream?", "None more from streams today — up to 2 more from lakes."),
        ("Kept 1 at a lake?", "Up to 2 more here, or up to 3 more at lakes."),
        ("Kept 4 at lakes?", "That’s your 4 for the day: none more here or on any other water.")]
    assert CC.check_group(P, g) == []


def test_1_no_info_where_the_total_is_a_waters_own():
    for w in (F.BELLA, F.COUNT, F.LAKE):
        g, _o, _P = _g(w)
        assert "info" not in g["headline"], w


def test_1_a_lift_keeps_the_info_words_but_no_arithmetic_examples():
    g, _o, P = _g(F.VEDDER)
    assert g["headline"]["info"]["examples"] == []
    assert CC.check_group(P, g) == []


# 2 -- the steelhead line ----------------------------------------------------------------------------------------------

def test_2_the_steelhead_line_is_short_and_its_info_says_the_accidental_hook():
    g, _o, _P = _g(F.FRASER)
    (x,) = g["not_kept"]
    assert (x["says"], x["status"], x["term"]) == ("Steelhead: closed, no catch and release", "closed", "no_fishing_for")
    import inspect
    from pipeline.deliver.answers import glossary
    src = inspect.getsource(glossary.t_no_fishing_for)                      # the ⓘ's words (p.4 legend, plain)
    assert f'"{CA.NO_FISHING_TERM}"' in src and "by " in src and "accident, release it at once." in src


def test_2_a_release_line_needs_no_info():
    g, _o, _P = _g(F.VEDDER_MAY, MAY15)
    assert [("term" in x, x["says"]) for x in g["not_kept"]] == [
        (False, "Rainbow trout and cutthroat trout: release every one")]


# 3 / 4 -- the sub-limits ----------------------------------------------------------------------------------------------

def test_3_4_size_and_count_tiers_and_notes_say_up_to():
    g, _o, _P = _g(F.LAKE)
    size = g["tree"][0]["children"][0]
    assert size["says"] == "Only 1 of all your trout and char may be over 50 cm (20 in) — not 1 per species."
    assert _kinds(g) == ["Up to 4 brook, brown, cutthroat or rainbow trout combined",
                         "Up to 1 Dolly Varden/bull trout or lake trout combined"]
    g, _o, _P = _g(F.COUNT)
    count = next(t for t, _p in walk(g["tree"]) if t["node"] == "tier" and t["tier"] == "count")
    assert count["says"] == "Up to 1 cutthroat trout, rainbow trout or steelhead combined"       # T623 F12, RULINGS S12
    rb = next(k for k, _p in walk(g["tree"]) if k["node"] == "kind" and k["species"] == ["RB"])
    assert (rb["says"], rb["size_rule"]) == ("Up to 1 hatchery rainbow trout", "must be 30–50 cm (12–20 in); "
                                                                               "release any shorter")   # T503
    g, _o, _P = _g(F.NESTED)
    assert [t["says"] for t, _p in walk(g["tree"]) if t["node"] == "tier" and t["tier"] == "size"] == [
        "Only 2 of all your trout and char may be over 30 cm (12 in) — not 2 per species; up to 1 of them over 50 cm (20 in)."]


# MUTATIONS: remove one rule, `dropped` names it ---------------------------------------------------------------------

def test_mutation_the_headline_without_its_origin(monkeypatch):
    real = CA.headline_of
    monkeypatch.setattr(CA, "headline_of", lambda n, origin, what, place: real(n, None, what, place))
    out, P = _card(F.STREAM, OCT10)
    got = dropped(out, P)
    assert any("keeps hatchery fish only and nothing on the card says it" in p for p in got), got
    assert any("the headline does not say it" in p for p in got), got          # the card gate refuses it too


def test_mutation_a_kind_without_its_origin(monkeypatch):
    real = TR.kind_names
    monkeypatch.setattr(TR, "kind_names", lambda species, steelhead=False, conj=" or ", origin=None:
                        real(species, steelhead, conj))
    out, P = _card(F.VEDDER, OCT10)                  # the river's own hatchery rainbows sit outside the share
    assert any("['RB'] keeps hatchery fish only" in p for p in dropped(out, P))


def test_mutation_the_steelhead_line_removed(monkeypatch):
    monkeypatch.setattr(CA, "_not_kept", lambda *a, **k: [])
    out, P = _card(F.FRASER, OCT10)
    assert any("ST is closed here and the card does not say it" in p for p in dropped(out, P))


def test_mutation_the_steelhead_info_removed(monkeypatch):
    real = CA._not_kept
    monkeypatch.setattr(CA, "_not_kept", lambda *a, **k: [{k_: v for k_, v in x.items() if k_ != "term"}
                                                           for x in real(*a, **k)])
    out, P = _card(F.FRASER, OCT10)
    assert any("no ⓘ for what an accidental hook means" in p for p in dropped(out, P))


def test_mutation_a_kind_line_without_its_number(monkeypatch):
    real = TR._kind_says
    monkeypatch.setattr(TR, "_kind_says", lambda k: real(k).replace(f"Up to {k['n']} ", "Some "))
    out, P = _card(F.LAKE, OCT10)
    assert any("not its number" in p for p in dropped(out, P))


def test_mutation_a_kind_removed_from_the_tree():
    out, P = _card(F.STREAM, OCT10)
    out = copy.deepcopy(out)
    g = F.trout_group(out)
    share = g["tree"][0]["children"][0]
    share["children"] = [c for c in share["children"] if not (c["node"] == "kind" and c["species"] == ["RB"])]
    assert any("the tree holds" in p for p in dropped(out, P))


def test_mutation_a_size_cap_removed_from_the_tree():
    out, P = _card(F.LAKE, OCT10)
    out = copy.deepcopy(out)
    root = F.trout_group(out)["tree"][0]
    (size,) = [c for c in root["children"] if c["node"] == "tier" and c["tier"] == "size"]
    root["children"] = [c for c in root["children"] if c is not size] + size["children"]
    assert any("in no size tier and on no kind" in p for p in dropped(out, P))


def test_mutation_an_info_example_that_overstates():
    out, P = _card(F.STREAM, OCT10)
    out = copy.deepcopy(out)
    w = F.trout_group(out)["headline"]["info"]["examples"][1]
    w["here"], w["a"] = 3, w["a"].replace("Up to 2 more here", "Up to 3 more here")
    assert any("the rules allow 2" in p for p in dropped(out, P))
