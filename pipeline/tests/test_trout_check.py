"""THE PROOF (trout table plan, T810 ticket J; contract 2.10): `answers/trout/check.py` reads every shipped trout table back
with G's grammar and proves it says the same law as today's answers and the keeping engine, every key and segment.

Anchors:
  (a) THE MINI CHAIN (no data: `mini_chain` builds it once per run; every release carries the section since the cutover,
      ticket L): the card gate's stamp carries the proof (`trout_check`), which checked every non-NULL trout cell and
      found nothing; the proof over the decoded answers with their bundle is clean.
  (b) MUTATIONS on that release's tables, one per check (contract 2.10): a wrong number, a dropped fish, a shifted date,
      an unparseable sentence, a size edge, an origin, a closure's scope, a possession line, a shared band's span: each
      fails. The floor: a stamp that checked fewer tables than the file holds, or none, is refused.
  (c) THE RE-AIMED CHECKS (fight list item 9; ticket L), each firing on a planted trout case OF THE CUTOVER FORM: T793's
      origin words (`tree.origin_unsaid`), T805's limit across rows (`tree.across_unsaid`), the tree gate's numbers
      (`card_check.check_tree` / `check_caps`: the engine's baskets), the possession line
      (`card_check.possession_problems` with F0's lead) — and each old check, which walks `group`, sees nothing of the
      same planted case once the covered rows carry no group: J is what stops it.
  (d) THE DECODER IS SEPARATE: `check.py` imports nothing from the builder.
  (e) THE BUILDER FAULTS THE PROOF FOUND (ticket J's full side build): each fix's pin off makes the proof fail its case.
  (f) the live named waters (`needs_bundle`): every segment of every pinned water proved.
"""
from __future__ import annotations

import ast
import copy
import json
from pathlib import Path

import pytest

from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers.trout import check as J
from pipeline.deliver.answers.trout import table as T
from pipeline.deliver.answers.trout import words as W

CHECK = Path(J.__file__)


# ------------------------------------------------------------------------------------------------------ the side release

class _Side:
    """The mini chain (`mini_chain`): its answers decoded (the cutover form), its export and bundle."""

    def __init__(self, deliver: Path):
        self.dir = deliver
        self.bundle = str(self.dir / "bundle.sqlite")
        self.raw = (self.dir / "ui-rules-answers.json").read_bytes()
        self.wire = json.loads(self.raw)
        self.data, _guide = C.load_export(self.dir)
        self.model = E.decode(self.wire, self.data)

    def keys_with_tables(self):
        return [k for k, cells in enumerate(self.model.sections["trout"]) if any(cells)]

    def prove(self, k, cells=None, frames=None) -> list:
        """The proof over key `k`, its cells (and rows frames) replaced."""
        m = copy.copy(self.model)
        m.sections = dict(self.model.sections)
        if cells is not None:
            m.sections["trout"] = list(m.sections["trout"])
            m.sections["trout"][k] = cells
        if frames is not None:
            m.sections["rows"] = list(m.sections["rows"])
            m.sections["rows"][k] = frames
        return J.check_model(m, self.bundle, self.data, keys=[k], log=lambda *_: None)["problems"]


@pytest.fixture(scope="module")
def side(tmp_path_factory):
    from pipeline.tests import mini_chain
    return _Side(mini_chain.deliver(tmp_path_factory))


def _cells(side, k):
    return copy.deepcopy(side.model.sections["trout"][k])


def _first(side, pred):
    """(key, segment) of the first table `pred(cell)` holds for."""
    for k in side.keys_with_tables():
        for j, c in enumerate(side.model.sections["trout"][k]):
            if c is not None and pred(c):
                return k, j
    raise AssertionError("no table of the side release has the planted feature")


def _row(cell, key):
    return next((bi, ri, r) for bi, b in enumerate(cell["table"]["boxes"]) for ri, r in enumerate(b["rows"])
                if r["key"] == key)


# ---------------------------------------------------------------------------------------------- (a) the release is proved

def test_the_card_gate_stamp_carries_the_proof_over_every_trout_cell(side):
    st = json.loads((side.dir / "card-check.json").read_text())
    tc = st["card_check"]["trout_check"]
    cells = sum(1 for cells in side.wire["sections"]["trout"]["at"] for x in cells if x is not None)
    assert tc == {"tables_checked": cells, "cells": cells, "problems": 0}
    assert cells >= 15                                            # not vacuous
    from pipeline.deliver.answers.card_check import stamp_problems
    assert stamp_problems(side.dir / "ui-rules-answers.json", side.raw) == []


def test_the_side_release_is_proved_every_key_and_segment(side):
    got = J.check_model(side.model, side.bundle, side.data, log=lambda *_: None)
    assert got["problems"] == []
    assert got["tables"] == got["cells"] >= 15


def test_every_text_of_the_side_release_reads_back(side):
    """G's grammar reads every text of every table (contract 2.10: none is free text)."""
    n = 0
    for cells in side.model.sections["trout"]:
        for c in cells:
            if c is not None:
                J.Table(c)
                for b in c["table"]["boxes"]:
                    for r in b["rows"]:
                        W.parse_sheet_head(r["sheet"]["head"])
                        for h in r["sheet"]["here"]:
                            W.parse_sheet_line(h["t"])
                n += 1
    assert n >= 15


#: one text of every form the tables of a full side build say (`fixtures/trout_texts.json`: [reader, text]; built from
#: r-46e4355a7277's bundle with `--trout-table`, ticket J), each read by G's grammar
TEXTS = json.loads((Path(__file__).parent / "fixtures" / "trout_texts.json").read_text())
READERS = {"top": W.parse_top_full, "fold": W.parse_fold, "together": W.parse_together, "elsewhere": W.parse_elsewhere,
           "strip": W.parse_strip, "line": W.parse_row_line, "quiet": W.parse_quiet, "head": W.parse_sheet_head,
           "here": W.parse_sheet_line}


@pytest.mark.parametrize("reader,text", TEXTS, ids=[f"{r}:{i}" for i, (r, _t) in enumerate(TEXTS)])
def test_every_form_a_full_build_says_reads_back(reader, text):
    READERS[reader](text)


@pytest.mark.parametrize("reader,text", TEXTS, ids=[f"{r}:{i}" for i, (r, _t) in enumerate(TEXTS)])
def test_a_garbled_text_of_every_form_is_refused(reader, text):
    """Contract 2.10: a text G's grammar cannot read fails the proof, never passes as free text."""
    with pytest.raises(W.WordsError):
        READERS[reader](text.replace(" ", "  ", 1) if " " in text else text + " x")


def test_the_grammar_reads_the_numbers_back():
    assert W.parse_row_line("only 1 over 50 cm (20 in), counting any brook or lake trout you keep") == \
        ("size_count", 1, 50, ("EB", "LT"))
    assert W.parse_row_line("up to 2 over 30 cm (12 in), and up to 1 over 50 cm (20 in)") == \
        ("also", ("size", 2, 30, None, (), ((1, 50),)))
    assert W.parse_row_line("none over 50 cm (20 in) if you’ve already kept a cutthroat trout over 50 cm (20 in)") == \
        ("also", ("size", 1, 50, None, ("CT",), ()))
    assert W.parse_size("60 cm (24 in) or less, or 90 cm (35 in) or longer") == ((0, 60), (90, W.INF))
    assert W.parse_size("any length, 50 cm (20 in) or less") == ((0, 50),)
    assert W.parse_possession("in your possession: no more than 2 lake trout, only 1 over 50 cm (20 in)") == \
        ("count", 2, ("LT",), ((1, ((50, W.INF),)),))
    assert W.parse_possession("in your possession: 1 day’s limit, not 2") == ("days", 1, 2)
    assert W.parse_top_full("Up to 2 {day}, or 4 if they’re hatchery rainbow trout 50 cm (20 in) or less") == \
        ("up_to", 2, ((4, ("RB",), ((0, 50),), "hatchery"),), (), ())
    assert W.parse_top_full("Up to 2 {day}, plus 2 Dolly Varden/bull trout") == ("up_to", 2, (), (), ((2, ("DV",)),))
    assert W.parse_top_full("Up to 6 rainbow trout or steelhead and 4 other trout and char {day}") == \
        ("apart", ((6, ("RB", "ST")),), 4)
    assert W.parse_elsewhere("Kept 2 on another stream? None more here.") == (("kept", 2, "stream"), ("more", 0))
    assert W.parse_sheet_line("Only 1 of these 4: this fish and brook trout, wild rainbow trout and lake trout.") == \
        ("shared", "only 1 of these 4", (("EB", None), ("RB", "wild"), ("LT", None)))


# ---------------------------------------------------------------------------------------------------- (b) the mutations

def _mutated(side, pred, change):
    k, j = _first(side, pred)
    cells = _cells(side, k)
    change(cells[j])
    return side.prove(k, cells)


def test_mutation_a_wrong_number_fails(side):
    def change(c):
        b = next(b for b in c["table"]["boxes"] if b["kind"] == "keep")
        n = W.parse_head(b["head"])[1]
        b["head"] = W.head_keep(n + 1)
        for x in b["bands"]:
            if x["kind"] == "upto":
                x["text"] = W.band_upto(n + 1)
    assert _mutated(side, lambda c: any(b["kind"] == "keep" for b in c["table"]["boxes"]), change)


def test_mutation_a_dropped_fish_fails(side):
    def change(c):
        b = next(b for b in c["table"]["boxes"] if b["kind"] == "keep" and len(b["rows"]) > 1)
        drop = b["rows"][-1]
        b["rows"] = b["rows"][:-1]
        b["bands"] = [x for x in b["bands"] if x["from"] < len(b["rows"])]
        b["bands"][-1]["to"] = min(b["bands"][-1]["to"], len(b["rows"]) - 1)
        if c["dates"]:
            c["dates"]["sheet"].pop(drop["key"], None)
            c["dates"]["row"] = {k: v for k, v in c["dates"]["row"].items() if k != f"0:{len(b['rows'])}"}
    got = _mutated(side, lambda c: any(b["kind"] == "keep" and len(b["rows"]) > 1 and
                                       b["bands"][-1]["from"] < len(b["rows"]) - 1 or
                                       b["kind"] == "keep" and len(b["rows"]) > 1 and b["bands"][-1]["kind"] == "upto"
                                       for b in c["table"]["boxes"]), change)
    assert any("said nowhere in the table" in p for p in got), got


def _shifted_date(side):
    def change(c):
        d = c["dates"]
        lvl = next(x for x in ("card", "top") if d.get(x))
        p = W.parse_date_line(d[lvl])
        shifted = W.until(p.last - 1, p.then) if p.form == "tighter" else W.until_bare(p.last - 1)
        d[lvl] = W.card_line(shifted) if lvl == "card" else shifted
        if lvl == "top":
            d["fold"] = d["top"]
    return _mutated(side, lambda c: c["dates"] and (c["dates"].get("card") or c["dates"].get("top")) and
                    W.parse_date_line(c["dates"].get("card") or c["dates"]["top"]).form in ("tighter", "opens"), change)


DATE_MARK = "today's answer lasts to day"


def test_mutation_a_shifted_date_fails(side):
    got = _shifted_date(side)
    assert any(DATE_MARK in p for p in got), got


def test_mutation_an_unparseable_sentence_fails(side):
    def change(c):
        b = next(b for b in c["table"]["boxes"] if b["kind"] == "keep")
        b["rows"][0]["lines"] = (*b["rows"][0]["lines"], "keep a few of the small ones")
    got = _mutated(side, lambda c: any(b["kind"] == "keep" for b in c["table"]["boxes"]), change)
    assert any("WordsError" in p and "keep a few" in p for p in got), got


def test_mutation_a_size_edge_fails(side):
    def change(c):
        _bi, _ri, r = _row(c, "RB")
        r["lines"] = tuple(W.size_line("40 cm (16 in) or less") if x == "50 cm (20 in) or less" else x
                           for x in r["lines"])
        r["sheet"]["here"] = [{**h, "t": h["t"].replace("50 cm (20 in) or less", "40 cm (16 in) or less")}
                              for h in r["sheet"]["here"]]
    got = _mutated(side, lambda c: any(r["key"] == "RB" and "50 cm (20 in) or less" in r["lines"]
                                       for b in c["table"]["boxes"] for r in b["rows"]), change)
    assert any("RB" in p and "45 cm" in p for p in got), got


def test_mutation_an_origin_fails(side):
    """The "Hatchery only" label dropped: the row reads as the same law for both origins, and wild ones go back."""
    def change(c):
        for b in c["table"]["boxes"]:
            for r in b["rows"]:
                if r["label"]:
                    r["label"] = None
                    r["sheet"]["here"] = [h for h in r["sheet"]["here"] if h["t"] != W.SHEET_HATCHERY_ONLY]
        c["table"]["hatch_link"] = any(b["label"] or any(r["label"] or r["origin"] for r in b["rows"])
                                       for b in c["table"]["boxes"])
    got = _mutated(side, lambda c: any(r["label"] for b in c["table"]["boxes"] for r in b["rows"]), change)
    assert any("different laws" in p or "the engine" in p for p in got), got


def test_mutation_a_scope_fails(side):
    def change(c):
        d = c["dates"]
        lvl = next(x for x in ("card", "top") if d.get(x) and "closed to all fishing" in d[x])
        d[lvl] = d[lvl].replace("closed to all fishing", "closed to trout and char")
        if lvl == "top":
            d["fold"] = d["top"]
        for k, v in d["sheet"].items():
            d["sheet"][k] = tuple(s.replace("the water is closed to all fishing", "it is closed to trout and char")
                                  for s in v)
    got = _mutated(side, lambda c: c["dates"] and any("closed to all fishing" in (c["dates"].get(x) or "")
                                                      for x in ("card", "top")), change)
    assert any("what tightens" in p for p in got), got


def _possession_line(side):
    """A possession line where the fish's possession is the page's lead (F0)."""
    def change(c):
        b = next(b for b in c["table"]["boxes"] if b["kind"] == "keep")
        r = b["rows"][0]
        r["lines"] = (*r["lines"], "in your possession: 1 day’s limit, not 2")
        r["sheet"]["here"] = [*r["sheet"]["here"], {"t": W.possession_sheet(
            "in your possession: 1 day’s limit, not 2" + W._home()), "bold": False}]
    return _mutated(side, lambda c: any(b["kind"] == "keep" and b["rows"][0]["fish"] != "ST"
                                        for b in c["table"]["boxes"]), change)


POSSESSION_MARK = "its possession is the page's"


def test_mutation_a_possession_line_fails(side):
    """The re-aimed possession check fires: the line says a number where the page's lead speaks for the fish."""
    got = _possession_line(side)
    assert any(POSSESSION_MARK in p for p in got), got


def _shared_band(side):
    """Two rows a box keeps up to its number each, planted as one shared band of 1."""
    def change(c):
        b = next(b for b in c["table"]["boxes"] if b["kind"] == "keep" and
                 [x["kind"] for x in b["bands"][:2]] == ["upto", "upto"])
        b["bands"] = [{"from": 0, "to": 1, "text": W.band_shared(1, 2), "kind": "shared"}, *b["bands"][2:]]
        for r in b["rows"][:2]:
            r["sheet"]["head"] = W.sheet_keep(W.band_shared(1, 2), W.parse_head(b["head"])[1])
    return _mutated(side, lambda c: any(b["kind"] == "keep" and [x["kind"] for x in b["bands"][:2]] == ["upto", "upto"]
                                        for b in c["table"]["boxes"]), change)


JOINT_MARK = "the engine accepts it"


def test_mutation_a_shared_band_span_fails(side):
    """The engine's baskets accept two of the band's fish; the table refuses them."""
    got = _shared_band(side)
    assert any(JOINT_MARK in p for p in got), got


def test_mutation_a_table_where_q_d3_says_null_fails(side):
    k, j = _first(side, lambda c: True)
    cells = _cells(side, k)
    frames = copy.deepcopy(side.model.sections["rows"][k])
    for f in frames:
        f["fish"] = {S: v for S, v in f["fish"].items() if S not in C.trout_char()}
    assert any("Q-D3" in p for p in side.prove(k, cells, frames))


@pytest.mark.parametrize("flag,plant,mark", [
    ("A_DATE_PROOF", _shifted_date, DATE_MARK),
    ("A_POSSESSION_PROOF", _possession_line, POSSESSION_MARK),
    ("A_JOINT_PROOF", _shared_band, JOINT_MARK),
], ids=["dates", "possession", "joint"])
def test_each_proof_pin_off_silences_its_check(side, monkeypatch, flag, plant, mark):
    """The pins are wired: with a check off, its planted fault no longer draws its message (another check may still see
    the same fault from its side)."""
    assert any(mark in p for p in plant(side))
    monkeypatch.setattr(J, flag, False)
    assert not any(mark in p for p in plant(side))


# -------------------------------------------------------------------------------------------------------- the floor

def _stamp(side, change):
    import shutil
    from pipeline.deliver.answers.card_check import STAMP_NAME, stamp_problems
    d = Path(side.dir).parent / "floor"
    d.mkdir(exist_ok=True)
    shutil.copy(side.dir / "ui-rules-answers.json", d / "ui-rules-answers.json")
    st = json.loads((side.dir / STAMP_NAME).read_text())
    change(st["card_check"])
    (d / STAMP_NAME).write_text(json.dumps(st))
    return stamp_problems(d / "ui-rules-answers.json", side.raw)


def test_a_stamp_that_checked_fewer_tables_than_the_file_holds_is_refused(side):
    def fewer(st):
        st["trout_check"]["tables_checked"] -= 1
    assert any("the floor" in p for p in _stamp(side, fewer))


def test_a_stamp_with_no_trout_proof_is_refused(side):
    assert any("no trout proof" in p for p in _stamp(side, lambda st: st.pop("trout_check")))


def test_a_stamp_whose_trout_proof_found_problems_is_refused(side):
    def bad(st):
        st["trout_check"]["problems"] = 1
    assert any("trout proof found 1" in p for p in _stamp(side, bad))


def test_the_committed_mini_release_answers_hold_the_section():
    """Ticket L: every release carries the trout section, the committed mini release too, so its stamp needs the proof
    (answers without it are refused at packaging: `test_trout_section`)."""
    from pipeline.deliver.answers.card_check import trout_cells
    from pipeline.tests import under_test
    from pipeline.deliver.store.release import province_store
    st = province_store(under_test.mini())
    try:
        raw = st.answers_bytes()
    finally:
        st.close()
    assert trout_cells(json.loads(raw)) >= 15


# ---------------------------------------------------------------------------------------------- (c) the re-aimed checks

def _cell(side, k, j, frame=None):
    c = side.model.sections["trout"][k][j]
    return J.Cell(c, frame or side.model.sections["rows"][k][j], side.model.sections["display"][k][j],
                  side.model.sections["licence"][k][j], lambda ref: {}, None, False, {}, side.data["rule_ids"]), J.Table(c)


def test_reaimed_t793_an_origin_condition_is_said_on_the_table(side):
    """T793 (`tree.origin_unsaid`) re-aimed: a keep row with an origin condition holding a table fish; the table says an
    origin for it (the label) — and with the label dropped, the check fires."""
    k, j = _first(side, lambda c: any(r["label"] for b in c["table"]["boxes"] for r in b["rows"]))
    frame = copy.deepcopy(side.model.sections["rows"][k][j])
    S = next(r["fish"] for b in side.model.sections["trout"][k][j]["table"]["boxes"] for r in b["rows"] if r["label"])
    row = next(r for r in frame["rows"] if r.get("kind") == "keep" and S in (r.get("members") or ()))
    row["conds"] = [*(row.get("conds") or []), {"c": "origin", "o": "wild", "who": [S], "r": 0}]
    c, Tb = _cell(side, k, j, frame)
    assert J.origin_problems(c, Tb) == []
    # the old check walks the row's group: the covered row has none since the cutover, so it sees nothing here
    from pipeline.deliver.answers.tree import origin_unsaid
    assert row.get("group") is None and origin_unsaid(frame) == []
    cell = copy.deepcopy(side.model.sections["trout"][k][j])
    for b in cell["table"]["boxes"]:
        for r in b["rows"]:
            r["label"] = None
        b["label"] = None
    c2 = J.Cell(cell, frame, c.display, c.licence, c.rule, None, False, {}, c.ids)
    assert any(f"{S} has an origin condition" in p for p in J.origin_problems(c2, J.Table(cell)))


def test_reaimed_t805_a_limit_across_rows_is_said_across_them(side):
    """T805 (`tree.across_rows`) re-aimed: two keep rows holding one origin's limit (a planted "Wild trout/char daily
    quota = 2" on each) must be said once across them (`together`); a table that says it nowhere fails."""
    k, j = _first(side, lambda c: sum(len(b["rows"]) for b in c["table"]["boxes"] if b["kind"] == "keep") >= 2)
    frame = copy.deepcopy(side.model.sections["rows"][k][j])
    keep = [r for r in frame["rows"] if r.get("kind") == "keep" and set(r.get("members") or ()) & C.trout_char()]
    cell = side.model.sections["trout"][k][j]
    fish = [r["fish"] for b in cell["table"]["boxes"] if b["kind"] == "keep" for r in b["rows"]]
    a, b_ = fish[0], fish[1]
    keep[0]["members"] = [S for S in keep[0]["members"] if S != b_]
    frame["rows"].append({**copy.deepcopy(keep[0]), "members": [b_], "all_members": [b_], "conds": []})
    for r, S in ((keep[0], a), (frame["rows"][-1], b_)):
        r["conds"] = [*(r.get("conds") or []), {"c": "origin2", "o": "wild", "daily": 2, "who": [S], "r": 7}]
    c, Tb = _cell(side, k, j, frame)
    assert any("wild limit of 2 across rows" in p for p in J.origin_problems(c, Tb))
    # the old check walks each keep row's group: none on the covered rows since the cutover, so it is silent here
    from pipeline.deliver.answers.tree import across_unsaid
    assert all(r.get("group") is None for r in keep) and across_unsaid(frame) == []
    told = copy.deepcopy(cell)
    told["table"]["together"] = {"text": W.together_origin(2, "wild", None), "src": [told["table"]["src"][0]]}
    told["table"]["top"]["second"] = told["table"]["together"]["text"]
    c2 = J.Cell(told, frame, c.display, c.licence, c.rule, None, False, {}, c.ids)
    assert not any("across rows" in p for p in J.origin_problems(c2, J.Table(told)))


def test_reaimed_tree_gate_a_number_the_engine_does_not_prove_fails(side):
    """The tree gate's numbers (`card_check.check_tree` / `check_caps`) re-aimed: a row's own limit planted below the
    engine's ("only 1" on a fish the box keeps up to its number) fails the alone count and the baskets."""
    def change(c):
        b = next(b for b in c["table"]["boxes"] if b["kind"] == "keep" and b["bands"][0]["kind"] == "upto")
        b["bands"][0] = {**b["bands"][0], "text": W.band_own(1), "kind": "own"}
        b["rows"][0]["sheet"]["head"] = W.sheet_keep(W.band_own(1), W.parse_head(b["head"])[1])
        b["rows"][0]["sheet"]["here"] = [*b["rows"][0]["sheet"]["here"], {"t": W.sheet_own(1), "bold": False}]
    pred = lambda c: any(b["kind"] == "keep" and b["bands"][0]["kind"] == "upto" and  # noqa: E731
                         W.parse_head(b["head"])[1] > 1 for b in c["table"]["boxes"])
    got = _mutated(side, pred, change)
    assert any("alone, the engine" in p for p in got), got
    # the old tree gate (`card_check.check_model` -> `check_group` -> `check_tree` / `check_caps`) proves `group` trees:
    # the covered trout rows carry none since the cutover, so on this key it has no trout card left to prove
    k, j = _first(side, pred)
    covered = side.model.sections["trout"][k][j]["covers"]
    assert covered and all(side.model.sections["rows"][k][j]["rows"][i].get("group") is None for i in covered)


def test_reaimed_possession_the_old_gate_sees_no_trout_kind_and_j_fires(side):
    """`card_check.possession_problems` ran on each trout kind of a group tree (`check_group`, T805 B); since the cutover
    no covered row has a group, so it is reached only through J (`check.possession_problems`, F0's lead): a planted
    possession line where the page's lead speaks for the fish fails J."""
    for k, cells in enumerate(side.model.sections["trout"]):
        for j, c in enumerate(cells):
            if c is not None:
                rows = side.model.sections["rows"][k][j]["rows"]
                assert not any(rows[i].get("group") or rows[i].get("elsewhere") for i in c["covers"]), (k, j)
    assert any(POSSESSION_MARK in p for p in _possession_line(side))


def test_reaimed_possession_problems_takes_f0s_lead():
    """`card_check.possession_problems` re-aimed with `lead` (F0's `card.page_possession`): a fish whose own multiplier the
    lead covers says no line; one the lead does not cover says its number and the lead's ("1 day’s limit, not 2")."""
    import types
    from pipeline.deliver.answers.card_check import possession_problems as PP

    def rule(key, k, per_daily, species):
        return types.SimpleNamespace(key=key, k=k, f={"per_daily": per_daily, "species": species}, rule_id=key,
                                     entry_id="e")
    page = rule("e::page", "possession", 2, ["ALL_GAME_FISH"])
    lake = rule("e::lt", "possession", 1, ["LT"])
    P = types.SimpleNamespace(cands=[page, lake], verdict=lambda S, o: {"e::page": ["speaks"], "e::lt": ["speaks"]})
    line = lambda w: [f"in your possession: {w} (fish at home don’t count)"]
    assert PP(P, {"species": ["LT"], "n": 3, "duties": line("1 day’s limit, not 2")}, "LT", lead=page) == []
    assert PP(P, {"species": ["LT"], "n": 3, "duties": []}, "LT", lead=page)            # fires: its own line missing
    assert PP(P, {"species": ["LT"], "n": 3, "duties": line("1 day’s limit, not 3")}, "LT", lead=page)
    # the lead is the lake trout's own multiplier: it says no line (F0: the page's lead speaks for it)
    P2 = types.SimpleNamespace(cands=[lake], verdict=lambda S, o: {"e::lt": ["speaks"]})
    assert PP(P2, {"species": ["LT"], "n": 3, "duties": []}, "LT", lead=lake) == []
    assert PP(P2, {"species": ["LT"], "n": 3, "duties": line("1 day’s limit")}, "LT", lead=lake)


# ------------------------------------------------------------------------------------------- (d) a separate decoder

def test_the_proof_imports_nothing_from_the_builder():
    tree = ast.parse(CHECK.read_text())
    names = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom):
            names.add(n.module or "")
            names |= {f"{n.module}.{a.name}" for a in n.names}
        elif isinstance(n, ast.Import):
            names |= {a.name for a in n.names}
    builder = ("pipeline.deliver.answers.trout.table", "pipeline.deliver.answers.trout.sheets",
               "pipeline.deliver.answers.trout.timeline")
    assert not [x for x in names if x.startswith(builder)], names


# ------------------------------------------------------------------------------- (e) the builder faults the proof found

def _recomposed(side, monkeypatch, flag, tmp_path_factory):
    """Every side key's tables composed again with one of F's fixes turned off, from the rows the builder read (before
    the rows 34 cut: `mini_chain.trout_inputs`); the proof over them."""
    from pipeline.tests import mini_chain
    m = mini_chain.trout_inputs(tmp_path_factory)
    _data, _guide, rules = mini_chain.export(tmp_path_factory)
    monkeypatch.setattr(T, flag, False)
    probs = []
    for k in side.keys_with_tables():
        got = T.compose_key(m.sections["rows"][k], m.sections["display"][k], m.segments[k], m.moments[k], rules,
                            licences=m.sections["licence"][k])
        cells = [None if f is None else json.loads(json.dumps(f, ensure_ascii=False)) for f in got]
        probs += side.prove(k, cells)
    return probs


@pytest.mark.parametrize("flag", ["A_KIND_SIZE_CAP_IS_SHARED", "A_RAINBOW_STOPS_AT_THE_STEELHEAD_SIZE"])
def test_each_fix_the_proof_found_turned_off_fails_the_side_release(side, monkeypatch, flag, tmp_path_factory):
    assert _recomposed(side, monkeypatch, flag, tmp_path_factory)


# ---------------------------------------------------------------------------------------- (f) the live named waters

@pytest.mark.needs_bundle
def test_every_segment_of_the_named_waters_is_proved(request):
    from pipeline.tests.test_trout_table import CASES, _Live
    from pipeline.tests import under_test
    BUNDLE = under_test.path("bundle")
    live = _Live(request)
    m = live.model
    keys = sorted({m.parts[item][part] for item, part, _md in CASES.values()})
    m.sections["trout"] = [[None] * len(s) for s in m.segments]
    for k in keys:
        got = T.compose_key(m.sections["rows"][k], m.sections["display"][k], m.segments[k], m.moments[k], live.rules,
                            licences=m.sections["licence"][k])
        m.sections["trout"][k] = [None if f is None else json.loads(json.dumps(f, ensure_ascii=False)) for f in got]
    data, _g = C.load_export(under_test.path("export_dir"))
    got = J.check_model(m, str(BUNDLE), data, keys=keys, log=lambda *_: None)
    assert got["tables"] == got["cells"] > 50
    assert got["problems"] == [], got["problems"][:10]


#: each fix the proof's full side build found in F's builder -> (a water it shows on, its part, what the proof says with
#: the fix off). The book: Sheldens Creek, a stream that is not a steelhead water (a steelhead kept under 50 cm uses no
#: "1 over 50 cm"); Inkaneep Creek (Region 8: "2 from streams" inside the 5; the brook trout's own 20 counted apart);
#: Humphrys Creek (Kitimat: wild rainbow trout 1 a day, its own row); Shamrock Creek (the kind's 2 held together);
#: Fraser River in Region 3 (one "1 over 50 cm" over every trout); the Kitimat River (Region 6's "1 trout from
#: streams Jul 1-Oct 31" holds brown, cutthroat and WILD rainbow trout together: its hatchery rainbow quota lifts hatchery
#: fish only).
#: `A_SHARE_HOLDS_ITS_UNLIFTED_FISH` is no longer pinned here: its one case was Kaslo River / Keen Creek, whose "Bull trout
#: daily quota = 2" lifted nothing, and T834 (RULINGS T17) now lifts it (`bundle.rules.lifts._share_lifts`). Measured on
#: the T834 side build over the live atlas and reach: the flag off leaves every answer byte-identical (5,950 tables, 0
#: problems), so no live water shows the fault; the flag stays in the builder for a row the rule does not lift.
FOUND = {
    "A_RAINBOW_STOPS_AT_THE_STEELHEAD_SIZE": ("blk:360846083", 0, "ST hatchery at"),
    "A_APART_SAYS_THE_BOX_MOST": ("blk:356370187", 0, "the top line says"),
    "A_WILD_QUOTA_FROM_EVERY_ROW": ("gnis:2046", 0, "RB wild at"),
    "A_KIND_NUMBER_IS_SHARED": ("blk:356196726", 0, "the engine refuses it"),
    "A_KIND_SIZE_CAP_IS_SHARED": ("gnis:39325", 19, "the engine refuses it"),
    "A_WILD_QUOTA_JOINS_ITS_COUNT": ("gnis:3225", 0, "the engine refuses it"),
}


@pytest.fixture(scope="module")
def found(request):
    from pipeline.deliver.answers import answers as A
    from pipeline.deliver.export.codec import expand
    from pipeline.tests.conftest import BUNDLE_HINT, EXPORT_HINT, need
    from pipeline.tests import under_test
    BUNDLE, EXPORT_DIR = under_test.path("bundle"), under_test.path("export_dir")
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    rules = T.Rules.from_export(data, expand(data, guide))
    # ONE MODEL PER WATER (T834): the proof's random baskets follow the key's position in the model, so a model shared by
    # every pin made each pin depend on which other waters were built (dropping Kaslo from FOUND hid the Kitimat's fault)
    models: dict = {}

    def of(item):
        if item not in models:
            models[item] = A.build(str(BUNDLE), EXPORT_DIR, workers=1, items=[item], sections=["rows", "display"],
                                   export=(data, guide), log=lambda *_: None)
        return models[item], data, rules, str(BUNDLE)
    return of


def _found_problems(found, item, part) -> list:
    model, data, rules, bundle = found(item)
    m = copy.copy(model)
    m.sections = dict(model.sections)
    k = m.parts[item][part]
    m.sections["trout"] = [[None] * len(s) for s in m.segments]
    got = T.compose_key(m.sections["rows"][k], m.sections["display"][k], m.segments[k], m.moments[k], rules,
                        licences=m.sections["licence"][k])
    m.sections["trout"][k] = [None if f is None else json.loads(json.dumps(f, ensure_ascii=False)) for f in got]
    return J.check_model(m, bundle, data, keys=[k], log=lambda *_: None)["problems"]


@pytest.mark.needs_bundle
@pytest.mark.parametrize("flag", sorted(FOUND))
def test_each_fault_the_full_side_build_found_is_fixed_and_pinned(found, monkeypatch, flag):
    item, part, mark = FOUND[flag]
    assert not any(mark in p for p in _found_problems(found, item, part))
    monkeypatch.setattr(T, flag, False)
    if flag == "A_APART_SAYS_THE_BOX_MOST":
        # T810 G2 Q1 also brings a box's number down to what it holds here, which hides this fault: pin it alone
        monkeypatch.setattr(T, "A_HEAD_SAYS_WHAT_IS_KEPT_HERE", False)
    assert any(mark in p for p in _found_problems(found, item, part))
