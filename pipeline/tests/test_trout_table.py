"""THE TROUT AND CHAR TABLE BUILDER (trout table plan, T810 ticket F): `answers/trout/table.py` builds each (answer key,
segment)'s `model.trout.TroutFrame`, every word from G (`trout/words.py`), every date from E (`trout/timeline.py`).

Anchors: (a) EVERY KEY of the mini chain (no data, `mini_chain`: the rows as the builder read them, before the rows 34
cutover; the property that every trout and char fish of a frame
is said exactly once, NULL exactly per Q-D3, and every table valid under the model); (b) frames planted on a mini-release
frame (a mixed trout + kokanee closed row split, a weekday split, an hours window, the model's refusals); (c) the plan's
named waters on the live bundle (`needs_bundle`), each table whole; (d) the whole built release (`needs_bundle`);
(e) MUTATIONS: each rule's flag turned off makes its named case fail.

The book (synopsis 2025-2027, printed pages) for the named cases: Skeena River Sedan Cr to Mill Cr (Region 6: 5 trout and
char, 1 over 50 cm, 3 lake trout, 1 of brown / cutthroat / rainbow / steelhead, hatchery steelhead only, stop after the
steelhead quota); Lois Lake p.24 and Khartoum Lake p.23 ("Wild trout/char daily quota = 2 (no wild trout 40 cm or
more), hatchery rainbow trout = 6"); Windsor Lake p.26; Vedder River p.22 (hatchery only from streams, 4 hatchery
rainbow 50 cm or less); Kamloops Lake (Region 3: the lake's "3" is the sum of 2 rainbow and 1 char, never drawn);
Adams River upstream of Adams Lake (80 cm Dolly Varden; one day, Jul 31, at 60 cm: owner "80 cm on the row");
Cunningham Lake p.59 (lake trout possession 2, only one over 50 cm); Kootenay Lake main body (two folds: Region 4's 5
and the lake's 10 rainbow); Elk River mouth to Elko Dam (release in winter); Fraser River in Region 3 (closed to trout
and char in October); Bish Creek (steelhead closed in May); Burnt River (one release row mixing trout and char with
kokanee, whitefish, burbot).
"""
from __future__ import annotations

import copy
import json
from collections import Counter

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers.model.trout import TroutFrame
from pipeline.deliver.answers.trout import table as T
from pipeline.deliver.answers.trout import words as W
from pipeline.tests import under_test

TC = C.trout_char()


# ------------------------------------------------------------------------------------------------ the whole-object view

def render(fr) -> str:
    """One table, every text it says, in payload order (a test's whole-object view)."""
    if fr is None:
        return "NULL"
    t, d = fr["table"], fr["dates"] or {}
    out = [f"top: {t['top']['lead']}" + (f" // {t['top']['second']}" if t["top"]["second"] else ""),
           f"fold: {t['fold']['value']} ({t['fold']['tone']})"]
    if t["top"].get("sub"):
        out.insert(1, f"sub: {t['top']['sub']}")
    if d.get("card"):
        out.append(f"card: {d['card']}")
    if d.get("top"):
        out.append(f"under top: {d['top']}")
    for bi, b in enumerate(t["boxes"]):
        head = " · ".join(x for x in (b["head"], b["label"] and b["label"].upper(), b["strip"], b.get("possession"),
                                      b["tag"]) if x)
        out.append(f"[{head}]" + (f"  ({d['box'][str(bi)]})" if str(bi) in d.get("box", {}) else ""))
        for ri, r in enumerate(b["rows"]):
            band = next((f"{x['text']}" + (f" ({d['band'][f'{bi}:{k}']})" if f"{bi}:{k}" in d.get("band", {}) else "")
                         for k, x in enumerate(b["bands"]) if x["from"] <= ri <= x["to"]), "")
            band += "".join(f" [{x['text']}]" for x in b.get("inner") or () if x["from"] <= ri <= x["to"])
            name = r["name"] + (f" {r['origin']}" if r["origin"] else "") + (f" [{r['label'].upper()}]" if r["label"] else "")
            bits = ([r["sub"]] if r["sub"] else []) + list(r["lines"]) + [f"~{q}" for q in r["quiet"]]
            when = d.get("row", {}).get(f"{bi}:{ri}")
            out.append(f"  {name}: {' | '.join(bits)}" + (f"  <{band}>" if band else "") + (f"  ({when})" if when else ""))
    if t["together"]:
        out.append(f"together: {t['together']['text']}")
    if t["year"]:
        out.append(f"year: {t['year']['title']}")
    return "\n".join(out)


def said(fr) -> Counter:
    """How many boxes say each fish (twin rows of one fish are one saying)."""
    got = Counter()
    for b in fr["table"]["boxes"]:
        for S in dict.fromkeys(r["fish"] for r in b["rows"]):
            got[S] += 1
    return got


def check_key(rows, displays, frames) -> list:
    """The properties every key holds: NULL exactly per Q-D3; each trout and char fish the frame answers said exactly
    once, by one box; `covers` exactly the split frame's trout rows, none mixed; the model holds."""
    problems = []
    for j, (f, d, fr) in enumerate(zip(rows, displays, frames)):
        asked = T.fish_states(f)
        null = d.get("status") in ("closed", "tidal") or not asked
        if (fr is None) != null:
            problems.append(f"segment {j}: NULL is {fr is None}, Q-D3 says {null}")
            continue
        if fr is None:
            continue
        TroutFrame.model_validate(fr)
        got = said(fr)
        if set(got) != set(asked) or any(n != 1 for n in got.values()):
            problems.append(f"segment {j}: fish said {dict(got)}, asked {sorted(asked)}")
        split, covers = T.split_mixed(f)
        if list(fr["covers"]) != covers:
            problems.append(f"segment {j}: covers {fr['covers']} vs the split frame's trout rows {covers}")
        for i in fr["covers"]:
            fish = set(split["rows"][i].get("all_members") or ()) | set(split["rows"][i].get("members") or ())
            if not fish or not fish <= TC:
                problems.append(f"segment {j}: covered row {i} holds {sorted(fish)}")
        held = {S for i in fr["covers"] for S in (split["rows"][i].get("all_members") or ())}
        rest = {S for i, r in enumerate(split["rows"]) if i not in fr["covers"] for S in r.get("all_members") or ()}
        if held & rest or rest & TC:
            problems.append(f"segment {j}: a fish both covered and not: {sorted(held & rest)} / {sorted(rest & TC)}")
    return problems


# ------------------------------------------------------------------------------- (a) every key of the mini release

class _Mini:
    """Every key of the mini chain (`mini_chain`, the committed mini release's own stages), its rows frames AS THE
    BUILDER READ THEM: since the cutover (rows 34) a shipped frame's covered rows carry no `group`, which the builder
    reads, so the answers are built again in this process with the cutover pass off (`mini_chain.trout_inputs`)."""

    def __init__(self, tmp_path_factory):
        from pipeline.tests import mini_chain
        m = mini_chain.trout_inputs(tmp_path_factory)
        _data, _guide, self.rules = mini_chain.export(tmp_path_factory)
        self.model = m
        self.keys = []
        #: per key, its post-pass licence frames (the fish sheets' stamps, ticket H)
        self.lics = {}
        for k, starts in enumerate(m.segments):
            self.lics[k] = m.sections["licence"][k]
            self.keys.append((k, starts, m.moments[k], m.sections["rows"][k], m.sections["display"][k]))


@pytest.fixture(scope="module")
def mini(tmp_path_factory):
    return _Mini(tmp_path_factory)


def test_every_key_of_the_mini_release_holds_the_properties(mini):
    n = Counter()
    for k, starts, moms, fs, ds in mini.keys:
        got = T.compose_key(fs, ds, starts, moms, mini.rules, licences=mini.lics[k])
        assert check_key(fs, ds, got) == [], k
        n["tables"] += sum(1 for x in got if x is not None)
        n["null"] += sum(1 for x in got if x is None)
    assert n["tables"] >= 15 and n["null"] >= 5, n                # not vacuous: both kinds of cell


def _keep_key(mini):
    """A mini-release key whose first segment holds a keep box (the planting ground)."""
    for k, starts, moms, fs, ds in mini.keys:
        got = T.compose_key(fs, ds, starts, moms, mini.rules, licences=mini.lics[k])
        for j, x in enumerate(got):
            if x is not None and x["table"]["boxes"][0]["kind"] == "keep":
                return k, starts, moms, fs, ds, j, got
    pytest.fail("the mini release holds no keep table")


# ------------------------------------------------------------------------------------------ (b) planted frames

def test_a_mixed_trout_and_kokanee_closed_row_is_split(mini):
    """THE A0 AMENDMENT: a closed row holding trout and kokanee becomes its trout part (covered) and a remainder row
    (kokanee, not covered); every fish of the original row is said exactly once (the trout by the table, the kokanee by
    the remainder row); a row that held the split row by index (`parent`) follows it."""
    k, starts, moms, fs, ds, j, got = _keep_key(mini)
    f = copy.deepcopy(fs[j])
    keep_fish = [S for S in T.fish_states(f) if T.fish_states(f)[S] == "keep"]
    S = keep_fish[0]
    closed = {"hatchery": {**f["fish"][S]["hatchery"], "status": "closed", "daily": None, "lines": [], "narrow": None},
              "wild": None, "lead": "hatchery"}
    f["fish"] = {**{x: v for x, v in f["fish"].items()}, "LT" if S != "LT" else "DV": closed,
                 "KO": {"hatchery": closed["hatchery"], "wild": None, "lead": "hatchery"}}
    other = "LT" if S != "LT" else "DV"
    mixed = {"kind": "closed", "pool": None, "win": closed["hatchery"]["win"], "members": [other, "KO"],
             "all_members": [other, "KO"], "daily": None, "narrow": None, "everyone": [], "groups": [], "prot": None,
             "wins": None, "lift_notes": [], "scope": None, "year": [["closed", 365]]}
    f["rows"] = [mixed] + list(f["rows"])
    for r in f["rows"][1:]:
        if r.get("parent") is not None:
            r["parent"] += 1
    split, covers = T.split_mixed(f)
    assert [r["all_members"] for r in split["rows"][:2]] == [[other], ["KO"]]
    assert 0 in covers and 1 not in covers
    assert all(split["rows"][i]["parent"] == fs[j]["rows"][i - 2]["parent"] + 2
               for i in range(2, len(split["rows"])) if split["rows"][i].get("parent") is not None)
    fr = T.compose_key([f], [ds[j]], [1], None, mini.rules, licences=[mini.lics[k][j]])[0]
    assert check_key([f], [ds[j]], [fr]) == []
    closed_box = [b for b in fr["table"]["boxes"] if b["kind"] == "closed"]
    assert [r["key"] for r in closed_box[0]["rows"]] == [other]
    assert said(fr)[other] == 1 and "KO" not in said(fr)


def test_the_weekday_words_reach_the_table_from_the_post_pass_rows(mini):
    """Contract 2.2: the weekday lines are composed from the post-pass `rows[].week` (`say_weekdays`), so the table says
    what the page says."""
    k, starts, moms, fs, ds, j, got = _keep_key(mini)
    f = copy.deepcopy(fs[j])
    box = got[j]["table"]["boxes"][0]
    S = box["rows"][0]["fish"]
    home = next(i for i, r in enumerate(f["rows"]) if S in (r.get("all_members") or ()))
    says = "Keep up to 2 on Saturdays and Sundays; release Monday to Friday"
    f["rows"][home]["week"] = {"species": [S], "says": says, "status": None, "parts": []}
    fr = T.compose_key([f], [ds[j]], [1], None, mini.rules, licences=[mini.lics[k][j]])[0]
    row = next(r for b in fr["table"]["boxes"] for r in b["rows"] if r["fish"] == S)
    assert says in row["lines"]


def test_an_hours_window_that_changes_a_trout_answer_stops_the_build(mini):
    """Contract 2.2 / A0 (3): the table is drawn from outside any hours window; a trout answer that differs inside one,
    the water open on both sides, has no words in the table: the build stops (A0 counted none)."""
    k, starts, moms, fs, ds, j, got = _keep_key(mini)
    f = fs[j]
    g = copy.deepcopy(f)
    S = next(iter(T.fish_states(f)))
    g["fish"][S] = {"hatchery": {**(f["fish"][S]["hatchery"] or f["fish"][S]["wild"]), "status": "release",
                                 "daily": None, "lines": []}, "wild": None, "lead": "hatchery"}
    every = list(CAL.WEEKDAYS)
    hours = {"start": {"solar": "sunset", "offset_min": 60}, "end": {"solar": "sunrise", "offset_min": -60}}
    moments = [{"weekdays": every, "hours": {**hours, "in": False}}, {"weekdays": every, "hours": {**hours, "in": True}}]
    with pytest.raises(T.TableError, match="hours window"):
        T.compose_key([f, g], [ds[j], ds[j]], [1, 1], moments, mini.rules, licences=[mini.lics[k][j]] * 2)
    assert T.compose_key([f, copy.deepcopy(f)], [ds[j], ds[j]], [1, 1], moments, mini.rules,
                         licences=[mini.lics[k][j]] * 2)[0] is not None


def _hand(fr, how):
    """A valid frame broken one way (contract 2.8's `trout-hand.json` cases, which the app's reader refuses too)."""
    x = copy.deepcopy(fr)
    t = x["table"]
    b = t["boxes"][0]
    if how == "band gap":
        b["bands"] = b["bands"][1:]
    elif how == "band overlap":
        b["bands"] = (b["bands"][0],) + tuple(b["bands"])
    elif how == "a dangling rule index":
        b["rows"][0]["src"] = tuple(b["rows"][0]["src"]) + (999999,)
    elif how == "a date key to a missing box":
        x["dates"] = {"top": None, "card": None, "fold": None, "box": {"9": "until Oct 31, then release"},
                      "band": {}, "row": {}, "sheet": {}}
    elif how == "a row with dates at two levels":
        x["dates"] = {"top": None, "card": None, "fold": None, "box": {"0": "until Oct 31, then release"},
                      "band": {}, "row": {"0:0": "until Oct 31, then release"}, "sheet": {}}
    elif how == "a key used twice":
        b["rows"] = tuple(b["rows"]) + (b["rows"][0],)
        b["bands"] = tuple(b["bands"]) + ({**b["bands"][-1], "from": len(b["rows"]) - 1, "to": len(b["rows"]) - 1},)
    elif how == "a label on a box and its rows":
        b["label"] = W.HATCHERY_ONLY
        b["rows"][0]["label"] = W.HATCHERY_ONLY
    elif how == "{day} twice":
        t["top"]["lead"] = t["top"]["lead"] + " " + W.DAY_SLOT
    elif how == "{day} outside the top line":
        b["head"] = b["head"] + " " + W.DAY_SLOT
    elif how == "covers out of order":
        x["covers"] = tuple(reversed(x["covers"])) + (x["covers"][0],)
    elif how == "a top date on a keep table":
        x["dates"] = {"top": "until Oct 31", "card": None, "fold": "until Oct 31", "box": {}, "band": {}, "row": {},
                      "sheet": {}}
    elif how == "a year bar on a keep table":
        t["year"] = {"title": W.YEAR_TITLE, "runs": (("keep", 365),)}
    elif how == "a reopening date in a keep box":
        x["dates"] = {"top": None, "card": None, "fold": None, "box": {"0": "reopens Nov 1"}, "band": {}, "row": {},
                      "sheet": {}}
    return x


HAND = ["band gap", "band overlap", "a dangling rule index", "a date key to a missing box",
        "a row with dates at two levels", "a key used twice", "a label on a box and its rows", "{day} twice",
        "{day} outside the top line", "covers out of order", "a top date on a keep table", "a year bar on a keep table",
        "a reopening date in a keep box"]


@pytest.mark.parametrize("how", HAND)
def test_the_model_refuses_each_hand_case(mini, how):
    k, starts, moms, fs, ds, j, got = _keep_key(mini)
    TroutFrame.model_validate(got[j])
    bad = _hand(got[j], how)
    if how == "a dangling rule index":
        with pytest.raises(T.TableError):
            for b in bad["table"]["boxes"]:
                for r in b["rows"]:
                    for ref in r["src"]:
                        mini.rules(ref)
        return
    with pytest.raises(Exception):
        TroutFrame.model_validate(bad)


def test_every_table_rule_index_resolves_over_the_mini_release(mini):
    for k, starts, moms, fs, ds in mini.keys:
        for fr in T.compose_key(fs, ds, starts, moms, mini.rules, licences=mini.lics[k]):
            if fr is None:
                continue
            for ref in fr["table"]["src"]:
                mini.rules(ref)


# --------------------------------------------------------------------------------------- (c) the named waters, live

BUNDLE = under_test.path("bundle")
EXPORT_DIR = under_test.path("export_dir")

#: name -> (item, part, month-day)
CASES = {
    "skeena-sedan-mill-jul15": ("gnis:2936", 13, (7, 15)),
    "lois-jul15": ("wbk:329197058", 0, (7, 15)),
    "khartoum-jul15": ("wbk:329197063", 0, (7, 15)),
    "windsor-jul15": ("wbk:329197074", 0, (7, 15)),
    "vedder-jul15": ("gnis:3062", 0, (7, 15)),
    "kamloops-lake-jul15": ("wbk:329563838", 0, (7, 15)),
    "adams-jul15": ("gnis:39257", 3, (7, 15)),
    "adams-jul31": ("gnis:39257", 3, (7, 31)),
    "kootenay-main-nov15": ("wbk:-20", 0, (11, 15)),
    "cunningham-jul15": ("wbk:329537190", 0, (7, 15)),
    "takla-jul15": ("wbk:328961757", 0, (7, 15)),
    "tagish-jul15": ("wbk:328961735", 0, (7, 15)),
    "bowron-jul15": ("wbk:329058703", 0, (7, 15)),
    "istsi-jul15": ("gnis:10793", 0, (7, 15)),
    "nation-lakes-jul15": ("wbk:329424457", 0, (7, 15)),
    "shamrock-jul15": ("blk:356196726", 0, (7, 15)),
    "echoes-jul15": ("wbk:329524034", 0, (7, 15)),
    "elk-nov15": ("gnis:16880", 0, (11, 15)),
    "elk-apr15": ("gnis:16880", 0, (4, 15)),
    "elk-jul15": ("gnis:16880", 0, (7, 15)),
    "fraser-r2-mission-jul15": ("gnis:39325", 1, (7, 15)),
    "fraser-r3-sep15": ("gnis:39325", 19, (9, 15)),
    "fraser-r3-oct15": ("gnis:39325", 19, (10, 15)),
    "fraser-r3-nov15": ("gnis:39325", 19, (11, 15)),
    "bish-may20": ("gnis:3480", 0, (5, 20)),
    "burnt-jan15": ("gnis:10657", 0, (1, 15)),
}
ITEMS = sorted({c[0] for c in CASES.values()})


class _Live:
    def __init__(self, request):
        from pipeline.deliver.answers import answers as A
        from pipeline.deliver.export.codec import expand
        from pipeline.tests.conftest import BUNDLE_HINT, EXPORT_HINT, need
        need(request, "bundle", BUNDLE, BUNDLE_HINT)
        need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
        data, guide = C.load_export(EXPORT_DIR)
        missing = [w for w in ITEMS if w not in data["waters"]]
        if missing:
            pytest.fail(f"pinned waters missing from this export: {missing}")
        self.model = A.build(str(BUNDLE), EXPORT_DIR, workers=1, items=ITEMS, sections=["rows", "display"],
                             export=(data, guide), log=lambda *_: None)
        self.rules = T.Rules.from_export(data, expand(data, guide))

    def key(self, name):
        item, part, _md = CASES[name]
        k = self.model.parts[item][part]
        m = self.model
        return m.sections["rows"][k], m.sections["display"][k], m.segments[k], m.moments[k]

    def licences(self, name):
        item, part, _md = CASES[name]
        return self.model.sections["licence"][self.model.parts[item][part]]

    def frame(self, name):
        rows, ds, starts, moms = self.key(name)
        got = T.compose_key(rows, ds, starts, moms, self.rules, licences=self.licences(name))
        j = E.segment_index(starts, CAL.day_of(CASES[name][2]), moms, "Wednesday", inside=False)
        return got[j]


@pytest.fixture(scope="module")
def live(request):
    return _Live(request)


#: each named case's table, whole (the words are G's; the owner reads them in G2's catalogue)
EXPECTED: dict = {}
EXPECTED.update({
    'adams-jul15': (
        'top: Keep 4 trout and char {day}\n'
        'fold: 4 a day (keep)\n'
        'card: Until Dec 31, then closed to all fishing\n'
        '[Keep 4 in all · only 1 over 50 cm (20 in)]\n'
        '  Rainbow trout: 30 cm (12 in) or longer  <counts toward the 4 · only 2>\n'
        '  Brook trout:   <counts toward the 4>\n'
        '  Brown trout:   <counts toward the 4>\n'
        '  Cutthroat trout:   <counts toward the 4>\n'
        '  Dolly Varden/bull trout: 80 cm (31 in) or longer  <counts toward the 4 · only 1 between them>  (rule changes Jul 31)\n'
        '  Lake trout: 60 cm (24 in) or longer  <counts toward the 4 · only 1 between them>  (until Oct 14, then release)\n'
        '[Release]\n'
        '  Steelhead: ~Steelhead rules apply here; steelhead may not be present in this water.'
    ),
    'adams-jul31': (
        'top: Keep 4 trout and char {day}\n'
        'fold: 4 a day (keep)\n'
        'card: Until Dec 31, then closed to all fishing\n'
        '[Keep 4 in all · only 1 over 50 cm (20 in)]\n'
        '  Rainbow trout: 30 cm (12 in) or longer  <counts toward the 4 · only 2>\n'
        '  Brook trout:   <counts toward the 4>\n'
        '  Brown trout:   <counts toward the 4>\n'
        '  Cutthroat trout:   <counts toward the 4>\n'
        '  Dolly Varden/bull trout: 80 cm (31 in) or longer  <counts toward the 4 · only 1 between them>  (until Jul 31, then release)\n'
        '  Lake trout: 60 cm (24 in) or longer  <counts toward the 4 · only 1 between them>  (until Oct 14, then release)\n'
        '[Release]\n'
        '  Steelhead: ~Steelhead rules apply here; steelhead may not be present in this water.'
    ),
    'bish-may20': (
        'top: Keep no trout and char {day}\n'
        'fold: None to keep (none)\n'
        '[Release]\n'
        '  Rainbow trout: \n'
        '  Cutthroat trout: \n'
        '  Brown trout: \n'
        '  Dolly Varden/bull trout: \n'
        '  Lake trout: \n'
        '  Brook trout: \n'
        '[Closed]  (reopens Jun 16)\n'
        '  Steelhead: ~Steelhead are known to be in this water.'
    ),
    'bowron-jul15': (
        'top: Keep 2 trout and char {day}\n'
        'fold: 2 a day (keep)\n'
        '[Keep 2 in all · only 1 over 50 cm (20 in) · In your possession: no more than 2 trout and char in all · Lake’s rule]\n'
        '  Brook trout:   <counts toward the 2>\n'
        '  Brown trout:   <counts toward the 2>\n'
        '  Cutthroat trout:   <counts toward the 2>\n'
        '  Lake trout:   <counts toward the 2>  (until Sep 30, then release)\n'
        '  Rainbow trout:   <counts toward the 2>\n'
        '  Dolly Varden/bull trout:   <counts toward the 2 · only 1>'
    ),
    'burnt-jan15': (
        'top: Release every trout and char {day}\n'
        'fold: Release (release)\n'
        '[Release]\n'
        '  Rainbow trout: \n'
        '  Cutthroat trout: \n'
        '  Brown trout: \n'
        '  Dolly Varden/bull trout: \n'
        '  Lake trout: \n'
        '  Brook trout: \n'
        'year: Trout and char through the year'
    ),
    'cunningham-jul15': (
        'top: Keep 5 trout and char {day}\n'
        'fold: 5 a day (keep)\n'
        'card: Until Oct 31, then closed to all fishing\n'
        '[Keep 5 in all · only 1 over 50 cm (20 in)]\n'
        '  Brook trout:   <counts toward the 5>\n'
        '  Brown trout:   <counts toward the 5>\n'
        '  Cutthroat trout:   <counts toward the 5>\n'
        '  Rainbow trout:   <counts toward the 5>\n'
        '  Lake trout: 30 cm (12 in) or longer | ~3 a day, but never more than 2 lake trout with you, and only 1 of them over 50 cm (20 in)  <only 2 in your possession (until Sep 14, then release)>\n'
        '  Dolly Varden/bull trout: 30–50 cm (12–20 in) | in your possession: 1 day’s limit, not 2  <counts toward the 5 · only 1 (until Aug 14, then release)>'
    ),
    'echoes-jul15': (
        'top: Keep 1 trout and char {day} // only 1 from both lakes together\n'
        'fold: 1 a day (keep)\n'
        '[Keep 1 in all]\n'
        '  Brook trout: 50 cm (20 in) or longer  <counts toward the 1>\n'
        '  Brown trout: 50 cm (20 in) or longer  <counts toward the 1>\n'
        '  Cutthroat trout: 50 cm (20 in) or longer  <counts toward the 1>\n'
        '  Dolly Varden/bull trout: 50 cm (20 in) or longer  <counts toward the 1>\n'
        '  Lake trout: 50 cm (20 in) or longer  <counts toward the 1>\n'
        '  Rainbow trout: 50 cm (20 in) or longer  <counts toward the 1>'
    ),
    'elk-apr15': (
        'NULL'
    ),
    'elk-jul15': (
        'top: Keep 1 trout and char {day}\n'
        'fold: 1 a day (keep)\n'
        'card: Until Oct 31, then release\n'
        '[Keep 1 in all · River’s rule]\n'
        '  Brook trout:   <counts toward the 1>\n'
        '  Brown trout:   <counts toward the 1>\n'
        '  Lake trout:   <counts toward the 1>\n'
        '  Rainbow trout:   <counts toward the 1>\n'
        '  Cutthroat trout: 30 cm (12 in) or longer  <counts toward the 1>\n'
        '  Dolly Varden/bull trout: 75 cm (30 in) or longer  <counts toward the 1>'
    ),
    'elk-nov15': (
        'top: Release every trout and char {day}\n'
        'fold: Release (release)\n'
        'under top: until Mar 31, then closed to all fishing\n'
        '[Release]\n'
        '  Rainbow trout: \n'
        '  Cutthroat trout: \n'
        '  Brown trout: \n'
        '  Dolly Varden/bull trout: \n'
        '  Lake trout: \n'
        '  Brook trout: \n'
        'year: Trout and char through the year'
    ),
    'fraser-r2-mission-jul15': (
        'top: Keep 2 trout and char {day}\n'
        'fold: 2 a day (keep)\n'
        '[Keep 2 in all · HATCHERY ONLY · only 1 over 50 cm (20 in), 2 if one is a hatchery steelhead]\n'
        '  Brook trout: 30 cm (12 in) or longer  <counts toward the 2>\n'
        '  Brown trout: 30 cm (12 in) or longer  <counts toward the 2>\n'
        '  Cutthroat trout: 30 cm (12 in) or longer  <counts toward the 2>\n'
        '  Rainbow trout: 30–50 cm (12–20 in)  <counts toward the 2>\n'
        '  Steelhead: a rainbow over 50 cm (20 in) | after keeping 2, stop fishing this river today | ~Steelhead are known to be in this water.  <counts toward the 2>  (until Aug 14, then closed for steelhead)\n'
        '  Dolly Varden/bull trout: 60 cm (24 in) or longer  <counts toward the 2 · only 1 between them>\n'
        '  Lake trout: 60 cm (24 in) or longer  <counts toward the 2 · only 1 between them>'
    ),
    'fraser-r3-nov15': (
        'top: Keep 2 trout and char {day}\n'
        'fold: 2 a day (keep)\n'
        '[Keep 2 in all · only 1 over 50 cm (20 in)]  (until Sep 30, then closed to trout and char)\n'
        '  Brook trout:   <counts toward the 2>\n'
        '  Brown trout:   <counts toward the 2>\n'
        '  Cutthroat trout:   <counts toward the 2>\n'
        '  Dolly Varden/bull trout:   <counts toward the 2 · only 1 (until Jul 31, then release)>\n'
        '  Rainbow trout: 50 cm (20 in) or less  <counts toward the 2>\n'
        '[Release]  (until Nov 30)\n'
        '  Lake trout: \n'
        '[Closed]\n'
        '  Steelhead: ~Steelhead are known to be in this water.'
    ),
    'fraser-r3-oct15': (
        'top: Closed for trout and char {day}\n'
        'fold: Closed (closed)\n'
        'under top: reopens Nov 1\n'
        '[Closed]\n'
        '  Rainbow trout: \n'
        '  Steelhead: closed all year | ~Steelhead are known to be in this water.\n'
        '  Cutthroat trout: \n'
        '  Brown trout: \n'
        '  Dolly Varden/bull trout: \n'
        '  Lake trout: \n'
        '  Brook trout: '
    ),
    'fraser-r3-sep15': (
        'top: Keep 2 trout and char {day}\n'
        'fold: 2 a day (keep)\n'
        '[Keep 2 in all · only 1 over 50 cm (20 in)]  (until Sep 30, then closed to trout and char)\n'
        '  Brook trout:   <counts toward the 2>\n'
        '  Brown trout:   <counts toward the 2>\n'
        '  Cutthroat trout:   <counts toward the 2>\n'
        '  Lake trout:   <counts toward the 2>\n'
        '  Rainbow trout: 50 cm (20 in) or less  <counts toward the 2>\n'
        '[Release]\n'
        '  Dolly Varden/bull trout:   (until Sep 30, then closed to trout and char)\n'
        '[Closed]\n'
        '  Steelhead: ~Steelhead are known to be in this water.'
    ),
    'istsi-jul15': (
        'top: Keep 1 trout and char {day}\n'
        'fold: 1 a day (keep)\n'
        'card: Until Mar 31, then closed to all fishing\n'
        '[Keep 1 in all · In your possession: just 1, the same as the day]\n'
        '  Brook trout: 30–50 cm (12–20 in)  <counts toward the 1>\n'
        '  Brown trout: 30–50 cm (12–20 in)  <counts toward the 1>\n'
        '  Cutthroat trout: 30–50 cm (12–20 in)  <counts toward the 1>\n'
        '  Rainbow trout: 30–50 cm (12–20 in)  <counts toward the 1>\n'
        '  Lake trout: 30–50 cm (12–20 in)  <counts toward the 1>  (until Sep 14, then release)\n'
        '[Release]\n'
        '  Dolly Varden/bull trout: '
    ),
    'kamloops-lake-jul15': (
        'top: Keep 5 trout and char {day}\n'
        'fold: 5 a day (keep)\n'
        '[Keep 5 in all · only 1 over 50 cm (20 in)]\n'
        '  Rainbow trout: 50 cm (20 in) or less  <counts toward the 5 · only 2>\n'
        '  Brook trout:   <counts toward the 5>\n'
        '  Brown trout:   <counts toward the 5>\n'
        '  Cutthroat trout:   <counts toward the 5>\n'
        '  Dolly Varden/bull trout: 60 cm (24 in) or longer  <counts toward the 5 · only 1 between them>\n'
        '  Lake trout: 60 cm (24 in) or longer  <counts toward the 5 · only 1 between them>  (until Oct 14, then release)'
    ),
    'khartoum-jul15': (
        'top: Keep 4 trout and char {day}\n'
        'sub: or 6 if they’re rainbow trout or steelhead\n'
        'fold: 4 or 6 a day (keep)\n'
        '[Keep 4 in all, or 6 if they’re rainbow trout or steelhead · only 1 over 50 cm (20 in), not counting rainbow trout or steelhead; only 2 wild, each under 40 cm (16 in)]\n'
        '  Brook trout: under 40 cm (16 in) if wild  <counts toward the 4>\n'
        '  Brown trout: under 40 cm (16 in) if wild  <counts toward the 4>\n'
        '  Cutthroat trout: under 40 cm (16 in) if wild  <counts toward the 4>\n'
        '  Dolly Varden/bull trout [HATCHERY ONLY]: 60 cm (24 in) or longer  <counts toward the 4 · only 1 between them>\n'
        '  Lake trout [HATCHERY ONLY]: 60 cm (24 in) or longer  <counts toward the 4 · only 1 between them>\n'
        '  Rainbow trout: under 40 cm (16 in) if wild  <counts toward the 6>\n'
        '  Steelhead [HATCHERY ONLY]: ~Steelhead are known to be in this water.  <counts toward the 6>'
    ),
    'kootenay-main-nov15': (
        'top: Keep 5 trout and char {day}\n'
        'sub: or 10 if they’re rainbow trout\n'
        'fold: 5 or 10 a day (keep)\n'
        '[Keep 5 in all, or 10 if they’re rainbow trout]\n'
        '  Brook trout:   <counts toward the 5>\n'
        '  Brown trout:   <counts toward the 5>\n'
        '  Cutthroat trout: only 1 over 50 cm (20 in)  <counts toward the 5>\n'
        '  Lake trout:   <counts toward the 5>\n'
        '  Dolly Varden/bull trout:   <counts toward the 5 · only 1>\n'
        '  Rainbow trout:   <counts toward the 10>'
    ),
    'lois-jul15': (
        'top: Keep 4 trout and char {day}\n'
        'sub: or 6 if they’re rainbow trout or steelhead\n'
        'fold: 4 or 6 a day (keep)\n'
        '[Keep 4 in all, or 6 if they’re rainbow trout or steelhead · only 1 over 50 cm (20 in), not counting rainbow trout or steelhead; only 2 wild, each under 40 cm (16 in)]\n'
        '  Brook trout: under 40 cm (16 in) if wild  <counts toward the 4>\n'
        '  Brown trout: under 40 cm (16 in) if wild  <counts toward the 4>\n'
        '  Cutthroat trout: under 40 cm (16 in) if wild  <counts toward the 4>\n'
        '  Dolly Varden/bull trout [HATCHERY ONLY]: 60 cm (24 in) or longer  <counts toward the 4 · only 1 between them>\n'
        '  Lake trout [HATCHERY ONLY]: 60 cm (24 in) or longer  <counts toward the 4 · only 1 between them>\n'
        '  Rainbow trout: under 40 cm (16 in) if wild  <counts toward the 6>\n'
        '  Steelhead [HATCHERY ONLY]: ~Steelhead are known to be in this water.  <counts toward the 6>'
    ),
    'nation-lakes-jul15': (
        'top: Keep 5 trout and char {day}\n'
        'fold: 5 a day (keep)\n'
        'card: Until Oct 31, then closed to all fishing\n'
        '[Keep 5 in all · only 1 over 50 cm (20 in)]\n'
        '  Brook trout:   <counts toward the 5>\n'
        '  Brown trout:   <counts toward the 5>\n'
        '  Cutthroat trout:   <counts toward the 5>\n'
        '  Rainbow trout:   <counts toward the 5>\n'
        '  Lake trout: 30 cm (12 in) or longer | ~3 a day, but never more than 2 lake trout with you, and only 1 of them over 50 cm (20 in)  <only 2 in your possession (until Sep 14, then release)>\n'
        '  Dolly Varden/bull trout: 30–50 cm (12–20 in) | in your possession: 1 day’s limit, not 2  <counts toward the 5 · only 1 (until Aug 14, then release)>'
    ),
    'shamrock-jul15': (
        'top: Keep 2 trout and char {day}\n'
        'fold: 2 a day (keep)\n'
        'card: Until Jan 31, then closed to all fishing\n'
        '[Keep 2 in all · HATCHERY ONLY]\n'
        '  Brook trout: 20–30 cm (8–12 in)  <counts toward the 2>\n'
        '  Brown trout: 20–30 cm (8–12 in)  <counts toward the 2>\n'
        '  Cutthroat trout: 20–30 cm (8–12 in)  <counts toward the 2>\n'
        '  Rainbow trout: 20–30 cm (8–12 in)  <counts toward the 2>\n'
        '[Release]\n'
        '  Steelhead: ~Steelhead rules apply here; steelhead may not be present in this water.\n'
        '  Dolly Varden/bull trout: \n'
        '  Lake trout: '
    ),
    'skeena-sedan-mill-jul15': (  # T854 (owner 2026-11-18): lake trout are in Region 6's "1 trout from streams"
        'top: Keep 1 trout and char {day}\n'
        'fold: 1 a day (keep)\n'
        'card: Until Oct 31, then release\n'
        '[Keep 1 in all]\n'
        '  Brook trout: 30 cm (12 in) or longer  <counts toward the 1>\n'
        '  Brown trout: 30 cm (12 in) or longer  <counts toward the 1>\n'
        '  Cutthroat trout: 30 cm (12 in) or longer  <counts toward the 1>\n'
        '  Lake trout: 30 cm (12 in) or longer  <counts toward the 1>  (until Sep 14, then release)\n'
        '  Rainbow trout: 30–50 cm (12–20 in)  <counts toward the 1>\n'
        '  Steelhead [HATCHERY ONLY]: a rainbow over 50 cm (20 in) | after keeping 1, stop fishing this stream today | ~Steelhead are known to be in this water.  <counts toward the 1>\n'
        '[Release]\n'
        '  Dolly Varden/bull trout:   (until Dec 31, then closed to all fishing)'
    ),
    'tagish-jul15': (
        'top: Keep 5 trout and char {day}\n'
        'fold: 5 a day (keep)\n'
        '[Keep 5 in all · only 1 over 50 cm (20 in), not counting lake trout]\n'
        '  Brook trout:   <counts toward the 5>\n'
        '  Brown trout:   <counts toward the 5>\n'
        '  Cutthroat trout:   <counts toward the 5>\n'
        '  Rainbow trout:   <counts toward the 5>\n'
        '  Dolly Varden/bull trout:   <counts toward the 5 · only 3 between them>\n'
        '  Lake trout: release over 60 cm (24 in) and under 90 cm (35 in) | only 2 | only 1 over 90 cm (35 in) | in your possession: no more than 2 lake trout, only 1 over 90 cm (35 in)  <counts toward the 5 · only 3 between them>'
    ),
    'takla-jul15': (
        'top: Keep 5 trout and char {day}\n'
        'fold: 5 a day (keep)\n'
        '[Keep 5 in all · only 1 over 50 cm (20 in)]\n'
        '  Brook trout:   <counts toward the 5>\n'
        '  Brown trout:   <counts toward the 5>\n'
        '  Cutthroat trout:   <counts toward the 5>\n'
        '  Rainbow trout:   <counts toward the 5>\n'
        '  Lake trout: 30 cm (12 in) or longer | in your possession: 1 day’s limit, not 2  <counts toward the 5 · only 3 (until Sep 14, then release)>\n'
        '  Dolly Varden/bull trout: 30–50 cm (12–20 in) | in your possession: 1 day’s limit, not 2  <counts toward the 5 · only 1 (until Aug 14, then release)>'
    ),
    'vedder-jul15': (
        'top: Keep 2 trout and char {day}\n'
        'sub: or 4 if they’re hatchery rainbow trout 50 cm (20 in) or less\n'
        'fold: 2 or 4 a day (keep)\n'
        'card: Until May 31, then closed to all fishing\n'
        '[Keep 4 in all · HATCHERY ONLY · only 1 over 50 cm (20 in), 2 if one is a hatchery steelhead]\n'
        '  Brook trout: 30 cm (12 in) or longer  <2 total from these 6>\n'
        '  Brown trout: 30 cm (12 in) or longer  <2 total from these 6>\n'
        '  Cutthroat trout: 30 cm (12 in) or longer  <2 total from these 6>  (until Apr 30, then release)\n'
        '  Steelhead: a rainbow over 50 cm (20 in) | after keeping 2, stop fishing this river today | ~Steelhead are known to be in this water.  <2 total from these 6>\n'
        '  Dolly Varden/bull trout: 60 cm (24 in) or longer  <2 total from these 6 [only 1 between them]>\n'
        '  Lake trout: 60 cm (24 in) or longer  <2 total from these 6 [only 1 between them]>\n'
        '  Rainbow trout: 50 cm (20 in) or less  <counts toward the 4>  (until Apr 30, then release)'
    ),
    'windsor-jul15': (
        'top: Keep 4 trout and char {day}\n'
        'fold: 4 a day (keep)\n'
        '[Keep 4 in all · only 1 over 50 cm (20 in)]\n'
        '  Brook trout hatchery:   <counts toward the 4>\n'
        '  Brown trout hatchery:   <counts toward the 4>\n'
        '  Cutthroat trout hatchery:   <counts toward the 4>\n'
        '  Rainbow trout hatchery:   <counts toward the 4>\n'
        '  Brook trout wild: 40 cm (16 in) or less  <counts toward the 4 · only 2 wild in all>\n'
        '  Brown trout wild: 40 cm (16 in) or less  <counts toward the 4 · only 2 wild in all>\n'
        '  Cutthroat trout wild: 40 cm (16 in) or less  <counts toward the 4 · only 2 wild in all>\n'
        '  Rainbow trout wild: 40 cm (16 in) or less  <counts toward the 4 · only 2 wild in all>\n'
        '  Dolly Varden/bull trout [HATCHERY ONLY]: 60 cm (24 in) or longer  <counts toward the 4 · only 1 between them>\n'
        '  Lake trout [HATCHERY ONLY]: 60 cm (24 in) or longer  <counts toward the 4 · only 1 between them>'
    ),
})



@pytest.mark.needs_bundle
@pytest.mark.parametrize("name", sorted(CASES))
def test_the_named_waters_whole(live, name):
    assert render(live.frame(name)) == EXPECTED[name]


@pytest.mark.needs_bundle
@pytest.mark.parametrize("name", sorted(CASES))
def test_every_segment_of_the_named_waters_holds_the_properties(live, name):
    rows, ds, starts, moms = live.key(name)
    assert check_key(rows, ds, T.compose_key(rows, ds, starts, moms, live.rules, licences=live.licences(name))) == []


@pytest.mark.needs_bundle
def test_adams_one_day_keeps_80_cm_on_the_row_and_says_the_day_in_the_sheet(live):
    """Spec 3.3 rule 6 (owner 15:36 "Yes, 80 cm on the row"): on Jul 31, the one day at 60 cm, the row keeps 80 cm and the
    sheet says the day; on Jul 15 the row's date is the looser-only "rule changes Jul 31"."""
    row = lambda name: next(r for b in live.frame(name)["table"]["boxes"] for r in b["rows"] if r["key"] == "DV")
    assert row("adams-jul31")["lines"][0] == "80 cm (31 in) or longer"
    assert row("adams-jul31")["sheet"]["here"][-1] == {"t": "On Jul 31 only: 60 cm (24 in) or longer.", "bold": False}
    assert not any("only:" in x["t"] for x in row("adams-jul15")["sheet"]["here"])


@pytest.mark.needs_bundle
def test_lois_and_khartoum_say_one_table(live):
    """T805 C: the two lakes' tables say the same words (their own rules differ only by lake)."""
    assert render(live.frame("lois-jul15")) == render(live.frame("khartoum-jul15"))


# ------------------------------------------------------------------------------------- (d) the whole built release

@pytest.mark.needs_bundle
def test_every_key_of_the_built_release(request):
    """Every answer key of the release under test: built, valid, every fish once, NULL per Q-D3 (a release cut before
    the switch still carries the trout groups the builder reads)."""
    from pipeline.tools.audit import trout_table as TT
    rel = under_test.release(request)
    m = json.loads((rel / "manifest.json").read_text()) if (rel / "manifest.json").exists() else {}
    rows = ((m.get("regulations") or {}).get("versions") or {}).get("rows")
    if rows is not None and int(rows) < 34:
        pytest.skip(f"the release under test is rows {rows}: this check waits for a rows-34 (cutover) release as LATEST")
    rep = TT.run(rel, log=lambda *_: None)
    assert rep["refused"] == {}, list(rep["refused"].items())[:5]
    assert rep["counts"]["tables"] > 1000, rep["counts"]


# ---------------------------------------------------------------------------------------------- (e) the mutations

MUTATIONS = [
    ("A_SUMMED_TIER_NEVER_DRAWN", "kamloops-lake-jul15"),
    ("A_SAME_RULE_NO_ORIGIN", "skeena-sedan-mill-jul15"),
    ("A_HATCHERY_ONLY_ONE_ROW", "skeena-sedan-mill-jul15"),
    ("A_BOX_LABEL_WHEN_EVERY_ROW", "vedder-jul15"),
    ("A_POSSESSION_ONLY_WHERE_IT_DIFFERS", "skeena-sedan-mill-jul15"),
    ("A_YEAR_BAR_ONE_STATE", "skeena-sedan-mill-jul15"),
    ("A_ONE_DAY_STRICTER_ON_ROW", "adams-jul31"),
]


@pytest.mark.needs_bundle
@pytest.mark.parametrize("flag,name", MUTATIONS, ids=[m[0] for m in MUTATIONS])
def test_each_rule_broken_fails_its_case(live, monkeypatch, flag, name):
    monkeypatch.setattr(T, flag, False)
    with pytest.raises((AssertionError, ValueError)):          # a changed table, or one the model refuses
        test_the_named_waters_whole(live, name)


@pytest.mark.needs_bundle
def test_the_mixed_row_split_broken_fails_its_case(live, monkeypatch):
    monkeypatch.setattr(T, "A_MIXED_ROWS_SPLIT", False)
    rows, ds, starts, moms = live.key("burnt-jan15")
    assert check_key(rows, ds, T.compose_key(rows, ds, starts, moms, live.rules,
                                             licences=live.licences("burnt-jan15"))) != []


def test_the_split_never_changes_the_frame_it_reads():
    """T810 L: `split_mixed` runs twice on one frame (the table, then the cutover), and rows are shared between keys:
    a parent index moved past a split row is moved on a copy, so the second split gives the same frame as the first."""
    closed_mixed = {"kind": "closed", "members": ["RB", "KO"], "all_members": ["RB", "KO"], "groups": []}
    pool = {"kind": "keep", "pool": 1, "members": ["CT"], "all_members": ["CT"], "groups": []}
    held = {"kind": "keep", "pool": 2, "members": ["CT"], "all_members": ["CT"], "groups": [], "parent": 1}
    frame = {"rows": [closed_mixed, pool, held]}
    before = copy.deepcopy(frame)
    first, covers = T.split_mixed(frame)
    assert frame == before
    assert [r.get("parent") for r in first["rows"]] == [None, None, None, 2] and covers == [0, 2, 3]
    assert T.split_mixed(frame) == (first, covers)


def test_the_steelhead_presence_line_stays_on_the_steelhead_part():
    """T810 cutover: a release row holding steelhead and a fish outside the table (the live bundle has them) is split;
    the steelhead presence line goes with the part holding ST (the table's), never the remainder (`model.Row` refuses a
    presence line on a row without ST). Mutation: the remainder keeping `presence` fails here and stops the build."""
    line = {"line": "possible_with_rules", "says": "Steelhead may be present."}
    mixed = {"kind": "release", "members": ["ST", "KO"], "all_members": ["ST", "KO"], "groups": [], "presence": line}
    split, covers = T.split_mixed({"rows": [mixed]})
    assert [r["all_members"] for r in split["rows"]] == [["ST"], ["KO"]] and covers == [0]
    assert [r.get("presence") for r in split["rows"]] == [line, None]
    assert mixed["presence"] == line
