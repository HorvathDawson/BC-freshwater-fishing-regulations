"""Trout table plan, ticket F0 (contract 2.11; review should-fix 1, fights #2 and #3): THE PAGE'S POSSESSION LINE, CHOSEN
ONCE IN PYTHON. `card.possession_lead` is the one choice of the page's lead; `card.page_line` ships it in
`RowsFrame.page_possession` (the sentence, its value words, its rule; the also-lines the page row keeps, no trout- or
char-only rule) from rows 34 only (`model.rows.page_possession_written`); `card.possession_duty` says a trout or char
fish's own possession only where it differs from that lead.

THE TEXT (synopsis 2025-2027, printed pages): Zone 7A p.58 "Possession quotas = 2 daily quotas for most species … Lake
trout: possession quota = 1 daily quota. Bull trout (Dolly Varden): possession quota = 1 daily quota" (7B p.64 the same);
Cunningham Lake p.59 "Lake trout possession quota = 2 (only one over 50 cm)"; Bowron Lake Park waters p.43 "Trout/char
daily and possession quotas = 1"; Region 6 Bennett Lake p.50 "Northern pike … possession quota = 4".

Anchors: (a) EVERY FRAME of the committed mini release and of the committed regs fixture (`app/packages/data/dev/
regs-release/`), decoded from their regs stores and read back as the reader's answers (`_Release`) — the duty and the lead
run on them with THIS code, so a fixture cut before T805 still proves the rule; (b) the answers 2.8 fixture bundle with
planted Region 2 possession rules, through the real reader and rows; (c) the live waters (`needs_bundle`).
"""
from __future__ import annotations

import copy
import json
import re
import types
from pathlib import Path
from typing import Dict, List

import pytest
from pydantic import TypeAdapter

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import card as CD
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.answers.model import rows as RM
from pipeline.deliver.answers.rows.section import refs
from pipeline.deliver.bundle import read
from pipeline.deliver.store.release import province_store
from pipeline.tests import answers28_fixture as F
from pipeline.tests import under_test

TC = C.trout_char()
REPO = Path(__file__).resolve().parents[2]
REGS_FIXTURE = REPO / "app" / "packages" / "data" / "dev" / "regs-release"


# ------------------------------------------------------------------------------------------- (a) the committed releases

def _regs_fixture() -> Path:
    got = sorted(p for p in REGS_FIXTURE.glob("r-*") if p.is_dir())
    if len(got) != 1:
        pytest.fail(f"the regs fixture: {len(got)} releases under {REGS_FIXTURE} (one expected) — "
                    "python -m pipeline.deliver.release.regs_fixture", pytrace=False)
    return got[0]


class _Release:
    """A committed release's rows frames, decoded from its regs store, as the reader's answers: per frame the part's
    rules (`rows.R`, from the release's rule table), `model["R"]` (each decided answer with its roles by rule key and
    its lines holding their rules) and the display section's sentence of every rule."""

    def __init__(self, d: Path):
        st = province_store(d, verify=False)
        try:
            self.rules: Dict[int, RW.R] = {}
            for rix, b in st.con.execute("SELECT rix, j FROM rule"):
                r = st.val("rule", b)
                x = {**(r.get("fields") or {}), "entry": r["entry_id"], "rule": r["rule_id"],
                     "entry_id": r["entry_id"], "rule_id": r["rule_id"], "type": r["type"], "family": r["family"],
                     "dimension": r.get("dimension"), "label": r.get("label"),
                     "entry_name": (r.get("prov") or {}).get("entry_name"), "_rank": (r.get("prov") or {})["rank"]}
                self.rules[rix] = RW.R(f"{r['entry_id']}::{r['rule_id']}", x, "reach")
            self.ix = {r.key: i for i, r in self.rules.items()}
            self.plain = {i: st.val("display_rule", b).get("plain")
                          for i, b in st.con.execute("SELECT id, j FROM display_rule")}
            self.frames = {i: st.val("rows_frame", b) for i, b in st.con.execute("SELECT id, j FROM rows_frame")}
            self.decided = {i: st.val("rows_decided", b) for i, b in st.con.execute("SELECT id, j FROM rows_decided")}
        finally:
            st.close()

    def _res(self, d):
        if d is None:
            return None
        x = self.decided[d]
        return {**x, "roles": {self.rules[k].key: {"role": role, "by": by} for k, role, by in x["roles"]},
                "lines": [{**ln, "r": self.rules[ln["r"]]} for ln in x["lines"]]}

    def each(self):
        """(frame id, P, model, {fish: its leading answer}) for every rows frame."""
        P = types.SimpleNamespace(by_key={r.key: r for r in self.rules.values()})
        for fid, fr in sorted(self.frames.items()):
            fish = fr[1]
            model = {"R": {S: {"H": self._res(h), "W": self._res(w)} for S, (h, w, _lead) in fish.items()}}
            main = {S: (self._res(h if lead == "hatchery" else w) if lead else None)
                    for S, (h, w, lead) in fish.items()}
            yield fid, P, model, main


@pytest.fixture(scope="module", params=["mini", "regs_fixture"])
def release(request):
    return request.param, _Release(under_test.mini() if request.param == "mini" else _regs_fixture())


def _covers(lead, S) -> bool:
    sp = lead.f.get("species") or []
    return "ALL_GAME_FISH" in sp or S in C.expand(sp)


def _iff_problems(rel: _Release) -> tuple:
    """(problems, counts): over every frame and every kept trout or char fish, a possession line on its row
    (`possession_duty`) exists exactly where its possession differs from the lead the page ships (`page_possession`)."""
    problems: List[str] = []
    n = {"frames": 0, "fish": 0, "differs": 0}
    for fid, P, model, main in rel.each():
        n["frames"] += 1
        lead = CD.page_possession(P, model)
        for S, m in main.items():
            if S not in TC or m is None or m["status"] not in C.KEEPS:
                continue
            n["fish"] += 1
            own = [P.by_key[k] for k, ro in m["roles"].items() if ro["role"] in ("possession", "possession_cap")]
            differs = any(r.k == "possession_cap" for r in own) or any(
                r.f.get("per_daily") and not (lead is not None and _covers(lead, S)
                                              and float(r.f["per_daily"]) == float(lead.f["per_daily"]))
                for r in own if r.k == "possession")
            n["differs"] += differs
            line = CD.possession_duty(P, model, S)
            if (line is not None) != differs:
                problems.append(f"frame {fid} {S}: line {line!r}, differs from the lead "
                                f"{None if lead is None else lead.key} = {differs} ({[r.key for r in own]})")
    return problems, n


def test_the_fish_line_and_the_page_lead_are_one_rule_over_every_frame(release):
    name, rel = release
    problems, n = _iff_problems(rel)
    assert problems == [], problems[:10]
    assert n["frames"] > 0
    if name == "regs_fixture":
        # not vacuous: Zone 7A/7B lake trout and bull trout, Cunningham, the Bowron park waters differ; most fish don't
        assert n["differs"] >= 20 and n["fish"] - n["differs"] >= 500, n
    else:
        # the invented world holds one region-wide multiplier and nothing else: every fish is the page's, no line
        assert n["differs"] == 0 and n["fish"] > 0, n


def _page_problems(rel: _Release) -> tuple:
    """Every frame's `page_line`, read back: the lead is `possession_lead`'s pick and says the display section's own
    sentence of its rule; the value words are in it; no kept also-line holds only trout or char (fight #3) and each
    is a possession rule some answer gives that role; the rows-34 frame with the field passes its model."""
    problems: List[str] = []
    n = {"leads": 0, "also": 0, "null": 0, "dropped_trout": 0}
    ad = TypeAdapter(RM.RowsFrame34)
    for fid, P, model, _main in rel.each():
        got = CD.page_line(P, model)
        per = CD.possession_role_rules(P, model)
        caps = CD.possession_role_rules(P, model, "possession_cap")
        n["dropped_trout"] += sum(1 for r in per + caps if set(C.expand(r.f.get("species"))) <= TC
                                  and C.expand(r.f.get("species")))
        if got is None:
            n["null"] += 1
            lead = CD.possession_lead(per)
            if lead is not None:
                problems.append(f"frame {fid}: no page line, but {lead.key} leads")
            continue
        if got["lead"] is not None:
            n["leads"] += 1
            src = rel.ix[got["lead"]["src"]]
            if got["lead"]["src"] != CD.possession_lead(per).key:
                problems.append(f"frame {fid}: lead {got['lead']['src']} is not possession_lead's pick")
            if got["lead"]["says"] != rel.plain[src]:
                problems.append(f"frame {fid}: lead says {got['lead']['says']!r}, display says {rel.plain[src]!r}")
        for a in got["also"]:
            n["also"] += 1
            sp = C.expand(P.by_key[a["src"]].f.get("species"))
            if sp and set(sp) <= TC:
                problems.append(f"frame {fid}: also-line {a['src']} holds only trout or char {sp}")
            if a["says"] != rel.plain[rel.ix[a["src"]]]:
                problems.append(f"frame {fid}: also says {a['says']!r}, display says {rel.plain[rel.ix[a['src']]]!r}")
        fr = rel.frames[fid]
        wire = {"spp": fr[0],
                "fish": {S: {"hatchery": None if h is None else rel.decided[h],
                             "wild": None if w is None else rel.decided[w], "lead": lead}
                         for S, (h, w, lead) in fr[1].items()},
                "rows": [], "steelhead_line": fr[3],
                "page_possession": {"lead": None if got["lead"] is None else {**got["lead"],
                                                                               "src": rel.ix[got["lead"]["src"]]},
                                    "also": [{**a, "src": rel.ix[a["src"]]} for a in got["also"]]}}
        try:
            ad.validate_json(json.dumps(wire))
        except Exception as e:                          # noqa: BLE001 — named in the report
            problems.append(f"frame {fid}: the rows-34 model refuses it: {str(e)[:300]}")
    return problems, n


def test_every_frames_page_line_is_the_lead_and_the_non_trout_lines(release):
    name, rel = release
    problems, n = _page_problems(rel)
    assert problems == [], problems[:10]
    if name == "regs_fixture":
        # the region's "2 days' quota" leads everywhere but a tidal lake; the trout counts (Bowron's 1, Cunningham's 2)
        # left the page row for the table, the non-trout ones (Bennett Lake's pike 4) stay on it
        assert n["leads"] >= 500 and n["dropped_trout"] >= 50 and n["also"] >= 1, n
    else:
        # the invented world's one region-wide multiplier leads every frame, alone
        assert n["leads"] == len(rel.frames) and n["also"] == 0, n


def test_the_lead_is_the_one_for_every_game_fish_else_the_most_days():
    """The rule itself, on the shapes no committed frame holds: two multipliers for every game fish (the larger leads),
    none for every game fish (the most days lead, ties by rule), none at all."""
    def r(key, per, sp):
        return types.SimpleNamespace(key=key, f={"per_daily": per, "species": sp})
    every2, every3, lt1, gr3 = (r("a", 2, ["ALL_GAME_FISH"]), r("b", 3, ["ALL_GAME_FISH"]), r("c", 1, ["LT"]),
                                r("d", 3, ["GR"]))
    assert CD.possession_lead([lt1, every2, gr3]) is every2         # for every game fish beats more days
    assert CD.possession_lead([every2, lt1, every3]) is every3
    assert CD.possession_lead([lt1, gr3]) is gr3
    assert CD.possession_lead([r("z", 1, ["DV"]), lt1]) is lt1        # a tie: by rule
    assert CD.possession_lead([r("e", None, ["LT"])]) is None and CD.possession_lead([]) is None
    assert CD.lead_covers(every2, "KO") and CD.lead_covers(lt1, "LT") and not CD.lead_covers(lt1, "DV")
    assert CD.lead_covers(r("f", 2, ["TROUT_CHAR"]), "DV") and not CD.lead_covers(None, "RB")


# ----------------------------------------------------------------- mutation: each check above fails on a planted defect

def _rel():
    return _Release(_regs_fixture())


@pytest.fixture(scope="module")
def fixture_release():
    return _rel()


def test_mutation_a_second_pick_of_the_lead_breaks_the_rule(fixture_release, monkeypatch):
    # the app's pick ("names no fish, else most days") differs only on ties; the pick of the FEWEST days is a second rule
    monkeypatch.setattr(CD, "possession_lead", lambda rules: (sorted(
        [r for r in rules if r.f.get("per_daily")], key=lambda r: (float(r.f["per_daily"]), r.key)) or [None])[0])
    problems, _n = _iff_problems(fixture_release)
    assert problems


def test_mutation_no_fish_line_breaks_the_rule(fixture_release, monkeypatch):
    monkeypatch.setattr(CD, "A_POSSESSION_ON_THE_FISH_ROW", False)
    problems, _n = _iff_problems(fixture_release)
    assert any("line None" in p for p in problems)


def test_mutation_a_line_where_the_lead_is_the_fishs_breaks_the_rule(fixture_release, monkeypatch):
    real = CD.possession_duty
    monkeypatch.setattr(CD, "possession_duty", lambda P, model, S: real(P, model, S) or (
        "in your possession: 2 days’ limits" if S == "RB" else None))
    problems, _n = _iff_problems(fixture_release)
    assert any(" RB: line 'in your possession" in p for p in problems)


def test_mutation_trout_lines_kept_on_the_page_row_are_refused(fixture_release, monkeypatch):
    monkeypatch.setattr(CD, "trout_only", lambda r: False)
    problems, _n = _page_problems(fixture_release)
    assert any("holds only trout or char" in p for p in problems)


def test_mutation_a_lead_other_than_the_pick_is_refused(fixture_release, monkeypatch):
    real = CD.page_line

    def wrong(P, model):
        got = real(P, model)
        if got and got["lead"]:
            got = {**got, "lead": {**got["lead"], "src": "z7a:possession_exceptions::possession_exceptions.r1"}}
        return got
    monkeypatch.setattr(CD, "page_line", wrong)
    problems, _n = _page_problems(fixture_release)
    assert any("is not possession_lead's pick" in p for p in problems)


# ----------------------------------------------------------- (b) the fixture bundle, through the real reader and rows

PATH = "fixture:f0_page_possession"
JUL15 = (7, 15)
PAGE = ("z2:possession_quota", "possession_quota.r1")
LT1, LT2, BOWRON = ("r2:f0_lt_one@2-12", "r2:f0_lt_two@2-12", "r2:f0_bowron@2-12")
_SETS = {LT1: 1, LT2: 2, BOWRON: 3, None: 4}
_ROWS = {
    # Zone 7A's words: lake trout 1 daily quota (another number than the page's 2)
    LT1: [("f0_lt_one.r1", {"label": "Lake trout — possession quota is 1 daily quota", "species": ["LT"],
                            "per_daily": 1, "period": "possession", "dimension": "possession"})],
    # the same number as the page's: no line on the row, nothing under the page's
    LT2: [("f0_lt_two.r1", {"label": "Lake trout — possession quota is 2 daily quotas", "species": ["LT"],
                            "per_daily": 2, "period": "possession", "dimension": "possession"})],
    # the Bowron park's words (p.43): a trout and char count leaves the page row for the table
    BOWRON: [("f0_bowron.r1", {"label": "Trout/char — 1 in possession", "species": ["TROUT_CHAR"], "take": 1,
                               "period": "possession", "dimension": "possession"})],
}


def _bundle():
    got = read._RULES_BY_PATH.get(PATH)
    if got is None:
        rules = json.loads(F.FIXTURE.read_text(encoding="utf-8"))["rules"]
        got = {(x["entry"], x["rule"]): x for x in rules}
        got[PAGE] = {
            "entry": PAGE[0], "entry_id": PAGE[0], "entry_name": "Region 2", "rule": PAGE[1], "rule_id": PAGE[1],
            "dimension": "possession", "extent_text": None, "extents": [{"area_id": "area:region:2", "op": "within"}],
            "family": "retention", "label": "Possession quotas = 2 daily quotas", "may_target": None, "parts": "{}",
            "per_daily": 2, "period": "possession", "scope": "area", "species": ["ALL_GAME_FISH"], "species_except": [],
            "type": "retention_limit", "uncertain": 0, "undrawn_part": None, "unresolved": None,
            "verbatim": "Possession quotas = 2 daily quotas"}
        for e, rows in _ROWS.items():
            for r, f in rows:
                got[(e, r)] = {"entry_id": e, "rule_id": r, "type": "retention_limit", "family": "retention",
                               "parts": json.dumps({"what": f["label"]}), "scope": "section", "species_except": [],
                               "uncertain": 0, "unresolved": None, "extent_text": None, "undrawn_part": None,
                               "extents": [{"op": "whole"}], "rule": r, "entry": e, "entry_name": e,
                               "may_target": None, "verbatim": f["label"], **f}
        for x in got.values():
            x["_rank"] = read.source_of(x).rank
        read._RULES_BY_PATH[PATH] = got
    lift = tuple(F.LIFT.split("::"))
    lake = [(e, r) for (e, r), x in got.items() if (e, r) != lift and x.get("water") != "stream" and e not in _ROWS]
    sets = {sid: sorted((e, r, "reach") for e, r in lake + [(e, r) for (e, r) in got if e == entry])
            for entry, sid in _SETS.items()}
    return F._B(rules=got, sets=sets)


def _frame(entry):
    B = _bundle()
    bound = B.sets[_SETS[entry]]
    lad = {f: {o: A.ladder_verdict(read.effective_rules_bound(
        bound, False, JUL15, f, PATH, steelhead_rules_here=True, origin=None if o == "none" else o, trace=True))
        for o in F.ORIGINS} for f in F.FISH}
    P = RW.Part(B, _SETS[entry], False, True, "lake", None, lad, JUL15, {})
    return RW.produce(P), P


@pytest.fixture
def rows34(monkeypatch):
    monkeypatch.setattr(RM, "ROWS_VERSION", 34)


def _lines(out, P) -> dict:
    """Each fish's own possession line (`card.possession_duty`: what the trout table's row says, ticket L — the card's
    kinds say none since the cutover), [] where the page's line is the fish's."""
    from pipeline.deliver.answers.rows.card_model import build_model
    model = build_model(P)
    return {S: [w] if (w := CD.possession_duty(P, model, S)) else [] for S in out["fish"]}


PAGE_LEAD = {"says": "Possession: 2 days’ quota. Fish at home don’t count.", "value": "2 days’ quota",
             "src": "::".join(PAGE)}


def test_rows_34_writes_page_possession_since_the_cutover():
    """T810 ticket L: rows is 34, so every frame carries `page_possession`, the spec says it and the frame model holds it."""
    out, _P = _frame(LT1)
    assert RM.ROWS_VERSION == 34 and RM.page_possession_written()
    assert "page_possession" in out
    assert A.SECTIONS[[s.name for s in A.SECTIONS].index("rows")].version == RM.ROWS_VERSION
    assert E.SPEC["sections"]["rows"]["version"] == str(RM.ROWS_VERSION)
    assert "page_possession" in E.SPEC["sections"]["rows"]["frames"]
    assert "page_possession" in json.dumps(TypeAdapter(RM.rows_frame_model()).json_schema())


def test_rows_33_wrote_no_page_possession(monkeypatch):
    monkeypatch.setattr(RM, "ROWS_VERSION", 33)
    out, _P = _frame(LT1)
    assert not RM.page_possession_written() and "page_possession" not in out


def test_rows_34_another_number_of_days_on_the_row_and_the_page_keeps_its_lead(rows34):
    out, P = _frame(LT1)
    assert out["page_possession"] == {"lead": PAGE_LEAD, "also": []}       # the lake trout's line is the table's
    assert _lines(out, P)["LT"] == ["in your possession: 1 day’s limit, not 2 (fish at home don’t count)"]
    assert _lines(out, P)["RB"] == []


def test_rows_34_the_same_number_as_the_page_says_nothing_anywhere(rows34):
    out, P = _frame(LT2)
    assert out["page_possession"] == {"lead": PAGE_LEAD, "also": []}
    assert _lines(out, P)["LT"] == []                   # F0: before, "in your possession: 2 days’ limits" was said here


def test_rows_34_a_trout_and_char_count_leaves_the_page_row(rows34):
    out, P = _frame(BOWRON)
    # the park's count displaces the region's multiplier for every trout and char (the fixture asks about no other
    # fish): nothing is left for the page row; the count is on the fish rows (on the table at the switch). The pike
    # and grayling counts that stay under the page line are pinned over the regs fixture's frames (Teslin, Morley,
    # Bennett Lakes; Zone 7B's grayling "1 daily quota")
    assert out["page_possession"] is None
    assert _lines(out, P)["LT"] == ["in your possession: no more than 1 trout and char in all "
                                    "(fish at home don’t count)"]


LT1_RULE = "r2:f0_lt_one@2-12::f0_lt_one.r1"


def _wire34():
    """The LT1 frame as shipped (rules by index, a year strip on every row), with one also-line planted (the lake
    trout's multiplier, a rule some answer gives the role `possession`), and the rule index."""
    out, P = _frame(LT1)
    keys = sorted(f"{e}::{r}" for e, r in _bundle().rules)
    wire = refs(json.loads(json.dumps(out)), keys.index)
    for r in wire["rows"]:
        r["year"] = [["keep", 365]]
    wire["page_possession"]["also"] = [{"says": "Possession: the daily quota for lake trout. Fish at home don’t "
                                                "count.", "src": keys.index(LT1_RULE)}]
    return wire, keys


def test_rows_34_the_shipped_frame_passes_its_model_and_round_trips(rows34):
    wire, keys = _wire34()
    pp = wire["page_possession"]
    assert pp["lead"]["src"] == keys.index("::".join(PAGE))
    TypeAdapter(RM.RowsFrame34).validate_json(json.dumps(wire))
    sec, refs_ = E.RowsCodec().encode([wire], None, None)
    assert len(sec["frames"][refs_[0]]) == 6
    back = E.RowsCodec().decode_frame(sec, refs_[0], None, None)
    assert back["page_possession"] == pp


def test_mutation_the_rows_34_model_refuses_a_wrong_page_possession(rows34):
    wire, keys = _wire34()
    ad = TypeAdapter(RM.RowsFrame34)

    def refused(fn) -> str:
        w = copy.deepcopy(wire)
        fn(w)
        with pytest.raises(Exception) as e:
            ad.validate_json(json.dumps(w))
        return str(e.value)
    assert "not in its sentence" in refused(lambda w: w["page_possession"]["lead"].update(value="3 days’ quota"))
    assert "role `possession`" in refused(lambda w: w["page_possession"]["lead"].update(src=0))
    assert "a possession role" in refused(lambda w: w["page_possession"]["also"][0].update(src=0))
    assert "names a rule twice" in refused(lambda w: w["page_possession"]["also"][0].update(
        src=w["page_possession"]["lead"]["src"]))
    assert "says nothing" in refused(lambda w: w.update(page_possession={"lead": None, "also": []}))
    assert "page_possession" in refused(lambda w: w.pop("page_possession"))
    with pytest.raises(Exception):                   # a rows-33 frame holds no such field
        TypeAdapter(RM.RowsFrame).validate_json(json.dumps(wire))
    with pytest.raises(C.AnswersError, match="lacks `page_possession`"):
        E.RowsCodec().encode([{k: v for k, v in wire.items() if k != "page_possession"}], None, None)


def test_mutation_without_the_same_number_guard_the_row_repeats_the_page(rows34, monkeypatch):
    # the guard blind (the lead never speaks for the fish): the row repeats the page's own number
    monkeypatch.setattr(CD, "lead_covers", lambda lead, S: False)
    out, P = _frame(LT2)
    assert _lines(out, P)["LT"] == ["in your possession: 2 days’ limits (fish at home don’t count)"]


# ------------------------------------------------- the value words every shipped lead carries (the app's S reads them)

#: the app's `possessionValue` (core/src/regs/standing.ts, deleted by S): the words it cut from the sentence, ported
#: verbatim, so a lead's shipped `value` is shown to be what the page row said before the switch
_APP_VALUE = (re.compile(r"^Possession\s*:\s*([^(.]+?)\s*(?:\(.*?\))?\.?\s*$"),
              re.compile(r"^Possession\s*:\s*([^.]+?)\.\s*.*$"))


def lead_value_problems(frames, rules) -> tuple:
    """Every shipped frame's page lead (`page_possession.lead`, rules by index): its `value` is non-empty, inside its
    sentence, the value words of ITS rule (`display.rule.possession_value`, the one function), and what the app's old
    regex cut from the sentence (so S's swap changes no word). (problems, leads checked)."""
    from pipeline.deliver.answers.display.rule import possession_value
    problems, n = [], 0
    for fid, fr in frames:
        pp = fr.get("page_possession")
        lead = None if pp is None else pp["lead"]
        if lead is None:
            continue
        n += 1
        v, says = lead["value"], lead["says"]
        if not v or not v.strip():
            problems.append(f"frame {fid}: empty value words")
            continue
        if v not in says:
            problems.append(f"frame {fid}: value {v!r} is not in its sentence {says!r}")
        want = possession_value(rules[lead["src"]].f)
        if v != want:
            problems.append(f"frame {fid}: value {v!r}, its rule's value words are {want!r}")
        m = _APP_VALUE[0].match(says) or _APP_VALUE[1].match(says)
        if m is None or m.group(1) != v:
            problems.append(f"frame {fid}: the app's old cut of {says!r} is {m and m.group(1)!r}, the value {v!r}")
    return problems, n


def _shipped_mini():
    """The committed mini release's rows frames as shipped (rows 34: each frame's sixth slot) and its rule table."""
    rel = _Release(under_test.mini())
    frames = [(fid, {"page_possession": fr[5]}) for fid, fr in sorted(rel.frames.items())]
    assert all(len(fr) == 6 for fr in rel.frames.values()), "the committed mini release is not rows 34: re-cut it"
    return frames, rel.rules


def test_every_shipped_lead_has_its_rules_value_words():
    """Contract 2.11 (agreed with the app chat): `page_possession.lead.value` is shipped so S deletes its
    `possessionValue` regex; every lead of the committed mini release carries them, consistent with its sentence."""
    frames, rules = _shipped_mini()
    problems, n = lead_value_problems(frames, rules)
    assert problems == [] and n > 0, (problems[:5], n)


@pytest.mark.parametrize("bad,mark", [
    ("", "empty value words"),
    ("3 days’ quota", "is not in its sentence"),
    ("2 days", "its rule's value words are"),
], ids=["empty", "not-in-sentence", "not-the-rules"])
def test_mutation_a_wrong_value_on_a_shipped_lead_fails(bad, mark):
    frames, rules = _shipped_mini()
    fid, fr = next((fid, fr) for fid, fr in frames if fr["page_possession"] and fr["page_possession"]["lead"])
    planted = [(i, copy.deepcopy(f)) if i == fid else (i, f) for i, f in frames]
    next(f for i, f in planted if i == fid)["page_possession"]["lead"]["value"] = bad
    problems, _n = lead_value_problems(planted, rules)
    assert any(mark in p for p in problems), problems
    # and the rows-34 model refuses the first two on its own (`PossessionLead._value_is_said`)
    if bad != "2 days":
        with pytest.raises(Exception):
            RM.PossessionLead.model_validate({**fr["page_possession"]["lead"], "value": bad})


# ------------------------------------------------------------------------------------------------ (c) the live waters

from pipeline.tests.conftest import BUNDLE_HINT, EXPORT_HINT, need  # noqa: E402

BUNDLE = under_test.path("bundle")
EXPORT_DIR = under_test.path("export_dir")
ISTSI, TAKLA, CUNNINGHAM, NATION, NITINAT = "gnis:10793", "wbk:328961757", "wbk:329537190", "wbk:329424457", \
    "wbk:329504244"
LIVE = (ISTSI, TAKLA, CUNNINGHAM, NATION, NITINAT)
REGION_LEAD = {"says": "Possession: 2 days’ quota. Fish at home don’t count.", "value": "2 days’ quota"}


@pytest.fixture(scope="module")
def live(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    missing = [w for w in LIVE if w not in data["waters"]]
    if missing:
        pytest.fail(f"pinned waters missing from this export: {missing}")
    # (ticket L) with the trout section: each fish's own possession line is on the table's row, not the card's
    return A.build(str(BUNDLE), EXPORT_DIR, workers=1, items=list(LIVE), sections=["rows", "trout"],
                   export=(data, guide), log=lambda *_: None)


def _live(model, item, md=JUL15):
    from pipeline.deliver import calendar as CAL
    k = model.parts[item][0]
    return model.sections["rows"][k][E.segment_index(model.segments[k], CAL.day_of(md))]


def _cell(model, item, md=JUL15):
    from pipeline.deliver import calendar as CAL
    k = model.parts[item][0]
    return model.sections["trout"][k][E.segment_index(model.segments[k], CAL.day_of(md))]


def _table_lines(model, item) -> dict:
    """Each fish's possession words on the trout table (ticket L), in the table's words: its row's line, the band that
    covers it when that band is a possession band, and its quiet line saying what you may have with you; {} where the
    cell holds no table."""
    c = _cell(model, item)
    if c is None:
        return {}
    out: dict = {}
    for b in c["table"]["boxes"]:
        for ri, r in enumerate(b["rows"]):
            got = [x for x in r["lines"] if x.startswith("in your possession")]
            got += [x["text"] for x in b["bands"] if x["kind"] == "possession" and x["from"] <= ri <= x["to"]]
            got += [q for q in r["quiet"] if "with you" in q]
            out.setdefault(r["fish"], [])
            out[r["fish"]] += [x for x in got if x not in out[r["fish"]]]
    return out


def _page(frame) -> dict:
    p = frame["page_possession"]
    return {**p, "lead": None if p["lead"] is None else {k: v for k, v in p["lead"].items() if k != "src"}}


@pytest.mark.needs_bundle
def test_bowron_park_istsi_creek(live):
    f = _live(live, ISTSI)
    assert _page(f) == {"lead": REGION_LEAD, "also": []}       # the park's "1 in all" is on the table, not here
    ln = _table_lines(live, ISTSI)
    assert ln["RB"] == ["in your possession: no more than 1 trout and char in all"]
    assert ln["LT"] == ["in your possession: 1 day’s limit, not 2"]


@pytest.mark.needs_bundle
def test_region_7_lake_trout_takla_lake(live):
    f = _live(live, TAKLA)
    assert _page(f) == {"lead": REGION_LEAD, "also": []}
    ln = _table_lines(live, TAKLA)
    assert ln["LT"] == ln["DV"] == ["in your possession: 1 day’s limit, not 2"]
    assert ln["RB"] == []


@pytest.mark.parametrize("item", [CUNNINGHAM, NATION])
@pytest.mark.needs_bundle
def test_cunningham_and_nation_lakes(live, item):
    f = _live(live, item)
    assert _page(f) == {"lead": REGION_LEAD, "also": []}
    ln = _table_lines(live, item)
    # below the day's number: a possession band and its quiet line (contract Q-D8)
    assert ln["LT"] == ["only 2 in your possession", "3 a day, but never more than 2 lake trout with you, and only 1 of "
                        "them over 50 cm (20 in)"]
    assert ln["DV"] == ["in your possession: 1 day’s limit, not 2"]
    assert ln["RB"] == []


@pytest.mark.needs_bundle
def test_a_water_with_no_possession_rule_nitinat_lake(live):
    f = _live(live, NITINAT)
    assert f["page_possession"] is None and not any(_table_lines(live, NITINAT).values())
