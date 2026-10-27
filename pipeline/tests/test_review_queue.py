"""Curation review phase 4 (T135): the one queue's order, the pre-checks, the ticket export, the
marks commit — each decided in one function, each pinned whole and by mutation.

    runfacts     what the reach run says of a rule (ambiguous cuts, unresolved, unplaced pieces)
    tiers        the risk factors (now with the run's) and THE ORDER (`queue_rank`; entries only, T184)
    precheck     severity A facts / B heuristics; measured over the corpus and the reach run
    tickets      wrong / unsure marks -> ticket text -> the mark's fields again (the round trip)
    commit_marks `git commit --only` of the marks store: nothing else moves

Fixtures are built here (a run's rows, entries, marks, a scratch git repo). The corpus measurement
reads the reach run and the registry (`needs_reach_run`, `needs_atlas`).
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED, GENERATED
from pipeline.regs.review import precheck as P
from pipeline.regs.review import tickets as TK
from pipeline.regs.review import tiers as T
from pipeline.regs.review.marks import Mark
from pipeline.regs.review.runfacts import RunFacts

REPO = Path(__file__).resolve().parents[2]


def _e(eid, rules=None, matched=("gnis:1",), **kw):
    return {"entry_id": eid, "matched": list(matched), "rules": rules or [], **kw}


CLOSE = {"rule_id": "x.r1", "type": "retention_limit", "take": 0, "may_target": 0,
         "extents": [{"op": "whole"}]}
QUOTA = {"rule_id": "x.r2", "type": "retention_limit", "take": 2, "extents": [{"op": "whole"}]}
CUT = {"op": "between", "splits": ["a", "b"]}

AMB = {"also_at": [12419.76], "candidates": [12419.76, 14994.7], "extent": 0,
       "split_id": "a", "toward": "b", "used": 14994.7}
DIAG = [
    {"entry_id": "r1:a@1-1", "kind": "ambiguous_cut", "rule_id": "x.r1", "payload": json.dumps(AMB)},
    {"entry_id": "r1:a@1-1", "kind": "tributaries", "rule_id": "x.r1", "payload": "{}"},
    {"entry_id": "r1:b@1-1", "kind": "unclassified", "rule_id": "x.r2", "payload": json.dumps(
        {"extent": 0, "included": False, "pieces": ["1:0", "2:0"],
         "reasons": {"1:0": "flows_to_the_sea_unjoined", "2:0": "straddles_the_cut"}})},
]
UNRES = [{"entry_id": "r1:c@1-1", "rule_id": "x.r2", "reason": "no_sections_for_items",
          "detail": "no scoped item is in the registry: []"}]


@pytest.fixture
def run():
    return RunFacts.from_rows(DIAG, UNRES, digest="d1")


# --------------------------------------------------------------------------- runfacts

def test_the_run_facts_whole(run):
    assert (dict(run.ambiguous), dict(run.unresolved), dict(run.unclassified), run.digest) == (
        {"r1:a@1-1": {"x.r1": (AMB,)}},
        {"r1:c@1-1": {"x.r2": {"reason": "no_sections_for_items",
                               "detail": "no scoped item is in the registry: []"}}},
        {"r1:b@1-1": {"x.r2": (json.loads(DIAG[2]["payload"]),)}},
        "d1")


def test_a_run_still_being_written_is_refused_never_read_as_empty(tmp_path):
    """No report.json (the last file a run writes): not "no findings" — an error naming the command."""
    for name in ("rule_diagnostic", "rule_unresolved"):
        (tmp_path / f"{name}.jsonl").write_text("")
    with pytest.raises(FileNotFoundError, match="report.json is missing .* pipeline build"):
        RunFacts.read(tmp_path)
    (tmp_path / "report.json").write_text(json.dumps({"digest": "abc"}))
    (tmp_path / "rule_diagnostic.jsonl").write_text(json.dumps(DIAG[0]) + "\n")
    got = RunFacts.read(tmp_path)
    assert (got.digest, dict(got.ambiguous)) == ("abc", {"r1:a@1-1": {"x.r1": (AMB,)}})


def test_every_extent_s_unplaced_pieces_are_kept_and_placed_ones_are_not():
    """T135 adversarial review: the builder writes one `unclassified` diagnostic PER EXTENT, so a
    rule with two cut extents reports two; keeping one per rule (the last) lost the first. And a
    payload with `included: true` was placed — it is not an unplaced piece."""
    two = [dict(DIAG[2], payload=json.dumps({"extent": 0, "included": False, "pieces": ["1:0"],
                                             "reasons": {"1:0": "straddles_the_cut"}})),
           dict(DIAG[2], payload=json.dumps({"extent": 1, "included": False, "pieces": ["2:0", "3:0"],
                                             "reasons": {"2:0": "flows_to_the_sea_unjoined",
                                                         "3:0": "flows_to_the_sea_unjoined"}})),
           dict(DIAG[2], rule_id="x.r9", payload=json.dumps(
               {"extent": 0, "included": True, "pieces": ["9:0"], "reasons": {}}))]
    run2 = RunFacts.from_rows(two, [])
    assert [p["extent"] for p in run2.unclassified_of("r1:b@1-1")["x.r2"]] == [0, 1]
    assert "x.r9" not in run2.unclassified_of("r1:b@1-1")
    got = P.prechecks(_e("r1:b@1-1", [QUOTA, dict(QUOTA, rule_id="x.r9")]), binds=True, run=run2)
    assert [(f["code"], f["rule_id"], f["what"].split(": ", 1)[1]) for f in got] == [
        ("unplaced_pieces", "x.r2", "3 piece(s) — flows_to_the_sea_unjoined, straddles_the_cut")]


# --------------------------------------------------------------------------- the risk factors

def test_every_risk_factor_in_its_order(run):
    every = _e("r1:a@1-1", [dict(CLOSE, extents=[CUT], undrawn_part="near the dam",
                                 unresolved_locators=["x"])], matched=("gnis:1", "gnis:2"))
    run2 = RunFacts.from_rows(DIAG, [dict(UNRES[0], entry_id="r1:a@1-1")])
    assert T.risks(every, binds=False, run=run2) == [
        "no match", "unresolved", "undrawn part", "ambiguous cut", "cut", "several waters", "flag"]
    assert T.RISKS == ("no match", "unresolved", "undrawn part", "ambiguous cut", "cut",
                       "several waters", "flag")
    # without a run, what only the run knows is not claimed
    assert T.risks(every, binds=False) == ["no match", "undrawn part", "cut", "several waters", "flag"]


def test_a_cut_decided_by_hand_is_not_an_ambiguous_cut_risk(run):
    """T135 adversarial review: a cut the extent's `cut_pick` decided (`toward: "curated"`) is a
    hint (pre-check `cut_picked`, B) — so it is not the `ambiguous cut` risk factor either; one
    rule (`runfacts.is_curated_pick`) for both. A rule with one open and one picked cut stays a risk.
    Mutant: `risks` reads `run.ambiguous_of` (every cut) -> this test fails."""
    picked = dict(AMB, toward="curated", reason="curated", pick="upper")
    only_picked = RunFacts.from_rows([dict(DIAG[0], payload=json.dumps(picked))], [])
    e = _e("r1:a@1-1", [dict(CLOSE, extents=[CUT])])
    assert T.risks(e, binds=True, run=only_picked) == ["cut"]
    assert [f["code"] for f in P.prechecks(e, binds=True, run=only_picked)] == ["cut_picked"]
    both = RunFacts.from_rows([dict(DIAG[0], payload=json.dumps(picked)),
                               dict(DIAG[0], payload=json.dumps(dict(AMB, split_id="b")))], [])
    assert T.risks(e, binds=True, run=both) == ["ambiguous cut", "cut"]
    assert dict(both.open_ambiguous_of("r1:a@1-1")) == {"x.r1": (dict(AMB, split_id="b"),)}


def test_the_splits_to_verify_are_one_function_of_splits_json_and_the_marks():
    """`tiers.splits_to_verify`: only `"verify": true` or a stale old mark (owner, 2026-10-26: every
    split was placed by hand, so a `_note` never counts) — the review app (`reuse.splits_to_verify`)
    and the pre-check CLI both ask it."""
    splits = {"waterbodies": [{"splits": [
        {"id": "a", "_note": "[auto-proposal M] x"}, {"id": "b", "_note": "Verify: the bridge", "verify": True},
        {"id": "c", "_note": "[human-review] ok"}, {"id": "d", "_note": ""}]}]}
    assert T.splits_to_verify(splits) == frozenset({"b"})
    assert T.splits_to_verify(splits, {"c": "stale", "d": "verified"}) == frozenset({"b", "c"})


def test_the_run_moves_an_entry_into_a_risk_tier(run):
    """A quota whose rule the run could not bind is P3, not P4; with no run it stays P4."""
    q = _e("r1:c@1-1", [QUOTA])
    assert (T.entry_tier(q, binds=True), T.entry_tier(q, binds=True, run=run)) == ("P4", "P3")
    assert T.risks(q, binds=True, run=run) == ["unresolved"]


def test_the_queue_order_whole(run):
    """THE ONE ORDER: entries only (T184), by tier, then the most urgent risk factor, then the
    more severity-A pre-checks the earlier; the book's order breaks the rest (the caller's)."""
    entries = {
        "plain quota": (_e("r1:p@1-1", [QUOTA]), 0),
        "zone": (_e("z1:z", [QUOTA], matched=()), 0),
        "closure, cut": (_e("r1:k@1-1", [dict(CLOSE, extents=[CUT])]), 0),
        "closure, cut, 3 A": (_e("r1:k3@1-1", [dict(CLOSE, extents=[CUT])]), 3),
        "closure, ambiguous": (_e("r1:a@1-1", [dict(CLOSE, extents=[CUT])]), 0),
        "closure, no match": (_e("r1:n@1-1", [CLOSE], matched=()), 0),
        "closure, undrawn": (_e("r1:u@1-1", [dict(CLOSE, undrawn_part="the dam")]), 0),
        "plain closure": (_e("r1:q@1-1", [CLOSE]), 0),
        "quota, unresolved": (_e("r1:c@1-1", [QUOTA]), 0),
        "pointer": (_e("r1:s@1-1", see=[{"entry_ids": ["r1:p@1-1"]}]), 0),
    }
    ranks = {k: T.queue_rank(e, binds=bool(e["matched"]), run=run, severe=n)
             for k, (e, n) in entries.items()}
    assert sorted(ranks, key=ranks.get) == [
        "closure, no match", "closure, undrawn", "closure, ambiguous", "closure, cut, 3 A",
        "closure, cut", "zone", "plain closure", "quota, unresolved", "plain quota", "pointer"]


def test_the_queue_order_is_pinned_by_mutation(run, monkeypatch):
    """Mutant: `queue_rank` ranks by tier alone (risks ignored) -> `test_the_queue_order_whole`'s
    order fails (no match no longer leads P1)."""
    monkeypatch.setattr(T, "risks", lambda e, *, binds, run=None: ["cut"] if e["entry_id"] != "z1:z" else [])
    with pytest.raises(AssertionError):
        test_the_queue_order_whole(run)


def test_no_split_is_a_review_target():
    """T184 (owner 2026-10-24): the queue is entries only — no S0 tier, no split rank; a split is
    checked by proxy on the map of each entry whose rules name it."""
    assert "S0" not in T.TIERS and T.TIERS == ("P1", "Z", "P2", "P3", "P4", "sweep")
    assert not hasattr(T, "split_rank") and not hasattr(T, "s0_split_ids")


# --------------------------------------------------------------------------- pre-checks

def test_every_precheck_on_one_entry_whole(run):
    e = _e("r1:a@1-1", [
        dict(CLOSE, extents=[CUT], review_reason="dates unclear", unresolved_locators=["the dam"],
             undrawn_part="near the dam", verbatim="No fishing"),
    ], matched=("gnis:9",), regs_verbatim="No fishing; bait ban near the old Hwy 19 bridge pilings",
        licensing=[{"id": "l1", "kind": "designation", "review_reason": "class?"}])
    got = P.prechecks(e, binds=False, run=run, to_verify={"a"},
                      gate_errors=[{"path": "", "msg": "take 15 is not in its verbatim"}])
    assert got == [
        {"code": "gate", "severity": "A", "rule_id": None,
         "what": "fails the gate — it cannot be marked until it is fixed: take 15 is not in its verbatim"},
        {"code": "binds_nothing", "severity": "A", "rule_id": None,
         "what": "the water row's `matched` names no item of this build — it binds nothing: gnis:9"},
        {"code": "ambiguous_cut", "severity": "A", "rule_id": "x.r1",
         "what": "a cut matched several places on its line; the builder took one by ruling E16 — "
                 "check the pick: a at 12,420 m, 14,995 m; took 14,995 m (toward b)"},
        {"code": "split_to_verify", "severity": "A", "rule_id": "x.r1",
         "what": "binds through a split point still to verify (an open auto-proposal or a "
                 "Verify: note) — check its place on the map: a"},
        {"code": "parser_uncertain", "severity": "A", "rule_id": "x.r1",
         "what": "the parser flagged it (`review_reason`): dates unclear"},
        {"code": "parser_uncertain", "severity": "A", "rule_id": "l1",
         "what": "the parser flagged it (`review_reason`): class?"},
        {"code": "unresolved_locator", "severity": "A", "rule_id": "x.r1",
         "what": "names a place nothing draws (`unresolved_locators`): the dam"},
        {"code": "undrawn_part", "severity": "A", "rule_id": "x.r1",
         "what": "applies to a part of its water nothing draws (`undrawn_part`): near the dam"},
        {"code": "unquoted_text", "severity": "B", "rule_id": None,
         "what": "more than 15 characters of the row no rule quotes: 35 characters"},
    ]
    assert P.severe(got) == 8


def test_the_run_prechecks_and_a_curated_pick(run):
    """Unresolved and unplaced pieces are facts; a cut decided by `cut_pick` is only a hint."""
    unres = P.prechecks(_e("r1:c@1-1", [QUOTA]), binds=True, run=run)
    assert [(f["code"], f["rule_id"], f["what"]) for f in unres] == [
        ("unresolved", "x.r2", "the reach run could not bind this rule: no_sections_for_items "
                               "(no scoped item is in the registry: [])")]
    unpl = P.prechecks(_e("r1:b@1-1", [QUOTA]), binds=True, run=run)
    assert [(f["code"], f["what"].split(": ", 1)[1]) for f in unpl] == [
        ("unplaced_pieces", "2 piece(s) — flows_to_the_sea_unjoined, straddles_the_cut")]
    picked = RunFacts.from_rows([dict(DIAG[0], payload=json.dumps(dict(AMB, toward="curated")))], [])
    assert [(f["code"], f["severity"]) for f in P.prechecks(
        _e("r1:a@1-1", [QUOTA]), binds=True, run=picked)] == [("cut_picked", "B")]
    # a zone entry names no water: never "binds nothing"; no run: no run-based finding
    assert P.prechecks(_e("z1:x", [QUOTA], matched=()), binds=False) == []


def _registry_ids():
    reg = json.loads((GENERATED.build() / "registry.json").read_text(encoding="utf-8"))
    return {i["id"] for i in reg["items"]}


def _reach_dir() -> Path:
    return Path(os.environ.get("UI_EXPORT_REACHES") or GENERATED.reaches / GENERATED.build().name)


@pytest.mark.needs_atlas
@pytest.mark.needs_reach_run
def test_the_prechecks_measured_over_the_corpus(request):
    """Measured 2026-10-23 on the reach run 81e726a3e6fbad3b (the corpus at lane-T135): the
    severity-A codes are facts whose counts reproduce PLAN §3's; within the day's drift.
    PLAN §3: 14 rows that bind nothing, 33 unresolved rules, 93 ambiguous cuts on 86 rules, 232
    rules with a review_reason, 32 with unresolved locators. T189: `parser_uncertain` no longer
    repeats the 126 undrawn parts whose review_reason the normaliser wrote (263 -> 137). T207: an
    "on parts" the book never identifies is a note (`unidentified_part`, 39), not `undrawn_part`
    (133 -> 94, counted on the curated corpus 2026-10-26)."""
    from pipeline.tests.conftest import ATLAS_HINT, REACH_HINT, need
    need(request, "atlas", GENERATED.build() / "registry.json", ATLAS_HINT)
    need(request, "reach_run", _reach_dir() / "report.json", REACH_HINT)
    run = RunFacts.read(_reach_dir())
    entries = P._corpus(CURATED.regulations.entries.catalogue)
    splits = json.loads(CURATED.waters.splits.read_text(encoding="utf-8"))
    got = P.corpus_findings(entries, registry_ids=_registry_ids(), run=run, splits=splits)
    from collections import Counter
    n = Counter(f["code"] for fs in got.values() for f in fs)
    # owner 2026-10-26: 13 -> 6 binds_nothing — six item-less area rows are matched by area
    # (`tiers.area_matched`) and Mackenzie Creek (book MU typo 4-21 -> 4-31) matches gnis:12042
    measured = {"binds_nothing": 6, "unresolved": 25, "unplaced_pieces": 2, "ambiguous_cut": 95,
                "split_to_verify": 0, "parser_uncertain": 137, "unresolved_locator": 31,
                "undrawn_part": 94, "unquoted_text": 214,
                # owner 2026-10-26 (drop_on_parts): 33 "on parts" rules hold on the whole water, so 39 -> 6 notes
                "unidentified_part": 6}
    for code, want in measured.items():
        assert abs(n[code] - want) <= max(3, want // 10), (code, n[code], want)
    # owner 2026-10-26: no split is to-verify from its note (was 164); unresolved 29 -> 25 re-pinned to the measured corpus
    assert n["split_to_verify"] == 0
    assert n["quote_not_in_row"] == 0, "the gate refuses a quote not in its row; the strip agrees"
    assert set(n) <= set(P.CHECKS)


@pytest.mark.needs_atlas
@pytest.mark.needs_reach_run
def test_the_tiers_with_the_run_match_plan_section_3(request):
    """PLAN §3: P1 227 (starting with 14 rows that bind nothing, the unresolved closures, 52
    closures with undrawn parts and 32 with ambiguous cuts), P2 261, P3 116, P4 730, sweep 59 —
    measured with the run's factors: P1 226, P2 261, P3 118, P4 728, sweep 60; closures with
    undrawn parts 52 RULES, with ambiguous cuts 32 RULES, exactly PLAN's."""
    from collections import Counter

    from pipeline.atlas.reach.covered import covered_ids
    from pipeline.deliver.bundle.rules import closure_grade
    from pipeline.tests.conftest import ATLAS_HINT, REACH_HINT, need
    need(request, "atlas", GENERATED.build() / "registry.json", ATLAS_HINT)
    need(request, "reach_run", _reach_dir() / "report.json", REACH_HINT)
    run, ids = RunFacts.read(_reach_dir()), _registry_ids()
    entries = P._corpus(CURATED.regulations.entries.catalogue)
    got = Counter(T.entry_tier(e, binds=bool(covered_ids(e, ids)), run=run) for e in entries)
    # Z 132 -> 122: the zone daily-quota merge (6335f73d) into zN:daily_quotas
    # T207: an unidentified "on parts" is no risk — 3 closures P1 -> P2, 13 entries P3 -> P4
    for tier, plan in {"P1": 224, "Z": 122, "P2": 264, "P3": 103, "P4": 743, "sweep": 59}.items():
        assert abs(got[tier] - plan) <= 3, (tier, got[tier], plan)
    closures = [(e, r) for e in entries for r in e.get("rules") or [] if closure_grade(r)]
    undrawn = sum(1 for _, r in closures if r.get("undrawn_part") and not T.unidentified_part(r))
    amb = sum(1 for e, r in closures if r["rule_id"] in run.ambiguous_of(e["entry_id"]))
    # 52 -> 48 closures with undrawn parts (owner 2026-10-26): Cranberry r1 and Fulton r3 are drawn by their new
    # cuts, and drop_on_parts made the pure "on parts" closures whole-water
    assert (abs(undrawn - 48) <= 3, abs(amb - 32) <= 3) == (True, True), (undrawn, amb)


# --------------------------------------------------------------------------- tickets

PRINTS_ENTRY = {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]}
SHA = "a" * 64


def _mark(**kw):
    base = {"scope": "entry", "target": "r1:adam_river_except_eve_river@1-10", "verdict": "wrong",
            "answers": {"T": "y", "R": "n", "P": "y", "A": "y"}, "problem": "quota",
            "note": "the row says 2, we store 4", "reviewer": "Dawson", "date": "2026-10-23",
            "pdf_sha256": SHA, "pages": [14], "prints": PRINTS_ENTRY}
    return Mark.model_validate({**base, **kw})


NOTES = ["", "one line", "two\nlines", "a blank line\n\nbetween", "> starts like a quote",
         "ends with a newline\n", "- **Problem:** looks like a field", "unicode — “quoted” ≥ 50 cm",
         "## a heading inside", "  leading spaces", "trailing spaces  "]


def test_a_ticket_round_trips_every_mark_field():
    """parse_ticket(ticket_text(mark)) == the mark's own fields, over awkward notes, a rule mark,
    a split mark, an unsure mark with no problem, and no pages."""
    marks = [_mark(note=n) if n else _mark(note="", verdict="unsure", answers={"T": "?"}, problem=None)
             for n in NOTES]
    marks.append(_mark(scope="rule", rule_id="adam_river.r1", verdict="unsure", problem=None,
                       answers={"T": "y", "R": "?", "P": "y", "A": "y"},
                       prints={"text": "t", "records": [
                           {"key": "entry", "meaning": "m", "binding": "b"},
                           {"key": "rule:adam_river.r1", "meaning": "m", "binding": "b"}]}))
    marks.append(_mark(scope="split", target="adam_river__eve_river_into_adam_river",
                       answers={"P": "n"}, problem="split misplaced", pages=[],
                       prints={"text": None, "records": [
                           {"key": "split:adam_river__eve_river_into_adam_river", "meaning": "m",
                            "binding": "b"}]}))
    entry = {"entry_id": "r1:adam_river_except_eve_river@1-10", "name": "ADAM RIVER (except Eve River)",
             "source_pages": [14], "regs_verbatim": "Artificial fly only\n- **Rule:** `x`",
             "rules": [{"rule_id": "adam_river.r1", "verbatim": "Artificial fly only"}]}
    for m in marks:
        text = TK.ticket_text(m, entry=entry if m.scope != "split" else None,
                              split={"id": m.target, "kind": "confluence", "waterbody": "Adam River",
                                     "_note": "Verify: - **Scope:** x"} if m.scope == "split" else None,
                              status={"status": "stale", "reasons": ["rule changed"]})
        assert TK.parse_ticket(text) == TK.mark_fields(m), text


def test_the_ticket_text_whole():
    text = TK.ticket_text(_mark(), entry={"entry_id": "r1:adam_river_except_eve_river@1-10",
                                          "name": "ADAM RIVER (except Eve River)", "source_pages": [14],
                                          "regs_verbatim": "Artificial fly only upstream of Eve River",
                                          "rules": [{"rule_id": "adam_river.r1",
                                                     "verbatim": "Artificial fly only"}]})
    assert text == """# Review: wrong — ADAM RIVER (except Eve River)

- **Scope:** entry
- **Target:** `r1:adam_river_except_eve_river@1-10`
- **Verdict:** wrong
- **Answers:** T y · R n · P y · A y
- **Problem:** quota
- **Reviewer:** Dawson
- **Date:** 2026-10-23
- **Printed pages:** 14
- **Book:** sha256 `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`

## What the reviewer said

> the row says 2, we store 4

## The entry

ADAM RIVER (except Eve River) (`r1:adam_river_except_eve_river@1-10`), printed p. 14

The row:

    Artificial fly only upstream of Eve River

Rules:

- `adam_river.r1` — Artificial fly only

## Work

1. Fix R (rules).
2. A reviewed change to the curated files (T185): a proposal the owner accepts (the entry in its region file; a cut pick is the extent's `cut_pick`).
3. Rebuild, then the mark goes stale and the target re-queues for review:
   - `scripts/heavy.sh build 30 -- python -m pipeline build --no-tiles`
"""


def test_only_wrong_and_unsure_marks_in_force_become_tickets():
    ok = _mark(verdict="verified", answers={"T": "y", "R": "y", "P": "y", "A": "y"}, problem=None,
               note="")
    later = _mark(target="r1:b@1-1")
    superseded = _mark(target="r1:c@1-1")
    fixed = _mark(target="r1:c@1-1", verdict="verified", answers={"T": "y", "R": "y", "P": "y", "A": "y"},
                  problem=None, note="")
    unsure = _mark(target="r1:d@1-1", verdict="unsure", answers={"T": "?"}, problem=None)
    got = TK.export([ok, later, superseded, fixed, unsure])
    assert [n for n, _ in got] == ["review-wrong-entry-r1-b-1-1.md", "review-unsure-entry-r1-d-1-1.md"]
    assert [n for n, _ in TK.export([later, unsure], verdicts=("wrong",))] == [
        "review-wrong-entry-r1-b-1-1.md"]


def test_two_marks_never_share_a_ticket_file():
    """`ticket_name` slugs the target, so `r1:a_b@1-1` and `r1:a-b@1-1` both make
    `review-wrong-entry-r1-a-b-1-1.md`; `--out` would write one over the other. Refused instead
    (T135 adversarial review; no two ids collide in the corpus today)."""
    with pytest.raises(ValueError, match="two marks make the ticket file review-wrong-entry-r1-a-b-1-1.md"):
        TK.export([_mark(target="r1:a_b@1-1"), _mark(target="r1:a-b@1-1")])


def test_the_round_trip_is_pinned_by_mutation(monkeypatch):
    """Mutant: the note is written without its quote prefix on continuation lines -> the round
    trip fails (`test_a_ticket_round_trips_every_mark_field`)."""
    monkeypatch.setattr(TK, "_quote", lambda t: [("> " + t) if t else TK._NO_NOTE])
    with pytest.raises((AssertionError, ValueError)):
        test_a_ticket_round_trips_every_mark_field()


def test_a_multi_line_field_is_refused_not_mangled():
    with pytest.raises(ValueError, match="problem is one line"):
        TK.ticket_text(_mark(problem="quota\nand size"))


# --------------------------------------------------------------------------- commit marks

def _git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True,
                          text=True).stdout


def test_commit_marks_commits_the_marks_file_and_nothing_else(tmp_path):
    """Another file staged and another edited: after the commit, HEAD changed only the marks
    store; the staged file is still staged, the edited one still edited."""
    from pipeline.regs.review.commit_marks import commit_marks
    from pipeline.regs.review.marks import line
    repo = tmp_path / "repo"
    store = repo / "data" / "curated" / "regulations" / "review" / "marks.jsonl"
    store.parent.mkdir(parents=True)
    store.write_text("")
    (repo / "other.json").write_text("{}\n")
    (repo / "edited.txt").write_text("a\n")
    _git(tmp_path, "init", "-q", str(repo))
    _git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "add", "-A")
    _git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "base")
    head0 = _git(repo, "rev-parse", "HEAD").strip()
    store.write_text(line(_mark()) + line(_mark(target="r1:b@1-1")))
    (repo / "other.json").write_text('{"staged": true}\n')
    _git(repo, "add", "other.json")
    (repo / "edited.txt").write_text("b\n")
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
           "GIT_COMMITTER_EMAIL": "t@t"}
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        assert commit_marks(repo, store, dry_run=True)["committed"] is False
        assert _git(repo, "rev-parse", "HEAD").strip() == head0
        got = commit_marks(repo, store)
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    assert (got["committed"], got["added"], got["message"]) == (True, 2, "review: 2 marks")
    assert _git(repo, "diff", "--name-only", head0, "HEAD").split() == [
        "data/curated/regulations/review/marks.jsonl"]
    assert _git(repo, "diff", "--cached", "--name-only").split() == ["other.json"]
    assert _git(repo, "diff", "--name-only").split() == ["edited.txt"]
    # nothing left to commit: says so, commits nothing
    assert commit_marks(repo, store)["committed"] is False


def test_commit_marks_commits_a_store_that_lost_lines(tmp_path):
    """T135 adversarial review: "is there anything to commit" was "how many lines were ADDED", so
    a store rewritten with lines removed (and none added) said "nothing to commit: the marks store
    is as HEAD has it" while it was not. Mutant: `store_changed` -> `added_marks(...) > 0` fails."""
    from pipeline.regs.review.commit_marks import commit_marks
    from pipeline.regs.review.marks import line
    repo = tmp_path / "repo"
    store = repo / "marks.jsonl"
    repo.mkdir()
    store.write_text(line(_mark()) + line(_mark(target="r1:b@1-1")))
    _git(tmp_path, "init", "-q", str(repo))
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
           "GIT_COMMITTER_EMAIL": "t@t"}
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        assert commit_marks(repo, store)["added"] == 2
        store.write_text(line(_mark()))                      # one line removed, none added
        got = commit_marks(repo, store)
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    assert (got["committed"], got["changed"], got["added"], got["message"]) == (
        True, True, 0, "review: marks store rewritten (no mark added)")
    assert _git(repo, "show", "HEAD:marks.jsonl") == line(_mark())


def test_commit_marks_refuses_a_store_that_does_not_load(tmp_path):
    from pipeline.regs.review.commit_marks import commit_marks
    store = tmp_path / "marks.jsonl"
    store.write_text("{not a mark}\n")
    _git(tmp_path, "init", "-q")
    with pytest.raises(ValueError, match="not a mark"):
        commit_marks(tmp_path, store)


# --------------------------------------------------------------------------- one function each

def _py_sources():
    for p in sorted((REPO / "pipeline").rglob("*.py")):
        yield p, p.read_text(encoding="utf-8")
    for p in sorted((REPO / "curation-review" / "backend").glob("*.py")):
        yield p, p.read_text(encoding="utf-8")


def test_no_second_copy_of_the_order_the_prechecks_the_tickets_or_the_run_reader():
    hits: dict[str, list] = {k: [] for k in (
        "def queue_rank(", "def prechecks(", "def ticket_text(",
        "def parse_ticket(", "def commit_marks(", "def refusal_words(", "def risks(",
        "def splits_to_verify(", "def is_curated_pick(", "def statuses_now(", "def on_disk(")}
    run_readers = []
    for p, text in _py_sources():
        if p.name.startswith("test_"):
            continue
        rel = str(p.relative_to(REPO))
        for k in hits:
            if k in text:
                hits[k].append(rel)
        # the review side reads the reach run's diagnostics in ONE place
        if rel.startswith(("curation-review/", "pipeline/regs/review/")) and (
                "rule_diagnostic" in text or "rule_unresolved" in text):
            run_readers.append(rel)
    assert hits == {
        "def queue_rank(": ["pipeline/regs/review/tiers.py"],
        "def prechecks(": ["pipeline/regs/review/precheck.py"],
        "def ticket_text(": ["pipeline/regs/review/tickets.py"],
        "def parse_ticket(": ["pipeline/regs/review/tickets.py"],
        "def commit_marks(": ["pipeline/regs/review/commit_marks.py"],
        "def refusal_words(": ["pipeline/regs/review/marks.py"],
        "def risks(": ["pipeline/regs/review/tiers.py"],
        "def splits_to_verify(": ["pipeline/regs/review/tiers.py", "curation-review/backend/reuse.py"],
        "def is_curated_pick(": ["pipeline/regs/review/runfacts.py"],
        "def statuses_now(": ["pipeline/regs/review/tickets.py"],
        "def on_disk(": ["pipeline/regs/review/fingerprint.py"],
    }
    assert run_readers == ["pipeline/regs/review/runfacts.py"]
    # T135 adversarial review: the splits to verify are `tiers.splits_to_verify` (reuse's is the disk
    # read around it), a curated pick `runfacts.is_curated_pick` — never re-derived beside them
    for p, text in _py_sources():
        rel = str(p.relative_to(REPO))
        if p.name.startswith("test_") or not rel.startswith(("curation-review/",
                                                              "pipeline/regs/review/")):
            continue
        if rel != "pipeline/regs/review/runfacts.py":
            assert '"toward") == "curated"' not in text, rel
        if rel != "pipeline/regs/review/tiers.py":
            assert "and split_needs_verify(" not in text and "and T.split_needs_verify(" not in text, rel


def test_the_screen_says_the_no_name_refusal_in_the_server_s_words():
    """The verdict bar refuses a mark with no name before the round trip; it says it in
    `marks.REFUSAL_WORDS`' words, not a second wording (T142 adversarial review)."""
    from pipeline.regs.review.marks import REFUSAL_WORDS
    src = (REPO / "curation-review" / "frontend" / "src" / "components" / "useMarking.tsx").read_text(
        encoding="utf-8")
    assert f'setErrors(["{dict(REFUSAL_WORDS)["a mark needs the reviewer\'s name"]}"])' in src
