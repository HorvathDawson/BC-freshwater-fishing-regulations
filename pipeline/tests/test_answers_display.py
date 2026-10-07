"""The answers layer's derived display facts (`pipeline.deliver.answers.display`).

TWO KINDS OF CHECK.

1. AGAINST THE PAGE (always run): `fixtures/answers_page_v35.json.gz` holds the consumer page v35's
   own inputs (its 492 rules and 28 waters, reshaped) and what its own functions made of them —
   `kindOf`, `bands`, `sayRule` per rule; `partLabels`, `runsLabel`, `partPlace`, `partLabel`,
   `partKm` per water — computed by running the page's JavaScript in node (agent D, 2026-10-06;
   agent C's `answers/reference` harness runs the whole page the same way). The port must give the
   same strings, so the page can drop its copy.
2. ON THE LIVE BUNDLE (skipped without one): the rule order is the export's, a bundle rule and the
   export's decoded rule give the same facts, every export part maps to exactly one key, and
   "closed all year" is the status index's answer.
"""
from __future__ import annotations

import gzip
import json
import os
from pathlib import Path

import pytest

from pipeline.deliver.answers import display as X

FIXTURE = Path(__file__).parent / "fixtures" / "answers_page_v35.json.gz"


@pytest.fixture(scope="module")
def page():
    return json.loads(gzip.decompress(FIXTURE.read_bytes()))


def _flat(r: dict) -> dict:
    return {**r["fields"], "type": r["type"], "family": r["family"], "dimension": r["dimension"]}


def page_doc(fx: dict):
    """The page's waters reshaped to the decoded export model: each part's [id, via] pairs become
    a rule set of its own (reach / trib)."""
    rules = {k: {**r, "provenance": {"rank": r["rank"]}} for k, r in fx["rules"].items()}
    rulesets, waters = {}, {wid: {"name": n} for wid, n in fx["wnames"].items()}
    ws = {}
    for wi, w in enumerate(fx["waters"]):
        parts = []
        for pi, p in enumerate(w["parts"]):
            sid = f"{wi}.{pi}"
            rulesets[sid] = {"reach": [k for k, v in p["rules"] if v == "reach"],
                             "trib": [k for k, v in p["rules"] if v == "trib"]}
            parts.append({"ruleset": sid, "sections": p["sections"], "runs": p["runs"]})
        ws[w["id"]] = {**w, "parts": parts}
    for k, v in ws.items():
        waters.setdefault(k, v)
    return {"rules": rules, "rulesets": rulesets, "entries": fx["entries"],
            "splits": fx["splits"], "waters": waters}, ws


# --------------------------------------------------------------------------------------------
# 1. The page's own outputs
# --------------------------------------------------------------------------------------------

def test_rule_kind_bands_and_plain_match_the_page(page):
    bad, possession = [], 0
    for k, r in page["rules"].items():
        x, want = _flat(r), page["expect"]["rules"][k]
        got = {"kind": X.kind_of(x), "closed": X.is_closure_gate(x), "bands": X.bands(x),
               "plain": X.plain(x)}
        if x.get("period") == "possession" and x.get("take"):
            # a documented page bug (rows F6): the page says "a day" of a possession limit
            possession += 1
            want = dict(want, plain=want["plain"] and want["plain"].replace(" a day", " in possession")
                        .replace("Keep up to", "Have no more than"))
        if got != want:
            bad.append((k, got, want))
    assert not bad, bad[:3]
    assert possession == 3
    kinds = {v["kind"] for v in page["expect"]["rules"].values()}
    assert {"gear", "pool", "gate", "sizecap", "size", "annual", "duty", "while"} <= kinds
    assert sum(v["plain"] is not None for v in page["expect"]["rules"].values()) > 150


def test_part_labels_match_the_page(page):
    doc, ws = page_doc(page)
    bad = []
    for want in page["expect"]["waters"]:
        W = X.Water(want["id"], ws[want["id"]], doc)
        got = {"labels": X.part_labels(W), "runs": [X.runs_label(doc, p) for p in W.parts],
               "place": [X.part_place(W, i) for i in range(len(W.parts))],
               "hint": [X.part_label(W, i) for i in range(len(W.parts))],
               "km": [X.part_km(p) for p in W.parts]}
        bad += [(want["name"], f, got[f], want[f]) for f in got if got[f] != want[f]]
    assert not bad, bad[:3]
    thompson = next(w for w in page["expect"]["waters"] if w["name"] == "Thompson River")
    assert thompson["place"][0] and len(set(thompson["labels"])) == len(thompson["labels"])


# --------------------------------------------------------------------------------------------
# Rulings the sentences carry (mutation-pinned: each flips with the one field it reads)
# --------------------------------------------------------------------------------------------

def test_trout_includes_char_unless_char_are_excluded():
    """AGENTS 46: "trout" is TROUT_CHAR; with `species_except: [CHAR]` it is trout alone."""
    both = {"type": "retention_limit", "species": ["TROUT_CHAR"], "take": 2}
    assert X.plain(both) == "Keep up to 2 trout and char a day, all kinds together."
    only = dict(both, species_except=["CHAR"])
    assert X.plain(only) == "Keep up to 2 trout a day, all kinds together."
    assert X.plain(dict(only, take=1)) == "Keep up to 1 trout a day."


def test_plain_says_place_origin_size_and_dates():
    x = {"type": "retention_limit", "species": ["TROUT_CHAR"], "take": 2, "water": "stream",
         "origin": "hatchery", "lengths": [{"max_cm": 30, "take": 0}],
         "when": {"dates": [{"from_month": 5, "from_day": 1, "to_month": 10, "to_day": 31}]}}
    assert X.plain(x) == ("In streams, keep up to 2 trout and char a day, all kinds together, "
                          "30 cm or longer, hatchery only, May 1–Oct 31.")
    assert X.plain(dict(x, take=0)) == \
        "In streams, release every hatchery trout and char, May 1–Oct 31."
    shut = {"type": "retention_limit", "species": ["ALL_GAME_FISH"], "take": 0, "may_target": 0}
    assert X.plain(shut) == "No fishing."
    assert X.rule_facts(shut) == {"kind": "gate", "closure": True, "plain": "No fishing."}
    assert "closure" not in X.rule_facts(dict(shut, may_target=1))


def test_bands_first_range_wins_and_uncovered_is_zero_on_a_pool():
    pool = {"type": "retention_limit", "take": 4,
            "lengths": [{"min_cm": 50, "take": 1}, {"max_cm": 30, "take": 0},
                        {"min_cm": 20, "max_cm": 60}]}
    assert X.bands(pool) == [[0, 30, 0], [30, 50, 4], [50, None, 1]]
    # a daily pool keeps only the ranges it names: 30-50 cm uncovered takes 0
    gap = dict(pool, lengths=pool["lengths"][:2])
    assert X.bands(gap) == [[0, 50, 0], [50, None, 1]]
    clause = dict(pool, within={"rule_id": "x"}, lengths=[{"min_cm": 50, "take": 1}])
    assert X.kind_of(clause) == "sizecap"
    assert X.bands(clause) == [[0, 50, None], [50, None, 1]]


def test_steelhead_line_codes():
    assert X.steelhead_line(None, True) is None
    assert X.steelhead_line("possible", True) == "possible_with_rules"
    assert X.steelhead_line("possible", False) is None
    assert X.steelhead_line("known", True) == "known_with_rules"
    assert X.steelhead_line("known", False) == "known_no_rules"


# --------------------------------------------------------------------------------------------
# 2. The live bundle
# --------------------------------------------------------------------------------------------

def _bundle():
    from pipeline.deliver.answers import common
    p = common.bundle_path()
    if not Path(p).exists():
        pytest.skip(f"no bundle at {p}")
    return common.load(p)


def _export(B):
    from pipeline.deliver.answers import common
    from pipeline.tools import export_codec
    regs = Path(os.environ.get("ANSWERS_EXPORT_DIR") or os.environ.get("UI_EXPORT_DIR")
                or Path(B.path).parents[1] / "regs")
    if not (regs / "ui-rules-export.json").exists():
        pytest.skip(f"no export at {regs}")
    data, guide = common.load_export(regs)
    try:
        common.check_export(B, data, guide)
    except common.AnswersError as e:
        pytest.skip(str(e))
    return data, guide, export_codec.expand(data, guide)


@pytest.fixture(scope="module")
def live():
    B = _bundle()
    data, guide, doc = _export(B)
    return B, doc, data


def test_rule_order_is_the_exports(live):
    B, doc, _ = live
    assert [f"{e}::{r}" for e, r in sorted(B.index, key=B.index.__getitem__)] == list(doc["rules"])


def test_a_bundle_rule_and_the_exports_rule_say_the_same(live):
    """The facts are computed from the bundle; the page reads the export's fields. They must give
    the same kind, bands and sentence for every rule."""
    B, doc, _ = live
    facts = X.build_rules(B)
    bad = []
    for i, (k, x) in enumerate(doc["rules"].items()):
        flat = {**(x.get("fields") or {}), "type": x["type"], "family": x["family"],
                "dimension": x.get("dimension")}
        if X.rule_facts(flat) != facts[i]:
            bad.append((k, X.rule_facts(flat), facts[i]))
    assert not bad, bad[:3]
    assert X.build_rules(B) == facts                                    # deterministic


def test_every_export_part_has_one_key_and_closed_all_year_is_the_status_index(live):
    from pipeline.deliver import status_index as SI
    from pipeline.deliver.answers import common
    B, doc, data = live
    keys, parts = common.part_keys(B, data)
    n = sum(1 for w in doc["waters"].values() for p in w["parts"] if p.get("ruleset") is not None)
    assert sum(1 for ks in parts.values() for k in ks if k is not None) == n
    rule_keys = sorted({common.rule_key(k) for k in keys})
    closed = [k for k in rule_keys if X.closed_all_year(B, k)]
    assert closed, "some rule set is closed all year (e.g. a water's 'No fishing' row)"
    sample = closed[:5] + rule_keys[:: max(1, len(rule_keys) // 25)]
    for k in sample:                       # the quick refusals change no answer
        prof = SI.set_profile(B.sets[k.set_id], k.steelhead_water, B.path, k.steelhead_rules)
        assert X.closed_all_year(B, k) == all(c == SI.CLOSED for c in prof), k
