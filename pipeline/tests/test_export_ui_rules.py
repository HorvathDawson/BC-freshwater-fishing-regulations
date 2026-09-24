"""The UI data export, checked against the BUNDLE it was built from.

Every check here reads the OUTPUT and compares it with the bundle's own rows, never with the
export's inputs, so a record the export dropped or invented is caught rather than laundered.
The retired-field and registry checks are pinned by mutation: a stale key or a missing word is
injected and the check must go red.

`UI_EXPORT_BUNDLE` points the suite at a side bundle; the default is the shipped one.
"""
from __future__ import annotations

import copy
import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.regs.parsing import catalogue as C
from pipeline.tools import export_ui_rules as X

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)


@pytest.fixture(scope="module")
def doc() -> dict:
    return X.build(BUNDLE)


@pytest.fixture(scope="module")
def db():
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


# ---------------------------------------------------------------------------------------
# Complete: every record once, and what it says is what the bundle says
# ---------------------------------------------------------------------------------------
def test_every_rule_appears_exactly_once_and_reads_as_the_bundle(doc, db):
    rows = {f"{e}::{r}": (lab, verb) for e, r, lab, verb in
            db.execute("SELECT entry_id, rule_id, label, verbatim FROM rule")}
    assert set(doc["rules"]) == set(rows)
    listed = [i for e in doc["entries"].values() for i in e["rules"]]
    assert sorted(listed) == sorted(rows), "each rule is listed under exactly one entry"
    for i, x in doc["rules"].items():
        assert (x["label"], x["verbatim"]) == rows[i], i
        assert x["label"] and x["verbatim"] and x["provenance"]["authority"]


def test_every_rule_carries_its_fields_exactly_as_shipped(doc, db):
    cols = [r[1] for r in db.execute("PRAGMA table_info(rule)")]
    for row in db.execute("SELECT * FROM rule"):
        r = dict(zip(cols, row))
        f = doc["rules"][f"{r['entry_id']}::{r['rule_id']}"]["fields"]
        for k, v in json.loads(r["conditions"] or "{}").items():
            assert f[k] == v, (r["rule_id"], k)
        assert f.get("when") == (json.loads(r["when_"]) if r["when_"] else None)
        assert f.get("while") == (json.loads(r["while_"]) if r["while_"] else None)
        assert f.get("take") == r["take"]
        assert f.get("may_target") == (None if r["may_target"] is None else bool(r["may_target"]))
        assert f.get("exempts") == (json.loads(r["exempts"]) if r["exempts"] else None), \
            (r["rule_id"], "exempts")


def _lifts_lost(doc, db) -> list[str]:
    """Rules whose `exempts` the bundle ships and the export does not carry, exactly."""
    return [f"{e}::{r}" for e, r, x in
            db.execute("SELECT entry_id, rule_id, exempts FROM rule WHERE exempts IS NOT NULL")
            if doc["rules"][f"{e}::{r}"]["fields"].get("exempts") != json.loads(x)]


def test_every_lift_in_the_bundle_reaches_the_export(doc, db):
    """`exempts` left `conditions` for a column of its own. An export that reads only the
    conditions ships a corpus in which nothing lifts anything — the North Thompson closed on
    May 1 beside its own "Exempt from spring closure" — and every other check still passes."""
    n = db.execute("SELECT COUNT(*) FROM rule WHERE exempts IS NOT NULL").fetchone()[0]
    assert n, "the bundle ships no lifts — nothing here would be tested"
    assert _lifts_lost(doc, db) == []
    assert doc["guide"]["exempts"]["rules"] == n
    assert "exempts" in doc["field_dictionary"]["rule.fields"]


def test_the_lift_check_catches_an_export_that_drops_the_column(db, monkeypatch):
    """Mutation pin: the export as it was — `exempts` not among the rule columns."""
    monkeypatch.setattr(X, "_RULE_COLUMNS",
                        tuple(c for c in X._RULE_COLUMNS if c[0] != "exempts"))
    lost = _lifts_lost(X.build(BUNDLE), db)
    assert lost and len(lost) == db.execute(
        "SELECT COUNT(*) FROM rule WHERE exempts IS NOT NULL").fetchone()[0]


def test_a_zone_rule_with_no_extents_of_its_own_binds_to_its_entrys_scope(doc):
    """The bundle ships a rule's OWN extents only. A zone rule with none (typically one the reach
    builder could not place) is scoped by its entry's — the model's "None inherits the entry" —
    and read from its own emptiness it was `water`: 14 region-wide rules left their region's
    standing table."""
    want = {"zone": "region", "area": "area", "province": "region"}
    seen = 0
    for x in doc["rules"].values():
        e = doc["entries"][x["entry_id"]]
        if not x["entry_id"].startswith("z") or x["fields"].get("extents") or not e["extents"]:
            continue
        if any(ex.get("op") != "within" for ex in e["extents"]):
            continue                    # an entry naming a water (Kootenay Lake's boundaries)
        seen += 1
        assert x["provenance"]["binds_to"] == want[e["kind"]], x["id"]
    assert seen, "no zone rule without extents of its own — nothing here was tested"


@pytest.mark.parametrize("table,idcol", [(t, c) for t, c, _ in X._LIC_TABLES])
def test_every_licensing_record_appears_exactly_once_with_its_placement(doc, db, table, idcol):
    cols = [r[1] for r in db.execute(f"PRAGMA table_info({table})")]
    rows = [dict(zip(cols, r)) for r in db.execute(f"SELECT * FROM {table}")]
    got = {i: x for i, x in doc["licensing"].items() if x["kind"] == table}
    assert sorted(got) == sorted(f"{r['entry_id']}#{r[idcol]}" for r in rows)
    for r in rows:
        x = got[f"{r['entry_id']}#{r[idcol]}"]
        assert x["label"] == r["label"] and x["verbatim"] == r["verbatim"]
        record = json.loads(r["record"])
        assert x["fields"] == {k: v for k, v in record.items()
                               if k not in ("kind", "id", "verbatim")}
        want = r["placement"] if "placement" in r else X.NOT_PLACED
        assert x["placement"] == want
        if want == "unresolved":
            assert x["provenance"]["uncertain"] and x["provenance"]["why"], x["id"]
    listed = [i for e in doc["entries"].values() for i in e["licensing"]]
    assert sorted(i for i in listed if i in got) == sorted(got)


def test_every_entry_is_exported(doc, db):
    assert set(doc["entries"]) == {e for (e,) in db.execute("SELECT entry_id FROM entry")}
    assert {e["kind"] for e in doc["entries"].values()} <= {"province", "zone", "area", "water"}


def test_membership_is_the_bundles(doc, db):
    for table, sets, n_table in (("ruleset", "rulesets", "section_ruleset"),
                                 ("licensing_set", "licensing_sets", "section_licensing")):
        idcol = "rule_id" if table == "ruleset" else "record_id"
        sep = "::" if table == "ruleset" else "#"
        want = sorted((str(s), v, f"{e}{sep}{i}") for s, e, i, v in
                      db.execute(f"SELECT set_id, entry_id, {idcol}, via FROM {table}"))
        got = sorted((s, v, i) for s, m in doc[sets].items()
                     for v, ids in m.items() if v != "sections" for i in ids)
        assert got == want
        assert sum(m["sections"] for m in doc[sets].values()) == \
            db.execute(f"SELECT COUNT(*) FROM {n_table}").fetchone()[0]
    for item, w in doc["waters"].items():
        assert sum(w["rulesets"].values()) <= w["sections"], item


def test_the_angler_closures_are_all_there(doc, db):
    want = {f"{e}::{r}" for e, r in
            db.execute("SELECT entry_id, rule_id FROM rule WHERE type = 'angler_closure'")}
    assert want and set(doc["guide"]["angler_closure"]["rules"]) == want
    assert set(doc["index"]["rules_by_type"]["angler_closure"]) == want


def test_counts_are_computed_not_remembered(doc, db):
    c = doc["about"]["counts"]
    assert c["rules"] == db.execute("SELECT COUNT(*) FROM rule").fetchone()[0]
    assert c["entries"] == db.execute("SELECT COUNT(*) FROM entry").fetchone()[0]
    assert c["rulesets"] == db.execute("SELECT COUNT(DISTINCT set_id) FROM section_ruleset"
                                       ).fetchone()[0]


# ---------------------------------------------------------------------------------------
# References
# ---------------------------------------------------------------------------------------
def test_every_reference_the_export_makes_resolves(doc):
    assert X.dangling(doc) == []


def test_every_reference_between_records_resolves(doc):
    """A defect in the CORPUS, not the export — the file ships the list under
    `about.unresolved_references`; this keeps it visible until it is empty."""
    assert X.corpus_references(doc) == []
    assert doc["about"]["unresolved_references"] == []


def test_the_reference_check_catches_a_dangling_id(doc):
    bad = {k: doc[k] for k in ("entries", "rulesets", "licensing_sets", "waters", "index",
                               "guide", "rules", "licensing")}
    bad = dict(bad, rulesets=dict(doc["rulesets"],
                                  extra={"sections": 1, "reach": ["nowhere::x.r1"]}))
    assert X.dangling(bad) == ["ruleset extra -> nowhere::x.r1"]


# ---------------------------------------------------------------------------------------
# No retired field, anywhere — and the check can fail
# ---------------------------------------------------------------------------------------
def test_no_retired_field_appears_anywhere(doc):
    assert X.retired_keys(doc) == []


def _one_rule_doc(doc, mutate):
    rid = next(iter(doc["rules"]))
    x = copy.deepcopy(doc["rules"][rid])
    mutate(x)
    return {"rules": {rid: x}, "licensing": {}, "guide": {}, "field_dictionary": {}}


@pytest.mark.parametrize("name,mutate", [
    ("windows", lambda x: x["fields"].update(windows=[])),
    ("when_open", lambda x: x["fields"].update(when_open=False)),
    ("method on a rule", lambda x: x["fields"].update(method="angling")),
    ("document on a rule", lambda x: x["fields"].update(document="basic_licence")),
    ("required in a clause", lambda x: x["fields"].setdefault("gear", []).append(
        {"slot": "barb", "only": ["barbless"], "required": False})),
    ("band in a length", lambda x: x["fields"].setdefault("lengths", []).append(
        {"min_cm": 50, "band": True})),
    ("a retired type", lambda x: x.update(type="document_required")),
])
def test_the_retired_check_catches_an_injected_key(doc, name, mutate):
    assert X.retired_keys(_one_rule_doc(doc, mutate)), name


def test_a_current_key_that_shares_a_retired_name_is_not_flagged(doc):
    """`method` is retired as a RULE field and current as a gear-clause condition."""
    ok = _one_rule_doc(doc, lambda x: x["fields"].update(
        gear=[{"slot": "bait", "allow": ["dead_fin_fish"], "when": {"method": "set_lining"}}]))
    assert X.retired_keys(ok) == []


def test_the_retired_check_catches_a_licensing_key(doc):
    lid = next(iter(doc["licensing"]))
    x = copy.deepcopy(doc["licensing"][lid])
    x["fields"]["on_retention"] = True
    assert X.retired_keys({"rules": {}, "licensing": {lid: x}})


# ---------------------------------------------------------------------------------------
# The guide explains every registry member, in words the model still has
# ---------------------------------------------------------------------------------------
def test_every_type_kind_slot_and_act_is_explained(doc):
    g = doc["guide"]
    assert set(g["rule_types"]) == {t.value for t in C.RuleType}
    assert set(g["gear"]["slots"]) == {s.value for s in C.Slot}
    assert set(g["gear"]["conduct"]["acts"]) == set(C.CONDUCT_ACTS)
    assert set(g["licensing"]["kinds"]) == set(X._licensing_models())
    assert set(g["licensing"]["who"]["axes"]) == set(C.WHO_AXES)
    for section in (g["rule_types"], g["gear"]["slots"], g["gear"]["conduct"]["acts"],
                    g["licensing"]["kinds"]):
        assert all(v["means"] for v in section.values())
    assert X.unexplained(doc) == []


def test_every_guide_section_in_the_contents_exists(doc):
    g = doc["guide"]
    assert list(g["contents"]) == [k for k in g if k != "contents"]


@pytest.mark.parametrize("table,drop", [("TYPE_TEXT", "angler_closure"), ("SLOT_TEXT", "barb"),
                                        ("CLAUSE_TEXT", "unless"),
                                        ("LICENSING_KIND_TEXT", "exemption")])
def test_a_missing_explanation_is_caught(doc, monkeypatch, table, drop):
    words = dict(getattr(X, table))
    words.pop(drop)
    monkeypatch.setattr(X, table, words)
    assert any(drop in p for p in X.unexplained(doc))


def test_words_for_a_field_the_model_lost_are_caught(doc, monkeypatch):
    monkeypatch.setattr(X, "RULE_FIELD_TEXT", dict(X.RULE_FIELD_TEXT, when_open="stale"))
    assert any("when_open" in p for p in X.unexplained(doc))


def test_the_field_dictionary_is_exactly_what_ships(doc):
    fd = doc["field_dictionary"]
    shipped = {k for x in doc["rules"].values() for k in x["fields"]}
    assert set(fd["rule.fields"]) == shipped
    assert all(fd["rule.fields"].values())
    model = {f.alias or n for n, f in C.CatalogueRule.model_fields.items()}
    assert shipped <= model
    assert not set(fd["model_rule_fields_not_shipped"]) & shipped
    for kind, fields in fd["licensing.fields"].items():
        got = {k for x in doc["licensing"].values() if x["kind"] == kind for k in x["fields"]}
        assert set(fields) == got, kind
        assert all(fields.values()), kind


def test_every_guide_example_is_a_real_record_quoted_exactly(doc):
    found = []

    def walk(o):
        if isinstance(o, dict):
            if {"id", "label", "verbatim"} <= set(o):
                found.append(o)
                return
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(doc["guide"])
    assert len(found) >= 60
    for ex in found:
        x = doc["rules"].get(ex["id"]) or doc["licensing"].get(ex["id"])
        assert x is not None, ex["id"]
        assert (ex["label"], ex["verbatim"]) == (x["label"], x["verbatim"])
        for k, v in (ex.get("fields") or {}).items():
            assert x["fields"][k] == v, (ex["id"], k)


def test_each_concept_has_a_live_example(doc):
    g = doc["guide"]
    for t, v in g["rule_types"].items():
        assert v["examples"] or not v["rules"], t
    for s, v in g["gear"]["slots"].items():
        assert v["examples"] or not v["rules"], s
    for name in ("closed", "release", "size gate"):
        assert g["retention"]["examples"][name], name
    for name in ("quota with a ceiling", "window (keep only between)", "hole (keep none between)"):
        assert g["sizes"]["examples"][name], name
    assert g["gear"]["first_match_per_slot"]["examples"]
    assert g["licensing"]["paths"]["examples"]["accompanied_by"]
    for name in ("default_id", "target"):
        assert g["exempts"]["examples"][name], name


# ---------------------------------------------------------------------------------------
# Deterministic, and refuses a bundle it cannot read whole
# ---------------------------------------------------------------------------------------
def test_the_output_is_deterministic(doc):
    assert X.dumps(X.build(BUNDLE)) == X.dumps(doc)


def test_a_bundle_without_the_columns_is_refused(tmp_path):
    p = tmp_path / "old.sqlite"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE meta (k TEXT, v TEXT)")
    con.execute("CREATE TABLE entry (entry_id TEXT, item_id TEXT)")
    con.execute("CREATE TABLE rule (entry_id TEXT, rule_id TEXT)")
    con.commit()
    con.close()
    with pytest.raises(SystemExit, match="entry.matched"):
        X.build(p)


def test_a_bundle_without_the_exempts_column_is_refused(tmp_path):
    """Lifts in `conditions` and no column: read as-is, every lift would silently vanish."""
    p = tmp_path / "pre-exempts.sqlite"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE meta (k TEXT, v TEXT)")
    con.execute("CREATE TABLE entry (entry_id TEXT, item_id TEXT, matched TEXT)")
    con.execute("CREATE TABLE rule (entry_id TEXT, rule_id TEXT, unresolved TEXT)")
    con.commit()
    con.close()
    with pytest.raises(SystemExit, match="rule.exempts"):
        X.build(p)
