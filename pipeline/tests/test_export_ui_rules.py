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


def test_a_zone_rule_is_scoped_by_what_it_states_itself(doc):
    """No rule inherits its entry's extents. A zone rule with extents of its own is scoped by them
    (a region table, carve-out or not, is `region`); one with none names its place in words
    (`extent_text`) and is scoped to that place (`water`) — "Main Body of Kootenay Lake"."""
    want = {"zone": "region", "area": "area", "province": "region"}
    own = named = 0
    for x in doc["rules"].values():
        if not x["entry_id"].startswith("z"):
            continue
        e = doc["entries"][x["entry_id"]]
        ext = x["fields"].get("extents")
        if ext and all(ex.get("op") == "within" and str(ex.get("area_id") or "")
                       .startswith("area:region:") for ex in ext):
            own += 1
            assert x["provenance"]["binds_to"] == want[e["kind"]], x["id"]
        elif not ext:
            named += 1
            assert x["fields"].get("extent_text"), x["id"]
            assert x["provenance"]["binds_to"] == "water", x["id"]
    assert own and named, "nothing here was tested"

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
        assert sum(p["sections"] for p in w["parts"]) == w["sections"], item


# ---------------------------------------------------------------------------------------
# A water's parts: the (ruleset, licensing set) pairs its sections carry TOGETHER
# ---------------------------------------------------------------------------------------
def test_a_water_s_parts_are_the_bundle_s_own_pairing(doc, db):
    """The Dean's eight (ruleset, licensing set) combinations used to read as five rule sets
    beside four licensing sets; which Class I unit went with which closure was lost."""
    want: dict = {}
    for item, rs, ls, n in db.execute(
            "SELECT i.item_id, r.set_id, l.set_id, COUNT(*) FROM item i "
            "JOIN item_section s ON s.ord = i.ord "
            "LEFT JOIN section_ruleset r ON r.sid = s.sid "
            "LEFT JOIN section_licensing l ON l.sid = s.sid GROUP BY 1, 2, 3"):
        want.setdefault(item, set()).add((None if rs is None else str(rs),
                                          None if ls is None else str(ls), n))
    for item, w in doc["waters"].items():
        got = {(p["ruleset"], p["licensing_set"], p["sections"]) for p in w["parts"]}
        assert got == want[item], item
        pairs = [(p["ruleset"], p["licensing_set"]) for p in w["parts"]]
        assert len(pairs) == len(set(pairs)), f"{item}: a pair appears twice"
        assert "rulesets" not in w and "licensing_sets" not in w, "one encoding, not two"


def test_a_licensing_set_with_no_rule_set_survives_as_null(doc, db):
    n = db.execute("SELECT COUNT(*) FROM section_licensing l LEFT JOIN section_ruleset r "
                   "ON r.sid = l.sid JOIN item_section s ON s.sid = l.sid "
                   "WHERE r.sid IS NULL").fetchone()[0]
    got = sum(p["sections"] for w in doc["waters"].values() for p in w["parts"]
              if p["ruleset"] is None and p["licensing_set"] is not None)
    assert got == n


# ---------------------------------------------------------------------------------------
# Water outside B.C.
# ---------------------------------------------------------------------------------------
def test_water_outside_bc_is_counted_and_carries_no_set(doc, db):
    want = dict(db.execute("SELECT i.item_id, COUNT(*) FROM item i JOIN item_section s "
                           "ON s.ord = i.ord JOIN outside_bc o ON o.sid = s.sid GROUP BY 1"))
    assert want, "the bundle lists no section outside B.C."
    for item, w in doc["waters"].items():
        assert w["outside_bc"] == want.get(item, 0), item
        free = sum(p["sections"] for p in w["parts"]
                   if p["ruleset"] is None and p["licensing_set"] is None)
        assert w["outside_bc"] <= free, f"{item}: a section outside B.C. carries a set"
    assert db.execute("SELECT COUNT(*) FROM outside_bc o JOIN section_ruleset r "
                      "ON r.sid = o.sid").fetchone()[0] == 0
    assert doc["guide"]["placement"]["outside_bc"]


# ---------------------------------------------------------------------------------------
# Where a rule is: never the whole water beside a part in words; labels are not list items
# ---------------------------------------------------------------------------------------
def test_no_rule_binds_the_whole_water_beside_a_part_in_words(doc):
    bad = [i for i, x in doc["rules"].items()
           if x["fields"].get("extents") == [{"op": "whole"}] and x["fields"].get("extent_text")]
    assert bad == []


def test_no_label_starts_with_a_list_marker(doc):
    import re
    marker = re.compile(r"^\s*(\d{1,2}[.)]|\([a-z0-9ivx]{1,3}\)|[•–-]\s)")
    bad = [i for i, x in {**doc["rules"], **doc["licensing"]}.items()
           if marker.match(x["label"])]
    assert bad == []


def test_a_bound_rule_on_a_cut_point_names_its_place(doc):
    """226 labels read exactly "No fishing" when a cut-point never reached the label."""
    from pipeline.tools.export_ui_rules import _f
    named = [x for x in doc["rules"].values() if not x["provenance"]["uncertain"]
             and any(e.get("splits") for e in _f(x).get("extents") or [])]
    assert named
    bare = [x["id"] for x in named if " — " not in x["label"] and x["type"] not in (
        "advisory", "hazard", "program_membership", "facility", "navigation_duty")
            and not x["label"] == x["verbatim"]]
    # a cut-point with no book name names no place (logged by the bundle build) — never many
    assert len(bare) <= 40, bare[:20]


# ---------------------------------------------------------------------------------------
# The guide: `while` is two kinds of token; the methods reading rule; fly only is two laws
# ---------------------------------------------------------------------------------------
def test_the_while_groups_partition_the_validator_s_vocabulary(doc):
    w = doc["guide"]["gear"]["while"]
    means, devices = set(w["means_of_fishing"]["tokens"]), set(w["devices"]["tokens"])
    assert means | devices == set(C.WHILE_TOKENS) and not (means & devices)
    assert "downrigger" in devices and "angling" in means


def test_the_province_allows_angling_and_ice_fishing(doc):
    """The book grants both in its Allowable Fishing Methods; the corpus stored only their counts,
    so a consumer read angling as a method no rule allows."""
    m = doc["guide"]["gear"]["methods"]
    assert {"angling", "ice_fishing", "spear_fishing", "crayfish_trapping"} <= \
        set(m["allowed_by_the_province"])
    assert "All other methods of taking fin fish and crayfish are illegal." in m["book"]["text"]


def test_the_two_fly_only_laws_each_name_the_other(doc):
    slots = doc["guide"]["gear"]["slots"]
    assert slots["lure"]["definitions"]["differs_from"] == "method"
    assert slots["method"]["definitions"]["differs_from"] == "lure"
    assert "floats and sinkers may be attached" in slots["lure"]["definitions"]["book"]
    assert "may not be attached" in slots["method"]["definitions"]["book"]


def test_an_exemption_releases_the_duties_that_presume_its_documents(doc):
    assert any("presumes" in line for line in doc["guide"]["licensing"]["rules_of_reading"])
    assert doc["licensing"]["zp:licence_administration#produce_licence"]["fields"]["presumes"] \
        == ["basic_licence"]


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
    # a water's part naming a set the file does not have
    item = next(iter(doc["waters"]))
    w = dict(doc["waters"][item], parts=[{"ruleset": "99999999", "licensing_set": None,
                                          "sections": 1}])
    bad = dict(bad, rulesets=doc["rulesets"], waters=dict(doc["waters"], **{item: w}))
    assert X.dangling(bad) == [f"water {item} -> ruleset 99999999"]


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
    for name in ("whole", "in part"):
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


def test_no_two_rules_of_an_entry_share_a_label_unless_they_say_the_same(doc):
    """Eighteen pairs of rules in one entry read the same "No fishing" over different water.
    A shared label is allowed only where the label IS the printed sentence, or the two rules differ
    only in which parent quota they count inside (`within`, `condition_of`)."""
    from collections import defaultdict
    by = defaultdict(list)
    for x in doc["rules"].values():
        by[(x["entry_id"], x["label"])].append(x)
    ignore = {"within", "condition_of"}
    bad = []
    for (eid, lab), xs in by.items():
        if len(xs) < 2 or all(x["label"] == x["verbatim"] for x in xs):
            continue
        shapes = {json.dumps({k: v for k, v in x["fields"].items() if k not in ignore},
                             sort_keys=True) for x in xs}
        if len(shapes) > 1:
            bad.append((eid, lab, [x["rule_id"] for x in xs]))
    assert bad == [], bad[:10]


def test_a_lake_cut_into_parts_is_read_through_its_parts(doc):
    """Kootenay Lake's own entry is the leftover of the cut (one section, no entry); the guide says
    so and names every such whole, each with the parts that point at it."""
    pl = doc["guide"]["placement"]
    assert "part_of" in pl["part_of"]
    wholes = pl["lakes_cut_into_parts"]
    assert wholes
    for whole in wholes:
        assert whole in doc["waters"], whole
        parts = [k for k, w in doc["waters"].items() if w.get("part_of") == whole]
        assert len(parts) >= 2, (whole, parts)
