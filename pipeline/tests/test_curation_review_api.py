"""The curation-review app curates the CURRENT catalogue model, end to end, through its HTTP API.

Runs the FastAPI app (curation-review/backend) with `TestClient` against a TEMP COPY of the
catalogue region files (`CURATION_ENTRIES_DIR`) — never the real ones. For one entry of every rule
type and every licensing kind it proves:

  GET        the entry is served with a generated `label` on every rule and record, the same
             string `catalogue.label` / `catalogue.licensing_label` produce;
  PUT as-is  saving the served entry unchanged is no semantic change (and no byte change);
  PUT edit   an edit to each editor's field persists, re-validates, and shows in the label;
  PUT bad    an invalid edit is refused (422) with an error addressed to the field, and the file
             is untouched.

Plus: `/api/check` answers labels and errors for a draft without writing; `/api/vocab` is read off
the model (not a list that can drift); and `matched` is the only source of what an entry covers.
"""

from __future__ import annotations

import copy
import json
import shutil
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED, GENERATED
from pipeline.regs.parsing import catalogue as C

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
REAL = CURATED.regulations.entries.catalogue

pytestmark = pytest.mark.skipif(
    not (GENERATED.build() / "registry.json").exists(),
    reason="needs the built registry the review app serves (data/generated/atlas/full)")


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("catalogue")
    for p in sorted(REAL.glob("region-*.json")):
        shutil.copy2(p, tmp / p.name)
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_ENTRIES_DIR", str(tmp))
    mp.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "model_api", "rebuild"):
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    import reuse                                              # noqa: E402
    assert reuse.ENTRIES_DIR == tmp, "the app must be pointed at the temp copy, not the real files"
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "dir": tmp, "reuse": reuse}
    mp.undo()


def _entries(d: Path) -> dict[str, tuple[str, dict]]:
    out = {}
    for p in sorted(d.glob("region-*.json")):
        region = p.stem.split("region-", 1)[1]
        for e in json.loads(p.read_text(encoding="utf-8"))["entries"]:
            out[e["entry_id"]] = (region, e)
    return out


def _smallest(d: Path) -> dict[str, str]:
    """The smallest entry carrying each rule type and each licensing kind."""
    best: dict[str, tuple[int, str]] = {}
    for eid, (_, e) in _entries(d).items():
        size = len(json.dumps(e))
        keys = [f"rule:{r['type']}" for r in e.get("rules") or []]
        keys += [f"licensing:{x['kind']}" for x in e.get("licensing") or []]
        for k in keys:
            if k not in best or size < best[k][0]:
                best[k] = (size, eid)
    return {k: v[1] for k, v in best.items()}


ALL_PARTS = [f"rule:{t.value}" for t in C.RuleType] + [
    f"licensing:{k}" for k in ("designation", "not_classified", "requirement", "licence_terms",
                               "exemption", "alternative")]


@pytest.fixture(scope="module")
def picks(env):
    got = _smallest(env["dir"])
    missing = [k for k in ALL_PARTS if k not in got]
    assert not missing, f"the corpus carries no entry of {missing} — nothing to curate them on"
    # the smallest entry whose FIRST angler_closure names a residency in its own sentence
    best = None
    for eid, (_, e) in _entries(env["dir"]).items():
        first = next((r for r in e.get("rules") or [] if r["type"] == "angler_closure"), None)
        if first and C.residency_said(first["verbatim"]):
            size = len(json.dumps(e))
            if best is None or size < best[0]:
                best = (size, eid)
    assert best, "no angler_closure names a residency"
    got["rule:angler_closure:residency"] = best[1]
    return got


def _file_of(env, eid) -> Path:
    region, _ = _entries(env["dir"])[eid]
    return env["dir"] / f"region-{region}.json"


def _get(env, eid) -> dict:
    r = env["client"].get(f"/api/entries/{eid}")
    assert r.status_code == 200, r.text
    return r.json()


def _put(env, eid, region, entry):
    return env["client"].put(f"/api/entries/{eid}", json={"region": region, "entry": entry})


# --------------------------------------------------------------------------- #
# GET: every rule and record carries the generated label
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("part", ALL_PARTS)
def test_get_serves_the_generated_label(env, picks, part):
    eid = picks[part]
    served = _get(env, eid)["entry"]
    stored = _entries(env["dir"])[eid][1]
    model = C.CatalogueEntry.model_validate(stored)
    siblings = {r.rule_id: r for r in model.rules}
    # THE BUNDLE'S LABEL: the same function, handed the same place-namer the bundle uses.
    place_of = env["reuse"]._place_namer().for_entry(model.matched)
    # and the corpus, so a lift of another entry's rule names it in words, as the bundle's does
    entries = sys.modules["model_api"].corpus_entries(env["dir"])
    for got, r in zip(served.get("rules") or [], model.rules):
        assert got["label"] == C.label(r, siblings, place_of, entries)
    for got, x in zip(served.get("licensing") or [], model.licensing):
        assert got["label"], f"{x.kind} {x.id} was served with no label"
        assert got["label"] == C.licensing_label(
            x, siblings, units=_units(env), refs=_refs(env, eid, model))


def _units(env) -> dict:
    out = {}
    for _eid, (_, e) in _entries(env["dir"]).items():
        for x in C.CatalogueEntry.model_validate(e).licensing:
            if isinstance(x, C.Designation):
                out.setdefault(x.unit, x.unit_name)
    return out


def _refs(env, eid, model) -> dict:
    out = {}
    for other, (_, e) in _entries(env["dir"]).items():
        for x in C.CatalogueEntry.model_validate(e).licensing:
            out[(other, x.id)] = x
    for x in model.licensing:
        out[(eid, x.id)] = x
    return out


# --------------------------------------------------------------------------- #
# PUT unchanged: no semantic change, no byte change
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("part", ALL_PARTS)
def test_put_unchanged_changes_nothing(env, picks, part):
    eid = picks[part]
    d = _get(env, eid)
    path = _file_of(env, eid)
    before_bytes = path.read_bytes()
    before = _entries(env["dir"])[eid][1]
    r = _put(env, eid, d["region"], d["entry"])            # served as-is, labels and all
    assert r.status_code == 200, r.text
    after = _entries(env["dir"])[eid][1]
    assert C.CatalogueEntry.model_validate(after) == C.CatalogueEntry.model_validate(before)
    assert after == before
    assert path.read_bytes() == before_bytes, "an unchanged save rewrote the file"


# --------------------------------------------------------------------------- #
# PUT an edit to each editor's field
# --------------------------------------------------------------------------- #

def _rule(e, rtype):
    return next(i for i, r in enumerate(e["rules"]) if r["type"] == rtype)


def _rec(e, kind):
    return next(i for i, x in enumerate(e["licensing"]) if x["kind"] == kind)


def _edit_retention(e):
    # Numbers are held to the rule's own sentence by the ingest gate the app now runs, so the
    # edit changes what a number does not carry: the season, and the retention record.
    i = _rule(e, "retention_limit")
    r = e["rules"][i]
    r["record_retention"] = True
    r["when"] = {"dates": [{"from_month": 6, "from_day": 15, "to_month": 10, "to_day": 31}]}
    return lambda got: (got["rules"][i].get("record_retention") is True
                        and got["rules"][i]["when"] == r["when"]), f"rules.{i}"


def _edit_gear(rtype):
    def edit(e):
        i = _rule(e, rtype)
        r = e["rules"][i]
        r["gear"] = [{"slot": "hooks_per_line", "max": 1, "when": {"water": "stream"}}] + \
            list(r.get("gear") or []) + [{"slot": "points_per_hook", "max": 1}]
        return lambda got: got["rules"][i]["gear"] == r["gear"], f"rules.{i}"
    return edit


def _edit_conduct(e):
    i = _rule(e, "handling_rule")
    e["rules"][i]["conduct"] = list(e["rules"][i].get("conduct") or []) + ["release_immediately"]
    return lambda got: "release_immediately" in got["rules"][i]["conduct"], f"rules.{i}"


def _edit_while(e):
    i = _rule(e, "method_rule")
    e["rules"][i]["while"] = ["ice_fishing"]
    return lambda got: got["rules"][i]["while"] == ["ice_fishing"], f"rules.{i}"


def _edit_vessel(e):
    i = _rule(e, "vessel_rule")
    r = e["rules"][i]
    r.update({"aspect": "propulsion", "level": "none"})
    r.pop("max_power_kw", None)
    return lambda got: got["rules"][i]["level"] == "none", f"rules.{i}"


def _edit_angler_closure(e):
    i = _rule(e, "angler_closure")
    e["rules"][i]["when"] = {"weekdays": ["Saturday"]}
    return lambda got: got["rules"][i]["when"] == {"weekdays": ["Saturday"]}, f"rules.{i}"


def _edit_where(rtype):
    def edit(e):
        i = _rule(e, rtype)
        r = e["rules"][i]
        r["tributaries_only"] = True
        r["includes_tributaries"] = True
        # A place nothing can draw keeps its words and NO extents (the model refuses a bare
        # `whole` beside `extent_text`).
        r.pop("extents", None)
        r["extent_text"] = "at the old bridge"
        r["unresolved_locators"] = ["the old bridge"]
        r["review_reason"] = "curator: locate the old bridge"
        r["exempts"] = [{"default_id": "bait_ban_streams", "note": "curator edit"}]
        return lambda got: (got["rules"][i]["unresolved_locators"] == ["the old bridge"]
                            and got["rules"][i]["exempts"] == r["exempts"]), f"rules.{i}"
    return edit


def _edit_review(rtype):
    def edit(e):
        i = _rule(e, rtype)
        e["rules"][i]["review_reason"] = "curator edit"
        e["rules"][i]["obligation"] = "should"
        return lambda got: got["rules"][i]["review_reason"] == "curator edit", f"rules.{i}"
    return edit


def _edit_stop(e):
    i = _rule(e, "stop_fishing_after_quota")
    e["rules"][i]["species"] = ["ST"]
    return lambda got: got["rules"][i]["species"] == ["ST"], f"rules.{i}"


def _edit_designation(e):
    j = _rec(e, "designation")
    x = e["licensing"][j]
    x["unit_name"] = x["unit_name"] + " (edited)"
    x["review_reason"] = "curator edit"
    return lambda got: got["licensing"][j]["unit_name"] == x["unit_name"], f"licensing.{j}"


def _edit_requirement(e):
    j = _rec(e, "requirement")
    x = e["licensing"][j]
    x["water"] = "lake"
    x["when"] = {"weekdays": ["Monday"]}
    return lambda got: got["licensing"][j]["water"] == "lake", f"licensing.{j}"


def _edit_terms(e):
    j = _rec(e, "licence_terms")
    x = e["licensing"][j]
    x["allocation"] = "open"
    return lambda got: got["licensing"][j]["allocation"] == "open", f"licensing.{j}"


def _edit_record_review(kind):
    def edit(e):
        j = _rec(e, kind)
        e["licensing"][j]["review_reason"] = "curator edit"
        return lambda got: got["licensing"][j]["review_reason"] == "curator edit", f"licensing.{j}"
    return edit


def _edit_alternative(e):
    j = _rec(e, "alternative")
    x = e["licensing"][j]
    x["satisfied_by"] = [{"hold": ["yukon_angling_licence", "basic_licence"]}]
    return lambda got: got["licensing"][j]["satisfied_by"] == x["satisfied_by"], f"licensing.{j}"


EDITS = {
    "rule:retention_limit": _edit_retention,
    "rule:stop_fishing_after_quota": _edit_stop,
    "rule:bait_restriction": _edit_gear("bait_restriction"),
    "rule:tackle_restriction": _edit_gear("tackle_restriction"),
    "rule:method_rule": _edit_while,
    "rule:vessel_rule": _edit_vessel,
    "rule:navigation_duty": _edit_review("navigation_duty"),
    "rule:angler_closure": _edit_angler_closure,
    "rule:handling_rule": _edit_conduct,
    "rule:hazard": _edit_where("hazard"),
    "rule:advisory": _edit_review("advisory"),
    "rule:program_membership": _edit_review("program_membership"),
    "rule:facility": _edit_review("facility"),
    "licensing:designation": _edit_designation,
    "licensing:not_classified": _edit_record_review("not_classified"),
    "licensing:requirement": _edit_requirement,
    "licensing:licence_terms": _edit_terms,
    "licensing:exemption": _edit_record_review("exemption"),
    "licensing:alternative": _edit_alternative,
}


def test_every_part_has_an_edit():
    assert sorted(EDITS) == sorted(ALL_PARTS)


@pytest.mark.parametrize("part", ALL_PARTS)
def test_put_edit_persists_and_revalidates(env, picks, part):
    eid = picks[part]
    d = _get(env, eid)
    entry = copy.deepcopy(d["entry"])
    ok, where = EDITS[part](entry)
    # the draft check answers first, without writing
    before_bytes = _file_of(env, eid).read_bytes()
    chk = env["client"].post("/api/check", json={"region": d["region"], "entry": entry}).json()
    assert chk["ok"], chk["errors"]
    assert _file_of(env, eid).read_bytes() == before_bytes, "/api/check wrote the file"
    r = _put(env, eid, d["region"], entry)
    assert r.status_code == 200, r.text
    got = _entries(env["dir"])[eid][1]
    assert ok(got), f"the edit to {where} did not persist"
    C.CatalogueEntry.model_validate(got)                      # it is the model's shape on disk
    # the served label is the label of what was saved, and the check predicted it
    served = _get(env, eid)["entry"]
    kind, idx = where.split(".")
    assert served[kind][int(idx)]["label"] == chk["labels"][kind][int(idx)]


def test_edit_changes_the_label_live(env, picks):
    eid = picks["rule:retention_limit"]
    d = _get(env, eid)
    entry = copy.deepcopy(d["entry"])
    i = _rule(entry, "retention_limit")
    old = entry["rules"][i]["label"]
    entry["rules"][i]["take"] = 17
    chk = env["client"].post("/api/check", json={"region": d["region"], "entry": entry}).json()
    assert "17" in chk["labels"]["rules"][i] and chk["labels"]["rules"][i] != old


def test_entry_fields_edit(env, picks):
    eid = picks["rule:tackle_restriction"]
    d = _get(env, eid)
    entry = copy.deepcopy(d["entry"])
    entry["scope_note"] = "curator note"
    entry["includes_tributaries"] = False
    entry["extents"] = [{"op": "whole"}]
    r = _put(env, eid, d["region"], entry)
    assert r.status_code == 200, r.text
    got = _entries(env["dir"])[eid][1]
    assert got["scope_note"] == "curator note" and got["includes_tributaries"] is False


# --------------------------------------------------------------------------- #
# PUT invalid: refused, addressed to the field, file untouched
# --------------------------------------------------------------------------- #

def _bad_rule(rtype, mutate, path_suffix, says):
    def make(e):
        i = _rule(e, rtype)
        mutate(e["rules"][i])
        return f"rules.{i}{path_suffix}", says
    return make


def _bad_rec(kind, mutate, path_suffix, says):
    def make(e):
        j = _rec(e, kind)
        mutate(e["licensing"][j])
        return f"licensing.{j}{path_suffix}", says
    return make


BAD = {
    "negative take": ("rule:retention_limit",
                      _bad_rule("retention_limit", lambda r: r.update(take=-1), "", "take")),
    "take 0 without may_target": ("rule:retention_limit",
                                  _bad_rule("retention_limit", lambda r: r.update(take=0), "",
                                            "may_target")),
    "empty ban": ("rule:bait_restriction",
                  _bad_rule("bait_restriction", lambda r: r.update(gear=[{"slot": "bait", "ban": []}]),
                            ".gear.0", "ban: []")),
    "count on a set slot": ("rule:tackle_restriction",
                            _bad_rule("tackle_restriction",
                                      lambda r: r.update(gear=[{"slot": "barb", "max": 1}]),
                                      ".gear.0", "chosen from a set")),
    "empty length range": ("rule:retention_limit",
                           _bad_rule("retention_limit",
                                     lambda r: r.update(lengths=[{"min_cm": 50, "max_cm": 40}]),
                                     ".lengths.0", "empty range")),
    "retired field": ("rule:retention_limit",
                      _bad_rule("retention_limit", lambda r: r.update(windows=["Jan 1-Mar 31"]),
                                ".windows", "not a field")),
    "retired type": ("rule:advisory",
                     _bad_rule("advisory", lambda r: r.update(type="document_required"), ".type",
                               "")),
    "unregistered conduct": ("rule:handling_rule",
                             _bad_rule("handling_rule", lambda r: r.update(conduct=["be_nice"]), "",
                                       "not a registered act")),
    "verbatim not in the passage": ("rule:vessel_rule",
                                    _bad_rule("vessel_rule",
                                              lambda r: r.update(verbatim="Something else"), "",
                                              "contiguous substring")),
    "bad when date": ("rule:angler_closure",
                      _bad_rule("angler_closure",
                                lambda r: r.update(when={"dates": [{"from_month": 2, "from_day": 30,
                                                                    "to_month": 3, "to_day": 1}]}),
                                ".when.dates.0", "does not exist")),
    # ON A CLOSURE WHOSE SENTENCE NAMES A RESIDENCY. The smallest angler_closure is now a
    # Youth/Disabled Accompanied Water row, whose sentence names none, so the check has nothing to
    # hold the `who` to there; the non-guided-alien closures are what it exists for.
    "closed_to wider than the sentence": ("rule:angler_closure:residency",
                                          _bad_rule("angler_closure",
                                                    lambda r: r.update(closed_to={"guidance": ["non_guided"]}),
                                                    "", "residency")),
    "unknown split": ("rule:tackle_restriction",
                      _bad_rule("tackle_restriction",
                                lambda r: r.update(extents=[{"op": "upstream_of",
                                                             "splits": ["no_such_split"]}]),
                                ".extents.0.splits", "unknown split")),
    "class III": ("licensing:designation",
                  _bad_rec("designation", lambda x: x.update(classified="III"), ".classified", "")),
    "unit not a slug": ("licensing:designation",
                        _bad_rec("designation", lambda x: x.update(unit="Nekite River"), ".unit",
                                 "pattern")),
    "quota on hold": ("licensing:requirement",
                      _bad_rec("requirement",
                               lambda x: x.update(satisfied_by=[{"hold": ["basic_licence"],
                                                                 "quota": "own"}]),
                               ".satisfied_by.0", "quota")),
    "no documents": ("licensing:exemption",
                     _bad_rec("exemption", lambda x: x.update(documents=[]), ".documents", "")),
    "terms that set nothing": ("licensing:licence_terms",
                               _bad_rec("licence_terms",
                                        lambda x: [x.pop(k, None) for k in list(x)
                                                   if k not in ("kind", "id", "document", "verbatim",
                                                                "label")],
                                        "", "say nothing")),
    "alternative everywhere": ("licensing:alternative",
                               _bad_rec("alternative",
                                        lambda x: x.update(extents=[{"op": "within",
                                                                     "area_kind": "region"}]),
                                        "", "whole province")),
    # THE INGEST GATE, not only the model: a curator saved `take: 15` on a rule whose sentence
    # says 20 and the app accepted it. Every number must be printed in the rule's own verbatim.
    "a number the sentence does not print": ("rule:retention_limit",
                                             _bad_rule("retention_limit",
                                                       lambda r: r.update(take=97531),
                                                       ".take", "does not appear in its own "
                                                                "verbatim")),
    "a size the sentence does not print": ("rule:retention_limit",
                                           _bad_rule("retention_limit",
                                                     lambda r: r.update(lengths=[{"min_cm": 97}]),
                                                     ".lengths.0.min_cm", "does not appear")),
    "a split parent": ("rule:tackle_restriction",
                       _bad_rule("tackle_restriction",
                                 lambda r: r.update(extents=[{"op": "whole",
                                                              "item_id": "wbk:328974235"}]),
                                 "", "is cut into")),
    "a bare whole beside a place in words": ("rule:tackle_restriction",
                                             _bad_rule("tackle_restriction",
                                                       lambda r: r.update(
                                                           extents=[{"op": "whole"}],
                                                           extent_text="south of the bridge"),
                                                       "", "extent_text names a place")),
    "not_classified quote": ("licensing:not_classified",
                             _bad_rec("not_classified",
                                      lambda x: x.update(verbatim="This tributary of St. Mary River"),
                                      "", "not a Classified Water")),
}


@pytest.mark.parametrize("name", sorted(BAD))
def test_put_invalid_is_refused_at_the_field(env, picks, name):
    part, make = BAD[name]
    eid = picks[part]
    d = _get(env, eid)
    entry = copy.deepcopy(d["entry"])
    path, says = make(entry)
    before = _file_of(env, eid).read_bytes()
    r = _put(env, eid, d["region"], entry)
    assert r.status_code == 422, f"{name}: accepted"
    errors = r.json()["detail"]
    at = [e for e in errors if e["path"] == path]
    assert at, f"{name}: no error addressed to {path}; got {errors}"
    assert any(says.lower() in e["msg"].lower() for e in at), f"{name}: {at}"
    assert _file_of(env, eid).read_bytes() == before, f"{name}: a refused save wrote the file"
    # the draft check says the same thing, before any save
    chk = env["client"].post("/api/check", json={"region": d["region"], "entry": entry}).json()
    assert not chk["ok"] and any(e["path"] == path for e in chk["errors"])


@pytest.mark.parametrize("field,value", [("name", "SOMETHING ELSE"), ("display_name", "Else"),
                                         ("region", "9"), ("symbols", ["Classified"]),
                                         ("regs_verbatim", "Trout daily quota = 8 (edited)")])
def test_pass_through_fields_are_refused(env, picks, field, value):
    eid = picks["rule:retention_limit"]
    d = _get(env, eid)
    entry = copy.deepcopy(d["entry"])
    entry[field] = value
    r = _put(env, eid, d["region"], entry)
    assert r.status_code == 422
    assert any(e["path"] == field and "passed through" in e["msg"] for e in r.json()["detail"])


def test_a_licensing_label_is_served_and_stripped_on_save(env, picks):
    """`label` is stamped on what is served and never written."""
    eid = picks["licensing:designation"]
    d = _get(env, eid)
    assert all("label" in x for x in d["entry"]["licensing"])
    _put(env, eid, d["region"], d["entry"])
    assert all("label" not in x for x in _entries(env["dir"])[eid][1]["licensing"])


# --------------------------------------------------------------------------- #
# The vocabulary is the model's
# --------------------------------------------------------------------------- #

def test_vocab_is_read_off_the_model(env):
    v = env["client"].get("/api/vocab").json()
    assert [t["type"] for t in v["rule_types"]] == [t.value for t in C.RuleType]
    assert [s["slot"] for s in v["slots"]] == [s.value for s in C.Slot]
    assert [c["act"] for c in v["conduct"]] == list(C.CONDUCT_ACTS)
    assert [d["doc"] for d in v["documents"]] == [d.value for d in C.Document]
    assert v["who_axes"] == {k: list(m) for k, m in C.WHO_AXES.items()}
    assert sorted(s["code"] for s in v["species"]) == sorted(C.KNOWN_SPECIES)
    assert v["exemptable_defaults"] == sorted(C.EXEMPTABLE_DEFAULTS)
    assert set(v["licensing_kinds"]) == {"designation", "not_classified", "requirement",
                                         "licence_terms", "exemption", "alternative"}
    for token in v["while"]:                                  # every offered token validates
        C.CatalogueRule(rule_id="x.r1", type="advisory", verbatim="x", **{"while": [token]})
    assert "document_required" not in [t["type"] for t in v["rule_types"]]


# --------------------------------------------------------------------------- #
# `matched` is authoritative; empty binds nothing
# --------------------------------------------------------------------------- #

def test_covered_ids_read_matched_only(env, picks):
    reuse = env["reuse"]
    _, e = _entries(env["dir"])[picks["rule:tackle_restriction"]]
    assert e["matched"] and reuse._covered_ids(e) == e["matched"]
    # the same row with `matched` emptied: its name still matches a registry item live, and the
    # app must NOT fall back to it
    empty = dict(e, matched=[])
    assert reuse._suggest_match(empty).item_id, "precondition: the live matcher finds this water"
    assert reuse._covered_ids(empty) == []
    assert reuse._item_for_entry(empty) is None
    assert reuse.entry_status(empty, None) == "no_registry"
