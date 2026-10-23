"""T119 — ONE ACCEPTANCE GATE: "is this catalogue entry acceptable?" is `validate_catalogue.gate`.

Ingest called the checks without a registry (so `absorbed_ids_named` never ran there), and the
review app ran its own partial set (never `resolve_exempt_ids`, never `canonicalise_splits`) beside
a docstring calling itself "ONE GATE". An entry one of them accepted could be refused by another.
Now ingest (`ingest_catalogue.ingest`), the parse agent's self-check (through ingest) and the review
app's check and save (`model_api.check`, from `reuse._gate`) call `gate`, and nothing else calls a
check.

MUTATION PINS — which mutation fails which test:
  * a check dropped from `gate` (or `gate` stops passing `menu` / `registry` to it)
        -> test_gate_runs_every_check[<that check>]
  * `resolve_exempt_ids` dropped from `gate`  -> test_gate_resolves_an_exemption_alias
  * ingest calling `gate` without its registry -> test_ingest_refuses_an_absorbed_id
  * the app's check calling `gate` without menu or registry, or rebuilding its own checks
        -> test_the_app_refuses_exactly_what_the_gate_refuses[*],
           test_the_app_stores_the_canonical_spelling
  * a check called anywhere but inside `gate`  -> test_only_gate_calls_a_check
  * `default_extents` moved into `gate` (a curator's bare rule silently widened)
        -> test_the_gate_never_widens_a_bare_rule
"""

from __future__ import annotations

import ast
import copy
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.regs.parsing import validate_catalogue as V

REPO = Path(__file__).resolve().parents[2]

SOURCE = "TRANQUILLE LAKE 3-29 Rainbow trout daily quota = 8. Bait ban."

#: A registry where `gone` was folded into `keep` (`flowing.alias_map` reads `aliases`).
REGISTRY = {"keep": SimpleNamespace(aliases=("gone",), name="Keep River", part_of="",
                                    boundaries=())}

MENU = {"bindable_ids": ["real_cut", "alias_cut"],
        "boundaries": [["real_cut", "", "", ["alias_cut"]]],
        "bindable_by_item": {"keep": ["real_cut", "alias_cut"]}}


def _entry(**rule):
    base = {"rule_id": "t.r1", "type": "retention_limit", "verbatim": "Rainbow trout daily quota = 8",
            "species": ["RB"], "take": 8, "extents": [{"op": "whole"}]}
    base.update(rule)
    return {"entry_id": "r3:tranquille@3-29", "name": "TRANQUILLE LAKE", "region": "3",
            "regs_verbatim": SOURCE, "matched": ["keep"], "rules": [base]}


def _lift_outside():
    e = _entry()
    e["rules"] = [
        dict(e["rules"][0], rule_id="t.r1", extents=[{"op": "within", "area_id": "area:region:7b"}]),
        dict(e["rules"][0], rule_id="t.r2", take=None, type="advisory",
             extents=[{"op": "within", "area_id": "area:region:6"}],
             exempts=[{"target": "t.r1"}]),
    ]
    return e


#: check -> (the entry only it refuses, the path it is addressed to, words in the message)
ONLY = {
    "absorbed_ids_named": (_entry(extents=[{"op": "whole", "item_id": "gone"}]),
                           ("matched",), "was folded into keep"),
    "exemptions_stay_inside_what_they_lift": (_lift_outside(), (), "share no ground"),
    "split_parents_named": (_entry(extents=[{"op": "whole", "item_id": "wbk:328974235"}]),
                            ("rules", 0, "extents", 0, "item_id"), "is cut into"),
    "canonicalise_splits": (_entry(extents=[{"op": "upstream_of", "splits": ["made_up"]}]),
                            ("rules", 0, "extents", 0, "splits"), "unknown split id 'made_up'"),
    "post_model_checks": (_entry(take=9), ("rules", 0, "take"), "does not appear in its own"),
}


def _gate(e, **kw):
    return V.gate(e, **{"menu": MENU, "registry": REGISTRY, "source_text": SOURCE, **kw})


def test_a_clean_entry_passes_every_check():
    v = _gate(_entry())
    assert (v.ok, v.refusals, v.model_error, v.advisories) == (True, (), None, ())


@pytest.mark.parametrize("check", sorted(ONLY))
def test_gate_runs_every_check(check, monkeypatch):
    """Each entry is refused by exactly one check, at its field — and by nothing once that check
    is switched off, so the refusal is that check's and the gate is what ran it."""
    entry, path, says = ONLY[check]
    v = _gate(copy.deepcopy(entry))
    assert [(p, says in m) for p, m in v.refusals] == [(path, True)], v.refusals
    monkeypatch.setattr(V, check, lambda *a, **k: [])
    assert _gate(copy.deepcopy(entry)).refusals == ()


def test_layer_three_is_the_gates():
    v = _gate(_entry(), source_text="SOMETHING ELSE ENTIRELY")
    assert [p for p, _m in v.refusals] == [("regs_verbatim",)]


def test_gate_resolves_an_exemption_alias(monkeypatch):
    e = _entry(type="advisory", take=None, exempts=[{"default_id": "spring_closure"}],
               verbatim="Rainbow trout daily quota = 8")
    assert _gate(e).entry.rules[0].exempts[0].default_id == "spring_stream_closure"
    monkeypatch.setattr(V, "resolve_exempt_ids", lambda data: 0)
    assert not _gate(e).ok                     # the book's name lifts nothing, and is refused


def test_gate_stores_an_alias_split_canonically_and_leaves_its_input_alone():
    e = _entry(extents=[{"op": "upstream_of", "splits": ["alias_cut"]}])
    v = _gate(e)
    assert v.ok and v.entry.rules[0].extents[0]["splits"] == ["real_cut"]
    assert e["rules"][0]["extents"][0]["splits"] == ["alias_cut"]


def test_the_gate_never_widens_a_bare_rule():
    """`default_extents` is ingest's normalisation of a parser's bare rule, never the gate's: a
    curator who empties a rule's extents is refused, not handed the whole water (AGENTS 13)."""
    e = _entry()
    del e["rules"][0]["extents"]
    v = _gate(e)
    assert v.entry is None and "says nothing about where it applies" in str(v.model_error)


def test_menu_and_registry_must_be_stated():
    """Keyword-only and required: a caller says it has none rather than forgetting one — which is
    how ingest came to skip the absorbed-id check."""
    with pytest.raises(TypeError):
        V.gate(_entry())                                      # noqa


# --- ingest --------------------------------------------------------------------------------------

def test_ingest_refuses_an_absorbed_id():
    from pipeline.regs.parsing.ingest_catalogue import ingest
    item = {"entry_id": "r3:tranquille@3-29", "raw_regs": SOURCE, "name": "TRANQUILLE LAKE",
            "region": "3", "item_id": "keep", **MENU}
    cand = _entry(extents=[{"op": "whole", "item_id": "gone"}])
    accepted, problems = ingest([copy.deepcopy(cand)], {cand["entry_id"]: item},
                                registry=REGISTRY)
    assert not accepted and any("was folded into keep" in p for p in problems), problems
    accepted, _ = ingest([_entry()], {cand["entry_id"]: item}, registry=REGISTRY)
    assert list(accepted) == [cand["entry_id"]]


# --- the review app ------------------------------------------------------------------------------

def _model_api():
    spec = importlib.util.spec_from_file_location(
        "model_api_t119", REPO / "curation-review" / "backend" / "model_api.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.mark.parametrize("check", sorted(ONLY))
def test_the_app_refuses_exactly_what_the_gate_refuses(check):
    entry, _path, _says = ONLY[check]
    _, errs = _model_api().check(copy.deepcopy(entry), menu=MENU, registry=REGISTRY)
    v = V.gate(copy.deepcopy(entry), menu=MENU, registry=REGISTRY)
    assert errs == [{"path": ".".join(map(str, p)), "msg": m} for p, m in v.refusals]


def test_the_app_stores_the_canonical_spelling():
    got, errs = _model_api().check(_entry(extents=[{"op": "upstream_of", "splits": ["alias_cut"]}]),
                                   menu=MENU, registry=REGISTRY)
    assert errs == [] and got.rules[0].extents[0]["splits"] == ["real_cut"]


def test_the_app_renders_the_models_own_refusal_in_its_field_terms():
    m, e = _model_api(), _entry(take=-1)
    _, errs = m.check(copy.deepcopy(e), menu=MENU, registry=REGISTRY)
    v = V.gate(copy.deepcopy(e), menu=MENU, registry=REGISTRY)
    assert v.model_error is not None and errs == m.errors_of(v.model_error, e)


# --- the grep pin --------------------------------------------------------------------------------

CHECKS = {"resolve_exempt_ids", "canonicalise_splits", "absorbed_ids_named", "split_parents_named",
          "exemptions_stay_inside_what_they_lift", "post_model_checks"}


def _calls(names: set[str]):
    """(file, enclosing function, called name) for every non-test call of `names`."""
    out = []
    files = list((REPO / "pipeline").rglob("*.py")) + list((REPO / "curation-review").rglob("*.py"))
    for p in sorted(files):
        if "tests" in p.parts or p.name.startswith("test_"):
            continue
        tree = ast.parse(p.read_text(encoding="utf-8"))
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for n in ast.walk(fn):
                if isinstance(n, ast.Call):
                    f = n.func
                    name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
                    if name in names:
                        out.append((str(p.relative_to(REPO)), fn.name, name))
    return sorted(set(out))


def test_only_gate_calls_a_check():
    assert not hasattr(V, "check_entry"), "the partial gate is gone; everything calls `gate`"
    assert {(f, fn) for f, fn, _ in _calls(CHECKS)} == {
        ("pipeline/regs/parsing/validate_catalogue.py", "gate")}
    assert {n for *_x, n in _calls(CHECKS)} == CHECKS


def test_ingest_and_the_app_call_the_gate():
    assert _calls({"gate"}) == [
        ("curation-review/backend/model_api.py", "check", "gate"),
        ("pipeline/regs/parsing/ingest_catalogue.py", "ingest", "gate")]


# --- the corpus ----------------------------------------------------------------------------------

@pytest.mark.needs_atlas
def test_the_corpus_passes_the_gate_with_its_build(request):
    """THE ACCEPTANCE (T119): every curated entry, run through the gate with the served build's
    registry and the cut-point menu of every water it names, is accepted — the same list (empty)
    the full validator gives. Measured 2026-10-23 on 1,525 entries."""
    import json
    from pipeline.atlas.registry import load_registry
    from pipeline.common.curated import CURATED, GENERATED
    from pipeline.tests.conftest import ATLAS_HINT, need
    need(request, "atlas", GENERATED.build() / "registry.json", ATLAS_HINT)
    reg = load_registry(str(GENERATED.registry()))
    cut_points = V.registry_cut_points(reg)
    entries = [e for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json"))
               for e in json.loads(p.read_text())["entries"]]
    refused = {}
    for e in entries:
        v = V.gate(e, menu=V.cut_point_menu(e, cut_points), registry=reg,
                   source_text=e.get("regs_verbatim") or "")
        if not v.ok:
            refused[e["entry_id"]] = v.messages()
    assert entries and refused == {}     # the corpus grows; the count is not the point
