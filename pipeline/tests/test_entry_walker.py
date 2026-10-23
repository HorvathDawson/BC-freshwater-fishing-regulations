"""T112 — ONE ENTRY WALKER: which extents and item references a catalogue entry holds.

Five places hold extents (the entry scope; each rule's `extents` and `tributary_excludes`; each
licensing record's `extents` and `tributary_excludes`) and every checker and remapper used to list
them itself, each list different: `canonicalise_splits` skipped the rules' carve-outs (62
curator-written split references never validated), `remap_boundaries` skipped every licensing
extent (about 30 split references went dead after a rebuild), the review app's split check skipped
both carve-out lists. Now `catalogue_parts/walk.py` is the one list, and every consumer is pinned
here to see ALL FIVE.

MUTATION PINS — which mutation fails which test:
  * drop RULE_EXCLUDE (or any role) from `walk._WHERE` / `iter_extent_lists`
        -> test_iter_extents_is_every_extent_in_file_order, and every
           test_<consumer>_sees_all_five_places below (each misses that place)
  * revert `canonicalise_splits` to its own list (no rule carve-outs)
        -> test_canonicalise_splits_sees_all_five_places
  * revert `remap_boundaries` to its own list (no licensing)
        -> test_remap_sees_all_five_places
  * `refs_to_parents_at` reading `outside_items` as a binding
        -> test_a_split_parent_may_be_subtracted_never_bound
  * a checker module reading `tributary_excludes` / licensing extents by key again
        -> test_no_checker_or_remapper_lists_extent_places_itself
"""

from __future__ import annotations

import ast
import copy
import importlib.util
from pathlib import Path

import pytest

from pipeline.regs.parsing.catalogue_parts import walk
from pipeline.regs.parsing.catalogue_parts.walk import (describe, iter_extent_lists, iter_extents,
                                                        iter_item_refs, ref_key)

REPO = Path(__file__).resolve().parents[2]

#: One extent in each of the five places, each with its own split id and item id, plus a record
#: that is not a dict (skipped) and an extent that is not a dict (skipped).
FIVE = {
    "entry_id": "e1",
    "matched": ["m:1"],
    "extents": [{"op": "upstream_of", "splits": ["s_scope"], "item_id": "i:scope"}],
    "rules": [
        "not a rule",
        {"rule_id": "e1.r1",
         "extents": [{"op": "downstream_of", "splits": ["s_rule"], "item_ids": ["i:rule"],
                      "outside_items": ["o:rule"]}, "not an extent"],
         "tributary_excludes": [{"op": "upstream_of", "splits": ["s_rule_x"],
                                 "item_id": "i:rule_x"}]},
    ],
    "licensing": [
        {"kind": "designation", "id": "d1",
         "extents": [{"op": "upstream_of", "splits": ["s_lic"], "item_id": "i:lic"}],
         "tributary_excludes": [{"op": "whole", "splits": ["s_lic_x"], "item_id": "i:lic_x"}]},
    ],
}

#: (path of the extent, role, its split id) — the whole value the walker must produce.
WANT = [
    (("extents", 0), walk.SCOPE, "s_scope"),
    (("rules", 1, "extents", 0), walk.RULE, "s_rule"),
    (("rules", 1, "tributary_excludes", 0), walk.RULE_EXCLUDE, "s_rule_x"),
    (("licensing", 0, "extents", 0), walk.LICENSING, "s_lic"),
    (("licensing", 0, "tributary_excludes", 0), walk.LICENSING_EXCLUDE, "s_lic_x"),
]
ALL_SPLITS = [s for _p, _r, s in WANT]


def test_iter_extents_is_every_extent_in_file_order():
    got = [(path, role, ex["splits"][0]) for path, ex, role in iter_extents(FIVE)]
    assert got == WANT
    assert {r for _p, _x, r in iter_extents(FIVE)} == set(walk.ROLES)


def test_iter_extent_lists_yields_every_list_even_an_empty_one():
    e = {"rules": [{"rule_id": "r"}], "licensing": [{"id": "x"}]}
    assert [(p, x, r) for p, x, r in iter_extent_lists(e)] == [
        (("extents",), [], walk.SCOPE),
        (("rules", 0, "extents"), [], walk.RULE),
        (("rules", 0, "tributary_excludes"), [], walk.RULE_EXCLUDE),
        (("licensing", 0, "extents"), [], walk.LICENSING),
        (("licensing", 0, "tributary_excludes"), [], walk.LICENSING_EXCLUDE)]


def test_iter_item_refs_is_every_item_id_with_its_key():
    got = [(p, i, ref_key(p)) for p, i in iter_item_refs(FIVE)]
    assert got == [
        (("matched", 0), "m:1", "matched"),
        (("extents", 0, "item_id"), "i:scope", "item_id"),
        (("rules", 1, "extents", 0, "item_ids", 0), "i:rule", "item_ids"),
        (("rules", 1, "extents", 0, "outside_items", 0), "o:rule", "outside_items"),
        (("rules", 1, "tributary_excludes", 0, "item_id"), "i:rule_x", "item_id"),
        (("licensing", 0, "extents", 0, "item_id"), "i:lic", "item_id"),
        (("licensing", 0, "tributary_excludes", 0, "item_id"), "i:lic_x", "item_id"),
    ]


def test_describe_names_each_place():
    assert [describe(p) for p, _r, _s in WANT] == [
        "entry scope", "rule 2", "rule 2 tributary_excludes", "licensing 1",
        "licensing 1 tributary_excludes"]


# --- every consumer sees all five places ------------------------------------------------------

def test_canonicalise_splits_sees_all_five_places():
    """An invented id in each place is refused, addressed to that place's `splits`."""
    from pipeline.regs.parsing.validate_catalogue import canonicalise_splits
    menu = {"bindable_ids": ["real"], "boundaries": [["real", "", "", []]]}
    got = canonicalise_splits(copy.deepcopy(FIVE), menu)
    assert [p for p, _m in got] == [p + ("splits",) for p, _r, _s in WANT]
    assert [m.split(":")[0] for _p, m in got] == [describe(p) for p, _r, _s in WANT]
    assert all("unknown split id" in m for _p, m in got)


def test_canonicalise_splits_rewrites_an_alias_in_every_place():
    from pipeline.regs.parsing.validate_catalogue import canonicalise_splits
    menu = {"bindable_ids": ["canon", *ALL_SPLITS],
            "boundaries": [["canon", "", "", ALL_SPLITS]]}
    e = copy.deepcopy(FIVE)
    assert canonicalise_splits(e, menu) == []
    assert [ex["splits"] for _p, ex, _r in iter_extents(e)] == [["canon"]] * 5


def test_an_empty_menu_refuses_every_split_and_an_absent_one_checks_nothing():
    """A menu that STATES no cut-points (a zone entry; a water with no boundaries) refuses every
    split — the review app always refused there, and ingest's batch item says the parser was shown
    none, so any split it wrote was invented. A batch item with no menu KEYS was written before
    boundaries were exported and checks nothing. (Before T119 an empty set checked nothing.)"""
    from pipeline.regs.parsing.validate_catalogue import canonicalise_splits
    got = canonicalise_splits(copy.deepcopy(FIVE), {"bindable_ids": [], "boundaries": []})
    assert [p for p, _m in got] == [p + ("splits",) for p, _r, _s in WANT]
    assert canonicalise_splits(copy.deepcopy(FIVE), {}) == []


def test_remap_sees_all_five_places(tmp_path):
    import json
    from pipeline.regs.parsing.remap_boundaries import apply_remap
    (tmp_path / "region-3.json").write_text(json.dumps({"region": "3", "entries": [FIVE]}))
    rep = apply_remap(tmp_path, {s: s + "_new" for s in ALL_SPLITS}, dry_run=True)
    assert rep["changed"] == [("3", "e1", sorted((s, s + "_new") for s in ALL_SPLITS))]


def test_cut_point_menu_takes_every_scoped_item_never_a_subtracted_one():
    from pipeline.regs.parsing.validate_catalogue import cut_point_menu
    asked = []
    cut_point_menu(FIVE, lambda iid: asked.append(iid) or [])
    assert asked == ["m:1", "i:scope", "i:rule", "i:rule_x", "i:lic", "i:lic_x"]


def test_a_split_parent_may_be_subtracted_never_bound():
    """Every BINDING reference to a lake cut into parts is refused, in each of the five places and
    `matched`; an `outside_items` naming it is not (taking Kootenay Lake out of the Creston Valley
    WMA takes out its parts and its ghost — the four Creston records do exactly that)."""
    from pipeline.atlas.waters.added_lakes.split_parents import refs_to_parents_at
    parents = {i: ["part"] for _p, i in iter_item_refs(FIVE)}
    got = [p for p, _m in refs_to_parents_at(FIVE, parents)]
    assert got == [p for p, _i in iter_item_refs(FIVE) if ref_key(p) != "outside_items"]
    assert len(got) == 6


def _model_api():
    spec = importlib.util.spec_from_file_location(
        "model_api_t112", REPO / "curation-review" / "backend" / "model_api.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_the_apps_extent_warnings_see_all_five_places():
    e = copy.deepcopy(FIVE)
    for _p, ex, _r in iter_extents(e):
        ex["bogus"] = 1
    got = [w["path"] for w in _model_api().warnings_of(e) if "not in the Extent model" in w["msg"]]
    assert got == [".".join(map(str, p)) for p, _r, _s in WANT]


# --- the grep pin -----------------------------------------------------------------------------

#: The modules that check or remap a catalogue entry's extents. The reach builder, the bundle and
#: the models READ a rule's carve-outs for what they mean; that is not a walk and is not here.
CHECKERS = ["pipeline/regs/parsing/validate_catalogue.py",
            "pipeline/regs/parsing/remap_boundaries.py",
            "pipeline/regs/parsing/ingest_catalogue.py",
            "pipeline/atlas/waters/added_lakes/split_parents.py",
            "curation-review/backend/model_api.py",
            "curation-review/backend/reuse.py"]

#: (module, function) that may read `tributary_excludes` by key, and why.
ALLOWED = {
    # the tributary preview counts ONE rule's authored carve-outs beside the sections they remove
    ("curation-review/backend/reuse.py", "rule_resolved_reach"),
}


def _key_reads(fn) -> set[str]:
    """String keys `fn` reads with `.get("k")` or `["k"]`."""
    out = set()
    for n in ast.walk(fn):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "get" \
                and n.args and isinstance(n.args[0], ast.Constant):
            out.add(n.args[0].value)
        elif isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Constant):
            out.add(n.slice.value)
    return out


def test_no_checker_or_remapper_lists_extent_places_itself():
    """A checker reading `tributary_excludes`, or a licensing record's `extents`, by key is a
    second copy of the walker — the shape every one of the five divergent lists had."""
    bad = []
    for rel in CHECKERS:
        tree = ast.parse((REPO / rel).read_text(encoding="utf-8"))
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            keys = _key_reads(fn)
            if "tributary_excludes" in keys and (rel, fn.name) not in ALLOWED:
                bad.append(f"{rel}:{fn.lineno} {fn.name} reads tributary_excludes")
            if {"licensing", "extents"} <= keys:
                bad.append(f"{rel}:{fn.lineno} {fn.name} reads licensing extents")
    assert bad == []


@pytest.mark.parametrize("rel,fn", sorted(ALLOWED))
def test_the_allowed_readers_still_exist(rel, fn):
    """An allowance for a function that is gone would let a new one in under its name."""
    tree = ast.parse((REPO / rel).read_text(encoding="utf-8"))
    assert fn in {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
