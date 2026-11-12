"""T349 (owner 2026-11-02): region / scope defaults are ENTRIES, salmon non-retention is closed, shared quotas are the
quota tree's aggregate tier."""
from __future__ import annotations

import pytest

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import cascade as C
from pipeline.regs.dfo_salmon import entries as E
from pipeline.regs.dfo_salmon import quota as Q

R2_28 = ("There is no fishing for salmon in waters of Region 2 flowing into Areas 28 and 29 except for the "
         "opportunities listed in the table below.")
R2_13 = ("Unless otherwise stated in the table below, the daily limit for waters in Region 2 flowing into Areas 13, 15, "
         "and 16 is zero (0).")
AUG1 = 801


def _rule(verbatim: str, may_target: bool, take: int = 0, sp=("CO",)) -> dict:
    return {"rule_id": "r", "type": "retention_limit", "species": list(sp), "take": take, "may_target": may_target,
            "verbatim": verbatim}


# ── salmon non-retention = closed ────────────────────────────────────────────────────────────────────────────────────

def test_non_retention_is_closed_to_salmon():
    for words in ("Non-retention", "No retention of coho", "0 per day"):
        v = S.verdict_on([[_rule(words, True)]], "CO", AUG1)
        assert v["s"] == "closed", words


def test_only_explicit_catch_and_release_is_release():
    v = S.verdict_on([[_rule("Open for salmon catch and release", True)]], "CO", AUG1)
    assert v["s"] == "release"
    # an opening still wins over both
    v = S.verdict_on([[_rule("2 per day", None, take=2), _rule("Non-retention", True)]], "CO", AUG1)
    assert v["s"] == "open"


# ── the page's sentences as default locations ────────────────────────────────────────────────────────────────────────

def test_statements_read_region_2s_two_scopes():
    st = C.statements(["You can only fish for salmon in Region 2 during daylight hours.", R2_28, R2_13])
    assert [(s["k"], s["areas"], s["unlisted"]) for s in st] == [
        ("closed_except_listed", [28, 29], "closed"), ("limit_zero", [13, 15, 16], "closed")]


def test_statement_locators_are_one_closed_rule_each():
    locs = C.statement_locators("2", [R2_28, R2_13])
    assert [l["fingerprint"] for l in locs] == ["page:2:areas-28-29", "page:2:areas-13-15-16"]
    for l in locs:
        (r,) = l["rules"]
        assert r["species"] == ["SA"] and r["take"] == 0 and r["may_target"] is False and r["verbatim"] == l["rows"][0]
        assert S.verdict_on([l["rules"]], "CH", AUG1)["s"] == "closed"


def test_every_region_has_one_scope_entry_for_its_sentence():
    """ONE model (owner 2026-11-02): the sentence sits on the page's own row where one prints the same scope, else on a
    page-default entry; never two entries for one scope, never an entry with nothing to answer."""
    for slug in ("1", "2", "3", "4", "5a", "5b", "6", "7", "8"):
        ef = E.load(slug)
        scope_ids = [l.location_id for l in ef.locations if not l.water_id]
        assert len(scope_ids) == len(set(scope_ids))
        holders = [l for l in ef.locations if any(f.startswith(f"page:{slug}:") for f in l.fingerprints)]
        assert holders, slug
        for l in ef.locations:
            if l.kind == E.PAGE_DEFAULT:
                assert l.precedence == E.PAGE_DEFAULT_PRECEDENCE and l.binding.extents and l.fingerprints
    for (slug, scope), lid in E.PAGE_DEFAULT_ON.items():
        ids = {l.location_id for l in E.load(slug).locations}
        assert lid in ids and f"{slug}:default:{scope}" not in ids
    r2 = {l.location_id for l in E.load("2").locations if l.kind == E.PAGE_DEFAULT}
    assert r2 == {"2:default:areas-28-29", "2:default:areas-13-15-16"}      # the US remainder: no entry, closed


def test_a_page_default_answers_after_every_table_row():
    order = S.chain_order({"w": 3, "a": 2, "s": 1, "r": 0, "d": E.PAGE_DEFAULT_PRECEDENCE},
                          {"w": 10, "a": 10_000, "s": 100, "r": 1_000_000, "d": 5})
    assert sorted("dwasr", key=order) == ["w", "a", "s", "r", "d"]


def test_a_page_default_is_never_marked_dormant():
    ef = E.load("3")
    added, dormant = E.apply_seed(ef, [], {})
    assert all(l.status == "active" for l in ef.locations if l.kind == E.PAGE_DEFAULT)
    rep = E.reconcile(E.load("3"), [], {})
    assert not any(o.location_id == "3:default:region" for o in rep.outcomes)


def test_seeding_page_defaults_is_idempotent():
    ef = E.load("2")
    assert E.seed_page_defaults(ef, C.statements([R2_28, R2_13])) == []


# ── shared quotas ────────────────────────────────────────────────────────────────────────────────────────────────────

R5B = ("All lakes or streams or part thereof in the aggregate (combined total), in Management Units 5-6 to 5-11 in "
       "Region 5B, unless otherwise stated below (i.e. Region 5 not including the Fraser River watershed).")


def test_r5b_settles_on_the_mu_list_and_its_count_is_shared():
    """(T358) the pool is STRUCTURAL: `pooled_across` the location's own `within` areas, the words the area's."""
    from types import SimpleNamespace as NS
    from pipeline.regs.dfo_salmon import entries as E
    mu = "area:mu_group:management_units_5_6_to_5_11"
    loc = next(l for l in E.load("5b").locations if l.location_id == "5b:region:section")
    assert Q.pool_of(loc) == [mu]
    assert Q.pool_words([mu]) == "Management Units 5-6 to 5-11"
    rules = Q.mark_shared([{**_rule("4 per day", None, take=4, sp=("CH",)), "rule_id": "a.r0"},
                           {"rule_id": "a.r0b", "type": "retention_limit", "species": ["CH"], "take": 1,
                            "within": "a.r0", "lengths": [{"min_cm": 50}], "verbatim": "only 1 over 50 cm"}], [mu])
    assert rules[0]["pooled_across"] == [mu] and "pooled_across" not in rules[1]
    v = S.verdict_on([rules], "CH", AUG1)
    t = v["tree"]
    assert v["s"] == "open" and t["tier"] == "total" and t["whose"] == "Management Units 5-6 to 5-11" and t["n"] == 4
    assert [c["n"] for c in t["children"]] == [1]                 # the size tier stays under the shared count
    assert Q.pool_of(NS(source_text={"specific_area": "All Region 4 waters"})) == []
    # the field is the catalogue model's own: a pooled count validates, words in it do not
    from pipeline.regs.parsing.catalogue_parts.rule import CatalogueRule
    CatalogueRule.model_validate({k: v for k, v in rules[0].items() if k != "when"} | {"verbatim": "4 per day"})
    with pytest.raises(ValueError, match="not area ids"):
        CatalogueRule.model_validate({**rules[0], "pooled_across": ["Management Units 5-6 to 5-11"]})


def test_squamish_hatchery_coho_is_an_aggregate_tier_over_the_watershed():
    notes = [{"id": "2:daily:x", "k": "daily", "t": "…", "n": 4},
             {"id": "2:aggregate:y", "k": "aggregate", "sp": ["CO"], "n": 1, "hatchery": True,
              "t": "Squamish watershed: the aggregate daily limit of hatchery-marked coho … is one (1).",
              "w": [{"n": "Squamish River", "ws": True, "key": "squamish river+ws"}]}]
    (sh,) = Q.shared_buckets(notes)
    assert "shared" not in Q.bucket(notes)               # T356: the region's own, not the bucket's
    assert S.header_answers(notes[1:])["shared"] == [sh]  # and there with no combined daily limit too
    assert sh["w"] == ["squamish river+ws"] and sh["id"] == "2:aggregate:y"
    t = sh["tree"]
    assert (t["tier"], t["n"], t["species"], t["origin"]) == ("total", 1, ["CO"], "hatchery")
    assert t["whose"] == "the Squamish River watershed"


# ── T679 #2: the scope places are curated data, read by the promote guards ───────────────────────────────────────────

CNR_BRIDGE = "skeena_river__cnr_railway_bridge_terrace"


def test_the_scope_places_come_from_the_curated_file(tmp_path):
    """The page defaults and cascade scopes are read from `dfo_salmon/scope_places.json`, never from code. Mutation: a
    copy whose section D extent names another area makes `bind_scopes` write that area on Region 6's D default."""
    import json
    from pipeline.tests import under_test
    src = under_test.curated().dfo_salmon.scope_places
    doc = json.loads(src.read_text(encoding="utf-8"))
    places = E.load_scope_places(src)
    assert places.scope_extents[("6", "B(i)")][0]["splits"] == [CNR_BRIDGE]
    assert places.scope_extents[("6", "B(i)")][0]["item_id"] == "gnis:2936"
    assert [k for k in places.page_defaults] == [(r["region"], r["scope"]) for r in doc["page_defaults"]]
    for row in doc["scope_extents"]:
        if (row["region"], row["section"]) == ("6", "D"):
            row["extents"] = [{"op": "within", "area_id": "area:region:1"}]
    moved = tmp_path / "scope_places.json"
    moved.write_text(json.dumps(doc), encoding="utf-8")
    assert E.load_scope_places(moved).scope_extents[("6", "D")] == [{"op": "within", "area_id": "area:region:1"}]
    with pytest.raises(FileNotFoundError):
        E.load_scope_places(tmp_path / "absent.json")
    bad = dict(doc)
    bad.pop("page_defaults")
    moved.write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(KeyError, match="page_defaults"):
        E.load_scope_places(moved)


def test_the_promote_guard_reads_the_scope_places():
    """`promote.curated_boundary_ids` scans the scope places file (the CNR bridge split B(i)/B(ii) name) — a rebuild that
    moves that cut no longer passes promote. Mutation: the file is in the guard's list and alone yields the bridge."""
    from pipeline.atlas import promote
    from pipeline.tests import under_test
    f = under_test.curated().dfo_salmon.scope_places
    assert f in promote.curated_boundary_files()
    assert CNR_BRIDGE in promote.split_ids_in([f])
    assert CNR_BRIDGE in promote.curated_boundary_ids()
