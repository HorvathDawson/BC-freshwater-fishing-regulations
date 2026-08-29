"""Matcher: diacritics folding, MU disambiguation, and override resolutions."""

from pipeline.models import RegistryItem
from pipeline.matching.matcher import _norm, match_rows, parse_reg_mus


def _it(iid, name, mus=(), variants=(), ref_ids=(), kind="stream"):
    return RegistryItem(id=iid, name=name, kind=kind, variants=tuple(variants), mus=tuple(mus),
                        section_ids=(iid,), ref_ids=tuple(ref_ids))


def test_diacritics_and_apostrophe_fold():
    assert _norm("Barrière River") == "barriere river"
    assert _norm("John's Lake") == "johns lake"
    assert _norm("ADAM RIVER (except Eve River)") == "adam river"


def test_type_word_kept_no_collision():
    assert _norm("Goose Lake") != _norm("Goose Creek")


def test_parse_reg_mus():
    assert parse_reg_mus({"mu": "1-5"}) == {"1-5"}
    assert parse_reg_mus({"mu": ["2-3", "2-4"]}) == {"2-3", "2-4"}
    assert parse_reg_mus({"mu": ""}) == set()


def test_unique_hit_and_diacritic_match():
    reg = {"g:1": _it("g:1", "Barriere River")}
    rows = [{"water": "Barrière River", "region": "REGION 3", "mu": "3-30"}]
    assert match_rows(rows, reg)[0].item_id == "g:1"


def test_mu_disambiguation():
    reg = {
        "g:1": _it("g:1", "Long Lake", mus=("1-5",)),
        "g:2": _it("g:2", "Long Lake", mus=("8-6",)),
    }
    # row in MU 1-5 -> the Vancouver Island Long Lake
    rows = [{"water": "Long Lake", "region": "REGION 1", "mu": "1-5"}]
    r = match_rows(rows, reg)[0]
    assert r.item_id == "g:1" and r.status == "matched"
    # row with a non-matching MU but region 8 still resolves by region fallback
    r2 = match_rows([{"water": "Long Lake", "region": "REGION 8", "mu": ""}], reg)[0]
    assert r2.item_id == "g:2"
    # genuinely ambiguous (region matches both would-be; here different regions so fine) -> ensure
    # a name with two same-region candidates stays ambiguous
    reg3 = {"a": _it("a", "Twin Lake", mus=("5-1",)), "b": _it("b", "Twin Lake", mus=("5-2",))}
    amb = match_rows([{"water": "Twin Lake", "region": "REGION 5", "mu": ""}], reg3)[0]
    assert amb.item_id is None and amb.status == "ambiguous"


def test_lone_candidate_cross_region_not_matched():
    # Only "White River" in the registry is the Region 4 one (MU 4-24). A Region 1 row (MU 1-10) must
    # NOT bind to it: its MU shares nothing with 4-24, so it needs a curator override.
    reg = {"g:r4": _it("g:r4", "White River", mus=("4-24",))}
    r = match_rows([{"water": "White River", "region": "REGION 1", "mu": "1-10"}], reg)[0]
    assert r.item_id is None and r.status == "unmatched"
    # But when the row's MU matches the item, it resolves.
    same = match_rows([{"water": "White River", "region": "REGION 4", "mu": "4-24"}], reg)[0]
    assert same.item_id == "g:r4" and same.status == "matched"


def test_mu_gate_requires_shared_mu_when_ambiguous():
    # Two same-name streams (both region 1) in different MUs. A reg needing 1-5 shares neither -> can't
    # disambiguate -> needs an override (unmatched). A reg needing 1-3 overlaps g:a -> matches.
    reg = {"g:a": _it("g:a", "Alpha Creek", mus=("1-1", "1-2", "1-3")),
           "g:b": _it("g:b", "Alpha Creek", mus=("1-7",))}
    miss = match_rows([{"water": "Alpha Creek", "region": "REGION 1", "mu": "1-5"}], reg)[0]
    assert miss.item_id is None and miss.status == "unmatched"
    hit = match_rows([{"water": "Alpha Creek", "region": "REGION 1", "mu": "1-3"}], reg)[0]
    assert hit.item_id == "g:a" and hit.status == "matched"


def test_mu_gate_strict_lone_candidate_no_overlap_unmatched():
    # STRICT: even the ONLY same-name item must share the reg's MU. A row needing 1-5 against an item
    # tagged 1-1/1-2/1-3 does not overlap -> unmatched (needs override). A row needing 1-3 -> matches.
    reg = {"g:a": _it("g:a", "Alpha Creek", mus=("1-1", "1-2", "1-3"))}
    miss = match_rows([{"water": "Alpha Creek", "region": "REGION 1", "mu": "1-5"}], reg)[0]
    assert miss.item_id is None and miss.status == "unmatched"
    hit = match_rows([{"water": "Alpha Creek", "region": "REGION 1", "mu": "1-3"}], reg)[0]
    assert hit.item_id == "g:a" and hit.status == "matched"


def test_mu_gate_item_without_mus_matches_but_warns(caplog):
    # A lone item with no MUs on record can't be gated, so it still matches a row that names an MU,
    # but the matcher must warn — a missing MU column signals incomplete registry data.
    reg = {"g:x": _it("g:x", "Nomu Lake")}
    with caplog.at_level("WARNING"):
        r = match_rows([{"water": "Nomu Lake", "region": "REGION 2", "mu": "2-9"}], reg)[0]
    assert r.item_id == "g:x" and r.status == "matched"
    assert any("NO MUs on record" in rec.message for rec in caplog.records)


def test_lone_candidate_haida_gwaii_mu_overlap_still_matches():
    # Haida Gwaii waters are printed in the Region 1 synopsis but carry Region 6 MUs. The row MU
    # overlaps the item MU, so the cross-region guard must NOT block the match.
    reg = {"g:hg": _it("g:hg", "Yakoun River", mus=("6-13",))}
    r = match_rows([{"water": "Yakoun River", "region": "REGION 1", "mu": "6-13"}], reg)[0]
    assert r.item_id == "g:hg" and r.status == "matched"


def test_override_typed_id_skip_and_variant_alias():
    # gnis override resolves via the id_index (ref_ids bridge); variant_of (no skip) resolves the
    # CORRECTED name by name; skip drops the row.
    reg = {"wbk:100": _it("wbk:100", "Long Lake", mus=("1-5",), ref_ids=("gnis:17501", "wbk:100")),
           "g:heber": _it("g:heber", "Heber River", mus=("1-9",), ref_ids=("gnis:heber",))}
    overrides = [
        {"type": "override", "criteria": {"name_verbatim": "LONG LAKE", "region": "1", "mus": ["1-5"]},
         "gnis_ids": ["17501"]},
        {"type": "override", "criteria": {"name_verbatim": "HEBER CREEK", "region": "1", "mus": []},
         "variant_of": {"name_verbatim": "Heber River"}},
        {"type": "override", "criteria": {"name_verbatim": "SOME DITCH", "region": "1", "mus": []},
         "skip": True, "skip_reason": "not a waterbody"},
    ]
    rows = [
        {"water": "Long Lake (Nanaimo)", "region": "REGION 1", "mu": "1-5"},
        {"water": "Heber Creek", "region": "REGION 1", "mu": "1-9"},
        {"water": "Some Ditch", "region": "REGION 1", "mu": ""},
    ]
    res = match_rows(rows, reg, overrides)
    assert res[0].item_id == "wbk:100" and res[0].status == "override" and res[0].via == "override"
    assert res[1].item_id == "g:heber" and res[1].status == "matched" and res[1].via == "override_alias"
    assert res[2].item_id is None and res[2].status == "skip"


def test_override_skip_with_variant_of():
    reg = {"g:1": _it("g:1", "Heber River", ref_ids=("gnis:1",))}
    overrides = [{"type": "override", "criteria": {"name_verbatim": "HEBER CREEK", "region": "1", "mus": []},
                  "skip": True, "skip_reason": "In-season name correction",
                  "variant_of": {"name_verbatim": "HEBER RIVER"}}]
    r = match_rows([{"water": "Heber Creek", "region": "REGION 1", "mu": ""}], reg, overrides)[0]
    assert r.status == "skip" and "variant_of HEBER RIVER" in r.reason


def test_override_dead_id_no_name_fallback():
    # An override whose typed id lands NO named item must NOT fall back to name matching (that would
    # reintroduce the ambiguity the override was created to fix). It becomes a feature_pin (deferred to
    # the resolver, which has the full graph) with the curated id preserved in unresolved_ids.
    reg = {"g:1": _it("g:1", "X River", ref_ids=("gnis:1",))}
    overrides = [{"type": "override", "criteria": {"name_verbatim": "X RIVER", "region": "3", "mus": []},
                  "waterbody_keys": ["999999999"]}]
    r = match_rows([{"water": "X River", "region": "REGION 3", "mu": ""}], reg, overrides)[0]
    assert r.item_id is None and r.status == "feature_pin" and r.via == "override_feature"
    assert r.unresolved_ids == ("wbk:999999999",)


def test_override_nameless_oxbow_pins_are_feature_pins():
    # The Okanagan-oxbows pattern: a curated override lists many wsc for nameless side-channels that
    # have no named registry item. The matcher defers all of them to the resolver, never guessing.
    reg = {"g:okanagan": _it("g:okanagan", "Okanagan River", ref_ids=("gnis:okanagan",))}
    overrides = [{"type": "override",
                  "criteria": {"name_verbatim": "OKANAGAN RIVER OXBOWS", "region": "8", "mus": ["8-9"]},
                  "fwa_watershed_codes": ["300-432687-461418", "300-432687-463105"]}]
    r = match_rows([{"water": "Okanagan River Oxbows", "region": "REGION 8", "mu": "8-9"}], reg, overrides)[0]
    assert r.status == "feature_pin" and len(r.unresolved_ids) == 2


def test_id_index_self_identity_beats_an_inherited_ref():
    """Every Fraser side channel carries the mainstem's gnis on its name tuples, so `gnis:39325`
    appears in 15 items' ref_ids. Plain first-writer-wins handed it to whichever was built first
    (Annacis Channel, ONE section), so the Fraser overrides pinned the river onto a side channel."""
    from pipeline.matching.matcher import build_id_index
    from pipeline.models import RegistryItem

    reg = {
        "gnis:10494": RegistryItem(id="gnis:10494", name="Annacis Channel", kind="stream",
                                   section_ids=("a",), ref_ids=("gnis:10494", "gnis:39325")),
        "gnis:39325": RegistryItem(id="gnis:39325", name="Fraser River", kind="stream",
                                   section_ids=tuple(f"f{i}" for i in range(9)),
                                   ref_ids=("gnis:39325",)),
    }
    idx = build_id_index(reg)
    assert idx["gnis:39325"] == "gnis:39325"
    assert idx["gnis:10494"] == "gnis:10494"
