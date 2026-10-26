"""T89 — RULES THAT BIND NO WATER. Every rule ends bound, or unresolved with a typed reason (AGENTS
14); this file pins WHICH rules may end unresolved, so a new rule that binds nowhere fails here
instead of vanishing from every screen (memory `unplaced-extent-rules`: on a closure, silence reads
as permission).

The four fixed in T89 (curated catalogue, 2026-10-22):
  * `nanaimo_river.r5` "Artificial fly only upstream of the westernmost of the two Nanaimo Lakes,
    known locally as "Second" Lake": `upstream_of` the Nanaimo River's boundary on wbk:329101307 —
    the WESTERN of the two (measured on the atlas, below) and the one FWA's variants call "SECOND
    NANAIMO LAKE".
  * `nilkitkwa_lake.r4` "Rainbow Alley … is open for fishing all year": the Babine River between
    Babine and Nilkitkwa lakes, the SAME stretch `babine_river.r3` names (item_id gnis:17687).
  * `fulton_river.r3` "Angling permitted in Fulton River opposite the spawning channels, as
    signed": a part of the Fulton nothing draws — `undrawn_part` beside the whole river, a note,
    never a colour; it does not walk the row's tributaries (AGENTS 13).
  * `r7:endako_river@7-12`: the row printed IN FULL under Region 7A as well as Region 6, of a river
    FWA puts wholly in Region 6 (20 of 20 sections). A per-region row would bind nothing in 7A, so
    the copy is the Region 6 row's TWIN (`See`, as the seven MU 6-1 lakes under Region 5): the
    closure shows on every Endako section through `r6:endako_river@6-4`.

Left unresolved, AWAITING A WATER (owner question, T89 ticket): eight rows whose printed MU holds no
registry water of that name — every same-name candidate is 75+ km away in another MU (Mackenzie
Creek 4-21, Frog/Hidden/Secret/Square Lake 5-6, Redfern Lake 5-15, Squirrel Lake 6-1; Squirrel's
Region 5 twin already points at the Region 6 row). Binding them to a same-name lake elsewhere would
put a quota on the wrong water; they need the lake located and named in the registry
(`name_variants.json`, an atlas registry rebuild).
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED, GENERATED
from pipeline.regs.parsing.catalogue import CatalogueEntry
from pipeline.regs.parsing.catalogue_parts.see import see_relation
from pipeline.tests.conftest import ATLAS_HINT, BUNDLE_HINT, need

CAT = Path(CURATED.regulations.entries.catalogue)

#: BY DESIGN — a rule that binds nowhere and is right to.
BY_DESIGN = frozenset({
    # the steelhead quota twins: their siblings bind every book-known steelhead water in the area (z1 trout r5b,
    # z5 r6b and z6 r9b left this list in T153: a steelhead river's unnamed widening is that river's lake water)
    ("z1:hg_quota", "hg_quota.r6b"),
    ("z3:daily_quotas", "trout_char_quota.r5b"),
    # working exclusions: the Columbia / Kootenay tributary rows carry no tributary rule of their own
    ("r4:kinbasket_mcnaughton_lake_s_tributaries@4-36", "kinbasket_lake_tributaries.r2"),
    ("r4:kootenay_lake_s_tributaries@4-19+4-7", "kootenay_lake_tributaries.r2"),
})

#: AWAITING A WATER — the row's printed MU holds no registry water of its name (owner, T89 Q-A).
AWAITING_WATER_ROWS = {
    # entry_id: (name as the registry would carry it, printed MU, rule ids)
    "r4:mackenzie_creek@4-21": ("Mackenzie Creek", "4-21", ("mackenzie_creek.r1",)),
    "r5:frog_lake@5-6": ("Frog Lake", "5-6", ("frog_lake.r1", "frog_lake.r2", "frog_lake.r3",
                                             "frog_lake.r4")),
    "r5:hidden_lake@5-6": ("Hidden Lake", "5-6", ("hidden_lake.r1", "hidden_lake.r2",
                                                 "hidden_lake.r3", "hidden_lake.r4")),
    "r5:redfern_lake@5-15": ("Redfern Lake", "5-15", ("redfern_lake.r1", "redfern_lake.r2",
                                                     "redfern_lake.r3")),
    "r5:secret_lake@5-6": ("Secret Lake", "5-6", ("secret_lake.r1", "secret_lake.r2",
                                                 "secret_lake.r3", "secret_lake.r4")),
    "r5:square_lake@5-6": ("Square Lake", "5-6", ("square_lake.r1", "square_lake.r2",
                                                 "square_lake.r3")),
    "r6:squirrel_lake@6-1": ("Squirrel Lake", "6-1", ("squirrel_lake.r1", "squirrel_lake.r2",
                                                     "squirrel_lake.r3")),
}
AWAITING_WATER = frozenset((e, r) for e, (_, _, rids) in AWAITING_WATER_ROWS.items() for r in rids)

#: Every rule allowed to end unresolved. 26 = 4 by design + 22 awaiting a water (T153 bound 3 steelhead twins).
ALLOWED_UNRESOLVED = BY_DESIGN | AWAITING_WATER


def unresolved_problems(found: set, allowed: frozenset = ALLOWED_UNRESOLVED) -> list[str]:
    """What is wrong with the set of unresolved `(entry_id, rule_id)` a run produced: a rule that
    binds nowhere and is not allowed to, or an allowed one that now binds (the list is stale —
    take it out, so the pin stays exact)."""
    out = [f"binds no water and is not on the T89 allow-list: {e}::{r}"
           for e, r in sorted(set(found) - allowed)]
    out += [f"on the T89 allow-list but now binds — remove it from the list: {e}::{r}"
            for e, r in sorted(allowed - set(found))]
    return out


def _entries(region: str) -> dict:
    return {e["entry_id"]: e for e in json.loads((CAT / f"region-{region}.json").read_text())["entries"]}


def _rule(region: str, entry_id: str, rule_id: str) -> dict:
    got = [r for r in _entries(region)[entry_id].get("rules") or [] if r["rule_id"] == rule_id]
    assert len(got) == 1, f"{entry_id}::{rule_id}"
    return got[0]


def _place(rule: dict) -> dict:
    """The fields that say WHERE a rule holds — compared whole."""
    keys = ("extents", "includes_tributaries", "extent_text", "unresolved_locators", "undrawn_part")
    return {k: rule[k] for k in keys if k in rule}


# ---------------------------------------------------------------------------------------------
# The allow-list itself (no data): exact, both ways — the key rule, mutation-pinned
# ---------------------------------------------------------------------------------------------
def test_the_allow_list_is_exact_26():
    assert len(BY_DESIGN) == 4 and len(AWAITING_WATER) == 22 and len(ALLOWED_UNRESOLVED) == 26
    assert unresolved_problems(set(ALLOWED_UNRESOLVED)) == []


def test_a_new_rule_binding_nowhere_fails():
    """MUTATION: one more unresolved rule (the Endako's Region 7 closure, as it was) is refused."""
    found = set(ALLOWED_UNRESOLVED) | {("r7:endako_river@7-12", "endako_river.r1")}
    assert unresolved_problems(found) == [
        "binds no water and is not on the T89 allow-list: r7:endako_river@7-12::endako_river.r1"]


def test_an_allowed_rule_that_now_binds_makes_the_list_stale():
    """MUTATION: Frog Lake's quota bound (its lake located) — the list must shrink with it."""
    found = set(ALLOWED_UNRESOLVED) - {("r5:frog_lake@5-6", "frog_lake.r2")}
    assert unresolved_problems(found) == [
        "on the T89 allow-list but now binds — remove it from the list: "
        "r5:frog_lake@5-6::frog_lake.r2"]


# ---------------------------------------------------------------------------------------------
# The four fixed rows, as curated (no data: the checked-in catalogue)
# ---------------------------------------------------------------------------------------------
def test_nanaimo_fly_only_is_upstream_of_the_second_nanaimo_lake():
    assert _place(_rule("1", "r1:nanaimo_river@1-5", "nanaimo_river.r5")) == {
        "extents": [{"op": "upstream_of", "splits": ["nanaimo_river__nanaimo_lakes_2"]}],
        "includes_tributaries": True,
    }


def test_rainbow_alley_binds_the_babine_between_the_two_lakes_as_the_babine_row_does():
    babine = _rule("6", "r6:babine_river@6-8", "babine_river.r3")
    assert babine["extents"] == [{"op": "between",
                                  "splits": ["babine_river__babine_lake",
                                             "babine_river__nilkitkwa_lake"]}]
    assert _place(_rule("6", "r6:nilkitkwa_lake@6-8", "nilkitkwa_lake.r4")) == {
        "extents": [{"op": "between", "item_id": "gnis:17687",
                     "splits": ["babine_river__babine_lake", "babine_river__nilkitkwa_lake"]}],
    }


def test_fulton_opposite_the_spawning_channels_is_drawn_and_does_not_walk():
    """T207 (owner 2026-10-26): the stretch lies between the owner's two points — drawn, never a trib walk."""
    assert _entries("6")["r6:fulton_river@6-8"]["includes_tributaries"] is True
    assert _place(_rule("6", "r6:fulton_river@6-8", "fulton_river.r3")) == {
        "extents": [{"op": "between", "splits": ["fulton_river__spawning_channels_downstream_end",
                                                 "fulton_river__spawning_channels_upstream_end"]}],
        "includes_tributaries": False,
    }


def test_endako_region_7_row_is_the_region_6_rows_twin():
    e7 = _entries("7")["r7:endako_river@7-12"]
    e6 = _entries("6")["r6:endako_river@6-4"]
    assert {k: e7.get(k) for k in ("matched", "rules", "see")} == {
        "matched": None, "rules": None,
        "see": [{"verbatim": "No Fishing Nov 15-June 30", "entry_ids": ["r6:endako_river@6-4"]}],
    }
    assert e6["matched"] == ["gnis:17338"]
    assert [r["verbatim"] for r in e6["rules"]] == ["No Fishing Nov 15-June 30"]
    m7, m6 = CatalogueEntry.model_validate(e7), CatalogueEntry.model_validate(e6)
    assert see_relation(m7, [m6]) == "twin"


def scope_misses(entry: dict) -> list[str]:
    """Rule extents that name a water OUTSIDE the row's own scope — which the reach run then
    clips to nothing (`build._clip` keeps only the sections of the entry's scope; the result is
    `empty_after_scope`). A row whose rule binds a water it does not match must name that water
    in its scope too, as Upper Arrow Lake (drawdown area) names the Columbia. An AREA scope
    (`within`) clips by place, not by water, and is not checked here."""
    scope = [x for x in entry.get("extents") or [] if isinstance(x, dict)]
    if not scope or all(x.get("op") == "within" for x in scope):
        return []
    matched = set(entry.get("matched") or ())
    in_scope = {x["item_id"] if x.get("item_id") else i
                for x in scope for i in ([x.get("item_id")] if x.get("item_id") else matched)}
    return [f"{entry['entry_id']}::{r['rule_id']} binds {x['item_id']}, outside its row's scope"
            for r in entry.get("rules") or [] for x in r.get("extents") or []
            if x.get("item_id") and x["item_id"] not in in_scope]


def test_no_rule_binds_a_water_its_rows_scope_clips_away():
    """Every rule extent naming another water lies inside its row's scope (corpus-wide)."""
    got = [m for f in sorted(CAT.glob("*.json"))
           for e in json.loads(f.read_text())["entries"] for m in scope_misses(e)]
    assert got == []


def test_rainbow_alley_is_in_the_nilkitkwa_rows_scope():
    """The row's scope is the lake AND Rainbow Alley, as Upper Arrow's is the lake and the
    Columbia. MUTATION: with the lake alone (the row before), the reach run clipped r4 to nothing
    (probe on the live atlas: `empty_after_scope`)."""
    e = _entries("6")["r6:nilkitkwa_lake@6-8"]
    assert e["extents"] == [
        {"op": "whole", "item_id": "wbk:329026685"},
        {"op": "between", "item_id": "gnis:17687",
         "splits": ["babine_river__babine_lake", "babine_river__nilkitkwa_lake"]}]
    assert scope_misses({**e, "extents": [{"op": "whole"}]}) == [
        "r6:nilkitkwa_lake@6-8::nilkitkwa_lake.r4 binds gnis:17687, outside its row's scope"]


def test_caribou_creek_slough_is_in_the_caribou_creek_rows_scope():
    """r1's `whole` of Caribou Creek slough (wbk:329262655, a stream item on lake:329262655) bound
    nothing: the row's bare `whole` scope is the creek alone, and `build._clip` dropped the slough
    (T179 side build: rule_section held only 356563426:1353). MUTATION: the bare scope misses it."""
    e = _entries("4")["r4:caribou_creek@4-15"]
    assert e["extents"] == [{"op": "whole"}, {"op": "whole", "item_id": "wbk:329262655"}]
    assert scope_misses(e) == []
    assert scope_misses({**e, "extents": [{"op": "whole"}]}) == [
        "r4:caribou_creek@4-15::caribou_creek.r1 binds wbk:329262655, outside its row's scope"]


@pytest.mark.parametrize("region,entry_id", [
    ("1", "r1:nanaimo_river@1-5"), ("6", "r6:nilkitkwa_lake@6-8"), ("6", "r6:fulton_river@6-8"),
    ("7", "r7:endako_river@7-12")])
def test_the_edited_rows_pass_the_model(region, entry_id):
    CatalogueEntry.model_validate(_entries(region)[entry_id])


def test_the_awaiting_rows_are_still_unmatched_and_still_say_so():
    """Nothing quietly matched them to a same-name water in another MU."""
    for eid, (_, mu, rids) in AWAITING_WATER_ROWS.items():
        e = _entries(eid.split(":")[0][1:])[eid]
        assert eid.endswith(f"@{mu}") and not e.get("matched"), eid
        assert tuple(r["rule_id"] for r in e["rules"]) == rids, eid


# ---------------------------------------------------------------------------------------------
# Measured on the atlas: why each choice is the right water
# ---------------------------------------------------------------------------------------------
def _build() -> Path:
    return Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())


@pytest.fixture(scope="module")
def registry(request):
    p = need(request, "atlas", _build() / "registry.json", ATLAS_HINT)
    return {i["id"]: i for i in json.loads(p.read_text())["items"]}


@pytest.fixture(scope="module")
def points(request):
    return json.loads(need(request, "atlas", _build() / "item_points.json", ATLAS_HINT).read_text())


@pytest.mark.needs_atlas
def test_the_second_nanaimo_lake_is_the_western_one(registry, points):
    """The Nanaimo River's two "Nanaimo Lakes" boundaries; `_2` is on the western lake, which FWA's
    variants call the second. MUTATION: `nanaimo_river__nanaimo_lakes` is on the eastern one."""
    lakes = {b["id"]: f"wbk:{b['wbk']}" for b in registry["gnis:16505"]["boundaries"]
             if b["label"] == "Nanaimo Lakes"}
    assert lakes == {"nanaimo_river__nanaimo_lakes": "wbk:329101304",
                     "nanaimo_river__nanaimo_lakes_2": "wbk:329101307"}
    west = min(lakes, key=lambda b: points[lakes[b]][0])
    assert west == "nanaimo_river__nanaimo_lakes_2"
    assert "SECOND NANAIMO LAKE" in registry["wbk:329101307"]["variants"]


@pytest.mark.needs_atlas
def test_rainbow_alleys_cuts_are_on_the_babine(registry):
    ids = {b["id"] for b in registry["gnis:17687"]["boundaries"]}
    assert {"babine_river__babine_lake", "babine_river__nilkitkwa_lake"} <= ids
    assert "Rainbow Alley" in registry["gnis:17687"]["variants"]


def _region_sections(registry, region: str) -> set:
    return set(registry[f"area:region:{region}"]["section_ids"])


@pytest.mark.needs_atlas
def test_the_endako_lies_wholly_in_region_6(registry):
    """Why the Region 7A copy is a twin: held to 7A (a per-region row) it would bind nothing."""
    s = set(registry["gnis:17338"]["section_ids"])
    assert len(s) == 20
    assert s <= _region_sections(registry, "6")
    assert not s & (_region_sections(registry, "7a") | _region_sections(registry, "7b"))


def _km(a, b) -> float:
    la = math.radians((a[1] + b[1]) / 2)
    return math.hypot((a[0] - b[0]) * 111.32 * math.cos(la), (a[1] - b[1]) * 110.57)


@pytest.mark.needs_atlas
def test_no_awaiting_row_has_a_water_of_its_name_in_its_mu(registry, points):
    """The default (`Q29`: the candidate in the row's MU) has nothing to take. Every same-name
    candidate lies in another MU, over 75 km from the nearest registry water of the printed MU.
    When a water is named there (name_variants), this fails: match the row and shrink the list."""
    for eid, (name, mu, _) in AWAITING_WATER_ROWS.items():
        want = name.lower()
        cands = [i for i in registry.values() if i.get("kind") != "area"
                 and want in {n.lower() for n in [i.get("name") or "", *(i.get("variants") or ())]}]
        assert not [i["id"] for i in cands if mu in (i.get("mus") or ())], eid
        in_mu = [points[i["id"]] for i in registry.values()
                 if mu in (i.get("mus") or ()) and i["id"] in points]
        for c in cands:
            if c["id"] in points:
                assert min(_km(points[c["id"]], q) for q in in_mu) > 75, (eid, c["id"])


# ---------------------------------------------------------------------------------------------
# The shipped bundle: exactly the allowed rules end unresolved
# ---------------------------------------------------------------------------------------------
@pytest.mark.needs_bundle
def test_only_the_allowed_rules_bind_no_water(request):
    path = need(request, "bundle",
                os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite", BUNDLE_HINT)
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        found = set(con.execute("select entry_id, rule_id from rule where unresolved is not null"))
    finally:
        con.close()
    assert unresolved_problems(found) == []
