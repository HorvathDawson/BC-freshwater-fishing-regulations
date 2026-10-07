"""THE GUIDE'S PROSE EXAMPLES, DECLARED AS DATA AND CHECKED AGAINST THE READER (CLEAN round,
2026-10-05; user ask).

The guide's ladder texts argue by example — "Clearwater Lake's 'No Fishing Nov 1-Apr 30' silences
Zone B's 'Burbot: 5'" — and an example is a claim about the reference reader
(`read.effective_rules`): on that water, on that day, for that fish, these rules speak and those do
not. Written only as prose, three were wrong when this was built: the Thompson's "2 per day" said to
speak in June (inside Region 3's spring stream closure), Kakwa Lake's 2 said to outrank the zone's
"Trout/char: 5" (both speak: different statements), and Denetiah Creek's "own bull trout rule" (the
creek prints none).

So every such example is declared here — water (`item_id`), date, fish, the rule ids that SPEAK and
the ones that must be SILENT (or a stated `state`) — and:

  * `examples(bundle)` asks the reader for each and returns them with its answer (`expect`); the
    export ships them as `guide.examples` and REFUSES to write when a declared example disagrees
    with the reader (`export_ui_rules.problems` -> `example_problems`);
  * `prose_gaps(guide, names)` finds every water the ladder and gotcha prose names (by the bundle's
    water names) and refuses one that no example cites from that text and that is not listed in
    `NAMED_NOT_CLAIMED` with the reason it is no reader claim. A new example in prose therefore
    cannot ship unchecked.

The section an example is asked on is the first of its water (lowest handle) whose rule set holds
every rule of `speaks` and `part` — the same section for every bundle that has that part.
"""
from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

Z2, Z3, Z4 = ("z2:trout_char_quota::trout_char_quota", "z3:trout_char_quota::trout_char_quota",
              "z4:trout_char_quota::trout_char_quota")
Z5, Z6, Z7A, Z7B, Z8 = ("z5:trout_char_quota::trout_char_quota",
                        "z6:trout_char_quota::trout_char_quota",
                        "z7a:trout_char_quota::trout_char_quota",
                        "z7b:trout_char_quota::trout_char_quota",
                        "z8:trout_char_quota::trout_char_quota")
THOMPSON = ("r3:thompson_river_downstream_of_signs_at_kamloops_lake_outlet_t@3-13+3-14+3-18::"
            "thompson_river_downstream_of_kamloops_lake")
CHILLIWACK = ("r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::"
              "chilliwack_vedder_rivers")
KOOTENAY = ("r4:kootenay_lake_main_body_for_location_see_map_on_page_34@4-19::"
            "kootenay_lake_main_body")
SHUSWAP = "r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26::shuswap_lake"
KITIMAT = "r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3::kitimat_river"
LIARD = "r7:liard_river_watershed_see_map_on_page_63@7-53::liard_river_watershed"
Z6_STEELHEAD_CLOSURE = "z6:steelhead_stream_closure::steelhead_stream_closure.r1"


@dataclass(frozen=True)
class Example:
    id: str
    cited_in: tuple            # guide keys whose prose makes the claim ("ladder.closures")
    water: str                 # item_id
    date: str                  # "MM-DD"
    fish: str                  # a leaf code
    speaks: tuple = ()         # rule ids that speak
    silent: tuple = ()         # rule ids absent from the answer
    states: dict = field(default_factory=dict)   # rule id -> another state ("not_yet_mapped")
    part: tuple = ()           # extra rule ids the asked section's rule set must hold
    says: str = ""             # the claim, in a few words
    named_as: str = ""         # the name the prose uses, where it is not the item's own (a lake
                               # part: "Kootenay Lake" for "Kootenay Lake — Main Body")


E = Example
EXAMPLES: tuple[Example, ...] = (
    # ---- ladder.closures ----------------------------------------------------------------------
    E("clearwater_closure_silences_burbot", ("ladder.closures",), "wbk:329472485", "11-15", "BB",
      ("r7:clearwater_lake@7-31::clearwater_lake.r1",), ("z7b:species_quotas::species_quotas.r1",),
      says="the lake's closure silences Zone B's 'Burbot: 5'"),
    E("okanagan_bass_lifts_region_8_closure", ("ladder.closures",), "gnis:32069", "07-01", "SMB",
      ("r8:okanagan_river@8-1::okanagan_river.r2",), ("z8:species_quotas::species_quotas.r1",),
      says="'bass daily quota = 8' lifts Region 8's bass closure"),
    E("west_road_row_never_reopens_steelhead_closure", ("ladder.closures",), "gnis:26104",
      "06-01", "ST", (Z6_STEELHEAD_CLOSURE,),
      says="Region 6's steelhead stream closure speaks on West Road's Region 6 pieces"),
    E("denetiah_closure_silences_the_liard_row", ("ladder.closures", "ladder.closure_any_key"),
      "gnis:39298", "07-05", "DV", ("r7:denetiah_creek@7-52::denetiah_creek.r1",),
      (f"{LIARD}.r2", f"{LIARD}.r3", f"{Z7B}.r1", f"{Z7B}.r3"),
      says="the creek's own 'No fishing, Jul 1-15' silences the Liard River watershed row's "
           "bull trout 1 a day and 1 in possession, and Zone B's quotas (DENETIAH ruling)"),
    E("kitimat_hatchery_steelhead_does_not_reopen", ("ladder.closures",), "gnis:3225", "06-01",
      "ST", (Z6_STEELHEAD_CLOSURE,), part=(f"{KITIMAT}.r5",),
      says="a closure printing its own exemption list takes no derived lift"),
    # ---- ladder.counted_apart / quotas_sit_beside --------------------------------------------
    E("kootenay_rainbow_10", ("ladder.counted_apart", "ladder.quotas_sit_beside",
                              "gotchas.size_clause_override"), "wbk:-20", "07-01", "RB",
      (f"{KOOTENAY}.r4",), (f"{Z4}.r1", f"{Z4}.r2"),
      says="the lake's 10 (any size) lifts Region 4's 5 and its 1 over 50 cm for rainbow",
      named_as="Kootenay Lake"),
    E("tranquille_rainbow_8", ("ladder.counted_apart",), "wbk:329563842", "07-01", "RB",
      ("r3:tranquille_lake@3-29::tranquille_lake.r1",), (f"{Z3}.r1",)),
    E("tranquille_kokanee_10", ("ladder.quotas_sit_beside", "ladder.counted_apart"),
      "wbk:329563842", "07-01", "KO", ("r3:tranquille_lake@3-29::tranquille_lake.r2",),
      ("z3:species_quotas::species_quotas.r4",), says="the lake's 10 replaces Region 3's 5"),
    E("lois_lake_6_in_the_aggregate", ("ladder.counted_apart", "ladder.quotas_sit_beside"),
      "wbk:329197058", "07-01", "RB", ("r2:lois_lake@2-12::lois_lake.r4",), (f"{Z2}.r1",)),
    E("khartoum_lake_6_in_the_aggregate", ("ladder.counted_apart",), "wbk:329197063", "07-01",
      "RB", ("r2:khartoum_lake@2-12::khartoum_lake.r5",), (f"{Z2}.r1",)),
    E("teslin_grayling_4", ("ladder.quotas_sit_beside",), "wbk:328961703", "07-01", "GR",
      ("r6:teslin_lake@6-25::teslin_lake.r4",), ("z6:species_quotas::species_quotas.r1",)),
    E("shuswap_annual_quota_one_limit", ("ladder.quotas_sit_beside",), "wbk:329518145", "07-01",
      "RB", (f"{SHUSWAP}.r8",), ("z3:shuswap_annual::shuswap_annual.r1",),
      says="the zone line naming Shuswap and the row's annual 5 are one limit; the row's speaks"),
    E("perry_creek_brook_trout_20", ("ladder.quotas_sit_beside",), "gnis:14741", "07-01", "EB",
      ("r4:perry_creek@4-20::perry_creek.r3",), (f"{Z4}.r1",)),
    E("jewel_lake_brook_trout_20", ("ladder.quotas_sit_beside", "gotchas.size_clause_override"),
      "wbk:329216614", "07-01", "EB", ("r8:jewel_lake@8-14::jewel_lake.r1",),
      (f"{Z8}.r1", f"{Z8}.r2")),
    E("polley_trout_8_replaces_the_5", ("ladder.quotas_sit_beside",), "wbk:329480772", "07-01",
      "RB", ("r5:polley_lake@5-2::polley_lake.r1",), (f"{Z5}.r1", f"{Z5}.r2")),
    E("polley_keeps_the_dolly_varden_clause", ("ladder.quotas_sit_beside",), "wbk:329480772",
      "07-01", "DV", ("r5:polley_lake@5-2::polley_lake.r1", f"{Z5}.r4")),
    E("williston_lake_trout_3_lifts_the_2", ("ladder.quotas_sit_beside",), "wbk:-19", "07-01",
      "LT", ("r7:williston_lake_in_zone_b@7-31+7-36::williston_lake_zone_b.r5",), (f"{Z7B}.r4",),
      named_as="Williston Lake"),
    E("duncan_rainbow_5_lifts_clauses", ("ladder.quotas_sit_beside",), "gnis:22255", "07-01", "RB",
      ("r4:duncan_river@4-19::duncan_river.r4", f"{Z4}.r1"), (f"{Z4}.r2", f"{Z4}.r3")),
    E("gwillim_lake_trout_sizes", ("ladder.quotas_sit_beside", "gotchas.size_clause_override"),
      "wbk:329397725", "07-01", "LT", ("r7:gwillim_lake@7-21::gwillim_lake.r1", f"{Z7B}.r1"),
      (f"{Z7B}.r2",)),
    E("quesnel_lake_trout_5_any_size", ("gotchas.size_clause_override",), "wbk:329480766",
      "07-01", "LT", ("r5:quesnel_lake@5-15::quesnel_lake.r3",), (f"{Z5}.r2", f"{Z5}.r5")),
    E("kitimat_hatchery_rainbow_partly_lifts", ("ladder.quotas_sit_beside",), "gnis:3225",
      "07-01", "RB", (f"{KITIMAT}.r4", f"{Z6}.r1")),
    E("dean_1_beside_the_5", ("ladder.quotas_sit_beside",), "gnis:16075", "07-01", "RB",
      ("r5:dean_river@5-9::dean_river.r6", f"{Z5}.r1")),
    E("kitimat_hatchery_steelhead_beside_the_5", ("ladder.quotas_sit_beside",), "gnis:3225",
      "07-01", "ST", (f"{KITIMAT}.r5", f"{Z6}.r1")),
    E("dodd_2_beside_the_4", ("ladder.quotas_sit_beside",), "wbk:329197061", "07-01", "RB",
      ("r2:dodd_lake@2-12::dodd_lake.r1", f"{Z2}.r1")),
    # ---- ladder.dated_zone_release / gotchas.dated_zone_release_stands / who_speaks ----------
    E("cheslatta_own_dates_override_nov_15", ("ladder.dated_zone_release", "ladder.who_speaks",
                                              "gotchas.dated_zone_release_stands"),
      "wbk:329075502", "11-15", "LT",
      ("r6:cheslatta_lake@6-4::cheslatta_lake.r2", f"{Z6}.r1", f"{Z6}.r3"), (f"{Z6}.r8",)),
    E("cheslatta_own_release_oct_1", ("ladder.dated_zone_release",), "wbk:329075502", "10-01",
      "LT", ("r6:cheslatta_lake@6-4::cheslatta_lake.r1",), (f"{Z6}.r8",)),
    E("murray_own_dates_override_nov_15", ("ladder.dated_zone_release",), "wbk:329075503",
      "11-15", "LT", ("r6:murray_lake@6-4::murray_lake.r2",), (f"{Z6}.r8",)),
    E("michel_creek_own_release_jan_15", ("ladder.dated_zone_release",), "gnis:28953", "01-15",
      "RB", ("r4:michel_creek_upstream_of_the_easternmost_hwy_3_bridge@4-23::michel_creek_upper.r2",),
      ("z4:trout_char_winter_release::trout_char_winter_release.r1",)),
    E("shuswap_dated_zone_release_stands", ("ladder.dated_zone_release",
                                            "gotchas.dated_zone_release_stands"),
      "wbk:329518145", "11-01", "LT", (f"{SHUSWAP}.r9", f"{Z3}.r7")),
    # ---- ladder.moot_size_clause ---------------------------------------------------------------
    E("bonaparte_moot_60cm", ("ladder.moot_size_clause",), "wbk:329054723", "11-01", "LT",
      (f"{Z3}.r7",), (f"{Z3}.r4b",)),
    E("griffin_water_release_silences_60cm", ("ladder.moot_size_clause",), "wbk:329518152",
      "11-01", "LT", ("r3:griffin_lake@3-34::griffin_lake.r1",), (f"{Z3}.r7", f"{Z3}.r4b")),
    # ---- ladder.not_yet_mapped / gotchas.not_yet_mapped -----------------------------------------
    E("kinbasket_undrawn_closure", ("ladder.not_yet_mapped", "gotchas.not_yet_mapped"),
      "wbk:328961767", "07-01", "RB", (f"{Z4}.r1",),
      states={"r4:kinbasket_mcnaughton_lake@4-36::kinbasket_lake.r1": "not_yet_mapped"}),
    # ---- ladder.region / two_regions ------------------------------------------------------------
    E("ahbau_two_tables_identical_once", ("ladder.region", "ladder.two_regions"), "wbk:329097613",
      "07-01", "RB", (f"{Z5}.r1", f"{Z5}.r2"), (f"{Z7A}.r1", f"{Z7A}.r2")),
    E("mara_both_tables", ("ladder.region",), "wbk:329518146", "07-01", "RB",
      (f"{Z3}.r1", "z8:possession_quota::possession_quota.r1")),
    # ---- ladder.same_row_release ----------------------------------------------------------------
    E("thompson_cnr_may_release", ("ladder.same_row_release",), "gnis:39492", "05-15", "RB",
      (f"{THOMPSON}.r3",), (f"{THOMPSON}.r2",)),
    E("thompson_cnr_june_spring_closure", ("ladder.same_row_release",), "gnis:39492", "06-15",
      "RB", ("z3:spring_stream_closure::spring_stream_closure.r1",), (f"{THOMPSON}.r2",),
      part=(f"{THOMPSON}.r3",)),
    E("thompson_cnr_july_the_2", ("ladder.same_row_release",), "gnis:39492", "07-01", "RB",
      (f"{THOMPSON}.r2",), part=(f"{THOMPSON}.r3",)),
    E("adams_lake_release_over_its_1", ("ladder.same_row_release",), "wbk:329014384", "11-01",
      "LT", ("r3:adams_lake@3-37::adams_lake.r2",), ("r3:adams_lake@3-37::adams_lake.r3",)),
    E("big_lake_release_over_its_1", ("ladder.same_row_release",), "wbk:329480768", "11-01", "LT",
      ("r5:big_lake_approx_30_km_west_of_likely@5-2::big_lake_likely.r2",),
      ("r5:big_lake_approx_30_km_west_of_likely@5-2::big_lake_likely.r1",)),
    E("sulphurous_release_over_its_1", ("ladder.same_row_release",), "wbk:329060774", "11-01",
      "LT", ("r5:sulphurous_lake@5-1::sulphurous_lake.r2",),
      ("r5:sulphurous_lake@5-1::sulphurous_lake.r1",)),
    E("koocanusa_size_clause_holds_through_release", ("ladder.same_row_release",),
      "wbk:328961702", "11-01", "DV", ("r4:koocanusa_reservoir@4-2+4-22+4-3::koocanusa_reservoir.r1",
                                       "r4:koocanusa_reservoir@4-2+4-22+4-3::koocanusa_reservoir.r2"),
      named_as="Koocanusa"),
    E("quatse_closure_silences_its_quota", ("ladder.same_row_release",), "gnis:32101", "05-15",
      "ST", ("r1:quatse_river@1-13::quatse_river.r1",), ("r1:quatse_river@1-13::quatse_river.r2",)),
    # ---- ladder.size_release_vs_size_clause / who_speaks ----------------------------------------
    E("lakelse_size_release", ("ladder.size_release_vs_size_clause", "ladder.who_speaks"),
      "wbk:329310750", "07-01", "RB", ("r6:lakelse_lake@6-11::lakelse_lake.r1", f"{Z6}.r1"),
      (f"{Z6}.r2",)),
    E("chilko_size_rule_leaves_the_2", ("ladder.who_speaks",), "wbk:329079684", "07-01", "RB",
      ("r5:chilko_lake@5-4::chilko_lake.r1", "r5:chilko_lake@5-4::chilko_lake.r2"),
      named_as="Chilko Lake"),
    # ---- ladder.steelhead_definition / steelhead_without_rules ----------------------------------
    E("vedder_hatchery_rainbow_release_may", ("ladder.steelhead_definition",), "gnis:3062",
      "05-15", "RB", (f"{CHILLIWACK}.r6",), (f"{Z2}.r1",), named_as="Chilliwack River"),
    E("okanagan_steelhead_is_a_rainbow", ("ladder.steelhead_definition",
                                          "ladder.steelhead_without_rules"),
      "gnis:32069", "07-01", "ST", ("r8:okanagan_river@8-1::okanagan_river.r5",), (f"{Z8}.r1",)),
    E("khartoum_lake_carries_the_release_twin", ("ladder.steelhead_definition",), "wbk:329197063",
      "07-01", "ST", (f"{Z2}.r7b",)),
    E("tenas_lake_is_not_steelhead_water", ("ladder.steelhead_definition",), "wbk:329021804",
      "09-01", "ST", (f"{Z5}.r1",),
      ("zp:steelhead::steelhead.r1b", "zp:steelhead::steelhead.r2b", f"{Z5}.r6b"),
      says="a steelhead row's closure binds it, but its own row prints no steelhead (user ruling "
           "2026-10-06): a big rainbow is a rainbow under Region 5's trout quota"),
    E("gravel_slough_is_a_stream", ("ladder.steelhead_definition",), "gnis:8009", "07-01", "ST",
      (f"{Z2}.r3", f"{Z2}.r7")),
    E("maria_slough_is_a_stream", ("ladder.steelhead_definition",), "gnis:13499", "07-01", "ST",
      (f"{Z2}.r7",)),
    # ---- ladder.water_release -------------------------------------------------------------------
    E("coquihalla_release_silences_hatchery_steelhead", ("ladder.water_release",), "gnis:19983",
      "12-01", "ST", ("r2:coquihalla_river@2-17::coquihalla_river.r6",), (f"{Z2}.r3",)),
    E("vedder_hatchery_cutthroat_release_may", ("ladder.water_release",), "gnis:3062", "05-15",
      "CT", (f"{CHILLIWACK}.r7",), (f"{Z2}.r1",), named_as="Chilliwack River"),
    E("morris_wild_release_leaves_the_4", ("ladder.water_release",), "wbk:329177943", "07-01",
      "RB", ("r2:morris_lake@2-19::morris_lake.r1", f"{Z2}.r1")),
    E("pine_release_silences_2_from_streams", ("ladder.water_release",), "gnis:23217", "07-01",
      "DV", (f"{Z7B}.r9",), (f"{Z7B}.r3",), part=("r7:pine_river@7-32::pine_river.r1",)),
    E("adams_river_release_over_60cm", ("ladder.water_release",), "gnis:39257", "07-01", "LT",
      ("r3:adams_river_downstream_of_adams_lake@3-37::adams_river_downstream.r2",),
      (f"{Z3}.r4b",)),
    # ---- ladder.who_speaks ----------------------------------------------------------------------
    E("kakwa_bull_trout_zone_release", ("ladder.who_speaks",), "wbk:329525854", "07-01", "DV",
      (f"{Z7B}.r9",), ("r7:kakwa_lake@7-19::kakwa_lake.r2",)),
    E("kakwa_other_trout_lake_2_beside_5", ("ladder.who_speaks",), "wbk:329525854", "07-01", "RB",
      ("r7:kakwa_lake@7-19::kakwa_lake.r2", f"{Z7B}.r1")),
    E("williston_bull_trout_row_names_the_fish", ("ladder.who_speaks",), "wbk:-19", "07-01", "DV",
      ("r7:williston_lake_in_zone_b@7-31+7-36::williston_lake_zone_b.r4",), (f"{Z7B}.r9",),
      named_as="Williston Lake"),
    E("duck_lake_row_beats_cvwma", ("ladder.who_speaks",), "wbk:329246292", "07-01", "SMB",
      ("r4:duck_lake_permit_required_see_note_on_page_34@4-6::duck_lake.r1",),
      ("r4:creston_valley_wildlife_management_area_cvwma_waters@4-6::creston_valley_wma_waters.r1",)),
)

#: Waters the ladder and gotcha prose NAME without making a claim about the reader's answer there —
#: each with why. `prose_gaps` refuses a named water that is neither cited by an example nor here.
NAMED_NOT_CLAIMED: dict[tuple[str, str], str] = {
    ("ladder.region", "Shuswap Lake"): "a pointer row's target ('See Shuswap Lake in Region 3')",
    ("ladder.same_row_release", "Kamloops Lake"): "a place ('the Thompson below Kamloops Lake')",
    ("ladder.steelhead_definition", "Cowichan River"): "presence on the curated list "
                                                       "(steelhead_source) — test_steelhead_waters",
    ("ladder.who_speaks", "Bowron Lake"): "names an area row ('Bowron Lake Park waters')",
    ("ladder.who_speaks", "Liard River"): "names an area row ('the Liard River watershed')",
    ("ladder.closure_any_key", "Liard River"): "names an area row ('the Liard River watershed "
                                               "row'); the claim is Denetiah Creek's example",
    ("ladder.who_speaks", "Peace River"): "names a watershed ('the Peace River watershed')",
}

#: The prose this gate reads: the ladder's texts and the gotchas' `says`.
SCOPES = ("ladder", "gotchas")


def _section(db, ex: Example) -> int | None:
    need = sorted(set(ex.speaks) | set(ex.part))
    q = ("SELECT MIN(s.sid) FROM item i JOIN item_section s ON s.ord = i.ord "
         "JOIN section_ruleset sr ON sr.sid = s.sid WHERE i.item_id = ?")
    args: list = [ex.water]
    for rid in need:
        e, r = rid.split("::", 1)
        q += (" AND sr.set_id IN (SELECT set_id FROM ruleset WHERE entry_id = ? AND rule_id = ?)")
        args += [e, r]
    return db.execute(q, args).fetchone()[0]


def examples(bundle: Path | str) -> dict:
    """{id: example as shipped}: the declaration and the reader's answer (`expect`) on its section
    — `expect` is None where no section of the water carries the rules it names."""
    from pipeline.deliver.bundle import read as RD
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    names = dict(db.execute("SELECT item_id, name FROM item"))
    out = {}
    try:
        for ex in EXAMPLES:
            sid = _section(db, ex)
            m, d = (int(x) for x in ex.date.split("-"))
            expect = None if sid is None else [
                {"id": f"{x['entry']}::{x['rule']}", "state": x["state"],
                 **({"partly_lifted": True} if x.get("partly_lifted") else {})}
                for x in RD.effective_rules(sid, (m, d), ex.fish, str(bundle))]
            out[ex.id] = {"cited_in": list(ex.cited_in),
                          "water": {"item_id": ex.water, "name": names.get(ex.water)},
                          **({"named_as": ex.named_as} if ex.named_as else {}),
                          "date": ex.date, "fish": ex.fish, "says": ex.says,
                          "speaks": list(ex.speaks), "silent": list(ex.silent),
                          **({"states": dict(ex.states)} if ex.states else {}),
                          **({"part": list(ex.part)} if ex.part else {}),
                          "expect": expect}
    finally:
        db.close()
    return out


def example_problems(shipped: dict) -> list[str]:
    """Every shipped example whose declaration the reader's answer contradicts."""
    out = []
    for i, x in sorted(shipped.items()):
        if x["expect"] is None:
            out.append(f"guide example {i}: no section of {x['water']['item_id']} carries "
                       f"{sorted(set(x['speaks']) | set(x.get('part') or []))}")
            continue
        state = {a["id"]: a["state"] for a in x["expect"]}
        for r in x["speaks"]:
            if state.get(r) != "speaks":
                out.append(f"guide example {i}: {r} does not speak ({state.get(r)})")
        for r in x["silent"]:
            if r in state:
                out.append(f"guide example {i}: {r} is not silent ({state[r]})")
        for r, s in (x.get("states") or {}).items():
            if state.get(r) != s:
                out.append(f"guide example {i}: {r} is {state.get(r)}, not {s}")
    return out


def _texts(guide: dict):
    for scope in SCOPES:
        for k, v in (guide.get(scope) or {}).items():
            if scope == "ladder" and isinstance(v, str):
                yield f"ladder.{k}", v
            elif scope == "gotchas" and isinstance(v, dict) and isinstance(v.get("says"), str):
                yield f"gotchas.{k}", v["says"]


def prose_mentions(guide: dict, water_names) -> set[tuple[str, str]]:
    """(guide key, water name) for every water the ladder or gotcha prose names (names of two words
    or more, so 'the Fraser' alone is not matched — a gate on what can be found, not more)."""
    names = sorted({n for n in water_names if n and len(n.split()) >= 2}, key=len, reverse=True)
    if not names:
        return set()
    pat = re.compile(r"\b(" + "|".join(map(re.escape, names)) + r")\b")
    return {(k, m.group(1)) for k, t in _texts(guide) for m in pat.finditer(t)}


def prose_gaps(guide: dict, water_names, shipped: dict, item_names: dict) -> list[str]:
    """A water the prose names that no example cites from that text, and that is not listed as
    named-but-not-claimed."""
    cited = {(k, n) for x in shipped.values() for k in x["cited_in"]
             for n in (item_names.get(x["water"]["item_id"]) or x["water"]["name"],
                       x.get("named_as")) if n}
    return sorted(f"guide prose {k} names {n!r} but no declared example checks it "
                  f"(pipeline/tools/guide_examples.py)"
                  for k, n in prose_mentions(guide, water_names)
                  if (k, n) not in cited and (k, n) not in NAMED_NOT_CLAIMED)
