"""Every provincial and regional gear rule, written in the new shape. What will not fit is named."""
from pipeline.regs.parsing.catalogue import (GearClause as G, Slot as S, GearWhen as W,
                                             GearSpec as Spec, Conduct as C, Method, WaterKind,
                                             AnglerState)

STREAM = W(water=WaterKind.stream)
SINGLE_BARBLESS = [G(slot=S.points_per_hook, max=1, when=STREAM),
                   G(slot=S.barb, allow=[], when=STREAM)]

FITS = {
 # --- the inversion, gone: this is now byte-identical to the 9 regional twins --------------
 "zp:barbless_single_hook_streams.r1": [G(slot=S.barb, allow=[], when=STREAM)],
 "zp:barbless_single_hook_streams.r3": [G(slot=S.points_per_hook, max=1, when=STREAM)],
 "z1:single_barbless_hook.r1": SINGLE_BARBLESS,
 # --- bait: a TOTAL ban and a PARTIAL one stop being one record ----------------------------
 "z1:bait_ban_streams.r1": [G(slot=S.bait, allow=[], when=STREAM)],
 "zp:bait.r1": [G(slot=S.bait, of=["fin_fish"], allow=[])],
 "zp:bait.r2": [G(slot=S.bait, of=["dead_fin_fish"], allow=["dead_fin_fish"],
                  when=W(method=Method.set_lining, water=WaterKind.lake))],
 "zp:bait.r3": [G(slot=S.bait, of=["dead_fin_fish"], allow=["dead_fin_fish"],
                  when=W(targeting=["WSG"]))],
 "zp:bait.r5": [G(slot=S.bait, of=["invertebrate"], allow=[], when=W(water=WaterKind.lake))],
 "zp:bait.r6": [G(slot=S.bait_possession_kg, of=["roe"], max=1,
                  unless=[W(note="roe from a commercial source that lawfully obtained it"),
                          W(note="you hold the freshly dressed fish the excess was taken from")])],
 # --- terminal tackle: the exception ordered first, and the OR the old field destroyed ------
 "zp:terminal_tackle.r1+r2": [G(slot=S.lines_per_angler, max=2,
                                when=W(water=WaterKind.lake, angler=AnglerState.alone_in_boat)),
                              G(slot=S.lines_per_angler, max=1)],
 "zp:terminal_tackle.r3": [G(slot=S.flies_per_line, max=1)],
 "zp:terminal_tackle.r4": [G(slot=S.weight_per_line_kg, max=1,
                             unless=[W(gear_in_use="downrigger_weight")])],
 "zp:terminal_tackle.r6": [G(slot=S.terminal_attachments_per_line, max=1,
                             members=["hook", "artificial_lure", "artificial_fly"])],
 # --- methods ------------------------------------------------------------------------------
 "zp:spear_fishing.r3": [G(slot=S.method, of=["spear_fishing"], allow=[])],
 "zp:prohibited_methods.r1": [G(slot=S.method, of=["netting"], allow=[])],
 "zp:prohibited_methods.r2": [G(slot=S.method, of=["snagging"], allow=[])],
 "zp:crayfish_trapping.r1": [G(slot=S.method, of=["crayfish_trapping"],
                               allow=["crayfish_trapping"])],
 "z7b:set_lining.r1": [G(slot=S.method, of=["set_lining"], allow=[])],
 "zp:set_lining.r2": [G(slot=S.lines_per_angler, max=1, when=W(method=Method.set_lining)),
                      G(slot=S.hooks_per_line, max=1, when=W(method=Method.set_lining)),
                      G(slot=S.hook_gap_mm, min=30, when=W(method=Method.set_lining))],
 # --- properties of the thing the clause names, not separate conditions -------------------
 "zp:allowable_methods.r1": [G(slot=S.method, of=["downrigger"], allow=["downrigger"],
                               requires=Spec(attached_to="fishing_line",
                                             attachment="quick_release"))],
 "zp:terminal_tackle.r5": [G(slot=S.light, allow=["light"],
                             requires=Spec(submerged=True, attached_to="fishing_line",
                                           within_m_of_hook=1)),
                           G(slot=S.light, allow=[])],
 "z8:crayfish_traps_turtles.r2": [G(slot=S.method, of=["crayfish_trapping"],
                                    allow=["crayfish_trapping"],
                                    requires=Spec(opening_shape="circular",
                                                  note="minimally-sized; the book prints no number"))],
 # --- a permission the scope ladder already settles ----------------------------------------
 "zp:bait.r4": [G(slot=S.bait, of=["invertebrate"], allow=["invertebrate"],
                  when=W(water=WaterKind.stream))],
 "zp:allowable_methods.r2": [G(slot=S.lines_per_angler, max=1, when=W(method=Method.ice_fishing)),
                             G(slot=S.terminal_attachments_per_line, max=1,
                               members=["lure", "artificial_fly", "terminal_attractor"],
                               when=W(method=Method.ice_fishing))],
}

CONDUCT = {
 # --- duties and prohibitions, direction in the KEY -----------------------------------------
 "zp:set_lining.r4": [C(must="mark_gear", **{"with": ["angler_name", "address", "telephone"]},
                        when=W(method=Method.set_lining))],
 "z5:ice_fishing_huts.r1": [C(must="remove_ice_hut", by="spring_breakup")],
 "zp:allowable_methods.r3": [C(must="warn_others_of_ice_hole"),
                             C(must="remove_ice_hut", by="ice_breakup")],
 "zp:prohibited_methods.r4": [C(must_not="chum")],
 "zp:further_prohibitions.r1": [C(must_not="gear_in_water_during_closure")],
 "zp:further_prohibitions.r2": [C(must_not="interfere_with_furbearer_trap")],
 "zp:conduct.r1": [C(must_not="waste_catch")],
 "zp:conduct.r2": [C(must_not="release_harmfully")],
 "zp:conduct.r3": [C(must_not="sell_catch")],
 "zp:conduct.r4": [C(must_not="possess_or_move_live_fish")],
 "zp:conduct.r5": [C(must="leave_head_tail_fins_on", by="permanent_residence")],
 "zp:protected_species.r3": [C(must="release_immediately")],
 "zp:preparing_catch.r1": [C(must_not="can_bottle_or_fillet",
                             note="except at your permanent residence")],
 "zp:preparing_catch.r2": [C(must_not="freeze_in_unrecognizable_block")],
}

DOES_NOT_FIT = {
 "zp:set_lining.r1":
   ('"You may ONLY fish with a set line in lakes of Region 6 and Region 7A." The permission binds '
    'to two extents; the EXCLUSION of everywhere else is not writable, because `extents` has no '
    'negation. Ten-odd Zone A lakes already carry their own "no set lines" rule because the '
    'complement could not be said once. This is the only one left.'),
}

#: NOT A GAP, though it looked like one. `zp:bait.r4` ends "…unless a bait ban applies", and that
#: is the SCOPE LADDER restating itself: a water-specific bait ban is narrower than a
#: province-wide permission and already outranks it. The clause needs no reference to "a bait
#: ban" because nothing would read it — the rule that wins is decided by where each was written.
SETTLED_BY_PRECEDENCE = ("zp:bait.r4",)

if __name__ == "__main__":
    n = sum(len(v) for v in FITS.values()) + sum(len(v) for v in CONDUCT.values())
    print(f"{len(FITS)} gear rules and {len(CONDUCT)} conduct rules FIT — {n} clauses, all validated")
    print(f"{len(DOES_NOT_FIT)} DO NOT:\n")
    for k, why in DOES_NOT_FIT.items():
        print(f"  {k}\n    {why}\n")
