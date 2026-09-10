# Region 2 — Lower Mainland

Transcribed verbatim from the 2025-2027 synopsis Region 2 chapter. **Source text — do not edit
to fit the model.** Companion to `definitions.md` and `provincial-regulations.md`.

---

## General Regulations

**No Fishing:** in any lake in the UBC Malcolm Knapp Research Forest near Maple Ridge.

**Single barbless hook:** must be used in all streams of Region 2, all year. See definition of
"angling" and "streams" on page 80.

**Dead fin fish as bait:** only permitted in Region 2 when sport fishing for sturgeon in the
Fraser River, Lower Pitt River (CPR Bridge upstream to Pitt Lake), Lower Harrison River (Fraser
River upstream to Harrison Lake). See page 8 for details.

**Steelhead fishing in the Lower Mainland Region:** Your basic licence must be validated with a
Conservation Surcharge Stamp if you fish for steelhead anywhere in B.C. When you have caught and
retained your daily quota of hatchery steelhead from any water, you must stop fishing that water
for the remainder of that day.

**Protected Species:** it is illegal to fish for or catch and then keep protected species. In
Region 2, these include: Nooksack dace · Salish sucker · Green sturgeon · Cultus Lake sculpin.

> **NOTICE TO ANGLERS** The area known as the **Rubble Creek Landslide Hazard Area** is a high
> risk slide area. People who fish in this area do so at THEIR OWN RISK.

## Region 2 Daily Quotas

(See tables for exceptions)

```
Trout/char: 4, but not more than
  • 1 over 50 cm (2 hatchery steelhead over 50 cm allowed)
  • 2 from streams (must be hatchery)
  • 1 char (bull trout, Dolly Varden, or lake trout), none under 60 cm
And you must release:
  • Wild trout/char from streams
  • All wild steelhead
  • Hatchery trout/char under 30 cm from streams

NOTE: There is no general minimum size limit for trout in lakes

Bass:           20 (excluding Mill Lake - see page 24 for quota)
Crappie:        20
Crayfish:       25
Kokanee:        5 (none from streams)
Whitefish:      15 (all species combined)
White Sturgeon: CATCH AND RELEASE ONLY
  • Fraser River: CLOSED TO ALL FISHING in the Fraser areas of Jesperson's Side Channel,
    Herrling Island Side Channel and Seabird Island north Side Channel, May 15-July 31
```

## Possession Quotas

Possession quotas = 2 daily quotas

## Annual Quotas

Annual catch quota for all B.C.: 10 steelhead per licence year (only hatchery steelhead may be
retained in B.C.)

## Stave River Steelhead Marking

> Maxillary (jaw) clipping is being used to assess hatchery release strategies. Refer to page 76
> for instructions on what to do if you catch a maxillary clipped fish.

## Note — night closure

> From one hour after sunset to one hour before sunrise fishing is prohibited on portions of the
> Fraser, Harrison, and Pitt Rivers (see Water-specific Tables for details).

---

## Checked against the curated entries

`data/curated/regulations/entries/zones/region-2.json` — 20 entries, 21 rules.

**Well covered.** Every quota, sub-limit and release rule is present and correct, including the
three easy ones to lose: the *"2 hatchery steelhead over 50 cm allowed"* carve-out, the char
sub-limit's *"none under 60 cm"*, and the *"release hatchery trout/char under 30 cm from
streams"* clause. Species codes for the protected list resolve (`NDC`, `SSU`, `GSG`, `CCL`).

### Missing — in the synopsis, not anywhere in the corpus

| # | rule | note |
|---|---|---|
| 1 | **Rubble Creek Landslide Hazard Area notice** | a safety advisory, verbatim-only. Nothing in the bundle mentions Rubble Creek. |
| 2 | **Stave River Steelhead Marking** (maxillary clipping) | the Region 2 parallel of `z6:r6_tagging_program`; absent. Same class as Region 1's missing Cutthroat Trout Reward Tagging Program. |

### Present, but filed somewhere else

| # | rule | where it actually is |
|---|---|---|
| 3 | **No Fishing in any lake in the UBC Malcolm Knapp Research Forest** | `zp:ubc_malcolm_knapp_research_forest`, in the **provincial** file. It is a Region 2 rule printed in the Region 2 chapter. Harmless as long as it binds, but it is filed by geography rather than by the chapter it came from. |
| 4 | **Dead fin fish as bait, sturgeon only, three named rivers** | now `zp:finfish_bait.r2` (provincial, authored from p.8) and `r2:fraser_river_upstream_of_the_cpr_bridge_at_mission@2-4` at water level. The Region 2 chapter restates it; no separate `z2:` rule is needed. |
| 5 | **Fraser River side-channel closure**, May 15 – Jul 31 | `r2:fraser_river_upstream_of_the_cpr_bridge_at_mission@2-4`, *"No fishing in named side channels"* — correctly at **water** level. The chapter line is a cross-reference, not a second rule. |
| 6 | **Night closure**, one hour after sunset to one hour before sunrise | three **water** rules: `r2:fraser_river…@2-4`, `r2:harrison_river…@2-18`, `r2:pitt_river@2-8`. The chapter note says *"see Water-specific Tables for details"* — it is a pointer, and dropping it loses nothing. **Confirmed droppable.** |

### Note

`z2:protected_species` lists **Green sturgeon**, which does **not** appear on the provincial
protected list on p.9. Either the province's list is not exhaustive or Region 2 adds to it —
worth one look at the printed page, because `zp:protected_species` is authored as
superior-authority and a species missing from it will never be protected outside Region 2.
