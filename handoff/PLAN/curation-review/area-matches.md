# Area matches — sweep (2026-10-26)

Owner: an entry that names an area (park, ecological reserve, protected area, national park, conservancy,
WMA, "waters within …") must bind through that area. Correction the same day: an entry whose rules ALREADY
carry an area extent is fine — its "no registry" look was a review-app display problem. Only entries whose
rules have NO area extent get a data fix.

## Display fix (no data change)
`pipeline.regs.review.tiers.area_matched`: no registry item + every rule has an area extent = MATCHED BY
AREA. The queue and the entry say "matched by area: <area>"; it is not `no_registry`, not `no match`, not
`binds_nothing`, so not high severity. Six water rows are matched by area (all already bind in the reach run):

| entry | area |
|---|---|
| r1:strathcona_park_waters@1-9 | area:park:strathcona_park |
| r4:creston_valley_wildlife_management_area_cvwma_waters@4-6 | area:wma:creston_valley_wildlife_management_area |
| r4:kikomun_creek_park_all_lakes_in_the_park@4-22 | area:park:kikomun_creek_park |
| r5:bowron_lake_park_waters_other_than_bowron_lake@5-16 | area:park:bowron_lake_park |
| r6:chilkoot_trail_national_historic_park_waters@6-28 | area:chilkoot_trail:chilkoot_trail_national_historic_site |
| r7:liard_river_watershed_see_map_on_page_63@7-53 | area:watershed:liard_river |

These are exactly the six overrides with `admin_targets` in overrides.json.

## Data fixes
None needed. Every water row whose name or override names an area already has an area extent on every rule.

## Checked and left (not area entries)
Rows that name a park only to say WHERE one water is — they match that one water and bind it whole, which is
right: Deer Lake / Trout Lake (Sasquatch Park), Lightning Lake (Manning Park), Jerry Sulina Park Pond, Sardis
Park Pond, Trout Creek (Wells Gray Park), Arrow Park (Mosquito) Creek, Bear River (Sustut Watershed), Bear /
Hart / Square lakes (Crooked River Provincial Park).

## Unsure
- Rows with no item and no rules (pointers: Arrow Lakes, Seven Mile / Waneta reservoir tributaries, Endako
  River, the r5 "@6-1" lakes) are not area entries; untouched.
- The Region 5/6 `not_found` lakes (Frog, Hidden, Redfern, Secret, Square, Squirrel) still bind nothing —
  water rows, not areas; out of this sweep.
- Zone (`z…`) entries are areas by definition and were never shown as no_registry; not swept.
