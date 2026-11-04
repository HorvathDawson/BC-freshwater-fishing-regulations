# T387 — Every tap on a part gives the same page (access warnings first)

Status: OPEN (owner 2026-11-04, in 0.5.0).

## Ask (owner 2026-11-04)
"the issue with nicomen slough is not all of them when clicked will get the land use warnings so yes it is one section
but in the one section not all clicks are equal." — "we need to make sure similar things dont happen for other items
other then landuse."

## Found
Access is per SECTION (`section_access`: sid, area, permille). Nicomen Slough (gnis:21105, one part, 9 sections: 5 line,
4 polygon) has Skumalasph 16 (reserve) and a private parcel on 4 of its 9 sections; a tap on the other 5 shows none.
Province-wide (bundle 2026-11-04): 20,700 parts, 7,101 touch an access area, 1,972 of them on only some of their
sections. Pooled per part: median 1 area, p99 3, max 7 (Similkameen, Fraser part 3) — no bloat.

## Work
1. Audit every per-section fact a Regulations page (and its sheets) shows on a tap: access (restricted note, no-access
   banner), licensing, region / MU / place words, tidal, salmon section, "Tributary of" / down, steelhead, closures by
   section, the sun line's point, … — which vary between sections of ONE part.
2. Each becomes PART-level on the page (the same for every tap on the part; access: the part's areas, each with how much
   of the part it covers) unless it is truly about the tapped point (list those with why; the owner sees the list).
3. A net on the live release: for every part with more than one section, the Regulations page built from each of its
   sections is equal (minus the listed point-bound facts) — with mutation pins.
4. App-side where the release has the data (no rebuild); anything needing the pipeline rides the one 0.5.0 rebuild.
