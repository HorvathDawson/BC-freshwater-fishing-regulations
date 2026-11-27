# T1246 — Turner Creek is filed under Noons Creek by a code fault; it is its own water

Status: IN REVIEW — built on fix/T1246-city-creeks-own-water; owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1246 | Turner Creek is filed under Noons Creek by a code fault; it is its own water | IN REVIEW — owner: see ## Owner | 9ca5edb50, 17ac1714e | STOPPED at the proof: two answer changes need the owner (## Owner); then the main chat's PR |

Source: the owner, 2026-11-27 (planning chat), verbatim. First "Show me the map first"; after the map: "Make it its own
water (Recommended)". Earlier, on Noons Creek (T1191, Q4): the FWA stream and the city stream of the same name in the
same area both get the Noons Creek regs, for safety. The finding is the data chat's (-c9), 00:45 MST 11-27.

## Cause (corrected)
NOT an alias. The city line for Turner Creek (Port Moody, 2.2 km) carries Noons Creek's own watershed code 900-057239:
the coastal-code estimate gave it that code, and the uniqueness check looks only at other city codes (the known T1150
hole). The registry groups lines by code, so the line joins the Noons item. Turner Creek is a separate creek: its own
mouth on Burrard Inlet about 0.7 km west of Noons Creek, never within about 0.6 km of it. The book's rule: Noons Creek
2-8 Incl. Tribs, "No Fishing upstream of railway bridge".

## Answers today are right (a filing fault only)
Turner Creek and its one tributary carry only Region 2 and provincial rules. The Noons row ("No Fishing upstream of
railway bridge", incl. tributaries) skips the Turner piece through a leave-out row the owner accepted on 2026-11-11
(`unplaced_piece_calls.json`). Nothing opens or closes.

## The fix (not done)
Give Turner its own code in the curated added-streams file and re-code its tributary under it. The tributary's water id
moves (marks and matches resync), and the accepted leave-out row goes stale (it stops the reach run until it is changed
or removed).

## The same fault in two more (T1150's three)
- Tunnel Creek (Burnaby, 0.22 km), filed under the Seymour River; the mouths are 1.07 km apart.
- Capitol Creek (Burnaby, 0.72 km), filed under "Squatters Creek"; the mouths are 0.29 km apart.

## Done (lane M, 2026-11-27; owner: "All three now, in the re-cut")
- `added_streams.json`, by hand (A2, 7 lines, `wsc` / `receiver_wsc` of five records): Turner 900-057239 -> 900-057238,
  Turner Creek Trib.1 -> 900-057238-436900, Tunnel 900-078372 -> 900-078371, Capitol 900-053687 -> 900-053686, Capitol
  Trib.1 -> 900-053686-249200. Each is the nearest Level-2 no FWA line, city line, earlier added-streams file or
  committed id holds (no FWA line is coded under it either).
- His 2026-11-11 Noons/Turner leave-out row removed from `unplaced_piece_calls.json`. Tunnel's Seymour row is KEPT (not
  approved): once Tunnel leaves the Seymour it names no piece, and a stale row stops the reach run (owner line below).
  Capitol has no row.
- Ids: new waters wsc:900-057238 (Turner), wsc:900-078371 (Tunnel), wsc:900-053686 (Capitol); moved:
  wsc:900-057239-436900 -> wsc:900-057238-436900, wsc:900-053687-249200 -> wsc:900-053686-249200 (their two URLs in
  `app/seo/slug-map.json` follow). No owner mark, match, gauge, bathymetry, in-season or name-variant row names a moved
  id; the Sheldens note in `name_variants.json` no longer cites Squatters Creek as a shared code.
- The hole closed: `coastal.estimate_coastal_wsc` bumps past every code an FWA line holds; `validate.fwa_code_collisions`
  refuses by name a city line on an FWA code (atlas build and generator); 7 lines still share one, named in
  `FWA_CODE_SHARED_PENDING` for T1150. Tests: `pipeline/tests/test_added_streams_fwa_codes.py`, `test_coastal.py`.

## Proof: STOPPED, two answers change (bbox side atlases, Port Moody and Burnaby, base vs branch)
Items as planned: new wsc:900-057238 Turner, wsc:900-078371 Tunnel, wsc:900-053686 Capitol; the two tributaries'
ids moved; Noons Creek, the Seymour River and "Squatters Creek" each lose exactly that one section and that one alias;
no other item in either box changes. But two answers change, so the lane stopped before any PR:
1. **Salmon, 3.92 km**: the five sections leave `area:drains_to:pfma_28`, so DFO's "no fishing for salmon in waters of
   Region 2 flowing into Areas 28 and 29" (`2:default:areas-28-29`) no longer reaches them. The drains-to areas are
   found from FWA's own basin mouths (`splits/pfma_drainage.py`), so a city creek with a code of its own is in no
   area: the 27 coastal city creeks that already had their own codes (Suter Brook, Mill, Pigeon creeks ...) are
   outside it today; the three were inside only because they borrowed an FWA river's code.
2. **Tidal, 21 m**: Turner, no longer under the Noons owner limit (T685), takes the coast cut at 21.3 m; its lowest
   piece becomes tidal (federal, no provincial rule), as every other city creek's is. Tunnel and Capitol take none.
Proof harness: a bbox build stops in `registry.outside_area_items` (areas named outside the box), so both sides
ran through one scratch harness that hands that pass only what the box holds. Not run: a reach run on the boxes.

## Report only
25 items answer to a city-supplied name that is not their own (Port Moody 4, Burnaby 15, Squamish 6). Only these 3 are
separate creeks; the other 22 are classed by which lines each item holds, not checked for connection one by one.

## Acceptance
No data change until the owner answers. Then: each of the creeks in his pick is its own water with its own rules, no
answer changes, the leave-out row is changed or removed, marks and matches resync.

Does not block the re-cut. Depends: T1150. Owner: yes (below).

## Owner

- [x] 2026-11-27 — Turner Creek and its two kin (Tunnel Creek, Capitol Creek): (a) fix all three together in T1150, in the hardening release (the data chat's recommendation; the re-cut ships Turner as a part of Noons with its right rules); (b) Turner only now, in the re-cut, removing his 2026-11-11 leave-out row; (c) all three now, in the re-cut. No data change until he answers. → "All three now, in the re-cut" (2026-11-27)
- [ ] 2026-11-27 — The three creeks (3.92 km) leave DFO's Region 2 "flowing into Areas 28 and 29" salmon closure, because a city creek with its own code drains to no Area today (27 others already do not): (a) give city creeks their Area from their own mouth (a fix in `pfma_drainage`; it also closes salmon on the 27, km to measure), (b) ship the three outside it like the 27, (c) hold T1246. Default: none; ask.
- [ ] 2026-11-27 — Turner Creek's lowest 21 m becomes tidal (the coast cut every city creek gets): yes / hold. Default: none; ask.
- [ ] 2026-11-27 — Your 2026-11-11 Seymour leave-out row (Tunnel Creek's piece): it names nothing once Tunnel is its own water, and a stale row stops the reach run. Remove it / keep it (the re-cut's reach run stops). Default: none; ask.
