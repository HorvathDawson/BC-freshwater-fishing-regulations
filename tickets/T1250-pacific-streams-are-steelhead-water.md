# T1250 — Pacific streams of the steelhead regions are steelhead water

Status: IN PROGRESS — built and measured on fix/T1250-pacific-streams-steelhead; HELD for the owner's two answers below

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1250 | Pacific streams of the steelhead regions are steelhead water | IN PROGRESS — held for the owner | — | his answers on Buckley Creek and on the Columbia / Skagit basins; the release-backed pins and fixtures re-cut on the 0.10.0 candidate |

Source: the Shelagyote feeders trace (19 creeks became steelhead-known through T1146; owner 2026-11-25 "Accept") and
his follow-up: "accept but i am a bit concerned about this for other streams that might have missed becoming a
streamhead water. maybe we should rethink that? how did it actually change ruling etc", then, asked with the option
sheets A-D and E: "should we just say all streams have rainbows over 50 as steelhead? we dont want to give wrong info
about being able to keep a "steelhead" they are tightly regulated", and the ruling (2026-11-27): **"Pacific streams
now, barriers in 0.11.0 (Recommended)"**, "In 0.10.0"; the stamp line's wording stays as it is.

## Work
1. `pipeline/atlas/reach/steelhead.py`: `PACIFIC_STREAMS_ARE_STEELHEAD_WATER` (RULINGS S14). A "possible" stream of
   Regions 1, 2, 3, 5, 6 whose FWA basin reaches the Pacific (`pacific_basin`: the code's first group 100, 300, 400,
   500, 600, 700 or a Coastal Rivers group 900-995) is known, source "Pacific stream". 200 (Mackenzie) and 800 (Yukon)
   stay "possible"; no code (or FWA's 999) stays "possible" and is counted; an unknown group is refused. Lakes stay out.
2. The bundle (`rules/presence.py`) and the export (`read.py`, `steelhead.py`, the guide) carry the third source.
3. POLICY_VERSION 55 (53 the walk, 54 the DFO branch). RULINGS S4-S6 + S14, AGENTS 54, OWNER-RULINGS.
4. Tests: `test_steelhead_streams.py` (the unit pass with basins, its mutation, the basin table, the refusal; the data
   tests: an interior Cariboo stream is a Pacific stream, the Dease / French / Gladys stay "possible", Kamloops Lake and
   the Kechika get nothing, the Horsefly above its falls is known). The mini world gains Northward Creek (FWA 200) so
   it keeps one "possible" stream for the export's case.

## Measured on the side build (main r-7d7848399e53 -> r-0dbad1dad6b3, T1250 only)
Rule bindings and licensing placements identical. 614,962 stream sections become known (the Arctic / Yukon 60,831
stay "possible"). A wild fish over 50 cm becomes a release on 511,506 km on Aug 1 (R3 100,177, R5 123,012, R6 288,273).
Not stricter or the same, every day checked (the review re-ran all 406 key pairs):
- Regions 1-2 hatchery fish over 50 cm: "1 over 50 cm" -> "2 over 50 cm" (R1 62,504 km, R2 50,310 km on Aug 1); the
  card's steelhead row already says 2 (the regional box, pp.9 and 17).
- **Buckley Creek (Region 6, 9 sections, 13.9 km, Jul 1-Oct 31): FOUND, NOT APPROVED.** A hatchery fish over 50 cm goes
  release -> keep 1: its row's "Rainbow trout 1 per day (none over 50 cm)" stays a rainbow line (S12). Awaits him.

## Acceptance
The data tests pass on the side build; the answers diff has nothing looser but the two cases above, Buckley Creek
answered by him; the T1053 release-backed pins and the T666 fixtures are re-cut on the 0.10.0 candidate.

Follow-up: T1257 (0.11.0) — dams and falls as barriers (a curated barrier list; the planning chat writes it).
Depends: none. Size: M. Owner: yes (two answers). Default: as built.

## Owner
- [ ] 2026-11-27 — Buckley Creek: under S14 a hatchery fish over 50 cm there can be kept (1, Jul 1-Oct 31), where its
  row says "none over 50 cm" for rainbow. Keep as built, or let a water's own rainbow cap also cap the hatchery
  steelhead (option (a), sketched in the lane's notes)? Default: as built (held, no auto-merge).
- [ ] 2026-11-27 — The Columbia basin (FWA 300: 661 sections, about 756 km, the Similkameen and its feeders, Shrimpton
  Creek in Region 3) and the Skagit (FWA 970: 2,893 sections, 2,877 km above Ross Dam) are cut off from the sea by dams:
  Pacific streams now, or held out (a basin entry with its reason) until T1257? Default: Pacific, as built.
