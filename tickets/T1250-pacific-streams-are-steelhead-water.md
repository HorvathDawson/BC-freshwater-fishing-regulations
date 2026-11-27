# T1250 — Pacific streams of the steelhead regions are steelhead water

Status: IN PROGRESS — built on fix/T1250-pacific-streams-steelhead (not pushed); left: the side build's answers diff

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1250 | Pacific streams of the steelhead regions are steelhead water | IN PROGRESS | — | the side build's reach + deliver run, the answers diff per region, the release-backed pins re-cut on the 0.10.0 candidate |

Source: the Shelagyote feeders trace (19 creeks became steelhead-known through T1146; owner 2026-11-25 "Accept") and
his follow-up: "accept but i am a bit concerned about this for other streams that might have missed becoming a
streamhead water. maybe we should rethink that? how did it actually change ruling etc", then, asked with the option
sheets A-D and E: "should we just say all streams have rainbows over 50 as steelhead? we dont want to give wrong info
about being able to keep a "steelhead" they are tightly regulated", and the ruling (2026-11-27): **"Pacific streams
now, barriers in 0.11.0 (Recommended)"**, "In 0.10.0"; the stamp line's wording stays as it is.

## Work
1. `pipeline/atlas/reach/steelhead.py`: `PACIFIC_STREAMS_ARE_STEELHEAD_WATER` (RULINGS S14). A "possible" stream of
   Regions 1, 2, 3, 5, 6 whose FWA basin reaches the Pacific (`pacific_basin`: the code's first group 100, 300, 400,
   500, 600, 700 or 9xx) is known, source "Pacific stream". 200 (Mackenzie) and 800 (Yukon) stay "possible"; no code
   stays "possible" and is counted (`possible_no_basin_code`); an unknown group is refused. Lakes stay out (S3).
2. The bundle (`rules/presence.py`) and the export (`read.py`, `steelhead.py`, the guide) carry the third source.
3. POLICY_VERSION 55 (52-54: open branches). RULINGS S4-S6 + S14, AGENTS 54, OWNER-RULINGS.
4. Tests: `test_steelhead_streams.py` (the unit pass with basins, its mutation, the basin table, the refusal; the data
   tests: an interior Cariboo stream is a Pacific stream, the Dease / French / Gladys stay "possible", Kamloops Lake and
   the Kechika get nothing, the Horsefly above its falls is known).

## Measured before the build (main r-7d7848399e53; laneN option E sheet)
675,793 possible stream sections today; S14 makes 614,962 known (the Arctic / Yukon 60,831 stay). A wild fish over 50
cm stops being a keepable rainbow on about 511,500 km on Aug 1 (Regions 3, 5, 6). Looser: only the Regions 1-2
hatchery fish over 50 cm, the rainbow's "1 over 50 cm" -> the hatchery steelhead's "2 over 50 cm" (the steelhead row
on the same card already says 2). The side build's answers diff replaces these estimates.

## Acceptance
The data tests pass on the side build; the answers diff shows nothing looser but the Regions 1-2 hatchery line; the
T1053 release-backed pins and the T666 fixtures are re-cut on the 0.10.0 candidate (they need the candidate release).

Follow-up: T1257 (0.11.0) — dams and falls as barriers (a curated barrier list; the planning chat writes it).
Depends: none. Size: M. Owner: no (ruled). Default: as built.
