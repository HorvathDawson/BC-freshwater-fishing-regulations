# T1250 — Pacific streams of the steelhead regions are steelhead water

Status: IN PROGRESS — every owner answer is in and built; left: the final side build's measure, then the PR

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1250 | Pacific streams of the steelhead regions are steelhead water | IN PROGRESS | — | the final side build (barrier list + S15), the every-day comparison; the release-backed pins and fixtures re-cut on the 0.10.0 candidate |

Source: the Shelagyote feeders trace (19 creeks became steelhead-known through T1146; owner 2026-11-25 "Accept") and
his follow-up: "accept but i am a bit concerned about this for other streams that might have missed becoming a
streamhead water. maybe we should rethink that? how did it actually change ruling etc", then, asked with the option
sheets A-D and E: "should we just say all streams have rainbows over 50 as steelhead? we dont want to give wrong info
about being able to keep a "steelhead" they are tightly regulated", and the ruling (2026-11-27): **"Pacific streams
now, barriers in 0.11.0 (Recommended)"**, "In 0.10.0"; the stamp line's wording stays as it is.

## Work
1. `pipeline/atlas/reach/steelhead.py`: `PACIFIC_STREAMS_ARE_STEELHEAD_WATER` (RULINGS S14). A "possible" stream of
   Regions 1, 2, 3, 5, 6 whose FWA basin reaches the Pacific (`pacific_basin`: the code's first group 100, 300, 400,
   500, 600, 700 or a Coastal Rivers group 900-995) is known, source "Pacific stream". What the curated barrier list
   `data/curated/waters/steelhead_barriers.json` holds out stays "possible" (`basin` and `upstream_of` rows; the
   Mackenzie, the Yukon, the Columbia, the Skagit above Ross Dam); no code stays "possible" and is counted; a basin
   neither Pacific nor held out stops the run. Lakes stay out. S15: a water row's rainbow cap holds for its steelhead.
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
- **Buckley Creek (Region 6, 9 sections, 13.9 km, Jul 1-Oct 31):** a hatchery fish over 50 cm went release -> keep 1
  under S14 alone; ruled "Release: the row's cap holds", built as S15 (the release stands).

## Acceptance
The data tests pass on the final side build; the every-day comparison has nothing looser but the Regions 1-2 hatchery
line; the T1053 release-backed pins and the T666 fixtures are re-cut on the 0.10.0 candidate.

Follow-up: T1257 (0.11.0) — dams and falls as barriers (a curated barrier list; the planning chat writes it).
Depends: none. Size: M. Owner: answered (below). Default: as built.

## Owner
- [x] 2026-11-27 — Buckley Creek: under S14 a hatchery fish over 50 cm there can be kept (1, Jul 1-Oct 31), where its
  row says "none over 50 cm" for rainbow. Answer, 2026-11-27: "Release: the row's cap holds (Recommended)" (S15).
- [x] 2026-11-27 — The Columbia basin (FWA 300: 661 sections, about 756 km, the Similkameen and its feeders, Shrimpton
  Creek in Region 3) and the Skagit (FWA 970: 2,893 sections, 2,877 km above Ross Dam) are cut off from the sea by dams:
  Pacific streams now, or held out (a basin entry with its reason) until T1257? Answer, 2026-11-27: "Hold both but make
  sure it will be compatible with a larger barrier list" (the barrier list, its two kinds of row).
- [ ] 2026-11-27 — His general words, "Also if rainbows over 50cm are ever referred to it always implies the rule is on
  steelhead too": measured as a follow-up (S12's amendment; S15 is its first case), built only on his word after the sheet.
  The over-50 sheet, 2026-11-27 (RULINGS S12a, verbatim): Kitimat's own 2 hatchery steelhead stand, Muchalat as built,
  Regions 1-2 is Q16. Found then, not T1250's (main the same): Kitimat's hatchery steelhead are released Nov 1-Jun 30
  (the region's "Trout of any size from streams" release; its rainbow row lifts it, its steelhead row does not).
