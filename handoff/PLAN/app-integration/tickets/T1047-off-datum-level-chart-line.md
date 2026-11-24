# T1047 — Under Level, an other-datum station's chart still draws its live levels against the old-datum band

Status: DONE 99d8ac9bc, b15f1165b (lane L28, PR #77; owner answered 2026-11-23); follow-up 66a331eef, 874929277, 6b36d695d (lane L8, 2026-11-24): datum change file (5 rows, all proposed), HYDAT remark proposals, the record put on today's zero for an accepted row. Left: the owner's accept/reject per row, then a clim.json + data release; the this-year line (below).

Source: independent review of PR #77 (T1043), 2026-11-23, item 3; split out by lane L28. Production 2026-11-23:
08HD006 (Salmon River near Sayward) reports a live level of 2.058 m that the publisher leaves out of `reports` (T550:
off the record's rating; the record is near 7 m). The other T550 stations: 07FD001, 08EF001, 08KH019, 08MH153 (each has a
level band, so Level is offered from the band).

## What is fixed already (T1043, 19a8485bb)
The live level is no longer shown as today's reading: no "2.06 m stage" under Flow, and under Level the card says
"Reports flow only today, not water level. Show Flow to see it." (`stationReading` drops a figure the publisher does not
stand behind).

## What is left
Under Level the 72 h chart (and the Year chart's this-year line) still draws the station file's live levels (about
1.7-2.1 m) inside the old-datum band (about 6.5-8 m), so the line sits far below "normal" with no word that the two are on
different datums. Shot: `T1043-shots/review-08HD006-salmon-sayward-level.png`.

## Work
Decide (owner) what an other-datum quantity's chart shows: (a) no chart under that quantity today (the card's refusal
stands alone), (b) the band only, no live line, (c) the live line with no band and a line saying the record is on another
datum. Then one rule for every T550 station, a test on 08HD006's shape, a shot.

Depends: T550. Size: S. Owner: yes (which of a-c). Default: none until he picks.

## Findings (lane L32, 2026-11-23; `scratchpad/l32/T1047-findings.txt` of the 0.9.0 batch session)
For all five stations the datum changed AFTER HYDAT's last level year: 08HD006 2025-06-05 (about 5 m), 08EF001
2025-01-30 (+2.37 m), 07FD001 and 08KH019 step in the live feed in 2026, 08MH153 drifts off the rating (likely weeds, not
a new zero). No record year is on today's zero.

## Built for 0.9.0 (99d8ac9bc, lane L28, coordinator 2026-11-23)
Under Level the chart draws the live line ALONE: no normal band, no earlier years, no year scale (they would mix zeros),
keyed on `reports` (a figure the station file carries that the index row's `reports` leaves out: `offRecord`), so
any station the publisher flags gets it. In the key, in place of "no normal range (short record)", the PROPOSED words
(`CONDITIONS_WORDS.key.recordApart`, for the owner's pick): "This gauge's level is measured from a different zero than its
record, so there is no normal range to compare it with." Flow keeps its band. Tests: `WaterConditions.levelOnly.test.tsx`
(08HD006's shape: Level key has the note and no band key; Flow has its band), `chart.render.test.tsx` (`lineOnly`: no band,
scale "reading", no year asked). Mutations: panel passes no `lineOnly`; note not drawn; band not stripped; no
`offRecord` -> each fails. Shot: `T1043-shots/t1047-08HD006-level-line-only.png`.
Open with the words: the card above still says "Reports flow only today, not water level. Show Flow to see it." (T1043
19a8485bb) over a drawn level line; the owner's look decides whether that line stays.

## After 0.9.0
A frozen datum-change file with owner-accepted rows, then the rating drop (L32's follow-up), so the record can be put on
today's zero instead of being left out.

## Owner, 2026-11-23 (verbatim, through the coordinator)
Top line: "Show the level reading (Recommended)". Note: "just use new data line same as cap. keep it simple".

## Built — the answers (b15f1165b)
Under Level an off-datum station shows today's level reading unranked, exactly as Capilano's ("2.16 m" + "Too new to say if
this is high or low.", no dot; `noRecordLine` takes `offRecord`), and its live line alone with Capilano's key note ("no
normal range (short record)"). The "Reports flow only today…" line no longer shows for these stations, and the proposed
note under the chart is removed (no new words). Under Flow the stage beside the flow shows again (it is today's level).
#77 re-review (low): the mark is keyed on `reports` from the index row while the figure is the station file's; the two can
be different publisher runs, so `offRecord` holds only for the same reading (no `rankedAt`, the feed's `same`), otherwise
the file is trusted. Tests (mutation-checked): `WaterConditions.levelOnly.test.tsx` (08HD006 under Level: "2.06", the
no-record line, no band key, Capilano's key note; Flow its band), `stationReading.test.ts` (same reading marked; a row a
run behind not; a feed before `reports` not). Shot: `T1043-shots/t1047-08HD006-level-reading.png`.

## After 0.9.0, built (lane L8, 2026-11-24, 66a331eef, 874929277): the datum change file and the rating drop
1. **Proposals** (`python -m pipeline.tools.audit.datum_remarks`, HYDAT 20261019, read-only): every `STN_REMARKS` row of
   a roster station (2,324, the stations the app uses) naming a datum, relocation or gauge zero, verbatim, with the date
   and offset parsed when the remark states them, else null with the reason. `data/generated/gauges/datum_proposals.json`
   (committed like `candidates.json`): 127 remarks at 58 stations, 32 sorted `change`. Beyond the five, relocations whose
   live level may now be on a new zero: **08MH044** (Fraser at Whonock, relocated + renamed Albion 2026-09-15; HYDAT to
   2025) and **08MH053** (Fraser at Deas Island, relocated 2026-05-15; HYDAT to 2024): both level-only, so no rating and
   the T550 guard cannot see a new zero; **08HA003** (relocated 2023-09-07) and **08NK026** (relocated 2024-08-28, "step
   change") sit inside HYDAT with fewer than 2 years after, below the shift detector's reach. None is added to the file.
2. **The file**: `data/curated/gauges/datum_change_stations.json` (`CURATED.gauges.datum_change_stations`, README row,
   written through `curated_write`, schema `pipeline.gauges.feed.datum.validate_text`, `load` raises on a missing file).
   One row per change: station, effective, `offset_m` (metres to ADD to a level on the old zero), `offset_from`, source
   (`hydat_remark` + the remark verbatim, checked against HYDAT; or `owner`), status, note, evidence. **All five rows are
   `proposed`**; `datum.accepted` returns accepted rows only (test; no other module reads the key, AST test).
3. **The consumer** (`climatology.build(datum=...)`): an ACCEPTED row with an offset adds it to every level dated before
   `effective`, before the shift test, the pooling and the rating: band, recent years (`priorYears`) and rating are on
   today's zero, so both publishers (they only read clim.json; no feed format change, no worker change) see the live pair
   on the rating, report and rank the level, and the app drops `offRecord` and draws the band again. The `clim` stage key
   carries the accepted rows' digest (a proposed edit reruns nothing). Proof with only proposed rows: the fixture's
   clim.json AND every file the Python publisher writes are byte-identical (`test_datum_changes`); on the real HYDAT the
   whole roster's clim.json (1,951 stations) is byte-identical to origin/main's
   (sha256 f27bd322…, both built from HYDAT 20261019 through heavy.sh). Mutations (8, listed in the test's docstring) each fail a test.
4. **Owner's morning look** (evidence: saved station files fetched 2026-10-28, record = clim.json HYDAT 20261019; "on
   rating" = the live (flow, level) day inside the record's rating ±0.5 m):

| station | HYDAT remark | proposed effective / offset | live data | question |
|---|---|---|---|---|
| 08HD006 Salmon nr Sayward | relocated 2025-06-05, 1956-81 datum restored, "~5m drop in stage" | 2025-06-05 / −5.0 m (remark, approximate) | 300/300 days of 2026 on rating with −5.0 (0 as is) | accept? |
| 08EF001 Skeena at Usk | moved ~1 km upstream 2025-01-30, "2.370m jump in stage" | 2025-01-30 / +2.370 m (remark) | 300/300 on rating with +2.370 (110 as is) | accept? |
| 07FD001 Kiskatinaw nr Farmington | "STATION RELOCATED ON 2026-08-05." (no size) | 2026-08-05 / +94.826 m (measured: each day after vs like-flow days before) | 83/83 after the move on rating with it (0 as is); live reads ~95.0 m | accept the measured offset? **Only after** the this-year-line fix below or once the year turns (the file refuses it before) |
| 08KH019 Moffat nr Horsefly | none | 2026-07-07 / +1.011 m (measured across the step at 1.37 m³/s) | step 1.29 → 2.30 m in one day, flow smooth; 112/112 after on rating with it (3 as is). `T1047-shots/datum-08KH019.png` | accept? **Only after** the this-year-line fix below or once the year turns (the file refuses it before) |
| 08MH153 Fishtrap at the Boundary | none | none (no day, no single offset) | creep, not a step: stage up Jul-Sep as flow falls to 0.01 m³/s, 2024 HYDAT already +0.3 m. `T1047-shots/datum-08MH153.png` | reject as a datum change (recommended)? |

08HD006 and 08EF001 can be accepted now (their change is before this year's line).

Caveats for the owner: a RELOCATED gauge is a new cross-section; the offset aligns the zero, the band's shape stays the
old site's (08HD006's January floods sit inside the 0.5 m pad only because the top bin is wide). 08HD006's offset is the
remark's "~5m".

**Still left** (Claude, after the owner's answers): flip the accepted rows to `accepted` (through `curated_write`),
rebuild clim.json (the HYDAT workflow or the `clim` stage) and the next data release; then the year chart's THIS-YEAR
line: the station file's `daily` for 07FD001 and 08KH019 (changes inside 2026) mixes both zeros until the year turns.
Putting it on today's zero is a publisher change on both sides (Python `merge_daily` and the worker twin, rule 40), not
built here.

## Review fixes (PR #103 review, lane L8, 2026-11-24, 6b36d695d)
1. `datum.check_remarks`: a quote HYDAT no longer holds is fatal only on an ACCEPTED row; on a proposed / rejected row it
   is a printed warning naming the station (a reworded remark in a new HYDAT no longer stops `feeds-hydat.yml`).
2. The file refuses an ACCEPTED change dated inside the this-year line (`publish._record_start`: 1 January, 1 December in
   January): the station file's `daily` would mix both zeros (07FD001 ~0.07 m then ~95 m under a 95 m band). 07FD001
   and 08KH019 can be accepted only after both publishers put `daily` on today's zero (`merge_daily` + the worker twin)
   or once 2027 begins; 08HD006 and 08EF001 now.
3. A moved station's levels (band, recent years, rating) are rounded to millimetres (HYDAT's resolution) instead of 4
   significant figures, which would keep 1 cm at 07FD001's ~95 m. Unmoved stations keep `_sig4`.
4. Proposal classifier: an end-of-record date ("DISCONTINUED", "NO DATA AFTER") is no change's date; "after <date>" is
   the next day; "datum is similar" is not a change; a signed size of the DATUM ("datum changed ... -4.923 m
   difference") moves readings the other way (HYDAT reads ~5.3 m higher from 2013). Regenerated: exactly 5 rows moved:
   08EE003 date 1971-12-31 -> null; 08KD006 date 2011-09-15 -> null, kind change -> other; 08NE087 (2000) 2000-12-31 ->
   2001-01-01; 08GA026 change -> quality; 08MG014 offset null -> +4.923 (approximate).
Proof: with no accepted row the real roster's clim.json is still byte-identical to origin/main's (sha256 f27bd322…,
through heavy.sh). Commit 6b36d695d.
