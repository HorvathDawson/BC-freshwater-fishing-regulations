# T1047 — Under Level, an other-datum station's chart still draws its live levels against the old-datum band

Status: DONE 99d8ac9bc, b15f1165b (lane L28, PR #77; owner answered 2026-11-23); follow-up 66a331eef, 874929277, 6b36d695d (lane L8, 2026-11-24): datum change file, HYDAT remark proposals, the record put on today's zero for an accepted row; owner 2026-11-24 accepted 08HD006 + 08EF001, built by lane L16 (78af8ebb4, e07717163): live with the next data release. Left: 07FD001, 08KH019, 08MH153 (not asked, still proposed); the this-year line (below).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1047 | Under Level, an other-datum station's chart drew its live levels against the old-datum band (08HD006); follow-up: datum change file, remark proposals, record on today's zero for accepted rows; 08HD006 + 08EF001 accepted, listed per station in the release checks | DONE (L16 11-24: 08HD006 + 08EF001 accepted, live with the next data release; 07FD001, 08KH019, 08MH153 not asked, still proposed) | 99d8ac9bc, b15f1165b, 66a331eef, 874929277, 6b36d695d, 78af8ebb4, e07717163 | #77 review 2026-11-23, item 3; L32 findings; owner 2026-11-23, 2026-11-24 |

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

## Owner, 2026-11-24 (verbatim, through the coordinator): 08HD006 and 08EF001
Asked in plain words whether to shift the history of 08HD006 (Salmon River near Sayward, 5.0 m lower since 2025-06-05)
and 08EF001 (2.37 m higher since 2025-01-30) so their normal range comes back: "Yes but make sure it is a clean well
documented update that happens which is easy to see". 07FD001, 08KH019, 08MH153 were NOT asked: they stay `proposed`.

## Built (lane L16, 2026-11-24): the two rows accepted, documented, easy to see
1. **The rows**: both `accepted` through `curated_write` (`python -m pipeline.gauges.feed.datum --set <station> accepted
   --by owner --on 2026-11-24 --asked … --words …`, `CURATED_WRITE_BY` "Claude (lane L16, T1047), on the owner's answer of
   2026-11-24"). Each carries `decision` {by, on, asked, words (verbatim)}; the schema now requires it on an accepted /
   rejected row and refuses it on a proposed one. The diff of the file: only those two rows, `status` + `decision`.
2. **The page**: `pipeline/docs/GAUGE-DATUM-CHANGES.md` (linked from the docs index and the gauges README): per station
   Environment Canada's note verbatim, the date, the offset and its sign, the app before / after, the pictures, how it
   goes live, the one-command undo (`… datum --set <station> proposed`).
3. **Easy to see**: no release channel covered gauge climatology (the binding diff, gone ids, live counts and
   `answers_diff` are all regs). Added the smallest one: the data release's checks (stage 4) list each gauge level record
   the release moves against the live release's commit, one plain line per station ("08HD006 level record moved 5.0 m
   down to the gauge's new zero (Environment Canada note, 2025-06-05)"; an undo reads "… back on HYDAT's own zero …"),
   printed, written to `checks/<r-id>/gauge_records.txt`, kept on the ledger (`checks.gauge_records`), counted in the
   stage line; listed, never a stop (`release.gauge_records`, `datum.record_changes` / `rows_at`). The climatology build
   prints the same line per accepted row. The anglers' What's new was not touched (their words are the owner's).
4. **Proof on real data (scratch, HYDAT 20261019 read-only, through heavy.sh)**: clim.json before (sha f27bd322…, = the
   Mac's live one) and after: only 08HD006 and 08EF001 differ, and only their level band, `recentYears` (2024) and
   `ratings`; flow bands, `stats` and the other 1,949 stations identical. Today's band (pentad 65, p10–p90): 08HD006
   6.736–8.156 → 1.736–3.156 m; 08EF001 2.218–3.432 → 4.588–5.802 m. The Python publisher on a saved ECCC snapshot
   (2026-11-24 15:47Z): before `reports` ["discharge"] for both; after ["discharge","level"], level ranked 0.95 (08HD006,
   3.178 m at 300 m³/s) and 0.46 (08EF001, 5.046 m at 441 m³/s). The feeds worker (TS) on one recording of both
   stations with each clim.json: the same 433 files as Python, the same reports and ranks; worker gauge tests 49/49.
5. **Pictures** (local web over production data, the live files frozen; after = the climate file's level blob packed from
   the after clim.json exactly as the release packs it (the before clim.json reproduces production's blob byte for
   byte), the index row and station file as the publisher writes them): `T1047-shots/accepted-08HD006-{before,after}.png`,
   `T1047-shots/accepted-08EF001-{before,after}.png`. Before: "Too new to say", line alone. After: band back, reading
   ranked ("(ranked on flow)": flow leads under the owner's rule; the level's own rank is in the feed).
6. **Live**: no app build. The 0.9.1 data release: `pipeline build` reruns `clim` (its key holds the accepted rows), so
   clim.json and the bundle's gauge_clim move together, and the release upload puts clim.json on R2 beside the release.
   Safe order: merge, the 0.9.1 STAGING data release, then the production upload. The one early path is staging and
   needs no person: `feeds-hydat.yml` has no cron, but the staging feeds worker starts it itself on `main` with upload
   when ECCC lists a newer HYDAT (`index.ts` hydat event, `dispatch.ts` `DEFAULT_REF = TRUNK`). If that comes before
   the staging data release, staging ranks these two levels on today's zero while the staging app still draws the old
   band (`gauge_clim`), the live line metres off it, until that release. Production cannot be reached that way: its
   clim.json changes only through `deploy/prod/upload.sh`, which puts clim.json and the release up in one run.
Tests: `test_datum_changes.py` (+7), `test_release_command.py` (+4); 14 mutations each caught (listed in the docstring).
Review #121 (e07717163): the early STAGING path written down (above); the clim build's printed lines pinned to accepted
rows; a datum file the live commit cannot give (git failure, schema refusal) is recorded as "could not be compared:
<reason>" in the checks output, `gauge_records.txt` and the ledger, and stage 4 goes on.
