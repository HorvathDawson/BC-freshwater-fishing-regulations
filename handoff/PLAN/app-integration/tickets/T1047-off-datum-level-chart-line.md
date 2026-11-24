# T1047 — Under Level, an other-datum station's chart still draws its live levels against the old-datum band

Status: DONE 99d8ac9bc, b15f1165b (lane L28, PR #77; owner answered 2026-11-23).

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
