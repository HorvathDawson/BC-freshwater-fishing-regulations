# T1047 — Under Level, an other-datum station's chart still draws its live levels against the old-datum band

Status: OPEN.

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
