# T1026 — The day picker stops at December 2026 although the synopsis is valid to March 31, 2027

Status: DONE — e2cdbee15 (no format change: the picker wraps past New Year to the release's valid-to; the answers already recur by month/day, weekdays are the real date's; checks a-d below). Not seen on a device.

Source: a closed tester, relayed by the owner 2026-11-22 (screenshots: the Sources list "Synopsis — Valid to Mar 31,
2027", and "Which day?" showing December 2026 with the next-month arrow disabled). Verbatim: "Synopsis valid til March
next year, but calendar doesn't go past the end of 2026. Might just be me not understanding all the different sources."

## Work
1. Find what bounds the day picker (calendar year, a horizon constant, the in-season / DFO window, or the release's
   validity). The picker should reach the end of what the regulations answer for (the synopsis's valid-to date), or say
   in words why a later day is not offered.
2. Every reader (status index, answers, the map's day) must answer for the days now offered: check the release's day
   range before widening (rules are per day; a day past what the release computed must not show as "open").
3. Test on the bound; shots.

Depends: none. Size: S–M (bigger if the release's day range must grow: then stop and say so). Owner: no.

## Owner answer (2026-11-23 ~00:05, verbatim)
"Yes, extend to Mar 31 2027 (Recommended)" — to the question whether the 0.9.0 data release should extend the day-by-day answers through Mar 31, 2027.

## Findings (lane L15): the cheap reading holds, no dated axis needed
The answers are year-less ON PURPOSE and every reader takes (month, day) + the real date's weekday, so Jan 1–Mar 31 2027 read the existing day 1..90 answers.
- (a) No curated rule or closure carries a year bound (the only "2026/2027" strings in `data/curated/regulations/synopsis` are ruling notes and the fee line "2025-2027 synopsis"); the synopsis term is 2025-2027, so its Jan–Mar windows are the ones in force in 2027. Mar 1 reads day 61 in every year (`readingDay`); 2027 has no Feb 29.
- (b) Weekday-limited rules exist as per-day weekday moments (Kootenay); they are picked by the REAL weekday (`tapDay(day, year)`, `regsDay` carries the picked date's year), pinned in regs.test (Jan 1 2027 Friday, Mar 1 Monday) and dayPicker.test.
- (c) Feeds: in-season notices compare real ISO dates (`inSeasonSpan`, unbounded rows keep showing until the page drops them: unchanged behaviour); DFO salmon windows are month/day recurring (`dfo_salmon/dates.py` strips years), the same reading Jan–Mar 2026 days already had, with the feed's own as-of on the card; sun times use the real y/m/d (WaterScreen `sunDay`); the Conditions forecast is by offset from today and a 2027 day is past it: "<day> is past the forecast" (`regsDayLink`), as for any far day.
- (d) Weekday names come from `Date`/`weekdayOf(year, …)` everywhere checked (`dayWords`, `fullDay`, `regsDay`).
Residual (not a bug, an owner call): DFO may change its Jan–Mar rules for 2027 before then; the salmon card shows the as-of of what it read.
