# T1026 — The day picker stops at December 2026 although the synopsis is valid to March 31, 2027

Status: PARTIAL — 641725521, dd48e15d8; left (Claude): decide whether the release's day range grows into 2027 (a data-release change; the synopsis is valid to Mar 31, 2027) — the bound is by design: the release's calendar is ONE calendar year with no year in it (`dayPicker.ts` header), so a 2027 day would read as the same day of 2026. Widening needs the release's day range to grow (bigger, STOP: owner/data decision). Done instead: the greyed December now says why, naming the synopsis's valid-to day (`DayRange.beyond`, `DAY_PICKER.beyondYear`). Test in dayPicker.test.

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

## Why it was not built in lane L15 (stopped, reported)
The day axis is YEAR-LESS by design: day 1..366 on the leap calendar (`pipeline/common/calendar_spec.py`, generated into TS/JSON/page JS), so a printed "Mar 1" is the same day every year; every answer, status index, strip (`year.py`: runs summing to 365), salmon/in-season reader and the app's `dayOfYear` read that axis. Running to Mar 31, 2027 needs a dated axis (2026-01-01..2027-03-31) through the answers encoder (~10k lines under `pipeline/deliver/answers`), the bundle format and every TS reader, plus a bundle format bump that installed 0.8.x apps must refuse loudly: a multi-day format change, not a small ticket. Needs its own plan/ticket.
