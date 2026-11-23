# T1026 — The day picker stops at December 2026 although the synopsis is valid to March 31, 2027

Status: OPEN — 0.9.0 batch.

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
