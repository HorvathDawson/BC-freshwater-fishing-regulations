# T582 — One day per water screen, and the same kept state on web and phone

Status: OPEN

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F2: Regulations and the water's Map use `mapDay`; Conditions uses its own horizon from today. Same water, two days, nothing says so.
- F3: the web restores the horizon from the address; the phone restores Show but not the horizon, so after a restart they lead with different gauges.
- Default until the owner answers: one day for all views of a water.

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: M. Owner: no (decided 2026-11-10).

## DECIDED 2026-11-10 (owner: "i trust your way of handling it"; replaces the "one day" default above)
Two days, each labelled, with a soft link. The forecast is a short window; regulations can be asked for any day.
- Regulations day = the day the angler plans to fish: any date, from the map's day picker. Conditions keeps its own
  window: today plus the forecast days. Changing the day inside Conditions never moves the regulations day.
- Each tab says which day it shows, so a tab switch never changes the day silently.
- Soft link: a regulations day inside the forecast window opens Conditions with that day marked on the chart; one past
  the window shows today and the forecast with one line, "<day> is past the forecast."
- Web and phone restore the same: Conditions reopens on today unless the link carries a day (F3).
