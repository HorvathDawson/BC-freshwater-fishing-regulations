# T582 — One day per water screen, and the same kept state on web and phone

Status: IN REVIEW (built per the DECIDED section; web shots in ../T582-shots/; phone not run, no build this session)

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

## Built (T582)
- core `dayLink.ts`: `regsDayLink` (today / inside the forecast window with the chart mark / past it), `DAY_LABELS` (what each tab says), one set of words for web and phone.
- Conditions (`ConditionsPanel`, from `WaterConditions`, given the Shell's `mapDay` as `regsDay`): a line "Conditions: today" (or its +N day), and under it "Regulations day: Sat 14 Nov, marked on the chart." (the chart's day rule) or "Sat 28 Nov is past the forecast." The Conditions horizon is its own and no longer follows or moves the regulations day. The water's Map view says "Map: rules for <day>"; the Regulations page's date pill already names its day.
- F3: the address keeps the regulations day (`date`) but not the horizon (`addressKept`, used by `systemBack.web`), so a web reload and a phone cold start both open Conditions on today. A shared link still names a horizon and both platforms read it alike. (The README has no "kept state" table to add a row to.)
- Tests: `core/src/dayLink.test.ts`, `ui-native/src/ConditionsDay.test.tsx` (the F2 gate: day named, inside = marked, past = the one line, horizon independent), `ui/src/keptDay.test.ts` (the F3 gate: address restores no horizon).
- Not built: when both the horizon and the regulations day are on the chart, only the horizon's own rule is drawn (the regulations day is still named in the line).

## Owner look pending (2026-11-10)
Owner will try it on build 25: "we just want to make sure it isn't clunky and hard for a user to know what it means."
Stays IN REVIEW until he says it reads clearly. If not: simplify the wording or drop to one line; ask him what confused.
