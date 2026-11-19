# T898 — A daylight-only rule shows today's actual times for the section

Status: IN PROGRESS — left (Claude): owner sees pictures, review, merge into app/integrate-082

Owner 2026-11-19 ~01:10 (app chat), verbatim: "stuff like this (Daylight only: closed from 1 h after sunset to 1 h
before sunrise Rule book p.23 ↗) could give you the times for it on the section you are currently on (average time for
the section maybe) could jsut use centroid of median location fo the section idk"

Build: under such a rule, the day's times for the open section, e.g. "Today: closed 18:12 to 6:58", from the SAME sun
times the header's sunrise/sunset line uses (T878) for that section and day, offset by the rule's own hours (read from
the rule's stored fields, never parsed from its words in the app). Changes with the chosen day. Where the rule's offsets
are not stored as data, say so in the ticket and show nothing new for it (no guessing). Pictures before it lands.

## What was built (lane `app/t898-t899`, 2026-11-19 ~01:25 MST)
- Where the offsets come from: STORED DATA. The pipeline resolves the rule's hours into the day's moments (answers file
  `Moment.hours`: `{ start: { solar: "sunset", offset_min: 60 }, end: { solar: "sunrise", offset_min: -60 }, in: true }`).
  Core `daylightOfMoment` (`app/packages/core/src/regs/caveats.ts`) reads that window (every day, inside it, both ends on the
  sun with a non-zero offset) and `timedOf` puts it on the item as `CaveatItemVM.daylight = { after, before }`. T473's
  regex over the composed `where` words ("60 min after sunset–60 min before sunrise") is gone: `daylightOffsets` now returns
  the stored field. Same items as before (the frozen page `edge_x15p2_1010.json` re-cut: only the new field).
- The times: core `daylightClosed` / `daylightAt` (`app/packages/core/src/sun.ts`) — the same `sunTimes` at the same point
  and on the same picked day as the date line (T878 `sunLine`; `@app/ui useDaylight` shares `useSunPoint` with `useSunLine`),
  sunset + after and sunrise − before, on its clock ("18:21", "7:23": no leading 0, no "PT"). Both ends are the picked day's
  own sunset and sunrise, as the date line shows them; the next morning's real sunrise is a minute or two later.
- The words: `WORDS.DAYLIGHT_CLOSED` (`regs/words.ts`), composed by core `daylightClosedText`: "Today: closed 18:21 to 7:23";
  another picked day is named as the date pill names it ("Fri Nov 20: closed …"). Drawn by `TimedLines` right under the
  rule line (`keep-timed-today`); a new day recomputes (the hook is memoised on point + day). No sun point: no line.
- Still there (T473, owner-approved 11-07): "Daylight only · fishing 07:23–18:21 PT" under the status. The two lines say
  the same hours from both sides; for the owner to pick at the pictures (keep both, or drop one).
- Tests: `app/packages/core/src/sun.test.ts` (whole strings at Mission, Nov 19 and Jun 21, 60/60 and 30/45; the stored
  window read and every near-miss refused), `app/packages/ui/src/sun.render.test.tsx` (the hook on two days),
  `app/packages/ui-native/src/regs/Edges.test.tsx` (the line under the rule on the frozen Fraser page, two days, no point).
- Picture: `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/t898-shots/t898-daylight-fraser-390-light.png` (Fraser River, CPR Bridge at Mission → Landstrom Bar, Nov 19, 390 light;
  the date line "8:23 17:21", the rule line, then "Today: closed 18:21 to 7:23"). The jsdom snapshot leaves the year bars
  blank (no layout width); the harness is `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/t898-shots/t898-shots.test.ts.txt`.
