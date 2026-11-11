# T620 — One answer on every app screen (search, spots, alerts, Runs, start links)

Status: IN REVIEW

Source: T608 part B (owner 2026-11-10: "audit the app web vs native more"),
`handoff/PLAN/app-integration/T608B-findings.md`: 28 findings G1–G28, each with file:line of both copies, the state
that shows it, and the fix. Rows marked P there are plausible, not fully traced: confirm before fixing, close with a
reason if not real.

## Work
Findings whose files are under `app/packages/ui`, `ui-native`, `core`, `data` (not `map`, not `tools/seo`, not the worker):
G1 (search rows show today's status while the map and water use the picked day: wrong answer), G3 (a saved spot's
"observed" time printed in unlabelled UTC: wrong answer; use the one B.C. clock), G10, G11, G12, G13, G14, G17, G20,
G25 and any other G row in these paths.
Structural fix: every reader-facing fact is asked once as (water, part, day, quantity) from core/ui with the day always
passed in. Gate: a same-answer-on-every-surface test (search row, map sheet, water pane, saved spot agree for one
water and day).

## Acceptance
- Each finding taken here no longer reproduces and has a test, or is closed with a reason; the gate named above is in.

Depends: —. Size: M. Owner: looks at the shots.

## Result (2026-11-10)
Done: G1, G3, G10 (Runs seeded from the map's day), G11, G12 (past day opens as today with one line), G13, G14, G20, G25, G26.
Not done: G17 (owner choice: app links vs fix the comment), G27 (reader sentences in screens, none wrong today), G28 (small,
mostly map/seo paths). Gate: `app/tools/one-answer.test.ts`. Shot: `T620-shots/g12-past-day-notice.png`.
