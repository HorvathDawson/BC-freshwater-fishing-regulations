# T620 — One answer on every app screen (search, spots, alerts, Runs, start links)

Status: DONE — adc3499db, d7abdf763, f508d2e96, 22ce46056, 5c86bbe3e, 416b447df, 5aaaaa6ab, afdc7a7e7, eb1fc77a5, 41cb367dc, e4fa8e00b, 045d8589a (closed on the owner's word 2026-11-20; see the last section)

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

## Closed 2026-11-20

Closed 2026-11-20 on the owner's word ("Couldn't we just assume most of these are done already?"): the owner's look at the shots is taken as done (G17 was built as T625, G27 closed, G28 sits elsewhere).

Status line before closing: IN REVIEW — adc3499db, d7abdf763, f508d2e96, 22ce46056, 5c86bbe3e, 416b447df, 5aaaaa6ab, afdc7a7e7, eb1fc77a5, 41cb367dc, e4fa8e00b, 045d8589a (G1 G3 G10 G11 G12 G13 G14 G20 G25 G26 done; gate app/tools/one-answer.test.ts). G17 decided by the owner 2026-11-10 (app links) and built as T625; G27 closed (none wrong today); G28 elsewhere. Proof: the owner looks at the shots.
