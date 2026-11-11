# T622 — The public pages and share cards use the app's own words and day

Status: IN REVIEW 1aed37981 (G2, G6-G9, G18, G22-G24, G28 done; G16 not done: the app's start link names no gauge)

Source: T608 part B (owner 2026-11-10: "audit the app web vs native more"),
`handoff/PLAN/app-integration/T608B-findings.md`: 28 findings G1–G28, each with file:line of both copies, the state
that shows it, and the fix. Rows marked P there are plausible, not fully traced: confirm before fixing, close with a
reason if not real.

## Work
Findings whose files are under `app/tools/seo` and the worker's share card (`app/workers/feeds/src/gauges/card/`):
G2 (the public water page's description and share picture bake the build day's "Closed to fishing today": wrong
answer), G6, G7, G8, G9, G16, G18, G22, G23, G24 and any other G row there. The public site and the worker card import
core's functions instead of their own copies.
Do **T605** in the same pass (same files): the gauge page script's clock, and the owner's list-page decision (lists
carry names and links only; weekend and opening/closing pages go, with redirects; a link to today's map).

## Acceptance
- Each finding taken here no longer reproduces and has a test, or is closed with a reason; the gate named above is in.

Depends: —. Size: M. Owner: looks at the shots.
