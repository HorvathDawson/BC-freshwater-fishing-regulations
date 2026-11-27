# T1070 — Timing-dependent app tests: `chart.render.test.tsx` waits a fixed 30 ms; vitest times out under load

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1070 | Timing-dependent app tests: `chart.render.test.tsx` waits a fixed 30 ms; vitest times out under load | OPEN —  | | planning chat 2026-11-24 |

Source: the 0.9.1 chat (docs/agents/RESTART-091-chat.md, 2026-11-24): `app/packages/ui/src/chart.render.test.tsx` uses a fixed 30 ms wait; vitest reports "Timeout calling onTaskUpdate" when the machine is loaded.

## Work
1. Replace the fixed wait with a condition wait.
2. Find why `onTaskUpdate` times out (worker count under `heavy.sh`?) and set the pool for it.

## Acceptance
50 loaded runs of the chart test with no failure; the timeout does not appear in a full `before_pr` run beside another heavy job.

Severity: low to medium (a false red costs a review round). Release: 0.9.2. Model: Sonnet. Size: S. Owner: no.

## Added 2026-11-25 (0.9.1 chat)
`packages/ui-native/src/WaterScreen.sources.test.tsx` times out at 40 s under machine load (twice on the night of
the 0.9.1 cut; passes alone). It fails the pre-push hook and `before_pr`.
