# T611 — The map's web and native halves agree

Status: OPEN

Source: T608 part B (owner 2026-11-10: "audit the app web vs native more"),
`handoff/PLAN/app-integration/T608B-findings.md`: 28 findings G1–G28, each with file:line of both copies, the state
that shows it, and the fix. Rows marked P there are plausible, not fully traced: confirm before fixing, close with a
reason if not real.

## Work
Findings whose files are under `app/packages/map`: G4 (wrong answer, P), G5, G15, G19, G21 and any other G row there.
Gate: a test that both map halves (`Map.web.tsx`, `Map.native.tsx`) honour every `MapProps` key, or list the key as
web-only / native-only by design with the reason.

## Acceptance
- Each finding taken here no longer reproduces and has a test, or is closed with a reason; the gate named above is in.

Depends: —. Size: M. Owner: looks at the shots.
