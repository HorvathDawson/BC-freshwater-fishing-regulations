# T582 — One day per water screen, and the same kept state on web and phone

Status: OPEN

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F2: Regulations and the water's Map use `mapDay`; Conditions uses its own horizon from today. Same water, two days, nothing says so.
- F3: the web restores the horizon from the address; the phone restores Show but not the horizon, so after a restart they lead with different gauges.
- Default until the owner answers: one day for all views of a water.

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: M. Owner: yes (F2: one day or two labelled).
