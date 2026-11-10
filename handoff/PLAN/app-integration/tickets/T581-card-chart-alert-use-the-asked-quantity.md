# T581 — The gauge card, chart and alert editor all use the quantity asked

Status: OPEN

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F1: a level-only lead under Flow turns the card into a Level card (lit Level toggle) while the map and estimate stay on Flow; `chartParameter`'s fallback stops the T567 guard firing.
- F7: the alert editor's "now" disagrees with the card above it.
- Owner rule 2026-11-10: Flow always leads, Level is auxiliary. Conformance test: lead kind × quantity × day.

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: S. Owner: no.
