# T586 — Small one-answer fixes: saved spot lead, silent gauge dot, two clocks

Status: OPEN

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F9: a saved spot freezes the Flow lead whatever the angler was looking at (check against the Flow-leads rule: may be correct now).
- F10 (owner): the browse map drops the dot of a station silent in the quantity, while the donor pin and row keep it and say why. Default: keep the dot, muted.
- F15 (plausible): two clocks; goes with T551.

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: S. Owner: yes (F10).
