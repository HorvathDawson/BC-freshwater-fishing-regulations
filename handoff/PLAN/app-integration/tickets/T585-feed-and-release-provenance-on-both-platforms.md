# T585 — Web and phone answer from the same feed and release, and say which

Status: OPEN

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F8 (plausible): the card's figure and percentile come from a different file than the rows, dots and estimate.
- F12: the phone can run an older release than the web for hours or days.
- F13 (plausible): a switchable phone build can keep another environment or channel.
- F14: offline, only the phone answers from a kept week, and its "now" has no standing.
- Goes with T550 (gauge readings age / datum / day): do them together.

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: M. Owner: no.
