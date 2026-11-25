# T583 — One wording for a gauge's standing, everywhere

Status: DONE — dad265255, 5a71f6bb1. One standing wording in core standing.ts with its gate and worker parity test; owner 2026-11-10 keeps the "Lower than 8 days in 10" estimate sentence (the measured-gauge words then moved to days-in-10 in T598).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T583 | One wording for a gauge's standing, everywhere | DONE | dad265255, 5a71f6bb1 | — |

Closed by the 2026-11-20 audit: the only open item was the owner's keep/retire question, answered 2026-11-10 ('i like it', DECIDED section, 5a71f6bb1); gate tools/one-standing-words.test.ts is in.

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F4: one standing is worded four ways on one sheet ("Low", "Below normal · 20th percentile", "Reads 2 in 10", "p20th") and a fifth way, with a rounded figure, on the public gauge page.
- One phrase module and one shared `figure()` in core; a speaker gate and a worker/app card parity test (AGENTS 23, 33, 40).

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: M. Owner: no.

## DECIDED 2026-11-10 (owner)
Keep the estimate sentence "Lower than 8 days in 10": "i like it", and it is "easier for a user uneducated to
understand" than a percentile. Plain "days in 10" wording stays for estimates; do not replace it with a percentile.
