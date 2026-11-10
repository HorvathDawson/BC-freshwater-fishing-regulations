# T583 — One wording for a gauge's standing, everywhere

Status: IN REVIEW — dad265255 (F4 held at HEAD. One module `app/packages/core/src/standing.ts`; forms: band "Below normal"; headline "Below normal for the time of year"; short "Below normal · 20th percentile"; long (the card) "Below normal · 20th percentile for the date"; reads (donor row) "Reads below normal · 20th percentile."; percentile (map dot, chips) "20th percentile"; place chip "≤10th percentile"; inTen kept for the estimate sentence only; one `figure()` (card, chart, dot, spots, stage, public page; the shipped gauge-page script is written from core's function). Gate `tools/one-standing-words.test.ts`; worker parity `workers/feeds/test/cards.test.ts`; shots ../T583-shots/. Owner: keep or retire the estimate sentence "Lower than 8 days in 10"?)

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F4: one standing is worded four ways on one sheet ("Low", "Below normal · 20th percentile", "Reads 2 in 10", "p20th") and a fifth way, with a rounded figure, on the public gauge page.
- One phrase module and one shared `figure()` in core; a speaker gate and a worker/app card parity test (AGENTS 23, 33, 40).

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: M. Owner: no.
