# T584 — Conditions reads the same whatever the way in (group strip, picks, part step)

Status: IN REVIEW — 5a2e4117b (F5 (group strip and sun line from the frame's point, `frameBasinAsk`), F6 (gauge pick dropped on section change; span and model in a session store `useChartPrefs`, not persisted across a cold start), F11 (body kept dimmed on a part step; reading placeholder). Plus the forecast-day map top: block over the map holds its height and the headline reserves two lines (map top 505 vs 530 px before, 530 vs 530 after). Tests: frameView.test.ts, chartPrefs.render.test.tsx. Reaches the phone with the next build; owner confirms there.)

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F5: the watershed-group day strip uses the tap point or else the water's middle: tap, step, search and reload of one part show different groups.
- F6: gauge pick, span and model pick survive a part step but reset on a new water, a reload or a deep link.
- F11: a part step swaps the whole Conditions body for a spinner; the reading block appears late.
- The sun-line variant named in the findings. Extends the T369 entry-path test.

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: S. Owner: no.
