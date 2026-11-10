# T584 — Conditions reads the same whatever the way in (group strip, picks, part step)

Status: OPEN

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F5: the watershed-group day strip uses the tap point or else the water's middle: tap, step, search and reload of one part show different groups.
- F6: gauge pick, span and model pick survive a part step but reset on a new water, a reload or a deep link.
- F11: a part step swaps the whole Conditions body for a spinner; the reading block appears late.
- The sun-line variant named in the findings. Extends the T369 entry-path test.

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: S. Owner: no.
