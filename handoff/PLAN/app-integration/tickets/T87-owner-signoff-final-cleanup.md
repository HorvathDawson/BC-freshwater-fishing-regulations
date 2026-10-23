# T87 — Owner sign-off walk, then the final clean-up phase

Status: OPEN. Owner needed (sign-off).

Source: implement_progress "FINAL CLEAN-UP PHASE (owner, 2026-10-20) — ONLY after the app is done AND the owner has …
confirmed it looks like the prototype": remove app/design, then audit handoff/ in the repo and in memory. T63 has no
owner gate.

## Work
The owner walks the app (web + phone). After sign-off: delete app/design/ and dead handoff/ material in dated commits;
prune stale memory notes.

## Acceptance
Sign-off recorded in decisions.md; nothing references app/design; the CI dead-link check passes.

Depends: T63, T77, T82. Size: M. Owner: yes. Default: hold until the owner walks it.
