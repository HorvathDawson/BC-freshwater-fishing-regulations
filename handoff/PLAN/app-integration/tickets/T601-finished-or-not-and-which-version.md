# T601 — Every reader knows a build is finished and which version it reads

Status: OPEN

Source: T599 findings step 3 (§1 "where a reader guesses a path or a vintage", §5); owner accepted 2026-11-10.

## Work
1. The atlas writes a COMPLETE marker last; promote is safe (never a half-written atlas).
2. One `latest()` for every Python and JS reader of "the live release"; no modified-time fallback.
3. A format tag on the gauge feed; each feed names the release it belongs to.
4. One writer for `clim.json`.
Overlaps T549, T555, T557, T585: do each item in whichever of those tickets already owns it and close the rest here.

## Acceptance
- No reader picks a file by modified time or by guessing a path; a half-built atlas cannot be promoted or released.

Depends: T547; overlaps T549, T555, T557, T585. Size: M. Owner: no.
