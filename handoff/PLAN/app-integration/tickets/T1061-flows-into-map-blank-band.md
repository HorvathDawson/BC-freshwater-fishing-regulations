# T1061 — A Map opened through "Flows into" shows a blank band of about 120 px (Skeena)

Status: OPEN.

Source: the 0.9.0 device pass, emulator-5554, 2026-11-24 (the 0.9.0 chat). Seen on the Skeena River's Map opened from a tributary's "Flows into" (T1023).

## Work
1. Reproduce on the emulator; find which inset or sheet height the Map keeps from the water it came from.
2. Fix on the one load path (`one-load-path-per-screen`): the same Map whatever the way in.

## Acceptance
before/after emulator shots; a layout test on the entry path.

Severity: low (cosmetic). Release: 0.9.1 (a 0.9.0 device-pass bug, roadmap row 2). Model: Sonnet. Size: S. Owner: no.
