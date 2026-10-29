# T247 — search upgrades

Status: OPEN — not started.

Source: owner 2026-10-28 — approved proposal P13 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
Fuzzy / typo-tolerant search (edit distance on names + variants, T224), search by management unit ("2-4"), region
("Region 2"), nearby town ("near Chilliwack": nearest-town data in core, T182), recent searches (device-only, clearable).
## Acceptance
Golden queries (typos, MU, town, region) return the right water first; recents survive a restart and clear; search stays
under its time budget on the phone.
Depends: T224 (variants). Size: M. Owner: no.
