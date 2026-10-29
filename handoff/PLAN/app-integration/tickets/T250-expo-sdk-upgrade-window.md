# T250 — Expo SDK / dependency upgrade window

Status: OPEN — not started.

Source: owner 2026-10-28 — approved proposal P16 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
Before store submission: upgrade Expo SDK + React Native + MapLibre RN to current, re-apply / re-derive
`app/patches/` (MapLibre RN patch), run the full suites, an Android device walk and a web walk; record in app/deps.md.
## Acceptance
Suites green, the patch re-applied (or dropped as upstreamed), device + web walks with shots, deps.md updated.
Depends: none (schedule before T244). Size: M. Owner: no (a phone walk at the end).
