# T323 — Saving a spot offline took 15 s to reach "Save spot" (T307 target < 3 s)

Status: DONE (415373ffd) — not the app: the test bed timed from the pick-the-water Next, which lands on "When were you there?", not Save spot, so it always ran to its 15 s cap. Timed from the step that fetches: 1946 / 1955 / 1960 ms offline (target < 3 s).
Trace where the 15 s goes (network timeout not failing fast? geocode/weather/gauge lookups awaited before the sheet?)
and make the save path never wait on the network — T307's fail-fast rule. Test bed: time "tap Add spot → Save spot"
offline; gate < 3 s.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T323 | Offline spot save took 15 s to reach "Save spot" (T307 target < 3 s): never wait on network | DONE (harness timed the wrong step; real offline Next → Save spot 1.9–2.0 s, 3 runs) | 415373ffd | T314 lane 2026-10-30 |

Depends: T307. Size: S–M.
