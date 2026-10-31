# T323 — Saving a spot offline took 15 s to reach "Save spot" (T307 target < 3 s)

Status: OPEN (found by the T314/T315 lane on the emulator, 2026-10-30, Cultus Lake, airplane mode).
Trace where the 15 s goes (network timeout not failing fast? geocode/weather/gauge lookups awaited before the sheet?)
and make the save path never wait on the network — T307's fail-fast rule. Test bed: time "tap Add spot → Save spot"
offline; gate < 3 s.

Depends: T307. Size: S–M.
