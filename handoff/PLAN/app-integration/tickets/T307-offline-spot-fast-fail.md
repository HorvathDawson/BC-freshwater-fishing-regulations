# T307 — Adding a spot offline is instant (no waiting on network that isn't there)

Status: DONE (d5e3fcf8e, 3404e25; emulator-checked 2026-11-06) (owner 2026-10-31: "When offline adding a spot takes a long time because it doesn't fast fail the trying to get data").

## Work
Find every fetch the spot capture waits on (weather/Open-Meteo, gauge reading, day flow, hydat history, place lookup) and
fail fast when the device is offline (NetInfo says no connection → don't even try; flaky → short timeout ~2–3 s) — save
the spot immediately with what's on the device, mark the missing parts "fills in when you're back online", and backfill
them later (the existing refresh path). Same for adding a visit (T294). Emulator check in airplane mode: save < 1 s.

Depends: T294. Size: S. Owner: no.

## Done (d5e3fcf8e)
`data/spots/fastFail.ts`: `captureSpot` / `captureVisit` wrap the network asks (gauge now/series/parameters, Open-Meteo at/days/ahead, feed index) — offline (`history.online()`) they are not made and answer as a failed fetch (null / refusal), so leadUp/weather stay pending and the existing reconnect refresh backfills; online-but-flaky each is cut at 2.5 s. On-device lookups pass through. Unit tests. Emulator: `spots` scenario now times Next -> Save spot offline (< 3 s incl. screen reads). Batch run 2026-10-30 14:01: APK built, but every Maestro flow hung 6 min (tab/spot-add-photo/spot-name-save, empty logs; maybe a concurrent Maestro from another lane) so no timing — rerun the `spots` + `locate` scenarios.

## Emulator check (2026-11-06, test bed `spots`, offline = airplane mode)
PASS. Offline Next -> "Save spot" 1.98 s (1.99 s on the rerun; each includes ~1 s of screen reads, so the capture itself is well under that); the spot saved, listed with its photo, survived a cold start offline, offline edit kept; back online its weather/gauge context filled by itself (expectSynced: nothing missing). First run FAILED on save: "This spot was not saved (JSON Parse error: Unexpected end of input)" - an empty `.bak` with no main file made `core.ts` load throw a raw parse error from every later save. Fixed 3404e25 (empty recovery copy = no spots; unreadable one kept aside + SpotsCorrupt) with 2 tests. Shots: app/tools/testbed/out/t307/ (local, gitignored). Not separately timed: Save tap -> row showing (scenario times Next -> Save spot).

- 2026-11-06 (review, 0e8ca3331): 3404e25's empty-backup case read an empty .bak as "no spots" and deleted it. That is a
  silent drop, and an empty file is what T446's bug left. Now kept aside + SpotsCorrupt (shown and reported), as for an
  unreadable one; the next save works.
