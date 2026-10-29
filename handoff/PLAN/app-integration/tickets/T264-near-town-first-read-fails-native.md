# T264 — "Could not read what is near this town" on Android, intermittently (the first town read after a cold start)

Status: OPEN (items 1 + 3 done 2026-10-29; the cold-start loop not run)

Source: T255 test bed 2026-10-29 (release APK, staging r-0fa7927b16e5, emulator, AIRPLANE MODE). Cold start offline →
search "Cultus" → tap the place row "Cultus Lake · Village" → "WATER NEAR CULTUS LAKE — Could not read what is near this
town." (shot `../T255-shots/near-town-failed-offline.png`, run b3). The same tap passed in run b5 (offline and online)
when a water's regulations had been opened first. T74 saw the same words for Vedder Crossing on native (2026-10-27) and
put them on the held release; this release is current, so it is not that.

## Suspect
`watersNear` (data/release/source.ts) is the first read of the `near` sqlite (and `CROSS.ITEMS_FOR_ORDS` /
`TOWNS_FOR_ORDS`); a failure there is shown as `nearFailed` and the error is swallowed. A first open racing another
open of the same database (expo-sqlite in place, `drivers/open.native.ts`), or the T254 ordered loader on native, would
fit "first time after a cold start only".

## Work
1. Log the swallowed error (teeConsoleErrors → the Updates log / logcat) so the next failure names itself.
2. Reproduce: test bed `offline` with the town tap moved FIRST after the cold start (`run.mjs offline`), several runs.
3. Fix the race; a failed near read retries once before saying `nearFailed`.
## Acceptance
Ten cold starts (offline and online) with the town tap first: every one lists the waters (test bed loop).
Depends: T255. Size: S–M. Owner: no.

## Partly done (2026-10-29)
`useTown` (ui search.ts) names a failed near read (`console.error` → the Updates log / logcat) and tries it once more
after 400 ms before `nearFailed`. The race itself is not found; the ten-cold-start loop (acceptance) is not run.
