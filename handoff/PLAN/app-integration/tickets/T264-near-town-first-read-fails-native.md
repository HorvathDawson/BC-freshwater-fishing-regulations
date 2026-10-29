# T264 — "Could not read what is near this town" on Android, intermittently (the first town read after a cold start)

Status: DONE 2026-10-29 (root cause found and fixed; 10 cold starts, 0 failures)

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

## Root cause + fix (2026-10-29)
Reproduced with the test bed's new `near-cold` scenario (`run.mjs near-cold`: cold start → search "Cultus" → tap the
village row, alternating offline/online). Before the fix, 3 of the first 5 runs failed, all with the same error:
`core: SELECT … item_section …: Call to function 'NativeDatabase.prepareAsync' has been rejected → The 2nd argument
cannot be cast to NativeStatement → Cannot use shared object that was already released`.
The failure is NOT the near file. It is the burst that comes after it: `useTown` reads ~139 waters at once (`water()`,
three statements each, so ~400 expo-sqlite calls in flight). On Android, expo-modules-core's
`SharedObjectRegistry.toNativeObject` reads its id map on the IO thread WITHOUT the lock. The JS thread adds one entry
per new statement, under the lock. While the map grows (after a cold start it is small, so it resizes), an IO-thread
lookup can miss a live statement. After a cold start the map is small, which is why only the first read failed. Once
it has grown, later bursts get through, which is also why opening a water's regulations first hid it.
Fix: every expo-sqlite call (open, get, all, close) goes through ONE app-wide queue (`drivers/serial.ts`, used by
`open.native.ts`), so a new statement is never created while another call's arguments are being looked up. SQLite
already ran a connection's statements one at a time. The blind 400 ms retry in `useTown` is removed. The
`console.error` log stays, and native read errors now name their file and query.
Acceptance: `near-cold` ×10 on the emulator (release APK, staging), 5 offline + 5 online: 10/10 listed (139 within
25 km), 0 failures, and no failed near read logged (the scenario also fails on a logged one).
