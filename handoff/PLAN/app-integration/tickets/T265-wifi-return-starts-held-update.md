# T265 — a big update held for Wi-Fi does not start when Wi-Fi comes back

Status: DONE (2026-10-29) — b96ffb83f, 38a508b3e

Source: T255 test bed 2026-10-29 (`run.mjs big-update`, emulator, release APK on staging test channel). A release over
the metered line (`test-updates.mjs big`, ~270 MB) on mobile data correctly reads "An update is waiting for Wi-Fi"
(sync button, More → Data, Updates log `waiting-wifi`). Wi-Fi back (unmetered, validated — `dumpsys connectivity`):
NOTHING starts — 90 s, then 15 more minutes, still "waiting for Wi-Fi". Only the next foreground return after the
release was due (`foregroundAfterMs.release`, 10 min) checked again, downloaded and applied it.

## Suspect (reading)
App.tsx's update-service network: `metered()` is `connectivity.metered() || (NET.mode().low && why !== "setting")`
and the Wi-Fi return reaches the service through `connectivity.watchMetered` → `on(true)` → `maybeStage()`. The
emulator's mobile data is HSPA, so T254's low-bandwidth mode was on ("slow mobile connection"); `NET.mode()` is set by
a React hook (`useNetMode`) a render AFTER NetInfo's event, so at the instant `maybeStage` asks, the mode is still low,
the update stays held, and nothing asks again when the mode clears.

## Work
1. Re-run `maybeStage` when the low mode leaves (a `Net` mode subscription, or the service's network subscription also
   told on `setMode` low → not low); test-first in data `update/service.test.ts` (held → Wi-Fi + mode clears → stages).
2. Re-check the T255 `big-update` step "Wi-Fi back → the download starts by itself".
## Acceptance
`run.mjs big-update` passes its Wi-Fi-back step; a service test pins the re-try on the mode change.
Depends: T231, T254. Size: S. Owner: no.

## Done (2026-10-29)
`phoneNetwork` (data update/service.ts) is the service's phone network (moved out of App.tsx): it also tells the
service when `Net`'s low mode clears (`Net.subscribeMode`, new). Service test "T265: held on a slow mobile line…"
pins it. Test bed `big-update`: "Wi-Fi back → the download starts by itself" PASS (twice); only the T263 bell step fails.
