# T822 — iPhone push: the phone never registers with the staging worker

Status: IN PROGRESS (fix on `app-fix/t822-push-register` f4598b112, merged with T739 on `lane/l5-push` 2026-11-20; left: the 2026-11-20 09:07: lane/l5-push merged (f9c2e42b5, reviewed twice), staging feeds worker deployed (push.missing.ios == [], 0 tokens); left (Claude): the TestFlight build with this release, then the owner phone test.
main chat merges it into redesign/stream-sections, deploys the staging worker, builds TestFlight; then the owner's phone test)

Source: owner 2026-11-16, in the app chat (~21:37, on iPhone build 36), verbatim: "allowed notifications in app and settings restarted app and it still doesnt
show up". After he turned on More → Notifications → "New regulations data" and allowed notifications in the app and in
Settings, two test pushes (21:34, 21:37, `POST /run/push` on the staging worker) found `ios.tokens: 0`: no phone
registered. The worker's /push endpoints answer (register 400 on an empty body). Next: tail the worker while the app
starts, read build 36's registrar (`packages/data/src/notify/push.ts`, `apps/mobile/src/pushToken.ios.ts`, `env.ts
pushUrlFor`) and the APNs entitlement / environment in build 36.

## What was found and fixed (2026-11-16, app chat; written here 2026-11-20 by lane L5)
The full write-up is T739 "Follow-up 3". In short: build 36's registrar had two silent paths. (1) The system's token call
(`getDevicePushTokenAsync`) had no deadline: when iOS never answers `didRegister…`, nothing was sent, nothing reported,
and the sync queue was held for the run. (2) Offline and the worker's 429 (its 2 s per-address gap, hit by a quick
off → on) were thrown as "the environment's": never reported, never shown. Fix (f4598b112): a 30 s token deadline then a
REPORTED error (a late token still registers through `onToken`); a 429 waited out once, a second one reported; every
outcome said on More → Notifications ("Server wake-ups: …", with "Try again"). Which of the two hit build 36 is not
known (the worker's dashboard logs for 2026-11-17 04:00–04:40 UTC would say); the new line will say it on the phone.

## Merge (2026-11-20, lane L5, `lane/l5-push`)
Pushed `app-fix/t822-push-register` to origin; merged redesign/stream-sections (27e98e4eb) into it with the T739 Android
removal and the privacy line (T739 "Merge into the shared branch"). The simulator register / unregister run and its
pictures are in T739.
