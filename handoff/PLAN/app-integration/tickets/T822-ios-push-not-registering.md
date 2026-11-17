# T822 — iPhone push: the phone never registers with the staging worker

Status: OPEN (app / worker; app chat; investigating)

Source: owner 2026-11-16, in the app chat (~21:37, on iPhone build 36), verbatim: "allowed notifications in app and settings restarted app and it still doesnt
show up". After he turned on More → Notifications → "New regulations data" and allowed notifications in the app and in
Settings, two test pushes (21:34, 21:37, `POST /run/push` on the staging worker) found `ios.tokens: 0`: no phone
registered. The worker's /push endpoints answer (register 400 on an empty body). Next: tail the worker while the app
starts, read build 36's registrar (`packages/data/src/notify/push.ts`, `apps/mobile/src/pushToken.ios.ts`, `env.ts
pushUrlFor`) and the APNs entitlement / environment in build 36.
