# T285 — Opening the synopsis PDF fails in the Android app ("couldn't load synopsis")

Status: OPEN (owner 2026-10-29, on the staging APK).

## Work
Reproduce on the testbed emulator (staging data). Find why (native path using a web-only blob/pdf-lib route, file URI
permissions, missing viewer intent, download/cache failure, the T256 pdf-lib split). Make "Open the official Synopsis" and
the regs page's rule-book links open the PDF at the right page on Android (system viewer via a content:// URI or an in-app
viewer), offline too when the release's synopsis is on the device. Clear message if no viewer exists.

## Acceptance
Emulator: link opens the PDF at the cited page, online and in airplane mode; web unchanged; test pins the native path.

Depends: none. Size: S. Owner: no.
