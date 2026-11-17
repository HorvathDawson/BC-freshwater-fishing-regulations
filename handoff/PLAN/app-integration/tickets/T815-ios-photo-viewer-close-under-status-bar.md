# T815 — iOS: the spots photo viewer's close ✕ sits under the status bar

Status: OPEN (app, iOS; app chat)

Source: owner 2026-11-16 ~21:22, testing iPhone TestFlight build 36 (app chat; pictures in the chat). Verbatim: "the picture viewer for spots has the x under the notification bar".

## Look at
The photo viewer (`photo-zoom` scenario on Android) does not inset its close control by the top safe area on iOS (the
notch / Dynamic Island). Fix with the safe-area inset the rest of the app uses; check the web and Android unchanged.
