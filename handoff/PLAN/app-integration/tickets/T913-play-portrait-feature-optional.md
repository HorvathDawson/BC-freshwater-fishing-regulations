# T913 — Play refuses review: the portrait lock declares a required hardware feature

Status: DONE — shipped in 0.8.3 (merged 0813cc2b1; staging + production 2026-11-19).

Owner 2026-11-19 ~11:05 (main chat), promoting 0.8.2 (37) to closed testing, pasted Play Console's stop: "We found
some common issues that are preventing your app from being sent for review … Required hardware feature declarations
are restricting your app's availability … android.hardware.screen.portrait affects Phone, Tablet, Desktop, and Android
XR users. To expand your app's availability, mark non-essential hardware features as optional in your app's manifest."
Also: "There is no deobfuscation file associated with this App Bundle" — not applicable: R8 is off unless
CANIFISHTHIS_R8=1 and never in a production build (T749); build 37 was not minified.

## Cause and fix
app.json `orientation: "portrait"` makes the activity `screenOrientation="portrait"`, which Play reads as an implied
`<uses-feature android:name="android.hardware.screen.portrait"/>` (required). The app still runs in portrait; it does
not need a portrait-capable screen to install. Fix: a config plugin (`plugins/with-optional-portrait.js`, applied by
`expo prebuild`) declares `<uses-feature android:name="android.hardware.screen.portrait" android:required="false"/>`,
pinned by `app/tools/optional-portrait.test.ts`. Then 0.8.2 build 38 to Play internal for the owner to promote.
