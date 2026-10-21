# D1 — phone testing: a development build now, a preview build on staging next

Owner, 2026-10-21: do both options.

## How the app uses the network (why this matters)
First launch downloads the manifest, the data release (~14 MB), the atlas tiles (~546 MB) and the basemap (~918 MB);
after that the app is offline. Online it also checks for updates (only changed files), reads the live gauge feed, Open-
Meteo weather and Esri imagery (online-only). So testing needs a server for the first install, the update check and the
feed; the APK itself carries no data.

## Option 1 — development build on the owner's phone (now; no hosting needed)
- Phone in developer mode, USB debugging on, on the same Wi-Fi as the Mac (or USB with `adb reverse`).
- `pnpm -C app/apps/mobile android` (`expo run:android`): builds a debug app, installs it, loads the JS live from Metro on
  the Mac; edits appear in seconds.
- Data: `EXPO_PUBLIC_DATA_BASE=http://<Mac LAN IP>:39217` with `pnpm tiles` running (serve-tiles listens on all
  interfaces). Debug builds allow plain http to the dev host; release builds never do.
- Use it to judge: gestures (edge back, view swipe, sheet drag, chart zoom), map pan/colour speed, the Vulkan vs OpenGL
  question (N1/N2), the Keep tab and Conditions on a real screen, memory with 1.4 GB of tiles open.
- Deliverable: a short step-by-step for the owner (developer mode, USB, the one command), and a checklist of what to try.

## Option 2 — preview build against a staging server (before sharing with anyone)
- Staging = an R2 bucket with a public HTTPS URL (the owner's; production hosting is planned on R2 anyway). The owner
  runs `pipeline/tools/publish.py` (human-run, credentials from env) to upload a release + tiles.
- An EAS / local "preview" build profile with `EXPO_PUBLIC_DATA_BASE=https://<staging>`; a standalone APK (sideload or
  Google Play internal-testing track). Release config: no cleartext, real signing key kept by the owner.
- Test what only a production-like build can: first-run download with progress, resume after a dropped connection, the
  update check downloading only changed files, offline launch in airplane mode, rollback of a bad set, attribution.
- Deliverable: the preview profile + a test script (the P11 boot/install/update/rollback cases on a phone).

## Order
Option 1 as soon as the owner has the phone handy; option 2 after the owner creates the R2 bucket (doubles as the
production-hosting dress rehearsal).
