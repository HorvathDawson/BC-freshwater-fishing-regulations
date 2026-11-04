# T390 — The app notices it is not the latest version and offers an Update button

Status: OPEN (owner 2026-11-04, after 0.5.0).

The app should know when a newer version of itself exists and let the reader update from inside it, with one button.

- **Android:** Google Play's in-app updates (Play Core "flexible" update: the download runs in the background, then a
  "Restart to update" button; "immediate" only when the data needs it). A well-established Expo-compatible package
  (owner rule: a better established package beats a minimal one). It works only for installs from Play (internal,
  Closed, production); a sideloaded staging APK points to the staging APK page instead.
- **iOS** (later, with T88): there is no in-app install, so the button opens the App Store page. The newer version
  comes from the store lookup.
- **When the data needs it:** a release whose `minAppVersion` is above this app (the staging releases since 0.5.0) is
  refused today and the app keeps its old data silently for the reader. That case must say so ("New data needs the latest
  app") with the same Update button. It is the strong prompt; a plain newer version is the gentle one.
- **Where:** the update notice the app already shows (the bell / updates sheet), never a modal on open; asked again at
  most once a day; offline, it says nothing.
- Failures are shown + `reportFailure` (T368); one load path.

Check first: what the update sheet says today when a release is refused for `minAppVersion` (is the reader told at all?).
Test: a release with `minAppVersion` above the app (deploy/staging/test-updates.mjs `min-app <semver>`) shows the
strong prompt; the Play flow on an internal-testing install (emulator with Play services or the owner's phone).
