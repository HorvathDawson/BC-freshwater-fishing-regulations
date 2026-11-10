# T595 — Error reports that are not defects stop arriving as errors

Status: IN REVIEW (b1f252e7c, 5af74464c, 40d8abb75; worker change waits for the next feeds deploy; items 2: test-bed + simulator builds tagged, an owner-APK installed on an emulator is not — see proposal)

Source: worker `GET /crashes` read 2026-11-10 (40 kinds since 10-29). Owner: "get feed worker reports and errors and make
sure all are covered."

## Today
1. **"Failed to check for updates: Failed to bind to the service"** (kind 8316104d): 11 reports, 11 installs, every
   build 0.6.5 → 0.7.2, Android 35. Each is an install with no Play Store to answer (the test bed emulators). The app's
   own-version check (T390) files the failure as an error on every fresh install.
2. **Test bed installs are not tagged test**: the above, plus `SecurityException: Unsupported path /data/local/tmp/…`
   (44215092). The test tag (T291) exists; these runs did not carry it.
3. **Plain network failures arrive as errors**: "first run: the manifest could not be read (HTTP 504)", "update: fetch
   failed: UnknownHostException", "Failed to fetch". They are shown to the reader already; as error kinds they hide
   real defects.
4. **An old app meeting a newer release** ("release r-… is not one this app reads", 4 kinds, 12 reports on 0.6.9):
   the app is right to refuse and prompt; it is not an error.

## Work
1. Own-version check: "no Play service" is a known state, not a failure. No error report; the More tab says the check
   could not run and why.
2. The test bed and emulator scripts send the test tag on every run (one place, checked by a test).
3. Give the worker a `class` on each kind: `defect` / `network` / `expected` (old app, no Play). `GET /crashes` lists
   defects by default; the digest counts the others in one line. Nothing is dropped.

## Acceptance
- A fresh emulator install with no Play Store files no error kind.
- `GET /crashes` with no flags shows only kinds that need a fix.

Depends: —. Size: S–M. Owner: no.
