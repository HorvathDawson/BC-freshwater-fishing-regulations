# T595 — Error reports that are not defects stop arriving as errors

Status: IN REVIEW — b1f252e7c, 5af74464c, 40d8abb75 (no Play service is a known state; test-bed builds tagged; the worker classes each crash kind). The worker change is deployed (staging 2026-11-12, production 2026-11-18); the emulator-install leftovers were done in T603 items 5-6 (46afd5ad1). The acceptance read is `GET /crashes`, which answers 403 without the RUN_TOKEN (checked 2026-11-20 on staging and production; the lane may not read the token). owner: see ## Owner: he (or a session he gives the token) runs the two curls and records that a fresh emulator install files no error kind and the unflagged list shows only defects.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T595 | Error reports that are not defects stop arriving as errors (no Play service, test bed untagged, network, old app) | IN REVIEW — owner: see ## Owner | b1f252e7c 5af74464c 40d8abb75 | worker deployed; leftovers done in T603 (46afd5ad1); /crashes needs the run token: the owner's two curls |

Source: worker `GET /crashes` read 2026-11-10 (40 kinds since 10-29). Owner: "get feed worker reports and errors and make
sure all are covered."

## Done (b1f252e7c, 5af74464c, 40d8abb75; worker deployed staging 2026-11-12, production 2026-11-18)
- "Failed to check for updates: Failed to bind to the service" (kind 8316104d, 11 installs, the test bed emulators with
  no Play Store): "no Play service" is a known state, not a failure (the own-version check, T390).
- Test bed installs tagged test (T291 tag; `SecurityException: Unsupported path /data/local/tmp/…`, 44215092); the
  emulator-install leftovers in T603 items 5-6 (46afd5ad1).
- The worker gives each kind a `class`: `defect` / `network` / `expected` (old app meeting a newer release, no Play);
  `GET /crashes` lists defects by default, the digest counts the others in one line. Nothing is dropped.
- T1005 (lane L5, 2026-11-22) added a `crawler` class and more EXPECTED / NETWORK patterns: T1005 "Done".

## Left
- The acceptance read: `GET /crashes` answers HTTP 403 without the bearer RUN_TOKEN (checked 2026-11-20 on staging and
  production); the lane may not read the token: the owner's two curls (## Owner).

## Acceptance
- A fresh emulator install with no Play Store files no error kind.
- `GET /crashes` with no flags shows only kinds that need a fix.

Depends: —. Size: S–M. Owner: no.
## Owner

- [ ] 2026-11-19 — Run the feeds worker's `GET /crashes` on staging (`https://bc-fishing-feeds-staging.horvath-dawson.workers.dev`) and production (`https://feeds.canifishthis.ca`) with your RUN_TOKEN (Claude may not read it): `curl -H "Authorization: Bearer $RUN_TOKEN" "<worker>/crashes?since=2026-11-09"`. Look for only `defect` kinds and none from the 11-19/20 emulator installs; tell Claude (or give a session the token).
