# T1014 — Play update check: production phones read `play/43.json`, which only the staging bucket holds (HTTP 404)

Status: OPEN — next release batch (the file copy is the owner's: production R2).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1014 | Play update check: production phones read `app/android/play/43.json`, which only the staging bucket holds (HTTP 404) | OPEN — next release batch | | owner: copy 43.json to production R2; build-apk.sh --play to both buckets |

Source: T1005 triage, crash kind `804e01a3d7d4fd97` (production, 3 Android 0.8.4 phones, build 42 c1f14c1, 2026-11-22
01:53–18:08 UTC): `HTTP 404 for …` in `readJson` (`apps/mobile/src/appUpdater.native.ts` `STORE_FILES`).

## What happens
The Play update check (T894) asks Play which build it offers (`availableVersionCode`) and reads that build's notes from
OUR data origin, `<data>/app/android/play/<versionCode>.json`. Play offers versionCode 43 (0.8.5, sha 558c527) to the
0.8.4 production phones on Internal / Closed: build 43 is a STAGING-data build put on the same tracks. Its version file
went only to the staging bucket (`deploy/staging/build-apk.sh --play` publishes to the bucket of the build's own
environment): `data-staging.canifishthis.ca/app/android/play/43.json` is 200, `data.canifishthis.ca/…/43.json` is 404
(checked 2026-11-22; production holds 42.json). Every check on those phones fails and is reported; they cannot show
"What's new in 0.8.5".

## Work
1. Now (owner, production R2): copy the staging file to production, e.g.
   `curl -s https://data-staging.canifishthis.ca/app/android/play/43.json > /tmp/43.json && npx --prefix app wrangler r2 object put --remote bc-fishing-regulations/app/android/play/43.json --file /tmp/43.json --content-type application/json --cache-control no-cache`.
2. `build-apk.sh --play` (lane that owns the Android build scripts): every Play upload's `play/<versionCode>.json` goes to
   BOTH buckets — a phone of either environment on that track is offered it. `play.json` (the last upload, read by 0.8.1
   installs) stays per environment.
3. A test (or the script's dry run) that names both destinations.

Depends: none. Size: S. Owner: item 1 (production R2).

## Owner

- [ ] 2026-11-22 — Copy the Play version file this ticket names from the staging bucket to production R2 (CONFIRM_PROD): 3 production phones get a 404 on their update check. Skip if it was done with the 0.10.0 production commands.
