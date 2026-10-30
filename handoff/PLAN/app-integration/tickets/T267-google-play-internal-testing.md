# T267 — Google Play internal testing delivers dev/staging builds to the owner's phone

Status: DONE — first API upload f977bd212 (versionCode 5, 0.2.0, release r-ce76b4ed3624), 2026-10-29

Source: owner 2026-10-29 paid for a Google Play developer account. Chrome's APK scan hangs on our debug-signed sideload,
so staging builds go through Play internal testing. Package `ca.canifishthis.app`.

## What is built

- **Upload key** (Claude, 2026-10-29): `~/.canifishthis/upload-keystore.jks` (PKCS12, RSA 4096, alias `upload`, valid to
  2054), password in `~/.canifishthis/upload-keystore.properties` (chmod 600), certificate `upload_certificate.pem`.
  Outside the repo, never committed. **Back up the folder** (password manager or encrypted drive). If it is lost, Play
  App Signing lets the owner ask Google for an upload-key reset; the app itself stays signed by Google's key.
  - SHA-1   `EF:CE:2D:3A:74:DC:88:B7:9C:E0:F6:EA:CF:3D:69:10:1E:06:E7:CE`
  - SHA-256 `79:50:98:B4:34:AD:FE:E7:D8:F5:35:8B:F4:28:16:C5:1F:67:41:5B:51:B9:06:36:3E:91:FF:E5:E8:92:43:FC`
- **Gradle signing**: `app/apps/mobile/plugins/with-upload-signing.js` (an Expo config plugin; android/ is generated) makes
  the release build sign with the upload key when `CANIFISHTHIS_UPLOAD_PROPERTIES` names the properties file, else with
  the debug key as before. The sideload APK (`build-apk.sh` without `--play`) is unchanged.
- **`deploy/staging/build-apk.sh --play`**: bumps `android.versionCode` (= `ios.buildNumber`) past max(app.json,
  `~/.canifishthis/play-last-version-code`, highest code on Play) and commits only app.json; builds that HEAD in a temp
  worktree as an AAB (`bundleRelease`, arm64-v8a; `PLAY_ABIS` overrides); refuses an AAB not signed by the upload
  certificate; keeps it at `app/dist/aab/canifishthis-<ver>-<code>-<sha>.aab` (+ `latest.aab`); uploads to the
  **internal** track (`deploy/staging/play-upload.py`, Android Publisher API v3, google-auth from the repo `.venv`) with
  release notes = `git log` since the last Play upload; emails the owner (`/notify/apk`, `channel: "play"`) the tester
  link from `~/.canifishthis/play-test-url` (else the store page). `--no-upload` stops after the AAB.
  A draft app (listing not complete) only takes draft releases: the uploader retries as draft and says so; roll it out
  in the console.
- First AAB (2026-10-29): `app/dist/aab/canifishthis-0.1.2-4-c66f90a4d.aab` (= `latest.aab`), versionCode 4, 31.1 MB, signature checked against the upload cert.

## Blocked on T268 (owner)

The Play Console / Google Cloud steps are their own owner ticket, T268, to do when Google finishes verifying the
Play developer account. Until T268 is done, `--play` can only build (`--no-upload`).

After that, each new build is one command (heavy, waits for the machine lock):

    deploy/staging/build-apk.sh --play          # bump, build, upload to internal, email the link

Check without a build: `.venv/bin/python deploy/staging/play-upload.py --package ca.canifishthis.app --max-version-code`.

## Open

- T268 (owner). Then the first `build-apk.sh --play` run is the end-to-end test of the API upload + email.
- The staging feeds worker must be redeployed (`cd app/workers/feeds && npx wrangler deploy`) for the Play email
  wording; until then a `--play` email arrives with the sideload steps.
- The Play build reads staging data (`EXPO_PUBLIC_APP_ENV=staging`, env switch on); a production track is T86's job.

## Done (2026-10-29)
- `build-apk.sh --play` end to end: versionCode 4 -> 5 (f977bd212), AAB `app/dist/aab/canifishthis-0.2.0-5-f977bd212.aab`
  (31.2 MB, upload-key signed), uploaded to internal testing; `play-upload.py --max-version-code` = 5.
- Email: the script read the run token inside its build worktree (gitignored, absent) and got HTTP 403; fixed (2f8c1bd6c:
  `$REPO/app/workers/feeds/.run-token`); the Play email was re-sent by hand: HTTP 200.
- Sideload page refreshed: canifishthis-staging-0.2.0-2f8c1bd6c.apk (49.8 MB).
