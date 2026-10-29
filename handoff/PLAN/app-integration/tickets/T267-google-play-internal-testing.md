# T267 — Google Play internal testing delivers dev/staging builds to the owner's phone

Status: PARTIAL — Claude part done; the owner's Play Console / Google Cloud steps below are open

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
- First AAB: built by `build-apk.sh --play --no-upload --no-email` (see the ledger row for the path and versionCode).

## Owner steps (only you can do these)

1. **Play Console → Create app**: name `canifishthis`, default language English (Canada or US), App, Free. Accept the
   declarations.
2. **Play App Signing**: on the first upload, accept "Let Google manage and protect your app signing key" (the default).
   Google holds the app signing key; we hold the upload key (fingerprints above, if the console asks).
3. **Testing → Internal testing → Create new release**: upload the first AAB by hand (Play requires the first one in the
   console): `app/dist/aab/latest.aab`. Release name/notes anything. Save, Review, Start rollout to Internal testing.
4. **Testers** tab: create an email list with your Gmail, save; copy the **opt-in link** ("Join on the web") and put it
   in `~/.canifishthis/play-test-url` (one line). Open it on the phone once and accept.
5. **App content** (Policy → App content) — Play blocks or warns until these are done; our answers:
   - Privacy policy URL: needed (the app asks for location/camera). A short page on canifishthis.ca: no account, no
     tracking, spots/photos stay on the device; location used only on the device to pin a spot or show what is near;
     the optional problem report sends the text you type and, only if you add it, your email, used only to reply.
   - App access: all functionality available without login.
   - Ads: no ads.
   - Content rating: questionnaire, category Reference/Utility, no violence/sharing/purchases → Everyone.
   - Target audience: 18+ (avoids the Families policy); not designed for children.
   - Data safety: collects data = Yes, only "Email address" (optional, from Report a problem; purpose App functionality /
     developer communications; not shared; user can ask for deletion) and the report's free text ("Other user-generated
     content", optional). Location and photos are processed on the device only, not collected. Encrypted in transit: yes
     (HTTPS). No data shared with third parties.
   - News app: no. Government app: no (it is not an official BC government app). Health / financial features: none.
   - Store listing (needed before a wider track, harmless now): short + full description, icon 512x512, feature graphic
     1024x500, 2+ phone screenshots, category Maps & Navigation or Sports, contact email.
6. **Google Cloud → service account**: in the Cloud project Play Console links (Setup → API access, or any project):
   enable "Google Play Android Developer API"; IAM → Service accounts → Create (`play-uploader`), no roles; Keys → Add
   key → JSON; save as `~/.canifishthis/play-service-account.json` and `chmod 600` it. Never in the repo.
7. **Play Console → Users and permissions → Invite new user**: the service account's email; App permissions → add
   canifishthis → **Release to testing tracks** (+ View app information). Access can take up to a day to start working.

After that, each new build is one command (heavy, waits for the machine lock):

    deploy/staging/build-apk.sh --play          # bump, build, upload to internal, email the link

Check without a build: `.venv/bin/python deploy/staging/play-upload.py --package ca.canifishthis.app --max-version-code`.

## Open

- Owner steps 1–7.
- The Play build reads staging data (`EXPO_PUBLIC_APP_ENV=staging`, env switch on); a production track is T86's job.
