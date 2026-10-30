# T268 — OWNER: set up Google Play internal testing (when Play account verification completes)

Status: DONE (9046ffa63) — owner set up Play 2026-10-29: app created, AAB versionCode 4 on internal testing, testers opt-in link saved, service account play-uploader@canifishthis.iam.gserviceaccount.com invited; `play-upload.py --max-version-code` answered 4 from the API (no hash: owner console work).

Unblocks T267 (Claude side, done: upload key, `deploy/staging/build-apk.sh --play`). Only the owner can do these, in
Play Console and Google Cloud. Package **ca.canifishthis.app**.

First AAB (versionCode 4, upload-key signed): `app/dist/aab/latest.aab` (pinned copy `app/dist/aab/canifishthis-0.1.2-4-c66f90a4d.aab`, 31.1 MB, built 2026-10-29 from c66f90a4d).
Upload certificate (if Play asks): SHA-1 `EF:CE:2D:3A:74:DC:88:B7:9C:E0:F6:EA:CF:3D:69:10:1E:06:E7:CE`,
SHA-256 `79:50:98:B4:34:AD:FE:E7:D8:F5:35:8B:F4:28:16:C5:1F:67:41:5B:51:B9:06:36:3E:91:FF:E5:E8:92:43:FC`
(certificate file `~/.canifishthis/upload_certificate.pem`).

## Checklist


1. **Play Console → Create app**: name `canifishthis`, default language English (Canada or US), App, Free. Accept the
   declarations.
2. **Play App Signing**: on the first upload, accept "Let Google manage and protect your app signing key" (the default).
   Google holds the app signing key; we hold the upload key (fingerprints above, if the console asks).
3. **Testing → Internal testing → Create new release**: upload the first AAB by hand (Play requires the first one in the
   console): `/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/app/dist/aab/latest.aab`
   (any later `--play --no-upload` build there is fine too). Release name/notes anything. Save, Review, Start rollout to Internal testing.
4. **Testers** tab: create an email list with your Gmail, save; copy the **opt-in link** ("Join on the web") and put it
   in `~/.canifishthis/play-test-url` (one line). Open it on the phone once and accept.
5. **App content** (Policy → App content) — Play blocks or warns until these are done; our answers:
   - **Privacy policy URL (built 2026-10-29): `https://canifishthis.ca/privacy`** (production; live once the site is on the .ca; staging copy: https://staging.canifishthis.ca/privacy). Static page from the SEO build (`tools/seo/html.ts` `privacyBody`), linked from the site footer, About, Sources and licences and the report form. Contact on it is the placeholder privacy@canifishthis.ca (OWNER-TODO).
   - (original note) Privacy policy URL: needed (the app asks for location/camera). A short page on canifishthis.ca: no account, no
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

## Done when

`.venv/bin/python deploy/staging/play-upload.py --package ca.canifishthis.app --max-version-code` prints a number (the
service account works), then `deploy/staging/build-apk.sh --play` uploads and the phone gets the update from Play.
Tell Claude; T267 closes on that first API upload.

## Gotcha (owner 2026-10-29)
"Item not found" on the internal test: the Play Store app had defaulted to a different Google account. Switch the Play Store to the tester account BEFORE opening the opt-in link. Tell every future tester this.
