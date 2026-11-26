# TestFlight external testing: what Apple asks for

Only the owner submits this. `deploy/staging/testflight-distribute.mjs` does the external group after a build uploads.

## How it works
- Internal testers (you, up to 100 people on your team): a build is usable as soon as Apple has processed it. No review.
- External testers (up to 10,000, by email or by a public link): the first build of each version goes through Apple's
  **Beta App Review** first. Usually about a day. Later builds of the same version are normally passed quickly.
- A build expires 90 days after upload.

## Fields under TestFlight → Test Information
| Field | Value (proposal) | Source |
|---|---|---|
| Beta App Description | Can I Fish This? shows the BC freshwater fishing regulations for the water you tap on a map, with river conditions, salmon notices and saved spots. It works without cell service after a first download of about 480 MB. | `listing.md`, the Play description |
| Feedback Email | the owner's support address (the owner enters it) | not written here |
| Marketing URL | https://canifishthis.ca | `listing.md` |
| Privacy Policy URL | https://canifishthis.ca/privacy | `listing.md` |
| Contact (first name, last name, phone, email) | the owner's | owner |
| Sign-in required | No (the app has no accounts) | `../APP-STORE-SUBMISSION.md` section 6 |
| Review notes | take them from `../APP-STORE-SUBMISSION.md` section 6 (no sign-in; the first open downloads about 480 MB, Wi-Fi advised) | `../APP-STORE-SUBMISSION.md` section 6 |

## "What to Test" (per build, shown to testers)
PROPOSAL: "First iPhone test build. Please try: opening the app and waiting for the first download (about 480 MB,
Wi-Fi advised); tapping a river or lake and reading its regulations; the Conditions tab; saving a spot with a photo;
turning on airplane mode and using the map. Tell us what was slow, confusing or wrong with the report button under
More, or reply to the invitation email."

## Export compliance
Already answered in the build: `usesNonExemptEncryption` is `false` in `app/apps/mobile/app.json`, so App Store Connect
does not ask.
