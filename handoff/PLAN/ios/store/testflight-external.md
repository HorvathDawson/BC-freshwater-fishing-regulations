# TestFlight external testing: what Apple asks for (PROPOSAL, nothing entered)

Asked by the owner 2026-11-14, verbatim: "could we go straight to external testers too?" Written the same evening,
after the first upload (0.7.10 build 33, staging data). Only the owner submits this; Claude enters nothing.

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
| Sign-in required | No (the app has no accounts) | `review-notes.md` |
| Review notes | the first three paragraphs of `review-notes.md` (no sign-in; the first open downloads about 480 MB, Wi-Fi advised; the data is a test copy of the BC regulations) | `review-notes.md` |

## "What to Test" (per build, shown to testers)
PROPOSAL: "First iPhone test build. Please try: opening the app and waiting for the first download (about 480 MB,
Wi-Fi advised); tapping a river or lake and reading its regulations; the Conditions tab; saving a spot with a photo;
turning on airplane mode and using the map. Tell us what was slow, confusing or wrong with the report button under
More, or reply to the invitation email."

## Export compliance
Already answered in the build: `usesNonExemptEncryption` is `false` in `app/apps/mobile/app.json`, so App Store Connect
does not ask.

## Before the first external build (my recommendation, the owner decides)
1. The owner installs build 33 from the internal group first. No iPhone has run this app yet; only the simulator has.
2. Do not send build 33 itself to outside testers. On it the first-download bar most likely does not move on iPhone,
   and relaunching the app restarts the 480 MB download. Both are fixed on `ios/t722-install`, not merged.
3. Build 33 reads staging data. Outside testers on staging data is the same call as Play closed testing; one answer for
   both platforms.
4. The next iPhone build (with the iOS branches merged) is the one to submit for Beta App Review.
