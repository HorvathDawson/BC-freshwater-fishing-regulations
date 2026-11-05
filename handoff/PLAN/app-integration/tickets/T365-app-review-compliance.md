# T365 — App Review compliance

Status: PARTIAL 2026-11-05 — f74621e99, 38de56f (privacy manifest, encryption flag); the rest is the owner's (OWNER-TODO "iOS").
(owner 2026-11-03: Android 0.5.0 first, iOS next; from the ticket check's iOS gaps.)

Before the first TestFlight/App Store submit: `PrivacyInfo.xcprivacy` (required-reason APIs), the tracking
declaration (none), privacy nutrition labels (with T244), final bundle id and product name (Xcode shows "CanIFish",
the app says "Can I Fish?"), and the account-deletion rule only if T223 accounts ever ship. Size: S.

## The pass (2026-11-05, against the built app: app/dist/ios-sim from f74621e99, Info.plist + PrivacyInfo read back)

### Met
| guideline / requirement | where |
|---|---|
| Privacy manifest (app's own) | app.json `ios.privacyManifests` → PrivacyInfo.xcprivacy: NSPrivacyTracking false, no tracking domains; required-reason APIs UserDefaults CA92.1, file timestamp C617.1, boot time 35F9.1, disk space E174.1. 12 pod manifests ride along (Expo, React, blob-util bundles). The new exit-info module uses none of the listed APIs (it names its files by time instead of reading their dates). |
| Collected data declared, none linked, none tracked | the same manifest, matching `store/privacy-labels-draft.md` (owner 10-29): crash data, other diagnostics, device id (the T352 install id), other user content (report text), email (only if typed), precise + coarse location (Open-Meteo, Esri, a report's map centre); all App Functionality |
| No ATT prompt needed (5.1.2) | no IDFA, no ad/analytics SDKs, no tracking |
| Export compliance | `ios.config.usesNonExemptEncryption: false` → ITSAppUsesNonExemptEncryption false (HTTPS only) |
| Purpose strings only for what is used (5.1.1) | location when-in-use, camera, photos; no Always-location, motion or microphone (T313 dropped them; read back from the built Info.plist) |
| Account deletion (5.1.1(v)), Sign in with Apple (4.8) | n/a: no accounts, no sign-in |
| Privacy policy reachable in the app (5.1.1) | More → About → Privacy policy (`PRIVACY_URL` https://canifishthis.ca/privacy); the page now says what an iPhone sends (T362, 5918cfa1e) |
| Government information (5.1.1 / 5.2) | More → Legal "Not the official regulations", licence wording (© Province of BC, not endorsed; DFO not official) |
| Downloaded content is data, never code (2.5.2) | releases are data files; no OTA JS updates |
| No payments, no ads | — |
| Crash reporting disclosed and switchable | More → Data "Send crash reports", privacy page |

### Missing (owner — OWNER-TODO "iOS")
1. Apple Developer account; the App Store Connect app record (bundle id `ca.canifishthis.app`, SKU, primary language).
2. **The name**: the home-screen name is "Can I Fish?" (expo.name), the permission prompts and the web say "Can I Fish This?"
   — the same question as Play's listing (T371). One answer, then app.json's strings follow.
3. App Privacy answers in App Store Connect from the draft; confirm the install id as "Device ID, not linked" (Play says
   Device IDs = Yes), and that Open-Meteo/Esri location counts as collected (decided 10-29).
4. Age rating questionnaire, category, support URL (support@canifishthis.ca / a page), screenshots (6.9" and 6.5"),
   review notes (no login; the data source; what "Simulate offline" is if a staging build is sent).
5. Background modes: the built app declares `fetch` + `processing` (expo-background-task: release downloads). Review may
   ask why; the answer is "downloads the next regulations release in the background".
6. iPad: `supportsTablet: false` → iPhone-only, but App Review also runs it on an iPad in compatibility mode. Not checked
   (no iPad simulator run yet; a follow-up if Review raises it).
7. The review build must read PRODUCTION data, which needs the production hosting (T86) live first.
The Xcode target/product name "CanIFish" is not user-visible (only in crash logs and the .app name): no change needed.
