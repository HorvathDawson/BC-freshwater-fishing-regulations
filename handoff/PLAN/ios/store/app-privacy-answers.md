# App Store Connect: App Privacy answers (T727, PROPOSAL for the owner)

Everything here is a proposal. Nothing is entered anywhere. Round 2 (owner 2026-11-14, mid-day): the Play pages are the source; Precise location, install-id purposes (Analytics + App Functionality) are DECIDED; email linked stays open (default linked). Drafted 2026-11-14 from three sources and the code:
(A) `handoff/PLAN/app-integration/store/privacy-labels-draft.md` (Apple section), (B) `handoff/PLAN/play/data-safety.md`,
(C) `app/apps/mobile/app.json` `ios.privacyManifests` (from line 42). Paths below are under `app/` unless stated.
I do not know Apple's exact current wording of every screen: where a label may differ, it says "check".

## What the app sends (verified in code 2026-11-14)
- Crash report: build, environment, platform, data release, screen kind, open water id and pane, random install id, error name,
  scrubbed message and stack. No IP or user agent stored. `packages/ui/src/crashReport.ts:2-17`, payload `:131-145`.
  Default on; switch is More, Settings, Privacy "Send crash reports" (`packages/ui-native/src/menu/SettingsPage.tsx:8`).
- Install id: 16 random hex characters made once on the phone, nothing taken from the device (so it is not Apple's
  advertising id or vendor id). `crashReport.ts:152-167`.
- Problem report: only when the reader presses Send; reason, typed text, optional email, diagnostics rows left on, map centre
  to 3 decimals (about 100 m). `packages/core/src/menu/report.ts` (per data-safety.md rows 2-3; not re-read line by line today).
- Weather for a saved spot goes to Open-Meteo with coordinates rounded to 0.01 degree (about 1 km): `packages/data/src/spots/weather.ts:53-55` (read today).
- Satellite tiles come from Esri only when the reader picks Satellite (data-safety.md row 5).
- Not present: accounts, ads, advertising id, analytics SDK, third-party crash SDK, purchases, background location,
  WebView (no WebView in `packages` or `apps/mobile/src`; links open in the phone's browser or mail: `packages/ui-native/src/openLink.ts`).
- Production builds DO send reports: `apps/mobile/src/env.ts:44` gives production a report URL. (data-safety.md says
  production has none; that text is out of date, see Mismatches row 4.)

## The questionnaire, answer by answer

1. "Do you or your third-party partners collect data from this app?" **Yes.**
2. "Data used to track you": **none.** No tracking, no ATT prompt, no IDFA. (manifest `NSPrivacyTracking: false`, no domains, `app.json:43-44`.)
3. "Data linked to you": **see the Email row (proposal: linked); everything else not linked.** There is no account.
4. Data types (select these; every other type: **not collected**):

| Apple category > type | Collected | Linked to user | Tracking | Purposes | Why (what sends it) | Source agreement |
|---|---|---|---|---|---|---|
| Contact Info > Email Address | Yes | **OPEN, default: Yes (linked)**; draft and manifest say not linked | No | App Functionality | typed into a problem report to get a reply; optional | A, C say not linked; B: collected. See Mismatches 5 |
| Location > Coarse Location | Yes (DECIDED, matches Play's Approximate row) | No | No | App Functionality | Open-Meteo weather (0.01 degree), Esri tiles for the viewed area | A, B, C agree it is collected |
| Location > Precise Location | Yes (DECIDED, owner 2026-11-14) | No | No | App Functionality | a problem report's map centre at 3 decimals (about 100 m) | A, C say yes; B says approximate only (Mismatches 2) |
| User Content > Other User Content | Yes | No | No | App Functionality | the report text | A, B, C agree |
| Identifiers > Device ID | Yes | No | No | Analytics + App Functionality (DECIDED: analytics and bug reports) | the random install id in crash and problem reports; used to count distinct installs per error | A, B, C agree it is collected; purposes differ (Mismatches 3) |
| Diagnostics > Crash Data | Yes | No | No | App Functionality | crash report | A, B, C agree |
| Diagnostics > Other Diagnostic Data | Yes | No | No | App Functionality | problem-report diagnostics rows, recent error log | A, C agree |
| Diagnostics > Performance Data | **Proposal: No** | | | | nothing sent measures speed or memory (not verified: only the crash and report payloads were read) | A lists it, C does not (Mismatches 6) |
| Usage Data > Product Interaction | **Proposal: Yes** | No | No | App Functionality | a crash or report names the screen kind, open water id and pane | B says yes (App interactions); A, C omit it (Mismatches 1) |
| Photos or Videos, Audio, Contacts, Search History, Browsing History, Purchases, Health, Financial, Sensitive Info, Name, Phone, Address, Other Usage Data, Advertising Data | **No** | | | | photos and camera stay on the phone; search runs on the phone | all agree |

5. For each collected type Apple asks the same three things: linked to the user? used for tracking? purposes? The table gives them.
   Optional on the app side: crash reports have an off switch; problem reports are sent only on Send. Apple has no
   "optional" box, so nothing to enter (check: Apple lets a type be marked optional in some flows; if shown, say yes).
6. Third-party partners (Open-Meteo, Esri): the app itself calls them, so their receipt of coordinates is declared above
   as collected (owner decision 2026-10-29, kept in A and in the manifest).
7. Privacy policy URL: https://canifishthis.ca/privacy. Check: privacy page must name the same list (rule T268).
8. Export compliance: HTTPS only, `ios.config.usesNonExemptEncryption: false` (`app.json:39-41`). The first upload may still ask; answer "uses only standard encryption, exempt" (check the exact choice shown).

## Mismatches (three-way cross-check)

A = privacy-labels-draft.md, B = play/data-safety.md, C = app.json manifest. Recommendations are PROPOSALS.

| # | Item | A (draft) | B (Play) | C (manifest) | Evidence | Recommended answer (proposal) |
|---|---|---|---|---|---|---|
| 1 | App interactions / Product Interaction | absent | Yes (App interactions, optional) | absent | B table row "App interactions"; `crashReport.ts:2-17` sends screen kind + water id + pane | Declare Usage Data > Product Interaction, not linked, App Functionality. Add it to the manifest at the next config edit (not done: app.json is not mine to touch) |
| 2 | Precise vs coarse location | Precise (via Open-Meteo/Esri) and Coarse (report centre) | Approximate only; Open-Meteo rounded to 0.01 degree (B "Owner answers 2026-11-04") | Both Precise and Coarse | `weather.ts:53-55` rounds to 2 decimals, so Open-Meteo gets coarse; report centre is 3 decimals, `report.ts:110-114` per B | DECIDED (owner 2026-11-14): declare Precise, and Coarse where Play has the approximate row. Matches the manifest |
| 3 | Purposes for install id and crash data | App Functionality only | App Functionality and Analytics (counting installs per error) | App Functionality only | B table row "Device or other IDs"; worker keeps up to 100 ids per error kind, `workers/feeds/src/crashes.ts:79,191` per B | DECIDED (owner 2026-11-14): Device ID = Analytics + App Functionality. The manifest block still lists App Functionality only; an app.json edit for the owner to schedule (not done here) |
| 4 | Do production builds send reports | draft assumes yes | "A production build sends no reports" (`env.ts:38` per B) | declares them | `apps/mobile/src/env.ts:41-45`: production report URL `https://feeds.canifishthis.ca/report` (T404) | FLAGGED FOR THE OWNER: B is stale and is a Play-side correction (not edited here). For iOS the store build is production, so it DOES send: keep the declarations |
| 5 | Email linked or not | not linked | collected, no link field on Play | not linked | email is typed with a report, so it identifies the sender | OPEN, default linked (an email is identity; the report carries both email and install id). Owner may choose not linked |
| 6 | Performance Data | listed "Performance Data/Other Diagnostic Data" | n/a | only Other Diagnostic Data | no speed, memory or frame figure is in the payloads read | Do not declare Performance Data; declare Other Diagnostic Data only. Not verified for the report's "recent error log" contents |
| 7 | Where the crash switch is | More, Data | More, Data | n/a | `SettingsPage.tsx:8`: More, Settings, Privacy; the comment at `crashReport.ts:3` is old | Say "More, Settings, Privacy" in review notes and the privacy page. The two older docs predate T703 |
| 8 | Purposes list per type | one purpose each | per type, some two | one purpose each | see rows 1 and 3 | If rows 1 and 3 are accepted, the manifest block gets Product Interaction and an Analytics purpose on Device ID: a small app.json edit, to be a ticket, not done here |

Rows 4 and 7 were fixed on the Play page on 2026-11-14 (fa0d0a45c): production sends reports; the switch is under More, Settings, Privacy. The Play form needed no change.

## Not verified
- The exact screens and wording of App Store Connect's current questionnaire (shapes above are from memory of Apple's categories).
- That no other payload carries more (only crashReport.ts and weather.ts were read in full today; report.ts per data-safety.md).
- Whether a freshly prebuilt iOS app's generated PrivacyInfo.xcprivacy matches app.json (T365 read it back 2026-11-05; a rebuild after IOS-1 should repeat that).
