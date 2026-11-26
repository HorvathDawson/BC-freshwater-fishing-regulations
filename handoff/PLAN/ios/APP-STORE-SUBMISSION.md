# App Store submission: the page to paste from

Every click is the owner's: **PASTE** (text is ready), **PICK** (choose the option named) or **YOURS** (only you know it),
in the order App Store Connect asks.

**The listing words are not on this page.** One source, `app/store-listing/listing.json` plus `full-description.txt`,
serves Google Play, the App Store and the website; `python3 deploy/staging/play-listing-push.py --appstore` prints every
App Store Connect field with its length and limit (no network), and `app/tools/store-listing.test.ts` holds the limits.
PASTE Name, Subtitle, Promotional Text, Description and Keywords from that output.

## 0. Where things stand

Apple REQUIRES the iPhone 6.1"/6.3" "medium display" pictures (1206 x 2622 or 1179 x 2556); the 6.9" and 6.5" sets are
optional. Submit after the privacy page is live on production.

## 2. Account level (once) and App Information (General)

Business, Agreements: the Free Apps Agreement must say Active (YOURS). Compliance, Digital Services Act: PICK **not a
trader**.

| Field | Answer |
|---|---|
| Name (30), Subtitle (30) | PASTE `Can I Fish This?`, `BC fishing rules, by the water` (exactly 30) |
| Primary language | PICK English (Canada) |
| Bundle ID | already `ca.canifishthis.app` |
| Category | PICK Primary: Navigation. Secondary: Sports. |
| Content Rights | PICK "Yes, it contains, shows, or accesses third-party content", tick the rights (section 5) |
| License Agreement | leave Apple's standard one |

## 3. App Privacy (Trust & Safety)

Privacy Policy URL: PASTE `https://canifishthis.ca/privacy`. "Do you or your third-party partners collect data?" PICK
**Yes**. Tick exactly these types; everything else unticked.

| Apple's group → type | Purposes to tick | Linked to the user? | Tracking? | What sends it |
|---|---|---|---|---|
| Contact Info → Email Address | App Functionality | **Yes** | No | the optional email on a problem report |
| Location → Precise Location | App Functionality | **Yes** | No | a problem report's map centre, 3 decimals (about 100 m; Apple calls 3 or more "precise") |
| Location → Coarse Location | App Functionality | No | No | weather for a saved spot (2 decimals, about 1 km, to Open-Meteo); the area on screen when Satellite is on (Esri) |
| User Content → Other User Content | App Functionality | **Yes** | No | the text typed in a problem report |
| Identifiers → Device ID | App Functionality, Analytics | **Yes** | No | the random install id in crash and problem reports; the Apple push token (kept while notifications are on) |
| Usage Data → Product Interaction | App Functionality | **Yes** | No | a crash or report names the screen, the open water and the tab |
| Diagnostics → Crash Data | App Functionality | **Yes** | No | crash reports (on by default; switch under More, Settings, Privacy) |
| Diagnostics → Other Diagnostic Data | App Functionality | **Yes** | No | the diagnostic rows and recent error lines on a problem report |

Not collected: Name, Phone, Address, Health, Fitness, Financial, Sensitive Info, Contacts, Emails or Text Messages,
Photos or Videos (they stay on the phone), Audio, Gameplay, Customer Support, Browsing and Search History (search runs on
the phone), User ID, Purchases, Advertising Data, Other Usage Data, Surroundings, Body, Other Data. Used to track you:
nothing (no ads, no advertising id, no analytics kit).

The answers are "Strict: linked" (owner): a problem report is stored whole, so the email, text, map centre, diagnostic
rows and install id sit in one record for 180 days, and the same install id is on crash records (90 days). Diagnostics →
Performance Data is also ticked on the live label (not linked) ahead of the app; it stays out of the table because
`app/tools/privacy-manifest.test.ts` holds the iPhone privacy manifest (`app.json` `ios.privacyManifests`) to it.

## 4. The version page (Distribution, iOS App, the version)

The version number must equal the build's (`0.10.0` for the 0.10.0 iPhone build).

| Field | Answer |
|---|---|
| Screenshots | iPhone medium display, up to 10; no iPad pictures (iPhone only) |
| Promotional Text (170), Description (4000), Keywords (100) | PASTE from the listing source (top of the page) |
| Support URL, Marketing URL | PASTE `https://canifishthis.ca/about`, `https://canifishthis.ca` |
| Copyright | YOURS: `2026 ` then your legal name as on the Apple account (Apple adds the ©) |
| What's New | not asked for a first version; if shown, PASTE below |
| Build | section 7 |

What's New (only if asked):
```
First release on the App Store. Tap any lake, river or stream in British Columbia to see its fishing regulations for the day you pick. Also in this version: river conditions and forecasts, salmon limits and run timing, flow alerts, saved spots with photos and notes, and offline use after the first download. The official regulations always take precedence; check them before you fish.
```

## 5. Age rating (App Information, Age Rating) and content rights

Expected **4+**. Answer No or None to every group: Parental Controls and Age Assurance (no accounts), Unrestricted Web
Access (links open Safari), User-Generated Content, Social Media, Messaging, Shared Location (spots, notes and photos stay
on the phone), Advertising, Mature themes, Medical, Sexuality, Violence, Chance. Made for Kids: **No** (cannot be undone).
Age Suitability URL: empty.

Content rights rest on the sources credited in the app (More, Sources and licences); Esri World Imagery is used through
the public tile address with no key, and whether that fits Esri's terms is YOURS (`app/packages/core/src/imagery.ts`).

## 6. Pricing and App Review Information

Price: PICK Free (CAD 0.00), no end date. Availability: PICK **All Countries or Regions** (if a region asks for something
extra, untick it and tell the chat). Pre-order No; tax category default. Version Release: PICK **Manually release this
version**. Export compliance: the build carries `ITSAppUsesNonExemptEncryption = NO`; if asked, PICK "None of the
algorithms mentioned above"; advertising identifier: **No**.

App Review Information: Sign-in required UNTICK, no user name or password; contact name and phone YOURS (phone with + and
country code); contact email YOURS (`support@canifishthis.ca`); no attachment. Notes (4000 bytes), PASTE:

```
Can I Fish This? is a reference app for British Columbia (Canada) freshwater fishing regulations. It shows a map of lakes, rivers and streams, coloured by what the regulations allow on the chosen day, and the rules for each water. It is free, with no ads and no purchases.

LOGIN: none. There are no accounts. Every feature is open when the app starts.

FIRST OPEN: the app downloads its data (regulations, map tiles, basemap), about 500 MB, once, so it works offline afterwards. A progress screen shows meanwhile. It takes a few minutes on Wi-Fi and continues if you switch apps. Please review on Wi-Fi. Then a short "Before you fish" notice and a short walkthrough, then the map.

DATA SOURCES: government information. Rules are compiled from the BC Freshwater Fishing Regulations Synopsis and in-season changes (Province of BC), federal salmon notices (Fisheries and Oceans Canada), and river data from the Water Survey of Canada and the BC River Forecast Centre. The app is NOT an official source and is not affiliated with, endorsed by or sponsored by the Province of British Columbia or Fisheries and Oceans Canada. It says so on first open, under More > About, at the foot of each regulations page, and in the store description, with links to the official sources. Licences: More > Sources and licences.

LOCATION: when-in-use only, asked once after the first download. It centres the map on you and can pin a saved spot where you stand. The app works fully if you tap Don't Allow. The GPS position is never sent automatically. A problem report the user chooses to send can include the centre of the map on screen (to about 100 m). No background location.

PHOTOS AND CAMERA: only when you add a picture to a saved spot. Photos stay on the phone.

NOTIFICATIONS: optional, off until the user turns one on (More > Notifications): river-flow alerts the user sets, and "New regulations data". Every visible notification is a local one raised by the app. Our server only ever sends a silent push (content-available, no text), which asks the app to check for new data.

BACKGROUND MODES: remote-notification is for that silent push. fetch and processing are declared by the background-task module, so that, when iOS allows, the app runs a short check of our small alert feed and data manifest and raises a local notification if a river the user chose crossed their line or new data exists. A silent push can start a data download through iOS background transfer, only if the app is already running and the user's download setting allows it. Otherwise the app raises a "new data" notice and downloads when opened. An update is applied the next time the app is used.

PRIVACY: no tracking, no ads, no analytics SDK. Crash reports (on by default; switch: More > Settings > Privacy) and problem reports (only when the user presses Send) go to our own server. Weather for a saved spot comes from Open-Meteo. Satellite tiles come from Esri only if the user picks Satellite. Policy: https://canifishthis.ca/privacy

HOW TO REACH EACH FEATURE (bottom bar: Map, Conditions, Runs, Spots, More):
- Regulations: Map tab, tap any water; its rules open in a panel. The Trout / Salmon switch on the map changes which rules colour it.
- Day: the day picker at the top of the map.
- Search: the search box at the top of the map (try "Vedder").
- River flow and level: Conditions tab, or Conditions on a water's panel.
- Salmon run timing: Runs tab.
- Saved spots: Spots tab; add a spot, a note and a photo.
- Flow alerts: More > Notifications.
- Offline: after the first download, turn on Airplane Mode; the map and rules still work.
- Help, Glossary, Sources and licences, About, Report a problem, Contact: More.

iPhone only, portrait.

CONTACT: support@canifishthis.ca
```


## 7. Which build goes to review

The newest iPhone production build uploaded by the time the pictures are ready (the 0.10.0 build if it is on TestFlight,
else 0.9.0 (45), which is fit for review). Never a staging build. Build 45's privacy manifest lacks Product Interaction and
the Analytics purpose and marks every type "not linked"; Apple does not compare them at review, and
`app/tools/privacy-manifest.test.ts` holds later builds to section 3.

## 8. Production push: not proven, not a gate

The production feeds worker's Apple push secrets (`APNS_KEY`, `APNS_KEY_ID`, `APNS_TEAM_ID`;
`handoff/PLAN/runbooks/secrets.md`) are set (owner 2026-11-24). What is not proven is a wake-up arriving on an iPhone:
the owner's test phone got no push token, so the proof waits on a second iPhone. Until then assume an iPhone learns
of new data from its own background check and when opened; a reviewer cannot see a silent push: review is not at risk.
To prove it: install from TestFlight, More, Notifications, turn on "New regulations data" ("Server wake-ups: this phone is
registered"), run `POST /run/push?reason=owner-phone-test` on production (yours, or allowed once); after a minute in the
background "Last background checks" should say "woken by our server". A fix ships as an ordinary update.

## 9. Submit

Pick the build, Save; **Add for Review** (answer any question from sections 3 to 6); **Submit for Review** (usually one
to two days). Approved: press **Release This Version** when you want it public.
