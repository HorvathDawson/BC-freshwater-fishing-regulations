# App Store submission: the page to paste from (T1071)

Written 2026-11-24 by the iOS release chat (roadmap row 5i). Nothing here is entered in App Store Connect yet. Every
click is the owner's. Each line says **PASTE** (text is ready), **PICK** (choose the option named) or **YOURS** (only
you know it). Sections follow the order App Store Connect asks. Facts were read from the code of the uploaded build,
iPhone 0.9.0 (45), commit 65f84020c; the evidence is in `store/listing-090-draft.md`, `store/app-privacy-answers.md`
and the T1071 ticket. Apple's rules were read from Apple's help pages on 2026-11-24; where a page did not show the
exact wording, the line says "check on the screen".

## 0. Where things stand

| Item | State |
|---|---|
| Build for review | DECIDED (owner 2026-11-24): the newest iPhone production build when the pictures are ready (section 9). 0.9.0 (45) is uploaded and is fit for review. |
| Pictures | READY 2026-11-25 (T1072, PR #192): 7 frames, the same set as Play, in `handoff/PLAN/app-integration/store/graphics/store/light/appstore-6.9/`, `appstore-6.5/` and `appstore-6.3/` (1206 x 2622, added 2026-11-25: the slot Apple now requires). Checked by this chat: 6.9" all 1320 x 2868, 6.5" all 1242 x 2688, PNG, no transparency. |
| Production push (T996) | NOT PROVEN, and no longer a gate. Owner 2026-11-25: the proof moves to after the iPhone release; a fix, if one is needed, ships as an update. The production secrets are set; the owner's iPhone gets no push token from iOS, cause open (state in `RESTART-ios-release-chat.md`). |
| App Privacy "linked" answers | DECIDED (owner 2026-11-24): strict, linked (section 3). |
| Countries | DECIDED (owner 2026-11-24): all 175 countries. EU trader status: DECIDED (owner 2026-11-25): not a trader (section 1). |
| Everything else | drafted below |

## 1. Before the version page (account level, once)

| Screen | Field | Answer |
|---|---|---|
| Business → Agreements | Free Apps Agreement | must say Active. YOURS to check; it is, if TestFlight works. |
| Business → Compliance → Digital Services Act | Trader status | PICK **not a trader** (your decision, 2026-11-25). Apple asks every developer, even without EU sales. |

## 2. App Information (left menu: General → App Information)

| Field | Answer |
|---|---|
| Name (30) | PASTE `Can I Fish This?` |
| Subtitle (30) | PASTE `BC fishing rules, by the water` (exactly 30) |
| Primary language | PICK English (Canada) |
| Bundle ID | already `ca.canifishthis.app` |
| Category | PICK Primary: Navigation. Secondary: Sports. |
| Content Rights | PICK "Yes, it contains, shows, or accesses third-party content", then tick that you have the rights. Basis: section 6. |
| Age Rating | section 5 |
| License Agreement | leave Apple's standard one. Enter nothing. |

## 3. App Privacy (left menu: Trust & Safety → App Privacy)

Privacy Policy URL: PASTE `https://canifishthis.ca/privacy`

"Do you or your third-party partners collect data from this app?" PICK **Yes**.

Tick exactly these data types. Everything not in the table: leave unticked.

| Apple's group → type | Purposes to tick | Linked to the user? | Tracking? | What sends it |
|---|---|---|---|---|
| Contact Info → Email Address | App Functionality | **Yes** | No | the optional email on a problem report |
| Location → Precise Location | App Functionality | see "Linked" below | No | a problem report's map centre, 3 decimals (about 100 m). Apple calls 3 decimals or more "precise". |
| Location → Coarse Location | App Functionality | No | No | weather for a saved spot (2 decimals, about 1 km, to Open-Meteo); the area on screen when Satellite is on (Esri) |
| User Content → Other User Content | App Functionality | see "Linked" below | No | the text typed in a problem report |
| Identifiers → Device ID | App Functionality, Analytics | see "Linked" below | No | the random install id in crash and problem reports; the Apple push token (kept by our server while notifications are on) |
| Usage Data → Product Interaction | App Functionality | see "Linked" below | No | a crash or report names the screen, the open water and the tab |
| Diagnostics → Crash Data | App Functionality | see "Linked" below | No | crash reports (on by default; switch under More, Settings, Privacy) |
| Diagnostics → Other Diagnostic Data | App Functionality | see "Linked" below | No | the diagnostic rows and recent error lines on a problem report |

Not collected (leave unticked): Name, Phone, Address, Health, Fitness, Financial, Sensitive Info, Contacts, Emails or
Text Messages, Photos or Videos (they stay on the phone), Audio, Gameplay, Customer Support, Browsing History, Search
History (search runs on the phone), User ID, Purchases, Advertising Data, Other Usage Data,
Surroundings, Body, Other Data. "Used to track you": nothing. No ads, no advertising id, no analytics kit.

**Performance Data: ticked on the label, NOT sent by the app (owner 2026-11-25).** He kept Diagnostics → Performance
Data ticked ("keep ticked but next release will add to rpivacy page and add the performance monitor"), so the live
label has 9 types and is one ahead of the app. Asked answers: App Functionality, linked Yes, tracking No (what the
screen holds is to be confirmed by him). It is left out of the table above on purpose: the table is what the build
sends, and `app/tools/privacy-manifest.test.ts` (T1099) holds the iPhone privacy manifest to it. So until T1132
(0.10.1) ships the monitor, the manifest entry and the privacy page words, the label and the manifest disagree on
this one type.

**Linked: DECIDED (owner 2026-11-24, "Strict: linked").** For every row marked "see Linked below" answer **Yes,
linked**; Coarse Location stays No. Why: a problem report is stored whole, so the email, the typed text, the map
centre, the diagnostic rows and the install id sit in one record for 180 days, and the same install id is on crash
records (90 days). No code change.
The privacy page sentence "only the random install id, which I cannot connect to a person" needs new words
(T1098).

## 4. The version page (Distribution → iOS App → the version)

The version number must equal the build's: `0.10.0` for the 0.10.0 iPhone build (`0.9.0` only if build 45 is sent instead). Rename the waiting "1.0" record if there is one.

| Field | Answer |
|---|---|
| Screenshots | READY. iPhone 6.9": the 7 files in `handoff/PLAN/app-integration/store/graphics/store/light/appstore-6.9/` (1320 x 2868), in file-name order 01 to 07. iPhone 6.5" (optional): the 7 in `appstore-6.5/` (1242 x 2688). iPhone with Dynamic Island, medium display (required, 2026-11-25): the 7 in `appstore-6.3/` (1206 x 2622). By hand: drag them onto the screen in that order. Or by script (T1127, on main since 2026-11-25): `.venv/bin/python deploy/staging/appstore-screenshots-push.py` shows the plan and sends nothing and must show 7 per size (three iPhone sizes); it stops by name on a stray or missing picture; `--commit` uploads and is yours to run. No iPad pictures (iPhone only). If the screen also demands another iPhone size, tell this chat. |
| App preview video | none (optional) |
| Promotional Text (170) | PASTE block below |
| Description (4000) | PASTE block below (3924) |
| Keywords (100) | PASTE block below (exactly 100) |
| Support URL | PASTE `https://canifishthis.ca/about` |
| Marketing URL | PASTE `https://canifishthis.ca` |
| Version | `0.10.0` |
| Copyright | YOURS: `2026 ` then your legal name as on the Apple account. Apple adds the © itself. |
| What's New | not asked for a first version. If the screen shows it: block below. |
| Build | section 9 |
| App icon | nothing to upload; it comes from the build. |

Promotional text:
```
Your pocket guide to BC fishing regulations. Less searching, more fishing!
```
Keywords:
```
regulations,freshwater,river,lake,trout,salmon,steelhead,angling,license,flow,gauge,map,synopsis,dfo
```
What's New (only if asked):
```
First release on the App Store. Tap any lake, river or stream in British Columbia to see its fishing regulations for the day you pick. Also in this version: river conditions and forecasts, salmon limits and run timing, flow alerts, saved spots with photos and notes, and offline use after the first download. The official regulations always take precedence; check them before you fish.
```
Description (the Play text, unchanged; it names no other platform):
```
Can I Fish This? is your pocket guide to fishing regulations across Beautiful British Columbia.
Spend less time searching, more time fishing.
Simply tap any lake, river or stream on an interactive map to instantly view fishing regulations, river conditions, and other information that matters before you cast your line.
Designed and developed by a passionate BC angler, Can I Fish This? was built to make fishing regulations easier to understand and visualize, whether you're planning a trip at home or standing on a riverbank.

Features:

Interactive Map
Explore thousands of lakes, rivers, and streams across British Columbia using an interactive map. Water bodies are colour-coded based on their fishing regulation status for the date you select, making it easy to see where regulations apply before you head out.

Understand Regulations Faster
Fishing regulations can be difficult to interpret. Can I Fish This? breaks the rules for each water body into a clear, organised view: seasons, limits, size and gear rules, and closures, section by section where the rules change along a river.
Need more detail? A glossary of terms is available for reference.

Salmon Limits, Closures and Run Timing
See the federal salmon limits and closures for the water you're looking at, and switch the map to a salmon view or a run timing view.

Plan Your Next Trip
Check regulations days in advance and see upcoming regulation changes before your trip. River conditions and forecasts help you choose the best place and time to fish.

Works Without Cell Service
Fishing often takes you beyond reliable cellular coverage. Can I Fish This? stores mapping and regulation data locally on your device, allowing you to view important information even when you're offline.

Save Your Favourite Fishing Spots
Create a personal log of fishing locations throughout British Columbia.
Save:
• Photos from your trip
• Personal notes and observations
• River conditions from your visit
• Important locations for future reference
Your photos and notes stay on your phone.

Share With Friends
Share a water with friends. The link opens the same water, map view and date.

River Conditions and Forecasts
View current river conditions using live federal hydrometric monitoring data, including:
• River flow rates
• Water levels
• Historical comparisons
• River forecasts

Custom Flow Alerts
Set personalized river alerts for the rivers you care about most. Choose a monitoring gauge and define your own flow or level threshold to receive a notification when the river crosses it.

More on the Map
The map also shows parks, ecological reserves and First Nations reserve lands, to help you understand the landscapes and communities surrounding your fishing destination.

---
Disclaimer
Can I Fish This? is a tool designed to help the average angler view fishing regulations across British Columbia. It is NOT an official source of regulatory information and should not be relied upon as the sole basis for determining legal fishing activity.
The official regulations published by the Province of British Columbia and Fisheries and Oceans Canada take precedence and are the regulations in force; check them before you fish.
Can I Fish This? is not affiliated with, endorsed by, or sponsored by the Province of British Columbia or Fisheries and Oceans Canada (DFO).

Official sources:
• BC Freshwater Fishing Regulations Synopsis and in-season changes: https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/fishing/fishing-regulations
• Fisheries and Oceans Canada, freshwater salmon regulations: https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/index-eng.html
• Water Survey of Canada, river flow and level data: https://wateroffice.ec.gc.ca

Important: The app's initial download includes a large data package (approximately 500 MB). For the best experience, I recommend downloading and setting up the app while connected to Wi-Fi.
```

## 5. Age rating (App Information → Age Rating → Set Up)

Expected result: **4+**. Apple's 2025 form asks in groups; check the wording on the screen, the answers follow from
what the app is.

| Group | Answer | Why |
|---|---|---|
| In-app controls: Parental Controls, Age Assurance | No / none | no accounts, nothing to restrict |
| Capabilities: Unrestricted Web Access | No | no browser inside the app; links open Safari |
| Capabilities: User-Generated Content, Social Media, Messaging and Chat, Shared Location | No | spots, notes and photos stay on the phone; reports go only to the developer |
| Capabilities: Advertising | No | none |
| Mature themes: Profanity, Horror, Alcohol/Tobacco/Drugs, Dating | None | none |
| Medical or wellness | None | none |
| Sexuality or nudity | None | none |
| Violence: Cartoon, Realistic, Graphic, Guns or Other Weapons | None | fishing rules as text; no violence shown |
| Chance: Gambling, Simulated Gambling, Contests, Loot Boxes | None | none |
| Made for Kids | No | an adult audience; cannot be undone once approved |
| Age Suitability URL | leave empty | optional |

## 6. Content rights: what the "Yes, I have the rights" answer rests on

The app credits each source under More → Sources and licences.

| Source | Licence shown in the app |
|---|---|
| BC Freshwater Fishing Regulations Synopsis, in-season changes | © Province of BC terms |
| DFO salmon notices | Government of Canada non-commercial reproduction terms (the app is free) |
| Freshwater Atlas, place names, access layers, obstacles, stocking | Open Government Licence, BC |
| Four municipal stream layers | each city's open licence |
| Water Survey of Canada (HYDAT, live gauges) | Open Government Licence, Canada |
| BC River Forecast Centre | Province of BC copyright |
| Salmon run timing (PSF, NuSEDS) | permission, and Open Government Licence, Canada |
| OpenStreetMap, Protomaps basemap | ODbL |
| Open-Meteo weather | CC BY 4.0 |
| Esri World Imagery (Satellite only) | Esri Master Agreement; credit drawn on the map. The app uses the public tile address with no key or account. Whether that fits Esri's terms is YOURS (`handoff/PLAN/app-data/satellite/esri/README.md`). |
| Lucide icons | ISC |

Not credited in the app (open-source notices, not an Apple rule; sent to the planning chat): MapLibre, the three
fonts, react-native-pdf, Expo and React Native, fflate, pmtiles.

## 7. Pricing and Availability (Monetization → Pricing and Availability)

| Field | Answer |
|---|---|
| Price | PICK Free (CAD 0.00), no end date |
| Availability | PICK **All Countries or Regions** (owner 2026-11-24). If a region asks for something extra on the screen (China mainland, Korea, Vietnam sometimes do), untick that region and tell this chat. |
| Pre-order | No |
| Tax category | leave the default (App Store software) |

## 8. App Review Information (bottom of the version page)

| Field | Answer |
|---|---|
| Sign-in required | UNTICK. Leave user name and password empty. |
| Contact name, phone | YOURS. Phone with + and country code, e.g. +1 604 ... |
| Contact email | YOURS (proposal `support@canifishthis.ca`) |
| Notes (4000 bytes) | PASTE block below |
| Attachment | none |

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

Version Release: PICK **Manually release this version** (you press the button once Apple approves). Phased release
is for updates only; it does nothing on a first version.

Export compliance: the build already carries `ITSAppUsesNonExemptEncryption = NO`, so App Store Connect should not ask.
If it does: the app uses only the operating system's HTTPS, plus SHA-256 to check downloaded files; PICK **"None of the
algorithms mentioned above"**. No documents. Advertising identifier, if asked: **No**.

## 9. Which build goes to review (DECIDED, owner 2026-11-24: "Newest by then")

Build 45 is fit for review, read from its code: no tester switches in a production build; the reviewer's likely
"Don't Allow" on location is caught everywhere (the Android crash T1093 has no iPhone twin); the first-open download
retries by itself; version 0.9.0 reads production data.
Known small things in build 45: About may say "build 44"; the privacy manifest inside the build lacks two lines the
labels above carry (Product Interaction, the Analytics purpose) and marks every type "not linked" (T1099, next iPhone build). Apple does not compare these at review.

Because the submission now waits for the new pictures, the choice is made on the day they are ready:
- The newest iPhone production build uploaded by then. The release once called 0.9.1 is cut as 0.10.0 (owner
  2026-11-25). If the 0.10.0 iPhone build is on TestFlight, send it (the version page is then named 0.10.0; push proof is no longer a gate). If not, send 0.9.0 (45).
- Never a staging build.

## 10. Production push (T996): not proven yet

**Owner 2026-11-25: this does not hold the submission.** It is finished after the release; if the cause is in the app,
the fix goes out as an ordinary update (a new build through App Review, about a day). What users get meanwhile is the
paragraph below.

What is missing: the production feeds worker has no Apple push key, so it cannot wake any iPhone. Until it does, an
iPhone still learns of new data from its own background check (when iOS allows, at best every 15 minutes or so, often
much later) and when the app is opened. Flow alerts work the same way. So a build reviewed without the proof is not
at risk of rejection (the reviewer cannot see a silent push); the risk is to users: late "new data" and flow notices.

Steps (the first is CONFIRM_PROD-level, so yours; full text in T996 "Owner phone steps"):
1. YOURS, from the main checkout, three commands, same values as staging (`handoff/PLAN/runbooks/secrets.md`):
   `npx --prefix app wrangler secret put APNS_KEY -c app/workers/feeds/wrangler.prod.toml` (paste the .p8 text), then
   the same with `APNS_KEY_ID` (6T3DBFF92H) and `APNS_TEAM_ID` (your team id: developer.apple.com → Account → Membership details).
2. YOURS: install 0.9.0 (45) from TestFlight. More → Notifications → turn on "New regulations data". The page should
   say "Server wake-ups: this phone is registered".
3. This chat reads `https://feeds.canifishthis.ca/status.json` (read only). The send itself,
   `POST /run/push?reason=owner-phone-test` on production, is yours to run or to allow that one time.
4. YOURS: put the app in the background for a minute, open it, More → Notifications → "Last background checks". The
   newest line should say "woken by our server". Send a screenshot.
5. This chat watches production's status for an hour for Apple token errors (staging and production share one key).

## 11. Submit

1. On the version page: pick the build (section 9), Save.
2. **Add for Review** (top right). Answer any question it raises from sections 3 to 8.
3. **Submit for Review**. Status goes Waiting for Review → In Review. Usually one to two days.
4. Tell this chat. It tells the planning chat, and again when Apple answers.
5. Approved: status "Pending Developer Release". Press **Release This Version** when you want it public.
6. Rejected: paste Apple's message here; nothing else changes meanwhile.
