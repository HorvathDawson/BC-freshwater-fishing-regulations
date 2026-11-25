# App Review notes and category (T727, PROPOSAL for the owner)

A proposal. Nothing is entered. The text between the lines is ready to paste into "Notes for App Review". Facts were checked
against the code on 2026-11-14; where I could not, it says "check" outside the paste block.

## Paste block (App Review > Notes)

---
Can I Fish This? is a reference app for British Columbia (Canada) freshwater fishing regulations. It shows a map of lakes,
rivers and streams, coloured by what the regulations allow on the chosen day, and the rules for each water.

LOGIN: none. There are no accounts and no sign-in. Every feature is open when the app starts.

FIRST OPEN: the app downloads its data (regulations, map tiles and basemap), about 500 MB, once, so it works offline
afterwards. A progress screen shows while this happens; it takes a few minutes on Wi-Fi and keeps going if you switch to
another app (iOS background transfer). Please review on Wi-Fi. After it finishes, all rules and the map work without a
connection.

WHERE THE DATA COMES FROM: government information. The rules are compiled from the BC Freshwater Fishing Regulations
Synopsis and the Province of BC's in-season changes (Province of BC), federal salmon notices (Fisheries and Oceans Canada),
and river data from the Water Survey of Canada and the BC River Forecast Centre. The app is NOT an official source and is not
affiliated with, endorsed by or sponsored by the Province of British Columbia or Fisheries and Oceans Canada. It says so in
the app (More, Legal: "Not the official regulations") and in the store description, with links to the official sources.

LOCATION: when-in-use only, asked once on first open. It only centres the map on you and can pin a saved spot where you
stand. The GPS position itself is never sent to us. Two things can show roughly where the reader was, and both are in our
privacy policy: the weather for a saved spot (the spot's place rounded to about 1 km, sent to Open-Meteo) and the map
centre in a problem report the reader chooses to send (about 100 m). There is no background location.

PHOTOS AND CAMERA: only when you add a picture to a saved fishing spot. The photos stay on the phone.

NOTIFICATIONS: optional, off until the user turns something on (More, Notifications). They are local notifications for
river-flow alerts the user sets, and "new regulations data" or "new app version" notices.

BACKGROUND MODES (fetch, processing): declared by the background-task module so the phone can, when iOS allows, run a short
check about every 15 minutes at best. The check reads our small alert feed, the data release manifest and the app version
file, then raises a local notification if a river the user chose crossed their line or a new data release exists. That
check downloads nothing. A data update the app starts while it is open is handed to iOS background transfer, so it keeps
downloading if the user switches away, and it is applied the next time the app is in use.

PRIVACY: no tracking, no ads, no analytics SDK. Crash reports (on by default; switch: More, Settings, Privacy) and problem
reports (sent only when the user presses Send) go to our own server. Satellite tiles come from Esri only if the user picks Satellite. Policy: https://canifishthis.ca/privacy

HOW TO REACH EACH FEATURE:
The bottom bar has five tabs: Map, Conditions, Runs, Spots, More.
- Map and regulations: Map tab; tap any water; its rules open in a panel. The Trout / Salmon switch changes which rules
  colour the map.
- Day: the day picker at the top of the map changes the date the colours and rules apply to.
- Search: the search box at the top of the map, by water name.
- Conditions: the Conditions tab (river flow and level against normal), or Conditions on a water's panel.
- Salmon run timing: the Runs tab.
- Saved spots: the Spots tab; add a spot, a note and a photo.
- Gauge alerts and notifications: More, Notifications.
- Glossary, About, Legal, Privacy policy, Contact, Report a problem: More.
- Satellite imagery: optional ground layer on the map (from Esri, only when chosen).

CONTACT: support@canifishthis.ca
---

## Add for a STAGING test build (TestFlight Beta App Review of a staging build only)
"This is a test build. It reads our test data server and has tester switches (an environment switch and Simulate offline,
under More); please ignore them. The App Store build has neither."

## Licence agreement (App Store Connect, App Information > License Agreement)
Claude's recommendation 2026-11-19: keep Apple's STANDARD EULA (the default; enter nothing). The app has no accounts,
purchases or user content to govern; the "not the official regulations" disclaimer is in the app (More, Legal), the
description and the review notes. A custom EULA would be a legal document to write and keep; the owner decides.

## Things to check before pasting (outside the block)
1. Every tap path in "How to reach each feature" is from the fact sheet (`handoff/PLAN/play/listing.md`) and the store
   description, not walked on iOS: confirm the names (Spots tab, Conditions, map mode control) on the first iOS build (IOS-1).
2. "about 480 MB" is the owner's figure; the Play listing says about 500 MB (`app/store-listing/listing.json`). Check the size of the
   release the review build installs and use one number in both places.
3. The background-mode paragraph REPLACES the T365 line "downloads the next regulations release in the background": the code
   says the check "never downloads a release" (`app/apps/mobile/src/notify.native.ts:8-9`, task at `:75-96`). A
   foreground download only (START-HERE-ios.md, "What is Android-only"). Use the paragraph above, not T365's. 0.8.3 (T912):
   an update started in the app goes on over background URLSession when the app is left; the check still starts nothing.
4. `fetch` and `processing` in UIBackgroundModes: verified on today's build (coordinator, 2026-11-14).
5. The review build must read PRODUCTION data (T86). A staging build has "Simulate offline" and an environment switch
   (`EXPO_PUBLIC_ENV_SWITCH=1`, `app/apps/mobile/eas.json:8`); do not send that to review. If a staging build ever goes to
   review, add: "Simulate offline (More, About) is a test switch; ignore it." (check where the switch sits).
6. iPad: the app is iPhone only (`supportsTablet: false`, `app.json:37`); Review may run it in compatibility mode; not run
   yet (T365 item 6). Add nothing to the notes unless Review raises it.
7. Orientation: portrait only (owner 2026-11-14); `app.json:7` says portrait. Not read back from a fresh Info.plist.
8. Demo account fields: leave empty and untick "Sign-in required".

## Category (proposal)
- **DECIDED (Claude's call under owner's "choose best", 2026-11-14): Primary Navigation, secondary Sports.** It is a map people use
  on the water, and it matches Play's Maps and Navigation. Reference is a fallback if Review pushes back.
- Check: App Store Connect may offer fewer or different category names than I list; pick the nearest.
- Play uses Maps and Navigation (listing.md): the same family.
