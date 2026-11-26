# Google Play: store listing (fact sheet, T371)

The fact sheet the store words are checked against (`app/tools/store-video/checks.mjs` captions rest on it). The words
themselves live in `app/store-listing/listing.json`.

Limits: Play short description 80 characters max; full description 4000 max; app name 30 max.

## App name
**Decided (owner 2026-11-06):** "Can I Fish?" only where the icon shows it (the phone's home-screen label,
`app/apps/mobile/app.json:3`; the web app's `shortName`); **"Can I Fish This?" everywhere else** — the store listing
name, the in-app name (`core/src/menu/words.ts`), the permission prompts, the feature graphic, the web tab. The app
already matches. Play allows 30 characters.

## Fact sheet (checked against the code and release data, 2026-11-13)
Paths are under `app/packages/` unless stated. Items marked **(next build)** were in the repo but not in the build the
sheet was checked against (0.7.7, versionCode 29).

### What the app is
- A map of BC freshwater fishing regulations: tap a stream or a lake and it shows what the regulations allow there on the day chosen, beside what the water is doing (`core/src/menu/words.ts` `about.what`).
- Name in the store: "Can I Fish This?". On the home screen the label is "Can I Fish?".
- It is built from the BC Freshwater Fishing Regulations Synopsis and other public sources; it is not the official regulations and can be wrong or out of date.
- The Synopsis edition in the current data release is "2025-2027 BC Freshwater Fishing Regulations Synopsis"; valid to 2027-03-31 (release r-adc4e248a163, published 2026-11-13).
- The app has a website too (canifishthis.ca); this sheet is about the phone app.

### Map
- Rivers, lakes and streams are drawn on a map, coloured by what the regulations say for the chosen day.
- The app opens centred on the reader's location if they allow it (asked once on first open; if refused, it opens on the default view).
- Ground: map by default; satellite imagery is optional and off by default.
- Colour palettes: Light, Dark, Colour-blind (Settings, Display).
- A legend and a day picker sit on the map.

### Regulations
- Tapping a water shows the rules for it: seasons, daily and possession limits, size limits (ordered ranges, first match wins), gear and bait rules, closures, licence notes.
- Rules that change along a water are split by section; the page names its own part of the water.
- Zone (region) rules are the base; a water's own rules are listed over them.
- A glossary explains the regulation words, with the Synopsis page cited (More -> Glossary).
- The Synopsis PDF is in the data release; "Rule book page N" opens it (on a slow connection the app offers it with its size first).
- Regulations are read on the phone; nothing is sent to a server to look up a rule.

### Salmon, in-season changes, run timing
- DFO (Fisheries and Oceans Canada) is the salmon feed only: federal salmon limits and closures. In the current release the DFO source is dated 2026-11-06.
- In-season changes are the Province's (BC Fish and Wildlife), not DFO's. The app shows them on the waters they apply to; some may not be placed on the map yet, and More links to the Province's in-season page.
- A salmon colouring of the map and a run-timing colouring exist (the report form names the map modes "Trout & more", "Salmon" and "Run timing").
- **(next build)** A New / Updated mark on an in-season notice until this device has seen it (T694).
- **(next build)** The bell opens an inbox of in-season notices: New and Earlier groups, Mark all as read, tap a notice to open its water; kept 30 days after a notice ends, at most 200; stored on the phone only (T706).

### Flows and conditions
- A water's Conditions view charts flow and level (and temperature where the gauge has it) from Water Survey of Canada gauges, with how the day compares with other years (historical data: HYDAT, dated 2026-10-19 in this release).
- River forecasts from the BC River Forecast Centre (CLEVER, COFFEE, ELF), used with its permission.
- Gauge alerts: pick a gauge, a flow or level and a line (a value, or a level for the day against other years); the phone notifies once when it crosses.
- Live gauge readings, in-season and salmon feeds need a connection; the app says so on screen.

### Search
- Search is on the phone, by water name. (Old names and nearby towns: see Not verified.)

### Saved spots
- Save a spot with notes and photos; photos and notes stay on the phone.
- A spot can be pinned at the reader's location.
- Visits are logged with the weather and river flow for the day; a spot shows a coming-days panel.
- Directions opens Google Maps with a spot's parking point.

### Notifications
- More -> Notifications: a master switch, gauge alerts, "New regulations data" and "New app version".
- Notifications are made on the phone; nothing is sent to a server for them. Android may check less often while the phone is idle (about every 15 minutes at best).

### Settings (More -> Settings) (next build; build 29 has the same controls on the More tab, T703)
- Data: "Download over mobile data": Always / Ask / Wi-Fi only (phone). The first download is about 500 MB and updates can be hundreds of MB. Wi-Fi only waits for Wi-Fi, then starts by itself.
- Data: "Low-bandwidth mode": Automatic / On / Off. It is a website setting only; it is not drawn on the phone.
- Privacy: "Send crash reports": on by default; off sends nothing.
- Display: the colour palette (Light, Dark, Colour-blind).
- So the old text's "refresh when you are on Wi-Fi" is wrong: the setting is about large downloads over mobile data, with three choices, default Ask.

### Sharing
- A Share button on a water sends a canifishthis.ca link (the same address the website opens) through the phone's share sheet: it carries the water, the view and the place, and the day when it is not today.

### Offline and updates
- The phone downloads the data release (regulations, map tiles, basemap, about 500 MB first time); after that the rules and the map work without a connection.
- Needs a connection: live gauge readings, in-season and salmon feeds, weather, satellite imagery, sending reports.
- Updates: a newer data release is offered with the app's update check; large ones follow the mobile-data setting. The More tab lists each source with its date and a freshness dot.
- Offline, the app says "Offline - showing what's saved on this device".

### Privacy and what leaves the phone (`handoff/PLAN/play/data-safety.md`)
- No account, no ads, no analytics SDK, no purchases, no background location.
- Location (GPS) goes to the map only, foreground only.
- Weather: a saved spot's position, rounded to about 1 km, goes to Open-Meteo (api.open-meteo.com).
- Satellite imagery: loaded from Esri, only if the reader picks Satellite.
- Problem report: sent only when the reader presses Send; shows every line first; carries the diagnostics rows left on, a random install id and the map centre (about 100 m); an email only if typed.
- Crash report: on by default, switch in Settings; carries the error, app version, kind of screen, open water's id and the random install id; never location, spots or photos.
- Install id: 16 random hex characters made once on the phone.
- Our server keeps no IP address (reports keep a salted hash for rate limiting). Retention: problem reports 180 days, crash kinds 90 days after last seen.
- Privacy policy: https://canifishthis.ca/privacy. Deletion requests: privacy@canifishthis.ca.

### Cost, ownership, not official
- Free, not run for profit. Not an official BC Government or DFO product.

### Disclaimer facts
- Not the official regulations; the Synopsis, the Province's in-season changes and federal notices are the law; check them before you fish; nothing in the app is legal advice.
- Rules can be wrong, out of date or not yet placed on the map.

### Platform and limits
- Android now (Play internal testing, build 29). iOS not yet (benched, owner 2026-11-06). Web at canifishthis.ca.
- Data sources: BC Freshwater Fishing Regulations Synopsis, DFO notices (salmon), BC in-season changes, Water Survey of Canada / ECCC gauges and HYDAT, BC River Forecast Centre forecasts, BC Freshwater Atlas (dated 2024-03-26), OpenStreetMap and Protomaps basemap, Open-Meteo weather, Esri imagery.

## Graphics (already made by T244; nothing new drawn here)
All files are in `handoff/PLAN/app-integration/store/graphics/`.

| Play asks for | File | State |
|---|---|---|
| App icon, 512 × 512 PNG | `app-icon-512.png` (the favicon fish, owner 2026-10-29; source `app/apps/mobile/assets/icon/play-512.png`, `app/tools/build-icons.mjs`) | ready |
| Feature graphic, 1024 × 500, no alpha | `feature-graphic-1024x500.png` (the fish, the name and one line; `app/tools/build-play-graphics.mjs`) | ready. **OWNER:** its name must match the app name you pick above. If you change it, rerun `node app/tools/build-play-graphics.mjs` |
| Phone screenshots, 2 to 8 | the one committed set `store/light/play-phone/` (7 pictures, `app/tools/store-shots.json`; the old `03-conditions.png` and `quick/` drafts were deleted, T1127) | drafts from the STAGING web app (`app/tools/play-shots.mjs`, headless). **OWNER:** keep them for closed testing, or retake them from the current phone build (0.7.7; the 0.5.0 named here before is stale). The shot list is in listing-draft.md. A saved spot and a share shot are not taken yet. |
| Tablet screenshots (7", 10") | none | Dropped by the owner 2026-11-25: `play-listing-push.py` clears both tablet slots on a push (T1072). |
| Video | none | optional |

## Pushing the listing to Play
The short and full descriptions now live in `app/store-listing/listing.json` (the owner's text); this page
is the fact sheet they are checked against. One command pushes title, both descriptions, the 8 phone screenshots
(`node app/tools/store-shots/run.mjs` output, file-name order, replacing Play's), and the icon and feature graphic
when Play's differ:

    .venv/bin/python deploy/staging/play-listing-push.py            # dry run: edit, validate, delete
    .venv/bin/python deploy/staging/play-listing-push.py --commit   # the real push

`--shots DIR` names the screenshot folder; `--video-url <YouTube URL>` sets the promo video (Play takes a URL, not
a file). Limits and image rules are checked before anything is sent; a wrong file is refused, never resized.

## Other listing fields (answers are in listing-draft.md)
Category: Maps & Navigation. Contact: support@canifishthis.ca. Privacy policy: https://canifishthis.ca/privacy. Ads:
none. In-app purchases: none. Target audience: 18+. Content rating: IARC questionnaire, expected Everyone. Data safety:
`handoff/PLAN/play/data-safety.md`.
