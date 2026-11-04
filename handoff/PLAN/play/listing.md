# Google Play: store listing (draft, T371)

2026-11-04. Nothing has been submitted. The listing text started in T244's
`handoff/PLAN/app-integration/store/listing-draft.md` (2026-10-29). That file still holds the name, keywords,
categories, support URLs, rating answers and the shot list. **This file is now the one place for the short and full
description**, and it is the same text for the App Store. Three things changed since 10-29: the app opens on your
location (T384), it can share a water (T385), and it sends an install id and the map centre with reports (T352). The
"private by design" paragraph below has been rewritten to match `handoff/PLAN/play/data-safety.md`. **OWNER:** please
sign off on the wording.

## App name
The code uses three names today. The phone's app name is "Can I Fish?" (`app/apps/mobile/app.json:3`, also the
permission strings). The in-app name is "Can I Fish This?" (`app/packages/core/src/menu/words.ts:8`, used for the share
sheet title). The 10-29 draft said "canifishthis". **OWNER:** pick one, and make the app match. T365 has the same open
question. Play allows 30 characters. The text below says "Can I Fish?".

## Short description (Play, 80 max; this is 78)
BC freshwater fishing rules for any river or lake: map, seasons, limits, flows.

## Full description (Play 4000 max)
Can I fish this? Tap a river or lake on the map and see the freshwater fishing rules that apply to that exact water, in plain words.

WHAT IT DOES
- A map of BC's named rivers, lakes and streams, coloured by what the rules say today. It opens where you are, if you allow it.
- The rules for the water you tapped: seasons, daily limits, size limits, bait and gear rules, closures, licences. Rules that change along a river are split by section, so you see the stretch you are standing on.
- Live river flow and temperature from the federal gauges, with how today compares with other years.
- Salmon notices and in-season changes, brought together when Fisheries and Oceans Canada publishes them.
- Search by water name, an old name or a nearby town.
- Save spots with notes and photos, and see the weather and river flow for the day you fished.
- Share a water with a friend as a link that opens the same place.
- Works offline once installed: the map and the rules live on your phone and refresh when you are on Wi-Fi.

PRIVATE BY DESIGN
No account, no ads, no tracking. Your photos, notes and saved spots stay on your phone, and your location is only used to show the map where you are. To show the weather at a saved spot, the app sends that spot's position to the weather service Open-Meteo. Satellite view, if you turn it on, loads its pictures from Esri. The app sends us two things only: a problem report when you press Send (you see every line before it goes), and, unless you switch it off, a short error report when the app itself fails. Both carry a random number made on your phone, so we can count how many phones see a problem. It is not linked to you.
Privacy policy: https://canifishthis.ca/privacy

FREE AND NON-PROFIT
Can I Fish? is free and not run for profit. It is not an official BC Government or DFO product.

IMPORTANT
The app is a reading aid built from the BC Freshwater Fishing Regulations Synopsis and DFO notices. Regulations change; always check the current printed synopsis, the BC Fisheries website and DFO notices before you fish, and carry the licences you need.

Notes on the text: about 1,900 characters. No line promises fish or a catch, or gives legal advice. "Works offline"
holds for the rules and the map; flows, notices and weather need a connection, which the app says on screen.
**OWNER:** "refresh when you are on Wi-Fi" depends on the More → Data setting (T254). Keep it, or say "when you are
online".

## Graphics (already made by T244; nothing new drawn here)
All files are in `handoff/PLAN/app-integration/store/graphics/`.

| Play asks for | File | State |
|---|---|---|
| App icon, 512 × 512 PNG | `app-icon-512.png` (the favicon fish, owner 2026-10-29; source `app/apps/mobile/assets/icon/play-512.png`, `app/tools/build-icons.mjs`) | ready |
| Feature graphic, 1024 × 500, no alpha | `feature-graphic-1024x500.png` (the fish, the name and one line; `app/tools/build-play-graphics.mjs`) | ready. **OWNER:** its name must match the app name you pick above. If you change it, rerun `node app/tools/build-play-graphics.mjs` |
| Phone screenshots, 2 to 8 | `03-conditions.png` (1080 × 1920), and `quick/01-map.png`, `02-regulations.png`, `03-salmon.png`, `04-runs.png`, `05-glossary.png` (all 1080 wide; four are 1080 × 2160) | drafts from the STAGING web app (`app/tools/play-shots.mjs`, headless). **OWNER:** keep them for closed testing, or retake them from the 0.5.0 phone build. The shot list is in listing-draft.md. A saved spot and a share shot are not taken yet. |
| Tablet screenshots (7", 10") | none | `play-shots.mjs` can make them at 1200 × 1920 and 1600 × 2560. Play only needs them if you list for tablets. |
| Video | none | optional |

## Other listing fields (answers are in listing-draft.md)
Category: Maps & Navigation. Contact: support@canifishthis.ca. Privacy policy: https://canifishthis.ca/privacy. Ads:
none. In-app purchases: none. Target audience: 18+. Content rating: IARC questionnaire, expected Everyone. Data safety:
`handoff/PLAN/play/data-safety.md`.
