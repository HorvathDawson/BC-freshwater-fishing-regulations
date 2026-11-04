# Google Play: Data safety form answers (draft, T371)

Drafted 2026-11-04 from the code on `redesign/stream-sections`. Nothing has been submitted. This replaces the Play part of
`handoff/PLAN/app-integration/store/privacy-labels-draft.md` (2026-10-29), which is now out of date in two places: the
install id (T352) and the map centre on a problem report (T352 + T384).

**OWNER** marks an answer you need to confirm. These are judgement calls, or depend on how the build is made.
Paths are under `app/` unless they say otherwise.

## First: which build is this form for?
The form has to describe the build you actually upload.
- The Play AAB is built by `deploy/staging/build-apk.sh`, which sets `EXPO_PUBLIC_APP_ENV=staging EXPO_PUBLIC_ENV_SWITCH=1`
  (`deploy/staging/build-apk.sh:42`). In that build, problem reports and crash reports go to the staging feeds worker
  (`apps/mobile/src/env.ts:35-38`, `:52-57`), and the data comes from `data-staging.canifishthis.ca` (`env.ts:25`).
- A `production` build (`apps/mobile/eas.json:15-16`) has no report endpoint (`env.ts:38`, `production: null`), and so
  no crash endpoint either (`env.ts:56`). Such a build sends **no** problem reports and **no** crash reports. A problem
  report can then only go by email or the share sheet.
- **OWNER:** the answers below are for the staging-env AAB you upload today, which does send reports. They are also safe
  for a later production build, because declaring more than a build sends is allowed. Declaring less is not.
- Aside: `ENV_SWITCH=1` gives testers the environment switch on About. Decide whether closed testers should have it.

## What leaves the phone (the facts every answer rests on)

| # | What | Goes to | When | Evidence |
|---|---|---|---|---|
| 1 | Crash report (`crash/2`): app build, environment, platform, data release, screen kind, the open water's id and its pane, a random install id, the error's name, a scrubbed message (quoted text, numbers and coordinates removed) and the stack | our server (feeds worker `POST /crash`) | on by default; switch in More → Data; only while online; never queued | `packages/ui/src/crashReport.ts:2-17` (what is sent), `:131-145` (the payload), `:182` (default on), `:242` (the POST) |
| 2 | Problem report (`report/2`): reason, the text typed, an optional email, and the diagnostics rows left switched on: where you were (screen, water name + id, part, day), app version, platform, release, map view (layer, forecast day, zoom, **map centre to 3 decimals ≈ 100 m**), install id, the recent error log | our server (feeds worker `POST /report`) | only when the user presses Send; queued on the phone while offline | `packages/core/src/menu/report.ts:1-10`, `:110-114` (centre rounded), `:226-262` (the payload: only rows left on), `packages/data/src/report/outbox.ts:62` (the POST) |
| 3 | Install id: 16 random hex characters, made once on the phone, no personal data | our server, inside 1 and 2 | with 1 (unless crash reports are off) and 2 (unless its row is off) | `packages/ui/src/crashReport.ts:158`, `apps/mobile/src/App.tsx:107`, `packages/data/src/spots/store.native.ts:101`; worker keeps up to 100 ids per error kind: `workers/feeds/src/crashes.ts:79`, `:191` |
| 4 | **A saved spot's coordinates** (full precision; a spot can be pinned "At my location", i.e. from GPS) and dates | **Open-Meteo** (third party, `api.open-meteo.com` / `archive-api.open-meteo.com`) | when a visit is logged, when a spot's coming-days panel loads, and when spots refresh (pull to refresh, and automatically on reconnect for a spot made offline) | `packages/data/src/spots/weather.ts:16-17`, `:55-56`, `:88`, `:146-147`; callers `packages/data/src/spots/visits.ts:169`, `:186-187`, `packages/data/src/spots/likeVisit.ts:176`, `apps/mobile/src/App.tsx:200`, `:461-465` |
| 5 | Satellite tiles for the area being viewed | **Esri** (`server.arcgisonline.com`, or `ibasemaps-api.arcgis.com` if a key is set) | only when the user picks the Satellite ground (off by default), plus one probe tile | `packages/core/src/layers.ts:141-143` (map default, satellite not), `packages/ui-native/src/shell/useMapColouring.ts:132-133`, `packages/ui-native/src/imageryProbe.ts:19`, `packages/map/style/layers.source.json:13`, `packages/map/src/endpoints.ts:46-58` |
| 6 | The phone's location (GPS fix) | **nowhere directly**: it centres the map and can pin a spot | foreground only; asked once on first open (T384), else on a tap | `apps/mobile/src/locate.native.ts:1-8`, `:26-31`, `:46`; `packages/ui/src/myLocation.ts:10` ("the fix goes to the map and nowhere else"); `apps/mobile/app.json:22-25` (when-in-use only, no background) |
| 7 | Photos / camera | nowhere: kept on the phone with the spot | user picks or takes one | `apps/mobile/src/pickPhoto.native.ts:12-29`, `packages/data/src/spots/store.native.ts:6`, `:16`; `apps/mobile/app.json:14-16` (no microphone) |
| 8 | Release, map tiles, gauge / salmon / in-season feeds, HYDAT | our data server (R2, `data-staging.` / `data.canifishthis.ca`) | app use | `apps/mobile/src/env.ts:25-26`; ordinary GETs with no user data and no app headers (only `Range` / `Content-Type`: `packages/data/src/release/install.ts:184`) |
| 9 | Share a water (a canifishthis.ca link); Directions (opens Google Maps with a spot's parking point) | the app or person **the user picks** | user tap only | `packages/core/src/share.ts:10-13`; `packages/ui-native/src/shell/SpotHost.tsx:107` |
| 10 | IP address | every server the phone talks to sees it | any request | our worker does **not** store it: reports keep only a salted hash for rate limiting (`workers/feeds/src/reports.ts:14-17`, `:144`); crashes store no IP, user agent or country (`workers/feeds/src/crashes.ts:12-15`) |

Not in the app: accounts, sign-in, ads, advertising ID, analytics SDKs, third-party crash SDKs, purchases, background
location. Search and the rules run on the phone. The recent error log lives in memory only and goes out only inside a
problem report (`packages/ui/src/errorLog.ts:1-6`).

Retention on our server: problem reports 180 days (`workers/feeds/src/reports.ts:213`); a crash kind is deleted 90 days
after it was last seen (`workers/feeds/src/crashes.ts:21`, `:44`); test reports 7 days (`reports.ts:26`).

## The form, question by question

**Does your app collect or share any of the required user data types?** Yes.

**Is all of the user data collected by your app encrypted in transit?** Yes. Every endpoint is `https://`
(`env.ts:25-38`, `weather.ts:16-17`, `layers.source.json:13`).

**Do you provide a way for users to request that their data is deleted?** Yes. There is no account. Users write to
privacy@canifishthis.ca, and reports and crash kinds are also deleted on their own (see retention above). URL:
https://canifishthis.ca/privacy. **OWNER:** the mailbox has to exist (OWNER-TODO).

### Data types
For each type Play asks: collected? shared? processed ephemerally? required or optional? purposes?

| Play type | Collected | Shared | Ephemeral | Required / optional | Purpose | Rests on |
|---|---|---|---|---|---|---|
| **Location → Precise location** | Yes | **Yes: Open-Meteo** | No (**OWNER**: Open-Meteo's own logging is unknown) | Optional (only for saved spots) | App functionality (weather at a spot) | row 4. **OWNER (decided 2026-10-29, re-confirm):** the spot's coordinates go unrounded and can be a GPS fix. See "a cheaper answer" below. |
| **Location → Approximate location** | Yes | **Yes: Esri** (**OWNER**, decided 2026-10-29) | No | Optional | App functionality | row 5 (the tiles for the viewed area), and row 2: the problem report's map centre (~100 m) is often where the reader is, since the app now opens centred on them (T384). **OWNER: new since 10-29.** |
| Personal info → Email address | Yes | No | No | Optional | App functionality (replying to a report) | row 2: sent only if typed (`report.ts:259-262`) |
| Personal info → name, address, phone, user IDs, other | No | | | | | no account, no field |
| App activity → **App interactions** | Yes | No | No | Optional | App functionality; Analytics (**OWNER**: pick one or both) | rows 1-2: the screen, the open water and its pane are sent with a crash and a report. **OWNER: new; the 10-29 draft left this out.** |
| App activity → Other user-generated content | Yes | No | No | Optional | App functionality | row 2: the report text |
| App activity → In-app search history, installed apps, other actions | No | | | | | search runs on the phone |
| App info and performance → **Crash logs** | Yes | No | No | Optional (**OWNER**: it is on by default with an off switch; Play counts that as optional only if users can turn it off, which they can) | App functionality; Analytics | row 1 |
| App info and performance → **Diagnostics** | Yes | No | No | Optional | App functionality | row 2: version, platform, release, recent errors |
| **Device or other IDs** | **Yes** | No | No | Optional (crash switch off and the report's Install id row off: nothing sent) | Analytics (counting distinct installs per error); App functionality | row 3. **OWNER: changed from "No" (10-29), because T352 added the install id.** |
| Photos and videos | No | | | | | row 7: on the phone only |
| Audio, files and docs, calendar, contacts, messages, health and fitness, financial info, web browsing | No | | | | | none read or sent |

### A cheaper answer for Open-Meteo (OWNER, optional)
If `weather.ts` rounded latitude and longitude to 2 decimals (about 1 km) before asking, the coordinates sent to
Open-Meteo would be approximate rather than precise. Open-Meteo's grid is coarser than 1 km anyway, so the weather would
not change. The form could then say "Approximate location" only, and nothing precise would leave the phone. That is
one small code change plus a weather test. It would be a new ticket. Nothing has been changed.

## Things to check before submitting
1. The privacy page (`app/tools/seo/html.ts` `privacyBody`, canifishthis.ca/privacy) says the same as this table,
   including the install id, the map centre on a report, Open-Meteo and Esri.
2. The uploaded AAB is the build these answers describe (the staging-env one: reports on).
3. The listing's "private by design" paragraph matches (`handoff/PLAN/play/listing.md`). The 10-29 text said location
   stays on the phone, which is no longer quite true.
4. Families / target audience: 18+ (the listing draft), so the Families policy questions do not apply.
