# T608 B findings: one logic, two shells (web vs phone, and every other surface)

Read-only audit at HEAD 805f34098, 2026-11-10. Nothing was built, run or edited. Follows `T580-findings.md`.

## Summary

- 28 findings: 4 wrong answer, 16 confusing, 8 cosmetic. 6 are marked PLAUSIBLE (read, not traced through every hop).
- Worst three:
  1. G1: a search row's Open/Closed dot is always today's. The map and the water it opens are on the picked day.
  2. G2: a water's public page and share picture say "Closed to fishing today" for the day the site was built, for weeks.
  3. G3: a saved spot prints its "observed" time in UTC with no label, 7 hours off the B.C. time beside it.
- The phone and the web app are one set of screens (`ui-native` runs on both), so most web-vs-phone gaps are not in
  the screens. They sit in three places: the two map halves (G5, G15, G19, G21), the public site and gauge page, which
  keep their own copies of app rules (G2, G6–G9, G22–G24), and state kept in the address on the web only (G12, G17).
- The other large group is "by the way in": one fact asked twice with a different day or source (G1, G10, G11, G14).
- One structural fix prevents most of it: **every reader-facing fact is asked once, as (water, part, day, quantity),
  from one function in `core`/`ui`, and the day is always passed in, never read from the clock by a screen or a page.**
  The public site and the worker card then import those functions the way they already import `figure()`.
  Two gates hold it: a "same answer on every surface" test (G-gate 1) and a "both map halves honour every prop" test
  (G-gate 2).
- T580 leftovers still open and not repeated below: F8, F12, F13, F14 (ticket T585) and the T605 clock items.

## What "web" and "phone" mean here

- `app/packages/ui` is shared hooks. `app/packages/ui-native` is the screens, drawn on both the phone and the web.
- Real platform splits are only: the `.web` / `.native` file pairs (36 pairs), 16 files with one allowed inline
  platform check (`tools/check-platform.mjs:199-216`), the two map halves, and the shell `apps/mobile/src`.
- There are three more surfaces that are not the app at all and keep their own code: the static water and hub pages
  (`app/tools/seo`), the public gauge page script (`tools/seo/gaugeClient.ts`) and the worker's share card
  (`app/workers/feeds/src/gauges/card/card.ts`). Most copies live there.

## Ranked findings

Paths are under `app/`. Size: S under a day, M a few days, L more. "P" after the severity means PLAUSIBLE.

| id | severity | where | what differs | state that shows it | fix | size |
|---|---|---|---|---|---|---|
| G1 | wrong answer | `packages/ui-native/src/SearchSheet.tsx:118` (`const today = bcNow()`), `search/SheetRow.tsx:119,134` (`rowStatus(item, statusIndex, today)`) vs `shell/useMapColouring.ts:237` (`day: mapDay ?? today`) | The search row's status dot and spoken label are always today's. The map and the water page use the picked day. | Pick Saturday on the date pill. Search a water that closes Saturday. Row says Open; the line it lights is drawn Closed; Regulations says closed. In Salmon colouring the dot is still the Standard status. | Pass the Shell's day (and the colour mode) to `SearchSheet`. | S |
| G2 | wrong answer | `tools/seo/model.ts:155-161,208-216`, `html.ts:90-94`, `site.ts:112`; build day `build.ts:250` | The water page's description and share picture carry "Open to fishing today" / "Closed to fishing today." for the build day. No daily rebuild. | A link shared two weeks after a build that fell on a closed day previews "Closed to fishing today". The app says open. | Take day-relative words out of the description and the picture (T605 item 5 decided this for hub pages only). | S |
| G3 | wrong answer | `packages/data/src/spots/words.ts:338` (`at.replace("T"," ").slice(0,16)`) vs `words.ts:345` (`localDateTime`, B.C.) ; weather hour is GMT (`spots/weather.ts:153-167`) | The spot page prints "observed 2026-08-30 16:00" (UTC, no label) under a headline in B.C. time. | A 09:00 visit reads "at 09:00", then "observed … 16:00". An evening visit shows the next day's date. (Weather confirmed; the gauge time is P.) | `observedLine` uses core `localStamp`. Two tests pin the UTC form and need changing. | S |
| G4 | wrong answer, P | `packages/map/src/sections.ts:91-97`, `adapters/web.ts:86`, `adapters/native.ts:107` | With several waters under the finger, each map takes the first hit in its own engine's order. No nearest-first sort. | A tap where a creek meets a river may open the creek on one platform and the river on the other. Not proven: the two engines' orders were not compared on a device. | Sort hits by distance to the finger in the shared code. | M |
| G5 | confusing | `packages/map/src/Map.web.tsx:95` (`useTrace`) vs `Map.native.tsx:104-108` (never reads `trace`); `adapters/contract.ts:145-149` says the phone owes it | The route overlay is never drawn on the phone. | On the gauge map in Conditions the web shows the routes to each gauge, share labels and the "you are here" pin. The phone shows only the lit water. | Feed the phone map's trace sources from the same `traceOverlay`. | M–L |
| G6 | confusing | `tools/seo/prerender.ts:173` (drops the ready-made page only for `view`) vs `apps/mobile/src/systemBack.web.ts:98-106` (address carries `blk`, `m`, `date`) | Reloading a water page with a part or a day first shows the whole water on the build day, then switches. | Reload `/water/<slug>?blk=…&m=…&date=…`: 1–5 s of another part and another day. | Also drop the ready-made page for `date`, `blk`, `m`, `ix`. | S |
| G7 | confusing | `workers/feeds/src/gauges/card/card.ts:223`, `tools/seo/gaugeClient.ts:62-63` (reading older than 2 h) vs `packages/ui/src/panel/conditions.ts:228` + `core/src/freshness.ts:62-70` (age of our copy) | "Out of date" is judged on different clocks. | Last reading 3 h old, feed ran 10 min ago. Page: "Out of date: no new reading for 3 hours". App: "checked 10 min ago", no warning. | One core rule, used by the card, the page and the app. | S–M |
| G8 | confusing | `tools/seo/gaugeClient.ts:192`, `card.ts:339,424,432` | The page's "Read by our feed X ago" is when the card was last drawn, not when the feed ran. An unchanged card is not redrawn. | Same state as G7: page "2 hours ago", app "10 min ago". | Drop the line or read the index's fetch time. | S |
| G9 | confusing | `card.ts:99-155` (`outlookOf`: past ±10 % is "Rising to …", 2 figures) vs `packages/core/src/change.ts:25,49-55` (within ×1.25 is "Little change expected", `figure()`) | Two rules for the forecast in words. | A +15 % forecast: the share card says rising; the app says little change. | Move the card's rule to core's. | M |
| G10 | confusing | `packages/ui/src/runTiming.ts:95` (`useState(today)`), `ui-native/src/Shell.tsx:254` | Runs keeps a third day of its own. It never reads the map's day. | Pick a day three weeks out on the Map tab, open Runs: it answers this week. Same class as F2; not in T582. | Owner choice: seed Runs from the map's day, or have Runs name its own day. | S–M |
| G11 | confusing | `packages/ui-native/src/AlertMe.tsx:206` (card figure) vs `:154` (72 h series' last point); `menu/NotificationsPage.tsx:89` (`current: null`) | The alert editor still holds two "now" values (what T581 left of F7). | One editor: "Now 30.2 m³/s" and "It is above that now (29.8 m³/s)". Opened from More the value box is empty; from the card it is filled. | One `AlertNow` from `@app/ui` for both lines and both ways in. | S |
| G12 | confusing, P | `packages/ui/src/startLink.ts:199-206`, `ui/src/regs.ts:394-397` (`mapDayOf`: any date not today is kept) vs `apps/mobile/src/systemBack.native.ts:16-18` (nothing kept) | The web address keeps the regulations day; nothing refuses a day already past. The phone always cold-starts on today. | Pick Saturday on the web, come back to that tab or bookmark on Monday: Regulations opens on last Saturday. The phone opens on today. Not traced: whether a later step clamps a past day. | A kept day earlier than today reopens as today, with one line saying so. | S |
| G13 | confusing | `ui-native/src/spot/Record.tsx:93,97,102`, `spot/WeatherWindow.tsx:32`, `core/src/likeVisit.ts:165-167`, `data/src/spots/words.ts:386` | One spot's weather is formatted by four code paths. | A 13.6 °C visit: "13.6°C" in the record and list, "14°" in the visit column, "14°C" in the strip. | One `weatherFigure` in core. | S |
| G14 | confusing, P | `core/src/salmonFeed.ts:222` (legend), `:251` (card), `packages/ui/src/dfoNotice.ts:24` (More, bypasses `pickSalmon` `:236`) | "DFO checked" comes from two reads. | A release cut after the last capture: More says DFO was checked 3 h ago; the card and legend show rules with no checked line. | More reads the same picked facts. | S |
| G15 | confusing | `packages/map/src/web/useMapInstance.ts:82-86` vs `Map.native.tsx:104-108`; `map-props.ts:112-118` | `zoomControls` is ignored on the phone, though the prop's own note gives a phone reason for the buttons. | A small map inside a scrolling sheet has + / − on the web and none on the phone. | Honour the prop on the phone. | S–M |
| G16 | confusing, P | `tools/seo/gauges.ts:120` (`/water/<slug>?view=conditions`), `packages/ui/src/startLink.ts:32-33` (no gauge key) | The gauge page's link into the app names no gauge. The app picks its own lead. | From gauge A's page on a water with several gauges, the app may open on gauge B's reading. | The link carries the gauge's place (`blk`, `m`). | M |
| G17 | confusing | `packages/core/src/share.ts:5,17` ("opens … in a browser or the app", link is `/?regs=…`) vs `apps/mobile/src/startLink.native.ts:7` (no links), `app.json:8` (custom scheme only) | A shared water link never opens the installed app, and its preview is the home card, not the water's. | Send a link from the phone to a phone with the app: it opens the website. The preview picture is the generic one. | Owner choice: app links on Android, or fix the comment. Share `/water/<slug>` when the slug is known. | S (link) / M (app links) |
| G18 | confusing, P | `tools/seo/model.ts:208-216`, `site.ts:80`, `verify.ts:54` vs `packages/core/src/status.ts:131-139` | The hub's open/closed tag is rolled up by the site's own code, and the site's check rebuilds the same roll-up from its own input. | A water whose parts differ could be tagged differently on a hub and in the app's search. The T605 hub decision (no day words) is not built yet. | Build the T605 decision; until then roll up with core's function. | S–M |
| G19 | confusing | `packages/map/src/web/markers.ts:65-78` vs `Map.native.tsx:778-780` | A titled pin opens its words on the web. The phone never reads the title. | Tap a named pin: words on the web, nothing on the phone. | Show the title on the phone. | S–M |
| G20 | confusing | `packages/ui-native/src/regs/KeepBlocks.tsx:95-96`, `KeepTab.tsx:112` (`key={i}`) | A caveat block's fold depends on how the day was reached. | From a day inside an area's dates to one outside: stays open. Open the outside day directly: folded. A part step carries the last part's fold. | Key by caveat id; derive the fold from the day unless the reader toggled it. | S |
| G21 | cosmetic | `packages/map/src/web/outsideMask.ts:56-84` vs no such filter in `native-style.ts` | Place names outside B.C. are hidden on the web only. | Alberta and Washington names draw over the grey mask on the phone. | Write the same filter into the phone's style. | M |
| G22 | cosmetic | `tools/seo/gaugeClient.ts:46-53` vs `packages/core/src/freshness.ts:33-51` | Age words: page "75 minutes ago", "40 hours ago"; app "1h ago", "40h ago". | Any gauge, page beside app. | Inline core's age words into the page script the way `figure` is. | S |
| G23 | cosmetic | `tools/seo/html.ts:21`, `model.ts:39-41` vs `packages/core/src/regs/story.ts:21-26` | The site says "Region 7" for both 7A and 7B. The app says "Region 7A". | Any Region 7A water page. | Use core's region name. | S |
| G24 | cosmetic | `card.ts:172,222` ("Nov 7, 2:20 p.m. PT") vs `core/src/freshness.ts:66` ("2026-11-07 at 14:20 PT") | Two time formats for the same reading time. | Gauge page beside the app card. | One format. | S |
| G25 | cosmetic | `packages/ui-native/src/format.ts:18,23` (`toLocaleString()`, the device's language) | Counts follow the device's language, not one rule. | A French-Canadian phone prints "60 648"; the web in English prints "60,648". | Fix the locale to `en-CA`. | S |
| G26 | cosmetic | 4 decimals: `ui-native/src/SpotCapture.tsx:209`, `data/src/spots/words.ts:345,387`; 5 decimals: `spot/Parking.tsx:18`, `spot/ParkingPick.tsx:26`, `ui/src/startLink.ts:49` | One point printed at two precisions. | A spot's position on its page vs its parking line. | One `latLon()` in core. | S |
| G27 | cosmetic | `ui-native/src/shell/MapLegend.tsx:308-314`, `SpotCapture.tsx:256-259`, `search/SheetRow.tsx:151-155`, `regs/SalmonCard.tsx:171,182,185`, `spot/Record.tsx:93-102` | Reader sentences built inside screens, not in core (AGENTS 25, 33). None wrong today. | None yet; this is where the next divergence starts. | Move each to core words. | S |
| G28 | cosmetic | Layer order: `map/src/web/markers.ts:29-32`, `web/useMe.ts:36-39`, `web/useSpots.tsx:70-73` vs `Map.native.tsx:837-854`. Follow-me: `web/useMe.ts:58-59` vs `Map.native.tsx:538-541`. Legend fold: `shell/MapLegend.tsx:46`. Words: `gaugeClient.ts:77,79` "out of date" vs `ui/src/panel/conditions.ts:207` "likely out of date"; `card.ts:220` vs `core/src/standing.ts:108`. | Small ones, grouped. The blue dot sits over a spot on the web and under it on the phone. A pinch stops follow-me on the phone only. The legend's fold resets after any water. Two wordings for an old forecast and for "no record". | As listed. | One order, one release rule, fold in the Shell, core words. | S each |

### Notes on the table

- G1, G5, G15 and the address/links rows (G12, G17) were re-read by me line by line. The rest come from three
  delegated readers; I spot-checked their file and line for G2 and G10 and did not re-read every other row.
- Duplicated constants in the two map halves were compared (centre zoom 15, fit padding, tap box 24 px, mark box
  44 px, glide times): none differ today.
- The spots, layers, feed and update stores (`packages/data/src/spots/store.web.ts` vs `store.native.ts`) hold the
  same keys and defaults. The one difference is the gauges' kept week (web: none), which is F14.

## T580 fixes checked at HEAD

- Held in code: F1 and F7's prefill (`cardQuantity`, `alertNowValue`: `ConditionsPanel.tsx:136`, `AlertMe.tsx:206`),
  F3 (`addressKept`, `ui/src/startLink.ts:46`), F5 (`frameBasinAsk`, `WaterViews.tsx:94`), F15 (no device-clock
  getters left in `core/src/salmonFeed.ts` or `ui/src/panel/conditions.ts`).
- Taken from the tickets, not re-read: F2, F4, F6, F9, F10, F11 (T582, T583, T584, T586, T598, T604 all IN REVIEW).
- Still open: F8, F12, F13, F14 (T585 OPEN); T605 (gauge page script on the device clock; "today" on return to the
  foreground).
- New on top of a fix: G11 (F7's second half), G10 (F2's class in Runs).

## One platform only

Phone only, by design:
- Alerts and notifications. `apps/mobile/src/notify.web.ts:1`: "phone-only v1, owner 2026-11-05".
- Saved spots. The web shows a notice in their place (`ui-native/src/WebSpotsNotice.tsx:20`, listed in
  `check-platform.mjs:205`). Note: the web still has a working spots store (`store.web.ts`), unused by readers.
- Store update prompt (`appUpdater.web.ts`, T390), the mobile-data question and Wi-Fi wait (`App.tsx:268`), background
  download, the "release needs a newer app" gate (`App.tsx:333`; the web is never gated).
- The gauges' kept week for offline (`store.native.ts:58`, T315). By design, but see F14.
- Android's back button (`systemBack.native.ts`), memory trim, the Map tab's regulations read-ahead
  (`shell/BrowseHost.tsx:209`).

Web only, by design:
- Start links, the address following the route, and reload-into-the-same-water (`startLink.native.ts:1,7`: "links are
  web-only", T41, T369). A phone cold start always opens the map on today; a web reload reopens the water, part, tab
  and day. This is the largest sanctioned "way in" difference; G12 and G17 come from it.
- The low-bandwidth row in More (`menu/MoreScreen.tsx:286`), idle preloading (`Shell.tsx:148`), the prerendered water
  pages, the developer `RegsDebug` hook.
- Map props `onRegion`, `onImageryError`, `insetTop` (said in `map/src/map-props.ts`).
- Help and the walkthrough are written per platform on purpose (`Walkthrough.tsx:156`, `menu/HelpPage.tsx:89`).

Web only, drift (no design says so):
- The route overlay on the gauge map (G5), the zoom buttons (G15), pin titles (G19), the B.C. label mask (G21).
- Tile and sprite errors reach the screen's error handler on the web only; on the phone they go to the report log
  (`web/useMapInstance.ts:105-112` vs `native/styleAssets.ts:43-46`). The phone half says this is on purpose; the
  result is that a failed tile is silent on the phone.

Not the app at all (third copies): the worker's share card, the gauge page script and the static site. G2, G6–G9,
G16, G18, G22–G24 are all here.

## Gates

What already catches a new divergence:
- `tools/check-platform.mjs`: every pair has both halves and the same exports; no new inline platform check.
- `tools/one-standing-words.test.ts`: standing words come from core only; it also scans the worker and `tools/seo`.
- `tools/clock.test.ts`: no device-clock getters on instants (one known exception, the gauge page script).
  It cannot see a time cut out of a string (G3).
- `packages/map/src/parity.test.ts` and `adapter-contract.render.test.tsx`: taps, lit water, fit and camera through
  both map adapters. They feed both halves the same hit order, so they cannot see G4, and they do not check props.
- `tools/seo/verify.ts`: page sentences equal the app's for the build day only.
- `tools/one-shown-quantity`, `one-water-screen`, `one-route-per-place`, `one-speaker`, `app/conformance`
  (map colour vs sheet): each holds one "asked once" rule inside the app.

Missing, proposed (most useful first):
1. **Same answer on every surface.** For a few fixture waters × (today, a later day): the search row's status, the
   map class, the Regulations headline and the water page sentence are equal. Catches G1, G2, G18. Size M.
2. **Both map halves honour every prop.** Each `MapProps` key is read in `Map.web.tsx` and `Map.native.tsx`, or sits
   on a short web-only list with its reason. Catches G5, G15, G19 and the next one. Size S.
3. **Public gauge page vs app card.** One fixture station: figure, standing words, stale verdict, age words and
   reading time are equal on the card, the page script and the app card (the parity test F4 asked for). Catches
   G7, G8, G9, G22, G24. Size M.
4. **No formatting in a screen.** A source gate over `ui-native/src`: no `toFixed`, `toLocaleString`, `Intl.`,
   `Math.round` on a shown value, or `.slice` on a time string outside a short allowed list. Catches G3, G13, G25,
   G26 and G27's class. Size S.
5. **One day per ask.** A source gate: under `ui-native/src` and `ui/src` only the Shell may call `bcNow()` for a
   shown day; the rest take the day as a prop. Catches G1 and G10. Size S.
6. **Kept day is never past.** A start-link test: a `date` earlier than today opens today and says so. G12. Size S.
7. **Hand-over keys.** `handoff.test.ts` covers every route key that changes the page (`date`, `blk`, `m`), not only
   `view`. G6. Size S.
8. **No day-relative word in a built page.** A check over the site's output for "today", "tomorrow", "this weekend"
   outside the live parts. G2 and T605 item 5. Size S.

## Not checked

- Nothing was run: no test, no build, no emulator, no device. Every finding is from reading code.
- The six rows marked P were not traced end to end. G4 needs two devices to prove.
- iOS, the patched MapLibre's internals, and how the two map engines evaluate the same paint rules.
- More / About / settings pages line by line, the walkthrough and first-run screens, report-a-problem, the error and
  loading screens' wording, the tidal and obstacle sheets, the Updates log page, the offline maps page beyond its
  progress bar.
- `tools/regs-web`, `tools/regs-recut`, the site's layout and budget code, the worker's DFO and in-season code
  (feed data, not reader text).
- Whether the phone's network stack honours the feeds' cache times as a browser does (bears on F8).
- Reader-facing differences from the device's own settings beyond G25 (font scale, 12/24-hour, language).
