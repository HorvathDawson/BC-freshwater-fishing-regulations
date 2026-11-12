# T605 — One B.C. clock: what T551 left

Status: PARTIAL — items 1, 2, 5 (1aed37981) and 3, 4 built; left (Claude): the start link's region (or bounds) key and its gauge key (T622 G16), see the end

Source: T551's report 2026-11-10 (commits d3b7f080e, e1142f148, 7af7f39e4). T551's ticket file numbers the items.

## Work
1. The public gauge page script (`gaugeClient.ts`) still reads the device clock: it ships as a plain script that may
   import only `figure`. Have `clientScript` inline the B.C. day helpers the same way, then drop its exception from
   `app/tools/clock.test.ts`. (T551 items 2 and 5, the part not done.)
2. T551 items 3 and 4 (only the build day moved to `bcNow()`), and item 8 (helper dedupe; fits T560).
3. The app re-reads "today" when it returns to the foreground (the shared hook has a 60 s heartbeat only, since shared
   ui may not use `document` / AppState): pass a platform "became active" signal in from each shell.
4. A midnight test for `useMapColouring`.
5. Owner choice: the static hub pages print day-relative words ("today", "yesterday") that are right only on the day
   they were built. Rebuild them daily, or print dates instead. Default until answered: print dates.

## Acceptance
- No exception left in the clock gate; the app shows the new day within a minute of B.C. midnight and at once on return.

Depends: T551. Size: S–M. Owner: no (item 5 decided).

## DECIDED 2026-11-10 (owner: "use the dates instead")
Item 5: the static hub pages print dates, never day-relative words. No daily rebuild.

## DECIDED 2026-11-10, later (owner; replaces "print dates" above)
The list pages carry less. Owner: "maybe carry less regulation info on them, instead just linking to real reg pages";
"it should link to the current day's map instead of Saturday"; "opening and closures for region can go too. too much
info for this page".
- Region, A–Z and topic pages: names and links to the water pages, plus facts that do not depend on the day. No
  "open today / closed today" words.
- "What is open this weekend" pages go. Each region page links to the live app's map for that region, opening on the
  current day (the app works out open/closed itself).
- "Waters opening and closing soon" pages go.
- The removed addresses redirect to their region page (or the map link), are dropped from the sitemap, and the site
  checks (no orphan water, duplicate titles, page budget) still pass. The "rule changes" page was not discussed: keep
  it, with dates, and say so in the report.
- No daily rebuild.

## Left after T622 + T605's pass (2026-11-10, commit 1aed37981)
Done: item 1 (gauge page script on the B.C. clock; the clock gate has no exceptions), item 2 as decided, item 5 (list
pages carry names and links; weekend and opening/closing pages removed with 301s; rule-changes page kept with dates).
Still open:
- Item 3 (the app re-reads "today" on return to the foreground) and item 4 (`useMapColouring` midnight test).
- The region page's map link opens the app on the current day but does NOT focus the region: the app's start link
  takes no region or bounds. Add a region (or bounds) key to the start link, parsed once in shared code for web and
  phone, and use it from the region pages (owner asked for "the live map for that region").
- T622 G16: the start link has no gauge key, so the public gauge page cannot open the app on that gauge. Same change.

## Built 2026-11-11 (items 3 and 4)
- **Item 3** (the app re-reads "today" on return to the foreground): `@app/ui` `appBecameActive()` is the shared word; every
  `useBcToday` re-reads the clock at once on it and re-arms its midnight timer. The shell says it: `ui-native`
  `shell/useResumed` (react-native's AppState; on the web react-native-web answers it from the page's visibility), which
  is also the count the app-update check already used (one listener in the Shell, not two). The app has ONE shell
  (`apps/mobile` + `ui-native` Shell, web and phone), so there is one place, not one per platform.
- **Item 4**: `ui-native/src/shell/useMapColouring.test.tsx`. With no day picked the status the map is handed moves to
  the new day at B.C. midnight with no tap (the same feed object until then); hidden overnight and shown again, it is on
  the new day before any timer runs; a picked day never moves. Both mutation-checked (a clock frozen at mount fails the
  midnight step; the shell not telling the clock fails the return step). `ui/src/today.render.test.tsx` pins the hook.
- Acceptance: no exception left in the clock gate (`tools/clock.test.ts`, 9 pass); new day within a minute of midnight
  (the heartbeat, as before) and at once on return (new). Not seen on a device: the phone's AppState is react-native's
  own and is not driven by a test here.

## Still open
- The region page's map link does not focus the region (the start link takes no region or bounds key), and the start
  link has no gauge key (T622 G16). Not built in this pass. One change for both, in shared code: `parseStartLink`
  (web) and `parseAppLink` (phone; it refuses unread keys today) read the key, the Shell frames the map through the
  arrival path `?show=` uses (`useShellSearch.landOn`), and `tools/seo` writes it on the region pages (it has each
  water's `mid`, so a region's box can be built there) and the gauge page. The gauge key needs a ruling first: the app
  has no gauge-only screen; the gauge page's "Open on the website" link already opens its first water's Conditions,
  where Flow picks the lead gauge.

## Owner 2026-11-12
Gauge link: leave, not built (accepted as built).
