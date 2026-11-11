# T605 — One B.C. clock: what T551 left

Status: OPEN (item 1, 2 and the decided list-page sections done 1aed37981; items 3 and 4 left)

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
