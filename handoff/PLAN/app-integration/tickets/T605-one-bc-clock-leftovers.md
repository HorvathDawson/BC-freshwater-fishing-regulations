# T605 — One B.C. clock: what T551 left

Status: OPEN

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

Depends: T551. Size: S–M. Owner: item 5.
