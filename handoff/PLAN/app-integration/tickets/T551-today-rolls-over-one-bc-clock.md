# T551 — "Today" rolls over at midnight B.C. time everywhere, from one clock; site pages do not print a stale day

Status: OPEN.

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 7 of 17; highest severity 4 of 5.

Line numbers are the reviewers' and may be off by a few; "Second opinion" is what the verifier corrected. Fixes are proposals: re-read the code before building.

## Today
### 1. "Today" never rolls over: map status, salmon colours, gear marks and Conditions keep the day the screen was first drawn
Severity 4, wrong-or-missing-data, confirmed, reaches an angler; area app-logic-parity.
- Evidence: With no day picked (mapDay null = today) the day is captured inside a memo that has no day dependency. app/packages/ui-native/src/shell/useMapColouring.ts:235-238: useMemo(() => statusFeed({ ..., day: mapDay ?? bcNow() }), [statusOk, statusIndex, mapDay, statusOnly]). app/packages/ui/src/salmonColour.ts:56-58 and 87-89: dayKey is the literal "today" when i.day is null, and the day is useMemo(() => i.day ?? bcNow(), [dayKey]). app/packages/ui/src/panel/conditions.ts:122: useMemo(() => day ?? bcNow(), [day]). No midnight timer exists (grep midnight/AppState in ui, ui-native, apps/mobile); the only resume counter (Shell.tsx:317-325) feeds the app-update check. The regs page recomputes regsDay(mapDay) on every render (ui/src/regs.ts:383-389), so it moves on while the map does not.
- Example: App opened Oct 31 evening on a water that closes Nov 1, left in memory, reopened Nov 1 morning: the map still paints Oct 31's open colour under "Today", while the water's page (re-rendered) says closed.
- Second opinion: Holds as written, with two overstatements. The page moves on only when something re-renders it; it has no timer either. In Conditions the frozen day is used only as the reference instant for silencing donor rows (conditions.ts:122-126), which is a much smaller effect than the map colours. The bug needs the process to survive midnight with no day pick, release change or isolate tap.
- Proposed fix: One useBcToday() hook in @app/ui (state set from bcToday(), re-armed by a timer to the next B.C. midnight and on AppState 'active' / visibilitychange); every `x ?? bcNow()` in a memo takes it as a dependency. Add a test that advances an injected clock past midnight and expects the status feed's day to change.

### 2. The B.C. clock has more than one home: gauge share page, worker card and two salmon lines do not use core clock.ts
Severity 3, divergence, confirmed, reaches an angler; area app-logic-parity.
- Evidence: core/src/clock.ts:20-45 writes UTC-7 all year in code because shipped tzdata still turns America/Vancouver to UTC-8 from 1 Nov 2026. Other sites: app/workers/feeds/src/gauges/card/card.ts:37,105-106,178-180 (Intl with timeZone America/Vancouver, timeZoneName short); app/tools/seo/gaugeClient.ts:91,139,144 (toLocale*String with America/Vancouver, and a typed `T12:00:00-08:00`); app/tools/seo/gauges.ts:81 (Open-Meteo timezone=America/Vancouver). core/src/salmonFeed.ts:255-256 formats "Checked with DFO ..." with getHours()/getMonth() (device zone, unlabelled) and :217-218 salmonCheckedLine uses getUTCMonth()/getUTCDate(). data/src/sql/shared.ts:230-232 takes the chart's year and "today" from toISOString() (UTC). app/tools/clock.test.ts only bans no-arg new Date()/Date.now() and does not scan tools/seo or workers.
- Example: Feed at 2026-11-09T07:03:43Z: the app's stamps say 00:03 PT Nov 9; a card/gauge page on 2026a tzdata says 11:03 p.m. PST Nov 8; salmonCheckedLine for a 17:30 PT check says the next day's date.
- Second opinion: Holds, and it is live now: since 1 Nov 2026 any 2026a runtime is an hour behind the app's clock. I could not check the Cloudflare runtime's tzdata, so the worker card's actual output is inferred from the same Intl call. shared.ts takes the day from the last observation's UTC date, a chart-extent effect only. The -08:00 literal at gaugeClient.ts:139 is harmless at noon. The "DFO rules checked" line names tomorrow for any check after 17:00 PT.
- Proposed fix: Export the offset rule (bcOffsetHours + label) as the one formatter; card.ts imports it, gaugeClient gets it inlined by the build (it already receives config), salmonFeedLine/salmonCheckedLine use localStamp/bcWall. Extend clock.test.ts to tools/seo and workers/feeds/src and to ban `timeZone:` / getHours() on an instant outside clock.ts/day.ts.

### 3. Static site pages bake the build day's open/closed status and nothing rebuilds them on a clock
Severity 3, wrong-or-missing-data, confirmed, reaches an angler; area deliver-contract. Related ticket: T544 (related, does not cover the day).
- Evidence: app/tools/seo/build.ts:261,322 builds with `on: buildDayOf(now)`; app/tools/seo/model.ts:52-56 takes the day from the build machine's local Date (not core's `bcNow`). app/tools/seo/site.ts:80 words 'open today / closed today'; 239-258 the per-region 'open this weekend' page; 261-270 'next 60 days'; each stamped 'as of <build day>' (224, 252). The status comes from the regs store only (`statusOn`), so in-season and live DFO changes are not in it. .github/workflows has no site job (app-ci, feeds-hydat, feeds-runtiming, pipeline-ci, staging-*); deploy/prod/site.sh is owner-run.
- Example: A site deployed Friday 2026-11-06 still serves '/…/open-this-weekend' for 'Weekend of 2026-11-07 and 2026-11-08' the following week, and its region lists say 'open today' for a water whose season closed on Nov 15.
- Second opinion: Confirmed; every such page prints its as-of day, so it is dated, not hidden. The local-Date point is real but minor while the build runs on the owner's B.C. machine. T544 does not cover it: this needs its own ticket (or a widened T544). The site is staging-only today, so no public reader is affected yet.
- Proposed fix: Either drop day-relative claims from static HTML (print the season dates, let the hydrated app say 'today'), or have the worker start a daily site rebuild (the T413 dispatch pattern) and fail the smoke when a page's as-of is older than N days. Use `bcNow` for the build day.

### 4. The static site prints "open today / closed today", "this weekend" and "next 60 days" for the build machine's day, with no rebuild cadence and not on the B.C. clock
Severity 3, wrong-or-missing-data, confirmed, reaches an angler; area app-logic-parity.
- Evidence: app/tools/seo/build.ts:249 and :320: now = --date ? new Date(`${date}T12:00:00`) : new Date(), then buildDayOf(now) (seo/model.ts:52-56 reads getMonth/getDate/getDay in the machine's zone, not core bcNow). site.ts:80 WORD = { open: "open today", closed: "closed today", ... } is listed beside every water on region, letter and topic hubs (:223, :233, :292); weekendDays (:84-91) and the season-changes page (:261-265, turns from model.ts:169-181) are relative to that day. The only qualifier is a small "as of <date>" (:224). The site is built by hand per release (deploy/staging/site.sh); no schedule exists. The share SVG/description carries m.answer (:112).
- Example: A site built on a Thursday still says "open today" and lists "this weekend" for that past weekend three weeks later; a build run after 17:00 PT on a UTC machine is already a day ahead.
- Second opinion: Holds for the hubs, weekend and changes pages. Water pages hydrate into the app (T269), so they correct themselves. The "UTC machine a day ahead" example is hypothetical: the site is built by hand on the owner's Mac, where the real clock error is the one hour between 23:00 and 24:00 PST. The staleness between hand builds is the substantive part. Currently staging only.
- Proposed fix: Either drop day-relative words from static hubs (say "open on <build date>" and let the hydrated app or a tiny script compute today from the shipped year runs), or rebuild/deploy the site daily from the worker-triggered workflow. Build day from core bcNow(); make verify.ts fail when asOf is not the B.C. day of the build.

### 5. Gauge share card and gauge web page read the clock from tz data (now UTC-8 'PST'); the app hard-codes B.C. as UTC-7 all year
Severity 2, divergence, confirmed, reaches an angler; area cross-duplicates.
- Evidence: app/packages/core/src/clock.ts:24-43 fixes B.C. at UTC-7 from 8 Mar 2026 and says tz data must NOT be read because 2026a still falls back on 1 Nov. But app/workers/feeds/src/gauges/card/card.ts:37 sets ZONE="America/Vancouver" and formats with Intl at :105-106 (weekday/month-day), :178-181 (`pacific` stamp, `dayOf`), used at :109 ("today"), :215-216, :230 (reading time), :388 (card key). app/tools/seo/gaugeClient.ts:91,139,144 does the same in the browser. Related: app/packages/data/src/sql/shared.ts:230-232 takes the year chart's year and 'today' from the UTC day, not the B.C. day.
- Example: Run here (Node 25.9, tzdata 2026a): Intl America/Vancouver formats 2026-11-09T19:00Z as "11:00 PST"; the app's bcNow gives 12:00 PT. If the Workers runtime and browsers carry the same tz data, a gauge reading is stamped one hour apart in app and card/page.
- Second opinion: Overstated: the card time carries its own zone label, so '11:00 PST' is the same instant as the app's '12:00 PT' and is not a wrong time. The real defects are the two different wall clocks for one reading and the card's 'today' / weekday boundary sitting an hour late. Which tz data the Workers runtime and each browser carry was not checked, so the page can differ per browser. The shared.ts UTC-day point is real but separate: after 17:00 B.C. time the chart's 'today' is one day ahead.
- Proposed fix: Move bcOffsetHours/bcWall formatting into one core helper and use it in card.ts and gaugeClient.ts (card.ts already imports core staleness). Add a test that formats a January instant in all three and compares. Use the B.C. day in shared.ts year span.

### 6. Manifest `published` is stamped in UTC, so 'Regulations as of' can read tomorrow's date
Severity 2, wrong-or-missing-data, confirmed, reaches an angler; area deliver-contract.
- Evidence: pipeline/deliver/release/stamp.py:64 `today = ... dt.datetime.now(dt.timezone.utc).date()` and pipeline/deliver/release/publish.py:96 the same. The app shows it as the regulations' date (app/packages/core/src/menu/legal.ts:63) and every other day in the app is a B.C. day (app/packages/core/src/clock.ts `bcNow`, UTC-7).
- Example: data/release/r-a178299f6005/manifest.json has `"published": "2026-11-10"` while the B.C. date of this review (and of the stamp) is 2026-11-09.
- Second opinion: Confirmed on the live release. Line numbers are 60 and 95, not 64 and 96. Impact is a one-day-ahead label only: the only other reader of `published` is a presence check (app/packages/data/src/manifest.ts:71); I found no date comparison that would go wrong.
- Proposed fix: Stamp with the B.C. day (one helper mirroring clock.ts's fixed UTC-7), in both stamp.py and publish.py; add a test that a 23:30 PT stamp gives the same day.

### 7. Map legend's 'DFO rules checked <date>' is the UTC date; the card's line is local
Severity 1, wrong-or-missing-data, confirmed, reaches an angler; area dfo-salmon.
- Evidence: app/packages/core/src/salmonFeed.ts:220-226 `salmonCheckedLine` builds the date from getUTCMonth()/getUTCDate() (used by ui-native/src/shell/MapLegend.tsx:48). The card line `salmonFeedLine` (salmonFeed.ts:243-255) uses getMonth()/getDate()/getHours() (device local).
- Example: In-season cron '15 */6' writes at 00:15 UTC = 5:15 pm PDT the day before: on Aug 14 evening the legend reads 'DFO rules checked Aug 15' while the card reads 'Checked with DFO Aug 14, 2026, 5:15 pm'.
- Second opinion: Confirmed. Reach is narrower than the example implies: the heartbeat write lands at 00:15 UTC only if that is where the daily write falls; off-season the write is 14:15 UTC, where both dates agree. A content change at any 00:15 or 18:15+ UTC run also triggers it. Cosmetic (a date one day ahead), no rule is wrong.
- Proposed fix: One formatter for the feed stamp (local date) used by both lines; a test at 00:15 UTC.

### 8. The same "a date as day / weekday / year" and "date to YYYY-MM-DD" helpers are typed in several places beside the one clock
Severity 1, duplication, confirmed; area app-logic-parity.
- Evidence: Page day {day, weekday via WEEKDAYS[(getDay()+6)%7], year}: app/packages/ui/src/share.ts:125-128 (pageDay), app/tools/seo/model.ts:52-56 (buildDayOf), and ui/src/regs.ts:383-389 (regsDay, which instead uses core tapDay's weekdayOf). Date to ISO day: core/src/day.ts:89-92 (wallIso), core/src/inseason.ts:110-112 (isoDayOf), ui/src/startLink.ts:38 (ymd), ui/src/panel/change.ts:186, ui/src/regs.ts:376-377 (isoDay), seo/model.ts:55; unpadded day keys at ui/src/regs.ts:250, ui/src/salmonColour.ts:56 and :87, ui-native/src/DayCalendar.tsx:94. one-day-table.test.ts pins month/weekday tables and isIsoDay only, not these.
- Example: pageDay (share.ts) and regsDay (regs.ts) both feed regsPage/waterHasPage for "today" through different weekday code paths.
- Second opinion: The copies exist as listed and all give the same output today, so this is a tidy-up with no current wrong answer. The unpadded keys are private memo keys and never cross a boundary; counting them as duplication is a stretch. The suggested Feb 29 divergence is speculative: every copy reads plain Date fields.
- Proposed fix: core day.ts exports pageDayOf(d: Date) (day, weekday through weekdayOf, year, iso) and wallIso is the only ISO formatter; replace the copies; extend one-day-table.test.ts to fail on a new `getFullYear()}-` template or WEEKDAYS[(…getDay()…)] outside day.ts.

## Work
Fix the items above in severity order, one commit per item where they are separable. Anything found and not done becomes a new ticket.

## Acceptance
With the app left open across midnight (fake clock), map status, salmon colours and gear marks move to the new day. Every date shown (gauge card, share page, legend, manifest "as of") comes from core clock.ts; a guard test fails on a second B.C. clock. Site hub pages either carry no day-dependent words or are rebuilt by the worker-started job.

Depends: —. Size: M. Owner: no.
