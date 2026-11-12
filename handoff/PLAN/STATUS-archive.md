# Status archive

Entries older than 7 days, moved out of `STATUS.md` by T540 (2026-11-11), newest first, unchanged. `STATUS.md` keeps the last 7 days.

## 2026-11-03 (chat 5): batch 2 memory, into batch 3 [T360, T366]
- **T360 measurement fixed:** the earlier memory runs were all over satellite after the satellite step (a second tap
  never turned it off). On the drawn map: Conditions still adds ~140 MB native and keeps it after you leave; switching
  its gauges/arrows off changes nothing. About 17 MB of that is the app's own data; the rest is the map engine's. Next:
  a native memory profile (heapprofd) to name it.
- **Bug fixed:** on Conditions, Gauges off left the gauge dots drawn (02433a339).
- **Memory given back when Android asks** (in code, emulator check running): the maps drop tiles they don't draw and
  the databases shrink their caches. Before this nothing answered.
- **Batch 3 started:** T366 (clean DFO refresh with a preview; region 2's FN1179) is being worked as a second lane.
  Found: regions 2 and 6 ship no region header notes today (their rules are older than any saved page); T366 keeps the
  header with the rules so it cannot go missing.
- **Where the memory goes (heapprofd):** the rule book LEAKS ~25 MB of pdfium pages per open (the PDF library closes
  the document but not its pages); patched (react-native-pdf, closes them on close). Conditions keeps only ~28 MB
  live: the rest is memory the map engine used and freed, which Android's allocator keeps; a trim now hands it back
  (`mallopt` purge). Both on the emulator now (t360-fix).
- **T366 done, waits on you:** the DFO refresh previews before it writes; ran for regions 1, 2, 6 (no marks moved).
  Region 2's Fraser was reworded twice (FN1179 is on one): confirm the two places in DFO mode (OWNER-TODO).
- **T338 done** (a stretch closure no longer closes the whole river; worker too), **T357 done** (Python side).
- **T356 + T358 done (481bc8266): salmon/4, dfo-feed/3.** The salmon card's closure, shared limits and sizes now come
  pre-resolved from Python; DFO's prose "pooled across" scope is a structural field. Yours: one `When` shape (6 rules
  read "until further notice": Python says "check DFO", the worker says open; proposal in T358), and the worker
  redeploy (dfo-feed/3) with 0.5.0. On a closed page (5A, 7) a water's own row now shows the closure card too.
- **T343 done (561115e24, data side):** every DFO header note is folded into the limit line / sizes / closure or
  dropped (100 m facility, how to measure); the daily total says "fresh and tidal waters combined"; adult chinook 50 cm
  default (5B; R6's Fraser watershed). Yours (OWNER-TODO): daylight / R1 bait ban / eulachon notes stay as words; R6's
  "Fraser River" binds no R6 water. Images + collapsible source line → T367 (UI chat).
- Owner 11-03 answers done: Fraser places = variants of the curated ones (9d5d52553, notes 5c31c2b10); "until further
  notice" = all year (cf7d7130a); daylight → `hours` (R2 too: 1 h either side), R1 bait ban dropped, R6 binds the Fraser
  (ec5b37257; scopes trimmed to each region's waters — the Fraser watershed is 6,713 items). Tickets for 0.5.0: T372
  (partial coverage the trout way, in Python), T373 (run timing built with the release); T374 (trib walk collects lakes).
- **Batch 3 release built + promoted locally: r-ac5a4e4f734c** (salmon/4, T369 lines; lines gate 1.25 → 1.4 MB, measured
  1.28). Not on staging yet. DFO tests re-pinned (461 rows / 548 rules).
- Memory: purge works (17d5f0dc3); **PDF leak FIXED (fe0f4e659)**: pdfium held after the rule book 0.9–2.5 MB (was
  50–77). T375 forecast every day +1..+10 in code (1c735272e); the UI chat's horizon tests are theirs to update (T367 e).
  Batch 3 release r-ac5a4e4f734c is LOCAL ONLY until the 0.5.0 cut (batch 7): salmon/4 needs MIN_APP_VERSION 0.5.0, so a
  staging upload now would leave the 0.4.0 testers on old data. Rebuilt 11-04 after the T358 non-retention change: same
  content (plain "Non-retention" already read closed — only "catch and release … until further notice" changes). 0.5.0
  plan grew 11-03 (bda00e2fb: 3b T374/T372/T373/T375, T376 in batch 4, 6d housekeeping, 6e T240/T370).
- Batch 3b (11-04): T374 closed, not a bug (owner-confirmed sloughs, e23994f5a); T373 done (1cca5a8ff: deliver all binds
  run timing per release, vintage refuses a mismatch, staging upload publishes it; local feed rebound).
- 11-04 test sweep: T375 leftovers fixed (c2b853f33: HORIZONS literal, dev feed every day, likeVisit); run-species tokens
  for the UI chat; ticket ledger whole; RELEASE PINS on r-ac5a4e4f734c (931dba1ba: taps/hydrate + regs-fixture
  r-056baa843558; the slough fold re-cut x5p0/x13p4/x9p5); map-gestures pins T345 (7cf72cd1c). Conformance 4138/4138.
  The rest of the app suite's reds are the UI chat's in-progress files (T367 i).
- T372 IN PROGRESS (uncommitted): owner 11-04 "trout way for both" for the map too — the undrawn place no longer paints
  `check` (salmon.py chain_calendar, live.compose, core composeChain; Python tests updated); next the caveat reuse.
## 2026-11-03 (chat 4): batch 2 emulator sitting [T368, T354, T360, T345, T352]
- **T368 (new, in 0.5.0, batch 6c):** the app-wide failures/fallbacks/reporting sweep, split from T333. Two found
  today and fixed on the way: boot's `.catch(() => null)` on the manifest (it hid a 404) and the source-line
  rule-book links dropping their boxes.
- **T354 on the emulator:** the rule book opens in the app at the cited page, back closes it, the header now clears
  the status bar. The highlight was missing for two reasons, both fixed: source-line links passed no box (app bug;
  3042 of 3401 released rules do carry one), and Android's pdfium draws no highlight without an appearance stream
  (now appended with it; checked in pdfium on the real book). Open takes ~10 s on the emulator (a copy of 11.7 MB):
  to measure on a phone. Emulator re-check of the highlight is next.
- **T360 measured** (emulator, Vulkan): boot 255 MB, Conditions peaks at ~392 MB before GPU memory (the phone
  crashed at 406/435 MB); the rule book now costs ~40 MB. Table in the ticket. Next: the OpenGL variant.
- **T345:** the tidal "Flows into" is plain text now (fixes the fake Fraser stretches). REPRODUCED the sliver: picking
  the lowest part from "Show all 3 parts" lights/frames only 440 m below Lougheed; stepping down to it is correct. An
  agent is on the cause. Your Coquitlam crashes are in the store (3 × Java CRASH on 0.4.0, no stack).
- **T352 done in code:** crash/2 + report/2 (water id + pane, random install id, background kills, module id kept,
  local env dropped, problem report lists its map facts before Send); no section id (AGENTS rule 5). Worker deploy
  goes with the 0.5.0 APK (batch 7).
- Test bed: `session.sh --local` runs the app against this machine's release; new scenarios `memory`, `water-nav`.
- **Later in chat 4:** T345 map bugs FIXED (native feature-state queue, web highlight wait, reload colour hold) and
  the water map draws the same by every way in. T369 links carry a place (stream key + distance), web-only `lines`
  file — rebuild planned with batch 3. Web `getSource` crash on opening regs FIXED; a lazy web part that fails is now
  shown + reported. T360: rule book ~0.5 s in-app; viewer keeps ~35–40 MB after close; Conditions +130–150 MB native.
  New tickets: T369 (one load path), T370 (water slices repeat 0.7 MB), T371 (0.5.0 to Play Closed testing).
  **Staging feeds worker DEPLOYED** (owner's call, fa62c851): crash/2 + report/2 stored; live DFO feed is dfo-feed/2,
  so 0.4.0 shows the release's salmon answers until 0.5.0.
- NEXT: two chats — batch 2 (`NEXT-SESSION-BATCH2.md`) and the UI loop (`NEXT-SESSION-UI.md`).
- Live-release conformance pins (reads/regs.release, 33) are from the previous release; refresh at batch 7.

## 2026-11-03 (chat 3): batch 1 done; rule book opens in the app [T322, T337, T354, T367, T333]
- T322 DONE locally: gate PASS, DFO re-match no change (T337 check clean), labels refreshed. LATEST = r-4df5bccc9360.
  Staging upload waits for 0.5.0 (SALMON_NEEDS).
- T354 IN REVIEW: shipped book 25 -> 11.7 MB (owner approved the photos). Opens IN THE APP (react-native-pdf) at the
  cited page, highlight appended in place (v2 index). No fallbacks; shown failures reported (`reportFailure`). Needs a
  dev build on the emulator (batch 2 sitting) + test bed pdf scenarios updated.
- Owner rules saved: better package over minimal deps; no silent fallbacks/callbacks, failures reported (app-wide sweep
  is T333, audit phase).
- T367 filed: UI text clean-up loop with the owner (regs, conditions map, More), batch 6b, dev web app first.
- NEXT: batch 2 emulator sitting (T360 measure -> T351, T354 dev build check, T345, T352).

## 2026-11-03 (chat 2, later): slough release re-running; PDF / DFO / basemap answered [T322, T354, T366]
- **Slough release (T322):** the first side build stopped at its gate. One Okanagan River Oxbow got folded into Park
  Rill, which would have spread the oxbows' spring-closure exemption over the rill. Your call: it stays its own water.
  Fixed in code (efe94fb); the side build is re-running (`data/generated/side/t322b`). Then gate, promote, live build,
  all local. Resume steps are in the T322 ticket.
- **Your answers, now in tickets:** PDF = append to the file + compress its photos (T354); DFO = you review places
  only, rule changes go to a digest, urgent on an unreadable row or unmatched location (T366; T361's six marks stand);
  forest basemap was already simplified (T280); legend count bug = in-view count not refreshing on pan/zoom (T348).
- **Outdated notes cleared** across tickets and your to-do list (b1accdef0).
- **Next:** T354 code while the build runs, then batch 2 on the emulator (T360 memory, OpenGL variant).

## 2026-11-03 (chat 2): plan to 0.5.0 approved; T361 explained; slough release running [T322, T361, T326]

- **Plan:** `handoff/PLAN/RELEASE-PLAN-0.5.0.md`. Your calls: the salmon clean-up goes in before 0.5.0 (one format
  change); 0.5.0 is Android only and iOS follows as its own TestFlight release; duplicate tickets folded. Four new iOS
  tickets (T362–T365) plus T88 re-scoped to "TestFlight".
- **Six DFO marks (T361):** not something to undo. DFO's current page has in-season notices that changed the coho
  dates and limits on Campbell, Quinsam, Nanaimo, Puntledge, Qualicum and Stamp; your 11-02 marks were made on older
  rows the review app still showed. Ruling: the marks stand; rule changes go to the digest (T366).
- **Tidal/split proposals (T326):** Sandy Pool is already in; the other three (Quatse weir, Stamp lagoon outlet,
  Little Qualicum Falls) wait for you to accept them in the DFO tab.
- **Audits (T333–T335):** each ticket now says the scope is agreed with you first.
- **Running:** the slough fold side build (T322). Then its gate and a local promote; staging waits for 0.5.0.

## 2026-11-03 (end of chat): tidal follow-ups in the review app; app memory looked at [T359, T360, T361]

- **Tidal tab → Follow-ups (T359, done):** the 8 held tidal marks now show there, each with why it is held and your
  last note. A new button, "the whole water is tidal", writes a `whole` row. Annacis, Cannery, Morey and Parsons
  already have your own `whole` rows from 10-29, which is why they looked done; one "whole water" re-mark confirms each.
  **Your call:** should a DEM point you confirmed become a cut (Caycuse 1,925 m, Dempster 550 m)? Skagit (US border)
  and Bear River (the bridge) also need a look.
- **App memory (T360, partly done):** the code read is checked. The map's Vulkan renderer was only a provisional pick
  and was never tried on a phone, and one crash was a segfault, so OpenGL is the first thing to try. The rule-book PDF
  is read whole (24 MB) and parsed on every highlighted open, which is also why it opens slowly (T354). Correction: the
  map's "ambient cache" limit is disk, not memory. Next: measure on the emulator, then put the PDF options to you.
- **Red test (T361):** six region-1 DFO marks of yours (Quinsam, Campbell, Nanaimo, Puntledge, Qualicum, Stamp) read
  "rule changed" after Claude re-queued region 1 for the Nanaimo places. Your marks are untouched; T361 finds what
  moved and puts it back, or asks you to re-review if DFO really changed.

## 2026-11-03 (afternoon): your marks saved, Sheldens cut proposed, loose files committed

- **Your three DFO marks** (Little Qualicum Falls, Sproat, Stamp Deer Creek) are now Place n, "split missing", with
  your original fixes and notes. They were saved through the review app, so they wrote through like your own saves.
- **Sheldens Creek:** the Spur 19 bridge cut (3,402 m up the line) is a proposed split. Accept it in the review app;
  it reaches the map with the next atlas build.
- **DFO queue:** the two Fraser rows staging holds are already placed locally (the T349 entries). Staging shows them
  as held only because its inputs wait for app 0.5.0. Two Nanaimo River "tidal portions" places are back in the
  DFO tab for review.
- **Tidal follow-ups (T359, in progress):** the Tidal tab is getting a Follow-ups filter. Two of the 8 you did confirm
  (Caycuse, Dempster): their DEM points fit none of the writer's point kinds, which is a gap in the writer, not on
  your side. Four say "this entire channel is tidal" and will get a "whole water" answer.
- **App memory (T360):** a code read points at MapLibre on Vulkan with no tile-cache limit, the 24 MB PDF parsed in
  memory on every rule-book tap, and release data kept in memory for good. A phone measurement comes next.
- **Committed:** the T202 audit, the T215 shot, binding-diff-amb.json, the Play tools and store graphics (T244), and
  the pngjs dev dependency; knip is clean again.
- **0.4.0:** no more phone checks on it; testing resumes on the next release.

## 2026-11-03 (later): housekeeping — ledger fixed, old test failures fixed, old worktrees gone

- **Ticket ledger:** fixed the status lines on T52, T183, T184, T322, T324, T327, T331, T332, T346, T349 and T355.
  T162 is merged into T199. T66 is done: every phase of its plan has its own ticket. T350 is done: the in-depth pass
  belongs to the T333 audit. T239 (the yearly synopsis parse) is now in OWNER-TODO. The ledger checker passes.
- **Old test failures fixed** (only the failing tests were rerun): 5 Python guards and 2 app tests.
  - The review app no longer imports the atlas build module: its two FWA readers moved to `pipeline/atlas/fwa.py`.
  - `proposals.anchor_digest` is renamed `proposal_digest`; the guarded `anchor_digest` is a different digest.
  - `region_cascade` reads watershed codes through `wsc.py`.
  - The split-preview test uses the `needs_*` markers instead of a skip.
  - Two out-of-date tests were updated: one was missing the `{slug}` route parameter, and SpotScreen still expected
    the old "they can be filled in now" text.
- **Worktrees:** 6 removed and 17 old branches deleted. Each one was checked first: its work was already merged, or
  the files it touched no longer exist.
- **From the digest:** no new crash type; one more Android background low-memory kill, which is not a crash. Two new
  DFO Fraser rows (region 2, first seen today) are held as unplaced: the page's wording drifted ("upstream of the CPR
  Bridge at Mission", and chum 2/day Nov 3–30 under FN1179).

## 2026-11-03: tidal is one file; the map and the data match again [T326, T349, T350]

- **What changed:** every tidal answer now lives in one file, `data/curated/waters/tidal.json`, one row per water: at
  the `mouth` (1,126), on an existing cut (`split`, 33), a new `cut` (none yet), or the `whole` water (10 Fraser delta
  channels). The old copies are gone: `tidal_waters.json`, the `tidal` flags in splits.json, and the handoff
  owner-decisions file.
- **Saving a tidal mark** in the review app now updates that file straight away, like sloughs and names.
- **Mouth waters** get "tidal boundary" as another name for their mouth. It adds no cuts and no new sections, and a
  rule can bind it.
- **Build:** one atlas build with T349 + T326 was checked (0 new sections, +1,126 mouth names, no rule changed what it
  covers), promoted, then the live build ran. New local release **r-d0ad76a6e6a1** (salmon/3). Tiles and bundle are
  the same vintage again. Nothing was uploaded: staging waits for app 0.5.0.
- **Merged:** the lean salmon audit (`audit/t350-lean`); its tests pass.
- **Tests:** Python 6,319 pass. 6 fail, all from earlier work (T355 split preview, region cascade; listed in
  NEXT-SESSION). App 8,275 pass. 13 fail: the ticket-ledger guard (old ledger gaps) and one SpotScreen "week before"
  test.
- **For you:** your review app on :8787 had stopped before the checks, so start it again with
  `bash curation-review/run.sh`. Claude checked a copy on :8788: the tidal, DFO and cascade endpoints all return 200.
  The 8 open tidal marks and Mamquam are still yours.

## 2026-11-02: crash and problem reports reviewed [T346]

- **The app has sent 26 reports to staging:** 25 crashes (7 kinds) and 1 problem report. Test ones are ignored.
- **Your Coquitlam crash is in there, and it is not only Coquitlam.** 19 reports, versions 0.1.2 to 0.3.0 on web: opening a
  linked water ("flows into") asks for a piece of code that is not in the web build. Added to [T345]. Not seen on 0.4.0.
- **Two crashes on your Android phone (0.4.0)** with no detail to go on: [T351]. Three more reports are just Android
  closing the app in the background, not crashes.
- **Your problem report** (map colours stay the same when you zoom or scroll, only changing the forecast redraws them) is
  added to [T348].
- **The reports are missing the facts that would find causes** (which water, who sent it, readable stack): [T352].

---

## 2026-10-31, 07:00: stopped at a safe point

- **Config tidy-up merged** [T327, T331]. Full fast suite after the merge: 6141 passed, 0 failed.
- **Old releases pruned:** kept r-4e709418ebd8 (live on staging) and r-038e6818e80f (previous); 26 deleted.
  - Prod has NO release yet, so 0.3.0/0.4.0 testers read staging.
- **Not started:**
  - Slough fold release [T322]: still off, no side build.
  - DFO trout-shape merge [T332]: not merged.
  - Both have exact RESUME commands in their tickets. Nothing is half-done.
- **iOS final run done:** 6 scenarios pass, including performance. The tap-water, offline and satellite failures cascade from the swipe-back step leaving a water page open, so they are test-script, not app [T313 has the next steps].
- **Three staged files that aren't Claude's** sit uncommitted in the main checkout: a T215 screenshot, binding-diff-amb.json, and the T202 audit. Yours to keep or drop.

## 2026-10-31, 06:30: credits low, wrapping up safely

- Both running lanes are told to finish only their current step, commit, and write a RESUME section in their ticket. Nothing is left half-merged or half-promoted.
  - **Merge and slough lane** [T322]: may stop before the slough fold is promoted. Check the T322 ticket's RESUME section.
  - **iOS** [T313]: finishes the run already going, then stops.
- **Found:** DFO water links are refreshed by hand, never by the build [T337]. The slough release re-runs the matcher first and stops if any DFO link changes.
- **Next credit cycle:** Fable audit [T333], tests and docs audit [T334], `archive/` removal and cleanup [T335], DFO links in the build [T337].
- **For you:**
  - Accept the Sheldens Spur 19 cut.
  - Prod publish of r-4e709418ebd8 (your command, in the 04:00 entry).
  - Phone checks on 0.4.0: spot marks, and an offline spot filling in.

## 2026-10-31, 06:00

- **Python tests are green again.** Fast: 6136 passed. Slow: 272 passed, plus 1 skip that the rules allow.
  - None of the failures was an app bug:
    - `decided/` hadn't been re-exported after the Hatzic mark edit.
    - Two DFO tests needed a real unbound water and none are left; they now build their own.
    - New code tripped three code checks; it now complies, or is classified with a reason.
    - One test fixture was stale after the new atlas, and one browser test was flaky.
  - The spot banner now reads "they'll fill in once published".
- **Now running, in this order:**
  1. Merge the config tidy-up.
  2. Run the slough fold through its side build. The automatic check must pass before it's promoted, then it goes to staging.
  3. Merge the DFO-to-trout data shape, held out of every release until the next APK. App 0.4.0 can't read the new salmon format, so a guard stops it reaching testers early.
- **iOS** final run and performance numbers: in progress.

## 2026-10-31, 04:00

- **APK 0.4.0 is on Play internal testing** (versionCode 7). Staging is on release r-4e709418ebd8; smoke passes 18/18.
  - **Rebuild:** the only change is the 08MH044 gauge fix. The Fraser cut moved from Whonnock to Albion, with one more section on Bedford Channel and one fewer Fraser tidal section.
  - Bedford Channel lost its "fair" gauge, Fraser at Red Pass. That's a station about 900 km upstream, so losing it is likely right.
  - The cut still carries the old HYDAT name, "Whonock".
  - **Android emulator checks:**
    - Offline "Save spot" takes 1.9 s. The 15 s was a test-timer mistake.
    - Spots fill themselves in on reconnect (15/15), and gauges work offline.
    - Pan jank is the same with or without spots; the old 5 % figure was the outlier.
    - Walkthrough 25/25, cold start under 4 s.
    - Memory peak 289–325 MB (was 345–390). Pan 43 fps (was 32–35).
  - **Caveat:** the APK lane shipped with 7 fast and 2 slow Python tests still failing. It judged them other lanes' in-progress work, outside the app. Claude is checking that call.

## 2026-10-30, 23:45

- **DFO limits now use the trout data shape** [T332]: built on branch `dfo/t332-shared-quota`; it merges after 0.4.0.
  - Salmon quotas are now the same quota tree the trout card uses. Size limits are the same size bands, and the app draws them with the trout display code. The salmon-only shape is deleted.
  - Added one shared field, `period` (monthly or annual). The tree had no way to say "per month".
  - Across all 329 open DFO limits, the wording shown is unchanged. Your 5 spot-checks read the same.
  - **Wording list:** the sentence-split bug is fixed, which leaves 78 wordings (the scraps are gone). 77 are checked against real DFO cells and accepted.
    - 1 is left for later: an old Fort Babine part-closure ("100 m above the bridge to Nichyeskwa Creek"). The code reads it as the whole river closed. It only appears on archived pages and never shows a number.
  - **Release order:** the release file and live feed get new versions. Old apps refuse them rather than misread them, so the release after 0.4.0 must set the minimum app version first, and only then the worker deploy.

## 2026-10-30, 23:30

- **iOS** [T313]: paused until the APK ships, then one final run that includes performance.
  - On the iPhone simulator, 5 of 8 test scenarios pass: first open (70 s to the map), map, satellite, location and the rule-book PDF.
  - Fixed iOS bugs (none affect Android, so they ride 0.4.0):
    - map data never downloading;
    - the "‹ Back" button under the clock;
    - the PDF opening at its cover;
    - permission prompts saying "CanIFish".
  - Open:
    - The walkthrough card at step 17 sits over the Layers sheet where VoiceOver can't reach it [T336].
    - One swipe-back gesture didn't go back; it's retried next run.
- **DFO marks:** the dry run found nothing waiting; every DFO extent mark is already in the curated files.
- **Audits and cleanup, next credit cycle:** Fable audit [T333], tests and docs audit [T334], remove `archive/` plus general cleanup [T335].

## 2026-10-30, 23:00

- **DFO limits move to the same data shape as trout** [T332]. Started on its own branch; it ships after 0.4.0.
  - The DFO wording list gets tidied: the 2 split scraps are fixed.
  - All 85 wordings are checked against real DFO cells, then accepted.
  - Salmon limits will use the same quota tree and size-range types as the game-fish rules, shown by the same display code.
  - The salmon-only shape built in T320 gets removed.
- **Big Fable audit** [T333]: planned for the next credit cycle. It hunts duplicate data types, duplicate paths and bad practices, checks each finding, ranks them, and turns them into cleanup tickets.
- **Stuck rebuild stopped** (owner OK). A bug in `heavy.sh` meant a stop freed the slot but didn't end the job; fixed. The rebuild is re-queued at 60% free memory.
- **DFO extents from now on:** saving an extent mark in the review app updates the DFO curated file a moment later. Nothing to run.

## 2026-10-30, 22:40

- **Hatzic:** the vessel speed rules do NOT include tributaries. Changed that one mark (owner-allowed). The catalogue
  already said so, so nothing will widen.
- **Rebuild was stalled for 3 hours.** It held the heavy-job slot from 19:21 while waiting for 70% free memory, and
  this Mac idles at about 67%. It has been told to re-queue at 60%, the level the last live build used.

## 2026-10-30 evening

### Finished today

**DFO salmon limits per species** [T320]
The app now shows each species' own daily limit under the region's overall salmon limit. For example:
"Up to 4 salmon a day in total · 8 in possession", then "Chinook: 4 a day · only 2 over 62 cm".
- Owner spot-checked 5 places; they read correctly.
- Regions 5A and 7: DFO's "no salmon fishing" header now wins over 2025 table rows that were left on the page.
- Live on the staging feed now. Reaches phones with the 0.4.0 rebuild and APK.

**DFO wording safety net** [T321]
Every DFO wording we have ever seen (85 phrasings across 75 saved copies of DFO's pages) is in a list,
`data/curated/dfo_salmon/wording.json`.
- If DFO uses a new phrasing (for example, a notice that changes a quota in new words):
  - the owner gets an URGENT email with the exact words, place and notice number;
  - that row shows "DFO changed the wording here — read their text", with DFO's own words and no number.
- Three conditions that were being silently dropped are now kept:
  - hours written in words ("one hour before sunrise…");
  - "in the aggregate";
  - "(see exception below)".
- The list is read from storage on every run. After editing it, run `deploy/staging/sync-inputs.sh` (or ask Claude); no redeploy is needed.

**Feeds worker deployed to staging**
It now carries the per-species limits, the wording alerts, the offline gauge history (`obs.json`) and the crash-report changes.

**Sheldens Creek release + map/rules agreement** [T306]
- Staging is on release r-038e6818e80f, and the staging smoke check passes 18/18.
- The map colours and the rules pane now use the same tidal logic. Before, 99 tidal sections showed tidal in the rules but were coloured as normal water.
- Three sloughs (Maria, Hogranch, Nicomen) now draw at the right zoom.

**Data flow documented** [T324]
`handoff/PLAN/data-flow.md` traces every data stream from its source to the app, lists the problems found, and sets out the agreed end state:
- one final curated file per data type;
- review marks flow into those files automatically.

**Cleanup done so far**
- Ponds review closed [T329]. A pond call only ever confirms "lake", so nothing needs it.
- 16 machine-made review files moved out of `handoff/` into `data/generated/review/` [T330].
- Slough files are now in the right places [T322]:
  - the owner's slough verdicts are in `data/curated/sloughs/`;
  - the 811 confirmed calls are in `data/curated/waters/slough_calls.json`.

### Running now

**Rebuild → staging → Android checks → APK 0.4.0 (Play internal testing)**
The lane stops and reports if any check fails.
1. Rebuild with the DFO changes and the 08MH044 gauge fix.
2. Compare against the live release. Only those changes are allowed.
3. Upload to staging, then run the smoke check.
4. Emulator checks:
   - offline gauges and spots;
   - panning with and without spots;
   - the 15 s offline "Save spot" [T323].
5. Full test suite, then the APK to Play internal testing.

**Sloughs join their streams** [T322]: CODE DONE, switched off until its own side build.
Of the 811 confirmed calls, the dry run against today's map gives:
- 749 join one of 236 named streams.
- 31 Okanagan oxbows become general streams in place.
- 29 are skipped because they're isolated.
- 1 is held: wbk 329708509 touches only unnamed lines and belongs to no registry water, and the registry has no way to mark that as a stream yet.
- 1 was already a stream.

An automatic check replaces a review. It stops the promote if any water other than these, or the creeks that now flow into them, changes its rules.

What the plan said for this item:
- Confirmed sloughs join the stream that flows through them, including unnamed streams and streams that only enter or only leave.
- The 31 Okanagan oxbows become general streams.
- Only polygons no stream line touches are skipped.
- After the rebuild above, this gets its own side build. Claude checks that only those waters change, then promotes it. There is no owner review step (owner's call).

**Reviews save straight into curated files** [T325]: DONE (1a403a926).
Saving a mark now updates the curated file in the same save:
- Sloughs and names: instantly.
- DFO and regulation extents: a moment later, in the background. The app shows "saving to curated…".
- Tidal: still uses its current path until it gets its own file [T326].
- If a save is refused, the mark is kept and an amber "Mark saved, NOT in the curated file" list says why. The next save retries.
- There is no lock step. An empty queue reads "done". Ponds shows "Closed — feeds nothing".
- Catch-up command if marks were made while the app was off: `python -m pipeline.regs.review.apply`.
- Heads-up: one of your entry decisions is waiting to apply on your next entry mark. Hatzic, with tributaries included, widens rules r2/r3 from 4 to 92 sections.

What the plan said for this item:
Saving a mark in the review app will update that data type's final curated file in the same save: tidal, sloughs, DFO extents, regulation extents, splits and names.
- There is no lock step and nothing to run.
- If a save is refused, the mark is kept and the app shows why.

**Config tidy-up** [T327, T331]: BUILT on branch `cleanup/t327-t331`, waiting to merge after the rebuild.
- Owner name decisions now live in `data/curated/waters/name_calls.json`, not in code. The registry is unchanged.
- One upload list feeds both the staging script and the GitHub job; the GitHub job now uploads `wording.json`.
- Scraped and parsed data moved out of curated into `data/source/`:
  - gauge water types → `data/source/bc_station_waterbody_type.json`;
  - run timing → `data/source/runtiming/timing.json`.
- One unused file deleted (`cross_references.json`).
- The 7 retired marks are closed. Delivery reruns only when a file it actually reads changes.
- Left for later: four test-only path overrides need a small design of their own.

What the plan said for this item:
This runs on a separate branch so the rebuild isn't disturbed.
- Name decisions move out of Python code into a curated file.
- Every curated file gets registered in the config.
- Old files with no reader get removed.
- Merged after the rebuild is promoted. Old releases get pruned to the live one plus the previous one.

**iOS** [T313]
- The app builds and loads on the iPhone simulator; the download fix worked.
- Map, satellite and location pass.
- Search, offline toggle, walkthrough and PDF failures are being sorted into test-script issues and real bugs.
- Performance timings get recorded next to Android's. iOS goes after the APK.

### Needs the owner

- **Sheldens Creek:** accept the suggested cut at the Spur 19 bridge (53.12968, -131.78224) in the review app. Until then the app shows it as "not yet mapped".
- **DFO wordings:** review the 85 in `data/curated/dfo_salmon/wording.json`. Mark each `accepted` or `rejected`, especially the 15 "notes" shown as DFO's words.
- **Tidal:** 7 open marks plus Mamquam. After that, tidal becomes one file.
- **Existing items:** see `handoff/PLAN/OWNER-TODO.md` (salmon extents to review, gestures hand-check).
