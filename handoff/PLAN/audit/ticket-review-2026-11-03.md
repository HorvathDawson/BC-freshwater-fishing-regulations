# Ticket review, 2026-11-03 (scratch, read-only pass; nothing fixed)

Inputs: tickets/README.md (363 ledger rows, T00-T361 plus T15b/T19b, T305/T306 appended out of order), every ticket
file, OWNER-TODO.md, NEXT-SESSION.md. Method: script-parsed ledger vs each file's `Status:` line; every commit sha in
every DONE README row checked with `git cat-file` (all present; the only "missing" hits were `r-...` release ids, not
commits); last-commit date of each open ticket file. 96 tickets are not DONE/DROPPED.

Legend: M = mechanical (fixable without a decision), O = needs the owner.

---------------------------------------------------------------------------------------------------------------------
## PART A - ledger problems

### Numbering / files
- Duplicate numbers or two files for one ticket: NONE now (363 rows = 363 files, 1:1; the T332 collision is gone).
- T305, T306 rows sit after T352 in the ledger (out of order). Cosmetic. M.

### README status vs ticket file `Status:`
- T97: README PARTIAL; file "DONE for phase 1 ... Umbrella open: phases 2-5 are T133-T136"; T133-T136 are all DONE -> close T97 DONE. M.
- T293: README PARTIAL; file head says "DONE on web ... emulator run owed with the next APK". Same fact, different word; pick PARTIAL in the file. M.
- T322: file says "fold OFF ... FOLD_CONFIRMED = False" (and line 71 "dfo/t332 NOT merged"); HEAD commit 9ee36a5f1 already set FOLD_CONFIRMED = True, and T332 merged 0c276aa10. Ticket text stale, status IN PROGRESS/PARTIAL still right. M.
- T192-T198 (7 tickets): identical Status text "PARTIAL - committed 1c4dd582, 435ce2d8; first real-release build, v1 redirects and owner steps left". Copy-paste; not per-ticket (T197 is owner-sends-outreach, T198 needs hosting T86). M.
- Status vocabulary: README allows OPEN/IN PROGRESS/PARTIAL/BLOCKED (owner)/POSTPONED/DONE/DROPPED only. "IN REVIEW" is used by T266, T295, T297, T315, T316, T325; "PARTIAL (in review...)" by T307, T308, T310. All of their code is in HEAD (ec2d60bdd, ea9cade2a, b1827b9d0, c9c18e5be, fd36533e3 are ancestors). Either add "IN REVIEW" to the vocabulary or map to PARTIAL. M (the T84 guard evidently does not check the vocabulary).

### DONE with commits not in git / open with no recent activity
- DONE rows with commits missing from git: NONE (all README DONE shas resolve).
- IN PROGRESS but effectively waiting, not worked:
  - T55: IN PROGRESS since 10-28; waits on the owner to confirm slough marks in the review app; file last touched 10-30. Should be BLOCKED (owner). M.
  - T244: IN PROGRESS 10-29; remaining items all owner/T268/T250. BLOCKED (owner) or PARTIAL. M.
  - T313: IN PROGRESS, last touched 10-31; real work remaining (one heavy run + T336). Fine, but "held behind APK 0.4.0 chain" is stale (0.4.0 shipped). M.
- PARTIAL with no touch since <= 10-28 (file date, README may be fresher): T41 (10-23), T75 (10-27), T80 (10-28), T125 (10-28), T252 (10-28), T192-T198 (10-28), T188 (10-24), T87 (10-28). OPEN with no touch since 10-22/23: T65, T77, T88, T132, T178 (due ~12-05, fine).
- T41 PARTIAL: its leftovers moved to T82 (DONE) and T50 (OPEN); nothing of its own remains -> close. M.
- T125 PARTIAL: only item 8 left, "Q118, owner-run promote" - owner-run, and not in OWNER-TODO (see below). O.

### Same work in two (or more) tickets
- T132 (curation review: salmon pass) vs T164 (README: "umbrella over T95/T132"): T164 carries the DFO review mode; T132 is a bare OPEN stub. Fold T132 into T164. M.
- T55 vs T322 vs T328: T55's "Next" (confirmed streams -> registry fold -> reach+deliver as side build) is T322's whole body; T328 locks after T322. T55 should shrink to "owner confirms marks". M.
- T88 (iOS build: CocoaPods, simulator build, parity walk, edge-back) vs T313 (iOS simulator build + tests, already built and 5/8 scenarios passing, edge-back included). T88 is now redundant as written; its real remaining meaning is "ship to TestFlight/App Store". Re-scope. M (the real-device/App Store timing is O).
- T312 (pan feel) vs T353 (fling still weak; text says "After T312's fling tuning" though T312 is OPEN) vs T273 (panning lag, PARTIAL). Overlapping gesture-feel tickets; T312's status needs to say what shipped in 0.4.0. M.
- T351 (native crash, 406/435 MB) vs T360 (memory footprint, "T351 crashes at 406/435 MB") vs T354 (PDF slow; T360 says "PDF part = T354"). Overlap by design; group them (Part B). M.
- T345 title/status say "crash (flows-into-Fraser -> map tap) + fake '2 stretches' + Lougheed highlight", but the file body only describes the highlight overlap. The crash cause is not scoped in the body (may be what T351/T352 are chasing). O/M: confirm whether the crash half is a separate item.
- T242 (crash reporting, PARTIAL), T291 (test tag, PARTIAL), T315 (gauge week, IN REVIEW) all wait on the same owner action: the feeds-worker deploy. NEXT-SESSION says do NOT deploy the worker until app 0.5.0. They are therefore all gated by the 0.5.0 decision. M to record; the deploy is O.

### Blocked on something already DONE (stale Depends/Status text)
- T343 "Depends on T332 ... do on top of it": T332 is DONE (0c276aa10). Unblocked. M.
- T356, T358: depend on T350 (DONE 11-03). Unblocked. M.
- T313 depends T217 (DONE 10-27). T88 depends T74 (DONE) but also T50 (OPEN, "runs after the mass cleanup T61-T63", which is DONE). M.
- T50 text "Runs after the mass cleanup (T61-T63)"; T79 "after the cleanup waves (T61-T63)": cleanups DONE. M.
- T75 depends T64 + T85 (both DONE); its only gap is a phone check (T77). M.
- T87 depends T63, T82 (DONE); only T77 (owner phone) remains. M.
- T164 depends T97-T136, T154 (all DONE). T218 depends T158 (DONE). T259 depends T85/T202 (DONE). T108 etc. n/a. M.
- T325 IN REVIEW, but write-through is live (the 11-03 `T185 apply mark-dfo-...` commits are written through; T326, T327, T329 depend on it and are DONE/PARTIAL). Looks closable. M (verify then DONE).

### Owner-only items missing from OWNER-TODO.md
OWNER-TODO does not name these tickets:
- T242 / T291 / T315: feeds-worker deploy (and T242's open "Sentry vs own endpoint (default own)" call). OWNER-TODO only mentions the worker deploy inside the T332 line. O (needs adding; note it is held until 0.5.0).
- T301: new HYDAT release, prod upload (generic `update-hydat.sh` line exists, ticket not named). M.
- T125 item 8 (gauges 9 / Q118 owner-run promote). O.
- T312 / T353: "feel check on the phone". T307 / T308 / T310 / T316: phone/emulator checks and shots pending. T261: native device run + owner look. O.
- T260: Q on item 2 (look decision) - the Q is in the ticket only. O.
- T313 / T88: Apple Developer account ($99), needed before TestFlight; only "iOS build (T88)" is listed generically. O.
- T339: owner "say if you want any of that different" is in the 0.4.0 bullet but with no ticket id (T339 now exists). M.
- T361: Claude's own red test; correctly not an owner item.

### OWNER-TODO.md itself is stale (lines for 0.4.0)
- Top item still gives the prod publish command for `r-4e709418ebd8`; a newer release r-d0ad76a6e6a1 exists (T326/T349, built 11-03). Its "Still red" test list (7 fast / 2 slow) predates T306/T361. O (which release goes to prod).

### NEXT-SESSION.md stale entries
- "Where things stand (2026-10-31)": staging `r-4e709418ebd8`, tests 6141/272, "Branch not yet merged: dfo/t332-shared-quota". T332 merged 11-02 (0c276aa10); release now r-d0ad76a6e6a1. M.
- Queue 1: "T332 | Merge dfo/t332-shared-quota" - already DONE. The part that remains is only "ships with next APK, MIN_APP_VERSION = that app, SALMON_NEEDS in upload.sh". `SALMON_NEEDS = {3: "0.5.0"}` already exists at deploy/staging/upload.sh:22, so "Add the SALMON_NEEDS guard" is also done. M.
- Queue 1: "T322 ... `FOLD_CONFIRMED = True`" first step: HEAD 9ee36a5f1 did that. M.
- Queue 2: "T346: go through every crash ... (do with T345)" - T346 DONE 11-02 (6c08121b3); its output is T351/T352. M.
- Queue 2: "T343 (after T332)" - T332 is done. M.
- Queue 4: "Prod publish of r-4e709418ebd8" - superseded (see above). "T326: tidal lock needs 7 marks plus Mamquam" is consistent with the T326 ticket ("Left: the 8"). Fine.
- Not in any queue section: T351, T352, T353, T354 (only via T360), T356, T357, T358, T312, T317, T304, T305, T306, T301, T315/T316/T310 verification, T324 remainder, T328. T306 (8 slow failures on live data) and T361 are the only red-test items; T361 is in FINISH FIRST, T306 is not. M.
- "Queue 3 ... T333/T334/T335 run as one workflow" matches the tickets. Fine.

---------------------------------------------------------------------------------------------------------------------
## PART B - every ticket NOT DONE (96)

Area codes: PIPE pipeline/data - DFO DFO salmon - REV review app - AND Android app - IOS iOS app - MAP map/tiles -
REL release/deploy - AUD audit/cleanup - DOC docs. Size from the ticket (S/M/L); "?" = ticket gives none (my guess in brackets).

| ticket | what | status | size | area | depends on |
|---|---|---|---|---|---|
| T41 | web parity walk (leftovers all moved) | PARTIAL | S | DOC | T82 done, T50 |
| T50 | Android emulator parity + system bars | OPEN | L | AND | T61-63 done |
| T55 | sloughs drawn as lakes; owner confirms marks in review app | IN PROGRESS | M | REV/PIPE | owner |
| T65 | determinism proof (two builds same inputs) | OPEN | L (heavy) | AUD/PIPE | - |
| T75 | basemap in manifest; offline native launch draws ground | PARTIAL | M | MAP | T77 phone |
| T77 | D1 phone testing | OPEN | M | AND | T50, owner |
| T79 | imagery corridor + pools validation | POSTPONED | M | MAP | owner parked |
| T80 | small open items (S1 minors, Kitimat, D19, search, CI) | PARTIAL | M | PIPE | none |
| T86 | production hosting + ship config | POSTPONED | M | REL | T72; owner |
| T87 | owner sign-off walk | PARTIAL | M | DOC | T77 |
| T88 | iOS build (CocoaPods, simulator, parity) | OPEN | L | IOS | T50, overlaps T313 |
| T97 | review phase 1 umbrella (phases 2-5 done) | PARTIAL | M | REV | - (close) |
| T125 | latent duplicate checks; item 8 gauges 9 | PARTIAL | M | PIPE | owner promote |
| T132 | curation review: salmon pass | OPEN | M | DFO/REV | fold into T164 |
| T164 | DFO dossier: 96+7 splits, extents, quotas | OPEN | L | DFO/REV | owner review |
| T178 | re-score CLEVER vs COFFEE from archive (~2026-12-05) | OPEN | S | PIPE | time |
| T188 | SEO plan umbrella (build = T192-T198) | PARTIAL | M | REL | T86 |
| T192 | SEO slug map + page model | PARTIAL | S | REL | - |
| T193 | SEO static water pages + About | PARTIAL | M | REL | T192 |
| T194 | SEO sitemap, robots, JSON-LD, gates | PARTIAL | M | REL | T193 |
| T195 | SEO region hubs, weekend pages, share images | PARTIAL | M | REL | T193 |
| T196 | SEO page perf budget + Lighthouse CI | PARTIAL | S | REL | T193 |
| T197 | SEO outreach drafts (owner sends) | PARTIAL | S | REL | T193 live |
| T198 | SEO .com->.ca 301, HSTS, Search Console | PARTIAL | S | REL | T86, T194, owner DNS |
| T199 | run timing: feed + mode built; data refinement | PARTIAL | M? | PIPE/AND | - |
| T200 | stocked lakes: feed Stocked mode | OPEN | M? | PIPE/MAP | - |
| T201 | bathymetry: feed Depth mode, contours | OPEN | M? | PIPE/MAP | - |
| T218 | bathymetry: 28 digitised lakes | OPEN (not yet, owner) | S | PIPE/MAP | T201 |
| T219 | bathymetry: trace contours from scans | OPEN (not yet, owner) | L | PIPE/MAP | T218, T200 |
| T223 | user accounts web+phone | OPEN (far future) | L | AND/IOS/REL | T86, T88, owner Qs |
| T225 | feeds hosting/triggers/alerting (staging on) | OPEN | M | REL | prod keys owner |
| T234 | push notifications | OPEN | L | AND/IOS | T225, T230, T231, T223 |
| T239 | annual synopsis update runbook + diff | OPEN | L | PIPE/DOC | - |
| T240 | production launch checklist | OPEN | S+owner | REL | T86, T198, T225, T232, T241, T249 |
| T242 | crash + error reporting | PARTIAL | S | AND/REL | worker deploy (owner), T225, T249 |
| T244 | app-store readiness | IN PROGRESS | M | AND/IOS/REL | T232, T240, T242, T250 |
| T245 | low-end Android perf budgets | PARTIAL | M | AND | T50/T74 |
| T250 | Expo SDK / dependency upgrade window | OPEN | M | AND/IOS | before T244 |
| T252 | donor paths through lakes; distance along river | PARTIAL | ? (M) | PIPE | next bundle+release |
| T258 | accessibility leftovers (owner TalkBack walk) | PARTIAL | S-M | AND/IOS | T257 |
| T259 | offline map by region | OPEN (deferred) | M | MAP/AND | T85/T202 done |
| T260 | shared components part 2 | PARTIAL | M | AND | T257, owner Q item 2 |
| T261 | run timing v2 (Runs tab, pentad slider) | PARTIAL | ? (M) | AND | device run, owner look |
| T266 | marks are review state; one exporter | IN REVIEW | M | PIPE/REV | owner: confirm split |
| T273 | panning lag on Android; cause = basemap tiles | PARTIAL | M | AND/MAP | owner choice |
| T281 | draw lake area an in-part rule holds on | POSTPONED | M | MAP | lake splits (deferred) |
| T287 | no freeze when a download finishes | PARTIAL | M | AND | T272, T273 |
| T291 | test-tag crash reports | PARTIAL | S | REL | worker deploy (owner) |
| T292 | sewage/tailing/dump features: colour + tap | PARTIAL | S-M | MAP | tiles export rerun |
| T293 | hands-on walkthrough (web done; emulator owed) | PARTIAL | M | AND | next APK |
| T295 | faster full rebuild | IN REVIEW | M | PIPE | - |
| T296 | gauge through a lake down-weighted | PARTIAL | S-M | PIPE | T252, next bundle |
| T297 | build clean-up (T266+T295 pass) | IN REVIEW | L | PIPE/AUD | T266, T295 |
| T301 | new HYDAT release refresh | OPEN | S-M | PIPE | T274 |
| T304 | water-page landing focus region (Fast-3G 37 s) | OPEN | S | REL/MAP | T256, T269, T282 (web) |
| T305 | tile_version non-deterministic (tippecanoe temp path) | OPEN | S | MAP | - |
| T306 | slow suite: 8 failures on live data | OPEN | M | AUD | - |
| T307 | offline spot add fails fast | PARTIAL (in review) | S | AND | T294 done; emulator check |
| T308 | location dot keeps updating | PARTIAL (in review) | S | AND | T303; emulator check |
| T310 | spots: parking + directions; visits own notes/photos | PARTIAL (in review) | M | AND | phone shots |
| T312 | map pan feel: 1:1 tracking, fling | OPEN | S-M | AND/MAP | T273, T280; owner feel check |
| T313 | iOS simulator build + basic tests | IN PROGRESS | M-L | IOS | T217 done, T88 |
| T315 | offline: last week of gauge readings | IN REVIEW | S-M | AND/REL | worker deploy (owner) |
| T316 | spot/parking pins as engine layers | IN REVIEW | S-M | AND/MAP | emulator pan numbers |
| T317 | audit: all overlays -> map engine; hot-path once-over | OPEN | M | AUD/AND | T316 |
| T319 | store screenshots, one command | POSTPONED | M | REL | T240, T244, T313 |
| T322 | fold 811 confirmed sloughs into streams (side build) | PARTIAL | M-L | PIPE | T55, T266 (fold flag now True in HEAD) |
| T324 | one clean data flow (plan done) | PARTIAL | M | PIPE/DOC | T266, T322 |
| T325 | write-through of marks | IN REVIEW (looks live) | M | REV/PIPE | - |
| T326 | lock tidal; 8 held marks | PARTIAL | S-M | PIPE/REV | owner re-review (T359 view) |
| T328 | lock sloughs | OPEN | S | PIPE | T322, T325 |
| T331 | curated config hygiene (items 2, 8 left) | PARTIAL | M | AUD | - |
| T333 | big Fable audit | POSTPONED | L | AUD | T324, T331; owner go |
| T334 | audit tests + docs | POSTPONED | M-L | AUD/DOC | T333 |
| T335 | remove archive/, general cleanup | POSTPONED | M | AUD | with T333/T334 |
| T336 | iOS: walkthrough card unreachable over Layers sheet | OPEN | S | IOS | T313 |
| T337 | DFO bindings refresh inside the build | OPEN | S-M | DFO/PIPE | - |
| T338 | DFO part-closure (Fort Babine) reads whole river closed | OPEN | S | DFO | - |
| T339 | 08MH044 cut label "Whonock"; Bedford Channel no gauge | OPEN | S | PIPE | next --atlas rebuild |
| T340 | test bed stream taps fail | OPEN | S | AND (testbed) | - |
| T341 | release-perf A5 search p95 9 ms vs 8 ms | OPEN | S | AUD | - |
| T342 | pane text copyable natively; remove salmon Copy button | OPEN | S-M | AND/IOS | - |
| T343 | regional salmon rules cleanup | OPEN | M | DFO | T332 (done) |
| T344 | species ID: swipeable set for grouped species | OPEN | S-M | AND/IOS | - |
| T345 | BUG HIGH: Coquitlam pane crash + highlight past bridge | OPEN | S-M | AND/MAP | - |
| T347 | river forecast weekday-only staleness + More note | OPEN | S | AND/REL | - |
| T348 | BUG: map colours do not refresh on new feed | OPEN | S-M | AND/MAP | T347 |
| T351 | native crash foreground Android 37 / 0.4.0 | OPEN | M | AND | T346 done, T360 |
| T352 | crash reports lack context | OPEN | S-M | REL/AND | T346 done |
| T353 | map fling still too weak | OPEN | S | AND/MAP | T312 |
| T354 | synopsis PDF slow on mobile | OPEN | S-M | AND | T360 (memory) |
| T356 | salmon card: core salmon.ts logic -> Python | OPEN | M | DFO/AND | T350 done |
| T357 | DFO silent fallbacks (missing header notes, corrupt feed) | OPEN | S | DFO | T337 |
| T358 | DFO fields outside catalogue model | OPEN | M | DFO | T350 done |
| T360 | app memory footprint (measure RSS by screen) | PARTIAL | ? (M) | AND | T351 |
| T361 | six region-1 DFO marks read "rule changed" | OPEN | ? (S) | DFO/REV | - |

### B2 - proposed groups (do together in one batch)

1. **Crash + memory + PDF** - T351, T352, T360, T354 (T345's crash half if separate). Same root question (400+ MB
   RSS, SIGSEGV at 406/435 MB, PDF held in memory) and same lane (n1_pixel AVD, `dumpsys meminfo`). T360 measures,
   T354 and T351 act on its numbers, T352 makes the next crash readable (needs worker redeploy, so lands with T242/T291).
   Size M-L, one emulator session. Do first: it gates 0.5.0 stability.
2. **Feeds and the map/screens they paint** - T347 + T348 (+ T315 verification). One "feeds changed" signal, weekday
   staleness rule. Size S-M.
3. **Crash-report plumbing** - T352 + T242 + T291 + T315 (all wait on one owner worker deploy) + T225 prod switch.
   Size S-M, mostly an owner deploy; sequence after 0.5.0 per the SALMON_NEEDS hold.
4. **Map gesture feel and overlays** - T312 + T353 + T273 + T316 + T317 (+ T287 freeze, T245 low-end). One emulator
   pan/fling lane, one calibration pass, then the overlay audit. Size M-L. T312/T353 are a duplicate pair.
5. **DFO salmon code lean-up** - T356 + T357 + T358 + T337 + T338 + T343 + T361. T356-358 came from one audit
   (T350), T337/T357 are both "silent build fallbacks", T343 and T338 touch the same card/header notes, T361 is the
   red test. Several change the salmon payload shape (a salmon FORMAT bump): do BEFORE the 0.5.0 cut so salmon/3 is
   final, not salmon/4 later. Size L total (M+M+M+S+S+S+S).
6. **DFO review work (owner-driven)** - T164 (+ T132 folded in) + T326 remainder + T359 follow-ups. Mostly owner
   reviews; Claude side S. Independent of the app.
7. **Slough chain** - T55 -> T322 -> T328 -> T324 remainder; T266, T295, T297, T325 are the "IN REVIEW" ledger closers
   (T305 tile_version determinism rides with T297/T65). One side build, one binding-diff gate. Size M-L, heavy jobs.
8. **iOS test + a11y** - T313, T336, T342, T344, T258 (VoiceOver/TalkBack). Size M. See iOS list below.
9. **Spots/offline in-review set** - T307, T308, T310 (+ T314/T323 already done): same emulator verification pass; close
   together. Size S-M (verification only).
10. **SEO / hosting / launch** - T86, T188, T192-T198, T225, T240, T244, T319, T250 are all gated by T86 (POSTPONED by the
    owner). Keep parked as one block. T250 (Expo SDK upgrade) is the exception: do it before T244 and ideally before
    the iOS release (needs a phone walk).
11. **Pipeline hygiene** - T306, T341, T305, T340, T339, T301, T304, T252/T296 (rides next bundle), T125, T80, T331.
    Small, independent; batch as "red tests + determinism".
12. **Audit cycle (owner says go)** - T333 + T334 + T335 (+ T65). See audit list.
13. **Far future / data features** - T199/T261 run timing, T200/T201/T218/T219 stocked + bathymetry, T223 accounts,
    T234 push, T239 annual update, T259 offline regions, T79/T281 parked. No batch yet.

### B3 - proposed "0.5.0 app release" scope

Must be true because of the SALMON_NEEDS hold (`deploy/staging/upload.sh:22`, `{3: "0.5.0"}`): staging cannot get the
salmon/3 feed or the feeds-worker deploy until an app >= 0.5.0 exists, and 0.5.0 must contain the app side of T332/T349
(already merged, 0c276aa10).

MUST ship with 0.5.0
- T332/T349/T320/T321 app side (already in HEAD) - the reason for 0.5.0.
- Salmon payload final before cutting: T356 (core salmon.ts logic to Python), T358 (field model), T357 (fallbacks), T338
  (Fort Babine reader), T343 (regional rules), T361. Otherwise a second salmon FORMAT bump.
- T345 (HIGH, crash) and T351/T360 (memory/native crash) - a crash fix cannot wait for 0.6.
- T348 (map colours not refreshing) and T347 - owner-visible bugs from 0.4.0.
- Verification of what is already in HEAD and unreleased: T307, T308, T310, T315, T316 (emulator/phone checks), T293
  (emulator walkthrough), T292 (needs tiles export rerun), T322 slough fold release (data, not app).
- Crash plumbing T352 + T242/T291 (worker deploy comes after 0.5.0 on staging, per the guard).

NICE to have
- T354 (PDF), T353/T312 (fling), T342 (native copy), T344 (species swipe), T287 (download freeze), T304 (landing
  focus), T339/T340, T250 (SDK upgrade if it can be walked first).

SHOULD WAIT
- T223, T234, T259, T200/T201/T218/T219, T199/T261 v2 run timing (staging only), T319, T79, T281, SEO/prod block,
  T317 audit and T333-T335 (T317 optional if T316 pattern is simply reused).

### B4 - iOS release (TestFlight / App Store) "working well"

What the tickets cover today
- T313 (IN PROGRESS): simulator build works; first-open, map, location, pdf, perf PASS; tap-water, offline, satellite
  failed as a test-bed cascade after edge-back (not shown to be app bugs); walkthrough 16/25 -> T336. Needs one heavy
  re-run with each scenario starting from the map and edge-back settled (50 %/400 ms drag stayed, 30 %/900 ms went back:
  edge-back verdict unresolved - real-phone gesture risk).
- T336 (OPEN, S): walkthrough card over the Layers sheet not in the accessibility tree (VoiceOver cannot reach Back/Next/
  Skip; a left-at-step-17 walk reopens the sheet with the card unreachable). Acceptance: walkthrough 25/25.
- T88 (OPEN, L): superseded by T313 as written; real content now = signing/TestFlight.
- T217 (DONE): per-tile colouring event on iOS. T74 note: "lake modes wait on lake facts".
- T244 (IN PROGRESS): store listing and privacy label drafts; icon/accounts/submissions are the owner's.
- T250 (OPEN): Expo SDK upgrade, ahead of T244.
- T258: VoiceOver walk (owner). T342/T344: pane text copy and species swipe must work on iOS too.
- T319: store screenshots (POSTPONED). T223: Sign in with Apple if accounts ship. T234: push on iOS (APNs).

Must be true
1. T313 green on all 8 scenarios, then T336 fixed (25/25 walkthrough).
2. Edge-back (T88/T313 finding) verdict settled on a real phone.
3. Real-device run, not only the Simulator: location prompt, gauge pill padding (OWNER-TODO "iPhone (T217)"), perf (RSS
   420 MB, peak 665 MB while panning in the simulator is high given Android crashes at 406-435 MB: T360 must cover iOS).
4. Free-account 7-day sign is the stated follow-up; TestFlight needs the paid Apple account ($99, owner).
5. Store readiness (T244) incl. iOS privacy nutrition labels, icon, listing, screenshots (T319).

GAPS (no ticket)
- No ticket for iOS signing / provisioning / TestFlight upload pipeline (build-ios-sim.sh is simulator-only, no
  `build-ios.sh --archive`); T88 predates it.
- No ticket for the iOS crash/exit reporting path (exit-info is a no-op on iOS, so T242/T351/T352 give iOS no crash
  records; MetricKit or equivalent).
- No ticket for iOS memory measurement (T360 is Android-only) or for real-iPhone performance.
- No ticket for iOS offline download behaviour on a real device: the background-URLSession first-open failure (fixed in
  b8e33ca80) suggests a background-transfer/T298 iOS check is missing.
- No iOS location "Always"/background or permission review ticket (strings changed in app.json) beyond T313's note.
- No iOS accessibility/Dynamic Type ticket (T258 is mostly Android TalkBack).
- No Apple review compliance ticket (privacy manifest `PrivacyInfo.xcprivacy`, tracking declaration, account-deletion
  rule if T223 accounts ship, offline-only/web-content rules).
- Branding/product name: the Xcode product is "CanIFish" vs display "Can I Fish?" - no ticket for the final bundle id /
  app name (T86 hosting Q26 "app name" is the nearest).

### B5 - audit tickets

Scope text and what is unanswered:
- **T333** (POSTPONED, next credit cycle; L): whole-repo multi-agent Fable audit - duplicate data types, duplicate
  pathways, bad practices, drift; method = finders per area and lens, dedupe, >=2 adversarial skeptics, ranked report
  with file:line + proposed fix -> cleanup tickets, no fixes in the run. Unanswered: when the "next credit cycle" starts
  (owner says go); whether T350's in-depth DFO pass (promised to T333) is inside it - T350 says yes, T333 text does not
  list DFO explicitly; whether T65 (determinism proof), T306 and T317 (map overlay once-over) belong inside or stay
  separate; the model name `claude-fable-5-1` is baked in the text; T324 (map) is "PARTIAL" yet listed as a dependency,
  and T331 "merged" is only PARTIAL (items 2, 8 open).
- **T334** (POSTPONED; M-L): tests (cannot-fail tests, xfail leftovers, coverage gaps on rules->bindings->bundle->app,
  mutation checks) and docs (AGENTS/CLAUDE/READMEs/handoff/pipeline docs, stale ticket states). Unanswered: "one index
  of what is current" - where it lives; overlap with the ledger check NEXT-SESSION step 1 already runs per chat;
  interaction with T306 (8 live-data failures) and T341.
- **T335** (POSTPONED; M): delete `archive/` (211 files, 22 MB) after moving anything still read by live code; clean
  stray files (`app/tools/build-play-graphics.mjs`, `play-shots.mjs`, `store/graphics/` "after asking what they are"),
  old side builds, releases to live+previous (T331 item 8), dead scripts. Unanswered: the untracked Play-graphics files
  (owner has to say what they are); the OWNER-TODO "delete the old basemap" yes (T202) still pending; memory says
  cleanup is "delete freely", the ticket says ask first for those files.
- **T350** (DONE 11-03): lean DFO audit; findings T356-T358; in-depth pass deferred to T333.
- **T317** (OPEN, M): app-wide once-over for overlays drawn over the map and per-frame JS - an audit in the app only.
  Unanswered: nothing for the owner; depends on T316 emulator numbers.
- **T65** (OPEN, L heavy): determinism proof; sibling T305 (tile_version) and the memory "atlas build is not
  deterministic" (~50 blocks differ) - question: is T65 still wanted, or has T297's byte-identical proof side run
  already answered it for everything but tile_version?
- **T306** (OPEN, M): the slow suite's 8 failures; **T341** (OPEN, S) perf budget; **T331** (PARTIAL) config hygiene -
  audit-adjacent fixes, belong with the T333-T335 cycle's cleanup tickets.
- **T60** (consistency auditors) and T350 are DONE and form the existing auditor gates the T333 cycle should reuse.
