- **T401 measured (emulator):** drag, fling, and fling close in (Hoy Creek) — 0 % of the new edge missing once the
  camera rests (~0.65 s drag, ~1 s fling). Not reproduced; asked the owner for a screen recording of one pan.
- **Test bed green on the 0.5.0 drift:** gestures (route map is above the gauge list), water-nav (the tap used the small
  water map's probe; now the tallest map's) — T345's last path (close → reopen → Map) confirmed, no crash; T399's
  highlight on the river (r1105d).
# Where things stand

**Start the review app yourself:** `bash curation-review/run.sh`, then open http://127.0.0.1:5173. It's local only and
spends no credits; Ctrl-C stops it. If Claude's copy is still running on port 8787, either use Claude's copy at
http://127.0.0.1:5183, or run your own beside it with `PORT=8788 CURATION_VITE_PORT=5174 bash curation-review/run.sh`.

Plain summary for the owner, updated as work lands. Newest at the top. Ticket numbers are in brackets for lookup only.

---

## 2026-11-09 11:50 — session stopped ~02:10, resumed
- Nothing shipped overnight: no build 22, no Play upload, nothing promoted. Committed by 02:04: T524 (+ review fixes),
  T525 round 1, T522 step cache (release builds use --no-step-cache until its review fixes land), T360 memory (+ review
  fixes), T496 tool fallbacks, red-test fixes, Section E dead piece. Uncommitted, lanes resumed: T517 (verify + commit),
  T525 round 2, T522 cache fixes, T360 swap proof, 2 remaining red tests.
- Then: T517 verify → promote → T523 apply → rebuild (no step cache) → explainer → build 22 → emulator → Play internal.

## 2026-11-09 morning (owner back, review app closed) — owner asleep again 01:10, target 6:10am PDT
- Owner labelled 100 items; last 25 marks committed (4c45bfd). Chilliwack/Sumas two-piece places marked "unsure" (T524
  proposals will settle them); the Forks not marked. T523 re-simulating against the new marks before it applies.
- Owner: speed work (T522) goes INTO build 22. Heavy queue order: T517 bindings diff → T522 proof → T524 dry run / T360.
- T496 4 silent tool fallbacks: fix lane running.

## 2026-11-09 night (chat 7, owner asleep): the track

**Order (do not reorder):**
1. T517 fix agent finishes (round-1 + 2nd-review findings + fast-fail name checks; ONE rebuild; display.ends holds
   EVERY end token used by parts, in-season notices and boundary marks; marks named by their end token; no stored-label
   fallbacks; joined stretches checked on every piece). Verify: bindings diff (6 expected T510 sections), size flat,
   name_check 0 on the SHIPPED release, paint_sync, suites, shots (Fraser, Dean, Squamish). Then local promote.
2. Then T523 data change (prepared, simulated, all mark statuses unchanged): 4 cut-id renames (Quatse weir u100m/d100m,
   Stamp lagoon inlet/outlet), 2-token surgical marks.jsonl edit (owner-approved), Squamish "9 mile" powerline
   re-anchored on OSM 5L32 [-123.185859, 49.785773]. Script in the T523 lane's scratchpad (t523_rename.py --dry first).
3. T524 (running): DFO extent of several pieces (Chilliwack-Vedder + Sumas; the Forks Bulkley/Morice — FWA check which
   water runs below the Forks), proposals for each, tributary call defaults from "(including tributaries)" titles,
   a clear "your fix REPLACES the current extent" banner. Then T525 (audit + mockup running; implement after T524):
   DFO review screen easy + safe, current extent always visible, confirm keeps it, redo starts from it.
4. Explainer artifact update → build 22 (new icon, clean prebuild, MLN_LOCAL_MAVEN lean patch) → emulator check →
   feeds worker deploy with the app (T521) → Play INTERNAL only, if every check is clean.
5. Also open: T496 found 4 silent fallbacks in tools (consistency.ts:22, check-boundaries.mjs:101, audit_splits.py:48/80)
   → fix; T522 rebuild speed (proof run); T360 memory lane (running).

**Owner, when you wake:** restart the review app (picks up T523/T524/T525 + the slough path fix); hold DFO marking of
two-river places (Chilliwack/Sumas, the Forks) until T524 lands; re-upload the Play listing icon + feature graphic.

---

## 2026-11-08 evening (chat 7, continued): parts + in-season redesigned after two fresh audits

**What changed**
- Bait card reworked (T515) and "Handling fish" moved to the page bottom (T516); Fraser stretch names by their ends
  (T509). All committed and pushed; they ship with the next Play build.
- Strawberry Slough rule A for all 11 connectors committed (T510, c4ce4b8b4); lands with the T517 rebuild.
- **Parts (T517):** owner decision — one part per connected run; stretches split on regulation OR live DFO salmon
  changes and auto-combine when equal; one navigation mode; names computed once in Python; paint (regular + salmon)
  checked against the shipped files so it can never diverge; size flat. Design in the T517 ticket (ad3f8dc).
- **In-season (T520):** audit found the GitHub sync failing every run since 11-05 (needs the atlas). New design: the
  Mac publishes only (GitHub workflow deleted, review-app save syncs staging); notices keyed by their own text;
  links carry exact FWA ranges (no part numbers); checks in publish only; banners on exactly the covered sections (T518).
- T519: the Fraser's exit reads Morey Channel since T497; fixed in the T517 rebuild.

**Running**
- T517 side build (atlas → bundle → release, with T510 + T519); nothing promoted.
- T520 implementation (in-season links/2, GitHub workflow removal).
- When each lands: a fresh high-effort Opus review of the code + side build before anything is promoted (owner ask).

**T520 done** (ece44d1f3 … abe296584, live on staging): 32/32 notices same sections; high-effort review fixed
(stale lines on page change, day mismatch, digest check, per-input sync, flock, refusals, typography ids, duplicate
rows collapse). Old APK shows every notice "not on the map yet" until build 22 — owner OK (no old-format shim).

**Next, in order (owner 2026-11-08)**
1. T517 lands → fresh high-effort Opus review → fixes → owner gets the moved list / parity list / sizes / shots.
   - 11-09: T517 side build done (r-d62af06225a2; paint sync passes; 6 sections change rules, all T510; size +1.1%).
     Shots + moved list sent to owner. Opus review done: 5 major (paint-sync maximal check can't fail; joined piece opens map at wrong place;
     header vs map names differ; no format bump; lake gap = owner) + minors. Fix agent running, then side rebuild 14.
     Owner 11-09: after the fixes, a SECOND fresh Opus review the same way, then fix again.
     Owner 11-09 added to the fix round (agent running): lakes never split (only different rules do); names composed
     in core from each part's two end tokens (no stored names); no " (n)" names: "Within <area>" and B.C.-border parts
     named by their ends (border: nearest B.C. town if we hold a source); fallback sweep (delete or fail loudly).
     T521 opened (unplaced DFO rules shown in app). Explainer: https://claude.ai/artifact/Sm3zaYhE2qYj79dDRWKkMY
     Icons (launcher only): rounds 1-3 in icon-proposals/; owner likes the linocut trout; round 4 = map pin + sun over
     waves (not the gov logo) + B.C. outline options. Owner 11-09 picked round4 "Leap across the sun" (interim until a real human-made icon); WIRED 87ef3bb (clean prebuild
     for build 22; Play listing icon + feature graphic re-upload = owner). Night 11-09: owner OK'd many lanes; running:
     T517 fixes (+ fast-fail name checks), T521, rebuild-speed lane, T496 cleanup, T360/T351/T401/T448; then 2nd review. Open owner item: Dean Anahim run has a lake
     between its pieces and no mark, so they never join.
2. **Explainer artifact** (owner ask): the new part + in-season flow and format, why each decision, with visuals.
3. Then the queue: build 22 (Play internal, `MLN_LOCAL_MAVEN` lean MapLibre; carries T509/T515/T516/T517/T520),
   T351/T360, T401/T448, T496.
4. **Owner approved (11-08):** once all of the above is done, upload the final build to Play INTERNAL (not production).

**Needs the owner**
- Remove the R2 keys from GitHub settings once T520 deletes the workflow (OWNER-TODO).
- On return: T510 moved list, T517 parity list + sizes + shots.

## 2026-11-08 afternoon (chat 7, continued): gauges unfrozen; map lag = stock MapLibre; bait card rework

- **Gauges were frozen (T513, DONE on staging):** 162 of 434 stations showed old readings (Capilano 0.742 vs 32.6 m³/s).
  Cloudflare's edge cache served old ECCC CSVs to the feeds worker; source requests now bypass it. 431/434 current.
  Production has no feeds yet — the first prod feeds deploy carries the fix.
- **Laggy main map (T512, IN REVIEW):** Play builds 20/21 shipped stock MapLibre. `build-apk.sh --play` now refuses
  without `MLN_LOCAL_MAVEN` (package kept in cache/maplibre/fslean-m2). Emulator: stock 60→84 ms after switches, lean
  flat ~34 ms. Build 22 = patched; owner expects it to fix it.
- **Share page (T487/T514):** "Open on the website" + greyed "Open in the app · coming soon" (committed; goes live with
  the next staging site deploy). T514 = real store link at the production app release.
- **Owner answers:** BC is on permanent daylight time (settled, never ask); production waits for the owner's DFO extent
  labelling; the regulations audit (T462) runs after the DFO extents land.
- **Running:** T509 Fraser part names (must not change painting/feeds/combining); T515 bait card layout (owner: the
  "NOW · All year: bait ban · in force today" line is gross; shots to owner).
- **Next:** T510 rule A (11 connectors, moved list to owner first), T351/T360 crash+memory session, then build 22
  (patched) with T488 B, hint removal, T509, T511, T515. Unpushed local commits — push with T509.

## 2026-11-08 night (chat 7, continued): r-6dce77a24e6a on staging — slough names + slough gaps fixed
- **On staging now:** r-6dce77a24e6a, 18/18 smoke. It carries T497 + T493 + T501 + T503 (and the T485 run-timing seed).
  Production is yours: `CONFIRM_PROD=r-6dce77a24e6a python -m pipeline release --prod r-6dce77a24e6a`, **after** you
  accept the moved list below.
- **T497 (wrong names on Fraser braids) IN REVIEW:** an unnamed channel now takes the water its two ends reach. 20
  sections change water, 26 rules change, nothing else. Nicomen Slough and Maria Slough lose the Fraser's night and
  steelhead closures (Nicomen gains its 8 km/h rule). Two to check: an 11 ha pond on Baldy Channel becomes stream water,
  and 4 km of Old Kootenay River Channel lose the Kootenay's exemptions. List: tickets/shots/T497-moved.md.
- **T493 (gaps in slough polygons) IN REVIEW:** a hidden piece of a water is drawn from where its own water breaks on
  both sides. 242 pieces drawn earlier; Nicomen z11 → z7, Camp Slough z12–13 → z11. Shots: tickets/shots/T493-*.png.
- **Wording (IN REVIEW):** T500 release/closed key is just the term with ⓘ; T501 no-fishing lines say the short place;
  T502 no words under the salmon table that repeat it; T503 rainbows read "30–50 cm" where steelhead rules apply; T506
  Conditions sources and notes at the bottom; T505 the tinted 1885 crayfish. App changes need the next APK.
- **Production setup:** Q171 reports go to feeds.canifishthis.ca (workers.dev off); Q174 v1 keys deleted on switch day.
- **Fixed on the way:** T466 walkthrough's closed step opens through the one water path; T468's lake check now asks the
  registry kind (test_t121 green). Left: T507 (a review-screen test pins live corpus counts, 452 vs 445).
- **T487 (shareable gauge page) IN REVIEW, on staging:** 450 gauge pages in the sitemap; the share card is drawn at the
  feeds refresh and served as a static file (almost no worker compute). Two questions on the morning page (Play link, PDT).
- **T488 (light park colour):** proposal only, A or B on the morning page; tokens unchanged until you pick.
- **T498 + T499 IN REVIEW (emulator):** a Regulations pane opens in one piece (built hidden, shown once its page, salmon
  card and access notices are in); the water map no longer blinks its part out every ~5 min (it re-lit unchanged ids).
- **T508 IN REVIEW (emulator):** Map and Conditions views open in one piece too (ready = map drew the lit part after the
  fit). Fraser search → Open 18–20 screens → 2; Conditions taps 3–6 → 2; 80 held Map opens, no crash. A DFO feed update
  no longer closes an open mark/chooser/parts list. App suite 6914 pass.
- **Staging APK** e7eb12a8f (reads 0.6.9) carries all of the above plus T500/T502/T505/T506/T466/T494.
- **Morning page:** https://claude.ai/artifact/9XDb7uXoQ9ciZf8sS7uz3U
- **Later 11-08 (owner awake):** Play internal versionCode 21 (6ef99a4e5) uploaded. T497 accepted (prod command in
  OWNER-TODO). T488 B applied to light (cvd theme kept: B vs noaccess fails cvd.test). Old mode hints + Help replays +
  CoachTour removed (owner; 96abc49). T511 gauge card v2 (forecast by shape, readable axis, normal range, bigger plot)
  owner-approved, not yet deployed. T509: owner says parts = contiguous merged runs, no "stretches" — agent reworking
  pipeline + app (rule diff must be 0). T510: owner picked rule A for all 11 connectors — waits for a lane (side atlas
  build + moved list to owner). T512: agent analysing owner's laggy-map trace (Map tab worst, worse after map switches).
  App suite 6911 pass at 96abc49+.

## 2026-11-08 early (chat 7, continued): phone check sent; T478 done; T494 found and fixed
- **Your phone check (0.6.9):** https://claude.ai/artifact/XsAWajGrhpgsCnK2kWv7sZ — 11 items that close the IN REVIEW
  tickets, plus the T459 pick (A or B). Reply with the numbers that fail. T449 is DONE (both Nanaimo marks are in r-f56b).
- **T478 DONE:** both 0.6.8 walk failures were the test, not the app. The Updates log now opens from the Developer card,
  and the Coquitlam tap target was picked while the river was lit (a lit river answers at the lake's tip; unlit, a tap
  there opens the lake, which is right). Test bed: install and water-nav pass.
- **T494 (web map error):** switching to Conditions or Runs from the Map tab threw in the stream layer for a frame. The
  T470 order fade made the stream's opacity a zoom curve on the Map tab only, and MapLibre cannot swap that kind on tiles
  it already built. Now a flat zoom curve in every mode, plus a test that no paint value changes kind across modes. The
  staging smoke now FAILS on a TypeError instead of warning.
- **Crash store:** nothing from your phone on 0.6.x; every report is the emulator or the web (T494).
- **Gate 2 (emulator):** a ~28 min session, 6/6 pass, no crash or ANR. Memory peaks at Conditions (~396 MB), with no
  growth over 32 tab switches. Your phone session is still the last step of this gate.
- **T404 (one switch to production) IN REVIEW, merged:** every address comes from one setting (with a guard test). The
  production deploy scripts refuse without your CONFIRM_PROD. Runbook: handoff/PLAN/release/PRODUCTION.md. Five
  questions with defaults (Q171–Q175) are at the bottom of the phone-check page.
- **T493 + T489 merged (IN REVIEW):** the creek is drawn through a hidden lake (the lake's colour and tap); lakes keep
  about twice the outline zoomed out (+1.6 MB tiles). Release **r-25a89ce9f8e6** is LATEST and **live on staging**
  (18 smoke checks pass). open.canada.ca was down, so the T485 kept copy was seeded by hand from this Mac's 10-29 fetch
  (staging `private/runtiming/src/sources.tar.gz`, FETCHED 2026-10-29); run timing no longer needs open.canada.ca
  unless run with `refresh=true` or the copy passes 400 days.
- **T466 merged (IN REVIEW):** the walkthrough scrolls Layers to Fish obstacles before ringing it; Spots is a real
  10-step creation (the example spot removed at the end); the closed example is the Chilliwack's stretch closed all year
  (reached Upstream ×3 from the Vedder). Emulator walk 41/41. Defaults for 3 small questions are on the phone-check page.

## 2026-11-07 night (chat 7): the goal is written down (T495); r-f56b live on staging
- **Goal (T495):** Play closed testing ON PRODUCTION, good enough for first users. Gates: no wrong regs (T462 audit), no
  crashes/ANRs (T351/T360), smooth map (T420 on your phone + T401/T451/T493/T494), clean first run (T478/T423/T466). The
  website moves v1 → new before closed testing; iOS right after. You: salmon review (T164) + prod steps. Claude: the rest.
- **Data r-f56b90ad2540** (T482 salmon fold, same bindings) is LATEST and live on staging; the staging site is rebuilt on
  it, smoke 17 pass / 0 fail. T490 DONE. Prod (yours): `CONFIRM_PROD=r-f56b90ad2540 python -m pipeline release --prod r-f56b90ad2540`.
- New: T494 (a web map console error), T496 (cleanup sweep). Phone push when I ask you something is on.

## 2026-11-07 evening (chat 7): /changes fixed, feeds fresh, fast-fail in
- **/changes (T490):** lists waters whose RULES moved (rule ids + values), not wording; a wording release says "Wording
  updated on N waters" once. r-1b4f vs the live site: 17 waters changed (T468), not thousands. Staging site deploy +
  release stage 6 rerun in progress.
- **In-season feed (T486):** never broken — a daily run just under 24 h after the last write skipped it, so the file aged
  to 48 h. Both feeds now rewrite within 20 h; staging worker deployed, the feed is fresh.
- **Fast-fail (T483, IN REVIEW):** `scripts/fast-fail.sh`, 8 cheap checks (~8 s), run first by the release and the APK
  build. **T484 (IN REVIEW):** a resumed release keeps its min app version; the push check is in preflight now.
- **T491** test fixed. **New: T493** — a creek gap where a small lake drops out zoomed out (Foley Lake): your answers
  are in the ticket (draw the creek through, lake's colour and tap).

## 2026-11-07 afternoon: 0.6.9 is on your phone's Play track (patched MapLibre) + stock on the staging page
- **Play internal, 0.6.9 (versionCode 19):** the lean MapLibre lag fix (T420), verified in the AAB. **Staging APK page:**
  the same app with stock MapLibre (0.6.9-677e99fb3). Pan Coquitlam z13 and Fraser Valley z11 on each.
- **In 0.6.9:** quota wording (T479), one size style (T480; the access banner is back where it was), the sticky header
  (T481: "Trout and char · 4 a day — up to 2 of any kind, the rest hatchery rainbow" / "↳ 1 of them may be over 50 cm…").
- **Data r-1b4f54fb6ed9** (Powell River + Little Slocan T468, 15 rules gain, none lose; T479/T481 fields) is LATEST
  locally and **live on staging** (an open.canada.ca outage blocked it until T485 step 1: the upload now skips the
  run-timing workflow when staging already has it for the same tiles). The staging website is NOT redeployed (T490:
  /changes over budget on a wording-only release). Owner on the phone: the lag is a lot better.
- **Next release:** T482 salmon fold (sizes into species rows, source folded; also fixes R1/R2/R6 shipping no salmon
  sizes). New: T483 fast-fail (AGENTS 20a), T484 release resume bug. Restart page: NEXT-SESSION-after-chat6.md.

## 2026-11-07 morning summary: the lag is fixed in MapLibre; your turn on 5 items (OWNER-TODO top)
- **Lag (T420): the fix works.** Patched MapLibre, stressed emulator: map frame p50 Coquitlam z13 76–148 → 31–46 ms,
  Fraser Valley z11 60–104 → 31–35 ms. It no longer slows down over a session. Feature-state CPU went 42 % → 2 %. The
  lean (upstream) patch matches the full one (z13 36–52 ms both), with colours right after mode switches. ANR: 1 in the
  first 9 patched blocks, never again in 6+6 targeted runs plus 1 lean run; the patch adds no lock. **Not uploaded:** the
  permission check refused Claude's build-and-upload, so you run the two commands in OWNER-TODO (patched lean → Play
  internal vc 19, stock → staging page).
- **Upstream (T420):** lean fix, 6 files +22 −7, tested on maplibre-native main (the new unit test fails before, passes
  after). Drafts + `gh` commands are in app/tools/maplibre-patch/upstream/. You post it, in your own words (their AI policy).
- **Powell River (T468):** fixed (a river running through a lake with no rule of its own is now the lake's tributary).
  Powell River is now closed Nov 1–Mar 31. 15 rules gain sections, none lose. Built in scratch, NOT promoted: first
  answer the Little Slocan question (OWNER-TODO).
- **Calmer mid zoom (T470):** B built (small waters recede; z12+ identical to today). The tuning knob is `fadeByOrder`.
  One dark-mode call is in OWNER-TODO.
- **Quota wording (T475):** built in scratch, wording only (0 numbers/statuses/bindings moved), IN REVIEW, not promoted.
  Shots were sent.
- **Done:** T469 legend in one row on wide screens; T471–T474 regs pane (sun line in header, in-season one line, daylight
  hours, the water's rule-book link); T476 Developer card (long-press ©, dev/staging only, About cleaned); T477
  date-dependent tests fixed.

## 2026-11-07 (overnight): map performance lane (T420) — the cause found, the fix being measured
- **Cause (MapLibre's own code, android-v13.6.1 and main):** every frame, MapLibre re-applies EVERY stream colour set this
  session to every tile on screen (`coalesceChanges`), and it grows over a session. **Your Pixel 9 trace** (0.6.8,
  Coquitlam z13): the map drew ~10 fps, its render thread busy 92–100 % on the prime core, the GPU ~1 ms a frame. So the
  lag is CPU work, not the GPU (pixel cap / Vulkan-vs-GL can't fix it).
- **Shipped:** the Map lab (0.6.8; long-press ©; 2f55534); "skip invisible layers" ON by default for every build (10fcb30;
  stressed z13 p50 −15..−19 ms, same look; your phone: helps). Colour batch gap and one bucket: no gain (to drop).
- **The fix:** a patched MapLibre (acf015b98, `app/tools/maplibre-patch/`): a tile gets all colours once, then only
  changes. Test-bed only (production refuses it). Running: stressed A/B patched vs stock (+ the long-session check).
  Pre-approved: if it CLEARLY wins, an APK pair (same app, stock vs patched) goes to staging / Play internal for the morning.
- **Upstream:** prepare only, the patch as lean as possible (agent running); you post the issue + PR from your account.
- **Records:** T420 (profile, correction, lab A/B, phone trace), COORD-068, OWNER-TODO (lab phone script).

## 2026-11-06: chat 5 — next APK (0.6.7)
- **0.6.7 (versionCode 16) is on Play internal** (e6071231a): T443's half-screen floor, T451 (no stream flicker on
  the Map switch), T452 (the new glide). Fresh-install walk 5/5 (`app/tools/testbed/out/walk-067/`). The camera where
  a glide stops is exact (0 dp travel error over 3 flings). Also fixed on the way: a swallowed error in the T451 change
  is now reported. `pnpm check`: no single fully green run (the machine was loaded, timeouts only; the one failing file
  passes alone). **Please try on the phone:** does the fling glide (longer, softer) feel right? Stream flicker on Map?
  T454 (photo zoom) is NOT in 0.6.7.
- **Staging site rebuilt** on 0.6.7 + T454: smoke 17 pass, 0 fail, 1 warn (the gauges feed reports "some parts
  held").
- **`pnpm check` on a quiet machine:** 6700 pass, 1 fail: the walkthrough's Layers test skipped a step. It passes
  alone, so it is a race, possibly one a reader could hit (a step skipped). Filed as T455, an agent is on it.
- **T454 on the emulator** (13eb712, recording sent): double-tap zoom, drag pan, a swipe while zoomed stays on the
  photo, double-tap back, a swipe at the fit changes photo. Pinch cannot be driven on the emulator (no multi-touch
  injection): yours to check on the phone, in the next APK.
- **T455 fixed** (2290b6e25): a real race, a reader could hit it. A tap landing just as a done step moved on was run by
  the previous step's button and skipped the next step. Now a tap always acts on the step on screen. Under load: 2/20
  runs failed before, 0/40 after. `pnpm check` fully green after (6701 pass, 0 fail).
- **T446 fixed** (7fc594427): the Expo SDK 58 upgrade made the file write/move/copy calls asynchronous, and ours did
  not wait. A rename moved an EMPTY file into place, so fresh installs got 0-byte release files (the retry usually
  recovered). The spots save does the same write-then-rename, so it could have emptied a spots file. Fresh install
  after the fix: 1 boot, 0 mismatches (before: 2 boots). If any of your spots look missing, say so before saving
  anything new: the store keeps a backup copy beside the file. T458 filed: a check that catches an un-awaited promise.
- **New tickets:** T456 (walkthrough rework from the tester's feedback), T457 (lake outline + see-through fill, shots
  for you first), T458 (above).
- **T307 + T308 DONE** on the emulator: a spot saved offline shows in under 2 s and fills its weather/gauge itself once
  back online; the location dot follows each move. Found on the way: an empty spots backup wedged every later save
  with a raw error. Fixed: it is kept aside and reported, never read as "no spots" (0e8ca33).
- **Park border from z11** (d9823e0, your ask): the tint and name still show a park from z8.
- **New tickets:** T460 (Fraser: ▲ from "Williams Lake River confluence → Region 7A boundary" offers 2 parts, one skips
  part 15: a side channel of that part touches the part above; first check whether that channel's sections carry the
  right region's rules), T461 (tidal polygon: measure, simplify per zoom, quieter edge; shots for you). T443's
  "landstorm bar" item corrected: two copies of the Landstrom Bar label ~50 m apart overlap.
- **Running:** T456 (walkthrough, agent); the step-screen lane (T451 flicker, T443's two items, T459 grey-out, agent;
  told about the Landstrom correction). **Map performance (T420/T401/T448) now belongs to its own chat**
  (handoff/PLAN/MAP-PERF-HANDOFF.md, started 11-06): this chat does not touch those tickets, and the two chats take
  turns on the test bed. That chat's perf changes join the 0.6.8 batch if they are ready in time.
- **Done 11-06 (evening):** T460 (Fraser step no longer skips a part; 10 waters; the side channel's rules were right), T464
  (Bulkley: 6 of 12 parts could not be stepped to; only the Bulkley changes, plus a release test that every part is
  reachable), T457 lakes + T461 tidal look (owner approved the shots), T458 (the floating-promise gate in `pnpm check`).
  The perf lane's Map lab is on the branch (94695e0, e88e3be); its in-app check is still running (COORD-068).
- **Left before 0.6.8 (late 11-06):** every lane is in except the perf lane's Map lab, which fails 2 checks
  (back-layers, clock; COORD-068). The build starts when COORD-068 says it is green, or with the lab's long-press unhooked.
  Fixed tonight on the way: T460 also removed the Skeena's side-channel fork (fixed: a side channel, which has no km, is
  never a skip; 8 waters change); the lighter lake fill failed the colour-blind status gate, so it is back to 0.6 / 0.85
  and the wider shoreline stays (T457).
- **Owner picks pending:** T459 A (the river it flows into keeps muted colours) or B (all grey); T443 removed the old
  "zoom in at most 2 levels per step" bound (so first open == ▲▼ framing). Then T463 / T462. iOS benched (OWNER-TODO).
  T457/T461 edit layers.source.json, which the perf chat may also edit: commit only your own paths, rebuild the style
  after a pull.
- **Your 0.6.7 check:** the glide feels good (T452 DONE). Still to do: the pan stutters while the map loads tiles
  (T420), the Map switch still flickers coming from Conditions/Run timing ("maybe better", T451 reopened), the
  boundary names in the ▲/▼ bar overlap, and the first open of a water's Map tab frames differently from ▲ then ▼
  (both added to T443). The Fraser is framed well.
- **Next:** the step-screen lane (T451 flicker, T443's two items, T459 grey-out) and T456 (walkthrough), then ONE 0.6.8
  APK with all of them + T446, T454, T455 (owner 11-06: wait until a few items are done). Then T457, T420.
- **Crash store (T351):** no foreground native crash since 0.6.6. Today's reports are emulator installs (android 35)
  failing a download check ("does not match the manifest", several "0 % of its size") plus one from 0.6.4 on your
  phone (before T446's change). The test-bed builds do tag theirs. These carry the Play build's version, so they come
  from walks of the store APK on the emulator: not a fault.
- **New tickets from you:** T453 (alerts by the day's percentile + an easier alert menu, my design; waits on T234),
  T454 (zoom/pan in the spot photo viewer).
- **T454 built** (4ede32a69, in review): pinch zooms around your fingers (1×–5×), drag pans while zoomed (stops at
  the edges), double-tap toggles 2.5×, a swipe changes photo only when not zoomed, the mouse wheel zooms on the web.
  Tests + `pnpm check` pass; not yet tried on a screen (emulator check next, then your phone).
- **T450 done** (dedd06c): the two pipeline tests that read the retired golden.json; collection is clean again.

## 2026-11-06: chat 4 — zoomed-out lag, then the list
- **Your 0.6.6 notes (late):** T443 now never frames a part under half the screen (the small Fraser part lands at
  ~z13, 55c7f54; in the next APK). Both Nanaimo places accepted as tidal water (your answer; marks saved, place file
  written). Running: an agent on the map-switch stream flicker + the fling stutter, and one taking the raw section ids
  out of the tests (T441).
- **Zoomed-out lag (T420), your call:** the lag agent's one fix is ready but not shipped: the basemap's green/land fills
  stop drawing a soft 1-px edge outline (a second pass over the most detailed shapes zoomed out). On a slow-GPU proxy of
  your Pixel 9 that is 14–15 % less frame time at z5–z9; the picture changes ≤ 1.3 % of pixels. Shots sent (today left,
  proposed right). Hiding parks would save about as much but changes the map a lot, so it is not proposed.
- **Zoomed-out fix: you said ship it** (e58b10a). It reaches you with the next APK and staging site.
- **Running:** T448 (phone map draws at ≤ 2 pixels per dp like the web). Found: MapLibre's own pixel-ratio switch on
  Android may only shrink the map, not save drawing; the agent checks that before changing anything.
- **T444 fixed** (6ea4073): the Chilliwack → Vedder crash report. For one screen refresh the map held the old river's
  parts with the new river's part. A test now reproduces the exact report; fixed at the source.
- **T445 fixed** (73daab938): "resolved to null" was not a style error. The map asked something of a view that had
  just closed (the map left the screen, or the style reloaded). Those are no longer reported; a live view's still are.
- **T443 in review** (7f36eb73a, your rule): a part is framed no closer than z12, and ▲/▼ zooms in at most 2 levels
  per step. Before, one Fraser step could jump 5 levels (z9 → z14). Opening or reloading a part still frames it whole.
  Look on the phone after the next APK.
- **T448 done on Android** (26c59849f): the phone map draws 42 % fewer pixels (823×1567 instead of 1080×2057), same
  scale and taps; a touch softer (crops sent). The emulator cannot show the speed gain (its GPU is the Mac's): your
  Pixel 9 is the check. iOS not done (no iOS build).
- **T449 in review** (e83f7811e): the two Nanaimo "tidal portions" DFO places are proposed as tidal water (no other
  region matched). Your 2 clicks in the review app are on OWNER-TODO; your call: accepted ones drop off the card.
- **Running now:** a T401 agent (prefetch: more detailed stand-in tiles while panning, measured before it ships). Earlier: a T449 agent (the Nanaimo's tidal DFO places get a "tidal water"
  state, as a proposal for you; plus a sweep of other regions). Next: T446 emulator repro, T441 one-step fixture
  re-cut, then the APK + staging site (T420 basemap fix, T448, T443, T444, T445).
- **T441 mostly done** (d2a5c59): after a release, `node app/tools/regs-recut/sync.mjs` now re-cuts every test fixture in
  one step (it was four rounds of `pnpm check` before 0.6.3); the stream-lines fixture is written from the release
  instead of by hand. Re-cut on today's release: 4290 tests pass. Left: section ids still typed into a few tests.
- **T401 measured, not shipped** (29b1e561b): a smaller prefetch delta is now possible but its gain stayed inside
  run-to-run noise on the emulator, so the app keeps MapLibre's default. If streams still look bare while panning on
  the Pixel 9, a side-by-side APK on the phone is the real test.
- **T446:** the next such report will say whether the file was cut short or overwritten (cddfdbe); not reproduced.
- **T164 proposals refreshed** on today's release: 177 waiting for you in the review app's DFO mode (4 fewer: placed
  by the T326 cuts).
- **T420 iOS half done:** the patched map code compiles and runs in the iOS simulator (iPhone 17, iOS 26.5): colours
  arrive, repeats carry no ids, no "not held" reports. No changes were needed.
- **Full check:** 2 failures fixed (a guard on my T445 change; START-HERE now points at after-chat3, the two older restart
  pages deleted). **0.6.6** committed (72142ff).
- **0.6.6 (versionCode 15) on Play internal**; staging site rebuilt with it (19,635 water pages), smoke 18/18.
- **0.6.6 emulator walk 5/5** (fresh install, pan, regs-open, walkthrough, water-nav; `app/tools/testbed/out/walk-066/`).
  T250: Android, iOS simulator and web walks all pass on SDK 58; your phone walk is what is left.
- **Chat 4 closed.** Next chat: `handoff/PLAN/START-HERE.md` → `NEXT-SESSION-after-chat4.md`. New T450 (two old
  pipeline tests still read golden.json).
- **T446 read, not reproduced:** the file check failed after a clean download, which needs a second writer (e.g. a
  retried start while the first was still running). It happened in the test channel's broken window (T442). Emulator
  repro once T448 frees the emulator.

## 2026-11-06: chat 2 heavy runner — T416 closed
- **A fresh clone now rebuilds the release exactly** (T416): the fresh copy and this Mac, same code, made the same release
  (r-0fd5c0e4e8b8), every file the same. Two last gaps were found and fixed: the basemap (a fresh machine shipped none)
  and the PDF highlight boxes (a fresh machine shipped every rule without its highlight). Both are now downloaded by
  `scripts/bootstrap.sh`, and a release that lacks either one stops with an error instead of shipping without it.
- **This Mac's data rebuilt with today's code**, held back from LATEST: the new release r-0fd5c0e4e8b8 waits in
  `data/release/CANDIDATE` (LATEST is still r-5a5999c94588). It is the same release the fresh clone made.
- **Full test suites:** app check green (8,701 tests); pipeline fast tier green after one fix — a clean-up commit
  earlier tonight (T436) had made the "where does this river continue" code ignore named test rivers (11 red tests;
  real data was not affected); slow tier: only the 6 known T306 failures, nothing new.
- **Water pages are faster** (T409, measured on this machine): phone Lighthouse 93 · 91 · 93 for Vedder, Cowichan and
  Skeena (they were 88 · 85 · 78), desktop 99-100, now that the map behind a water page is only built when it is seen.
- **A water page on a slow phone line hands over to the app in about 25 s instead of 45 s** (T304): every page now names
  its region, so it fetches its region's small files (3.5 MB less). Staging gets this with the next deploy that rebuilds
  all the water pages (not the quick relink). Found on the way: each landing downloads the fonts twice (~315 kB) — noted
  in T409 as the next item.
- **Web and phone code are cleanly apart** (T410): the web bundle holds no phone-only file and the Android bundle no
  web-only file. Left for you: a phone pass over the moved pieces.
- **The map tiles get the same id when nothing changed** (T305 closed): two rebuilds gave identical files.

## 2026-11-06: chat 3 — your calls, then the next build
- **Your calls recorded:** golden.json is retired (new T439, in progress); T383 and T341 dropped; T389 deferred;
  the name is "Can I Fish?" on the icon only and "Can I Fish This?" everywhere else (the app already matched; store
  texts updated); privacy drafts approved. Salmon (T164) is next on data; sloughs (T322/T55) on hold.
- **Running:** T250 Expo SDK 57 → 58 upgrade (React Native 0.88, gesture handler 3) in its own copy of the repo, so
  chat 2's test run is not disturbed; T439 in another lane.
- **Next, before the build:** T423 (dead switch), T428 (one sync control), T424 (attribution), T419 (Salmon River
  detail), T402 (flaky tests), T306 re-check; the "phone" bugs (pan lag T420/T401, crash T242, walkthrough T293)
  reproduced on the emulator, not your phone. Then an APK + staging release (you're the only user).
- **T326 done:** Mamquam is not tidal (your call); every proposed DFO cut you accepted is applied — 7 cuts (Quatse
  weir ±100 m, Stamp lagoon inlet/outlet + powerline, Little Qualicum park boundary, Sheldens Spur 19) and the 4 DFO
  extents waiting on them. Checked first on one side rebuild: rules only GAIN the new pieces, nothing lost. They reach
  the map with the next atlas build.
- **T439 done:** golden.json and the tests that only compared against it are gone; the release-backed fixtures stay.
- **T250 merged (in review):** Expo SDK 58 (React Native 0.88, React 19.3, gesture handler 3); full app check green;
  the release APK starts and walks on the emulator. Release-build shrinking (R8) stays off as before — SDK 58 turned
  it on and the app crashed at start.
- **T423 / T428 / T424 in (in review, need an emulator look):** the switches now say "applies when you reopen the
  app" and show a failed save; More → Data has ONE sync control; the map credits are one © chip + card, the same on
  web and phone, and no longer on the scale bar. OpenStreetMap's licence wants its credit SHOWN FIRST, so the first map
  of a session shows "© OpenStreetMap contributors" for 5 s, then folds to the chip.
- **T402 done:** the two tests that failed only under load have wait ceilings that fit a full run.
- **T419 cause found:** below z12 the tiles kept every meander bend bigger than a pixel, so meandering rivers like the
  Langley Salmon River drew as a sawtooth (a class, not one water). The tolerance there is doubled; it lands with the
  build below, then before/after shots for you.
- **New release built: r-4230bc0c1dd5** (held as a candidate; LATEST still r-5a5999c94588). 51 min; tiles, bundle
  and every file agree on one vintage; the 7 DFO cuts are on the map. **Tiles 8.8 MB smaller** (202.8 → 194.1 MB) from
  T419; the Salmon River drops a third of its points at z9–z12 (z13 unchanged).
- **r-4230bc0c1dd5 is LATEST and on staging** (data). Its rule check against the live release: exactly the 7 cuts,
  109 rules each GAIN the new pieces, nothing lost or moved — the same diff the side rebuild showed. Staging smoke:
  17 of 18; the one failure is the credits check looking for the new © chip on the OLD staging website, so the site
  is being rebuilt now (new web app + every water page, T304), which reruns the smoke test.
- **T306 done:** all 6 slow-suite failures were tests pinned to values your later calls changed (mostly the sloughs);
  the status-index one was already fixed on 10-30, so the shipped map was right. No pipeline code changed.
- **T410 re-checked:** the web/phone split guard had been crashing silently on the iOS build leftovers; fixed, and
  today's code is clean (35 pairs, 16 allowed inline files). Left: the moved phone halves walked on the emulator.
- **Done (chat 3):** staging site rebuilt on r-4230bc0c1dd5, smoke 18/18 (attribution check passes now). T419 after
  shots on staging (`T419-shots/*-after.png`, `t419-out2-compare.png`): sawtooth mostly gone, tiles −8.8 MB; owner look.
- **Emulator lane merged (chat 3, c1c838f58..c087e337d):** T293 DONE (Android back + full walk 25/25). T242 crash test
  bed: all three crash kinds reach the report path; fixed a JS fatal being reported twice and a lost exit record (still
  PARTIAL: owner reads `GET /crashes`, privacy wording, T407). T420/T401 reproduce only on a CPU-starved emulator;
  lever = the colouring's per-tile events (fix proposed: send each tile's ids once per watch, native patch). T351 not
  reproduced (peak 385 MB). T410 native pass 6/6. New T440: first-run notice missing from the Android a11y tree.
- **0.6.3 on Play internal (versionCode 12, 12f3dd109):** `pnpm check` 6674 pass after re-cutting every fixture on
  r-4230bc0c1dd5 (taps, fixture release, VM fixtures, section-lines, pinned ids — four rounds; T441 filed to make it one
  step). Emulator walk first failed 0/4: NOT the app — the test bed's `test/` channel named the
  release T433's upload had cleared (T442 DONE 51e05a4ef: upload re-points test/; test/ reset). Play builds read the
  live manifest: 0.6.3 is fine. Rerun (`out/walk-063b`): download ok, regs taps 73–250 ms; the rest blocked by T440
  (first-run notice body missing from the Android a11y tree since SDK 58 — agent fixing in Sheet's RNGH ScrollView).
  **Staging feeds worker deployed** (5dc96631; 218 worker tests; gauges/dfo/inseason ok).
- **Owner 11-06 (late):** lag is THE priority → agent on T420/T401 (native tile-id dedupe + JS/web costs, stressed
  emulator). T419 verdict: better but meanders stay kinked and gentle streams over-simplified → agent comparing
  visvalingam / smoothing variants (images get SENT to the owner; they cannot open repo files). Crash + problem reports
  read from the staging worker: T242 DONE (privacy wording approved), new T443 (Fraser step camera), T444 (part not in
  its group crash), T445 (MapLibre null expression), T446 (first-install file mismatch). **T440 DONE** (48efaa5fb: RNGH detector
  directly around RNGH's ScrollView drops the subtree from Android's a11y tree; an unflattened View between them;
  test-bed walk 4/4 incl. install + walkthrough 25/25).
- **0.6.4 on Play internal (versionCode 13):** T420 native lag fix (deb635819: colouring re-watched every tile on each
  camera settle; ids now once per watch) — stressed emulator p50 84→50 ms, tile events 958→37; T440. Check 6678 pass
  (swipe test load flake fixed). **Lag agent resumed** on mobile-Chrome lag (owner: web lag is ONLY Pixel 9 Chrome):
  pixel ratio cap + per-layer overdraw; parcel layer is 113k/132k vertices at z11. **T419 rework done, not shipped**
  (2d62d60ff, 49ab00cd5: per-zoom smoothing in pipeline/deliver/tiles/smooth.py; stream vertices z9 −70 %, z10 −51 %;
  full build data/generated/side/t419/tiles/atlas.pmtiles, same handles as r-4230bc0c1dd5): owner picks from the sent
  sheets, then a tiles release to staging.
- **Late 11-06:** release **r-67b456b5e64b** (T419 smoothed streams, owner chose L) LATEST + on staging, smoke 18/18,
  test/ re-pointed. Owner: zoomed-in lag "way better", zoomed-out still laggy (suspects parks/polygons). T420 web
  pixel-ratio cap ≤2 (2b891823f, −28–32 % frame time on the Pixel 9 Chrome proxy). T390: Play builds say a new version
  only once Play offers it (0b96e8848, owner). AGENTS.md rules 34a (web/native split by file), 45 (new chat / compact),
  46 (audit guards), absolute 5–6. Owner decisions: icon ships (artist icon T447 postponed), support URL /about,
  T425/T427 good, T319 titles kept. Filed T443–T449 (T448 native pixel-ratio cap, T449 DFO wholly-tidal places:
  Nanaimo below Cedar Rd bridge is tidal, nothing to bind).
  **In flight:** (1) background chain: staging site rebuild (web cap) → `pnpm check` → `build-apk.sh --play` 0.6.5
  (logs `~/site-1106b-*.log`, `~/check-1106h.log`, `~/apk-1106c.log`); if the check fails on release fixtures, re-cut per
  rule 46 / T441. (2) lag agent: zoomed-out lag z5–z9, polygon layers cost, fix + numbers (proposals for anything that
  changes what shows). **Then:** emulator walk incl. fresh install on 0.6.5, NEXT-SESSION file, new chat.
  T420 zoomed out (a893b06fe): the low-zoom cost is the BASEMAP's green fills (geometry + antialias outline), not our parks
  (≤5 %); basemap fill antialias off = −10–13 % frame z5–z9 (proxy), looks ≤1.33 % px; next = harder landuse simplify
  (tile build) + 2 look proposals, in T420 "Zoomed out".
- **Chat 3 closed:** staging site rebuilt with the web cap (smoke 18/18). Check: one load flake (Walkthrough test,
  fixed c35b0866e), rest green. **0.6.5 (versionCode 14) on Play internal**; fresh-install emulator walk 5/5.
  Zoomed-out lag findings: see T420. Next chat: `handoff/PLAN/NEXT-SESSION-after-chat3.md`.

## 2026-11-05 (night): chat 3 — the audit's fix tickets
- Chat 2's heavy run is still going; nothing heavy started here.
- **T429 done:** a missing review input can no longer empty a curated file. The name / slough / tidal lists now stop
  with an error if their input is missing, and a write that would drop more than 10 rows is refused unless you say
  `--accept-shrink` (the review app shows the refusal on the item).
- **T433 done:** the staging upload no longer takes staging dark if it fails half way (the old files are cleared only
  after the new pointer is up); the inputs sync stops on a failed copy instead of saying "in sync"; HYDAT has one
  checked downloader; the secret scan fails when it cannot read a secret; a production TestFlight build needs
  `CONFIRM_PROD=canifishthis.ca`.
- **T430 done:** the app's crash and problem-report formats are now checked against the feeds worker's, so one renamed
  field can no longer lose every report.
- **T431 done:** the feeds worker's tests now run in CI and before every staging worker deploy. **`REPORT_SALT` is now
  required** — without it the worker refuses reports (they stay queued on the phone). Staging and prod both have it
  (owner 11-06: already set on the prod feeds worker); it stays required.
- **T432 done:** the full test run fails when the golden parity check finds nothing to compare; every app test skip is
  listed with its reason; every pipeline test file must have a quick test in the smoke list (148 still to pick).
- **T434 done:** the review app no longer turns errors into empty answers. A failed load now shows "could not load X"
  with the reason in a banner. **The backend no longer auto-reloads on code changes** (start with `DEV=1` if you want
  that): a reload could cut off a save half way. API.md now lists every route.
- **T435 done:** pipeline tools read data only through the declared paths (and CI now catches the form that slipped
  through 23 times); the gauge tools that judged staleness by file times are gone; one release-id formula; several
  "quiet default" spots now stop with a reason instead.
- **T436 done:** dead modules deleted, duplicate helpers folded into one each.
- **T437 done:** docs and ticket statuses made true; the ticket check now refuses a ticket whose file and ledger row
  disagree. New `handoff/PLAN/START-HERE.md`; 5 old handoff pages deleted.
- **T438 done:** the stale 1.4 GB agent worktree removed (clean, merged, unused).
- **All 10 audit fix tickets (T429–T438) done.**
- **T335 done:** `archive/` is gone (the old v1 code; any file a comment names is still readable from git history,
  as README says). The 4 old docs that code still points at moved to `pipeline/docs/`. About 25 GB of old side builds
  and scratch removed; the two still needed for T322 / T326 kept.
- Next on the list: whatever chat 2's heavy run leaves (still running its Mac deliver), then the APK batch when you
  say "build".

## 2026-11-05 (late): chat 2 started — launch prep, CI back on
- **T416 proof is running** (fresh clone, then a Mac build at the same commit; verdict in `~/t416-repro.json`, ~23:15).
  It holds the heavy slot, so everything below is light work.
- **CI was red since 10-29 and nobody saw it.** Fixed:
  - app: the loading-fish and splash pictures were saved with bytes that differ by computer, so the check failed on
    GitHub. Same pictures, now saved the same way everywhere (1b6cd7283). CI also uses the Mac's Node (25b07f669).
  - pipeline: 29 tests read local build data without saying so, and some needed tools or git history (897984554).
  - Waiting on GitHub to prove both green.
- **T86:** a production build with no contact address is now refused.
- **T240:** secrets list written (`handoff/PLAN/runbooks/secrets.md`). One GitHub secret, `CLOUDFLARE_API_TOKEN`, is used
  by nothing: yours to delete or keep. The prod configs and scripts are T404's, for when prod is near.
- **T244 / T371:** the store privacy draft now lists the install id and the report's map centre (the privacy page
  already did). Apple "Device ID": a proposal for you. T371 now says promote **0.6.2 (build 11)**.
- **T409:** water pages no longer build the hidden map until you go back to it, at every width (your call: desktop
  too). Code + tests in (a2946f62e, 99574d8c8). The score measurement waits for the heavy slot.
- **T331 closed; T422 done** (5820c2ae4): the review app's sandbox is now one folder (`CURATED_ROOT`) instead of four
  path settings; the old settings are refused if set. How you start the app is unchanged — **restart it after pulling.**
  Side builds made before today have the old layout.
- **CI:** pipeline green (first since 10-20). App: 38 failures were BC-time vs UTC (all tests now run in BC time); the
  last 3 fixes are on GitHub now.
- **CI green on both** (app + pipeline) since c1ae5d734.
- **T421 done:** without a build, the review app now says so instead of calling your marks "binding moved".
- **T304 done (code):** a water page names its region, so a landing loads that region's files, not the whole province.
  The next staging site deploy needs a full SEO build (site.sh now refuses old pages). Speed measurement owed.
- **T410 partial:** web-only and phone-only code now sit in their own files (27 → 35 pairs), and a check refuses new
  mixed code. Owed: the bundle listings + one full check (heavy), and a phone pass on the next APK.
- **T366 done:** the review app shows when DFO rows are older than the pages (DFO mode), and the worker sends DFO row
  changes in the daily digest instead of at once. The worker ships with the next app (not deployed).
- **T319 started:** store screenshots in every Play / App Store size from one command (`node app/tools/store-shots.mjs`),
  framed with a title and a line. The titles are a proposal for you (`app/tools/store-shots.json`). The saved-spot shot
  needs the phone build (the web shows "get the app" there).
- New: T421, T422.

## 2026-11-05: 0.6.1 on Play internal
- **0.6.1** (9e610a6f2, versionCode 10, 40741d6de): the T411 highlight fix. Tester email sent; play.json published.
  T417 (watershed flicker) and T418 (style assets) go in the next APK.
- **Staging feeds worker** deployed with it (50a816d9): one wording copy (T324). The gauges, DFO and in-season jobs ran
  clean. In-season links re-synced: same 32 links, new checksum.
- **Your phone check:** Coquitlam → Map → Downstream to the bottom → Upstream once. The highlight should be one line.

## 2026-11-05 (late night): T416 done — a fresh machine can rebuild the live release
- The fresh clone, built with the pinned libraries, matches a Mac build of the same code. The tiles are identical, and
  so are 40 of the 52 data files.
- The synopsis is identical once rebuilt with the pinned libraries.
- The water files differed only in a recorded folder path. The bundle now records it relative to the repo.
- The only real difference is DFO's salmon pages, fetched on different days. That is expected.
- Two small follow-ups are in the next chat's list: a release must refuse a missing basemap, and the synopsis cache.
- T418 passed on your phone (the tile-folder move on the test channel).
- New tickets: T423 (the channel switch looks dead), T424 (map credits: web = app, no overlap with the scale bar),
  T428 (one sync control in More).
- T420 notes the lag is on the web too.

## 2026-11-05 (night): 0.6.2 on Play internal; chat 1b done
- **0.6.2 (versionCode 11) is on Play internal.** It holds T390 (a new version shows as soon as it is out), T417
  (watershed conditions don't blank, and follow the watershed most on screen) and T418 (the map keeps its labels and
  symbols while an update applies). `play.json` names 0.6.2 build 11. App check before it: 8,662 tests pass, 0 fail.
  - Your phone checks: in each ticket.
  - T417/T418 were not tried on the emulator.
- **Staging feeds worker deployed** (version ab249a19). All jobs ran ok.
  - Your GitHub token works: the worker read the HYDAT workflow with it (200).
  - The weekly NuSEDS check runs. Nothing is new yet.
- **Prod feeds worker placeholder** `bc-fishing-feeds` exists, with its 4 secrets (you ran it). It is for T404.
- **T416:**
  - A fresh clone builds with no hand steps: about 7 min of bootstrap, then about 1h15 of build. Runbook:
    `handoff/PLAN/runbooks/fresh-machine.md`.
  - It is not yet byte-identical to live. Unpinned Python libraries (now pinned in `constraints.txt`), today's DFO
    pages, and newer code explain it so far.
  - The fair proof is running now: the fresh clone vs a Mac build at the same commit, ~1.5 h. The verdict lands in
    `~/t416-repro.json`, and the next chat closes T416 from it.
  - Gap found: a fresh machine's release ships no basemap, and nothing says so. To fix in T416.
- **New tickets:** T419 (Salmon River simplification), T420 (map panning laggy on a Pixel 9, on 0.6.1).
- **Next chat:** Launch and hygiene (`NEXT-SESSION-after-0.6.0.md`, Chat 2).

## 2026-11-05 (late): Chat 1b — T416, T412, T413; next APK batch started
- **Pushed:** every commit is on GitHub (one at a time; no "commit_refs" errors this time).
- **T416 (a fresh clone can build):**
  - The big inputs that are not in git (the 10 GB map data, HYDAT, fish obstacles) are now copied to the build-assets
    bucket, each under its own fingerprint (`data/source/pins.json`). The basemap was already there.
  - One command sets up a new machine: `bash scripts/bootstrap.sh`. On this Mac it took ~7 min to download and check
    everything.
  - `fetch_data.py` now fails when a layer fails (it used to say "complete").
  - The timed build from that fresh clone is running now (atlas first). The result goes in T416 and a runbook.
- **T412 (run timing):** one publisher now, its GitHub workflow. Each release ships only the map data run timing
  needs. The staging upload starts the workflow and keeps uploading, then switches the app to the new release only
  once run timing for that map is up (your "in parallel, then wait" answer). The worker checks DFO's NuSEDS weekly and
  starts the workflow when it changes. Never a GitHub schedule.
- **T413 (HYDAT):** the worker starts the HYDAT workflow itself once you add the token (OWNER-TODO has the steps).
  Without the token, the email still names the button. Both workflows can now be started from the dev branch, so
  `main` stays untouched.
- **Worker changes (T412, T413) are not deployed yet.** They go out with the next APK, as usual.
- **Next APK batch (in code, not built):**
  - T390: the app reads our play.json every time it opens or comes back, so a new version shows at once. Update uses
    Play's flow, or opens the Play listing while Play catches up.
  - T417 + T418: in progress in a second lane.
  - Say "build" when you want the APK.
- **New ticket T419:** Salmon River (Region 2) is not simplified enough when zoomed out. It is checked with the UI work.

## 2026-11-05: data flow walkthrough done (T408)
- **Every stream decided.** Your answers are in `handoff/PLAN/data-flow-decisions.md`. In short:
  - `decided/` goes; marks write straight to the final file.
  - Hand-edited waters files get a seconds-fast check.
  - Machine-made files leave curated.
  - DFO matches are final, and the build fails if one breaks.
  - Worker wording has one copy.
  - Worker inputs sync to staging on push.
  - Run timing becomes a "slow feed" workflow (T412), and the worker triggers HYDAT itself (T413).
  - One `pipeline release --staging` command, the same for you and me.
  - Curated files get clearer, consistent names.
- **Tickets:** T324, T325, T266, T295, T297, T337 re-scoped to those answers; T412, T413 new.
- **T164 (salmon):** the dossier is clean and every live DFO rule lands on a place. Your part: 179 places still need an
  extent. I refreshed each proposal against today's build (OWNER-TODO has the steps).
- **T178:** waits for the worker's first monthly score on 2026-12-01.
- **T239 done:** the yearly-synopsis runbook (`handoff/PLAN/runbooks/annual-synopsis.md`) and a book-vs-book diff.
  - The dry run spent no credits: 1,512 of 1,515 entries came out identical. The other 3 are explained (two hand-fixed
    texts, one extraction regression → T415).
  - A new book re-parses only changed rows, so your curation carries over.
- **`decided/` is gone (T266/T325):** marks now go straight to the final file. The apply dry run is unchanged.
- **T337 done:** each build checks every DFO place's water against the map and stops, naming the place, if one is gone.
  Today all 191 resolve. No manual matching step remains.
- **Reports and crashes read (since 11-04):**
  - Your six 0.5.0 notes were already fixed (T393–T398).
  - One new 0.6.0 note: the watershed conditions flicker, and the app should pick the watershed most on screen → T417.
  - The 0.5.0 reports all logged map sprite/font load failures; 0.6.0 doesn't so far → T418, to check.
  - Crashes are all from 0.3.0–0.5.0 and already fixed (T391, T403). Two background low-memory kills are normal.
  - No 0.6.0 crash yet.
- **T413:** no GitHub schedules (your call). The worker's email is the trigger today. With a token you make, the
  worker starts the HYDAT workflow itself. The same applies to run timing (T412).
- **Release command built (T295/T297):** `python -m pipeline release --staging` runs build → checks (binding diff, app
  gate, manifest) → promote → staging upload. It stops on a failure and resumes where it stopped. Prod is a separate
  human-only command. Runbook: `handoff/PLAN/runbooks/release.md`. Not run end to end yet.
- **T324 done:** curated files renamed (one consistent naming, each with an `_about` and a folder README). Machine
  files moved out of curated. Hand-edited waters files are checked in under a second as the build's first step.
  Worker inputs sync to staging on push. `data-flow.md` is rewritten. The renames rerun no atlas: the build now hashes
  content, not paths. **Restart the review app after pulling** (it reads its paths at start).
- **Ticket audit applied** (`ticket-audit-2026-11-05.md`); both next-session lists rewritten.
- **T416 first fetch: a fresh clone cannot build today.** The government's FWA download (the core map data) answers
  "file not found". The Mac's 9.7 GB copy is the only copy. Five other inputs have no fetch path at all. Details in
  T416.
- **staging-security.yml:** button only (your call). No workflow has a GitHub schedule now.
- **T411 fixed in code** (0.6.1 batch): stepping between parts no longer leaves the highlight's copy on an old zoom's line.
  The emulator scenario failed before the fix and passes after. Your phone check comes with the next APK
  (say "build").
- **FWA is not lost:** it moved to the government's cloud storage. The fetch is fixed, and a fresh fetch of every source
  works (about 55 min).
- **This chat is done.** The next chat starts from `NEXT-SESSION-after-0.6.0.md`: T416, T412, T413, then launch and hygiene.

## 2026-11-05: 0.6.0 built for Play internal; handoff ready
- **0.6.0:** version + changelog committed (1590e08d6). On Play internal: versionCode 9 (97459e986), play.json
  published, tester link emailed. Then 2–3 days of internal testing → Closed testing (T371).
- **Staging feeds worker** deployed with it (b960604d): "App bug" / "Feature request" reports (T407), the monthly
  forecast score (T178), the version notices. All three feed jobs ran clean.
- **Website speed (T409):** water pages 77–83 (were 40–45), hubs 100. Web-only. The rest is the map warm-up (left, owner).
- **Next chat:** `handoff/PLAN/NEXT-SESSION-after-0.6.0.md`. T408 first (walk you through the data flow), then data and
  build, then a second chat for launch and hygiene (T404, T410 …).

## 2026-11-05: the order to 0.6.0 (owner)
- **T345 closed** (it was fixed: fdf938745, 8a5b5bdf1). A quick phone glance at Coquitlam is in OWNER-TODO.
- **Now:** crash reports (item 2). Your phone sent 2 non-fatal errors on 0.5.0 right after install: the data release on
  it (r-4e70…) is older than the app reads (no `section_line`, old salmon format). A lane is finding why and fixing it
  [T403], plus keeping source maps per release [T352]. No 0.5.0 native crash yet [T351 waits].
- **Then, in order:** map feel + performance (T312, T287, T245, T360) → SEO (T188–T198) → iOS (T88, T313, T336,
  T362–T365) → **0.6.0 release** (with T234 notifications, T390 update prompt and the 0.5.0 fixes).
- **T234 notifications v1.1 + changelog built** (665dcb49b, 3dbc91380, 981c60cf5): above/below/±% in a day, one per
  water per day (or a set gap), More → Notifications cards + editor, new-version notice, "What's new". Phone checklist
  in the ticket; your calls in OWNER-TODO (changelog drafts).
- **Crash reports done** [T403, T352]: your phone kept the 0.4.0-era data (r-4e70) through the app update, and 0.5.0
  opened it before fetching the new one. Now a release the app can't read is never opened: the current one installs
  first, like a first run (offline: the "needs a connection" screen). Source maps are kept per build, never served,
  and a tool turns a crash's stack back into file:line. Reaches the phone with the next APK. [T351] still waits.
- **Alerts: a crossing the limit blocks is dropped, never queued** (your call, e885af5e9).
- **Map feel + performance done (emulator)** [T312, T400 done; T287, T360, T245 partial] (d91f300f6, 2bdcc1931):
  flings glide as far as Android's own, a drag tracks the finger; an update no longer stalls the map (same-files
  case); memory down 15–55 MB per screen. Your calls in OWNER-TODO: salmon decoded only when chosen (−16 MB, first
  toggle +0.7 s); z8 pans at 29 fps (lighter = visual change; measure on a low-end phone first).
- **SEO done on staging** [T192–T195, T197 done; T196, T188 partial; T198 yours] (21a2d17ff, b1a5c7fa8, c237502d3):
  19,927 pages, sitemap clean, every old link one 301. Water pages score low on Lighthouse (the app taking over the
  page shifts it) → T405. Your steps (www rule drops the path, Search Console, Bing, prod deploy) in OWNER-TODO.
- **iOS lane done (simulator)** [T313, T336, T362 done; T363, T365, T88 partial; T364 yours after the hygiene chat]:
  11/11 simulator scenarios on 0.5.0; walkthrough card no longer hidden behind the Layers sheet; iOS crashes reported
  on the next start (same path as Android); privacy manifest; signed-build + TestFlight script ready for your Apple
  account. Memory 147–273 MB footprint, lower than Android. Your steps: OWNER-TODO "iOS".
- **Also 11-05:** Report a problem takes "App bug" / "Feature request" [T407]; salmon memory 15.8 → 6.3 MB, same files
  [T360; 6.3 MB is fine, no lazy decode]; changelog 0.1.0–0.5.0 approved; no lighter z8 map [T245].
- **Now:** SEO speed on staging [T405, T406]; then the 0.6.0 release.
- **0.6.0 rollout plan (owner 11-05):** internal testing 2–3 days (phone checks in OWNER-TODO, a gauge alert, one
  "App bug" report) → if clean, promote the SAME build to Play closed testing (T371); else 0.6.1 internal first.
  Closed testers read staging until the prod switch (T404), which follows the owner's DFO label review.
- **Next chat starts with T408:** an agent walks you through the data flow one question at a time, records your
  answers, then simplifies the pipeline to match (T324–T326, T266, T295, T297, T337).
- **Running:** map feel + performance; SEO (T188–T198) started.
- **Deferred, thought later:** T383, T389, T304, T401 (waits on your recording), T307/T308/T310 sign-offs.
- **Next chat after 0.6.0:** data + build (T164, T178, T239, T266, T295, T297, T324–T326, T337) and launch + hygiene
  (T86, T240, T404 one switch to prod + runbook, T244, T371, T305, T306, T331, T341, T402, T333–T335).

## 2026-11-05 (after 0.5.0): reports read, web crash fixed, phone map reports in a lane [T391]
- Reports read (staging worker): 1 problem report (owner's, 11-02, already T348, done). New crash kinds since T346:
  - **T391 DONE (191953b4b):** every web water page filed a FATAL "Requiring unknown module" (9 reports, 5 installs,
    0.5.0; also 0.3.0). Metro reports a lazy chunk's first miss as fatal while ErrorUtils is set; the import now asks
    without it. Verified on the live page with the fixed bundle swapped in: no report, chunk loads, page shows. Live at
    the next staging site deploy.
  - "not a dfo-feed/3 feed" (10, non-fatal, 21:28–21:34 11-04): 0.5.0 site up before the feed was rewritten by hand.
    A sequencing slip, gone; no ticket (production rule already: feeds worker ships with the app).
  - 0.4.0 native crashes 11-01..03: before T360's memory fixes; T351 waits for a 0.5.0 report.
- Owner 11-05 (phone, 0.5.0): highlighted part drawn off the river (pink/yellow lines crossing, Coquitlam part 1),
  choppy fly-to, too little stream buffer outside the view when panning. Lane done (adfbe330e, app code only):
  - **T399 fixed:** the coarse water copy drew over the river past its own zoom (one ceiling for the whole copy); each
    section now has its own. Owner checks Coquitlam part 1 on the next APK.
  - **T400 partly:** camera fits now ease instead of the 450 ms "fly" arc; extra re-fit rounds on search arrival still
    to measure.
  - **T401 open, owner decision after measuring:** MapLibre Native loads only on-screen tiles and has no prefetch
    setting; options are a patch, drawing past the screen edge, or leaving it.
- Test-bed drift (c7ac3e9ac): gestures opens "Sources and notes" first; water-nav logs the probe's hit id. Verified on
  the emulator with the next APK (APK held for the batch).
- **T392 DONE (6f0a04a1c), owner 11-05:** a feeds worker release now reruns the feeds. The deployed version is part of
  every job's reuse key (no more hand-bumped GAUGE_REV), and `deploy/staging/feeds.sh` deploys then runs all three jobs.
  Deployed: staging forecasts recomputed (all stations), so the new forecast detail is live on staging now.
- Owner's 6 reports from playing with 0.5.0 filed (ed8aef131): T393 sizes on species rows, T394 spawning image on quota
  rows, T395 vessel tab, T396 legend 2 lines / no jump, T397 BUG Runs chips don't scroll, T398 ELF-only → real-time,
  forecast always switchable off.
- **T397 fixed in code (f2f726456):** the map's bottom block hung from a zero-height row, and Android only hands a
  sideways drag to a view inside its parent's box, so the Runs fish row never scrolled (taps still worked). Moved inside
  the map area; a test guards it. Phone check with the next APK.
- **T390 PARTIAL (0798fd27b, c62aeb4b8, c0e416056):** the Update button is written: package `expo-in-app-updates`
  0.13.0 (Expo module, web stub, no extra native deps; built for SDK 58, so the next APK build proves it on SDK 57). The
  bell has an "App update" section; a release refused for minAppVersion now says "New data needs the latest app." with
  the button. Sideloaded staging builds point to the staging APK page (build-apk.sh writes latest.json). iOS stub
  until T88. Owner decision: Play restarts the app itself after the download; a separate "Restart to update" step
  needs a Kotlin patch. Phone checks with the next APK.
- **T377 DONE in code (lane):** tappable legend key 8658433c3, collapsible bar f42d32696, Runs peak dots aea5620a1,
  closed streams' dashed stripe c1114b75d, several forecast models at once 6326964ad. Readings to confirm: OWNER-TODO.
- **T234 PARTIAL (phone-only v1, owner 11-05):** expo-notifications (local only) + expo-background-task (~15 min):
  gauge threshold alerts checked on the phone (once per crossing), "new regulations data ready" once per release; a
  Notifications section in More (owner ask); "Alert me" on the gauge card (13b963619, 77790b907, 8b7b45866, dd0a49dde).
  10-step phone checklist in the ticket.
- **T402:** the pipeline flake fixed (93d274234: a temp marks path stuck at import). Quiet full `pnpm -C app check`:
  8564 passed, 0 failed — the other failures were load from two parallel lanes.
- **SDK risk settled:** no expo-in-app-updates release targets SDK 57 (0.12 → 56, 0.13 → 58); a full release APK with
  0.13.0 builds on our SDK 57 (local, not uploaded).
- **T394 DONE (77e738b54):** salmon species rows show the spawning look (T343 item 6), not T318's ocean one.
- **T393 DONE (bfc758925):** every species row (closed too) carries its own DFO size line; the region block drops the
  size notes a row says (Region 2's five leave the block on gnis:14589).
- **T401 measuring:** new test-bed `edge` scenario (9ab90059f) times how long a pan's new strip takes to draw, and
  whether the wait is tiles or colouring. Running on the emulator with gestures + water-nav reruns.
- **T398 / T396 / T395 done in code (lane):** T398 (f2796e37f) "Real-time only" in the chart's model control on every
  gauge; an ELF-only gauge (Capilano) opens on measured data. T396 (8d1500036) both legends two fixed rows, same height
  (360 dp phones may still wrap). T395 (65e8bd6d7) vessel restrictions → a Methods-tab section; owner questions in
  OWNER-TODO. All need the next APK.
- T401 first run (slow drag, river zoom): no pop-in on the emulator — the first frame had every stream, only offset by
  the camera still settling; matched at ~450 ms. Fling + zoomed-in runs going. Test-bed fixes: water-nav tapped the
  wrong place (origin from an off-screen map's accessibility node → now the app's own rect); gestures' route map is
  above the gauge list, not in the fold (script scrolls to it).
- T399 follow-up (13dca580c): the T124 one-layer-list guard caught a per-layer object in the copy code; fixed.
  `pnpm -C app check` green (8495 passed). Graph updated.
- Full suites (post-0.5.0): app 8457 passed / 13 failed, pipeline 6268 passed / 1 failed. 12 were the ticket ledger
  (DONE rows without hashes, fixed); the other 2 pass alone (flakes under load) → T402. `pnpm -C app check` reruns
  after the T390 lane lands.

## 2026-11-04 (merged chat, night): one clock, T368/T369 landed, the 0.5.0 list closing [T368, T369, T382, T386, T378, T377, T312, T381, T387, T384, T385, T240]
- Landed: T368 (one reporter, one failure face, catch net), T369 (one load path per screen, 14 divergences, net; T379
  flake fixed), T382 (core clock.ts the only clock reader, B.C. UTC−7 written in code, sun card; spots on B.C. dates too),
  T386 (lines phone-held; an unnamed reach carried across a data update), T378 (dead block deleted), T377 item 4,
  T312 fling patch (base 450 ms, threshold 500), T240 closed-testing checklist, closed-day Licence/where-not.
- Also landed: T387 (every tap on a part gives the same page, net), T384 + T385 (open on my location; share), T178
  (worker scores monthly), T371 drafts + owner answers (Open-Meteo at 0.01°, web name "Can I Fish This?"), T377 item 1
  (forecast days by watershed: browse = map-centre group in the legend bar, a water = its own group; 5b69686c2).
- Release r-b86b60ca533b built + promoted (reach 11m37s, deliver 11m07s; vintages agree; test_release 24/24; app
  release tests 4202/4202). Request walk re-cut (08b263e): it caught ~2.5 MB of SQLite on web FIRST PAINT (T293's
  walkthrough water search + T369's address store at boot + two whole-water routes) — all fixed, a test guards it.
- Owner decided (11-04): the web's `lines` (1.2 MB) only for unnamed reaches → T388.
- T65 DONE (0d5d3577f): the atlas is deterministic (two builds byte-identical but timings, build_ts, a pickled
  set's order; graphs equal).
- T388 DONE + live (r-d458fbe165ac): named waters' link place from `part_line` (+79 kB gz water), never `lines`.
- Staging: r-d458fbe165ac uploaded (minAppVersion 0.5.0), app 0.5.0 site deployed (SEO runner aliases now read from
  package exports), feeds worker deployed (T347/T375/T380/T178 cron); smoke 18/18 (gauge check moved Pitt → Vedder:
  the Pitt's one freshwater part has no gauge since T369).
- Licence guiding folded; page foot as fine print (owner 11-04).
- 0.5.0 SHIPPED to Play internal testing: versionCode 8 (AAB 981398053, 34.2 MB); the owner promotes it to Closed
  testing. Release r-5a5999c94588 on staging (stamp glossary entries, "or Dolly Varden"); staging site redeployed;
  DFO feed refreshed to dfo-feed/3 by hand (the worker's daily run); test channel reset. Licence sources sheet removed.
- Emulator (out/r050b): install + regs-open PASS (T340 fixed: closing search restores the camera, T48 — the scenario
  now opens the water from its result); gestures / memory / water-nav = test-script drift (handoff lists them).
- Lean pre-release tests green (owner 11-04: lean, full suites right after release).
- Owner 11-04: full suites skipped before release, run right after it. Handoff for the next chat:
  `NEXT-SESSION-after-0.5.0.md`. T389 filed (unnamed links without `lines`, after 0.5.0).

## 2026-11-04 (merged chat, evening): owner answers in, T368/T369 lanes, T370 done [T368, T369, T370, T375, T360, T77, T381]
- Owner answered T368 (9) and T369 (3): recorded at the end of each audit file; two agent lanes implement them now.
- T370 DONE in code: `water-shared` beside the slices (nine regions 7.1 → 2.0 MB gz); conformance passes on a cut made
  from the live release. Live with the rebuild; re-pin the request walk then.
- Owner asks: Licence section + tab gone on a closed day (c574bef9e, 6fa905b56); T381 filed (Nicomen Slough: a water
  with no splits is one section — pipeline, not started).
- T375 feed size estimated (+5–11 kB gz); T360 Salmon step fixed (by id); T77 → OWNER-PHONE.md.
- Not started: T50 + the second emulator sitting (fling patch, T340, T342/T344 checks, T360 viewer check), T65 (heavy),
  T239, T240, T381.

## 2026-11-04 (merged chat, later): batch 5 before-run, 6c audits, owner asks [T380, T328, T370, T368, T369, T340, T312]
- Owner asks done: T380 (Change says "from the 5th percentile today to about the 95th (about 3× today's flow)"; publisher +
  worker carry `change.percentile`, live with the worker deploy) 528620c65; web map hand cursor over clickable
  things (2efd849eb); closed water: stretch list folded, "Where you can't fish" not said on a closed day (cc169c9d6).
- Batch 5: emulator before-run app/tools/testbed/out/b5-before (pan numbers; T340 stream taps fail — camera not framed
  on the water; pinch not driven: emulator refuses sendevent). Fling tuning + T340 fix next sitting.
- 6c: T368 + T369 read-only audits in handoff/PLAN/app-integration/T368-audit.md / T369-audit.md; owner questions
  pending (9 + 3), fixes wait for them.
- 6d/6e: T328 DONE (1a27dd3af); T178 checked early (staging archive, not due ~12-05); T370 measured (shared 623 kB of
  each slice; recommendation after 0.5.0, owner decides). T50/T65/T77/T239/T240 not started.
- Release rebuild still owed before the cut (bundle → release): T372 `n`, T376 tables, T339 names.

## 2026-11-04 (merged chat): T367 closed, T372 done [T367, T377, T372, T378]
- **T367 closed** in the ledger (95b633c38). The full app suite had 8 reds left from it; all fixed (back button closes
  the forecast day list, the PDF viewer's ground colour, the "couldn't load" words moved to core, and so on). What
  T367 left open is now T377 (the forecast-day list by watershed, after T376; the rest after 0.5.0).
- **T372 done** (777a4fc2b). The salmon card now works the trout way. It keeps the answer for the drawn water and
  lists DFO places that aren't on the map in the same "Fishing is different in some spots today" block trout uses,
  with DFO's words for each place. Places DFO speaks for that day come first. It no longer turns rows into "check DFO".
  Python decides which days each place is in force.
- **Needs a release rebuild:** the local release r-ac5a4e4f734c was built before this change. Until the next build,
  the dev app shows an error on waters that have an undrawn DFO place. That rebuild is part of the batch 7 cut, or
  sooner if you want to check the card.
- New ticket T378: trout's "couldn't be placed" block never gets data (found while doing T372).
- **T347 done** (61f819c52). River forecasts now stay current over weekends and BC government holidays. A run lasts
  until the next working day's issue plus 12 h, so Friday's run lasts until Monday evening. Before, the feeds worker
  dropped it after 36 h, which blanked the forecast map on Saturday evenings. It takes effect when the feeds worker is
  redeployed with 0.5.0. More → Sources now says forecasts are updated on weekdays.
- **T348 done** (9907af3c0). The legend's "N reaches coloured" count now updates after a pan or zoom on the phone.
  A new feed already repaints the map in place; that now has a test.
- **T376 done** (e21cdbeac). The release now names all 246 watershed groups as sourced. The Conditions map knows which
  basin its centre is in (`col.centreBasin`), and a water knows its own basin (`useWaterBasin`). This needs a bundle
  rebuild and then a release (about 42 kB more, conditions file 915 kB of its 950 kB budget). Until then, a release
  build from the old bundle stops on purpose. The forecast-day list that uses it is T377.
- **T339 done** (in e21cdbeac). Gauge names now come from HYDAT: 08MH044 reads "Fraser River at Albion", and the
  Courtenay gauge "5th Street Bridge". Bedford Channel losing the Red Pass gauge is correct.
- **T342 done in code** (e604eb5ae). You can copy any text on the panes (regulations, conditions, sheets, More pages,
  spot screen) with the phone's own long-press. Buttons, tabs and the map are not selectable. The salmon-only Copy
  button is gone.
- **T344 done in code** (09d2d845e). Tapping a group such as Whitefish opens one swipeable page per fish, showing
  "1 of 2".
- T342 and T344 still need a check on the emulator; that happens in the batch 5 emulator sitting (T312 map feel, T340).
- New ticket T379: one WaterMode test fails on and off. It did so before today's work, and it has to be fixed before
  the batch 7 full suite run.
- **Release rebuild owed** before the cut: bundle (T376, T339) → release (T372's salmon `n`, T376 tables).

## 2026-11-04: chats merged for 0.5.0 [T372, T367]
- **One chat from here:** the UI chat and the pipeline chat merge. Start from `handoff/PLAN/NEXT-SESSION-0.5.0.md`.
- **T367 done** (owner call; last commit d3577f2e3). The new chat marks it in the ledger and runs the full app suite
  once.
- **T372 part 1 committed** (4a7cb4a1d): a DFO place that isn't on the map no longer paints its water "check". The map
  shows the drawn answer, the trout way. Part 2 is next: salmon reuses trout's warning block, so the card names the
  unmapped places.

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
