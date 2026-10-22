# Implementation progress (IMPLEMENT-HANDOFF.md)

Restart here. Update this file after EVERY step: what is done, what is running, and the exact next action.
Logs from timing runs live in the session scratchpad, which a reboot wipes. The numbers that matter are copied here.

## CURRENT STATE (2026-10-22 ~15:20) — READ THIS FIRST ON A RESTART
- Branch redesign/stream-sections (NEVER main). REFINEMENT PASS MERGED (map-style 48c4fa3c..6b5da3ce, legend+scrim
  a25685e3..5a6399c1, More+sync e7ac4eb8+1086aa45, integration fix 7af2880); strict check 7609 pass + smoke 18/18 green.
  wf_259c396f worktrees removed.
- PANELS LANE MERGED (wf_ac3c87d1-cf6): every gauge per quantity (MEMBERS_PER_QUANTITY=8; CORE_MEMBERS=4 core, the rest a
  reserve that may answer but never silences the core). Real data: Flow 100,020→100,095, Level 107,296→107,373, 0 losses.
- DONE: rebuild → release r-98ded5f443e1 live (13,675,295 B gz; paint 942,794); roster refreshed 23b7309; dev fixture
  re-cut ac8d56a; strict 7626 green. Live feed published to data/generated/gauges/feeds with --clim (432 answered, 422 with
  a percentile). NOTE: the local feed goes to data/generated/gauges/feeds WITH --clim; never --out the dev fixture dir.
- MERGED: spots switch in Layers + spot pins on every map + layer choice persisted (2e77f1af d5bea71a); "not a real-time
  gauge" from feed `listed` (70fe7ac3). Full check + smoke pending with the next lanes.
- TILE REBUILD PENDING: parcels from z11 + basins coverage simplify (100 m), batched with S2 tile-size work.
- NEXT: app reads feed `listed` ("not a real-time gauge"); spots on/off in Layers; T51; T52; Fable consistency auditors;
  cleanup waves 2-4 + app test cleanup + knip; S2; determinism proof (heavy.sh); T50 Android.
- MERGED: per-feed staleness table core/staleness.ts (869512ba), dark water names #2498FC (8a5e5130 684b6f61), release
  valid_until from the synopsis cover (41b4d232 a9c41c21 a5492dec) + integration fix 809bfdb.
- valid_until stamped (2027-03-31). FOUND + FIXED 1c2b671: the release id hashed only the files, so a manifest-only
  change kept the same id (apps would never refetch). RUNNING: deliver-id.sh (re-deliver under the new id + fixture re-cut;
  log scratchpad deliver-id.log) — commit the new dev fixture dir afterwards.
- MERGED: silent donors hidden + one line with last data (00d17da4..a4af7e52 picked; reviewer fixed 3 defects).
- RUNNING: wf_e3d5fe4e-dcc (owner: back steps out ONE layer; moves inside a water replace history; close to main);
  wf_54c90876-8ac (owner: desktop search close — Esc, outside click, panel close, field X clears only). Both touch
  navigation: merge back first, then reconcile search's back layer.
- RUNNING: wf_d528f135-885 (owner: slough/canal stream-item polygons take the stream Conditions look; Nicomen Slough,
  Vedder River; may need deliver/tiles — lane says what; finding the remaining polygons stays postponed, ticketed).
- QUEUED T53 (owner: unnamed stream shows no regulations): app-only — frameViews/openWater/WaterScreen gate item:null
  to Conditions; data answers all 1,893,082 unnamed sections. Start AFTER the back lane merges (same files: openWater,
  route.ts). Live release now r-66f94fa635c1 (valid_until; id covers manifest); dev fixture a44b232.
- RUNNING: wf_b88d9e03-10e (owner: HYDAT
  maintenance notices — detect, notify once via FEED_NOTIFY_URL, scripts/update-hydat.sh, app HYDAT line follows the
  notice; decisions.md "upstream updates the owner runs"). THEN: merge both, full strict check + smoke once.
- FOLLOW-UPS noted by lanes: Forecasts line on More needs feed-wide forecastIssuedAt in publish.py; HYDAT row could use
  index hydat.stale; dark water names now near status.base blue and flow f5 (owner may want to see).

## EARLIER STATE (2026-10-22 ~11:00) — READ THIS FIRST ON A RESTART
- Branch redesign/stream-sections. Live data release r-f734cbb93724 (option A side channels + own gauge lead; served by
  `pnpm tiles` from app/ on :39217 — restart it if down). Strict `REGS_STORE_REQUIRED=1 pnpm -C app check` 7466 + smoke
  green at 36ec882a. Owner answers for this round: decisions.md "OWNER 2026-10-22 — answers".
- RUNNING wf_70e5b37a-2d5 (script workflows/scripts/finish-nav-colours-regs-polish-wf_70e5b37a-2d5.js, session 368da779):
  T48 nav (top search + sync line; bottom bar Map · Conditions · Spots · More; rail Layers only), T47 part 2 (palette D,
  more distinct, applied + gate), regs polish (R1 spacing, sheet kickers, report link once, Done, paper tint — Claude
  decides). Resume with Workflow({scriptPath, resumeFromRunId}); merge each lane (cherry-pick, conflict-marker grep,
  fixtures if VMs moved, strict check, smoke, remove worktrees).
- AUDIT DONE: handoff/PLAN/audit/PLAN.md (pipeline diagram; cleanup waves 1-4 = 15/21/11/14 items; tickets N1-N23;
  13 owner calls in §4) + audit/icons-compare.html. The mass cleanup works from this plan. (was: wf_bdacabdc-f09 (script workflows/scripts/audit-cleanup-pipeline-icons-wf_bdacabdc-f09.js):
  app cleanup finder, pipeline data-flow audit, docs/tickets audit, bottom-bar icon directions; each verified; output
  scratchpad audit/PLAN.md + icons/compare.html (copy into handoff/PLAN/audit/ when done; show the owner).
- OWNER 2026-10-22: ICONS = Lucide (ISC), rounded, 1.75 stroke (decisions.md; an earlier "T47 option C" reading was wrong
  and is corrected — T47 stays palette D from B). After the T48 nav lane merges: swap the bottom bar / rail / search
  icons to Lucide (pinned package or vendored SVG paths + licence in credits), test, shots.
- CLEANUP SCOPE decided (decisions.md "OWNER 2026-10-22 — the audit plan"): waves 1-4 with the owner's corrections
  (keep stocking/bathy/salmon code as "prepared"; prove every script unneeded before deleting), the calls 1-12, a big test
  cleanup, and the ambiguous-cut rule (nearest the other extent; reach rerun + deliver; diff bindings first).
- DONE while the owner was away: scripts/heavy.sh in the repo (65240529); disk cleanup — 12 scratch builds + r9live/r10live
  + 2 logs deleted from data/generated (79 → 158 GiB free; nothing in code referenced them). Old releases kept until N10.
- RUNNING wf_3c34e85f-9c5 (task ws1zwh53z): pipeline cleanup lane A (wave-1 deletions + answers-reference archive + pipeline
  test cleanup) and lane B (ambiguous cuts nearest the other extent; continuation into the bundle; deliver key inputs;
  matcher out of the bundle; atlas records the matches sha). Merger then: reach rerun + deliver all via scripts/heavy.sh,
  binding diff before promoting.
- MERGED: add-a-spot (dbdf8a68: my location / pick on the map) and the lead-up refresh fix (6630879d: still-to-come
  days refill; unpublished stations settle). FEED COVERAGE: cause = the roster (data/source/bc_hydrometric_stations.json, fetched once
  2026-09-01) drifted from ECCC: 13 "live" stations are no longer real-time, 6 "silent" ones now transmit. FIX merged
  (publish.py reads ECCC's live station list each run; index.json `listed`; drift warning). TO DO WITH THE NEXT PIPELINE
  REBUILD: refresh the roster (`rm data/source/bc_hydrometric_stations.json && python data/fetch_data.py --layers
  hydrometric_stations`), diff panel leads (08EG012/08LG028 lead ~1,200 sections as live but don't transmit), deliver all,
  then a local feed run. App follow-up: read `listed` to say "not a real-time gauge" vs "not reporting". Flakes seen under load: Search.shell sync line, apps/mobile back.test browser-back step.
- RUNNING wf_ac3c87d1-cf6 (task w022i2596): panels over every possible gauge, live choice at read time (owner). Its
  merge + the roster refresh + deliver all go together in ONE rebuild (diff panel leads first).
- NEW TICKET T52 (owner 2026-10-22): Conditions "Expected change" chip after +10 from daily forecast series (spikes
  included); pipeline carries per-station daily series politely. Queued after T51.
- QUEUE (owner 2026-10-22, spots): add-a-spot = my location / pick on map (agent running); lead-up refresh BUG (agent
  running); spots on/off toggle in Layers (after the refinement pass merges — layers registry); T51 spot similarity
  forecast (after the add-spot + bug lanes merge). Icons MERGED 5c968853+ca4b29a6 (strict 7524).
- 2026-10-22 evening: cleanup rebuild DONE (no rule binding changed; 93 ambiguous cuts recorded; Skeena curated pick;
  water_continue 17 in the bundle) → release r-81d40ad73176 live; fixtures re-cut 9b598a33; strict check 7500 pass;
  smoke: 1 timing flake in Search.shell's sync-line test (that line is being removed by the refinement pass).
  reaches/full_prev deleted. RUNNING: icons-lucide (wof4ijphk) and REFINEMENT PASS wf_259c396f-4eb (w3t8a05ep: map-style,
  legend-motion, more-sync). Consistency auditors + remaining cleanup waves follow; then S2, determinism, T50.
- MERGED (2026-10-22 afternoon, on redesign/stream-sections): pipeline cleanup lanes A+B (22 commits) + curated-pick
  f2bc9fd3; T48 nav 349968b0+3f8c277f; T47 palette D daf61b67..5473776d; regs polish 459811a8+8421137b. Typecheck clean.
  FULL CHECK + SMOKE PENDING until the rebuild chain finishes (memory). RUNNING: rebuild chain (2nd run, with the
  curated pick; expect 0 binding changes) and icons-lucide wf_312e4dbd-8c1 (task wof4ijphk).
  OWNER QUESTIONS (palette D): restricted near-black aubergine in light; base periwinkle; open no longer green.
- (done) READY TO MERGE after the running reach/deliver chain finishes (it edits curated region-6.json, which reach reads):
  branch worktree-agent-a369ec71bb77b7d8f commit 3a64d300 "a curated pick outranks the general ambiguous-cut rule"
  (Extent.cut_pick; skeena_river_2 keeps the lower crossing). Then the NEXT reach rerun restores skeena_river_2's 5
  licensing sections; run test_skeena_river_2_keeps_the_reserves_lower_mainstem_crossing via heavy.sh with ATLAS_BUILD.
- RUNNING chain (scratchpad cleanup-rebuild.sh, log cleanup-rebuild.log): reach → binding diff → swap → stamp → deliver.
- T50 (owner 2026-10-22): Android emulator parity + system bars — runs AFTER the mass cleanup (owner: "android could come
  after all the cleanup too"), as its own HEAVY run (heavy.sh, alone). AVD n1_pixel exists; SDK at ~/Library/Android/sdk.
- THEN, in order: (1) Shell slimming P14/P17 + S2 tile/basemap size wins (tile rebuild via heavy.sh, alone; promote file
  by file); (2) Fable-model consistency auditors — every route into a screen reaches the SAME screen, no duplicate menus,
  data and UI agree — then fix what they find; (3) the MASS CLEANUP (owner 2026-10-22 asked for it after the last
  implementation); (4) T50 Android emulator parity. D1 phone testing needs the owner's phone / an R2 bucket.
- Hygiene: agents must never pkill; after any web-check lane, kill leftover headless Chromes on /var/folders/*/T/regs-*
  profiles by PID (12 orphans were found 2026-10-22).

## IN FLIGHT (2026-10-20 evening) — check these first on a restart
- Heavy: (1) DONE 2026-10-20: live `deliver all` (9.3 min): card gate 0 problems, strict vintage agrees, 9 files
  promoted + release data/release/r-6960fe8201e3 (13.89 MB gz). Live Tier 2 data checks: test_release + test_part_geo +
  card-gate chain + feb29 + vintage 53/53; release-perf 13/13 (A5 5.7 ms, accepted with debounce).
  (2) RUNNING after it: tile rebuild into a SIDE folder scratchpad/tiles_next (then run the tile tests, promote
  atlas.pmtiles + atlas.meta.json + layers/*.geojsonl FILE BY FILE into data/generated/tiles; never replace the dir).
- Batch 1 MERGED (cherry-picked, conflicts resolved, Tier 1 green: vitest 861, pytest smoke 192, check.mjs 18):
  T20 803bc30d, T00 40b2c67d, T02 53ea8298, spots-no-sid 2e92ddee, P12b 22ba0b08 + review fix 9c56fa3a, P12a 386fa5bb,
  P12c 3a8ed83d, search debounce 200 ms 3f1602d0. All passed adversarial review (one major fixed: a saved spot's
  estimate named today's season). Worktrees removed.
- S1 DONE (owner 2026-10-20 ask): aa1012ad S1 (photos: expo-image-picker 57.0.20 -> documentDirectory/spots/photos,
  web IndexedDB; lead-up: 7 days Open-Meteo + station daily means placed in band, frozen at capture, refresh backfills)
  + ab0f8ba1 review fixes (last December reaches a January lead-up: feed keeps Dec through Jan, prior years whole +
  yearBand; photo refs clock-stamped). pnpm check 953 pass; smoke pass. Open minors (not fixed): draft photos named
  after the first draft id; web picker hangs where the input has no "cancel" event (Safari <16.4); unit words "m",
  "m3/s" in ui/leadUp.ts not words.ts; two web photo-store instances (an object URL not revoked on delete); 5-day
  archive lag and "most severe" weather code are disclosed deviations. Not verified on a device: picking, downscale
  (needs expo-image-manipulator), real IndexedDB, live Open-Meteo.
- Batch 2 MERGED (wf_7703a540-b9f; a network outage killed implement:p16 and fix:p11): T06 fc69e20a + c764595b,
  F9 68016392 (test_answers_reference RUNS, slow+needs_bundle, and FAILS on purpose: 72 unexplained real_daily rows on
  Kitimat River / Gotchen Lake / Phillipps Lake, needs an answers-lane ruling), P11 e635eed7 c820ea00 7a4da843 (release
  adapters, makeReleaseSource, App boots manifest -> paint -> Shell), merge fixes de2f697b. pnpm check 1076 pass, smoke
  pass. P11 review majors OPEN (see follow-up workflow): no status colours on the default path (manifest names no tiles,
  so the pair is never proven); a tile-only release never updates on native; prune leaves old tiles; no rollback when
  the new set fails to open; the app's regs reads bypass openRegsFor. Full findings: scratchpad b2_*.json (session
  368da779).
- Tile rebuild DONE 2026-10-20 (691 s): tile_version d768d6e135e62b99.ffac7c1eb56e; promoted file by file into
  data/generated/tiles (atlas.pmtiles, atlas.meta.json, 15 layers/*.geojsonl; basemap untouched; old copies in scratchpad
  tiles_prev); data/release/tiles/<version>/ holds atlas + mask + map-assets-1. Release rebuilt (same rid r-6960fe8201e3,
  data unchanged): manifest now names tiles.current/mask/assets; basemap still null (P18 gap). Slow tile tests 2/2,
  test_tiles+vintage 46, test_release 29, release-boot 5, release-perf 13 all pass.
- Batch 3 MERGED (wf_5c95925e-a40): P11 review fixes (pair proven from the drawn tiles' tile_version; native update on
  any moved part + tile prune + rollback + PREVIOUS before swap; regs reads via loader.regionDb; sql.js retry; counts on
  demand), P16 Conditions formatting (+ review fixes: local labelled times, fold, one horizon control, dead forecast
  fetch removed; web shots in handoff/PLAN/app-data/p16/), F9 ruling notes (F9-72-rows.md: both causes are page bugs
  under DECISIONS F1 / F16), T06 follow-up. Lead change 92c3b8fe: Centre disclaimer kept VISIBLE under the chart
  (legal: "wherever a forecast appears"); owner may fold it. pnpm check 1089 pass; smoke pass. OPEN: P16 native
  screenshots (need emulator); Kitimat ladder question for owner (r2 lifted for hatchery ST by kitimat r5).
- Batch 4 MERGED: F9 detectors (port.py: F1 rest item; new PAGE_BUGS F16, narrowed in review so it fires only where
  the orphan IS the cap) -> test_answers_reference 0 unexplained; P11 minors (native installs map-assets and the style
  reads glyphs/sprites from them; only a verified defect refuses a set; unproven pair says why; App map input tested;
  live colours shot handoff/PLAN/app-data/p11/live-colours.png). pnpm check 1154 pass. Old tile folder ffac… deleted.
  Tier 2 (test-full.sh, 57 min) 2026-10-21: everything green except one stale source pin (test_bundle_clim: the
  reader moved to bundle/shared.ts in P11), fixed. slow 266 pass incl. F9; card gate; release parity 13.
- G1 GESTURES MERGED (30c7d35c + 6b9e7bf3): RNGH 2.32.0; useBack (Android BackHandler, web History API,
  EdgeBack iOS-only), map policy (web: no touch/keyboard rotation, compass hidden), Sheet drag-to-dismiss, PartSwipe
  wired but OFF until P15 supplies the part order (acceptance "part swipe in browser" OPEN). Review fix: spot edit
  ends on ANY back (useOnLeave). pnpm check 1133 pass, smoke pass. Shots: handoff/PLAN/app-data/g1/.
- Tiles with MVT ids LIVE: tile_version d768d6e135e62b99.c9f9604f12bf promoted (atlas + meta), release manifest names
  it. Fix a9da3f75: the release names the PROMOTED tile version (atlas.meta.json), not the last folder by name
  (hash order). Old tile folder deleted.
- OWNER 2026-10-20 (later): gesture feel = the core/gestures.ts defaults, tuned on real devices later (no feel
  gate); the Centre disclaimer stays VISIBLE; add a Kitimat test (r5 lifting r2 for hatchery ST); lake OUTLINES must
  match their regulation colour. MERGED: 3f1a824a lake__edge follows the lake's colour mode in the shared adapter
  (native gets it too; shots handoff/PLAN/app-data/lakes/); 24252f4b + 3b27a13e Kitimat tests (r5 lifts r2 and r4 for
  hatchery ST only; never reopens the May 15-Jun 15 steelhead closure). OWNER RULINGS OPEN: (a) hatchery ST keep 2 in
  Nov-Jun (r7 lifted by curated exempts; text gives "all year" only on the rainbow line; reviewer leans lift);
  (b) whether the 2 hatchery ST use up r2's 1-over-50 for other trout (current: they do not, except ['ST']).
- Batch 5 RUNNING: T03 -> T04 -> T01 (regs store over the release, store reads, plumb) and T05 (core tap).
- OWNER 2026-10-20: "pan takes a second to colour" -> workflow map-colour-latency MEASURING options (per-tile sync
  feature-state; status as a style expression evaluated at tile layout; region-base + override expression; set-all).
  Decide + implement after N2 merges.
- OWNER 2026-10-20: the Conditions map's 1/3/5-day control is GONE (P16 review fix 2d784837 removed it) -> restore it
  AND add +10 days end to end (publisher + app): workflow horizons-10day RUNNING. Owner OK (2026-10-20): the LEAD runs the
  live feed republish after the merge, then checks +10 is in index.json.
- OWNER 2026-10-20: "Kooryet Lake — the map cannot place it yet": locate() built extents only from gauges + town
  rings; part_geo has bboxes for 19,638/19,641 items (missing: Williston, Kootenay, Shannon lakes, 0 sections). Plus
  search PREVIEW MAP (open it; tap a result -> preview; eye -> navigate). Workflow search-place-preview RUNNING.
- MERGED 2026-10-21: horizons 86f97e89 (map control back on Conditions, one shared state with the panel; +10 days
  end to end); LIVE FEED REPUBLISHED by the lead (425 stations, 363 with +10). Batch 5: T03 openRegsStore, T04 store
  reads, T01 plumbing, T05 core tap (+ review fixes). pnpm check 1562 pass; smoke pass.
- OWNER 2026-10-21: Conditions (river + spot) still off; Conditions map up/down separate, you-are-here icon,
  contribution %, trace geometry from tiles on click, zoom; chart zoom/inspect; SWIPE = between views (Regs <->
  Conditions <-> Map), not sections; Kitimat (b) ruled "+1 for steelhead, like the Vedder" (no other trout over 50).
  Workflow conditions-and-nav RUNNING (5 groups incl. T05 follow-up). After kitimat-plus1 merges the lead runs
  `deliver all` (answers change) and reruns the Kitimat/reference tests.
- OWNER RULING (a) 2026-10-21: Kitimat r5 does NOT lift z6 trout_char_quota.r7 — hatchery ST catch-and-release
  Nov 1-Jun 30, quota 2 outside. GOTCHAS.md entry added (696529f0). TODO after kitimat-plus1 merges (same test file +
  answers): drop r7 from r5's curated `exempts` (find where the export reads them: pipeline/deliver/bundle/read.py
  _rules_of), flip test DEC1_HATCHERY_ST_CURRENT to the ruling, then ONE `deliver all` for both rulings.
- OWNER RULE 2026-10-21: a stream tapped on the Conditions map or the Map tab opens the EXACT SAME water screen, only
  the front view differs (decisions.md). Today they diverge (browse Conditions reach panel vs water mode). QUEUED: start
  the unification as soon as batch 9 (T21 Keep tab, which wires WaterScreen/Shell) merges.
- OWNER 2026-10-21 (SPEC ONLY, do not build): downloadable high-res imagery CORRIDOR around a stream or one section,
  for offline use (the offline exception to online-only satellite); find free sources that legally allow offline
  storage. SPEC MERGED (handoff/PLAN/app-data/stream-imagery/SPEC.md): no free current sub-metre imagery of rural BC
  is allowed offline; usable tiers = 10 m Sentinel-2 composites (ESA WorldCover CC BY), 1-2 m LiDAR relief (open), town
  orthos (North Cowichan, Surrey...). 8 OWNER QUESTIONS in the spec.
- OWNER 2026-10-21: (1) Conditions chart flickers when switching gauges -> prefetch every donor series; (2) flow-
  direction arrows on Conditions streams (verify tile line orientation first); (3) "this year" flow gaps -> LEAD
  FOUND: 08MH001 daily has 35 rows (feed began Aug 27; no publisher runs Sep 26-Oct 12, Oct 16-18; the 30-day ECCC
  backfill only runs for never-seen stations). Workflow conditions-chart-arrows-feed RUNNING (fix gap refill +
  research this year's provisional daily source). The feed only stays current with a SCHEDULED publisher (owner).
- T49 (owner 2026-10-21): the map's day becomes a calendar icon + month-grid date sheet (one component for map and
  regs "Which day?"); after the layers-B lane merges.
- MERGED ~01:45: T49 calendar (02570d07 + 2 focus fixes), T47 part 1 measurements + candidates (459d707c, 4e0b108d),
  own gauge leads (d7ba348e, d1faab4f; report 85bec6d7). DELIVER ALL done: r-f734cbb93724 (option A + own gauge live;
  card gate 0; 13.67 MB; answers unchanged). Strict 7069, smoke, pipeline 109. T47 compare page for the owner:
  https://claude.ai/artifact/LAPkAZz8a8eFkCXuJZi37B (recommend B). Waiting on: T47 pick + its questions; own-gauge
  questions (tidal level gauges leading Level; Vedder own gauge via the Chilliwack line; far = river-size scaled; 70%);
  T49 questions (day range = this calendar year; button stays in the strip; Sunday-first). MERGED T44 825cf1a8 + review fix f2e98e0d (one WaterFrame; nothing shifts between views at 390/768/1440; header
  swipe moves only the body). Strict 7078, smoke. T44 owner questions: name size on Conditions/Map (now the 17 px regs
  row); desktop column 1280 vs ~720 for regs; Map view full-bleed vs column; wheel outside the column. MERGED Level view in Conditions layers 476b06d8 (Show: Flow/Level/Temperature; legend switch removed) and T48
  mocks d1b5680a+f733ec5e, shown at https://claude.ai/artifact/UefSumgDKKB4dWMjQZL8dj (owner picks bottom bar 1/2a/2b
  + 8 questions before T48 is built). Strict 7093, smoke. MERGED T45 919cdb6f + T46 e3332e62 + review fix 412f4bfc (regs reading column 560 px ~70 chars on wide screens, side
  column on desktop, regs sheets as centred dialogs [map sheets stay bottom sheets], fish looks side by side at max
  360 px). Strict 7130, smoke. T45/T46 owner questions: frame 1280 + 560 column OK?; date pill in the side column?;
  dialog vs side panel; landscape fish looks; small notes ~103 chars. MERGED T38 e924a347 (adipose drawings + How to tell), T42 b2e116c6+0e36a2d5 (edge states), T43 b4e5734c (book link,
  picture credits; merge fix: credits list outside pictures only). Strict 7202, smoke. Owner questions: T38 image
  before caption / no tile behind drawings / side by side on wide; T42 tabs outside B.C. / caveat look / tidal licence box;
  T43 amend D15 (permissions not a ship gate) / 18-line Fish pictures foot. MERGED T39 e4a5bef9+2ebe4055 (palette render gate light/dark/CVD; census tool) and T40 d9cb96fa+cdc1edb8 (a11y,
  200% text, 44x44, style-token gate). Strict 7446, smoke. Owner questions: dark fish cut-outs OK (D4)?; licence paper
  note tint; Keep colour bound 7; inline links stay small (no hitSlop in RN Text)?; licence radiogroups named "Age" /
  "Residency"?; ⓘ label wording; 2x text wraps tabs. MERGED T41 bf2a0dda..5dcce89d (start link ?regs=&date=&tab=&sheet=; walk 736 PASS / 0 FAIL over 23 states x light+dark;
  109 side-by-sides: 27 match, 2 deliberate, 4 fixed [possession/hatch/sources sheets], 76 open = mostly R1 spacing).
  Report handoff/PLAN/app-integration/T41-walk/REPORT.md. Strict 7466, smoke. Owner Qs: R1 Gear/Methods/Licence 15-45%
  taller than prototype (tighten?); O4 sheet kicker style; report link per tab or once; "DONE" vs "Done". Human: run the
  reader gate (compare-regs --reader) per tests.md §4. After T41: P14/P17 Shell slimming, S2 size wins, then D1 (phone).
- 00:31 restart: credits ran out ~00:00 during both fix stages; RESUMED own-gauge (task whzdwc3j1) and T47+T49 (task
  w20bqpq58), same run ids; only the fix agents rerun. Tile server up.
- MERGED option A a6e0df2e (side channels follow their own river; Widgeon 50->17 like the Pitt; 13,639 sections change).
  NOT LIVE until deliver all. RUNNING own-gauge-leads wf_3c35e1af-596 (task wwqu8q52r); after it merges: ONE
  `deliver all` via heavy.sh (alone, when T47/T49 agents are idle or done), then strict check + smoke.
- ORDER AGREED (owner 2026-10-22 "ok happy"): A -> own gauge (flow only) + deliver all -> T47 -> T49 -> T44 -> Level view in
  Conditions layers -> T48 -> T45 -> T46 -> T38, T42, T43, T39, T40, T41 -> P14/P17 -> S2 -> D1 (phone) -> native engine.
  RUNNING wf_da80e97d-547 (task w2eqvy6x0): T47 part 1 (candidates for owner pick, no token change) + T49 calendar.
- NEXT after option A merges (same panel code, one rebuild for both): "own gauge first" (decisions.md OWNER 2026-10-22):
  the Pitt uses 08MH035 (level-only) instead of North Alouette; label the quantity; check tidal swing.
- RUNNING: side channels option A (owner 2026-10-22) wf_a3a90e27-906 (task wnrcwqr2u). After merge: bundle + release rebuild
  via heavy.sh (deliver all, alone), then taps/fixture re-cut if answers moved, strict check, smoke.
- MERGED (wf_199d0858-41a): LAYERS MENU B a3b43827 (one water colouring for all features; layer registry; coming soon
  greyed; always shown incl Indigenous land [indigenous_land tile layer, 780 features]; private-land switch; palette
  -> menu Display page; credits -> map attribution + Data sources page; count badge on the rail). Small fixes:
  reversed continuations refused by FWA code 9defa9a5 (17 rows), crossing button names the water 88478f44, keyless
  Esri recorded 2b708201, netinfo 12.0.1 797bd0d5 (needs a dev-build rebuild on a device). Side-channel REPORT
  f8f71d90 + scripts d343c2b8 (Option A recommended: mainstem_of must stay on its own watershed code). Release-only
  rebuild r-8a55ff2506cd served. Strict 7028, smoke, release tests 26.
  OWNER: pick side-channel option(s) A/B/C1/D; Pitt from North Alouette only OK?; sheets full-width on desktop (T45)?
- MERGED: Esri online imagery 388e5e08 (World_Imagery, maxzoom 19, overzoom; offline -> drawn map + reason, comes back by
  itself; credit switches). Strict 6996, smoke pass. MUST NOT SHIP without an ArcGIS Location Platform key
  (EXPO_PUBLIC_ARCGIS_API_KEY): the token-free URL is dev-only. OWNER QUESTIONS: create the key (tile vs session
  billing)?; offline 10 m base still wanted?; add netinfo for instant offline on phones?; persist the Satellite choice?
  Everything else merged; waiting on owner: layers-menu pick + questions, T47 start, the question lists below.
- MERGED ~00:30: WATER MAP FOLLOW-UPS (0db23b54, a9876032, 7171e865 + test fix): current part keeps its colour with an
  outline; ▲/▼ cross waters through openWater (Vedder <-> Chilliwack); every part reachable; stepped part is the frame's.
  water_continue rule rebuilt on the atlas graph (the first rule used the gauge-shed-only section_down: 89 of 92 rows
  wrong); 18 rows now, list + verdicts in handoff/PLAN/app-data/p15/continuations.md. RELEASE-ONLY rebuild done:
  r-f579fe5eac3e (water_continue 18 rows + manifest sources), served on :39217. Strict 6968, smoke, release tests 38.
  OWNER QUESTIONS: keep the 10 municipal "Trib" continuations?; the crossing button says only "▲ Upstream" on screen
  (name only in the screen-reader label); Eagle Trib.5 flow looks reversed in added-streams data.
- MERGED ~23:30: MAP CHROME — Android/browser back everywhere (71a4a519, 06011c65), map side rail + Spots menu + long-press
  drop (96a97c2a, c90553ad; Spots tab removed), menu (02542c85 release manifest `sources`, 5b686686 hamburger + drawer:
  Glossary, Data sources & dates, About, Report a problem, Contact; 2a4f8a42 horizons row). Merge fixes a6fb1b66 (back
  scan accepts useBackLayer-wired Modals; menu page content covered). Strict 6943, smoke, release tests 32 pass.
  NEEDS a release-only rebuild so the live manifest carries `sources` (do it with water_continue after that merge).
  OWNER QUESTIONS: rail is square (old Riffle style) not Fish Washington round; native "At my location" needs
  expo-location; long press on the Conditions map also drops a spot?; app name "Can I Fish This?"; contact value;
  synopsis publish date; ingest in-season changes?; back on Search/Conditions root -> Map first (Material) or exit?;
  back on Search clears a selected row first?
- LAYERS MENU design done (wf_dc903d04-d42): handoff/PLAN/app-data/layers-menu/ (inventory, 3 mockups A/B/C, RECOMMENDATION.md:
  C compact + More layers with grafts). Shown to owner: https://claude.ai/artifact/UjhcUcXmStkGGcoyFZWuNm. Waiting on the owner's
  pick + 5 questions; build after the map-rail lane merges.
- MERGED: lake modes prepared-not-fed (6 commits 2ce772dd..f13f623a + layers sheet stocking row removed): optional `lakes`
  slot + reader + engine channels kept; FFSBC fetch, lake_facts step and derived data removed; Stocked/Depth greyed
  ("No data yet") until a release carries values. Strict 6827, smoke, release/lake/deliver tests 27 pass.
- T48 (owner 2026-10-21, recommended + accepted pattern): search field in the top bar (menu · search · sync slot), opens
  results over the SAME mounted map, one placeholder line per opening from a words list; bottom bar Map/Conditions
  options mocked for the owner first. Build after the map-chrome lanes merge.
- NEW TICKETS (owner 2026-10-21): T44 one frame for the water screen (no left-margin shift between Regulations/Conditions/
  Map; FaceBar drawn once), T45 Regulations desktop layout, T46 fish ID looks side by side + max picture width on desktop,
  T47 status colours distinct (own vs closed vs restricted; region boundary red reads as closed -> neutral boundaries;
  CVD gate; owner picks from side-by-side renders).
- ~22:00: tile server restarted (pnpm tiles, :39217, serves r-0efa5d05fc92). Follow-ups wf_3d498e10-baf DONE: engine-polish
  already merged; lake-values reviewed+fixed (worktree-wf_3d498e10-baf-2, 5 commits) -> CORRECTION PASS agent running
  (prepared-not-fed; FFSBC source has no open licence); water-map lane was BLOCKED by the permission classifier, finished
  by me: release built in its .scratch (r-2507b6dcc7fe, 13,670,048 B, water_continue 92 rows), fixture re-cut,
  committed 1f1ffb11 on worktree-wf_3d498e10-baf-3 -> REVIEW+FINISH agent running (continuation rule check, web shots,
  step time). OWNER QUESTION: Chilliwack and Vedder are different FWA blue lines; continuation rule = "A's main line
  begins at the top of B's and is the only inflow there". Flaky: tools/no-localhost vs check-boundaries temp _bnd.ts race.
  Layers-menu DESIGN workflow wf_dc903d04-d42 running (3 mockups + recommendation). Map chrome w72sww8p4 running.
- OWNER 2026-10-21: lake Stocked/Depth = PREPARED, NOT FED (decisions.md). When follow-ups wgw0lg80t finishes, run a
  correction pass on lake-values (worktree-wf_3d498e10-baf-2 + wip/lake-values): keep slot/reader/engine channels,
  drop the stocking fetch + shipping from deliver, toggles disabled while the release has no lake values. Then merge.
- MERGED (after restart): WAVE B COMPLETE — T36 part 2 (src:/srcs:/licsrc:/sp:/other: sheets), T37 SheetHost (moved
  into RegsScreen: one sheet over the whole page, Parts button opens the parts sheet), review fix (parts sheet from
  stored day status; drag from content at top). Sheets parity 134/134, 7181 sheets incl. gl/hatch linked (pdf_pages)
  and parts 98; only 3 D19 legacy-pool other: sheets not compared. Strict 6802, smoke pass. Map chrome RESUMED as task
  w72sww8p4; follow-ups task wgw0lg80t still running.
- RESTARTED 2026-10-21 after the computer restart: step (1) DONE — deliver all r-0efa5d05fc92 (rows 32 / licence 7 /
  pdf_pages; card gate 0 problems; 13.67 MB gz), taps + regs fixture re-cut (1173321a), blob test id 2002; strict 6735,
  smoke pass; pipeline-needs tests 22 pass. WIP commits tagged wip/* in the main repo (sheets-fix, lake-values,
  water-map, map-rail-spots, menu-info, android-back). RESUMED wave B (task wn4xiw5gi, only fix:sheets) and
  follow-ups (task wgw0lg80t; water-map lane told to cherry-pick f3974f89 + wip/water-map). Map chrome
  (wf_5f5e25c3-271, lanes told to cherry-pick their wip/* tag) STARTS when one of those finishes (memory).
- PAUSED 2026-10-21 ~20:15 for a COMPUTER RESTART (memory). Everything stopped; orphan workers killed; main tree clean.
  RESTART STEPS, in order:
  (1) `deliver all` FIRST, alone, on a quiet machine (no workflows running): `~/.claude/projects/-Users-dawson-horvath-dev-
      personal-BC-freshwater-fishing-regulations/handoff/tools/heavy.sh deliver 40 -- .venv/bin/python -m pipeline.deliver all`.
      It was killed twice by the memory guard (free 2%). MAIN IS AHEAD OF THE LIVE DATA: cd0a2a59 bumps rows 32 / licence 7,
      so the app refuses the live release r-1bb06fa28daf and strict store suites fail until this deliver lands. Then:
      `.venv/bin/python app/design/regs-v11/port/taps.py`, `pnpm -C app regs-fixture`, `UPDATE_REGS_FIXTURES=1
      REGS_STORE_REQUIRED=1 pnpm -C app exec vitest run conformance/src/regs/harness conformance/src/regs/goldenSlices`,
      strict check, smoke, commit. Then remove worktree wf_e2463c2d-be5-4 (merged).
  (2) Resume the 3 workflows ONE OR TWO AT A TIME (memory): each SETUP does `git reset --hard`, so first edit each
      lane prompt to cherry-pick its WIP commit after the reset. WIP commits (reference, not reviewed):
        wave B sheets fix  run wf_e2463c2d-be5 (wave-b-regs-screens-and-pipeline-needs-wf_e2463c2d-be5.js): branch
          worktree-wf_e2463c2d-be5-3 = f8ca3491 T36p2 + 28046406 T37 (reviewed; 2 majors open: Sheet drag only on
          grabber; parts sheet null) + WIP 9092951e of fix:sheets. Merge note: T37 must move sheet state +
          SheetHost into RegsScreen (RegsView is gone).
        follow-ups  run wf_3d498e10-baf (engine-and-water-map-followups-wf_3d498e10-baf.js): lake-values
          worktree-wf_3d498e10-baf-2 = 29977b7e + 1f5cd065 (+ WIP 722815b3) awaiting review; water-map
          worktree-wf_3d498e10-baf-3 = f3974f89 (+ WIP 0101b663) awaiting review. engine-polish MERGED.
        map chrome  run wf_5f5e25c3-271 (map-chrome-menu-back-wf_5f5e25c3-271.js): WIP only — map-rail-spots
          worktree-wf_5f5e25c3-271-1 f70763c8, menu-info -271-2 199690d5, android-back -271-3 1b2e80b4.
      Lake-values merge also needs: `.venv/bin/python -m pipeline.stocking.fetch`, then lake_facts + release (or the
      next deliver all).
- 2026-10-21 evening (after the 7:30 PM credit reset): all three interrupted workflows RESUMED (wave B wf_e2463c2d-be5 —
  only fix:sheets reran; follow-ups wf_3d498e10-baf — lake-values review + water-map impl reran; map-chrome
  wf_5f5e25c3-271 — all 3 impl reran). MERGED: T23 header + T29 pin bar (+ fix), T34 Gear + T35 Methods/Licence (+ fix;
  RegsView gone, RegsScreen slots take render functions with the screen's actions), engine polish (2 commits). Strict
  6728, smoke pass. Pipeline lane cd0a2a59 (rows 32 / licence 7 / pdf_pages) merged; deliver all RUNNING under heavy.sh
  (log scratchpad deliver-rows32.log); after it: taps.py, pnpm regs-fixture, UPDATE_REGS_FIXTURES harness, strict check.
- OWNER 2026-10-21 ~16:20 asks, RUNNING wf_5f5e25c3-271 (script map-chrome-menu-back-wf_5f5e25c3-271.js): lanes
  map-rail-spots (long-press -> Add a spot here / Cancel -> existing spot flow; spots + layers buttons on a map side
  rail, Fish Washington style; spots menu), menu-info (top-left hamburger: glossary, about, contact [placeholder
  config], report a problem [user-visible diagnostics, sent only by share/mailto], data sources + dates from the
  manifest; report link on each Regs tab; sync slot later), android-back (whole-app audit, one mechanism, tests).
  Species sheet (sp:) and "How this was decided" (other:) are wave B sheets lane T36 part 2 + T37 — verify on merge.
- OWNER 2026-10-21 ~16:10 on the water map view: keep every part's regulation colour and OUTLINE the current part
  (no highlight fill); step ▲/▼ across waters when one stream continues directly as another (Vedder from
  Chilliwack; same watercourse, not tributaries) — pipeline ships a continuation link. Folded into the follow-ups
  lane water-map-followups; workflow restarted as task wpy391edp, same run id wf_3d498e10-baf.
- MERGED 2026-10-21 ~16:05: colouring engine (1f3af369 + bfa83326; merge 1 commit: restore re-applies P15 water parts).
  Pans 0 grey frames (was ~500 ms), horizon correct on the first frame, feed-only recolour with no tile bytes; strict
  check 6367, smoke pass. RUNNING follow-ups wf_3d498e10-baf (script engine-and-water-map-followups-wf_3d498e10-baf.js):
  engine-polish (no look pass on horizon, route mini-map through the engine, native plan), lake-values (stocked/survey
  shipped in the release + engine channel; needs a release rebuild after), water-map-followups (P15 b-d below).
- MERGED 2026-10-21 ~15:45: P15 water map view (088c52c4 + review fix 6af87170: web marks drawn at their point).
  Merge made its exit / Show-regs actions go through openWater() and other-section taps through opener.tap. Strict
  check 6348, smoke pass. P15 FOLLOW-UPS (after the colouring engine merges): (a) ANSWERED (continuation link, see above): the Vedder is 1 part
  in the release (the prototype hand-grouped Vedder+Chilliwack, 5 parts); needs a pipeline continuation link/group or
  a chooser over its predecessors; (b) Zymoetz lake part unreachable by ▼/▲; (c) back after stepping returns to the
  part the map opened on, not the stepped part; (d) `water` paint only widens streams (no lake/wetland, no ink
  casing); (e) native not run, step perf not measured.
- RUNNING 2026-10-21 ~15:20: WAVE B run wf_e2463c2d-be5 (script workflows/scripts/wave-b-regs-screens-and-pipeline-needs-wf_e2463c2d-be5.js,
  session 368da779): lanes header-pins (T23,T29), rn-gear-licence (T34,T35), sheets (T36 part 2, T37), pipeline-answers
  (the 4 needs below + D21). After merging the pipeline lane: reach check, then `deliver all` via heavy.sh, re-cut taps.
- MERGED 2026-10-21 ~15:15: WAVE A (wf_31b13f2b-581): T30 T31 T32 Gear/Bait/Methods (+ one levelTag in level.ts), T33 Licence (+
  review fix: youth box reads stored under_16), T28 pins, T36 part 1 sheets (rule/gl/pos/hatch). Parity 134/134 on gear,
  bait, methods, licence (133 + x31p tidal), pins, sheets built so far. pnpm check strict 6295, smoke pass. PIPELINE NEEDS
  (keyed exclusions until shipped): (1) licence drops steelhead_stamp where steelhead closed (7 states,
  STEELHEAD_DROP_IDS); (2) under_16 buy[] carries `or` alternatives (3 states, YOUTH_ALT_IDS); (3) Statics.pdfPages in
  release statics (735 gl + 38 hatch sheets unlinked); (4) Other fish rows `stamps` (14 states). Open: D21 row in
  decisions.md -> (b); T36 part 2 (src:, other:, srcs:, licsrc:, sp: needs FISH_TIPS emit, parts); owner points: bait
  also_allowed not shown (parity), set_line note on Methods item.
- MERGED 2026-10-21 ~14:50: one-water-screen (595bb174 + review fix 7f6d4692: an unnamed reach opens the same
  Conditions-only frame from either map; BACK IS NOW LATERAL, back leaves the water to the map it was tapped on; both
  noted in decisions.md for the owner to confirm). Reference 308 RESOLVED as page-vs-ruling, not a regression: Shuswap
  stamp record rules stored as duty (batch 9) vs page v35 floor -> PAGE_BUGS B9.4 (b5f86988); slow test 0 unexplained;
  no deliver needed. pnpm check strict 4889 pass, smoke pass. Worktrees removed.
- RESUMED 2026-10-21 14:40 after the credit reset: all four workflows resumed with the same run ids (below); a
  single agent is investigating the 308 unexplained reference answers. Was PAUSED 2026-10-21 (owner low on credits). Main tree CLEAN at the commit below; everything merged is committed and
  green (pnpm check 4877; smoke). STOPPED workflows — resume each with Workflow({scriptPath, resumeFromRunId}); finished
  agents replay from cache. Partial work was saved as WIP commits on the worktree branches (reference, not reviewed):
    colouring engine (one pathway, lake modes, feed-only)  run wf_b01b27c4-44c  /Users/dawson.horvath/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/workflows/scripts/conditions-colour-ap-wf_e6f1ab3e-f02.js   WIP 48be10d9 (worktree-wf_b01b27c4-44c-1)
    one-water-screen                                       run wf_9d16c370-feb  /Users/dawson.horvath/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/workflows/scripts/one-water-screen-wf_9d16c370-feb.js        1 commit on worktree-wf_9d16c370-feb-1 (implement done; review/fix pending)
    wave A Gear/Methods/Licence/pins (batch 10)            run wf_31b13f2b-581  /Users/dawson.horvath/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/workflows/scripts/batch-10-gear-methods-licence-core-wf_31b13f2b-581.js  lane1 WIP 50101cd8, lane2 1 commit, lane3 1 commit + WIP 0df41933
    P15 water map                                          run wf_aef3d199-122  /Users/dawson.horvath/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/workflows/scripts/p15-water-map-wf_aef3d199-122.js            WIP 745fd43e (worktree-wf_aef3d199-122-1)
  Tier 2 during the pause: 5 fast fails FIXED (see the commit after this note); 1 OPEN: test_answers_reference
  (slow) now has 308 unexplained answers on wbk:329518145 — investigate after the restart (likely from batch 9's
  pipeline answers: closers/record duty/stamps vs page v35; classify as a PAGE_BUGS class or fix).
  FEED BACKFILL DONE: 100,702 days filled (5,844 HYDAT final, 94,858 real-time, mostly provisional) in 3,399
  requests, 19 failed; 379/428 stations complete Jan 1-Oct 21, 1,334 days still missing (no source holds them).
  NO-CREDIT JOBS (done): and Tier 2 test-full + graphify update (log:
  scratchpad full_pause.log, session 368da779). QUEUED after these: wave B (T23, T29, T34/T35, T37), Esri online imagery, P14, Other-fish stamps,
  S2 tile/basemap size easy wins (handoff/PLAN/app-data/S2-tile-size-easy-wins.md), D1 phone testing — option 1 dev build
  when the owner has the phone, option 2 preview build after the owner's R2 staging bucket (D1-phone-testing.md).
- OWNER 2026-10-21 (screenshot): the water Map view's section selector does not work (P15 was never built) -> workflow
  p15-water-map RUNNING (part highlight, framing, down/up + side-channel chooser, marks, Flows into, Show regulations).
  Conditions route overlay made ARROWS ONLY (97d4536c: no casing/tone line/dashes/lake wash over coloured rivers).
- OWNER 2026-10-21: "we need the gear, methods, licence etc from the prototype too". WAVE A RUNNING (core presenters,
  wf batch-10): T30->T31->T32 (Gear/Methods), T33 (Licence), T28 pins + T36 sheet presenters. WAVE B after
  one-water-screen merges: T23 header, T24-T27 RN Keep components (as needed), T29 pin bar, T34/T35 RN Gear/Methods/
  Licence + parity, T37 sheet host. Then T39-T43 (dark theme gate, a11y, web parity walk, edge states, book links).
- 2026-10-21 later: deliver all -> release r-1bb06fa28daf PROMOTED (rows 31 / display 18; 13.71 MB; card gate 0);
  taps.json + regs fixture r-d88e97e5bf83 re-cut; Keep parity compares group stamps; only the 2 Other-fish stamp
  documents (White Sturgeon licence, Kootenay Lake rainbow surcharge) excluded until the pipeline ships Other-fish
  stamps (OPEN). MERGED conditions: chart prefetch (no flicker), flow arrows on Conditions (tiles run mouth->source,
  verified), feed gap refill every run + year_backfill tool. Feed republished + province year_backfill RUNNING
  (~3-4 h, polite). pnpm check 4877; smoke pass. Pools spike: handoff/PLAN/app-data/pools/ (feasible, unvalidated).
  NEXT: one-water-screen unification; Esri online imagery (after the colouring engine merges).
- 2026-10-21 afternoon MERGED batch 9: cb71e1d0 the 5 Keep answers now from the PIPELINE (closers, narrow_all,
  parent, record duty, licence stamps; rows 31 / display 18; TS derivations deleted; scratch release 13.71 MB) +
  T21 Keep tab 3665f151 + review fix a445aa91 (.gyear line drawn; verify_map.py fixed for v38). Keep parity 134/134;
  shots handoff/PLAN/app-integration/keep-shots/ (close to the prototype; header T23 + Gear/Methods/Licence pending).
  deliver all RUNNING (bzwoho73y) for rows 31/display 18; then taps re-cut, regs-fixture, blob/display-dict literals,
  try dropping T21's whole-tab stamp exclusion ("keep 19 ^lines$"), strict check, Tier 2. Then the ONE-WATER-SCREEN
  unification (queued on batch 9 — now unblocked).
- OWNER 2026-10-21: satellite imagery is too low-res (today EOX Sentinel-2 cloudless 2016, 10 m) -> research +
  verification of higher-res sources (licence, cost, offline) -> OWNER PICKS [owner: satellite is ONLINE-ONLY, so
  offline caching terms no longer constrain the pick]; and allow OVERZOOM past the pmtiles
  max. MERGED: overzoom 24cb5888 + 2bd4a605 (camera z18; each source held at its archive maxzoom; shots
  handoff/PLAN/app-data/overzoom/); research 8f80fd13 + verified corrections 80d3935d (handoff/PLAN/app-data/satellite/
  README.md + local compare.html). Recommendation: online Esri World Imagery (0.3-0.5 m rural BC, z17-18, 2M tiles/mo
  free then $0.15/1k, client key) over the 10 m base; runner-up Stadia ($80/mo). OWNER Qs: client key + usage billing
  OK? [OWNER: ESRI World Imagery online, like the original app — QUEUED to implement after the colouring engine merges]
  Build a 10 m Sentinel-2 offline base (2-3 GB)? [open] NOTE: tippe.py simplifies z14 at ~4 m, so z18
  overzoom shows corner-cutting on bends (a tile-build trade-off; size vs fidelity).
- OWNER 2026-10-21: the Map tab's faster painting (AP) is "very good" — do the same for the CONDITIONS map.
  OWNER: ONE colouring pathway for both map views (generalise ProfileColouring into one engine, modes status +
  standing; audit other Map-vs-Conditions divergences), and colours must update from a NEW FEED ALONE (no tile
  change, no data release); ALSO the lake modes (stocked, depth/surveyed + contour toggle, plain) and the basemap
  toggle through the same engine, and stocked/surveyed values must not be baked into tiles. Workflow
  conditions-colour-ap RESTARTED with all three as requirements.
- Tier 2 on r-7b6e802bf22b: everything green (fast 3839, slow 266, card gate, release parity, placeable, checks)
  except 2 fast tests pinned to the pre-ruling Kitimat; fixed (feb release names ST; exemption inventory). OPEN: the
  page-bug "count comparison drops a 2-fish release" mutation lost its Kitimat witness — find another water.
- 2026-10-21 late morning: KEEP CARD presenters MERGED (lane A T13 strip/T14 banner/T16 header; lane B T15/T15b/
  T19b; lane C T17/T18/T19; parity 134/134, Kitimat 3 states keyed by the ruling) + merge fixes 0b99102a (one
  ruleBookLine; THE_REGION_LC). REGS_STORE_REQUIRED=1 pnpm check 4787 pass / 19 skip; smoke pass. Owner rule found 5
  answers still derived in TS (closers, narrow_all, parent, record duty, stamps) -> batch 9 moves them into the
  pipeline (version bump, then lead runs deliver all) + T21 [first run hit the session limit at 6:40 am with nothing
  saved; RESUMED same run id wf_878ffca2-5ae] Keep tab wired into the water screen with screenshots vs
  the prototype (handoff/PLAN/app-integration/keep-shots/). OWNER 2026-10-21: one-part waters KEEP their label ("Whole water"); decisions.md.
- 2026-10-21 morning: deliver all OK -> release r-7b6e802bf22b PROMOTED (13.85 MB gz; card gate 0 problems; both
  Kitimat rulings live: hatchery ST keep 2 Jul 1-Oct 31, release Nov 1-Jun 30; +1 over 50 like the Vedder). taps.json
  re-cut, regs fixture r-d3b990efee47, Kitimat + year-strip + card parity pin the ruling (3 Kitimat states differ
  from page v36 by design). REGS_STORE_REQUIRED=1 pnpm check: 3364 pass / 19 skip. Dev server serves the new release.
  verify_map.py (was stale on the v38 golden tree): fixed in the T21 review — REGS_STORE=<store> reads any store
  read-only; strip/groups/otherFish 134/134, tiers 124/124, kinds 258/258 (3 Kitimat-ruled trees not walked); exits 1 on a difference. Tier 2 RUNNING (bpf411jqt). Batch 8 (Keep tab card, lanes
  A T13->T14->T16, B T15/T15b/T19b, C T17->T18->T19) RUNNING; then T21 build-keep + screenshots vs the prototype.
- 2026-10-21 early morning: MERGED strip dedupe ad0252d4 (254 distinct strips; release 13.90 MB, ~99 KB under the
  14.0 MB gate, gate untouched; lossless over 9175 cells); colour-latency AP on WEB merged 8af5dcd1 (0 grey frames after
  pans at z8/z11/z13, ~0.2 ms/pan, day change 0 state calls; native keeps the old 'status' path — native per-tile
  colouring still OPEN); T05 redo 46ccd2d0. pnpm check 2358 pass; smoke pass. deliver all RE-RUNNING (b25dhsb2x).
  AFTER IT: taps.py re-cut (goldens), pnpm -C app regs-fixture, flip Kitimat test DEC1 hatchery ST -> release,
  REGS_STORE_REQUIRED=1 pnpm check, live pytest (regs_store, kitimat), Tier 2.
- 2026-10-21 night (cont.): N2 MERGED (84b3b357 3a53ecdd 8f959fc3 465595fe): native map behind the shared
  MapRenderer contract, MapLibre RN 11.5.0 (vulkan, provisional), feature-state by MVT id on native, web keeps
  promoteId, metric scale bar, basin field on native; Android emulator verified (shots handoff/PLAN/app-data/n2/).
  OPEN device-only: iOS (no CocoaPods; owner deferred), two-finger rotate/tilt, basin <z9, hatches/gauge pill on
  native (no runtime-image API). G1 follow-ups PORTED onto ViewSwipe and merged edb338e6 (back closes an open sheet
  first on web + Android; view swipe keeps pinch-zoom + text selection; cancelled gestures never commit). Kitimat (a) committed cc775df3. deliver all: first attempt refused (reach run older than the
  curated entries) -> reach rerun OK (786 s); deliver all passed bundle/answers/CARD GATE 0 problems/store, then the
  RELEASE SIZE GATE refused it: 14,156,995 B gz > 14,000,000 (year strip +~270 KB). Nothing promoted; live data
  unchanged. Workflow year-strip-size RUNNING (dedupe strips, gate NOT raised); then re-run deliver all.
- 2026-10-21 night MERGED: conditions-and-nav (layout: switch by plot, full-width chart, desktop 2 columns, last
  report, zoomable chart; map: up/down apart, here pin, share labels, trace geometry from tiles, zoom; view swipe
  Regs<->Conditions<->Map with chart/minimap shielded; Kitimat +1 like the Vedder; T05 leap rule from the calendar
  spec) + merge fixes 663939f0; batch 7: rows 30 YEAR STRIP from the pipeline (year.py, page ROUND14 rule) + T12
  harness (+6e9e0443). SKIPPED 8cc353a4 (T05 parity over openRegsStore: collided with batch 6's golden.ts) — redo
  on HEAD. Colour study DONE + merged b021f25d (web AP verified: 0 grey frames, ~0.1 ms/pan; native E1P NOT supported —
  verifier found the E numbers 4.6-6x low; native colouring decided after N2). Workflow colour-ap-web-and-t05 RUNNING
  (wf_d0e2368f-253): AP on web with the verifier's 4 conditions + the T05 parity redo. Kitimat ruling (a) applied in curated
  region-6.json (r5 no longer exempts r7). `deliver all` RUNNING via heavy.sh (both rulings + rows 30); after it:
  taps.py re-cut (sync goldens), `pnpm -C app regs-fixture`, flip the Kitimat test DEC1 hatchery ST to release,
  pnpm check with REGS_STORE_REQUIRED=1, then Tier 2.
- RESUMED 2026-10-21 ~01:45 by the scheduled restart: all four workflows resumed with the same run ids (below);
  stale worktrees wf_7703a540-b9f-4 and wf_2c5dbf14-b47-1 removed. Was PAUSED 2026-10-20 ~23:00 (owner out of credit). MERGED before pause: search placement + preview
  (32ee103b 94c2f5d3 1f065176; 19,641/19,641 placeable; eye -> main map, back -> search). pnpm check 1597; smoke pass.
  STOPPED workflows — RESUME each with Workflow({scriptPath, resumeFromRunId}); completed agents replay from cache,
  their worktrees under .claude/worktrees/ keep any commits:
    N2 + G1 follow-ups   run wf_67dab776-5a7  script /Users/dawson.horvath/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/workflows/scripts/n2-native-map-wf_67dab776-5a7.js
    colour latency study run wf_9ac4524f-637  script /Users/dawson.horvath/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/workflows/scripts/map-colour-latency-wf_2c5dbf14-b47.js
    conditions-and-nav   run wf_fd35cebb-60b  script /Users/dawson.horvath/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/workflows/scripts/conditions-and-nav-wf_fd35cebb-60b.js
    batch 6 hydrate      run wf_1f0c1772-d15  script /Users/dawson.horvath/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/workflows/scripts/batch-6-hydrate-chain-wf_1f0c1772-d15.js
  Before resuming: check each stopped worktree (git -C <wt> log redesign/stream-sections..HEAD) for partial commits.
  AFTER the Kitimat +1 merges: apply ruling (a) (drop r7 from r5 exempts; flip the test) then ONE deliver all.
  OPEN owner items: whole "Kootenay Lake" not a search result (only its 3 areas); search memory now persists across tabs.
- Batch 6 MERGED 2026-10-21: T09 loadRegsInputs, T07 ladder/settle, T08 rows model (parity 134/134 vs hydrate.json),
  T10 regs fixture release (pnpm regs-fixture) + T11 ui regs hooks. pnpm check 2821 pass; smoke pass. OWNER RULE CAUGHT:
  the per-row YEAR STRIP colour was derived in TS (most permissive over weekday moments) — removed (430fc927); the
  PIPELINE must ship per rows_row a run-length [[status, days]] over the 365 book days. Batch 7 (year strip + T12) next.
- The owner's data server (pnpm tiles, :39217) had stopped; restarted 2026-10-20. Agents must never stop it.
- N2 + G1 follow-ups RUNNING (wf n2-native-map): N2 = native map behind the adapter contract, emulator via heavy.sh.
- N1 DONE (12713069, doc + shots only; spike commits eeef439b/9138c896 on worktree-agent-a8261f149d43eead6 are
  SPIKE-ONLY, never merge): binding @maplibre/maplibre-react-native 11.5.0; offline pmtiles + glyphs from file://;
  feature-state needs section_id as tile feature id (MapLibre Native ignores promoteId); Android ids arrive as strings;
  Vulkan variant 51-60 fps on the emulator (OpenGL 21-57); A4 paint decode 105 ms and A8 p95 41.6 ms are the budgets
  at risk on a real phone. AVD n1_pixel exists; `adb shell reboot -p` stops it (emu kill hangs).
- P18 tile ids MERGED 6ec40af3 (stream/lake/wetland carry section_id as MVT id AND property; +3% atlas).
- N1 spike agent (worktree) → handoff/PLAN/app-data/N1-SPIKE.md. REDIRECTED by the owner: ANDROID emulator + WEB
  (react-native-web); iOS deferred, code kept platform-neutral so iOS is a build later.

## FINAL CLEAN-UP PHASE (owner, 2026-10-20) — ONLY after the app is done AND the owner has looked at it and confirmed it
## looks like the prototype
1. Remove the design work from app/design/ (the prototype is in the app by then).
2. Audit handoff/ (repo + the memory project's handoff/) and remove what is no longer needed.
3. Audit pipeline/ for OLD files nothing uses any more (not planned/future work), and remove them.
Do not start any of this early.

## Owner direction (2026-10-20, third round)
- Platform order: ANDROID (mobile) + the WEB app (react-native-web) now; iOS later. Keep everything portable to iOS
  (cross-platform binding, platform code only behind .native/.web). Acceptance on native = the Android emulator for now.

## Owner answers (2026-10-20, second round)
- Native builds + tile rebuild: allowed; keep other work going while they run. Live deliver: do it.
- Search A5 ~5 ms: ACCEPTED, provided the input is debounced well.
- Saved spots: remove the sid; a spot = coordinate + river id (item_id); migrate saved files on load.

## Owner answers (2026-10-20)
- Heavy jobs: allowed now for the day-1 baseline (via heavy.sh, one at a time, ≤4 workers).
- Order: **P00 first**, then P05 + P03 (then P06, N1 scaffold).

## §7 First day
| step | state |
|---|---|
| 1 read docs | AGENTS.md + IMPLEMENT-HANDOFF read; APP-PREP §0–§1, MINIMAL-PACKAGING, integration README still to read before P05/P03 |
| 2 log / pause / go-ahead | HEAD 7a326133; UR7 committed; no `heavy-job.pause`; go-ahead given |
| 3 reader files | ALREADY DONE in a4b37eff (`app/design/regs-v11/port/reader/`) |
| 4 baseline | DONE, see Baseline counts |
| 5 golden re-sync | NOT NEEDED: golden.json meta ref 7fb6809f, v38, misses unchanged (4) |
| 6 first tickets | P00 a4ee3a9b, P03 c2d15a05, P05 8cbfd8c7, P06 done → N1 next (then P07, P01/P02/P04/P12/P13/T22 in parallel) |

## P00: test tiers
- [x] measure: pytest fast (durations), pytest slow (collect count only), vitest per file, `pnpm check` per step, check.mjs total + per round, card gate
- [x] pytest `smoke` marker (registered in pytest.ini) on ~1 test per gate/invariant, no live-bundle scans
- [x] vitest smoke (not needed: whole suite 5 s) filter/project
- [x] check.mjs `ONLY=smoke` (NOTE: check.mjs is in the prototype lane, rule 12; P00 asks for it explicitly, so the edit is additive only)
- [x] `scripts/test-smoke.sh`, `scripts/test-full.sh`; one line in AGENTS.md
- [x] commit with a before/after time table

### Measurements (2026-10-20, laptop, through heavy.sh)
- pytest fast (default `-m "not slow"`): **3572 passed, 6 skipped, 264 deselected in 504.8 s (8:24)**, wall 528 s,
  serial (no xdist). 196 files. The top 6 files take 50 %: test_anchors 75 s, test_pfma_drainage 53 s,
  test_curation_review_api 43 s, test_export_ui_rules 37 s, test_steelhead_streams 33 s, test_competition 29 s.
  811 of the fast tests carry a `needs_*` marker. 11 files hold only `needs_*` tests.
- pytest slow: 264 tests (collect only, not run).
- `pnpm -C app check`, per step: boundaries 0.3, platform 0.3, style 0.4, colours 0.3, spinner 0.4, **fixture 8.4 (FAILS)**,
  gauge-policy 0.3, calendar 0.3, deps 0.3, typecheck 0.7 (incremental), test 4.7 s. Total ≈ 17 s.
  - vitest: **58 files, 642/642 passed**. The slowest are check-boundaries.test 2.9 s and agree.render 1.9 s; the rest are < 1 s.
- check.mjs full: **136/136 in 439 s** (7.3 min). By round: round20 94 s, core/unattributed 94 s, 20e 45 s, ur6 26 s,
  18 26 s, 20b 22 s, 20c 21 s, 20h 17 s, 19 16 s, ur7 16 s, ur1 14 s, 20g 13 s, ur5 13 s, 20f 7 s, int1 6 s, ur2 4 s, ur3 4 s.
- card gate (shipped answers): **0 problems**, 15,862 groups on 7,144 distinct cards, 87 s wall.

### P00 result: Tier 1 = 74 s (app check 13 s, pytest smoke 40 s / 185 tests, check.mjs smoke 21 s / 18 checks)
- pytest: repo-root `conftest.py` marks the ids in `pipeline/tests/smoke.txt` (the fastest no-needs test per file, 185 of
  196 files; 11 files have only needs_* tests). A whole-tree `-m smoke` run exits 4 naming any stale id (mutation-checked).
  The default and slow collections are unchanged (3578 / 264).
- vitest: no smoke split needed. The whole suite is 5 s, so Tier 1 runs all of it inside `pnpm check`.
- check.mjs `ONLY=smoke` = no-hand-facts + ur2, ur3, int1 (all 134 states rendered and scrolled), 20f. This is one
  additive line in a prototype-lane file, which P00 asked for.
- The rebuilt dev fixture was committed separately (P03's fixture item), because it blocked `pnpm check`.

## P03: CI gaps (DONE)
- f6332295: the rebuilt dev fixture. APP-PREP calls this regeneration "human-run", but `pnpm check`'s fixture step
  rebuilds it on every run anyway; committing it was the only change. Flagged to the owner.
- app-ci now runs `pnpm check` itself (this adds colours, spinner, fixture, calendar, gauge-policy and answers) with a
  `.venv` from requirements.txt. It also triggers on `schema.sql` / `indexes.sql`.
- pipeline-ci triggers on `app/packages/core/src/*.generated.*` (it was calendar only). `reference/calendar.generated.js`
  is under `pipeline/**`.
- `app/package.json` `answers` script, inside `check`.
- `app/packages/data/dev/fixture-schema.test.ts`: the fixture DDL equals schema.sql + indexes.sql. On the old fixture it
  FAILED (6 missing tables + section_ruleset changed); it passes now.
- Mutation: a hand edit to answers.generated.ts makes `pnpm -C app check` exit 1 ("stale"). Clean run: 59 files / 645 tests.
- NOT verified: a real CI run on a scratch branch (nothing pushed). Open: the fixture writes meta `shed_rule` where the
  bundle writes `trust_bands`; no app code reads either, so it is left for whoever next touches build-fixture.mjs.

## P05: card gate on the shipped answers (DONE, except the full `deliver all` run)
- `card_check.check_shipped` decodes `ui-rules-answers.json` from its bytes (`encode.decode`) and proves every key.
  `check_built` (rebuild, `--items`) is unchanged and shares `check_model`.
- `card_check --all` writes the stamp + report `card-check.json` BESIDE the answers: problems, parts_checked,
  keys_checked, cards, groups, dates, code_sha (all of pipeline/deliver/answers/*.py), answers_sha256. Any old stamp is
  deleted first.
- DEVIATION from APP-PREP: the stamp is a sidecar, not inside `answers.about`. Writing it into the file after checking would
  change the bytes that were checked (and the `.gz`). The sha binds the stamp to the exact bytes instead. The store copies
  it into its meta `card_check`; P08 copies it into the package meta.
- `deliver all`: answers → **card_check** → store. There is also a `deliver card_check` step. The store build refuses
  without a passing stamp for its exact bytes (`require_card_check=False` is used only by test_regs_store's encoding
  fixture).
- Tests: `test_card_gate_in_chain.py`, 5 tests (chain order, failing gate stops before store, stamp refusals, store
  refusal, live mutation: Vedder headline n+1 fails). All 5 FAILED on the old code; all pass now.
- Live: `deliver card_check` → 0 problems, parts_checked 20461 (= the answers' part count), keys 2268, 57 s. A store built
  to scratch carries meta.card_check. The live `data/generated/regs/card-check.json` now exists.
- NOT YET RUN: `python -m pipeline.deliver all --out-dir <tmp>` (the full heavy chain). Batched with P07, which changes
  the same chain (owner: heavy jobs sparingly).

## P06: Feb 29 (DONE)
- One rule: `calendar_spec.reading_day` (Feb 29 → 59) + `weekday_of` (the real weekday) + `cases()`, generated to TS
  (`readingDay`, `weekdayOf`, `CALENDAR_CASES`), JSON, and the page JS (`readingDayOf`, `realWeekdayOf`).
- Readers: app `statusIndex.dayOfYear` → readingDay; Python `calendar.reading_day_of`, used by `status_index.Index.code`
  / `item_code` and `encode.tap_key`. Builders keep `day_of`'s 366 slots.
- Page `page_v36.js`: LEAP_VIEW makes Feb 29 read Feb 28 on its own weekday; the date box keeps 02-29. The script was
  re-inlined into `page_v36.html` (only the script block changed). The prototype `regs-v11.html` has no weekdayOf and
  was not touched.
- Tests: test_feb29.py adds 4 (spec cases; the page's weekdayOf + calendar in node; needs_bundle: no segment starts at
  day 60 and status profile slot 60 == 59; Zymoetz Fri 2036-02-29 → r10 speaks, Thu does not, Thu Feb 29 == Feb 28).
  test_status_index's tiny fixture is now closed on Feb 28 only (Feb 29 must read closed). The app test reads Feb 29
  as 59 and loops over CALENDAR_CASES.
- Before the change, 6 Python tests failed and the app test failed to load; all pass after. Tier 1: 75 s.
- NOT done: the port harness `hydrate.mjs` YEAR 2036 case (prototype lane; T05 covers taps.json parity at 2036-02-29).
  The next golden sync will report page_v36.js `weekdayOf` / `setMd` / `fromBox` as changed functions.

## P07: bundle format + atomic deliver all (DONE, except the heavy live run)
- bundle meta gains `format` = "bundle/1" (`build.BUNDLE_FORMAT`) and `schema_sha256` (schema.sql + indexes.sql). `schema`
  is kept.
- `deliver all` (`_all`): every step writes into `.deliver-staging-*` beside the destination; promotion by os.replace
  happens only after the last step + `report_all(strict)`. A file with no home refuses the promotion. Staging is
  removed on every exit.
- `vintage.Artifacts` + `report_all`: tiles · bundle (+format) · status index · export pair (+rule_ids) · answers ·
  card-check stamp · store (handles, reach, rule ids, answers_sha256). The package joins at P08. `pipeline build` calls it.
- Tests: test_deliver_atomic.py (8: a failing step at 5 points leaves live bytes and mtimes unchanged; a vintage
  mismatch promotes nothing; a pass promotes all; canonical homes) and test_vintage.py +6. All failed before.
- Live report_all: lenient = every artifact agrees; strict refuses only the bundle's missing format/schema_sha256
  (it predates P07; the next bundle build writes them).
- HEAVY ACCEPTANCE RUN DONE (2026-10-20, heavy.sh, 493 s): `deliver all --out-dir <scratch>` passed. Card gate 0
  problems / 20461 parts / 59 s; strict report_all: every artifact agrees; 9 files promoted, staging gone. The answers
  are byte-identical to live (sha 03e9d33b…). The live files were not touched.

## P04 (DONE): the 3 cron workflows deleted; archive/README updated.

## P02 (DONE): feed freshness
- http `stamp` → null for a missing or malformed fetchedAt; `Observations.fetchedAt: number | null`; source.ts no longer
  fabricates `Date.now()`.
- core `STALE_AFTER_MS` = 2 h (the default for `freshness`), and `readingAgeLine(f, observedAt)`, the whole sentence
  ("… · observed … UTC · checked 10 min ago" / "last checked 3 h ago, may be out of date" / "age unknown").
- `Conditions.observedAt`; ConditionsPanel shows readingAgeLine. `useStationReading` re-polls every 5 min (usePoll),
  and `useAsync(…, sameAs)` keeps the last reading during and after a failed refresh.
- Tests: http +3, core +6 (8 failed before), async refresh +2. pnpm check 657/657.

## P01 (DONE): spots store
- `spots/core.ts` `openSpotsWith(fs, name)`: atomic save (.writing → .bak → rename); load prefers a parseable
  .writing, then the file, then .bak; a corrupt file is renamed `.corrupt-<ts>` and `SpotsCorrupt` is thrown; writes
  are serialised (`queue.ts`); `patch(id, f)` changes a spot only if it still exists. Native = expo-file-system
  adapter; web = localStorage adapter (a quota error now rejects).
- ui `useSpots(store)` (awaited save/remove/refresh, `spotSaveFailed` sentence); App uses it; the refresh writes through
  patch only onto the spot as it was read.
- Shell awaits save and delete and shows the error; a tab change with a dirty edit asks (SpotScreen askToDiscard /
  onKeepEditing). SpotCapture awaits onSaved (error shown, sheet stays open); a capture failure goes back to When
  with the reason; When's back goes to the point step (it looped to itself).
- Tests: core 9 (corrupt kept + nothing written, crash between write and rename recovered, torn .writing, concurrent
  puts, remove during refresh, patch on the current spot, .bak, quota rejects, queue order). useSpots 3. SpotScreen +2.
  pnpm check 671/671.
- NOT done: a mounted SpotCapture flow test (it needs the map; covered through SpotScreen + useSpots instead).
- FOUND, not changed: `Spot.section` stores a sid ("only to replay the trace"), against AGENTS 5. Owner decision.

## P21 (DONE): the atlas build no longer writes blk_chains.pkl; guard in test_dataflow_gates. The live copies were
deleted with the owner's §6 approval: atlas/full, atlas/full.prev, and the scratch_a26 symlink (5.1 GB).

## P18 part 1 (DONE, code; the HUMAN tile rebuild is pending)
- `tiles/version.py`: version = "<handles16>.<tilebuild12>", where tilebuild12 = the sha of the archive BEFORE the
  stamp. `pmtiles edit --metadata` writes tile_version / section_handles / labels_registry into the archive; the
  sidecar also gets tile_version. `to_release` copies the archive + bc_outside.geojson (boundary.build) into
  data/release/tiles/<version>/, and an existing version folder is refused (immutable).
- The tile step (both paths) stamps and releases; `--no-release` is available for scratch builds.
- export: `region_id_of` gives one spelling ("7A") on all 18 region features (9 had a name only).
- build.py: `tiles_key` = handles + gpkg stamp + areas.json + tile code (deliver/tiles, atlas/splits). No registry,
  places or corpus. `--labels` forces tiles.
- vintage.report: a registry difference is a NOTE (labels), never a refusal; pairing is on handles only.
- GENERATED.release = data/release (config.yaml), ignored by data/.gitignore.
- Tests: test_tiles +4 fast (inventory snapshot, version id + metadata merge, region_id, the tiles key ignores the
  registry) + 2 slow needs_tiles (version stamped, release has archive + mask; every region feature has region_id). The
  2 slow ones FAIL on today's tiles (as they should) until the rebuild. test_vintage: the registry tests now say "note".
- HUMAN: rebuild tiles, `.venv/bin/python -m pipeline.deliver.tiles --build data/generated/atlas/full`
  (≈ 1 h; via heavy.sh 70). Then: the tile tests (`pytest pipeline/tests/test_tiles.py -m slow`).
- P18 part 2 (DONE): `tiles/assets.py` fetches the pinned Protomaps glyphs (every style text-font + the basemap's
  Regular/Medium/Italic, 256 ranges each) + the v4 light/dark sprites ONCE into data/source/map-assets/<pin>/ (13 MB,
  git-ignored; already fetched 2026-10-20), and to_release writes map-assets-1/ + assets.json (per-file sha) into the
  version folder. The tile step fetches before the hour of work. Verified end to end into scratch: archive + mask
  (61 KB) + 768 pbf + sprites = 14 MB; a second release of the same version is refused.
- LEFT FOR P10 (app): point TileEndpoints.glyphs / .sprite / the mask at the release folder (runtime-style.ts already
  takes overrides; the Protomaps CDN is only its fallback). The app still reads the mask from its old place until P10/P11.

## P13 (DONE, except the line target): route state
- ui/route.ts: Route (browse | water stack with `from`), RouteAction, reduce, bottomBarShown, useRoute, STACK_CAP 8,
  one back rule. route.test.ts: 7 (the spec's 5 + browse back + tab reset).
- Shell reads tab / water / spot / add flow / Conditions drill from the route and dispatches every move. The TabBar is
  hidden in water mode. Back from a water now restores the Conditions drill it came from (before, the reach was lost).
  vitest 678/678; the steps other than fixture pass (fixture differs only by P09's schema, committed with P09).
- NOT met: Shell is 709 lines (target < 400); P14 (water-mode shell) and P17 take out the rest.

## P09 (DONE): part_geo / part_mark / water_exit
- `bundle/topology.py`: keep_named (1-based handles via id_at; lake outlines from waterbody_polys.pkl) → write after
  derived.write; rank (km mouth-first), midpoint (half the stem by route measure, point_on), marks (≤ 60 m, split
  name), exit (item_down, nearest receiving part). water_map_export imports order_parts/split_label from it.
- Side bundle (heavy, 4.8 min): 20,461 parts, 774 marks, 17,964 exits; 131 stream parts placed by section length.
  test_part_geo.py 10/10 on it: the Chilliwack + Cowichan ranks and marks (words included) = the prototype's
  watermap.js; Chilliwack → Vedder part 0; every mid_sid in its part and inside its bbox.
- The tests caught an off-by-one: sid N is LINE N of section_handles.txt (1-based). Pinned in a no-data test.
- Offline harness (scratchpad p09/extract.py + offline.py) iterates in seconds: extract named geoms once, then run
  topology.write on a live-bundle copy.
- KNOWN GAP (unchanged): Kootenay Lake's marks are between lake-part ITEMS; decide at P15.
- T22 follow-ups committed: tokens test moved to app/tools (boundaries), fish-sprite.html regenerated.

## P08 stage 1 (DONE): the release writer
- `pipeline/deliver/release/` {build, paint, shards, manifest, __main__}; `python -m pipeline.deliver release` and the
  last step of `deliver all` (staged + promoted with the rest; report_all checks each release manifest's handles).
- Files: paint (status + flow runs/members/basins; paint/1), flow-area (target areas as varints, 0.19 MB; as a sid-keyed
  table they cost 0.9 MB), core (item + size/gauged/has_rows flags, alias, item_alias, item_section WITHOUT ROWID
  (sid, ord) + ord index, place; NOCASE indexes, OSM index dropped), near, conditions (gauge_clim → float16 clim/2),
  water (store common tables + display_rule + akey_region + part_section/part_geo (e6 ints)/part_mark (e6)/water_exit
  (no label)), regs-<9 regions> (cells + every row their frames reach), regs-sid. Each gz, sha-named; manifest app-manifest/2;
  data_release id = r-<sha12 of the file shas> unless given.
- Sizes (gzip): paint 0.93, core 1.27, near 1.99, conditions 2.88, water 2.75, regs-sid 1.22, regions 0.13–0.44,
  flow-area 0.19; TOTAL 13.88 MB (gate 14). The plan's 12.5 estimate left out the panel target areas.
- test_release.py: 3 no-data + 5 needs_bundle (on the P09 side bundle): only manifest files; sha + bytes true; every
  file's handles + format; card stamp in water; core ord = water item_ord; paint = status_index.bin + flow tables
  exactly; water + 9 regions + regs-sid = the regs store row for row; each region holds exactly its akeys' cells;
  two builds identical; size gates.
- Stage 2a (DONE): release/contract.py (exact tables + columns per file, units) → emit_pack →
  app/packages/data/src/pack.generated.ts; `--check` in pnpm check (`pack`) + pipeline-ci; a built release must equal
  the contract exactly.
- Stage 2b (DONE): app/packages/data/src/release/queries.ts (the 11 single-file reads, SEARCH rewritten to core's
  size + pieces flags; the cross-file reads listed for P11/T04 as two keyed reads). app/tools/release-perf.test.ts
  (Tier 2, RELEASE_DIR + BUNDLE): 11/11 parity on sampled keys (climatology within float16), keyed plans = SEARCH
  only, p95 A1 0.018 / A2 0.008 / A3 0.007 / A6 0.007 / A7 0.006 / A9 0.004 ms.
- OWNER DECISION PENDING: A5 search p95 5.4 ms against the 2 ms budget. The app's search is a substring scan (the bare
  name scan alone is 1.9 ms); the budget came from a prefix measurement. Options: accept ~5 ms / trigram index
  (+~1 MB, over the core gate) / prefix + word-prefix only (changes results). The test holds A5 to an 8 ms ceiling.
- The live bundle predates P07/P09: the next human `deliver all` (or pipeline build) writes format + part_geo, after
  which the release step runs in the chain on live data.

## P11 parts DONE: core paint decoder (paint.ts, fixture-held to the Python encoder; 28 ms real decode),
release/loader.ts (lazy, verified, per-region; real Vedder opens core + water + regs-2), release/install.ts (native
install/update core, atomic pointer swap). LEFT: the .web/.native adapters, source.ts routing, hooks (useStatusIndex →
paint, usePairing), App first-run screen, conformance on a fixture release — after the P12 workflow lands and N2.
P20 part DONE: stats.json + record() gone.

## P10 (DONE, except device): endpoints, fetchVerified, manifest diff/pair, publish
- app/apps/mobile/src/config.ts: ONE base (EXPO_PUBLIC_DATA_BASE; default the dev server), App reads ENDPOINTS;
  tools/no-localhost.test.ts guards it.
- @app/data: sha256.ts (pure TS; FIPS vectors), fetchVerified.ts (decoded bytes + sha256, gzip either way; fflate added +
  deps.md + layers.json), manifest.ts (parseManifest refuses unknown formats, diff per sha / tiles / basemap / assets,
  pair keeps the last good pair). release.test.ts 7.
- serve-tiles.mjs serves data/release as R2 will (/release/manifest.json = newest r-*, .gz with Content-Encoding gzip;
  RELEASE / RELEASE_ROOT env). Verified end to end against the scratch release (manifest → core fetched, decoded, verified).
- pipeline/tools/publish.py (HUMAN-RUN; dry-run by default; --yes uploads with R2_* env via boto3): tiles first, data,
  manifest last; immutable cache headers. test_publish.py 2.
- LEFT FOR P11: the resumable native variant (.part files) and deleting engine.web/native's duplicated fetch bodies
  (they still open the dev bundle until the reader switches).
- NOT verified: a device build reading the manifest from the LAN dev server (N1/devices pending).
- CAUTION: my server test ran `pkill -f serve-tiles.mjs`, which also stops a `pnpm tiles` server if one was running.

## N1: native map spike (BLOCKED on the owner, not started)
- It needs a native dev-client build (xcodebuild / gradle: heavy and long) plus a simulator or emulator.
  - On 2026-10-20 `xcrun simctl list` hung for more than 2 min (CoreSimulator not answering).
  - The Android SDK is at ~/Library/Android/sdk (it has emulator/ and build-tools/), but `adb` is not on PATH.
- Asked the owner: go-ahead for the native builds, and whether to fix the simulator / emulator setup.
- Spike rules (APP-PREP N1): throwaway branch, nothing merges; results go in handoff/PLAN/app-data/N1-SPIKE.md.
- Light tickets that can run meanwhile: P01, P02, P04, P12, P13, T22; P07 (it heads packaging; the full
  `deliver all` heavy run is batched with P05's acceptance).

## Baseline counts
- pytest fast: 3572 passed / 6 skipped (unfiltered; matches the UR7 report's 3462 plus the tests added since).
- `pnpm -C app check`: **RED at `fixture`.** `build-fixture.mjs` is deterministic (two rebuilds are byte-identical), but the
  committed `app/packages/data/dev/bundle.sqlite` is stale. The rebuild adds 6 empty tables (item_down, mu_set,
  region_place, section_straddle, trib_nearer, unnamed_part) and changes the `section_ruleset` schema. This is audit #5, so
  it **belongs to P03**: commit the rebuilt fixture there. The tree was restored to HEAD after measuring.
  Every step after `fixture` passes when run alone.

## Not verified
- MERGE GATE (cleanup lane A, worktree-wf_3c34e85f-9c5-1): `pipeline/tests/fixtures/answers_v35_port.json.gz` is NOT on
  the branch. `test_port_on_the_pages_own_ladder` runs in the CI no-data tier and FAILS (by design, never skips) until it is
  committed. Before merging, from the main repo root (golden dir c3ff564eb6b2a43d matches the live export's stamp):
  `~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/handoff/tools/heavy.sh v35-port-fixture 60 -- .venv/bin/python -m pipeline.tests.answers_v35_port data/generated/bundle/bundle.sqlite data/generated/regs data/generated/scratch/answers_reference/c3ff564eb6b2a43d/golden pipeline/tests/fixtures/answers_v35_port.json.gz 60`
  then `git add` the fixture (not gitignored; checked) and commit it on the branch before the merge.
(fill in)
