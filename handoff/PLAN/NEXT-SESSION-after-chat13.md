# Next session, after chat 13 (written 2026-11-15 20:45, system clock)

You are the new MAIN chat (Fable). Read AGENTS.md and CLAUDE.md first, then this page, then the top of STATUS.md,
OWNER-TODO.md from "## 2026-11-15 evening" to the end (the owner's answers, verbatim), and
`handoff/PLAN/OWNER-RETURN-2026-11-15.md`. The second chat ("iOS chat", session name
`bc-freshwater-fishing-regulations-8f`, reach it with SendMessage) owns iOS, the feeds worker and the Android R8 build
work; its page is `handoff/PLAN/ios/START-HERE-ios.md`. Tell it you have taken over.
Owner 2026-11-15 night: the second chat is now THE APP CHAT (Android and iOS, feeds worker, app tickets), session
`bc-freshwater-fishing-regulations-01`; the Play build in step 4 is run by it once the main chat names the head.

## What the owner asked for last (verbatim, 2026-11-15 20:38)
"okay i think all dfo items are labelled now? can you please go through them all and double check there is nothing
weird then build them all into the app. then we need to make sure the skeena cascade of rules is correct still (i think
i review cadscade in app to make usre it looks proper but you should review too with fable) then we need to have fable
go over everything and really check everything is good. maybe even a general audit. ? first though should we reset you
to a new chat ? have all the stuff currently being worked on plus what i just said moved over?"

Note "with fable" / "have fable go over everything": for the cascade review and the general audit he wants Fable, so do
those yourself in the main chat (or with Fable subagents: this is the "really needed" exception to the rule that
subagents get Opus/Sonnet). Everything else: explicit `model` on every Agent call (opus pipeline/data/review-app,
sonnet docs/UI).

## Order of work
1. COLLECT what chat 13 left running. A cut lane was applying the owner's cut answers and a confluence sweep in the
   main checkout when chat 13 ended; its hand-back went to the old chat. Read
   `handoff/PLAN/curation-review/confluence-sweep-2026-11-15.md` (its report, "REBUILD INPUT 2") and
   `git status` / `git log`: uncommitted at 20:40 were `data/curated/waters/cuts.json`,
   `data/curated/dfo_salmon/places/region-2.json`, `data/curated/regulations/synopsis/region-2.json`,
   `pipeline/deliver/release/build.py` (size gate 320 kB to 420 kB), the T686 ticket and the report. Check each item of
   its brief is done or listed (Skeena Lakelse + Babine confluence cuts + Ferry Island bridge; Puntledge tailrace cut
   27 m up; Nanaimo 500 m cut (mark re-pointed, 38026b2bb); Sproat signs cut 50 m; Hell's Gate label (done); Sheldens
   Creek hunt; Shuswap closed stretch = real gauge point to DFO's other printed point; Thompson three closed areas
   accepted (849c93d6b); five side channels; size gate). Validate, run the full pipeline CI tier, commit with explicit
   paths, push. 15 local commits were unpushed at 20:40.
2. DOUBLE CHECK THE LABELS ("nothing weird"). Run the marks audit on the final marks (2,769 lines at 20:40):
   `PYTHONPATH="$PWD" scripts/heavy.sh marks-audit 40 -- .venv/bin/python -m pipeline.regs.review.audit --out <scratch>/audit-final.md`
   (runbook: `handoff/PLAN/curation-review/final-marks-audit-plan.md`). Then a second look by eye at what it lists,
   the way `resaved-marks-second-look-2026-11-15.md` did: tributaries (120 candidates on the trial), "old extent in
   force" (4), one-bank (Kitimat saved whole-width: must not ship as a whole-width closure), rest-of, mouth/tidal,
   offsets, unmarked (16 left at 20:30 by the app's counter). Known to check: the two Bulkley "all tributaries" places
   (saved by Claude as unsure), Blanchard (tributaries on, BC-only limit removed: confirm no Yukon water binds),
   Puntledge "from the weir" (should be weir cut to the existing 75 m cut, no new cut), Kasiks (inferred point, medium
   confidence, in GOTCHAS.md), Illiance / Wilauks (whole river: owner "Whole river is right"), Gitnadoix (whole river),
   Morice (signs point kept). Questions go to the owner in plain words; what a wording means is his call.
3. REBUILD AND RELEASE TO STAGING. One side atlas build with every new cut (the 23 are in; "REBUILD INPUT 2" adds
   tonight's; the owner's evening saves added more new cuts: run `proposals build` and read the list), parity fully
   attributed, promote (previous atlas kept: `atlas/full.b9de120e` is the one the LIVE release was built on, keep it),
   land the waiting marks, reach run, `pipeline build`, data-tier tests, then `python -m pipeline release --staging`,
   a FULL staging site build from a clean worktree (not --relink; it also writes the Apple link file), smoke
   (`node app/tools/smoke-staging.mjs`). Templates: `cut-cleanup-2026-11-15.md` section 5, `promote-2026-11-15.md`,
   `rebuild-finish-2026-11-15.md`, `stikine-part-names-2026-11-15.md` (rollback notes in each). Message the iOS chat
   BEFORE the staging release and after it (it redeploys its staging worker: `ios/staging-worker`). Locally now:
   atlas, tiles and reach are on the new build; the bundle and data/release/LATEST are still the release named in the STATUS top entry.
   Known stops already fixed: Kotcho Lake loop, Seton cut, six dead-end channels (Stikine/Taku), size gate (in the
   uncommitted build.py). Pallant tidal boundary at the bridge and T753 lake loops ride on it.
   A release that binds a closed-area piece is refused unless the worker serves `dfo.pieces` (staging does).
4. ANDROID BUILD 33 (0.7.11, R8 on). Six benches passed; merged (35dce9bf3); the build stopped three times at
   fast-fail (two proposal rows; an unlanded mark; a test of the iOS chat's that inherited the build's environment):
   all fixed, and `FAST_FAIL_ALL=1` under the build's environment passes. To build: a head that holds still (the
   owner's review saves make T185 commits), push, both CIs green on it (`scripts/ci_status.py`; ask the iOS chat to
   watch), then `HEAVY_LOCK=$HOME/.claude/heavy-job-3.lock CANIFISHTHIS_R8=1 deploy/staging/build-apk.sh --play`.
   Never `--no-fast-fail` / `--no-ci-check`. Before starting, run fast-fail the way the build does:
   `FAST_FAIL_BASE=$(cat app/dist/apk/last-play-sha) FAST_FAIL_ALL=1 CANIFISHTHIS_R8=1 MLN_LOCAL_MAVEN=$PWD/cache/maplibre/fslean-m2 scripts/fast-fail.sh`
   (12 minutes). Report version code, AAB size and whether `data/sourcemaps/0.7.11-b<code>-<sha7>/android/r8-mapping.txt`
   was kept to the iOS chat; it puts the phone walk on the owner's row. The iOS merges (ios/integration, T746, T738,
   T741, T756, T757 halves, T758, T759 halves, T739 last) come AFTER the upload.
5. SKEENA CASCADE REVIEW (Fable, you). The owner reviews the cascade in the review app's Cascade tab; you review it
   independently: region base + named overrides, every difference attributable (memory: base-plus-deltas invariant;
   zone rules are the base), the DFO chain order narrowest first, the Skeena mainstem places and its tributaries after
   today's landings (Kispiox signs areas, Kitsumkalum mouth, Kitwanga mouth, Hell's Gate cut, the new confluence cuts),
   the region-default sentences (T758), in-season notices (BC, not DFO). Write findings as a report with questions.
6. GENERAL AUDIT (Fable). His words: "have fable go over everything and really check everything is good. maybe even a
   general audit". Scope it before starting and tell him the scope; candidates: every commit of 2026-11-14/15 on the
   shared branch (many lanes, several CI reds, three lanes failed the full tier), the release content against the
   marks, the app's answers on a sample of waters per region against the sources, the code-review flag list in
   `NEXT-SESSION-after-chat12.md` plus today's (T573, T753, T755, T759, lake_loops, tributaries dead ends, anchors
   picker, pieces_gate and both upload scripts incl. deploy/prod edits never run). This is separate from the owner's
   own `/code-review ultra`, which he launches himself.
7. THEN: closed testing steps; production stays the owner's (old production commands on the To-Do page are out of
   date; build fresh ones after the staging release).

## Decided tonight, to be BUILT (owner answers in OWNER-TODO.md "2026-11-15 evening", rounds 1-6)
- T759 (lost closures): no "not known" status. Removal or exact replacement: nothing shown. A new wording we cannot
  match, a close (drifted) match he has not confirmed, or a reworded closed area: an ISSUE on that water: every stretch
  shows a banner in the salmon section quoting the row, the stream line gets a distinct look (same colour, like the
  bait-ban variation), until he acts in the review app. Python half: branch the branch named data/t759-missing-place-check on origin
  (5018cd328 / dfc95d971, worktree ~/bcfish-t759), not merged; 17 tests could not run in the worktree (run them in the
  main checkout before merge). Worker + app halves: iOS chat (branches feeds/t759-issues and app/t759-issues on origin). The line look
  needs pictures to the owner first. Day-one banners on today's pages: 0.
- A place whose last save is "wrong" with no fix: "I want you to for it to be redrawn … Just be safe". He re-marks
  them; until then such a place must not ship its disputed extent (4 flagged "old extent in force"). NOT BUILT.
- Rest-of extents: fix both (retired sibling stops taking water; an undrawable sibling no longer blanks the rest,
  visible note). NOT BUILT. Stamp "everything else" covers nothing now: check after.
- One-bank places (T754): build as proposed (bank = half the water; owner picks left/right; card names the bank;
  one-side stripe; tributaries off; rule-book Kitimat twin too). NOT BUILT; until then Kitimat west bank must not
  ship as a whole-width closure.
- Kept as saved: Seton mouth its own cut; Lillooet signs; Atnarko lodge sign; Kitsumkalum 22 m piece; Little Campbell
  and Babine sign cuts; Gitnadoix whole river; Morice signs point; Illiance and Wilauks whole river; lake rules (lake
  never takes its through-river; Sproat pond stays; creek-fed loops stay); tributary readings (Nass x3 mainstem = no
  tributaries; Babine Lake x3 + Meziadin "excluding tributaries" win; Somass + Stamp "X River tributaries" =
  tributaries only).
- iOS chat's two: push privacy wording as written and all fish puns: "Yes to both".

## Open defects and follow-ups (not yet ticketed unless numbered)
- Suggester proposes a NEW cut at the same position as an existing one when the words differ (Puntledge
  `puntledge_lower_weir_d75` vs the existing 75 m cut): match by position.
- Review app: when Confirm is greyed, name the unticked row and jump to it (owner got stuck three times).
- Hope gauge: the 66 m between station 08MF005 and the new bridge cut reads 08MF076: fix in station placement
  (matches.json is FROZEN). Three gauge cuts never placed in any recent atlas: 08HD024, 08ME002, 08MH032.
- T760 (iOS chat): first install waits for ever on a paused system download: main chat rules on "N minutes, how many
  tries". T756: reopen restarts downloads from zero; fix on a branch.
- Review of the iOS chat's changes to `pipeline/deliver/release/pieces_gate.py` on the branch named deploy/t757-worker-shape-refusal on origin
  (owed by main).
- Owner rule question, not asked yet: should fast-fail's review step read committed files instead of the working copy
  of his proposals file? Dark-mode highlight pick (T735; needs pictures). Unanswered rows on the To-Do page.
- Python page fixtures for the DFO vectors are older than the worker's saved pages (refresh).
- Worktrees to remove once merged: ~/bcfish-review-fixes, -review-followup, -marks-audit, -except-closed, -t573-build,
  -t752, -t753, -tatshenshini (keep ~/bcfish-t759, ~/bcfish-clean-latest, ~/bcfish-clean-staging).

## Rules that bit today (the rest are in AGENTS.md and the memory files)
- Before pushing pipeline or review-app code: the FULL pipeline CI tier in the main checkout, not a lane's selection:
  `.venv/bin/python -m pytest -q -p no:cacheprovider -m "not slow and not needs_bundle and not needs_atlas and not needs_reach_run and not needs_tiles and not needs_source and not needs_cache"`
  through heavy.sh. Four landings went red on test-layout rules (anchors, doc drift, emitter registry, reader
  registry). Lanes work in worktrees without built data: re-run their "could not run here" tests after landing.
- A command the automatic permission check refused to a lane is NOT run by the main chat; it goes to the owner, who
  answers in his own words (twice today: "You can run it", "Yes regenerate proposals if you need to").
- Read `date` before writing a time. Chat 13's stamps drifted twice.
- The owner's review saves commit to the shared checkout at any time; lanes that write there use explicit-path commits,
  `marks.append` only, and never edit review-app code under him (worktree, cherry-pick).
- Every owner answer goes verbatim into OWNER-TODO.md and the To-Do page the same turn. Decisions by question dialog,
  four per round, plain words; combine where one recommendation covers several.
- Network drops killed eight lanes: resume them with SendMessage, and make mark-writers check for double appends first.

## From the iOS chat at hand-over (2026-11-15 20:50): must not be lost
1. The STAGING WORKER is deployed from a scratch branch, not the shared branch: branch ios/staging-worker at
   173867c4c (push + crash limit + T757 worker + T758), version 92b74def. Any staging release or site/worker deploy
   from the shared branch puts the OLD worker back (no closed-area pieces, no default sentences, no push). Until the
   worker branches are merged: have the iOS chat re-deploy it right after, every time. Step 3 above bites on this.
2. T756 is NOT ready to merge: bg-install passed 8/8 once and failed once on the same code; after a reopen the app is
   silent for 16 s / 46 s / more than 284 s before any file reports, cause not found (T760).
3. the next Play build (R8): the tested app tree is 6a2dc612d plus one test file (0f32e4b0f, the R8 config test). ANY app/ commit after
   that means the six benches are re-run before the build. The ten-step phone walk is in the T749 ticket.
4. Owed to the owner as PICTURES before T759 ships: the banner on a real river and two or three looks for the stream
   line (banner words on his To-Do row t759-banner-words). The production feeds-worker deploy is his, after the merges.
Branch heads (all on origin, none merged unless said): T749 merged (35dce9bf3) + 0f32e4b0f; T756/T760 bench work
fix/t756-keep-running-job 0a17e1fe5; T757 worker feeds/t757-worker-pieces 230ad909d (on staging), app
app/t757-min-feed-shape fa0c7a1c5 (app/t757-piece-not-known a5af25920 is ABANDONED, never merge), deploy refusals
05383894d; T758 feeds/t758-page-defaults 179c6cba5 (on staging); T759 app 478896b84 stacked on fa0c7a1c5, worker lane
was still running. iOS merge order after the next Play build (R8): ios/integration; T746, T738, the crash-send app half, perf/t741,
T756 when green; push (T739) last (privacy wording approved; the line still has to go into the page's source).
