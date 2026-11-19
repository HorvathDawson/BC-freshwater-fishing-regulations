# Handoff 2026-11-19 ~02:50: ONE chat takes over (main chat + app chat both retired)

You are the only chat now. You do both the pipeline work the main chat did and the app/web/phone work the app chat
did. Use subagents (Opus for data/pipeline, Sonnet for UI/mechanical) in parallel when their files don't collide.

## The owner's ask (02:41, verbatim)
"then i am going to sleep and i think all these items plus the map speed up idea and other loose ends being finished
should all go into the release on staging while i sleep and i can test and release to prod tomorrow. does tat make
sense? maybe it is worth doing a handoff now and having aone chat do everything up to where we were plannign on ending"

**Tonight (the owner is asleep):** get everything below onto STAGING: the data release, the staging website, and a
staging Play internal build. **Tomorrow:** the owner tests staging, then runs the production steps (see the end of this
note), and you deploy prod web from the same commit. The version stays **0.8.2**: no 0.8.2 has gone to production
anywhere. Prod web 0.8.2 was stopped at 02:43 before it deployed.

## Rules (read AGENTS.md and CLAUDE.md first; these are the ones that bit us)
- Never run the LLM parser or anything that calls the `claude` CLI; give the command instead.
- **Never production.** Prod data (`release --prod`), the prod data and feeds workers, prod phone builds and
  Cloudflare are the owner's. Prod web: you may deploy it after he tests and says go (memory
  prod-web-deploy-when-tested). Staging deploys, staging R2, Play INTERNAL uploads and local release promotion are yours.
- Production Android always uses the patched MapLibre (T893; build-apk.sh refuses `--stock-maplibre` with `--production`).
- Heavy jobs only through `scripts/heavy.sh` (one at a time per lock; Android benches use
  `HEAVY_LOCK=$HOME/.claude/heavy-job-2.lock`). Never pkill. Never touch ports 39217/39218.
- Nothing merges into `redesign/stream-sections` without the full tier, the Android benches, and an independent review
  by a second agent (both reviews found real defects every time).
- Commit with `git commit -- <paths>`. Before the commit, run `node app/tools/check-tickets.mjs` and **check its exit
  code**. Never name a Tnnn in a commit before its ticket file and ledger row exist on the shared branch.
- In the main checkout: no stash, no `reset --hard` (the owner's review files live there uncommitted). Sync with fetch +
  `merge --ff-only`.
- Read `date` before writing a time. Record every owner answer in the ticket, OWNER-TODO.md and the To-Do page
  (claude.ai/artifact/Lh2ougAwfxx1wNmCV18m8f) in the same turn.
- Send the owner pictures (SendUserFile), never paths. Show him UI changes before they land; tonight, put them in the
  morning summary and he judges them on staging.
- Don't read `.env`. Never touch /Users/dawson.horvath/dev/personal/BC-prod-web except to rebuild prod web from a clean
  commit (below).

## State at handoff
- **Prod (live since 2026-11-18 22:30):** web 0.8.1 (d0b7bf863), data = the release named in the STATUS.md top entry. Workers: data `bc-fishing-r2`
  (data.canifishthis.ca) and feeds `bc-fishing-feeds` (feeds.canifishthis.ca). Fresh prod order: feeds deploy →
  `deploy/prod/sync-inputs.sh` → /run/dfo → data upload → /run/gauges.
- **0.8.2 app = `bae744a19` on origin/redesign/stream-sections** (content identical to 928a4294b). The full tier is green
  (7,760 vitest tests, pytest, mini_release). The Android bench passed 4/4 (pinch ~57 fps). Staging site deployed, smoke
  19/0/0. It reads the current prod data, so it does not need tonight's data release.
  Contains T876–T887, T888 (update note, phones only; the web keeps What's new in About, with no popup), T892 table
  marks (DFO/B.C., "tributaries of"), T893, T894 (version files per track/version; play.json carries `env`), T895
  forecast colours per model, T896 (the map key never folds), T897 (Back plus a sticky header on More pages), T898
  (sunrise/sunset times in daylight-only rule lines), T899 (in-season notices open by default), T901 (web Get the app:
  both stores "coming soon" plus Join the test buttons), T902.
  The changelog draft (`app/packages/core/src/changelog.ts`, 0.8.2, five bullets max) needs a line for each thing
  added tonight.
- **The main chat's state (02:45):** nothing is running or half-done. Data Part A has not started: no code, no branch.
  88c5471e8 (T891) and 10eb34d42 (T892) are ticket-only commits. Its old worktrees under
  `.../b8ddfca9-.../scratchpad/` are stale; delete them freely.
- **The app chat's state:** integration worktree
  `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/wt-081w`
  (branch app/integrate-082 = bae744a19). Its tier script: `scripts/heavy.sh <label> 40 -- bash $S/tier-081w.sh`, where
  `$S` = that scratchpad (it runs pnpm check, pytest and mini_release). The other `wt-*` there are merged lanes; delete freely.
  Two agents were still running there at handoff (results below when they land):
  - **T903 bench:** paint only what differs from a default colour (Salmon red; Conditions/Runs grey). Worktree
    `.../scratchpad/wt-t903` (prototype b66b1e58d, ticket 0b63be200, not merged). It lands only if it clearly wins on the emulator.
  - **T319 store screenshots:** 7 shots with splits, all from the web app, uploaded to Play (phone, 7", 10") and App
    Store Connect (6.9", 6.5"), plus the Play listing text, no video. The owner allowed the uploads.

## Tonight's work, in order
1. **Data Part A** (no owner review needed; estimate ~1 working day of agent time plus ~3 h for the release; run
   these in parallel lanes):
   - T869: the chinook annual limit says "adult" (owner picked wording A; see the ticket).
   - T870: the salmon card's Source (DFO) lists every DFO row behind it; whose bait ban, once; "all year". Pipeline
     `pipeline/deliver/salmon.py` place_gear, PLUS its twin in the feeds worker (the live salmon path). Deploy the
     worker twin to STAGING only.
   - T892 (PARTIAL): the trout-table row says when it comes from another water's tributary rule. It needs a per-row
     line from the pipeline (1,777 tables are partial today), then the app shows it in the same style as the salmon rows.
   - T891 part one: tiles carry `sc: 1` on side-channel pieces (`is_side_channel_piece`, export.py:685; add `sc` to
     the prop list in layers.py), and the app styles `["has","sc"]` thinner. Pictures go in the morning summary. Part
     two (braid simplification) waits for the owner.
   Then: staging release `pipeline release --staging --push` (add `--accept-fixtures <id>` if the golden page changes),
   promote locally, smoke with `node app/tools/smoke-staging.mjs`. Check that the 0.8.2 app reads the new release (the
   `app_reads.json` row).
2. **T903** (the owner's "map speed up idea"): the first bench was modest, not clear (result below). Rerun the STRESSED
   bench (it failed on app setup). If `paintDefault` + `labelMatch` clearly win there, land them with benches; if not, the
   numbers stay in the ticket and the morning summary says so.
3. **Loose ends, small and already approved:** T860 (Contact words), T859 (desktop More pages as a card), T864, T884
   if not done, T894 leftovers, T883's pipeline steps only if trivial. Skip T866 (test trim: after the releases), T867
   (waits for the owner's nod), T890 (joint) and T900 (Gear and Methods: owner said next chat. Make pictures as a
   PROPOSAL for the morning; don't land it).
4. **Integrate:** one integration branch on top of bae744a19, then the full tier, the Android benches (`regs-open
   water-nav part-steps zoom-pinch`), an independent review, and the push. Then the staging site
   (`deploy/staging/site-all.sh`, with the main checkout's release folder symlinked into the worktree), smoke, and a staging Play internal APK (patched MapLibre).
   The changelog gets new 0.8.2 lines, still five bullets at most.
5. **Morning summary for the owner:** what's on staging, what to test (with pictures), what didn't make it and why,
   and the production commands below.

## Tomorrow: production (the owner runs these after testing staging)
- Data: `release --prod` (it refuses agents).
- Android: `CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh` (= build-apk.sh --play --production, internal
  track; then he promotes it to closed testing in Play).
- iPhone: `CONFIRM_PROD=canifishthis.ca deploy/staging/build-ios-testflight.sh --production`.
- Web (yours, after his go): from a clean tree at the pushed sha (BC-prod-web; `rm -rf app/dist/{web-prod,seo-prod,prod-site}`
  first): export with `EXPO_PUBLIC_APP_ENV=production`, then build-seo on the data release prod serves (AFTER he
  publishes the new data), then `CONFIRM_PROD=canifishthis.ca deploy/prod/site.sh`, then
  `node app/tools/smoke-staging.mjs --production`.
- At public launch (not tomorrow): flip PLAY_PUBLIC / APP_STORE_PUBLIC in core `storeLinks.ts`, then run
  `pnpm run store-pages`.

## Later (owner needed, or next chats)
- Part B data: T880 (per-exception "leave out this stream" in the review app, then his marks), T885 (map all 31 DFO
  places, as proposals), T891 braid simplification (pictures first).
- T900 Gear and Methods; T866 test trim; T867 inbox; T890 word density; a better store video before the real release.
- From the main chat: T862 part 2, T840 (with T767), T665 stale ledger row, 38 stale bundle tests, branch structure
  main/staging/feature ("eventually"), PROD_R2 secrets for feeds-hydat/feeds-runtiming. T883 steps[] HELD.
- The owner owes: the CO question (water quota inside a share), the 71 book closures review page, the Search Console
  sitemap (optional), the ultra-review (T463), and `release --prod`.

## Results from the retiring app chat (filled in as the two agents land)
- T903 (agent, ~03:00): no clear win on the unstressed emulator run. The write path is already per-section and
  Conditions/Runs already write only sections with a value (7% / 22%); never writing the most common label saves only
  10% of writes. Salmon's real cost: linear `in` scans over a 920-label closed list (54,699 labels per feature vs Trout's
  4,318). Prototype = two map-lab switches, `paintDefault` (test the small complement) and `labelMatch` (hashed
  lookups); the look is unchanged (0 of 6.48M evaluations differ). Medians of 3: Trout→Salmon 319 → 306 → 298 ms;
  Salmon→Conditions 414 → 345 → 337 ms; Salmon pinch 53.0 fps/44 gaps → 55.6/30 → 53.3/39; pinch worker CPU 2,692 →
  2,390 → 1,780 ms (base → default → +match). The stressed rerun failed on app setup: rerun it.
- T319 store upload: (pending)
