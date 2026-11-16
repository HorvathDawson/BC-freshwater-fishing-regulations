# The app chat: start here (iOS hardening page, written 2026-11-14; since 2026-11-15 the page of THE APP CHAT)

## RESTART HERE (written 2026-11-15 20:47 for a NEW iOS chat; everything below this section is history, newest notes are NOT at the top there)

**This chat is THE APP CHAT (owner 2026-11-15 about 21:55, relayed word for word by the main chat: "it is the andorid
and ios chat now not just ios. it is just the app chat."):** the Android and iOS app, the feeds worker, the app-side
tickets, Android bugs and phone fixes the owner reports, and the Play build. Data, pipeline, review app, releases and
the audits stay with the main chat, which keeps out of the app folder except what the release command re-cuts. Where
this page says "iOS chat" below, read "app chat". Session since 2026-11-15 21:27: `bc-freshwater-fishing-regulations-01`.

**WHERE TONIGHT STANDS (written 2026-11-15 23:25; read this first after a compaction):**
1. Owner 22:55: "The bad display oddities must be fixed on this build". T762 (the trout year line) is on the shared
   checkout as 4e793d61f; T763 (Kootenay band) is the main chat's and is on the branch too. Both ride tonight's release.
2. The six R8 benches run on lane 2 since 23:20 on the shared head with T762 (bench subagent, scratchpad worktree
   `r8b-wt`, APK tag `r8n`); about 00:40. WHEN THEY ARE DONE, TELL THE MAIN CHAT: it holds its staging release until
   then (the benches need staging's test channel on the live release). A bench that fails on a dead emulator line gets
   one re-run.
3. Then, on the main chat's "after" message: redeploy the staging worker (section 1), check the feed, tell it. On the
   head it names: `git diff --stat <bench commit> <head> -- app deploy/staging/build-apk.sh` may show only the
   release's fixture re-cuts and test files (else benches again), fast-fail under the build's environment, both CIs
   green, the Play build, the phone walk onto row `t749-r8-phone-walk`.
4. Owner's answers of 23:00 are in OWNER-TODO.md and the tickets. Held: every other merge (he did not answer the
   merge question). Owed in the morning: a summary; later: T760 "keep trying by itself", the T759 stripe for real
   (Salmon view only; colour is Claude's call), one picture of the final stripe.

**Added 23:53 (same night):**
5. The Mac's disk filled at about 23:38 (the main chat cleared 150 GB of test leftovers). The bench session
   `t749-r8-night` lost its first-install check to it (FAIL at 23:39, 0.6 of 479 MB); the bench subagent was told to
   rerun what failed in that window on the same APK. The main chat's build also stopped for its own reason (T778), so
   its "before the release" message will not come before about 01:30. It still waits for "benches done".
6. T764 (app tests leave their temp folders behind) is this chat's: ticket on the shared checkout (c5a53aaff), fix on
   branch `app-fix/t764-test-temp` @ 5d3f8e426 (scratchpad worktree `t762-wt`): every vitest run gets its own temp
   folder, removed at the end. Test and tool files only (not in the app bundle), so it does not void the benches. The
   main chat said: land it on the shared checkout any time BEFORE its "before" message, never between "before" and
   "after". Still to do before landing: `check:fast` on the branch. After the benches: clear the 13.5 GB of
   `maestro_xctestrunner_xcodebuild_output*` in the system temp folder (mine, no run uses them).

7. T764 LANDED on the shared checkout as e62331249 (23:54; `check:fast` clean; the main chat was told). Its files are
   test and tool files only, so the bench-diff check may also show `app/tools/test-tmp*.ts`, the two vitest configs
   and `app/packages/data/src/sql/drivers/releaseDir.ts` (node-only, not in the app bundle).
8. Two build lanes were started at 23:55 for the two things the owner answered at 23:00, each on its own branch,
   nothing merged, no emulator: (a) T760 "keep trying by itself" + the probe at the "Wi-Fi back" step + why the
   app-log capture was empty, Opus, worktree `t760-wt`, branch `fix/t760-stuck-download`; (b) the T759 stripe for
   real (Salmon view only, both platforms, legend, the folded-row mark, tests, pictures into scratchpad
   `t759-pictures/final/`), Sonnet, new worktree `t759-wt`, new branch `app/t759-stripe` from `app/t759-issues`.
   When (b) reports: send the owner the contact sheet with row `t759-line-look`.

9. 00:39: ALL SIX R8 BENCHES PASS (T749 ticket, last section; APK from 5aac6932e) and the main chat was TOLD
   "benches done". T764 is done (Maestro output cleared). Both build lanes reported: T760 keep-trying is on
   `fix/t760-stuck-download` @ 4e3bfffe9 (unit-tested, NOT benched); the T759 stripe is on a new branch (T759 ticket,
   last section; `check:fast` rc 0) and its pictures went to the owner with To-Do row `t759-stripe-final`. Lane 2 is
   free. NEXT: wait for the main chat's "before" then "after" message; then item 3 above (worker redeploy, bench-diff
   against 5aac6932e, fast-fail, both CIs, Play build on lane 3, phone walk row).

**Who you talk to.** The main (data) chat is session `bc-freshwater-fishing-regulations-17` (it took over
from chat 13 on 2026-11-15 evening; its page: `handoff/PLAN/NEXT-SESSION-after-chat13.md`). Tell it your session name
first. It messages you BEFORE and AFTER any staging release.

**Tonight's order of the main chat (as of 21:50):** two fix lanes for the second-look findings and a re-check of every
DFO extent, one more side atlas build, promote, `pipeline build`, the staging release + site build ("before" message
expected about 00:50), then it names the Play build head. **The Play build 0.7.11 with R8 is now THIS chat's:** on the
named head run the bench-diff check (section 1), fast-fail the way the build does
(`FAST_FAIL_BASE=$(cat app/dist/apk/last-play-sha) FAST_FAIL_ALL=1 CANIFISHTHIS_R8=1 MLN_LOCAL_MAVEN=$PWD/cache/maplibre/fslean-m2 scripts/fast-fail.sh`),
both CIs green (`scripts/ci_status.py`), then
`HEAVY_LOCK=$HOME/.claude/heavy-job-3.lock CANIFISHTHIS_R8=1 deploy/staging/build-apk.sh --play` (never
`--no-fast-fail` / `--no-ci-check`); report version code, AAB size and whether the R8 mapping file was kept to the main
chat and onto the owner's row `t749-r8-phone-walk`.

### 1. What you may be asked for at any moment
- **After a staging release: redeploy the staging feeds worker** from branch `ios/staging-worker` (173867c4c, on
  origin: push + crash limit + T757 worker + T758). A release or worker deploy from the shared branch puts the OLD
  worker back. How: a worktree of that branch with `app/node_modules`, `app/workers/feeds/.run-token` and a `.venv`
  symlink (the last one lived at this session's scratchpad `worker-wt`, it may be gone: make a new one), then
  `deploy/staging/feeds.sh` (staging only; it runs the worker tests, deploys, runs the three feed jobs). Then check
  `https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/status.json` (`"dfo": {"pieces": 1}`) and
  `https://data-staging.canifishthis.ca/feeds/dfo/index.json` (17 `#closed:` locations, 7 `:default:` places), and
  tell the main chat. Never production.
- **The CI watch for the Play build** when the main chat names the build head; then **the phone walk**: the ten steps
  in `handoff/PLAN/app-integration/tickets/T749-android-r8-code-shrinker.md` ("Phone walk for the owner") go onto the
  owner's To-Do row `t749-r8-phone-walk` once you have the version code. The tested app code is 6a2dc612d plus one
  test file (0f32e4b0f); `git diff --stat 6a2dc612d <build head> -- app deploy/staging/build-apk.sh` must show only
  `app/tools/r8-config.test.ts`, otherwise the six benches re-run first (install, crash-bed, bg-install, regs-open,
  tap-stalls, memory; `--no-r2`; lane 2).

### 2. Branches (all on origin; NONE merged into `redesign/stream-sections` unless said)
| Ticket | Branch @ head | State |
|---|---|---|
| T749 R8 | merged (35dce9bf3) + 0f32e4b0f | on the shared branch, flag off by default; Play build not made yet |
| T756 reopen keeps downloads + T760 bench + T760 stalled-fetch fix | `fix/t756-keep-running-job` @ d4eed7594 | NOT ready: the silent wait after a reopen is found and fixed on the branch (a small file's fetch that stalls after its headers was never cut; T760 ticket, "Added 2026-11-15 late"); three `bg-install` runs of d4eed7594 started 21:54 on lane 2 |
| T760 works 2 + 3 (stuck system download restarted; setup screen says when nothing moves) | `fix/t760-stuck-download` @ 53d9610d4 (stacked on d4eed7594) | written, unit-tested, NOT run on the emulator; words are a proposal (To-Do row `t760-download-words`) |
| T757 worker (closed areas emitted) | `feeds/t757-worker-pieces` @ 230ad909d | LIVE ON STAGING |
| T757 app (minimum feed shape) | `app/t757-min-feed-shape` @ fa0c7a1c5 | written, tested |
| T757 app, first try | `app/t757-piece-not-known` @ a5af25920 | ABANDONED (the owner ruled out "not known"); never merge |
| T757 deploy refusals | `deploy/t757-worker-shape-refusal` @ 05383894d | written; the main chat owes a review of `pieces_gate.py` |
| T758 region-default sentences | `feeds/t758-page-defaults` @ 179c6cba5 | LIVE ON STAGING; production worker deploy is the owner's |
| T759 app (issue banners) | `app/t759-issues` @ 1289d8c20 (stacked on fa0c7a1c5) | written, tested; line look: see the next row |
| T759 line look, PROTOTYPE for pictures | `app/t759-line-looks` @ 1bddb6181 (on 1289d8c20) | web only, three looks behind `?issuelook=A|B|C`; never merge as is: after the owner's pick (To-Do row `t759-line-look`) it needs the native half, a legend entry, tests, the theme and colour-blind checks |
| T759 worker + T757 item 5 | `feeds/t759-issues` @ 8bd68cce2 (stacked on 179c6cba5 + the main chat's dfc95d971) | written 20:50, tested (506 worker tests, re-run by me); not deployed |
| T759 Python (the main chat's) | `data/t759-missing-place-check` @ dfc95d971 | the rewritten ticket is on this branch |
| iOS | `ios/integration` @ 639d54d0d, `ios/t746-apply-splash`, `ios/t738-bar`, `ios/crash-send`, `perf/t741` @ a02e09fb5, `ios/t739-push` @ ca91889bb, `ios/next` @ a2078ac33 | waiting for build 33 |

**Merge order after build 33 is uploaded (agreed):** `ios/integration` (two small known conflicts: take the shared
side's comments in `app/packages/ui/src/prefetch.render.test.tsx`, keep both sections in the T723 ticket); then T746,
T738, the crash-send app half, `perf/t741`; the T757 worker + T758 + T757 app (min feed shape) + deploy refusals +
T759 halves when the main chat has reviewed its part; T756 only when green; push (T739) LAST. Each needs the Android
benches where it touches shared app code and a message to the main chat first; each green on CI.

### 3. Owed to the owner
- **Pictures for T759: SENT 2026-11-15 22:53** (page https://claude.ai/artifact/T5yS8HymzuhAXq8kYaLEwL and in chat);
  his picks are on To-Do row `t759-line-look` (A halo / B edges / C stripe; magenta or black; a mark on the folded
  Salmon row). Files: this session's scratchpad `t759-pictures`.
- **Rows on his To-Do page (claude.ai artifact, collection `todo`):** `t759-banner-words` (the banner's words and
  the T757 fallback sentence, proposals, open); `t758-prod-feeds-worker` (production worker deploy, his, later);
  `t749-r8-phone-walk` (gets the walk after the upload). Answered and ticked: privacy wording + fish puns ("Yes to
  both"). Still to do by Claude from that: the privacy line into the page's source on the push branch.
- Open from 2026-11-14, unchanged: iOS day defaults, oldest iPhone + iPad, links without a Team ID, report email
  "linked", T686 picks, backup words, keyboard/back picks, clean-up calls, Play location answer; and his phone checks
  on TestFlight build 36 and the push test.

### 4. First things to do
1. (Done 20:51, kept for the record.) The T759 worker lane finished: `feeds/t759-issues` @ 8bd68cce2. The worker's
   whole `index.json` equals the Python's on the saved pages and on 16 edited cases (every issue kind, the unread
   kinds, `gone`, the two T758 cases); `index.json` carries `issues` and `dfo` (one constant shared with
   `/status.json`); `/status.json` gains `dfoIssues`; the old "check" verdict for a reworded row is removed; notices
   equal the Python's word for word except one named action line. 506 worker tests, `check:fast`, emitter `--check`,
   48 pytest. Saved pages: 9 issues on 7 waters, all `unmatched`/`undrawn` (the main chat's live sweep: 8 on 6; the
   extra one is Region 3's Fraser row, seeded but not drawn in the entry copies; why only it differs is not pinned
   down). Two Python behaviours it mirrors that the ticket does not state: a confirmed or proposed variant also makes
   its original place `gone`; a region not asked is listed `not asked` only when the last feed had it. Also: an
   unchanged page (not re-typed by the worker) carries its issues from the last feed. NOT deployed to staging, and by the main
   chat's word of 2026-11-15 evening it is NOT to be deployed until T759 merges (after the Play build is uploaded and
   the owner has seen the banner pictures): across the staging release, redeploy only `ios/staging-worker` 173867c4c.
2. (Found 21:50 by the new iOS chat, session `bc-freshwater-fishing-regulations-01`.) T760: the app waited on its own
   fetch of a small data file (`salmon-5`), stalled after its headers and never cut: `shareFetch` drops the asker's
   abort once headers arrive and Android's HTTP client has no read timeout. Fixed on `fix/t756-keep-running-job`
   d4eed7594 with tests; the emulator bench is running. Left of T760: works 2 and 3 (need the main chat's N and count).
3. The line looks and banner pictures for the owner (web build of the T759 app branch on a real river).

### 5. What went wrong today, so it is not repeated (also in memory)
- I reported "the internet line is down" for two hours from a probe that was timing a 404. A probe checks the status
  code and is proven once against a known-good answer.
- The emulator's system downloader kept half-finished rows from cut-short runs; `pm clear` does not remove them. The
  bench on the T756 branch now uninstalls first; the shared branch's bench does NOT yet (T760).
- A test that spawned `expo config` with the caller's environment passed everywhere except inside the real build.
- I wrote clock times without reading the clock, several times. Read `date` first.
- A branch name in backticks that looks like a path (`app/…`) fails the docs guard in OWNER-TODO.md.


## Midday update, 2026-11-15 (written 12:49)

**The R8 build is NOT on Play yet.** You picked "R8 build to Play internal, if the checks pass on that exact build".
Five of the six checks passed on that exact build. The sixth (first install that carries on with the app closed) did
not pass, and the reason is not the build: the same check fails the same way with the shrinker off. The cause is this
Mac's internet line, which was crawling from late morning (a 25 KB file from our staging server took up to 20 s at
12:46; this morning the whole 479 MB came down in 233 s). I do not call a check passed that did not pass, so nothing is
merged and nothing is uploaded. A watch on the line is running; when it is fast again I re-run that one check and, if
it passes, the Android chat makes build 33 (version 0.7.11) and uploads it to Play internal. If the line stays bad
into the evening the build slips to tomorrow.

| check (shrinker on, branch `perf/r8-play` 164c928c2) | result |
|---|---|
| install | pass 4/4 |
| crash reports | pass 5/5 |
| tap to regulations page | pass 26/26 (cue 33–89 ms, page 333–370 ms) |
| no long stalls | pass 5/5 (worst 138 ms) |
| memory and rule book | pass 14/14 |
| first install with the app closed | NOT passed in 3 runs; fails the same with the shrinker off; internet line |

**Changed from what I told you at 10:35.** The Android chat makes the Play build and uploads it (that step uses the Play
sign-in); I run the checks and merge the switch, off by default. Build 33 also carries what the Android chat already
put on the shared branch since build 32 (T737 web loader, T747 search opens the water map on its part, T750 one-line
Upstream / Downstream). None of my other branches are in it.

**A defect found on the way (T756), not caused by the shrinker.** On Android, if you choose "Wait for Wi-Fi" on the
first install and then open the app again while it downloads on Wi-Fi, the app cancels the downloads and starts the big
files again from zero. It has been like that since the background download was built (T298). A fix with tests is on
branch `fix/t756-keep-running-job` (not merged, no phone or emulator run yet). It is not in build 33.

**New for you:** To-Do page row "Push: the new privacy-page line and the App Store privacy answer". The push code
merges only after your yes or your changes to those words.

**iOS merge:** you said it can merge whenever. Order agreed with the Android chat: the four iOS fix branches first, right
after build 33 is uploaded; then the loading screen, bar fix, crash-report fix and faster-and-lighter set after one
Android run; push last. A trial merge of step 1 has two small conflicts (one test file, one ticket).

**Added 13:08.** (1) Disk: I deleted seven orphaned Xcode build caches from the TestFlight builds and four simulators
of finished lanes (integ-17, t739-17, t741-17, t746-17); free space went from 20 to 32 GB for the Android chat's atlas
build. The crash symbols of builds 33–36 and simulator next-17 are kept. (2) Coming from the Android chat, not started:
the feeds worker must emit the "closed areas inside a DFO place" pieces (T573) before any release binds one, or the app
would show such a closed area as open. Nothing binds today. I take it as T757 when its spec lands
(`handoff/PLAN/app-integration/tickets/T573-files/worker-handoff.md`); I asked for a mechanical guard (the release
refuses when the worker does not report the new feed shape) and for the app to say "not known" instead of "open".

**Added later the same evening (about 20:50), after the owner's T759 decision was written up by the Android chat
(`origin/data/t759-missing-place-check`, ticket rewritten there):**
- T757's app half is replaced: `app/t757-min-feed-shape` fa0c7a1c5 (a release that binds a closed area + a feed that
  does not say `dfo.pieces >= 1` → the release's own salmon calendar, said on the card, reported once);
  `app/t757-piece-not-known` is abandoned. Deploy refusals: `deploy/t757-worker-shape-refusal` 05383894d (staging and
  production feeds deploy refuse a worker going backwards; `RELEASES_DIR` names the release folder from another
  checkout; `pieces_gate.py` additions wait for the Android chat's review).
- T759 app half: `app/t759-issues` 478896b84 (on top of the branch above): `issues` parsed, one banner per issue on
  every stretch of the water through its registry items, DFO's rows verbatim, no status or colour change; `undrawn`
  already "not mapped", a region not read, and no feed all show nothing. Words are a proposal on the owner's row
  `t759-banner-words`. NOT built: the stream line's "issue" look; `salmonIssueItems(payload)` gives the items for it.
  OWED TO THE OWNER AS PICTURES: the banner on a real river and two or three line looks.
- T759 worker half + T757 item 5: lane running on `feeds/t759-issues` when this was written (issues, drift applied,
  feed shape in index.json, issue counts in `/status.json`, the old "check" verdict removed). Check its report first.
- `no_water` issues (a water no entry knows): shown nowhere in the app, by the Android chat's ruling; the owner's
  notice covers them.

**State 20:02, 2026-11-15 (evening; the owner is back and reviewing in the review app). Newest first.**
1. **Build 33 (R8, Play internal): NOT built yet; nothing is wrong, it waits for a quiet shared head.** The owner
   answered the blocking item ("Yes regenerate proposals if you need to"); two more stops followed and are fixed (an
   unlanded review mark, the Android chat's; and a test of mine, `app/tools/r8-config.test.ts`, that passed
   everywhere except inside the real `--play` build because it let the spawned Expo config inherit the build's
   variables: fixed in 0f32e4b0f, test-only). The Android chat ran the build's whole quick-test step under the build's
   own environment: all passed. Every save in the review app makes a commit on the shared checkout, so the head
   cannot hold still for CI + build tonight: **the Android chat builds when the owner stops reviewing; it names the
   build head, I watch both CIs, it builds.** Then I put the ten-step phone walk (in the T749 ticket) on the To-Do
   row `t749-r8-phone-walk`. The tested app code: only that one test file differs from the benched 6a2dc612d.
2. **Owner answers tonight that are mine (OWNER-TODO.md "owner answers, round 6"): "Yes to both".** Push privacy-page
   line and App Store answer approved as written (T739; left: the line into the privacy page's source on the push
   branch; putting it live is his); all fish puns stay (T746). Both To-Do rows ticked.
3. **T759 is decided by the owner, differently from the proposal: NO "not known" status** (rounds 2, 3, 5, 6). A place
   DFO removed, or replaced by a row we match exactly: no change, the remaining rule answers. A NEW wording we cannot
   match on a river, or a close match he has not confirmed: the status colour stays, the stream line gets an "issue"
   look, and every stretch of that river shows a banner in the salmon section quoting the unmatched rule, until he
   re-matches or confirms it. The Android chat rewrites the ticket and the Python half tomorrow. **I hold: the worker
   and app halves of T759, and `app/t757-piece-not-known`** (T757's app half reads a missing closed-area piece as
   "not known", which his rule now contradicts; it is reworked inside T759's model). T757's worker half and T758 stay
   live on staging and are unaffected.
4. **T756 / T760 (first install), branch `fix/t756-keep-running-job` 0a17e1fe5, not merged:** install passes; a reopen
   mid-download keeps its bytes; `bg-install` passed 8 of 8 once (the notification tap opens the app) and failed once
   on the same code: after the reopen the app was silent for more than 284 s (16 s and 46 s in the runs that
   finished). What it waits on there is NOT found: first thing to chase (T760). The bench now does a real
   uninstall + install, reads the app's own byte line, and keeps both logs on a failure.
5. **Lessons written to memory tonight:** a probe must check the status code (my "line is down" was a 404); a test
   that spawns a process gets a minimal environment and is run under the build's variables.
6. **Next session, in order:** (a) the build 33 lap when the Android chat calls it; (b) the phone walk onto the row;
   (c) the iOS merges in the agreed order (`ios/integration`; then T746, T738, the crash-send app half, `perf/t741`,
   the T756 fix after one more green `bg-install` + `install`; push last, now unblocked on wording); the T757 worker
   branch and T758 go in with them, the T757 app branch does NOT; (d) T760's silent stretch; (e) T759's worker and
   app halves once its ticket is rewritten; (f) the privacy line on the push branch.

**State 17:18, 2026-11-15: the R8 Play build (build 33) is ready on every side but one, and that one needs the owner.**
- All six emulator checks passed on the shrinker-on build of the merged code (clean run 15:54–16:43: 61 steps pass,
  none fail; install map up 481 s; tap to regulations page 344–360 ms). Report in the R8 lane's worktree:
  `app/tools/testbed/out/runs/2026-11-15T22-54-45/index.html`.
- Merged into `redesign/stream-sections` and pushed 16:50: head 35dce9bf3 (shrinker off by default; its app code is
  byte-identical to the tested commit 6a2dc612d). Both GitHub runs green on it (app 43776985369, pipeline
  43776985305).
- **Not built:** the Android chat's `CANIFISHTHIS_R8=1 deploy/staging/build-apk.sh --play` stopped in its quick test
  step: `pipeline/tests/test_dfo_suggest.py::test_the_hunt_file_covers_its_targets_and_names_real_cuts` fails in the
  main checkout because it reads the owner's UNCOMMITTED `data/generated/review/dfo-suggestions.json`, where two rows
  merged at midday lack their evidence. GitHub is green because it reads the committed copy. Nobody skips the test
  (`--no-fast-fail` is forbidden). The fix is to regenerate those two rows; when a lane of the Android chat tried,
  the automatic permission check refused it, so no agent does it: it is the first item on the owner's To-Do page and
  on his return page ("regenerate the two proposal rows"). Nothing built, no version commit, nothing uploaded.
  Build 33 follows his word: tonight if he answers, otherwise tomorrow. A CI re-run on the then head comes first
  (the Android chat is pushing pipeline commits meanwhile; it leaves app/ and build-apk.sh untouched so the tested
  app code stays the head's).

**State 16:08, 2026-11-15 (newest first; the notes below it are older).**
1. **R8 Play build (build 33): still possible tonight.** The 15:17 run of the six checks failed at the first one, and
   I traced why on the emulator instead of guessing: the system's downloader still held half-finished atlas and
   basemap downloads from the morning's cut-short runs; the freshly cleared app adopted them, and they never moved
   ("paused, 18 of 197 MB", the same bytes for 35 minutes). That, not R8 and not the internet line, is very likely
   what failed this morning's sixth check too. After an uninstall + install of the same APK the clean run started
   15:54: install PASS (map up 481 s), crash reports PASS; four checks to go, due about 17:10. If all pass: merge,
   CI, and the Android chat builds for Play internal tonight; if not, tomorrow.
2. **A real bug came out of that (T760, open):** a first install waits for ever, without a word, on a system download
   that is paused and never moves. On a phone a battery saver can do the same. Also in the ticket: the bench must
   clear the downloader between runs.
3. **Region-default salmon sentences were missing under live data (T758): fixed and live on STAGING 15:57.** With
   the live feed, waters covered only by a region's default rule showed the right status (closed) but a generic line
   instead of DFO's own sentence; about 1.1 M stream sections. The worker now publishes the seven default places
   and equals the pipeline's feed on all 435 chains of the staging release. Production's worker has the same gap;
   that deploy is the owner's, after the merge.
4. **A wider question went to the owner from the Android chat (T759):** when DFO rewords or drops a restricting
   place, the wider opening answers and a closed stretch can read open (worst example found: a Campbell River
   closure). Proposal on branches only: such places read "not known, check the DFO notice". My parts (worker port,
   app backstop using a new `known` list in the feed) start after build 33; nothing deploys before his answer.
5. **T756 (reopen restarts downloads):** the fix now also keeps a paused download that already has bytes, every
   restart from zero is written to the log, and the bench reads that line (branch `fix/t756-keep-running-job`
   81591d974, not merged, not yet run on the emulator).
6. **T757 (closed areas inside a DFO place):** worker half live on staging since 15:13; app half on its branch.

**CORRECTION 15:19 (my error).** The internet line was NOT down all afternoon. My probe asked for the staging basemap
without its `release/` prefix and timed a 9-byte "not found" answer from 13:04 on; "30-46 bytes a second" was that.
Measured properly at 15:16: 8 MB in 4.7 s (1.77 MB/s). The failures up to 12:49 were real; when the line came back
after that is not known. So the "still down" lines in the two notes below are wrong, and the R8 Play build (build 33)
is back on for TODAY if the checks pass: all six are running since 15:17 on the shrinker-on APK of `perf/r8-play`
6a2dc612d. Also at 15:13 the feeds worker with T757 went to STAGING (version f2b98bd4): its status says
`"dfo": {"pieces": 1}` and the live feed carries the 17 closed areas. Found on the way: the worker's live feed has no
answer for the 7 region-default places (`1:default:region` and six more) that the Python feed makes from the page's own
sentence; older than T757; being ticketed, first step = what the app shows for such a water today.

**Added 14:44.** (1) The internet line is still down (30–46 bytes a second on every probe from 13:04 to 14:39), so
the sixth R8 check cannot be re-run. The Android chat now plans the R8 Play build (build 33) for TOMORROW. It is
landing its desktop split (T752) on the shared branch first, so I re-run all six checks on that new head, not on
164c928c2. Final word to it at 17:00 as promised; if the line recovers before then I still re-run the sixth check today.
(2) T757 started: the spec landed with the Android chat's push (b58aa325a … 554558a12). Two lanes on branches, nothing
merged: `feeds/t757-worker-pieces` (the worker emits the pieces and says `"dfo": {"pieces": 1}` in `/status.json`,
tested against the Android chat's oracle file) and `app/t757-piece-not-known` (a bound piece missing from the feed
reads "not known, check the DFO notice" and is reported, never open). No staging worker deploy without the Android
chat's word. Its release step already refuses an upload that binds a piece when the worker does not say it emits them.

**Added 14:58.** (1) T757 is written, both halves pushed, nothing merged: worker `feeds/t757-worker-pieces` 2523097a9
(479 worker tests pass, equal to the Android chat's oracle), app `app/t757-piece-not-known` a5af25920 (the card reads
"Not known: today's DFO check does not include this closed area. Check the DFO notice before you fish."). Left: the
Android chat's read, a staging worker deploy with its word, merge after build 33, one emulator look. (2) The R8 branch
`perf/r8-play` carries the new shared head (merge 6a2dc612d, pushed) and its shrinker-on test APK is built
(`app-testbed-stub-r8play2.apk` in the R8 lane's worktree): tomorrow only the six runs remain. (3) T756's bench changes
are written and unit-tested on `fix/t756-keep-running-job` c0ad16d62, not yet run on the emulator (needs the line).

## Morning summary, 2026-11-15 (written 07:32; the night's work in one place)

**For you to do, in this order**
1. **Install TestFlight build 36** (uploaded 07:32; it replaces 34 and 35: everything below is in it). App Store
   Connect → TestFlight → your internal group → add build 36; on the iPhone, TestFlight → install.
2. **Try on the phone what only a phone can show:** start the first download, lock the screen a few minutes, switch
   apps, swipe the app away and reopen (it should carry on, and the bar should show what is already down); tap a
   river (it should light at once); left-edge swipe on About, Help, Layers.
3. **Push test:** More → Notifications → turn on "New regulations data", allow notifications, put the app in the
   background, tell Claude; Claude sends one test push; then More → Notifications → "Last background checks" should
   show a newest line starting "woken by our server".
4. **Answer when you can** (also on the To-Do page): which fish-pun lines stay; the open picks further down
   (questions 2 to 5, 9 to 13, 15 to 18). ANSWERED 10:33: the iOS work may merge whenever ("ios work could merge
   when ever I think?"), in three steps timed with the Android chat; and the R8 build goes to Play internal if its
   checks pass ("if that build passes the tests then it should be good to release").

**What build 36 carries (branch `ios/next`, app CI green, nothing merged into the shared branch)**
| part | what it does | how well proven |
|---|---|---|
| iOS fixes (T719, T722, T723, audit) | background first download, early tap cue, keyboard and back-swipe fixes | iPhone simulator and Android emulator, quiet machine: pass; Android unchanged |
| First-install bar (T738) | the number counts the big files from the start and after a relaunch | simulator: bar 325 MB with 362 MB on disk, never went down; Android not run |
| "Applying new data" screen (T746) | fish and a pun line, no taps, a failed apply says so | simulator and web: seen, timed; Android not run |
| Push wake-ups (T739) | the server wakes the phone for alerts and new data | registering proven on the simulator; the wake itself needs your phone (step 3) |
| Crash reports (T718) | a refused report is logged and reported; test runs no longer use up the daily limit on staging | unit tests; staging worker deployed 02:05 |
| Faster and lighter (T741) | cold start 786 ms against 1,030 ms (simulator); app 3.9 MB smaller (8 fonts, not 41); background checks ask "changed?" first | simulator, measured; Android not measured |

**Checked on the combined build this morning (iPhone simulator):** walkthrough 41 of 41, tap a water 3 of 3, offline
3 of 3, satellite; one first-open step failed on a test-bed timing fault (a notice covered the tabs). Second session
(ended 08:33, quiet machine): rule book, keyboard and back swipes pass, the photo viewer included; tap to page: cue 7
to 99 ms after the lift, page 372 to 388 ms on 15 of 18 taps (the 3 Cultus taps opened a test spot an earlier check
had left on the lake: test bed); no long stalls, but the worst at cold start is now 268 ms against a limit of 300
(214 ms on the build before the faster cold start: less room, to watch on Android and a real phone). **Found, app:**
on Conditions → Alert me the number pad covers the value field (written into T723; in build 36 as before).

**Android's code shrinker trial (T749, R8; branch `perf/r8`, behind a switch that is off by default; done 10:09)**
- **It works on the emulator.** The shrunk app starts and passes all six checks with no keep-rule fix needed: install
  4 of 4, crash reports 5 of 5, tap to page 26 of 26 (cue 31 to 86 ms, page 338 to 373 ms), background install 7 of 7
  (the path that crashed the first attempt), no long stalls (worst 141 ms), memory and the rule book 14 of 14.
- **Lighter:** test APK 55.0 MB against 62.4 MB (app code 7.9 MB against 30.4 MB); the Play download saving is
  estimated at about 7.4 MB (no Play bundle was built). Memory at boot about 35 MB lower (248 MB against 282 MB, one
  boot). **Speed:** cold start and tap to page the same within drift; the rule book opened 3 to 6 % slower in two
  samples (unconfirmed).
- **Android cold start today, for reference:** about 1.5 to 1.65 s on the emulator from process start to the first
  drawn frame (the faster launch decode from T741 is not in that build).
- **Not proven:** a real phone; Play's own processing; paths no check walks (the background task firing later, Play's
  in-app update, a notification tap, the photo picker, location, the rule book's table of contents).
- **Recommended next step, yours to decide:** one Play INTERNAL build with the switch on and a phone walk (update,
  notifications, a spot photo, location, the rule book). Before it, the build script must keep R8's mapping file so
  crash reports stay readable.

**What went wrong overnight (mine unless said)**
- A push from one of my lanes turned app CI red on the shared branch for about 40 minutes (a list test); fixed.
- I first told you crash reports were not held back by the daily limit; they were (the limit's day turns at 17:00).
- My noted clock times ran up to an hour fast from about 21:45; corrected against the logs.
- The first TestFlight tries failed twice on setup (no registered iPhone; an API key without the Admin role, which my
  own steps had asked for).
- Three Android bench runs failed on the test bed's driver under load; they passed on a quiet machine.

**Not done**
- Android runs for T746, T738 and `perf/t741`; T721 (edge-swipe grid), T726 (four iPhone
  sizes, large text: the pun line "Casting the new regulations into place…" wraps at the largest text size; the photo
  viewer's "Photo 1 of 1" sits under the clock), T727 screenshots and video, the T731 step into the next water.
- Nothing is merged into `redesign/stream-sections` except the crash-limit worker commit (your yes). No Play upload,
  no production, no release.

## Summary of the unattended day, 2026-11-14 (state at 03:20 on 2026-11-15; lanes still running are marked; times from 21:45 on were corrected against the logs at 03:20, earlier notes of mine ran up to an hour fast)

Nothing here touched Play, production, the atlas or a release. The Apple account was used only for the owner's first TestFlight build in the evening (below). Every app
fix sits on its own branch, not on `redesign/stream-sections`, until it is proven on the simulator and the Android
benches have run. Tickets: IOS-n is T(717+n), so T718 to T727; the backup proposal is T732.

**Done and proven**
- **The app builds for iOS as it is (T718).** 0.7.9 built first time for the simulator; compile 122 s. Needs iOS 16.4 or
  newer. Already portrait only. Home-screen name "Can I Fish?".
- **Baseline on the simulator (T718):** 8 of the 11 existing checks pass outright; the walkthrough passes 41 of 41.
  Offline (one step, a test-bed fault), location (one step skipped) and crash (one step) do not; see failed.
- **No long stalls (T720):** the same bench as Android passes; worst stall 227 ms at cold start, limit 300 ms.
- **Memory (T725):** peak 274 MB on 0.7.9 against 290 MB on 0.5.0. The rule book does not leak. Cold start to map about
  0.6 s. A tap opens a page in 54 to 123 ms.
- **Tap speed measured (T719):** the page opens as fast as on Android (about 400 to 550 ms after the finger lifts). The
  early cue is missing: it lights 371 to 495 ms after the lift, Android 28 to 73 ms. Cause confirmed: iOS waits 350 ms
  to rule out a double tap.
- **Shared links (T724):** the site build can now write Apple's link file and the app declares the two domains. The real
  hand-off needs your Team ID and a signed build.
- **App Store text pack (T727):** `handoff/PLAN/ios/store/`: listing (Play's text, fits Apple's limits), privacy answers,
  age rating, review notes, checklist. Re-checked against the privacy page reworded today.
- **Two audits of iOS against Android, read from the code:** 29 findings in `handoff/PLAN/ios/parity-audit.md`.
- **Pipeline lane (finished its list):** T667 parts 4c, 6 and 7 done. T686 survey done and ranked, stopped before your
  pick. T667 slice 8: a small invented release (53 files, 396 KB) is built by the real pipeline from invented inputs in
  about 8 s, two runs identical; it is committed in the app, CI rebuilds it and compares on every pipeline change, and
  the first app test file runs on it. Left for later sessions: about 36 more app test files, one package at a time.
- **Clean-up lane:** 219 files, about 29 MB, removed; 18 candidates listed in `handoff/PLAN/CLEANUP-CANDIDATES.md`.

**Built on a branch, proof still running or waiting for the simulator**
- `ios/t722-install` (**proven on the simulator 18:45**, not merged): the iOS first download now runs with the app not
  in front and with the app closed. In the run: 152 MB arrived in 60 s with Settings in front; after the app was
  force-closed mid-download it kept going, and the relaunch picked up the same downloads with nothing fetched twice;
  installed about 6 minutes after first open. Also on the branch: free-space check before a download (both platforms),
  downloaded data kept out of iCloud backup (seen on the files), iOS-only wording for the two promises that do not hold
  there, Wi-Fi-only refuses hotspot and Low Data Mode, the progress-number fix. Needs a real phone: lock screen,
  cellular, hotspot, swipe-away. Found in the proof and not fixed: the bar reads low while the big files download
  (T738, both platforms).
- `ios/t723-keyboard-back` (**proven on the simulator 20:25**, head 97f3523ca, not merged): the keyboard no longer
  covers the gauge search under Notifications or the line being typed in a spot's notes; a left-edge swipe now closes
  About, Help, Layers, the rule-book viewer and the photo viewer (About and Layers did not before); the number pads on
  the alert fields get a Done key. Not proven: a real thumb, the Save row on a spot (still under the keyboard until you
  drag down), one tap acting while the keyboard is up, the alert box on the Conditions card. Shots: before/after pair
  sent; 18 in `handoff/PLAN/ios/shots/t723/` on the branch.
- `ios/audit-fixes` (head e7b3df8a8, not merged; 5 of 8 **proven on the simulator 20:40**): the "↗" is a plain arrow, not a
  blue emoji tile; the location prompt says what location is for; "Open Settings" shows under both location-refused
  lines and opens Settings; Directions on a spot opens Maps, not Safari; "What's new" lists no Android-only notes.
  Not provable on the simulator, unit-tested only: a link the phone cannot open now says so; a spot saved from an
  approximate location says so; a staged update no longer applies on a brief "inactive" (no update was applied in the
  run, so nothing to compare). Shots sent (arrow, Open Settings).
- `ios/t719-tapup` (**proven on the simulator 20:50**, head d6afcf124, not merged): the early tap cue on iOS, through
  the same event Android uses. The cue now lights 8 to 121 ms after the finger lifts (before: 371 to 495 ms) on all 18
  taps, and the page shows at 374 to 390 ms (before 401 to 553). A double tap still zooms and opens nothing. The
  no-stall bench still passes. Only the iOS half of the MapLibre patch changed; the Android half is byte-identical.
  Not proven: a real finger (drift), pinch or long press starting on a water, VoiceOver.
- `android/t732-backup` (**proven on the Android emulator, 19:10 and again 21:15 with photos**, head 56b6ea539, app CI
  green, not merged): today a backup of the app carries nothing at all ("Size quota exceeded", 516 MB of app data against
  Android's 25 MB limit). With the branch: the backup carries the spots, the settings and smaller copies of the photos
  (227 KB with 3 photos; 20.4 MB with 29); after a wipe and restore the spots and photos are back and the app downloads
  the map data again by itself. The Spots tab says what the backup holds. A phone-to-phone transfer carries the photos at
  full size. Not shown on the device: photos left out when they do not fit (all 29 fit; unit-tested only).
- The iOS look at today's five shared fixes (**done 21:50**, build 081d93f90, simulator, light and dark; write-up in
  T723): the credits card (T729), search (T730) and Back from a page (T717) pass. The step bar (T731) passes for a
  step within one water; the step into the next water was not reached (Coquitlam has none; needs another water).
  Flow / Level (T728) **passes** on the Vedder (the card opened on Level 1.81 m, switched to Flow 29.7 m³/s, the map
  layer stayed Level). **Found, memory:** measured again on
  the baseline's own simulator model, a water's page takes 279 MB against 213 to 230 MB on the baseline (the map is now
  kept alive under the page), and the peak is 389 MB against 274 MB: with a page up, closing the rule book frees about
  10 MB where it used to free about 50. Not fully like for like (the build carries every change since the baseline).
  Told to the Android chat, which owns T717 and will ticket it. On iPhone, changing tab with the credits card open
  takes two taps (Android: one); taken as an iOS follow-up, not fixed.
- `ios/integration` (head 51349c3bf plus notes, app CI green, not merged; TestFlight build 34 was made from it): the
  four iOS branches (install, keyboard and back, audit fixes, tap cue) merged together. The whole app test suite
  passes (7,707 tests) and the feeds worker's (447). **iPhone simulator on the merged build, functional checks done
  00:50:** offline 3 of 3, satellite, walkthrough 41 of 41, rule book, keyboard 7 of 7, edge-swipe back on About, Help,
  Layers and the rule-book viewer, the audit fixes (arrow, Open Settings, Maps, What's new) pass. Not judged: the
  photo viewer's back swipe (the test misread a viewer that was up), the staged-update check (0 of 3 on the branch
  alone too; no control), first open (slow line that hour). **Found:** the photo viewer's "Photo 1 of 1" sits under
  the status-bar clock (base too; written into T726).
  **Quiet window 02:06 to 02:59, both platforms done (head 639d54d0d; tables in `integration-2026-11-14.md`):**
  Android on the emulator: background install 7 of 7 (asks on mobile data, waits for Wi-Fi, carries on with the app
  killed, map up after relaunch); tap to page: cue 29 to 100 ms, page 339 to 360 ms (before: 22 to 112 and 352 to
  402); no long stalls (worst 140 ms); spots 10 pass, 2 fail on a test-bed fault (a screenshot path the driver
  refuses), so the Android keyboard edit of a spot is unverified. iPhone simulator, timed: the tap cue lights 7 to
  90 ms after the lift on every tap (was 371 to 495), the page shows at 372 to 389 ms, a double tap zooms and opens
  nothing, no long stalls (worst 214 ms at cold start). The 202 / 223 ms Android cue seen earlier was machine load
  (48 / 63 ms quiet). **Verdict: nothing that passed on a branch alone fails on the merged branch because of the app,
  and Android's behaviour did not change.** Not proven: iPhone first open on a good line, the Android spot edit, the
  photo viewer's back swipe, the staged-update check, and everything that needs a real phone.
- `ios/t739-push` (T739, head 1734a8429, app CI green, nothing deployed or merged): push wake-ups for iPhone and
  Android (owner: "okay i ticked push notifications for app yes build for iphone." and "for android too."), with a
  new-data wake-up (owner: "i guess if new data is there we could send a notification that the user needs to download
  it."); no time-sensitive notices ("dont need this"). **Written and unit-tested:** the worker stores a phone's push
  address, sends a silent wake after a feed run that changed and on a new release, and says on its status page when
  the keys are missing; the iPhone registers while alerts are on and withdraws when they are turned off; Android is
  switched off until the Firebase file exists (the Android build config is byte-identical before and after).
  **Proven on the simulator about 22:55:** registering and withdrawing. **Cannot be proven on the simulator:** that a silent
  push runs the app's wake task. Settled from the library code and the logs (file and line in the ticket): the
  simulator hands a pushed file to the app as a "background fetch", a different call from the one a real phone gets
  from Apple, and our task only answers the real one. Nothing of ours was wrong. So the first real check is on a
  phone: one push, then the app's log says "woken by our server". That needs the Apple push key, the worker on
  staging and a TestFlight build from this branch: each one a step for the owner's go. The existing "new data ready"
  notice keeps its old words (Android unchanged); the push path has its own sentence. Head 650665c61, app CI green.
  **01:10: the owner made the push key, stored the three secrets on the staging worker himself, and said yes**
  ("yes i do i ran command") to the worker with the push code on staging and a TestFlight build 35 from this
  branch. **Build 35 uploaded 01:24** (0.7.10 build 35 = build 34's app plus the push code; symbols in
  `data/sourcemaps/0.7.10-b35-650665c/ios/`). The worker deploy (branch `ios/staging-worker` = this branch plus the
  crash-limit commit, 467 worker tests pass) waits for the Android chat's site deploy (it re-runs every feed job);
  a staging release tonight would put the old worker back, then it is deployed again. The push worker commits stay
  off the shared branch tonight (production worker and privacy page first).
  **Worker deployed to staging 02:05** (branch `ios/staging-worker` cc43d998c = this branch's head ca91889bb plus the
  crash-limit commit; version 2733f1d2; the three feed jobs answered 200; /status: all feeds ok, push: iOS keys
  complete, 0 phones registered, Android lacks its Firebase key). The phone now registers once per app start (fix
  ca91889bb, not in build 35). Next: the owner installs build 35, turns on "New regulations data", one test push.
  Needs from the owner later: an Apple push key and three `wrangler secret put` commands (in the ticket), and for
  Android a Firebase project with its `google-services.json`.
- `perf/t741` (T741, head f1938dd60, not merged): the standing pass to make the app faster and lighter on both
  platforms (owner: "can you make sure to keep working on optimizations? make app faster and lighter for both ios and
  android"). Survey done (ranked table in the ticket). **Landed on the branch:** only the 8 type faces the app draws
  are shipped, not 41 (the built iPhone app went from 65.7 to 61.8 MB, measured; the same 3.9 MB on Android by the
  export); the file check written in JS is 16 to 20 % faster (measured in node as a stand-in, not on a phone); the
  background checks for alerts, for new data and for a new app version ask "changed?" first (an unchanged answer is
  0 bytes; it was 24 KB and 6 KB each run). **Simulator, functional (01:40):** the branch passes first open, search,
  edge-back and tap to page, and every face is drawn as before (by eye against the baseline shots). **Timed on a quiet simulator (03:00 to 04:39), base against
  branch:** cold start the same (launch to first drawn frame 1,086 against 1,074 ms, spread 115 ms), stalls the same
  (worst 225 / 240 ms, limit 300), memory the same within noise (240 / 241 MB on a water's page). So the branch is
  lighter (3.9 MB) and uses less background data, and is neither faster nor slower. **Tried and not kept:** "load code
  on first use" (24 ms gain inside a 115 ms spread, and a 153 KB bigger bundle); a smaller database cache (no
  measurable memory saving, so closing database connections is not worth a change either). **Learned:** about 440 to
  490 ms of the 1.07 s cold start is opening the release and decoding the map colours; that is the next thing to
  look at for speed. **Looked at and left:** 35 KB of
  developer-only code comes in through Expo's own modules; nothing else in the bundle is dead. **Proposals:** photo
  thumbnails for the spots list (after T732), closing five kinds of database connection with their screen, Android's
  code shrinker (T749). The 44 s check after a first download on iPhone is already gone on `ios/t722-install`.
- `ios/t746-apply-splash` (T746, head 9975a07fe, from `ios/t722-install`, app CI green, not merged): while new data
  is applied the app shows the boot screen's fish and one line of text over everything, takes no taps, and a failed
  apply now says so with an OK button (before, a failure was reported but shown nowhere, and one kind hung the
  apply). Owner, relayed by the Android chat: "also when changes apply to app we want to make it so that it has a
  loading icon with text like splash so it doesnt looking like it is frozen. (applying changes as in new data regs)".
  **Measured, rough (machine under load):** web 164 and 346 ms from apply to swap; iPhone simulator 702 ms (map left
  still) and 880 ms (back from the background); cold start with new data waiting about 1.3 s. The screen shows only
  when the wait passes 300 ms, so a quick apply does not flash. **Not measured:** Android, and a release whose map
  tiles changed. **Found:** on the way back from the background the app stalls about 550 ms just after the swap.
  Wording options sent to the owner with pictures (question 20).

- `ios/t738-bar` (T738, head 0be685b0c, from `ios/t722-install`, app CI green, no device run yet): the first-install
  bar now counts the four big files from the first moment and after a relaunch. Cause: the bar counted only the small
  data files (about 44 MB) while the big files were already downloading; the same on both platforms. Unit tests pin
  the shown line (a relaunch with 104 MB held reads "104.0 of 482.0 MB downloaded", was "2.0 of 482.0").
- `perf/t741`, cold start (head 571edb7b7, app CI green, **measured 06:45 on a quiet iPhone simulator: launch to the
  first drawn frame 786 ms against 1,030 ms before, 6 launches each, the two ranges do not overlap (733 to 858 against
  997 to 1,084); no long stalls, worst 233 ms; kept**; Android not measured yet): at launch the map colours
  are decoded in 40 ms slices instead of 8. Found from the app's own log: of about 1.07 s from launch to the first
  frame, about 440 to 500 ms was that decode, of which only about 190 ms is work; the rest was waiting between about
  25 slices (each pause costs about 12 ms on a phone). The estimate was 200 to 250 ms; the measure gave 244 ms. Other places with the same cost are listed in T741 as proposals (install and update
  about 1 s, the in-session apply 130 to 220 ms).
- `perf/r8` (T749, head 9fe5e2d6c, **nothing built yet**): Android's code shrinker behind a switch that is off by
  default (switch off = today's build, pinned by a test; refused in a production build). The old crash's cause is
  found (the shrinker removed a constructor Android's background-work database is created through) and covered by
  a keep rule, with six more, each with its reason. Next: two test APKs and six emulator checks.
- `ios/next` (head d658f88ac, **what TestFlight build 36 will be made from**; notes in `next-2026-11-15.md` on the
  branch): `ios/integration` + the shared branch + T746 + T738 + the crash-report app half + T739 push + `perf/t741`.
  Six merges, every conflict resolved by keeping both sides. App CI green on the first five merges (run 43711925555);
  the run on the full head was in progress at 05:25. Simulator checks on the combined build: not run yet.

**What failed or was found**
- **iOS background download refused on the simulator, then fixed:** the system service rejected every task ("does not
  have a bundle ID") in two runs. Cause: our simulator build skipped Xcode's signing step, which is what gives the app
  its identity there. The build script now signs "to run locally" (no account, no team); the third run passed.
- **First-open progress most likely does not move on iPhone today (0.7.9/0.7.10):** about 5,000 number-conversion
  errors from the progress callback during the download. Fixed on `ios/t722-install` (zero such lines in its run).
- **Crash reports from the iPhone build were refused by the staging worker's own daily limit; the report itself is
  fine** (cause found 00:20, branch `ios/crash-send`, code and tests only). 24 of 25 sends in today's iOS runs were
  answered 429 "too many reports today": the worker takes 40 crash reports per address per UTC day, the test beds on
  this Mac share one address, and the UTC day turns at 17:00 here (the one send at 17:01 was taken), so the 23:30
  failure was not a new day; my earlier line that the limit was unlikely was wrong. A phone on the same home line
  shares that address, so the owner's phone can be refused the same way on a heavy test day. On the branch: the app's
  log names the worker's answer and a refused report is reported, not dropped (441ec090e, 2daa9175d); the worker, on
  staging only, counts test-bed crashes against their own limit of 200 a day so they cannot use up the 40 real phones
  get. **The worker change alters the limit policy: owner's OK needed before any deploy** (question 19). Not proven
  on a device.
- **No "At my location" row in iOS search** in the baseline run; cause not settled (app or test bed).
- **The memory-warning trim could not be tested:** the headless simulator has no way to send the warning.
- **First open took 397 s** (0.5.0: 168 s). The download is 479 MB and ran at 1.7 MB/s on the line today; 44 s more was
  checking the files after the last byte; about 70 s was the test bed itself.
- **Mine, fixed:** pipeline CI went red twice on slice 8 commits (a lane tested only its own files). Both fixed at the
  cause without loosening a guard; fast-fail now runs the repo-wide scans. I also cut the ticket ledger short once for
  about a minute and restored it; nothing was lost.
- **On today's iPhone build (0.7.10) every app launch restarts the big first downloads from zero** (seen by the tap
  lane on a fresh simulator: its test's cold starts kept killing the download). The install branch fixes this (the
  download carries on across a relaunch).
- **The simulator was the bottleneck all day:** five lanes behind one lock, waits of up to three hours.

**First TestFlight run (owner's go-ahead 21:30, "you can do first ios release test for me")**
- Tried 21:32 from f51870652 (last green app CI, same app code as Play build 32), staging data, 0.7.10 as build 33.
  Stopped after 1 minute in signing; nothing was uploaded. Cause: the team has no registered iPhone, and Xcode's
  automatic signing needs one to archive. Waits for the owner to register his iPhone; then one re-run.
- Second try 22:07, after the owner registered his iPhone: signing and the archive passed (2 minutes), the export for
  the store failed; nothing was uploaded. Apple: "You haven't been given access to cloud-managed distribution
  certificates". Cause: the API key was made with the App Manager role, as our own steps said; signing for the store
  needs an Admin key (Apple offers no switch for it on a key). The steps now say Admin. Waits for the owner to make a
  new key with the Admin role; then a third run.
- **Third try 22:17: uploaded.** With the owner's new Admin key the archive, the export and the upload passed in 296 s:
  0.7.10 build 33 (commit f51870652, staging data) is in App Store Connect; TestFlight shows it after Apple's
  processing. `~/.canifishthis/testflight-last-build` reads 33; the debug symbols are in
  `data/sourcemaps/0.7.10-b33-f518706/ios/`. Not yet seen: the build on the owner's phone. Later TestFlight uploads:
  ask the owner first (his go-ahead was for the first test).
- **Build 34, 23:30:** the owner asked why the iPhone build does not download in the background (build 33 is the shared
  branch's code); offered a build from `ios/integration` (51349c3bf: the four iOS fix branches merged, app CI green,
  simulator: walkthrough 41 of 41, rule book, keyboard, offline, satellite pass); owner: "yes". **Uploaded 23:39**
  (0.7.10 build 34, staging data, 363 s; a first try failed in setup when the engine download dropped on the bad
  line). Symbols in `data/sourcemaps/0.7.10-b34-51349c3/ios/`. Not yet seen on the phone.
- The Apple account side is otherwise complete (individual account, Team ID, App ID with Associated Domains, app
  record, registered iPhone).

**Not started**
- T721 (edge-swipe back grid), T726 (screens on four iPhone sizes, light and dark, large text, VoiceOver), the store
  screenshots and video, the remaining test-bed ports (saved spots, photos, feeds, offline update), the simulator run of
  a shared link. IOS-11 (TestFlight) and T602 were not to be started.

**Questions for you (default first; all are also on the Owner To-Do page)**
1. Apple account: ANSWERED, individual ("it is a individual evelper account"); the App Store will show your legal name
   as the seller. The Team ID appears only once Apple approves the enrolment; then save it, create the app record, make
   the API key.
2. Oldest iPhone: keep iOS 16.4 and iPhone only / go lower / add iPad.
3. App Store privacy: the report email "linked to the user": yes / no.
4. Category: Navigation then Sports (my pick) / other.
5. Site build with no Team ID: skip Apple's link file with a warning / always fail.
6. Backup (T732): ANSWERED in part ("Only need to backup photos and spots the rest of the data isn't important"): then
   "Settings could also be saved": the backup carries spots, settings and photos, with smaller photo copies in the cloud
   backup. Still open: back up only
   with a screen lock: no / yes; copy size 1600 px quality 70 / other.
7. iOS background download (proven on the simulator): merge after the Android benches pass on the branch, phone checks
   follow with TestFlight / wait for the phone checks first. Not merged tonight either way (production move).
8. Early tap cue on iOS (branch `ios/t719-tapup`, proven on the simulator: cue 8 to 121 ms, was 371 to 495): merge with
   the other iOS branches after the Android benches / leave.
9. About page gains "not affiliated with the government" (Apple asks): About only / also show the first-run notice again.
10. Play's form says approximate location, the App Store answers say precise: change Play to match? (no default)
11. T686: which data savings become tickets: (a) three lossless, about 1.94 MB / others in the ticket.
12. Slice 8: the small test release has no map tiles, and the export takes its checklist as a named input: keep / other.
13. Clean-up: five calls in `CLEANUP-CANDIDATES.md` (defaults there).
17. The first-install bar reads low on both platforms (38.8 MB shown with 116 MB on disk): the four big files download
    at once and the bar counts one at a time. Fix it as a small ticket (T738) / leave it.
15. A spot's Save row stays under the keyboard while typing: leave it (drag down or Done first) / lift the row above
    the keyboard.
16. The new left-edge back strip on full-screen pages is 16 pt wide and takes taps in that gutter: keep / narrow to 8 pt.
14. ANSWERED 18:15 ("Maybe we can run 2 at same time might have enough resources") and later, relayed by the Android
    chat ("i just meant use as much of the resources as you can to speed stuff up"): three heavy lanes exist; each job
    still waits for free memory.
18. Push (T739): ANSWERED, both platforms, no time-sensitive notices, new-data wake-up added. Open: "for android too"
    is read as your exception to "self-host everything" for push delivery only (Google's Firebase Cloud Messaging):
    right / not right.

19. ANSWERED 2026-11-15 01:05 ("yes to this"): on staging, test-bed crashes get their own daily limit (200 per
    address). Worker commit 441ec090e; landing and the staging deploy are being timed with the Android chat.

20. ANSWERED 02:09: the line on the "applying new data" screen (T746) becomes fish puns ("can we do more fish puns
    here that are directed at this ?"); a small rotating set is built on the branch, candidates sent; the owner
    strikes or adds. The failure message stays plain unless he says otherwise.
21. ANSWERED 02:09 ("yes you can upload"): TestFlight build 36 in the morning with everything that passed overnight.

Owner, 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is rock
solid too?" This **reopens iOS**, which the owner benched on 2026-11-06 ("no iOS device work until DFO extents labelled +
Play closed testing started; then paid TestFlight"). Read that as: simulator and code work may start now, in its own chat,
while Android closed testing proceeds. Whether the Apple Developer account is bought, and when, is the owner's to confirm
(decision 1 below). Nothing in this page is approved work: the ticket stubs are **proposals for the owner to accept**
and are not in the tickets ledger.

**First message to paste into the new chat:** "read handoff/PLAN/ios/START-HERE-ios.md and work the Unattended day section; I am away, batch questions for when I am back"

Read first: `AGENTS.md`, `CLAUDE.md`, `app/MAP.md`, then this page. History: tickets T88, T217, T250 (iOS walk), T313,
T336, T362, T363, T364, T365, T420 ("iOS half"), T244. The newest restart page is `handoff/PLAN/START-HERE.md`.

## Unattended day (owner away 2026-11-14): follow this without stopping

Owner, verbatim: "Portrait only. We want to do as much work as possible without the Apple account in the next agent also
anything else the agent could work on at same time? Need enough tasks for the agent for the day while I am gone."
The IOS-1..10 stubs below are **owner-approved to start** as of this message ("as much work as possible"); open each as a
real ticket in the ledger (next free number) when you start it. IOS-11 is not approved.

- **Goal:** everything that needs no Apple account, IOS-1 then IOS-2 ... IOS-10, simulator only. **IOS-11 (TestFlight, real
  iPhone) is NOT started.**
- **Open decisions do not block.** Use the default, mark it a proposal in the ticket, queue the question for the owner's
  return (batched, multiple choice, with pictures sent by SendUserFile). Defaults:
  minimum iOS = leave what the current Expo/RN setup gives and record the number; iPhone only (no iPad layout work);
  tap-up patch = measure first (IOS-2), write up the numbers, build an iOS patch only if the measured delay matches
  Android's and the patch is lean, as a proposal behind the same event name; Apple-account questions wait. Rotation and name
  are answered (above).
- **Parallel lanes** (disjoint files; at most 2 heavy jobs via `scripts/heavy.sh`, the second on `HEAVY_LOCK=~/.claude/heavy-job-2.lock`):
  1. iOS lane(s) in `app/` and the iOS test bed (this document).
  2. **Pipeline/tests lane** works `handoff/PLAN/PARKED-TICKETS.md` in this order: T667 4c, T667 7, T667 6,
     T686 survey + prototypes + ranking (**stop before the owner's pick**), then T667 slice 8. **T602 is NOT started**
     (needs a full build and the owner).
  3. **Docs lane when idle:** iOS store pack drafts (IOS-10): review notes, age-rating answers, App Privacy answers checked
     against `handoff/PLAN/play/data-safety.md`, iPhone 6.9" / 6.5" screenshots from the simulator.
  4. **Clean-up hunt (Sonnet, lowest priority, runs when a slot is free).** Owner 2026-11-14, verbatim: "can we send another
     thing to that agent to look for things that could be cleaned up too". Survey the repo for what can go or be tidied: dead
     code and unused exports, unused dependencies, files nothing reads, duplicate helpers, stale `handoff/PLAN` and
     NEXT-SESSION pages that newer ones replace, tickets marked open that are done, old generated outputs and scratch dirs
     inside the repo, old releases beyond live + previous, leftover experiment flags (e.g. `EXPO_PUBLIC_TAP_AT_LIFT`),
     TODO/FIXME comments that are done, tests that pin nothing, scripts no longer called, large files tracked in git that
     should not be. Write findings to `handoff/PLAN/CLEANUP-CANDIDATES.md` as a table: what, where, evidence it is unused
     (who was checked for readers; `graphify query` first), size or lines saved, risk (safe / needs a look / owner's call).
     Do the **safe** ones straight away (owner rule 2026-10-24: git is the safety net, quick importance check, no approval
     list): one small commit per group, `git commit -- <paths>`, tests for what changed plus `pnpm check:fast` or the
     pipeline fast-fail as fits, no behaviour change, app CI and fast-fail green. **Do NOT touch:** `marks.jsonl`,
     `data/generated/review/dfo-suggestions.json`, `data/curated/**` content, the tiles dir, guards and their tests,
     anything the other chat is changing (`app/packages/ui-native` Shell, `app/packages/map`, the test bed), files
     T667/T686/T602 already cover (the pipeline lane's), credentials and anything under `~/.canifishthis`. Never drop, rename
     or filter data content. Never pipe `ls` into `rm`. Anything not clearly safe stays in the table for the owner, batched
     with the end-of-day questions.
- **Each IOS ticket:** open it as a ticket when started; record baseline numbers; fix the parity gaps found; add or port the
  bench so it runs on the simulator; tests only for what changed plus `pnpm check:fast` run unfiltered; app CI green on push.
- **Hard limits:** no Play upload, no TestFlight, no production, no atlas promote or release, no parser or `claude` CLI,
  nothing needing the owner's Apple ID. Android behaviour must not change: run the Android `regs-open` and `tap-stalls`
  benches after any shared-code change. Never loosen a guard or skip CI. **The other chat owns the Android build train and is
  changing the tap/back path** (`packages/ui-native` Shell, `map/`, test bed): `git pull --rebase` often, commit small with
  `git commit -- <paths>`, keep iOS changes in `.native`/iOS-only files where possible.
- **If a lane is blocked, park it with a note and move to the next item; do not wait.**
- **End of day:** a plain summary at the TOP of this document (what was done, numbers, what failed, batched owner
  questions with pictures), `handoff/PLAN/OWNER-TODO.md` and the Owner To-Do page rows updated.

## Where things stand (checked 2026-11-14; "unknown" means not confirmed)

1. **It builds and runs on the simulator.** `deploy/staging/build-ios-sim.sh [--testbed|--probes]` builds committed HEAD in
   a temp worktree (pnpm offline, `expo prebuild -p ios`, pods, `xcodebuild` Release, iphonesimulator, no signing, staging
   data) into `app/dist/ios-sim/`. Last verified build: 2026-11-06, HEAD b99268518, SDK 58 / RN 0.88 (T420 "iOS half").
   The app is now 0.7.9 on Android (versionCode 31); **no iOS run has happened since 0.7.x began** (the last
   run-ios.mjs run found: 2026-11-06; `deploy/staging/build-ios-sim.sh` last touched 2026-11-10). Expect drift: unknown.
2. **Config.** `app/apps/mobile/app.json` `ios`: bundle id `ca.canifishthis.app`, `supportsTablet: false` (iPhone only),
   `buildNumber` "31", `usesNonExemptEncryption: false`, a full `privacyManifests` block (required-reason APIs + collected
   data, matches `handoff/PLAN/app-integration/store/privacy-labels-draft.md`). Plugins: expo-image-picker (photos +
   camera strings), expo-location (when-in-use only), expo-notifications, expo-background-task (declares `fetch` +
   `processing`), the MapLibre RN plugin. Minimum iOS version: **unknown** (not set in app.json; Expo SDK 58's default
   applies; read `app/apps/mobile/ios/Podfile.properties.json` / the generated Podfile after a prebuild).
   `ios/` is generated by `expo prebuild` and git-ignored (`app/apps/mobile/.gitignore`): never edit it.
3. **Signing / TestFlight: scripted, never run.** `deploy/staging/build-ios-testflight.sh [--production] [--no-upload |
   --unsigned] [--dry-run]` (T88). `--unsigned` device archive compiled and linked in 80 s on 2026-11-05. The signed run
   needs `~/.canifishthis/ios-team-id` and `~/.canifishthis/asc-api-key.json` (+ the .p8): **neither exists** (only the
   Play files are in `~/.canifishthis/`). The App Store Connect app record (bundle id, SKU, "English (Canada)") is not
   made. `eas.json` has `ios` only on the `development` profile and is not the release path; the scripts are.
4. **Simulator evidence exists.** Run `ios-t313g` (0.5.0, iPhone 17, iOS 26.5): 11 of 11 scenarios passed (first open
   168 s, map, tap-water, offline, satellite, location, walkthrough 25 of 25, PDF in PDFKit, crash record, memory, perf).
   Numbers in `handoff/PLAN/app-integration/tickets/T313-ios-simulator-basic.md` and `T363-ios-memory.md`
   (footprint 147 to 290 MB; a memory warning freed nothing then; `memoryTrim.native.ts` now has an iOS `memoryWarning`
   listener, so that line is out of date: re-measure).
5. **Never on a real iPhone.** T364 (real-iPhone run) was merged into T88. Real GPU memory, the left-edge swipe back
   by thumb, location prompt, cellular download and the gauge pill padding are all unverified.
6. **Tooling on this Mac (2026-11-14):** Xcode 27.2, simulators iOS 26.5 (iPhone 17 / 17 Pro / 17 Pro Max / 17e / Air,
   iPad Pro 13 M5), CocoaPods installed. A Xcode first-launch had to be run by the owner once (T313); `lib/xcrun-shim.sh`
   is the insurance.

### What is Android-only or different on iOS (each with evidence)

| Piece | Android | iOS today |
|---|---|---|
| MapLibre binding patch `app/patches/@maplibre__maplibre-react-native@11.5.0.patch` (one patch file, both halves) | Kotlin: tile event, `setFeatureStates`, stretch images, T420 lean changes | Obj-C half (T217): `MLRNMapView.m`, `MLRNVectorSource.m`, `MLRNPmTilesIds.{h,m}`, `MLRNVectorSourceModule.mm`, `MLRNImageLoader.m`. T420 dedupe/repeat-tile LRU is in both; iOS compiled and ran 2026-11-06 (0 "not held"). iOS frame times never measured (`app/deps.md` says re-run emulator AND simulator pans on any binding bump) |
| T716 `onTapUp` (a tap said at the finger's lift, before MapLibre's 300 ms double-tap wait) | `MLRNMapView.tuneTap`, Kotlin | **iOS emits nothing** (`app/deps.md`); `app/packages/map/src/tapLift.ts` + `Map.native.tsx` `onTapUp` just never fire, so the cue arrives with the click. Whether iOS also waits ~300 ms for a click is **unknown, never measured** |
| Back | system back button / predictive back | left-edge swipe: `app/packages/ui-native/src/EdgeBack.tsx`, `@app/ui` `useEdgeBack`, `useShellNav.ts`. Simulator verdict unsettled: fast mid-height drag stays, slower higher one goes back (`edge-back*.yaml`); a thumb is the judge |
| Notifications | channels (`notify.native.ts` creates three); background task checks the alert feed | `ensureChannels` returns early on iOS (fine); `raise` uses `trigger: null`. iOS background tasks are opportunistic (expo-background-task): **delivery timing unknown, never run**; permission prompt text seen on simulator only |
| Release download (~480 MB) | background download | foreground URLSession (b8e33ca80); a background session was lost on kill. 168 s on simulator at 0.5.0. Cellular + app-switch mid-download: unverified |
| Crash records | ApplicationExitInfo | own signal/NSException handler (`modules/exit-info/ios/ExitInfoModule.swift`, T362); jetsam and watchdog kills not caught (MetricKit is the follow-up) |
| Share | `{message: url}` | `{url}` (`share.native.ts`) |
| Shared links | Android App Links shipped (T625: `assetlinks.json`, `app.json` intent filters, `appLink.native.ts`) | **none**: no `associatedDomains`, no `apple-app-site-association` served. `appLink.native.ts` is platform-free (`Linking`), so only the entitlement + site file are missing. T625 item 5 says "out of scope while iOS is benched" |
| App update notice | Play in-app update (T390) | `appUpdateChannel({os})`: iOS has no channel ("iOS before T88" in `notify.native.ts`); `EXPO_PUBLIC_APP_STORE_URL` is the hook. Unknown whether anything shows |
| PDF viewer | react-native-pdf (patched, `app/patches/react-native-pdf@7.0.5.patch`) | PDFKit (T354), opens on the marked page; ~50 MB while open, no leak |
| Permissions | location, photos, camera, notifications | strings in app.json plugins; Always-location, motion, microphone removed (T313) |
| Safe areas | `SafeAreaView` | `WindowInset.native.tsx` has an iOS branch (`IosInsets`); menu pages once sat under the status bar (fixed 404a65da0). Dynamic Island / home indicator on 17-series: seen only on iPhone 17 simulator |
| Gauge pill | stretch + content box | one stretch box per axis: text sits a few px further in; owner never saw it on a phone |
| Fonts, sprites, data | self-hosted | same JS; nothing iOS-specific found. Not separately verified |
| Rotation | unknown | unknown (app.json orientation not checked) |
| iPad | n/a | `supportsTablet: false`; App Review may still run it compatibility-mode; no iPad sim run yet (T365 item 6) |

`Platform.OS` branches in shared code (all on the platform allow-list, `app/tools/check-platform.mjs`): `appUpdater.native.ts`,
`notify.native.ts` (3), `share.native.ts`, `memoryTrim.native.ts`, `WindowInset.native.tsx`, `openBook.ts`,
`useShellNav.ts` (edge back), `emulator.native.ts` (Android only), `platformLine.native.ts`; the rest are web-vs-native.

### Test bed on iOS
`app/tools/testbed/session-ios.sh` (boots a simulator, installs, runs one capped command, shuts down only what it booted)
and `run-ios.mjs` with scenarios: `first-open map tap-water offline satellite location walkthrough pdf crash perf memory`.
It drives Maestro flows through `lib/device-ios.mjs` (simctl) and `lib/maestro.mjs` (a sandbox profile refuses
localhost:5555-5585 so Maestro does not wait forever). Offline = the app's own "Simulate offline" (staging builds only).
**Missing on iOS:** the benches added since 11-05: `regs-open`, `tap-stalls`, `saved-spots`, `gestures`/`glide`, `feel`,
`pane`, `regs-switch`, `route-scroll`, `photo-zoom`, `report`, `big-update`, `bg-install`, `feeds`, `locate` (all
`app/tools/testbed/scenarios/*.mjs`, Android `run.mjs` only). Maestro flows like `spot-add-photo.yaml` are shared and
may already work. Whether the scenario code is portable: unknown; the `pidOf`, `adb`, `gfxinfo` helpers are Android.
Store video (`app/tools/store-video`, T709/T715): Android only; T709 step 2 plans the iOS run at 886 x 1920 and
1200 x 1600 "when iOS opens".

### Store side
- Shared with Play: listing text (`handoff/PLAN/play/store-description.md`, "same text for the App Store"; `listing.md`),
  icon (`icon-1024.png`, `app/tools/build-icons.mjs`), support URL https://canifishthis.ca/about, privacy page.
- Exists: `store/privacy-labels-draft.md` (Apple section; the Device ID row is a proposal), `handoff/PLAN/play/data-safety.md`
  (Play), the `ios.privacyManifests` block, T365 compliance pass (met / missing table), `app/tools/store-shots.json` has
  `iphone67` / `iphone55` raw sizes defined (`store-shots.mjs` `RAW_PRE`); whether it has ever produced iPhone shots: unknown.
- Missing: App Store Connect record and App Privacy answers entered; screenshots at 6.9" and 6.5" actually captured
  (the tool needs an iOS capture source); age rating, category; review notes (no login, data source, "Simulate offline"
  if a staging build goes to review); the name decision ("Can I Fish?" home screen vs "Can I Fish This?" prompts, T371);
  the review build must read PRODUCTION data (needs T86 production hosting live); iPad stance; the "background modes"
  answer (downloads the next regulations release).

## Rules that carry over (all in `AGENTS.md`; these bite)
- **No parser runs, ever** (A1): `run_parse.sh`, `dispatch`, anything spawning `claude -p`. Hand the command over.
- Branch `redesign/stream-sections`; merge only into it, named explicitly; `main` is untouched. Commit only your own
  paths: `git commit -- <paths>` (others share the checkout). Never commit `data/generated/review/pfma_basin_areas.json`,
  `write-through.jsonl`, or store images. `data/generated/review/dfo-suggestions.json` is uncommitted and the owner's.
- Prefix commands with `rtk`; `graphify query` before grepping. Verify gating test runs unfiltered (rtk once false-passed).
- Heavy jobs (iOS builds, simulator runs, pods) go through `scripts/heavy.sh <label> <min free %> -- <cmd>`, one per
  lane, two lanes max. **Kill only your own PIDs and ports; never `pkill`/`killall`** (one took down the owner's tile
  server). `session-ios.sh` already shuts down only what it booted.
- Self-host everything: fonts, sprites, data only from our servers (Esri imagery and Open-Meteo are the only exceptions).
- No silent fallbacks: a failure that is shown is also reported (`reportFailure`); if iOS lacks a feature, it says so.
- One load path per screen: same output whatever the way in. No TS cascade logic: Python resolves, app reads.
- Owner decisions: ask by multiple-choice dialog; record the answer in the ticket + `handoff/PLAN/OWNER-TODO.md` + the
  Owner To-Do page (https://claude.ai/artifact/Lh2ougAwfxx1wNmCV18m8f, collection `todos`) the same turn. Search the
  transcript before re-asking.
- Subagents get an explicit Opus or Sonnet model (Fable is for the main chat). Test only what changed (Tier 0 file,
  Tier 1 `scripts/test-smoke.sh` per commit, full suite once). Send images, not paths, to the owner.
- Anything the owner must see goes to staging; the owner is the only user; reproduce device bugs in the simulator.
- Binding patch changes: re-run emulator AND simulator pans (`app/deps.md`). Lean T420 patch stays; no iOS-only fork.
- Tickets are the sole source of work; new numbers come from the README ledger (next free number: check the README, it
  is later than T716). The stubs below use IOS-n labels until the owner accepts them.

## Work list (IOS-1..10 approved to start 2026-11-14, see Unattended day; IOS-11 still a proposal)

Order matters: baseline first, then parity gaps by severity, then benches, then TestFlight.

**IOS-1. Build and run on the simulator; record the baseline.** Goal: know today's truth on 0.7.9. Do: `heavy.sh` ->
`deploy/staging/build-ios-sim.sh --testbed`, then `session-ios.sh --fresh --max-min 40 -- node app/tools/testbed/run-ios.mjs all`;
fix test-bed drift only (not app behaviour yet). Acceptance: report with the 11 existing scenarios, build time, first-open
time, memory table, shots; every failure listed with cause (app vs test bed). Size: M (one heavy run + drift fixes).
  Sub-steps: (a) `rtk git pull`, check `heavy.sh --status`; (b) build with `--testbed`, then again with `--probes` for perf/memory;
  (c) run all 11 scenarios, triage each failure app vs test bed; (d) fix bed drift (hard-coded tab/back coordinates in
  `run-ios.mjs`, Maestro ids); (e) record minimum iOS from the generated Podfile; (f) shots into `handoff/PLAN/ios/shots/`;
  (g) list every Android scenario with no iOS twin (feeds IOS-3..6). Mirrors: Android `run.mjs all`.

**IOS-2. Tap open speed and cue on iOS (T716 parity).** Goal: a tapped water shows something within ~100 ms of the tap
and opens in the Android time class. Do: add `regs-open` to `run-ios.mjs` (press / cue / shown from the `[open]` lines);
measure whether iOS waits for a double-tap before the click; then decide: accept (if click is prompt), or an iOS
`onTapUp` in the patch (decision 4). Acceptance: cold + 2 warm taps on small/mid/lake waters, numbers beside Android's,
double tap still zooms and does not open twice. Size: M (L if a patch).
  Sub-steps: port `scenarios/regs-open.mjs` (press/cue/shown) using the `[open]` lines; measure click delay with and
  without a double-tap recogniser; cold + 2 warm taps on small/unnamed/mid/Fraser/lake waters; double-tap-zoom check;
  write numbers beside Android's (T716); lean iOS `onTapUp` only per the default. Mirrors: Android `regs-open`.

**IOS-3. No-stall bench on iOS (`tap-stalls`).** Goal: no JS stall of 300 ms or more at cold start, Salmon colouring,
Run timing tab, the bell. Acceptance: bench passes on the simulator with the same threshold. Size: S-M (needs `[stall]`
lines in the `--probes` build).
  Sub-steps: port `scenarios/stalls.mjs`; make `--probes` print `[stall]`/`[work]`; cover cold start, Salmon colouring, Run
  timing tab, the bell; fail at 300 ms; fix any iOS-only stall. Mirrors: `tap-stalls`.

**IOS-4. Back and swipe.** Goal: left-edge swipe back is dependable by thumb and by the test bed; sheets, menu pages,
walkthrough, rule book, salmon/run-timing screens all leave predictably. Acceptance: `back-layers`-style walk on
simulator; real-iPhone verdict (needs owner) recorded; if a fast mid-height drag still stays, fix the gesture or its
hit area. Size: M.
  Sub-steps: port `edge.mjs` and the `edge-back*.yaml` flows; sweep drag height/speed grid, record which go back; walk every
  layer (water page, sheets, menu pages, walkthrough, rule book, salmon/run timing, search) one step per swipe; fix hit
  area/threshold in `EdgeBack.tsx` if the grid fails (iOS-only file); confirm Android untouched. Mirrors: `edge`,
  `back-layers.test.ts`.

**IOS-5. Downloads, offline, background.** Goal: first run survives a kill, app-switch, Wi-Fi to cellular, low disk, and
a big update; background release check and notices behave on iOS. Acceptance: `first-open`, `offline`, `big-update`,
`bg-install` equivalents on iOS; notification permission and one delivered notice shown; the Always/background
declarations explained for review. Size: L (background behaviour needs a real device to be honest).
  Sub-steps: port `install.mjs`, `bg-install.mjs`, `big-update.mjs`, `offline.mjs`, `gauge-offline.mjs`; kill mid-download and
  relaunch; app to background mid-download; low-disk (simctl) message; notification permission prompt flow and one
  raised notice via the feeds test channel; read what expo-background-task allows on iOS and write it down; background-
  mode declarations explained for review. Mirrors: same-named Android scenarios.

**IOS-6. Map, salmon and run timing, search, saved spots, photos.** Goal: the screens added since 11-05 work on iOS.
Do: port `saved-spots`, `photo-zoom`, `route-scroll`, `pane` scenarios; shots side by side with Android. Acceptance: each
scenario passes or its failure is ticketed. Size: M-L.
  Sub-steps: port `saved-spots`, `spots`, `photo-zoom`, `route-scroll`, `pane`, `part-steps`, `regs-switch`, `report`, `feeds`,
  `locate`; photos via simctl addmedia; side-by-side shots with Android; salmon + run timing map walks. Mirrors: those
  scenarios; `spot-*.yaml` flows are shared.

**IOS-7. Shared links as universal links.** Goal: a shared `canifishthis.ca/water/...` link opens the app on that water.
Do: `associatedDomains` in app config for both hosts, site build serves `/.well-known/apple-app-site-association` (mirror
the T625 `assetlinks.json` work and its site test), Team ID from the owner. Acceptance: simulator `xcrun simctl openurl`
opens the water; link without the app opens the web. Size: M (final proof needs a signed build).
  Sub-steps: copy the T625 site test for `/.well-known/apple-app-site-association` (Team ID as a placeholder read from config,
  never invented); `associatedDomains` via app config for both hosts; check `appLink.native.ts` and `resolveAppLink` handle
  the iOS launch URL; `xcrun simctl openurl` runs; no-app case = web. Real proof needs a signed build (IOS-11, parked).
  Mirrors: T625 items 2-4.

**IOS-8. Memory and cold start on a real-class device.** Goal: the footprint table re-run on 0.7.9; the memory-warning
trim measured; cold start to map. Acceptance: table in T363 shape; a verdict on the oldest supported iPhone (needs the
minimum-version decision and, ideally, a real older phone). Size: S-M.
  Sub-steps: re-run `memory.mjs` on 0.7.9 and diff with T363; confirm the `memoryWarning` trim (`simctl` memory warning) frees
  what it claims; cold start x3 via `perf`; record minimum-iOS number; no device claims. Mirrors: `memory`, `perf`,
  T360 table.

**IOS-9. Layout: safe areas, Dynamic Island, rotation, dark/light, Dynamic Type, VoiceOver.** Acceptance: shots on iPhone
17 / 17e / Air / 17 Pro Max sims in light and dark; large text; VoiceOver reaches every card and button (T336 class).
Size: M.
  Sub-steps: **lock portrait on iPhone** (`expo.orientation`/`ios` Info.plist; check Android's setting and leave it);
  shots on iPhone 17 / 17e / Air / 17 Pro Max in light and dark; largest Dynamic Type; VoiceOver tree walk like T336
  (every card/button reachable); Dynamic Island / home-indicator insets; port `a11y-walk.mjs`. Mirrors: `check-colours`,
  `regs-text-scale.test.ts`, `a11y-walk.mjs`.

**IOS-10. Store pack for iOS.** Goal: everything for the App Store listing except what needs the account. Do: iPhone
6.9"/6.5" screenshots from `store-shots`, the iOS store video (T709 step 2), App Privacy answers drafted from
`privacy-labels-draft.md`, review notes, age-rating answers, name decision applied. Acceptance: a folder + a checklist the
owner pastes from. Size: M.
  Sub-steps: 6.9"/6.5" shots from the simulator through `store-shots.mjs` (iphone67 / iphone55); iOS store video via
  `store-video` (886 x 1920, 1200 x 1600); review notes; age-rating answers; App Privacy answers cross-checked with
  `play/data-safety.md` and the manifest; apply the name "Can I Fish This?" where `app.json` differs. Mirrors: T319, T709,
  T365 missing list.

**IOS-11. TestFlight (needs account; owner runs the signed script).** Goal: build 1 on TestFlight, internal testers, the
real-iPhone run (T364 list: location prompt, download on Wi-Fi and cellular, edge-back by thumb, pill padding,
walkthrough with VoiceOver, crash reports staying on). Acceptance: installed on the owner's iPhone, checks answered in the
Owner To-Do page. Size: S for Claude, owner time for the rest.

## "Rock solid" means (acceptance checklist for iOS parity)

Each line names the Android check to reuse; "new" means no iOS or Android bench exists yet.

| Check | Reuse | Pass |
|---|---|---|
| First run + download of the release | `first-open`, `install`, `bg-install` | map drawn from fresh install on Wi-Fi; kill mid-download then relaunch resumes or restarts cleanly with a shown, reported state |
| Offline | `offline`, `gauge-offline` | regs and map from the installed set, "Check now" says offline, no blank screens |
| Tap open + cue | `regs-open` | cue within ~100 ms, page first frame in Android's class, no double open on double tap |
| Double tap | `gestures`, `glide`, `feel` | zoom, not open; pinch momentum |
| Back / swipe | `edge`, `back-layers.test.ts`, `edge-back*.yaml` | every layer leaves one step at a time; cannot swipe past the root |
| Salmon + Run timing maps | `tap-stalls`, `pane`, `part-steps` | no stall of 300 ms or more; same colours and words as Android |
| Search | `search.yaml`, `search-golden` | same results; "At my location" row (missing on iOS search today: check) |
| Saved spots + photos | `saved-spots`, `spot-add-photo.yaml`, `photo-zoom` | add, rename, notes, photo from library and camera, delete, survives a restart |
| Alerts / notices | `feeds`, `report` | permission prompt once; a notice arrives; tap opens the water (`openRequest`) |
| Share link open | T625 flow (iOS: IOS-7) | link opens the app on that water |
| Satellite, location | `satellite`, `locate` | as `ios-t313g` |
| Rule book PDF | `pdf`, `regs-open` | opens on the marked page in PDFKit, closing returns to the app |
| Dark / light, text size | `check-colours`, `regs-text-scale.test.ts` | contrast and no clipping at the largest text |
| Rotation | new | decide supported or locked; no half-rotated state |
| Memory | `memory`, T360 table | footprint under half the RAM of the oldest supported phone; memory warning frees what the trim says |
| Cold start | `perf`, `near-cold` | map drawn within the same class as 0.5.0's 0.63 s warm / first-open time recorded |
| Crash path | `crash` | a signal crash reports on next start; no report loop |
| Privacy / review | T365 table | every "Met" row still true on the new build |

## Decisions that are the owner's (ask as multiple choice dialogs, batched on return; defaults in the Unattended section)

1. **Apple Developer account and TestFlight.** "Buy the Apple Developer account ($99/year)?" (a) now, so TestFlight
   starts as soon as the Android closed test is running; (b) after Android closed testing has begun (default, per the
   2026-11-06 bench); (c) not yet, simulator work only. Also: individual or organization (T88 asked this; an
   organization needs a D-U-N-S number; the name on the listing differs).
2. **Minimum iOS version.** (a) the Expo SDK 58 default (default); (b) iOS 17; (c) iOS 16 (more old phones, more risk and
   more testing). Say which oldest iPhone you want to support.
3. **iPad.** (a) iPhone only, as now (default; Review may run compatibility mode); (b) iPhone plus iPad layout (new
   design and test work, T365 item 6).
4. **iOS own tap-up patch.** "If iOS also waits for a double tap before opening a water:" (a) measure first, then decide
   (default); (b) always add an iOS `onTapUp` to the binding patch for parity; (c) accept the iOS delay. Note the patch
   is not upstreamed and each addition makes bumping the binding harder.

**ANSWERED 2026-11-14 (owner, "Portrait only."):** rotation = portrait only. Lock it on iPhone (part of IOS-9).
**ANSWERED (name; settled for Play 2026-11-13, same for iOS; T371 closed on that point):** "Can I Fish This?" on both stores.
Apply it to app.json's `expo.name` / prompts / listing in IOS-10 if they still differ.

## What needs the owner physically (kept to the minimum)

- Apple ID with 2FA to enrol, pay and accept agreements (only after decision 1).
- The Team ID and an App Store Connect API key (.p8, role App Manager), saved as `~/.canifishthis/ios-team-id` and
  `~/.canifishthis/asc-api-key.json`; Claude must not make or hold them. Create the app record (bundle id
  `ca.canifishthis.app`) in App Store Connect.
- One real iPhone for the TestFlight check (and a note of its model and iOS version); an older iPhone if one exists.
- Entering App Privacy answers and the age rating in App Store Connect (Claude supplies the text).
- Nothing else until IOS-11: IOS-1 to IOS-10 run on the simulator without the owner.

## Unknowns and risks (honest)

- **Drift since 2026-11-06.** Two weeks of app changes (0.6.x to 0.7.9: salmon build, run timing, saved spots, tap cue,
  Qualcho/tidal release) have never built for iOS. The first build may fail or a screen may break; IOS-1 says.
- **Simulator is not a phone.** Its numbers use the Mac's CPU/GPU; GPU memory, thermal limits, jetsam kills and real
  gesture feel can only show on a device. A "rock solid" claim needs IOS-11.
- **Xcode 27.2 / iOS 26.5 simulator.** Tooling is new; Maestro and `simctl` quirks were already hit (first-launch hang,
  localhost probe). Expect more.
- **Background work on iOS is not guaranteed** (the OS decides when expo-background-task runs); alert notices may be late
  or absent; the product promise on iOS may need wording changes.
- **Review risks:** 480 MB first download (Review may hit slow networks; state the progress clearly), `fetch`/`processing`
  background modes, government-information wording (T365), production data must be live first (T86).
- **Binding patch on two platforms:** every MapLibre bump needs both halves re-applied and both pans re-run.
- **Nothing here has been measured for iOS frame times**, only JS lag and stalls.
- Unconfirmed in this research: minimum iOS target, rotation setting, whether the app-update notice appears on iOS, whether
  `store-shots.mjs` has ever produced iPhone images, whether `build-ios-sim.sh` still works on HEAD.

## First moves for the new chat
1. Read the files above; `rtk git status`; check `scripts/heavy.sh --status` and that the emulator is not in use by an APK
   lane (Android lane is editing `app/`: read, do not edit while it builds).
2. Ask the owner decision 1 and 2 (they shape scope); default to simulator-only work meanwhile.
3. Run IOS-1 as one heavy job; post the baseline with shots; then propose the accepted stubs as tickets.
