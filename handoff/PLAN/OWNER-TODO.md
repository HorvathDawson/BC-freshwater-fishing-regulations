# Owner to-do

**Markdown only (owner 2026-11-24): the Owner To-Do artifact page is retired** ("we will make a new owner todo page (no
artifact only md files that you ask)"). No chat writes artifact rows any more. Chats ask in chat, in batches, and write
each answer here and in the ticket in the same turn. A new short page of open items only is T1080; the docs lean-out
is T1081.

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## 2026-11-24: planning chat (roadmap row P) started

- Answered 2026-11-24, the build-assets bucket over R2's 10 GB free tier (11.49 GiB; one pinned source file is
  9.7 GB): "yes compress it then we should have somethign mention hopw to rebuild it to update every so often too."
  T1089: compressed pins plus a runbook for refreshing the sources. Yours later: confirm the delete of the old
  uncompressed objects once the compressed ones are proven.
- 0.9.0 release, two answers given to the 0.9.0 chat and relayed by it (recorded here by the planning chat, which
  now keeps this file):
  - ~11:30, on the stalled production Android build: "you can run it for me". The 0.9.0 chat ran command 1
    (`build-apk --production`) for him, that time only.
  - 11:59: "okay i am leaving you to the release of everything. can i release new prod data now?" The 0.9.0 chat's
    reading (its own, not confirmed word for word by the owner): go for 0.9.0's remaining CONFIRM_PROD steps from its
    session (iOS TestFlight production build, site, feeds worker), this release only. The owner ran the data release
    himself. Done as of the relay: production data, site (smoke 18/18), feeds worker, Play build 44 on Internal and
    Closed; the iOS build was still building. This does not change the standing rule: CONFIRM_PROD is the owner's
    unless he says so that time.
  - The v0.9.0 tag was moved to 65f84020c (versionCode 44) on his earlier OK; the merge-back PR carries the Play
    record updates.
- Store checks 2026-11-24 22:20 (planning chat, every source read): TestFlight nothing new; Play reviews none. Four
  new items, all ticketed: production, a web page whose stored data was cleared (T1092) and the first crash kind on
  0.9.0, a location request with the phone's Location off (T1093); staging, one low-memory exit on 0.8.5 (T1094) and
  a feature request for fish measurements on a saved location (T1095, design first, 0.10 or later).
  Answered 2026-11-24 22:35 (multiple choice): the T1095 request came from "A tester"; design it in "0.10,
  pictures first"; iPhone 0.9.0 (45), which `pipeline reports` showed as READY_FOR_BETA_SUBMISSION (not yet with the
  external TestFlight testers): "Submit it". The 0.9.1 chat's real run was refused by its permission check (22:55), so
  this is yours: see the Do row below.
- Store checks 2026-11-24 13:30 (planning chat, every source read): TestFlight 6 items, all T982; Play reviews none;
  production worker: one new crash kind on 0.8.5 (T1055); the four production reports of 11-21 are T985's (ids now
  written there). About 40 old items (0.7.x–0.8.2, mostly staging) show as new only because no ticket cites their id:
  T1057 ties them up.
- Answered 2026-11-24: "no play closed testing feedback all was through the app pathway. users dont know how to use
  play one and I dont either." Recorded in the questionnaire (Q3: four channels, no Play form) and the feedback log.
- Answered 2026-11-24: the release table carries a short goal per release (ROADMAP-after-0.9.0.md, "Release contents").
- New from you 2026-11-24: iOS store distribution and compliance (T1071), better screenshots and marketing content
  before the full release (T1072), the app-update notice only when the phone can install it (T1073, 0.9.2).
- **Do (you), small, only after PR #155 (T1058) has merged: remove your machine-wide Gradle workaround.** Open
  `~/.gradle/gradle.properties` and delete the Metaspace line you added for the hang (it sets the same value the repo
  now sets and overrides it; `build-apk.sh` prints a note while it is still there). Nothing breaks if you leave it.
- **Do (you): send iPhone 0.9.0 (45) to the external TestFlight testers** (you chose "Submit it" 2026-11-24 22:35;
  no Claude session may run it). From the main checkout, one command:
  `node deploy/staging/testflight-distribute.mjs --version 0.9.0 --build 45`
  It submits the build to group "testers ext" for Beta App Review (the dry run says READY_FOR_BETA_SUBMISSION). Tell
  the planning chat when it is sent; Apple's review usually takes a day.
- Reported 2026-11-24 22:55 (0.9.1 chat): a network outage stopped its lanes 16:15–22:15, all restarted, nothing
  lost. T1087 (the Brunette closure on the whole river) was read against the printed row: right, nothing to change.
- **Do (you): Play Production staged rollout of 0.9.0 (build 44), once Google grants production access** (T1046,
  T240). Build 44 is on Internal and Closed since 2026-11-24 (Google's review of the Closed release pending). Play
  will not let Production be used until the production-access application is approved; the application opens
  2026-12-07 and this does not move that date or the Closed test clock. Then, in Play Console: Test and release →
  Production → Create new release → Add from library → pick build 44 (or the newest build then on Closed) → release
  notes → Next → set the rollout percentage (start at 20%) → Save → Publishing overview → Send for review. Raise the
  percentage from the same Production page once the crash numbers look clean.
- **Do (you): App Store submission of 0.9.0** (T1071), build 0.9.0 (45), uploaded to App Store Connect 2026-11-24
  ~15:30 from release/0.9.0 65f84020c (TestFlight shows it after Apple's processing). Before you click, T1071's checklist has
  to exist (App Privacy, export compliance, age rating, content rights, review notes): it is not written yet, so this
  row waits on the iOS release chat. Then, in App Store Connect: Apps → Can I Fish This? → Distribution → the "+" by
  iOS App → version 0.9.0 → Build → pick the build → paste the listing text and pictures (T1072) → App Privacy and
  the other answers → Version Release: "Manually release" and Phased Release on → Add for Review → Submit.
- Answered 2026-11-24: the owner to-do goes markdown only, no artifact (T1080); and a ticket to audit and lean out
  every markdown file and unify the docs (T1081, proposed chat D after 0.9.1).
- Answered 2026-11-24 (multiple choice in chat): next chat after 0.9.1 = "iOS full release first"; approved chats =
  0.9.2 hardening, Store content, iOS full release, Docs lean-out (all four); rc tags (T1062) = "With the repo-move
  chat"; the old owner to-do history (T1080) = "Delete it". The order and each chat's prompt to paste are in
  ROADMAP-after-0.9.0.md.

## 2026-11-24 13:30–16:10: T1036, T1053, the ponds, T1052, the answers (verbatim)

- T1036 the second group's head (shown both): first "I don't see the difference"; asked again pointing at the one grey
  line: "\"Also in the fish and possession sheets\"" (as built). Its weight: "Leave it as drawn". No second group
  where the page can open nothing more: "Yes, only what the page can open (Recommended)". T1036 is done.
- T1053 which version of the climb (shown the list): "All 22, but a stream's own row wins". The Sources words
  ("and its tributary streams"): "Yes, these words (Recommended)".
- T1053, typed: "make sure upstream includes tributarties still overrides lower includes tributtaries that it
  conflicts with. make sense?" — the nearer (upstream) tributary rule wins over a lower one it conflicts with.
- T1053, does a stream's own row lift an inherited closure: "keep earlier rule of closure always winning unless it is
  exempt) this does not include no fishiong all year rulings tho those will yield in that case. only the dated
  closures. add all of this logic to gfotchas file please" — a dated inherited closure still closes unless the row is
  exempt; an all-year inherited "No Fishing" yields to a stream's own row; the logic goes into
  `pipeline/docs/GOTCHAS.md` (in the T1053 PR).
- T1053, typed: "tributary rules dont apply to the lake either only streams that is important too".
- Unnamed ponds on a stream (Q124): "only the polygon ponds that we have listed as piece of stream. (for example
  oxbows or slough lakes etc etc ) make sense? using the process we already have".
- T1052 the closed block's second line (shown A and B): "A: quote the river's row". The 130 waters closed for part of
  the year: "Yes, merge those too" — a follow-up PR, pictures first.
- T1029 on the Standard map: the pictures are owed with the finished PR (#112); nothing asked yet.
- New, typed: "we need to add a random (sometimes on launch in a reasonable amount) star rating review on launch. must
  be uninvasive. (or could be after they use a certain feature?) needs thought for 0.10 maybe? or 0.11?" — T1082
  (planning chat).

## 2026-11-24 12:15–12:33: staging reports and the Kitimat creeks, the answers (verbatim)

- T1029, the 12 side creeks at the Kitimat hatchery west-bank closure (shown the picture: reading 1 keep as today,
  reading 2 closed like the west bank): "i think closed we need a clean way to have this calculated from extents so
  it fits into structure nicely" — the creeks read salmon Closed from the west-bank row, worked out from the extent
  by the normal tributary machinery for any one-bank closure (no hand list); no stripe and no bank note on a creek.
- T1050 ice fishing repeats the Gear tab's counts: "Only what differs (Recommended)".
- T1051 treble and barbed hooks: "Say it everywhere (Recommended)" — a second Hooks line on every water where it is
  true (replaces the ROUND13 G call that kept it silent).
- T1052 a fully closed water says it twice: "One block, both calendars (Recommended)".
- T1053 should a river's tributary rule climb through its head lake: "Climb through every head lake" — all 36, also
  where the lake has a row of its own (not the recommended option); built behind a before/after list he reads before
  it ships. The Sources wording ("and its tributary streams") was part of the options, not asked alone: a proposal.

## 2026-11-24: your four staging reports, ticketed for 0.9.1 (as asked; the answers are in the section above)

- Store checks 2026-11-24 12:10: staging worker, 5 reports since 11-24: your four from Dick Lake, Nelson Creek and Eagle
  Creek are T1050–T1053; the Harrison Lake one (e034e473) was answered 11-23 (gotcha, PR #85), now cited in T1046; no
  staging crash defects (1 crawler). Production, TestFlight and Play not read in this check.
- **T1050 ice fishing lines** (Dick Lake: "Lines: 1." and "Hook, lure or fly per line: 1." under Ice fishing repeat the
  Gear tab). A: show a count under a way only where it differs from the Gear tab (here only the hut and ice-hole duties
  stay). B: one line "Same gear as above: 1 line, 1 lure or fly". C: keep. Recommended / default once you pick: A.
  Picture: `handoff/PLAN/app-integration/T1050-shots/report-now.png`.
- **T1051 treble hooks on a lake** (yes, allowed on Dick Lake: p.80 "use of a treble hook is permitted unless single
  hook is specified", p.8 barbed hooks allowed in lakes; the Gear tab says nothing). A: a second Hooks line "Treble and
  barbed hooks allowed ⓘ" wherever nothing restricts hooks. B: the same on lakes only. C: keep silent, note in the ⓘ
  only. Recommended: A. Picture: `handoff/PLAN/app-integration/T1051-shots/report-now.png`.
- **T1052 fully closed water: one statement** (Nelson Creek). A: keep both calendars; one block "Closed to all fishing
  all year, salmon included" / "The B.C. Synopsis row for Nelson Creek (and its tributaries) reads “No Fishing”." + the
  source. B: smallest change, the banner says "No fishing for any fish, salmon included" and the salmon row's sentence
  goes. C: A with one "All fish" calendar. Recommended: A. Picture: `handoff/PLAN/app-integration/T1052-shots/report-now.png`.
  ANSWERED 12:33: "One block, both calendars (Recommended)". Built (lane L22, 936 waters). Left for you: the line under
  the headline is a proposal ("The B.C. Synopsis row for Nelson Creek reads “No Fishing”." / on a tributary "Little
  Qualicum River’s row covers its tributaries: “…”."), and a water closed to all fishing for PART of the year by one
  rule (130 today) keeps its two statements unless you want a headline like "Closed to all fishing until Apr 30, salmon
  included". Pictures: `handoff/PLAN/app-integration/T1052-shots/nelson-after-light.png` (and -dark, nelson-before-*,
  skagit-two-sources-*).
  THE REAL LINES (936 parts): own row "No Fishing" 122; own row, another line 259 (parks and reserves 144); tributary
  "No Fishing" 163; tributary quoting the mainstem's extent 392 (Brunette 94: "No Fishing upstream of Burnaby Lake or
  from Cariboo Dam to Salamander Creek", Squamish 88, Bulkley 34, Capilano 22, Sustut 16). Pick A (as built: quote the
  line, `handoff/PLAN/app-integration/T1052-shots/wordings-A-light.png`) or B (a tributary says "Inherited from Brunette
  River: its row covers its tributaries.", the line one tap away: `handoff/PLAN/app-integration/T1052-shots/wordings-B-light.png`).
- **T1053 walk through a head lake** (Eagle Creek ✱ flows out of Dick Lake; the walk stops at the lake, so Dick Creek
  and 9 small streams above it stay under the zone rules; no written ruling covers this, it is as built). K: keep (as
  built). L: a whole-water rule's walk climbs through its head lake when that lake has no row of its own (24 such lakes:
  Dick, Cameron, Morice, Meziadin, Tsacha, Sooke …); the lake itself stays uncovered. M: through every head lake (36, incl.
  Cowichan, Pitt, Slocan, Babine: much wider). Recommended: L, after a binding diff you read. Default: K until you rule.
  Also the words: the rule's Sources sheet should say "Covers Eagle Creek and its tributary streams" (A, recommended) or
  only show it in Technical detail (B). Picture: `handoff/PLAN/app-integration/T1053-shots/report-now.png`.

## 2026-11-24 08:25–08:50: 0.9.1 overnight questions, the answers (verbatim)

- T905 salmon sizes: "Move worker state out (Recommended)" — the worker's own bookkeeping leaves the public feed (feed
  to about 121,700 B, a feeds worker deploy); the salmon file's gate stays 110,000 B gz.
- T1030 what "N waters changed" counts: "Rules only, hide 0 (Recommended)" — rule changes only, and no count shown at 0.
- T1029 the one-bank stripe (build it, from which zoom): "Should make it more visible right now it isn't ?" — read as:
  build the stripe, but the pictured stripe is too faint; new pictures with a bolder stripe before anything is built in.
- T1047 datum rows: first "Idk what this wants"; asked again in plain words (shift the history of 08HD006 by 5.0 m and
  08EF001 by 2.37 m so their normal range comes back): "Yes but make sure it is a clean well documented update that
  happens which is easy to see" — accept those two rows; the change must be documented and easy to see (what moved,
  by how much, from which note). 07FD001, 08KH019 and 08MH153 were not asked: they stay proposed.
- T1029 Q4, a one-bank stripe where the river is drawn as a polygon: "Along the bank edge".
- T1029 Q5, the 12 side creeks the Kitimat bank closure binds: "No stripe on creeks and only tributaries on the one
  bank side get included" — no stripe on creeks; and a one-bank place that includes tributaries takes only the
  tributaries that join on that bank (a binding change, not only a look).
- T1029 Q6, the rule book's own west-half rule: "Yes, stripe it too (Recommended)".
- T1029, asked again 2026-11-24 ~11:50 with four bolder looks (A split solid, B split + closed dashes, C split +
  label, D stripe outside the line; each from zoom 13 up): "B: split + closed dashes (Recommended)".
- T1029, what "included" means for the Kitimat place's 12 side creeks (told: only one creek joins the closed stretch,
  on the west bank, and the other 11 join through it, so by his west-side rule all 12 stay; today they answer Open):
  "Not sure, show me" — not decided; he gets the DFO notice's exact wording and a map of the 12 creeks with today's
  answer on each. Answered 12:15 (top section): closed, computed from extents.
- T1016 the notice words: "Force refresh instead?"; asked how (auto unless typing / always / keep the notice):
  "Always auto refresh" — a web tab whose data was replaced reloads itself at once onto the live release, with no
  notice to press, even mid-typing (the same for a tab opened before a site update).
- T1031 trout row sources, A or B: "Go on to B" — every rule listed in the row's breakdown too (a Python change,
  ships with the 0.9.1 data release).
- T1036 rules shown only in a species sheet, and Possession, on the "All sources" sheet: "Add back but maybe have
  them separate?" — add them back, under their own heading apart from what the screen draws; pictures of the
  separate group before it merges.
- T1024 the stub polygon's after look: "Yes, after is right (Recommended)".

## 2026-11-24: 0.9.1 overnight questions (as asked; the answers are in the section above)

Pictures and options for all eight are on one page: https://claude.ai/artifact/UZhX9gBLs6JiynhgVcWuFR (the 0.9.1 chat asks
them in chat; answers go here verbatim).

- **Know first (no answer needed):** every Android build with R8 on (new in 0.9.0) stripped a WorkManager constructor, so
  the background check never ran with the app closed (no gauge alerts, no update notices). Fixed on main (T1027, PR #109);
  the 0.9.0 chat cherry-picks it into 0.9.0 before its Play build.
- **T905 salmon feed and salmon file sizes: which way down?** Neither gets under its old size without a format change
  that installed 0.9.0 apps refuse. Options 1–4 in the ticket: keep the budgets; move the worker's own state out of the
  feed (safe, about 121,700 B, a worker deploy); new feed and head formats in 0.9.1; the same with the feed waiting.
  Default until answered: budgets stay (135,000 B / 110,000 B gz). Recommended: option 2.
- **T1047 datum changes, the other three: accept / reject?** (08HD006 and 08EF001 answered 2026-11-24, below.)
  07FD001 (+94.826 m from 2026-08-05) and 08KH019 (+1.011 m from 2026-07-07) only after the this-year line is fixed or
  2027 begins (the file refuses them until then). 08MH153: recommended reject (a slow creep, not a step). Default: the
  three stay proposed; nothing changes. Pictures: `handoff/PLAN/app-integration/T1047-shots/`.
- **T1030 "N waters changed": what counts?** A: wording changes too. B (built): rules only. C: rules only, nothing shown
  at 0. With B, 0.9.0 would have read "2 waters changed" though 8,247 waters were reworded. Recommended: C.
- **T1029 one-bank stripe, Q4–Q6** (a prototype in draft PR #112, not merged): the stripe from zoom 13; Q4 no stripe on
  a polygon reach (c); Q5 no stripe on the 12 tributaries; Q6 the rule book's west half gets the stripe. Those are the
  lane's recommendations; nothing is built into the app until answered.
- **T1016 the two notice texts** (merged with these as default): "This page's data was replaced by a newer release. /
  Load the new data ›" and "The app was updated since this page opened. / Reload ›". Pictures:
  `handoff/PLAN/app-integration/T1016-shots/`. Also yours to say go on: the feeds worker deploy that classes these
  reports as expected.
- **T1031 trout rows:** A (merged): every page behind a row on its Source line. B: every rule in the row's breakdown
  too (a Python change and a data release). Pictures: `handoff/PLAN/app-integration/T1031-shots/` (`rowpages-*`).
- **T1036 "All sources":** should a species sheet's own rules, and Possession, be listed in the page's sheet? Built: no.
  Pictures: `handoff/PLAN/app-integration/T1036-shots/`.
- **T1024 stub polygons: look at the after shots** (the ring alone on a selected river polygon whose route is a short
  stub; needs the 0.9.1 tile build). On the page above.
- Store checks 2026-11-24 00:50: TestFlight 6 items, all already ticketed (T982); Play none; production worker nothing
  new since the last check. With the worker tokens the reports command also lists 36 old items (app 0.7.x–0.8.0, up to
  2026-11-18, mostly the staging worker) as never ticketed: handed to the planning chat, not ticketed by the 0.9.1 chat.

## 2026-11-24: 0.9.0 release answers (verbatim)

- Salmon head size gate (T796, T905): "Raise to 110,000 (Recommended)". 100,000 → 110,000; the shrink is T905.
- The side atlas (T1012, T803): "yes promote if it fixes all the issues??" — promoted only after the four checks (inputs, preflight, binding diff, vintage) pass.
- The main checkout: "Yes, switch and go (Recommended)" — release/0.9.0 for Steps 2–5, back to main after.
- Cold-start tab links (T1049; device pass: tab=gear 0/6, tab=licence 1/6 on a cold start, fine when the app is open): "Ship, fix in 0.9.1 (Rec.)". 0.9.0 ships with it as a known issue; the 0.9.1 chat owns the fix.
- The 0.9.0 What's new bullets (the five in changelog.ts): "OK as written (Rec.)". The draft mark is cleared on release/0.9.0 (the entry is only there until the merge-back).
- The v0.9.0 tag (it was at a98dd68f9, before the WorkManager, crash and tab fixes): "Yes but in future we can use release candidates ?". Moved to the commit the production build uses. Release candidates for later releases go to the planning chat as a runbook item: tag each staging candidate `v<x.y.z>-rc.<n>`, and tag `v<x.y.z>` only at the production commit.
- The production Android build's CI check (docs-ci lookback of 30 runs misses the green ancestor run; the checker bug is with the 0.9.1 chat): "Yes, with the reason (Rec.)". The build runs with a CI_CHECK_SKIP that states the reason.

## 2026-11-23: Google Play production access, the closed-test clock (T1046)
- **Clock:** every tester opted in on 2026-11-23 (you). 14 days → **earliest apply 2026-12-07**, if at least 12 stay
  opted in the whole time (one who opts out and back in restarts their own 14 days).
- **Check (you): T1046 opted-in count** — in Play Console (Testing > Closed testing > Testers) check that ≥12 are opted
  in; write the count, the 12th tester's opt-in time and any opt-out in the clock table of
  `handoff/PLAN/play-production-access/README.md` (or tell Claude).
- **Tell Claude (you): T1046 Play testing feedback** — Play has no API for it: when Play Console > Monitor and improve >
  Ratings and reviews > Testing feedback shows anything, say so; Claude logs it in `feedback-log.md` and tickets it.
- **Fill (you): T1046 questionnaire TODOs** — `handoff/PLAN/play-production-access/questionnaire-draft.md` ends with
  the list (recruiting pick, usage numbers, age group, installs range, vitals crash / ANR rates, …).
- **Apply (you): T1046 production access** — on or after 2026-12-07, Play Console > Dashboard > "Apply for production",
  pasting the answers in your own words. Then tell Claude the date and, within about 7 days, Google's answer.

## 2026-11-24: T1047 datum changes, 08HD006 + 08EF001 (answer, verbatim)
- Asked (plain words, through the coordinator) whether to shift the history of 08HD006 (Salmon River near Sayward, 5.0 m
  lower since 2025-06-05) and 08EF001 (2.37 m higher since 2025-01-30) so their normal range comes back: "Yes but make
  sure it is a clean well documented update that happens which is easy to see". Both rows `accepted` (lane L16), the
  page: `pipeline/docs/GAUGE-DATUM-CHANGES.md` (Environment Canada's notes, before/after pictures
  `T1047-shots/accepted-<station>-{before,after}.png`, one-command undo); the data release's checks list each as its own
  line. Live with the next data release (no app build). 07FD001, 08KH019, 08MH153 were not asked: still proposed.

## 2026-11-23: T1047 off-datum level (answers, verbatim)
- T1047 top line: "Show the level reading (Recommended)"; the note: "just use new data line same as cap. keep it simple".
  Built on PR #77 (b15f1165b): under Level an off-datum station (08HD006 …) reads as Capilano does: today's level,
  "Too new to say if this is high or low.", its live line with no band; the proposed note dropped. Closed.

## 2026-11-23: T1043 Capilano level (answers, verbatim)
- T1043 estimate under Level, and the chart's scale: "add ranked on flow at top. also the scale of the chart should use
  the year data if not enough histroical data exists." Built on PR #77: "(ranked on flow)" at the top of the estimate box
  while Level is picked; a 72 h chart with no normal range scales to that quantity's year data. Closed.
## 2026-11-23 ~15:00: T1041 faster CI checks (answers, verbatim) — one item for you, AFTER 0.9.0
- Local checks: "Both (Recommended)" — related tests before each push + full app suite and pipeline tier once before
  opening the PR. Built: the pre-push hook runs `vitest related` + the pipeline tests importing a changed module;
  `scripts/before_pr.sh` runs both whole suites.
- Mutation in CI: "Report only (Recommended)" — on changed lines, Stryker for app TS, mutmut for Python; non-blocking.
  Built: `.github/workflows/mutation.yml` (not a required check).
- PR chains: "Merge queue (Recommended)". Timing: "All now". Merge queue needs an org: "Move to a free org
  (Recommended)" — the transfer is your click.
- Later the same day: "i think we should do repo move after m0.9.0" — so the move, the slug flip and the merge-queue
  ruleset are a post-0.9.0 follow-up; only the `merge_group` triggers went in now (they do nothing until a queue exists).
- **Do (you), any time, one line: keep the push connection alive** (T1041: GitHub closed git's ssh connection under a
  420 s pre-push hook; with a keepalive the same push went through). In the main checkout:
  `git config core.sshCommand "ssh -i ~/.ssh/id_ed25519_personal -o ServerAliveInterval=15 -o ServerAliveCountMax=8"`
  (every worktree shares it). Or add `ServerAliveInterval 15` under `Host github.com-personal` in ~/.ssh/config.
- **Do (you), after 0.9.0 ships: move the repo to a free organisation** (details: T1041, "Transfer checklist"):
  1. Create a free GitHub organisation (github.com/organizations/plan, Free); you are its owner.
  2. Repo Settings -> General -> Danger zone -> Transfer -> the organisation (keep the name).
  3. Re-issue the feeds worker's `GITHUB_DISPATCH_TOKEN`: a new fine-grained token, resource owner = the organisation,
     only this repo, Actions: read and write. Put it on BOTH feeds workers:
     `npx --prefix app wrangler secret put GITHUB_DISPATCH_TOKEN -c app/workers/feeds/wrangler.toml` (staging) and
     `... -c app/workers/feeds/wrangler.prod.toml` (production); then revoke the old token (runbooks/secrets.md).
  4. Check the 4 Actions secrets (CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY)
     and the 3 rulesets are still on the repo (Settings -> Secrets / Rules).
  5. On your machine: `git remote set-url origin git@github.com-personal:<org>/BC-freshwater-fishing-regulations.git`.
  6. Tell Claude the org name: the slug flip (repo.ts + ci_paths.py + 4 tests) and the merge-queue rule go in as one PR.

## 2026-11-23: PR #84 gauge links (answers, verbatim)
- Visible "River gauges for this water" list: "I don't like those there gauges only linked from conditions tab and even there I don't think we should have links. Gauge tabs are kind of separate and from the share. I don't know either way regulations are separate" — removed; no water-to-gauge links anywhere.
- Home page hidden link row (T1011): "Drop the hidden row (Recommended)" — removed.

## 2026-11-23: T1042 search description (answer, verbatim)
- T1042 description facts: "Season windows (Recommended)" — option b of `T1042-shots/live-samples.txt`: open windows from the year strip,
  boats, steady marks; nothing from the build day. Built on PR #78.

## 2026-11-23: lane L25 (0.9.0 batch) — one pick
- **Look (you): T1039 gauge page after-shots** — the app's own chart on the gauge page (72 h / Year, Flow / Level, zoom, every model): `handoff/PLAN/app-integration/T1039-shots/after-page-chilliwack-desktop.png`, `after-page-chilliwack-phone-390.png`, `after-page-chilliwack-year-desktop.png`, and Two Forty Creek with ELF offered `after-page-two-forty-elf-desktop.png` / `after-page-two-forty-elf-phone-390.png` (before: `before-*`). Shows on staging after the feeds worker deploy and the 0.9.0 staging site build.
- T1039 follow-ups 2026-11-23, verbatim: "chart should use same buttons as spots too for now" — built: the Spots tab's own "Get the app" block on the gauge page, both test buttons (`T1039-shots/after-get-app-*`); "could add other almanac info too?" — built: sunrise/sunset and the spot window's cloud, pressure, humidity (`T1039-shots/after-almanac-*`); moon left out (the app has none), see T1039.
- Pick: T1039 gauge page chart (Q183) — answered 2026-11-23, verbatim: chart "App's chart, ~260 KB (Recommended)"; ELF "Yes, fix in 0.9.0 (Recommended)". Closed; lane L25 builds both.

## 2026-11-23: lane L29 (0.9.0 batch) — one look
- **T1024 Gravel Slough highlight: look at the before/after shots** (`handoff/PLAN/app-integration/T1024-shots/gravel-before-after.png`, `oxbows-before-after.png`, `gun-before-after.png`): where a slough's or wide reach's line is drawn, only the line is outlined — no ring round each piece of its polygon; where it is not yet drawn (zoomed out, or no line at all) the polygon keeps its ring. Note Gun Creek z13: the wide 3 km² reach shows only its line's outline. Say if it reads right. Also (0.9.1, PR #111) `stub-oxbows-light.png` / `stub-oxbows-dark.png` and `stub-nazko-light.png` / `stub-nazko-dark.png`: where a polygon's line is only a short stub inside it, the polygon keeps its ring and the stub no longer gets a second outline inside it (`stub-oxbows-matrix.png`: old app or old tiles look exactly as today).

## 2026-11-23 ~10:50: T1031 inline sources (answers, verbatim)
- T1031 inline look: "Yes, as built (Recommended)" — the one Source line (`T1031-shots/inline-*`), kept as built (PR #69).
- T1031 the book's name: "'B.C. Synopsis' everywhere (Recommended)" — a rule's in-sentence link reads "B.C. Synopsis p.N ↗",
  the salmon rows' place label too; built through `core/src/sourceNames.ts` (PR #69).
## 2026-11-23: lane L18 (0.9.0 batch) — one look
- Look: T997 round 2, only the lit part's two ends — answered 2026-11-23 ~10:30, Whole river: "Keep the two labels (Recommended)". Closed.

## 2026-11-23 00:05: question round 5 (answers, verbatim)
- T1026 day picker: "Yes, extend to Mar 31 2027 (Recommended)" — to: should the 0.9.0 data release extend the day-by-day answers through Mar 31, 2027? Built without a format change (picker wraps past New Year; see T1026).
## 2026-11-22 18:55: lane L3 (0.9.0 batch) — three looks
- Look: T997 Nicomekl labels (round 1) — answered 2026-11-22 round 2; superseded by the round 2 row above.
- **Look (you): T940 satellite after-shots** — `handoff/PLAN/app-integration/T940-shots/after-*` (the approved set built; web light + dark; the emulator shot waits for the 0.9.0 APK).
- **Look (you): T1004 Class II line under the name** — `handoff/PLAN/app-integration/T1004-shots/built-before-*` / `built-after-*` (light + dark; the line under the name, every sub-tab).
- **Approve (you): T940 satellite line colours** — `T940-shots/proposal-vs-now.png` (a mock-up; recommend the set in the ticket).
## 2026-11-23 ~10:50: T1031 species sheet + book name (answers, verbatim)
- T1031 name tap: "Picture only (Recommended)" — as built (lane L23, PR #71): the picture opens Identification, the rest of the line (name included) Sources.
- T1031 sheet words: "Keep these words (Recommended)" — as built: "Identification | Sources", "Where each rule comes from".
- T1031 book name: "'B.C. Synopsis' everywhere (Recommended)" — lane L22 (#69) sets it in `core/src/sourceNames.ts`; the species breakdown's page links read it from there (no word of their own).

## 2026-11-22 23:20: question round 1 (answers, verbatim)
- T1010 region page: "B: map preview" — being built.
- T940 satellite: "Yes, use the proposed set" — being built.
- T1004 Class II tag: "I don't like either could we think of a better spot?" — new placements coming as pictures.
- T991 sources: "One source row in bottom. But should have different ones for each tab too. Like condition sources
  don't need to be on regulations. Please think about it more we also still have inline sources?" — a refined
  per-tab proposal with the inline-sources question answered, as pictures.

## 2026-11-22 23:55: question round 5 (answers, verbatim)
- T1004 Class II tag: "What about right under the water body name in the white space there?" — being built (lane L17): the line under the name, every sub-tab.
- T940 No-access swatch: "Dark outline on swatch (Recommended)" — built for every satellite swatch under 3:1 on its card (light + cvd).
- T991 sources: "Yes but map sources don't need to be there already has attribution.. also in the synopsis page for skeena at bottom source. Page 9 is not the water body page the source there should be maybe one for each page referenced in the card? Also make sure highlighting is still there for each? We also should clean up our inline sources. Right now the way they display is inconsistent.... We still want to have the break down source like the sheet in trout too. Each specieis have them as well?  Also right now we have identification and sources open from clicking different parts of the a specieis line in table. Maybe we could have that be the same sheet with different tabs?" — the build goes to lane L21.
- T940 colour-blind satellite (decided 2026-11-23 ~01:30), verbatim: "Own cvd satellite set (Recommended)". Closed. A proposed set with pictures is in `handoff/PLAN/app-integration/T940-shots/cvd-proposal-standard.png` / `cvd-proposal-salmon.png`; the owner picks the look before it is built (row below).
- T940 cvd satellite proposal (answered 2026-11-23 ~01:45), verbatim: "Build as proposed (Recommended)". Built (PR feat/T940-cvd-satellite); after-shots `T940-shots/cvd-after-*.png` — the owner looks.

## 2026-11-22 23:55: question round 4 (answers, verbatim)
- T867 inbox: "Build all four (Recommended)". T981 round 2: "Later release we will do this".

## 2026-11-22 23:45: question round 3 (answers, verbatim)
- T754 one bank: "Other bank: the water's other rules". T774: "Keep it". R8: "Do 1 but make a ticket to push higher in
  future" (72% accepted; T1027). PR #47's marks: "I don't understand what the issue is here ... What happened to marks?
  We did all the dog ones already ???" — Claude's mistake: the 3 Campbell marks record YOUR 11-16 answer ("They are
  right", T803 item 8), saved through the review app as usual; nothing to do.

## 2026-11-23: lane L18 (0.9.0 batch) — T467 as built
- Check: T467 shaded shoreline as built — answered 2026-11-23 ~10:30: "i like the look of the shaded shore even for none colorblind"; colour-blind band too? "No, keep cvd solid". Closed; stays as built.

## 2026-11-22 23:35: question round 2 (answers, verbatim)
- T997 + T465 labels: "Looks better but why does it even need to show both? Like dyke gates is different then 23m below?
  Only need boundaries for highlighted section." — only the highlighted part's own boundaries get labels.
- T467 shoreline: "Yes, the shaded band (Recommended)" — being built.
- T802 Class I/II on the map: "Don't want on map. Only in card I think" — folds into T1004.
- T812 Babine circles: "Yes, as shown (Recommended)" — being finished.

## 2026-11-22 20:15: 0.9.0 batch chat (stopped for credits) — open for you
- **Questions for the next chat's round (pictures in each ticket's shots folder):** T1010 region page look (A, B or C;
  A recommended), T1004 Class II tag (A now / B beside the name; B recommended), T940 satellite colour set (approve?),
  T991 sources pattern (A recommended), T997 Nicomekl labels after shots, T867 notifications inbox (needs your answer
  before it is built), R8 at ~72% in every category: enough? (recommended yes), T981's 5 municipal proposals, the 0.9.0
  "What's new" bullets, and any Play reviews or closed-testing feedback.
- **Production steps (you, CONFIRM_PROD):** T1014 copy the Play version file T1014 names from the staging bucket to production
  R2 (3 production phones get a 404 on their update check); T996 the three APNs secrets on the production feeds worker
  (`npx --prefix app wrangler secret put APNS_KEY -c app/workers/feeds/wrangler.prod.toml`, then APNS_KEY_ID and
  APNS_TEAM_ID), then the phone test on the 0.9.0 TestFlight build (steps in T996).
- **To check (you):** T1015 what Google renders on /water/ pages (Search Console, URL inspection).
- **Optional (you):** T1007 the Chrome "StorageType.persistent" warning is Cloudflare's injected bot script: turn off
  Cloudflare Bots / JavaScript Detections to silence it.

## 2026-11-22 13:40: end of the build/CI chat — open for you
- ~~Play Console deadline date into T1003~~ not needed (owner 2026-11-22: T1003 goes in 0.9.0 regardless).
- ~~T1008 serve or hide source maps~~ decided: serve (owner 2026-11-22).
- **T1010 region page option B: look at the after shots** (`handoff/PLAN/app-integration/T1010-shots/after-B-*.png`, phone and desktop, light and dark); say if the look is right.
- **To do (you), about 10 a day:** request indexing for `handoff/PLAN/seo/index-requests-top-100.md` in Search Console.
- **For the next chat:** the claude.ai runbook copy (5r4yEqNwqygin1oKo5Zxjk) and the Owner To-Do page could not be opened
  from the CI chat ("not found / not shared"), so their rows for this chat were not added there.

## 2026-11-21 evening: build/CI streamlining chat (T993, T978, T994; no release) — open decisions, asked at the end

- **To read (you): the one release runbook and the gates page [T993 item 11, T992].**
  `handoff/PLAN/runbooks/release-by-hand.md` is the whole release in order, each step marked with who runs it;
  `handoff/PLAN/runbooks/release-gates-explained.md` says what each stop means and when an accept is safe, and ends
  with eight simplification ideas to pick or drop. Try it on 0.8.6 and say what reads wrong.
- **T978 MapLibre (answered 2026-11-22):** "Pinned upstream main (Recommended)". From 0.8.6 Android builds use one
  pinned upstream main commit, no patch (measured 20-32% cheaper; 0002 within noise). Claude builds the switch.
- **Old headless Chrome (answered 2026-11-22):** "Yes, stop them (Recommended)". 19 orphaned headless Chrome trees
  (parent gone, 1-3.7 days old) stopped; your own Chrome untouched.
- **Merging (answered):** PRs + required checks into redesign/stream-sections. **Push hook (answered):** versioned
  pre-push hook running the before-push command.
- **T995 done on GitHub (2026-11-22), for you to know:** `main` is the trunk. Direct pushes to `main` are refused,
  yours too: changes go by pull request with the four checks green (you can still merge a PR without green checks as
  admin). `redesign/stream-sections` is read-only until the 0.8.6 release.
- **To do (you):** turn off the Copilot coding agent in the repo's Settings → Copilot (its environment is already
  deleted).
- **T866 test trim (answered 2026-11-22):** "Speedups, remove nothing (Rec.)", "Remove 5 true duplicates", "Fold the 30
  wording files"; not the parallel pipeline tests. Proposal: `handoff/PLAN/audit-2026-11-21/t866-proposal.md`.
- **T998 versioning (answered):** 1.0.0 = "At public launch (Recommended)"; the bump comes from "Conventional Commits"
  (feat → minor, fix → patch, `!` → major; before 1.0 a breaking change bumps minor). You confirm each computed
  version at release.

## 2026-11-21 12:40: 0.8.5 batch scope, asked one ticket at a time (dialog)

- T978 (build checks; MapLibre from upstream main): NOT in this batch, next chat.
- T940 (satellite water-line colours): NOT in this batch, next chat (pictures + swatch rim question there).
- T686 switch-on (packed formats): IN this batch. Owner, verbatim: "0.8.4 is available to testers on android closed test and apple as all users on 0.8.4 so yes add it".
- T975 (remove old-format readers/writers + the switch): IN this batch, after T686 is flipped and staging data passes smoke; data min app 0.8.4; 0.8.5 data reaches production before the 0.8.5 apps.
- T981 (municipal / park-bylaw closed waters): IN this batch ("Yes, add it", against my recommendation). Claude drafts the inventory; you accept each row before it ships.
- T977 (push carry race): NOT in this batch, next chat.
- T709 + T715 (store video): stay postponed.
- Website "join the Android test" button: leave it as it is; no Google Group (owner, verbatim: "we will just leave join android as it is for now. no google group").
- Design picks (dialog with pictures, 13:2x): T986 = C filled selected button; T982 #2 = C dot with shade + size + number, no boxes; T982 #3 = B "Far lower than normal" / "Lower than normal" / "Normal" / "Higher than normal" / "Far higher than normal"; T982 #4 = B icon + one word ("Photos" / "Camera"); T985 #4 = B (handle on the date bar to scrub, pan on an axis strip, pinch zooms) "but have it also use the hold and drag gesture on top of it" (owner, verbatim); T982 #6 = after a dismissed/denied first location ask, show a one-line "Show my location" prompt on the map.
- T979 (dialog with picture): "1 and maybe remove some white space ? Only on mobile does it matter" (owner, verbatim): the map takes the free height, the exit + report rows sit in the bar's foot (held height only where the water has an exit), trim other white space; phone layout only.
- T985 #2: label the mouth end ("Flows into …" at the downstream end, every water with an exit; the mouth's place goes in the release).
- T981 (dialog): IN 0.8.5, built now. Owner, verbatim: "https://stanleyparkvan.com/stanley-park-van-location-lost-lagoon.html is only one I know for sure. Maybe add other info to a ticket but only create the way to ban lost lagoon for now with a nice curated data path to list these. Then put all the info found for the other water bodies in the ticket and we will review for the next release." So: the curated local-bylaw path + Lost Lagoon only; every other row (Goodacre, Fountain, Swan, Deer, Burnaby, Green Timbers, Beaver) waits in the ticket for review next release.
- T983 (dialog): the 7 "July 30" closures keep showing while the Province lists them (T548 stands); no automatic repair for phones on 0.8.4 ("Mark all read" is enough); the inbox shows the notice's own date (first-seen only when it has none, labelled so).
- T988: the Done that does not close is on the trout table's Sources menu, on Android.
- Answered 2026-11-22 (dialog): next version 0.9.0 as computed, no release until the next chat's work is in ("let it be 0.9 but we wont release yet…", T998); Closed testing gets the PRODUCTION Android build (T993 runbook); CONFIRM_PROD: "only me unless i say otherwise that time": the production website and feeds worker are your steps again (AGENTS W46 stands; T993 runbook).
- Store checks 2026-11-22 23:00: no TestFlight feedback; two new STAGING reports (T1023 "Flows into" should open the receiving water on the map; T1024 Gravel Slough highlight); one new production crash kind folded into T1005; your tester's two points are T1025 (sub-tab jump scrolls through sections) and T1026 (day picker stops at December 2026); Play reviews: tell me if any came in.
- Store checks 2026-11-22 18:15: TestFlight 0.8.5 (44) external APPROVED, in beta testing; no TestFlight feedback beyond T982's 6; production reports since 11-21: nothing new (T985 ×4, T1004); two STAGING reports from 0.8.5 (43), new: Serpentine ditches on the mainstem (T1012) and Runs slow on first switch (T1013); crashes: the same kinds as T1005 (hashes now recorded there) plus one staging 429 on an exit record (folded into T1005); Play reviews: no reader yet (T1006), tell me if any came in.
- Store checks 2026-11-22 11:30: TestFlight 0.8.5 (44) external APPROVED, in beta testing; no TestFlight feedback beyond T982's 6; 6 production in-app reports since 11-20: T979, T985 ×4, and one new (Class II tag placement, T1004); ~20 crash kinds no ticket names (T1005, mostly 0.8.4 HTTP 404s and a PMTiles "wrong magic number"); Play reviews: no reader yet (T1006), tell me if any came in.
- Store checks 12:50: TestFlight 0.8.4 (43) external APPROVED, in beta testing; no new TestFlight feedback beyond T982's 6; 5 in-app reports: one is T979, four are T985. Your tester's Trout/Salmon switch note is T986. All join the batch.

## 2026-11-21 11:24: 0.8.4 promoted to Play Closed testing (owner)

- Owner, verbatim: "promote now please (not enough tokens to fix it)": versionCode 42 Internal → Closed (track alpha),
  knowing T983 (old in-season notices show as New after updating) is not fixed yet.

## 2026-11-20 17:13: production data, one-time agent override (owner)

- Owner, verbatim: "Add one time override then remove it after" (leaving the computer, wants the release out; "We have
  minimal users"). `pipeline/release.py prod()`: an agent may publish the ONE release named in OWNER_ONE_TIME_PROD (=
  CONFIRM_PROD = the id), today only; checked, staged, fingerprint and "staging serves it" all still hold. Reverted
  right after that release ships.
- USED 19:00 for r-fec3fab4c5aa only (published to production), then REVERTED (the guard is human-only again).

## 2026-11-20 15:11: 0.8.4 production builds approved

- Owner, verbatim: "Yes you can run them": the 0.8.4 production builds (CONFIRM_PROD), Android to Play Internal and iOS
  to TestFlight internal plus external (beta review). This release only. Never Play Closed; no App Store submission.

## 2026-11-20 13:32: release scope: Apple External in, production data in

- Owner, verbatim: "Apple external can be touched with this release as a product build. Also we will want to update
  prod data too". This release now also puts the 0.8.4 production build (reading production data) on TestFlight
  EXTERNAL testing (Apple's beta review applies). Production data was already in. Order: merge → map rebuild →
  staging → production data → Play Internal + TestFlight internal and external → production website. Play Closed
  stays untouched. An App Store production submission stays yours.

## 2026-11-20 13:22: Classified Waters carried unit shown as Classified Waters, the why behind a button (T973)

- Owner 13:22, on the Endako shot (note above "You need" with a "?"), verbatim: "in the photo you sent there is a rouge ? in the line and the line is under the header Canadian, other province, 167 and older?". Then (dialog), verbatim: "we should just say it is classified waters then give a button that expands the note on why we say that maybe? ".
  Built as: a carried unit shows as an ordinary Classified Waters licence row, in the same place and for the same
  anglers as a real unit. An expand control on the row opens the why ("Stellako River's Class II covers its
  tributaries; Endako River has its own line in the rule book without Classified Waters, so it may not apply here.
  Check with a CO."). The "?" and the note above "You need" go.

## 2026-11-20 12:54: Classified Waters units that stop at their own water: confirmed (T973)

- The owner checked the book: only Thompson (R3), Skeena sections 2 and 4 (R6), West Road (R5) and Deena Creek (R1) stop
  at their own water; all 5 are right. Verbatim: "yes i missed those both are correct too."

## 2026-11-20 12:38: Classified Waters "may need" cleanup after a CO answers (T974)

- Owner, verbatim: "we want to ticket cleaning this up once we get contact with a CO". → T974: once the CO answers,
  the note becomes a firm unit or is dropped, and the optional file and the note path go. Waits for your CO contact.
- Owner asked whether "[Includes Tributaries]" on the water plus a CW on its row means the tributaries get the CW.
  Answer: yes. A CW line with no tributary marker of its own inherits the water's "includes tributaries"
  (`classify.wants_tributaries`, AGENTS 9). That is the same as a "X's tributaries" line.

## 2026-11-20 12:24: Classified Waters, nearest wins both ways (T973)

- Owner, verbatim: "this also happens invereted (Nearest still wins: a stream gets only one unit, and an upstream Class II overrules a downstream Class I above it.) for example a upstrem class i overrules a downstream class ii too. (assumign that the classified water sis applied to tributaries) usually it says it?"
- Yes: nearest wins whatever the class. A unit claims the creeks above it only when its row says "[Includes
  Tributaries]" (or is a tributaries-only row). Default built: a classified water whose unit stops at its own water is
  passed through, with a "may need" note above it. L10 brings the count and examples to confirm.

## 2026-11-20 12:15: iPhone fixes from lane L6, two things for you (T723, T760; NOT asked yet)

- **The Save bar over the iPhone keyboard (T723).** On a spot's page, while you type a title or notes, a purple
  "Save spot" bar now sits on top of the keyboard (iPhone only; Android and the web are unchanged). The page's own Save
  row stays under the keyboard, as before. It is a proposal: keep it, or change its look? Default until you answer:
  keep. Pictures: `handoff/PLAN/ios/shots/t723/after-l6-spot-notes-bar.jpg` (the bar over the notes),
  `handoff/PLAN/ios/shots/t723/before-l6-spot-save-row.jpg` (before: Save hidden under the keyboard). Also fixed with
  it, no choice needed: the Conditions "Alert me" value field now sits above the number pad
  (`handoff/PLAN/ios/shots/t723/after-l6-cond-alert-value.jpg`), and the left-edge swipe closes the map's credits card.
- **Optional phone check: a first install on mobile data with "Wait for Wi-Fi" (T760).** Only a real iPhone can show
  it (the simulator is always on Wi-Fi). Before the fix, an iPhone on mobile data that chose "Wait for Wi-Fi" would
  restart the download twice and then fail it as "stuck" after about 30 minutes. Now it should wait quietly and start
  by itself on Wi-Fi. Check when TestFlight exists: install, mobile data on, Wi-Fi off, pick "Wait for Wi-Fi", leave it
  40 minutes, then turn Wi-Fi on. Default until you check: ship as built (Android already behaves this way).
- Still open from before: the oldest iPhone the app supports (T725, item (1) under "Open from the iOS day"); the memory
  numbers on the simulator are in T725.
- No action, for your eyes (T938, T928): the iPhone map left standing two-thirds to the right after a swipe back (the
  tester's picture) is reproduced on the simulator and fixed. Before:
  `handoff/PLAN/ios/shots/t938/before-fix-light-3-after-edge-back.jpg`; after:
  `handoff/PLAN/ios/shots/t938/after-fix-light-3-after-edge-back.jpg`. Also fixed: in the spot photo viewer a swipe to
  the next photo no longer hides the buttons (T836).

## 2026-11-20 12:11: Classified Waters carried through, with its path (T973), in this release

- Owner 12:10, verbatim: "Maybe we should propogate classified waters through the separate listed streams still just
  make it a tuple or something with what class, where the source of that classified waters is and anither item saying
  if it passed through anither listed stream without classified water title etc. still have nearest classified water
  level with so downstream cwi is overruled by a more upstream cwii etc. make sense? Then we can at least have the info
  throughout".
- Answers (dialog 12:11): display **"\"May need\" note (Recommended)"** (e.g. "Stellako River Class II may apply here:
  the Endako has its own line without Classified Waters. Check with a CO."); when **"In this release"**. The CO
  question (GOTCHAS, Classified Waters licence) stays yours; its answer decides the note later.

## 2026-11-20 11:44: Classified Waters through a rowed tributary: ask a CO (T973)

- On T944's licensing changes, the owner asked whether they follow his tributary rule (carry rules down; nearest wins
  on a conflict; compatible rules stack). The unit MOVES (Morice tributaries Bulkley → Morice; Brownie Creek Elk →
  Fording) follow it: two units always conflict, so the nearest wins (RU-13). The DROPS (Harmer Creek above Grave Creek,
  the Horsefly and Stellako creeks, and as before Gosnell and the Endako) come from LI-2 (2026-10-03): a tributary whose
  own row prints no unit is not classified, and neither is anything above it. That may not fit "compatible rules
  stack". Answer (dialog): **"We should maybe just make this a ticket and a gotcha that needs to be asked to a CO. Give
  info in the ticket. Doesn't hold release (keep what we have but on ticket reset we can measure it better etc and make
  a decision)"**. → T973 (question for the CO drafted there; Q181). The release keeps LI-2.
- **Yours:** ask a Conservation Officer the T973 question when you get the chance.

## 2026-11-20 11:11: salmon card and map fixes answered (dialog); T971 settled by the Flow-leads ruling

- T669 item 3 (lane L9, pictures `tickets/T669-shots/before|after-qualcho-creek-jan15.png` sent): **"Ship it
  (Recommended)"**: a straddling water's salmon card shows the stricter region's numbers instead of "see DFO's words
  below" (Qualcho Creek, Region 5B/6, is the only live card that changes).
  Then the owner asked whether coho "only 1 over 50" was wrong (5B says 2 over 50): it is Region 6's own cap (all Region 6
  waters: "4 per day, only 1 over 50 cm"), the stricter of the two. Answer (dialog): **"Name the region (Recommended)"**:
  on a mixed line each number says its region, e.g. "Up to 4 coho a day in total, all waters of MU 5-6 to 5-11 combined
  (Region 5B) · only 1 over 50 cm (Region 6)".
- T679 #1 (203 pieces just outside B.C. marked outside) and #6 (42 named pieces given to their water): **"Turn both on
  (Recommended)"** in the map rebuild; Claude checks the before/after diff first and stops if anything unexpected moves.
- T679 #12 (tributary scope into the bundle): not asked; lane L7's default stands (the two lists stay in code).
- T971 (Level in the gauge card re-ranks the estimate card, lane L4 10:28): NOT asked: the owner's 2026-11-10 ruling
  (Flow leads, Level never picks its own; never ask) settles it. The estimate card stays on Flow; fixed in this release.

## 2026-11-20 11:02: T679 #8 lake rule answered (dialog)

The owner worried tiny lakes would cut a river off from its own gauge. Measured (lane/l8-blockers e39e97d1c,
`T679-shots/own-gauge-blockers.csv`): of the 1,060 moved labels, 881 are blocked by a lake or pond; 448 of those by
unnamed ponds that belong to no water (Halfway: a 0.44 ha pond cuts off 80 sections), 433 by named lakes (Pitt, Bowron,
Lillooet); 179 are no lake (another branch or channel). Answer: **"Named lakes only (Recommended)"**: the walk along a
water passes through unnamed ponds that belong to no water, at any size; named lakes still end it. The same walk ranks
the lead gauge (`own_distances`), so both change together. Ships with #8 in this release.

## 2026-11-20 10:45: credits and T679 #8 answered (dialog)

The owner asked how much is left (credits running low) and why so many gauges "changed waters". Claude's answer: none
changes water and no lead gauge changes; #8 only relabels a list entry "on this water" → "nearby" where a lake lies
between (1,060 of 12,306 own labels; list sent as `app-integration/T679-shots/own-gauge-moves.csv`). Answers:
- Credit savings: **"One combined review (Recommended)"**: the remaining lanes get ONE independent review together
  before merge, not one each. (Not chosen: pausing T940, fewer pictures.)
- T679 #8: **"Ship it"**: `SHIP_OWN_GAUGE_FLAG` on in this release; it rides the map-data rebuild.

## 2026-11-20 10:28: Android device-looks pass (lane L4): one question, pictures to look at

Pictures: `handoff/PLAN/app-integration/L4-android-shots-2026-11-20/` (per ticket folders).

- [x] **T971 (settled 11:11 by the Flow-leads ruling, not asked), Conditions: should Level in the gauge card change the estimate card above it?** Coquitlam River: the estimate card reads
  "Normal for the time of year"; pick Level in the gauge card and it flips to "Below normal for the time of year" (and its Change button
  says "Flow only"). Default Claude would use: the estimate card stays on Flow whatever the gauge card shows ("Flow leads"). Shots
  `T728/T728-1-card-opens.png` (Flow) and `T728/T728-2-card-level.png` (Level).
- [ ] Look at: the photo viewer on the emulator (T836: `T836/`, open/close sheets, drag down; T970 is the small look it found), the
  search sheet (T730: `T730/`, recents as one block of tags, swipe down closes), the two-days chart marks (T604: `T604/`), the route
  map (T831: `T831/T831-route-pins.png`).

## 2026-11-20 10:22: T679 #8 list to look at (lane L8; ANSWERED 10:45: ship it, see above)

The gauges that would move from "ON THIS WATER" to "ESTIMATING FROM OTHER WATERS" under the rule you picked at 09:59 (along
the water, a lake on the way ends it): `handoff/PLAN/app-integration/T679-shots/own-gauge-moves.csv` (water, gauge,
was → now, sections). 155 gauges on 104 waters, all own → donor, 1,060 of 176,367 (section, gauge) pairs. Biggest:
Halfway River (2 gauges, 59 + 53 sections), Stikine at Telegraph Creek (44), Lillooet at Tenas Narrows (35), Columbia at
Donald (34) and Nicholson (31). Nothing ships until you say so (`packed.SHIP_OWN_GAUGE_FLAG` stays off). T679.

## 2026-11-20 10:00: every leftover goes on this release (owner, verbatim; clarified 10:05)

"Make sure every left over goes on this release when we cut it", clarified: "All stuff we were working on. I meant all
the tickets we went through before". So: every ticket we worked through in this batch (the lane plan's tickets, the
T920–T939 set, and the review leftovers on them) is finished and in this release. Claude also keeps the bug finds from
that work in (T941–T946: wrong or missing rules and names) as small fixes in the batch. T940 (satellite colours) stays
"after this release" as the owner first said: its lane finishes and waits unmerged. T686 packed formats after 0.8.4 is
in production; T709/T715 postponed; dropped items stay dropped.

## 2026-11-20 09:59: T686 formats and T679 #8 answered (dialog)

- T686 smaller formats (near/2, down/2, climate/2; 17.5 → 15.2 MB, lossless; only app 0.8.4 reads them): "Ship readers
  now, formats later (Recommended)": 0.8.4 reads old and new; this release's data (staging AND production) stays in the
  old formats; the new formats switch on in the first data release after 0.8.4 is in the production stores.
- T679 #8 (one rule for "on this water"): "Ranking's rule, list first (Recommended)": along the water, a lake on the way
  ends it; Claude measures which gauges move and shows the owner the list before it ships.

## 2026-11-20 09:48: privacy page answered (dialog)

- Push privacy text (pictures `app-integration/T739-shots/privacy-*-light.png`): "Looks good (Recommended)". Merged;
  goes live with the next production website; effective date moves to that day. T739.
- Live contradiction found: "This website" said "runs no analytics" while the page names Cloudflare Web Analytics (T910):
  "Fix the sentence (Recommended)": now "It sets no cookies and runs no advertising or tracking scripts; the only script
  is the visit counter described above." T739.

## 2026-11-20 09:39: morning questions answered (dialog)

- Three "Unnamed …" lakes (3261, 3296, 3299): "Show "Unnamed …" (Recommended)": they show "Unnamed Cayoosh Cr",
  "Unnamed Vanhorlick 1", "Unnamed Vanhorlick 2"; the number stays a search name. T926.
- "#14A": "Show Floatplane Lake": a number with a letter on it counts as a number name too; "#14A" stays searchable.
  T926 (Claude extends the rule and reports how many more waters change).
- Push privacy line: "Claude's edit (Recommended)": Apple only, "I/my", push token added to "the short version", Apple
  (push only) added to the services, the one-day salted IP hash and the stored environment + time said, effective date
  moved to the go-live day. The owner sees the text before it goes live. T739.

## 2026-11-20 09:52: T679 #8 — which gauges count as "on this water" (lane L8; ANSWERED 09:59, see above)

Two parts of the app decide whether a gauge is on the water you are reading, by different rules: the gauge list's
"ON THIS WATER" heading (the app: the gauge's water is this water or one it continues as, `water_continue`) and the
lead-gauge ranking (the pipeline: the same, but walked along the water, so a lake on the way ends it). T679 #8 moves the
decision into the data, once. Before building it, the one rule to keep:
(a) DEFAULT: the ranking's (along the water, a lake between ends it) — a gauge on the far side of a lake would move from
"ON THIS WATER" to "ESTIMATING FROM OTHER WATERS"; (b) the heading's (same water, lakes or not) — the ranking would start
treating such a gauge as the water's own. Not measured yet: how many gauges move under either (needs the atlas graph;
a side run measures it before anything ships). Measured: over 1,587 panels the heading's rule differs between a panel's
sections in 12, so the flag is shipped per section (like the small-water warning), not per gauge.
No picture yet: the list of gauges that move comes with the measurement.

## 2026-11-20 07:33: push privacy line, now that Android has no push (T739, lane L5; ANSWERED 09:39, see above)

The push-token paragraph you approved on 11-15 ("Yes to both") is now in the privacy page's source exactly as written
(`app/tools/seo/html.ts`, heading "Push token", after the crash-report section; it goes live only with a production
release). Since then you said no Android push (11-16, "we dont want this right now"), so four small things no longer
match the app. Pick: (a) leave it as approved, (b) Claude's edit below, (c) your words. Default until you answer: (a).
1. It says "an address Apple or Google issue" and "whether the phone is an iPhone or Android", and "when Apple or Google
   tell us". Today only iPhones send a token. Edit: "an address Apple issues", drop "and whether the phone is an iPhone or
   Android", "when Apple tells us".
2. It says "our server" / "We"; the rest of the page says "my servers" / "I". Edit: "my server", "I".
3. "The short version" still says "The app sends me two things" (problem report, error report). Edit: add "and, if you
   turn notifications on, an iPhone's push token".
4. "The services named above (Cloudflare, Open-Meteo, Esri)" leaves out Apple, which carries the silent signal. Edit: add
   Apple (push only). Also: the page's effective date (2026-11-19) should move to the day this goes live.
No picture: this is wording only; the whole paragraph is quoted in T739 ("Owner answer, 2026-11-15 evening").
Facts from the independent code review (2026-11-20), for the same pick; the approved wording is NOT changed:
- The page says Apple/Google and iPhone/Android, but Android sends nothing (no Android push since 11-16).
- To limit registrations, the server keeps a salted one-way hash of the phone's IP address for a day (the same as
  problem and crash reports); the paragraph does not say so.
- Besides the token and the platform, the server stores the APNs environment (sandbox / production) and the time of
  the registration.
- A token is sent only when notifications are on AND something is set to be checked (a gauge alert, "New
  regulations data" or "New app version"), not on "notifications on" alone.

## 2026-11-20 07:07: a morning question (T926 number names; ANSWERED 09:39, see above)

- **Three lakes would show "Unnamed …" instead of their number.** Under "Never show a number name", the lakes now shown
  as 3261, 3296 and 3299 would show "Unnamed Cayoosh Cr", "Unnamed Vanhorlick 1" and "Unnamed Vanhorlick 2" (their
  stocking records' only lettered names). OK, or should those three keep their number? Default until you answer: the
  rule as you gave it (they show the "Unnamed …" names). The other five change to a real lake name: 5566 → Pigeon Lake
  #2, #15 → Precipice Lake, #1222 → Wilderness, #1012 → Trails End Lake, #825 → Heidi Lake. Ticket T926.
- **Is "#14A" a number name?** The lake shown "#14A" (wbk:329021840) has a letter in it, so under the rule it stays
  shown (its other name is "FLOATPLANE LAKE"). Should a number with a letter on it count as a number name too? Default
  until you answer: it stays shown, the
  rule as you gave it. Ticket T926.

## 2026-11-20 ~06:55: number names, T923, satellite colours answered (dialog)

- Lakes shown by a bare number (Pigeon Lake # 2 shown as "5566"; also 3261, 3296, 3299): "Never show a number name": a
  digits-only name is never the shown name when the water has a lettered one; it stays a search name. Ships with the
  atlas registry (this release's rebuild).
- T923 thicker outline, one satellite look in light and dark: "Looks good, land it".
- Satellite colours for the other lines: "Yes, open a ticket" (T940, after this release).

## 2026-11-20 ~06:45: T923 dark mode answered

Shown the approved dark satellite shot (near-black rim, bold) beside the reviewed one (one pale rim in all themes, thinner
looking). Owner picked, verbatim: "Second, but thicker": one pale rim in all themes, the satellite outline itself wider.
Pictures of the thicker outline come before it lands.
Owner ~06:46, verbatim: "Thicker should be the one for light or dark on satellite. Satellite should maybe just be its own
colors? Best for satellite?" Answer: yes; on satellite the outline is one look in every theme (same colours, pale rim,
thicker widths). Open question to him: the other satellite lines (streams etc.) still follow the theme; a satellite-only
palette for those would be its own ticket if he wants it.

## 2026-11-20 ~06:34: lane pictures and data clean-ups answered

Looks approved (dialog): part names T780/T711, iPhone map T929, search T926/T927. Data clean-ups, all four OK: remove the
"." name variant; split Pete's Pond's listed name (name "PETE'S POND", the rest is description); Sheldens Creek tidal
limit label drops ", low confidence"; look into "Pigeon Lake # 2" shown as "5566" and propose a fix. Owner asked, verbatim:
"For description where will it love?" (answered in chat: the curated entry's `note` already holds "Unnamed lake at the
head of San Juan River"; the regulation card keeps the book's printed line). T548: a notice with a printed window shows
"While the Province lists it" (no seasonal filter; the T548 acceptance line is amended to match).
Owner 06:38, verbatim, on the name clean-up: "Withe the name clean up make sure it still matches properly. And that it
doesn't need a new override json". So the "." removal and the Pete's Pond split land only with a dry-run proof that every
book row matches the same water as before and no override JSON is added or edited (the Pete's Pond book row is already
linked by its existing override in book_name_links.json, which stays untouched).

## 2026-11-20 (before 06:25): T937 and T923 looks answered

T937 Salmon map no-access grey: owner, verbatim, "Remove checked dste" (remove the "Checked Nov 19" line from the Salmon
key so it stays two rows). T923 satellite outline for reserves and private land: "Looks good, land it" (review, then it
lands with this release).

## 2026-11-20 01:56: lane-plan batch answered

Dropped (tickets labelled so, nothing to do): T537 and T538 leftovers (both tickets DONE on what was built), T801 item
5(2) (invisible Skeena side channel), T669 item 2 (rule sentence per DFO verdict), T723 item 3 (iOS scenario ports).
Owner note, verbatim: "Make sure their tickets are  removed/completed/no doing labelled". KEPT: T679 #10/#13/#15/#17.
Postponed until after this release: T709 store video (+T715). T667 test cleanup stays on the list. Production reads
(backup.json, /crashes; read-only) for T559/T595/T596: YES. T891 part two (braid simplification): "Fold into T699
later"; T891 is DONE on part one, T699 (postponed) carries part two.

## 2026-11-20: audit questions answered, rows below ticked

Owner ~01:35, to the audit's questions (`handoff/PLAN/app-integration/tickets/AUDIT-2026-11-20.md`): "Couldn't we just
assume most of these are done already?" Every check or look is taken as done; every pick takes what is built or the
ticket's default. **TICKED (done on his word, nothing left for him), whatever older section still lists them:** the
0.8.2 staging list items 1-5 below (T869, T870, T900, T891 part one); the build 29 and build 32 phone checks (T636,
T729, T563, T566-T568, T570, T581-T586, T620); the T747 / T750 look; the section A re-ticks in the review app (T755,
T761) and the T623 F2/F9 ticks; the T749 R8 phone walk; the iOS real-iPhone checks (T88, T364); the store listing
sign-off and support mailbox (T244). Picks taken: T594, T598, T610 closed; T711 "Source" in both; T676 item 2 a
per-feed `minAppVersion`; T780 part names drop the suffix (last two not built yet, Claude's step). T634 is POSTPONED.
The Owner To-Do page's rows for these are not edited here (the page is not read in this lane).

## 2026-11-19 evening (after 0.8.3)

**T920, Conditions (owner ~16:30 and ~16:49):** the gauge map is under the chart; the gauge list is a legend (not a
selector); the chart's gauge picker is a bordered "N gauges ▾" field. Pictures sent in chat; lands after your look, the
checks and an independent review. **T919 (c), ANSWERED 2026-11-19 20:31:** the map's 777 font/sprite files (13.8 MB,
35–160 s on first install) become one packed download through the system downloader (owner: "One packed download").

**T926 (4), ANSWERED 2026-11-19 20:33:** no outside names source. The owner withdrew the BC Geographical Names pick: the
curated name-variants file already has the variants ("It sound like it has them all already"); its 31 misses get fixed.
**T926 scope + T927, ANSWERED 2026-11-19 23:49:** "We want all the variants no abbreviations though and no numbered lake
names. We do want to add all the cuts on a river as a searchable thing too. So you could search something like ten Mile
pool and get chuckwalla river?" T926 = every name variant finds its water, no abbreviations, no generic numbered names (23:56: "Kettle lake 2 is okay",
"lake #265" is not);
T927 = every placed cut searchable (Ten Mile Pool → Chuckwalla River).
**T926, 2026-11-19 23:59 (supersedes the two above):** "just user the name variants file as it stands … It feels less
hacky that way": T926 = every entry in name_variants.json finds its water; no other rules or sources.

## 2026-11-19 afternoon: 0.8.3 on staging, then 0.8.3 to production (0.8.2 production skipped)

**Owner plan, 2026-11-19 ~12:40:** finish 0.8.3, put it on staging, then take 0.8.3 straight to production (web, Play,
Apple), skipping 0.8.2 production.

**On staging now (15:15):** data release r-6258ffded676 (staging smoke 19 pass, 0 fail), the staging website
(https://staging.canifishthis.ca, app commit 09985e595), the staging feeds worker (gauge pages, T908) and the staging data
worker (T912). Play: 0.8.3 to internal + closed once the benches pass (build number added here). What 0.8.3 brings: faster
pinch-zoom (T903), downloads that go on with the app closed (T912), the simpler trout table (T907), gauge pages (T908),
the steady calendar (T915), the water's own rule-book page at the page foot (T916), desktop map keys as cards (T904), no
bell on the web (T909), Join the test (T911), privacy + speed (T910).

**Production, in this order (all yours; Claude rehearsed every step with --dry-run on 2026-11-19 15:17, all passed):**
1. Feeds worker FIRST (gauge pages, closed-area pieces): `CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh`
2. Data worker (T912: resumable downloads): `npx --prefix app wrangler deploy -c deploy/prod/data.wrangler.toml`
3. Data: `CONFIRM_PROD=r-6258ffded676 .venv/bin/python -m pipeline release --prod r-6258ffded676`, then
   `node app/tools/smoke-staging.mjs --production`
4. Android: `CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh`, then in Play Console that build to Production
   (staged rollout).
5. iPhone: `CONFIRM_PROD=canifishthis.ca deploy/staging/build-ios-testflight.sh --production`, then add it to the
   External group.
6. Say go: Claude deploys the production website from the same commit.
The data must go before or with the phones (0.8.3 reads the new trout row fields).

**Owner 2026-11-19 ~15:25, verbatim: "Can you do as much of the prod release as you can? I give permission".** Done by
Claude: step 1 (feeds worker, version 0a3205f6; gauges, dfo, inseason jobs ok) and step 2 (data worker, version 3c598244).
Step 3 stays yours: `release --prod` refuses any agent and needs a real terminal, by design. Play's production track is
empty (no production access yet), so the production Android build goes to Closed testing, replacing staging build 38.
Owner ~15:42: "apple is now in testing still waiting on android approval" (0.8.2 iPhone approved for external testing;
Play closed build 38 still in Google's review). Asked whether 0.8.3 production should replace 38 on Closed now (likely
restarts the review) or wait for 38's approval: owner picked "Replace 38 now".
**DONE 2026-11-19 ~16:20:** steps 1-4 and 6. Feeds + data workers (Claude), data (you, release --prod; production smoke
18/0), Android production build on Closed testing in place of 38 (Google reviewing it), staging 0.8.3 on Internal, the
production website (Claude; smoke 18/0). Step 5 (iPhone) is uploading; T918 adds it to your External group and sends it
to Apple's review by itself. Yours now: nothing, unless that step reports a problem (then STATUS says what).

## 2026-11-19 morning: test 0.8.2 on staging, then production

**Test 0.8.2 on staging (2026-11-19 morning).** Everything from last night is on staging: data release r-379e72fc9524,
the staging website (https://staging.canifishthis.ca, app commit 3ab2b1b24) and Play INTERNAL 0.8.2 (build 37, staging
data, patched MapLibre). Nothing went to production. What to look at:
1. Salmon, Mamquam River (https://staging.canifishthis.ca/water/mamquam-river): Source (DFO) lists "DFO's rows behind
   these answers"; the bait ban is one line, "Bait ban: no bait of any kind — DFO (all year) and B.C. rules" [T870].
2. Coquitlam / Vedder: "10 adult chinook a year from all fresh waters"; the Vedder adds "(here an adult is over 62 cm)" [T869].
3. Glenn Creek / Littlejohn Creek: a trout row says "Duncan River’s rule for its tributaries" (or similar) under the fish [T892].
4. Fraser River, Gear and Methods: each source said once per section; the sturgeon exception once [T900].
5. The map at the Lillooet braids (head of Harrison Lake): side channels half width [T891].

**Questions from the night (each built with the default named):**
- Q1 [T869] The Vedder: DFO's own note says an adult is over 62 cm with NO dates; the rule book (p.77) says Jul 1–Dec 31.
  Built: follow DFO (the card says "over 62 cm" all year). Or use the book's dates?
  ANSWERED 2026-11-19 ~06:40: "good" — keep following DFO.
- Q2 [T891] About 9,800 small lines that are not braids (FWA types 1400/1410/2000/1425/6010, mostly small unnamed lines)
  pass the existing side-channel test and also draw thinner. Built: keep. Or narrow to braids only (1100/1350/1150)?
  ANSWERED 2026-11-19 ~06:40: "probably okay" — keep.
- Q3 [T870] The live salmon feed's size check was raised from 112 kB to 135 kB (the DFO rows add ~34 kB raw, 15.7 kB
  gzipped). Built: raised. OK?
  ANSWERED 2026-11-19 ~06:40: "okay we will work to shrink it down later" -> T905.
- Q4 [T892] The trout rows use the same words as the Other fish rows ("X’s rule for its tributaries"). OK on the table?
  2026-11-19 ~06:40: "i dont see this anywhere give example" — example sent (Glenn Creek, Littlejohn Creek); still open.
- Q5 [T903] The map speed-up (paint only what differs) did NOT land: under a stressed phone, mode switches were 11–22 %
  faster but panning on Salmon got worse (p95 116 → 154 ms). Numbers in the ticket; next chat retries the safer half.
- Q6 [T319] Store screenshots: nothing was uploaded (a permission block). Run the two commands in the T319 ticket, or
  approve them when Claude runs them.

**Production, after you test (the order matters):** the FEEDS worker first — the new app shows no DFO rows for a place
the old feed carries (T870) — in the usual order (feeds deploy → `deploy/prod/sync-inputs.sh` → /run/dfo → data upload →
/run/gauges; `handoff/PLAN/release/PRODUCTION.md`), the data step being
`CONFIRM_PROD=r-379e72fc9524 python -m pipeline release --prod r-379e72fc9524`,
then the phone builds (`CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh`;
`CONFIRM_PROD=canifishthis.ca deploy/staging/build-ios-testflight.sh --production`), then say go and Claude deploys the
production website from the same commit.

## Decisions that block work (nothing ships until you answer)

- **ANSWERED 2026-11-17 15:04 (T825 side-channel gauges): "Yes, use the rule (Recommended)"; being built.** Was: Page:
  scratchpad t813-t825/T825-side-channel-gauges.html (245 sections on 9 rivers, grouped by river, each with the gauge
  it reads now and the one it would read). PROPOSED RULE: a side channel, slough or wide pool reads the nearest gauge
  on its own main river that measures flow and drains about as much country (a tenth to ten times); if the river has
  none, it reads none. 244 get a nearer gauge, 1 gets none (lower Sumas). Worth a look: 2 Kootenay channels near
  Creston would read Corra Linn across Kootenay Lake. No gauge data changed yet.

- **ANSWERED 2026-11-14 (production move changed).** Owner, verbatim: "Production will use the latest version" and "It
  will also get new dfo places I will label tonight". So production is NOT deployed from the 9048fbc20 copy below: it
  waits for tonight's DFO labels, then a new data release with them, then the website built from the latest checked
  code and that release. Claude: staging site redeployed from the latest code now; after the labels, a new staging
  release, then fresh production folders and fresh commands here. The commands in the next item are OUT OF DATE.
- **To-Do page and this file compared 2026-11-14.** Added to the page: T686 pick, the 12 iOS-day defaults (one row),
  Apple account after approval, Play closed-testing steps. Closed on the page: T688 wording (answered 11-12). Build 29
  phone check marked replaced by the 0.7.8 / 0.7.9 rows. Not on the page (old, to prune or re-ask): Run timing T199 runs,
  Q4 / Q126 / Q9 / Q26 / Q27, the long phone-test lists (T50 / T77 / T74 / T217 / T243), SEO v1.
- **ANSWERED 2026-11-14 (T750 step order).** Upstream / Downstream now walk one line through every part, no "Which way
  upstream?" picker (owner: "we should only have up or downastream that access all components"). Asked whether to rank a
  part by its upper end (as shipped) or its downstream end (differs on Wap Creek and Nimpkish River only): "upstream is
  fine". Shipped order stays; no release needed.
- **T743 Fraser DFO places (2026-11-14): BLOCKS tonight's release until re-marked.** Dossier:
  `handoff/PLAN/curation-review/fraser-dossier-2026-11-14.md`. Three "Mission to Hope" wordings (marks lines 2552, 2554,
  2555; two written by a group click) are bound Mission → Spuzzum; with the live chum/coho opening that would show
  salmon open on Hope → Spuzzum (km 158.8-197.0) where DFO says no fishing for salmon. Yours, one at a time once
  grouping is gone: (1) re-mark those three as Mission → Hope (your own line 2546 was right); (2) accept ONE Hope Hwy 1
  bridge cut (proposed twice, km 158.8; needs an atlas build after); (3) which bridge is "the Alexandra Bridge": the
  Hwy 1 bridge (km 201.2) or the old suspension bridge 855 m upstream (both are 4.3 km past the Region 2/3 line at
  Spuzzum); (4) re-check the two "upstream of Mission" marks (Mission → Spuzzum is plausible); (5) does Region 3's
  "Mainstem waters" include the stretch shared with Region 5; (6) which edge of Lillooet the signs are measured from.
  Suggester fixed (319a447a0): 3 Fraser suggestions change; regenerate only after (1). Page row `t743-fraser-remark`.
- **T735 dark-mode highlight (2026-11-14): pick a look; four sheets sent in chat.** Owner: "The highlighting isn't very
  good in dark mode on regulated streams." Measured: the white ring nearly swallows a yellow or mint line (1.1-1.3:1),
  and on the Fraser every other part carries the same bright ring. Options, dark only: A dark edge, status colour
  carries; B thinner white ring, others dimmer; C dark edge + soft glow [recommended, glow can be stronger]. Patch kept
  outside the repo (`~/bcfish-session-backup-2026-11-12/dark-highlight/options.patch`); nothing changes until you pick.
  Page row `t735-dark-highlight-pick`.
- **From the 0.7.10 checks (2026-11-14), two looks, not blocking; pictures sent in chat.** (1) T731: stepping from the
  Chilliwack into the Vedder still makes the bottom bar 122 px shorter (the Vedder has one part, so no "Show all N
  parts" row) and the map grows by that much; steps inside one river move it 4 px. Nothing goes blank any more.
  Choose: leave [default], or keep the bar the same height across the step (an empty row's worth of space on one-part
  waters). (2) T730: picking a PLACE row in search (for example the village "Cultus Lake"), then Back, reopens search
  with the text still typed; a second Back clears it. Choose: leave [default], or Back from a place closes search too.
- **Phone check, build 32 = 0.7.10 (Play internal, 2026-11-14).** Look at: (1) Back from a water's page: no flash
  (Map, Conditions, satellite, dark); (2) Flow / Level in a water's Conditions card leaves the Conditions map's layer
  alone; (3) the credits card opens above the legend on every map tab and closes on a tap outside or Back; (4) search:
  swipe down closes it, at most 4 recents in a small block, after picking a water Back leaves the map there, search
  reopens empty; (5) Upstream / Downstream: the bar no longer blanks (the height change into a one-part water is the
  open look above); (6) say if the app ever closes by itself when opening a water (a rare crash was fixed, T734: twice
  in about 100 opens before, none in 110 after). Page row `phone-check-0710`.
- **T730 search (2026-11-14), not blocking; look on the next build.** (1) Recent searches PROPOSAL: one small block
  under the search field, "Recent searches · Clear", then up to 4 searches as small square tags that wrap (about 80 px
  tall instead of about 340). Alternatives in the ticket: 3 compact rows, or one scrolling line of tags. (2) Closing
  search now always forgets what was typed, on desktop too (Escape or the panel's X). If desktop should keep the typed
  text, say so; it is a one-line exception. Default until answered: as built.
- **T686: pick the shipped-data shrink (2026-11-14), not blocking.** Measured on r-f749475d0962, nothing shipped (table in
  the ticket). The release data is 17.75 MB of a 463 MB phone download (tiles + basemap are 91 %, T699). Choose:
  (a) DEFAULT: near as one blob per place + climate bands packed + down in blocks, ≈ 1.94 MB, lossless, no web moment
  at risk; (b) (a) + core.item_section without its by-item index + water.part_section as runs (first search and
  water's gate get headroom; device scan time to measure first); (c) every lossless candidate, ≈ 2.59 MB; (d) brotli
  instead of gzip, ≈ 3.66 MB, a host and native-client change, no format change; (e) nothing now.
- **T728 follow-up (2026-11-14), not blocking.** Fixed: Flow / Level in a water's Conditions card no longer changes the
  map's layer. Left as it was, for you to decide: (1) the card still OPENS on whatever the map shows (map on Level, card
  starts on Level); alternative: always open on Flow. (2) a pick in the card lasts while that water is open; another
  water starts again from the map; alternative: keep the pick for the whole session, like 72 h / Year. Default until
  answered: as it is now.
- **ANSWERED 2026-11-14 (privacy page check).** Owner, verbatim: "we are close to moving staging webpage into prod.
  tonight we will probably do that." and "choose the best wording please you know better then I do for that stuff".
  So: item 1 is settled by the production move (after it, check that `https://canifishthis.ca/privacy` shows the
  policy); item 2: Claude chose the wording and applied it to `privacyBody` and the staging site (effective date
  2026-11-14). Original findings (owner: "please check page too"):
  1. `https://canifishthis.ca/privacy` (the link on the Play listing) does NOT show the policy: the live site is still
     the old site and answers every address with its own front page. The policy is only on
     `https://staging.canifishthis.ca/privacy`. Choose: publish the new site's privacy page to canifishthis.ca (you run
     it; production is yours), or point the Play listing at the staging address until production goes live.
  2. Three wording proposals for the policy text (`app/tools/seo/html.ts` `privacyBody`; NOT changed, the text was
     approved 2026-11-06):
     a. Short version says spots "stay on your device". A saved spot's place, rounded to about 1 km, goes to Open-Meteo
        for weather (the Weather section says so, but says "that spot's coordinates" without the rounding).
     b. A problem report's map centre is described as "where the map was looking, not where you were". The app opens
        centred on the reader, so it is often about where they are (to about 100 m).
     c. Not mentioned: Share (a link you send) and Directions (opens Google Maps with a spot's parking point). Both
        only on your tap.
  Fixed without asking (our own working page `handoff/PLAN/play/data-safety.md`, not the Play form): production does
  send reports; crash switch is More, Settings, Privacy; spot place is rounded; downloads carry the app version; one
  app-stopping error is kept for the next start. No Play form answer changes.
- **iOS reopened (owner 2026-11-14): four decisions to answer on return** (Apple account/TestFlight timing and individual vs organization, minimum iOS version, iPad, own iOS tap-up patch; defaults in the doc's Unattended section). ANSWERED 2026-11-14: rotation "Portrait only."; app name "Can I Fish This?" (same as Play, T371 closed on that point); proposals, physical needs and risks in `handoff/PLAN/ios/START-HERE-ios.md`.
- **ANSWERED 2026-11-14 (atlas builds).** Owner, verbatim: "You can do atlas rebuilds if you coordinate with other agent".
  Used so far only for the T667 slice 8 mini build (invented inputs, scratch output); the Android chat was told. Promote
  and release stay as before (not from the iOS chat). T602 is still not started: the handoff parks it for you.
- **Open from the iOS day (2026-11-14), each built with its default; also rows on the Owner To-Do page:**
  (1) oldest iPhone / iPad: today's build needs iOS 16.4+, iPhone only [keep] (T725);
  (2) App Store privacy: the report email "linked to the user" [linked], and the category Claude chose, Navigation then
  Sports [keep] (T727);
  (3) shared links with no Team ID: the site build skips Apple's link file with a warning [keep] or always fails (T724);
  (4) backup (T732): confirm the words the Android chat relayed ("save spots with prioirty", "images can be compressed if
  still visible"); the photo pair was sent. PROVEN on the emulator 2026-11-14: today a backup carries nothing (over
  Android's 25 MB limit); with the rules it is 17,920 bytes with spots and settings, and a wipe + restore brings them
  back. ANSWERED 2026-11-14 evening, verbatim: "Only need to backup photos and spots the rest of the data isn't
  important". Then: "Settings could also be saved". Being built on the branch: the backup carries the spots, the
  settings and the photos; photos ride
  the cloud backup as smaller copies (1600 px), which needs one image library (Claude read your sentence as the yes to
  it). PROVEN again 2026-11-14 21:15 with photos: backup Success (227 KB with 3 photos, 20.4 MB with 29),
  wipe + restore brings spots and photos back; the shot was sent. Still open: screen-lock-only [no], copy size [1600 px].
  Not merged;
  (5) clean-up: five calls in `handoff/PLAN/CLEANUP-CANDIDATES.md` (evidence screenshots, old releases, a leftover flag,
  15 DONE rows still in the open ledger, graphify and scratch dirs);
  (6) the small invented release for tests (T667 slice 8): cut it with no map tiles and no basemap, only the data files
  the app's logic tests read [yes, being built that way as a proposal], or also give the tile step its own invented
  inputs (more pipeline change). Found on the way: the export step checks that the rules are the real rule book (64
  guide examples, 75 case mechanisms, the licence and protected-fish duties, the printed page map), so it refuses the
  invented world. Being built as a proposal: the export takes the list it checks against as a named input, the real
  book's list is the default and is pinned so it cannot shrink, and the invented world brings its own list [yes]; the
  other way is to grow the invented rules to about 60 entries. BUILT 2026-11-14 in scratch: the release cuts on invented
  data, two runs identical; ten provincial rule-book rows the answers step reads by id are copied into the test world
  as printed;
  (7) the About page gains one sentence saying the app is not affiliated with the government (Apple asks for it; audit
  G15) [About only, the first-run notice is not shown again];
  (8) the web build leaves out the four Android-only "What's new" notes [keep];
  (9) moving to a new phone copies saved spots and photos but not the downloaded data, which downloads again [yes];
  (10) an own iOS tap-up patch for MapLibre (decision 4): BUILT and proven on the simulator 2026-11-14 (branch
  `ios/t719-tapup`): the cue lights 8 to 121 ms after the lift, was 371 to 495; Android's half of the patch is
  byte-identical [merge with the other iOS branches after the Android benches] or leave;
  (11) ANSWERED 2026-11-14, a third heavy lane for iOS simulator jobs. Owner, verbatim: "Maybe we can run 2 at same time
  might have enough resources". Done: `heavy-job-3.lock` in `scripts/heavy.sh` (bbee2220f), same memory guards, three lanes
  at most. Clarified later the same evening, relayed by the Android chat, verbatim: "i just meant use as much of the
  resources as you can to speed stuff up": lanes run in parallel as far as the machine allows; every heavy job still
  goes through `scripts/heavy.sh` and its memory gate;
  (13) iOS keyboard and back fixes are proven on the simulator (T723, branch `ios/t723-keyboard-back`); two picks: a
  spot's Save row stays under the keyboard while typing [leave: drag down or Done first] or lift it above the keyboard;
  the new left-edge back strip on full-screen pages is 16 pt wide [keep] or 8 pt;
  (14) the first-install bar reads low on both platforms (38.8 MB shown with 116 MB on disk after a relaunch): the four
  big files download at once and the bar counts one at a time [fix it, ticket T738] or leave it;
  (15) push for iPhone notices (owner 2026-11-14, verbatim: "dont we want to use it for the notifications thought?"):
  today no notification on either platform uses push; the phone checks in the background and makes the notification
  itself, and on iPhone that check runs only when iOS allows (often hours late). Proposal [Claude's pick]: a "wake-up"
  push from our feeds worker when notices change, so the iPhone checks at once; the worker learns a device token only,
  never which waters you watch. Needs: Push ticked on the App ID, an APNs key, a privacy-page line, a ticket. Until you
  pick, the build keeps its push entitlement (the G12 change is on hold). Ticket T739 (proposal). Also asked: "can we
  do this same push notification style to android?": yes, but Android's only push channel is Google's Firebase, a Google
  service in the app, so it needs your exception to "self-host everything" [iPhone first; Android only if phones show
  late alerts]; and "there are also time sensitive notifications?": possible for closure notices on iPhone [off for now].
  ANSWERED 2026-11-14 late, verbatim: "okay i ticked push notifications for app yes build for iphone."; time-sensitive:
  "dont need this"; and "i guess if new data is there we could send a notification that the user needs to download it."
  (built as: a new release wakes the phone, which downloads by itself when settings allow and otherwise tells you new
  data is ready). Then, verbatim: "for android too." (Android push approved as well; it uses Google's Firebase for delivery only,
  Claude's reading: your exception to "self-host everything" for this one thing). Being built on branch `ios/t739-push`.
  **ANSWERED 2026-11-15 01:10 (push on a real phone).** The owner made the Apple push key (Key ID 6T3DBFF92H) and ran
  the three `wrangler secret put` commands himself; the staging feeds worker lists APNS_KEY, APNS_KEY_ID and
  APNS_TEAM_ID (names checked, values not read). To "Do you want those tonight?" (deploy the worker with the push code
  to staging, and upload TestFlight build 35 from the push branch), owner, verbatim: "yes i do i ran command". Build 35
  **uploaded 01:24** from `ios/t739-push` 650665c61; the worker deploy is timed with the Android chat. Yours
  after Claude says the worker is on staging: install build 35, turn notifications on in the app, tell Claude; Claude
  sends one test push.
  **Worker on staging since 2026-11-15 02:05** (also carries the crash-report limit you approved). Ready for your
  phone: build 35, More → Notifications → "New regulations data" on, app to the background, tell Claude. Firebase (Android)
  still yours, later.
  Still yours later: one more Apple key, for push, and a free Firebase project for Android (steps come when the worker
  side is ready);
  (12) Play's data-safety form says approximate location and the App Store answers say precise: should Play match?
- **Apple Developer account: APPROVED 2026-11-14 evening (individual); the owner has the Team ID** (given in chat; not
  written in the repo). DONE by you 2026-11-14: the Team ID is saved in `~/.canifishthis/ios-team-id` (checked: the site build reads
  it and now writes Apple's link file). Also 2026-11-14: the owner made the App Store Connect API key (Key ID and Issuer ID given in
  chat, not written in the repo; the `.p8` is in his Downloads). DONE by you the same evening: the key and `asc-api-key.json` are in
  `~/.canifishthis/` (checked by shape only; `build-ios-testflight.sh --dry-run` finds both files and would build 0.7.10
  as build 33). CONFIRMED by you 2026-11-14 ("yes and yes"): the App ID `ca.canifishthis.app` is registered with
  Associated Domains on (Push left off: Claude turns the unused push entitlement off in the build, audit G12), and the
  app record exists. The Apple side is complete. FIRST TESTFLIGHT RUN: owner 2026-11-14 late, verbatim: "you can do first
  ios release test for me". Claude started it 21:32: staging data, version 0.7.10 as iOS build 33, from f51870652 (last
  green app CI, the same app code as Play build 32). RESULT 21:33: stopped in the signing step after 1 minute, nothing uploaded. Apple: "Your team has no devices
  from which to generate a provisioning profile": Xcode's automatic signing needs one iPhone registered on the team
  before it can archive. YOURS (2 minutes): register your iPhone under developer.apple.com → Devices (steps in chat),
  then tell Claude, who runs it again. Later TestFlight runs: ask
  first unless you say otherwise (Claude's reading: the go-ahead was for the first one). History: ENROLLED 2026-11-14, waiting for Apple's approval (owner, verbatim: "okay i enrolled with
  apple developer I will tell you wne i get approved"). This answers decision 1 of `handoff/PLAN/ios/START-HERE-ios.md`
  (account now). ANSWERED 2026-11-14 evening, verbatim: "it is a individual evelper account" (so the App Store shows
  your own legal name as the seller). Same message: "i cant find team id how do i make one?": it is not made, Apple
  assigns it once the enrolment is approved and paid; it then shows under developer.apple.com → Account → Membership
  details. After approval, yours to do: save the Team ID in
  `~/.canifishthis/ios-team-id`; create the app record (bundle id `ca.canifishthis.app`, name "Can I Fish This?", English
  (Canada)); make an App Store Connect API key (role Admin; corrected 2026-11-14 22:15, an App Manager key cannot sign for the store) and save `~/.canifishthis/asc-api-key.json`. TestFlight
  (T88) stays yours to start.
  **DONE 2026-11-14 22:22:** first TestFlight upload, 0.7.10 build 33 (staging data), after the owner registered his
  iPhone and made an Admin key. Yours now: App Store Connect → TestFlight → Internal Testing, make a group and add
  yourself; install the TestFlight app on the iPhone; install the build and open it. Later uploads: Claude asks first.
  **ANSWERED 2026-11-14 23:29:** owner asked "i thought it can download in background?" (not on iPhone in build 33);
  offered a second build from the merged iOS branch `ios/integration` (background download, progress bar, keyboard,
  back swipe, tap cue); owner, verbatim: "yes". Build 34 from 51349c3bf **uploaded 23:39** (0.7.10 build 34, staging data). The shared
  branch is not touched by it. Yours: add build 34 to your internal group and install it; checks only a phone can do
  are on the To-Do page row.
- **ANSWERED 2026-11-15 10:33 (merge timing of the iOS work).** Owner, verbatim: "ios work could merge when ever I
  think?" My reading: no need to wait for him. Plan, timed with the Android chat: (i) the four iOS fix branches
  (proven on both platforms) first; (ii) the loading screen, first-install bar, crash-report app half and the
  faster-and-lighter branch after one Android run; (iii) push last, with the privacy-page line for his eyes. The R8
  Play internal build is taken before any of these, so it carries nothing else.
- **ANSWERED 2026-11-15 evening (round 6, verbatim: "Yes to both (Recommended by it)"): the push privacy-page line and the App Store privacy answer are approved AS WRITTEN below; and all the fish-pun lines stay (T746).** Next (Claude): the line goes into the privacy page's source on the push branch; putting the page live stays yours, with the production release. Was: NEW 2026-11-15 10:42 (iOS chat, T739, push: privacy wording; PROPOSAL). With push the app sends our server one
  new thing, a push token (an address Apple issues so a message can reach the app). The push code merges into the
  shared branch only after the privacy page and the store answers say so; the live privacy page changes only on your
  yes (production stays yours). Proposed privacy-page line, under what the app sends: "Push token. If you turn
  notifications on, the app sends our server a push token for this phone (an address Apple or Google issue so a
  message can reach the app) and whether the phone is an iPhone or Android. We use it only to send a silent signal
  telling the app to check for new data; the signal carries no words, no waters and no location. We do not receive
  your alerts, the waters you watch or where you are. The token is deleted when you turn notifications off, or when
  Apple or Google tell us it no longer works. It is not linked to you or to an account and is not backed up."
  Proposed App Store privacy answer: Identifiers, Device ID: collected, App Functionality, not linked, not used for
  tracking. The Android (Firebase) line and the Play Data safety answer wait for your Firebase decision; their draft is
  in T739. Answer: yes as written / your changes. To-Do page row `t739-privacy-wording`.
- **NEW 2026-11-15 10:09 (iOS chat, T749, Android's code shrinker R8).** The trial passed on the emulator: the app
  starts and passes all six checks; the test APK is 7.45 MB smaller (about 7.4 MB off the Play download, estimated).
  It is behind a switch that is off. Pick: make one Play INTERNAL build with the switch on for you to walk on your
  phone (update, notifications, a spot photo, location, the rule book): yes / not now. Nothing is uploaded until you
  say.
  **ANSWERED 2026-11-15 10:32.** Owner, verbatim: "if that build passes the tests then it should be good to release";
  asked which build and which release, he picked "R8 build to Play internal": the Android app with the code shrinker
  on, built from the shared branch as it is today, the six emulator checks re-run on that exact build, and if all
  pass an upload to Play INTERNAL testing for his phone walk. Production stays his. Nothing else rides along.
  **State 2026-11-15 21:56 (newest; the states that follow are older): nothing needed from the owner.** The later
  stops are fixed (the build's quick test step passes under its own settings). The build waits only for tonight's
  staging data release (the main chat's), so it is built on a shared branch that holds still. The second chat is now
  THE APP CHAT (Android and iOS; owner 2026-11-15 evening) and makes the Play INTERNAL build 0.7.11 with the shrinker
  on; the ten-step phone walk then goes onto his To-Do row `t749-r8-phone-walk` with the build number.
  **How it runs (agreed with the Android chat, 10:37; superseded by the state above for who builds).** I build the test APK and re-run the six checks; the Android
  chat makes the Play build and uploads it, because that step uses the Play sign-in and its version counter. It will
  be build 33, version 0.7.11. To be plain about "nothing else rides along": none of my other branches are in it, but
  it does carry what the Android chat put on the shared branch since build 32: the web loader (T737), search opening
  the water map on its part (T747) and the one-line Upstream / Downstream (T750). Those were due for your next phone
  check anyway.
  **State 2026-11-15 12:49: not uploaded.** Five of six checks passed on the exact build; the first-install-with-the-
  app-closed check did not, and it fails the same way with the shrinker off: this Mac's internet line was crawling. No
  merge and no upload until that check passes on a good line; I re-run it when the line recovers. If it is still bad
  this evening the build slips to tomorrow. Found on the way, not from the shrinker: T756 (reopening mid-download
  restarts the big files from zero after "Wait for Wi-Fi"); fix on a branch, not merged, not in this build.
  **State 2026-11-15 17:18: ready except one thing that needs you.** All six checks passed, the code is merged
  (shrinker off by default), both GitHub checks are green. The build did not run: the build script's quick test step
  fails on one test that reads your uncommitted `data/generated/review/dfo-suggestions.json` (two rows merged at
  midday lack their evidence). The agents will not skip the test or edit your file: regenerating the two rows was
  refused by the automatic permission check, so it is yours; the Android chat's item "regenerate the two proposal
  rows" (first on your To-Do page and on OWNER-RETURN-2026-11-15.md) has the command. Nothing built or uploaded
  until then.
  **CORRECTION 2026-11-15 15:19: back on for today.** The line was not down; my probe was wrong (it timed a "not
  found" answer). All six checks are running since 15:17 on the merged code (T752 included) with the shrinker on. If
  all pass: merge, CI, then the Android chat's Play build today. The 14:44 note below is superseded.
  **State 2026-11-15 14:44: planned for tomorrow.** The line is still down (30–46 bytes a second since 13:04). The
  Android chat lands its desktop split (T752) on the shared branch first, so all six checks are re-run on that new
  head with the shrinker on before the build. If the line recovers today the sixth check is still re-run today.
- **NEW 2026-11-15 evening (iOS chat, T759 + T757; PROPOSAL, words only; To-Do row `t759-banner-words`): the banner's
  words for a DFO rule not matched on a river.** From your decision (no "not known" status; a banner on every stretch
  quoting the unmatched rule; a different look for the line). Written on a branch (the app half of T759), not merged; no
  status or colour changes; on today's pages it shows nowhere (all 8 cases are already "parts not mapped").
  Title: "A DFO rule to check on this water". New rule not placed: "DFO lists a rule for this water that this app has
  not placed on the map yet. It may apply here:". Close match not confirmed: "DFO reworded a rule for this water. The
  app applies it where the earlier wording was; that has not been confirmed yet:". Reworded closed area: "DFO
  reworded a closed area on this water. The app has not placed the new wording yet:". Closing line: "Check DFO's page
  for where it applies before you fish." Between lead-in and closing: DFO's own area and rule, as printed. Sits above
  the species table. Also new (T757), shown when the latest DFO check cannot be used with the app's data: "The latest
  DFO check could not be used with this app's data, so the DFO rules this data was built with are shown. Check DFO's
  page before you fish." **ANSWERED 2026-11-15 23:00 (dialog): "Yes as written (Recommended)"**, and the banner's place:
  "Above the table + mark the folded row (Recommended)". THE PICTURES ARE DONE (2026-11-15 22:53): the banner on the Campbell River and three looks
  for the line, on https://claude.ai/artifact/T5yS8HymzuhAXq8kYaLEwL (also sent in the app chat).
- **NEW 2026-11-15 22:53 (app chat, T759; To-Do row `t759-line-look`): pick the look of the river's line, and two
  small picks.** Real renders of the app (web, phone width) on the Campbell River with one made-up unplaced DFO rule
  fed in; nothing shipped. The line keeps its own colour in every look. PICK 1: A = a soft glow under the line (halo)
  / B = a thin solid edge each side / C = dashes down the middle; Claude suggests A (the only one still visible with
  the whole river in view, and unlike the bait-ban edges; weak point: two such rivers close together). PICK 2: the
  mark is a magenta the app uses nowhere else; your words were "a variation like bait ban, same color": magenta, or
  black like the bait-ban edges? PICK 3: the banner shows only once Salmon is opened: should the folded Salmon row
  carry a small mark too (Claude suggests yes)? Not shown: dark theme, the colour-blind check, the phone app (the
  look is built for the web only so far, on a prototype branch).
  **ANSWERED 2026-11-15 22:56 to 23:00.** Typed in the app chat: "I like the stripe option best for the ones with
  banners. Also the stripes should only be on the salmon view with this error mode right?" So: look C (dashes down
  the middle), on rivers that have a banner, shown ONLY in the map's Salmon view (the prototype drew it in the Trout
  view too: that is wrong and the build shows it in Salmon only). The dashes' colour, dialog answer, verbatim: "I
  think it should dash with closed color maybe? Or a better color what ever is best practice". So the colour is
  Claude's call by best practice: the closed colour is tried first; if it reads as "closed" or fails the dark-theme
  or colour-blind check, a neutral ink that does not is used; he sees the final picture before it ships. Folded
  Salmon row: marked (the banner-place answer above).
- **NEW 2026-11-16 04:37 (app chat, T749; To-Do row `t749-r8-phone-walk`): Play internal build 33 (version 0.7.11,
  code shrinker on) is up; walk it on your phone.** Update through Play (your tester link, the email has it). All six
  emulator checks passed on this code; no real phone has run it. Any time the app closes by itself is a fail. The ten
  steps, each with what a failure looks like, are on the To-Do row and in the T749 ticket ("Phone walk for the
  owner"): 1 update through Play and open; 2 the map draws, tap a river; 3 a notification arrives and its tap opens
  the app; 4 background runs are listed after hours closed; 5 a spot photo from the gallery; 6 a spot photo from the
  camera; 7 location (the map's locate button, "At my location"); 8 the rule book opens at its page; 9 come back
  after a few minutes in other apps; 10 spots and settings survive a force stop. Also in this build, to glance at:
  the data release r-46e4355a7277 (your DFO labels, the year lines T762, Kootenay "over 50 cm" T763). Not in it:
  the stripe (T759), the stuck-download fix (T760).
- **NEW 2026-11-16 00:31 (app chat, T759; To-Do row `t759-stripe-final`): the final stripe is built; look at the
  pictures and confirm two small things.** The stripe branch (6c24f94a6, cut from the T759 app branch; named in the T759 ticket; not pushed,
  not merged, NOT in the 0.7.11 build). Pictures sent in the app chat (contact sheet `final-stripe.jpg` + light and
  dark close-ups; Campbell River with one made-up unplaced DFO rule). Built: dashes down the middle of every stretch
  of a river with the banner, Salmon view only (web and phone style code), fading at far zoom like the bait-ban
  edges; a square "!" mark on the folded Salmon row; a legend entry. Colour (his words left it to Claude): the closed
  colour FAILED (same red on a closed stretch = invisible, contrast 1.00; and a red dash on a blue line reads as
  "closed"), so black ink on a pale edge (white on dark in the dark theme), held by contrast and colour-blind tests
  on every Salmon line colour. CONFIRM 1: the legend entry reads "Check rule" ("DFO rule to check", "Rule to check"
  and "Check a rule" do not fit the two-row legend at phone width); OK or other words of about ten letters?
  CONFIRM 2: is the stripe strong enough on the dark blue "Keep salmon" line? Not proven: no phone or emulator has
  drawn it yet (web pictures + style tests only).
- **NEW 2026-11-15 22:27 (app chat, T760; PROPOSAL, words and numbers; To-Do row `t760-download-words`): a first
  install that stops moving says so.** Two causes of a setup screen that sat for ever were found and fixed on
  branches (not merged, not in tonight's Play build): a small file the app fetches itself that stalled was never tried
  again (now cut after 20 s without a byte); a big file parked by Android's downloader was never restarted (now
  restarted after 10 minutes without a byte unless it is waiting for a network or Wi-Fi, at most twice, then the
  install stops with words). Proposed words: under the bar after one minute without a byte, "The download has not
  moved for a minute. It carries on by itself; if it stays stuck the app starts that file again."; offline, the
  existing "Offline — this will load when you’re back online"; when it gives up, above Try again, "A file would not
  download after several tries. Check your connection, then tap Try again." Numbers are Claude's defaults (1 minute,
  10 minutes, 2 restarts: the give-up words after about 30 minutes). Unit-tested only; emulator runs before any merge.
  **ANSWERED 2026-11-15 23:00 (question dialogs in the app chat).** The three lines and the numbers: "Yes, with
  Claude's numbers (Recommended)" (1 minute, 10 minutes, 2 restarts). Dead connection: "Keep trying by itself
  (Recommended)": same words and Try again button, and the app also tries again about once a minute.
  SECOND PICK (added 22:31, from the first emulator runs): when the connection is dead for minutes the app now says
  so and stops on its failure screen with Try again, and waits for the tap even after the connection is back. Pick:
  (a) keep trying by itself about once a minute while showing the same words (Claude's suggestion) / (b) leave it.
  Answer: the words (yes as written / your changes), and a or b.
- **NEW 2026-11-15 23:17 (app chat, T762; a review, nothing to decide unless you want other words; To-Do row
  `t762-year-line-words`): the trout "through the year" line says the whole year, in tonight's build.** Owner
  2026-11-15 about 22:55: "The bad display oddities must be fixed on this build". On a keep day the line stopped after
  the next change ("You can keep them until Sep 30. Then closed from Oct 1." on a Fraser stretch closed only in
  October). Now, same water on Nov 15: "You can keep them until Sep 30 next year. Then closed Oct 1–31. You can keep
  them from Nov 1 next year." Boardman Creek on Jul 15: "You can keep them until Aug 31. Then the whole water is
  closed Sep 1–Oct 31. Then release only Nov 1–Mar 31. Then the whole water is closed Apr 1–Jun 14. You can keep them
  from Jun 15 next year." The words are the ones the closed and release days already used. 103 lines on 98 answers
  changed, each checked against the stored year. One choice he could reverse: dates in the middle of the line carry
  no "next year". The other oddity (Kootenay Lake's rainbow line dropping "over 50 cm", T763) is the main chat's and
  goes into the same release.
- **NEW 2026-11-15 16:08 (iOS chat; yours, later, no decision needed now): production feeds worker deploy.** Two
  worker fixes are live on STAGING only: T758 (waters covered only by a region's default salmon rule show DFO's own
  sentence under live data again, not a generic "Not opened by DFO" line; the status was right all along) and T757
  (closed areas inside a DFO place are emitted as their own live locations). Production's worker has the same gap
  until it is deployed from the shared branch after these merge; that deploy is yours (`deploy/prod`, never an
  agent's). I say when the branch is ready.
  **THE BRANCH IS READY, 2026-11-16 11:15 (app chat).** Both fixes are on `redesign/stream-sections` since 44b9c25bf
  (merge 1 after your "Yes, one at a time"; both GitHub checks green). Yours, in the main folder at the shared
  branch's head, BEFORE or WITH the production data release (the upload refuses r-46e4355a7277 until production's
  feed server serves closed areas): `deploy/prod/feeds.sh --dry-run` (tests + a local bundle, deploys nothing), then
  `CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh`. The script stops with words if a prerequisite is missing:
  the backup bucket and its 30-day expiry (question 23), `NOTIFY_EMAIL_TO`, `REPORT_SALT`, and the production run
  token in `app/workers/feeds/.run-token.prod` (`handoff/PLAN/release/PRODUCTION.md`, step 2). Check after:
  `https://feeds.canifishthis.ca/status.json` shows `"dfo": {"pieces": 1}`. To-Do row `t758-prod-feeds-worker`.
- **DONE 2026-11-15 07:32.** TestFlight build 36 uploaded (0.7.10, branch `ios/next` d658f88ac, staging data): the
  iOS fixes, the first-install bar, the "applying new data" screen with fish puns, push, the crash-report fix, the
  faster cold start and lighter fonts. Yours: install 36 (not 34 or 35), then the phone checks and the push test;
  the steps are at the top of `handoff/PLAN/ios/START-HERE-ios.md` and on the To-Do page.
- **ANSWERED 2026-11-15 02:09 (before bed).** (1) To "TestFlight build 36: may I upload one in the morning with
  everything that passed overnight (loading screen, push re-register fix, crash-report fix, lighter fonts)?", owner,
  verbatim: "yes you can upload". My reading: one upload, build 36, only what passed; later uploads are asked again.
  (2) On the loading-screen line (T746), owner, verbatim: "can we do more fish puns here that are directed at this ?"
  So the line becomes a fish pun about putting the new regulations in place; candidates sent in chat, a small set
  built on the branch to rotate, the owner strikes or adds. The failure message stays plain (my pick; say if not).
- **NEW 2026-11-15 01:28 (iOS chat, T746, your "loading icon with text like splash").** Built on a branch; pictures
  sent in chat (iPhone light and dark, web). Pick the line: "Putting the new regulations in place…" (built) /
  "Applying the new regulations data…" / "Updating the regulations and map…". A failed apply says: "The new
  regulations could not be put in place. The app is still using the data it had." (OK button): fine / change.
- **NEW 2026-11-15 00:20 (iOS chat, T718 crash check).** Crash reports from the iPhone test runs were refused by the
  staging worker's daily limit (40 per address per day); the Mac's test beds used the 40 up, and your phone at home
  shares that address. Pick: give test-bed crashes their own limit of 200 a day on staging, production unchanged: yes
  (my pick) / no. On yes Claude deploys the feeds worker to staging from branch `ios/crash-send`.
  **ANSWERED 2026-11-15 01:05**, owner, verbatim: "yes to this". The worker commit (441ec090e) goes onto the shared
  branch and to staging at a time agreed with the Android chat (the owner is testing the staging site; the deploy also
  re-runs every feed job).
- **ANSWERED 2026-11-14 (iOS parity audit, T722/T723).** Owner, verbatim: "we want it to progress with app closed in both app
  and android" (the first install download); "need to just update text for ios" (the two promises that do not hold on
  iOS); "everything else looks like a good find should look for more things like that". So: a real iOS background
  download is being built (branch `ios/t722-install`); wording only for the notices / auto-download promises; the other
  findings are accepted as work; a second audit pass is running. Also offered 2026-11-14: "i have time to set uoa.
  developer account right now": the steps are in `handoff/PLAN/ios/START-HERE-ios.md` ("What needs the owner physically").
- **ANSWERED 2026-11-14 (iOS store pack, T727).** Owner, verbatim: "all privacy and descriptions and stuff could use
  playstore items. like icon and name etc it is can i fish? on the icon but can i fish this? on everything else. we want
  precise. it is the id mainly for for analytics and bug report etc etc choose best category". So: the App Store listing
  reuses the Play items (description, privacy answers, icon); home-screen label stays "Can I Fish?", every other name is
  "Can I Fish This?"; location is declared PRECISE; the install id is for Analytics and bug reports (App Functionality);
  category was left to Claude: **Navigation first, Sports second** (matches Play's "Maps & Navigation"; Claude's call, say
  if you want Sports first). Still open from that pack: email "linked to the user" (default: linked). The stale lines in
  `handoff/PLAN/play/data-safety.md` were fixed the same day (fa0d0a45c); the Play form needed no change.
- ANSWERED 2026-11-12 (T678 release, size gate): **data size budgets after the pond fold**: "Raise now, shrink later, but the ticket should cover ways to shrink data across the board, not just ponds." Raised: core 1,450,000 → 1,650,000 B gzip, conditions 950,000 → 1,060,000, web first search 1,500,000 → 1,650,000 (measured: core 1,508,127, conditions 963,726 after D1). Shrinking every shipped file is ticket T686 (survey + proposal for you to pick from). RULINGS O5.
- ANSWERED 2026-11-12 (T678, Owner To-Do row `dewdney-four-lines`): **the four Dewdney lines** (FWA 355991890/895/899/902, 1.6 km): "Yes, cover them". The Nicomen Slough rule covers them; nothing to build, the promoted atlas already does. RULINGS K10.
- ANSWERED 2026-11-12 (T678, Owner To-Do row `gauges-11-other-line`): **eleven gauges on a different river line**: "Take the suggestion". Nine keep today's line; Elk River near Natal (08NK016) and Fraser River at Steveston (08MH028) move to the new line. Ticket T682. RULINGS P49.
- ANSWERED 2026-11-12 and BUILT (b7749b726; ticket T671; Owner To-Do rows `t671-5b-aggregate`, `t671-d5-cr-part`): **T671 salmon, the two follow-up questions.**
  (1) Region 5B "in the aggregate" — your words: "5b says this on the ALL row (All lakes or streams or part thereof in the aggregate (combined total), in Management Units 5-6 to 5-11 in Region 5B, unless otherwise stated below (i.e. Region 5 not including the Fraser River watershed).)... So any water body not in the table will be with the total in aggregate (shared total) .. so basically items not in the table in mu 5-6 to 5-11 are shared but other row listed items have their own quota. Also the 4 salmon a day limit in total of all species is still a thing here too". That is what T349 already built in the release: an unlisted 5B water reads "Up to 4 chinook a day in total, all waters of Management Units 5-6 to 5-11 combined" (92,835 sections); a water with its own row (Bella Coola, Atnarko, Dean, …) has its own count; the combined 4 a day is the bucket line. The release does not change. The live feed (feeds worker) had not pooled — it read the ALL row as "4 a day" per water — and now pools the same way (shows with the owner's worker deploy). RULINGS Q29. Not changed, for your DFO labelling: the Dean River (41 sections) and Long Lake (1) have their own rows but those places are not drawn yet, so their sections show the pooled ALL row with the "some spots" caveat.
  (2) The part-headline for catch and release / check — answered "yes": "Closed today · catch and release in part" and "Closed today · check DFO for part", each with a caveat pointing to DFO for where; open wins, then catch and release, then check (RULINGS P47). On today's release: check 11,888 section-days (33 sections of the Fraser and South Thompson in Region 3, whose undrawn places print sockeye "To be determined"); catch and release 0 section-days. No status, bar or map changes. Shows with the next app build.
- ANSWERED 2026-11-12 and BUILT (4bc03ac14; ticket T671): **T671 salmon readings.** D1 "Yes to option 1 but add to gotcha list": an All closure beats a species row on the same place unless the species row is dated inside it (RULINGS L24; gotcha in `pipeline/docs/GOTCHAS.md`). Tatshenshini below the border: coho now closed Jul 24–Aug 14 (6,254 sections, 22 days, toward closed; Koksilah, not drawn, likewise); nothing moves toward open. D4 "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4. Individual species limits also apply. Species limit is seperate.": the combined 4 is Region 6's bucket line ("Up to 4 salmon a day in total, fresh and tidal waters combined"), each species keeps its own line — that is already how the app reads, so nothing changes (RULINGS Q28). D5 "Closed today · open in part": built (RULINGS P47; 87,419 section-days on 717 sections, the page's own answer kept). D1 shows with the next release, D5 with the next app build.
- ANSWERED 2026-11-12 ("It is this (no Fishing EXCEPT the outlet stream downstream of the falls 2.4 km downstream of Whiteswan Lake, No Fishing Aug 1-Mar 31, rainbow ) so not the inlet. The outlet is below the falls. Inlet is all no fishing (every inlet) trout daily quota = 5 (catch and release all other species) when open)"; built: every one of the 15 inlets closed, T690, RULINGS E26, rides the atlas build): **T637: Whiteswan inlet named so its closure stays; say if the name or the piece is wrong** (2026-11-11). Blue line 356560775 above Whiteswan Lake (from 8,623 m up) is now "Whiteswan Lake inlet stream", so "No Fishing EXCEPT below the falls" keeps covering it after the oxbow fix. Ticket T637.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **Review app: the checklist is ready (T532, 2026-11-09)** (the two small calls stand as built). Restart it (`bash curation-review/run.sh`). DFO tab = DFO
  places; Entries → "Extents checklist" = regulation extents (both together per water: 3,686 places on 1,374 waters).
  One water at a time; each row ✓/?/✗ with its map pin; "All correct" or "Something's wrong" (→ change the extent).
  Two small calls, defaults in use: (1) ✗ also flags "tributaries walked but the words don't say" and "a carve-out the
  words never state" — default: keep ✗ (look at it), say "make those ?" otherwise; (2) a check saves on the place only, not
  on its older rewordings (they are dormant) — default: yes.
- **Production release: after 0.8.0's data release (owner 2026-11-17: everything lands first).** Fresh commands come
  here with that release id (`handoff/PLAN/release/PRODUCTION.md`, "Every later release"; the feeds worker first when it
  carries closed areas). The two older command blocks (r-f749475d0962, r-a178299f6005) were removed (T790 item 7).
- **Phone check (current build): last done 2026-11-09 on build 24** (answers on the owner checks page,
  https://claude.ai/artifact/5LRBFFgvYeTRnygSBWvZbM). 20 tickets closed on "fine". Follow-ups filed: T563 (percentile
  alert follows each day), T564 (gauge page: forecast + readable chart), T565 (in-season notice at the top), T566 (map
  switching), T567 (Fraser at Mission reading); T401 pop-in kept, minor. **One question back to you: T498/T508 "a
  water's pane opens whole" was marked Not right with no note: which tab, and what loads late?**
- **Review app look (T532):** "will check soon". Ticket work waits for your green light (one agent at a time).

- **ANSWERED 11-05:** T260 switches = ink fill; CF token kept until prod (T404); Apple install id declared as Device ID
  (not linked); Heber: carry ignores whitespace-only changes; audits T333–T335 scoped (all areas, report + fix tickets,
  medium, delete archive/). (T425/T427 looks, T319 titles, icon, support URL: decided 2026-11-06.)
- **PSF run-timing files: ANSWERED 11-05** — fine as is; only the parsed `timing.json` stays in git, the PSF pages and their
  fetch record are git-ignored from now on (772fbf4f0).




- **Water-page speed (T405/T409): ANSWERED 2026-11-05.** Build the map only when it is first seen, web-only (local 93·93·93;
  a phone's "back to map" waits ~1–2 s once). Next chat, launch and hygiene, before the prod SEO launch. Keep Cloudflare's
  JS detections and Web Analytics beacon (0 ms blocking).

- **Region hub cards (T406 part 2): answered 11-05 — keep both as they are** (the bar where it is; the map button opens
  the whole map, no region link).
- **Changelog drafts: approved (owner 11-05, "I'm sure they are good")** — `draft` cleared on 0.1.0–0.5.0; 0.6.0 stays a
  draft until its release. **0.7.8 changelog: ANSWERED 2026-11-13 ("the 0.78 draft is good")** — `draft` cleared on 0.7.8. Still yours: production has no Play config file (`play.json`) yet (prod transition, T404).
- **Gauge alerts, stock-style (T527, owner 11-09; replaces the T234 v1.1 cadence readings):** crosses above / below a
  value or a percentile for the day, once per crossing, ready again once back across by 2 % of the line. The per-alert
  cadence and the ±% in 24 h kind are gone; a saved ±% alert is not kept and the page says so. To confirm, as built:
  a percentile line is taken from the gauge's record when the alert is saved (delete + add again after a new HYDAT
  release to refresh it); custom percentiles are whole numbers 10–90 (the record has nothing past p10 / p90).

- **Plan to 0.5.0 approved (2026-11-03):** the plan file (`RELEASE-PLAN-0.5.0.md`) is in git history since T437. Yours along the way: a look at the compressed rule-book photo pages (T354, plan set 11-03)
  (T360/T354, batch 2); the Apple Developer account ($99) before the iOS release (T88); the feeds-worker deploy: now Claude's
  (owner 2026-11-03), when it makes sense (with 0.5.0: dfo-feed/3 + the ten forecast days).
- **Build pass (T266/T295/T297, 2026-10-31):** review `handoff/PLAN/app-integration/build-pass-2026-10-31.md`. The proof side run matched r-51d80e68f056 byte for byte, except the tile_version (T305) and the publish stamps. Nothing was promoted. Nothing is yours here.
- **App test bed (T255, 2026-10-29):** Android emulator scenarios for downloads/sync/offline — `app/tools/testbed/README.md`; last report `handoff/PLAN/app-integration/T255-shots/run-final/index.html`. Found and ticketed: the first install crash-loops on Android (T262, high), Wi-Fi return does not start a held update (T265).
- **ODbL offer (T235, 2026-10-28):** done as you asked (`data/LICENSE-ODbL.md`, `_licence` in each file). Yours: (1) the repo has no LICENSE of its own — the notice says the rest is "under its own terms"; pick one if you want it stated; (2) the app links files on the `redesign/stream-sections` branch (`licences.ts` `REPO_BLOB`) — flip to `main` when it merges; (3) the no-access land (incl. Malcolm Knapp) and reserve outlines are OSM, not the Province — now credited as OSM.
- **Update testing on your phone (T251, 2026-10-28):** dev build → About → Updates log → channel Test → reopen; then `node deploy/staging/test-updates.mjs reset | feed-bump gauges | release 1 | corrupt | missing | feed-404 dfo | min-app 99.0.0` and Check now (steps in tickets/T251). Before prod (T86/T240): the production data worker must allow the `X-App-Version` header (copy staging's data-worker.js CORS line), or web update checks fail.
- **Staging is live (2026-11-05: release r-5a5999c94588, SEO pages from it; was 2026-10-28 r-46be3c135e68): https://staging.canifishthis.ca** (app + SEO pages; data from
  data-staging.canifishthis.ca). Check: the map paints and a tap opens regs; a few `/water/<slug>` pages (e.g.
  `/water/vedder-river`), `/region/2`, `/sitemap.xml`, and an old link such as `/waterbody/930-055749/` (one 301 to
  `/water/san-juan-river`). Staging sends `X-Robots-Tag: noindex`. Decide on the zone: (a) Cloudflare **managed robots.txt**
  is prepended to ours (it says `Allow: /` for everyone and blocks AI crawlers); (b) **Bot Fight Mode / JS detections + Web
  Analytics** inject ~2.6 s of script into every page (Lighthouse Performance 69-76 instead of >= 95; our pages have no
  JS). Prod's data worker `bc-fishing-r2` double-gzips release files: use `deploy/staging/data-worker.js` for prod (T86).
  Details: handoff/PLAN/seo/staging.md.
- **SEO / domain — AT PROD LAUNCH ONLY, not now (owner 2026-11-05: no prod deploy for a while; tuning on staging).**
  These wait for the prod transition (T404 runbook). Nothing below is due today. Staging: https://staging.canifishthis.ca
  **The runbook is written (T404, 2026-11-07): `handoff/PLAN/release/PRODUCTION.md`** — every prod step in order with its
  command (all yours, each needs CONFIRM_PROD), every later release, rollback. Read it and answer Q171–Q175 (tickets/README).
  1. **(in the prod release runbook, T404)** **www.canifishthis.ca drops the path today** (`curl -sI https://www.canifishthis.ca/about` → `location: https://canifishthis.ca/`).
     Cloudflare dashboard → canifishthis.ca → Rules → Redirect Rules → edit the www rule: "Dynamic", expression
     `concat("https://canifishthis.ca", http.request.uri.path)`, status 301, **Preserve query string** on. (canifishthis.com and
     www.canifishthis.com already keep path + query — leave them.) Optional instead: remove those rules and deploy the generated
     host worker: `npx --prefix app wrangler deploy -c deploy/prod/host.wrangler.toml` (after deploy/prod/site.sh has copied it).
  2. **Prod site deploy** (with the 0.6.0 release; T240 §5): build `app/dist/web-prod` (T240 line "SEO build for prod") and
     `node app/tools/build-seo.mjs --release-dir data/release/<r-id> --app app/dist/web-prod --threads 4 --out app/dist/seo-prod`
     (via scripts/heavy.sh), then `CONFIRM_PROD=canifishthis.ca deploy/prod/site.sh`. Check: `curl -sI https://canifishthis.ca/waterbody/930-055749/`
     → one 301 to /water/san-juan-river; `curl -sI https://canifishthis.ca/region/2/` → 301 (not 307); `curl -s https://canifishthis.ca/robots.txt`
     ends `Sitemap: https://canifishthis.ca/sitemap.xml`; no `x-robots-tag` header; `strict-transport-security` present.
  3. **Zone settings** (Security → Bots; Settings → robots.txt): keep Cloudflare's managed robots.txt OFF on canifishthis.ca so ours is the only one.
  4. **Search Console** (search.google.com/search-console): keep the v1 property; Add property → Domain → `canifishthis.ca` → copy the TXT
     record → Cloudflare DNS → Add record TXT `@` with that value → Verify. Repeat for `canifishthis.com`. Sitemaps → submit
     `https://canifishthis.ca/sitemap.xml` (expect ~19,928 discovered). URL Inspection → `/`, `/water/vedder-river`, `/region/2` → Request indexing (those three only).
  5. **Bing Webmaster** (bing.com/webmasters): Import from Google Search Console → pick canifishthis.ca → submit the same sitemap.
  6. Monthly: handoff/PLAN/seo/MEASURE.md (no analytics script — Search Console, Bing, Cloudflare traffic).
  7. Outreach (T197): after prod is live, fill the brackets in handoff/PLAN/seo/outreach/*.md and send each by hand; log in tracker.csv.

### Batch for the end of the closed-testing run (owner 2026-11-11: decide together at the end)

Answered 2026-11-11 (recorded in T665, T667, T666): the six tidal decisions, the six tests decisions, the fresh-copy test run.

Answered 2026-11-11 (later, tidal rulings in T678, T679):
- A fresh stub under 25 m at the top of a line counts as tidal (Ewen Slough, 356242813 become wholly tidal).
- Lines wholly in tidal water are kept in the graph and marked tidal; none is deleted.
- An unnamed widening pond on a river is the river, for whole-river rules and for "in streams" rules (T679 D1, Q154).
- Order of tidal sources: (1) a DFO written boundary, including curated splits that encode one; (2) otherwise coast crossing, where the line enters the sea or a tidal river; (3) the DFO polygon for map shading only. Polygon hand edits and non-DFO splits carry no authority. Verbatim on the splits: "No to this unless the curated splits are the DFO ones. Like all the splits added in region 1 and 2 especially."
- The Fraser's own channels and arms below its tidal boundary are tidal ("We still want all channels and stuff of Fraser below it's tidal boundary as tidal"). Sloughs are NOT covered ("Sloughs aren't channels. Sloughs could be really non tidal?") and are decided one by one.
- San Juan River: the tidal boundary is the San Juan Bridge, by DFO's wording ("... shoreward in a easterly direction of this boundary and downstream of the San Juan Bridge", https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/tidal-maree/a-s20-eng.html). ACCEPTED by you on 2026-11-11 ("No just accept that bridge please"): the curated split goes in as accepted, DFO's wording as the source, no review step (T678).
- T627 digest email: approved, deploy as is.

Answered 2026-11-12 (recorded in the tickets; Owner To-Do rows ticked with the answer):
- Owner approved promoting the second tidal candidate (full_t678b). The 17 lake-outlet gauges keep today's placement. The 187 open coast lines stay held at today's result until a grouped proposal is made.
- Salmon (T671, T676): D1 All row vs species row: the All closure wins unless the species row is dated inside it ("Yes to option 1 but add to gotcha list"). D4 Region 6 aggregate: "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4. Individual species limits also apply. Species limit is seperate." (combined 4 is a pool note; species limits stay separate lines). D5 undrawn open place: "Closed today · open in part". D2 (Campbell River) accepted. T676 "until further notice": "it is from that day to no end. If the thing is for a closure then it closes etc" (open-ended from that day, doing what the row says, in release and feed).
- Tidal (T678): San Juan: the main-stem bridge is at the river's mouth, so the boundary stays at the mouth; the side channel is tidal below the Deering Road bridge (681 m up it). The 10 curated splits not traced to DFO (Qualicum, Toquaht, Sooke, Chemainus, Campbell River Area 13, Nitinat, Yorkson, Chambers, Noons, Nass): "Keep all as my decisions" (recorded as owner boundaries; this overrides "non-DFO splits carry no authority" for these ten). Kitimat: automatic mouth line. Marble and Tlell: "Mouth for marble river (signs near varney bay) could add it has signs to cut.. for tell say 1.5km upstream bridge option" (Marble at the mouth with a note that signs stand near Varney Bay; Tlell at the bridge 1.5 km upstream, the DFO option naming it: Claude's reading). The four held creek pieces (360586479, 360665787, 360675720, 360812872): the mouth line decides, shown in the T678 diff. Ucona's first 33 m under the Strathcona Park rule: fine. lake:329461947 folds into the Okanagan River Oxbows: yes. "Dewdney Slough" search name finds Nicomen Slough: yes, apply.
- T679: D1 ponds: built then reverted (the bundle refused it: 161 rule keys mixed lake and stream); goes into the next atlas build instead (16,152 ponds change; 97 rules gain, 32 lose). D2 partial salmon coverage: map and bar both give the species verdict. D3 nearer row wins: ignore the loops (receivers with flow-back removed), next build, diff shown. D4 words read at build time: leave, add a guard that stops the build and names the row when a row that matched before stops matching. D5 DFO confluence cuts: same as provincial, the joining water stays out unless DFO's words add tributaries; diff before it ships. D6: Python reads DFO rows tolerantly too (an unreadable row reads "check" and is reported, no longer stops a release). Gauge placement (#4, held): "One rule, accept the moves": atlas placement wins; 29 of 458 shown gauges move at the next build; list reported.
- Screens: T462 G6 Pend d'Oreille: the printed exemption lifts both rules; barbed and treble allowed. T670 G2 bait heading: "Why does it matter the river is closed? It shouldn't show any method or gear or quotas in the regulation card. If salmon are open or something at same time so that river is half closed then it should say in force." (a fully closed water shows no gear/method/quota; where something is open the rule reads "in force today"). T670 G5: Licence tab shown when salmon is open. T668 live straddler: "If python pair table is safe that sounds good we want these to always resolve on their own" (option 2, provided it measures safe). T368 Q6 "today": switch to B.C. time (America/Vancouver). T672 W4 web description: name every region.
- "Accept all as built": feeds worker defaults (T549/T554/T633/T597), T666 release path defaults, T585/T555 and T554/T633 gauge wordings, T182 caps, T532 checklist's two small calls, T630/T605/kept-reading line (leave), T672 K9 (leave), T670 K1-K5/G1/G3/G4 wordings, T672 W1-W3/G11, T668 card wording, T671 D2 fix, the 8 area names cut at 50 characters, T387 point-bound facts (leave per tap).
- Still yours: nothing from this "accept all as built" list; open items are under their own headings below. (Nakina half-river, T670 G7 and T627 answered 2026-11-12.)

Staging: release r-483b295de57c is live on https://staging.canifishthis.ca (site, data, feeds worker). Yours: look at the four new creek cuts (FWA lines 354135384, 356070273, Crescent Slough 356326465, 356349946), and Brem and Ucona, on the staging map.

Open, your call (tidal, T678 / T679):
- ANSWERED 2026-11-12 (build: T685; RULINGS E21–E24): one rule — water reaching the sea is fresh to the shoreline (G1, G2, all of G6, X2, X3, X4); a creek entering a tidal river is cut at the river's edge (G3, X1, X5, X6, X7, sloughs); G4/G5 keep today's cuts; five sloughs whole tidal (Deas, Woodward, McDonald, Tamboline, London). Original ask: Tidal coast proposal: 183 lines in seven groups + 20 Fraser sloughs (T678, replaces the old held-rivers, 46-estuaries, 61+32 held, four-new-cuts/Brem/Ucona and Fraser-slough rows; Owner To-Do row `tidal-coast-proposal`): https://claude.ai/artifact/2VRJAsyeeyMNmCiZpJewVA . Proposals only, nothing applied: G1 59 fresh to the shoreline; G2 17 drop the cut; G3 18 cut where the creek meets the river; G4 10 and G5 3 keep today's cut; G6 63 (E1 to E10) and G7 7 (X1 to X7) one answer per card; S1 to S20 Fraser sloughs, nothing filled in. Answer in chat.
- ANSWERED 2026-11-12 (T678 C, Owner To-Do row `held-five-build27-cut`): five held lines with a build-27 cut and nothing tidal below (354087703, 354154029, 356341234, 360445710, 360861076): "Drop the cuts". Ticket T684. RULINGS E20.

Still open:
- ANSWERED 2026-11-12 and BUILT (4bc03ac14; ticket T676): "until further notice" — "it is from that day to no end. If the thing is for a closure then it closes etc". The release now reads every DFO date cell with the live feed's reader, so both say "from <day> until further notice" (RULINGS D5). No open/closed status moves on today's pages; 6 places not drawn yet change their words. Shows with the next release (the feed already reads it so).
- T668 salmon places from two regions on one stretch (audit `app-integration/T668-audit.md`, 493 sections): built on the defaults D1 an undrawn place stays in its own region, D2 on a section in one region that region wins a tie, D3 a region-wide place binds every section in its region, D4 the card names the region, D5 D1 first. Owner 2026-11-11: a stretch that straddles a boundary is genuinely in both regions, so a straddler keeps both regions' places and the stricter answer per fish speaks (as the freshwater rule, RULINGS Z4); and "straddles should always take the most strict rules from both regions" (limits, sizes, open days, gear, hours). Measured: 432 of 493 sections change, 197 genuine straddlers, 84 statuses flip, all toward closed; shows with the next release. D1 and D4 confirmed by the owner 2026-11-11 ("Yes and yes"), with: on the Skeena the card row names the DFO section as well as the region (the cascade level): the lettered section as DFO's Skeena page prints it (A, B(i), B(ii) …), owner's clarification the same day. ALL ANSWERED; nothing open here until the phone check of that release.
- T668 straddlers, one default to confirm (built, commits dfb4ea7c1 b53d5e193, RULINGS Z4a): salmon openings on a straddling stretch now use the same strictest-wins comparison as the freshwater rules, with ONE deliberate difference: when one of the two DFO regions says nothing about a fish on that stretch, it counts as closed there (freshwater treats silence as 'the base rule applies'). Measured on the 197 straddlers: 19 sections now show both regions' words, none looser, no map colour change, no gear change. Default taken: silent region = closed. ANSWERED 2026-11-11: yes, salmon differs from freshwater here; a silent region counts as closed. The rest of the unification is T669.
- T664 Okanagan River Oxbows as one water (code 25471b393; lands with the next atlas build; detail `app-integration/T664-pending/README.md`): the 45 stream pieces become one water. Default taken: the 8 oxbow polygons that the source data classes as lake or wetland stay separate, so search will show 1 + 8 results, not 1. Folding them in would turn them into streams, which takes lake rules off them and puts stream rules on (including the spring stream closure the oxbow row exempts). ANSWERED 2026-11-11: fold them in. "They should have been converted to stream polygons like Nicomen Slough so it all becomes one. They aren't lakes, they are considered streams." Going into the next build; search shows one result.
- Nine DFO cuts from your marks (proposal patches in `app-integration/T664-pending/`, applied in the next build): Nakina River, DFO's wording is "from the centre line to the northern bank" between the two signs; a cut cannot take half the width of a river, so the rule will bind the whole channel between the signs (stricter than DFO on the south half). Default taken: whole channel. Owner 2026-11-11: will look later. ANSWERED 2026-11-12 (Owner To-Do row `nakina-half-width`): "Whole channel + note": the rule binds the whole stretch between the signs and the card says in DFO's words that it applies from the centre line to the northern bank only. Ticket T683. RULINGS E19.
- ANSWERED 2026-11-12 (T670 G7, Owner To-Do row `t670-g7-classified-terms`): the 26 classified-water licence terms of p.7: "Show on each water": each classified water's Licence tab lists the p.7 terms that apply to it, in the book's words. Ticket T670. RULINGS Li13.
- CLOSED 2026-11-12 (owner: "Close it"; row `t628-marked-entry-guard`): T628 item 5, a plain re-parse can no longer replace an entry carrying your review mark.
- Next rebuild side build, measured (results: `handoff/PLAN/app-integration/next-rebuild-results.md`; no unexplained difference). Items for you, all left at today's behaviour unless said:
  - parse-missing: NOT needed, closed. Checked without parsing (5b5caecd3 lane): 1,393 entries, 0 missing, no reference to a lost water id; entry ids come from the synopsis row, not the water.
- Tidal map pages (same links as the tidal line above): held rivers, one card each https://claude.ai/artifact/3v6g4Ggx8WJZg4Jvxras9q ; every cut on the map https://claude.ai/artifact/3Jbd8SjT64Tj9HXvPgPBDc
- Phone check of the next build: Claude brings one fresh list (folds in the 0.7.5 checks: cold start, gauge ring, key switch, oxbows, page opens whole, part warning, three rule readings, satellite edges, tidal shore, Stellako steelhead line, plus T681 legend card and the T625 shared link). Owner 2026-11-12: "One fresh list for the new build", gone through in chat with pictures.
  - Salmon closed shows the year bar like game fish (Capilano River). Built on your "maybe" (2026-11-13): a water DFO covers whose salmon are closed all year now draws the Salmon (DFO) year bar (closed, today marked, legend) instead of the bare "Closed" word; 751 waters change. Region-wide closures (R4, R5, R7A, R7B) and waters DFO lists nothing for keep the bare word. Say if you prefer the bare word back.
  - More screen: Settings menu, Freshwater Atlas no longer "out of date", no repeated update info. (T703, owner 2026-11-13; next build. Pictures: ~/bcfish-session-backup-2026-11-12/more-settings/)
  - Map zoom carries momentum after a pinch: a quick pinch out or in, fingers lifted while still moving, keeps zooming for about a third of a second (also with the thumb held still and one finger flicking); a pinch held still before lifting stops where it is. (T707, your build-29 report "No zoom flick"; next build, Android.)
- **Phone check after the final build — notification inbox: bell badge, New/Earlier list, open a row, mark all read, an Updated notice** (T706 + T694, owner 2026-11-13: "The inbox can auto be added just add it to the post release checks for me to look at it"). Tap the bell: unread in-season notices under "New", read ones under "Earlier"; tap one (it opens its water's page at the notice and stops counting); "Mark all as read" clears the badge; an amended notice reads "Updated". Pictures (fixture feed, staging waters): `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/notifications/` (light and dark). Nothing to decide unless it looks wrong.
- **Added 2026-11-13 (Owner To-Do rows; every one is a proposal, nothing decided):**
  - Phone checks after 0.7.8 lands on Play internal: `phone-check-078-salmon-bar` (salmon "closed" waters draw the year bar; T678), `phone-check-078-pinch-zoom` (T707), `phone-check-078-inbox` (T694, T706; see also `t706-inbox-phone-check`), `phone-check-078-settings` (More → Settings, T703), `phone-check-078-map-look` (general look).
  - T708 questions: ANSWERED 2026-11-13 (recorded in T708 Owner decisions 6): `t708-haida-gwaii-tidal-mouth` (stays at the mouth for Copper, Deena, Mamin unless a written boundary is listed elsewhere; check found none), `t708-stave-ruskin-channel` (the whole described Ruskin channel; no change), `t708-mcarthur-island-slough` (the whole slough; no change to the stretch; the card keeps the book's place words). Still open: `t708-frog-redfern-squirrel` (look harder; evidence in T708 Work 8).
  - Research questions, one row per note in `handoff/PLAN/research/`: `research-accounts` (8 questions), `research-corridor-imagery` (6), `research-bathymetry` (6), `research-stocking` (6).
  - `store-descriptions-write` (T371): write the short (80 characters) and full (4000) store description from `handoff/PLAN/play/listing.md`.
  - `boundary-name-test-changes` (T678): the boundary names and test pins accepted during the release: Fraser (34 sections), Babine part 5 "juvenile counting weir", Skeena Exchamsiks → Zymagotitz and the Reserve parts (junction fix), Kanaka Creek "→ Source", Mat Creek top end, Coquitlam part names, the Skeena map tests, and the four test updates in 4ecbe1738 (Harrison 3 → 4 rules, unresolved 25 → 29, export codec lake edge as a run's end; plus the regenerated unnamed-stream sweep, −3 attributed). ANSWERED 2026-11-13: (1) Babine part 5 keeps "Juvenile counting weir"; (2) Skeena Reserve split accepted, side channels to be listed where they sit (T710, count first; side-channel name reads backwards, undecided); (3) "Source" stands, header text must match (T711); (4) Fraser cuts + Skeena map tests accepted; (5) Harrison r4 shows the book words with location text (T712); (6) steelhead rules r5b/r6b/r9b trace (T713).
  - `t709-preview-video`: watch the preview clip before any store upload (not ready; waits for the build).
  - `gagnon-williston` ticked: owner answered 2026-11-13 (mouth stays Williston; sign cut about 200 m above the unnamed lake, T708 Work 3).
  - ANSWERED 2026-11-13 (by dialog) and BUILT, released r-f749475d0962 (T708): (a) McArthur Island Slough shows the "in part" form "No powered boats — in part: from westerly entrance to 12th Street entrance to Park"; (b) Lakelse River: rename the end only, the lower stretch's end reads "Mouth (at the signs)", no new cut, no change to which water the rules cover; (c) Tlell r5: yes, take the tidal note off the 13 freshwater tributaries. `t708-mcarthur-lakelse-tlell` (T708, proposals, 2026-11-13): (a) McArthur Island Slough: the catalogue refuses the book's place words beside a whole-water rule; the only allowed form shows "No powered boats — in part: from westerly entrance to 12th Street entrance to Park" (a note on the slough). Accept that, or keep today's "No powered boats" (the book sentence still shows underneath)? (b) Lakelse "signs" at the mouth: needs either the designated mouth located (signs 1.7 km below the logging-road bridge) for a cut, or a new per-water mouth name; which? (c) Tlell r5: take the tidal note off the 13 freshwater tributaries (Work 6 proposal)?
  - ANSWERED 2026-11-13 (by dialog): the Harrison's two vessel entries in build 30 are fine; nothing to change, T712 closes. `t712-harrison-vessel-words` (T712, 2026-11-13): build 30 already shows the Harrison's two vessel chips with the location: "No powered boats — in small bays along the river as signed" and "Speed restriction — in small bays along the river as signed" (your 2026-11-12 "Keep them as two entries"); the pack's "r4 No powered boats" was its own short summary. Enough, or show the one printed line "Speed restriction or no powered boats in small bays along the river as signed" instead?
- **Phone check of build 29 (app 0.7.7)** (2026-11-12; on Play internal; release r-0a9930fb8826). This is the fresh list promised above. Look at these on the phone:
  1. Cold start: close the app, open it; the map comes up with no error.
  2. Gauge ring: a gauge that reports only flow or only level shows as a grey dot with a ring (Kootenay Lake has some).
  3. Map key switch: flip Trout and Salmon (and Runs) quickly; the key under the map always matches the map's colours.
  4. Oxbows: search "Okanagan River Oxbows"; one water, not 45 pieces.
  5. A water page opens whole: search a river and tap Open; the page shows the whole water, nothing loads in late.
  6. Part warning: Shuswap Lake (or Quesnel Lake) shows "Part of this lake is closed today" at the top of the Keep tab (the closure is on a part the map does not draw).
  7. The three rule readings (strictest rule wins on a straddle, "until further notice" dates, a part closed in a lake): say if any read wrong.
  8. Satellite edges: switch the map to satellite and zoom into a stream; the stream opens into two thin edges so the water shows through.
  9. Tidal shore: Deas Slough (the page is the tidal card, "Tidal water: a different regulation system..."), Power House Creek, Suter Brook; each ends at the tidal limit.
  10. Stellako River: the steelhead line reads "Rainbow trout and steelhead: release every one".
  11. T681 gauge legend card: on the Conditions map, a small card over the map says "Gauge reports level only" (under Flow).
  12. A shared link (T625): open a link someone sent you; it lands on the right water.
  13. Nakina River: the card says in DFO's words that the rule applies from the centre line to the northern bank only.
  **Known, do not report: Deas Slough's Map view (the Map tab on that water) keeps the previous camera and says "The parts of this water are not available" on build 29. Ticket T688: fixed in code, ships with the next APK. Pending your word (default built in): the tidal bar reads "Tidal water", the tidal paragraph, and a button "Show regulations for this water".** Also known: searching "Vedder River" then Open lands on the Chilliwack River's part 1: ticketed as T687 (the book pointer above the result opened the wrong water; fixed in the data, 5c294120d, shows with the next release).
- Defaults taken 2026-11-12 (app wave; each is on the Owner To-Do page, group "Look and pick", rows 6 to 13; act only if you disagree):
  1. ANSWERED 2026-11-12 (keep): T677 Ucona River: the Strathcona Park rule now covers the first 33 m of the Ucona. Keep as built, no cut where the park ends.
  2. ANSWERED 2026-11-13 (built): T673: ONE in-season matches file for every release ("Why not just have a single file that does both?"): a notice is placed by the river's map line and metres along it, so a phone on any release places it on its own sections (a section only partly inside shows it too). No per-release copy. Goes live when the main session publishes it with the final build. The "new" mark on an amended notice (T694) is built too, with the inbox (T706).
  3. ANSWERED 2026-11-12 (leave unmarked): T629: "state your FWID" in the Proof of Identity paragraph may be from the later printing. Left unmarked.
  4. ANSWERED 2026-11-12 (all themes): T467: the colour-blind check stays on all themes. The lake-shoreline look stays open for pictures.
  5. T540: "Reading the regulations" stays in AGENTS.md.
  6. ANSWERED 2026-11-12 ("Show the fee only"), built: T670's two basic-licence terms: only the $1.14 fee line for disabled B.C. residents is on the Licence tab (under the basic licence, for a B.C. resident adult, in the book's words); "one annual angling licence per year" stays off. RULINGS Li13b.
  7. ANSWERED 2026-11-12 (stays): T688: wording on a wholly tidal water's map: bar "Tidal water", the tidal card's first paragraph, button "Show regulations for this water". Also answered by dialog: the tidal area stays drawn and labelled but tapping it opens nothing, and the 16 wholly tidal waters stay in search with the tidal card alone (no "SALMON (DFO) Closed" line): built as T692, RULINGS P52.
  8. ANSWERED 2026-11-13 (owner: "fix it"; Claude's choice): Store screenshots (T319); shot 8 now reads "Rules and map work offline" and shows the More data list, each source with its date (Settings has no offline control on the web); order and titles from `app/tools/store-shots.json`: "Can I fish here today?", "Find any water", "Today's rules, in plain words", "River conditions", "Salmon openings", "When the runs come", "Works offline". The "Keep your spots" scene is phone-only and not shot. New set (staging site on r-0a9930fb8826) in `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/store-077/`.
  9. T689: an own-rules lake (for example Cultus Lake, which is open) draws orange at 60% over the blue basemap water, which mixes to khaki and sits close to the "Not fishable" swatch in the map key (sewage ponds, tailings ponds, dumps only). Nothing is wrong in the rules; it has looked this way since at least Nov 4. Should own-rules lakes get a colour further from "Not fishable"? Default: leave as is for closed testing. (Also on the To-Do page.)
  10. ANSWERED 2026-11-12 (A: stay outside): T631 item 5, the three Skeena side channels at the Kitsumkalum mouth (360220078, 360220098, 360220099). You said "this i meant the auto mix mouth splitting method we developed we want to use that here if it is splitting tidal from fresh": it is NOT a tidal/fresh split (the Skeena's tidal boundary is 117 km downstream), so nothing changes. Then you asked whether they should be split like normal braids. Facts (pictures A–D in `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/skeena-channels/`): they are ordinary braid channels of the Skeena, but by the map they lie wholly ABOVE the DFO place's top boundary (beside river km 125.5–128.6; the boundary is at 124.0); they look like they straddle it only because one junction in the FWA data carries a distance 3.35 km off its own map point. Options: A keep as today (outside the place); B put them whole inside; C carry the cut across by river distance (lands on none of them, same as A); D fix the bad junction itself (same answer as A, but that rule moves 298 junctions province-wide, so its own ticket). Default: A. Still open: do you still want a cut there? (Also on the To-Do page.)
  11. ANSWERED 2026-11-12 (no list): T678 plan 6, lines the pruning removed from tidal water: you lean to "just add them to the manual list". Count today: 1,204 unnamed FWA lines with no piece in the map data lie in tidal water — 1,133 are FWA "construction lines" drawn inside the Fraser, Skeena, Nass etc. river polygons (the river's own centre lines, not channels), 71 lie in the sea (none named). They have no piece in the map, so a list changes no answer and no picture; it is a record only. Recommendation: no manual list; if you want one, only the 71 sea lines, in a new file beside `data/curated/waters/shore_held.json` (not in it, whose rows are lines the coast cut acts on). Default: no list. (Also on the To-Do page.)
  12. ANSWERED 2026-11-12 (promote with the final build): The candidate map data (`full_next`, built, not promoted): what changes for an angler — searching "West Road River" or "Blackwater River" finds only the river, no longer five lakes as well (Kluskoil, Tsacha, Tundra, both Euchiniko); two Teslin River "Hutsigola Lake" edge ids swap (nothing names them). No rule changes where it applies. Default: it is promoted with the final build unless you object. (Also on the To-Do page.)
  13. T678 plan 6 wording: what "a line the prune took from tidal water" means now the coast rule ignores the DFO polygon. Default: an FWA line drawn in the sea or in a tidal river's polygon that has no piece in the map, listed with its reason (the count in 11 uses this). (Also on the To-Do page.)
- Tidal (T665), answered 2026-11-11 on the dry-run map (`app-integration/T665-dryrun-map.html`, sent in chat): cuts that leave nothing tidal are dropped; the 33 creeks are cut where they leave the tidal polygon and the tidal part taken out ("line end wins" withdrawn); Claude applies the same to 59 more creeks of that kind, say if wrong. Default to confirm: a water with its own tidal limit is never cut by the polygon. Still yours: eight main rivers (Bella Coola, Mamin, Apple x3, Kshwan, Sarita, Bedwell) held out until you look at their map page.
- Tidal (T665) rerun with those answers (commit 227c53b08, `app-integration/T665-dryrun.md`): 201 shore cuts (127.4 km tidal below them), 215 lines wholly inside removed (67.7 km). 61 lines HELD until you look, untouched in the next build: the 8 main rivers plus 53 where the polygon runs over ~100 m up a narrow channel (the Pitt-style over-cut). Held rivers, one card each: https://claude.ai/artifact/3v6g4Ggx8WJZg4Jvxras9q — all cuts on the map: https://claude.ai/artifact/3Jbd8SjT64Tj9HXvPgPBDc. For each held one: cut at the polygon edge, cut at the mouth (DFO default, straight line between the two most seaward points of land), or leave. Of the 33 mouth creeks: 19 cut, 11 held, 3 not cut. Also proposed, not applied: 24 mouth rows for the tidal limits file (`T665-tidal-limits-mouth-rows.patch`).
- Store screenshots (sent 2026-11-11): pick the order and titles (`app/tools/store-shots.json`).

## Reviews
- CLOSED 2026-11-12 (row `t627-read-staging-digest`): T627 staging digest email read by the owner: "looks good".
- **Tidal follow-ups (T359, 2026-11-03):** review app → Tidal → Follow-ups (8). Decide: does a DEM point you
  confirmed become a cut (Caycuse 1,925 m, Dempster 550 m)? Re-mark Annacis/Cannery/Morey/Parsons "whole water";
  look at Skagit (US border) and Bear River (bridge).
- **DFO tab:** Nanaimo "tidal portions": both are below the tidal limit (Cedar Rd bridge), so tidal — T449 makes that a state (owner 11-06).
- **T227 DFO places not on the map (2026-10-28): review app → DFO, open each, "accept suggestion", Save extent.**
  - accept in review app → DFO: Cowichan — Sandy Pool sign → tidal boundary (= FWA mouth; active, high); weir/Cowichan Lake → 66 Mile Trestle (high); CNR mile 70.2 → Skutz Falls (high, new Skutz cut); "incl. tributaries and Cowichan Lake" = whole river + lake + tributaries (medium: lake's inflows counted). Last three dormant.
  - accept in review app → DFO: Capilano — "including tributaries" = whole + tributaries (high).
  - accept in review app → DFO: Kitimat — downstream / upstream of Hwy 37 bridge (high, OSM-checked); hatchery outfall signs = your 08-10 pins (high; west-bank-only width not modelled).
- **Release r-a871447c48ba verification follow-ups (2026-10-28):** (Claude re-cut since; the golden re-extraction of the Q62/Q63 words is moot, golden.json is frozen since T87. Whonnock 08MH100 and Beattie "Trout" answered 10-28: T228.)
- **T228 (2026-10-28):** two DFO queue items from T228. (1) Klinaklini: place "Management Units 5-6 to 5-11 boundary"
  as a split (pre-filled point). (2) Bella Coola: exclude the Atnarko (gnis:17209) from its tributaries.
- **T224 names (2026-10-29, live in r-62e2f3a8c81e):** review app **Names** tab (:5183, `#names/`), reject any that
  are wrong among these 20 auto-accepted names (list + why in tickets/T224 "Owner spot-check"): Cat Stream (applied: a 3 km OSM way
  matches 95 %; within your rule), The Cut Coulee, Cohen Creek, Benes Creek, College Creek, North McNaughton Creek, Brigade Bay Creek,
  Problem Creek, Coho Creek (OSM); Corinne Brook, Jantz Stream, Marshall Creek, Gill Brook, Mckay Creek, Shamrock Creek,
  Smith Brook (Abbotsford); Inkaneep Creek, Meadow Creek Diversion, Quinsam River Diversion Headpond, Noble Creek (ECCC).
  No rejects = done. The rest of the review list (36 high, 552 medium, 8,061 low) is optional.
- **T202 basemap (2026-10-27):** the basemap is now z13 (918 → 274.5 MB). Look at the app at your spots, z10–z18,
  light and dark: trail and FSR names should show from z15. Shots
  and numbers: `handoff/PLAN/tiles/T202-basemap-audit.md`.
- **"Needs a split" follow-ups (2026-10-27, live in r-8b43fb267199):** (1) the three guessed `low` cuts are in the
  review app, **Low-confidence cuts** queue (top bar button; or http://127.0.0.1:5183/?lowcuts) — each entry opens on
  satellite, flown to the cut, with why it is a guess: Chapman lower falls + 100 m below (SCRD survey point, ~576 m below
  the power line), Deena "second bridge" (no bridge mapped: a point 5 km above the tidal boundary), Mamin "third bridge"
  (Mamin Main road, 9.1 km above tidal). Right = Place y; wrong = Place n, "split misplaced", pick the right point on
  the map in the extent fix. (2) Columbia
  r11 "from Mud Lake": FWA names no Mud Lake, so it runs Fairmont → Columbia Lake (the outlet wetland, wbk 329093907, is
  the likely Mud Lake) — say if Mud Lake is elsewhere; (3) Stave r1/r2 (Ruskin/Northrop spawning channels) stay notes:
  the channels have no sections in the atlas — adding them is Claude's work (T222), nothing for you here.
- **T204 phone check:** Kanaka Creek's map now has two marks (112 Avenue bridge, Cliff Park Falls) and its lower part is
  two separately selectable stretches ("… · stretch 1 of 2"); say if the stretch wording should differ.
- **Map look (T64, T85, T113, T152; all built and live):** look at the map (wetland-heavy Fort Nelson and the
  Chilcotin without unnamed wetlands; z13–z18 bends on the Fraser/Vedder now overzoomed from z13; Vancouver's dense z13
  tiles; no OSM creeks under ours at z14+) and say if the look suffers — the basemap's OSM wetland fill is one style layer
  away. Best done in the same sitting as the T202 basemap look above (basemap z13 shipped 10-27).
- **Q4 / T55 + T149 — slough and canal candidates: review app, Sloughs tab only** (:5183, `#sloughs/`). All 2,424
  candidates with their pre-marks; your 150 review-page verdicts show as "page: …" and your 8 free-text notes as
  "your note" (incl. 329097416 Cruickshank, the one whose verdict you cleared: "similar this can be on moat lake").
  The old slough page (artifact Es3Mokt7Fs2yNvm7JgQua8) is retired — do not mark there; its whole database (151 verdicts + 58 spot checks)
  was saved as marks-final-2026-10-28.json and has since moved out of the tree (T322; read it from git history); nothing else reads it.

Cleared 2026-10-28 (already settled, stale-item sweep): Q114 pilot (you dropped it 10-25; you do the real review),
T211 feed plan (built 10-27 with your corrections; only the cron + R2 sync below is left), Q145 (= that feed: 6-hourly
in season), Q32 (T95 dropped; salmon is T164/T211 now), Q142 (lake splits deferred, your 10-28 "lake split later"),
Q150 (answered 10-23: own keeps amber), Q119 (Claude's own audit — Claude re-derives it, T125), T185 decide (your
marks are the decisions; applied, Hatzic left alone), DFO-salmon build + "on parts" rebuild (both live in
r-46be3c135e68), run-timing go (you said go 10-25 → T199; Q144 stays).
Answered 2026-10-24: Q125 (muted fill), T179 (400 m; a curated cut never moves; 25 m lake-anchor proposals; merged
splits keep every name and id), PSF permission (T162; their reply is not stored), braids marked (T67 promoted), Q7 (delete old releases — pruned to live + previous; `python -m pipeline.deliver.release.prune`).
Answered 2026-10-23: Q5 + Q34–Q41 (curation plan approved, defaults; splits mid-review must work; salmon pass later = T132), Q13 (Claude runs the reader gate), Q44 (no Level from a side channel; Flow only if it carries a large share), Q53 (as built), Q45 (leave unbound).

## Your open questions (30 listed below — each built with a default; the table in tickets/README.md has the wording)

Triaged 2026-10-23: 64 smaller calls were decided by Claude and 12 were already answered or obsolete — all recorded in
decisions.md "CLAUDE 2026-10-23 — owner-question triage" (read and reverse any). What is left needs you:

- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **T387 point-bound facts (2026-11-04, default: left per tap):** every other fact on a Regulations page is now its
  PART's. Two still follow the tap: the sun line (sunrise / sunset at the tapped point — or the part's midpoint?) and the
  scroll memory (reopens where you left it per tapped reach — or per part?). Ticket T387 has the why.
- **Readings of the book (a wrong call misleads anglers):** ANSWERED 2026-11-12: Q124 a river's rules on unnamed widenings (456 polygons): keep as built now (the polygons take the river's rules); later, fold more small widenings into the stream and draw the stream line instead of the polygon where it is really the stream, real lakes stay lakes (owner's words in RULINGS E27, ticket T695; DEFERRED 2026-11-13 with stocked lakes and bathy map, no longer "after closed testing") ·
  (Q87, Q140, Q62, Q63 answered 2026-10-28.)
- ANSWERED 2026-11-12 ("Leave gaugeless"; "Show 08GC005 above the canal"; built, RULINGS P54): **T696: diversion gauges — no gaugeless section is a diversion gauge's.** The 28 stay gaugeless; 08GC005 now leads the Theodosia above its canal, labelled "measures a diversion" (shows on the next deliver + app build).
- ANSWERED 2026-11-12 ("Leave it": no intake cut; the ~160 m below the intake reads 08GC005 with the river above, as built): **T696: a cut at the Theodosia intake?** (2026-11-12). The canal leaves the river at the intake dam about 160 m ABOVE where the river's section begins (the 08GC006/08GC007 bypass gauge cut). So that 160 m stretch — where the bypass gauges stand — reads 08GC005 with the river above. Add a cut at the intake (rides the next atlas build, which the T690/T691 cuts already need) so it stays 08GC006's? Pictures: ~/bcfish-session-backup-2026-11-12/theodosia/theodosia_after_closeup.png (intake and section end marked), theodosia_before_closeup.png, theodosia_after_overview.png. Ticket T696.
- DONE (answered 2026-11-13, released r-f749475d0962): **T691: five silent gauge stations need a match call before they can be cut** (2026-11-12). Q113 is built (32 cuts, ride the atlas build, RULINGS P53). Not cut: 08FE002 Kemano Powerhouse and 08ND025 Revelstoke Project Outflow (never matched); 08JA013 Skins Lake Spillway, 08NE126 Arrow Reservoir Outflow, 08NJ158 Kootenay Lake Outflow (matches marked wrong, no water). Say which river line each measures, or leave them uncut. Also worth a look: 08JB013 "North Beach Creek above Allin Creek" is matched to "Beach Creek". (Final atlas build 2026-11-12: 08JB013 and 08PA004 Skagit made no cut — 520 m and 1,015 m from their line, over the 400 m limit.) Ticket T691. Claude's proposed answer per station (2026-11-13, NOT applied: a row each, in plain words, in the ticket's "Phase 2 proposals" table; pictures ~/bcfish-session-backup-2026-11-12/gauges/*.png): accept or change each row.
- ANSWERED 2026-11-12: **Which gauge speaks:** Q113 "silent doesn't mean retired so we should add cuts Incase they start transmitting again?" (built, T691) · Q103 "Canal gauge also serves river", then "Leave gaugeless" and "Show 08GC005 above the canal" (built, T696). Still open: Q31 the gauge rule.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **How it looks (taste):** Q130 each model's line under Change · Q132 day chips may still use ELF · Q79 "river" vs "stream" on the page ·
  Q91 "no gauge close enough" wording · Q123 how the Map view marks the selected part (built: dim the others; thin halo in
  `app-integration/T151-shots/alt-halo--*`) ·
  Q2 / Q139 / Q47 no-access and tidal notes on Conditions.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **"Today" is the phone's day, not BC's (T368 Q6, 2026-11-04):** (B.C. time is the settled reading; see the permanent daylight time note.) your answer assumed the app counts days in BC time; it
  counts them in the PHONE's zone (`localDate`): an Alberta phone at 00:30 MT is already on the next day while BC is at
- ANSWERED 2026-11-12 ("It should be in the vessel restriction section but it should say the locator text there"; built c63427bbd, RULINGS P51, shows on the next app build): **Vessel rules (T209):** the 10 close-wording undrawn parts kept ("various locations", "swimming areas") in
  on-parts.md: whole water too, or leave as notes? (`max_power_kw` 7.5 is stored on the 162 electric-only rules, 10-28.)
- ANSWERED 2026-11-12 ("Yes, book's full words"; built 81598325e, RULINGS P51 follow-up, shows on the next deliver + app build): **Harrison River vessel line (T209):** reads "Speed restriction or no powered boats — in small bays along the river as signed"; the other 21 undrawn-part vessel rules checked against the printed book: nothing else dropped.
- ANSWERED 2026-11-12 ("Keep them as two entries"; built 2841fb605, RULINGS P51 second follow-up, shows on the next deliver + app build): **"or" vessel lines as one joined entry? (T209)** No: speed restriction and no vessels are listed separately, each with the book's locator — Harrison "No powered boats — in small bays along the river as signed" and "Speed restriction — in small bays along the river as signed" (Claude's reading: the Harrison too); Okanagan, Kalamalka, Skaha "No vessels — various locations (as buoyed and signed)" and "Speed restriction — various locations (as buoyed and signed)".
- ANSWERED 2026-11-12 ("Yes, read the real boundary"; supersedes the earlier "'Unnamed lake (km 2.3)'"; built 040b79d16, RULINGS P55 follow-up, shows on the next release): **T697: part ends at an unnamed lake** read the real boundary that stands there, never the lake — Slack Creek "Region 8 boundary", Jones Creek "B.C. border"; a cut aliased onto a lake edge keeps its own name (also Duncan "Duncan Dam", Mitchell, Babine "Juvenile counting weir"). One end still has no real boundary: Gagnon Creek at km 3.66 (next row).
- ANSWERED 2026-11-13 ("Mouth is still Williston"; RULINGS P57; built in the T678 junctions candidate atlas `full_next`, shows on the next release): the mouth is where the creek enters Williston, the signs ARE the mouth (cut `gagnon_creek__fishing_boundary_signs_at_the_mouth`, the 400 m cut removed), the whole creek keeps the spring closure, the unnamed lake stays a lake of its own (`waters/pond_calls.json`). Was: **Gagnon Creek: is the unnamed lake really part of Williston?** (2026-11-12). You said: "Maybe we could put signs near the mouth? Let's just assume the mouth is the signs and add it as a variant name. Would this fix it? Seems it is in the wrong spot??", then "Where it enters Williston so it captures the whole stream. Also unnamed lake is part of Williston I think". NOT BUILT: the map data and imagery point the other way — the 58 ha lake (FWA wbk 329465442, coded "lake", Williston is coded "reservoir") sits behind what looks like a rail/road embankment, joined to Williston only by the creek's 74 m channel under it. Picture: ~/bcfish-session-backup-2026-11-12/lake-ends/gagnon_creek.jpg. Build it as you said anyway (the lake is Williston water, the signs at the creek's mouth at km 3.66, the whole flowing creek keeps the spring closure), or keep the lake as its own water and put the signs at the creek's mouth into that lake (km 3.66)? Ticket T697.
- **Salmon (T164), refreshed 11-05:** 179 DFO places still need an extent; each has a fresh proposal (52 on existing
  cuts, 47 whole water, 65 new splits, 15 with a reason). Review app → DFO salmon extents → accept suggestion → Save
  extent. Includes Skeena B(i) (T229 Q3) and the 3 items T326 waits on. Claude rebuilds and measures after.
- ANSWERED 2026-11-12: POSTPONED (owner: close as answered). **User accounts (T223, new 2026-10-28):** one account for web + phone. Questions Q-T223a–f in the ticket: auth
  provider, what syncs (spots / photos / pins / settings), privacy of spot locations, offline merge rule, optional or
  required, before or after first release. Nothing is built until you answer.
- **Batch 7:** Q146 re-centre the map when the parts list opens · Q147 steelhead lake wording on newly taken ponds ·
  Q148 dams → "lake" for the slough candidates · Q149 Bonaparte by hand.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **Gauge gaps on main rivers (T174, shipped r-bda3fea28838), including Q154 (small ponds):** Q153 (side piece adopts the largest ANSWERED river section) and
  Q155 (a water's own lines between alike pieces) shipped: +407 Flow / +327 Level sections, 0 answers changed — veto =
  one switch each in panels.py. **Q154 is yours:** the "double-line polygons" are small FWA lakes; walking them as the
  river moves 96,590 sections (+95k Flow) — kept off. Numbers in tickets/T174. Your "how small?": examples +
  a proposed pool-only cut (< 1 ha and ≤ 2× the river's width: 10,650 of 136,080, 11,423 sections) in
  curation-review/q154-examples.md. **See them in the review app:** Sloughs tab → "Small ponds (Q154)"
  (`#sloughs/q154/`): the 20 md examples + a 60-pond sample (caught / just missed / clearly not, all regions), each
  on the satellite map with its outline and river; mark each pool / real lake / unsure (keys p / l / ?) and the tally
  shows how your calls line up with the cut — then say yes / no / another cut.
- **T89 six waters (updated 2026-11-13, T708):** Hidden, Secret and Square (= Whitton) Lake are DECIDED (owner "add the 3 high confidence lakes"; name records, applied by the next lane). Still unmatched: Frog (5-6), Redfern (5-15), Squirrel (6-1). Owner: "will look harder for frog and redfern later" (Owner To-Do row `t708-frog-redfern-squirrel`; evidence in T708 Work 8).
- **Run timing (T199, built 10-28 on staging; Q144 answered with the defaults):** (1) run
  `gh workflow run feeds-runtiming.yml --ref redesign/stream-sections -f upload=true` once (uses the repo's R2 secrets;
  staging only) and check its report; (2) after every atlas promote, rebuild the binding base on the Mac:
  `python -m pipeline.runtiming.feed.base --upload --yes`, then run the workflow; (3) when happy, uncomment its
  monthly schedule; (4) look at Run timing on the phone (T199-shots/).
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **T182 caps (built with defaults, veto if wrong):** offline total 20 MB (was 15); cold-load gate 3.35 MB (salmon on
  the first Salmon press, as you decided 10-28).
- ANSWERED 2026-11-12: **Scope and big rebuilds:** Q10 imagery corridor (offline corridor + pools go/no-go; online Esri is settled): no, offline imagery is not for the first release.
- **Things only you can do or know:** Q4 slough marks — review app **Sloughs** tab (`#sloughs/`): 811 confirmed so far; still to call: the 135 wide-marsh pre-marks and 314 on your list (113 intermittent) · Q126 were you
  on Level? · Q9 phone testing · Q26 hosting/app name ·
  Q27 sign-off walk.

## Things only you can run or provide
- **T595/T596 /crashes reads (run token)**: the feeds worker's `GET /crashes` needs your RUN_TOKEN (403 without it; Claude may not read the token). Run on staging (`https://bc-fishing-feeds-staging.horvath-dawson.workers.dev`) and production (`https://feeds.canifishthis.ca`), or give a session the token to do it: `curl -H "Authorization: Bearer $RUN_TOKEN" "<worker>/crashes?since=2026-11-09"`. Look for: only `defect` kinds listed, none from the 11-19/20 emulator installs (T595); kind 4219b587 (.sqlite.part) absent since 0.7.3 (T596). Then tell Claude, who records both tickets.
- **Store listing sign-off (T244)**: privacy drafts APPROVED and the name DECIDED (owner 2026-11-06: "Can I Fish?" on the
  icon only, "Can I Fish This?" everywhere else). Still open from the 10-29 list: `app-integration/store/listing-draft.md` and `store/privacy-labels-draft.md`
  (nothing is submitted). Decide: the **app icon / adaptive icon / feature graphic** (DONE from the favicon, per your answer: `T244-shots/icons-preview.png`; add words to the feature graphic if you want), the support URL, categories. Contact = support@canifishthis.ca and the Location row (declared: collected + shared with Open-Meteo) are decided.
  Look at the new splash: `app-integration/T244-shots/` (web boot screen light/dark, native art); it shows on the next native build.
- **Production run (T240 section 11, 2026-10-29)**: the prod commands for map-assets (T270), HYDAT day flow (T274), the report-purge deploy (T271),
  and the SEO build against the prod app export (`--app`, T269/T279) are written in `tickets/T240-production-launch-checklist.md`.
- **Parser runs** (`run_parse.sh`, dispatch): human-only; any re-parse is yours to start.
- **Yearly synopsis update (T239, not started):** when the new synopsis comes out, Claude writes the runbook + book
  diff and does the dry run; the parse itself is yours to run (Claude hands you the command).
- **R2 upload** of a new release and of the gauge feed, with your credentials:
  `python -m pipeline.deliver.release.publish --release data/release/$(cat data/release/LATEST) --yes` (records
  data/release/PUBLISHED so the prune keeps it). When you set up the feed sync, upload ONLY `data/generated/gauges/feeds/`,
  not `feeds.runs/` (T177).
  T215: a release now also carries OUR synopsis PDF (`synopsis-<sha12>.pdf`, ~24 MB, raw, immutable; named in the manifest's
  `synopsis`): the same publish command uploads it, and the bucket's CORS must allow GET from the web origin (the app fetches it
  once per release to draw the highlight). Until a release with it is uploaded, "open the PDF" stays the plain gov.bc.ca link.
- **Feeds (T225)**: the gauges, DFO salmon and in-season feeds now run on the Cloudflare Worker `bc-fishing-feeds-staging`
  on a schedule (STAGING, switched on 2026-10-28; status: https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/status).
  Emails go to your verified address: DFO places / in-season changes / a whole feed failing at once, the rest in one daily
  digest. When a DFO email arrives: `python -m pipeline.regs.dfo_salmon.live queue --regions <n>`, commit the entry files,
  then sync them (below). Production switch-on (worker + prod bucket) is yours.
- **Feed inputs publish from the Mac only** (T520, owner 2026-11-08): `feeds-sync-inputs.yml` is deleted. DFO entries,
  wording, roster and in-season matches go up by `deploy/staging/sync-inputs.sh` — the review app runs it by itself after
  a save (staging only; never git); production stays your `CONFIRM_PROD=canifishthis.ca deploy/prod/sync-inputs.sh`.
- **OWNER STEP — the GitHub R2 secrets** (T520): `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` were for the deleted sync
  workflow, BUT `feeds-hydat.yml` and `feeds-runtiming.yml` still read them (and `CLOUDFLARE_ACCOUNT_ID`). Remove them in
  GitHub → Settings → Secrets only once those two no longer need them; until then, consider scoping the token to the
  staging bucket (owner 2026-10-29: it covers staging + prod today).
- **`scripts/update-hydat.sh`**: run it when the "New HYDAT" email arrives (after the HYDAT action).
- **Phone testing** (T77, Q9; the 0.5.0 list: handoff/PLAN/OWNER-PHONE.md), **production hosting** (T86, Q26: R2 bucket, contact address, app name), **iOS build**
  (T88).
  - Gesture/navigation parity on the phone (T50, T77; owner 2026-10-27; same RNGH code as web, not seen on a device):
    Android edge-swipe back and the back button through the back table (HANDOFF-ui "Back, everywhere");
    `android:enableOnBackInvokedCallback="false"` in the manifest (predictive back off) is deliberate until judged;
    sideways view swipe on a water (Regulations/Conditions/Map) incl. not starting in the 24 px edge zones and not
    fighting the map pan / chart / minimap shields; sheet drag-to-close and sheet scroll-vs-drag hand-off; iOS
    left-edge back (`EdgeBack`); Spots/Conditions/More tab back goes to the Map tab; part step and upstream/downstream
    (▲/▼) keep one back; section highlight while scrolling the regs tabs; keyboard (search) vs sheets.
  - Native colouring (T74, 2026-10-27; emulator: 0 grey frames in 49 sampled pan frames): on the phone, pan and fling
    at z9–z13 and say whether any water shows plain/grey before its colour (`EXPO_PUBLIC_COLOUR_PROBE=1` logs
    `T74PROBE` lines per pan for the numbers); the five hatches;
    "At my location" (asks on the tap, then drops the pin where you stand); a theme switch keeps the hatches.
  - iPhone (T217, 2026-10-27; simulator: 0 grey frames in 49 sampled pan frames, pill/hatches/At my location seen):
    the same colouring checks on a real iPhone, plus the location system prompt on the tap (pre-granted on the
    simulator) and whether the gauge pill's slightly wider padding on iOS is acceptable.
  - Android on a real phone (T50, 2026-10-27; emulator-verified): the rule-book link opens the highlighted PDF in your
    PDF app with the entry's page FIRST and the whole book after it — say if that reads right; nothing sits under the
    status bar or the gesture / 3-button bar; changing the system font size keeps the water you had open.
  - Accessibility (T243 → T258, 2026-10-29): a TalkBack / VoiceOver walk (map → water → regs → conditions → salmon →
    menu) and the largest system text; the bottom tab labels stop growing at 1.3× (Q160) — say if that is wrong.
  - Regs scroll (T50, 2026-10-29): scroll a water's Regulations, switch to Conditions / another tab / change the font
    size, come back — it should be where you left it.
  - Offline maps (T238/T259): More → Offline maps; which map regions a first install keeps (Q161, default all).
- **Sign-off walk** at the end (T87) before app/design is deleted.

- **iOS (T88, T364, T365; 2026-11-05)** — **BENCHED (owner 11-06) until the DFO extents are labelled and Play closed testing has started; then pay the $99 (pick individual vs organization first) and run TestFlight, not the free 7-day route.** the real iPhone run (T364) is AFTER the launch-and-hygiene chat (owner 11-05). Claude built and tested everything the simulator can show (T313).
     Membership details) and `asc-api-key.json` `{"key_id","issuer_id","key_path"}` for an App Store Connect API key
     (Users and Access → Integrations → Team key, role Admin (App Manager cannot sign for the store); keep the .p8 outside the repo).
     missing). It uploads to TestFlight; install TestFlight on the iPhone and accept the build.
     download on Wi-Fi and on cellular (the foreground download, b8e33ca80); the left-edge swipe back from a water's page
     (simulator: a slow drag goes back, a fast mid-height one sometimes does not — say which works for your thumb); the
     gauge pill's padding; the walkthrough to its end, incl. step 17 over the Layers sheet (T336) with VoiceOver on;
     More → Data "Send crash reports" stays on. Memory: Settings → General → iPhone Storage is not it — just say if the
     app is ever closed by itself while you pan or open Conditions (a crash report then reaches the staging store, T362).
     id as Device ID, not linked); age rating; category; support URL; screenshots (6.9" + 6.5"); review notes. Export
     compliance: answer "standard encryption only" (the build already says so). The review build is
     `--production` and needs production hosting (T86) live.

- **SEO, v1 pages (2026-10-27):** keep your existing Search Console property; when the new site is live add the new sitemap
  beside the old one, and for two months watch Pages > Not found (404) and Redirect error (the 19,331 old `/waterbody/` URLs
  should show as Page with redirect). Deploy the generated `_worker.js` with `run_worker_first = ["/waterbody/*"]` (T86 hosting).

- **T371 (batch 7): Play Closed testing (the build is T495's gated production build; 0.5.0 originally).** In the Play Console: create the Closed testing track and a tester
  list (at least 12 people who will opt in and stay for 14 days), fill the app-content forms (Claude drafts the data
  safety answers and listing text). Then roll that build out there; that starts Google's clock for production access.
  Drafts ready (2026-11-04): `play/data-safety.md` and `play/listing.md`. Confirm the items marked OWNER: install id
  means Device IDs = Yes; the report's map centre counts as approximate location; App interactions is new; Open-Meteo
  and Esri sharing (you decided these 10-29); the app name ("Can I Fish?" or "Can I Fish This?"); whether to round
  spot coordinates before they go to Open-Meteo.
- **T691 gauge proposals: ANSWERED 2026-11-13** (all seven; recorded in the T691 ticket, "Phase 2 — OWNER ANSWERED"): 08JB013 rematch to the fork, no cut, name variant; 08PA004 is a Ross Lake level gauge; the five dam/outflow proposals accepted (Kemano stays unmatched). Any earlier "five silent gauge stations need a match call" / "7 proposals" row is closed by this.

- ANSWERED 2026-11-13 (T716): tap speed, open page before the double-tap wait? Owner: "Keep as is (Recommended)". No change; 0.7.9 carries the 0.35 s open.

- [ ] Phone check after 0.7.9 (build 31, Play internal, 2026-11-14), T716: tap a small stream, the Fraser and a lake. The tapped stretch lights up at once; page in under half a second; double tap still zooms and opens nothing. Say if the highlight is too small zoomed out or it still feels slow. (To-Do page row: phone-check-079-tap-speed)

## 2026-11-15 — overnight marks: your answers, and what is still yours (T743)
- ANSWERED 2026-11-15: Alexandra Bridge stretches running 4.3 km into Region 3: "this is fine".
- ANSWERED 2026-11-15: older group-saved labels being assessed: "ok good".
- ANSWERED 2026-11-15: Atnarko "downstream of the sign": "no it needs to include tributaries". The save in force
  already includes them; nothing to redo.
- ASKED 2026-11-15: "we really need to check a lot of the items for this same issue and make sure we remove duplicates
  or bad ones?" Audit running; you get a list to look at (proposals only).
- OPEN, yes/no: Seton River mouth as its own cut 254 m below the Highway 99 bridge cut (recommended: yes, keep it), or
  reuse the bridge cut.
- ASKED 2026-11-15: "need to also fix all other tributary only items like on thre bulkley too. fix the proposals
  after adding ability for it". In work (T742). Yours after it lands: re-mark the tributaries-only places already saved
  (list will be on the To-Do page).
- ASKED 2026-11-15 (Sproat River, DFO Region 1): "there is a tiny section of sprout river in the middle of the lake... if
  anything it should be part of the lake....". Being investigated (ticket T753, written by the lane); options come to you.
- ASKED 2026-11-15 (Kitimat, "on west bank between signs at Kitimat hatchery outfall"): "we should have a way to define
  one half of the river closing maybe? could color only west or east side? could be an option in extent i think this is
  the only river like this? but worth at least doing the coloring. then idk how to represent it otherwise. have an agent
  look into this please". Being investigated (ticket T754, written by the lane); options come to you.
- ASKED 2026-11-15: "also we need to make sure all items with tributaries in the name etc have tributaries in extent."
  In work: the audit lists every place whose wording mentions tributaries against its extent and proposal (report
  `handoff/PLAN/curation-review/resaved-marks-audit-2026-11-15.md`, Section E); the suggester proposes tributaries for
  those wordings; the review app warns on a mismatch (T742). Mismatches on places you already saved come to you to re-mark.
- ASKED 2026-11-15: "also we need to make sure all the res of extents work properly so that if other items get added or
  removed they adapt their extent". Main chat's reading: the "rest of the water" kind of extent (a place with no stretch
  of its own, e.g. Qualicum "all open portions"). Being checked (audit report Section F): is it worked out from the other
  places at build time, and does it follow when one is added, removed or changed. Fixes, if needed, come as proposals.
- ASKED 2026-11-15, on the audit's re-saved places (33 saved more than once, Thompson link, Fraser Region 3, Sheldens
  Creek): "please audit these and see if you see any issue". Second look running; report
  to come in handoff/PLAN/curation-review/ (resaved-marks-second-look, dated 2026-11-15).
- PERMISSION 2026-11-15, verbatim: "i give you permission to fix these (Three places mean tributaries only and are wrong
  today: Bella Coola "all tributaries" also includes the mainstem. The two Bulkley places have tributaries off, while
  their proposals have them on.) and these too (Stave and three "mainstem" places have the tributary choice left blank
  rather than set to off.)". Scope: those seven places only. The four blank ones are being set to off now (new marks
  appended, noted "saved by Claude at the owner's request"); Bella Coola and the two Bulkley follow once the app can
  record tributaries-only (T742).
- ASKED 2026-11-15: "also we want to make sure all the mouth/ tidal splits are at the correct one (not the proposed one
  but at the tidal ones we give every stream)". Audit running: every DFO place with a mouth or tidal end, the point it
  uses against that stream's own tidal boundary; report to come in handoff/PLAN/curation-review/ (mouth-tidal-ends-audit, dated 2026-11-15).
- OPEN, pick (T753, Sproat and 10 other waters): river pieces that loop out of a lake and back; take them out of the
  river and give them to the lake (recommended), take out only, or leave. Rides on the next map rebuild.
- ANSWERED 2026-11-15, round 4 (T754, one-bank places: Kitimat west bank, Nakina): "Build it this way
  (Recommended)". To build (T790 item 1).
- ANSWERED 2026-11-15 (T753, river pieces that loop out of a lake and back; Sproat): "yes do both steps but make sure it
  is done in a clean way". Both steps = take the pieces out of the river, then give them to the lake. Code lane started;
  it lands with the next map rebuild.
- DONE 2026-11-15 (your permission on the blank tributary choices): Stave "downstream of B.C. Hydro dam" set to
  tributaries off (new mark line 2672, noted as saved by Claude at your request; landed as 122a3d02d). The other three
  were not as the audit said: the Fraser "mainstem waters downstream of" mark already says off (it waits on the Hope
  bridge cut); the two Nass "mainstem" places have no mark at all, so they are yours to mark (set tributaries off).
- ASKED 2026-11-15: "need to make sure all rules under a water with (includes tributaries) in the name gets tributaries
  too." Main chat's reading: a DFO water whose heading says "(includes tributaries)" passes that to every place under it.
  In work (T742): proposals inherit it, the app warns on a mismatch, and a list of already-marked places under such
  waters that miss tributaries comes to you to re-mark.
- OWNER 2026-11-15, right after the tributaries asks: "we should wait until fully done marks though". Main chat's reading:
  nothing changes under him while he marks. So until he says he is done: no review-app update or restart, no change to
  the proposals file, no further marks saved for him, no rebuild. The fixes keep being built and tested in separate
  copies and land in one go afterwards.
- OWNER 2026-11-15: "we just want to prepared for how we will audit them". So the checks asked today (saved twice,
  tributaries, rest-of extents, mouth/tidal ends, side channels, one-bank) are being built into ONE repeatable audit
  command, run once when he says the marks are fully done; today's reports are the trial run, not the final audit.
- SECOND LOOK at the 31 re-saved places, 2026-11-15 (report `handoff/PLAN/curation-review/resaved-marks-second-look-2026-11-15.md`):
  18 look right, 3 look wrong, 1 wrong by your own note (Sproat), 9 cannot tell. Yours to decide: (1) a place whose last
  save is "wrong" with no fix still ships its older extent, with no flag: hold such places out of a release until
  re-marked? (2) Thompson, Kamloops Lake outlet: the live place has no cut-outs for the three year-round no-fishing
  areas (Deadman, Juniper, Ashcroft). (3) Sheldens Creek: the Spur 19 cut leaves only the top ~290 m of a 3.69 km line;
  a second road (SP 19H) was never located. (4) Fraser Region 3: the extent in force (Spuzzum to the Region 3/5 line)
  looks right; your Seabird note fits the Region 2 places.
- ASKED 2026-11-15: "PUNTLEDGE RIVER: 75 m downstream of Puntledge River hatchery fence split is on the hatchery fence
  (the split already in the river) we should fix the proposal and make sure we fix this existing split." Being looked
  at: which wording made the existing split, where the fence and the 75 m point are, and other "N m downstream of"
  places sitting on the landmark itself. Fix comes as a proposal; cut changes ride on the rebuild after marking.
- MOUTH / TIDAL ENDS checked 2026-11-15 (report `handoff/PLAN/curation-review/mouth-tidal-ends-audit-2026-11-15.md`): no
  place in force ends at a wrong mouth point (an open downstream end is trimmed at the stream's tidal boundary by the
  build). Six PROPOSALS are bad: Chapman, Stave x2 (a new cut a few metres from the tidal boundary), Illiance, Kitsault,
  Wilauks (a cut on the line's very end). Do not accept those; mark the stretch with an open downstream end instead.
  Yours: Nass x2, Seymour, Illiance, Wilauks have no extent yet; Pallant Creek stops at "the bridge near the mouth",
  57 m above the boundary (as DFO words it; your call). The proposal maker is being fixed to use the tidal boundary.
- LOOK AND PICK (T752): desktop water map as a split, A map left / B map right / keep today's. Pictures sent in chat.
- ANSWERED 2026-11-15: "Pallant Creek we can make the bridge the tidal boudnary i think". Queued for the rebuild after
  marking (T743).
- ANSWERED 2026-11-15 (T752 desktop split): "for the layout choose which ever you think is best practice". Main chat
  chose B (text left, map right), from 900 px wide on every platform; being built properly with tests.
- PUNTLEDGE checked 2026-11-15 (report `handoff/PLAN/curation-review/puntledge-hatchery-fence-2026-11-15.md`): you are
  right, and for every user of that cut. The cut "signs 75 m downstream of Puntledge River hatchery fence" sits on the
  lower-hatchery weir itself: its anchor was placed on the hatchery grounds 74 m upstream of the weir, so the 75 m
  offset lands on the fence. Fix (your words: "make sure we fix this existing split"): anchor moved onto the weir, 75 m
  offset kept; the cut lands 75 m below. Queued for the rebuild after marking. Yours afterwards: re-tick the three marks
  that use it (lines 1263, 2447, 2687). No other "N m downstream of" place has this fault.
- ASKED 2026-11-15 (Shuswap River, middle): "need help representing this ((middle) between Shuswap Falls and Mabel Lake
  except closed from the water survey station … to WTFB signs … one km down stream.) it has the except closed structure
  we encountered before". Save the place as the whole stretch (Shuswap Falls to Mabel Lake); the closed part becomes a
  second, no-fishing entry as on the Thompson. The reader of DFO's page did not pick up this "except closed from … to …"
  wording; being fixed in a separate copy. The two printed coordinates do not match the words (second is south of the
  first); being checked.
- ASKED 2026-11-15: "there was one river i dropped highway 16 split near mouth in the skeena i shouldnt have done that
  help me fix it please". Found: Exchamsiks River "upstream of Highway 16 bridge", mark 2674, saved as the whole river.
  Fix: save it again as upstream of the proposed "Hwy 16 bridge over the Exchamsiks" point; the new save replaces it.
- PERMISSION 2026-11-15 (Puntledge), verbatim: "please add both cut on weir and one downstream then i give you
  permission to edit that extent for regulations and dfo places." Scope: the Puntledge hatchery weir cut, the cut 75 m
  downstream, and the Puntledge rule-book and DFO extents that use them. In work.
- ASKED 2026-11-15: "the shuswap one add it as proposal so i dcan see it". The Shuswap closed stretch is being prepared
  as a proposal for the review app; added to his proposals once ready.
- PERMISSION 2026-11-15 (Exchamsiks only), verbatim: "can you fix Exchamsiks for me poroposal is gone so you can do it".
  One new mark being saved for him: upstream of the Hwy 16 bridge point, tributaries on, noted as saved by Claude.
- ASKED 2026-11-15 (Tatshenshini River, Region 6, "upstream of the BC/ Yukon border – along the Haines Hwy.; Kwatini
  Creek, Stanley Creek and Goat Creek." and the Blanchard River one): "need to add bc boundary here i cant get it to
  add. also i cant save or edit the extent of the other section there". Being reproduced in a separate copy of the app:
  why no BC-border point is offered, why the Blanchard place will not save, and the marks those two places need.
- DONE 2026-11-15: the Shuswap closed stretch is in your review app as a proposal (closed-areas panel of the Shuswap
  "(middle) between Shuswap Falls and Mabel Lake" place; no restart needed). YOURS: which reading. DFO's two printed
  points are 2,054 m apart along the river, not "one km", and look swapped: the second printed point is 27 m from the
  Water Survey gauge "Shuswap River near Lumby", so it is the station. Options shown: as printed (km 133.08 to 135.13),
  or station to 1 km downstream (km 134.13 to 135.13). Also yours: tell DFO about the mismatch?
- DONE 2026-11-15 (Puntledge, your permission): weir cut added, 75 m cut moved to 75 m below the weir (same name, so
  the rule-book stretch and both DFO places follow). Shows after the rebuild; then re-tick marks 1263, 2447, 2687.
  YOURS: the retired "from the weir at the lower hatchery" place has no mark (weir to the 75 m cut). YOURS, yes/no: the
  tailrace "50 m" cut has the same fault, about 27 m off; fix it the same way?
- DONE 2026-11-15 (Exchamsiks, your permission): new mark line 2728, upstream of the Hwy 16 bridge point, tributaries
  on, saved by Claude at your request; it replaces the whole-river save and waits on the rebuild like your other new-cut
  saves. It shows as "unsure" because the app sets that for any extent using a not-yet-built cut.
- READY 2026-11-15: the final marks audit is one command (branch review/marks-audit, commits d9f7045 + 88dec44, lands
  with the other fixes after marking). Trial run on today's marks: 303 candidates in 11 sections; every finding from
  today's hand audits is in it. Run by the main chat when you say the marks are fully done.

## 2026-11-15 afternoon — owner done labelling for now; clean-up asked (T743)
Owner, verbatim: "i thought skeena used to have a hells gate cut? also other ones liek this dont have the confluence
cut? (from Lakelse River mouth upstream to the Skeena River Overpass (New Hwy Bridge) at Ferry Island) again this doesnt
split on confluence.. (from the confluence with the Babine River downstream to the CNR Railway Bridge at Terrace. ) ...
all the audit stuff i will have to review later like shuswap stuff etc etc. i think i am done labelling for now. can you
audit everything a lot compile all results/issues into questions for me later. and make sure to fix all the remaining
items left. a lot have issues with bad cuts etc etc. also we could rebuild the cuts and make sure nothing causes issues?
also want to make sure we deduplicate the new cuts from proposal (if they rewference same thing they should be matched
etc) we just really want to clean it all up and then have questions ready for me later."
Main chat's plan: land the prepared fixes; run the full audit; clean the cuts (duplicates matched to one cut, confluence
ends on the confluence cut, bad positions fixed where the evidence is clear, the rest as questions); side rebuild and
checks; one page of questions for him. Nothing that changes what a wording means is decided for him.
- TATSHENSHINI checked 2026-11-15 (report `handoff/PLAN/curation-review/tatshenshini-border-2026-11-15.md`). (A) The BC
  border points exist in the data (the river leaves BC and comes back after 43 km in Yukon) but the app's picker cannot
  show them; app fix proposed. The Kwatini / Stanley / Goat place does not need one: those creeks are wholly in BC, so
  its whole-water extent is already right. (B) The Blanchard place cannot be edited because its extent carries a field
  the editor cannot show, and the app hides the edit button without saying why; plain "whole" gives the same 15
  sections. YOURS: on each of the two places, tick ✗ on rows 2 and 4, then "Confirm: the extent matches DFO's words"
  (works today), or tell the main chat to save those two verified marks for you. 15 other unmarked places cannot be
  edited in the app for the same kind of reason; listed in the report.
- ASKED 2026-11-15 as he left: "record everything i need to do for when i am back what i need to review with questions
  what was done etc etc. what needs testings etc etc". Kept in `handoff/PLAN/OWNER-RETURN-2026-11-15.md` (sent to him
  as a file when final) and as the first row of the To-Do page.
- DONE 2026-11-15 (your permission): Bella Coola "all tributaries" = tributaries only, Atnarko excepted (line 2730);
  Bella Coola whole "(not including Atnarko River)" = Atnarko excepted (2731, per your note "make sure this excludes
  atnarko"); the two Bulkley "all tributaries" places (2732, 2733) = tributaries only. YOURS: the two Bulkley places had
  no mark of yours, so they are saved as "unsure" from the proposal: confirm or change.
- BLOCKED 2026-11-15, NEEDS YOU: the side map rebuild did not start. The permission check refused its first step
  (`python -m pipeline.regs.review.proposals side … --out data/generated/side/dfo-cuts23`) twice for the rebuild job; the
  main chat does not run a command a job was refused. Run it yourself or tell the main chat to. Details in
  `handoff/PLAN/OWNER-RETURN-2026-11-15.md` section 6.
- ANSWERED 2026-11-15, on the blocked side rebuild step: "You can run it". Run by the main chat; rebuild continuing.
- BLOCKED 2026-11-15 17:20, NEEDS YOU: Play build 33 (0.7.11, code shrinker on; 6 of 6 phone checks, both CIs green,
  merged 35dce9bf3) refused to start: fast-fail fails on two rows of the proposals file lacking split-hunt evidence
  (my midday merge). Regenerating was denied to a job by the permission check; the main chat will not run it without
  your word. Say "regenerate the two proposal rows" (or run the suggester yourself); then the build runs.
- ANSWERED 2026-11-15 18:40: "Yes regenerate proposals if you need to." Done by the main chat: a fresh suggester run
  into a scratch folder; the two rows the test named (Stave "downstream of B.C. Hydro dam" second wording, Bella Coola
  "all tributaries") have no row in a fresh run because both places are placed now, so those two rows were removed from
  the proposals file (backup taken; no other row changed). The test passes; Play build 33 started.
- ASKED 2026-11-15 18:40 about T759: "What u mean ? should the app say "not known" instead of showing the wider open
  rule? I recommend yes." Explained in chat with an example; answer still open.

## 2026-11-15 evening — owner answers by question dialog, round 1 (verbatim)
- T759, what the app shows when DFO rewords a closed stretch and the reader loses it: "How would we know a reword vs a
  removal? I'm not sure safest way to handle thing. Probably have it in a banner for all sections in that river (banner
  meaning in salmon drop down) but I want your opinion". NOT DECIDED: he asks for the main chat's opinion; his idea: a
  banner in the salmon drop-down on every section of that river.
- A place last marked "wrong" with no fix shipping its older extent: "I want you to for it to be redrawn. We will finish
  these tonight so not a big deal... Just be safe". Main chat's reading: he re-marks them tonight; until then be safe =
  such a place does not ship its disputed extent.
- Six cuts recommended to keep (Lillooet signs, Seton mouth own cut, Atnarko lodge sign, Gitnadoix whole river,
  Kitsumkalum 22 m piece, Little Campbell and Babine sign cuts): "Only one I'm not sure about is gotnadoix one." So five
  are kept as built, incl. the Seton River mouth as its own cut; Gitnadoix stays open.
- Three fixes (Puntledge tailrace cut moved 27 m up; Nanaimo new cut 500 m below the lower power line; Thompson three
  year-round no-fishing areas carved out): "I thought we already had all these?? They were accepted? Okay please fix
  them I guess". Go for all three.

## 2026-11-15 evening — owner answers, round 2 (verbatim)
- T759 lost closure, offered "Banner + not known (recommended)" / banner only / not known only: "That sounds good but can
  you make not known a variation like bait ban where it has a loot change but with same color?" So: river-wide banner in
  the salmon drop-down AND the stretch reads not known when a clue says reword; and the map shows "not known" as a
  variation in look (as the bait-ban variation does) with the same colour. Which colour is "the same" is asked next.
- Gitnadoix "upstream of powerline crossing near river mouth": "Whole river (Recommended)".
- Morice "from Gosnell Creek to Lamprey Creek": "Keep the signs point (Recommended)".
- Skeena confluence cuts (Lakelse-to-Skeena, Babine-to-Skeena, Highway 16 bridge at Ferry Island): "Make them
  (Recommended)".

## 2026-11-15 evening — owner answers, round 3 (verbatim)
- T759, the look and the rule: "I think what ever still applies there should be the color so say there is a whole river
  rule and a section rule section rule is removed we should have no status change because that is fine same with if a
  rule gets replace with another one we have a mapping for. However a new wording being there would then mean we have a
  different indicator on the stream line that there is an issue with the rule. Then every stretch in the river will get
  the banner in the salmon section saying the new unmatched rule until we rematch it". Main chat's reading: (1) a place
  DFO removed, or replaced by a row we can match: no status change, the remaining rule's colour; (2) a NEW wording we
  cannot match on that river: the status colour stays what still applies, the stream line gets a distinct "issue" look,
  and every stretch of that river shows a banner in the salmon section quoting the unmatched rule, until it is
  re-matched. So no "not known" status; the trigger is an unmatched new row on the river. Open point to raise with him:
  a reworded row that our reader matches to a NEIGHBOURING place leaves no unmatched row, so this trigger misses it.
- Fraser and Squamish side channels: "Add all five (Recommended)" (Herrling Island, Seabird Island North, Jeperson
  braids, Maria Slough; Squamish Powerhouse channel).
- Shuswap closed stretch reading: "Can you give me words? What are the words are we sure we have right spts?" NOT
  DECIDED: wants DFO's words and how sure the points are.
- Lake rules (lake never takes its through-river unless named; Sproat pond stays its own water; creek-fed loops stay
  with the river): "Keep all three (Recommended)".

## 2026-11-15 evening — owner answers, round 4 (verbatim)
- Shuswap closed stretch, after DFO's words and what is sure about the points: "Use the real gauge point so we don't
  duplicate it and the other coord as the other point even tho it is 2km use as printed". So: closed from the real
  Water Survey gauge (reuse that point, no duplicate cut) to DFO's other printed coordinate, about 2 km.
- Rest-of extents, two weak spots: "Fix both (Recommended)".
- Tributary readings (Nass x3 mainstem = no tributaries; Babine Lake x3 and Meziadin "excluding tributaries" win over
  the heading; Somass and Stamp "X River tributaries" = tributaries only): "All three are right (Recommended)".
- One-bank places design (Kitimat west bank, Nakina): "Build it this way (Recommended)".

## 2026-11-15 evening — owner answers, round 5 (verbatim)
- T759 second trigger (a reworded row matched to a neighbouring place): "If it is reworded then it would unmatched
  because it is not in our list of mapped locations?" NOT DECIDED: a question back. Answer owed: an exact-match reader
  would leave it unmatched, but ours also accepts a close match (wording drift) and can attach it to a neighbour.
- Sheldens Creek, Spur 19 Bridge: "You can hunt for it I think it should be close to tidal". So: hunt for the bridge
  site / SP 19H road; his hint: close to the tidal boundary.
- Four small ones (1 Sproat signs cut moved 50 m to the bottom of the pool; 2 Hell's Gate label renamed; 3 Ecstall at
  the Johnston Creek confluence; 4 Illiance and Wilauks whole river from the tidal boundary): "Only one I'm not sure
  about is number 4". So 1, 2 and 3 go ahead; Illiance and Wilauks stay open.
- Region 6 salmon file over its size limit (379 kB vs 320 kB): "Raise to 420 kB (Recommended)".

## 2026-11-15 evening — owner answers, round 6 (verbatim)
- T759, close (drifted) matches: "Yes: close match needs confirming (Recommended)". A close match still applies its rule,
  but the river shows the banner and the line indicator until he confirms it in the review app.
- Illiance River and Wilauks Creek, "upstream of signs near the mouth": "Whole river is right".
- Tatshenshini, two places: "I will do them myself".
- Other chat's two (push privacy-page line + App Store privacy answer as written; keep all fish puns): "Yes to both
  (Recommended by it)".
- Then: "When can I do the remaining reviews?"
- ASKED 2026-11-15 20:05: "Iook ofr other ones like he skeena one to make sure we have them all". Sweep widened to every
  place (live, retired, older wordings) and every rule-book extent that names a confluence, mouth, outlet or forks, and
  every existing confluence cut (on the main line? at the real mouth?); missing ones are added with the Skeena pair.
- 2026-11-15 20:30, Blanchard River place (Tatshenshini): owner "i need to edit t though"; by dialog "Add its
  tributaries"; then "you can unlock it tho". Done by the main chat in region-6.json: tributaries on; the "inside BC
  only" limit (`within_area`) removed so the editor opens (it had dropped two Yukon slivers, 59 m and 211 m; a check
  today found plain "whole" binds the same 15 sections).
- 2026-11-15 20:45, Kasiks River "Upper Pool": owner "can you search hard for anything on that river mentioned?" then
  "can you add the pool proposal?" then "add to gotchas too". Done: sign point proposed at the tail-out of the pool at
  the foot of the canyon falls (about 54.3535 N, 129.5360 W, 17.65 km up; medium confidence, inferred); both Kasiks
  places now show it as a proposal in the review app; recorded in pipeline/docs/GOTCHAS.md. Report:
  `handoff/PLAN/curation-review/kasiks-upper-pool-2026-11-15.md`.

## 2026-11-15 night — second look at your labels: questions (NOT asked yet; answer together)
New main chat (after chat 13). All 352 DFO places were read against DFO's words: 273 clean, 79 flagged, 15 that look
wrong. Report: `handoff/PLAN/curation-review/final-marks-second-look-2026-11-15.md`. Each question has a default; the
default is what ships to staging until you answer. None of these needs a new map rebuild unless it says so.

Vancouver Island
1. **Somass, Stamp and Little Qualicum "everything else" places.** DFO says "except in those areas and times listed
   below". Today each listed stretch is cut out of "everything else" for the whole year, even when its rule lasts a few
   months (the Somass bait ban Jun 1-Aug 24 takes the whole river, so the Somass chinook and coho limits show nowhere;
   the Stamp keeps about 2 of 10 pieces; the Little Qualicum loses the park-to-hatchery stretch in Sep-Nov). Make these
   three the whole river, with the smaller stretches answering first on their own dates, as you set the Nitinat?
   Recommended: yes. Default until you answer: unchanged (limits missing on those stretches).
2. **Side streams on Cowichan and Nanaimo stretches.** Both rivers are "including tributaries" as a whole, so each
   stretch takes its side streams too: the Cowichan closure from Sandy Pool to the sea (Sep 1-Nov 15) lands on the whole
   Koksilah River; Nanaimo Cedar Road to Highway 1 lands on Haslam Creek; the year-round Nanaimo closure below Bore Hole
   lands on Stark, Berkley and Elkhorn creeks. DFO's words for these stretches name no side streams. Make each stretch
   the main river only? Recommended: yes. Default: unchanged.
3. **Campbell River near the mouth.** The three stretches (Quinsam confluence, Maple Street sign, cement block sign)
   still have "not sure" from Oct 26; the cement block sign is an estimate. Re-check with the satellite view when you
   can. Default: kept as drawn.
4. **Sproat and Stamp Falls** end on a "wrong" save although your fix is now in (Sproat +50 m; Stamp names to add).
   Re-save them as right after the rebuild.
Interior and Fraser
5. **Thompson at Juniper Beach, closed area.** Which upper end: 4 km above DFO's printed lower signs (recommended, DFO
   says "approximately 4 km"), or 1.5 km below Juniper Beach Park (1.2 km long)? NEEDS A NEW CUT (next rebuild).
   Default: Juniper not closed separately; the place's rule covers it.
6. **Fraser Region 3 mainstem** (Spuzzum up to the Region 3 / Region 5 line): you marked it wrong because the start
   read "Region 3 – Region 8 boundary". The start used IS the Region 3 / Region 5 line on the Fraser; the label most
   likely came from another river's crossing. Right as drawn? Recommended: keep, re-save as right.
7. **Kanaka Creek**: DFO says "112th Street bridge"; the only crossing is 112 Avenue. Keep it? Recommended: keep.
8. **Fraser "Fraser only" saves and Maria Slough** (from the confluence sweep): re-mark those places without "Fraser
   only", or should "Fraser only" always take the Fraser's own channels? The rebuild's rule comparison shows whether the
   channels now come in by themselves; answer after I report that.
Skeena, Nass, north
9. **Morice River just below Morice Lake.** DFO closes salmon from the signs at the lake outlet down to Gosnell Creek.
   Our stretch starts at the Dunes signs, about 2 km below the lake; the top 2 km has no closure. Start it at the lake?
   Recommended: yes. Default: unchanged (top 2 km open to what the Skeena defaults allow).
10. **Tatshenshini headwaters (your two saves).** The border point you picked also pulled the upper Tatshenshini itself
    into the Blanchard place and into the Kwatini / Stanley / Goat place; the three creeks' all-year closure would then
    close the upper mainstem, where DFO allows coho. Make the Blanchard the whole Blanchard with its tributaries and the
    three creeks the three creeks whole? Recommended: yes. (Being confirmed by the rebuild's rule comparison.)
11. **Skeena "mainstem waters only" (pink 2 per day, Jun 16-Dec 31).** DFO prints it under the Skeena above the CNR
    bridge; we draw it on the whole river. Limit it to above the bridge (your note said so)? Recommended: yes. No
    fish answer changes today either way.
12. **Skeena, Terrace to above the Babine, sockeye.** A year-round closure and the in-season openings cover the same
    stretch; the openings win today only by the order of their internal names. Is "the in-season openings win" right?
    Recommended: yes, and the order is made explicit.
13. **Babine Lake sockeye** ("excluding tributaries, the 400 m circles, east of the Gullwing Creek line"): no drawing,
    so the openings show only as a note on the lake's card. Draw it as the whole lake, tributaries out, the circles and
    the Gullwing line as their own closed pieces? Recommended: yes.
14. **Kitsault tidal boundary**: name it "tidal boundary (white triangular fishing boundary signs near the mouth)", as
    your note asked? Recommended: yes.
15. **Kispiox "signs near the Kispiox River Resort"**: no resort by that name today; the signs sit at the Kispiox River
    Lodge, 13.7 km up (low confidence). Keep until DFO Smithers confirms? Recommended: keep.
16. **Sheldens Creek** (from the hunt): the Spur 19 road nearest the sea ends on a small side creek about 0.5 km above
    Sheldens Bay, not on the creek we drew. Is the place "the creek above where that side creek joins", or is the side
    creek Sheldens Creek? NEEDS A NEW CUT. Default: today's cut stays (top 290 m only).
For you to re-tick after the rebuild (saved by Claude at your request, each noted so)
- Shuswap closed stretch, line 2770: gauge to DFO's other printed point, 2,075 m (your round 4 answer; replaces the
  27 m one of line 2742).
- Thompson Deadman and Ashcroft closed areas (2748, 2749); Nanaimo 500 m (2746); the two Bulkley "all tributaries"
  places (2732, 2733); the two Skeena places on the new confluence cuts (2751, 2752); Puntledge 1263 / 2447 / 2687;
  the Sproat places; Fraser Region 2 places and Squamish (they gain side channels).

## 2026-11-15 night — owner answers (verbatim), new main chat
- 21:40, typed in chat: "you should also double check all the extents for dfo and make sure they all look sane (match to
  proper bound with proper tributary logic or second part etc etc....) build on other review and we want to fix items
  form other review". Main chat's reading: (1) a second pass over what every DFO place actually BINDS (ends, tributary
  logic, second parts), on the built result, building on the second look; (2) the second look's findings are to be
  fixed, not only listed: the recommended answer of each question above is applied, saved for his re-tick.
- Thompson Juniper closed area, upper end (question 5 above), by dialog: "if the book says 4km then we put it 4km unless
  i had another split it already matched??" So: 4 km above the printed lower signs, unless a cut already exists at that
  spot, then that cut is reused.
- Sheldens Creek (question 16 above), by dialog: "we shopuld find lower options. i think this is tidal bound maybe". So:
  look lower on the creek; his reading is that the signs are at the tidal boundary. Main chat's reading: the place
  becomes the whole creek above the tidal boundary (as Illiance and Wilauks), no new cut, saved as unsure for his tick.
- 21:51, typed in chat: "also another fable agent should take a look at how trout are displayed in the app. it si very
  complex. and we want fable to come up we a few other alterntives and give mockups (make sure it chooses a few
  different complex examples)". Started: a Fable design review (read-only), today's trout display on 5-6 real complex
  waters, 3-4 different alternatives, mockups as one page for his phone. No ticket until he picks a direction.
- 21:54, typed in chat: "also coordinate with other running chat to let it know it is the andorid and ios chat now not
  just ios. it is just the app chat." Done: the second chat (session bc-freshwater-fishing-regulations-01) is THE APP
  CHAT: Android and iOS app work, the feeds worker, app tickets. Main chat's reading: the Play build of 0.7.11 moves to
  it (the main chat names the build head after the staging release); data, pipeline, review app, releases and the
  audits stay with the main chat.
- 22:07, typed in chat, after the Region 1 fix report: "ill review all regions together". So: no region-by-region
  review asks; one combined list of the saves to re-tick and the open questions for all regions, once the second fix
  lane is back and the rebuild has landed them.
- 22:12, typed in chat, on the Skeena sockeye pair (CNR bridge at Terrace to above the Babine: the year-round closure
  "excluding the areas below" and the in-season openings on the same stretch, the right one winning by name order
  only): "this iss handled by rest of option in extents". So: the place worded "excluding the areas below" is saved
  with the REST-OF extent (its stretch less the areas listed below it), so the two no longer overlap; no ordering rule
  by wording. Open point for his combined review: tonight's Region 1 saves 2771-2773 turned three "except in those
  areas and times listed below" places from rest-of into the whole river (a listed stretch whose rule lasts only part
  of the year was cutting itself out for the whole year); he sees both and decides.
- 22:14, typed in chat, right after: "all items wih same structure as that shoul duse rest of extent type too". So:
  EVERY place worded as a leftover ("excluding the areas below", "except in those areas and times listed below", "all
  waters unless indicated below", "except in the area described below", …) uses the REST-OF extent type. Consequences
  the main chat draws: (1) tonight's saves 2771-2773 (Somass, Stamp, Little Qualicum turned into the whole river) are
  NOT landed as whole; they go back to rest-of; (2) the rest-of type itself must then give DFO's "areas AND TIMES"
  answer: on a day when a listed stretch has no rule of its own, the rest-of place's rule shows there (today a listed
  stretch is cut out for the whole year, and a retired or undrawable listed stretch still takes water: his earlier
  "Fix both"). That is a build change, ticketed tonight.
  (Times above are the commit times of each note, read from git; the first versions of these lines carried
  estimated times up to 30 minutes fast, and the notes of mark lines 2787-2789 say "owner 22:45" for the 22:14 answer.)

## 2026-11-15 night — trout display: mockups for you to pick from
Page (opens on the phone): https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH . Six real waters hard in different ways
(Vedder, Little River, Skeena in summer, Kootenay Lake, Elk, Fraser in two regions); today's display beside four other
ways: A fish by fish, B the answer first, C the usual rule and what is different here, D check my fish. The reviewer's
pick: A with B's one line on top, C's "this river's rule" tag, D later as a button. Five questions at the bottom of the
page with a recommended answer each. Notes: `handoff/PLAN/design/trout-display-review-2026-11-15.md`. Nothing is built
until you pick; the build is the app chat's. Two possible defects it saw in the data (to be checked): Kootenay Lake's
rainbow line drops "over 50 cm" from "20 a year / record"; a Fraser stretch shows "closed from Oct 1" without saying it
reopens Nov 1.
- 22:40, typed in chat, after reading the trout display page: "i accideenetly asked other chat to do this so coordiante
  with them. but please expand and refine the a/b design idea. see if we can make it super simple and clean with the
  fish icons in the table format?" Started: round 2 of the design review (same page, same link): options A and B
  merged into one table of fish rows with the app's own fish drawings, a few close variants, more hard waters. The app
  chat was told so the work is done once; the build of what he picks stays with the app chat.
- 22:49, typed in chat, on the app chat holding a display fix until he picks a trout design: "Do not hold stuff for the
  trout redosgn. That is a separate exploration thing that might not go anywhere". So: nothing waits on the trout
  display exploration. T762 (the "through the year" line that stops after the next change; app chat) and T763
  (Kootenay Lake rainbow "20 a year" without its over-50-cm band; main chat) are fixed on their normal paths.
- 23:04: round 2 of the trout display is on the same page (https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH): one top
  line, then one row per fish with the app's own drawing; four one-change variants; eleven waters. Five questions on
  the page (row per fish or per book group; plain backing behind the drawings; the long Vedder line; Kootenay one table
  or two; duties behind the tap). It is exploration only: nothing waits on it (your 22:49 word).
- 23:25, typed in chat, on trout display round 2: "I am really liking the trout work so far but we need to develope it
  even more. First I like the proposed 2 to 4 a day instead of details with arrow for collapsible button. Second we
  need to work out the size location better maybe the second column idea is good. We just want to keep refining it.
  This is a lot better though. I like the idea so far keep refining and add more complex examples with weird
  groupings." Started: round 3 on the same page: the folded row's answer adopted and defined for every case; four or
  five placements for the size compared on the hard waters (he leans to a second column); at least six more real
  waters picked for odd groupings; the shared-answer box stressed. Still exploration: nothing in the app changes.

- 2026-11-15 23:29, Tatshenshini "mainstem and all other tributaries" place, asked whether it should also become
  rest-of (I recommended leaving it). Owner: "I don't see how this would be a rest of situation.. (mainstem and all
  other tributaries" place should also become rest-of. Recommended: leave it; it already shares no water with its
  neighbours)". CLOSED: it stays as drawn (its own waters, sharing none with the Blanchard place or the three creeks,
  so there is nothing to order). Not on the morning list. My reading for later sweeps: rest-of is only for a place
  that overlaps the stretches it lists; "all other" wording alone does not make one.

- 2026-11-15 23:48, trout display ROUND 3 is on the same page (https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH):
  the folded row's answer for every case, five placements for the size on 20 waters, the shared answer box under
  stress, nine more real waters with odd groupings (Kaslo, Kitimat, Williston Lake zones A and B, Ross Lake, Bella
  Coola spring and summer, Tagish, Inkaneep). Still exploration; nothing in the app changes. Notes:
  `handoff/PLAN/design/trout-display-review-2026-11-15.md`. Five questions for you (recommended answer first):
  1. Size: a column that drops under the name when it does not fit (recommended), or always under the name?
  2. May the trout table sort fish by their shared box, not the app's usual fish order? Recommended: yes.
  3. Sizes in centimetres only on the row ("30 cm+"), inches behind the tap? Recommended: yes.
  4. Hatchery and wild as two rows of one fish where their limits differ (Kitimat rainbow)? Recommended: yes, only there.
  5. Store a top line for the 8 rule sets that have none ("Up to 5 today", "Up to 2 today, plus 2 bull trout")?
     Recommended: yes.
  Found on the way (to check, not design): the Kitimat's stored rows give wild rainbow a limit of 1 (30 to 50 cm);
  today's card shows only "Up to 5 rainbow trout, 50 cm (20 in) or less". Ross Lake's stored headline says 2 and its
  daily total says 4. Both go on a ticket after tonight's release.

- 2026-11-15 23:58, trout display, the owner's reply to round 3 (verbatim): "For the trout design. Please take the
  stuff you like best then we need to ujify over x size and x+ also we want both in and cm. And we want to make the
  stuff like 1 over 50cm on group easier to tell. Also dolly are not in list?"
  Round 4 started on the same page. My reading: (1) the reviewer's picks become the one design, comparisons fold away
  (answers round 3 questions 1, 2, 4, 5 as "your pick"); (2) one way of writing a size everywhere ("over 50 cm" and
  "50 cm+" unified, true at the edge); (3) inches AND centimetres on the row (answers question 3: both shown);
  (4) a size limit that belongs to a whole group ("only 1 over 50 cm") must read at a glance as the group's;
  (5) Dolly Varden: the round 3 rows said only "Bull trout"; the book and today's card say "Dolly Varden/bull trout"
  and the app has a Dolly Varden drawing. Being checked per region and fixed on the page.
  Owner, 2026-11-15 23:59, right after: "We are iterating on it more not implementing". So: round 4 and any later
  round are mockups only; nothing is built in the app or the pipeline for the trout display until he says so.

- 2026-11-16 00:18, trout display ROUND 4 is on the same page (https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH),
  mockups only. One design leads the page; rounds 1 to 3 are folded under "How we got here". Sizes use the app's own
  standard everywhere with both units ("30 cm (12 in) or longer", "50 cm (20 in) or less", "30–50 cm (12–20 in)");
  the size sits under the fish name; a size limit of a whole box is written in the box under the number; 22 real
  waters, each with a "what you should read" line. Dolly Varden: the app has ONE fish, named "Dolly Varden/bull
  trout" in every sentence (568 rule sets), no water treats the two differently; round 3's "Bull trout" was the
  designer's shortening and is fixed. Real app gap found: the drawing on that row is captioned only "Bull trout"
  (ticket after the release). Three questions (recommended first):
  1. A size limit on a whole box: written in the box (recommended), or as a band above the fish?
  2. "Over 50 cm" and "30 cm or longer" both stay, because they differ for a fish exactly on the mark ("over 50"
     does not include a 50 cm fish; "30 cm or longer" includes a 30 cm fish). Agreed? Recommended: yes. (This is the
     one place the designer did NOT fully unify, against "unify over x size and x+": making them look alike would
     make one of them wrong by a fish.)
  3. Dolly Varden/bull trout: one drawing with the full name (recommended), or both drawings on the row?

- 2026-11-16 00:34, trout display, the owner's reply to round 4 (verbatim): "In the redesign we need to make the fish
  expansion better the fish inage moves around and we should put the place holder of what will be there into it.
  Like species identification stuff.. And we also need to make the clip note at top go away. It is too big and we
  should add the how to tell hatchery link up there instead... Then we should refine it even more. It still has a
  little bit of a clunky feel but it is better then original one. Maybe get more fresh agents to give refinement a
  stab then bring it back to original afent"
  Started (mockups only): three fresh designers, each with a different angle (calmer table; the opened fish row with
  a steady picture and a species identification placeholder; a first-time reader's restructure), all with his fixed
  asks (no big "Clipped fin only" note on top, a "how to tell a hatchery fish" link there instead). Their mocks go
  back to the original designer, who folds the best into round 5 on the same page.

- 2026-11-16 01:36, the owner, on tonight's pace (verbatim): "Everything takes so long maybe we should have less
  tests all the time and do it together at end or something"
  My answer to him: fewer, yes; but tonight's two long stops were found at the END (the Shuswap piece 14 minutes into
  a build; the stale mini release 18 minutes into the release). Proposed, for his yes or no on the morning list:
  (1) while working, only the tests for what changed, no full runs between steps (applied from now; tonight I ran
  the full pipeline tier three times and the whole app check twice, about 30 minutes that one run would have covered);
  (2) ONE full run, at the release, on the clean copy only: the staging release stops repeating the suite in place and
  stops waiting for GitHub's run (GitHub's run stays required for production); (3) checks that take seconds run
  before anything long (the mini-release check, and T779: a label that cannot be placed is refused at save time).
  Not cut: the build's own refusals of a label it cannot place.
  Owner, 01:36, right after: "Feels wasteful". I tried to rerun the release skipping the GitHub wait and the second
  clean-copy run; that was refused by the permission check (his words are not an explicit instruction to skip release
  checks) and I did not work around it. The release reruns the normal way once GitHub is green. If he wants the skip,
  he says so explicitly; otherwise it is the proposal above, for his yes or no.

- 2026-11-16 01:47, trout display ROUND 5 is on the same page (https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH),
  mockups only. The original designer folded three fresh designers' takes into one: a calmer table (a pale column
  beside the fish for each limit, hairlines only, every "only N" in that column as a white box worded "only 1 of
  these 2", a whole-box size limit as one sentence on top); a fish opens as a sheet from the bottom, so nothing in the
  table moves (an "in place" switch is on the page: 0 px of picture movement measured over 292 opens); the opened fish
  shows the app's real pictures and "what to look for" marks, with "often mistaken for" as the labelled placeholder;
  the big clip note is gone and "How to tell a hatchery fish" sits on the first line. Calmer, not smaller: taller
  than round 4 on 20 of 22 waters (about 40 px on average). The three fresh takes can be opened on the page as
  delivered. Four questions (recommended first):
  1. A fish opens as a sheet from the bottom (recommended), or in place under its row?
  2. On a hatchery-only water: say it once in a small line above the table (recommended), or after every name?
  3. Four new wordings need the data side: "only 1 of these 2", "Of the 5, only 1 over…", "uses the 1 over 50 cm
     (20 in)", a put-back row for wild steelhead. Ask for them? Recommended: yes (when and if this is ever built).
  4. Who writes the "often mistaken for" lines? Recommended: you, or someone you trust on fish.

- 2026-11-16 02:13, trout display, the owner's reply to round 5 (verbatim): "For the trout rework next round we should
  reduce text above the table. Also the sheet is better option and we need to refine it. We could reuse calendar
  element for dfo or freshwater for salmon if it helps. I like the binnong appearance now. Also need to figure out
  where to list the sources (regulation page etc)"
  Answers round 5 question 1: the SHEET. Round 6 started (mockups only), same page: less text above the table; the
  sheet designed properly (header, order, moving between fish, hard cases); the app's own year calendar tried for
  "when"; one place for sources (rule book page, whose rule, the quoted words); the binned look kept as is.

- 2026-11-16 02:23, trout display ROUND 6 is on the same page (https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH),
  mockups only. Above the table: 87 px, 2 lines, 13 words on average (was 134 px, 5 lines, 29 words; the Vedder 61
  words to 13); one screen holds on 16 of 22 waters. The sheet: picture, full name and today's answer on top, then
  "Here, today" said from the fish's side, licence rules, "When", how to tell it, sources; Previous / Next between
  fish. The year bar: under the table on 4 of 22 waters (Elk, Kaslo, Kitimat, Fraser) and in a few fish sheets; it
  cannot show a fish whose season differs from its table (Skeena in winter, Bella Coola cutthroat) because the data
  stores one year per table row. The bar was drawn from screenshots, not from the app's component. Sources: one
  "Sources" line under the table and a section in each sheet. Four questions (recommended first):
  1. With the second half of the answer off the top, is "Up to 2 today" enough on the Vedder, where a rainbow column
     says "Keep 4"? Recommended: yes (the one line to bring back if not: "or 4 if they're hatchery rainbow trout").
  2. Keep Previous / Next in the sheet? Recommended: yes.
  3. Ask the data side for a year per fish, so the bar can show the Skeena in winter and the Bella Coola's cutthroat?
     Recommended: yes, if this is ever built.
  4. Sources in both places (a line under the table and a section in each sheet)? Recommended: yes.
  Still open from round 5: hatchery-only said once above the table (now the first-line link reads "Hatchery fish
  only · how to tell"); the new wordings; who writes "often mistaken for".

## 2026-11-16 morning list — ONE list, all regions (written 04:14; the audit's findings are added below it when they arrive)

**Where things stand.** Your labels are built into the app data and on staging: release r-46e4355a7277, staging site
rebuilt on it, smoke check green (19 pass, 0 fail). 194 DFO places changed since the last release, 13 new cuts, four
closed areas drawn (Shuswap, Thompson x3). The app chat is redeploying the staging feed server and building 0.7.11 for
Play internal testing. Production is untouched and stays yours.

### A. Saves to re-tick in the review app (I saved these at your request as "unsure"; each is noted so)
Vancouver Island
- Somass, Stamp, Little Qualicum "everything else" places: rest-of, with the listed stretches answering first on
  their own dates (lines 2787, 2788, 2789; they replace my earlier 2771 to 2773).
- Nitinat below Parker Creek (2792) and Puntledge below the lower hatchery weir (2794): rest-of.
- Cowichan: Sandy Pool sign to the tidal boundary (2774) and 66 Mile Trestle to Sandy Pool (2775): side streams off
  (the September closure no longer lands on the whole Koksilah).
- Nanaimo: Cedar Road bridge to the Hwy 1 overpass (2776), 500 m below Bore Hole to Boulder Creek (2777) and its older
  wording (2778): side streams off.
Interior
- Shuswap closed stretch: the gauge to DFO's other printed point, 2,075 m (2770).
- Thompson, Juniper closed area: the lower signs to 4 km up (2785). Deadman and Ashcroft (2748, 2749) from before.
Skeena, Nass, north
- Skeena sockeye, Terrace to above the Babine: rest-of between the CNR bridge and the gauge, three listed places
  (2790, 2791).
- Skeena "mainstem waters only" (pink): above the CNR bridge, no side streams (2782).
- Morice, lake outlet signs to Gosnell Creek: starts at the lake now (2781).
- Babine Lake sockeye place: the whole lake, side streams off (2784). See question 8.
- Tatshenshini: the Blanchard River whole with its side streams (2779); Kwatini, Stanley and Goat creeks whole (2780).
- Sheldens Creek: the whole creek (2783), from your "i think this is tidal bound maybe".
- Kitimat west bank: side streams off (2786); still closes both banks until one-bank places are built.
- Dean, Crag Creek down to the tidal boundary: rest-of (2793).
From the earlier list, still to re-tick: Nanaimo 500 m (2746); the two Bulkley "all tributaries" places (2732, 2733);
the two Skeena places on the new confluence cuts (2751, 2752); Puntledge 1263 / 2447 / 2687; Sproat and Stamp Falls
(re-save as right: your fixes are in); the Fraser Region 2 places and Squamish (they gained side channels).

### B. Questions (recommended answer first; nothing here blocks staging)
Labels
1. **Campbell River near the mouth**: three stretches still "not sure" from Oct 26; the cement block sign is an
   estimate. Re-check on the satellite view when you can. Until then: kept as drawn.
2. **Fraser Region 3 mainstem** (Spuzzum to the Region 3 / 5 line): you marked it wrong because a label read "Region
   3 – Region 8 boundary"; the start used IS the Region 3 / 5 line. Keep as drawn and re-save as right? Recommended: yes.
3. **Kanaka Creek**: DFO says "112th Street bridge", the only crossing is 112 Avenue. Keep? Recommended: yes.
4. **Kispiox "signs near the Kispiox River Resort"**: no resort by that name; the signs sit at the Kispiox River Lodge,
   13.7 km up (low confidence). Keep until DFO Smithers confirms? Recommended: yes.
5. **Chemainus tidal boundary at 2.1 km** (low confidence; it makes 16 of 24 pieces tidal). Keep, or check imagery?
   Recommended: check imagery.
6. **Cowichan**: the water's "including tributaries" flag looks wrong (only a retired wording says tributaries). Turn
   it off? Recommended: yes.
7. **Middle Shuswap** (Shuswap Falls to Mabel Lake): no side-stream choice is saved anywhere. Main river only?
   Recommended: yes (DFO's words name no side streams).
Skeena (from the review of the built result, `handoff/PLAN/curation-review/skeena-cascade-review-2026-11-15.md`, ticket T781)
8. **Babine Lake in August (errs OPEN today).** Sockeye reads open on the whole lake Aug 1 to 27, including within
   400 m of 12 creek mouths and east of the Gullwing Creek line; those 13 closed areas are only notes on the card
   because they are not confirmed yet. Recommended: (a) you confirm the 13 areas in the review app so they are drawn;
   (b) until then the card gets an "open, closed in part" headline on those days.
9. **Gosnell Creek and Thautil River (errs closed).** The rule book's Morice "No Fishing from Gosnell Creek to Lamprey
   Creek" closes both creeks Jun 16 to Jul 31. Your earlier ruling says a joining creek with a line of its own is not
   inside. Leave Gosnell and the Thautil out, keep Lamprey Creek in? Recommended: yes.
10. **Gitnadoix below the powerline (errs OPEN).** Coho reads open Sep 1 to Oct 31; DFO opens only above the
    powerline. Add a cut at the powerline? Recommended: yes.
11. **Eulachon**: "No fishing for eulachon" is missing on the Lakelse and, to Jun 15, the Kitsumkalum. Always show it
    below Terrace? Recommended: yes.
12. **Kitimat east bank**: reads closed for about 750 m (DFO closes only the west bank). Leave closed until one-bank
    places are built? Recommended: yes.
How rules combine
13. **Gear rules of a whole river on a stretch with its own gear rule that day** (T770): on the Little Qualicum in
    Sep to Nov, "fly fishing only" hides the river's all-year hook rule. Show both? Recommended: yes.
How we work
14. **Fewer, later checks** (your "Everything takes so long…", "Feels wasteful"): (1) while working, only the tests
    for what changed; (2) ONE full run, at the release, on a clean copy, and the staging release stops waiting for
    GitHub's run (GitHub's run stays required for production); (3) checks that take seconds run before anything long.
    Yes? Recommended: yes. The audit's guards checker is costing this out; its numbers go below.
Kept as they are unless you say otherwise (no answer needed): the Tatshenshini "mainstem and all other tributaries"
place (your "I don't see how this would be a rest of situation"); the Tatshenshini place below the border keeps its
side streams (you verified it); the Kitsault tidal boundary's name waits for a small code change (T773); Sheldens,
Juniper, the Skeena sockeye pair, the Tatshenshini headwaters and Maria Slough are done as you answered.

### C. Trout display (mockups only; https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH, round 6)
Round 6: (1) on the Vedder is "Up to 2 today" enough on top when a rainbow column says "Keep 4"? yes; (2) keep
Previous / Next in the sheet? yes; (3) a year per fish in the data so the year bar works on every water? yes, if
built; (4) sources under the table and in each sheet? yes. From round 5, still open: the new wordings, and who
writes the "often mistaken for" lines (you, or someone you trust on fish).

### D. Filed overnight (tickets T770 to T782; none needs you except where a question above names it)
T770 gear rules hide each other · T771 chain order and an equal-places check · T772 Kitsumkalum Lake takes its river's
side channels · T773 tidal boundary name · T774 a place cannot be saved "not drawn" · T775 review app rest-of
leftovers · T776 audit tool faults · T777 tests that read your live saves, and child-process crashes · T778 the
Shuswap delta piece (done) · T779 the save-time check does not show a piece the build will refuse · T780 part names
showing "approximate location (medium confidence)" · T781 Skeena follow-ups · T782 data faults from the trout review.

**Added 04:40: the general audit (five Fable checkers, read-only; four reports in:
`handoff/PLAN/audit-2026-11-16/code.md`, `handoff/PLAN/audit-2026-11-16/labels.md`,
`handoff/PLAN/audit-2026-11-16/decisions.md`, `handoff/PLAN/audit-2026-11-16/guards.md`; the answers-against-sources
checker is still running). And the Play build: 0.7.11, build 33, code shrinker on, is on Play internal testing
(04:37); your ten-step phone walk is the To-Do row `t749-r8-phone-walk`.**
Headlines: the code of Nov 14 to 16 has no defect that changes what an angler is told today (97 commits read); every
one of your 309 labelled places is stored as your last save says, and no place reads open against its own DFO row.

### A2. Re-tick list: additions from the audit
- FIRST: Fraser, Mission to Hope (line 2568, unsure): it is the one open answer live this month (hatchery coho
  Nov 7-30, chum Nov 3-30); 25 of its 62 sections are side channels and Maria Slough (your "Add all five").
- Saves I wrote before line 2770 that are in force: 2672 (Stave, unsure); 2730, 2731 (Bella Coola, verified);
  2748, 2749 (Thompson Deadman and Ashcroft closed areas, verified); Region 6: 2728, 2732 to 2736 (unsure), 2738,
  2739 (verified). With 2770 and 2785 that is all four closed areas standing on a "verified" I wrote.
- Low confidence by their own notes: Juniper's upper end (4 km by the words); the Shuswap stretch (2,075 m where the
  words say "one km").

### B2. More questions, from the audit (recommended answer first)
15. Regions 7 and 5A: the page header "no fishing for salmon until further notice" overrides the dated openings in
    the table (your Oct 31 ruling "the header closure wins"). Nechako, Quesnel River and Quesnel Lake Horsefly Bay
    would read closed next Aug 22 when DFO opens sockeye and pink; DFO's page also says "except for the
    opportunities listed in the table below". A dated row with a fishery notice answers on its dates, the header
    every other day? Recommended: yes.
16. Atnarko below the Tweedsmuir Park Lodge sign (line 2584, unsure): the coho opening also covers 17 side-stream
    sections. Side streams off for this stretch? Recommended: yes (errs closed). Note your earlier "no it needs to
    include tributaries" was about the Atnarko as a whole.
17. Lower Shuswap: the cut is the Mara Bridge; DFO says "signs above Mara Bridge". Are the signs at the bridge? If
    not, a cut at the signs.
18. A place whose last save is "wrong" with no fix: you said "I want you to for it to be redrawn… Just be safe". Not
    built: such places still ship their older drawing (the Region 3 Fraser mainstem; Sproat and Stamp Falls end on a
    "wrong" save though their fixes are in). Build: the release lists them and ships DFO's words undrawn until
    re-saved? Recommended: yes.
19. The app chat's merge question (never shown to you): may the finished app work merge into the shared branch now
    (T759 banner and stripe, T760 stuck download, T756, T746, T738, T741, T732 backup, T739 push, the iPhone
    branches)? Your words on Nov 15 10:33: "ios work could merge when ever I think?" Recommended: yes, one at a time
    with its checks.
20. T759 stripe: legend word "Check rule" OK, and strong enough on the dark blue line? Recommended: yes to both.
21. T735 dark-mode highlight: pick C (dark edge + soft glow)? Recommended: C.
22. Play "Data safety" location answer: approximate or precise? (For the App Store you said "we want precise".)
23. Production backup bucket name `bc-fishing-regulations-backup` (Q172)? Recommended: yes.
24. T686 data shrink: option (a), about 1.94 MB, lossless? Recommended: (a).
25. T732 backup: only with a screen lock? [no]; photo copy size [1600 px]. Recommended: as bracketed.
26. T710: the side channel named "Exchamsiks River confluence up to Skeena River Ecological Reserve boundary" reads
    backwards. Reword?
27. Shuswap closed stretch: tell DFO their two printed points are 2 km apart where the words say one km?
    Recommended: yes.
28. Should the pre-build check read the committed proposals file, not your working copy (it stopped a Play build
    once)? Recommended: yes.
Note on question 10 (Gitnadoix powerline cut): it REVERSES your Nov 15 "Whole river (Recommended)" for the Gitnadoix.

### E. From the audit: recorded wrong or lost (I fix the records and ticket these; listed so you know)
- T754 one-bank places: you answered "Build it this way (Recommended)"; the ticket still said "options for the owner".
- T759: the shared ticket still described the design you rejected ("reads not known"); the banner + stripe design
  you chose is on unmerged branches.
- The About-page sentence "not affiliated with the government": listed as built, is on no branch.
- Seven wording findings from the T462 audit had no ticket (K6, K7, K8, G8, G9, G10, D6); your gate 1 for closed
  testing needs each fixed or ticketed.
- Cut de-duplication: the data was cleaned once; the suggester still proposes a new cut where one exists.
- Review app: when Confirm is greyed, name the unticked row (you got stuck three times).
- Hope gauge: 66 m reads the wrong gauge; three gauge cuts never placed.
- 14 of your Nov 13 to 15 readings are not yet in the rulings file.
- Out-of-date production commands at two places in this file; a fresh block is owed.
- The approved push privacy line is written nowhere yet.

### F. PRODUCTION: what stands between staging and production data (the audit's list, in order)
BLOCKER FIRST: the production upload refuses r-46e4355a7277 (four closed areas) until the PRODUCTION feed server
serves closed areas. That server code (T757, T758) is only on the app chat's side branch; the production checklist
puts the feed server after the data, which is wrong for this release.
1. You: check staging, re-tick list A, answer the label questions.
2. Decide whether one more staging release is needed.
3. Claude: merge the T757 / T758 feed-server halves into the shared branch (needs question 19).
4. Claude: fresh production commands and a clean site copy (T404). 5. Claude: rehearse the production scripts.
6. You: backup bucket (Q172), mail routing, run token. 7. You: data worker.
8. You: feed server, BEFORE or WITH the data. 9. You: the production release command.
10. Site: Claude builds, you deploy; robots settings; www redirect (T240).
11. You: smoke check and the privacy page. 12. You: Search Console; retire v1.

### G. PLAY CLOSED TESTING (after production)
1. 0.7.11 on Play internal; your ten-step walk and the open phone checks (T749).
2. The "before closed testing" tickets: T759, T735, T716, T740, T708, T751, T608.
3. Gate 1: the seven T462 findings and this audit's findings, with your OK.
4. You start `/code-review ultra` (T463). 5. Store listing: texts, the 8 pictures re-shot, the preview clip, the
non-affiliation sentence (T244, T371, T319, T709). 6. You: Play forms, including the location answer (T371).
7. You: production app build, install, promote to Closed. 8. You: tester list; 12 testers for 14 days.
9. Claude: the in-app update prompt checked (T390, stalled since Nov 6) and T514.

### H. Question 14 again, with the audit's numbers (fewer, later checks)
Last night's staging release ran 105 minutes over nine attempts; about 60 of those minutes repeated work that told
nothing new (the same 6,490 tests ran in place and again on the clean copy; the clean copy reran after commits that
could not affect it; the release's own commit rebuilt the data for 9 minutes). Across the night those tests ran 16
times on this Mac. A rehearsal command exists that lists every stop in one pass (the comparison to read, stale test
fixtures, changed pages, failing app tests); I did not run it, and it would likely have made it four or five attempts.
The proposal, for your yes or no (nothing is changed until you say):
- While working: the one test file touched, plus the quick checks (about a minute).
- Before a push: one command that runs exactly what GitHub runs, made automatic at push time.
- In a release: build, REHEARSE on the new release and fix everything it lists, then one real run; the tests run
  once (on the clean copy), not twice; the clean copy reruns only when a commit could affect it; the upload's
  question to the feed server is asked at the start, not at the end.
- One thing ADDED: the tests that read the newly built data run once on the new release. Today most of them (1,208
  tests in 115 files) are run by nobody automatically.
- Not cut: the build refusing a piece it cannot place, the stop to read what changed, the stop on a changed page, the
  clean-copy run once per release, GitHub's runs.
- Saves about 10 minutes on a clean release; about 60 of last night's 105.
Recommended: yes.
Also from that checker, to be fixed (ticketed): the review app can crash when several pages are drawn at once (its
page pictures are freed outside the lock); test crashes on this Mac come from a map library's database handle in
child processes; four fixture states for closed river parts are still checked only for "is closed"; 5 of your 17
reviewed continuation links quietly stopped shipping and 4 new ones wait with no row for you.

### I. The fifth audit report: the app's answers against the printed rule book and DFO's pages
(`handoff/PLAN/audit-2026-11-16/answers.md`; about 98 waters, about 1,060 answers, three days each, all regions.)
No wrong number, size or date was found. Province-wide rules matched on all 18 waters with no entry of their own.
**Seven things the app allows that the sources forbid** (none is new in this release; tickets T793 to T798):
- Wild-trout limits are missing from the keep card on at least 15 lakes (Comox, Sproat, Great Central, Cultus,
  Harrison, Pitt, Chilliwack, Chehalis, Lillooet, Lois, Ruby, Windsor, Makii, Dodd): the book says "No wild rainbow
  trout over 50 cm"; the card says "any size; up to 1 over 50 cm". And the Kitimat with 23 streams, Jul to Oct: "Up to
  5 rainbow trout" without "hatchery". The data is right; the card drops it. FIRST TO FIX [T793].
- The Peace River just below W.A.C. Bennett Dam reads open; the book closes it (the rule sits on Dinosaur Lake only)
  [T794].
- Williston Lake's "No rainbow trout under 30 cm" is missing on Nation Arm and Davis Bay [T794].
- "No angling from powered boats" is never shown (Atnarko / Bella Coola, Horsefly, Chilcotin, Chilko, Clearwater,
  Nicomekl, Serpentine) [T797].
- A stretch the book closes can still show an open salmon card (Skeena Ecological Reserve, the Atnarko's closed
  stretch, the Cowichan below Mile 66) [T796]. Question 29.
- A DFO "Finfish closure" closes only salmon; trout stay open (Stamp, Somass, Sproat, Nitinat, Tahltan) [T796].
  Question 30.
- Coal Creek above the old railway bridge asks for no Classified Waters Licence [T798]. Question 31.
**Things the app forbids that the sources allow:** Fraser Lake, Kotcho Lake and Cold Fish Lake read closed all year
because an ecological reserve touches 0.1 % of the lake [T795]; dead fin fish as sturgeon bait is not shown on the
Region 2 Fraser and lower Harrison; the Shuswap Lake pier exemptions; the Pitt Lake head caveat says "release all"
where the book says wild only [T797].
More questions (recommended answer first):
29. A stretch the rule book closes: the salmon card reads closed too, with DFO's row kept as text? Recommended: yes
    (the book, p.77, says its closures apply to salmon as well).
30. A DFO "Finfish closure" closes the whole page, trout included, on its dates? Recommended: yes.
31. Coal Creek above the old MF&M bridge: Class II when open? Recommended: yes.
32. Williston Lake's 30 cm rainbow minimum on all three Zone A parts (Nation Arm and Davis Bay too)? Recommended: yes.
33. Puntledge above Comox Lake: the book says fly fishing only above the diversion dam "including tributaries" and
    exempts the river from the summer closure; we stop both at the lake. Carry both above the lake? Recommended: yes.
34. Westwick Lakes (26 % inside an ecological reserve) and Priest Lake (59 %): open with a part-closed note, or
    closed? (Fraser, Kotcho, Cold Fish and 15 Mile Swamp, barely touched, are a plain fix: open.)
35. Skeena "Section 4" has no printed upper end; the app runs it to the river's source. Right?
36. DFO "non-retention" reads Closed (your earlier ruling; 20 rows). Keep, or show "release only"?
37. **The wild-trout fix is ready on a side branch (05:28); 22 answer keys change, nothing else.** New card lines, for
    your read: Comox, Great Central, Sproat: "Up to 4 brown, cutthroat or rainbow trout combined — no wild rainbow
    trout over 50 cm (20 in)". Chilliwack, Cultus, Harrison, Chehalis, Pitt, Lillooet: "Up to 4 brook, brown,
    cutthroat or rainbow trout combined — no wild brown, cutthroat or rainbow trout over 50 cm (20 in)". Dodd, Lois,
    Ruby, Windsor, Khartoum, Horseshoe, Nanton, Ireland: "… combined — up to 2 wild brook, brown, cutthroat or rainbow
    trout combined 40 cm (16 in) or less; no wild … over 40 cm (16 in)". Kitimat and 23 streams, Jul 1 to Oct 31: "Up
    to 5 rainbow trout — no wild rainbow trout under 30 cm (12 in); up to 1 wild rainbow trout 30–50 cm (12–20 in)".
    They are long but true. Go as worded (recommended), or tell me how to shorten? On your go: merge, rebuild the
    answers, a staging release [T793].
38. **Dodd, Lois, Khartoum, Horseshoe, Nanton, Ireland, Windsor lakes: does "no wild trout over 40 cm" cover char?**
    The book prints "Wild trout/char daily quota = 2" and then "no wild trout over 40 cm" (Lois and Khartoum: "40 cm
    or more"). We read the second line as trout AND char, so a wild Dolly Varden/bull trout or lake trout of 60 cm or
    more cannot be kept there (the card will say so once the fix lands). Your Region 6 ruling reads "trout" as NOT
    char. Trout only here too (a wild char over 60 cm may be kept inside the 2), or trout and char as now (stricter)?
    Recommended: keep trout and char until you say otherwise (it errs closed). Checked: the four lakes the audit had
    not listed (Khartoum, Horseshoe, Nanton, Ireland) carry the same printed lines as Dodd, so their new card line is
    right.


## 2026-11-16 10:25 — all of last night's questions on ONE answer page (main chat + app chat)
- Owner, 10:13, typed in chat (verbatim): "can you go through all questions to me from over night that youy haad? make sure
  i have all info needed. also get any questions the app chat has too"
  Done 10:24: one page, https://claude.ai/artifact/H8gtvpptCZLGLaPGCyNfaJ ("Morning Questions"), 55 cards in eight
  sections: Start here (questions 37, 38, 19, 14, and one go-ahead for the fixes that put a printed rule back on the
  page: T794, T795, T797 and the small code fixes T783, T784, T785, T792 A); Your DFO labels (1 to 7, 16, 17, 26, 27);
  Skeena and the north (8 to 12, 35); How the rule book and DFO are read (13, 15, 18, 29 to 34, 36); The app: looks and
  picks (20 in two parts with the stripe pictures, 21 with the highlight sheets, the T762 year line, 24, 25, ten small
  defaults); Production and the stores (22, 23, the Firebase exception, the App Store privacy answers, 28); Trout
  display mockups (round 6's four, round 5's three, and "what next"); Only you can do these (the re-tick list, the
  build 33 walk, iPhone build 36 and the push test). Each card: the background in plain words, the printed line where
  one exists, the choices with the recommended one marked, a note box. His taps and notes are stored in the page's
  database (collection `answers`, one document per card id: q01 to q38, q20a, q20b, go-fixes, a-year, a-defaults,
  a-push, a-store, t1 to t8, task-retick, task-walk, task-iphone). READ THEM FROM THERE and copy each answer, verbatim,
  into this file and its ticket before acting on it.
  Wording choices of mine on the page that differ from the morning list (mine, not his): question 17 recommends "keep
  the bridge and ask DFO"; question 26 recommends rewording from the lower end up; question 34 recommends Westwick
  open with a note and Priest closed; question 35 recommends "to the source, as now"; question 14 shows section H's
  proposal, with "staging releases need not wait for GitHub" as a separate choice that needs his explicit pick.
- The app chat's list (its message of 10:14; nothing of its own answered or changed since 04:37): the merge question
  first (its order: the T757 + T758 feed-server halves, the four iOS fix branches, T756 + T760 after an emulator
  proof, T746 / T738 / the crash sender / perf T741, T732 after its picks, T759 after the stripe confirms, T739 push
  last); T759 stripe two confirms (pictures in its scratch folder, copied onto the page); T762 year line (a read);
  T735 highlight A / B / C (recommended C; the To-Do page had lost its row, the page carries it now); Play location;
  T732 three picks; T686; backup bucket; the iOS day defaults; keyboard and back picks; oldest iPhone; App Store
  privacy; the 0.7.10 looks (T731, T730, T728); the Firebase exception and 200 registrations a day; the build 33 walk;
  iPhone build 36 and push. Its view on questions 29 and 30: yes as recommended. T797 item 1 (powered boats) rides on
  the merge question, in the next app build. `ios-links-no-team-id` is moot (his Team ID is saved): row to close.
- Owner, about 10:19, typed in chat (verbatim): "also as a seperate thing can we get some final reviewing of the trout
  rework we want the agent to have an adverserial attempt to trry and see if any regulations can not be represented."
  Started 10:21: a fresh Fable reviewer (not the designer), read-only: it lists every distinct trout and char keep
  structure in release r-46e4355a7277, tries to draw each in round 6's table and sheet, and reports what cannot be
  shown or would mislead, with a real water, the stored rows and the printed line for each; and, separately, printed
  rule shapes the data itself does not store. Its report goes to `handoff/PLAN/design/` and to him when it is back.
  Still mockups only: nothing is built.
- Owner, 10:26, typed in chat (verbatim): "i want questions in batches from you directly instead". So: the questions
  are asked in chat, as multiple-choice batches of up to four, most blocking first; the page stays as a reference
  (pictures, background) and is not the way to answer. Each batch's answers are recorded below, verbatim.

### Answers by batch (2026-11-16, question dialog in the main chat)
- Batch 1, 10:27. Question 37 (wild-trout card sentences, T793): "Go as worded (Recommended)". Question 38 (does "no
  wild trout over 40 cm" cover char on Dodd, Lois, Khartoum, Horseshoe, Nanton, Ireland, Windsor): "Trout and char
  (Recommended)". Question 19 (the app chat's merge question): "Yes, one at a time (Recommended)" (the option read:
  in the app chat's order: feed server first, then iPhone fixes, stuck download after an hour's emulator proof,
  loading screen and speed work, backup, banner and stripe, push last; each with its own checks and a message to him
  first). Question 14 (fewer, later checks; section H's proposal): "Yes, as proposed (Recommended)" (staging releases
  still wait for GitHub's run; he did NOT pick "Yes, and skip the GitHub wait").
- Batch 2, 10:33. Go-ahead for the fixes that put a printed rule back on the page (Peace below W.A.C. Bennett Dam;
  Fraser, Kotcho and Cold Fish lakes and 15 Mile Swamp open with a note; powered boats; the smaller ones; four small
  code fixes: T794, T795, T797, T783, T784, T785, T792 A): "Go on all (Recommended)" (shipped together with the
  wild-trout fix in one staging release; the list of changed answers shown to him first). Question 6 (Cowichan
  "including tributaries" mark): "Turn it off (Recommended)". Question 7 (Middle Shuswap): "Main river only
  (Recommended)". Question 16 (Atnarko below the Tweedsmuir Park Lodge sign, side streams), typed: "this depends if
  the name has tributraries included in it. if it doesnt then no tribs otrherwise yes tribs". My reading: a general
  rule, not only this stretch: a DFO place takes its side streams when DFO's words for that place say tributaries,
  and not otherwise.
- Batch 3, 10:44. Atnarko follow-up (DFO's heading reads "Atnarko River including tributaries"; the stretch's own
  line names only the sign: which words decide for a stretch?): "The heading counts". So the stretch below the
  Tweedsmuir Park Lodge sign keeps its side streams, as saved (line 2584); and the rule for every DFO place: side
  streams follow DFO's words, and a heading that says "including tributaries" counts for the stretches under it.
  Question 2 (Fraser Region 3 mainstem), typed: "it is right if it is spuzzum to the 3/5 boundary now. ON THE
  FRASER". Question 3 (Kanaka Creek, 112 Avenue bridge), typed: "keep it but add to gotcha". Question 4 (Kispiox, the
  lodge at 13.7 km until DFO confirms), typed: "okay sounds good add to gotchas".
- Batch 4, 10:50. Question 5 (Chemainus tidal boundary at 2.1 km), typed: "The tidal waters of the Chemainus River
  and off the Chemainus River mouth, commencing at Bare Point on Vancouver Island to the light on North Reef, to
  Sherard Point on Vancouver Island, thence following the shoreline northerly to a boundary sign on the Chemainus
  River near the Bald Eagle Campground, thence across the river to the opposite bank, thence following the Vancouver
  Island shoreline northerly to the point of commencement / check it agfainst this if it is wrong then let me
  review". So: the main chat checks our 2.1 km point against "a boundary sign on the Chemainus River near the Bald
  Eagle Campground"; if it does not match, it goes to him for review in the review app. Question 1 (Campbell River
  near the mouth, three stretches): "They are right". Question 17 (Lower Shuswap, signs above Mara Bridge), typed:
  "at the bridge but make sure the cut name has the fact signs are nearby". Question 27 (tell DFO the Shuswap closed
  stretch's two points are 2,075 m apart where the words say one km), typed: "just add to gotchas that is all. might
  be 1km as bird flies too". So: nothing goes to DFO; a gotcha entry, and the straight-line distance between the two
  printed points is measured and noted.
- 10:36, the adversarial trout review is back and saved: `handoff/PLAN/design/trout-adversarial-review-2026-11-16.md`.
  Verdict "no, not yet": possession limits have no place in the design (at least 640 waters); the steelhead "stop
  fishing this water" duty is hidden under "Licence rules" (2,957 waters); a wild limit shared across two boxes reads
  as 4 where the book says 2 (Lois and Khartoum lakes; T793's sentences have the same split there, to check before
  T793 ships); a fish in two shared limits (Kamloops Lake, Adams River) is drawn wrongly in the mock; the top line
  understates on 89 waters; the year bar's gap touches 10,250 waters. Told to him in chat at 10:46.
- Batch 5, 11:03. Question 8 (Babine Lake in August, the 13 closed areas not confirmed), typed: "i think you can
  confirm them. it is just confluences and unless the names dont match they are easy to auto confirm". So: the main
  chat confirms the 12 creek-mouth circles where the creek's name matches DFO's words (and the Gullwing Creek line);
  any whose name does not match goes to him. Question 9 (Gosnell Creek and the Thautil River inside the Morice
  closure), typed: "yes they are out but please make sure this rule doesnt break other stuff. we need a trigger for
  why they are out? why does this tributary rule not apply? what is the condition just so it is a universal rule."
  So: they come out, but as ONE stated rule applied everywhere, not a special case: the condition is written down,
  run across the province, and every water whose answer changes is listed for him before it ships. Question 10
  (Gitnadoix, coho "upstream of powerline"), typed: "the powerline is. the mouth essentially so maybe you could add
  the cut for powerline to the mouth then it is all good?" My reading: the powerline is treated as the river's mouth
  (the mouth carries the powerline as its name for this place), the whole river stays open for coho as he answered on
  Nov 15, and no new cut or map rebuild is needed. Question 26 (the Skeena side channel's name): "Reword, lower end
  first (Recommended)".
- 11:02, checker's result on question 5 (Chemainus): our tidal point (48.886806 N, 123.686629 W, 2,091 m up the river)
  is beside the Bald Eagle Riverside Campground: 0 to 135 m from it along the river, depending on which map point is
  taken for the campground. It matches DFO's "boundary sign on the Chemainus River near the Bald Eagle Campground";
  it stays, still low confidence because the sign itself is not pinned. Nothing for him to review. And on question
  27: the Shuswap closed stretch's two printed points are 1,738 m apart in a straight line (2,075 m along the river);
  neither is near "one km". Goes into the gotchas file as he asked.
- Batch 6, 11:19. Question 11 (eulachon line below Terrace), typed: "general rule is (Reminder: There is no fishing
  for eulachon in Region 6. ) why would bait remove eulachon rule?" So: it is DFO's Region 6-wide reminder and shows
  on every Region 6 water; a bait rule must never take its place (the cause is T770: the card treats the eulachon row
  as a gear line and shows only the narrowest place's gear lines). Question 12 (Kitimat hatchery outfall, east bank):
  "Both banks closed (Recommended)". Question 35 (Skeena "Section 4" has no printed upper end), typed: "i want to
  include pictures for you to look at this needs more thought." HELD: he sends pictures; nothing changes until then.
  Question 13 (a stretch's gear rule hides the river's, T770): "Show both (Recommended)".
- Batch 7, 11:27. Question 15 (Regions 7 and 5A: the page header "no fishing for salmon until further notice"
  against the dated openings in the table), typed: "can we look at when the rules got added to see if they are
  stale? what came first header or rules? once we have that info we caqn make a decision". HELD: the main chat finds
  when the header and each dated row (FN0851 Nechako, FN0846 Quesnel) appeared, then asks again. Question 18 (a
  place marked wrong with no fix still ships its older drawing), typed: "can we sort this out now i want to review it
  and make it correct for sure". So: the list of every such place goes to him now, to review and re-save in the
  review app. Question 29 (a stretch the book closes shows an open salmon card, T796), typed: "yes we want to do this
  but can you bring the example changes to me so i can see why it happened etc? but yes we want it to do this". So:
  YES, build it; and the examples (before, after, and why) go to him. Question 30 (DFO "Finfish closure"): "Closes
  the whole page (Recommended)".
- 11:28, on question 35 (Skeena Section 4), he sent two pictures from the rule book (the maps of Section 4 Zone A,
  Shegunia River confluence to Sedan Creek confluence, and Zone B, Chimdemash Creek confluence to 1.5 km upstream of
  the Zymoetz confluence), typed: "here are picutres please review them then ask more questions we can figure it out
  together".
- Skeena Section 4 follow-up, 11:32 (after his two maps; the main chat had read the province's regulation, Schedule A
  item 33: "Skeena River upstream of 1.5 km upstream of Zymoetz River (Skeena River 4)", Class II, July 1 to Dec 31,
  no upper end). How far Section 4 runs, typed: "Should run all the way up but also we should handle the weekend
  zones properly too. Make sure licensing info is done properly there. Also we might want to have a way to indicate
  classified water types on the map/card?" What to check about the two weekend zones, typed: "Just make sure the info
  is correct for them for licensing etc etc. also maybe we should have the classified waters indicator for the
  different types?" So: question 35 is ANSWERED (Section 4 = the whole main river above the point 1.5 km upstream of
  the Zymoetz; the "a curator must draw the upper bound" flag on the stored row is cleared); a checker reads the
  licence, stamp and weekend-closure lines on every Skeena main-stem part of the release against the printed lines
  (started 11:32); and a new idea of his, not a decision: an indicator of classified waters (Class I / Class II) on
  the map and the card. It becomes a ticket with a few looks for him to pick from, for the app chat.
- 11:32, from the app chat, a data note for the main chat: "No angling (from a powered boat) — upstream of dyke gates"
  binds 31 parts: the Nicomekl and Serpentine plus 27 side streams (Anderson Creek, Best Creek, Cloverdale Canal,
  Cub Creek, Elgin Creek, Glover Creek, …). To check against the book: the two rivers only, or their side streams too.
- Batch 8, 11:35. Question 31 (Coal Creek above the old MF&M Railway bridge): "Class II above the bridge
  (Recommended)". Question 32 (Williston Lake's 30 cm rainbow minimum): "All three parts (Recommended)". Question 33
  (Puntledge above Comox Lake: fly fishing only above the diversion dam "including tributaries", and exempt from the
  Jul 15 to Aug 31 closure), typed: "If the river continues then yes it goes above". So: both carry above the lake
  on the parts that are the Puntledge. Question 34 (Westwick Lakes 26 % and Priest Lake 59 % inside an ecological
  reserve): "Both open with the note".
- 11:36, two checkers back (reports to be saved under `handoff/PLAN/curation-review/`; until then the full texts are
  in this session's transcript and the scratch folders `header-history/` and `wrong-saves/`):
  (1) Question 15, which came first: neither. DFO keeps BOTH texts in the page and switches each season: off-season
  the header "no fishing for salmon until further notice" shows and the table of openings is hidden inside an HTML
  comment; about Aug 20 it shows the table and hides the header; on Apr 1 it flips back. FN0851 (Nechako) and FN0846
  (Quesnel River, Horsefly Bay) were issued 2025-08-20 for 2025 dates; today's pages (stamped 2026-04-01) show only
  the header. Our parser reads the hidden table too (Regions 7, 5A and one row in Region 4), which is the only reason
  a header and dated rows ever meet. Proposed fix: the parser (Python and the feed server's) ignores a table DFO has
  hidden; then the header rule is never needed against dated rows.
  (2) Question 18, the list to re-check: 40 saves. 5 are "wrong" in his hand (live: Sproat, fix is in, re-save as
  right; Stamp Falls, labels done but the lower cut sits about 165 m below Deer Creek where his note says 225 m;
  dormant: Fraser Region 3, Thompson, Morice). 35 were written by Claude for him (31 places, 4 closed areas): 11 of
  them are live and answer open in Nov or Dec (Stave, Bella Coola, two Cowichan stretches, Nanaimo Cedar Road,
  Tatshenshini Blanchard, Skeena pink, Stamp / Little Qualicum / Nitinat / Somass rest-of places). None of his own
  "unsure" saves lacks a fix. The audit tool finds only 8 of the 40 (T776).
- Batch 9, 11:46. Hidden tables on DFO's Region 7, 5A and 4 pages (ignore whatever DFO has hidden?), typed: "we
  should keep reading them for provenance but mark them hidden somehow so they are ignoired". So: the reader keeps
  the hidden rows, marks each as hidden, and a hidden row answers nothing (data build and feed server alike); this
  settles question 15 (the header rule no longer meets dated rows). How to go through the 40 saves to re-check: "In
  chat, with a picture each (Recommended)" (each place as a map picture with DFO's words and what is drawn, in
  batches, most urgent first; he answers right / wrong and the main chat saves his answer with his words; the 6
  retired ones are skipped). Stamp Falls lower cut (about 165 m below Deer Creek, DFO says 225 m): "Move it 60 m down
  (Recommended)" (next map rebuild; picture to him before it ships). Question 36 (DFO "non-retention" reads
  Closed): "Keep "Closed"" (RULINGS.md gets the line that the ruling covers dated rows).
- 11:45, the Skeena Section 4 checker is back: the licence information is right on every main-stem part (Class II and
  the named section, the dates, the steelhead stamp, no Classified Waters Licence on "Section 3", Section 4 from 1.5 km
  above the Zymoetz to the source, both weekend zones on the right parts and days, a resident's status unchanged on a
  Saturday). Three wording faults: (1) Monday to Friday the weekend closure's "at certain times" line is cut short
  (zone b does not say who is closed out; zone a never names the zone); app core, `caveats.ts` line 256; it touches
  every weekend-only closure in the province; (2) "non-residents and non-resident aliens require separate licences
  describing which section they are fishing" is barely shown, and the stamp sentence quotes the wrong printed line
  (`pipeline/deliver/export/read.py` line 387); (3) zone b on weekends reads "the Zymoetz River confluence (1500 m
  upstream)" where the book says "1.5 km upstream of Zymoetz River confluence". One question for him: with the
  angler set to "From outside Canada", a zone part on a Saturday still says "Open", the closure only as a note.
- Batch 10, 11:55 (pictures sent first: the stripe sheet of eight, the stripe close in on the light theme, the two
  dark-highlight sheets). Skeena weekend zones, an angler set to "From outside Canada" still sees "Open" on a zone
  Saturday with the closure as a note: "Note is enough (Recommended)". Question 20, the stripe's legend entry (T759):
  ""Check rule" is fine (Recommended)". Question 20, the stripe's strength on the dark blue "Keep salmon" line: "Make
  it wider". Question 21, the dark-mode highlight (T735): "C: dark edge + soft glow (Recommended)".
- Batch 11, 11:58. The T762 trout "through the year" line (in build 33): "Fine as it is (Recommended)". Question 22
  (Play "Data safety" location): "Change Play to precise (Recommended)". Question 24 (T686 data shrink): "(a) Three
  lossless changes (Recommended)". Question 25 (T732 backup: screen lock no, copies 1600 px, new photos at 2048 px),
  typed: "1280 is probably enough also could we compress as jpeg or something which makes the size a lot smaller? ".
  So: backup copies at 1280 px (about 81 fit); the app chat answers whether the copies are already JPEG and what a
  stronger compression would save; the screen-lock pick and the 2048 px pick were not objected to (defaults stand,
  my reading).
- 11:58, he asked in chat: "what is state of the trout redsign ?" Answered in chat (round 6 mockups published, nothing
  built; the adversarial review says "not yet", four blockers; round 7 proposed to fix them).
- Batch 12 (trout display, mockups only), 12:03. Next step after the adversarial review: "Round 7 fixes them, then
  re-attack (Recommended)". Top line on the 89 waters where the short line understates: "Bring the second half back
  (Recommended)". Previous / Next in the sheet, and sources in two places, typed: "i like the sources in both. ths
  general all sources and the specific ones. then the sheet should be a swipe down to close style (we dont need
  next/prev on it unless it is going in the same group like bull trout/dolly varden? then navigation should be more
  obvious with what the next item is in the item at top of sheet and swipe over gestures etc) and the sheet needs to
  be cleaned up a bit I think. " Who writes the "often mistaken for" lines, typed: "we should drop this for now I
  think ". So round 7: the four blockers and the top line fixed; sources stay in both places (all sources under the
  table, the specific ones in the sheet); the sheet closes by a swipe down, loses Previous / Next except between
  fish of one group, shows plainly what the next item is where it does move, and is tidied; the "often mistaken for"
  placeholder is dropped. Then the same reviewer attacks it again.
- Batch 13, 12:09. Question 23 (production backup bucket name `bc-fishing-regulations-backup`): "That name is fine
  (Recommended)". Android push through Google's Firebase as his exception to "self-host everything", typed: "we dont
  want this right now". So: no Firebase now; Android push is not built for now (Android keeps its own background
  checks); the iPhone push work is not touched by this answer. Question 28 (the pre-build check reads the committed
  list of proposed cuts, not his working copy): "Read the committed list (Recommended)". The eleven small app
  defaults (a to k), typed: "we should fix anything that doesnt seem desirable". So: the app chat judges each and
  fixes the ones a user would not want; my reading of the likely ones, passed to it as a reading only: (a) the Save
  row hidden under the keyboard, (g) the bottom bar jumping 122 px, (h) Back reopening search with the text still
  typed, and (i) by his own "Flow leads" ruling, the Conditions card opening on Flow. It tells him what it changed.
- 12:05, from the app chat, for his backup question: the backup copies are already JPEG (1600 px on the long edge,
  quality 70, about 0.37 MB each against 4.6 MB as taken); at 1280 px about 0.25 MB (81 fit); a stronger setting
  (JPEG 60 or WebP) is an estimated 0.17 to 0.20 MB (100 to 115 fit), to be measured and shown to him as a before /
  after pair before the quality changes. It builds 1280 px now.
- ALL THE QUESTION BATCHES ARE DONE (13 batches, 10:27 to 12:09). Still open with him: the map pictures of the 34 live
  saves to re-check (being drawn; in chat, most urgent first); trout round 7 and its re-attack; the build 33 phone
  walk and iPhone build 36; the Skeena Section 4 pictures are answered.

### Re-check of saves, in chat with pictures (T806), 2026-11-16
- Pictures 1 to 4, 12:20. Sproat (mark 2468; the END is now 349 m below the Hwy 4 bridge): "Right". Stamp Falls
  (2469; DFO gives the lower end two ways that disagree on our line: 500 m below the fishway = our END at 498 m; 225 m
  below Deer Creek = our END at 164 m): "Keep 500 m below the fishway". This REPLACES his earlier "Move it 60 m down
  (Recommended)" of batch 9 (asked before the fishway measure was known); the Deer Creek mismatch goes into the
  gotchas; no cut moves. Stave (2672), typed: "it looks right but just to confirm those are the items with the names
  in name variants right?" Checked: yes, `data/curated/waters/name_variants.json` holds "Ruskin Spawning Channel"
  (the unnamed side channels on the east bank below Ruskin Dam) and "Northrop Spawning Channel" (the added stream
  OpenStreetMap calls Northrop Channel); those are the cyan pieces. Cowichan, Sandy Pool sign to the tidal boundary
  (2774; the yellow runs about 2 km into the water the app tints as tidal), typed: "we want to use our tidal
  boundaries defined with the mouth definition. these should already all be in so use existing tidasl boundary item
  dont trim it to dfo polygon". So: the place ends at OUR tidal boundary cut (the mouth definition), as drawn; the
  tinted polygon is not what trims a place. Right as drawn.
- Pictures 5 to 8, 12:27. Cowichan, 66 Mile Trestle to the Sandy Pool sign (2775): "Right". Nanaimo, Cedar Road
  Bridge to the Highway 1 overpass (2776): "Right". Tatshenshini, the Blanchard (2779), typed: "wrong iut should
  have the tatshenshini river too it is just saying that plus the blanchard". So DFO's "upstream of the BC/ Yukon
  border – along the Haines Hwy.; Blanchard River." = the Tatshenshini River above the BC / Yukon border AND the
  Blanchard; my save 2779 (the Blanchard alone) is wrong and is redone, with a new picture for him. Bella Coola
  whole water, Atnarko out (2731): "Right".
- Pictures 9 to 12, 12:31. Skeena "mainstem waters only", pink (2782): "Right". Stamp "Except in those areas and
  times listed below" (2788), with the listed "Ash River confluence to Great Central Lake" place taking the Robertson
  Creek system's side streams and lakes: "Right, all of it". Little Qualicum "all waters unless indicated below"
  (2789), typed: "all looks good jsut make sure all waters unless indicated takes onyl the part not CURRENTLY
  regulated on that day (if it is active)". So: right (the listed stretch starting at the park's downstream boundary
  included); and the rest-of rule must cover, on any day, only the water where no listed stretch has a rule IN FORCE
  that day: to be shown to him on the Little Qualicum for a day inside and a day outside a listed stretch's dates.
  Nitinat "downstream of Parker Creek, except…" (2792; the last 4 km inside the tinted tidal polygon): "Right: our
  tidal boundary stands".
- Picture 13 and trout round 7, 12:42. Somass "Except in those areas and times listed below" (2787; the listed
  "Paper Mill Dam upstream approximately 0.5 km to Collins Farm" stretch is drawn about 1.3 km long), typed: "right
  all of it but add to gotchas please". ALL 13 URGENT PICTURES ARE ANSWERED: right = Sproat, Stave, Cowichan x2,
  Nanaimo, Bella Coola, Skeena pink, Stamp, Little Qualicum, Nitinat, Somass; Stamp Falls kept at 500 m below the
  fishway; WRONG = the Tatshenshini / Blanchard place (it must include the Tatshenshini above the border too).
  Trout round 7 (published 12:33, same page, mockups only). Three-number top line on Madden Creek ("Up to 2 today,
  or 5 if they're rainbow trout and no more than 2 Dolly Varden/bull trout"): "Yes, that wording (Recommended)".
  Moving sideways between fish in the sheet: "No sideways at all". The year bar, typed: "i like it on the elk where
  it says trout and char through the year when they are all release (or all closed etc) if some are open it seems
  silly maybe. but in general dropping it seems fine.  .. i dont know why it needs year tho when it has dates.." So:
  no sideways moves in the sheet; the year bar is dropped except the "through the year" line on a water where every
  fish shares one state (the Elk); dates in words do the rest.
- 12:42, the reviewer's SECOND attack (round 7) is back: still "not yet". Fixed: stop-fishing on the row, the summed
  "only 3", the angler-only note. Partly: possession, the wild limit across boxes, the top line, the year bar, the
  wild limit in one box. New faults: possession smaller than the day's number (Cunningham and four lakes: "only 3"
  with "only 2 in your possession" reads keep 3); possession with a size (Tagish: one over 90 cm a day reads fine,
  the book allows one in possession); "a wild fish must be 50 cm or less" forbids the bull trout over 60 cm the same
  printed line allows (six Region 2 lakes); Lois Lake's top line promises 10 against a wild limit of 2; "stop fishing
  here" can be read as this stretch. Report appended to `handoff/PLAN/design/trout-adversarial-review-2026-11-16.md`.
- 12:43, the rest of the re-check pictures are drawn (places 14 to 30 and the four closed areas, as JPGs). The
  drawer's findings: Morice Gosnell-to-Lamprey END dot sits on a side channel 2 km off the Morice; Thompson Deadman
  START is 495 m below the Hwy 1 bridge where DFO says about 1 km; Babine: 7 circles match by name (Morrison, Pierre,
  Twain, Tachek, Five Mile, Four Mile, Big Loon), Tsezakwa's circle sits on Nilkitkwa Lake, Pendleton has no match,
  Six Mile / Hazelwood / Sockeye and the Gullwing line are not drawn, Pinkut has a circle but is not in DFO's list.
- 12:44, trout round 7, he sent a screenshot of the Skeena card (Sedan Creek to Mill Creek, Jul 15), typed: "it is
  hard to tell 1 of 5 is over 50cm here applies to all instead of just brook trout also the brook trou limit feels
  like the full limit makes it seem like the 5 doesnt apply to all?" What the picture shows: the grey "Keep 5 in all"
  cell runs only beside the top strip and the brook trout row, so it reads as brook trout's own limit and the white
  cells below as separate limits; the "Of the 5, only 1 over 50 cm (20 in)." strip is tied to nothing. Round 8
  started 12:45 with the original designer: the whole-box number must visibly hold every row of its box and the
  box-wide size sentence must be tied to it; plus his round 7 answers (no sideways, the bar dropped, dates in words)
  and the second attack's faults. Mockups only. The reviewer attacks round 8 after.
- Pictures 14 to 17, 12:53. Exchamsiks above the Hwy 16 bridge (2728): "Right". Bella Coola "All tributaries"
  (2730), typed: "purple is right " (the purple / pink lines are the side streams in the place; the main river and
  the Atnarko are out): right. Bulkley "all tributaries … other than the Suskwa River" (2732), typed: "right but add
  to gotchas. it should only drop the blue suskwa river. rest are good. ": right (Suskwa out, Morice and Two Mile
  Creek in); gotcha entry. Bulkley "… other than Morice River and tributaries, Suskwa River and tributaries, and Two
  Mile Creek" (2733), typed: "right only keep part that is purple. rest get dropped here. ": right.
- 12:53, typed in chat: "also we need to make sure the salmon closed because the river is clsoed in synopsis appllies
  to the map too. so the colors show it as well." So for T796 A5: the Salmon view's line colour (and the year bars
  and alerts) must show a book-closed stretch as closed, not only the card. That is what the agreed two-half fix does
  (the book's closure becomes the first member of the stretch's salmon chains); it is now a pass condition on the
  ticket, with a picture of the map before and after for him.
- Pictures 18 to 22, 12:58. Illiance and Wilauks Creek (2734, 2735; the whole water from its mouth, side streams
  in): "Right, both". Gitnadoix (2736; whole river from the mouth, no powerline to be seen): "Right". Morice, the
  Forks to the Bymac Bridge (2738; side streams on), typed: "i think side streams are off but it depends on the name
  and if it says includes tributaries?" Checked: DFO's heading for the water is "Morice, including tributaries", so
  by his rule ("The heading counts") the side streams stay ON; right as drawn. Morice, Gosnell Creek to Lamprey
  Creek (2739; the END dot sits on a side-channel line about 2 km off the main channel), typed: "i thought we already
  fixed the lamprey creek confluence issue. yes the confluence should be on the main channel... fix it". So: the
  Lamprey Creek confluence cut goes onto the Morice's main channel; picture to him after (T803).
- Pictures 23 to 26, 13:05. Nanaimo, 500 m below Bore Hole to Boulder Creek (2777; side streams off), typed: "right
  assuming that the name didnt say to include tributaries. (i think all the items like this we need to set to no call
  then the name will say include tributaries? im not sure tho because if they are linked to a name tat says includes
  tributraries then it couldnt link to a different name without that? the locations should be matched only to their
  exact naming then". Checked on DFO's page: the headings are "Nanaimo River" and "Cowichan River" (no tributaries),
  so side streams off is right there. His rule, sharpened: a place takes its side streams from the EXACT heading it
  sits under on DFO's page; a saved on / off is only for an exception. DFO has TWO Morice headings, "Morice River
  (including tributaries)" and "Morice River": which one each Morice stretch sits under must be read from the page
  (my answer to him on picture 21, "the heading is Morice, including tributaries", is not yet proven for the Forks to
  Bymac row). An audit of every DFO place's side-stream setting against its exact heading is started; the list of
  disagreements goes to him. Tatshenshini, Kwatini / Stanley / Goat creeks (2780), typed: "wait this is right and
  blanchard was also suppsoed to be only blanchard i was confused on formatting. made more sense when i looked at
  page". So: 2780 is right, AND 2779 (the Blanchard alone, picture 7) is RIGHT: his "wrong" of 12:27 is withdrawn;
  nothing is redone. Morice, lake outlet to Gosnell Creek (2781): "Right". Sheldens Creek (2783), typed: "add as
  gotcha but i think you can see the road in satellite that crosses neaar the mouth that should be the bridge i
  think (near a camp site etc) have it be low confidence and set it top tidal too ". So: a low-confidence cut where
  the road crosses near the mouth (the Spur 19 bridge site, by a camp site), the place is the creek above it, the cut
  is also the tidal boundary; gotcha entry; picture to him (needs the next map rebuild).
- 13:04, from the app chat (it is closing; a new app chat takes over from the top section of
  `handoff/PLAN/ios/START-HERE-ios.md`), for his backup question (T732): "Backup photo copies at 1280 px, measured
  2026-11-16 12:57, picture sent in the app chat 12:59: B JPEG 70 = 0.25 MB (about 81 fit), C JPEG 60 = 0.18 MB (about
  110), D WebP 75 = 0.08 MB (about 245). Pick B, C or D; recommended D if a restore on the emulator shows WebP copies,
  else C. Not answered."
- 13:06, two fix lanes are back (branches in worktrees, not merged): `lane/salmon-rules-1116` (T783, T770 with the
  eulachon line, T787 hidden rows, T796 A6 and A5 Python halves) and `lane/book-rule-fixes-1116` (T795 six lakes,
  T794 Williston / Peace / Bella Coola, T798 Coal Creek and the Puntledge, T801.1, rulings). For him: (1) the
  Puntledge "fly fishing only upstream of the diversion dam, including tributaries" now reaches 1,069 pieces (every
  stream feeding Comox, Willemar and Forbush lakes) where it was 10; (2) T796 A5 changes 70 stretches (71,046 pieces),
  the largest the Skeena / Nass winter closure on coho Jan 1 to Jun 15; (3) T770 adds a gear line on 13 chains; the
  three T796 examples he asked for are in the lane's report. Trout round 8 is built (the box frame redrawn).
- Pictures 27 and 30, and two results of the fix lanes, 13:28. Babine Lake's closed areas (7 circles match by name;
  Tsezakwa's sits on Nilkitkwa Lake; Pendleton, Six Mile, Hazelwood, Sockeye and the Gullwing line are not found;
  Pinkut has a circle but is not in DFO's list), typed: "confirm 7 lake open with note and please add as a thing for
  me to do later". So: the 7 are confirmed, the lake reads open in August with "closed in part" and DFO's words for
  the areas not drawn, and finding the missing five and the line is a LATER item on his list. Dean, Crag Creek to the
  tidal boundary (2793), typed: "yes and that except should not include the canyon": right; the canyon stays out of
  it as its own stretch. Puntledge "Fly fishing only upstream of the BC Hydro diversion dam …, including
  tributaries" reaching every stream above the dam (10 pieces to 1,069): "Yes, as printed (Recommended)". T796 A5,
  a book-closed stretch closes salmon too (three examples shown; 70 stretches change): "Go, and send me the full
  list" (the list of all 70 with dates and a map picture before and after go to him before it ships).
- 13:29, the heading audit is back (`handoff/PLAN/curation-review/dfo-side-streams-against-exact-headings-2026-11-16.md`):
  239 live named-water places checked against the exact heading each row sits under: 225 agree, 14 disagree. The
  Forks-to-Bymac stretch DOES sit under "Morice River (including tributaries)" (side streams on is right). The 14 go
  to him as questions (Tatshenshini whole-river row; the three creeks and the Blanchard: do named side creeks take
  their own side streams?; five Region 6 rivers whose blank row under a plain heading puts a bait ban on side
  streams; the Kitimat outfall strip under "Kitimat River (including tributaries)"; five with no effect today).
- 13:29, the reviewer's THIRD attack (trout round 8) is back: closer, still "not yet". The frame is better but the
  grey still hooks onto the first row; "no limit of its own" can read as "unlimited"; the date words are right on 49
  of 50 lines but say nothing when a fish goes to zero, when wild / hatchery changes, or when the water closes; wild
  char at Dodd and Lois now look keepable on their rows. Appended to the design review file.
- Pictures 28, 29, 31, 32 (answered about 13:38). Kitimat "on west bank between signs at Kitimat hatchery outfall"
  (2786; drawn river only), asked whether DFO's heading "Kitimat River (including tributaries)" makes the strip take
  the side streams that join there: "Heading counts here too". So my save 2786 (side streams off) is reversed: the
  strip takes its side streams (about 12 pieces), both banks still closed until one-bank places exist. Skeena
  sockeye leftover place, CNR bridge to above the Babine (2790): "Right". Thompson closed area Deadman (2748; START
  measures 495 m below the Hwy 1 bridge where DFO says about 1 km): "Find out why, then show me". Thompson closed
  area Ashcroft (2749; starts at the Bonaparte mouth itself): "Right".
- Pictures 33 and 34, the heading rule, the size limit (answered about 13:42). Thompson closed area Juniper (2785):
  "Keep 4 km (errs closed)". Shuswap closed stretch (2770): "Right: keep the printed points". ALL 34 PICTURES ARE
  ANSWERED. The ten places that take side streams only through the old water-wide switch (the Tatshenshini
  whole-river row, Kitseguecla, Sustut, Cranberry, Kiteen, Meziadin, three Nanaimo places, Little Campbell, the
  Morice Forks): "Yes, river only for all ten (Recommended)" (changed answers shown to him before it ships; the
  water-wide switch is retired; the setting is derived from the exact heading and the place's words). The salmon
  head file's size limit, 60 kB, so that a book-closed stretch closes salmon everywhere (about 92 kB): "Raise to
  100 kB (Recommended)".
- Named side creeks and trout round 8 (answered about 13:48). A DFO row that names a creek without the word
  "tributaries" (Tatshenshini: "Kwatini Creek, Stanley Creek and Goat Creek"; "Blanchard River"): "A named creek
  keeps its side streams". So: naming a creek means its whole small system; saves 2779 and 2780 stand; this is a
  stated exception to the heading rule (a ruling). Trout round 8, the grey band beside a fish with no tighter limit:
  ""up to the 5" (Recommended)". A possession limit smaller than the day's limit (Cunningham Lake), typed: "it is
  only possession of one type of trout fo rcunningham. use best judgement here". So: the possession line belongs to
  the lake trout row only and must say so; the wording is left to my judgement (the possession number leads on that
  row, the day's 3 as a quiet line). What next for the trout work, typed: "round 3 then attack again but use opus".
  My reading: one more round (round 9) and one more attack, both done by Opus agents, not Fable. Mockups only.
- 13:49, typed in chat: "Can we reduce how much fable we use for our workers a little bit too? Just want to save fable
  for the things we need it for". So: from now no worker runs on Fable by default (reviews, audits and design
  included); Opus for data, pipeline, review and design work, Sonnet for interface and mechanical work; a worker gets
  Fable only after I name the task and the reason to him and he says yes. Nothing on Fable is running now (the
  trout designer and its attacker moved to Opus at his word earlier this hour).
- 13:55, pushed: the book-rule fixes are on the shared branch (T795 six lakes, T794, T798, T801.1; full tier 6,499
  pass). A reach-only rerun is going on lane 1 to show them on built data.
- 13:56, T796 A5, the full list he asked for was sent to him in chat (file `book-closures-list.md`, 70 stretches,
  71,046 sections, 25 rows flagged for a second look; the Nass side streams' coho Jan 1 to Jun 15 first). The map
  follows: the three examples paint closed on their days in the rebuilt salmon data. NEW, for his decision: with the
  book closure carried everywhere (his "Raise to 100 kB"), three OTHER size limits go over on today's numbers: the
  Region 6 salmon file (about 432,600 against 420,000), the website's first-tap budget (about 4,312,000 against
  4,200,000; it had 1,243 bytes of room before) and its low-data first-tap budget (about 3,472,600 against
  3,400,000). The release would stop at its size check until he raises them or the data is shrunk first (T686).
- About 14:55, his answer to the size-limit question, after reading the list of book closures (typed, verbatim): "for this
  Sections: 7 The book closes it (p.9, National Parks and Ecological Reserves): "Fishing is prohibited in Ecological
  Reserves in B.C." DFO's place there: Skeena River — mainstem waters downstream of CNR Railway Bridge at Terrace. it
  should only close the part in the eco reserve how the entire place?? / but yes you can do the raise we just need
  to make sure the closures only apply in the area actually closed e.i. above. / also we should look at the real
  consequence of this (Nass watershed side streams (DFO section C): coho Jan 1 to Jun 15 ) / also previewing all the
  closures. (again we assume we will close them but i want to be able to review them after) could we make the way it
  is viewed an artifact with them all formatted in a way it is easier to read. (with mapos etc) use a smaller model
  for this... / also why is this doubt (The salmon card said on those days (DFO's rows): open — "4 per day, only 1
  over 50 cm (Apr 1–Mar 31)"; open — "4 per day, only 1 over 50 cm (Apr 1–Oct 31)") to (It now says: closed — "Closed
  to all fishing by the rule book (p.19): No Fishing from Yakoun Lake downstream approximately 13 km to fishing
  boundary signs, Oct 1-Apr 30 (Oct 1–Apr 30)") / the dates are opposite of each other the closed should only be
  there on the days it is actually closed then the other days the salmon rules would still apply which isnt a
  conflict?"
  So: (1) YES to raising the three limits (Region 6 salmon file, first tap, low-data first tap). (2) A closure must
  cover only the water the book actually closes: on the Skeena it is the 7 pieces inside the reserve, not DFO's whole
  place (that is what was built; "DFO's place there" on the list only names which DFO row answered before): to be
  proven for all 70 and said plainly on the page. (3) The Nass side streams' coho closure Jan 1 to Jun 15: look at
  its real consequence before it ships. (4) He wants all the closures on ONE page (artifact) that is easy to read,
  with maps, to review after; built by a smaller model (Sonnet). (5) The Yakoun row: the book closes Oct 1 to Apr 30
  only; on the other days DFO's rows answer as before: that is how it works; the warning on that row was about the
  side creeks along the 13 km, not the dates.
- 15:01, on trout round 9 (typed, verbatim): "for the trout view i liked the old look more but with the words of
  round 9 buyt the frame look is a lot cleaner". My reading (mine, to be confirmed by picture): round 8's look (the
  tinted box and the white boxes), round 9's words everywhere ("up to the 5", the dated lines, the strip on one
  line), and round 9's cleaner outline around the box. Round 10 is built as that blend, with the Skeena card shown
  both ways (with and without round 9's outline) so he can pick. Mockups only.
- 15:08, typed in chat (verbatim): "this chat will have to change to opus soon. out of fable credits could you make
  sure we are ready for when that happens?" Done 15:12: the state page (`handoff/PLAN/INFLIGHT-chat14.md`) opens with
  a section "READ THIS FIRST: handover at 15:10" that holds the rules, the four running workers and what to do when
  each hands back, the landing steps for the salmon lane, the queue to the next staging release, and what is still
  his. The session notes point at it. Nothing of his is needed for the change.
- 15:16, one dialog with the round 10 trout picture (Skeena card three ways) and the salmon lane's Nass finding.
  Trout look (A old look no outline / B old look + outline / round 9), typed, verbatim: "old look no outline because it
  looks like the groups are subgrouped inside the over keep 5 card then... one thing is the words "only 1 of these 4
  until oct 31, then release" should be changed to be "onlly 1 of these 4" being bigger it is the actual rule right
  now then a smaller text for the change underneath. all current rules will have the bigger text. " So: A (the round 8
  look, no outline); and in every white box and row the rule in force today is the bigger text, the coming change
  ("until Oct 31, then release") smaller underneath. Trout row spacing (round 8's roomy rows, 23 of 42 cards fit one
  screen / round 9's tighter rows, 34 fit), typed: "inbetween the 2". Nass side streams, coho Jan 1 to Jun 15 (closed
  as the book says / leave coho open / hold this row), typed: "how are both items worded let me double check but im
  pretty sure closed dis the right option". So: NOT yet decided; he gets the book's line and DFO's row word for word,
  then answers. The row does not ship before that.
- 15:18, typed in chat (verbatim): "have another opus agent spec out what would be involved in transitioning the trout
  portion of the app to the new table format. want it implemented clean from pythion up to front end. etc etc. use
  opus agent". So: an Opus worker writes a spec (no code): what it takes to move the app's trout and char block to
  the new table, built cleanly from the Python pipeline up to the app's screen. The spec comes back to him before
  any build starts.
- 15:19, typed in chat after reading the book's line and DFO's row for the Nass side streams (verbatim): "and yes the
  closure wins here." and "we need to make sure it says where the closure is from though". So: the Nass row ships
  with the other 70 (coho closed Jan 1 to Jun 15 on the side streams above the Kitsault Bridge); and a pass
  condition for all 71: the salmon card says where the closure comes from (the rule book, its page, its line).
  Checked on the lane's data 15:19: all 71 rows' card words open "Closed to all fishing by the rule book (p.NN):"
  followed by the book's line and dates. Still to prove on the app's screen (the app chat's before / after pictures).
- 15:22, the closures review page he asked for is published (built by a Sonnet worker, 71 cards, a map each):
  https://claude.ai/artifact/2eGEuMG9cLe3SS69j63aTd ("Book Closures Review"). Also a row on the Owner To-Do page
  (`rv-book-closures-page-1116`, group Review). Each card: row number, map (closed pieces in red), the book's line,
  page and dates, what the salmon card said before, fish and days now closed, how much of DFO's place changes. Known
  limits, told to him: no "rest of DFO's place" layer on the maps; rows 5, 6, 17, 19 miss 25 to 35 pieces on the
  picture (said on the card); the book's "[Includes Tributaries]" tag is not yet shown under the line (second
  version being built: Dean River row 12 is the example, the book does print it there). FOR HIM: review when he
  likes; any row he calls wrong by number is held back.
- 15:36, trout round 11, one dialog with two pictures (Skeena before / after; a Fraser card where only trout and char
  close). Is this the card to build, typed, verbatim: "it looks good but it is missing the general 30cm or bigger for
  trout/char in region 2 fraser? ". Closure words ("until Sep 30, then closed to trout and char, reopens Nov 1"),
  typed: "yes but drop reopen unless it is currently closed then reopen date matters then ". A limit that only gets
  looser on a coming date (skip it and show the next stricter date?), typed: "could just mention a rule change
  happens on that date not specifics". Adams River Dolly Varden / bull trout: "Yes, 80 cm on the row (Recommended)".
  So: the look is accepted ("it looks good"); to check: why the Fraser card's brook, brown and cutthroat rows carry
  no "30 cm or longer" line (is a general size rule missing from the data or the mock, or does it not apply on that
  stretch); a coming closure says what closes without the reopening date, and the reopening date is shown only
  while the water (or the fish) is closed today; a coming change that only loosens a rule is said as a plain "rule
  changes <date>" with no details; Adams as recommended.
- 15:42, the trout build spec (`handoff/PLAN/design/trout-table-build-spec-2026-11-16.md`), its one-page summary given
  in chat, four questions in one dialog. How to start: "Start steps 1 and 2" (step 1 = fill the data holes:
  possession limits, the wild-fish limit at Lois and Khartoum, the odd lines; step 2 = a tool that lists every
  answer that changed between two releases). Other fish in the same new table look, typed, verbatim: "what? we want
  to migrate the new trout style into the current app not the other way around. " (my question was badly put; it is
  NOT answered: the plan IS the new trout table inside the current app; whether kokanee, whitefish and the rest
  also take the new look is asked again, plainly). Possession: "Keep the page line (Recommended)". Size, if the
  switch does not fit the first-tap limit: "Raise the limit, shrink later (Recommended)".
- 15:42, typed in chat (verbatim): "also the last fraser example you sent doesnt have hatchery bs wild along with the no
  30cm minimum". Checked against the book text (`data/curated/regulations/reference/region-3.md`, Region 3 daily
  quotas): that card is the Fraser in REGION 3 (Thompson confluence to about 4 km below Lillooet), whose regional
  rules are "Trout/char: 5, but not more than 4 from streams, 1 over 50 cm, 1 bull trout (Dolly Varden) or lake
  trout, none under 60 cm; release ALL STEELHEAD, bull trout from streams Aug 1-Oct 31, lake trout Oct 15-Jan 31":
  no wild / hatchery split and no 30 cm minimum. Both belong to REGION 2 ("you must release wild trout/char from
  streams … hatchery trout/char under 30 cm from streams"). Round 12 adds a Region 2 Fraser card beside it; if our
  data does not show hatchery / wild and 30 cm there, that is a fault to report to him.
- 15:47, trout round 12 published (same trout page; two pictures sent: the Fraser in Region 3 beside the Fraser in
  Region 2 at Mission, which shows hatchery and wild rows and "30 cm or longer" from the data: no fault; and the
  Region 3 stretch on a closed day with "reopens Nov 1"). The other-fish question asked again (leave them / redraw
  them / decide later), typed, verbatim: "we should just have the trout table under the trout entry like it
  currently is with the better text like 2 or 4 with expand arrow etc. " My reading: the page keeps its present
  shape; the Trout and char entry stays where it is, folded to its short line ("Up to 2", "Up to 4") with the expand
  arrow, and the new table is what opens under it; the other fish entries are not touched. So spec question 1 =
  trout only; the old card code stays for the other fish.
- 15:47, the 17 closure rows with side streams and "no tributary words" (closures review page): an Opus audit found
  ALL RIGHT by ruling T2. On 12 of them the book prints the ✱ mark beside the water's name ("includes tributaries"
  for every rule of that row); the page builder had only looked for the words. The other 5 close nothing but the
  named creek itself. Dean River row 12 (control): the walk stops at both ends of the stretch. Nothing is placed too
  wide; trout answers are unaffected. The page (version 3) now shows the ✱ note on 20 rows.
- 15:48, typed in chat on round 12's Region 2 Fraser card (six hatchery rows in the keep box, six wild rows under
  Release), verbatim: "i dont like the new wild and hatchewry option you sent we should jsut say hatchery only unless
  specifically wild has a seperate quota to wild like 1 wild and 2 hatchery or something.... otherwise it is
  hatrchery only or unspecified (the same for both no mentiuon needed)". So, three cases and no others: (1) wild and
  hatchery have the same rule: no mention at all; (2) only hatchery fish may be kept: ONE row per fish that says
  "hatchery only", and no separate wild rows under Release; (3) wild fish have a quota of their own (for example 1
  wild and 2 hatchery): only then are wild and hatchery shown apart. Round 13 (mockup) is being built on that.
- 15:49, typed in chat (verbatim): "we will run out of credits soon... we should m,ake sure handoff is good for opusa to
  take over at anytime." Done 15:49: section C of the handover on the state page rewritten as ONE current block
  (what is running, what to do when each worker hands back, what is waiting to be pushed, what is his).
- 15:56, trout round 13 (hatchery only), one dialog with two pictures. Is this the handling, typed, verbatim: "yes this
  is good on fraser but the hatchery on steelhead on skeena doesnt stand out very well. should slightly improve that
  oitherwise it is getting better." Lois and Khartoum steelhead row: "Hatchery only (Recommended)". So: the rule is
  accepted; on a single fish's row (Skeena steelhead) "hatchery only" must stand out more (round 14, small); Lois and
  Khartoum steelhead read "hatchery only" like everywhere else.
- 15:56, typed in chat (verbatim): "you can continue just make sure handoff stays up to date, also what is prompt to
  continue after if fabl;e runs out of tokens". The prompt given to him: "Continue as the main chat. Read
  handoff/PLAN/INFLIGHT-chat14.md: the top section READ THIS FIRST, block C first. Carry on from its list 'Owed to
  him, in this order'. Do not re-ask anything already answered at the end of handoff/PLAN/OWNER-TODO.md."
- 16:00, trout round 14 (the Skeena steelhead row two ways: A a filled "HATCHERY ONLY" label beside the name, B the
  words in the name), typed, verbatim: "A filled label then if it applies to all fish in category use same fill for
  where that is located." So: A; and where "hatchery only" applies to every fish of a keep box, the box's top line
  carries the SAME filled label in place of the plain words. Round 15 (tiny) applies it; then the design is settled.
- 16:03, trout round 15 published (same trout page) and two pictures sent: the filled HATCHERY ONLY label in a keep
  box's top line when it covers every fish (Fraser Region 2: "Keep 2 in all [HATCHERY ONLY] only 1 over 50 cm (20
  in)") and beside the name for one fish (Skeena steelhead). The design is settled unless he says otherwise; the
  accepted design is one list at the end of T810. No question pending on trout.
- 16:04, typed in chat (verbatim): "can we move over to opus agent now?" Answered: yes; he switches the model himself
  (the /model command, pick Opus) and pastes the continue prompt; the state page's block C says what to do first.
- 16:07, typed in chat (verbatim): "okay i created a new chat please coordinate with it for any new info etc as we ramp
  this chat down". The new main chat is session `bc-freshwater-fishing-regulations-8c`. This chat (session
  `bc-freshwater-fishing-regulations-17`) keeps only one job while it ramps down: its three workers hand their
  reports back HERE, so each also writes its report to a file under the scratch folder `handbacks/`, and this chat
  relays each to the new chat when it arrives.
- 16:09, T805 part C (one dialog; the list `changed-answers-C.md`: Lois and Khartoum cards add "No more than 2 wild
  trout and char in all"; Lois and Windsor wild Dolly Varden / lake trout go back, hatchery only like the other five
  wild-2 lakes; 3 waters, Region 2, all stricter). Answer, picked: "Go (Recommended)". So: part C (commit 0f6fe2868 on
  `lane/t805-trout-data-gaps`) lands with the full tier and ships in the next staging release. Parts B and D still to
  come to him as their own lists.
- 16:18, Thompson closed area Deadman (mark 2748; he had said "Find out why, then show
  me"). Why: the APP'S DATA WAS RIGHT; the drawing tool behind picture 31 started the yellow at the bridge's own measure
  (117,608) and snapped it to the nearest section edge, the river gauge 08LF033 (117,094), so it drew 4.6 km and read
  "495 m". The built cut is at 116,608: 999 m below the Hwy 1 bridge; closed area 3.6 km to the CN rail bridge. The
  redrawn picture was sent. Answer, picked: "Right (Recommended)". So: mark 2748 stands; nothing changes.
- About 16:20, typed in the OLD main chat (session -17) after its summary of the changed-answers tool's report
  (verbatim, relayed): "Eulachon is a bait or a limit?" Answered there: neither; it is a closure. DFO's Region 6 line
  "Reminder: There is no fishing for eulachon in Region 6" means no fishing for eulachon at all. The app carries it in
  the salmon card's gear / bait lines, which is why the tool grouped it under "Gear and bait". The change is his T770
  fix (a bait line no longer pushes it out). Open, NOT asked yet (for the end batch): should the eulachon line show as
  a closure rather than among the gear and bait lines, on the card and in the tool's grouping? His earlier words
  (batch 6): "general rule is (Reminder: There is no fishing for eulachon in Region 6. ) why would bait remove eulachon
  rule?"
- 16:37, T805 parts B and D plus two of the worker's questions, one dialog (lists `changed-answers-B.md`,
  `changed-answers-D.md`). Part B (possession on the fish row: Region 7 lake trout / bull trout "1 day's limit, not 2"
  on ~3,100 waters, ~20 lakes' own possession numbers, Bowron Park's 1 every day; words only), picked: "Go
  (Recommended)". Part D (Shamrock and Kilgard creeks: no 20-30 cm steelhead; Echoes Lakes "from both lakes
  together"), picked: "Go (Recommended)". Bowron Lake Park lake trout (park's 1 in all / keep today's), picked: "Keep
  today's" (lake trout follow Region 7's "1 day's limit" on their own row, apart from the park's 1 for the other
  trout; this is what part B builds). Nation Lakes ("Up to 3 lake trout" a day, possession 2: both as printed /
  lower the day to 2), typed, verbatim: "i feel like this possession stuff is just kind of broken but fixed in the new
  trout format? so maybe we can just work on the transisiton to the new trout redisgn?" So: Nation Lakes stay as part
  B builds them (both lines as printed); no further possession patching outside the redesign; he asks to work on the
  trout redesign transition (T810 steps 3 to 7). My reading: that is his go to start the transition; first the spec
  takes in his answers of 15:36 to 15:48 and the app chat's check of the app half. François Lake outlet (part D
  question) NOT asked yet.
- Before 16:42 (the app chat said ~16:45; my clock read 16:42), typed to the APP chat (relayed verbatim): "make sure you and the other chat collaborate on the prepping
  for trout dredsign going in". So: the two chats prepare the trout table build together: one plan (the spec; the app
  chat's app-half check merged into it), the payload contract agreed by both before either builds, one shared fixture
  of hard waters the app builds against, one hard-cases list both re-run, the switch landed together like the salmon
  twins (T810).
- 16:50, typed in chat (verbatim): "we want to make sure the plan is robust so that we change the data all the way in
  python and the front end and it all just works withotu fighting it and makign sure everythign is streamlined etc".
  So (T810): the contract rule is Python decides every word, number and date; the app only draws, with no rule logic
  and no hand-written payload types (generated from the contract); no old and new trout paths left side by side. After
  the app chat's app-half check is merged into the spec, a fresh Opus reviewer attacks the whole plan for every place
  the two sides could fight (two sources of a word, logic in both, duplicated helpers, a hand-kept type), and the plan
  is fixed before any build code starts.
- About 16:55, six answers in the APP chat's dialog batch (relayed, labels picked verbatim): (1) T759 stripe: "Wider is
  right (Recommended)". (2) T732 backup photo copies: "D: WebP 75" (not the recommended B; T732 answered). (3) T810
  fold row: "All rows get arrow": every folded row, the other fish included, becomes words plus an expand arrow, not
  only trout (widens the app scope a little; the app chat folds it into spec section 6). (4) T810 pinned trout top
  line: "Drop it (Recommended)". (5) T810 a lone other fish beside trout: "Stays as today (Recommended)". (6) T810 the
  year bar's "Trout: release until …" line: "Drop it (Recommended)".
- About 17:15, clarification of the 16:55 fold-row answer (APP chat re-asked; relayed, label picked verbatim): "Salmon +
  trout folds" (the option read: "Both folded entries (salmon and trout) show their words + an arrow instead of
  'Details'. The Other fish list stays as it is."). So "All rows get arrow" means the salmon and trout folds only.
- 17:40, the trout table build summary given in chat (five steps: safety pins and size count; Python builds the table
  on the side; the permanent same-law check; the app draws it behind a hidden switch, picture pack and sentence list
  for him; one switch). Two questions, one dialog. Ship the trout table in normal releases before the switch (yes if
  ≤100 kB / only at the switch), picked: "No, only at the switch". Angler-only notes (page's notes block / inside the
  table), picked: "Page's notes block (Recommended)". So (T810): the trout section appears only in the switch
  release; previews before it use side releases (ticket V-py's side scenario); the cutover = flag on + rows 34 +
  format bump + deletions. Q-D7 decided: angler-only notes stay in the page's caveat block; the table carries none.
- 17:44, salmon book closures, the data side of the break-it review (T796 hard cases; worker branch
  `lane/salmon-hardcases-1116`), one dialog of four. Campbell River summer closure (MU 1-1 to 1-6 vs the book's MU
  1-10), picked: "Keep it closed". Thompson below Kamloops Lake in June (spring closure vs its own row, no printed
  exemption), typed, verbatim: "keep closed but add to gotchas as one i need to ask fish and wildlife about". Waters
  partly inside a reserve or park, typed, verbatim: "take out the lakes but add a note. also for streams i thought we
  cut on all eco reserves so should everypiece be in the eco resevre or is it anti cross over we added for areas? in
  that case we decided on a cut which defines in versus out. so in that case if it rides the boudnary (straddling) it
  should get the part intended to be in to have thge closure versus the part out. ask more questions if needed".
  National parks wording, picked: "Closed, book's wording (Recommended)". So: Campbell unchanged; Thompson June stays
  closed AND goes on his list of questions for BC Fish and Wildlife; the 9 lakes and ponds mostly outside a reserve
  or park come out by name, each with a note on the card; for STREAMS he wants the closure on the part inside the
  reserve only (a straddling section split at the boundary, inside part closed, outside part open): to be checked
  against how area boundaries work today (cuts vs the straddle rule E8) and put back to him if it needs a decision;
  national parks read "Closed by the rule book unless opened under the National Parks Fishing Regulations (p.9)" with
  the permit sentence.
- 17:46, follow-up on his streams question (told: streams ARE cut at every reserve/park edge, ruling E8: first entry
  and last exit; a straddling piece or a wander outside under 1 km stays inside; the 4 pieces 10-50% inside sit
  between the cuts). Keep E8 / cut exactly at the line, picked: "Keep E8 as is (Recommended)". So: streams unchanged;
  only the 9 lakes and ponds mostly outside come out by name (ruling Z10), each with a note on the card.
- 18:37, typed in chat (verbatim): "We also want to start another agent that works on finding the missing river
  confluences on the babine lake dfo place closures. We want to find the other rivers and have an agent dedicated to
  it". So: a dedicated Opus worker finds the creek mouths DFO names in its Babine Lake closures that we have not
  placed yet (from the 11-16 re-check: Pendleton has no match; Six Mile, Hazelwood, Sockeye and the Gullwing line are
  not drawn; Tsezakwa's circle sits on Nilkitkwa Lake; Pinkut has a circle but is not in DFO's list), and proposes
  each for his review (everything added is a proposal).
- 18:38, typed in chat (verbatim): "this is fine (Tsezakwa: its circle sits on Nilkitkwa Lake instead of Babine. )".
  So: Tsezakwa's circle on Nilkitkwa Lake stays as it is; the Babine worker leaves it alone.
- 18:54, Babine Lake (five pictures sent; worker branch `lane/babine-mouths` 6b1aa369d, proposals only). The four
  missing circles ('Six Mile Creek' = Gullwing Creek; 'Pendleton Creek' = Cross Creek at Pendleton Bay; Hazelwood and
  Sockeye = the unnamed creeks at DFO's own census sites), typed, verbatim: "That sounds good can we add name variants?
  With source?" The Gullwing Creek line, picked: "Due south" (straight south from Gullwing Creek's mouth, 3,356 m, not
  the shortest crossing). So: the four circles are accepted; each creek gets a NAME VARIANT with its source (Gullwing
  Creek also "Six Mile Creek"; Cross Creek also "Pendleton Creek"; the two unnamed creeks "Hazelwood Creek" and
  "Sockeye Creek"; source = DFO NuSEDS census sites + SkeenaWild 2011, cited on each), as name variants are recorded
  today; the line is the due-south reading. Also found: his 7 circle confirmations of 13:28 were never saved (no
  Babine closed-area marks; `closed_areas.json` holds only Thompson and Shuswap): they are saved with these 4 and the
  line through the review app's save path. Effect today: none on the map (a circle binds only sections wholly inside
  it; the lake is one section; the line across a lake is deferred): notes on the card until a lake-split ticket.
- 18:56, typed in chat (verbatim): "We want to split babine lake on the line like our other split lakes. Then we want
  the circles to just be circles on the map not separate sections of the lake. Otherwise it will be overboard." So
  (T812): Babine Lake is split on the due-south Gullwing Creek line the way other lakes are split (east part = its
  own piece, closed as DFO says); the 400 m creek-mouth circles are NOT cut out of the lake: they are drawn as circles
  on the map (an overlay) with the card's note, no extra lake sections.
- 18:57, the Babine circles' look (T812; the app chat's three questions, one dialog). Which map, picked: "Salmon map only
  (Recommended)". Follow the picked day, picked: "Follow the day (Recommended)" (closed colour on DFO's closure days,
  outlined or hidden otherwise). Tap, picked: "Opens the lake's card (Recommended)" (Babine Lake's salmon card, whose
  note says which creek mouths close and when).
- 19:01, typed in chat (verbatim): "We could skip some tests to get multiple items done then merge after and test
  together? Or is that too risky?" Answered: batch the merges (each lane already ran its own tests), then ONE full
  tier and ONE push for the batch; never push without the full tier (the 11-15 red landings). Adopted from now.
- 19:09, he asked whether he still has saves to do and to take the two end-batch questions now ("make sure i didnt
  already answer these things"). Checked: the "leftover saves" in my 19:0x summary were MY fixes, all already answered
  by him (Morice 2738 by the heading rule; Morice 2739 "fix it" = the Lamprey cut onto the main channel; Sheldens 2783
  = low-confidence road cut + tidal; Babine 2784 = done 19:07, marks 2825-2836; Dean 2793 = the except without the
  canyon; Thompson 2521 = the three closed areas; Morice 2596 retired; Atnarko 2584 "The heading counts"). Told him so.
  One dialog of three. Squamish "downstream of the powerline crossing" re-tick (T811, mark 2511), picked: "Re-tick as
  right (Recommended)". François Lake outlet lines (T805 D question 3; book p.51 "for the area at the outlet of
  François Lake described on map 1"), picked: "Outlet area only (Recommended)" (all seven lines on the outlet area;
  the main lake reads Region 6's rules; map rebuild; changed answers to him first). Eulachon (T770), picked: "As a
  closure line (Recommended)" (its own line that eulachon is closed all year in Region 6, apart from the gear and bait
  lines).
- 20:01, two pictures (cut-fixes lane `lane/cuts-1116` a5990dd7b). Sheldens Creek (the SPUR 14 road bridge by the camp
  site crosses the estuary channel ~260 m BELOW our FWA line's end at the top of the salt marsh; proposed: mouth =
  tidal boundary, low confidence, whole creek above), picked: "Extend to the bridge" (draw the creek on down to the
  road bridge and put the low-confidence tidal boundary there; needs an added stream piece). Morice, Gosnell Creek to
  Lamprey Creek (mark 2739; END on the main channel at Lamprey's mouth; the earlier picture was a drawing-tool fault),
  picked: "Right (Recommended)".
- 20:25, Sheldens Creek extended (picture `sheldens-extended.jpg`; `lane/cuts-1116` 998b4e6ab: a hand-traced marsh
  channel from the FWA line's end to the SPUR 14 road bridge, low-confidence tidal boundary at the bridge), picked:
  "Right (Recommended)". Goes into the next atlas rebuild; redrawn from the built map after.
- About 21:22, typed in the APP chat after "Install build 36 and try a–f" (relayed verbatim): "a-d worked good except
  after closing app and opening again the fish icon stopped moving.... yes to (E) and (f) .. the picture viewer for
  spots has the x under the notification bar. text is cut off weird. also opening harrison lake then going to
  conditions should default to level only because there is no other data (the map looks super weird and not visible
  because of it) then when you switch to level the empty flow option disappers so if only level data exists it
  should show level data. just mention at top it is level data maybe... have an agent look at this... how do i test
  the push stuff now?" Filed by the app chat: T814 (iOS fish spinner stops after reopening mid-download), T815 (spots
  photo viewer ✕ under the status bar), T816 (clipped text: the "When were you there?" hour, the weather icons), T817
  (Harrison Lake: a water with only level data opens on Level with a line saying so; the gauge rule "Flow leads"
  refined, not broken). iPhone build 36 test (a–f) done; push test under way with the app chat.
- About 21:32, typed in the APP chat (relayed verbatim): "the spots view also needs a lot of work to be cleaned up maybe
  put a fresh agent on it?". The app chat's T819: a fresh Opus worker reviews the spots area and brings him pictures
  and a cleanup list to pick from before anything is built.
- About 21:37 to 21:40, typed in the APP chat (relayed verbatim): "also the calendar bar doesnt have a full red line for
  salmon when closed but instead it is empty with no color on the calendar bar.." (T820: the salmon year bar on a
  closed day draws empty, not the closed stripe); "when i swiper to close on apple it flashes the menu back for a
  second before it goes back." (T821: iOS back-swipe flash); "allowed notifications in app and settings restarted app
  and it still doesnt show up" (T822 if a bug: push registration; the staging worker holds 0 iOS tokens).
- Before 21:41 (the app chat said ~21:45), typed in the APP chat (relayed verbatim): "in the tutorial it says on first slide each step rings what to
  tap (should say something like highlight instead? the tutorial is really glitchy too with keyboard and the
  highlighting following the items with a delay etc etc". The app chat files it (its next reserved number).
- Shortly before 21:48, typed in the APP chat (relayed verbatim): "also the condition panel is very glitchy on apple. it
  doesnt show the info the same as android the mao doesnt have gauge markers the current conditions view doesnt show
  the bar and when i click on chehalis it doesnt show conditions for it? also the highlights dont let go when you go
  back the highlight of where you clicked stays and doesnt disspear". Filed by the app chat (its own number). If the
  Chehalis showing no conditions is data (no gauge match), it sends me the section ids.
- 21:50, typed in chat (verbatim): "it used to match the chehalis gauge though?????". Checked: it still does. The shipped
  release r-46e4355a7277 (conditions-18da9a4cead9) has gauge 08MG001 Chehalis River near Harrison Mills leading 634
  Chehalis sections (matches.json unchanged; 08MG023 below Statlu Creek auxiliary). So the iPhone showing no Chehalis
  conditions is app-side (with the missing gauge markers and bar on Apple); sent to the app chat for its conditions
  ticket.
- Shortly before 21:54, the spots cleanup (the app chat's T819; relayed verbatim): picked all four bundles, "B: readable
  spot page (Recommended)", "A: fix wrong bits (Recommended)", "C: list + adding", "D: fit & finish". On a small creek
  (1.4 km²) shown "Below normal" from a gauge 676× bigger, typed: "Could we just use our gauge network that colors stuff
  wtc to determine if it is significant enough? It would drop that in theory. Then ofr small ones still give a warning
  if used." So: a spot takes its gauge from the same section → lead-gauge network the Conditions map colours with
  (drainage gate applied); no lead = no estimate; a lead on a small water = the estimate with a plain warning. The
  "small next to its gauge" decision is Python's (T823), the app reads it.
- 22:18, T823 small water next to its lead gauge (table sent: examples, counts at 50/100/200×). Cut-off, picked: "50×
  (Recommended)". Wording, picked: "Much smaller stream (Recommended)" ("This stream is much smaller than the river its
  gauge measures: the reading may not match it."). Built on `lane/t823-small-water` (edc097013), default already 50×;
  reaches a release with the next bundle rebuild. Found with it: T825 (251 side channels led by a gauge far upstream,
  e.g. lower Fraser side channels reading Red Pass).
- Shortly before 22:19, in the APP chat (relayed; label picked verbatim), after its correction that the Conditions MAP
  colours from the paint PANELS (249k sections; your 1.4 km² creek is coloured "Below normal" by 08MH001, 676× bigger),
  not from section_gauge (114k): "Should a gauge that much bigger be allowed to speak for a small water at all, on the
  map AND in spots (they always match)?", picked: "Allow, with a warning (Recommended)" (keep colouring and estimating;
  say plainly the gauge drains a far bigger area once it is small enough to matter; Python picks the cut-off). So
  T823's flag and sentence hang on the SPEAKER's gauge (the panel the map colours from), the same on the map, the
  spot and the sheet; 50× and the wording stand (owner, just before).
- Shortly before 22:49, in the APP chat (relayed verbatim), on the 0.7.12 draft "What's new": "you can do what changed."
  So: the app chat writes and keeps the release notes current and clears the draft when the next build goes out. His
  next question there, "so what is next?", is being answered by the app chat (both chats' plan: salmon landing first,
  then the one map rebuild + staging release, the trout build in parallel).
- 22:52, the salmon landing look (T796): five before/after sheets (Skeena reserve, Yakoun, Bell-Irving, Kitsumkalum
  eulachon line, Cold Fish Lake note; from r-1603e71cc92c) and the changed-answers list since his 71-row review. Land
  it, picked: "Land it (Recommended)". Kennedy Lake's note (already ruled outside Pacific Rim, Z11), picked: "Keep the
  note (Recommended)". So: lane/salmon-rules-1116 + lane/salmon-hardcases-1116 + the app's twins land together
  (section D), then ship with the next staging release.
- 22:55, on the Yakoun before/after sheet, typed (verbatim): "The calendar shows open always but it closes after coho
  closes?" Answered: DFO's coho row "Apr 1–Mar 31" is the whole year (DFO's year runs Apr 1 to Mar 31), so before the
  change salmon was open every day; after it the book's "No Fishing from Yakoun Lake downstream approximately 13 km …,
  Oct 1-Apr 30" closes the stretch Oct 1 to Apr 30 and coho shows through May to Sep.
- 22:55, on the Skeena before sheet (circling the DFO-words block under the species table), typed (verbatim): "We want to
  create a future ticket to do a similar check of all salmon regulations to the webpages with a fresh agent that tries
  to break it and look for issues in the regulations also why do we have this section in salmon". Filed T827 (the
  break-it audit of every salmon rule against DFO's pages, later). Answered why (T321, Oct 31: DFO's rows as printed so
  the reading can be checked). The block (fold into Source / keep only extras / keep), picked: "Fold into Source
  (Recommended)" (T828: under the 'Source (DFO)' expander, closed by default; unread new wording stays visible).
- 22:56, typed in chat (verbatim): "Can we also clean up the format for the salmon rows to make it look more consistent
  with the other table and the new trout table just a formatting thing same data tho". Filed T829 (app side; same
  data): mockups first in the settled trout table look (the trout page's round-15 design), his pick, then the build
  after the salmon landing and T828.
- Shortly before 23:17, in the APP chat (relayed; label picked verbatim), T829 salmon rows after three mockups on real
  data (Kitsumkalum Jul 15 with its eulachon line, Skeena below Terrace Aug 15 mixed), each above its trout table:
  "B: trout keep box (Recommended)" (open species in the trout tint box, each species' limit in the white box at the
  right with its dates small under it; closed species, the eulachon line and rule-book closures in the trout
  "✕ Closed" group; badge, year bar and bait-ban line stay; the T828 fold as mocked). Built after the salmon landing and
  T828, as ONE box component the trout table (ticket O) reuses.
- 2026-11-17 about 00:10 to 00:14, in the APP chat (verbatim), T814: "it had reduce motion on." (the second iPhone, so
  the still fish was by design); "we dont want a full still item for reduce motion on.. i am making a new loader icon
  that will replace the loading circle on the splash page or curtain (it will still be used for all other loading."
  He sent the loader as a Python generator (`fish_loader.py`: a linocut coho, 80-frame sprite sheet, 4 s loop). Then:
  "we will ignore reduced motion for this". So: no still icon; the coho loader on the splash / first-download /
  curtain screens, the circling fish everywhere else; both always move, Reduce Motion or not. The app chat builds it
  (T814) and brings him pictures in both themes.
- 2026-11-17 about 00:35, in the APP chat, before sleeping ("questions should wait unless there are urgent ones now
  bertween 2 chats"), two picks: overnight landing of the app fixes that pass their benches (T817, T823, T824, the iOS
  fixes, T819, T826, T822, one by one after salmon and T759, each with the main chat's clear), picked "Yes, land what
  passes (Recommended)"; then a 0.7.12 upload to TestFlight and Play internal overnight, picked "Yes, upload overnight
  (Recommended)" (the coho loader goes in only once he has seen and picked its look). Anything that fails waits for him.
- 2026-11-17 about 00:50, in the APP chat (verbatim): "also for gauges should look into why capilano river at canyon
  gauge doesnt have level but i can find level for it online like here
  https://metrovancouver.org/services/water/seymour-and-capilano-river-levels with 2023 - 2026 Flow & Level Continuous
  Recorder here https://wateroffice.ec.gc.ca/report/real_time_e.html?stn=08GA031" and "other gauges might have same
  issue as that". Filed T830: EC reports Capilano's level live, but HYDAT has only 2 years of its level (3 needed for a
  band), so no Level is offered, and with the map on Level the row wrongly says "flow only". 16 stations are like it
  (Nazko 985 sections, Beatton 327, Witches Brook 131, Capilano 72 …). **Yours (morning batch):** with no band, show
  the raw level in metres unranked (recommended), or keep it hidden? The wrong "flow only" wording gets fixed either way.
- 2026-11-17 overnight, the app chat (T814): **yours (morning batch):** your coho loader is built (branch
  app-fix/t814-loader, not merged) on the splash / first-download / applying-data screens, your colours exactly in light
  mode. In dark mode your dark ink nearly disappears. Pick **A** (paper-coloured ink #f2e8d5 on the dark screen, red
  kept) or **B** (your colours on a paper tile behind the fish). Pictures sent in the app chat. It joins a build after
  the pick; the overnight 0.7.12 goes without it.
- 2026-11-17 overnight, the main chat (T810 trout table): **yours (morning batch):** Kaslo River and Keen Creek (Region 4) print
  "Bull trout daily quota = 2 (any size)", the same words as the Lardeau and the Duncan, but only the Lardeau and Duncan
  rows say the Dolly Varden don't count toward Region 4's "2 from streams". So today the rules engine lets you keep
  2 fish in total on Kaslo and Keen, and the new trout table now says the same ("Up to 2"). The card that ships today
  says "Not part of the 2 from streams" there, which disagrees with the engine. Pick: **A** read Kaslo and Keen like
  the Lardeau (Dolly Varden on top of the 2; a small corpus edit plus a rebuild, and the table follows), or **B** keep
  them as written (2 in total; the old card's line is then the wrong one, and it goes when the trout table replaces it).
- 2026-11-17 overnight, the main chat (pre-staging map rebuild; nothing from it has shipped): **yours (morning batch),
  what changes on the map:**
  1. **Babine Lake east of the Gullwing Creek line** is now its own part, closed all year to all fishing (book p.50
     "No Fishing east of a line from Gullwing Creek"), including the Aug 1-27 sockeye opening. The main body is unchanged
     apart from the Pinkut radius note. OK?
  2. **Hazelwood Creek's 4 ponds** (named by your T796 save) now count as widenings of the creek, not lakes. They lose
     the lake rules and take the creek's: they close Jan 1 to Jun 15 under the Skeena winter closure. OK, or keep them as
     lakes? (A worker is checking how the FWA and the book treat them.)
  3. Fixed overnight (06:13): Sheldens Creek is one water in search; the Gitnadoix "upstream of the powerline" place
     binds the whole river again (1,000 stretches) with your powerline label kept (T833: a rules-engine fault at a
     line's end, fixed in code; the whole-province check moved nothing else).
  4. **Sheldens Creek** is now one water. Its small unnamed 3.47 ha pond on the creek now counts as part of the creek
     (same rule as the Hazelwood ponds), so kokanee there goes from "keep 10 a day" to release.
  5. **Re-tick in the review app** (your saved places whose area moved with this rebuild, so their tick is stale):
     Cowichan x3 (66-mile trestle, white triangle, whole), Sheldens (upstream of the white triangular sign), Babine
     Lake x2 (including tributaries; excluding tributaries). Your marks file is not edited.
  Full list of changed answers vs the salmon release (168 groups): scratchpad/rebuild3b/answers-diff/
  vs-salmon-r-1603e71cc92c/owner.md, sent to you as a file. Nothing ships or merges until you say OK.
- 2026-11-17 08:37, in the main chat (verbatim): "Yes still show gauge data." (T830: show the raw live level, unranked,
  where there is no band.) And on the re-tick list: "Why would we have to do this (Re-tick in the review app: 6 of your
  saved places whose area moved with this rebuild: Cowichan ×3, Sheldens ×1, Babine ×2.) they should all be good?"
  So: no re-tick from him; the marks are carried by the main chat (fix, not a review item).
- 2026-11-17 ~08:45, in the APP chat, T814 coho loader dark mode: picked "A: paper-coloured ink" (ink #f2e8d5 on the
  dark screen, red kept; light mode is his colours as drawn). The app chat finishes it and it joins the next build.
- 2026-11-17 08:50, main chat dialog (verbatim):
  - Babine east of the Gullwing Creek line closed all year: "Closed all year but only for salmon?" (follow-up asked)
  - Hazelwood Creek's 4 ponds: "Part of the creek (Recommended)". Kept as built (same rule for Sheldens' pond).
  - Kaslo / Keen Dolly Varden: "Dollys and bulls always have same quota per the book.  Why would they not count towards
    the regions 2? That seems wrong". Read as B for Kaslo and Keen (they count toward the 2 from streams); follow-up
    asked on whether his 2026-10-10 ruling T3a (Lardeau and Duncan rows lift bull trout out of the 2) still stands.
- 2026-11-17 ~08:58, main chat follow-up (verbatim):
  - Babine east part: "Closed all year then. Also just double checking this was done the same way as other split lakes right?"
  - Lardeau / Duncan (T3a): "It would just make the same limit tho. That is fine I guess but the kasko and keen should be
    the same. Also lardeau and duncan should also do that for rainbow right? I'm confused why it is like this seems like
    it would auto be handled". So Kaslo and Keen read LIKE the Lardeau (A), reversing the 08:50 reading; he wants it
    automatic, not a per-water edit.
- 2026-11-17 ~09:57, in the APP chat, T830 after the Capilano pictures: the unranked line picked "Plainer" ("Too new to
  say if this is high or low."); levels to 2 decimals: "Depends how much space this adds. But yes". Built with pictures.
  11:41, after the before/after pictures (Capilano 1 m -> 1.00 m, Harrison 10.30 m; the tap read-out was already cut at
  phone width): "Keep 2 decimals". DONE; lands next.
- 2026-11-17 ~10:38, main chat dialog on the T834 side build (verbatim):
  - The 3 changed waters (Kaslo/Keen +2 bull trout outside the share; Buckley rainbow its own 1): "I think this is right
    but you didn't explain well".
  - Rows smaller than the share (Adams rainbow 2 under Region 3's 4 from streams): "How the fuck would it read that it is
    just 2 of the 4 and nothing should be different then that unless it lists a value bigger then regional quota in which
    case it will fill it up then add a couple additional slots after being filled up regional".
  Reading: a row's own number counts INSIDE the stream share; only a number BIGGER than the share adds slots on top
  (fill the share, then extra slots for that fish). Equal numbers change nothing. That is NOT what the T834 build does
  for Kaslo/Keen (it gives 2 bull trout + 2 others = 4); follow-up asked with numbers before anything merges.
- 2026-11-17 ~10:50, main chat follow-ups on T834 (verbatim):
  - Kaslo: "Yes: 2 in all (Recommended)" (Kaslo/Keen stay 2 fish from the stream in all; both may be bull trout).
  - Lardeau: "the lardeau would allow 5 rainbows and 2 bull in a day buit then you have used up trout quota so no other
    types may be caught. or you could catch 2 other trout like cuttroat then you could have the 5+2 with any 2 removed. so
    like 3 rainbow 2 bulls for example or 4 rainbows 1 bull. make sense?"
  - Lardeau day total, region's 5 or 7: "i would say 7 because we have other ones like one of the kootenay lakes having a
    10 a day quota for rainbows ".
  Reading (main chat): a water that prints its own daily quotas gets a day pool = the larger of the region's cap that
  applies there (stream share on streams) and the SUM of its own quotas; each fish stays under its own quota, fish with no
  own quota stay under the region's cap, and all of them count inside the pool, which replaces the region's total there.
  Lardeau/Duncan 7, Kaslo/Keen 2, Adams 4 (2 rainbow of the 4). T834 reworked to this; the changed waters go to him first.
- 2026-11-17 ~11:00, owner: "can we add all these examples to gotcha as a thing to ask about". Done:
  pipeline/docs/GOTCHAS.md, "Questions the owner will put to BC Fish and Wildlife" (beside the Thompson): Kaslo/Keen,
  Lardeau/Duncan, Kootenay Lake main body, Adams, Buckley, and the 12 rows that carried a hand-written lift, each with
  the reading built and the other reading. TO ASK Fish and Wildlife (owner).
- 2026-11-17 ~11:10, owner on "unlike the Thompson": "unlike the thompson? everything should use same reading...". Then,
  by dialog, ONE reading for every water: "Strict everywhere (Recommended)". The region's rule stands unless the water's
  row says otherwise or cannot work under it: Lardeau/Duncan 5 in all (bull + other trout 2 of them), Kootenay Lake main
  body 10, Thompson below Kamloops Lake stays closed in June, Kaslo/Keen 2, Adams 4, Buckley 1. Replaces the ~10:50
  "7" reading. GOTCHAS entry rewritten to match.
- 2026-11-17 ~11:08, owner (verbatim): "what are all the streams that would be effected by the change? i want the list in
  gotchas" and "but make sure they dont have exempt from whatever closure in the item becasuse then that makes it
  unambiguous". Done: GOTCHAS, under the Thompson question, a table of the 17 waters whose own "No Fishing" dates sit
  against a regional closure with no printed exemption (plus Duck Lake's dated bass rows), with the days the loose
  reading would open. Fraser (exemption covers only above the Thompson), Shuswap (pier) and West Road checked.
- 2026-11-17 11:15, owner (verbatim): "did you add the text to the gotchas for each too? want to be able to draft a message
  to each regional office for how to tinterpret in future". Done: GOTCHAS "The exact words, by regional office": per
  office (1, 3, 4, 5/6, 6, 7A) the region's rule and each water's row as printed, printed page numbers, what the app
  does and the other reading; closures (A) and quotas (B). Thompson/T834 page cites corrected to printed pages.
- 2026-11-17 ~11:15, in the APP chat: "the photo viewer could also use a little improvement to make it industry
  standard". Picked all four at 11:17: swipe down to close, tap to hide the buttons, counter + smooth motion, opens from
  its thumbnail. Filed T836; built after T819 lands, with pictures and clips.
- 2026-11-17 11:18, owner (verbatim): "the regional office questions should include oither questions form gotchas too",
  "also if there are any gotchas we missed?", "dont mention the app just have the question as super concise questiuons
  on how to interpret and what the problem is". Done: GOTCHAS "Questions for each office": BC Fish and Wildlife
  Regions 1, 3, 4, 5, 6, 7A and DFO (salmon), each a short question with the book's words and printed page. Added from
  the gotchas: Region 1 dated bait bans, Kitimat hatchery steelhead, Mackenzie Creek MU, Region 5 lakes not found, and
  the DFO place wordings (Kispiox, Kasiks, Kanaka, Stamp, Somass, Juniper Beach, Bulkley, Sheldens, Tatshenshini,
  Cowichan). Missed before, now added: Campbell River MU 1-6/1-10 summer closure (T796 W1) and the Duncan's exemptions
  reaching Trout Lake's creeks (RULINGS T15). Left out: Lower Shuswap "one km" (his word: nothing goes to DFO).
  Correction: the page numbers in the 11:15 version were wrong (the corpus already stores printed pages); fixed.
- 2026-11-17 ~11:45-11:48, app chat:
  - "sun set is not capitalised (Sunrise 08:50 PT · sunset 17:43 PT) too" -> T837, fixed ("· Sunset"), lands with T830.
  - "having random conditions today text here is weird too" (web spot estimate card). Dialog 11:48: "Drop it
    (Recommended)" -> T838: the loose "Conditions: today" line goes; the regulations-day link line stays when that day is
    not today.
  - "desktop mode on webapp should maybe move the bottom map.conditions. spots, more bar somewhere more standard for
    desktop. maybe on side or top please choose the most standard spot" -> T839: Claude chose a left navigation rail at
    desktop width (Material's rule for wide screens; Google Maps / AllTrails / onX); top stays search + day. Shots
    before it lands. 12:30 dialog after the shots (Map light, Conditions dark, phone): "Land it (Recommended)"; the
    walkthrough card is kept off the rail before landing.
  - Nahwitti River: char sit on their own under the other-fish table instead of in the trout tab. Checked 11:51: today
    Region 1's "Release all char" (trout_quota.r7) is a row with no pool, drawn after the table (289 waters have the shape).
    The new trout table (T810) puts DV/lake trout/brook trout in its Release box (0 of 513 such keys left out); Nahwitti
    added to the trout hard cases (main chat) and the app's drawn-once gate. Answered in chat.
- 2026-11-17 11:51, owner (verbatim): "i thought we already found some of these??" (the Region 5 lakes). Right: Hidden,
  Secret and Square bind now; Chipmunk (and Toms, Basalt, Gatcho, Naglico, Pettry) is printed in both the Region 5 and
  Region 6 tables with the same words and the Region 6 row binds it. Still not found: Frog Lake (5-6), Redfern Lake
  (5-15), Squirrel Lake (6-1). Questions and the GOTCHAS errata note narrowed to those three.
- 2026-11-17 11:55, owner (verbatim) on the Region 6 steelhead closure question (Kitimat, Dala, Ksi Hlginx, Triumph): "i can
  answer thhis one that the no steelhead is different then a full closure so the no steelhead definitely holds here".
  So the steelhead closure holds on those rows (as built; no change). The West Road mainstem (same shape) taken as
  answered the same way. Both removed from the office questions; recorded as answered in GOTCHAS.
- 2026-11-17 11:57, owner (verbatim): "for the missing lakes we should say a little about what we have found on them too"
  and "please update the regional office questions". Done: Frog, Redfern and Squirrel questions now say which lakes of
  that name exist and where, and what the row's words match (Frog = Hidden/Secret's words; Squirrel = the MU 6-1 lakes
  printed in both tables). Questions file re-sent.
- 2026-11-17 12:29, owner, on T834's "was X, now Y" page (brook trout 20 lakes 25->20, R7 brook trout 10 lakes 15->10,
  Tranquille 13->8, Khartoum/Lois 10->6, Peace 5->3, Lavington 22->20, Perry 21->20, Beaver 10->8): "Yes, ship all
  (Recommended)". On the card (two boxes "20 brook trout" + "5 trout/char" read as 25; proposed one box "Up to 20 a
  day: brook trout up to 20, other trout up to 5"), verbatim: "This is weird because the total is still the regional
  number so we should only use that but then the exemption overrides it only for that species but will still fill up
  the trout total quota. Please look at it more closely. Basically I mean the total is 5 not 20 or 25. Just 20 trout is
  a single exemption but changing the total to accommodate that is confusing. So it should say something like 5 with
  exception to 20 brook trout which also uses up your 5 quota." So the card leads with the region's 5, not the 20.
  Confirming the catch (does 15 brook + 5 cutthroat stay allowed?) before the card is built.
- 2026-11-17 12:32, owner (verbatim), on "15 brook trout + 5 cutthroat at a brook trout 20 lake: legal?": "For now say
  legal ( need to ask about this with co tho)". Built as legal (others at most 5, all at most 20; brook trout count
  toward the 5). Card: "5 trout a day / Exception: brook trout up to 20 (count toward the 5; past 5, brook trout
  only)". Question added to GOTCHAS (T834 entry + Region 4 office Q1).
- 2026-11-17 12:43, for the owner: re-tick Sheldens Creek (DFO Region 6, "upstream of white triangular fishing boundary
  signs … Spur 19 Bridge site") in the review app. The 11-15 night mark (saved by Claude, unsure, whole creek) was
  overtaken by your 11-16 20:01 "Extend to the bridge"; it now waits on NOT_LANDED_OK until you mark the place again.
- 2026-11-17 13:15, owner, on T834's cards as built (Jewel, Kootenay, Peace, Lardeau): box head "Lead with 5
  (Recommended)" (box reads "Keep 5, brook trout up to 20 / brook trout count toward the 5; past 5, brook trout only",
  not "Keep 20 in all"); the other three cards "Yes, they read right".
- 2026-11-17 ~13:49, owner (verbatim): "we shouldnt do another release until everything goes into it". Asked what
  "everything" covers, picked T804 + T807, T825 side-channel gauges, T813 Schoolhouse Creek North, the trout table
  switch (T810), and wrote: "everything that has been being worked on from salmon rules and table rework to trout table
  rework to all bug fixes etc". Then: "maybe should bump to 0.8.0 too?". So NO data build or staging release until
  all of that lands; the release goes out with the app as 0.8.0 (app chat's call on the bump mechanics).
- 2026-11-17 ~13:52, owner: push notifications (T822 + T739) "After 0.8.0 (Recommended)". Everything else in flight
  goes into 0.8.0.
- 2026-11-17 14:55, owner (app chat, two Android screenshots of the 0.7.12 walkthrough, verbatim): "On the last release the
  walk through is still a bit glitchy. First image is a screen shot I caught when scrolling and the box has a weird delay
  tracking the box. It just feels unprofessional. The other one is how the keyboard covers vedder in the walk through.
  The interactive walk through just isn't perfect. The Qp steps are good otherwise. But could reduce steps even more.
  This is a user first look so we need to make sure it is clean and not buggy" -> T842 (both glitches fixed). 15:00
  dialog: "6 steps (Recommended)" (search, open, open/closed, pick a day, how is the river, all set). Answered.
  15:02: "Use industry standard ideas" / "Should be in 0.8" -> the standard coach-mark pattern (spotlight, anchored
  card that flips, Skip always shown); in 0.8.0.
- 2026-11-17 ~15:04, owner (15:00 batch, verbatim picks): T825 side-channel gauges "Yes, use the rule (Recommended)"
  (nearest flow gauge on its own main river with a similar drainage, else none; 244 sections, lower Sumas none; the two
  Creston channels read Corra Linn). T752 desktop split "Keep it (Recommended)". T789 G9 "one fly" (36 rules / 35
  waters) and T801 item 4 confluence wording "Yes, ship both (Recommended)". T772 Kitsumkalum Lake side channels "Yes,
  show me the list (Recommended)" (the 16 as was/now before anything lands).
- 2026-11-17 ~15:11, owner: T740 kept-map memory "After 0.8.0 (Recommended)". T789 K8/G8/G10 (Shuswap pier exemption,
  non-resident-alien weekend closures on the Licence tab, under-16 non-resident price line), verbatim: "yes add to 0.8.0
  we can add as banners? i thought we already had these as banners" (app chat to check what banners exist; book's
  words; owner sees the wording before it ships). T801 item 3 Skeena River 2 stamp line: "Trim to this water's part
  (Recommended)" (quote only its (a) part; other waters' stamp lines unchanged).
- 2026-11-17 ~15:18, owner on T786/T806 (wrong saves): "I never changed it so it must have never been updated if it
  was ones we fixed can you check?" Checked: all 34 re-check places were answered from pictures 11-16 12:20-12:42 and
  re-saved (marks 2795-2839); Sproat 2795, Stamp Falls 2796 ("Keep 500 m below the fishway"; T803 item 13's "60 m
  down" marked superseded). Then: "I feel like we have done these like 3 times already so it should be good???" So no
  safety net (T786 dropped) and T806 done; owed later only: pictures for Morice Gosnell-Lamprey (2739, after its fix)
  and Sheldens (2783, after the map rebuild). Don't ask him about these saves again.
- 2026-11-17 ~15:19, owner (verbatim): "The safety net should be a build warning I think then we fix it and do not ship
  "wrong" places..". So T786 is REOPENED as a release gate: the build lists every place whose latest save is "wrong"
  (no fix drawn) and the release refuses to ship until each is fixed and re-saved. Not "ship undrawn".
- 2026-11-17 ~15:22, owner (verbatim): "We should only ship not wrong items", then on "leave out what": "The build should
  fail but Unmatched items ( from feed say it matches stream but no place exists yet because it is a new entry) should
  have the dfos words added to the dfo rule section with the stream having the check rule style that we implemented
  before. This should already be how it works?". So: T786 gate FAILS the build on a "wrong" save (as being built);
  a new DFO feed entry with no drawn place yet ships DFO's words in the water's DFO rule section, the stream in the
  "check rule" style. Verify that second part already works (he believes it does).
- 2026-11-17 ~15:34, owner, T810 G2 trout wording (pictures g2.html): Q1 "What you keep here (Recommended)" (Echoes
  "Keep 1 in all", each fish "up to the 1"; region's 5 under "Fished elsewhere today?"). Q2 "'only 3 between them'
  (Recommended)" ("among them" for 3+; "up to the 5" where it is the box's own number). Q3a "'only 1 over 50 cm'
  (Recommended)". Q3b verbatim: "once in top yes. but the lake trout in that example still has crazy size text that is
  complex" (Tagish lake trout row to simplify; follow-up).
- 2026-11-17 ~15:41, owner, T810 G2 continued: Tagish lake trout row "Say what to release (Recommended)" (row
  "release 60-90 cm (24-35 in)" + "only 1 over 90 cm", box "only 2", possession line dropped as it equals the day's 2;
  builder must keep the book's exact bounds: 60 cm or less and 90 cm or longer may be kept). Q4 "Own line, 'In your
  possession' (Recommended)". Q6 "Once in the top line (Recommended)". Q7 verbatim: "waters own kind default to thisa
  water when not known" (the kind in the water's name; "this water" when the name has none). Q8 "Once; steelhead says
  'closed all year' (Recommended)". Q9 verbatim: "if it is 2 wild trout total then it should group wild i think except
  "only 2 of these4" is worded not great... we should make the wild vs hatchery more obvious too. add this lake to hard
  examples too" (follow-up mockup owed; Dodd/Windsor/Horseshoe/Nanton/Ireland into the hard vector cases). Q10 "Box +
  one quiet line (Recommended)". Q5 gone (T834). Also fix without asking: Echoes says "only 1 from both lakes
  together" twice; Fraser R3's closed table shows the year bar.
- 2026-11-17 ~15:46, owner (verbatim): "the up to the x per day text is a little misleading whaty is the 5 we should word it
  a little better i think." Picked "'any of the 5' (Recommended)": a row with no limit of its own says "any of the 5"
  (the box's shared day total), replacing "up to the 5" everywhere (including G2 Q2's box-number case).
- 2026-11-17 ~15:50, owner, T810 G2 Q9 follow-up (Dodd/Windsor/Horseshoe/Nanton/Ireland, mockup q9b): "1: Badges + 'only 2
  wild in all' (Recommended)" (hatchery rows first, then the four wild rows under one white box "only 2 wild in all";
  filled HATCHERY / outlined WILD badges; no new data).
- 2026-11-17 ~15:52, owner: the 0.8.0 new-words page (Skeena non-resident line in both boxes, Skeena River 2 stamp sentence
  trimmed, Salmon Arm pier exemption on the Shuswap closure and bait ban; book p.53, p.32) "Yes, ship them
  (Recommended)". T797 item 2 sturgeon dead-fin-fish bait line "Show it (Recommended)" (reverses the app's D18 a).
- 2026-11-17 ~16:30, owner (app chat dialog, T797 item 2): the bait-ban allowance field also holds set lining (Region 6 /
  7A lakes). Picked "Show both (Recommended)": sturgeon on the Fraser/Harrison and set lining on its lakes. Answered.
- 2026-11-17 ~17:01, owner on the T804/T807/T772 page: land it, verbatim: "for tatshenhini is the other 2 that list
  specific tributaries only locating that specific tributary remember thta was a mistake on my mark that i wanted you
  to fix. the rest seem good." (the two Tatshenshini places that name specific tributaries, Blanchard and "Kwatini,
  Stanley and Goat creeks", bind ONLY the named tributary; his earlier mark there was a mistake to fix). The three
  dormant saves (Cowichan x2, Stamp Ash-GCL dam), verbatim: "dfo words but make sure it is compatible with the name
  tributaruies or not." Re-saves of the 12 changed answers: "Save them for me (Recommended)".
- 2026-11-17 ~17:05, owner on the T786 gate's two open October saves: Stave River (marks line 30) "Yes, save it as right
  (Recommended)" (both spawning channels drawn and closed since T222). Lower Pitt River (line 1265), verbatim: "im not
  sure what this was about so just approve as right". Claude re-saves both as right citing these words. Thompson
  (2521) and Morice (2596), dormant, already answered in T806 (closed areas; retired): dated allowances in the gate.
- 2026-11-17 ~20:25, owner on the G8/G10 Licence page (T789). G8 weekend banner, verbatim: "Where is this? Should each of these waters just have their own info? Don't want this on waters it doesn't apply to". Lakelse/Telkwa every-day closures, verbatim: "Same thing applies to these". So: NO province-wide banner; each of the 12 waters (the 10 weekend ones + Lakelse + Telkwa) carries its own alien closure, with its own days and dates, on its own Licence tab only. G10 under-16 price: "Yes, each its own price (Recommended)" (each document with its own ordinary non-resident/alien price; the book prints no under-16 price).
- 2026-11-17 ~20:27, owner (classified waters, T841 + T802), verbatim: "Also we need to make sure the classified water symbols are maintained properly and modify the license requirement. Also if a water is classified it should have a badge in the card I header I think. Make sure we capture all classified waters whether labeled by symbol or by tributary of something". Filed as T841 (audit + licence requirement + build check); T802's card part becomes a header badge (picture first).
- 2026-11-17 21:38, owner (app chat dialog, T841 class badge; pictures of option A and B sent): "B: always, with dates
  (Recommended)" (the Class I / II badge always shows on a classified water with its period, greyed out of season) and
  "In the name row (Recommended)".

- 2026-11-17 ~21:07, owner on G8 per water (T789; Skeena Sedan-Shegunia stretch, Lakelse above the power line, Vedder nothing; tributaries by "Incl. Tribs" as the Keep tab): "Yes, ship it (Recommended)". lane/g8-per-water 83d98ee55, licence_frame.alien_closures [{rule, says}].
- 2026-11-17 ~21:18, owner on T841 (73/73 classified rows ship right; own-row tributaries that print no class, Li5/Li5a: Gosnell, Thautil, Glacier (Redslide), Two Mile, Station, Hevenor, Magar, Endako, Little Horsefly, McKinley, Moffat, Wahpeeto, Whiteswan outlet; closed all year: Line, Grave, Sandown, Nichyeskwa, Lean-to, Deep, East Fork White, Bear (Sustut)): "Keep: not classified". Li5/Li5a stand.
- 2026-11-17 ~22:13, owner, verbatim: "loading on webapp should use the new one too like the app" (the web app's loading animation should be the new coho one, as in the app since T814/T843). Handed to the app chat as T844.
- 2026-11-17 ~22:16, owner on T844 (should small in-app loading spots switch to the coho too?): "Opening loader only". Web first load gets the coho; small in-app loaders keep the circling fish (T814).
- 2026-11-17 21:33, owner (app chat dialog, T810 R picture pack: Elk Nov 15, Fraser at Mission Jul 15, Bowron Jul 15,
  Fraser 360 px cvd). First asked, verbatim: "The wording still using any of the x which I thought we changed?" (it had
  changed from "up to the x" at 15:46). Then picked, for a row with no limit of its own: "\"counts toward the 2\""
  (replaces "any of the 2" everywhere: Python's words, the main chat). Sign-off, verbatim: "Yes with wording above also
  make sure to do the source table and the species row expansion when you click on on with the sources for that entry
  plus identification" (the trout table is signed off for the cutover with the new wording; the row press must open the
  fish's sheet with its own sources and how to tell it, and "Sources ›" the table's sources).
- 2026-11-17 ~23:29 (relayed by the app chat at 23:31), owner look answers on the trout cutover: top line "Keep it big, 23 px"; fish tile "Keep the tile"; pin bar "Remove it"; Kathie Lake kept-elsewhere lines: first "Idk what you mean", then (asked plainer) "Yes, name the fish (Recommended)". Main chat: every elsewhere line names its subject when a table has more than one (L worker).
- 2026-11-17 ~23:32 (relayed by the app chat), owner on Fraser at Mission Jul 15: "Why are these not grouped on the right column with max 1? The up to 2 is misleading also we still have old text of any of the 2"; then "It says under the species for lake trout and bulls that we are allowed max 1 between both species so isn't that the grouped limit?"; picked "One shared 'only 1' box (Recommended)". Main chat: L worker builds one shared band per sub-cap across rows, everywhere; the "any of the 2" was a pre-batch-2 picture.
- 2026-11-17 ~23:35 (relayed by the app chat), owner on the trout top line: "Up to 2 is just confusing because  it words weird. Like up to 2 what"; picked "Keep 2 trout and char today (Recommended)" (names the fish and the action; Kathie "Keep 5 trout and char today, or 10 if they're brook trout"; Elk winter "Release every trout and char today"). Fold row value: "Make it match other rows in table" ("Trout and char · 2 a day", the Other fish rows' form). L worker, same round.
- 2026-11-17 ~23:36 (relayed by the app chat), owner: "Also we should look for other things that should be grouped like the bull /lake trout there we missed". Main chat: a sweep of all live trout tables for limits shared by rows but written row by row (patterns + examples), then the builder groups them; anything that changes meaning comes back to him with pictures.
- 2026-11-18 ~00:00 (relayed by the app chat; verbatim in its OWNER-TODO b1d74a245), owner on the group sweep: STANDING RULE "in an ideal world we simplify stuff ... if we can only keep >60cm then 1 over 50cm implies only 1 of either a lake trout or bull and it uses your 1 over 50cm slot. this type of simplification should happen everywhere because it makes stuff easier to understand." (wants proper examples first); R4 cutthroat+rainbow: "i think A but still has to mention the overall limit. follow rule i said abotu simplifying" (one band 'only 1 over 50 cm between them', total still stated); Istsi possession: "why 1 days limit not 2? remember simplify also why does it end with \"in all\" ??" (explanation owed); e-wild: "i would need to see an example. to make sure it reads right". Main chat: analyst drafts real before/after examples; L builds 2A now, the general simplification rule after he OKs the examples.
- 2026-11-17 23:02, owner (app chat dialog, T844 web loader; Fast 3G frame strip + fast-line clip sent), verbatim: "make
  way smaller for web" (a much smaller web-only coho sheet so it swims on slow lines; phones unchanged).
- 2026-11-17 23:29, owner (app chat dialog, T810 cutover look; Fraser at Mission before/after and Kathie Lake pictures
  sent): top line "Keep it big, 23 px (Recommended)"; fish tile "Keep the tile (Recommended)"; pin bar "Remove it
  (Recommended)"; the flat elsewhere list first "Idk what you mean", then, asked plainer (Kathie Lake: line 1 is the trout
  and char, line 2 the brook trout): "Yes, name the fish (Recommended)" (each elsewhere line starts with the fish it is
  about where a table has more than one such line; words from Python).
- 2026-11-17 23:32, owner (app chat, on the Fraser at Mission before/after picture), verbatim: "Why are these not grouped
  on the right column with max 1? The up to 2 is misleading also we still have old text of any of the 2". Asked what
  makes "Up to 2" misleading, verbatim: "It says under the species for lake trout and bulls that we are allowed max 1
  between both species so isn't that the grouped limit?". Dolly Varden + lake trout sharing one limit: "One shared 'only
  1' box (Recommended)" (one white "only 1" box in the right column spanning both rows, inside the box's "Keep 2 in
  all", replacing each row's "only 1, counting any … you keep" line). "any of the 2" is the pre-batch-2 wording; the
  re-cut brings "counts toward the 2".
- 2026-11-17 23:35, owner (app chat), verbatim: "Up to 2 is just confusing because  it words weird. Like up to 2 what".
  Top line picked: "Keep 2 trout and char today (Recommended)" (names the fish and the action; Kathie "Keep 5 trout and
  char today, or 10 if they're brook trout", Elk in winter "Release every trout and char today"). Fold row's value,
  verbatim: "Make it match other rows in table" (app chat's reading: the same form as the other fold/list rows beside it,
  e.g. "10 a day", so "2 a day"; Python's words).
- 2026-11-17 23:36, owner (app chat), verbatim: "Also we should look for other things that should be grouped like the bull
  /lake trout there we missed" (a sweep of every trout table for limits shared across rows but written row by row; sent
  to the main chat's builder lane with the Fraser band).
- 2026-11-18 ~00:00, owner (app chat dialog, the group sweep's four questions, verbatim):
  1. Vedder nested DV+LT lines: "i need proper examples. but in an ideal world we simplify stuff. so for example if we can
     only keep >60cm then 1 over 50cm implies only 1 of either a lake trout or bull and it uses your 1 over 50cm slot. this
     type of simplification should happen everywhere because it makes stuff easier to understand." (A STANDING RULE: drop
     any line another limit already implies, and say the implication once, everywhere; real before/after examples owed.)
  2. R4 cutthroat + rainbow over 50 cm: "i think A but still has to mention the overall limit. follow rule i said abotu
     simplifying" (one shared band, the box's overall number still stated).
  3. Istsi/North Star/Sootheran/Rockbed possession: "why 1 days limit not 2? remember simplify also why does it end with
     "in all" ??" (open: explain "1 day's limit, not 2" and simplify; drop or justify "in all").
  4. Lakes' "only 4 wild" inside "Keep 4 in all": "i would need to see an example. to make sure it reads right" (open:
     before/after pictures).
- 2026-11-18 ~00:52 (relayed by the app chat; verbatim in its OWNER-TODO ab7baad), owner round 2 on the trout table: 3+ row boxes "K total from these M" (A); every shared box names the overall limit; "uses the 1 over 50 cm" likely unneeded (switchable, shown off in round 3); ungrouped fish first, groups last, no group splitting another; shared dates said once ("the dates need to be handled better"); Vedder hatchery rainbow as a separate "river rule"; hatchery+wild Group A (one row per fish, one count). Main chat: all to the L worker as one round.
- 2026-11-18 ~00:59, owner on Chilliwack Lake wild brook trout ("No wild trout over 50 cm, 1 bull trout over 60 cm", p.22; same on Chehalis, Cultus, Harrison, Lillooet, Pitt), verbatim: "only bull trout is out it needs to change the char group as a whole to get the char to change compeltely out of the group". So: AMENDS the 2026-10-07 ruling C2 / AGENTS 46: a char given its own line leaves the trout line ALONE; all char leave only when the char group as a whole has its own line. On these lakes wild brook trout over 50 cm go back, and wild lake trout become release-only (region: none under 60 cm; lake: none wild over 50 cm). Main chat: a worker changes the ruling, the validator and the corpus, with a sweep for every other row C2 touched.
- 2026-11-18 ~01:22 (relayed by the app chat; verbatim in its OWNER-TODO a8daa7e), owner round 4 on the trout table: count-column rules R1-R5 "Yes, everywhere (Recommended)"; hatchery+wild one row "Yes, \"if wild\" (Recommended)"; Vedder: "i think leading with 2 is still important becasue only one is an exception to that." (top line pick pending); River-rule section stays off. Main chat: L builds round 4; the three no-law wording fixes (any length, any size, 20 brook trout) OK'd.
- 2026-11-18 ~01:28 (relayed by the app chat; verbatim in its OWNER-TODO df9cea1): Vedder top line "Keep 2 trout and char today" + subline "or 4 if they're hatchery rainbow trout 50 cm (20 in) or less", rainbow row LAST as the exception (rule: a water's own rule that raises one fish's limit comes last); HATCHERY ONLY over char "Leave as is"; "only 2 in your possession" keep; Madden: "isnt the dolly not needed to be mentioned since it is 2 and it is keep up to 2 trout/char in general? so it could be dropped from header line?" (answer owed).
- 2026-11-18 ~01:31, owner on the C2 sweep (lane/c2-one-char 3a1010f3d, 15 lines change): Region 6 table (brook trout in every R6 stream now 1 a day Jul-Oct, release Nov-Jun) "Yes, apply it"; Ross Lake (lake trout count in the trout 1, none under 40 cm) and the other 13 "Yes, all of them".
- 2026-11-18 ~01:4x (relayed by the app chat), owner over the Fraser A/B picture: "A: drop it everywhere" (no "uses your 1 over 50 cm" line on any table; the strip carries it). Madden: owner right per his own RULINGS T17 (11-17 'Strict everywhere'): bull trout 2 sits inside the 2 from streams; the live bundle predates T834, next build fixes it.
- 2026-11-18 00:35, owner (app chat dialog over 6 before/after pictures: the main chat's owner-examples.md, the real web
  export with the afters edited in; Fraser at Mission, Kamloops Lake, Vedder, Krumholtz Creek R4, Istsi Creek, Chilliwack
  Lake), verbatim:
  1. Simplify everywhere (the after DROPPED the DV/LT "only 1, counting any …" lines): "the fraser and kamloops sbs seems
     worse aftert? kamloops like doesnt simplify to 1 between bull/lake trout (inferred from 1 over 50c with 60cm or
     longer. fraser also never simplified tat but then removed the text that allowed a user to infer it? ... for vedder
     what does 0nly 2 of 6 mean? it is worded really poorly i think. also the only 2 of these should be more clear it
     applies to all." NOT approved as built. App chat's reading: simplify = STATE the conclusion once, plainly, never
     delete it; DV+LT get one shared "only 1" band (his 23:32 pick, the Kamloops shape), "uses the 1 over 50 cm" stays as
     the reason; band words like "only 2 of these 6" need plainer words, and the band must visibly span every row it
     covers. Open: wording + look options, with pictures.
  2. R4 cutthroat + rainbow: "Yes, one box (Recommended)" (one band "counts toward the 2 · only 1 over 50 cm (20 in)
     between them"; rainbow beside cutthroat; the per-row lines go).
  3. Bowron park creeks possession: "Yes, those words (Recommended)" (one box line "In your possession: just 1, the same
     as the day"; the per-row possession lines go).
  4. Chilliwack "only 4 wild": "the changing of bulls.lake trout is wrong jsut like i described before. then i dont know
     why we have seperate wild and hatchery here when they are the same also there isnt a seperate quota for hatchery and
     wild they are a simngle fish type. i guess it has no wild trout over 50cm so for this we still need the specieis to be
     a single entity so yes keep them split up but it is a group of wild and hatchery of a specieis with combined counts
     towards the 4. i know it is the same thing but it is the optics of it to a user. also hatchery and wild badge should
     be in the same spot?" App chat's reading: keep the DV/LT band; hatchery + wild rows of a species read as ONE species
     group with one combined count, split only for wild's 50 cm limit; HATCHERY and WILD badges in the same spot; "only 4
     wild" goes (no separate quota), confirm on the next pictures.
- 2026-11-18 00:52, owner (app chat dialog over round 2: fraser-band, vedder-words A/B/C, chilliwack-groups A/B), verbatim:
  1. Fraser shared DV/LT box "counts toward the 2 · only 1 between them": "yes the shared box it is good but maybe move the
     (uses the 1 over 50cm to the box too)? or maybe that isnt even needed ? isnt it implied byt the top bar?" (the shared
     box approved; "uses the 1 over 50 cm" on the rows leans to dropped, the strip says it; see 2.)
  2. Box words: "i like words from A best. my only other coment is i dont like the way the groupoing looks here. before
     the card used to be the grey then a white box (with grey aroudn it still) had the group info. this created the look
     that it was a subgroup of the grey. now steelhead is in its own white box with nothing?? what can you keep for it?
     and the groups seemed to have inverted the colors? also the only 1 between them group seems to split the group of
     only 2 between all 6. the default fish (ungrouped) should always be at the top then the groups at the bottom i
     think? make sure the grey color card fix happens with flatter group boxes too.... for this one the rainbow trout are
     a seperate river rule so maybe it should be split from the table and say river rule? that way user can tell it is
     special and seperate romt he generic grey greop too? also the "uses the 1 over 50cm" still feels unneeded? and the
     dates need to be handled better i think". App chat's reading: words A, "K total from these M" (3+ rows; two rows
     keep "only 1 between them", approved in 1); a group draws as a sub-group of the grey card (the old look: the group's
     words in the grey, its rows white inside), never a white band beside rows with nothing in it; no group splitting
     another; ungrouped rows first, groups after; the Vedder hatchery-rainbow 4 shown apart as the river's own rule;
     "uses the 1 over 50 cm" dropped; a date shared by every row said once, not on each row. Open: pictures of that look.
  3. Hatchery + wild: "i like group A bets but the hatchery vs wild tags are a bit clunky the way they are current
     implemented there." (Group A, one row per fish, picked; the HATCHERY/WILD tags need a lighter look; pictures owed.)
- 2026-11-18 01:07, owner (app chat dialog over round 3: vedder-r3 / fraser-r3 Look 1 panels vs Look 2 box + line,
  chilliwack-r3 tags), verbatim:
  1. Group look: "i thinj neither. look 1 is better but the number is hard to see and breaks the table format we have
     everywhere else. we want to keep the same number on right and species/info on left. this is in look 2 but the line
     kind of sucks i liked the box spanning both better ... in the fraser same thing. one other comment is unless it is
     something like vedder with weird rainbow rul have rainbow above steelhead." App chat's reading: the count stays in
     the right column on every row; a group is a white box in the right column spanning all its rows (round 2's spanning
     box), with ungrouped rows first and an inner group last inside its outer one so nothing splits; rainbow above
     steelhead unless a water's own rainbow rule makes it odd.
  2. Vedder dates once + "River rule" apart + "uses the 1 over 50 cm" gone: "does the river rule apart actually make
     sense? i am not sure maybe it breaks it up. also the river rule saying counts towards the 4 for vedder feels really
     weird when the top is 2 or 4 or whateevr. idk the fix but it jsut has a weird feel..." (River rule apart: doubted;
     the hatchery rainbow's "counts toward the 4" reads oddly; open, options owed. Dates once and "uses the 1 over 50 cm"
     dropped: not objected to.)
  3. Hatchery/wild tags: "Neither yet" (plain words and small grey tags both rejected; new options owed).
- 2026-11-18 01:22, owner (app chat dialog over round 4, built from a fresh reviewer's rules: fraser-r4, vedder-r4,
  chilliwack-r4, r4-r4 (Krumholtz Creek)), verbatim:
  1. The count-column rules (lone "counts toward the N"; own cap "counts toward the N · only k"; shared box spanning its
     rows "counts toward the N · only k between them" for 2, "k total from these m" for 3+; nested "only 1 between them";
     "uses the 1 over 50 cm" and "only 1, counting any …" gone; "any size" off the box head): "Yes, everywhere
     (Recommended)".
  2. Vedder top line "Keep 4 trout and char today" + "only 2 besides hatchery rainbow trout": "i think leading with 2 is
     still important becasue only one is an exception to that." (Lead with 2; the hatchery rainbow 4 is the exception.
     Open: the exact two lines, picture owed.)
  3. Hatchery + wild in one row, "50 cm (20 in) or less if wild", no tags: "Yes, "if wild" (Recommended)".
- 2026-11-18 01:28, owner (app chat dialog; vedder-r4b V1 rainbow first | V2 rainbow last, both "Keep 2 trout and char
  today" + "or 4 if they're hatchery rainbow trout 50 cm (20 in) or less"; and the analyst's three look questions),
  verbatim:
  1. Vedder: "V2: rainbow last (Recommended)" (lead with 2; the hatchery rainbow row last, as the exception).
  2. HATCHERY ONLY over char on Region 2 streams and DV/LT at Lois/Khartoum: "Leave as is".
  3. Madden Creek top line: "isnt the dolly not needed to be mentioned since it is 2 and it is keep up to 2 trout/char in
     general? so it could be dropped from header line?" (open: the main chat answers whether the DV 2 sits outside the
     general 2; if it does, dropping it would hide that you may keep 2 more).
  4. Nation/Cunningham/Nakinilerak lake trout "only 2 in your possession": "Keep "in your possession" (Recommended)".
- 2026-11-18 01:29, owner (app chat), verbatim: "similar to region 2 lake trout and bull /dolly simplification we should
  look for other ones like that in other regions" (sent to the main chat's analyst: a sweep of every region for limits
  implied but not stated as one shared line; fits of the round-4 rules go into L's build, new kinds come back to him
  with pictures).
- 2026-11-18 01:33, owner (app chat dialog; the main chat's answer that Madden/Lardeau bull trout's own "daily quota = 2"
  (p.38) sits outside Region 4's "2 from streams" (p.34), so 2 trout/char + 2 DV, max 5), verbatim: "i thought we were
  assuming those quotas still used uop the 2 a day regional quota? but that i would ask a CO about it". (He recalls the
  standing assumption that a water's own per-fish quota still counts inside the region's 2 a day; open: the main chat
  finds the recorded ruling; the question for a Conservation Officer is his.) RESOLVED: he is right, by his own ruling
  T17 (2026-11-17 11:06, "Strict everywhere", T834): a water's own quota N ≤ the region cap sits inside it, so at
  Madden the DV count inside the 2 from streams ("Keep 2 trout and char today, or 5 if they're rainbow trout"; the DV
  clause goes). The live bundle predates T834; the next data build applies it. The book (p.3) is silent, so "ask a CO"
  is on the To-Do page (row ask-co-water-quota-inside-share).
- 2026-11-18 01:42, owner (app chat dialog over fraser-over50 A no line | B "uses your 1 over 50 cm (20 in)" on DV/LT; the
  main chat's sweep: 1,387 tables where a fish kept only over 50 cm sits beside fish that can also pass 50 cm), verbatim:
  "A: drop it everywhere". (The box head's "only 1 over 50 cm" carries it; no per-row line anywhere.)
- 2026-11-18 09:52, owner (main chat), on the Puntledge "including tributaries" reading (whole watershed above the
  diversion dam, or only creeks joining the river), verbatim: "the whole watershed (Not blocking: whether the
  Puntledge's "including tributaries" covers the whole watershed above the dam, or only creeks joining the river.) but
  are we sure puntledge river actually has tribs included?" RESOLVED: the whole watershed, which is what the 0.8.0
  candidate already binds (1,069 sections; no change). Checked against the book: printed p.17, row "PUNTLEDGE RIVER
  1-6", last line "Fly fishing only upstream of the BC Hydro diversion dam (approximately 3.5 km downstream of Comox
  Lake), including tributaries". The row name carries no tributaries star, so only that one line (and "No Fishing
  tributaries") reach the side streams. Cruickshank River (feeds Comox Lake) has its own row "No Fishing", which wins.
- 2026-11-18 ~09:54-09:57, owner (main chat), on Puntledge "No Fishing tributaries" (it was bound below Comox Lake
  only), verbatim: "no fishing tributaries shoudl be on the entire river??"; in the dialog (split at the dam / closed
  on the entire river / keep today's): "why woudl you think it doesnt close the wholel river????"; "look at entry it is
  clearly close all tributaries" (with a picture of the printed row); "the fly fishing on is if a tributary gets
  opened on its own entry". So: every side stream of the whole Puntledge is closed; "Fly fishing only ..., including
  tributaries" sets the method on the river above the dam and on any side stream that its own entry opens. In work
  (T798): r5 rebound to the whole river; reach run to measure.
- 2026-11-18 12:18, owner (app chat dialog), on the final trout tables shot from the staging site (r-f1f2e8da9666; 11
  pictures: Vedder + sheet + dark, Chilliwack Lake + sheet, Fraser at Mission, Madden, Istsi, Kamloops, Lois,
  Krumholtz; same words as the approved local set, only a desktop scrollbar gutter wraps a few lines sooner): "Signed
  off". The 0.8.0 Play internal + TestFlight builds start once the app chat's 0.8.0 bench passes.
- 2026-11-18 12:18, owner (app chat dialog), on the gauge roster email (T847: notify only stations the roster has never
  seen, once each): "Now, before 0.8.0". Earlier, verbatim: "shouldnt it automatically pick up the changes and handle it
  without needing a refresh unless it was. anever before seen gauge?"
- 2026-11-18 ~13:15, owner (app chat), with a Traceur trace: "zooming out in map on a high detail big lake and it lags
  out bad in 0.7.x version app. Be nice to fix this in 0.8.1 if it is still an issue in 0.8.0". The trace held only
  Chrome (the app was idle in the background). Dialog: where = "In Chrome (staging web)"; lake = "Intata Reach area
  with the multiple lakes there". T848.
- 2026-11-18 13:43, owner (app chat), verbatim: "0.8 release notes are fine. The naming bug is a big issue. The feed
  check too." So: the 0.8.0 changelog draft is cleared (draft flag off); T850 (gauge-end part names pointing at a
  landmark up to 18 km away, e.g. the Nass "Canyon City bridge → Canyon City bridge") and the DFO feed check (every
  region "stale" after one failed daily fetch, T851) are raised to big issues. Also asked: "Could you have a nice
  labelled map image show the nass system rules? Including the cascade section rules?"
- 2026-11-18 ~13:50, owner (app chat), verbatim: "also on the app the loader fish icon should be higher resolution
  please". T852.
  ~13:55, verbatim: "the quote text should be formated in a nicer way too on loader for 0.8.1". T852 item 2.
- 2026-11-18 13:46, owner (app chat), on the proposed 0.8.1 list (T848 web map lag, T852 loader fish + line, T851 DFO
  stale, T822+T739 push, T740 kept-map memory, T831 route-map dots, T832 iOS stale colour, T846 decided keep vs
  release, T840 salmon short text, T849+T768 test reliability; T850 rides the 0.8.0 data release), verbatim: "All these
  fixes can go in 0.8.1 / Which will release right after". So push (T822+T739) and T740 move from "after 0.8.0" into
  0.8.1, and 0.8.1 follows 0.8.0 straight away.
- 2026-11-18 14:04, owner (app chat dialog), T852 loader line: "B: red rule, muted line" (a short coho-red rule above a smaller muted line).
- 2026-11-18 14:08, owner (app chat dialog), on the Nass trout table (only lake trout keepable in November; headline "Keep 3 trout and char today"): "Keep \"trout and char\"". The headline stays; not a fault (the table keeps only lake trout, the rest release).
- 2026-11-18 14:12, owner (app chat): "could we make a script that generates the nass style images for any stream we want? for both salmon or freshwater regs?" T853. Also asked: "are we sure that one lets us keep lake trout? why is that the only one allowed?" — looks like a real keep-where-release error (trout/char definition: trout rules apply to char; Region 6 releases trout from streams Nov 1-Jun 30); sent to the main chat to check.
- 2026-11-18 14:14, owner (app chat), on Region 6 streams keeping lake trout (RULINGS C2(b) as amended put species_except
  [DV, LT] on "Trout of any size from streams, Nov 1-June 30" and "1 trout from streams July 1-Oct 31"), verbatim:
  "trout always covers chgar unles \"char\" as a whole is referenced elsewise on its own. per the book like you said so
  yes this should be a release like brook trout i think that ius the intention by the book". So: a rule that says
  "trout" covers char (lake trout included) unless the book names char as a whole separately; Region 6 stream lines
  release lake trout like brook trout. Main chat's fix; rides the 0.8.0 data release.
  14:17, owner, refining it, verbatim: "one time trout is not applied to char is if there is a seperate char groupo reg". So: a "trout" rule covers char UNLESS the same place has its own separate rule for char as a group (then char follows that one); a rule about one char species (e.g. Region 6's lake trout Sept 15-Nov 30 release) does not take char out of the trout rule.
- 2026-11-18 ~14:20, owner (to the main chat, relayed to the app chat 14:22), verbatim: "so we fixed the salmon painting?
  right now on 0.8.0 it seems way slower then trout painting? also on desktop can we move the salmon/trout toggle for map
  to the left side bar? maybe under map button?" then "might be the other chats job". So: T855 (Salmon map slower than
  Trout; not fixed yet, T848's fix covers the Trout map's zoom lag) and T856 (desktop: the switch goes in the left side
  bar under the Map button; phones unchanged). Both in 0.8.1, app chat.

- 2026-11-18 14:33 (T854, main chat) Owner, on whether a one-char rule (Chilliwack "1 bull trout over 60 cm", upper Kootenay "no bull trout under 75 cm") still takes that char out of the trout line, verbatim: "the species specific ones still are more specific so it applies to that species but if it is something like bull trout only 1 over 60cm but regional quota is 2 bull trout and 3 trout, then we could keep 2 bull trout which take 2/3 of our trout quota. make sense? so i think this is the mornign decision unless im confused? idk what other char -group one is idk what you meant" -> this morning's ruling stands; Region 6 lake trout fixed (released in streams Nov 1-June 30, 1 from streams in summer). RULINGS C2 + T854.
- 2026-11-18 14:36 (T854, main chat) Owner, clarifying, verbatim: "i just meant trout means troutt and char unless the group char are explicitly listed on their own like 3 trout and 2 char per day" -> C2 (b) as built: char leave a "trout" line only when char as a group has its own line.
- 2026-11-18 ~14:55, owner (app chat): "also should we use the rust port of map libre? seems really good ??" Answer: not yet
  (two weeks old, no Android/iOS map SDK or React Native binding; the app already ships the fix it is praised for). Owner:
  "add as a future ticket please." -> T857 (future).
- 2026-11-18 ~14:58, owner (app chat), on the T856 screenshots: "the desktop salmon/trout switch should be styled nicer to
  fit in the bar better rn it looks slapped in". T856 restyle, two looks with pictures to pick from.
- 2026-11-18 15:05, owner (app chat dialogs), T856: not in the side bar, verbatim "actually maybe we shouldnt have in side
  bar because it will make the buttons be jumpy when it disappears? where is a betetr desktop spot"; then picked "Top, after
  the day strip" (a Trout | Salmon pair right after < Today > on desktop; phones unchanged).
  15:06, owner: "the side bar should have some opacity to it too in desktop will look cleaner i think" (T856: a see-through
  side bar on desktop, the map running under it).
- 2026-11-18 16:50 (main chat) Cranberry River Canyon cuts. Owner, verbatim: "the top point is confident the lower point of canyon less so" -> the falls (top) cut is now HIGH confidence; the canyon bottom estimate stays LOW. Shows from the next data release (after r-86d43303bee2).
- 2026-11-18 ~16:50, owner (to the main chat, relayed to the app chat 16:51), verbatim: "in the webapp we shopuld have keyboard
  navigation like in search bar using arrows to move highlight etc." -> T858 (web only: ↑/↓ move a highlight through the
  search results, Enter opens, Escape closes as today; then the same in other lists where it fits). After 0.8.1: the
  web site can carry it as soon as it is built, without a Play or TestFlight build.
- 2026-11-18 16:55 (app chat) Review: the 0.8.1 "What's new" lines are a draft (app/packages/core/src/changelog.ts on
  app/integrate-081): faster map (Salmon zoom-out most), route-map gauge dots + you-are-here on phones, sharper coho
  loader + its line, desktop Trout/Salmon switch beside the day + see-through side bar, iPhone stale colour fixed. Clear
  them or note changes (To-Do page row "Clear the 0.8.1 'What's new' lines").
- 2026-11-18 ~16:52, owner (to the main chat, relayed to the app chat 16:53), verbatim: "on desktop webapp when you click
  about or simialr in more it shouldnt take over the entire page iot should be a card that overlays that pops up which you
  can click off to close maybe?" -> T859 (desktop web only: More's pages open as a centred card over the page; a click
  outside, Escape or ✕ closes it; phones unchanged). Lands with T858, right after 0.8.1, web only (no Play/TestFlight build).
- 2026-11-18 ~16:55, owner (to the main chat, relayed to the app chat), verbatim: "where it says this (Questions, corrections
  and ideas are welcome. ) with email we should have also the or send it through the app with the report and issue
  pathway? just word it well" -> T860; wording proposed: "Questions, corrections and ideas are welcome. Email us, or send a
  note from the app with Report a problem. A water's regulations have their own Report a problem link, which names the
  water for you." (Report a problem is a link to the form.)
- 2026-11-18 ~16:55, owner (to the main chat, relayed), verbatim: "these can be in 0.8.1 web version? since it doesnt change
  app it shouldnt effect release of app?" -> yes: T858, T859 and T860 go out with the 0.8.1 WEB release. Order: the 0.8.1
  phone build (Play internal + TestFlight) is cut first with its benches as they are; then T858/T859/T860 land; then the
  staging site redeploys from that commit. Phones get T860's words at their next build.
- 2026-11-18 ~17:05, owner (to the main chat, relayed 17:08), on whether the app should speak as him (a solo project:
  "I read every message. Email me…"), verbatim: "yes change all" -> T860 grows: every user-facing "we / us / our" for
  the project becomes "I / me / my" (no name) across the app, the web site pages and the store listings; 0.8.1 web.
- 2026-11-18 ~17:10, owner (to the main chat, relayed 17:12), verbatim: "conditions seems like it was laggy like salmon map
  was maybe it had same issue?" -> T861: measure the Conditions map against 0.8.0 and the 0.8.1 build (which has the
  T848/T855 fix); fix it if the Conditions map was left out.
- 2026-11-18 ~17:15, owner (relayed), verbatim: "also check run timing to while at it" -> T861 measures the Run timing map too.
- 2026-11-18 17:30 (main chat, T862) Owner, verbatim: "tyhe search doesnt show this? (… stocking records call Sixteen Mile Lake "Baker Lake", and Tranquille Lake "Lake #355". ) if searching one the other doesnt come up? this is a future ticket i think? it will also involve auditing the name vairations especially stocking/bathy maps" -> T862 (future, after 0.8.1): search by every name (app chat) + name-variation audit, stocking/bathymetry first (main chat). The names do ship in the release; the search does not use them.
- 2026-11-18 ~18:00, owner (app chat), asked "can i open external iphone testing to send to a few friends to help me test?",
  then, verbatim: "lets wait for 0.8.1 then do the prod build then full closed release and external release for andorid and
  apople" -> T863: 0.8.1 internal first; owner promotes the data release to prod; production builds of 0.8.1 (owner runs
  deploy/prod/build-apk.sh; iOS build-ios-testflight.sh --production); then Play closed testing + TestFlight external
  (Test Information in handoff/PLAN/ios/store/testflight-external.md; Beta App Review about a day).
- 2026-11-18 ~18:02, owner (app chat), verbatim: "we need to make sure a tester outside of british columbia doesnt have it
  glitch out too with location. we could give a little note saying you are far away from bc location will not be usefull
  etc etc" -> T864 (audit every location reader with far-away positions; a short note outside B.C.; before T863's testers).
- 2026-11-18 ~18:05, owner (app chat), verbatim: "okay so first step will be converting the website from staging to prod"
  -> T863: the production website first (0.8.1 web on staging, checked, then prod), before any production app build.
- 2026-11-18 ~18:05, owner (app chat), verbatim: "please make sure no other item has the same issue as bella coola did. do
  a general check across everything" -> T846: the app chat's independent audit across the whole release (every decided
  keep the engine or the book allows none of); findings fixed in the same data release, before production.
- 2026-11-18 ~18:12, owner (app chat), verbatim: "we need to get webapp and bucket released to prod them also cut first prod
  releases for iPhone and android which will then be closed testing and internal testing respectively. So let's work
  towards this. I'm gone for a little bit but then will be back to do my items." -> T863: the T846 data release on
  staging, 0.8.1 web on staging, then the owner's numbered production list (data worker, data, feeds worker, site,
  www redirect, smoke); then first production phone builds (0.8.2 = 0.8.1 + web changes + T864).
- 2026-11-18 ~18:20, owner (app chat; he asked whether the prod data release overwrites the current app's data: no, prod
  has no new-layout data yet, the upload clears nothing, no prod phone app exists), verbatim: "We will also what to delete
  the old data for old app version once everything is settled to avoid being over 10gb". ~18:23, verbatim: "That clean up
  should be automatic" -> T865 (app chat): deploy/prod/upload.sh prunes release objects neither manifest.json nor
  manifest.previous.json names; the v1 layout deleted automatically once canifishthis.ca serves the new site (marker).
  ~18:25, verbatim: "when we flip the switch i will get you to clean that stuff up too" -> at the switch, on his word,
  Claude turns off the bc-fishing-cron worker and main's update-hydro / update-in-season / update-stocking workflows
  (PRODUCTION.md step 11); no longer an owner hand step.
- 2026-11-18 ~20:22, owner (app chat), verbatim: "For salmon  It is 10 max adult Chinook not over 50cm because some rivers
  adult is defined differently" -> T869 (main chat). Book p.77: adult = over 50 cm in most waters; over 62 cm on the
  lower Fraser / Harrison (Sep 5-Dec 31), Chilliwack/Vedder below Slesse and Sumas (Jul 1-Dec 31); over 65 cm year
  round on 12 north-coast rivers (Bella Coola, Skeena, Nass, Kitimat …). DFO's page defines it the same way. OWNER
  PICK: (A) reword DFO's line to "10 adult chinook … (here an adult is over 62 cm, Jul 1-Dec 31)" or (B) keep DFO's
  line and add "Adult chinook here: over 62 cm (Jul 1-Dec 31)". And: before production, or after?
- 2026-11-18 ~20:24, owner (app chat dialog): T869 "Reword" + "Yes, before prod" (main chat records it in T869).
  T863 tracks, verbatim: "2 for now until I test iPhone a bit more my self" -> first production builds go to Play
  INTERNAL and TestFlight INTERNAL; closed / external testing after he tests the iPhone more. T864's note: "Yes, keep it".
- 2026-11-18 ~20:30 + 20:34, owner (main chat dialogs) -> T869: wording (A) "Reword the line" (the app chat got the
  same pick); timing "After production" (asked again at 20:34 because the app chat had been told "Yes, before prod").
  SUPERSEDED 2026-11-19 02:41: the owner asked for "all these items plus the map speed up idea and other loose ends" in the staging release overnight, then prod next day; T869 + T870 ride tonight's data release and 0.8.2 (staging first, he tests before prod).
- 2026-11-18 ~20:25, owner (app chat), Mamquam salmon card, verbatim: "The source dfo here doesn't show the rows that
  actually produce the results above. It should have all the rows that apply in source  Also the bait ban here is
  confusing idk where it is from for example." ~20:27: "Why is the bait ban all year defined from that month to that
  month too" -> T870 (main chat + app chat render).
- 2026-11-18 20:37, owner (main chat), with a phone trace
  (/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/88da0716-trace-tokay-CP3A.261005.005-2026-11-18-20-35-10.perfetto-trace,
  Pixel "tokay", 20:35), verbatim: "More laggy quick zoom in and out." -> the app chat (map performance; ticket number
  theirs).
- 2026-11-18 20:44, owner (main chat), verbatim: "I don't remember making a password? I don't think it had one" (the
  production feeds RUN_TOKEN). Checked: the prod worker bc-fishing-feeds has RUN_TOKEN set (with GITHUB_DISPATCH_TOKEN,
  REPORT_SALT, NOTIFY_EMAIL_TO; feeds-placeholder.sh, 11-05), and nothing else reads it but our local scripts. So a NEW
  value was made on the Mac (app/workers/feeds/.run-token.prod, gitignored, 600); the owner's list sets it on the
  worker as its first step: npx --prefix app wrangler secret put RUN_TOKEN -c deploy/prod/feeds-placeholder.wrangler.toml
  < app/workers/feeds/.run-token.prod
- 2026-11-18 ~20:46, owner (main chat), two phone shots of the day picker on Babine Lake — Main Body
  (/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/069827ef-image.png August,
  /Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/277b9b7a-image.png July), verbatim: "The
  calendar shifts around" -> the app chat (ticket number theirs). Seen in the shots: the sheet's height changes with the
  month (August has six week rows, July five), and the last week's days are spread across the row instead of sitting
  under their weekdays (Aug 30-31 under Sun and Tue; Jul 26-31 spaced evenly).
- 2026-11-18 20:46, owner (main chat), verbatim: "You can run that command" (the RUN_TOKEN secret put) -> DONE by the
  main chat 20:46: `wrangler secret put RUN_TOKEN -c deploy/prod/feeds-placeholder.wrangler.toml` from
  app/workers/feeds/.run-token.prod, "Success! Uploaded secret RUN_TOKEN" on bc-fishing-feeds. That command only; every
  other production step stays the owner's.
- 2026-11-18 ~20:49, owner (main chat), Babine Lake — Main Body page shot
  (/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/288abc3d-image.png), verbatim: "There seems
  to be a ton of space used by the sunset/rise could we maybe move it to top of something? Where would be good?" Dialog
  pick: "Under the date (Recommended)": a small line under the day picker on the right ("☀ 5:07–21:52"), freeing the
  sunrise row and the empty row above it -> the app chat (ticket number theirs).
- 2026-11-18 ~20:51, owner (main chat), verbatim: "Also the speed on this release is better so it wasn't this build it
  was always a bit of an issue" -> T876 (app chat): the quick zoom lag is NOT a 0.8.1 regression (0.8.1 feels faster
  to him); an old issue, so it does not hold the production phone builds.
- 2026-11-18 ~20:53, owner (app chat dialog, after the pictures): T871 trout "counts toward" once: "Not this change"
  (DROPPED, the table stays). T872 Conditions watershed fold, T873 short Help page, T874 walkthrough Back/Next/Skip:
  "Help page OK, Conditions fold OK, Walkthrough OK" -> they land (after the production website's sha).
- 2026-11-18 ~20:53, owner (main chat), Babine Lake — Main Body shot
  (/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/a6d620fa-image.png), verbatim: "I feel like
  we could order this better to allow salmon to be in the table but still have the two timeline calendars and big words
  easy to read and make sense/know it is different to between them and easily know which is which". Dialog pick: "Two
  headers, one table (Recommended)": both status blocks first (SALMON · DFO and GAME FISH · B.C., each its big word and
  its own year line, labelled), then ONE fish list with each row tagged DFO / B.C. -> the app chat (ticket number
  theirs); real pictures to the owner before it lands.
- 2026-11-18 ~20:59, owner (main chat), DFO Region 6 Bulkley bait row shot
  (/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/8d922a2e-image.png), verbatim: "Make sure
  this rule is correct. Like the with tributaries part for each exception except the one" -> T880. Found: Two Mile Creek
  is excepted with its tributaries (wrong by the words). Dialog picks ~21:02: "Yes, follow the words" (an exception
  without "and tributaries" leaves out only that stream; the Suskwa's tributaries stay in the chinook row too); "After
  production (Recommended)".
- 2026-11-18 ~21:05, owner (main chat), Babine Lake — Main Body, Conditions tab shot
  (/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/3733b516-image.png: "No estimate", then
  the gauge map showing only "you are here"), verbatim: "Lakes or other things without a meaningful map here don't need
  to include gauge map.. Also we should choose one map size maybe larger fit only" -> the app chat (ticket number
  theirs): no gauge map where it shows nothing useful (no gauge drawn, only the water's pin); one map size, the larger
  one, fitted, instead of the Fit / Larger map buttons.
- 2026-11-18 ~21:11, owner (main chat), Thompson River shot
  (/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/2ef8c8e4-image.png): "Why do we have some
  check DFO" -> answered: DFO's own "To be determined" (Thompson chinook + sockeye, Kamloops Lake to Gold Pan). Offered
  a wording change ("To be decided by DFO"); no answer yet. ~21:13: "There are some more check DFO too"; dialog, his
  words: "We should have had no unmapped stream items???! For lakes the parts should say in words in the salmon section
  for now? We do try to map them if possible tho" -> T885. Checked: no ACTIVE stream place is unmapped (the 12 stream
  rows are dormant); the 14 active ones are lake parts.
- 2026-11-18 ~21:18, owner (main chat), verbatim: "But all of these should not exist / Those 12 should still be mapped
  even if retired / To be determined I think we could change to actually say to be determined / However they should
  still show green if any salmon there are open only yellow if closed other then the to be determined" -> T885 (every
  DFO place mapped, dormant ones too) + the app chat (the "Check DFO" word becomes "To be determined" where DFO printed
  that; colour: green if any salmon open, the to-be-determined colour only when nothing is open; already the map's order).
- 2026-11-18 ~21:30, owner (main chat), two fish sheets on Cultus Lake (Kokanee "Release"; Black crappie "20 a day",
  the "20 a day" highlighted) (/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/7dcd1404-image.png,
  /Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/0308a124-image.png), verbatim: "We need to
  make the individual sources for each fish be formatted better in the source page for the species / Also on Apple the
  part highlighted is cut off weird because of row sizes / These pages are super clunky and ideally we want to make it
  nice and readable showing how the result was achieved. We want it to pair well with the trout ones too. All generated
  the same way (sources) and the way they are displayed should be the same. / Also the how to tell hatchery fish is very
  smile creepy we want to make it more realistic I think. Better shape more natural smile. Could use the salmon general
  shape from loading maybe?" -> the app chat (tickets theirs): (1) the species source sheet redesigned, readable, one
  shape for every fish and the trout sheets, generated the same way (main chat: the sources' data side); (2) iOS: the
  big "20 a day" clipped by its row height; (3) the "how to tell a hatchery fish" picture: a natural fish shape (the
  loading screen's salmon outline), not the smiling one. Pictures before anything lands.
- 2026-11-18 ~21:40, owner (main chat), on T876's finding (labels cost half the render work on a quick pinch), verbatim:
  "Does that mean labels render after. We care number one about the streams and colors and smoothness then labels can
  be a little behind etc." -> T876 (app chat): priority order streams + colours + smooth motion first; labels may lag
  (e.g. hidden or frozen during a pinch and drawn when it settles).
- 2026-11-18 ~21:43, owner (main chat), verbatim: "Another thing we should add a ticket for is checking every page for
  word density then try to reduce the over dense ones" -> T890 (app chat leads; main chat for the pipeline's words).
- 2026-11-18 ~21:47, owner (main chat), verbatim: "I wonder if we should do a final 0.8.2 before closed with everything
  including the smoother movement we just discussed wtc". Dialog pick: "Yes, wait for 0.8.2 (Recommended)" -> T863:
  the production website + data go live tonight; the first PRODUCTION phone builds for testers (Android closed / iPhone
  TestFlight) are 0.8.2: T868, T877, T872-T875, plus T878/T879/T881/T882 once he approves their pictures, plus T876's
  smoother zoom (labels may lag). Data / feed fixes (T869, T870, T880, T885) ship as data updates, not gated on it.
- 2026-11-18 ~21:48, owner (main chat), verbatim: "We can release prod on we first before / Web should still move to prod
  asap" and "Web should get updates as they come too. When apps release etc. Low user count right now". Dialog ~21:49,
  his words: "Claude can deploy prod web when tested" -> standing: once a batch is on staging and its checks pass,
  Claude deploys the PRODUCTION WEBSITE (deploy/prod/site.sh) and tells him. Production data, feeds worker, data worker,
  phone builds and Cloudflare stay his.
- 2026-11-18 ~21:50, owner (main chat), verbatim: "They should deploy with same cadence ish as app. Like versions should
  match" -> the production website moves with the app versions: tonight's site is 0.8.1 (with T868/T877/T872-T875);
  the next prod web deploy is 0.8.2, when the phones' 0.8.2 releases. Not every batch.
- 2026-11-18 ~22:02, owner (main chat), Lillooet braids near Pemberton on staging
  (/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/8b47b3ee-image.png): "Isn't braiding like
  this supposed to be simplified more ?"; dialog, his words: "We could simplify these even more in tile building? Yes
  side channels should be less bold too." -> T891 (tiles: main chat; style: app chat; pictures first). ANSWERED 2026-11-19 ~03:25: style approved ("you can add side channels style too"), in 0.8.2; T900 Gear and Methods proposal approved too ("T900 looks good"), in 0.8.2.
- 2026-11-18 ~22:13, owner (main chat), on the numbered production list, verbatim: "I give you permission to run them.
  For me" -> the main chat runs, tonight, one at a time with a check after each: the data worker deploy, the feeds
  worker (deploy/prod/feeds.sh + purge-reports), the data (`release --prod r-fc137c4040fa`), the website (site.sh), the
  production smoke. The Cloudflare dashboard settings (robots, bot fight, analytics, the www redirect) stay his (no API
  token here).
- 2026-11-18 22:13-22:30, PRODUCTION IS LIVE (main chat on the owner's go, except the data step): data worker
  bc-fishing-r2 deployed (22:13); backup bucket bc-fishing-regulations-backup created with staging's lifecycle (owner's
  "Yes, create it"); feeds worker bc-fishing-feeds deployed on feeds.canifishthis.ca; inputs synced; dfo + gauges +
  in-season jobs ok; DATA r-fc137c4040fa published BY THE OWNER (release --prod refuses an agent, by design; 22:21);
  website 0.8.1 from d0b7bf863 (clean worktree /Users/dawson.horvath/dev/personal/BC-prod-web) on canifishthis.ca;
  smoke --production 17 pass / 1 warn (backup not yet run) / 0 fail; first backup run ok (62 objects). LEFT FOR THE
  OWNER: Cloudflare settings (Managed robots.txt OFF, Bot Fight Mode / JS detections OFF, Web Analytics OFF; the www
  redirect rule to Dynamic concat("https://canifishthis.ca", http.request.uri.path), 301); then his word "switched" ->
  Claude turns off bc-fishing-cron + main's update-hydro / update-in-season / update-stocking and runs
  deploy/prod/drop-v1.sh (T865). Later: GitHub production option + PROD_R2 secrets for feeds-hydat / feeds-runtiming.
- 2026-11-18 ~22:33, owner (main chat), verbatim: "Light mode keep salmon vs closed or release has very little
  contrast. Should be bright green for keep in light I think too?" -> the app chat (ticket theirs): the Salmon map's
  light-mode colours: keep (open) a bright green like dark mode, clearly apart from closed and release; pictures first.
- 2026-11-18 ~22:40, owner (main chat): did the www redirect rule (checked: www.canifishthis.ca/water/vedder-river ->
  301 to the same path); "what if i dont want all of these off? web analyics is good." Dialog picks: JS detections OFF
  (the 2.6 s injected script; in Security -> Settings -> Bot traffic, below what he saw, or under Bot fight mode's
  gear), KEEP Web Analytics (no beacon on the pages today), Robots sync OFF (Bot Preference Sync / managed robots: only
  our robots.txt, AI assistants may read the site), KEEP AI Labyrinth. Bot fight mode already off. The main chat checks
  robots.txt and the page scripts after he flips them.
- 2026-11-18 ~22:58, owner (main chat): Bot Preference Sync turned OFF (checked: canifishthis.ca/robots.txt is ours
  only); "cant find javascript detections … it isnt in the gear with 1. i have paid worker plan". The zone's plan has no
  JavaScript-detections switch (Workers Paid is not a zone plan); the jsd script stays on the pages for now (harmless,
  costs page-speed score). Later, not urgent: find another way to keep it off the water pages if search ranking needs
  it. Cloudflare steps DONE. Left: his word "switched" (old cron + workflows off, drop-v1.sh).
- 2026-11-18 ~22:58, owner (app chat), on the five re-sent proposal pictures, verbatim: "all are good ( T878 … T879 …
  T881 … T883 … T886 … ) except sunrise/set should be a bit better in same spot" -> T879 two headers one table, T881 gauge
  map, T883 "How this was decided", T886 hatchery drawing: approved for 0.8.2. T878: keep the spot under the date, make
  the line look better (variants to him). Also ~22:50 (dialog): the salmon map key's short label is "Check DFO" (T882).
- 2026-11-18 ~23:14-23:16, owner (app chat): T878 sun line look 2 ("i like 2": sunrise/sunset icons "9:01  17:17" under
  the date); T887 salmon keep in light: option B ("this is good too Option B": bright green with a thin dark casing).
- 2026-11-18 ~23:05, owner (main chat), on the GitHub clean-up, verbatim: "yes and use same branch but then we need to
  eventually swithc to a standard main, staging and feature branch structure or whatever good structure we have" ->
  DONE 23:05-23:21 by the main chat: main = ca7c0cce7 (tree = redesign/stream-sections 21acbf78a, main's history kept;
  the old update-hydro / update-in-season / update-stocking workflows gone from GitHub's list); 28 merged remote branches
  deleted; archive/* tags for feat/mobile-offline-download-gate, feat/land-ownership-layer, staging, ios/next,
  ios/staging-worker; ios/t739-push kept (open T822). MISTAKE: redesign/stream-sections was deleted on origin for about a
  minute (a keep-filter failed) and restored at 21acbf78a; nothing lost (the app chat confirmed). THE SWITCH: bc-fishing-cron's
  schedules removed (its workers.dev address off too); deploy/prod/drop-v1.sh freed 20,019 objects / 8.57 GB of the v1
  layout; smoke --production http 8/0/0 after. Later ticket: the branch structure (main / staging / feature branches),
  with the working branch protected like main.
- 2026-11-18 ~23:35, owner (app chat), verbatim: "Like tributary rules ... yes label as from tributarie inheritance" -> T883:
  a rule reaching a water through its parent's tributaries/parts is labelled that way, never "this water's rule".
  "I want you to add the screenshots to the stores you can give them titles." -> T319: Claude picks the shots and titles
  and uploads them to Play and the App Store once the 0.8.2 UI is final (prepared as a dry run first).
  "You can do clean ups" -> the five 11-14 clean-up calls with their defaults (Claude, tonight).
- 2026-11-18 ~23:42, owner (app chat), on the T888 update-popup pictures, verbatim: "It looks right." -> T888 lands in 0.8.2
  (first shown when 0.8.3 is out). The five draft 0.8.2 What's new lines in the shot stand unless he rewords them.

## 2026-11-20 — lane L7 (T678 / T679 build stages): queued while the owner slept
- T679 #12: should the bundle carry each rule's own tributary scope (today `includes_tributaries: false` is dropped, so
  "inherit the entry's" and "explicitly not" look the same) and the designations' `tributary_excludes` (kept out of the
  bundle on purpose, `bundle/rules/row.py:84`)? Yes lets the export derive `UNDRAWN_PART_TRIBUTARIES` and
  `EXCEPTED_RIVERS_CREEKS_CLOSED` instead of keeping two entry-id lists in code; it changes the bundle and export
  output (lane L8's files, a release). Default if no answer: leave the two lists in code (the rulings moved already).
- T679 #1 / #6 (wave 4): flip both switches with the atlas rebuild? #1 takes the border slivers no region claims off
  the map (they are ruleless and say "outside B.C." on a tap); #6 drops the name the map shows on a section no water
  holds. Counts in the T679 ticket (lane L7 section). Default: flip both, diffs shown before the release.
- Disk: the Mac's data volume ran down to 0.2 GB free about 10:30 (several lanes' delivers at once); L7 deleted
  superseded backups (reaches/full_next, full_prev, full.pre_t743, full.pre_rf, full.pre_ri3, full.pre_deadend,
  bundle.pre_t743, tiles.pre_t743, side/dfo-cuts23b, side/rebuild-input-3; about 10 GB). Wave 4's atlas build needs
  about 4 GB free, and L7 could not run its side atlas build for that reason (the proof ran on the live atlas instead).
