# After 0.9.0: the chats, in order (owner 2026-11-24)

Planned with the owner at the end of the 0.9.0 batch chat. Each line below is its own chat. The orchestrator runs on
Fable and picks each worker's model by task: Opus where it does better (pipeline, data, rules, tricky app logic),
Sonnet for UI, docs, mechanical work and most reviews.

**Rules for every chat** (memory `credit-efficient-structure`, owner 2026-11-23; they start after the 0.9.0 chat):
- Scope is frozen when the chat starts. New asks become tickets for the next chat unless something is broken.
- At most 3–4 lanes at once, one ticket each, reports of 15 lines or fewer.
- One review round. Lanes run `scripts/before_pr.sh` and check their own diff first (W46, mutation-proved tests).
- Each chat keeps its own restart page and ledger rows current, so the next chat starts cheap.
- Every chat keeps the Play production-access record current (owner 2026-11-24): each piece of feedback becomes a
  feedback-log row and a ticket; each release adds a releases.md row, sets "Fixed in", and updates questionnaire Q3, Q7
  and Q8.
- Questions are batched with pictures and go out by push notification. CONFIRM_PROD is only ever typed by the owner.

Owner's order: "repo move first maybe? the 0.9.1 then feature then launch"; changed 2026-11-24: "We want 0.9.1 before
repo change. I want models to have stuff to work on over night", so 0.9.1 opens tonight beside the 0.9.0 chat and runs
overnight, and the repo move comes after it.

| # | Chat | Scope (frozen) |
|---|---|---|
| P | Planning (runs in the background, alongside every chat) | Intake and planning only, never code: the owner's new asks, tester feedback and reports become tickets + feedback-log rows; it keeps the roadmap, the release contents and OWNER-TODO current. Build chats never write new tickets: they hand new finds to this chat. |
| 1 | Ship 0.9.0 | Finish `runbooks/release-by-hand.md` from where the restart page says: the staging check, the emulator device pass, the owner's production commands, Play Closed, and the releases/feedback-log rows. |
| 2 | 0.9.1 follow-ups (opens tonight, runs overnight beside chat 1) | Trout per-row source pages. T1029 (owner Q4–6 with pictures, in the morning). T1030. The T1047 follow-up: a datum-change list seeded from HYDAT `STN_REMARKS`, with owner-accepted rows for 08KH019/08MH153. The stub-polygon double outline (#81 review). The T1049 and T1041 low findings. Any 0.9.0 device-pass bugs. One file per ledger row and per OWNER-TODO item, with README/OWNER-TODO built from them. Added by the owner 2026-11-24: T905 (salmon feed back under its old size, so the gate can come back down from 110,000 B), T1027 (R8 scores past ~72%), T1016 (a tab open across a release reloads), T1036 (the "All sources" sheet matches the screen), T1021 (the hard-coded-path gate), T1020 (the licence-frame diff gap), T1048 (the cut writes the app_reads row). |
| 3 | Repo move + merge queue | T1041's "after 0.9.0" checklist: the owner moves the repo to a free GitHub org, then the slug flip, the `GITHUB_DISPATCH_TOKEN` re-issue on both feeds workers, the rulesets and the merge queue. |
| 4a | 0.10 feature: stocked lakes | T200 (+ T634 stocking half): research the source and licence, pictures of the Stocked mode, then build. |
| 4b | 0.10 feature: bathymetry maps and charts | T201, T218, T219 (+ T634 depth half): the owner says "0.10 is bathy maps/ charts too". |
| 4c | 0.10 feature: corridors | T79 (imagery corridor + pools validation): the owner says "coul look into the corridors again too". Research and pictures first. |
| 5 | Launch (Play production) | T1046 (application from 2026-12-07; `play-production-access/`), T240, T404, T319, T514, T463, T495. |

**What "launch" means** (owner asked "idk what launch ever needs"): Play only lets the app out of Closed testing after
Google approves the production-access application (`play-production-access/README.md`). That needs ≥12 testers opted in
for 14 days (from 2026-11-23, so 2026-12-07 at the earliest) and the questionnaire. Then come the store listing pieces
(screenshots T319, the "Open in the app" link T514) and the production checklist (T240, T404). Nothing breaks while it
waits, as long as the testers stay opted in. The feedback log and releases.md have to stay current in every chat.

## Prompts to paste (one per chat)

**P. Planning (background, keep it open)**
> Start at handoff/PLAN/START-HERE.md (step 0, reports first) and ROADMAP-after-0.9.0.md. You are the planning chat:
> never write code, never run lanes. Your job: turn my asks, tester feedback and reports into tickets (you alone
> create ticket numbers), keep play-production-access/feedback-log.md, ROADMAP-after-0.9.0.md, the release contents
> and the owner to-do (one file per item in handoff/PLAN/owner-todo/, T1041) current, and say which future chat or release each item belongs to. Small docs PRs with auto-merge.
> Ask me questions in batches. Build chats send you their new finds; you ticket them.

**1. Ship 0.9.0**
> Start at handoff/PLAN/START-HERE.md (step 0, reports first). You orchestrate. Scope: ship 0.9.0 only, nothing new.
> Continue runbooks/release-by-hand.md from where the restart page says: the staging check, then the emulator device
> pass (the restart page's T-list), then hand me every production command in order (CONFIRM_PROD is mine), then the
> Play Closed promote. Update play-production-access/releases.md and the feedback-log "Fixed in" column. End with the
> next restart page.

**2. 0.9.1 follow-ups (paste tonight; runs overnight)**
> First make a scratchpad worktree of origin/main and read everything from it: the main checkout is on release/0.9.0
> and has neither the roadmap nor the newest tickets. Then handoff/PLAN/START-HERE.md (step 0) and
> handoff/PLAN/ROADMAP-after-0.9.0.md row 2. You orchestrate on Fable; scope frozen to row 2. I'm asleep: work
> overnight unattended, at most 4 lanes, one ticket each (group the small ones), the model by task (Opus for
> pipeline/data/Android/tricky app logic, Sonnet for docs and mechanical work), one review round, PRs to main with
> auto-merge once the independent review is clean. Another chat is shipping 0.9.0 tonight: never touch the main
> checkout (it is on release/0.9.0), only scratchpad worktrees off origin/main; no staging data release, no staging
> deploy, no Play upload (that chat owns them); heavy jobs (Gradle builds, emulator runs, pipeline tiers) only through
> scripts/heavy.sh. Emulator: use only the AVD n1_pixel_b on port 5556 (`emulator -avd n1_pixel_b -port 5556`), every
> adb call with `ANDROID_SERIAL=emulator-5556`, Metro on port 8091, never port 8081; never touch n1_pixel /
> emulator-5554 (the 0.9.0 device pass); stop only your own emulator. Never write into the main checkout's data/
> (data/release, LATEST, tiles, curated): a lane that needs a build or release writes into its own worktree; a test that
> reads the live release may see 0.9.0 promoted mid-run, so rerun it before calling it red. Write your own memory
> in-flight file; never rewrite the 0.9.0 chat's. The T666 fixture re-cut is on release/0.9.0, not main, until that
> chat merges it back. Do the one-file-per-ledger-row lane last, after the others merge. T1029 waits for my morning
> answers (prepare the pictures overnight). Leave questions batched with pictures for the morning, send one push
> notification when the night's work is done, and keep the restart page current. Then, once 0.9.0 is on production,
> cut 0.9.1 to staging + Play Closed with the production commands for me.

**3. Repo move + merge queue**
> Start at handoff/PLAN/START-HERE.md, then T1041's "after 0.9.0" checklist. Walk me through moving the repo to a free
> GitHub org (my clicks). Then the slug-flip commit, the GITHUB_DISPATCH_TOKEN re-issue for both feeds workers (staging
> is yours, prod is mine), checking the rulesets survived, and turning on the merge queue. One lane at most. Update the
> restart page.

**4a / 4b / 4c. A 0.10 feature** (one chat per feature)
> Start at handoff/PLAN/START-HERE.md and ROADMAP-after-0.9.0.md row 4a (or 4b, 4c). Feature chat for 0.10. Research
> first (source, licence, effort), then pictures of the look for me to pick. Build only after I pick: at most 3 lanes,
> one review round. Keep the feature's own page in handoff/PLAN current.

**5. Launch**
> Start at handoff/PLAN/START-HERE.md and handoff/PLAN/play-production-access/. You orchestrate; mostly docs and my
> clicks. Get the production-access application ready (I fill the TODO numbers), then T240, T404, T319, T514 and T463
> (I run /code-review). No feature work.
