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
| 1 | Ship 0.9.0 — DONE 2026-11-24 (production data, site, feeds worker; Play build 44 on Internal + Closed; iPhone build 45 uploaded; `release/0.9.0` merged back, PR #140) | Finish `runbooks/release-by-hand.md` from where the restart page says: the staging check, the emulator device pass, the owner's production commands, Play Closed, and the releases/feedback-log rows. |
| 2 | 0.9.1 follow-ups (opens tonight, runs overnight beside chat 1) | Trout per-row source pages. T1029 (owner Q4–6 with pictures, in the morning). T1030. The T1047 follow-up: a datum-change list seeded from HYDAT `STN_REMARKS`, with owner-accepted rows for 08KH019/08MH153. The stub-polygon double outline (#81 review). The T1049 and T1041 low findings. Any 0.9.0 device-pass bugs. One file per ledger row and per OWNER-TODO item, with README/OWNER-TODO built from them. Added by the owner 2026-11-24: T905 (salmon feed back under its old size, so the gate can come back down from 110,000 B), T1027 (R8 scores past ~72%), T1016 (a tab open across a release reloads), T1036 (the "All sources" sheet matches the screen), T1021 (the hard-coded-path gate), T1020 (the licence-frame diff gap), T1048 (the cut writes the app_reads row). |
| 5i | iOS full release (started 2026-11-24; owner 2026-11-24) | T1071: the App Store distribution and compliance checklist (App Privacy, export compliance, age rating, content rights, availability, review notes), production push proved (T996), the release step list; the owner submits 0.9.x to the App Store. Docs and owner clicks, no feature work. |
| S | Store content (START NOW: the App Store submission waits for its new set, owner 2026-11-24 23:06) | T1072: 2–3 screenshot directions as pictures, the owner picks, one set in every Play and App Store size, one source of listing words for both stores and the website. T1076: the reshoot tool builds its own store-shots export. With T319, T709. |
| 3 | Repo move + merge queue | T1041's "after 0.9.0" checklist: the owner moves the repo to a free GitHub org, then the slug flip, the `GITHUB_DISPATCH_TOKEN` re-issue on both feeds workers, the rulesets and the merge queue. Also T1062: the release-candidate (rc tag) plan, written for the owner to agree before anything is built (owner 2026-11-24). |
| 2b | 0.9.2 hardening (after chat 3; owner approved 2026-11-24) | No features. T1055, T1057, T1060, T1063, T1064, T1065, T1066, T1067, T1070, T1073, T1074, T1075, T1077, T1078, T1079, T1084, T1085, T1088, T1089, T1090, T1091, T1092, T1093, T1094, T1096, T1097, T1100, T1102, T1103, T1104, T1105, T1106; T1099 and T1101 if they miss the 0.9.1 iPhone build (Standard map and the salmon facts: a cold-open test and a live check). Also T914 (web first load: the owner's 2026-11-24 Lighthouse run on production, 25.2 s of main-thread work and 7.1 MB first load): an Opus lane that starts when the first of the four merges, measure first. Four lanes: Opus T1060+T1073+T1091+T1104; Opus T1064+T1066+T1067+T1089+T1103; Opus T1065+T1075+T1078+T1084+T1088+T1096+T1105+T1106; Sonnet T1055+T1057+T1063+T1070+T1074+T1077+T1079+T1085+T1090+T1092+T1093+T1094+T1097+T1100+T1102. |
| D | Docs lean-out (after 2b; owner approved 2026-11-24) | T1081: inventory of every markdown file with a verdict, the owner agrees the layout, then the cut by folder and a guard that keeps it lean. T1080 (the new short owner page) is done before it by the planning chat. |
| 4a | 0.10 feature: stocked lakes | T200 (+ T634 stocking half): research the source and licence, pictures of the Stocked mode, then build. |
| 4b | 0.10 feature: bathymetry maps and charts | T201, T218, T219 (+ T634 depth half): the owner says "0.10 is bathy maps/ charts too". |
| 4c | 0.10 feature: corridors | T79 (imagery corridor + pools validation): the owner says "coul look into the corridors again too". Research and pictures first. |
| 5 | Launch (Play production) | T1046 (application from 2026-12-07; `play-production-access/`), T240, T404, T319, T514, T463, T495. |

**What "launch" means** (owner asked "idk what launch ever needs"): Play only lets the app out of Closed testing after
Google approves the production-access application (`play-production-access/README.md`). That needs ≥12 testers opted in
for 14 days (from 2026-11-23, so 2026-12-07 at the earliest) and the questionnaire. Then come the store listing pieces
(screenshots T319, the "Open in the app" link T514) and the production checklist (T240, T404). Nothing breaks while it
waits, as long as the testers stay opted in. The feedback log and releases.md have to stay current in every chat.

**Order (owner 2026-11-24, planning chat questions):** 1 ship 0.9.0 → 2 0.9.1 → 5i iOS full release (started 11-24), with S store
content beside it (the submission waits for S's new picture set, owner 11-24) → 3 repo move (with the rc-tag plan) → 2b 0.9.2 hardening → D docs lean-out → 4a–4c 0.10 features →
5 Play launch (application from 2026-12-07; it starts whenever Google's date allows, whatever else is running). The
order of 3, 2b and D after the iOS release is the planning chat's proposal; the owner chose only "iOS first".

## Who is who (2026-11-24; `ListAgents` shows the live names)

| Chat | Session | Restart page |
|---|---|---|
| P planning | `bc-freshwater-fishing-regulations-8e` | this file + the ledger; it alone numbers tickets (from T1055) |
| 1 ship 0.9.0 | `bc-freshwater-fishing-regulations-37` | `handoff/PLAN/NEXT-SESSION-2026-11-24-after-090.md` (chat finished 2026-11-24) |
| 2 0.9.1 follow-ups | `bc-freshwater-fishing-regulations-56` | `handoff/PLAN/RESTART-091-chat.md` |
| 5i iOS full release | `bc-freshwater-fishing-regulations-06` (started 2026-11-24 23:10, beside chat 2 at the owner's ask) | its own page under `handoff/PLAN/ios/` (to come) |

Build chats list new finds under "For the planning chat" on their restart page and message the planning chat; it
replies with the Tnnn.

## Release contents (kept by the planning chat)

Triage of everything ticketed since the roadmap was written. Severity / model / size are on each ticket's last line.

| Release / chat | Goal (short) | Tickets added by triage |
|---|---|---|
| 0.9.0 (chat 1) | Ship the batch of small fixes from closed-test feedback to production, Play Closed and TestFlight. | none (shipped 2026-11-24) |
| 0.9.1 (chat 2) | Follow-ups to 0.9.0, anything found broken on its device pass, and the owner's four staging reports (gear, closed-water and tributary wording; a data release). | T1058 Gradle Metaspace hang and T1059 `ci_status --strict` (both bite its Play build; the 0.9.1 chat took them 2026-11-24), T1061 "Flows into" blank band, T1083 the web start link refuses `wsc:` ids (HIGH: confirmed on production 0.9.0), T1086 ruling RU-14 rewritten with T1053, T1087 the Brunette's "upstream of Burnaby Lake" closure bound to the whole river (traced 2026-11-24: right, closes with the T1053 PR), T1050–T1053 (the owner's four staging reports) as the 0.9.1 chat triaged them; T1098 the privacy page made true (site only; must be live before the App Store submission); T1099 and T1101 (taken by chat 2, one lane); the 0.9.1 iPhone build is chat 2's too (owner 2026-11-24 23:55: "0.9.1 chat"), built and uploaded after its Play Closed build |
| Chat 3 | Move the repo to an org and turn on the merge queue; settle how releases are tagged. | T1062 release candidates (rc tags): plan first, owner agrees |
| 0.9.2 (chat 2b; owner approved 2026-11-24) | Harden: crash containment, failures that hide, release tooling and test flakiness; clear the report backlog so new reports stand out. No new features. | T1055, T1057, T1060, T1063, T1064, T1065, T1066, T1067, T1070, T1077 ("All sources" bait source with no line), T1078 (a two-rule all-year closure names one), T1079 (Playwright logs land in the main checkout), T1074 (rule 28 vs runtime map layers), T1075 (a lake's "Not on lakes" gear line), T1073 (the app-update notice only when the phone can install it; owner 2026-11-24), T1084, T1085, T1088, T1089 (build-assets bucket), T1090 (data host robots.txt), T1091, T1092–T1094 (three crash kinds of 11-24), T914 (web start, PageSpeed mobile); the lanes are in chat row 2b above |
| Store content (chat S, beside 5i; owner approved 2026-11-24) | Screenshots and marketing words that sell the app, one source for both stores and the website. Pictures first, owner picks. | T1072 (with T319, T709), T1076 (the runbook's reshoot step cannot be followed as written) |
| iOS full release (chat 5i, first after 0.9.1; owner approved 2026-11-24: "IOS can have full release soon") | The iPhone app on the App Store for everyone: distribution and compliance answers complete, production push proved, submitted by the owner. Does not wait for Play's 14 days. | T1071 (with T727, T996) |
| Docs lean-out (chat D; owner approved 2026-11-24) | A human can read the docs: old files deleted, bloated ones cut, one unified layout, and a guard that keeps it lean. Inventory first, owner agrees the layout. | T1081; T1080 (the new short owner page) is done first by the planning chat, after PR #114 |
| 0.10 (chats 4a–4c) | The features: stocked lakes, bathymetry maps and charts, corridors; tides if it fits. | T1054 tides layer (or later), T1082 in-app rating prompt (0.10 or 0.11; design first), T1068 R8 leftovers, T1069 polygon route in pieces, T1095 fish measurements on a saved spot (owner: 0.10, pictures first) |
| Launch on Play (chat 5) | Production access from Google (apply on or after 2026-12-07), then the public Play release. | none new (uses T1072's content) |

## Prompts to paste (one per chat)

**P. Planning (background, keep it open)**
> Start in a scratchpad worktree of origin/main and read everything from there (never touch the main checkout). Then
> handoff/PLAN/START-HERE.md (step 0, reports first; link the git-ignored worker tokens from the main checkout into
> the worktree's app/workers/feeds/ so both workers are read) and ROADMAP-after-0.9.0.md (row P, "Who is who",
> "Release contents"). You are the planning chat: never write code, never run lanes, builds, emulators or heavy jobs.
> You alone create ticket numbers (file + ledger row, check-tickets.mjs before every commit). Triage each item:
> severity, release or future chat, model and lane size. Keep current: this roadmap, the release contents,
> OWNER-TODO.md (markdown only: the artifact page is retired, T1080) and play-production-access/ (feedback-log rows, releases.md,
> questionnaire Q3/Q7/Q8). Find the other chats with ListAgents, read their restart pages, ticket their "For the
> planning chat" items and tell each chat the Tnnn. When a chat's scope finishes, write the next chat's frozen scope
> and prompt here. Small docs PRs to main with auto-merge, `docs(Tnnn): ...`. Questions in batches with pictures, each
> batch also as a push notification; answers into the ticket and OWNER-TODO the same turn. CONFIRM_PROD is the owner's.

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

**5i. iOS full release (first after 0.9.1)**
> First make a scratchpad worktree of origin/main and read from it. Then handoff/PLAN/START-HERE.md (step 0),
> ROADMAP-after-0.9.0.md row 5i, T1071, T727 and handoff/PLAN/ios/. You orchestrate on Fable; scope frozen to T1071;
> docs and my clicks, no feature work; at most 2 lanes (Sonnet for the checklist and listing pages, Opus only where
> a privacy or compliance answer needs the code read), one review round. Write one page under handoff/PLAN/ios/ I
> paste from, with every App Store Connect answer drafted or marked mine. Prove production push (T996) before the
> build that goes to review. Then walk me through the submission: I do every App Store Connect click and anything
> with CONFIRM_PROD. Take the pictures and listing words from the store content chat (T1072); if they are not ready,
> say so and use the current set. New finds go to the planning chat, which numbers the tickets. Keep your own
> restart page.

**S. Store content (beside 5i)**
> First make a scratchpad worktree of origin/main and read from it. Then START-HERE.md (step 0),
> ROADMAP-after-0.9.0.md row S, T1072, T1076, T319. You orchestrate on Fable; scope frozen to T1072 + T1076; Sonnet
> lanes, at most 2, one review round. First fix the reshoot tool (T1076) so it builds its own store-shots export.
> Then show me 2–3 directions for the screenshot set as pictures, in chat, and wait for my pick. Then ONE new set
> from the 0.9.x app, the same frames for Android and iPhone (my ask of 11-24), in every Play and App Store size, in
> the repo where the push script looks, and one source of listing words for both stores and the website. The iPhone
> submission (chat 5i, session in "Who is who") waits for this set, and App Store pictures change only with a new
> version: tell that chat when the set is ready. No uploads to
> either store: hand me the files and the steps. New finds go to the planning chat.

**2b. 0.9.2 hardening (after chat 3)**
> First make a scratchpad worktree of origin/main and read from it. Then START-HERE.md (step 0) and
> ROADMAP-after-0.9.0.md row 2b. You orchestrate on Fable; scope frozen to row 2b, no features; the four lanes and
> their models are in the row; one review round; PRs to main with auto-merge once the independent review is clean;
> heavy jobs only through scripts/heavy.sh. New finds go to the planning chat, which numbers the tickets. When the
> lanes are merged, cut 0.9.2 to staging and Play Closed and hand me the production commands (CONFIRM_PROD is mine).
> Update play-production-access/releases.md and the feedback log's "Fixed in" column. Keep your own restart page.

**D. Docs lean-out (after 2b)**
> First make a scratchpad worktree of origin/main and read from it. Then START-HERE.md and T1081 (T1080 is already
> done). You orchestrate on Fable; scope frozen to T1081; docs only, no code beyond the docs guard. Step 1, one
> Sonnet lane: the inventory of every markdown file with a verdict and the proposed layout; show it to me and wait
> for my yes. Step 2, at most 3 lanes by folder (Sonnet; Opus for AGENTS): delete freely, merge, cut, fix every
> citation. Step 3: the size guard. One review round. Report the file and line counts before and after.

**4a / 4b / 4c. A 0.10 feature** (one chat per feature)
> Start at handoff/PLAN/START-HERE.md and ROADMAP-after-0.9.0.md row 4a (or 4b, 4c). Feature chat for 0.10. Research
> first (source, licence, effort), then pictures of the look for me to pick. Build only after I pick: at most 3 lanes,
> one review round. Keep the feature's own page in handoff/PLAN current.

**5. Launch**
> Start at handoff/PLAN/START-HERE.md and handoff/PLAN/play-production-access/. You orchestrate; mostly docs and my
> clicks. Get the production-access application ready (I fill the TODO numbers), then T240, T404, T319, T514 and T463
> (I run /code-review). No feature work.
