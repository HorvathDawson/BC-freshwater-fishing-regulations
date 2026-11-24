# After 0.9.0: the chats, in order (owner 2026-11-24)

Planned with the owner at the end of the 0.9.0 batch chat. Each line below is its own chat. The orchestrator runs on
Fable and picks each worker's model by task: Opus where it does better (pipeline, data, rules, tricky app logic),
Sonnet for UI, docs, mechanical work and most reviews.

**Rules for every chat** (memory `credit-efficient-structure`, owner 2026-11-23; they start after the 0.9.0 chat):
- Scope is frozen when the chat starts. New asks become tickets for the next chat unless something is broken.
- At most 3–4 lanes at once, one ticket each, reports of 15 lines or fewer.
- One review round. Lanes run `scripts/before_pr.sh` and check their own diff first (W46, mutation-proved tests).
- Each chat keeps its own restart page and ledger rows current, so the next chat starts cheap.
- Questions are batched with pictures and go out by push notification. CONFIRM_PROD is only ever typed by the owner.

Owner's order: "repo move first maybe? the 0.9.1 then feature then launch".

| # | Chat | Scope (frozen) |
|---|---|---|
| 1 | Ship 0.9.0 | Finish `runbooks/release-by-hand.md` from where the restart page says: the staging check, the emulator device pass, the owner's production commands, Play Closed, and the releases/feedback-log rows. |
| 2 | Repo move + merge queue | T1041's "after 0.9.0" checklist: the owner moves the repo to a free GitHub org, then the slug flip, the `GITHUB_DISPATCH_TOKEN` re-issue on both feeds workers, the rulesets and the merge queue. |
| 3 | 0.9.1 follow-ups | Trout per-row source pages. T1029 (owner Q4–6 with pictures). T1030. The T1047 follow-up: a datum-change list seeded from HYDAT `STN_REMARKS`, with owner-accepted rows for 08KH019/08MH153. The stub-polygon double outline (#81 review). The T1049 and T1041 low findings. Any 0.9.0 device-pass bugs. |
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

**1. Ship 0.9.0**
> Start at handoff/PLAN/START-HERE.md (step 0, reports first). You orchestrate. Scope: ship 0.9.0 only, nothing new.
> Continue runbooks/release-by-hand.md from where the restart page says: the staging check, then the emulator device
> pass (the restart page's T-list), then hand me every production command in order (CONFIRM_PROD is mine), then the
> Play Closed promote. Update play-production-access/releases.md and the feedback-log "Fixed in" column. End with the
> next restart page.

**2. Repo move + merge queue**
> Start at handoff/PLAN/START-HERE.md, then T1041's "after 0.9.0" checklist. Walk me through moving the repo to a free
> GitHub org (my clicks). Then the slug-flip commit, the GITHUB_DISPATCH_TOKEN re-issue for both feeds workers (staging
> is yours, prod is mine), checking the rulesets survived, and turning on the merge queue. One lane at most. Update the
> restart page.

**3. 0.9.1 follow-ups**
> Start at handoff/PLAN/START-HERE.md and ROADMAP-after-0.9.0.md row 3. You orchestrate: at most 3 lanes, one ticket
> each, one review round, scope frozen to row 3. Ask my T1029 Q4–6 with pictures first. Batch questions to the end.
> Then cut 0.9.1 to staging + Play Closed, with the production commands for me.

**4a / 4b / 4c. A 0.10 feature** (one chat per feature)
> Start at handoff/PLAN/START-HERE.md and ROADMAP-after-0.9.0.md row 4a (or 4b, 4c). Feature chat for 0.10. Research
> first (source, licence, effort), then pictures of the look for me to pick. Build only after I pick: at most 3 lanes,
> one review round. Keep the feature's own page in handoff/PLAN current.

**5. Launch**
> Start at handoff/PLAN/START-HERE.md and handoff/PLAN/play-production-access/. You orchestrate; mostly docs and my
> clicks. Get the production-access application ready (I fill the TODO numbers), then T240, T404, T319, T514 and T463
> (I run /code-review). No feature work.
