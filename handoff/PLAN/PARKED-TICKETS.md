# Next session: the parked tickets (written 2026-11-14)

A second chat, running beside the main one. It works the list below. The main chat keeps: the back-from-regs flash
fix, the owner's `/code-review ultra` fixes, the final build, and closed testing. iOS has its own page:
`handoff/PLAN/ios/START-HERE-ios.md` (not this chat's work).

Read in order: `AGENTS.md` (binding), `CLAUDE.md`, then this page, then the ticket you pick. Work list = the ledger
`handoff/PLAN/app-integration/tickets/README.md` (tickets are the sole source of work). Next free ticket number was
**T717** when this was written: look at the last ledger row first, the main chat files tickets too.

## State
- Branch `redesign/stream-sections`. Never `main`. Merge only into it, naming it.
- Release **r-f749475d0962** is on staging and is local `data/release/LATEST`.
- **Build 31 = 0.7.9** is on Play internal (versionCode 31).
- `data/generated/review/dfo-suggestions.json` is modified and the owner's: do not touch or commit it.

## Who is changing what (avoid these)
The main chat is still changing the app tap/back path: `app/packages/ui-native` (the Shell), the `map/` code, and the
emulator test bed (`app/tools/testbed/`). Do not edit those without asking. The `app/` tree and `handoff/PLAN/ios/`
are also being edited by other lanes: stay out of `handoff/PLAN/ios/`.
Code-review fixes will land across the whole tree soon. So: commit small, always `git commit -- <paths>`, `git pull
--rebase` often, re-read a file before editing it if you left it open for a while.

## Hands off the build train
The main chat owns the build train. From this chat, **no app build, no Play upload, no atlas promote, no release**
without checking with the owner first. Pipeline heavy jobs go through `scripts/heavy.sh <label> <min free %> -- <cmd>`,
one at a time; if the first lane is held (`scripts/heavy.sh --status`) use the second:
`HEAVY_LOCK=~/.claude/heavy-job-2.lock`. Never more than two lanes in total, and read the status before starting.

## The rules that bite (all in `AGENTS.md`; these are the ones that cost us)
- **No LLM parser runs, no `claude -p`** (`run_parse.sh`, `pipeline.regs.parsing.dispatch`): hand the owner the command.
- Branch name as above. Prefix commands with `rtk`. `graphify query` before grepping source. Commit with `-- <paths>`.
- **Never pattern-kill** (`pkill`, `killall`): only your own PID/port. Never touch ports 8787, 5173, 39217, 5555.
- **Never touch** `marks.jsonl`, `data/generated/review/dfo-suggestions.json`, or regenerate gauges `matches.json`
  (`data/curated/gauges/`): surgical edits only, on the owner's say.
- **No silent fallbacks** (a failure is shown and also reported with `reportFailure`). **One load path per screen**
  (same output whichever way in). **Self-host everything** (Esri imagery and Open-Meteo are the only exceptions).
- **Test only what changed** (owner 2026-11-13); the whole suite at milestones only (`scripts/test-smoke.sh` per
  commit, `scripts/test-full.sh` through `heavy.sh` before a merge). Do not trust `rtk pytest` for a gating run.
- **Owner decisions: multiple-choice dialog, with a stated default.** Record the answer the same turn in three places:
  the ticket, `handoff/PLAN/OWNER-TODO.md`, and the To-Do page
  (https://claude.ai/artifact/Lh2ougAwfxx1wNmCV18m8f, collection `todos`). Search this transcript before re-asking.
- Everything Claude adds for the owner to review is a **proposal**, never pre-accepted. Never filter or rename data
  content (offensive names etc.) without asking.
- **Subagents:** Opus for pipeline / data / build work, Sonnet for docs / UI / mechanical work. Not Fable.
- **Images to the owner:** send with SendUserFile (or a staging link). The owner cannot open repo paths.
- Summaries in plain words; short briefs and reports.

## The list

### A. Do now (parked from the main chat)
| ticket | real status | done | left | files | size | owner first? |
|---|---|---|---|---|---|---|
| **T667** part 4c: `smoke.txt` derived by rule (D6) | slices 1-3, 4a, 4b, 5, 5b done; 6 begun; 4c, 7, 8 not started | guard is on for every test file (pipeline 0 of 432, app 0 of 642) | derive `pipeline/tests/smoke.txt` from a rule instead of a hand list; then delete `test_answers_year_strip_live.py::test_the_ruling_moves_only_the_nov_jun_keep_days` and its helper, which only stay because `smoke.txt` names that node id | `pipeline/tests/smoke.txt`, `test_smoke_coverage.py`, `scripts/test-smoke.sh` | S-M | no. Check first that the other lane that held `smoke.txt` has merged (`git log -- pipeline/tests/smoke.txt`) |
| **T667** part 6: live counts become invariants or release-diff lines (D3) | not done | `test_answers_year_strip_live` first pin turned into a relation | `test_one_water_kind` (137 / 13 / 9 / 12 sections, 117 aliases) and `test_review_queue` (measured codes and tiers); each replacement needs a planted violation proved on an atlas, or a release-diff printer in `pipeline/release.py`. `test_steelhead_streams`' 57 is a ruling, leave it | `pipeline/tests/`, `pipeline/release.py` | M | no; needs an atlas on disk to prove (use the live one read-only, never rebuild it) |
| **T667** part 7: one test-environment contract and tiers | not started | none | one env script, no-build default, chunk memory footprint; touches `conftest.py` and `ci_no_build.py` | `pipeline/tests/conftest.py`, `ci_no_build.py` | M | no |
| **T667** slice 8 (the "slice 8" of the owner's note; it is T667's, plan in the ticket's "Slice 8 plan") | planned, not built | plan only | one small invented release built by the real pipeline from a tiny committed input tree (about 12 invented waters, about 15 entries); `python -m pipeline.tools.mini_release --check/--write`; then move about 37 app logic test files onto it and shrink the regs fixture release to about 10 real-water walks | new `app/packages/data/dev/mini-release/`, `pipeline/tests/fixtures/mini/`, `pipeline/tools/mini_release.py`; later many tests | L (about 4 sessions) | no. Order in the ticket: after 4c and 7; steps 1-2 before any test moves; first measure whether `pipeline build --side` can take a prebuilt `graph.pkl` |
| **T686** shrink shipped data | OPEN, survey started | measured table of 52 files on r-483b295de57c (in the ticket) | finish the survey (tiles, basemap, app bundle, phone offline total), prototype each candidate's saving (no shipped change), rank them for the owner. Candidates 8 (tiles, T699) and 9 (polygons, T695) are deferred | none shipped; prototypes in the scratchpad | M | yes, for the pick at the end (dialog); the survey itself needs none |
| **T602** tidy the data layout | OPEN | nothing | built files out of `source/`, one `generated/tmp/`, one rules page linked from AGENTS.md; same release hashes before and after | `data/` layout, pipeline writers, `AGENTS.md` | M | no, but it **depends on T600 and T601 (check they are DONE in the ledger)**, and paths under `data/curated/review/` and `data/generated/review/` are the review lane's: plan them, move them last. Needs a full build to prove "same outputs": a heavy job, check with the owner before it |

### B. After closed testing only (list, do not start)
| ticket | status | note |
|---|---|---|
| T714 share saved spots (one, or a group) | OPEN, after closed | needs owner decisions by dialog first (what travels, link vs file vs server, exact place, web or phone only); store text must not promise it until it ships |
| T715 store video builds its own starting state | OPEN, after closed | before the next video regeneration; the iOS run will reuse the same setup step |

Also deferred by the owner and not for now: T200 stocked lakes, T201 depth maps, T634 lake tab, T695 fewer polygons,
T699 trim tiles and basemap.

### C. Open nits recorded but with no owner (file a ticket for each before working it)
| nit | where it is recorded | what is known | decision needed |
|---|---|---|---|
| T711 header says "up to the top of the river" while the name says "-> Source" | `T711-part-header-follows-source-end-name.md` (status IN PROGRESS; code and tests done, left: the Kanaka shot after the next release) | the ticket made the header follow the same end rule as the name, but the header words are the ordinary source wording; it is a wording question, not a bug | owner: should the header say "up to the Source" to match? default: leave as built |
| Run timing tab freezes 140-260 ms on switching to it | T716, "No stall left" line (262 ms; 141-151 ms before the round) | the tab's own restyle; no salmon work in it | none; measure and fix, or accept. Touches the Shell / map path: ask the main chat first |
| 08JB013 still speaks for 11 sections | T691 (match moved to the north fork, no cut) | the "11" is the user's count; I did not find it written down | none; read T691 and the gauge match for the 11 sections |
| Ross Lake level gauge (08PA004) shows "not reporting" | T691 (owner: matched as a Ross Lake lake gauge, level only, no cut) | station off, or ours: **not established** | none; check the live feed for 08PA004 first. Never regenerate `matches.json` |
| Frog / Redfern / Squirrel lakes unplaced | T708 "Still unmatched" (stay in `AWAITING_WATER_ROWS`) | every source tried (FISS, EcoCat, BCGNIS, 2008 tables, Wayback); Hidden, Secret and Square were placed | owner looks later; no more digging without a new lead |

### D. Other ledger rows OPEN or PARTIAL that no chat obviously owns
I could not know a chat's ownership, so this is by status text and age. "left (Claude)" means no owner decision is
needed. The main chat may already hold some: ask before starting any.

| id | title | status | blocker |
|---|---|---|---|
| T593 | keep DFO heading bytes verbatim in the scrape | OPEN | waits on the owner's next scrape (re-keys DFO entries) |
| T630 | one hydrograph presenter for app and public page | OPEN | none; design question first |
| T608 | read-only audits before closed testing | OPEN | A and B and C done; rest of the ticket |
| T463 | deep code review of the branch | OPEN | the owner's `/code-review ultra`, main chat's |
| T465, T467 | boundary label overlap; lake shoreline shade | OPEN | owner look |
| T504, T528 | "see every regulation line" format; flood advisories | OPEN (future) | later; mockup first |
| T319 | store screenshots | OPEN | shot list and titles wait for the owner |
| T240 | production launch checklist | OPEN | mixed Claude and human items |
| T589, T590 | cut proposals: withdraw button; one-click swap | PARTIAL | the Allco click is the owner's |
| T611, T623, T631, T638, T661, T677 | pipeline leftovers (chain order, rules-vs-book findings, reach-run follow-ups, suite leftovers, heavy.sh gate, rebuild leftovers) | PARTIAL | most wait on the next atlas rebuild or an idle tree; T677 item 2 is the owner's |
| T555, T558, T560, T561, T597, T605, T609 | hardening leftovers (update service, feeds worker, shared helpers, static layers, B.C. clock) | PARTIAL / IN PROGRESS | "left (Claude)"; some need an emulator or a worker deploy (deploys: ask) |
| T534-T538, T542 | release / build / test-bed tooling | IN PROGRESS | each wants one real run of a release or APK command: the build train, ask |
| T401, T360, T409, T410 | map pop-in, memory, Lighthouse, web/native split | PARTIAL | measurements on a device or emulator |
| T496, T485, T552, T540, T539 | cleanup sweep, run-timing release path, forecasts, reading cut, ledger guard | PARTIAL | "left (Claude)" items |
| T625, T514, T371 | App Links, share page link, Play closed-testing forms | PARTIAL / IN PROGRESS | Play Console values (owner) |
| T55, T88, T164, T178, T244 | sloughs, iOS TestFlight, DFO salmon dossier, CLEVER vs COFFEE, store readiness | BLOCKED / PARTIAL / OPEN | owner confirmations, accounts, time (T178: 2026-12-01) |
| many "IN REVIEW" rows | e.g. T498, T508, T563-T568, T636 | IN REVIEW | the owner's phone look; not Claude work |

The full table with commits is in the ledger; I did not list its POSTPONED and DONE rows.

## Suggested order
1. **T667 4c (smoke.txt derived)**: small, unblocks the dead test and slice 8's shared files.
2. **T667 part 7 (env contract)**: touches the same two files as 4c; slice 8 must come after it.
3. **T667 part 6 (live counts)**: independent, needs the live atlas read-only; do it while 7 is reviewed.
4. **The nits in C**: file the tickets first (cheap), check Ross Lake's feed and the 08JB013 count (small facts), ask the
   owner the one T711 wording question in a dialog.
5. **T686 survey and proposal**: read-only measuring, no collision with anyone; ends in an owner pick.
6. **T667 slice 8**: the big one (L); start once 4c and 7 have merged, steps 1-2 before moving any test.
7. **T602**: last, because it needs T600 and T601 done, a full build to prove same outputs, and it moves paths the
   review lane uses.
Between steps, pick items from D marked "left (Claude)" if you are waiting on something.

## The first message the owner can paste into the new chat
> Read `handoff/PLAN/PARKED-TICKETS.md` and start work. The main chat owns the build train and the app
> tap/back path; ask me before any app build, Play upload or atlas promote.

## Added 2026-11-14 (from the iOS chat's read-only audit, `handoff/PLAN/ios/parity-audit.md`)
- **G14, Android, no ticket yet (needs one; unproven on a device):** saved spots and photos are never in Google backup,
  because the installed release puts the app over the 25 MB backup limit. Fix idea to check: exclude the downloaded
  release from backup so spots and photos fit. Decide before or after closed testing with the owner.
  OWNER DECIDED 2026-11-14, verbatim: "we should have it save spots with prioirty" and "images can be compressed if
  still visible". Sent to the iOS chat as a task (it already does the iOS backup exclusion in the same files); it opens
  the ticket (T732 or later). Photo compression lands as a proposal with a before/after pair for the owner.
- **T729 follow-up:** on iOS a React Native Modal has no edge swipe and `onRequestClose` is Android-only; wrap the
  credits card in the iOS chat's `ModalBack` once its `ios/t723-keyboard-back` branch is merged.
