# Next session: after chat 12 (written 2026-11-14)

Read first: `AGENTS.md`, `CLAUDE.md`, this page, then the top entry of `STATUS.md` and `OWNER-TODO.md`. The work list is
the ledger `app-integration/tickets/README.md` (tickets are the sole source). **Next free ticket: T733** (the main
chat used T717 and T728-T731; the iOS chat T718-T727 and T732+; read the ledger's last row before taking a number).

## Where things are
- Branch `redesign/stream-sections`. The release in `data/release/LATEST` is the one on staging, and the last Play
  internal build is named in the STATUS top entry (the restart page holds neither: `check-tickets.mjs`).
- Five owner-reported fixes are committed but NOT in any Play build yet: T717 (Back from a water's page flashed; the
  browse map stays mounted, hidden), T728 (the card's Flow/Level pick no longer changes the Conditions map layer),
  T729 (the credits card is a modal above the legend), T730 (search: swipe down closes, at most 4 recents, the map stays
  where search moved it, a fresh search each open), T731 (a step up/down a water no longer closes the bar;
  `useKeptWhilePending`).
- **0.7.10 / versionCode 32:** a verify-and-build lane was running when this was written. See the newest entry in
  `app-integration/T678-progress.md` for whether it was uploaded.
- Two chats run side by side: this main chat (heavy lock 1) and the iOS chat (`HEAVY_LOCK=heavy-job-2.lock`; its page
  is `handoff/PLAN/ios/START-HERE-ios.md`). `data/generated/review/dfo-suggestions.json` is modified and the owner's:
  do not touch or commit it.

## The agreed order from here
1. The 0.7.10 result, then the owner's phone check.
2. The owner runs `/code-review ultra`. 3. Fix its comments. 4. Final build, ONE full suite, a short phone check.
5. DFO labelling (owner). 6. Play closed testing (T495). After closed testing: T714, T715.

## Production move (owner's, human only)
- The owner runs it himself; Claude never runs a prod script, not even `--dry-run`. Steps: `release/PRODUCTION.md`,
  first-transition steps 3-8. The To-Do page row `hold-prod-move-staging-dirty-1114` has the commands.
- The clean worktree `~/bcfish-clean-staging` (detached at 9048fbc20, prod folders `app/dist/web-prod` and
  `app/dist/seo-prod` built) must NOT be removed or changed until the owner has deployed. After the move, check
  https://canifishthis.ca/privacy.
- The staging site serves 9048fbc20 (0.7.9 + T728 + reworded privacy page, smoke 19/19); it lacks T717, T729, T730,
  T731. `deploy/staging/site-all.sh` now refuses a dirty `app/` (dae26147e): build the site from a clean checkout.

## Privacy owed
- The privacy page (reworded 2026-11-14) says the crash switch is More, Settings, "Send crash reports". The real path is
  More, Settings, Privacy, "Send crash reports". Fix `privacyBody` in `app/tools/seo/html.ts` once the build lane is done.
- Open owner question: the Play form's location answer ("approximate only", owner 11-04) against Precise + Approximate
  (the table, the App Store pack). Row (12) in OWNER-TODO.
- T732 (Android backup: spots first, photos second) is on branch `android/t732-backup`, not merged, not in 0.7.10. When
  it merges: update the privacy page sentences and `play/data-safety.md` row 7. T729 on iOS: wrap the credits Modal in
  `ModalBack` after `ios/t723-keyboard-back` merges.

## For the code review: look hard at
- The self-accepted binding diffs (385 / 145 / 103 rules) and the accepted golden pages.
- T531. The T227 reversal (a770d64dd). Tap declarations and the test pins T89 / T100. The test updates in 4ecbe1738.
- T716 prepare-at-lift (`tapAhead`, `useTapAwaiting`).
- T717 kept hidden map: memory with the page up rose 303 to 363 MB over 4 Conditions repeats; the web hidden map is not
  `inert`; one unexplained app close with no log.
- T729 Modal + scrim (c099e9977). T730 search close. T731 `useKeptWhilePending`.

## Still open from chat 11 (checked against the ledger, closed ones dropped)
- **T667** tests that grow with the app: slices 1-7 done, slice 8 steps 1-2b done on invented inputs; left: the release,
  parked at the export that checks the corpus is the book (decision in the ticket), then move the app logic tests.
- **T666** one release path: built, the first real `release --staging` proves it. **T664 / T665** oxbows and tidal: PARTIAL.
- **Closed testing:** T495 (OPEN), T625 items 2-4, T514, T240 checklist.
- **Hardening still open:** T555, T560, T585, T593 (waits on the owner's scrape), T595 / T596 (IN REVIEW), T603, T605,
  T606, T609, T630, T631, T633, T661 item 3 (the heavy.sh gate), T540, T668, T669, T671, T672.
- **Owner-look items:** T465, T467, T504 (future), T634 lake tab. T602 (data layout) is not started and needs the owner.
- Shipped-data shrink T686 waits on the owner's pick; T699 and T695 are deferred.
- Dropped since chat 11 (closed): T462, T549, T554, T556, T557, T601, T507, T627, T628, T629.

## Open small items
- Run timing tab restyle freeze (262 ms, seen in T716): no ticket of its own. Ross Lake gauge 08PA004 shows "not
  reporting" (it is a level gauge matched to the lake, T691): no ticket. 08JB013 speaks for 11 sections: no ticket.
  The Columbia below Revelstoke shows a non-real-time outflow (08ND025, T691 by design): no ticket. File them if wanted.
- Frog, Redfern and Squirrel lakes unplaced: covered by T708 and the OWNER-TODO row "T89 six waters".

## The owner's list is a page
- Owner To-Do: https://claude.ai/artifact/Lh2ougAwfxx1wNmCV18m8f, collection `todos` (fields added, title, group,
  order, ticket, detail, done, note; answered rows get a note starting "ANSWERED <date>:"). Every owner item goes in the
  page AND `OWNER-TODO.md`. The owner answers decisions by question dialog: record each answer verbatim in the ticket,
  OWNER-TODO and the page row the same turn; search the transcript before re-asking.

## Rules added since chat 11
- Subagents get an explicit model: Opus for pipeline / data / build, Sonnet for docs / UI / mechanical; Fable only in the
  main chat. Lanes test only what changed plus `rtk proxy pnpm check:fast`; one whole suite at milestones.
- Never `--no-ci-check` or `--no-fast-fail`. Send images to the owner with SendUserFile.
- Atlas builds are allowed with coordination (owner 2026-11-14); promote and release stay as before.
