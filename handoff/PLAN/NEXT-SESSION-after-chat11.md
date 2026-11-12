# Next session: after chat 11 (written 2026-11-11)

> **Restart 2026-11-12 (computer restart mid-run).** Read first: "Restart 2026-11-12" in
> `app-integration/T678-progress.md` (release stopped at the Qualcho Creek card defect) and
> `app-integration/T678-coast-proposal.md` (tidal proposal DRAFT, page not published). Working copies outside the
> repo: `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/STATE-at-restart.md` (order of work). Owner answers
> to-do items in chat by question dialog; page ticks do not work. Next free ticket T685.


## Where things are
- **The latest build is out** on staging and Play internal; the STATUS top entry names the release and the app
  version. The owner's phone check is pending.
- Branch `redesign/stream-sections`, everything pushed. Next free ticket: **T669**.
- The owner lifted the fence around chat 10's area (review app, `pipeline/regs/review`, `pipeline/regs/dfo_salmon`,
  review data): edit and commit there. Marks are still committed as-is and never edited; do not disturb the review
  app copy the owner is labelling in (ports 8787/5173). `data/generated/review/dfo-suggestions.json` is uncommitted
  and theirs: look before touching.

## The owner's list is a page
- Owner To-Do: https://claude.ai/artifact/Fm5KqHqutxRJsk8UymFo6F (database collection `todo`). Read it at the start (ticks and notes are the owner's answers);
  add a row for every new owner item (ArtifactData `set`, fields: title, detail, group, ticket, order, added, done:false).
  Groups in use: "Check on your phone", "Look and pick", "Decide", "Review", "Run or provide". **First thing (owner 2026-11-11): the older
  phone checks page (claude.ai/artifact/5LRBFFgvYeTRnygSBWvZbM) is OUT OF DATE. Write the phone checks for the
  latest build (what changed since the previous Play build, plus the cold-start water step the test bed missed) as
  rows in the Owner To-Do page, group "Check on your phone"; do not send the owner to the old page.**
- Chat 11's emulator test-bed run on the latest build: install, feeds, big-update, offline, gestures passed;
  water-nav passed 6 of 7 and its last step (cold start, the water again) failed on a test-bed bug, not the app
  (`pidOf` threw when the app was stopped; fixed in `app/tools/testbed/lib/device.mjs`, not yet rerun). Rerun
  `water-nav` once to cover that step.

## The agreed order (owner 2026-11-11)
1. Owner phone check of that build (answers in the checks artifact; fix what it finds).
2. **This chat's job:** hardening and leftovers + closed-testing items (T495) + the next atlas rebuild, all aimed at
   the closed-testing build. Parallel agents when files do not collide. Fable is allowed for high-impact work.
3. Owner runs `/code-review ultra`. 4. Fix its comments. 5. One more build + short phone check, then closed testing.
- Decisions are NOT asked one by one: take the stated default, log it in `OWNER-TODO.md` ("Batch for the end"), and
  go through the batch with the owner at the end. Interrupt only when a wrong default would be costly.

## Work list for step 2 (tickets hold the detail)
- **FIRST TASK (owner): T667 — migrate every test that pins raw section numbers or reads live data outside the
  helpers (~230 files), then turn the guard on for all of them (no shrinking list).** Owner answers are in T667.
- **Release and tests (do first, they make everything after cheaper):** T666 one release path; T667 tests that grow
  with the app (audit + design done: `app-integration/T667-audit.md`; slices 1–3 first).
- **Next atlas rebuild, one build:** T664 Oxbows as one water; T665 tidal (design done:
  `app-integration/T665-design.md`, owner answered D1–D6 in the ticket: note D4 line end wins on 33 creeks, D5 Brae Slough dropped from search; starts with an in-memory dry run); the nine DFO marks
  that need new cuts (on `NOT_LANDED_OK`).
- **Closed testing (T495) and links:** T625 items 2–4 (fingerprint is in the ticket); T514; T240 checklist.
- **Hardening:** T549, T554, T555, T556, T557, T560, T585, T593, T597, T600, T601, T602, T603, T605, T606, T608,
  T611, T628, T630, T631, T633, T661 item 3 (install the parked heavy.sh gate), T507, T540.
- **Audits:** T668 DFO places covering one stretch in two regions (Fraser 2/3), read-only first; T462.
- **Owner-look items:** T465, T467, T504, T627, T629; T634 lake tab (owner decisions of 11-11).
- **Small debts from chat 11:** send the Stellako F12 card image (OWNER-TODO); re-measure release perf A5 on an idle
  machine (9.4–9.9 ms against 8 ms, same on the previous release); the staging site was exported as 0.7.4 while Play
  has 0.7.5 (same code; redeploy with the next app change); two untracked `.d.ts` files in `app/packages/core/src`
  are stray; tell nobody to pull (chat 10 is closed).

## Lessons from this release (why T666/T667 exist)
- Promote moves LATEST; fixtures cut from LATEST (`node app/tools/regs-recut/sync.mjs`) must be re-cut and committed
  before the next fast-fail; every commit must be pushed before `python -m pipeline release --staging`.
- A rerun after promote takes no `--baseline` / `--accept-diff`.
- GitHub can refuse to start the run-timing workflow ("startup failure", generic error); it recovered in ~30 minutes.
- The site build's share cards needed a turn between renders (fixed, guard test `app/tools/seo/share.test.ts`).
