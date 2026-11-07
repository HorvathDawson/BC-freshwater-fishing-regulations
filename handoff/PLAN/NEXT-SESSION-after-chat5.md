# After chat 5 (0.6.8): start here

Written 2026-11-06, as chat 5 closed. It replaces `NEXT-SESSION-after-chat4.md`.

## Paste this to start the chat

> Read `handoff/PLAN/START-HERE.md`, the restart page it names, and the STATUS.md top entries. Do the list in order.
> Start ticket work without asking; ask me only for decisions. Keep replies short and STATUS.md current.

## Where chat 5 left things
- **Data:** release r-67b456b5e64b is still LATEST and on staging. Prod is the owner's step (T404).
- **App:** **0.6.8 (versionCode 18)** is on Play internal. `pnpm check` is green (6789 tests). It holds:
  - **T456:** a new walkthrough (30 steps).
  - **T454:** photo pinch/pan.
  - **T446:** un-awaited file writes, which emptied files on a fresh install.
  - **T455:** a walkthrough tap race.
  - **T451:** no grey frame when switching into Map. The binding holds the picture over a style swap.
  - **T443:** one label for same-words marks; the step bar wraps. First open frames a part the same as ▲ then ▼, after
    the owner OK'd removing the "≤ 2 levels per step" bound.
  - **T459:** in step mode other waters go grey, and the water it flows into stays muted. The owner picked this.
  - **T460:** a step never skips a part. A side channel (no km) stays a fork.
  - **T464:** every part of every water can be stepped to. There is a release test for it.
  - **Looks:** T457 lake shoreline 1.5× (the fill stays: a lighter one failed the colour-blind gate). T461 tidal: no edge
    below z12 and a lighter wash. Park borders from z11.
  - **T458:** the floating-promise gate in `pnpm check`.
  - **The perf lane's Map lab:** staging/test-bed builds only; long-press the credits chip.
- **Map performance (T420/T401/T448)** has its own chat (`MAP-PERF-HANDOFF.md`) and is done for now. Its lab A/B
  results are in T420. Its uncommitted `app/apps/mobile/app.config.js` + `app/tools/testbed/session.sh` (MLN_LOCAL_MAVEN,
  a locally built MapLibre for test-bed builds) belong to that lane. Leave them unless the owner says.
- **iOS is benched** (owner): it waits for the DFO extents labelled + Play closed testing started. Then pay the $99 and
  run TestFlight (OWNER-TODO).

## The list, in order
1. **Owner's 0.6.8 phone check** (OWNER-TODO). Act on what comes back:
   - the walkthrough's wording (Claude's draft),
   - the T451 flicker on the Pixel 9,
   - T459 / T460 steps,
   - the lake look,
   - the Map lab script.
2. **Remove T459's test-bed-only all-grey switch.** The owner picked A. Then move T451/T443/T459/T456 to DONE as the
   owner confirms each.
3. **T420 follow-ups** from the lab's results: ship what the owner's lab A/B on the Pixel 9 shows helps (T420's own
   notes say which; "skip invisible layers" measured a real z13 gain).
4. **T466** (owner's 0.6.8 walkthrough notes: dams/obstacles step must scroll to them, spots must walk a real creation,
   the closed example must be a part closed all year, e.g. an Ecological Reserve) and **T467** (lakes still look solid:
   shoreline as a shade of the fill, inside the colour-blind gate; OWNER QUESTION OPEN in T467: run the colour-blind
   check on the cvd theme only? Claude recommends keeping it on all themes; ask first, then follow the answer). Then **T465:** boundary labels with different words
   overlapping zoomed out.
5. **T351:** read the staging `/crashes` for anything new since 0.6.6. **T446** is reopened only on a new report.
6. **T462 regs-presentation audit:** a multi-agent workflow; the owner starts it with "use a workflow". Read-only.
   **T463:** the owner runs `/code-review ultra` (Claude can't); Claude triages the findings into tickets.
7. **With the next data release:** T461's polygon simplification (tile build), T164 after the owner's DFO review.
8. **T453** (alerts by the day's percentile + the new alert menu) waits for T234 to resume.

## Owner items (not Claude's)
- The 0.6.8 phone check and the Map lab script.
- T164's DFO proposals and the DFO extents labelling.
- T371 Play closed testing: needs 12 testers × 14 days; it gates production.
- Apple Developer ($99; pick individual or organization first) once iOS is unbenched.
- T404 prod.

## Notes
- A background check must end on the gated command. A trailing `echo RC=$?` reported exit 0 on a failed `pnpm check`
  twice in chat 5. Read the log's tail.
- After any release: `node app/tools/regs-recut/sync.mjs`, then `pnpm check`.
- The release-wide nav counts (T460/T464) came from a throwaway test over `loadWaterGroup`. The kept guard is
  `app/tools/every-part-reachable.test.ts`.
- Never commit `data/generated/review/pfma_basin_areas.json` or `data/generated/review/write-through.jsonl`.
