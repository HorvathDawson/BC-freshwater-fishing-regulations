# One chat to 0.5.0: start here

**Superseded once 0.5.0 is on Play: start from `NEXT-SESSION-after-0.5.0.md`.**

Written 2026-11-04. It replaces `NEXT-SESSION-BATCH2.md` and `NEXT-SESSION-UI.md`. The pipeline chat and the UI chat
(T367) are now merged, so this chat owns the whole tree: pipeline, worker, core, ui, ui-native, map. The old rule about
which chat edits which files no longer applies.

## Paste this to start the chat

> Read `handoff/PLAN/NEXT-SESSION-0.5.0.md`, then `handoff/PLAN/RELEASE-PLAN-0.5.0.md` and the STATUS.md top entry.
> We're one chat now (UI + pipeline). Carry on with the batches in order; start ticket work without asking, ask me only
> for decisions. Keep replies short and STATUS.md current.

## First: close T367
The owner called T367 done on 2026-11-04. Its last commit is d3577f2e3 (round 9). Do these four things:
1. Check `git status` for anything of T367's still uncommitted. If there is some, run its tests and commit it.
2. Set T367 to DONE with that hash, in the ticket's Status line and in the ledger (`tickets/README.md`).
   `node scripts/check-tickets.mjs` must pass.
3. Run the full app suite once (`cd app && npx vitest run`, unfiltered). Fix what's red. T367 (i) lists the reds that
   were open when the UI chat was still working.
4. Carry anything T367 left open into a new ticket. The SalmonCard caveat block, which comes from T372 part 2, belongs
   to T372 now.

## Then the work, in order (detail in RELEASE-PLAN-0.5.0.md)
1. **T372 part 2.** Part 1 is done (4a7cb4a1d): the map shows the drawn answer and never paints "check" for a DFO place
   that isn't on the map. Part 2, as the ticket plans it:
   - Python ships each undrawn place's caveat items, and whether each one is in force per day.
   - Core `caveats.ts` (`inPartOf` / `item()` / `areaOf`) is made generic over caveat items. Trout builds its items
     from `inpart`, salmon from the shipped places. There is one `CaveatVM` and one `CaveatBlock`, and SalmonCard draws
     it.
   - Remove `agrees` (salmon.ts ~664), `at1`'s downgrade (~818) and the `partial` VM.
   - Fix trout's "couldn't be placed" block, which gets no data (`item.unresolved` is licensing), or ticket it.
   - Owner rule: reuse the trout code so trout and salmon stay consistent, and keep all the logic in Python as the
     single source of truth.
2. **Batch 4:**
   - T376: `centreBasin`, a water's own basin, and basin names (`WATERSHED_GROUP_NAME`,
     `atlas.registry.basins.basin_name`). There is no fallback between the centre basin and the water's basin.
   - T347: weekday forecast labels go stale.
   - T348: map colours refresh when a new feed arrives.
3. **Batches 5, 6, 6c, 6d, 6e.** 6b (T367) is done.
4. **T360 loose ends:**
   - why each trace logs only one viewer;
   - the memory scenario's "Salmon" button step.
5. **T375 leftover:** measure the gauge feed size at ten days.
6. **Batch 7, the cut:**
   - promote r-ac5a4e4f734c (batch 3, held local because salmon/4 needs 0.5.0);
   - staging upload with `MIN_APP_VERSION=0.5.0`;
   - deploy the feeds worker (Claude's call, with the app, never ahead of it);
   - build the APK for Play internal and Closed testing.

## Rules in force (AGENTS.md has them all)
- Parser runs are human-only. Prod R2 and the prod deploy are human.
- Every heavy job goes through `scripts/heavy.sh`, one at a time (start gate ≤50%).
- Commit with `git commit -- <paths>`. Never `git stash`, never `git add -A`, never edit `marks.jsonl`, never
  pattern-kill processes.
- No silent fallbacks (T368). One load path per screen (T369). Ask before filtering data content. Everything added is
  a proposal.
- Text that comes from the data is shown as sourced. Rewording it needs the owner's yes.
- Prefix commands with `rtk`. Run gating suites unfiltered (rtk once hid failures).
