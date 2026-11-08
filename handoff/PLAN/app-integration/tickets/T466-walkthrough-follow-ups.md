# T466 — Walkthrough follow-ups from the owner's 0.6.8 phone check

Status: IN REVIEW — bc938e624 (owner phone check; the new steps' words are Claude's draft)

Owner: "the walkthrough generally is pretty good but for dams and obstacles it never scrolled down on the menu to show
it, and the spots tutorial seemed rushed or didn't really show how to create a spot. Also how would it work when salmon
regs are added and they aren't closed anymore? Maybe it could navigate sections up to an ecological zone that is
always closed?"

## Work
1. **Dams and obstacles:** the step about them must scroll the Layers sheet until the control is on screen before it
   rings it (T456's rule: a step never refers to a control that is not laid out and ringed). Add a test for a ring
   target inside a scrolled sheet, at phone size.
2. **Spots:** walk the real creation. Long-press (or the add button) → place the pin → name it → save → it appears on the
   map and in Spots, each step done by the reader (DO-steps). "Show me" does each step visibly. Clean up the example spot
   at the end, or ask whether to keep it. Today the capture step rings nothing (T456 report).
3. **A closed example that stays closed:** the "a closed fish / closed water" step must not depend on a season that
   changes (salmon rules arrive and open). Pick the example from the data: a part closed all year (`part.closed_all_year`,
   e.g. an Ecological Reserve such as the Skeena's), reached the way a reader would get there (open the water, step ▲ to
   the reserve's part). Chosen by stream key + start, never a raw id (rule 46). If the release has none in the walk's
   area, the step is skipped and SAID, not faked.
4. Emulator walk with shots of each changed step; a short recording to the owner.
Related: T456, T293, T454 (photos), T460 (steps). Size: M.

## Done (bc938e624) — owner phone check left
1. Sheet bodies give the walk a scroller (`coachScroll.ts`); a ring laid out past the window is scrolled to first. Tests: a ring
   in a scrolled sheet at phone size; LayersSheet registers Falls and dams with its page. Emulator: step 27 ringed in view.
2. Spots, 10 DO steps with Show me: Spots → Pick on the map → pin → Continue → water → day → name → Save → Show on map →
   Spots → the spot. The example spot is removed on reaching the last card, which says so (or that it could not be).
   A Skip before the end keeps the spot (it is the reader's).
3. `GUIDE_CLOSED`: Chilliwack River, run 380887781 from 23325 m (100 m below Slesse Creek → Ecological Reserve boundary),
   checked `closed_all_year`; reached from the Vedder's Map with ▲ ×3, then Show regulations. None in a release: the step
   says so, the rest are passed. The salmon step no longer says "a closed fish". `WALKTHROUGH_VERSION` → walkthrough-5.
4. Test bed `walkthrough`: 41/41 PASS, no card over its ring, example spot gone from Spots. Shots: `app/tools/testbed/out/t466/`.
