# After chat 4 (0.6.6, crash fixes, one-step re-cut): start here

Written 2026-11-06, as chat 4 closed. It replaces `NEXT-SESSION-after-chat3.md`.

## Paste this to start the chat

> Read `handoff/PLAN/START-HERE.md`, the restart page it names, and the STATUS.md top entries. Do the list in order.
> Start ticket work without asking; ask me only for decisions. Keep replies short and STATUS.md current.

## Where chat 4 left things
- **Data:** release r-67b456b5e64b is LATEST and on staging (unchanged this chat). Prod is the owner's step (T404).
- **App:** **0.6.6 (versionCode 15)** on Play internal; fresh-install emulator walk 5/5 (`app/tools/testbed/out/walk-066/`).
  0.6.6 = T420 basemap fills without the antialias pass (zoomed-out lag, owner approved), T448 native pixel cap ≤ 2
  (Android: smaller surface, same scale and taps), T443 water-map steps (parts framed ≤ z12, a step zooms in ≤ 2
  levels), T444 (stale group on a frame move), T445 (calls that outlived their view not reported).
- **Staging site:** rebuilt on 0.6.6, smoke 18/18.
- **Measured, not shipped:** T401 prefetch delta (prop exists, default kept: gain inside noise). iOS half of the binding
  patch compiles and runs in the simulator (T420).
- **T441:** `node app/tools/regs-recut/sync.mjs` re-cuts every release fixture in one step (section-lines.json now
  written from the release). Left: raw section ids in a few tests.
- **T449 in review:** two Nanaimo DFO places proposed as tidal water; owner accepts in the review app (OWNER-TODO).

## The list, in order
1. **Owner feedback on 0.6.6** (zoomed-out feel, softness, Fraser steps) → act on it. If streams still pop in while
   panning: a delta-2 APK side by side on the phone (T401).
2. **T351** native foreground crash (read the staging worker's `/crashes` for anything new since 0.6.6 first).
3. **T446** reopen only if a new report arrives (it now says short file vs other bytes).
4. **T441 Work 2:** tests name sections by water + part + index instead of raw ids.
5. **T164** after the owner's DFO review: the reach-only rerun + binding diff (memory reach-only-rerun).
6. iOS pixel cap (T448 iOS) if the owner wants it.

## Owner items (not Claude's)
- Phone checks of 0.6.6 · T449's two clicks · T164's 177 DFO proposals · T371 Closed testing · T404 prod.

## Notes
- After any release: `node app/tools/regs-recut/sync.mjs`, then `pnpm check` (rule 46).
- Two pipeline "live" tests still read the retired golden.json at collection (`test_answers_keep_answers_live.py`,
  `test_answers_pipeline_needs_live.py`): seen this chat, not fixed — filed as T450.
- Never commit `data/generated/review/pfma_basin_areas.json`, `data/generated/review/write-through.jsonl`.
