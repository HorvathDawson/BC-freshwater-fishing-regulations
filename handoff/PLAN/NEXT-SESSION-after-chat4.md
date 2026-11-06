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

## Owner feedback on 0.6.6 (late 11-06) and what chat 4 did with it
- Pixel 9: zoomed in and out now feel alike; a flick still STUTTERS a little during the glide.
- Switching to Map (from Conditions / Runs) sometimes FLICKERS the streams (not the basemap): colours should be ready
  before the switch.
- T443: the small Fraser part was unreadable at z12 → fixed (55c7f54: a part is never framed under half the screen,
  up to z14). NOT in an APK yet.
- T449: both Nanaimo places accepted (owner, in chat); marks + place file committed.
- Section ids typed into tests: owner says they should not be there (T441 Work 2).
- **T441 DONE** (23c9aed6a, d03aab79e): tests name sections by stream key + start (`sectionNames` in
  `@app/data/release/node`); no raw ids left.
- **T451 DONE** (e63c921c3): the map-switch flicker — each tab switch handed MapLibre two styles (new modes, then
  the values a render later); now one style per switch (32 switches: 0 stray frames, was 21).
- **T452 IN REVIEW** (e63c921c3): the fling stutter — MapLibre's fling set the camera off the screen's frame timing;
  the binding now runs its own frame-timed glide (gaps > 20 ms 51 → 0; starts at finger speed). It FEELS different:
  ~1.25 s, softer ease-out (was 0.67 s) — a proposal for the owner; length is `platformFlingDp`. Not yet checked: the
  camera reported at the glide's end (rerun the test bed's `feel` scenario).

## The list, in order
1. **Next APK (0.6.7)** with T443's floor, T451, T452: first rerun the test bed `feel` scenario (T452's unchecked end
   camera), then `pnpm check` once, `deploy/staging/build-apk.sh --play`, fresh-install walk (memory: walk = session.sh
   --build … run.mjs install pan regs-open walkthrough water-nav). Ask the owner about T452's glide feel on the phone.
2. Rebuild the staging site for the web side of T443 (export:web:staging → build-seo → site.sh).
3. If streams still pop in while panning: a delta-2 APK side by side on the phone (T401).
4. **T351** native foreground crash (read the staging worker's `/crashes` for anything new since 0.6.6 first).
5. **T446** reopen only if a new report arrives (it now says short file vs other bytes).
6. **T164** after the owner's DFO review: the reach-only rerun + binding diff (memory reach-only-rerun).
7. iOS pixel cap (T448 iOS) if the owner wants it.

## Owner items (not Claude's)
- Phone check of the next APK · T164's 177 DFO proposals · T371 Closed testing · T404 prod.

## Notes
- After any release: `node app/tools/regs-recut/sync.mjs`, then `pnpm check` (rule 46).
- Two pipeline "live" tests still read the retired golden.json at collection (`test_answers_keep_answers_live.py`,
  `test_answers_pipeline_needs_live.py`): seen this chat, not fixed — filed as T450.
- Never commit `data/generated/review/pfma_basin_areas.json`, `data/generated/review/write-through.jsonl`.
