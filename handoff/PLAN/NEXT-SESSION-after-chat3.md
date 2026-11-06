# After chat 3 (lag, smoothing, 0.6.5): start here

Written 2026-11-06, as chat 3 closed. It replaces `NEXT-SESSION-after-chat2.md`. The owner approved this plan.

## Paste this to start the chat

> Read `handoff/PLAN/NEXT-SESSION-after-chat3.md` and the STATUS.md top entries. Do the list in order. Start ticket
> work without asking; ask me only for decisions. Keep replies short and STATUS.md current.

## Where chat 3 left things
- **Data:** release **r-67b456b5e64b** (T419 smoothed streams, owner chose variant L) is LATEST and on staging. Smoke
  18/18; `test/` re-pointed. Prod is still the owner's step (T404).
- **App:** **0.6.5 (versionCode 14)** on Play internal. The fresh-install emulator walk passed 5/5 (install, walkthrough,
  regs-open, water-nav, pan; `app/tools/testbed/out/walk-065/`). 0.6.5 = T420 lag fixes (map colouring re-watch, binding
  dedupe), T440 first-run notice readable by screen readers, T390 "new version" only once Play offers it.
- **Staging site:** rebuilt with the web pixel-ratio cap (≤2); smoke 18/18.
- **Lag:** zoomed in is "way better" (owner). Zoomed out (z5–z9) is still laggy; the owner suspects parks and other
  polygons. A lag agent was measuring this when chat 3 closed: **read T420 for its findings and the next step**
  (any tiles change = tile build through heavy.sh, then a release; anything that changes what shows is a proposal).
- **AGENTS.md** gained 34a (web/native split by file), 45 (when to compact vs start a new chat), 46 (the 11-05 audit
  guards stay), absolutes 5–6. Read them.

## The list, in order
1. **Zoomed-out lag (T420, "Zoomed out, z5–z9")**: the cost is the basemap's green landuse fills, not our parks.
   a893b06fe (antialias outline off on basemap fills, −10–13 % frame) is committed but **not shipped**. It needs a
   staging site rebuild and the next APK. Ask the owner about the three look changes in T420, with a before/after image
   for each: harder basemap landuse simplification at z≤9 (tile build), landuse_park from z8, dropping the region/MU 4 %
   tints. Then T448.
2. **T448** native pixel-ratio cap (web already capped).
3. **T401** prefetch (`setPrefetchZoomDelta`).
4. **T420 iOS half** — the binding patch is not compiled on iOS yet.
5. **Crash/report tickets:** T444 (water map: part 0 of gnis:3062 not in its group), T445 (MapLibre "resolved to null"),
   T446 (first install "does not match the manifest"), T443 (Fraser step camera jump).
6. **T441** one-step fixture re-cut after a release (today it is three commands; see rule 46).
7. **T449** DFO places wholly in tidal water get a tidal state (Nanaimo below Cedar Rd bridge).
8. **T164** salmon proposals (data).
9. **T250** iOS simulator + web walk; **T351**.

## Owner items (not Claude's)
- T371 Play Closed testing (Console steps, same build) · T404 prod release/deploy · Pixel 9 check of 0.6.5 and the
  staging site on phone Chrome.

## On hold
- T389 (unnamed streams skip the big download; needs an atlas rebuild) — postponed, not dropped. Sloughs (T322/T55).
- T447 artist icon — postponed; the current icon ships.

## Notes
- After any release, re-cut every release-backed fixture (rule 46 / T441) before `pnpm check`.
- The 11-06 check had one load flake (Walkthrough test, emulator running beside it); fixed in c35b0866e.
- Ticket numbers: take highest + 1 right before writing; `node app/tools/check-tickets.mjs` must pass.
- Never commit `data/generated/review/pfma_basin_areas.json`, `data/generated/review/write-through.jsonl`, store images.
