# T287 — No freeze when a download finishes: updates apply in the background

Status: MERGED into T360 (2026-11-05 audit) — was: PARTIAL 2026-11-05 — a3cc1f269, b3ca664cf, f1e2d5abc, 2bdcc1931 (update apply: no stall left; first-install mount not re-measured)
not freeze. it should update in background cleanly").

## Work
1. Reproduce on the testbed emulator: an update (and a region/offline download) finishing while the user pans/taps; record
   JS-thread long tasks and dropped frames around the finish (T273 pan probe).
2. Find what blocks (likely: hashing/verify or gunzip on the JS thread, opening/attaching the new SQLite files, swapping the
   store and re-rendering every consumer at once, map source reload, big JSON parse).
3. Move the work off the JS thread (native hash/unzip, chunked/yielding work, worker thread), prepare the new release fully in
   the background, then swap atomically at an idle moment; only the components whose data changed re-render; the map keeps
   its camera and doesn't flash.

## Acceptance
No JS long task > 50 ms and no dropped-frame burst at download finish on the emulator (before/after numbers); same for the
first-install finish (T272 path).

Depends: T272, T273. Size: M. Owner: no.

## Measured + first fixes (2026-10-29)
Test bed `run.mjs update-freeze`: a new release on the test channel (same files, new id: the phone copies and
re-checks every file it holds), "Check now", back to the map and pan 40 s while it stages; then the map left still.
The test-bed build logs every JS stall over 50 ms (`[stall]`, apps/mobile index.js; background time excluded) and times
the decodes (`[work]`) and the swap (`[swap]`), so each stall names its neighbour.

| | stalls > 50 ms while panning through the finish | worst | then, map still |
|---|---|---|---|
| before | 4 (146, 76, 178, 366 ms) / 15 with T286's first prewarm (ten ~125 ms salmon-slice decodes) | 366–424 ms | — |
| after | **0** | — | 3 (178, 168, 60 ms) |

What froze was NOT the hashing on this path: it was the SWAP landing mid-pan the moment the download finished (the
Shell applied at the first "safe" moment, i.e. a browse tab): the new set's paint decoded (`[work] decode paint 156 ms`),
every screen re-keyed onto the new loader (re-render, method / salmon decodes), then T286's prewarm (rules all at once,
salmon slices). Fixes:
- The swap waits for the map to have stood still 3 s (`APPLY_STILL_MS`, Shell; a pan restarts the wait); back from
  the background stays the other moment.
- T286's prewarm no longer decodes salmon slices; the rules are inflated 250 at a time with a turn between.
- Release files are hashed and inflated a slice at a time (`sha256HexAsync`, `ungzipAsync`, `verifyAsync`: 256 KB, a
  macrotask between; install's copy-check, resume check and re-verify, and every `fetchVerified`). Same answers
  (tests); `EXPO_PUBLIC_T287_BEFORE=1` is the one-shot "before" build.

Left (why PARTIAL): the swap's own work still holds the JS thread 60–180 ms, now only while the map is still —
decodePaint in one go (156 ms) and the re-render onto the new loader; a sliced paint decode / preparing the new set's
decodes before the swap would remove it. The first-install finish (T272 path) and a region/offline download finishing
were not measured here.

## Swap work cut (2026-10-30, b3ca664cf, f1e2d5abc)
- **Decodes in slices.** paint (with its status index), method and each salmon slice are generators (`@app/core`
  `sliced.ts`: `yield` every 2048 rows; `runSliced` runs 8 ms at a time with a turn between, a MessageChannel on the
  web, a 0 ms timer on Hermes); the loader uses them (`decodePaintAsync`, `decodeMethodAsync`, `decodeSalmonShardAsync`;
  the `EXPO_PUBLIC_T287_BEFORE` build still decodes in one go). Same answers (`sliced.test.ts`).
- **Warmed before the commit.** An applied update decodes, on the new set and before it replaces the old one, every file
  the set on screen had decoded (`warmLike`: paint, method, the salmon slices read, lakes, access, flow-area), so the new
  set lands with nothing left to decode (`ui/boot.ts`).
- Measured (test bed `update-freeze`, `runs/2026-10-30T07-51-09`; same-files update):

| | panning through the finish | map still, at the swap |
|---|---|---|
| before (a3cc1f269) | 0 | 138 (paint decode, one go) · 247 · 79 ms |
| after | 1 × 65 ms | **124 · 152 ms**; every decode now sliced (paint 448 ms wall, method 158, salmon-6 163 — no stall) |

- **First install (T272/T298 path), measured for the first time** (`install`, `runs/` 2026-10-30 08:3x, `T287-first-install.txt`):
  installed → map: paint sliced (402 ms wall, no stall), then 66 · 62 · **275 ms** — the 275 is the app's first mount
  of the map screen (Shell + colouring), not a decode.
- **Left (why still PARTIAL):** the swap's commit re-renders every screen on the new loader in one React commit
  (~124 ms) and the map then relabels/rebuilds its style for the new status/method/salmon values (~150 ms: MapLibre
  "source in use" warnings = a style swap; map package). The same first-mount cost is the 275 ms at a first install.
  Next steps: hand the map the same feed objects when a file's sha256 is unchanged (no style swap for unchanged data),
  and split the commit (map colouring first, the rest a turn later).
- Test bed: `install` now records the first-install finish (`T287-first-install.txt`, note `install finish → map`) and
  dismisses T303's locate hint ("Got it") — it covered the map and failed `install` / smoke taps.

## The commit's map work removed (2026-11-05, 2bdcc1931; test bed `update-freeze`, runs t287-base / t312-b)
Cause read off the log: at the commit every map input went BLANK for a frame and came back — the pairing (new release
key → "not proven"), the status index, lake / method / salmon facts (new source → loading) — so the map cleared, swapped
its style twice and relabelled everything; and every salmon slice was decoded again (Promise.all, a 69 ms stall) and the
salmon facts rebuilt (157 ms).
- `adoptDecoded` (data/release/loader.ts, called by `warmLike`): a file the new set repeats (same sha256, same handles)
  is handed over decoded — the same objects (paint, method, lakes, access, flow-area, salmon head + slices, facts memo).
- The pairing of a manifest that names its tiles is answered in the render the release changes (`usePairing` peek).
- Status index, lake / method / salmon facts: a new source in the same section numbering (`WaterSource.handles`) is a
  REFRESH (`useAsync` sameAs) — the old answer stays until the new one is in; failures still reported.
- The native style keys on its values' CONTENT (Map.native.tsx `valuesKey`, d91f300f6): equal values, no style swap.
- Tests: loader.test (adoption), release.render.test (no blank frame), adapter-contract (no style swap).

| same-files update (test channel) | while panning | map still, at the swap | apply → committed |
|---|---|---|---|
| before (t287-base) | 1 × 64 ms | **69 · 157 · 162 ms** | 1139 ms |
| after (t312-b) | 1 × 63 ms (before the swap) | **0** | 586 ms |

Left (why PARTIAL): a release whose paint/salmon files DID change still relabels once (necessary; no blank frame, one
style swap at most) — the test channel publishes the same files, so that case is unmeasured; the first-install first map
mount (275 ms, 2026-10-30) was not re-measured.
