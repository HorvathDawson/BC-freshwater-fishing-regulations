# T287 — No freeze when a download finishes: updates apply in the background

Status: PARTIAL 2026-10-29 — a3cc1f269 (owner 2026-10-29: "after a download finishes the app freezes for a tiny bit. this feels really bad. it should
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
