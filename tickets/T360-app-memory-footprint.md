# T360 — App memory footprint

Status: PARTIAL 2026-11-09 — 25a6b7d54, 17d5f0dc3, fe0f4e659, 2bdcc1931, 84ef47b (peak 379 MB on the build-21-era APK; allocator decay A/B unfinished, held out of build 22); left (Claude): the decay A/B rerun (t360-nodecay) and the real-phone GPU memory + Vulkan/OpenGL check

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T360 | App memory footprint: measure Android RSS by screen/feature, find what grows past ~400 MB (T351 crashes at 406/435 MB) | PARTIAL | 25a6b7d54, 17d5f0dc3, fe0f4e659, 2bdcc1931, 84ef47b | 11-09: idle/replaced SQLite closed, per-source panel cache, status index capped, rule-book copy reused; peak 379 MB (old APK) / 314 MB (HEAD+decay, not clean A/B); left: decay A/B … (detail: ticket file) |

Owner 2026-11-03: "could we look into the memory footprint of the app". Evidence: the two 0.4.0 foreground native
crashes (T351) happened at 406 MB and 435 MB in use (one SIGSEGV); background LOW_MEMORY kills at 169 MB (0.3.0, 0.4.0).
Merged in (ticket audit 2026-11-05, owner): the leftovers of T245, T287, T317 — read each file's open items and finish them here; each closes pointing here.

## Done
- Trim (17d5f0dc3): `modules/memory-trim` (Android) answers `onTrimMemory` (RUNNING_MODERATE and up): `onLowMemory()` on
  every MapLibre MapView + M_PURGE_ALL (`mallopt` looked up with `dlsym`, API 26+); JS (`src/memoryTrim.ts`) shrinks
  every SQLite connection (`ReleaseLoader.trim`, `PRAGMA shrink_memory`). iOS: same shrink on `memoryWarning`. A CRITICAL trim frees ~16–40 MB.
- PDF leak: react-native-pdf patch skips a duplicate `drawPdf` of the same path + page (`drawnKey`), `PdfViewer.native.tsx`
  memoizes `source`; pdfium held after open+close 2.5 / 0.9 MB (was 50–77 MB). Gauges-off dots bug fixed (02433a339).
- T287 `adoptDecoded`: an applied update keeps one copy of unchanged files (ext was 50 → 148 MB at the swap).
- Salmon run table 20 → 8 bytes a run: 15.8 → 6.3 MB, 0 differences on r-5a5999c94588; a run or chain over 16 bits is
  refused on decode. Owner 11-05 ("16mb seems like a lot … easy wins"): 6.3 MB is fine — salmon stays decoded at map load.
- 84ef47b: one SQLite handle per file, `closeIdle(ms)` (trim closes those idle 60 s; an update closes the replaced set's
  ~14); panel cache per `WaterSource` (WeakMap); `statusOf` holds the 2 newest keys; the rule book reuses `synopsis-open.pdf`.
- Checked, not changed: MapLibre's ambient cache is DISK (no RAM win); salmonMemo is tiny.

## Where it stands
- Native heap (map + SQLite + Hermes) is the whole story; Java is small. Conditions/Salmon are the peak. 2026-11-08 gate 2:
  Conditions 396 (native 217), after CRITICAL trim 364 MB; no growth across 32 tab switches.
- Conditions' ~100 MB native over a water page: heapprofd sees ~28 MB live growth (Hermes ~12, MapLibre ~11), JS externals
  +11 MB; the rest is freed MapLibre memory the allocator keeps. No app-side fix found that is not a visual/behaviour change.
- OpenGL vs Vulkan: 5–25 MB apart on the emulator (Graphics reads 0 there: host GPU). `nativeVariant: "vulkan"`
  (apps/mobile/app.json) was PROVISIONAL from the N1 spike, never judged on a phone; the SIGSEGV makes OpenGL the first try.
- Round 7 (runs `app/tools/testbed/out/runs/t360-{base3,decay2}`): base3 (t512lean APK) peak Salmon **379** / 206 native;
  decay2 (HEAD 229c464de + `mallopt(M_DECAY_TIME, 1)`) **314** / 164. Not a clean A/B: decay2 skipped the rule book
  (~33 MB) and is a newer app; net ~−10 MB at Conditions, ~−30 MB at Salmon/after-trim, inside noise for one sample.

## Work (left)
1. Decay A/B: requeue `t360-nodecay` (same worktree, decay off). Patch: `app/tools/testbed/out/t360/t360-decay.patch`
   (also worktree `../bcfr-t360`); NOT committed, NOT in build 22.
2. Real phone: GPU memory, and Vulkan → OpenGL on that phone (compare memory and N1's pan budget, p95 ≤ ~20 ms).
3. Test-bed: on HEAD a search result opens the water on its MAP view, so the memory scenario's rule-book step needs a
   tap on Regulations first; the Salmon step taps `regs-paint-salmon` (the fishery bar shows only when salmon is offered);
   check "one viewer per trace" (the second PDF open's shot + its `drawPdf` lines). Build test-bed APKs from a clean worktree.
4. (3) of round 7: the Cultus Map view said "The parts of this water are not available" (HEAD 229c464de) — for the T517 lane to check.

Tools: `run.mjs memory` / `run.mjs heapprof` (`heapprof.py`), `[heap]` probe in test-bed builds (apps/mobile/index.js),
`session.sh --opengl`; emulator `n1_pixel`, heavy jobs via `scripts/heavy.sh`.

## Acceptance
Keep decay only if decay2 beats t360-nodecay by more than noise and pan frame time is unchanged; real-phone numbers and
the Vulkan/OpenGL result recorded here.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
11-09: idle/replaced SQLite closed, per-source panel cache, status index capped, rule-book copy reused; peak 379 MB (old APK) / 314 MB (HEAD+decay, not clean A/B); left: decay A/B rerun (t360-nodecay), real-phone GPU + Vulkan/OpenGL
