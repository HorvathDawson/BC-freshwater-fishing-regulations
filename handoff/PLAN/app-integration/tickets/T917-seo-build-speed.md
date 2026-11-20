# T917 — The website's page build (build-seo) is slow: about 15 minutes per site

Status: IN PROGRESS — round 1 built in worktree branch `worktree-agent-a72298791d841faf6` (full build 1,136 s → 470 s,
byte-identical); left (Claude): independent review + merge. Items 3-4 (rebuild only what changed, prod reusing staging) not
started.

Owner 2026-11-19 ~15:42 (main chat), watching the 0.8.3 production website build ("building the water pages, about 3,000
done so far"), verbatim: "this is so slow ... im sure we could speed it up make that a future ticket".

## Where it stands (0.8.3 builds, 2026-11-19)
`app/tools/build-seo.mjs` renders 20,340 pages (19,610 waters) for every site deploy, staging and production each. Staging
(site-all, log scratchpad site-083.log): first 1,000 water pages at 143 s, then ~39-45 s per 1,000, all done ~880 s.
So ~100 s of start-up, then ~40 ms a page on `--threads 4`. The render threads are capped at 4 (tools/seo/pane-worker.mjs,
T279); this Mac has 18 cores. Every page is rebuilt every time, even when the release and the app export are unchanged
for it.

## Baseline (measured 2026-11-19 on the 0.8.3 PRODUCTION web build, commit 32a56ab20, release r-6258ffded676)
Owner ~15:58: "could we add meassuring on this build?" Run alone on the machine (heavy lock), `--threads 4`, Node CPU
profiles at 10 ms (output unchanged; deployed to canifishthis.ca after). Script: `/usr/bin/time -l` per phase + a 5 s
CPU/RSS sampler of the process tree.
- **Phases:** expo export 8.6 s; web-shell 0.07 s; sourcemaps 0.09 s; **build-seo 1,055 s** (17.6 min; 20,340 pages,
  19,610 waters; 1.75 GB raw, 204 MB gzip, 165 MB br written).
- **CPU:** build-seo 3,499 s user + 85 s sys over 1,055 s wall = **3.4 cores of 18** (sampler median 408 %, max 568 %).
  Peak RSS 9.8 GB (sampler: ~6.7 GB steady), so ~1.6 GB per render thread.
- **Render threads (each ~890 s profiled, 4 alike):** **31 % synchronous zlib** (`processChunkSync`: each page's gzip +
  brotli compressed on the render thread), 13 % idle (waiting on the main thread), ~20 % react-native-web/React render
  (`createDOMProps` 7 %, `createElement` 4 %, renderWithHooks 2 %), `client.mjs get` 3 %, GC ~1.3 %.
- **Main thread:** 73 % idle, but **`sharePng` 119 s + `Resvg` 34 s = 153 s of share-image PNG rendering done serially
  on the main thread**, plus `ladder.ts at` 47 s.
- So, cheapest first: (a) compression off the render threads or cheaper (brotli quality / a compression pool, the same
  bytes if the quality is unchanged), (b) share PNGs on the threads (or cached by input hash: they rarely change),
  (c) more threads (8 at ~1.6 GB each fits), (d) start-up (~100 s before page 1,000).

## Work, in order, each measured (wall time of a full build, same release, nothing else heavy running)
0. **Profile one thread:** where a page's ~160 ms of thread time goes (runner start, data reads, React render, gzip,
   writes). Report before changing anything.
1. **More threads when the machine has room:** lift the cap of 4 (memory per thread measured first; heavy.sh lanes still
   respected). Expected: close to linear up to ~8.
2. **Start-up:** the ~100 s before the first page (vite-node runner per thread, release load): share or cache what can be.
3. **Only rebuild what changed:** a page's inputs (its water's data, the app export's script name, the template) hashed;
   an unchanged page is copied from the last build. Staging and production of the same commit + release then build once.
4. **Production reuses staging's pages** when the commit and release match, with only the host-specific parts (robots,
   canonical, X-Robots-Tag) rewritten, if 3 is not enough.

Output must stay byte-identical to today's (diff a full build before/after); seo vitest stays green.

## Round 1 (2026-11-19, worktree lane): 1,136 s → 470 s, every file byte-identical
Same inputs for every run: release r-6258ffded676, one copy of the staging web export (script index-d8a8c26d…),
`--date 2026-11-19 --previous r-86d43303bee2.json`, output in the lane's scratchpad; each run alone under
`scripts/heavy.sh`, `/usr/bin/time -l` plus a 5 s CPU/RSS sampler. Baseline = this branch's base commit fc09ec0c2.

| run | threads | wall | CPU (user+sys) | cores used | peak RSS / footprint | 1,000th page |
|---|---|---|---|---|---|---|
| baseline (fc09ec0c2) | 4 render | **1,136 s** | 3,748 + 89 s | 3.4 | 9.4 / 9.0 GB | 150 s |
| after (a)-(c) + verify overlapped | 10 render + 6 job | 567 s | 5,963 + 182 s | 10.8 | 13.7 / 16.4 GB | 129 s |
| **after, + ladder decode once** | 10 render + 6 job | **470 s** | 5,527 + 159 s | 12.0 | 13.6 / 15.0 GB | 58 s |

(The baseline's 1,136 s vs the 0.8.3 production run's 1,055 s: another export, and two single-core profiling runs of mine
overlapped it for ~4 min on an otherwise idle 18-core machine; its median CPU, 405 %, is the production run's 408 %.)

**Byte-identical proof:** `find . -type f | sort | shasum -a 256` over the whole output tree (59,647 files: every water
page's `.br` + `.gz`, every share PNG, hubs, sitemaps, CSS, budget.json, snapshot, workers): baseline vs each after run,
`diff` empty both times. The fixture build (`--fixture --app`, 305 files) is also identical at `--threads 1`, `4` and `auto`.

What changed:
- **(a) compression off the render threads.** A render thread now renders and checks the page and hands back its HTML;
  job threads (`tools/seo/jobs.ts` → `jobs-worker.mjs`, plain workers, no module runner) gzip 9 + brotli 11 and write
  the `.gz`/`.br`. The byte-making code is one file, `tools/seo/bytes.mjs` (`compressPage`, `drawCard`), used by the job
  threads and the in-process paths alike. A render thread is not given a page while more than 4 pages per job thread
  wait (bounded HTML held on the main thread).
- **(b) share PNGs on the job threads** (in parallel, a turn after each card as before; results kept in the order
  asked). The serial `drawSharePngs` stays for the in-process path (tests).
- **(c) thread count from cores and free memory** (`tools/seo/threads.ts`, tested): `--threads auto` (default) = as many
  render threads as the cores keep busy (18 cores → 10 render + 6 job), held to what the free memory pays for
  (`memory_pressure` on macOS, as heavy.sh reads it): 1.3 GB a render thread (measured: footprint 9.0 GB at 4, 15-16.4 GB
  at 10), 4.5 GB main, 10 % of RAM kept free; the result is printed (`seo: 10 render + 6 job threads (...)`).
  `--threads N` still works and is held the same way. The cap of 4 is gone. `deploy/staging/site-all.sh` and the
  production runbook (`runbooks/release-by-hand.md` step 3) now pass `--threads auto`.
- **(d) start-up: 150 s → 58 s to the 1,000th page.** Main-thread prep before the first page request was 61 s models +
  70 s `verifyAgainstApp` (measured with `--snapshot-only`: 132-140 s total). (1) the verify gate now runs while the
  render threads draw (site.ts; still gated before any page is awaited, its own error message unchanged); (2)
  `packages/core/src/regs/ladder.ts` `ladderAt` decodes a frame once per (frame, reasons, states) (WeakMap; settle.ts
  asked per segment AND day, the frame does not change with the day), shares one map between origins carrying the same
  wire verdict, and overlays by copy + set (same insertion order as `new Map([...base, ...own])`). `--snapshot-only`:
  140 s → 41 s (55 s after the review moved the cache to the inputs, below). Core + conformance suites green (3,713 + the 261 Python-parity tests).

Checks run: `vitest run tools/seo` (24 files, 169 tests), `tools/web-shell.test.ts`, `packages/core` + `conformance`,
`tsc -b`, `tsc -p tools/seo/tsconfig.json`, knip (memory_pressure added to ignoreBinaries), check-promises/deps/
boundaries/platform/anchors/tickets. `scripts/fast-fail.sh` fails in this worktree on 7 review marks NOT LANDED
(pipeline curation state, unrelated to this change).

**Review fixes (independent review of 7d85dd1bc, no blockers):**
- `threads.ts`: when the free memory fits no render thread the build is **refused** with the free memory and the budget
  named (was: ran 1 thread anyway). Tested.
- `jobs.ts` / `panePool.ts`: a thread that **exits** before `close()` without an 'error' event (process.exit, a native
  crash) rejects its job, every waiting job, later asks and the stylesheet ask: the build fails, never hangs.
  `jobs.test.ts` (dying stand-in threads, 5 s timeouts; mutation-checked: without the handler the test times out).
- **Ladder cache moved to the inputs:** `ladderAt` is pure again (shared maps + copy/set kept); the decode-once cache is
  now `settle.ts` `ladderOfSeg`, a WeakMap on the `RegsInputs` (beside settle's own MEMO), so in the app a decoded ladder
  goes when its inputs go instead of living as long as the store's never-emptied frame cache. Tests: every segment ×
  84 days equals a fresh decode and is the cached object; other inputs with other reason words decode their own;
  origins sharing a verdict share one map and every origin equals the old `new Map([...base, ...own])` in order.
  Cost: `--snapshot-only` 41 s → 55 s (still 140 s before T917): frames shared between waters are now decoded once per
  water. No full rerun (no output path changed); the 305-file fixture build is identical at `--threads 1` and `auto`.
- Checks: `vitest run tools/seo packages/core/src/regs` (73 files, 842 tests), `tsc -b`, seo typecheck, knip (clean with
  the repo's `.venv` present), check-tickets, check-promises.

Where the time goes now: the render phase is CPU-bound (~14-15 of 18 cores busy, ~19.5 s per 1,000 pages; a render
thread's per-page time rose from ~110 ms to ~200 ms under contention, so more threads will not help); start-up ~50 s
(module runners + models). Next, if wanted: a render-thread profile (react-native-web render ~20 % of a thread before),
then item 3 (hash a page's inputs, copy unchanged pages) — the biggest remaining win, since staging and production of
the same commit + release would build once.
