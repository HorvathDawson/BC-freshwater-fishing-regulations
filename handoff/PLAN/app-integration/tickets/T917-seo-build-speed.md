# T917 — The website's page build (build-seo) is slow: about 15 minutes per site

Status: OPEN (after 0.8.3; owner 2026-11-19 ~15:42).

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
