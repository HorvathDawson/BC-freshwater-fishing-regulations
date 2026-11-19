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
