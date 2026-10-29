# T279 — Water pages stored pre-compressed; SEO build on several cores

Status: DONE (86947fe9f; owner 2026-10-29: "can we just store all the layouts like we have now but have them be pre compressed?").

Source: T269 made each /water page a snapshot of the app's pane in all three layouts (~75 KB raw, ~10 KB gzip; 19,936 pages
= 1.5 GB raw, 26.5 min build on one core).

## Work
1. Keep the three layouts. The build writes each page compressed (brotli + gzip), and the site serves the stored bytes with the
   right `Content-Encoding` by the request's Accept-Encoding (staging site worker; same design for production hosting).
   Uncompressed copies are not uploaded.
2. Run the page renders across worker threads (≤4, per heavy-jobs rule) so the build takes minutes, not half an hour.
3. Check the host's per-deploy file count and size limits with the pre-compressed files; report the headroom.

## Acceptance
Upload size and build time before/after; a staging page loads with `content-encoding: br` and renders identically; the T196
budgets still pass.

Depends: T269. Size: S. Owner: no (production hosting is the owner's).

## Outcome (2026-10-29)
- **Stored compressed only:** each water page is written as `index.html.br` (brotli 11) + `index.html.gz` (gzip 9), never raw
  (`tools/seo/compressed.ts`). The site worker (`legacy.ts` `legacyWorker`, generated as `_worker.js`; staging
  `run_worker_first = ["/waterbody/*", "/water/*"]`) answers `/water/<slug>` with the stored bytes by Accept-Encoding (br, else
  gzip, else inflated gzip), `Vary: Accept-Encoding`, `/water/<slug>/` → 301, a missing slug → the 404 page. Production hosting
  needs the same `run_worker_first` (owner). Hub pages stay plain (the host compresses them).
- **Render threads:** `--threads` (default 4, capped at 4) — `panePool.ts` hands each page to the next free thread
  (`pane-worker.mjs` → `paneWorker.ts`, its own vite-node runner from `runner.mjs`), which renders, checks (`checkWaterPage`)
  and writes it; theme variables and per-layout classes are named by content hash so every thread names them alike, and the
  threads' react-native-web sheets merge by group into one stylesheet (`pane.tsx` `paneCss`). Tests/fixture without `--app`
  render in-process, one at a time.
- **Numbers (r-62e2f3a8c81e, 19,936 pages / 19,643 waters):**

  | | T269 (1 core, raw) | T279 (4 threads, br+gz) |
  |---|---|---|
  | build | 26.5 min | 17.0 min (1,019 s; 3,234 s CPU; peak RSS 5.6 GB) |
  | water pages on disk | 1.48 GB raw | 176 MB gz + 143 MB br (raw 1.48 GB never written) |
  | whole SEO dist | 1.5 GB | 473 MB |
  | files deployed | 39,738 | 59,381 (2 per water page) |
  | site.sh (upload + deploy + smoke) | — | 4 min 07 s |

  The main thread's model + hubs (~100 s) and brotli 11 bound the speed-up.
- **Host headroom:** Workers static assets allow 100,000 files per version (paid plan; 20,000 free) and 25 MiB per file:
  59,381 used → ~40,600 files spare; the largest file is a 10.9 KB page.
- **Staging** version 11996b77: `curl -H 'Accept-Encoding: br, gzip'` → `content-encoding: br`, `x-robots-tag: noindex`,
  `cache-control: public, max-age=300, must-revalidate`; gzip-only → gzip; identity → 92,934 B plain HTML. Cloudflare's edge
  re-encodes on the way out (10.3 KB br on the wire vs 7.6 KB stored). Page vs tap: phone 0.53 %, desktop 0.20 % pixels (the
  year bars' hatch, as in T269). Smoke 18/0/0. Budgets (T196): largest 10,900 B gzip, p95 9,733 B, pane CSS 4,686 B, critical
  284 B.
