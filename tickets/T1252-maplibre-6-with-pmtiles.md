# T1252 — maplibre-gl 6 on the web map, with the pmtiles that works with it

Status: IN PROGRESS — lane branch `chore/T1252-maplibre-6-with-pmtiles` (committed locally, not pushed); left: the
planning chat's pre-PR check, a second agent's review, the PR; the review tool's own bump (#317) is a second lane.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1252 | maplibre-gl 6 on the web map, with the pmtiles that works with it | IN PROGRESS | ecba9af15 | left (Claude): before_pr.sh + second review + PR (supersedes Dependabot #355); the review tool's frontend (#317) is a second lane |

Source: Dependabot #355 (maplibre-gl 5.24.0 → 6.4.1 in /app, red on `gates` and `vitest (3/4)`: tools/gl-internals.test.ts)
and #317 (the same in tools/curation-review/frontend). Owner, 2026-11-27: "do upgrade now just make sure maplibre isnt
causing issues sicne we have patched version". Number from the planning chat.

## Work

1. maplibre-gl 5.24 → 6.19.0 in `packages/map` (6.20.0 was hours old); pmtiles stays 4.5.0 (latest), whose protocol is
   GL 6's `addProtocol` signature unchanged.
2. The "pmtiles fails silently on 6" of deps.md was GL 6's WORKER: ES modules only, the worker is its own file found by
   `import.meta.url`, which inside Metro's bundle 404s (6.19 now says "Worker failed to load"). `apps/mobile/gl-worker.js`
   copies the worker (and whatever it imports) into `public/maplibre-gl-<version>/` when Metro starts; `setWorkerUrl`
   names it (`packages/map/src/web/glWorker.ts`).
3. GL 6's breaking list against our use: `styleimagemissing` is notify-only → `setMissingStyleImageResolver`; the
   no-WebGL error is now `GPUInitializationError` → `isNoWebGL` / `noWebGLError` (T1005's words and crash class kept);
   typed `error` event and paint names (two casts); `dist/maplibre-gl.js` (UMD) is gone → the two shot tools load the
   `.mjs`. Namespace imports, no `map.transform`, no `setData` second argument, no hash, no `instanceof` on events.
4. `tools/gl-internals.test.ts` re-read on 6.19: same private names and order; the bundle is `.mjs`, the tile event a
   `MapSourceDataEvent`. A new pair of tests pins the worker's path and its imports.
5. The colours gate reads everything under `apps/`: it skips the copied worker folder only (GL's minified debug colours),
   pinned by a test with the near misses that are still read.

## Acceptance

`pnpm -C app check` green. In a browser (local tiles, dev server and a production export): the worker loads, each archive's
header then tile range requests (206) follow, regulation lines draw, the colouring takes the private route (`tile`
events), a tap opens the water, wheel / drag / pinch / double-click move the camera with no rotation, the console holds
only the errors main has too. Before/after shots of the same three views.

Depends: none. Size: M. Owner: no.
