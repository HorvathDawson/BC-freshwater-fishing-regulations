# T186 — Web prefetch after first paint (Q164) and the icon set's chevrons in place of typed glyphs (Q162)

Status: DONE (d54842ec, b8b5b357, 0a129a14) — owner decisions 2026-10-24 (decisions.md Q164, Q162).

Strict check (`REGS_STORE_REQUIRED=1 pnpm -C app check`, through heavy.sh, unfiltered): green against
r-bb4331929428 (the release the walk was cut on: 373 files / 8,208 tests passed, 27 skipped). Against the main
checkout's newest release r-0ed3c2b2a0a7 (written by another lane during this work) the release-pinned conformance
suites fail — answers f66888bef6a2 vs the oracles' a5d3c00ae099, moved akeys and handles: re-run
`app/design/regs-v11/port/sync.mjs` and re-cut the walk pin on that release; nothing in this ticket touches them.

## Q164 — the web prefetch

The rule as built (`app/packages/data/src/release/prefetch.ts` `prefetchPlan`, pure; `@app/ui` `useReleasePrefetch`):

- WEB ONLY: App passes `prefetch` on `Platform.OS === "web"`; a phone holds the release installed.
- After first paint — the web map's first `idle`, which reports the region under its centre (`MapProps.onRegion`, the
  `region` layer's `region_id` at the centre; the native map accepts the prop and never calls it):
  - ALWAYS the regs file of that region, and again for each new region the view settles in. Bytes only
    (`ReleaseLoader.prefetch`: the source's own fetch into the release cache, not opened, so no sql.js for it).
  - `core` (opened, which loads the sql.js engine / sql-wasm with it) once `requestIdleCallback` fires (no rIC, e.g.
    Safari: 2 s after first paint), unless `navigator.connection.saveData` or `effectiveType` is `slow-2g`/`2g`/`3g`.
- Through the release loader, so the later open reads the SAME cached fetch (Cache Storage by immutable url, else
  memory): nothing is downloaded twice. Each prefetch sets a `release-prefetch:*` performance mark first.

### The walk (`app/tools/request-walk.mjs`, pin re-cut on r-bb4331929428 via the owner's :39217, app on a dev server)

First paint is still only what painting needs; the prefetch is a moment of its own (`prefetch after paint`: release
files and the wasm sent at or after the app's first prefetch mark).

| moment | before (T129 pin) | after |
|---|---|---|
| first paint | manifest, paint, mask, feed:index | same |
| prefetch after paint | — | regs-2, core, wasm (1,345.9 kB + 658.7 kB wasm) |
| map tap: a named water (R2) | access, core, regs-2, wasm, water | **access, water** |
| map tap: an unnamed stream (R2) | access, conditions, core, regs-2, regs-sid, wasm, water | access, conditions, regs-sid, water |
| first search | core, near, wasm | near |
| open Conditions | conditions, core, flow-area, wasm | conditions, flow-area |
| search: show the Thompson (R3) / open it | water / regs-3 | water + regs-3 (the view moved into R3) / nothing |
| tap named, Save-Data (new walk) | — | prefetch regs-2 only; the tap fetches core, wasm, water, access |

The first tap fetches no file the prefetch took (core, wasm, the region's file): zero re-fetches. It still fetches
`water` (2.74 MB) and `access` — the owner's rule prefetches core + wasm, not water (T129 proposal 2 is the way to
shrink that).

### Gates and tests

- `release-budget.test.ts`: each WEB_BUDGET gate prices its moment's files plus what the prefetch already took of them
  (the cold cost a Save-Data visitor pays); first paint is still exactly manifest/paint/mask/feed index in every walk;
  every prefetch moment is regs-2/core/wasm (Save-Data: regs-2); no moment after a prefetch fetches a prefetched file;
  live: a prefetched regs-2 and core are read from the cache by the open (one fetch, water).
- `packages/data/src/release/prefetch.test.ts`: the decision (region, idle, Save-Data, slow-2g/2g/3g, unknown speed),
  the tile region word, one download per file through the real web source and cache, a failed prefetch not kept.
- `packages/ui/src/prefetch.render.test.tsx`: nothing before the first region report; region file at once, core on
  idle; Save-Data / 3g / slow-2g: region file only; a phone gets no callback.

## Q162 — chevrons

`WORDS.DATE_CARET` (" ▾") is removed: the date pill draws `<Icon name="expand">` after the day word.
`WATER_MAP_WORDS` up/down/upInto/downInto are "Upstream" / "Downstream" / "Upstream into" / "Downstream into": the
water map's step button draws `collapse` (up) / `expand` (down) beside the words, none on a disabled end. The
one-icon-set gate's core-word allowance is gone: a test refuses a triangle or caret in any `WORDS` /
`WATER_MAP_WORDS` value (mutation-pinned). Render tests updated (text loses the glyph, nothing else).
