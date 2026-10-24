# The app, as it is (map)

The one current map of `app/` (T63, checked against the code 2026-10-24). Why the layers are shaped this way is
[`README.md`](README.md); the binding app rules are [`AGENTS.md`](../AGENTS.md) 21–34; every dependency's reason is
[`deps.md`](deps.md). When this map and the code disagree, the code wins — fix the map.

## Packages (the layer rules are `layers.json`, enforced by `tools/check-boundaries.mjs`)

| package | holds | imports |
|---|---|---|
| `packages/core` | every answer and every sentence: status, regs presenters (`src/regs/`), conditions, search, menu words, freshness, the generated vocabularies (`*.generated.ts`) | nothing platform, no React |
| `packages/data` | the data seam: the release loader + install (`src/release/`), the regs store reader (`src/regs/`), `WaterSource` over the release (`src/release/source.ts`), SQL drivers (`src/sql/`), spots (`src/spots/`), the gauge feed (`src/feed/`) | core |
| `packages/ui` | headless React hooks — data, never elements: boot, search session, water/regs/route hooks, layout class | core, data, react |
| `packages/ui-native` | every component (RN primitives) and every layout: `Shell.tsx` + `shell/`, water screen, regs screen (`src/regs/`), sheets, panels | core, data, ui, map |
| `packages/map` | the ONE `<Map/>` (`Map.web.tsx` / `Map.native.tsx`), the generated style (`style/`, from `layers.source.json`) | core |
| `apps/mobile` | the one shell for native and web (Expo + react-native-web): `src/App.tsx`, config, platform glue | all |
| `conformance` | suites run against every `WaterSource` and the regs store: the fixture releases, the live release, the oracle parities (`src/regs/`) | all |

## The data path (one path: the release, P11)

```
data/release/r-*/ (R2 in production)
  └─ manifest ─▶ @app/data release loader (bootRelease; phone: install to disk; web: fetch on first need, and
                   after first paint prefetch the region in view's regs file + core/sql-wasm on idle — T186,
                   `release/prefetch.ts`, `@app/ui` `useReleasePrefetch`, the map's `onRegion`)
                   ├─ WaterSource (release/source.ts): core, near, paint, conditions, flow-area …
                   └─ RegsStore (regs/store.ts): water + regs-<region> + regs-sid
                         └─▶ @app/ui hooks ─▶ @app/ui-native screens ─▶ apps/mobile App.tsx
gauge feed (index.json, per-station) ─▶ data/src/feed/http.ts ─▶ panels / conditions
tiles (atlas.pmtiles + basemap) ─▶ @app/map
```

The app decides nothing about a regulation: every answer is looked up in the release (AGENTS 23, 32, 56).

## Fixtures

| where | what | made by |
|---|---|---|
| `packages/data/dev/fixture-release/` | three frozen BUNDLE-half releases (plain, lakeGauged, conformance) | cut once (T61), frozen |
| `packages/data/dev/regs-release/` | the regs fixture release, cut from a live one | `pnpm regs-fixture` (owner-run on a new release) |
| `packages/data/dev/feeds/` | a gauge feed for tests | `pipeline.gauges.feed.publish` |
| `fixtures/golden/` | the prototype oracles: golden, hydrate, taps, tokens, inventory, pins | `design/regs-v11/port/sync.mjs` |
| `fixtures/reader-states.json`, `fixtures/fishart.js` | the reader tool's states; the fish drawings `tools/emit-adipose.mjs` reads | hand / prototype |

`design/` is the mock-up archive and the port generators that read it; nothing in `packages/` imports from it.

## Gates (`pnpm check`, the order is `package.json` `scripts.check`)

tickets → boundaries → platform → style → colours → spinner → gauge-policy / calendar / fish / answers / pack
(generated files current) → deps → **knip** (no unused file, export or dependency; allowances with reasons in
`knip.jsonc`) → typecheck (`tsc -b`, `noUnusedLocals`) → test (vitest: packages, conformance, `tools/*.test.ts`).
`REGS_STORE_REQUIRED=1` makes the live-release suites fail instead of skip when no release is on disk; the strict run
goes through `scripts/heavy.sh`. `tools/vintage-pins.test.ts` refuses an id literal a new release would move.

## Tools (`tools/`)

The gates (`check-*.mjs`, `build-style.mjs`, `emit-*.mjs`), the consistency auditors (`one-*.test.ts`, `consistency.ts`),
the local server (`serve-tiles.mjs`, `pnpm tiles`), and the browser drivers for shots and walks (`regs-web/`,
`regs-census.mjs`, `regs-walk.mjs`, `compare-regs.mjs`, and `request-walk.mjs` — T129's pinned list of what a web visitor
fetches per moment, `request-walk.pin.json`, priced against the release's `WEB_BUDGET` by `release-budget.test.ts`; they
need Chrome and the running app).
