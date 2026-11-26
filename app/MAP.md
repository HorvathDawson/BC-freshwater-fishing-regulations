# The app, as it is (map)

The one current map of `app/`. Why the layers are shaped this way: [`README.md`](README.md). The binding rules are
`AGENTS.md` 22-34; every dependency's reason is [`deps.md`](deps.md); the back table is
[`BACK.md`](BACK.md). When this map and the code disagree, the code wins: fix the map. Each package has its own README.

## Packages (the layer rules are `layers.json`, enforced by `tools/check-boundaries.mjs`)

| package | holds | may import |
|---|---|---|
| `packages/core` | every answer and sentence: status, regs presenters (`src/regs/`), conditions, search, menu words, freshness, the generated vocabularies (`*.generated.ts`) | nothing platform, no React |
| `packages/data` | the data seam: release loader and install (`src/release/`), regs store (`src/regs/`), `WaterSource` over the release (`release/source.ts`), SQL drivers (`src/sql/`), spots, gauge/in-season/salmon feeds (`src/feed/`), phone notifications check (`src/notify/`) | core |
| `packages/ui` | headless React hooks (data, never elements): boot, search, water/regs/route, layout class | core, data, react |
| `packages/ui-native` | every component and layout (RN primitives, drawn on the web by react-native-web): `Shell.tsx`, `shell/`, water, regs, sheets, panels | core, data, ui |
| `packages/map` | the ONE `<Map/>` (`Map.web.tsx` / `Map.native.tsx`), the generated style (`style/`), the colouring engine | core, ui, the two map SDKs |
| `apps/mobile` | the one shell for native and web: `src/App.tsx`, config (`config.ts`, `env.ts`: data environment local / staging / production), platform glue, local Expo module `modules/bg-download` | all |
| `conformance` | suites every `WaterSource` and regs store must pass: fixture releases, the live release, oracle parities | all |
| `workers/feeds` | the Cloudflare feeds worker (gauges, DFO salmon, in-season, run timing, push, reports); not in the pnpm workspace | `core` (generated policy) |

## The data path (one path: the release)

```
data/release/r-*/ (R2 in production) -> manifest -> @app/data release loader (phone: install to disk;
  web: fetch on first need, prefetch the region in view when idle)
    |- WaterSource (release/source.ts): core, near, paint, conditions, flow-area
    |- RegsStore (regs/store.ts): water + regs-<region> + regs-sid; core regs/ composes a page, ui-native regs/ draws it
    |- salmon (loader.salmon) -> ui useSalmonCard, the map's Standard/Salmon colouring
    |- offline (release/offline.ts): More -> Offline maps
    `-> @app/ui hooks -> @app/ui-native screens -> apps/mobile App.tsx
feeds (served by the feeds worker; read by packages/data/src/feed/): gauge index + per-station files, in-season, salmon,
  run timing. Every index carries `minAppVersion`; core `feedGate.ts` refuses an index meant for a newer app.
phone background check (notify.native.ts, expo-background-task) -> data notify/check.ts -> core alerts.ts -> local notification
tiles (atlas.pmtiles + basemap) -> @app/map
```

The app decides nothing about a regulation: every answer is looked up in the release (AGENTS 23, 32, 56). A page older
than what is served is told so (`apps/mobile/src/replaced.web.ts`, ui `replaced.ts`) and refreshes itself once.

## Fixtures

| where | what | made by |
|---|---|---|
| `packages/data/dev/fixture-release/` | three frozen bundle-half releases | cut once, frozen |
| `packages/data/dev/regs-release/` | the regs fixture release, cut from a live one | `pnpm regs-fixture` |
| `packages/data/dev/mini-release/` | a whole release built by the real pipeline from an invented world | `python -m pipeline.tools.mini_release --write` |
| `packages/data/dev/feeds/` | a gauge feed for tests | `pipeline.gauges.feed.publish` |
| `fixtures/golden/` | release-backed regs oracles: `taps.json`, `page-oracle.json` | `node app/tools/regs-recut/sync.mjs` (the release re-cuts them, AGENTS W46) |
| `fixtures/reference/` | page v36 (html, js) and the calendar copy, frozen: the parity oracle | `python -m pipeline.tools.emit.calendar` (calendar only) |
| `fixtures/trout/`, `fixtures/reader-states.json`, `fixtures/dfo-feed-*.json` | test inputs for the trout table, the reader tool, the DFO feed | by hand |

## Gates (`pnpm check` = `check:fast` then `test`; the order is `package.json` `scripts.check`)

`check:fast` runs every static gate (`scripts/fast-fail.sh` runs it too); CI (`app-ci.yml`) splits it into `gates`,
`vitest` (4 shards, `tools/vitest-shards.mjs`) and the web and native `bundles`. In order: tickets, anchors, changelog,
boundaries, platform, style, colours, spinner / coho / adipose / splash (generated art current), the Python-emitted files
(`gauge-policy`, `fish`, `calendar`, `answers`, `pack`, `app-reads`, `feed-formats`: each `--check`s a generated file
against `pipeline/`), deps, knip, typecheck (`tsc -b`), typecheck:worker, typecheck:seo, promises, no-render-throw, inventories.
- `pnpm anchors` fails a test that types a section number, release id or digest, or reads `data/` past the helpers
  (`liveRelease`, `namedRelease`, `repoData`, `openRegsFixture*`); the Python twin is `pipeline/tests/anchors.py`.
- Failures: every catch is thrown on or reported through core `failure.ts` (`reportFailure`), shown with `ui-native`
  `ErrorState`; `tools/failures.test.ts` fails a new catch that does neither (AGENTS W46).
- Faults: every screen is mounted through `ui-native` `ScreenBoundary` (`Shell.tsx` `screen(…)`): a throw while one draws shows that screen's line with Try again / Go back and is reported (`reportFailure`); a menu page, the block of sheets, the search bar and the map's key each have their own (a band, with Close, where it is a panel); the app itself sits under a last-resort boundary (`apps/mobile/src/rootBoundary.tsx`, `index.js`) for faults in the chrome, the boot screen and `App`; `tools/screen-boundary.test.ts` fails a screen mounted outside the first and the root outside the second, `tools/check-no-render-throw.mjs` a new `throw` in a component, hook, effect, or a same-file helper they call (listed ones carry a count and a reason).
- One clock: core `clock.ts` is the only reader of the device clock (`now()`, `bcNow()`, `bcToday()`); `tools/clock.test.ts` enforces it.
- `REGS_STORE_REQUIRED=1` makes the live-release suites fail instead of skip when no release is on disk
  (`scripts/test-full.sh` sets it); `tools/no-silent-skips.test.ts` counts every skip site.

## Tools (`tools/`, see its README)

Gates (`check-*.mjs`), builders (`build-*.mjs`, `emit-*.mjs`), consistency auditors (`one-*.test.ts`), the local server
(`serve-tiles.mjs`, `pnpm tiles`), browser walks and shots (`regs-web/`, `*-walk.mjs`, `store-shots/`), the Android test
bed (`testbed/`), the SEO site (`build-seo.mjs` over `seo/` and `tools/seo/`), the ticket tools (`ledger.mjs`,
`check-tickets.mjs`) and the MapLibre Android build (`maplibre-android/`).

The SEO site: `app/seo/` holds slugs and legacy redirects; a water page IS the app, server-rendered
(`tools/seo/pane.tsx`, `prerender.ts`) and taken over by the app when it loads; `/gauge/<station>/<slug>` pages come from
`tools/seo/gauges.ts`. Source maps per build go to `data/sourcemaps/` (`tools/sourcemaps.mjs`, gitignored).
