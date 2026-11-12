# The app, as it is (map)

The one current map of `app/` (T63). Why the layers are shaped this way is
[`README.md`](README.md); the binding app rules are [`AGENTS.md`](../AGENTS.md) 21–34; every dependency's reason is
[`deps.md`](deps.md). When this map and the code disagree, the code wins — fix the map.

## Packages (the layer rules are `layers.json`, enforced by `tools/check-boundaries.mjs`)

| package | holds | imports |
|---|---|---|
| `packages/core` | every answer and every sentence: status, regs presenters (`src/regs/`), conditions, search, menu words, freshness, the generated vocabularies (`*.generated.ts`) | nothing platform, no React |
| `packages/data` | the data seam: the release loader + install (`src/release/`), the regs store reader (`src/regs/`), `WaterSource` over the release (`src/release/source.ts`), SQL drivers (`src/sql/`), spots (`src/spots/`), the gauge feed (`src/feed/`) | core |
| `packages/ui` | headless React hooks — data, never elements: boot, search session, water/regs/route hooks, layout class | core, data, react |
| `packages/ui-native` | every component (RN primitives) and every layout: `Shell.tsx` + `shell/`, water screen, regs screen (`src/regs/`), sheets, panels | core, data, ui, map |
| `packages/map` | the ONE `<Map/>` (`Map.web.tsx` / `Map.native.tsx`; T409: the web map is built when first seen, `web/firstSeen.ts` — the warm map behind a water's page waits until shown, at every width), the generated style (`style/`, from `layers.source.json`) | core |
| `apps/mobile` | the one shell for native and web (Expo + react-native-web): `src/App.tsx`, config (`config.ts` + `env.ts`: the data environment local / staging / production, T232; `eas.json` profiles), platform glue | all |
| `conformance` | suites run against every `WaterSource` and the regs store: the fixture releases, the live release, the oracle parities (`src/regs/`) | all |

## The data path (one path: the release, P11)

```
data/release/r-*/ (R2 in production)
  └─ manifest ─▶ @app/data release loader (bootRelease; phone: install to disk; web: fetch on first need, and
                   after first paint prefetch the region in view's regs file + core/sql-wasm on idle — T186,
                   `release/prefetch.ts`, `@app/ui` `useReleasePrefetch`, the map's `onRegion`)
                   ├─ WaterSource (release/source.ts): core, near, paint, conditions, flow-area …
                   ├─ answer slices (T256, `release/focus.ts`): web only — core / water read ROUTED: a read the map's
                   │    region's `core-<r>` / `water-<r>` covers goes there, anything else to the whole file
                   ├─ salmon (T211, `loader.salmon`): the Salmon card (`@app/ui` `useSalmonCard`) and the map's
                   │    Standard / Salmon toggle (`useSalmonColour` → the colouring engine's `salmon` channel)
                   ├─ offline (T238, `release/offline.ts`): More → Offline maps — the regulations for all of B.C. (never
                   │    per region; web: what the release cache holds), the province map, `@app/ui` `useOfflineMaps`
                   └─ RegsStore (regs/store.ts): water + regs-<region> + regs-sid
                         └─▶ @app/ui hooks ─▶ @app/ui-native screens ─▶ apps/mobile App.tsx
gauge feed (index.json, per-station) ─▶ data/src/feed/http.ts ─▶ panels / conditions
in-season feed + links (feeds/inseason/, T230) ─▶ data/src/feed/inseason.ts ─▶ ui inseason.ts ─▶ banners, notice
background check (T234, phone only: apps/mobile notify.native.ts, expo-background-task ~15 min) ─▶ data/src/notify/check.ts
                   (obs.json via feed/http.ts `fetchObs`; the release manifest; our version file, core appUpdate.ts
                   `appVersionFileUrl`) ─▶ core alerts.ts (`evaluateAlerts`, T527 stock-style: crosses above / below a
                   value or a percentile for the day — the line per day kept on the alert from the chart's year envelope —
                   once per crossing, re-armed past a 2 % margin) ─▶ local notification (expo-notifications);
                   settings v3 + alerts + run log: data/src/notify/settings.ts ─▶ ui notify.ts ─▶ More → Notifications
                   (a plain list; Add an alert: gauge → condition → value), a gauge card's "Alert me" (ui-native
                   AlertMe.tsx `AlertEditor`)
changelog (T234 v1.1): core changelog.ts (the one source) ─▶ About → What's new (`changelogVM`); build-apk.sh copies a
                   version's bullets into latest.json / play.json ─▶ the bell's App update "What's new in …"
tiles (atlas.pmtiles + basemap) ─▶ @app/map
```

The app decides nothing about a regulation: every answer is looked up in the release (AGENTS 23, 32, 56).

## Fixtures

| where | what | made by |
|---|---|---|
| `packages/data/dev/fixture-release/` | three frozen BUNDLE-half releases (plain, lakeGauged, conformance) | cut once (T61), frozen |
| `packages/data/dev/regs-release/` | the regs fixture release, cut from a live one | `pnpm regs-fixture` (owner-run on a new release) |
| `packages/data/dev/feeds/` | a gauge feed for tests | `pipeline.gauges.feed.publish` |
| `fixtures/golden/` | the release-backed regs oracles, re-cut after a release: `taps.json` (the Python decoder's answers) and `hydrate.json` (page v36's own decode), from `states.json` (the 134 tap questions, no answers). The prototype extracts (golden, pins, tokens, inventory) are retired (T439) | `tools/regs-recut/sync.mjs` |
| `fixtures/reference/` | page v36 (html, js) and its calendar copy, frozen: the parity tests' and the re-cut's oracle | `python -m pipeline.tools.emit.calendar` (calendar only) |
| `fixtures/reader-states.json`, `fixtures/fishart.js` | the reader tool's states; the fish drawings `tools/emit-adipose.mjs` reads | hand / prototype |
| `apps/mobile/assets/fish/thumbs/` | (T405) a 240 px thumbnail per fish picture, drawn by every row and chip (the fish sheet alone draws the full picture); `thumbs.json` records what each was made from, and `pnpm fish` refuses a missing or stale one | `node tools/emit-fish-thumbs.mjs` (Chrome), then `pnpm fish:build` |
| `tools/seo/fonts/` | (T409) the app's faces as WOFF2 (lossless), served to the water pages at `/assets/fonts/`; `fonts.json` records the TTF each was made from, and the SEO build (`webFonts.ts`) refuses a missing or stale one | `node tools/emit-web-fonts.mjs` (Python fontTools + brotli) |

The design prototype (`app/design/`) was deleted in T87 (2026-10-28); git keeps it (regs v38 @ 7fb6809f).

## Gates (`pnpm check`, the order is `package.json` `scripts.check`)

`pnpm check` = `check:fast` (every gate below but vitest, ~21 s; `scripts/fast-fail.sh` runs it, T535) then `test`.

tickets → changelog (app.json's version has the newest changelog entry, `scripts/check-changelog.mjs`, T538) → boundaries → platform (pairs complete; no inline `Platform.OS` / `typeof window` outside `INLINE_ALLOWED`, README "Web and native code") → style → colours → spinner → splash → gauge-policy / calendar / fish / answers / pack / app-reads (T555: app.json's version has its row in `pipeline/deliver/release/app_reads.json` and it is what the code reads; a format moved needs a version bump, then `python -m pipeline.tools.emit.app_reads`)
(generated files current) → deps → **knip** (no unused file, export or dependency; allowances with reasons in
`knip.jsonc`) → typecheck (`tsc -b`, `noUnusedLocals`) → typecheck:seo → promises → test (vitest: packages, conformance, `tools/*.test.ts`).
`REGS_STORE_REQUIRED=1` makes the live-release suites fail instead of skip when no release is on disk, or when the release
is not the one `taps.json` was cut from; `scripts/test-full.sh` (Tier 2) runs its app step with it set (T432), and a plain
`pnpm check` does not. `tools/no-silent-skips.test.ts` counts every app skip site against its allowlist. `tools/vintage-pins.test.ts` refuses an id literal a new release would move.
`pnpm anchors` (`tools/check-test-anchors.mjs --strict`, T667; in `check:fast`) FAILS on a test file that types a section /
part number, a release id or a digest in a release-following test, or reads `data/…` / `RELEASE_DIR` past the helpers
(`liveRelease`, `namedRelease`, `repoData`, `openRegsFixture*`, `sectionNames`), and says what to write instead; no
baseline (`--report` only counts). The pipeline twin is `python -m pipeline.tests.anchors` (helper:
`pipeline/tests/under_test.py`), in `scripts/fast-fail.sh` and the pytest suite.
**Failures (T368):** every caught failure is thrown on or reported through core's `failure.ts` (`reportFailure`, `reported`,
`offlineAs`, `reportOnce`; the environment — offline, a refused permission — is never reported), shown in the one face
(`ui-native` `ErrorState`); `useAsync` reports every hook's failure; links leave through `ui-native` `openLink`.
`tools/failures.test.ts` fails on a new catch that does neither, unless its file's `FINE` count and reason say why.
**One clock (T382):** core `clock.ts` is the only place app source reads the device clock — `now()` for an instant, `bcNow()` / `bcToday()` for the day (the B.C. clock, UTC−7 all year from 8 Mar 2026, written in code); `tools/clock.test.ts` fails on a `new Date()` / `Date.now()` anywhere else (and lists the `performance.now()` frame timers).

## Tools (`tools/`)

The gates (`check-*.mjs`, `build-style.mjs`, `emit-*.mjs`), the consistency auditors (`one-*.test.ts`, `consistency.ts`),
the local server (`serve-tiles.mjs`, `pnpm tiles`), and the browser drivers for shots and walks (`regs-web/`,
`regs-census.mjs`, `regs-walk.mjs`, and `request-walk.mjs` — T129's pinned list of what a web visitor
fetches per moment, `request-walk.pin.json`, priced against the release's `WEB_BUDGET` by `release-budget.test.ts`, and `a11y-walk.mjs` (T243: axe-core WCAG A/AA + a Tab focus walk per scene); they
need Chrome and the running app).

Source maps (T352, `sourcemaps.mjs`): the web export (`export:web*`, `--source-maps`) and `deploy/staging/build-apk.sh`
(the Hermes map) keep their maps per build in `data/sourcemaps/<version>-b<build>-<sha7>/` (gitignored, never served);
`node app/tools/sourcemaps.mjs symbolicate <crash json>` reads a crash's stack back with them.

The SEO site (`build-seo.mjs` over `seo/`, T192–T198): hub pages, sitemap, slugs (`app/seo/slug-map.json`), v1 301s. **A water
page is the app (T269):** `seo/pane.tsx` server-renders the real `Shell` on that water's Regulations view (react-native-web;
the hooks answered first through `@app/ui` `setPrerenderAnswers`), `seo/prerender.ts` + `seo/layouts.ts` merge the light/dark
and phone/tablet/desktop renders into one markup, and the page is the web export's own index.html (`--app <export>`) with the
pane before `#root`; the app takes over when its own pane is up. The app reads `/water/<slug>` as a start link and writes it
back (`apps/mobile` `startLink.web.ts`, `systemBack.web.ts`, `waterSlugs.ts` over the build's `water-slugs/<n>.json`).

(T487) **A gauge's page** is `/gauge/<station>/<slug>` (`seo/gauges.ts`, index at `/gauges`, in the sitemap): a static hub-style
page naming the waters the gauge serves (one regulations link each, no open/closed word) whose live half (`seo/gaugeClient.ts`,
shipped as one plain script) reads the facts the feeds worker writes beside each card (`feeds/cards/<station>.json`) and
Open-Meteo at the gauge. Its preview card is drawn in the feeds worker's gauge run (`workers/feeds/src/gauges/card/`: a tiny
palette rasteriser + glyphs pre-drawn from Archivo, `workers/feeds/tools/card_glyphs.py`), stored on R2, and only MAPPED by the site worker
(`seo/legacy.ts`: `/gauge/<station>/card.png` → R2 binding CARDS; missing → `/share/gauge.png` + a `private/cards-missing/`
marker the gauge run reports). The worker reads which gauges have pages from the site's `/gauge/stations.json`. (T530) The slug/path/URL is one core function (`core/gaugePage.ts`) used by this build and by the app's Share on the Conditions gauge card (`ui` `useGaugeShare`, `ui-native` `GaugeShare`).
