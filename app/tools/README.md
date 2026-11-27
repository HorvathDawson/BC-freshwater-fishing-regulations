# app/tools: gates, builders, walks and the ticket tools

Node scripts and their tests for the app workspace. Run from `app/` (most through a `package.json` script). Every
top-level `.mjs` is named below (`pipeline/tests/test_readmes_name_every_tool.py` fails when one is not). A tool marked
"one-off (ran for Tnnn)" has no caller in code, a script or CI; it is kept for the next time that check is wanted.

## Gates (a `pnpm` name in brackets means `pnpm check` runs it; the rest are named where they run)
| tool | what |
|---|---|
| `check-boundaries.mjs` | enforces `layers.json`: which package may import which (`pnpm boundaries`) |
| `check-colours.mjs` | every colour lives in the token set (`pnpm colours`) |
| `check-deps.mjs` | every top-level dependency has a one-line reason in `deps.md` (`pnpm deps`) |
| `check-no-render-throw.mjs` | no `throw` in a render path (`pnpm no-render-throw`) |
| `check-platform.mjs` | the mechanically detectable "works on one platform only" faults (`pnpm platform`) |
| `check-promises.mjs` | no floating promises in app source (`pnpm promises`) |
| `check-style.mjs` | `style.json` is generated, never hand-edited, and no app defines layers (inside `pnpm style`) |
| `check-test-anchors.mjs` | a test stands on data it builds, a fixture or the release under test (`pnpm anchors`) |
| `check-tickets.mjs` | the ticket ledger and the docs guard (`pnpm tickets`; AGENTS W49); runs `check-size.mjs` |
| `check-size.mjs` | the docs size budgets, with their exception maps (run by `check-tickets.mjs`) |
| `check-commits.mjs` | Conventional Commits: the commit-msg hook, `before_push.py`, docs-ci (config: `commitlint.config.mjs`) |
| `vitest-shards.mjs` | every test file runs in exactly one of CI's vitest shards |
| `check-built-plist.mjs` | the built `Info.plist` declares what the app config declares (the TestFlight build runs it) |
| `check-store-pages.mjs` | the store-button flags match the public store pages (by hand, needs the network: `pnpm store-pages`) |
| `r8-report.mjs` | R8's scores for an Android release build (`deploy/staging/build-apk.sh` runs it) |

## Builders: `X` checks, `X:build` writes (each `X` is in `pnpm check`)
| tool | what |
|---|---|
| `build-style.mjs` | `layers.source.json` + tokens + themes to `style.json` (`style`, `style:build`) |
| `build-spinner.mjs` | the loading fish baked into a sprite strip (`spinner`, `spinner:build`, `spinner:view`) |
| `build-coho.mjs` | the coho loader's sprite sheets (`coho`, `coho:build`) |
| `build-adipose.mjs` | the "hatchery or wild?" sheet's two drawings (`adipose`, `adipose:build`) |
| `build-splash.mjs` | the native splash and the web's opening picture (`splash`, `splash:build`) |
| `emit-fish.mjs` | the fish manifest, written twice (`fish`, `fish:build`) |

## Other builders and emitters (no `X` / `X:build` pair)
| tool | what |
|---|---|
| `build-seo.mjs` | the static SEO site: water pages and share cards (`pnpm seo`) |
| `build-icons.mjs` | the app icons from the one chosen mark |
| `build-play-graphics.mjs` | the Google Play feature graphic, 1024x500 |
| `emit-fflate-worker.mjs` | the rules worker's copy of fflate, as a generated string |
| `emit-fish-thumbs.mjs` | the fish thumbnails, made from the fish pictures |
| `emit-web-fonts.mjs` | the water pages' fonts as WOFF2, from the app's TTF files |
| `web-icons.mjs` | the site's PNG icons for Google Search and the touch icon |
| `web-shell.mjs` | the coho's first frame inside the web export's `index.html` |
| `sourcemaps.mjs` | keeps each build's source maps under `data/sourcemaps/` (git-ignored) |

## Dev servers and housekeeping
| tool | what |
|---|---|
| `serve-tiles.mjs` | `pnpm tiles`: the local tiles, release and feeds for the dev app, with range requests |
| `clean.mjs` | `pnpm clean`: delete every workspace `dist/` |
| `ledger.mjs` | the ticket ledger, number claims, archive and the owner-asks page (its header lists its commands) |
| `part-end-proof.mjs` | the part-end proof: a new part end on a fixture water fails exactly one test (a test and the anchors scanner name it) |

## Store tools
| tool | what |
|---|---|
| `store-shots.mjs` | the store listing's pictures from the web app, in one command (`store-shots/`, `store-shots.json`) |
| `play-shots.mjs` | the Google Play and App Store screenshots in headless Chrome (store-shots drives it) |

## Measurement, walks and screenshots (need Chrome and the running app)
| tool | what |
|---|---|
| `smoke-staging.mjs` | the staging smoke test after every staging deploy |
| `smoke-rulebook.mjs` | the smoke's rule-book step, split out so it is testable without a browser |
| `request-walk.mjs` | what a web visitor fetches, moment by moment, checked against `request-walk.pin.json` |
| `map-tap.mjs` | shared tap targets for the tools that tap the map on a first view |
| `regs-walk.mjs` | every golden state of the Regulations page, light and dark, with no console error |
| `basemap-shots.mjs` | the basemap render gate: does a rebuilt basemap look like the one it replaces |
| `water-frame-measure.mjs`, `wide-measure.mjs` | lay pages out in headless Chrome for the layout tests; a test starts them |
| `bad-net.mjs` | the bad-connection harness (1-2 bars, flaky); `request-walk.mjs` imports it |
| `a11y-walk.mjs` | axe and keyboard walk of every scene (one-off, ran for T243) |
| `label-shots.mjs` | what names the map draws, and are any outside BC (one-off, ran for T299) |
| `spot-shots.mjs` | a saved spot's page with and without photos (one-off, ran for T284) |
| `prefetch-measure.mjs` | the background prefetch's tap-to-regulations time (one-off, ran for T253) |
| `updates-e2e.mjs` | the update scenarios on the staging test channel (one-off, ran for T251) |
| `regs-census.mjs` | the palette census on the rendered Regulations page (one-off, ran for T39) |

Folders: `testbed/` (the Android emulator test bed, its own README), `seo/` (the SEO site's code; `build-seo.mjs`),
`maplibre-android/` (the pinned MapLibre build, README), `regs-recut/` (`sync.mjs` re-cuts the golden fixtures),
`regs-web/` (browser drivers for the regs page), `fish-loader/`, `fish-sprite/`, `fish-sources/` (art builders),
`perfetto/` (`pinch.py`, the zoom-pinch trace reader), `store-shots/`, `store-video/`.
Also here: `one-*.test.ts` (this exists exactly once), the other `*.test.ts` (vitest suites for the tools and
cross-cutting source scans), and shared config: `versioning.json`, `commits-contract.json`, `commitlint.config.mjs`,
`render-setup.ts`. A gate goes in `check:fast`; a generated file gets a `--check` there too (AGENTS 31).
