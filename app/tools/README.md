# app/tools: gates, builders, walks and the ticket tools

Node scripts and their tests for the app workspace. Everything here is run from `app/` (most through a `package.json`
script) and most files are small; the naming says what a file is.

| name | what | examples |
|---|---|---|
| `check-*.mjs` | a gate: exits non-zero when a rule is broken (each has a `package.json` script, run by `pnpm check`) | `check-boundaries`, `check-platform`, `check-colours`, `check-deps`, `check-test-anchors`, `check-commits`, `check-tickets` |
| `one-*.test.ts` | consistency auditors: "this exists exactly once" (menu, colouring engine, ...) | `one-menu.test.ts`, `one-colouring-engine.test.ts` |
| `build-*.mjs`, `emit-*.mjs` | generate a committed file (`--check` verifies it is current) | `build-style`, `build-spinner`, `build-coho`, `emit-fish`, `emit-fflate-worker` |
| `ledger.mjs`, `check-tickets.mjs` | the ticket ledger tools (AGENTS W49) | `ledger.mjs` header lists its commands |
| `serve-tiles.mjs` | `pnpm tiles`: the local tiles, release and feeds for the dev app | |
| `*-walk.mjs`, `*-shots.mjs`, `store-shots/`, `store-video/` | browser-driven walks and store screenshots (need Chrome and the running app) | `regs-walk`, `a11y-walk`, `store-shots.mjs` |
| `*.test.ts` | vitest suites for the tools and cross-cutting source scans (`failures`, `clock`, `back-layers`, `no-silent-skips`) | |

Folders: `testbed/` (the Android emulator test bed, its own README), `seo/` (the static SEO site build, `build-seo.mjs` and
`pnpm seo`), `maplibre-android/` (the pinned MapLibre Android build, README), `regs-recut/` (re-cuts the release-backed
golden fixtures: `sync.mjs`), `regs-web/` (browser drivers for the regs page), `fish-loader/`, `fish-sprite/`,
`fish-sources/` (art builders), `perfetto/` (trace queries, README), `store-shots/`, `store-video/`.

Shared config: `versioning.json` and `commits-contract.json` (the commit format the release bump also reads),
`commitlint.config.mjs`, `vitest-shards.mjs`, `render-setup.ts` (the vitest render setup).
Rule: a gate is added to `package.json` `check:fast`, and a generated file gets a `--check` there too (AGENTS 31).
