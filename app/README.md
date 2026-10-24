# canifishthis — app workspace

Greenfield, mobile-first. Inherits **no code** from `archive/webapp` or `archive/mobile`; those are prior art.
**What is where today: [`MAP.md`](MAP.md).** This file is the WHY of the layers, the one React and the one map style.
History: [`pipeline/docs/archive/13-build-plan.md`](../pipeline/docs/archive/13-build-plan.md) §4.

## Why the layers are shaped this way

The previous two apps were created independently, so neither was ever the shared
source. Four files that should have been one implementation diverged completely:

| file | web | mobile | lines differing |
|---|---|---|---|
| `useDawnDusk.ts` | 77 | 83 | **every line** |
| `waterbodyDataService.ts` | 1,157 | 116 | 1,203 |
| `featureUtils.ts` | 407 | 171 | 494 |
| `regulationsService.ts` | 144 | 89 | 134 |

Nothing here prevents that by convention. `layers.json` states the rule and
`tools/check-boundaries.mjs` fails CI on it.

## Layout

```
packages/core       domain logic — pure TS, no React, no platform   100% shared
packages/data       ONE WaterSource interface, one impl per platform interface shared
packages/ui         headless React hooks (data, not elements)       100% shared
packages/ui-native  the components (RN primitives), every layout     native app + web (phone, tablet, desktop)
packages/map        the ONE <Map/>; both adapters behind it         100% shared
apps/mobile         the one app shell: native, and web through RNW
conformance         one suite, run against EVERY WaterSource
```

## One component set; desktop is a layout class

| target | components | why |
|---|---|---|
| native app | `@app/ui-native` | — |
| **mobile web** | `@app/ui-native` via react-native-web | must look identical to the app **by construction**, not by discipline |
| **tablet / desktop web** | `@app/ui-native`, arranged by `useLayoutClass()` | a different arrangement, not a stretched phone |

The layout class (`@app/ui` `layout.ts`, T45) turns the window's width into `phone`,
`tablet` or `desktop` from the breakpoint tokens; components take the class and arrange
themselves (a reading column, a side column). There is no second app and no separate DOM
component set (AGENTS rule 26).

**That stays cheap only while components hold no logic.** Behaviour lives in `@app/ui`
hooks, so a wider arrangement is a purely presentational decision. The test: *if a layout
wants to duplicate logic, the logic is in the wrong place.* Move it to a hook. That
inversion is why `ui/` exists as a separate package from the component set.

**Hooks are shared; components are not.** A hook returns data, so it runs under both
renderers. `<div>` and `<View>` do not. This is the opposite of where most teams draw
the line and it is why this works without committing to react-native-web up front.

## Commands

```bash
pnpm install
pnpm check        # every gate, knip, typecheck, vitest — what CI runs (the steps: package.json scripts.check)
pnpm boundaries   # layer rules (layers.json)
pnpm platform     # variant sets complete, shared code portable
pnpm style        # rebuild the style, verify hash, fail if it was stale
pnpm style:build  # after editing layers.source.json
pnpm colours      # no colour literal outside tokens.json + themes/ (the one palette)
pnpm deps         # every dependency justified in deps.md
pnpm knip         # no unused file, export or dependency (allowances in knip.jsonc)
pnpm clean        # delete every workspace dist/ (typecheck output; nothing reads it)
```

### Running it

```bash
pnpm dev:web      # Expo web — the phone UI in a browser. The fast loop.
pnpm dev:android  # expo run:android — needs a JDK + Android SDK
pnpm dev:ios      # expo run:ios — needs Xcode + CocoaPods
```

All three are the **same Expo project** (`apps/mobile`) on three platforms, and all three
render the same `Shell` out of `@app/ui-native`. That is the point: mobile web
is not a port of the phone UI, it *is* the phone UI, compiled by react-native-web.

`android/` and `ios/` are **not committed**. They are generated from `app.json` by
`expo prebuild` (Continuous Native Generation) on the first `run:` — a config change is one
edit, not two hand-applied ones. `pnpm --filter @app/mobile run prebuild` regenerates them.

**Where `dev:web` lives.** Expo's web output is a *platform* of the `apps/mobile` project,
not a separate app, so the web app — phone, tablet and desktop widths alike — is served by
`apps/mobile`. Desktop is a layout class (above), not a second shell.

## One React

**react 19.2.3, react-dom 19.2.3, react-native 0.86.3, Expo SDK 57** — pinned as exact
versions, not ranges, and pinned *again* as `pnpm.overrides` in the root `package.json`.

Why twice: `packages/ui` and `packages/map` declare `react` as a peer with range `*`, and
pnpm's `auto-install-peers` cheerfully satisfied that with 19.2.8 while `apps/mobile` had
19.2.3 — two Reacts, one bundle, `Invalid hook call`. Verified: before the overrides
`pnpm ls -r` showed both; after, one. The override makes the pin structural instead of a
thing six `package.json` files have to agree about.

**19.2.3 specifically because Expo SDK 57 pins it.** The React version is not an independent
choice — Expo's Metro config, `babel-preset-expo` and the native modules are built against
one triple. `npx expo install --check` is the authority; bump the three together with the SDK
or not at all. §4.3 rule 1 exists because the two v1 apps drifted to React 19 vs 18.3.1 by
being created separately, which is exactly what an unpinned range reproduces.

`.npmrc` sets `node-linker=hoisted`: Metro's resolver, Gradle autolinking and CocoaPods all
assume a flat `node_modules`, and pnpm's isolated layout breaks them at bundle/build time
rather than at install. The dependency gate is unaffected — `check-deps` reads
`package.json`, not the tree.

## How the two maps are kept identical

Two apps drifting visually is the same failure as two apps drifting logically, and it
is harder to notice. Three mechanisms, all enforced:

### The layer schema

`layers.source.json` is the authored file. Its shape is documented in
[`style/schema/layers.schema.json`](packages/map/style/schema/layers.schema.json); the
rules that matter are enforced by `pnpm style:build`.

Four concepts, and the third is the one a naive schema gets wrong:

| | what it is |
|---|---|
| **source** | where geometry comes from (our PMTiles, or an external set like parcels) |
| **layer** | one drawn thing, in one **group** — the unit an app can toggle |
| **colour mode** | **one geometry, many colourings.** `streams` can be drawn plain, by closure status, or by discharge. Same tiles, different paint |
| **view** | a named picture: which colour mode each layer uses. This is what a user switches between |

A colour mode is `static`, `categorical` (over a feature-state property, optionally
declared over a named **enum**), or `continuous` (a stop ramp). Categorical and
continuous modes read **feature-state**, so switching view or theme refetches no tiles —
the closure colouring is the same bytes as the plain one.

**Ten rules the build enforces** (each verified to fail):

- a colour mode over an enum must colour **every member** — so `unknown` and
  `default_only` cannot be silently dropped, which doc 10 ⑨/㉜ calls the one failure with
  real consequences
- a continuous mode must define `missing` — a feature with no reading must never inherit
  a colour implying one
- colours must be `{"token": "..."}`; literals are rejected
- every theme must define every themeable token
- an external source must carry `attribution`
- a highlightable layer must declare `featureIdProperty`
- views must name real layers and real modes; exactly one is default
- stops must ascend

**Adding a layer or colouring:** edit `layers.source.json` (plus `tokens.json` /
`themes/*` for new colours), run `pnpm style:build`, commit the generated `style.json` +
`style.meta.json`.

**External layers** (private parcels, etc.) declare `"external": true` and must carry an
`attribution` — a test fails without one, because a third-party tile source has licence
terms our own pipeline does not.

1. **One generated style.** `layers.source.json` → `style.json`. Both adapters import it
   through `@app/map` and neither may modify it.
2. **It cannot be hand-edited or go stale.** `pnpm style` rebuilds and fails if the
   committed output differs. Verified: tampering exits 1.
3. **An app cannot define layers.** `layers.json` bans `maplibre-gl` and
   `@maplibre/maplibre-react-native` outside `packages/map`, so no app can reach the SDK
   and add one of its own.
4. **Parity is asserted, not assumed.** `packages/map/src/parity.test.ts` drives both
   adapters through a recording handle and asserts they emit the **identical call
   sequence** for a group toggle, for every theme, and for a highlight — not just that
   they hold the same style. It also checks layer ids match and that the adapters'
   surfaces differ only in `platform`.
5. **Apps cannot address layers directly.** The contract exposes *groups*, not layers,
   and there is no method to add a layer or set a colour outright.

## One palette

Every colour in the app — map paint AND the phone's own surfaces — is a token in
`packages/map/style/tokens.json` with a value per theme in `themes/*.json`. The map reads them
through `resolveTheme()`; `palette` in `packages/ui-native/src/theme.ts` is built from them
(`paletteFor`) and holds no value of its own; MapLibre's controls and markers take
`mapChrome(theme)` from the same tokens. The app's chrome is the `color.ui.*` family, which the
map's ΔE/duplicate guards skip and `theme.test.ts` / `tools/cvd.test.ts` hold instead. A theme
value may name another token (`"@color.ui.ink"`) when two are one colour by design.
`tools/check-colours.mjs` fails on a hex, `rgb()`/`hsl()` or named colour anywhere else.
