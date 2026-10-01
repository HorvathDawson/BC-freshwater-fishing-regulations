# canifishthis — app workspace

Greenfield, mobile-first. Inherits **no code** from `webapp/` or `mobile/` at the repo
root; those are prior art. Plan: [`pipeline/docs/13-build-plan.md`](../pipeline/docs/13-build-plan.md) §4.

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
packages/data       ONE RegsSource interface, one impl per platform interface shared
packages/ui         headless React hooks (data, not elements)       100% shared
packages/ui-native  PHONE components (RN primitives)                native app + mobile web
packages/map        the ONE <Map/>; both adapters behind it         100% shared
apps/mobile         native app — built first
apps/web            mobile web (via RNW) + desktop (its own DOM)    built second
conformance         one suite, run against EVERY RegsSource
```

## Three form factors, two component sets

| target | components | why |
|---|---|---|
| native app | `@app/ui-native` | — |
| **mobile web** | `@app/ui-native` via react-native-web | must look identical to the app **by construction**, not by discipline |
| **desktop web** | its own DOM components in `apps/web` | a different layout, not a stretched phone |

**Desktop having separate components costs nothing — provided components hold no logic.**
Behaviour lives in `@app/ui` hooks, which all three targets call. So splitting desktop out
is a purely presentational decision.

The test: *if you ever want to share a component with desktop in order to avoid duplicating
logic, the logic is in the wrong place.* Move it to a hook and the urge disappears. That
inversion is the whole design — it is why `ui/` exists as a separate package from any
component set.

`apps/web` code-splits on form factor, so a desktop visitor never downloads the RNW phone
tree and vice versa.

**Hooks are shared; components are not.** A hook returns data, so it runs under both
renderers. `<div>` and `<View>` do not. This is the opposite of where most teams draw
the line and it is why this works without committing to react-native-web up front.

## Build order (the ordering is the point)

1. `core` + the `RegsSource` interface + the fixture implementation — **before either app exists**
2. `apps/mobile` against a real bundle, offline on first run
3. `apps/web` against the same store via range reads
4. Conformance runs **both** implementations on every commit

## Commands

```bash
pnpm install
pnpm check        # boundaries + platform + style + colours + deps + typecheck + test — what CI runs
pnpm boundaries   # layer rules (layers.json)
pnpm platform     # variant sets complete, shared code portable
pnpm style        # rebuild the style, verify hash, fail if it was stale
pnpm style:build  # after editing layers.source.json
pnpm colours      # no colour literal outside tokens.json + themes/ (the one palette)
pnpm deps         # every dependency justified in deps.md
```

### Running it

```bash
pnpm dev:web      # Expo web — the phone UI in a browser. The fast loop.
pnpm dev:android  # expo run:android — needs a JDK + Android SDK
pnpm dev:ios      # expo run:ios — needs Xcode + CocoaPods
```

All three are the **same Expo project** (`apps/mobile`) on three platforms, and all three
render the same `PlaceholderScreen` out of `@app/ui-native`. That is the point: mobile web
is not a port of the phone UI, it *is* the phone UI, compiled by react-native-web.

`android/` and `ios/` are **not committed**. They are generated from `app.json` by
`expo prebuild` (Continuous Native Generation) on the first `run:` — a config change is one
edit, not two hand-applied ones. `pnpm --filter @app/mobile run prebuild` regenerates them.

**Where `dev:web` lives, and why it is not `apps/web`.** `layers.json` grants `expo` to
`apps/mobile` and not to `apps/web` — the Expo project is the native one, and Expo's web
output is a *platform* of that project, not a separate app. So today's mobile web is served
by `apps/mobile`. `apps/web` stays empty until desktop exists, at which point it becomes the
production web app: `formFactorFor(width)` picks between its own DOM tree
(`src/desktop/`) and the react-native-web phone tree (`src/phone/`), which re-exports
`@app/ui-native` and adds nothing.

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

## What is deliberately NOT here

No schema, no queries, no components, no map adapters. The content store's shape is
decided by 13-build-plan steps 6–7; writing types against a guess would bake in a
shape the data cannot fill. What exists today is the **structure and its enforcement**.

Two things are already real, because neither depends on the schema:
- `core/freshness` — a stale value must never render as a live one (§5)
- the conformance suite — one query, but the wiring both implementations must satisfy
