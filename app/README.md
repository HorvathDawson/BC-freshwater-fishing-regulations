# canifishthis: the app workspace

A pnpm workspace: one Expo / React Native app for iOS, Android and the web (phone, tablet, desktop), its packages, the
feeds worker and every gate. It decides nothing about a regulation: it looks answers up in a release built by
`pipeline/`. What is where: [`MAP.md`](MAP.md). Dependencies and why: [`deps.md`](deps.md). Android back: [`BACK.md`](BACK.md).

## Why the layers are shaped this way

The layers keep web and native one implementation; `layers.json` states the rule and `tools/check-boundaries.mjs`
fails CI on it (AGENTS 22-27).
```
packages/core       domain logic: pure TS, no React, no platform
packages/data       ONE WaterSource interface, one implementation per platform
packages/ui         headless React hooks (return data, not elements)
packages/ui-native  every component, every layout (RN primitives; the web draws them through react-native-web)
packages/map        the ONE <Map/>, both renderers behind it, one generated style
apps/mobile         the one app shell: native, and web
conformance         one suite, run against every WaterSource
```

There is no second component set: tablet and desktop are layout classes (`useLayoutClass()`: phone / tablet / desktop)
of the same components. That stays cheap only while components hold no logic: behaviour lives in `@app/ui` hooks. If a
layout wants to duplicate logic, the logic is in the wrong place.

## Web and native code

Where the platforms really differ, the difference is a FILE, never a branch: `foo.web.ts` + `foo.native.ts` typed by one
`foo.d.ts`; callers import `./foo` and the bundler picks (vitest runs the `.web` half). A one-value difference may stay
inline (`Platform.select`). `pnpm platform` (`tools/check-platform.mjs`) refuses an incomplete pair, a DOM API in shared
code, and `Platform.OS` / `typeof window` outside its allow-list.

## Commands (run in `app/`)

```bash
pnpm install --frozen-lockfile
pnpm dev:web       # Expo web: the phone UI in a browser, the fast loop
pnpm dev:android   # expo run:android (JDK + Android SDK)
pnpm dev:ios       # expo run:ios (Xcode + CocoaPods)
pnpm tiles         # serve the local tiles, release and feeds for the dev app
pnpm check         # every gate, knip, typecheck, vitest (the steps: package.json scripts.check)
pnpm check:fast    # every gate but vitest;  pnpm test = vitest only (CI runs 4 shards)
pnpm style:build   # after editing packages/map/style/layers.source.json
```

Single-gate scripts (`boundaries`, `platform`, `style`, `colours`, `deps`, `knip`, ...) are in `package.json`. The
feeds worker is not in `pnpm check`: `pnpm exec vitest run --root workers/feeds` (workflow `feeds-worker-ci.yml`).
`android/` and `ios/` are generated from `app.json` by `expo prebuild` and never committed (`apps/mobile/README.md`).

## One React

react, react-dom and react-native are pinned to exact versions, and pinned again in the root `package.json`
`pnpm.overrides`: `packages/ui` and `packages/map` declare react as a peer, and without the override pnpm installed two
Reacts (`Invalid hook call`). The versions are Expo's SDK set, not an independent choice: bump them with the SDK or not
at all (AGENTS 27; `npx expo install --check` is the authority).

## How the two maps stay identical

1. One generated style: `packages/map/style/layers.source.json` (+ `tokens.json`, `themes/`) builds `style.json`;
   both renderers import it through `@app/map` and neither modifies it. `pnpm style` fails if the committed output
   differs.
2. Four concepts: a source, a layer (in a toggleable group), a colour mode (one geometry, many colourings, read from
   feature-state so switching refetches no tiles) and a view (which mode each layer uses). `pnpm style:build`
   enforces the rules (AGENTS 29): a mode over an enum colours every member, a continuous mode defines `missing`,
   colours are tokens, every theme defines every token.
3. An app cannot define layers: `layers.json` bans the map SDKs outside `packages/map` (AGENTS 28).
4. Parity is asserted: `packages/map/src/parity.test.ts` requires both adapters to make the identical call sequence.

To add a layer or colouring: edit `layers.source.json` (and `tokens.json` / `themes/*`), run `pnpm style:build`,
commit the generated `style.json` and `style.meta.json`.

## One palette

Every colour, map paint and phone surfaces alike, is a token in `packages/map/style/tokens.json` with a value per theme
in `themes/*.json`. The map reads them through `resolveTheme()`; `palette` in `packages/ui-native/src/theme.ts` is built
from them and holds no value of its own. `tools/check-colours.mjs` fails on a hex, `rgb()`, `hsl()` or named colour
anywhere else.
