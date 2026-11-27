# @app/map: the one map

The ONE `<Map/>` for the whole app and the only package that knows `maplibre-gl` (web, `src/Map.web.tsx`) and
`@maplibre/maplibre-react-native` (phone, `src/Map.native.tsx` over `src/native/maplibre/`) differ (AGENTS 28). Both adapters (`src/adapters/`) take
the same generated style and are held to the same call sequence by `src/parity.test.ts`.

- Entry: `src/index.ts` (`Map`, `runtimeStyle`, theme and colour helpers; `./colours`). Platform halves: `src/web/`, `src/native/`.
- `style/`: `layers.source.json` (authored) + `tokens.json` + `themes/` build `style.json` and `style.meta.json`
  (`pnpm style:build` in `app/`; `pnpm style` fails on a stale or hand-edited output). Schema: `style/schema/`.
- The phone's map (T1154): `src/Map.native.tsx` is the engine-neutral SHELL (everything that decides); the engine draws, in
  `src/native/maplibre/` (`View.tsx` the component tree, `handle.ts` the element refs, `tiles.ts` the per-tile event,
  `log.ts`, `HeldLabels.tsx`, `dialect.ts`). The shell reaches it only through `src/native/engine.ts` (the neutral types
  and the one re-export); only that folder may import the SDK (`app/layers.json`, `app/tools/check-boundaries.mjs`).
- `src/colouring.ts` + `native/useColouring.ts` / `web/useChannels.ts`: the one colouring engine (per-tile feature ids to feature-state).
- Reads: `atlas.pmtiles` and `basemap.pmtiles` by byte range (`pmtiles://`), from R2 or local files. How a style addresses
  them is `src/tile-access.ts`, apart from the paths (`TileEndpoints`).
- The phone's style: `src/native-style.ts` composes it engine-neutrally and the engine's dialect finishes it
  (`src/native/maplibre/dialect.ts`); `tools/style-golden.test.ts` holds both composed styles byte for byte.

## Rules

- Add or recolour a layer only in `layers.source.json` / `tokens.json`, then `pnpm style:build` (AGENTS 28, 29); the app
  addresses groups, never layers.
- `maplibre-gl` 6's worker is its own file (`apps/mobile/gl-worker.js`, `src/web/glWorker.ts`: without it no tile loads); the native
  binding is patched (`app/patches/`, `app/deps.md`).

## Tests

`src/**/*.test.ts(x)` beside the code, plus `style-valid` (the style-spec validator over every theme and mode) in `app/tools/`.
