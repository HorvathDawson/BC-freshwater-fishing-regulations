# @app/ui-native: every component and layout

The app's components, in React Native primitives. The native app renders them directly and the web renders the same
ones through react-native-web, so phone, tablet and desktop match by construction (AGENTS 26). Each takes its data from an
`@app/ui` hook and draws it; there is no logic here to duplicate.

- Entry: `src/index.ts` exports `Shell` (`src/Shell.tsx`, the whole screen) and the always-needed parts. Metro does not
  tree-shake, so the parts loaded later (menu pages, More, Spots, charts, the water's map and Regulations page) are NOT
  exported there: they are imported by path from `src/part.tsx` / `later.ts`.
- Folders: `shell/`, `water/`, `regs/` (the Regulations page, PDF viewer), `menu/`, `search/`, `spot/`, `adipose/`, `coho/`.
- Theme: `src/theme.ts` builds the palette from `packages/map/style/tokens.json`; no colour literal lives here (`pnpm colours`).
- Generated, never edited: `*.generated.ts` (fish sprite, coho, adipose), built by `app/tools/build-{spinner,coho,adipose}.mjs`.
- Platform differences are `.web.tsx` / `.native.tsx` pairs (`pnpm platform`).

## Tests

`src/**/*.test.tsx` and `*.shell.test.tsx` beside the code (jsdom + react-native-web). What a jsdom render cannot prove
(native-only components) is listed in [`RENDER-TESTS.md`](RENDER-TESTS.md) and in `src/RENDER-TESTS.md`.
