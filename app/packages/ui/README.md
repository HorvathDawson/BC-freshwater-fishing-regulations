# @app/ui: headless React hooks

The behaviour of the app as hooks that return data, never elements, so they run under both renderers (react-native and
react-native-web): boot (`boot.ts`), search session, route (`route.ts`, the one back in `gestures.ts` `useBack`), water and
regs hooks, release prefetch and update state (`replaced.ts`, `appUpdate.ts`), the layout class (`layout.ts`), chart and
hydrograph arithmetic, map modes and colour choices, notifications, spots, offline maps.

- Entry: `src/index.ts` (and `./place`). Hooks are under `src/hooks/` and `src/*.ts`.
- Reads: `@app/core` and `@app/data` only; it writes nothing itself.
- Imports no `react-dom`, `react-native` or map SDK (`layers.json`).

## Rules

- Hooks are shared, components are not (AGENTS 24): if a layout wants to duplicate logic, the logic belongs here (AGENTS 25).
- A new surface that opens over the screen registers one back with `useBackLayer(open, closeOneStep)`; see
  [`app/BACK.md`](../../BACK.md).

## Tests

`src/**/*.test.ts(x)` beside the code (`pnpm test` in `app/`).
