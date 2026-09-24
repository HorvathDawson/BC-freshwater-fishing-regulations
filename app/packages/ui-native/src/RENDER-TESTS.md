# What renders in tests, and what does not

`vitest` mounts `@app/ui-native` through **react-native-web** (see `vitest.config.ts`), so
components built from React Native primitives — `View`, `Text`, `Animated` — are really
rendered and really asserted. `WaterScreen.test.tsx` is that.

**Components importing `react-native-svg` cannot be mounted here**, and it is worth writing
down why so nobody spends an afternoon on it twice:

```
react-native-svg -> lib/module/ReactNativeSVG.web.js
                 -> react-native/Libraries/Utilities/codegenNativeComponent.js
                 -> import type {HostComponent} from ...      <-- Flow, not TypeScript
```

The library's *web* build still reaches into React Native's Flow-typed internals. Making
that parse needs Metro's babel pipeline (`babel-preset-expo` with the Flow plugin), which
is a bundler, not a test transform. Aliasing `react-native` to `react-native-web` does not
help: the failing import is a deep path (`react-native/Libraries/...`) that has no
react-native-web equivalent.

So `Hydrograph` and `FishSpinner` are covered as follows instead:

| what | how | where |
|---|---|---|
| every scale, tick and path string | unit tested | `@app/ui/hydrograph.test.ts` |
| the loader runs no per-frame JS | source assertion | `tools/no-per-frame-animation.test.ts` |
| the components themselves draw correctly | **not covered by tests** | `pnpm dev:web` |

That last row is a real gap, not a covered one. Both components are thin — they take
numbers and emit shapes — which is exactly why the arithmetic is tested separately and
why the gap is tolerable rather than fine.

Closing it properly means a Metro-based test runner (`jest-expo`), which is a second test
stack for two components. Worth revisiting when there are twenty.
