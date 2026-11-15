# iOS integration branch, 2026-11-14 (`ios/integration`)

Not merged into `redesign/stream-sections`. Base: `origin/redesign/stream-sections` 053480a7e.

## Merges (`git merge --no-ff`, in this order)
| branch | head | conflicts | resolution |
|---|---|---|---|
| `ios/t722-install` | b2c4d51af | T722 ticket (Owner answers vs Code half) | both sections kept, base's first |
| `ios/t723-keyboard-back` | 97f3523ca | `run-ios.mjs` (imports, SCENARIOS), T723 ticket | both scenario sets kept; base's T734 section, then the branch's |
| `ios/audit-fixes` | e7b3df8a8 | `appUpdate.test.ts` import, `run-ios.mjs`, T723 ticket | the branch's import (`ChangelogEntry` used); all scenarios; both ticket sections |
| `ios/t719-tapup` | d6afcf124 | none | patch check below |

Auto-merged and read: `App.tsx` (t722 noBackup / space line / iOS words + audit-fixes G2 / link sink: both kept), `app.json`,
`changelog.ts`, `deps.md`. Conflict markers grepped across the tree before each commit: none.

t719 patch check: `app/patches/@maplibre__maplibre-react-native@11.5.0.patch` split per file and compared with the base's:
every Android/Kotlin/Java and JS/TS section byte-identical; only `ios/components/map-view/MLRNMapView.{h,m}` and
`MLRNMapViewComponentView.mm` differ. Its JS changes (`Map.native.tsx`, `tapLift.ts`, `BrowseHost.tsx`, `regs-open.mjs`)
are comments only. `pnpm -C app install --frozen-lockfile`: exit 0; `MLRNTapUpRecognizer` present in the installed binding.

## Step 1: no simulator
- `pnpm -C app install --frozen-lockfile`: exit 0.
- `pnpm -C app check:fast` (unfiltered): exit 0 (after the three merges, and again after t719).
- Whole app vitest (`pnpm -C app test` = `vitest run`, as `pnpm check` in app CI), after three merges: **6 failed** / 7701
  passed / 27 skipped, 650 files. Fixed at the cause in 08f892abf:
  - `tools/back-layers.test.ts` (2): `ModalBack` (**t723-keyboard-back**) is the edge strip inside a Modal that registers;
    listed in COVERED with why, and in the "found today" list.
  - `tools/failures.test.ts`: `mapRail.ts` FINE 2 -> 1 (**audit-fixes** G6 now reports the locate failure: one fewer
    silent catch, the allowance tightened).
  - `tools/no-silent-skips.test.ts`: the new iOS scenarios' named SKIPs listed with counts (**t723-keyboard-back**
    keyboard 1, modal-back 2; **audit-fixes** 4).
  - `tools/testbed-static.test.ts`: audit-fixes scenario tapped a stale label ("Open this app's settings", which the app
    no longer has): dropped (**audit-fixes**); "Audit spot" / "kb probe" are titles the scenarios type themselves:
    allowed with why.
  - `packages/ui/src/prefetch.render.test.tsx`: red on the BASE too (T737, a43e750b9, changed `runPrefetch` to fetch core
    ahead then open it; this test was not updated). Test now asserts fetch, then open after the fetch resolves. **Belongs
    to the base / T737**, not to an iOS branch.
- Feeds worker suite (`npx vitest run --root .` in `app/workers/feeds`): 447 passed.
- After the t719 merge (head below): `install --frozen-lockfile` exit 0, `check:fast` exit 0, whole vitest **exit 0: 645
  files passed, 5 skipped; 7707 tests passed, 27 skipped, 0 failed**.

## Later commits
- 90eb55685 audit G12 (`expo-notifications` `enableRemoteNotifications: false`): `expo prebuild -p ios` showed the
  entitlements without `aps-environment`, associated-domains kept; `expo config --type introspect`: Android modResults
  identical. Then **HELD by the main iOS chat** (the owner is weighing real push): reverted with a plain `git revert`
  (3212f4936). Tree at 3212f4936 = tree at 107207143 (the iOS simulator build below), plus these notes.

## Step 2: app CI on the branch
- Run 43674353270 (107207143): **failed**, 2 tests: `WaterMode.test.tsx` ("▲ on the Vedder opens the Chilliwack's
  map…", expected false to be true) and `WaterScreen.unnamed.test.tsx` (timed out at 30 s). Both pass locally alone and
  in the whole local suite; no merged branch touches them. Re-run of the failed job was cancelled by the next push.
- Run 43676645443 (90eb55685, G12): superseded.
- Run 43677144021 (3212f4936, same tree as 107207143): **success**, every step (pnpm check, feeds worker, web bundle,
  native bundles). So the two reds read as CI-load flakes, not fixed; worth watching.
