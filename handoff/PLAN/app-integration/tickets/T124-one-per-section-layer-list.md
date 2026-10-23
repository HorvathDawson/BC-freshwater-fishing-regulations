# T124 — One per-section layer list: three hand-typed lists beside `SECTION_SOURCE_LAYERS`

Status: DONE — aa9f02e7, review fix 0fd7efff (app only; ships with the next app build, no data rebuild).

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, findings app 3 and 4
(CONFIRMED, latent).

## Decision being unified
"Which tile source layers carry per-section features?"

## Where it is decided today
- `pushedRoute`: `{stream, lake}`.
- `pushedStandings`: `{stream, lake, wetland_flowing, basin}`.
- `statusData`: `{stream, lake, wetland}`.
- Three more hand-typed lists beside `SECTION_SOURCE_LAYERS` in `app/packages/map/src/`.

How they differ: a wetland is pushed for standings but not for the route; `wetland` vs `wetland_flowing` differ by
list. A new section layer silently drops out of whichever list is not updated.

## Work
- `perSectionLayers()` in `app/packages/map/src/sections.ts`; the lists become aliases of it, or subsets with a named
  reason and a set-equality test.
- Pin: extend `check-boundaries` or a one-* test so no module spells layer-name arrays outside `sections.ts`.

## Acceptance
A test that every list equals `perSectionLayers()` or its declared subset. App shots unchanged.

Rebuild: none (app). Depends: none. Size: S. Owner: no.

## As built (aa9f02e7)
- **`perSectionLayers()`** in `app/packages/map/src/sections.ts` returns `["stream", "lake", "wetland"]`, typed
  `PerSectionLayer`. The list is spelled only here.
  - `SECTION_SOURCE_LAYERS`, `WATER_SOURCE_LAYERS`, colouring's `LABEL_SOURCE_LAYERS` (type `LabelSourceLayer =
    PerSectionLayer`) and trace-geometry's `TRACE_LAYERS` are now aliases of it.
  - `SECTION_SOURCE_LAYERS` used to be derived from the style. That derivation now lives in the test and proves the
    list instead.
- **The three pushed builders could not import it.** `@app/map` depends on `@app/ui`, so `@app/ui` cannot import from
  `@app/map`. The builders now name no layers:
  - `pushedRoute`, `pushedStandings` and `statusData` hand each section's values once, under `"sections"`.
  - `"sections"` is `SECTION_DATA`, documented on `MapProps.data`.
  - Both renderers fan it out to every per-section layer with `spreadSections` (`Map.web.tsx` before
    `withoutEngineKeys`, `Map.native.tsx` before the area/section split).
  - `basin` passes through unchanged.
- **What changes on screen**: nothing on the web. Its status, standing and route go through the colouring engine, and
  the pushed status/standing keys are refused there. On the pushed (native) path:
  - The route now also reaches the `wetland` source-layer, so a slough drawn as a wetland polygon in a donor's chain
    is coloured, as standings and status already did.
  - Standings now go to the `wetland` layer instead of `wetland_flowing`. Both address the same source-layer, so the
    feature-state is identical.
- **Declared subsets and exceptions**, each with its reason, kept in `sections.ts` or allowed in the pin:
  - `VISIBLE_LAYERS` is every section layer except plain marsh.
  - `TAP_GROUPS` holds every section layer exactly once.
  - `SELECTION_RINGS` (`adapters/contract.ts`) covers the polygon section layers, i.e. `perSectionLayers()` without
    the stream, which wears a casing instead. Set equality is pinned.
  - `MapModes` (`ui/mapModes.ts`) is a colour mode per style layer, which is a different decision.
- **Pin** `app/tools/one-section-layers.test.ts`:
  1. The list is exactly the source-layers of the style's highlightable (section-id) layers, and each is a style
     layer that addresses itself.
  2. Every alias is the list, and the declared subsets read only per-section source-layers.
  3. The three builders hand the same values under `SECTION_DATA`, and `spreadSections` produces exactly the list
     plus `basin`. No style layer is named `sections`.
  4. Each grep pattern is shown to catch the spelling it replaced.
  5. No module outside `sections.ts` spells an array (`["stream", "lake", …]`) or an object (`{ stream: x, lake: y }`)
     of section layer names, apart from the two declared exceptions.
  6. The declared exceptions are what they say.
- **Updated tests** (whole values): `routeColour.test`, `conditionsColour.test`, `change.test`, `statusIndex.test`,
  `mapModes.test`, `conditions.render.test`, and `ui-native` `NativeAgrees.test`, which now spreads with the map's
  own function and loops over `perSectionLayers()`.
- **Mutations** (each was run, failed the named test, and was reverted):
  - Adding `"basin"` to the list fails pins 1, 3 and 6.
  - Putting `{ stream: values, lake: values }` back in `pushedRoute` fails pins 3 and 5 (`object`).
  - Putting the literal `new Set(["stream", "lake", "wetland"])` back in trace-geometry fails pin 5 (`array`).
  - Dropping `...rest` from `spreadSections`, which loses `basin`, fails pin 3.
- **Checks** (T123 + T124 together), `REGS_STORE_REQUIRED=1 pnpm -C app check` via heavy.sh:
  - The first step, `tickets`, stops the chain. The ledger rows for T140, T141 and T142 have no ticket file on this
    branch. That is not this lane's: README.md is untouched here.
  - So the remaining steps were run one by one under heavy.sh. Boundaries, platform, style, colours, spinner,
    fixture, gauge-policy, calendar, fish, answers, pack, deps and typecheck are all clean.
  - Test: 7238 passed, 148 failed. None of the failures touch this change:
    - 134 are store pairing in `conformance/src/regs/*`. The `regs.sqlite` store db98ffb0563359a2 is not the store
      the oracles were cut from (63f524bf06038467). Other lanes were rebuilding the bundle meanwhile.
    - 13 are `check-tickets.test` (the same T140–T142 rows).
    - 2 are an ENOENT race. `check-boundaries.test` writes and removes the probe `packages/ui/src/_bnd.ts` while the
      grep tests walk the tree. It hit `one-formula.test` (not this lane's, left as is) and this lane's
      `one-donor-state.test`. Both new pins now skip a file that vanished; fixed in the ticket commit.
  - After that fix, both pins and `check-boundaries.test` pass together (38/38). `tsc -b` is clean.

## Adversarial review (2026-10-23, 0fd7efff)
- **Defect: the web map's second path into `setData` did not fan out.** `Map.web.tsx` applies pushed data in two
  places. One is the `data` effect, which was spread. The other is the colouring engine's `restore`, which puts back
  pushed data after a relabel clears the tiles. It ran `withoutEngineKeys(latestData.current)` with no
  `spreadSections`. So a `sections` key carrying any non-engine value reached `adapter.setData(h, "sections", …)`. That
  throws `unknown layer "sections"` inside `Colouring.set`'s swap. It is latent today. The three builders push only
  `standing`/`status` under `sections`, `withoutEngineKeys` refuses those on the web, and the key empties out. The
  first non-engine value pushed that way would have broken the relabel.
  - **Fix.** The fan-out now lives in `withoutEngineKeys` itself (`return spreadSections(out)`). `one-colouring-engine`
    already pins that function as the only web path into `setData`. The effect's own spread is dropped. A refusal is
    reported once, under the key the caller used (`"sections"`), not once per layer.
  - **Pin.** New test in `one-section-layers` ("the web map fans `sections` out on EVERY path"): the whole value of
    `withoutEngineKeys({ sections, basin })`, the single report, and every resulting key is a style layer the adapter
    knows. It also checks that the native renderer spreads before its area split. It failed before the fix. The
    mutation `return out` fails it again.
- **Pin strength.** The `object` pattern needed the two layer keys side by side. `{ stream: v, basin: b, lake: v }`
  passed the grep. The pattern now allows other keys between them, and that spelling is added to its proofs.
  Mutation: that spelling in `pushedRoute` fails tests 3 and 5, then reverted.
- **Rechecked, fine.** On native, `wetland` is a style layer that addresses itself (pin 1), and the native sink keys
  state by source-layer, so standings on `wetland` equal the old `wetland_flowing`. No consumer reads `.stream` or
  `.lake` off the pushed data. Shell hands either the status data or the conditions data, never a merge, so the
  `sections` keys cannot collide. `SECTION_SOURCE_LAYERS` changed order from the old sorted
  `[lake, stream, wetland]`. Its users (`native.ts` `ours`, `waterBox`) only test membership or loop over it.
- **Checks.** The pins, `packages/map`, `packages/ui/src`, `packages/core` and `NativeAgrees` all pass (144 files,
  1718 tests). `tsc -b` is clean. Both ran under heavy.sh. The full `app check` was not rerun: its `tickets` and
  regs-store failures are the ones listed above, and none of them touch this change.
- **Ships with** the next app build. No data rebuild.

## Owner questions
- On a phone, the route map now colours a donor chain's wetland-drawn sloughs, as the standings map already did.
  Default built: yes, because the two lists were meant to agree.

## Left / follow-ups
- `tools/one-formula.test.ts` has the same `_bnd.ts` ENOENT race (it reads every file it walked). That is a separate
  one-line fix (skip a vanished file), not made here.
- `tools/*.mjs` (build-style, the fixture builders) are outside the pin's roots (`apps`, `packages`), the same scope
  as the other one-* tests. None of them spells a per-section list today.
