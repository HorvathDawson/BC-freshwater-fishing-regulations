# T01 — serve `/regs.sqlite`, `REGS` URL, prop plumbing to the water screen

Phase 0 · Depends on — (parallel with T00) · Blocks T11, T23 · Decisions D1 (map action), D7 (date pill) · Size ½ day

## Goal
The dev server serves the regs store; the app knows its URL; `Shell` hands the water screen everything the Regulations page
will need — the store URL, the shared day (the map's `mapDay`, null = today) with its setter, and the section the reader tapped
(so the page opens on the PART they tapped, not part 0). Nothing is drawn with them yet; the water screen still shows
`RegulationsPlaceholder`. Every existing test keeps passing because the new props are optional.

## Files
- modify `app/tools/serve-tiles.mjs` — `ARCHIVES` (:19–37)
- modify `app/apps/mobile/src/App.tsx` — constants block (:34–77) and the `<Shell …>` mount (:140)
- modify `app/packages/ui-native/src/Shell.tsx` — props (:59–81), state (:95–131), `onPressFeature` (:350–354), `openWater` (:363–367),
  the resets at :399, :406, :442, :478, :676, the water mount (:424)
- modify `app/packages/ui-native/src/WaterScreen.tsx` — props (:32–43)
- create `app/packages/ui-native/src/WaterScreen.regs-props.test.tsx`

## Signatures
`serve-tiles.mjs` — add after the `/status_index.bin` entry:
```js
  // THE REGS STORE (`python -m pipeline.deliver store` → data/generated/bundle/regs.sqlite, format regs-store/4): every
  // Regulations-page answer. Paired with /province.sqlite by meta digests (section_handles, reach_digest,
  // rule_ids_sha256 — the app refuses a store from another build). `REGS_STORE` serves a side build without promoting it.
  "/regs.sqlite": process.env.REGS_STORE ?? `${repo}data/generated/bundle/regs.sqlite`,
```
(Range + Content-Length are already handled generically; sql.js on web downloads the whole file — 14,237,696 B.)

`App.tsx` — after `const BUNDLE = …` (:57):
```ts
/**
 * The regs store: the Regulations page's answers (regs-store/4). One set with BUNDLE — the province bundle and this store
 * must come from one build (the page checks meta.section_handles / reach_digest / rule_ids_sha256 and refuses a mixed pair).
 * There is no fixture regs store paired with /bundle.sqlite; pointing BUNDLE at the fixture shows "Regulations unavailable".
 */
const REGS = "http://localhost:39217/regs.sqlite";
```
and the mount gains one prop: `<Shell source={source} … regs={REGS} …>`.

`Shell.tsx` props — add to the destructuring and the type:
```ts
  /** Where the regs store is served (App's REGS). Absent → the water screen shows no Regulations (never a guess). */
  regs?: string;
```
state — after `const [condSection, setCondSection] = useState<SectionId | null>(null);` (:125):
```ts
  /** The section the reader TAPPED to open this water (null: opened from search/list). The Regulations page opens on the
   *  part holding it. Cleared by every route that opens a different water — the same discipline as condSection. */
  const [tapSection, setTapSection] = useState<SectionId | null>(null);
```
`onPressFeature`: `if (found) { setPicked(null); setTapSection(Number(featureId) as SectionId); setItem(found); }`;
`openWater`: add `setTapSection(null);`; every `setItem(null)` site above (:399, :406, :442, :444, :478, :676) also calls
`setTapSection(null)`.
Water mount (:424):
```tsx
    ? <WaterScreen source={source} item={item} palette={palette}
                   regsUrl={regs} day={mapDay} onDay={setMapDay} section={tapSection}
                   …existing props…
```
`WaterScreen.tsx` props:
```ts
export function WaterScreen({ source, item, palette, onBack, onConditions,
                              regsUrl, day = null, onDay, section = null }: {
  source: RegsSource; item: ItemId; palette: Palette; onBack?: () => void;
  onConditions?: (section: SectionId) => void;
  /** The regs store URL (T11 `useRegsStore`). Absent → no Regulations data is asked. */
  regsUrl?: string;
  /** THE ONE DAY: the map's mapDay (null = today). The page's date pill moves it for the map too (D7). */
  day?: Date | null;
  onDay?: (d: Date | null) => void;
  /** The tapped section, to pick the part (T09 `targetFor`). */
  section?: SectionId | null;
})
```
The new props are accepted and unused until T23 (prefix nothing; TS allows unused destructured props under the current
tsconfig — `noUnusedParameters` is off in `tsconfig.base.json`).

## Ports
Prototype `S.d` (the selected day) is shared between header and the map in the app (D7); prototype `WATERS[...].map` → `section`.

## Reads
None.

## Tests
`app/packages/ui-native/src/WaterScreen.regs-props.test.tsx` (project `render`, imitate `WaterScreen.test.tsx`):
1. `render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} regsUrl="http://x/regs.sqlite"
   day={new Date(2026, 9, 10)} onDay={() => {}} section={7 as SectionId} />)` renders the same heading as without the props
   (`screen.getByRole("heading", { name: … })` — reuse the name the existing test asserts).
2. A node test `app/tools/serve-routes.test.ts` (project `logic`): read `tools/serve-tiles.mjs` as text and assert it contains
   `"/regs.sqlite"` and `process.env.REGS_STORE` (the file starts a server on import, so it is read, not imported).
The existing `WaterScreen.test.tsx` and Shell tests must pass unchanged.

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project render WaterScreen
pnpm -C app exec vitest run --project logic tools/serve-routes
node app/tools/serve-tiles.mjs   # prints "  /regs.sqlite  14 MB"; curl -sI http://localhost:39217/regs.sqlite → 200, Content-Length 14237696
```

## Notes
- facts.md #14/#15: the store does NOT go beside the atlas (that is the status index); it is a bundle-like artifact beside
  `/province.sqlite`. In production it sits on R2 next to the province bundle.
- A tap resolves `itemForSection` first; `tapSection` is set only when an item was found, so a tap on unnamed water still opens
  nothing (unchanged). The unnamed-section Regulations page (target `{kind:"section"}`) is T42's.
- `mapDay` is today-relative null; the page must convert with `day ?? new Date()` at the hook (T11), never store `new Date()` in state.
