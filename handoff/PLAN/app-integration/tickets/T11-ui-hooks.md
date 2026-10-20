# T11 — ui hooks: `useRegsStore`, `useRegulations`, `useAnglerProfile`; the one day

Phase 0 · Depends on T01, T03, T09 (T10 for the render test) · Blocks T23, T33, T35 · Decisions D7 (the shared day) · Size ½ day

## Goal
Three headless hooks the water screen will call (T23): open + pair the regs store once per URL and bundle, resolve the tap to
inputs for the selected day, and hold who is fishing for the session. The day is THE ONE DAY the Shell already owns
(`mapDay: Date | null`, null = today): the Regulations page reads it and its date pill writes it, so the map's colours and the
page always answer for the same date.

## Files
- create `app/packages/ui/src/regs.ts` (+ `export * from "./regs"` in `app/packages/ui/src/index.ts`)
- create `app/packages/ui/src/regs.render.test.tsx` (project `render`: hooks must be MOUNTED, ui/src/index.ts comment)
- create `app/packages/ui/src/regs.test.ts` (project `logic`: the pure adapter + the module cache)
- modify `app/apps/mobile/src/App.tsx` — pass `openRegsDb={loadBundle}` to `<Shell>` (loadBundle is already imported, :25)
- modify `app/packages/ui-native/src/Shell.tsx` — prop `openRegsDb?: RegsOpenDb`, forwarded to `<WaterScreen openRegsDb={openRegsDb} …>`
- modify `app/packages/ui-native/src/WaterScreen.tsx` — prop `openRegsDb?: RegsOpenDb` (unused until T23)

## Signatures
```ts
// app/packages/ui/src/regs.ts
import type { Day, Weekday, Who, RegsTarget, RegsInputs } from "@app/core";
import type { Db, RegsSource, BundleInfo, ItemId, SectionId } from "@app/data";
import { openRegsStore, loadRegsInputs, targetFor, RegsStoreError, type RegsStore } from "@app/data/regs";
import { useAsync, type Async } from "./async";

/** How the app opens a SQLite file from a URL: App.tsx passes `loadBundle` (sql.js on web, expo-sqlite on native). Injected —
 *  `@app/ui` never imports a platform driver (the `@app/data/bundle/open` split resolves by platform extension, which vitest
 *  and the ui layer must not depend on). */
export type RegsOpenDb = (url: string) => Promise<Db>;

/** One open + pairing per (url, bundle digests), shared by every screen; a refusal is cached too (never retried in a loop). */
export function loadRegsStore(url: string, info: BundleInfo, openDb: RegsOpenDb): Promise<RegsStore>;
/** module cache key: `${url}#${info.sectionHandles}#${info.reachDigest ?? ""}#${info.ruleIdsSha256 ?? ""}` */
export function regsStoreKey(url: string, info: BundleInfo): string;

/** The store for this bundle. failed (RegsStoreError, `why` says format/version/digest) → the page shows "Regulations unavailable"
 *  with the reason; ready(null) when no URL or no opener was given. Prewarms statics() and rules() once ready (T09 timing). */
export function useRegsStore(source: RegsSource, regsUrl: string | undefined, openDb: RegsOpenDb | undefined):
  Async<RegsStore | null>;
//   useAsync key: `regs-store:${regsUrl ?? ""}`; enabled: !!regsUrl && !!openDb

/** The part the water screen opens: T09 targetFor(store, item, tapped section). */
export function useRegsTarget(store: RegsStore | null, item: ItemId | null, section: SectionId | null): Async<RegsTarget | null>;
//   key: `regs-target:${item ?? ""}:${section ?? ""}`; enabled: !!store && !!item

/** The inputs for a target on THE ONE DAY (mapDay null = today, read at call time — never stored). */
export function useRegulations(store: RegsStore | null, target: RegsTarget | null, mapDay: Date | null): Async<RegsInputs | null>;
//   key: `regs:${targetKey(target)}:${isoDay(mapDay ?? new Date())}` where targetKey = `part:${item}:${partIx}` | `section:${handle}`;
//   enabled: !!store && !!target

/** Shell mapDay (Date | null) → the core day: local calendar date, its Weekday, its year (names every weekday of the strip). */
export function regsDay(mapDay: Date | null, now?: Date): { day: Day; weekday: Weekday; calendarYear: number; today: Day; isToday: boolean };
/** core Day (+ year) back to the Shell's Date | null: today → null (so "Today" follows midnight), else local noon of that date. */
export function mapDayOf(day: Day, calendarYear: number, now?: Date): Date | null;

/** Who is fishing, for the session (in memory; no storage until a decision says so). Default adult resident. Shared across mounts. */
export const DEFAULT_WHO: Who;   // { age: "adult", residency: "resident" }
export function useAnglerProfile(): readonly [Who, (who: Who) => void];
```
`useAnglerProfile` = a module-level `let who = DEFAULT_WHO` + `Set<() => void>` listeners + React `useSyncExternalStore(subscribe, () => who)`
(no context provider needed; resets on reload — D-decision "persist who" is open, see decisions.md).
`regsDay`: `day = {month: d.getMonth()+1, day: d.getDate()}`, `weekday = WEEKDAYS[(d.getDay() + 6) % 7]` (calendar.generated, Monday first),
`calendarYear = d.getFullYear()`; `today` from `now ?? new Date()`.

## Ports
Prototype `S.d` / `S.who` state (regs-v11.html @04765ab5, `state` object; header `hdr`); page_v36 `state.{md, who}`, `TODAY` (:30–31,
the viewer's own clock), `YEAR` (:131). `ui/src/hooks.ts` `loadStatusIndex` (:521–541) module-cache pattern; `useAsync` (`ui/src/async.ts`).

## Reads
`source.info()` → `BundleInfo.{sectionHandles, reachDigest, ruleIdsSha256}` (T03); everything `loadRegsInputs` reads (T09).

## Tests
`app/packages/ui/src/regs.test.ts` (logic, pure):
1. `regsDay(new Date(2026, 9, 10, 15, 0))` → `{day:{month:10,day:10}, weekday:"Saturday", calendarYear:2026}`; `regsDay(null, new Date(2026,4,15))`
   → day `{5,15}`, weekday "Friday", `isToday: true`; `regsDay(new Date(2026,9,12), new Date(2026,9,10))` → "Monday", `isToday: false`.
2. `mapDayOf({month:10,day:10}, 2026, new Date(2026,9,10))` → `null`; `mapDayOf({6,15}, 2026, new Date(2026,9,10))` → a Date with
   `getFullYear() 2026, getMonth() 5, getDate() 15`.
3. `regsStoreKey("u", {version:"v", validUntil:null, sectionHandles:"d768d6e135e62b99", reachDigest:"cad1c21fe6eb9ba9", ruleIdsSha256:"16be720be08b0fca"})`
   → `"u#d768d6e135e62b99#cad1c21fe6eb9ba9#16be720be08b0fca"`.
4. `loadRegsStore` twice with the same key and a counting `openDb` (returns `openRegsDb(DEV_REGS_FIXTURE)` — via `@app/data/regs/node`;
   the ui logic test may import it because it is a TEST file importing `@app/data`, which ui's `mayImport` allows) → `openDb` called once.
5. A refused store (`info.sectionHandles = "fixture-local"` against the fixture's `d768…`) → rejects `RegsStoreError`, `why "digest"`; a
   second call returns the cached rejection without calling `openDb` again.

`app/packages/ui/src/regs.render.test.tsx` (render, `@testing-library/react` `renderHook`):
6. `useRegsStore(makeFixtureSource() /* sectionHandles "fixture-local" */, "x", openDb)` → settles `failed` with `RegsStoreError` (the dev
   fixture set is not paired with a regs store — T01 note).
7. With a source stub whose `info()` returns `{sectionHandles:null, reachDigest:null, ruleIdsSha256:null}` and the fixture regs db:
   `useRegsStore` → ready; `useRegsTarget(store, "gnis:3062", null)` → `{kind:"part", item:"gnis:3062", partIx:0}`;
   `useRegulations(store, target, new Date(2026,5,15))` → ready, `value.s === 2`, `value.frames.display.status === "closed"` (vedder_615).
8. Re-render with `mapDay` `new Date(2026,4,15)` → loading then ready with `s === 1`; the key changed, the stale promise never overwrites.
9. `useAnglerProfile()` in two hooks: setting `{age:"u16", residency:"non_resident"}` in one updates the other; initial value `DEFAULT_WHO`.
10. `regsUrl` undefined → `useRegsStore` returns `{state:"loading"}` (useAsync `enabled=false` never runs) and `openDb` is never called.

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/ui/src/regs.test
pnpm -C app exec vitest run --project render packages/ui/src/regs.render
```

## Notes
- **CONTRACT CHANGE** (facts.md #10, contracts): `useRegulations(store, target, mapDay: Date | null)` — not `useRegsStore(bundleDb)`
  (facts #16/#17: Shell has no `Db`); the store comes from a URL prop + an injected opener (`openRegsDb` prop added by this ticket to
  Shell and WaterScreen, beside T01's `regsUrl`/`day`/`onDay`/`section`).
- `useAsync` with `enabled=false` returns the LOADING sentinel forever — the water screen must check `regsUrl && openRegsDb` itself to
  draw "no regulations data configured" rather than a spinner (T23).
- The web opener downloads the whole 14 MB store through sql.js (`loadBundle`), the same path as the province bundle; native reads it
  through expo-sqlite `deserializeDatabaseAsync`. A range-reading driver is future work and changes nothing above `Db`.
- `who` is not persisted (decisions.md); the licence tab's selectors (T33/T35) call the setter.
