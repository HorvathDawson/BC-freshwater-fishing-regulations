/**
 * THE REGULATIONS PAGE'S HOOKS (T11): open + pair the regs store once per release loader, resolve a tap to the store's
 * inputs on THE ONE DAY, and hold who is fishing for the session. Headless: they return data; the water screen (T23)
 * draws it.
 *
 * RECONCILED WITH THE TICKET (written for a `regs.sqlite` URL + an injected opener, before D22/P11): the store is opened
 * through the release LOADER (T01 already hands it to the water screen: `WaterScreen.loader`), so `useRegsStore(loader)`
 * replaces `useRegsStore(source, regsUrl, openDb)` and no `openRegsDb` prop is threaded through Shell/App. The pairing
 * is the store's own (`openRegsStore`: the tiles drawn, then each file against water), not `BundleInfo` digests.
 *
 * THE OWNER RULE (decisions.md 2026-10-21): nothing here decides a regulation. The store answers; these hooks only say
 * which day and which target to ask about, and hand the answer on.
 *
 * THE T09 SEAM: `loadRegsInputs` (tap → `RegsInputs`) and `targetFor` (tapped section → part) are T09's, in
 * `@app/data/regs`. `useRegsTarget` / `useRegulations` take them as their last argument (a test injects its own) and
 * default to T09's two functions (T21 bound them when it drew the page).
 */
import { useMemo, useSyncExternalStore } from "react";
import { accessNotice, buildRegsPage, tapDay, waterAccessNotice, type AccessNotice, type Day, type RegsInputs, type RegsPage, type RegsTarget,
         type Weekday, type Who } from "@app/core";
import { downstreamWater, type ItemId, type WaterSource, type SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { loadRegsInputs, openRegsStore, RegsStoreError, siblingItems, targetFor, type RegsExpect, type RegsLoader,
         type RegsStore } from "@app/data/regs";
import { useAsync, type Async } from "./async";

// ── the store ────────────────────────────────────────────────────────────────────────────────────────────────────────

/** A stable number per object (a loader, a store): part of a hook's key, so a NEW loader is a new question. */
const ids = new WeakMap<object, number>();
let nextId = 0;
const idOf = (o: object): number => { let i = ids.get(o); if (i === undefined) ids.set(o, i = ++nextId); return i; };

/** The module cache's key within one loader: what the store is held to (`expect.tiles`; absent = the manifest's). */
export function regsStoreKey(loader: Pick<RegsLoader, "release">, expect: RegsExpect = {}): string {
  return `${loader.release}#${expect.tiles === undefined ? "manifest" : expect.tiles ?? "none"}`;
}

const stores = new WeakMap<RegsLoader, Map<string, Promise<RegsStore>>>();

/**
 * One open + pairing per (loader, expectation), shared by every screen. A refusal is cached too — the same release
 * against the same tiles is refused the same way, so it is never retried in a loop. A failure that is not a refusal (a
 * fetch that failed) is dropped from the cache: the next ask tries again. Once open, `statics()` and `rules()` are
 * prewarmed (the page needs both for any tap; their own caches hold the promise).
 */
export function loadRegsStore(loader: RegsLoader, expect: RegsExpect = {}): Promise<RegsStore> {
  let byKey = stores.get(loader);
  if (!byKey) stores.set(loader, byKey = new Map());
  const key = regsStoreKey(loader, expect);
  let p = byKey.get(key);
  if (!p) {
    p = openRegsStore(loader, expect);
    byKey.set(key, p);
    p.then((st) => { st.statics().catch(() => {}); st.rules().catch(() => {}); },
           (e: unknown) => { if (!(e instanceof RegsStoreError)) byKey!.delete(key); });
  }
  return p;
}

/**
 * (T58) A water's split-lake areas, itself among them, read through this release's store (`siblingItems`): what the
 * opener (`useOpenWater`'s `group`) keeps on a frame so another area of a lake held is that lake, not a second one.
 */
export function regsGroupOf(loader: RegsLoader): (item: ItemId) => Promise<readonly ItemId[]> {
  return async (item) => (await siblingItems(await loadRegsStore(loader), item)) as readonly ItemId[];
}

/**
 * The store for this release. `failed` with a `RegsStoreError` (its `why`: format / version / digest / missing …) → the
 * page says "Regulations unavailable" with the reason. `ready(null)`: no loader (no release is open — a screen mounted alone in a test).
 */
export function useRegsStore(loader: RegsLoader | null, expect: RegsExpect = {}): Async<RegsStore | null> {
  const key = `regs-store:${loader ? `${idOf(loader)}:${regsStoreKey(loader, expect)}` : ""}`;
  return useAsync(async () => (loader ? loadRegsStore(loader, expect) : null), key);
}

// ── the T09 functions ─────────────────────────────────────────────────────────────────────────────────────────────────────

/** T09 `loadRegsInputs`: everything the page reads for one tap on one day. */
export type LoadRegsInputs = (store: RegsStore, target: RegsTarget, day: Day, weekday: Weekday, calendarYear: number) => Promise<RegsInputs>;
/** T09 `targetFor`: the part a tap on `section` of `item` opens. */
export type TargetFor = (store: RegsStore, item: string, section: number | null) => Promise<RegsTarget>;

/** A target as a key: `part:<item>:<partIx>` | `section:<handle>`. */
export const regsTargetKey = (t: RegsTarget): string => (t.kind === "part" ? `part:${t.item}:${t.partIx}` : `section:${t.handle}`);

/**
 * The part the water screen opens for `item` (and the tapped section, when one was tapped). (T53) An UNNAMED reach (no
 * item) is answered by its section: the target is `{kind: "section", handle}` (the store's `section_akey` →
 * `section_place`, T09's section branch), never a part.
 */
export function useRegsTarget(store: RegsStore | null, item: ItemId | null, section: SectionId | null,
                              targetForPart: TargetFor = targetFor): Async<RegsTarget | null> {
  const key = `regs-target:${store ? idOf(store) : ""}:${item ?? ""}:${section ?? ""}`;
  const asks = !!store && (item !== null || section !== null);
  return useAsync(async () => (!store ? null : item ? targetForPart(store, item, section)
    : section !== null ? { kind: "section", handle: section } as RegsTarget : null), key, asks);
}

/**
 * (T53) An unnamed reach's name, as the store words it ("Unnamed stream · Region 1": `unnamedName`, the same name its
 * Regulations page heads with), so its Conditions view is titled as its Regulations view is. Disabled for a named water.
 */
export function useUnnamedName(store: RegsStore | null, item: ItemId | null, section: SectionId | null): Async<string | null> {
  const asks = !!store && item === null && section !== null;
  return useAsync(async () => (asks ? store!.unnamedName(section!) : null),
    `unnamed-name:${store ? idOf(store) : ""}:${asks ? section : ""}`, asks);
}

// ── where an unnamed reach flows (T53) ──────────────────────────────────────────────────────────────────────────────

/**
 * `downstreamWater` (@app/data: the first named water down the reach's flow pointers) for an unnamed reach on screen;
 * disabled (loading, never asked) for a named water or no section. Feeds the header's "Tributary of …" line.
 */
export function useDownstreamWater(source: Pick<WaterSource, "traceToGauge" | "waterFor">, item: ItemId | null,
                                   section: SectionId | null): Async<{ item: ItemId; name: string; kind: string } | null> {
  const asks = item === null && section !== null;
  return useAsync(async () => (asks ? downstreamWater(source, section!) : null), `downstream:${asks ? section : ""}`, asks);
}

/**
 * (T54) WHAT A SECTION'S PAGE SAYS ABOUT ACCESS: paint's no-access rows and the `access` file's gentler ones (permit-only
 * land, reserves, private land), worded by core (`accessNotice`). Null: nothing to say, or no tapped section (a water
 * opened from search names no section, so no access is claimed for it).
 */
export async function sectionAccessNotice(loader: Pick<ReleaseLoader, "paint" | "access">,
                                          section: SectionId): Promise<AccessNotice | null> {
  // the gentle file failing (offline, a bad fetch) costs only the gentle note: the no-access banner is paint's alone,
  // and must never be silenced by it. The loader forgets a failed fetch, so the next page asks again.
  const gentleOf = loader.access().catch((e: unknown) => {
    console.warn("access:", e instanceof Error ? e.message : e);
    return null;
  });
  const [paint, gentle] = await Promise.all([loader.paint(), gentleOf]);
  return accessNotice([...paint.access.of(section), ...(gentle?.of(section) ?? [])]);
}

/** (T147) The page a water opened with no tapped section shows: its item (by ord), its part, and how many parts it has. */
export interface AccessPage { readonly ord: number; readonly partIx: number; readonly parts: number }

/**
 * (T147) WHAT A PAGE OPENED WITH NO TAPPED SECTION SAYS ABOUT ACCESS: the no-access banner over its part's sections (the
 * water's `part_section`, read through the store's `navRows`), worded by core (`waterAccessNotice`): "this water" for a
 * one-part water, "this part of the water" otherwise. Paint's no-access rows only — the gentle note stays per section.
 */
export async function pageAccessNotice(loader: Pick<ReleaseLoader, "paint">, store: Pick<RegsStore, "navRows">,
                                       page: AccessPage): Promise<AccessNotice | null> {
  const [paint, nav] = await Promise.all([loader.paint(), store.navRows(page.ord)]);
  const sids = nav.sections.filter((s) => s.partIx === page.partIx).map((s) => s.sid);
  return waterAccessNotice(sids.map((sid) => paint.access.of(sid)), page.parts > 1 ? "part" : "water");
}

/**
 * The notice a Regulations page leads with: the TAPPED section's (T54), else (T147) the page's own part's, when the
 * store and the page's inputs are there to name it. Null: nothing to say, or not yet known.
 */
export function useAccessNotice(loader: Pick<ReleaseLoader, "paint" | "access"> | null,
                                section: SectionId | null, store: Pick<RegsStore, "navRows"> | null = null,
                                page: AccessPage | null = null): Async<AccessNotice | null> {
  const bySection = !!loader && section !== null;
  const byPage = !!loader && !bySection && !!store && !!page;
  const asks = bySection || byPage;
  return useAsync(async () => (bySection ? sectionAccessNotice(loader!, section!)
                               : byPage ? pageAccessNotice(loader!, store!, page!) : null),
                  `access:${loader ? idOf(loader) : ""}:${bySection ? `s${section}` : ""}:`
                  + `${byPage ? `${idOf(store!)}:${page!.ord}:${page!.partIx}:${page!.parts}` : ""}`, asks);
}

/**
 * The store's inputs for a target on THE ONE DAY (`mapDay`, the Shell's; null = today, read at call time and never
 * stored, so "today" follows midnight). A new day or target is a new key: a slower answer for the old one never
 * overwrites the new one (useAsync).
 */
export function useRegulations(store: RegsStore | null, target: RegsTarget | null, mapDay: Date | null,
                               load: LoadRegsInputs = loadRegsInputs): Async<RegsInputs | null> {
  const d = regsDay(mapDay);
  const key = `regs:${store ? idOf(store) : ""}:${target ? regsTargetKey(target) : ""}:${isoDay(d.day, d.calendarYear)}`;
  return useAsync(async () => (store && target ? load(store, target, d.day, d.weekday, d.calendarYear) : null), key, !!store && !!target);
}

/**
 * The page for those inputs (core `buildRegsPage`, T21): a `who` change rebuilds from the same inputs, no reload. Every
 * screen uses this; nobody calls buildRegsPage in a component.
 */
export function useRegsPage(inputs: RegsInputs | null, who: Who, today: Day, downstream: string | null = null): RegsPage | null {
  // (T53) `downstream`: an unnamed reach's named water down its flow — the header's label slot (core `downstreamLabel`)
  return useMemo(() => (inputs ? buildRegsPage(inputs, { today, who, downstream }) : null),
    [inputs, who, today.month, today.day, downstream]);
}

// ── the one day ──────────────────────────────────────────────────────────────────────────────────────────────────────

const sameDate = (a: Date, b: Date) =>
  a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
const isoDay = (d: Day, year: number) =>
  `${year}-${String(d.month).padStart(2, "0")}-${String(d.day).padStart(2, "0")}`;

/**
 * The Shell's `mapDay` (Date | null; null = today) as the core's day: the LOCAL calendar date, its real weekday (core's
 * calendar, `tapDay`), its year (which names every weekday of the strip), today's date, and whether it is today.
 */
export function regsDay(mapDay: Date | null, now: Date = new Date()):
  { day: Day; weekday: Weekday; calendarYear: number; today: Day; isToday: boolean } {
  const d = mapDay ?? now;
  const day = { month: d.getMonth() + 1, day: d.getDate() };
  return { day, weekday: tapDay(day, d.getFullYear()).weekday, calendarYear: d.getFullYear(),
           today: { month: now.getMonth() + 1, day: now.getDate() }, isToday: sameDate(d, now) };
}

/** A core day (+ its year) back to the Shell's `mapDay`: today → null (so "Today" follows midnight), else local noon. */
export function mapDayOf(day: Day, calendarYear: number, now: Date = new Date()): Date | null {
  const d = new Date(calendarYear, day.month - 1, day.day, 12);
  return sameDate(d, now) ? null : d;
}

// ── who is fishing ───────────────────────────────────────────────────────────────────────────────────────────────────

/** The default angler: a B.C. resident, 16 or older (decisions.md D8). */
export const DEFAULT_WHO: Who = Object.freeze({ age: "adult", residency: "resident" });

let who: Who = DEFAULT_WHO;
const listeners = new Set<() => void>();
const subscribe = (f: () => void) => { listeners.add(f); return () => { listeners.delete(f); }; };

/** Set who is fishing, for every mounted reader (in memory, for the session: D8). */
export function setAnglerProfile(next: Who): void {
  if (next.age === who.age && next.residency === who.residency) return;
  who = Object.freeze({ age: next.age, residency: next.residency });
  for (const f of [...listeners]) f();
}

/** Who is fishing, shared by every mount, and its setter. No storage (D8 default). */
export function useAnglerProfile(): readonly [Who, (who: Who) => void] {
  return [useSyncExternalStore(subscribe, () => who, () => who), setAnglerProfile] as const;
}
