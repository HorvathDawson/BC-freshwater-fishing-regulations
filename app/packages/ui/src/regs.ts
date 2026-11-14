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
import { useCallback, useEffect, useMemo, useRef, useSyncExternalStore } from "react";
import { accessNeedsWeights, buildRegsPage, offlineAs, reported, headerOf, methodClassOn, pageAccessNotices as coreAccessNotices, partAccess,
         salmonCardLive, soloWater, tapDay, type AccessNotice, type Day, type RegsInputs,
         type RegsPage, type RegsTarget, type SalmonCardLiveVM,
         type Weekday, type Who, bcNow, now as clockNow } from "@app/core";
import { downstreamWater, openProbe, type ItemId, type WaterSource, type SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { itemsOfSections, loadRegsInputs, loadWaterGroup, openRegsStore, RegsStoreError, siblingItems, targetFor, type RegsExpect, type RegsLoader,
         type RegsStore } from "@app/data/regs";
import { useAsync, type Async } from "./async";
import { askOnce, peekAnswer } from "./answers";
import { salmonKeyOf, type SalmonLoader as SalmonKeyLoader } from "./salmonKey";

/** (T286) The test bed's "before" build (`EXPO_PUBLIC_REGS_ONE_BY_ONE=1`): no page held, every open read afresh. */
const oneByOne = (): boolean => (globalThis as { __regsOneByOne?: boolean }).__regsOneByOne === true;
import { useFeedVersion } from "./feedVersion";
import { useBcToday } from "./today";

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
 * (T286) THE FIRST TAP AS FAST AS THE NEXT: what any tap's page needs, done before the reader taps — the store opened and
 * paired, its statics, rules and licensing read (all of them, once), regs-sid / core / geo opened, and (`files`, a
 * phone, where every file is already on the device) every region file opened too. Before this the first tap of a
 * session paid for all of it (~0.5 s on the emulator, the spinner's threshold). A failure is dropped: the tap asks again.
 */
export async function prewarmRegs(loader: RegsLoader, { files = false }: { files?: boolean } = {}): Promise<void> {
  const st = await loadRegsStore(loader);
  await Promise.all([st.statics(), st.rules(), st.lics(), st.sid(), st.core(),
                    loader.manifest.files.geo ? loader.db("geo").catch(() => null) : null]);
  if (!files) return;
  for (const name of Object.keys(loader.manifest.files))
    if (/^regs-(?!sid$)/.test(name)) await loader.db(name as never).catch(() => null);
}

/**
 * (T58) A water's split-lake areas, itself among them, read through this release's store (`siblingItems`): what the
 * opener (`useOpenWater`'s `group`) keeps on a frame so another area of a lake held is that lake, not a second one.
 */
export function regsGroupOf(loader: RegsLoader): (item: ItemId) => Promise<readonly ItemId[]> {
  return async (item) => (await siblingItems(await loadRegsStore(loader), item)) as readonly ItemId[];
}

/**
 * (T221) Whether a water is solo — one entry on its map, going on as / from no other water (`@app/core` `soloWater` over
 * its group, `loadWaterGroup`): what the opener asks of a search result (solo: its Regulations, not its Map). (T517) The
 * group as its map joins it: the same live salmon key (`salmonKeyOf`), so two pieces the live salmon reads alike are one
 * entry here as on the map.
 */
export function regsSoloOf(loader: RegsLoader & SalmonKeyLoader): (item: ItemId) => Promise<boolean> {
  return async (item) => {
    const store = await loadRegsStore(loader);
    return soloWater(await loadWaterGroup(store, item, await salmonKeyOf(loader, store, item)));
  };
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
  // (T286) asked once per store and held (`askOnce`): a water opened again has its target in the very first render
  const fresh = targetForPart === targetFor;
  return useAsync(async () => (!store ? null : item
    ? (fresh && !oneByOne() ? askOnce(store, key, () => targetForPart(store, item, section), Infinity) : targetForPart(store, item, section))
    : section !== null ? { kind: "section", handle: section } as RegsTarget : null), key, asks, undefined,
    () => (store && item && fresh && !oneByOne() ? peekAnswer<RegsTarget>(store, key, Infinity) : null));
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
 * (T387) WHAT A REGULATIONS PAGE IS ABOUT — read off its inputs, never off the tap: a named water's PART (the target a
 * tap, a search, a flow or a part pick all resolve to), or an unnamed reach's section (its page is that section's). Every
 * part-level fact on the page (access, the Salmon card, …) is asked by this, so no tap on a part can answer differently.
 */
export type PageSubject =
  | { readonly kind: "part"; readonly item: string; readonly ord: number; readonly partIx: number; readonly parts: number }
  | { readonly kind: "section"; readonly sid: number };

export function pageSubjectOf(inputs: RegsInputs | null): PageSubject | null {
  if (!inputs) return null;
  if (inputs.item) return { kind: "part", item: inputs.item.id, ord: inputs.item.ord, partIx: inputs.partIx,
                            parts: inputs.item.parts.length };
  return inputs.target.kind === "section" ? { kind: "section", sid: inputs.target.handle } : null;
}

/** A subject as a key (`part:<ord>:<partIx>/<parts>` | `section:<sid>`). */
const pageSubjectKey = (s: PageSubject | null): string =>
  !s ? "" : s.kind === "part" ? `part:${s.ord}:${s.partIx}/${s.parts}` : `section:${s.sid}`;

/** The sections a page is about: its part's (the water's `part_section`, through the store's `navRows`), or its one. */
async function pageSections(store: Pick<RegsStore, "navRows">, s: PageSubject): Promise<number[]> {
  if (s.kind === "section") return [s.sid];
  return (await store.navRows(s.ord)).sections.filter((x) => x.partIx === s.partIx).map((x) => x.sid);
}

/**
 * (T54, T147, T367, T387) WHAT A PAGE SAYS ABOUT ACCESS — the PART's, the same for every tap on it (owner 2026-11-04:
 * "in the one section not all clicks are equal"): paint's no-access rows and the `access` file's gentler ones (permit-only
 * land, reserves, private land) over every section of the page's subject, pooled by core (`partAccess`: each area with
 * the share of the part it covers, the sections weighed by their length along the water — the store's `partLengths`,
 * read only when the sections differ; a section unmeasured says no share) and worded by core (`pageAccessNotices`:
 * the banner, then the note). Empty: nothing to say.
 */
export async function regsAccessNotices(loader: Pick<ReleaseLoader, "paint" | "access">,
                                        store: Pick<RegsStore, "navRows" | "partLengths">,
                                        subject: PageSubject): Promise<AccessNotice[]> {
  // the gentle file failing costs only the gentle note: the no-access banner is paint's alone, and must never be
  // silenced by it. (T368, owner Q4) Never a silent drop: a failure that is not the line being down is REPORTED. The
  // loader forgets a failed fetch, so the next page asks again.
  const gentleOf = loader.access().catch(reported(null));
  const [paint, gentle, sids] = await Promise.all([loader.paint(), gentleOf, pageSections(store, subject)]);
  const rows = sids.map((sid) => [...paint.access.of(sid), ...(gentle?.of(sid) ?? [])]);
  let lengths: (number | null)[] | null = null;
  if (subject.kind === "part" && accessNeedsWeights(rows)) {
    const m = await store.partLengths(subject.ord, subject.partIx);
    lengths = sids.map((sid) => m.get(sid) ?? null);
  }
  const word = subject.kind === "section" ? "section" : subject.parts > 1 ? "part" : "water";
  return coreAccessNotices(partAccess(rows, lengths), word);
}

/** (T211, T387) Which sections a page's Salmon card is about: its subject's; and the part's answer key. */
export interface SalmonPage {
  readonly subject: PageSubject;
  /** (owner 2026-10-28) the part's answer key: its bundle regions name the salmon slices the card reads (null: all) */
  readonly akey?: number | null;
}

type SalmonLoader = Pick<ReleaseLoader, "salmon"> & Partial<Pick<ReleaseLoader, "regionsOf" | "method">>;

/**
 * (T211) THE SALMON CARD a Regulations page draws under the provincial rules (option A): the release's DFO salmon for
 * (T387) every section of the page's part — never the tapped section alone — through the store's `navRows`, worded by
 * core (`salmonCard`). Null where the release ships no salmon. A water DFO does not list reads "Salmon — closed".
 */
export async function salmonCardFor(loader: SalmonLoader, store: Pick<RegsStore, "navRows">,
                                    page: SalmonPage, on: Date): Promise<SalmonCardLiveVM | null> {
  // only the page's own region's salmon slice (owner 2026-10-28); a page with no key reads them all
  const regions = page.akey != null && loader.regionsOf ? await loader.regionsOf(page.akey) : [];
  const f = await loader.salmon(regions.length ? regions : undefined);
  if (!f) return null;
  const sids = await pageSections(store, page.subject);
  // (owner 2026-10-28: "bait and fly rules also apply to salmon") the provincial gear marks on the page that day
  // (T368) offline: no gear marks; any other failure fails the card (said with Try again, reported) — never a card
  // silently missing its gear rules
  const m = loader.method ? await loader.method().catch(offlineAs(null)) : null;
  const gear = openProbe.work("salmon gear", () => (m ? [...new Set(sids.map((sid) => methodClassOn(m, sid, on)))] : []));
  return openProbe.work("salmon card", () => salmonCardLive(f, { sids, item: page.subject.kind === "part" ? page.subject.item : null, gear }, on));
}

export function useSalmonCard(loader: SalmonLoader | null, store: Pick<RegsStore, "navRows"> | null,
                              page: SalmonPage | null, mapDay: Date | null,
                              /** (T254) a new number asks again (Try again; the line coming back after a failure) */
                              retry = 0): Async<SalmonCardLiveVM | null> {
  const asks = !!loader && !!store && !!page;
  const today = useBcToday();             // (T551) no day picked = today, which rolls over at midnight
  const on = mapDay ?? today;
  const dayKey = `${on.getFullYear()}-${on.getMonth() + 1}-${on.getDate()}`;
  // (owner 2026-10-29) a DFO feed the update service took redraws the open page's card in place: a refresh
  const dfo = useFeedVersion("dfo");
  const key = `salmon:${loader ? idOf(loader) : ""}:${store ? idOf(store) : ""}:${pageSubjectKey(page?.subject ?? null)}:`
    + `${page?.akey ?? ""}:${dayKey}` + (retry ? `:r${retry}` : "");
  return useAsync(async () => (asks ? salmonCardFor(loader!, store!, page!, on) : null), `${key}#${dfo}`, asks, key);
}

/** (T387) The page's access notices (`regsAccessNotices`): asked by the page's subject alone — the tap is not an input. */
export function useAccessNotices(loader: Pick<ReleaseLoader, "paint" | "access"> | null,
                                 store: Pick<RegsStore, "navRows" | "partLengths"> | null,
                                 subject: PageSubject | null): Async<AccessNotice[]> {
  const asks = !!loader && !!store && !!subject;
  return useAsync(async () => (asks ? regsAccessNotices(loader!, store!, subject!) : []),
                  `access:${loader ? idOf(loader) : ""}:${store ? idOf(store) : ""}:${pageSubjectKey(subject)}`, asks);
}

/** (T387) `pageSubjectOf`, held by its key: a new object only when the page's subject changes. */
export function usePageSubject(inputs: RegsInputs | null): PageSubject | null {
  const s = pageSubjectOf(inputs);
  const key = pageSubjectKey(s);
  // eslint-disable-next-line react-hooks/exhaustive-deps -- the subject's identity is its key
  return useMemo(() => s, [key]);
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
  // (T286) THE PAGES ALREADY READ, per release and day: the store's answers are fixed for a release, so a tap's inputs
  // are asked once (`askOnce`, the most recent `ANSWER_CAP` kept per store) and a water opened again is drawn in the
  // very render it is opened — no read, no loading frame
  const fresh = load === loadRegsInputs;
  return useAsync(async () => (store && target
    ? (fresh && !oneByOne() ? askOnce(store, key, () => load(store, target, d.day, d.weekday, d.calendarYear), Infinity)
      : load(store, target, d.day, d.weekday, d.calendarYear))
    : null), key, !!store && !!target, undefined,
    () => (store && target && fresh && !oneByOne() ? peekAnswer<RegsInputs>(store, key, Infinity) : null));
}

/**
 * The page for those inputs (core `buildRegsPage`, T21): a `who` change rebuilds from the same inputs, no reload. Every
 * screen uses this; nobody calls buildRegsPage in a component.
 */
export function useRegsPage(inputs: RegsInputs | null, who: Who, today: Day, downstream: string | null = null,
                            names: ReadonlyMap<number, string> | null = null): RegsPage | null {
  // (T53) `downstream`: an unnamed reach's named water down its flow — the header's label slot (core `downstreamLabel`);
  // (T517) `names`: the parts as the water's map names them (`useMapEntries`) — the header's and the Parts sheet's
  return useMemo(() => {
    if (!inputs) return null;
    const t = openProbe.on() ? performance.now() : 0;
    const page = buildRegsPage(inputs, { today, who, downstream, names });
    if (t) openProbe.span("build", performance.now() - t);
    return page;
  },
    [inputs, who, today.month, today.day, downstream, names]);
}

// ── (T286) the waters on screen, read before they are tapped ─────────────────────────────────────────────────────────────

/** How long the map must stay still before its waters are read (a pan that goes on cancels it). */
export const PREFETCH_IDLE_MS = 400;
/** At most this many waters per settle: the ones with the most sections on screen, the likeliest taps. */
export const PREFETCH_MAX = 8;

/**
 * (T286) THE PAGES OF THE WATERS ON SCREEN, read while the reader looks: when the map settles, the waters with the most
 * sections in view (`PREFETCH_MAX`) have their water sheet, target and inputs read into the same held answers a tap reads
 * (`useWater`, `useRegsTarget`, `useRegulations` — the keys are theirs), one water at a time, idle only: a move of the
 * map, or a new settle, stops it between waters. A tap on one of them then draws its page in the very first render.
 * Null: no release.
 */
export function useRegsPrefetch(loader: RegsLoader | null, source: Pick<WaterSource, "water">,
                                mapDay: Date | null): RegsPrefetch | null {
  const live = useRef({ gen: 0, timer: null as ReturnType<typeof setTimeout> | null });
  const day = useRef(mapDay);
  day.current = mapDay;
  useEffect(() => () => { live.current.gen += 1; if (live.current.timer) clearTimeout(live.current.timer); }, []);
  const cb = useCallback((ids: readonly (string | number)[]) => {
    if (!loader || oneByOne()) return;
    const l = live.current, gen = ++l.gen;
    if (l.timer) clearTimeout(l.timer);
    l.timer = setTimeout(() => { void prefetchPages(loader, source, ids.map(Number), day.current, () => l.gen !== gen).catch(() => {}); },
                         PREFETCH_IDLE_MS);
  }, [loader, source]);
  /** A tap: what is still to read waits (the tap's own reads go first through the one SQLite queue). */
  const stop = useCallback(() => { const l = live.current; l.gen += 1; if (l.timer) clearTimeout(l.timer); }, []);
  return useMemo(() => (loader ? { onVisible: cb, stop } : null), [loader, cb, stop]);
}

/** (T286) `useRegsPrefetch`'s handle: the map's `onVisible`, and `stop` (a tap). */
export interface RegsPrefetch { onVisible(sids: readonly (string | number)[]): void; stop(): void }

async function prefetchPages(loader: RegsLoader, source: Pick<WaterSource, "water">, sids: readonly number[],
                             mapDay: Date | null, stale: () => boolean): Promise<void> {
  const store = await loadRegsStore(loader);
  if (stale()) return;
  const owner = await itemsOfSections(store, sids);
  const by = new Map<string, { n: number; sid: number }>();
  for (const [sid, item] of owner) { const b = by.get(item); if (b) b.n += 1; else by.set(item, { n: 1, sid }); }
  const d = regsDay(mapDay);
  const top = [...by].sort((a, b) => b[1].n - a[1].n).slice(0, PREFETCH_MAX);
  const t0 = clockNow();
  let done = 0;
  for (const [item, { sid }] of top) {
    if (stale()) break;
    await askOnce(source, `water-item:${item}`, () => source.water(item as ItemId), Infinity);
    const tKey = `regs-target:${idOf(store)}:${item}:${sid}`;
    const target = await askOnce(store, tKey, () => targetFor(store, item, sid), Infinity);
    const key = `regs:${idOf(store)}:${regsTargetKey(target)}:${isoDay(d.day, d.calendarYear)}`;
    await askOnce(store, key, () => loadRegsInputs(store, target, d.day, d.weekday, d.calendarYear), Infinity);
    done += 1;
  }
  // the test bed's open probe: what the settle read
  if (openProbe.on()) console.log(`[prefetch] ${done}/${top.length} waters of ${sids.length} sections in ${clockNow() - t0} ms`);
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
export function regsDay(mapDay: Date | null, now: Date = bcNow()):
  { day: Day; weekday: Weekday; calendarYear: number; today: Day; isToday: boolean } {
  const d = mapDay ?? now;
  const day = { month: d.getMonth() + 1, day: d.getDate() };
  return { day, weekday: tapDay(day, d.getFullYear()).weekday, calendarYear: d.getFullYear(),
           today: { month: now.getMonth() + 1, day: now.getDate() }, isToday: sameDate(d, now) };
}

/** A core day (+ its year) back to the Shell's `mapDay`: today → null (so "Today" follows midnight), else local noon. */
export function mapDayOf(day: Day, calendarYear: number, now: Date = bcNow()): Date | null {
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

/**
 * (T367, owner 2026-11-04: "in condition like regs add the extent text too … and land use notes … just like regs") A
 * WATER'S OTHER VIEWS SAY WHERE AND WHAT LAND, AS ITS REGULATIONS PAGE DOES: the part's place label (core `headerOf`'s,
 * "Mouth → Vedder Crossing Bridge"; an unnamed reach's "Tributary of …") and the page's access notices (the banner and
 * the gentle note, `useAccessNotices` with the same page) — read through the same store, target and inputs the
 * Regulations view asks, so both answer alike.
 */
export function useWaterPlace(loader: ReleaseLoader | null, source: Pick<WaterSource, "traceToGauge" | "waterFor">,
                              item: ItemId | null, section: SectionId | null,
                              /** (T517) the parts as the water's map names them (`useMapEntries`) */
                              names: ReadonlyMap<number, string> | null = null) {
  const store = useRegsStore(loader);
  const found = useRegsTarget(store.value ?? null, item, section);
  const inputs = useRegulations(store.value ?? null, found.value ?? null, null);
  const down = useDownstreamWater(source, item, section);
  const inp = inputs.value ?? null;
  const place = useMemo(() => (inp ? headerOf(inp, { today: regsDay(null).today, status: "unknown",
                                                     downstream: down.value?.name ?? null, names }).place : null),
                        [inp, down.value?.name, names]);
  // (T387) the page's subject — its part, never the tap — so this view's notices are its Regulations page's
  const subject = usePageSubject(inp);
  const access = useAccessNotices(loader, store.value ?? null, subject);
  // (T508) SETTLED: the place label and the notices are what they will be (or cannot be read) — what a held pane waits
  // for. A disabled read stays "loading" for good, so each step's own "nothing to ask" is read as settled too.
  const failed = store.state === "failed" || found.state === "failed" || inputs.state === "failed" || access.state === "failed";
  const noTarget = !!store.value && found.state === "ready" && !found.value;
  const settled = !loader || failed || noTarget
    || (inputs.state === "ready" && (subject === null || access.state === "ready") && (item !== null || section === null || down.state !== "loading"));
  return { place, access: access.value ?? [], settled };
}
