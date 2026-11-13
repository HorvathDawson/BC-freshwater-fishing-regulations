/**
 * (T230) THE PROVINCIAL IN-SEASON CHANGES, as hooks: the feed + links read once per poll (`useInSeason`, the Shell's),
 * handed down by `InSeasonContext`; WHERE each lands on a page (`useInSeasonCovers`, T520: core's one `inSeasonCover`
 * over the water's section lines — the banners, the KeepTab line and the water map's highlight all read it); the
 * banners on one Regulations page (`useInSeasonBanners`, with T694's "New"/"Updated" marks); the device's history of
 * notices, the bell's inbox (`useInSeasonInbox`, T700: it opens by itself once for a notice not on the map yet, and the
 * bell opens it). Every word is core's (`inSeasonBanner`, `inboxVM`, `INSEASON_WORDS`, `INBOX_WORDS`).
 *
 * Informational only: nothing here touches an answer, a status or the map's colours.
 */
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { INBOX_WORDS, INSEASON_FAILURES, inSeasonAbsentWaters, inSeasonBanner, inSeasonCover, reportFailure, isoDayOf, joinInSeason,
         inboxDetail, inboxMark, inboxMerge, inboxPrompted, inboxRead, inboxReadAll, inboxTarget, inboxText, inboxUnread,
         inboxVM, inboxWantsOpen, parseInbox,
         type InboxArrival, type InboxDetailVM, type InboxState, type InboxVM,
         type InSeasonBannerVM, type InSeasonChange, type InSeasonCover, type InSeasonSection,
         bcNow } from "@app/core";
import type { InSeasonData, InSeasonSource } from "@app/data";
import type { RegsStore } from "@app/data/regs";
import { openLineReads, type LineReads } from "@app/data/regs/place";
import type { ReleaseLoader } from "@app/data/release";
import { useAsync, usePoll } from "./async";

const ids = new WeakMap<object, number>();
let nextId = 0;
const idOf = (o: object): number => { let i = ids.get(o); if (i === undefined) ids.set(o, i = ++nextId); return i; };

/** How often the feed is asked again while the app is open (the source's own cache decides whether it fetches). */
export const INSEASON_POLL_MS = 60 * 60_000;

export interface InSeasonNow {
  readonly matched: readonly InSeasonChange[];
  readonly unmatched: readonly InSeasonChange[];
  /** the feed + links read (null: none yet, or none captured) — the day's join is per page (`useInSeasonBanners`) */
  readonly data: InSeasonData | null;
  /** this device's data release (T693: which matched waters it holds is asked per release) */
  readonly release: string | null;
  /** (T700) why the newest feed could not be read (the source's `failure()`, also reported), or null */
  readonly failure?: string | null;
}

const NONE: InSeasonNow = { matched: [], unmatched: [], data: null, release: null };
export const InSeasonContext = createContext<InSeasonNow>(NONE);

/** (T693) Whether this device's release holds a water, by its item id (the release's `core.item`, `itemOrd`). */
export type HoldsWater = (item: string) => Promise<boolean>;

/**
 * The feed + links, joined for TODAY (the notice's day). A page joins again for its own day.
 *
 * (T693, owner 2026-11-12 "Fix gap 1 only") `holds`: the links may name a water this release does not have (a newer
 * links file, T673), and that notice would sit on no page and in no bell. Each matched water is asked once per release;
 * a notice whose every water is absent falls into the bell like an unmatched one, and is REPORTED once. No `holds` (no
 * release open): nothing to ask. A failed ask is reported and the notices stay matched — never dropped.
 */
export function useInSeason(source: InSeasonSource | null | undefined, epoch = 0,
                            release: string | null = null, holds: HoldsWater | null = null): InSeasonNow {
  const tick = usePoll(INSEASON_POLL_MS, !!source);
  const [data, setData] = useState<InSeasonData | null>(null);
  const [failure, setFailure] = useState<string | null>(null);
  // (T520) the device's release changed (a data update applied): the links are asked again (T673: the same file is
  // read on any release, but which waters it holds and their section lines are the new release's)
  const heldRelease = useRef(release);
  useEffect(() => {
    let live = true;
    if (heldRelease.current !== release) { heldRelease.current = release; source?.refresh?.(); }
    // (T368) a failure is reported (shown by the update service's own check of this feed, the sync badge); the last
    // read stays — offline is not a failure here (the source answers its capture)
    if (source) void source.latest().then((d) => {
      if (!live) return;
      if (d) setData(d);
      setFailure(source.failure?.() ?? null);
    }, reportFailure);
    return () => { live = false; };
    // (T231) `epoch`: bumped when the update service sees the feed move (the source was told to forget it)
  }, [source, tick, epoch, release]);
  const today = isoDayOf(bcNow());
  const base = useMemo(() => (data ? joinInSeason(data.feed, data.links, today) : null), [data, today]);
  const waters = useMemo(() => [...new Set((base?.matched ?? []).flatMap((c) => Object.keys(c.waters ?? {})))].sort(), [base]);
  const key = `inseason-holds:${release ?? ""}:${waters.join(",")}`;
  const asked = useAsync(async () => {
    const gone = new Set<string>();
    // a failed ask is reported by `useAsync` (once), in this sentence; the notices stay matched meanwhile
    try { for (const w of waters) if (!(await holds!(w))) gone.add(w); }
    catch (e) { throw new Error(INSEASON_FAILURES.noHolds(e instanceof Error ? e.message : String(e))); }
    return gone;
  }, key, !!holds && waters.length > 0);
  const absent = asked.state === "ready" && asked.value.size ? asked.value : null;
  const out = useMemo(() => (data && base
    ? { ...(absent ? joinInSeason(data.feed, data.links, today, absent) : base), data, release, failure }
    : failure ? { ...NONE, failure } : NONE),
    [data, base, absent, today, release, failure]);
  useEffect(() => {
    if (!absent) return;
    for (const c of base?.matched ?? []) {
      const items = inSeasonAbsentWaters(c, absent);
      const k = `absent|${c.id}|${items.join(",")}|${release}`;
      if (!items.length || TOLD.has(k)) continue;
      TOLD.add(k);
      reportFailure(new Error(INSEASON_FAILURES.absentWaters(c.id, items)));
    }
  }, [base, absent, release]);
  return out;
}

export interface InSeasonPage {
  readonly item: string;
  readonly ord: number;
  /** (T517) the page's parts: every piece of the map entry holding the page's part (a live-joined entry is several),
   *  so its coverage is the entry's; null: the whole water's page */
  readonly pieces: readonly number[] | null;
}

/**
 * (T517) THE IN-SEASON PAGE OF A REGULATIONS PAGE: its water and the pieces of the map entry holding its part (`entries`,
 * `useMapEntries`: every piece of a live-joined entry), so the banner covers what the map lights. Its own part alone —
 * a documented rule — before the map's group is read (the entry it is before a salmon reading joins any), and for a
 * part no entry holds (a tidal piece: never listed on the map). Null: no named water on the page.
 */
export function useInSeasonPage(item: { readonly id: string; readonly ord: number } | null, partIx: number | null,
                                entries: { readonly pieces: ReadonlyMap<number, readonly number[]> } | null): InSeasonPage | null {
  const pieces = partIx === null ? null : entries?.pieces.get(partIx) ?? [partIx];
  const key = pieces ? pieces.join(",") : "";
  // eslint-disable-next-line react-hooks/exhaustive-deps -- the page's identity: its water and its pieces
  return useMemo(() => (item && pieces ? { item: item.id, ord: item.ord, pieces } : null), [item?.id, item?.ord, key]);
}

/** One change on one page, and where on it (`inSeasonCover`). */
export interface InSeasonOnPage { readonly change: InSeasonChange; readonly cover: InSeasonCover }

/** a water's section lines, tagged with the water they were read for: `useAsync` keeps the previous key's value while
 *  the next loads, so an answer for another water is never placed (Fraser's lines read as Pitt's) */
type WaterLines = { readonly ord: number; readonly water: readonly InSeasonSection[];
                    readonly partOf: ReadonlyMap<number, number> };

/** (no silent fallbacks) what was reported, once per app run: a change shown whole because it could not be placed. */
const TOLD = new Set<string>();

/**
 * (T520) THE CHANGES ON ONE PAGE AND WHERE, for its day (null: today) — the one load path every view of them reads.
 * The water's section lines (`navRows` + the release's `lines`) are read only when a change on it covers part of it on
 * this release (T673: any release — the ranges are stream key + metres, placed on THIS release's lines). A failure (lines
 * unreadable, ranges touching nothing) shows the change on the whole water with its ends, and is REPORTED (once per
 * change, water and reason).
 */
export function useInSeasonCovers(store: Pick<RegsStore, "navRows" | "statics"> | null,
                                  lines: Pick<LineReads, "hasLines" | "sectionLine"> | null,
                                  page: InSeasonPage | null, day: Date | null): readonly InSeasonOnPage[] {
  const { data } = useContext(InSeasonContext);
  const iso = isoDayOf(day ?? bcNow());
  const matched = useMemo(() => (data ? joinInSeason(data.feed, data.links, iso).matched : []), [data, iso]);
  const here = useMemo(() => (page ? matched.filter((c) => c.waters?.[page.item]) : []), [matched, page?.item]);
  const needs = !!page && here.some((c) => !c.waters![page.item]!.all);
  const read = useAsync<WaterLines>(
    async () => {
      if (!(await lines!.hasLines())) throw new Error("in-season: this release has no lines file");
      const nav = await store!.navRows(page!.ord);
      const water = await Promise.all(nav.sections.map(async (s) => {
        const l = await lines!.sectionLine(s.sid);
        return { sid: s.sid, line: l ? { blk: l.blk, from: l.from, to: l.to } : null };
      }));
      return { ord: page!.ord, water, partOf: new Map(nav.sections.map((s) => [s.sid, s.partIx] as const)) };
    },
    `inseason-lines:${page?.ord ?? ""}`, needs && !!store && !!lines);
  // (T517) the release's `display.ends`: the words "Where" says (the statics are read once per store, prewarmed). One
  // that could not be read never hides a notice: the changes are shown with no "Where", and that is reported
  const ends = useAsync(async () => (await store!.statics()).ends, `inseason-ends:${store ? idOf(store) : ""}`,
                        !!store && here.length > 0);
  const table = ends.state === "ready" ? ends.value : null;
  const tableWait = ends.state === "loading" && !!store && here.length > 0;
  const pieces = page?.pieces ? page.pieces.join(",") : null;
  const out = useMemo(() => {
    const got = read.state === "ready" && read.value.ord === page?.ord ? read.value : null;
    if (!page || tableWait || (needs && (read.state === "loading" || (read.state === "ready" && !got)))) return [];
    const mine = page.pieces ? new Set(page.pieces) : null;
    const onPage = got && mine ? new Set([...got.partOf].filter(([, ix]) => mine.has(ix)).map(([sid]) => sid)) : null;
    const res: InSeasonOnPage[] = [];
    for (const c of here) {
      const cover = inSeasonCover(c, page.item, got?.water ?? null, onPage, table);
      if (cover) res.push({ change: c, cover });
    }
    return res;
  }, [page?.item, pieces, here, needs, read.state, read.state === "ready" ? read.value : null, table, tableWait]);
  // (no silent fallbacks) a change shown on the whole water because it could not be placed is also reported
  useEffect(() => {
    for (const p of out) {
      // (T517) a "Where" that could not be said: the line is gone, the notice and its sections stay — reported
      const wf = p.cover.whereFailure;
      if (wf && !TOLD.has(`${wf}|${p.change.id}|${page?.item}`)) {
        TOLD.add(`${wf}|${p.change.id}|${page?.item}`);
        reportFailure(new Error(wf));
      }
      const f = p.cover.failure;
      if (!f) continue;
      const k = `${f}|${p.change.id}|${page?.item}`;
      if (TOLD.has(k)) continue;
      TOLD.add(k);
      reportFailure(new Error(f));
    }
  }, [out]);
  return out;
}

/** (T520) The open release's section lines, once per release: what `useInSeasonCovers` places a range on. */
export function useReleaseLines(loader: Pick<ReleaseLoader, "manifest" | "db"> | null): LineReads | null {
  return useMemo(() => (loader ? openLineReads(loader) : null), [loader]);
}

/**
 * The banners on one Regulations page, for its day (null: today): `useInSeasonCovers`, worded.
 *
 * (T694) An unread notice carries its "New" / "Updated" mark (the inbox's, `InSeasonInboxContext`). Shown on the page,
 * it is SEEN: read in the inbox at once, while this page keeps the mark it was opened with (taken once per notice per
 * water), so the reader is not shown a mark that vanishes under them.
 */
export function useInSeasonBanners(store: Pick<RegsStore, "navRows" | "statics"> | null,
                                   lines: Pick<LineReads, "hasLines" | "sectionLine"> | null,
                                   page: InSeasonPage | null, day: Date | null): readonly InSeasonBannerVM[] {
  const covers = useInSeasonCovers(store, lines, page, day);
  const inbox = useContext(InSeasonInboxContext);
  const marks = useRef<{ item: string | null; of: Map<string, InboxArrival | null> }>({ item: null, of: new Map() });
  const banners = useMemo(() => {
    if (marks.current.item !== (page?.item ?? null)) marks.current = { item: page?.item ?? null, of: new Map() };
    const of = marks.current.of;
    for (const p of covers) if (!of.has(p.change.id)) of.set(p.change.id, inbox?.markOf(p.change.id) ?? null);
    return covers.map((p) => inSeasonBanner(p.change, p.cover, of.get(p.change.id) ?? null));
  }, [covers, inbox, page?.item]);
  const marked = banners.filter((b) => b.mark).map((b) => b.id).join(",");
  useEffect(() => { if (marked && inbox) inbox.read(marked.split(",")); }, [marked, inbox]);
  return banners;
}

/** (T700) Whether an inbox row opened this page at one of these notices: true once (the banners open). */
export function useInSeasonFocus(ids: readonly string[]): boolean {
  const inbox = useContext(InSeasonInboxContext);
  const [on, setOn] = useState(false);
  const key = ids.join(",");
  useEffect(() => { if (key && inbox?.takeFocus(key.split(","))) setOn(true); }, [key, inbox]);
  return on;
}

/** (T518) The sections the water's map outlines for the changes on it today: each change's own sections, or (`all`,
 *  or a failure) every section of the water (`all`). */
export function inSeasonSections(covers: readonly InSeasonOnPage[], all: readonly number[]): number[] {
  const out = new Set<number>();
  for (const p of covers) for (const s of p.cover.sections ?? all) out.add(s);
  return [...out].sort((a, b) => a - b);
}

export interface InSeasonSeenStore { read(): string | null; write(text: string): void }

/** (T230) the ids T230's notice showed on this device: read once, to start the T700 history quiet for them */
function readSeen(store: InSeasonSeenStore | null | undefined): Set<string> {
  if (!store) return new Set();
  try { const t = store.read(); const j = t ? JSON.parse(t) : []; return new Set(Array.isArray(j) ? j.map(String) : []); }
  catch (e) { reportFailure(e); return new Set(); }
}

/** (T694) What a Regulations page asks of the inbox: a notice's mark, reading it, and the notice a row opened it at. */
export interface InSeasonInboxReads {
  markOf(id: string): InboxArrival | null;
  read(ids: readonly string[]): void;
  /** the notice an inbox row opened its water at (its banner opens), once */
  takeFocus(ids: readonly string[]): boolean;
}
export const InSeasonInboxContext = createContext<InSeasonInboxReads | null>(null);

export interface InSeasonInboxState {
  /** the list, grouped "New" then "Earlier" (core `inboxVM`) */
  readonly vm: InboxVM;
  /** a notice opened that has no one page to go to: its text (core `inboxDetail`); null: the list */
  readonly detail: InboxDetailVM | null;
  /** the feed could not be read: the inbox's line (`INBOX_WORDS.failure`), else null */
  readonly failure: string | null;
  /** the bell's in-season count (unread) and its words */
  readonly count: number;
  readonly label: string | null;
  /** it opened by itself (an unread notice not on the map yet it has not opened for) */
  readonly open: boolean;
  /** a row tapped: read; its one water's page (`onOpenWater`), else its text */
  pick(id: string): void;
  back(): void;
  readAll(): void;
  /** the sheet closed: back to the list, and what it opened by itself for is not opened for again */
  close(): void;
  readonly reads: InSeasonInboxReads;
}

/**
 * (T700, T694) THE IN-SEASON INBOX: this device's history of in-season notices (core `inboxMerge` after every feed read),
 * kept in `store` (per device, nothing sent). Unread count for the bell; "New"/"Updated" marks for the pages; it opens by
 * itself once for a notice not on the map yet (as T230's notice did) — the same sheet the bell opens. A history that
 * cannot be read or saved is REPORTED (`INSEASON_FAILURES.unreadableInbox` / `inboxWrite`), never dropped quietly.
 */
export function useInSeasonInbox(now: InSeasonNow, store: InSeasonSeenStore | null | undefined,
                                 seenStore: InSeasonSeenStore | null | undefined,
                                 onOpenWater: (item: string, id: string) => void): InSeasonInboxState {
  const [state, setState] = useState<InboxState | null>(() => {
    if (!store) return null;
    try { return parseInbox(store.read()); }
    catch (e) {
      reportFailure(new Error(INSEASON_FAILURES.unreadableInbox(e instanceof Error ? e.message : String(e))));
      return null;
    }
  });
  const [detail, setDetail] = useState<string | null>(null);
  const [opened, setOpened] = useState(false);
  /** the notice a row opened its water's page at, and the mark it had: the page shows it once though it is read now */
  const focus = useRef<{ id: string; mark: InboxArrival | null } | null>(null);
  const today = isoDayOf(bcNow());
  // the latest history, ahead of the render: two changes in one press (read, then prompted) build on each other
  const stateRef = useRef(state);
  const save = useCallback((next: InboxState) => {
    stateRef.current = next;
    setState(next);
    if (!store) return;
    try { store.write(inboxText(next)); }
    catch (e) { reportFailure(new Error(INSEASON_FAILURES.inboxWrite(e instanceof Error ? e.message : String(e)))); }
  }, [store]);
  // every feed read (and every change of which waters this release holds) folds into the history
  useEffect(() => {
    if (!now.data) return;
    const was = stateRef.current;
    const next = inboxMerge(was, now.data.feed, now, today, was ? new Set() : readSeen(seenStore));
    if (!was || inboxText(next) !== inboxText(was)) save(next);
  }, [now, today, save, seenStore]);
  useEffect(() => { if (inboxWantsOpen(state)) setOpened(true); }, [state]);
  const update = useCallback((f: (s: InboxState) => InboxState) => {
    const s = stateRef.current;
    if (!s) return;
    const next = f(s);
    if (inboxText(next) !== inboxText(s)) save(next);
  }, [save]);
  const close = useCallback(() => {
    setOpened(false); setDetail(null);
    update(inboxPrompted);
  }, [update]);
  const pick = useCallback((id: string) => {
    const s = stateRef.current;
    update((x) => inboxRead(x, [id]));
    const to = inboxTarget(s, id);
    if (!to) { setDetail(id); return; }
    focus.current = { id, mark: inboxMark(s, id) };
    close();
    onOpenWater(to.item, id);
  }, [update, close, onOpenWater]);
  const reads = useMemo<InSeasonInboxReads>(() => ({
    markOf: (id) => (focus.current?.id === id ? focus.current.mark : inboxMark(stateRef.current, id)),
    read: (ids) => update((x) => inboxRead(x, ids)),
    takeFocus: (ids) => {
      if (focus.current === null || !ids.includes(focus.current.id)) return false;
      focus.current = null;
      return true;
    },
  }), [update]);
  const vm = useMemo(() => inboxVM(state, today), [state, today]);
  const count = inboxUnread(state);
  return {
    vm, detail: detail ? inboxDetail(state, detail, today) : null,
    failure: now.failure ? INBOX_WORDS.failure : null,
    count, label: count ? INBOX_WORDS.badge(count) : null,
    open: opened, pick, back: () => setDetail(null), readAll: () => update(inboxReadAll), close, reads,
  };
}
