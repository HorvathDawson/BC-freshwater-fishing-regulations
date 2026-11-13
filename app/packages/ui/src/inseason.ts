/**
 * (T230) THE PROVINCIAL IN-SEASON CHANGES, as hooks: the feed + links read once per poll (`useInSeason`, the Shell's),
 * handed down by `InSeasonContext`; WHERE each lands on a page (`useInSeasonCovers`, T520: core's one `inSeasonCover`
 * over the water's section lines — the banners, the KeepTab line and the water map's highlight all read it); the
 * banners on one Regulations page (`useInSeasonBanners`); the app-wide notice of
 * the changes not matched to a water yet (`useInSeasonNotice`: it opens by itself once per row per device, and its
 * badge re-opens it). Every word is core's (`inSeasonBanner`, `inSeasonNoticeRows`, `INSEASON_WORDS`).
 *
 * Informational only: nothing here touches an answer, a status or the map's colours.
 */
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { INSEASON_FAILURES, INSEASON_WORDS, inSeasonAbsentWaters, inSeasonBanner, inSeasonCover, reportFailure, inSeasonNoticeRows, inSeasonUnseen, isoDayOf, joinInSeason,
         type InSeasonBannerVM, type InSeasonChange, type InSeasonCover, type InSeasonNoticeRow, type InSeasonSection,
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
  // (T520) the device's release changed (a data update applied): the links are asked again (T673: the same file is
  // read on any release, but which waters it holds and their section lines are the new release's)
  const heldRelease = useRef(release);
  useEffect(() => {
    let live = true;
    if (heldRelease.current !== release) { heldRelease.current = release; source?.refresh?.(); }
    // (T368) a failure is reported (shown by the update service's own check of this feed, the sync badge); the last
    // read stays — offline is not a failure here (the source answers its capture)
    if (source) void source.latest().then((d) => { if (live && d) setData(d); }, reportFailure);
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
    ? { ...(absent ? joinInSeason(data.feed, data.links, today, absent) : base), data, release } : NONE),
    [data, base, absent, today, release]);
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

/** The banners on one Regulations page, for its day (null: today): `useInSeasonCovers`, worded. */
export function useInSeasonBanners(store: Pick<RegsStore, "navRows" | "statics"> | null,
                                   lines: Pick<LineReads, "hasLines" | "sectionLine"> | null,
                                   page: InSeasonPage | null, day: Date | null): readonly InSeasonBannerVM[] {
  const covers = useInSeasonCovers(store, lines, page, day);
  return useMemo(() => covers.map((p) => inSeasonBanner(p.change, p.cover)), [covers]);
}

/** (T518) The sections the water's map outlines for the changes on it today: each change's own sections, or (`all`,
 *  or a failure) every section of the water (`all`). */
export function inSeasonSections(covers: readonly InSeasonOnPage[], all: readonly number[]): number[] {
  const out = new Set<number>();
  for (const p of covers) for (const s of p.cover.sections ?? all) out.add(s);
  return [...out].sort((a, b) => a - b);
}

export interface InSeasonSeenStore { read(): string | null; write(text: string): void }

export interface InSeasonNoticeState {
  /** the unmatched changes in effect today, worded */
  readonly rows: readonly InSeasonNoticeRow[];
  /** the badge's count (0: no badge) and its words */
  readonly count: number;
  readonly label: string;
  readonly open: boolean;
  show(): void;
  /** close, and remember every row shown as seen on this device */
  close(): void;
}

function readSeen(store: InSeasonSeenStore | null | undefined): Set<string> {
  try { const t = store?.read(); const j = t ? JSON.parse(t) : []; return new Set(Array.isArray(j) ? j.map(String) : []); }
  catch { return new Set(); }
}

/**
 * THE APP-WIDE NOTICE: opens by itself when a row in effect with no match yet has not been shown on this device; the
 * badge (count) re-opens it any time; a row leaves it by itself once it is matched or no longer in effect.
 */
export function useInSeasonNotice(unmatched: readonly InSeasonChange[],
                                  store?: InSeasonSeenStore | null): InSeasonNoticeState {
  const [open, setOpen] = useState(false);
  const rows = useMemo(() => inSeasonNoticeRows(unmatched), [unmatched]);
  const ids = rows.map((r) => r.id).join(",");
  useEffect(() => {
    if (rows.length && inSeasonUnseen(unmatched, readSeen(store)).length) setOpen(true);
  // once per set of ids
  }, [ids]);
  const close = useCallback(() => {
    setOpen(false);
    const seen = readSeen(store);
    for (const r of rows) seen.add(r.id);
    try { store?.write(JSON.stringify([...seen].sort())); } catch { /* a preference: it only opens once more */ }
  }, [rows, store]);
  const show = useCallback(() => setOpen(true), []);
  return { rows, count: rows.length, label: INSEASON_WORDS.badge(rows.length), open: open && rows.length > 0, show, close };
}
