/**
 * (T230) THE PROVINCIAL IN-SEASON CHANGES, as hooks: the feed + links read once per poll (`useInSeason`, the Shell's),
 * handed down by `InSeasonContext`; the banners on one Regulations page (`useInSeasonBanners`); the app-wide notice of
 * the changes not matched to a water yet (`useInSeasonNotice`: it opens by itself once per row per device, and its
 * badge re-opens it). Every word is core's (`inSeasonBanner`, `inSeasonNoticeRows`, `INSEASON_WORDS`).
 *
 * Informational only: nothing here touches an answer, a status or the map's colours.
 */
import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import { INSEASON_WORDS, inSeasonBanner, inSeasonNoticeRows, inSeasonOn, inSeasonUnseen, isoDayOf, joinInSeason,
         type InSeasonBannerVM, type InSeasonChange, type InSeasonMarks, type InSeasonNoticeRow } from "@app/core";
import type { InSeasonData, InSeasonSource } from "@app/data";
import type { RegsStore } from "@app/data/regs";
import { useAsync, usePoll } from "./async";

/** How often the feed is asked again while the app is open (the source's own cache decides whether it fetches). */
export const INSEASON_POLL_MS = 60 * 60_000;

export interface InSeasonNow {
  readonly matched: readonly InSeasonChange[];
  readonly unmatched: readonly InSeasonChange[];
  /** the feed + links read (null: none yet, or none captured) — the day's join is per page (`useInSeasonBanners`) */
  readonly data: InSeasonData | null;
}

const NONE: InSeasonNow = { matched: [], unmatched: [], data: null };
export const InSeasonContext = createContext<InSeasonNow>(NONE);

/** The feed + links, joined for TODAY (the notice's day). A page joins again for its own day. */
export function useInSeason(source: InSeasonSource | null | undefined, epoch = 0): InSeasonNow {
  const tick = usePoll(INSEASON_POLL_MS, !!source);
  const [data, setData] = useState<InSeasonData | null>(null);
  useEffect(() => {
    let live = true;
    if (source) void source.latest().then((d) => { if (live && d) setData(d); }, () => {});
    return () => { live = false; };
    // (T231) `epoch`: bumped when the update service sees the feed move (the source was told to forget it)
  }, [source, tick, epoch]);
  const today = isoDayOf(new Date());
  return useMemo(() => (data ? { ...joinInSeason(data.feed, data.links, today), data } : NONE), [data, today]);
}

export interface InSeasonPage {
  readonly item: string;
  readonly ord: number;
  /** null: the whole water's page */
  readonly partIx: number | null;
  readonly parts: number;
}

/** The banners on one Regulations page, for its day (null: today). Its water's part marks are read once per water. */
export function useInSeasonBanners(store: Pick<RegsStore, "navRows"> | null, page: InSeasonPage | null,
                                   day: Date | null): readonly InSeasonBannerVM[] {
  const { data } = useContext(InSeasonContext);
  const iso = isoDayOf(day ?? new Date());
  const matched = useMemo(() => (data ? joinInSeason(data.feed, data.links, iso).matched : []), [data, iso]);
  const here = page ? matched.filter((c) => c.extents.some((e) => e.item === page.item)) : [];
  const needsMarks = !!store && !!page && here.some((c) => c.extents.some((e) => e.item === page.item && e.op !== "whole"));
  const marks = useAsync<InSeasonMarks | null>(
    async () => {
      const nav = await store!.navRows(page!.ord);
      return { parts: page!.parts, marks: nav.marks.map((m) => ({ label: m.label, down: m.down, up: m.up })) };
    },
    `inseason-marks:${page?.ord ?? ""}:${page?.parts ?? ""}`, needsMarks);
  return useMemo(() => (page
    ? inSeasonOn(here, page.item, page.partIx, marks.state === "ready" ? marks.value : null).map(inSeasonBanner)
    : []),
  // the page, the day's matches, the marks
  [page?.item, page?.partIx, matched, marks.state, marks.value]);
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
