/**
 * (T230, owner 2026-10-28) THE PROVINCIAL IN-SEASON CHANGES: the T225 feed (`feeds/inseason/index.json`,
 * `inseason-feed/1`, `app/workers/feeds/src/inseason.ts`) joined to the curated links (`feeds/inseason/matches.json`,
 * `inseason-links/2`, `pipeline.regs.inseason.publish` from `data/curated/inseason/notice_waters.json`).
 *
 * INFORMATIONAL ONLY. A change is shown as a banner on every water (and part) it is matched to while it is in effect;
 * it never changes a computed answer or a status. (A future option, not built: a full closure could set "Check".)
 * A row in effect with NO match yet is listed app-wide (`inSeasonNotice`), so a reader still learns of it.
 *
 * IN EFFECT on a day: the row is on the page (not `removed`), its `from` (if any) is on or before the day and its `to`
 * (if any) on or after it. `to` is the curated override's, else the feed's, else — for a change whose text prints ONE
 * date range (`parsed.window`) — that range's end in the `from` year (rolling to the next year when it ends before it
 * starts). A row with only a `from` stays in effect until the page drops it.
 *
 * WHERE (T520, links/2): per water, `all` or FWA RANGES resolved on one release where the atlas is (never a part index
 * or a section id); `inSeasonCover` reads them over the water's section lines — the one core every view of it reads.
 *
 * Every sentence is composed here, whole (AGENTS 33).
 */
import { MONTHS } from "./calendar.generated";
import { MENU_WORDS } from "./menu/words";

export const INSEASON_FEED_FORMAT = "inseason-feed/1";
export const INSEASON_LINKS_FORMAT = "inseason-links/2";

export interface InSeasonRow {
  readonly id: string;
  readonly region: string;
  readonly section?: string;
  readonly water: string;
  readonly effective: string;
  readonly from: string | null;
  readonly to: string | null;
  readonly change: string;
  readonly parsed?: { readonly kinds?: readonly string[];
                      readonly window?: { readonly from_month?: number; readonly from_day?: number;
                                          readonly to_month?: number; readonly to_day?: number } | null };
  readonly status?: string;
}
export interface InSeasonFeed {
  readonly format: typeof INSEASON_FEED_FORMAT;
  readonly fetchedAt: string;
  readonly source: string;
  readonly rows: readonly InSeasonRow[];
  readonly removed: readonly { readonly id: string }[];
}
/** (T520) one FWA range: stream key, start, end (metres) — the sections whose START lies in [m0, m1) on that stream */
export type InSeasonRun = readonly [number, number, number];
/** (T520) where one notice lands on one water: the whole of it, or its ranges (+ a river's covered lake pieces, by
 *  their place among the water's lake pieces in handle order) and the notice's own ends in words */
export interface InSeasonWater {
  readonly all?: true;
  readonly runs?: readonly InSeasonRun[];
  readonly lakes?: readonly number[];
  readonly ends?: string;
}
export interface InSeasonLinks {
  readonly format: typeof INSEASON_LINKS_FORMAT;
  /** the data release the ranges are resolved on (`pipeline.regs.inseason.resolve`); read only there (AGENTS 6) */
  readonly release: string;
  readonly notices: Readonly<Record<string, { readonly from?: string; readonly to?: string;
                                              readonly waters: Readonly<Record<string, InSeasonWater>> }>>;
}

/** The feed as the app reads it, or null (never a half-read feed). */
export function parseInSeasonFeed(x: unknown): InSeasonFeed | null {
  const f = x as Partial<InSeasonFeed> | null;
  if (!f || typeof f !== "object" || f.format !== INSEASON_FEED_FORMAT || typeof f.fetchedAt !== "string") return null;
  if (!Array.isArray(f.rows) || !Array.isArray(f.removed ?? [])) return null;
  for (const r of f.rows)
    if (!r || typeof r.id !== "string" || typeof r.water !== "string" || typeof r.change !== "string") return null;
  return { ...f, source: typeof f.source === "string" ? f.source : "", removed: f.removed ?? [] } as InSeasonFeed;
}

const isNat = (n: unknown): n is number => Number.isSafeInteger(n) && (n as number) >= 0;
const isRun = (r: unknown): boolean => Array.isArray(r) && r.length === 3 && Number.isSafeInteger(r[0])
  && isNat(r[1]) && isNat(r[2]) && (r[1] as number) < (r[2] as number);
function isWater(w: unknown): w is InSeasonWater {
  const x = w as InSeasonWater | null;
  if (!x || typeof x !== "object") return false;
  if (x.all === true) return x.runs === undefined && x.lakes === undefined;
  const runs = x.runs ?? [], lakes = x.lakes ?? [];
  return Array.isArray(runs) && Array.isArray(lakes) && runs.length + lakes.length > 0 && runs.every(isRun)
    && lakes.every(isNat) && typeof x.ends === "string" && x.ends.length > 0;
}

/** The links as the app reads them, or null: ANY notice it cannot read refuses the whole file (the caller reports it,
 *  T520) — never a narrower or wider reading of it. */
export function parseInSeasonLinks(x: unknown): InSeasonLinks | null {
  const l = x as Partial<InSeasonLinks> | null;
  if (!l || typeof l !== "object" || l.format !== INSEASON_LINKS_FORMAT || typeof l.release !== "string"
      || !l.notices || typeof l.notices !== "object") return null;
  for (const n of Object.values(l.notices)) {
    if (!n || typeof n !== "object" || !n.waters || typeof n.waters !== "object") return null;
    const ws = Object.values(n.waters);
    if (!ws.length || !ws.every(isWater)) return null;
    if ((n.from !== undefined && typeof n.from !== "string") || (n.to !== undefined && typeof n.to !== "string")) return null;
  }
  return { format: INSEASON_LINKS_FORMAT, release: l.release, notices: l.notices };
}

/** "2026-07-30" for a local date. */
export function isoDayOf(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

const pad = (n: number) => String(n).padStart(2, "0");

/** When a row is in force: the curated override, else the feed's dates, else its one printed window's end. */
export function inSeasonSpan(row: InSeasonRow, link?: { from?: string; to?: string }): { from: string | null; to: string | null } {
  const from = link?.from ?? row.from ?? null;
  let to = link?.to ?? row.to ?? null;
  const w = row.parsed?.window;
  const kinds = row.parsed?.kinds ?? [];
  if (!to && from && w && w.to_month && w.to_day && !kinds.includes("rescind") && !kinds.includes("clarification")) {
    const y = Number(from.slice(0, 4));
    let end = `${y}-${pad(w.to_month)}-${pad(w.to_day)}`;
    if (w.from_month && w.from_day && end < `${y}-${pad(w.from_month)}-${pad(w.from_day)}`) end = `${y + 1}-${end.slice(5)}`;
    if (end >= from) to = end;
  }
  return { from, to };
}

export interface InSeasonChange {
  readonly id: string;
  readonly region: string;
  readonly water: string;
  readonly text: string;
  readonly from: string | null;
  readonly to: string | null;
  readonly source: string;
  /** where it lands, per water (`InSeasonWater`); null = not matched yet */
  readonly waters: Readonly<Record<string, InSeasonWater>> | null;
  /** the links' release is this device's: only then are `runs` / `lakes` read (an `all` water holds anywhere) */
  readonly onRelease: boolean;
}

/** The waters a matched change is on. */
export function inSeasonWaters(c: InSeasonChange): string[] {
  return Object.keys(c.waters ?? {});
}

/** Every row of the feed in effect on `day` ("YYYY-MM-DD"), split into matched and not. Removed rows are never in. */
export function joinInSeason(feed: InSeasonFeed | null, links: InSeasonLinks | null, day: string,
                             release: string | null = null):
  { matched: InSeasonChange[]; unmatched: InSeasonChange[] } {
  const matched: InSeasonChange[] = [], unmatched: InSeasonChange[] = [];
  if (!feed) return { matched, unmatched };
  const gone = new Set(feed.removed.map((r) => r.id));
  const onRelease = !!links && links.release === release;
  for (const r of feed.rows) {
    if (gone.has(r.id)) continue;
    const link = links?.notices[r.id];
    const span = inSeasonSpan(r, link);
    if ((span.from && span.from > day) || (span.to && span.to < day)) continue;
    const c: InSeasonChange = { id: r.id, region: r.region, water: r.water, text: r.change, ...span,
                                source: feed.source, waters: link?.waters ?? null, onRelease };
    (c.waters ? matched : unmatched).push(c);
  }
  return { matched, unmatched };
}

/** One section of a water as `inSeasonCover` reads it: its handle and its line (null: a lake piece, no line). */
export interface InSeasonSection { readonly sid: number; readonly line: { readonly blk: number; readonly from: number } | null }

/** Where one change lands on one page. */
export interface InSeasonCover {
  /** the page's covered sections, in handle order; null: the whole page (the whole water, or — `failure` — not
   *  placeable here, so shown on the whole water) */
  readonly sections: readonly number[] | null;
  /** the change's own ends ("From Hell's Gate to Spuzzum Creek confluence") when it covers only some of the page */
  readonly ends: string | null;
  /** why its ranges could not be placed on this device (shown on the whole water AND reported by the caller) */
  readonly failure: string | null;
}

export const INSEASON_FAILURES = {
  offRelease: "in-season: the ranges are resolved on another data release than this device's",
  noLines: "in-season: this release's section lines could not be read",
  nothing: (id: string, item: string) => `in-season: ${id}'s ranges land on no section of ${item} in this release`,
} as const;

/**
 * (T520) THE ONE CORE OF "WHERE IS THIS CHANGE": change `c` on water `item`, on a page holding `page` (null: the whole
 * water's page) of the water's sections `water` (every one of them, with its line; null: they could not be read).
 * Null: the change is not on this page. The banner, the KeepTab line, the water map's highlight and the screen reader
 * all read this.
 *
 * `all`: every section, no ends. Ranges: a section is covered when its start lies in [m0, m1) on its stream (a lake
 * piece: its place among the water's lake pieces is in `lakes`) — read ONLY on the release the links name. Off that
 * release, with no lines, or landing on nothing, the change is on the whole water with its ends and a `failure`.
 */
export function inSeasonCover(c: InSeasonChange, item: string, water: readonly InSeasonSection[] | null,
                              page: ReadonlySet<number> | null): InSeasonCover | null {
  const w = c.waters?.[item];
  if (!w) return null;
  if (w.all) return { sections: null, ends: null, failure: null };
  const ends = w.ends ?? null;
  if (!c.onRelease) return { sections: null, ends, failure: INSEASON_FAILURES.offRelease };
  if (!water) return { sections: null, ends, failure: INSEASON_FAILURES.noLines };
  const runs = w.runs ?? [];
  const lakes = water.filter((s) => !s.line).map((s) => s.sid).sort((a, b) => a - b);
  const lakeHit = new Set((w.lakes ?? []).map((i) => lakes[i]).filter((s): s is number => s !== undefined));
  const hit = water.filter((s) => (s.line
    ? runs.some(([b, m0, m1]) => b === s.line!.blk && m0 <= s.line!.from && s.line!.from < m1)
    : lakeHit.has(s.sid))).map((s) => s.sid).sort((a, b) => a - b);
  if (!hit.length) return { sections: null, ends, failure: INSEASON_FAILURES.nothing(c.id, item) };
  const mine = page ? hit.filter((s) => page.has(s)) : hit;
  if (!mine.length) return null;
  const size = page ? page.size : water.length;
  return { sections: mine, ends: mine.length === size ? null : ends, failure: null };
}

function dayWords(iso: string, year: boolean): string {
  const [y, m, d] = iso.split("-").map(Number);
  return `${MONTHS[m! - 1]} ${d}${year ? `, ${y}` : ""}`;
}

/** "Oct 15–Nov 30" (one year), "Sep 16, 2025–Jan 31, 2026", "since Apr 1, 2026", "until Sep 15, 2026", or null. */
export function inSeasonDates(from: string | null, to: string | null): string | null {
  if (from && to) return from.slice(0, 4) === to.slice(0, 4)
    ? `${dayWords(from, false)}–${dayWords(to, false)}` : `${dayWords(from, true)}–${dayWords(to, true)}`;
  if (from) return `since ${dayWords(from, true)}`;
  if (to) return `until ${dayWords(to, true)}`;
  return null;
}

export const INSEASON_WORDS = {
  source: "source ›",
  see: "see ›",
  sourceLabel: "Open the province's in-season changes page",
  /** (T518) where on the page's water a change covering only part of it applies: its own ends */
  where: (ends: string) => `Where: ${ends}`,
  informational: "Shown as posted; the answers on this page do not include it yet.",
  noticeTitle: MENU_WORDS.sources.names.inseason,
  noticeIntro: "The province has posted changes we have not placed on the map yet. Check each one before you fish.",
  noticeClose: "Got it",
  badge: (n: number) => `${n} in-season ${n === 1 ? "change" : "changes"} not on the map yet`,
} as const;

export interface InSeasonBannerVM {
  readonly id: string;
  /** the whole line: "In-season change (Oct 15–Nov 30): <verbatim text> — source ›" */
  readonly line: string;
  readonly head: string;
  readonly text: string;
  readonly source: string;
  readonly url: string;
  /** (T518) "Where: From A to B" when the change covers only part of the page; null: all of it */
  readonly where: string | null;
  readonly note: string;
}

/**
 * (T472, owner 2026-11-07) THE COLLAPSED LINE of a page's in-season changes, drawn just above the species table: one change
 * says its head ("In-season change (since May 16, 2025) · see ›"), several say how many ("3 in-season changes · see ›").
 * The full notices open under it on a press. Nothing in effect: null.
 */
export function inSeasonSummary(banners: readonly InSeasonBannerVM[]): string | null {
  if (!banners.length) return null;
  return `${banners.length === 1 ? banners[0]!.head : `${banners.length} in-season changes`} · ${INSEASON_WORDS.see}`;
}

export function inSeasonBanner(change: InSeasonChange, cover: Pick<InSeasonCover, "ends">): InSeasonBannerVM {
  const dates = inSeasonDates(change.from, change.to);
  const head = `In-season change${dates ? ` (${dates})` : ""}`;
  const where = cover.ends ? INSEASON_WORDS.where(cover.ends) : null;
  return { id: change.id, head, text: change.text, source: INSEASON_WORDS.source, url: change.source,
           line: `${head}: ${change.text}${where ? ` ${where}.` : ""} — ${INSEASON_WORDS.source}`,
           where, note: INSEASON_WORDS.informational };
}

export interface InSeasonNoticeRow {
  readonly id: string;
  /** "Region 4 · Kaslo River" — the region and the water as the province printed them */
  readonly where: string;
  readonly dates: string | null;
  readonly text: string;
  readonly url: string;
}

const regionWords = (r: string) => (/^\d[AB]?$/i.test(r) ? `Region ${r.toUpperCase()}` : r === "provincial" ? "Provincial" : r);

export function inSeasonNoticeRows(unmatched: readonly InSeasonChange[]): InSeasonNoticeRow[] {
  return unmatched.map((c) => ({ id: c.id, where: `${regionWords(c.region)} · ${c.water}`,
                                 dates: inSeasonDates(c.from, c.to), text: c.text, url: c.source }));
}

/** The ids the notice should open for on load: unmatched rows in effect this device has not been shown. */
export function inSeasonUnseen(unmatched: readonly InSeasonChange[], seen: ReadonlySet<string>): string[] {
  return unmatched.map((c) => c.id).filter((id) => !seen.has(id));
}
