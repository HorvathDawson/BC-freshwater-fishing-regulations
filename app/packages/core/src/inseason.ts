/**
 * (T230, owner 2026-10-28) THE PROVINCIAL IN-SEASON CHANGES: the T225 feed (`feeds/inseason/index.json`,
 * `inseason-feed/1`, `app/workers/feeds/src/inseason.ts`) joined to the curated links (`feeds/inseason/matches.json`,
 * `inseason-links/3`, `pipeline.regs.inseason.publish` from `data/curated/inseason/notice_waters.json`).
 *
 * INFORMATIONAL ONLY. A change is shown as a banner on every water (and part) it is matched to while it is in effect;
 * it never changes a computed answer or a status. (A future option, not built: a full closure could set "Check".)
 * A row in effect with NO match yet is listed app-wide (`inSeasonNotice`), so a reader still learns of it.
 *
 * IN EFFECT on a day: the row is on the page (not `removed`), its `from` (if any) is on or before the day and its `to`
 * (if any) on or after it. `to` is the curated override's, else the feed's, else — for a change whose text prints ONE
 * date range (`parsed.window`) that OPENS on its `from` day (a one-off, T548) — that range's end. A row with only a
 * `from` (a yearly window included) stays in effect until the page drops it.
 *
 * WHERE (T520, links/2): per water, `all` or FWA RANGES resolved on one release where the atlas is (never a part index
 * or a section id); `inSeasonCover` reads them over the water's section lines — the one core every view of it reads.
 *
 * Every sentence is composed here, whole (AGENTS 33).
 */
import { MONTHS } from "./calendar.generated";
import { FEED_FORMATS } from "./feedFormats.generated";
import { MENU_WORDS } from "./menu/words";
import { endWords, type EndsTable } from "./ends";

// (T556) generated from the Python writers' one table (pipeline/common/feed_formats.py): never a literal here
export const INSEASON_FEED_FORMAT = FEED_FORMATS.inseasonFeed;
export const INSEASON_LINKS_FORMAT = FEED_FORMATS.inseasonLinks;

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
/** (T520) one covered stretch of a water's main line: its two end TOKENS, mouth upward (a split id, a pick-point id,
 *  `mouth`, `source`) — said in words only here (`inSeasonWhere`) */
export type InSeasonEnds = readonly [string, string];
/** (T520) where one notice lands on one water: the whole of it, or its ranges (+ a river's covered lake pieces, by
 *  their place among the water's lake pieces in handle order) and its covered stretches' end tokens */
export interface InSeasonWater {
  readonly all?: true;
  readonly runs?: readonly InSeasonRun[];
  readonly lakes?: readonly number[];
  readonly ends?: readonly InSeasonEnds[];
}
export interface InSeasonLinks {
  readonly format: typeof INSEASON_LINKS_FORMAT;
  /** the data release the ranges are resolved on (`pipeline.regs.inseason.resolve`); read only there (AGENTS 6) */
  readonly release: string;
  // (T517, inseason-links/3) no words ride in the links: every end token is said through the release's `display.ends`
  // (`ends.ts`), which holds every curated notice's ends (publish refuses one the release lacks)
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
  if (x.all === true) return x.runs === undefined && x.lakes === undefined && x.ends === undefined;
  const runs = x.runs ?? [], lakes = x.lakes ?? [], ends = x.ends ?? [];
  return Array.isArray(runs) && Array.isArray(lakes) && Array.isArray(ends) && runs.length + lakes.length > 0
    && runs.every(isRun) && lakes.every(isNat)
    && ends.every((p) => Array.isArray(p) && p.length === 2 && p.every((t) => typeof t === "string" && t.length > 0));
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

/**
 * When a row is in force: the curated override, else the feed's dates, else — ONLY for a one-off window — its one
 * printed window's end. (T548) A window is one-off when it OPENS on the row's `from` day ("July 30, 2026" / "No fishing
 * July 30 to September 15"): its end in the `from` year (rolling on over the new year). Any other printed window is a
 * yearly season the notice sets ("Sept 16, 2025" / "... August 15 to December 31"): no end is inferred, and the row
 * stays in force until the page drops it or a printed / curated `to` passes. One case file pins this rule and
 * `pipeline.regs.inseason.matches.span` (`inseason-span.parity.json`).
 */
export function inSeasonSpan(row: InSeasonRow, link?: { from?: string; to?: string }): { from: string | null; to: string | null } {
  const from = link?.from ?? row.from ?? null;
  let to = link?.to ?? row.to ?? null;
  const w = row.parsed?.window;
  const kinds = row.parsed?.kinds ?? [];
  if (!to && from && w && w.to_month && w.to_day && w.from_month && w.from_day
      && from.slice(5) === `${pad(w.from_month)}-${pad(w.from_day)}`
      && !kinds.includes("rescind") && !kinds.includes("clarification")) {
    const y = Number(from.slice(0, 4));
    const end = `${y}-${pad(w.to_month)}-${pad(w.to_day)}`;
    to = end < from ? `${y + 1}-${end.slice(5)}` : end;
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
  /** the links' release (null: no links) — an off-release failure names it once */
  readonly linksRelease: string | null;
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
                                source: feed.source, waters: link?.waters ?? null, onRelease,
                                linksRelease: links?.release ?? null };
    (c.waters ? matched : unmatched).push(c);
  }
  return { matched, unmatched };
}

const MOUTH = "mouth", SOURCE = "source";

/**
 * (T518, T520) WHERE ON THE WATER, AS A SENTENCE — composed here only (AGENTS 33) from the covered stretches' end
 * tokens: "From A to B", "Upstream of A" (to the source), "Downstream of B" (from the mouth); several stretches joined
 * by "; ". The whole main line (mouth to source): null. (T517) The words are the release's `display.ends` through the
 * one end resolver every part name uses (`ends.ts` `endWords`): a token this release does not hold throws, naming it.
 */
export function inSeasonWhere(ends: readonly InSeasonEnds[], table: EndsTable): string | null {
  const lo = (t: string) => endWords(table, t, "lo"), hi = (t: string) => endWords(table, t, "hi");
  const parts = ends.map(([a, b]) => (a === MOUTH && b === SOURCE ? null
    : a === MOUTH ? `Downstream of ${hi(b)}` : b === SOURCE ? `Upstream of ${lo(a)}` : `From ${lo(a)} to ${hi(b)}`))
    .filter((x): x is string => x !== null);
  return parts.length ? parts.join("; ") : null;
}

/** One section of a water as `inSeasonCover` reads it: its handle and its line (null: a lake piece, no line). */
export interface InSeasonSection { readonly sid: number; readonly line: { readonly blk: number; readonly from: number } | null }

/** Where one change lands on one page. */
export interface InSeasonCover {
  /** the page's covered sections, in handle order; null: the whole page (the whole water, or — `failure` — not
   *  placeable here, so shown on the whole water) */
  readonly sections: readonly number[] | null;
  /** where on the water ("From Spuzzum Creek confluence to Hell's Gate", `inSeasonWhere`) when it covers only some of
   *  the page; null: all of it */
  readonly where: string | null;
  /** why its ranges could not be placed on this device (shown on the whole water AND reported by the caller) */
  readonly failure: string | null;
  /** (T517) why "Where" could not be said (an end this release's `display.ends` does not hold, or the table could not
   *  be read): ONLY the "Where" line goes — the sections stay — and the caller reports it */
  readonly whereFailure: string | null;
}

export const INSEASON_FAILURES = {
  offRelease: "in-season: the ranges are resolved on another data release than this device's",
  noLines: "in-season: this release's section lines could not be read",
  nothing: (id: string, item: string) => `in-season: ${id}'s ranges land on no section of ${item} in this release`,
  /** (T517) an end the links name that this release's `display.ends` does not (publish refuses such links) */
  noWords: (id: string, e: string) => `in-season: ${id}: ${e}`,
  /** (T517) the release's `display.ends` could not be read: no "Where" can be said */
  noTable: "in-season: this release's display.ends could not be read",
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
                              page: ReadonlySet<number> | null, table: EndsTable | null): InSeasonCover | null {
  const w = c.waters?.[item];
  if (!w) return null;
  if (w.all) return { sections: null, where: null, failure: null, whereFailure: null };
  // the words of its ends: the release's `display.ends` only (T517). An end it cannot say — or no table (`null`: it
  // could not be read) — drops ONLY the "Where" line, and the caller reports it; the sections are placed as ever
  let where: string | null = null, whereFailure: string | null = null;
  if (!table) whereFailure = INSEASON_FAILURES.noTable;
  else {
    try { where = inSeasonWhere(w.ends ?? [], table); }
    catch (e) { whereFailure = INSEASON_FAILURES.noWords(c.id, e instanceof Error ? e.message : String(e)); }
  }
  if (!c.onRelease) return { sections: null, where, failure: INSEASON_FAILURES.offRelease, whereFailure };
  if (!water) return { sections: null, where, failure: INSEASON_FAILURES.noLines, whereFailure };
  const runs = w.runs ?? [];
  const lakes = water.filter((s) => !s.line).map((s) => s.sid).sort((a, b) => a - b);
  const lakeHit = new Set((w.lakes ?? []).map((i) => lakes[i]).filter((s): s is number => s !== undefined));
  const hit = water.filter((s) => (s.line
    ? runs.some(([b, m0, m1]) => b === s.line!.blk && m0 <= s.line!.from && s.line!.from < m1)
    : lakeHit.has(s.sid))).map((s) => s.sid).sort((a, b) => a - b);
  if (!hit.length) return { sections: null, where, failure: INSEASON_FAILURES.nothing(c.id, item), whereFailure };
  const mine = page ? hit.filter((s) => page.has(s)) : hit;
  if (!mine.length) return null;
  const size = page ? page.size : water.length;
  return { sections: mine, where: mine.length === size ? null : where, failure: null, whereFailure };
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
  /** (T518) where on the page's water a change covering only part of it applies (`inSeasonWhere`) */
  where: (where: string) => `Where: ${where}`,
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

export function inSeasonBanner(change: InSeasonChange, cover: Pick<InSeasonCover, "where">): InSeasonBannerVM {
  const dates = inSeasonDates(change.from, change.to);
  const head = `In-season change${dates ? ` (${dates})` : ""}`;
  const where = cover.where ? INSEASON_WORDS.where(cover.where) : null;
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

/** "7a" -> "Region 7A": a region code as every surface names it (T622: the public site's water pages read it too) */
export const regionWords = (r: string) => (/^\d[AB]?$/i.test(r) ? `Region ${r.toUpperCase()}` : r === "provincial" ? "Provincial" : r);

export function inSeasonNoticeRows(unmatched: readonly InSeasonChange[]): InSeasonNoticeRow[] {
  return unmatched.map((c) => ({ id: c.id, where: `${regionWords(c.region)} · ${c.water}`,
                                 dates: inSeasonDates(c.from, c.to), text: c.text, url: c.source }));
}

/** The ids the notice should open for on load: unmatched rows in effect this device has not been shown. */
export function inSeasonUnseen(unmatched: readonly InSeasonChange[], seen: ReadonlySet<string>): string[] {
  return unmatched.map((c) => c.id).filter((id) => !seen.has(id));
}
