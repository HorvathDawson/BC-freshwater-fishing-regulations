/**
 * (T230, owner 2026-10-28) THE PROVINCIAL IN-SEASON CHANGES: the T225 feed (`feeds/inseason/index.json`,
 * `inseason-feed/1`, `app/workers/feeds/src/inseason.ts`) joined to the curated links (`feeds/inseason/matches.json`,
 * `inseason-links/4`, `pipeline.regs.inseason.publish` from `data/curated/inseason/notice_waters.json`).
 *
 * INFORMATIONAL ONLY. A change is shown as a banner on every water (and part) it is matched to while it is in effect;
 * it never changes a computed answer or a status. (A future option, not built: a full closure could set "Check".)
 * A row in effect with NO match yet is listed app-wide (`inSeasonNotice`), so a reader still learns of it.
 *
 * IN EFFECT on a day: the row is on the page (not `removed`), its `from` (if any) is on or before the day and its `to`
 * (if any) on or after it. (T1167, owner 2026-11-25) A notice whose own text states a range whose year reads
 * (`inSeasonDating`: "No fishing July 30 to September 15", posted July 30, 2026) is in force from its first day through
 * its last and shown as UPCOMING the 14 days before (`inSeasonOn`), then not at all. Otherwise `to` is the curated
 * override's, else the feed's — never guessed (owner 2026-11-11, T548: "Never invent an end date"): a row with only a
 * `from` stays in effect until the page drops it, and a range whose year cannot be read is reported. Banner only: the
 * answer and the map's colour stay the book's ("Banner only, but louder").
 *
 * WHERE (T520; T673, owner 2026-11-13: ONE file for every release — "Why not just have a single file that does both?"):
 * per water (its durable item id), `all` or FWA RANGES — stream key + metres along it, never a release, part index or
 * section id. `inSeasonCover` places them over THIS device's release's section lines (`lines`: each section's stream key
 * and [from, to)) — the one core every view of it reads — so a phone on any release reads the same file.
 *
 * Every sentence is composed here, whole (AGENTS 33).
 */
import { MONTHS } from "./calendar.generated";
import { SOURCE_AUTHORITIES } from "./sourceNames";
import { FEED_FORMATS, INSEASON_LEAD_DAYS } from "./feedFormats.generated";
import { endWords, type EndsTable } from "./ends";
import { bookIndex } from "./regs/strip";

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
  /** (T694) the feeds worker's link to the text this row amends: set with status "changed" (`buildFeed`) */
  readonly previous?: { readonly change: string; readonly effective: string };
  /** when the worker first met the row, and when it last changed (ISO) */
  readonly firstSeen?: string;
  readonly changedAt?: string;
}
export interface InSeasonFeed {
  readonly format: typeof INSEASON_FEED_FORMAT;
  readonly fetchedAt: string;
  readonly source: string;
  readonly rows: readonly InSeasonRow[];
  readonly removed: readonly { readonly id: string; readonly removedAt?: string }[];
  /** (T548 item 4) rows of the index this app could not read, dropped (each said in `unreadRows`); absent or 0: none */
  readonly unread?: number;
  /** (T548 item 4) the id (or the position) of each dropped row, for the report */
  readonly unreadRows?: readonly string[];
}
/** (T520, T673) one FWA range: stream key, start, end (metres) — every section whose line [from, to) on that stream
 *  overlaps [m0, m1) (`inSeasonCover`) */
export type InSeasonRun = readonly [number, number, number];
/** (T520) one covered stretch of a water's main line: its two end TOKENS, mouth upward (a split id, a pick-point id,
 *  `mouth`, `source`) — said in words only here (`inSeasonWhere`) */
export type InSeasonEnds = readonly [string, string];
/** (T520) where one notice lands on one water: the whole of it, or its ranges (+ `lakes`, T673: EVERY lake piece of
 *  the river — a lake piece has no line in any release, so no range names one) and its covered stretches' end tokens */
export interface InSeasonWater {
  readonly all?: true;
  readonly runs?: readonly InSeasonRun[];
  readonly lakes?: true;
  readonly ends?: readonly InSeasonEnds[];
}
export interface InSeasonLinks {
  readonly format: typeof INSEASON_LINKS_FORMAT;
  // (T673, inseason-links/4) no release: the ranges are read on whatever release this device holds (`inSeasonCover`).
  // (T517) no words ride in the links: every end token is said through the release's `display.ends` (`ends.ts`)
  readonly notices: Readonly<Record<string, { readonly from?: string; readonly to?: string;
                                              readonly waters: Readonly<Record<string, InSeasonWater>> }>>;
}

const dayOrNull = (v: unknown) => v === undefined || v === null || typeof v === "string";
/** (T548 item 4) a row the app can read: an id, its water and its words; its dates, when given, are text */
function rowReadable(r: unknown): r is InSeasonRow {
  const x = r as Partial<InSeasonRow> | null;
  return !!x && typeof x === "object" && typeof x.id === "string" && typeof x.water === "string"
    && typeof x.change === "string" && dayOrNull(x.from) && dayOrNull(x.to);
}

/**
 * The feed as the app reads it, or null when the INDEX is not one (another format, no time, no rows list). (T548 item 4)
 * A ROW it cannot read is DROPPED, never the whole feed: one bad row must not hide every other notice. The feed says how
 * many it dropped and which (`unread`, `unreadRows`) so the reader reports them and the inbox says so. MORE THAN HALF of
 * the rows unreadable is not a bad row but a feed this app does not read (a change of shape): refused whole (null), so
 * the capture keeps answering and the update service's offer throws, as for a format it does not read.
 */
export function parseInSeasonFeed(x: unknown): InSeasonFeed | null {
  const f = x as Partial<InSeasonFeed> | null;
  if (inSeasonFeedRefusal(x)) return null;
  const rows = (f!.rows as unknown[]).filter(rowReadable);
  const unreadRows = unreadRowIds(f!.rows as unknown[]);
  return { ...f, source: typeof f!.source === "string" ? f!.source : "", removed: f!.removed ?? [], rows,
           ...(unreadRows.length ? { unread: unreadRows.length, unreadRows } : { unread: 0, unreadRows: [] }) } as InSeasonFeed;
}

/** each unreadable row's id (`row <n>` when it has none), in the feed's order */
const unreadRowIds = (rows: readonly unknown[]): string[] => rows.flatMap((r, i) => (rowReadable(r) ? [] : [
  r && typeof r === "object" && typeof (r as { id?: unknown }).id === "string" ? (r as { id: string }).id : `row ${i + 1}`]));

/** the row ids a refusal names before "and n more" */
const REFUSAL_IDS = 5;

/**
 * (T548 L5) WHY `parseInSeasonFeed` REFUSES an index, in words (the report, the source's `failure()`, the update
 * service's error); null: it reads. An index refused because MORE THAN HALF its rows cannot be read says so: how many
 * of how many, and the first few row ids ("not a readable inseason-feed/1 feed" alone did not say it was the rows).
 */
export function inSeasonFeedRefusal(x: unknown): string | null {
  const f = x as Partial<InSeasonFeed> | null;
  if (!f || typeof f !== "object" || Array.isArray(f)) return `not a readable ${INSEASON_FEED_FORMAT} feed (not an object)`;
  if (f.format !== INSEASON_FEED_FORMAT)
    return `not a readable ${INSEASON_FEED_FORMAT} feed (${typeof f.format === "string" ? `format ${f.format}` : "no format"})`;
  if (typeof f.fetchedAt !== "string") return `not a readable ${INSEASON_FEED_FORMAT} feed (no fetchedAt)`;
  if (!Array.isArray(f.rows)) return `not a readable ${INSEASON_FEED_FORMAT} feed (no rows list)`;
  if (!Array.isArray(f.removed ?? [])) return `not a readable ${INSEASON_FEED_FORMAT} feed (removed is not a list)`;
  const rows = inSeasonRowsRefused(f);
  return rows ? INSEASON_FAILURES.rowsRefused(rows.unread, rows.of, rows.ids) : null;
}

/** (T548 L5) the index is refused for its rows (more than half unreadable): how many, of how many, which; else null */
export function inSeasonRowsRefused(x: unknown): { readonly unread: number; readonly of: number; readonly ids: readonly string[] } | null {
  const rows = (x as { rows?: unknown } | null)?.rows;
  if (!Array.isArray(rows)) return null;
  const ids = unreadRowIds(rows);
  return ids.length * 2 > rows.length ? { unread: ids.length, of: rows.length, ids } : null;
}

const isNat = (n: unknown): n is number => Number.isSafeInteger(n) && (n as number) >= 0;
const isRun = (r: unknown): boolean => Array.isArray(r) && r.length === 3 && Number.isSafeInteger(r[0])
  && isNat(r[1]) && isNat(r[2]) && (r[1] as number) < (r[2] as number);
function isWater(w: unknown): w is InSeasonWater {
  const x = w as InSeasonWater | null;
  if (!x || typeof x !== "object") return false;
  if (x.all === true) return x.runs === undefined && x.lakes === undefined && x.ends === undefined;
  const runs = x.runs ?? [], ends = x.ends ?? [];
  if (x.lakes !== undefined && x.lakes !== true) return false;
  return Array.isArray(runs) && Array.isArray(ends) && (runs.length > 0 || x.lakes === true)
    && runs.every(isRun)
    && ends.every((p) => Array.isArray(p) && p.length === 2 && p.every((t) => typeof t === "string" && t.length > 0));
}

/** The links as the app reads them, or null: ANY notice it cannot read refuses the whole file (the caller reports it,
 *  T520, in `INSEASON_FAILURES.unreadableLinks`) — never a narrower or wider reading of it. Another format (an older
 *  file, or a newer one this app was built before) is refused whole. */
export function parseInSeasonLinks(x: unknown): InSeasonLinks | null {
  const l = x as Partial<InSeasonLinks> | null;
  if (!l || typeof l !== "object" || l.format !== INSEASON_LINKS_FORMAT
      || !l.notices || typeof l.notices !== "object") return null;
  for (const n of Object.values(l.notices)) {
    if (!n || typeof n !== "object" || !n.waters || typeof n.waters !== "object") return null;
    const ws = Object.values(n.waters);
    if (!ws.length || !ws.every(isWater)) return null;
    if ((n.from !== undefined && typeof n.from !== "string") || (n.to !== undefined && typeof n.to !== "string")) return null;
  }
  return { format: INSEASON_LINKS_FORMAT, notices: l.notices };
}

/** "2026-07-30" for a local date. */
export function isoDayOf(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

/** (T1167) "2026-07-30" moved `n` days (UTC arithmetic on a calendar day: no clock, no time zone) */
function addDays(day: string, n: number): string {
  const d = new Date(`${day}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
const ISO_DAY = /^\d{4}-\d{2}-\d{2}$/;
/** a calendar day as ISO, or null when the month has no such day */
function isoOf(y: number, m: number, d: number): string | null {
  const t = new Date(Date.UTC(y, m - 1, d));
  return t.getUTCFullYear() === y && t.getUTCMonth() === m - 1 && t.getUTCDate() === d ? t.toISOString().slice(0, 10) : null;
}

/** (T1167) the range a notice states in its own text, with its year (`inSeasonDating`) */
export type InSeasonDating =
  | { readonly kind: "dated"; readonly from: string; readonly to: string }
  | { readonly kind: "undated" }
  | { readonly kind: "unreadable"; readonly why: string };

/**
 * (T1167, owner 2026-11-25) THE RANGE A NOTICE STATES IN ITS OWN TEXT, read with its year. The text's one range is the
 * feeds worker's `parsed.window` (month and day, its `parseChange`: a text with no range, or several, has none, and a
 * time of day — "from 12:00 pm to midnight" — is no range). Its YEAR is read, never guessed: the range must open on the
 * notice's own effective day (`from`: "July 30, 2026" with "No fishing July 30 to September 15"), and its last day is the
 * next one on or after the first (a range over the new year ends the year after). A rescind's quoted range is the
 * rescinded rule's, not the notice's: undated. Anything else that prints a range — no start day, a day the month does
 * not have, no effective day, or a range that does not open on it ("August 15 to December 31" posted Sept 16, a yearly
 * closure) — is UNREADABLE: shown as undated and REPORTED (`inSeasonUnreadRanges`), never guessed.
 * One case file pins this rule and `pipeline.regs.inseason.matches.dating` (`inseason-span.parity.json`).
 */
export function inSeasonDating(row: InSeasonRow): InSeasonDating {
  const w = row.parsed?.window;
  if (!w || row.parsed?.kinds?.includes("rescind")) return { kind: "undated" };
  const { from_month: fm, from_day: fd, to_month: tm, to_day: td } = w;
  if (![fm, fd, tm, td].every((n) => Number.isSafeInteger(n))) return { kind: "unreadable", why: INSEASON_UNDATED.noEnds };
  if (!row.from || !ISO_DAY.test(row.from)) return { kind: "unreadable", why: INSEASON_UNDATED.noEffective };
  const y = Number(row.from.slice(0, 4));
  const first = isoOf(y, fm!, fd!);
  let last = isoOf(y, tm!, td!);
  if (first && last && last < first) last = isoOf(y + 1, tm!, td!);
  if (!first || !last) return { kind: "unreadable", why: INSEASON_UNDATED.noSuchDay };
  if (first !== row.from) return { kind: "unreadable", why: INSEASON_UNDATED.notOnEffective };
  return { kind: "dated", from: first, to: last };
}

/** (T1167) why a notice's range could not be dated: said in the report (`INSEASON_FAILURES.unreadRanges`) */
export const INSEASON_UNDATED = {
  noEnds: "its range has no whole start and end",
  noEffective: "it has no effective day to give its range a year",
  noSuchDay: "its range names a day the month does not have",
  notOnEffective: "its range does not open on its effective day, so its year cannot be read",
} as const;

/** When a row is in force, and whether by a range its own text states (`dated`) */
export interface InSeasonSpan { readonly from: string | null; readonly to: string | null; readonly dated: boolean }

/**
 * When a row is in force: the curated override first, then (T1167) the range the notice states in its own text
 * (`inSeasonDating`), then the feed's own dates. A row with no readable range keeps the rule of 2026-11-11 (T548, "A
 * notice stops showing when the Province drops it. Never invent an end date"): in force from its `from` until the page
 * drops it or a printed / curated `to` passes. One case file pins this rule and `pipeline.regs.inseason.matches.span`
 * (`inseason-span.parity.json`).
 */
export function inSeasonSpan(row: InSeasonRow, link?: { from?: string; to?: string }): InSeasonSpan {
  const d = inSeasonDating(row), dated = d.kind === "dated";
  return { from: link?.from ?? (dated ? d.from : row.from) ?? null, to: link?.to ?? (dated ? d.to : row.to) ?? null, dated };
}

/** (T1167) a notice's banner on a day: in force, upcoming (the lead days before a dated notice's first day), or none */
export type InSeasonPhase = "upcoming" | "in force";

/**
 * (T1167, owner 2026-11-25 "2 weeks before to the end (Recommended)") A ROW'S BANNER ON `day` ("YYYY-MM-DD", BC's day,
 * the caller's `bcNow()`): "in force" from its first day through its last (inclusive), "upcoming" in the
 * `INSEASON_LEAD_DAYS` before the first day of a DATED row, null otherwise (not yet, or over). Pinned with
 * `pipeline.regs.inseason.matches.on` by `inseason-span.parity.json`.
 */
export function inSeasonOn(span: InSeasonSpan, day: string): InSeasonPhase | null {
  if (span.to && span.to < day) return null;
  if (span.from && span.from > day) return span.dated && addDays(span.from, -INSEASON_LEAD_DAYS) <= day ? "upcoming" : null;
  return "in force";
}

/**
 * (T1167) The rows whose text prints a range this app could not date (`inSeasonDating` unreadable), each with why —
 * shown as undated and REPORTED (`INSEASON_FAILURES.unreadRanges`). A row the curator gave dates (`effective` in
 * notice_waters.json, the links' from / to) is dated by them, so it is not one; nor is a removed row.
 */
export function inSeasonUnreadRanges(feed: InSeasonFeed, links: InSeasonLinks | null): { readonly id: string; readonly why: string }[] {
  const gone = new Set(feed.removed.map((r) => r.id));
  return feed.rows.flatMap((r) => {
    const l = links?.notices[r.id], d = inSeasonDating(r);
    return d.kind === "unreadable" && !gone.has(r.id) && !l?.from && !l?.to ? [{ id: r.id, why: d.why }] : [];
  });
}

export interface InSeasonChange {
  readonly id: string;
  readonly region: string;
  readonly water: string;
  readonly text: string;
  readonly from: string | null;
  readonly to: string | null;
  /** (T1167) dated by the range its own text states (`inSeasonSpan`) */
  readonly dated: boolean;
  /** (T1167) on the day joined: in force, or upcoming (`inSeasonOn`) */
  readonly phase: InSeasonPhase;
  readonly source: string;
  /** where it lands, per water (`InSeasonWater`); null = not matched yet */
  readonly waters: Readonly<Record<string, InSeasonWater>> | null;
}


/**
 * Every row of the feed in effect on `day` ("YYYY-MM-DD"), split into matched and not. Removed rows are never in.
 * (T1167) A dated row is in from `INSEASON_LEAD_DAYS` before its first day, as "upcoming" (`phase`, `inSeasonOn`).
 * (T983) `ended`: the ids of the rows still on the page whose `to` has passed (begun and over). The inbox counts them as
 * met (`inboxMerge`'s `known`), so a row that comes back into force because the in-force rule or a curated date moved
 * (as T548 did to the seven "July 30 to September 15" closures in 0.8.4) is not news.
 *
 * (T693, owner 2026-11-12 "Fix gap 1 only") `absent`: the water ids the links name that THIS device's release does not
 * hold (the one links file may be newer than the release, T673). A notice whose every water is absent has no page on this phone, so
 * it is NOT matched here: it falls into the bell as "not on the map yet", like an unmatched one (`waters` null). One with
 * some water this release holds stays matched (its pages show it); `inSeasonAbsentWaters` names the rest for the report.
 */
export function joinInSeason(feed: InSeasonFeed | null, links: InSeasonLinks | null, day: string,
                             absent: ReadonlySet<string> | null = null):
  { matched: InSeasonChange[]; unmatched: InSeasonChange[]; ended: string[] } {
  const matched: InSeasonChange[] = [], unmatched: InSeasonChange[] = [], ended: string[] = [];
  if (!feed) return { matched, unmatched, ended };
  const gone = new Set(feed.removed.map((r) => r.id));
  for (const r of feed.rows) {
    if (gone.has(r.id)) continue;
    const link = links?.notices[r.id];
    const span = inSeasonSpan(r, link);
    if (span.to && span.to < day) { ended.push(r.id); continue; }
    const phase = inSeasonOn(span, day);
    if (!phase) continue;
    const here = link?.waters ?? null;
    const held = here !== null && !(absent && Object.keys(here).length > 0 && Object.keys(here).every((i) => absent.has(i)));
    const c: InSeasonChange = { id: r.id, region: r.region, water: r.water, text: r.change, ...span, phase,
                                source: feed.source, waters: held ? here : null };
    (c.waters ? matched : unmatched).push(c);
  }
  return { matched, unmatched, ended };
}

/** (T693) The water ids a matched change names that this release does not hold (`absent`): what the report names. */
export function inSeasonAbsentWaters(c: { readonly waters: InSeasonChange["waters"] }, absent: ReadonlySet<string>): string[] {
  return Object.keys(c.waters ?? {}).filter((i) => absent.has(i)).sort();
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

/** One section of a water as `inSeasonCover` reads it: its handle and its line — stream key and [from, to) metres, `to`
 *  `LINE_OPEN` (`@app/data`, 0xFFFFFFFF) for a stream's one section (the release's `lines`) — or null: a lake piece, no line. */
export interface InSeasonSection {
  readonly sid: number;
  readonly line: { readonly blk: number; readonly from: number; readonly to: number } | null;
}

/** (T673) THE RULE, one section: its line [from, to) overlaps the range [m0, m1) on the range's stream — wholly or
 *  partly inside. A section of no length counts by its start; an open end runs to the stream's end. One fixture pins
 *  it here and in `pipeline.regs.inseason.resolve.section_touched` (`inseason-cover.parity.json`). */
export function inSeasonTouches(line: { readonly blk: number; readonly from: number; readonly to: number },
                                [blk, m0, m1]: InSeasonRun): boolean {
  return line.blk === blk && line.from < m1 && m0 < Math.max(line.to, line.from + 1);
}

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
  noLines: "in-season: this release's section lines could not be read",
  nothing: (id: string, item: string) => `in-season: ${id}'s ranges touch no section of ${item} in this release`,
  /** (T673) `matches.json` came back in a format this app does not read (an older file, or a newer one) */
  unreadableLinks: (seen: unknown) =>
    `in-season matches.json: ${typeof seen === "string" ? seen : "no format"}, but this app reads ${INSEASON_LINKS_FORMAT} — refused (matched notices fall back to the last links read, else to the list of changes not on the map)`,
  /** (T517) an end the links name that this release's `display.ends` does not (publish refuses such links) */
  noWords: (id: string, e: string) => `in-season: ${id}: ${e}`,
  /** (T548 item 4) rows of the index this app could not read: dropped, the rest shown */
  /** (T1167) rows whose text prints a range this app could not date (`inSeasonUnreadRanges`): shown as undated, until
   *  the page drops them — a curated `effective` in notice_waters.json dates one */
  unreadRanges: (rows: readonly { readonly id: string; readonly why: string }[]) =>
    `in-season index.json: ${rows.length} notice${rows.length === 1 ? "" : "s"} print${rows.length === 1 ? "s" : ""} a range this app could not date (${rows.map((r) => `${r.id}: ${r.why}`).join("; ")}) — shown until the page drops ${rows.length === 1 ? "it" : "them"}, never ended by a guess`,
  unreadRows: (rows: readonly string[]) =>
    `in-season index.json: ${rows.length} row${rows.length === 1 ? "" : "s"} this app could not read (${rows.join(", ")}) — dropped, the other changes shown`,
  /** (T548 L5) more than half the rows could not be read: the index is refused whole (a change of shape, not a bad row) */
  rowsRefused: (unread: number, of: number, ids: readonly string[]) =>
    `not a readable ${INSEASON_FEED_FORMAT} feed: ${unread} of ${of} rows could not be read (${ids.slice(0, REFUSAL_IDS).join(", ")}` +
    `${ids.length > REFUSAL_IDS ? ` and ${ids.length - REFUSAL_IDS} more` : ""}), more than half`,
  /** (T693) the links match a notice to waters this device's release does not hold: it is in the bell, not on a page */
  absentWaters: (id: string, items: readonly string[]) =>
    `in-season: ${id} is matched to ${items.join(", ")}, which this release does not hold (shown in the bell, not on a page)`,
  /** (T693) the release could not be asked whether it holds a matched water: the notice stays as matched */
  noHolds: (e: string) => `in-season: could not ask this release which matched waters it holds: ${e}`,
  /** (T517) the release's `display.ends` could not be read: no "Where" can be said */
  noTable: "in-season: this release's display.ends could not be read",
  /** (T700) this device's notification history could not be read: it starts again (a first read) */
  unreadableInbox: (e: string) => `in-season inbox: the saved history could not be read (${e}) — started again`,
  /** (T700) this device's notification history could not be saved: what was read may show as unread again */
  inboxWrite: (e: string) => `in-season inbox: the history could not be saved (${e})`,
} as const;

/**
 * (T520) THE ONE CORE OF "WHERE IS THIS CHANGE": change `c` on water `item`, on a page holding `page` (null: the whole
 * water's page) of the water's sections `water` (every one of them, with its line; null: they could not be read).
 * Null: the change is not on this page. The banner, the KeepTab line, the water map's highlight and the screen reader
 * all read this.
 *
 * `all`: every section, no ends. Ranges (T673, on ANY release): a section is covered when its line TOUCHES a range on
 * its stream (`inSeasonTouches`: wholly or partly inside — a notice's stretch end need not sit on a section boundary of
 * this release, and a section it reaches into shows it, never drops it); a lake piece (no line) when `lakes`. A
 * covered stretch that is not the whole page keeps its "Where: from A to B" words. With no lines, or touching nothing,
 * the change is on the whole water with its ends and a `failure`.
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
  if (!water) return { sections: null, where, failure: INSEASON_FAILURES.noLines, whereFailure };
  const runs = w.runs ?? [];
  const hit = water.filter((s) => (s.line ? runs.some((r) => inSeasonTouches(s.line!, r)) : w.lakes === true))
    .map((s) => s.sid).sort((a, b) => a - b);
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
  /** (T1031 item 3) the notice's page, as its Source line names it (`tabSources.inSeasonSourcesOf`) */
  source: `${SOURCE_AUTHORITIES.inseason.name} ↗`,
  see: "see ›",
  /** (T518) where on the page's water a change covering only part of it applies (`inSeasonWhere`) */
  where: (where: string) => `Where: ${where}`,
  informational: "Shown as posted; the answers on this page do not include it yet.",
  noticeClose: "Got it",
  /** (T694) the mark an unread notice carries on its water's page and in the inbox */
  marks: { new: "New", updated: "Updated" },
  /** (T1167) a dated notice in the lead days before its first day: its banner's head word */
  upcoming: "Upcoming in-season change",
  inForce: "In-season change",
  /** (T1167) the several notices' line counts the upcoming ones: "3 in-season changes (1 upcoming) · see ›" */
  upcomingCount: (n: number) => `${n} upcoming`,
  /** (T1167, "Banner only, but louder") the Keep strip's status line while a notice is in force on the page: the answer
   *  stays the book's, and points at the notice above it */
  statusSee: (n: number) => (n === 1 ? "See the in-season notice above" : `See the ${n} in-season notices above`),
  /** (T1167) the whole-year bars' legend for a dated notice's range */
  yearMark: (dates: string) => `In-season notice, ${dates}`,
  yearMarks: "In-season notices",
} as const;

export interface InSeasonBannerVM {
  readonly id: string;
  /** the whole line: "In-season change (Oct 15–Nov 30): <verbatim text> — B.C. in-season changes ↗" */
  readonly line: string;
  readonly head: string;
  readonly text: string;
  readonly source: string;
  readonly url: string;
  /** (T518) "Where: From A to B" when the change covers only part of the page; null: all of it */
  readonly where: string | null;
  readonly note: string;
  /** (T694) "New" / "Updated": an unread notice on this device; null: none */
  readonly mark: string | null;
  /** (T1167) in force, or upcoming (the lead days before a dated notice's first day) */
  readonly phase: InSeasonPhase;
  /** (T1167) the range its own text states, when it is dated (`inSeasonSpan`): what the whole-year bars mark */
  readonly window: { readonly from: string; readonly to: string } | null;
}

/**
 * (T472, owner 2026-11-07) THE COLLAPSED LINE of a page's in-season changes, drawn just above the species table: one change
 * says its head ("In-season change (since May 16, 2025) · see ›"), several say how many ("3 in-season changes · see ›").
 * The full notices open under it on a press. Nothing in effect: null.
 */
export function inSeasonSummary(banners: readonly InSeasonBannerVM[]): string | null {
  if (!banners.length) return null;
  if (banners.length === 1) {
    const b = banners[0]!;
    return `${b.mark ? `${b.mark} · ` : ""}${b.head} · ${INSEASON_WORDS.see}`;
  }
  // (T694) "3 in-season changes (1 new, 1 updated) · see ›"; (T1167) "(1 new, 1 upcoming)"
  const n = (m: string) => banners.filter((b) => b.mark === m).length;
  const marks = [[n(INSEASON_WORDS.marks.new), "new"], [n(INSEASON_WORDS.marks.updated), "updated"]] as const;
  const soon = banners.filter((b) => b.phase === "upcoming").length;
  const said = [...marks.filter(([k]) => k > 0).map(([k, w]) => `${k} ${w}`),
                ...(soon ? [INSEASON_WORDS.upcomingCount(soon)] : [])].join(", ");
  return `${banners.length} in-season changes${said ? ` (${said})` : ""} · ${INSEASON_WORDS.see}`;
}

/** (T694) `mark`: the notice is unread on this device, as it arrived (core `inboxMark`) */
export function inSeasonBanner(change: InSeasonChange, cover: Pick<InSeasonCover, "where">,
                               mark: "new" | "updated" | null = null): InSeasonBannerVM {
  const dates = inSeasonDates(change.from, change.to);
  // (T1167) the lead days before a dated notice: "Upcoming in-season change (Jul 30–Sep 15)"
  const head = `${change.phase === "upcoming" ? INSEASON_WORDS.upcoming : INSEASON_WORDS.inForce}${dates ? ` (${dates})` : ""}`;
  const where = cover.where ? INSEASON_WORDS.where(cover.where) : null;
  const m = mark ? INSEASON_WORDS.marks[mark] : null;
  return { id: change.id, head, text: change.text, source: INSEASON_WORDS.source, url: change.source,
           line: `${m ? `${m} · ` : ""}${head}: ${change.text}${where ? ` ${where}.` : ""} — ${INSEASON_WORDS.source}`,
           where, note: INSEASON_WORDS.informational, mark: m, phase: change.phase,
           window: change.dated && change.from && change.to ? { from: change.from, to: change.to } : null };
}

/**
 * (T1167, owner 2026-11-25 "Banner only, but louder") THE KEEP STRIP'S POINTER while a notice is IN FORCE on the page:
 * "See the in-season notice above" (the banners sit above the answer). The answer and the map's colour stay the book's;
 * an upcoming notice points nowhere (its banner says it is coming). None in force: null.
 */
export function inSeasonStatusLine(banners: readonly InSeasonBannerVM[]): string | null {
  const n = banners.filter((b) => b.phase === "in force" && b.text.trim()).length;
  return n ? INSEASON_WORDS.statusSee(n) : null;
}

/** (T1167) what the whole-year bars mark: each dated notice's range as book-year runs (fractions of the 365 days, a range
 *  over the new year in two), and the legend's words */
export interface InSeasonYearVM { readonly runs: readonly { readonly at: number; readonly len: number }[]; readonly label: string }

const BOOK = 365;
const dayIx = (iso: string) => bookIndex({ month: Number(iso.slice(5, 7)), day: Number(iso.slice(8, 10)) });

/**
 * (T1167) "Whole-year calendars mark the window": the ranges of the page's DATED notices (upcoming or in force) on the
 * book year every bar draws (365 days, Feb 29 read as Feb 28), merged where they overlap; a range of a year or more is
 * the whole bar. No dated notice: null.
 */
export function inSeasonYearMarks(banners: readonly InSeasonBannerVM[]): InSeasonYearVM | null {
  const dated = banners.filter((b) => b.window && b.text.trim());
  if (!dated.length) return null;
  const spans: [number, number][] = [];
  for (const b of dated) {
    const { from, to } = b.window!;
    const a = dayIx(from), z = dayIx(to);
    if (addDays(from, BOOK - 1) <= to) spans.push([0, BOOK]);
    else if (a <= z && to.slice(0, 4) === from.slice(0, 4)) spans.push([a, z + 1]);
    else spans.push([a, BOOK], [0, z + 1]);
  }
  spans.sort((x, y) => x[0] - y[0]);
  const merged: [number, number][] = [];
  for (const [a, z] of spans) {
    const l = merged[merged.length - 1];
    if (l && a <= l[1]) l[1] = Math.max(l[1], z); else merged.push([a, z]);
  }
  const one = dated.length === 1 ? inSeasonDates(dated[0]!.window!.from, dated[0]!.window!.to) : null;
  return { runs: merged.map(([a, z]) => ({ at: a / BOOK, len: (z - a) / BOOK })),
           label: one ? INSEASON_WORDS.yearMark(one) : INSEASON_WORDS.yearMarks };
}

/** "7a" -> "Region 7A": a region code as every surface names it (T622: the public site's water pages read it too) */
export const regionWords = (r: string) => (/^\d[AB]?$/i.test(r) ? `Region ${r.toUpperCase()}` : r === "provincial" ? "Provincial" : r);
