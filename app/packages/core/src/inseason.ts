/**
 * (T230, owner 2026-10-28) THE PROVINCIAL IN-SEASON CHANGES: the T225 feed (`feeds/inseason/index.json`,
 * `inseason-feed/1`, `app/workers/feeds/src/inseason.ts`) joined to the curated links (`feeds/inseason/matches.json`,
 * `inseason-links/1`, `pipeline.regs.inseason.publish` from `data/curated/inseason/matches.json`).
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
 * WHERE: a link names its water by `item` and its cut(s) by the split LABEL the release's `part_mark.label` carries
 * (`inSeasonParts`). A label that is not a part boundary of the water in this release is not guessed: the banner goes
 * on the whole water and says the change is on part of it (`partial`).
 *
 * Every sentence is composed here, whole (AGENTS 33).
 */
import { MONTHS } from "./calendar.generated";

export const INSEASON_FEED_FORMAT = "inseason-feed/1";
export const INSEASON_LINKS_FORMAT = "inseason-links/1";

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
export interface InSeasonExtent {
  readonly item: string;
  readonly op: "whole" | "upstream_of" | "downstream_of" | "between";
  /** the split labels, as the release's part marks carry them (1 for up/downstream_of, 2 for between) */
  readonly at?: readonly string[];
}
export interface InSeasonLinks {
  readonly format: typeof INSEASON_LINKS_FORMAT;
  readonly matches: Readonly<Record<string, { readonly extents: readonly InSeasonExtent[];
                                              readonly from?: string; readonly to?: string }>>;
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

const OPS = new Set(["whole", "upstream_of", "downstream_of", "between"]);
const NEEDS: Record<string, number> = { whole: 0, upstream_of: 1, downstream_of: 1, between: 2 };

/** The links as the app reads them, or null. An extent it cannot read drops its whole match (never a wider one). */
export function parseInSeasonLinks(x: unknown): InSeasonLinks | null {
  const l = x as Partial<InSeasonLinks> | null;
  if (!l || typeof l !== "object" || l.format !== INSEASON_LINKS_FORMAT || !l.matches || typeof l.matches !== "object")
    return null;
  const matches: Record<string, InSeasonLinks["matches"][string]> = {};
  for (const [id, m] of Object.entries(l.matches)) {
    const exts = Array.isArray(m?.extents) ? m.extents : [];
    const ok = exts.length > 0 && exts.every((e) => e && typeof e.item === "string" && OPS.has(e.op)
      && (e.at ?? []).length === NEEDS[e.op] && (e.at ?? []).every((a: unknown) => typeof a === "string" && (a as string).length > 0));
    if (ok) matches[id] = m;
  }
  return { format: INSEASON_LINKS_FORMAT, matches };
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
  /** the matched extents; empty = not matched yet */
  readonly extents: readonly InSeasonExtent[];
}

/** Every row of the feed in effect on `day` ("YYYY-MM-DD"), split into matched and not. Removed rows are never in. */
export function joinInSeason(feed: InSeasonFeed | null, links: InSeasonLinks | null, day: string):
  { matched: InSeasonChange[]; unmatched: InSeasonChange[] } {
  const matched: InSeasonChange[] = [], unmatched: InSeasonChange[] = [];
  if (!feed) return { matched, unmatched };
  const gone = new Set(feed.removed.map((r) => r.id));
  for (const r of feed.rows) {
    if (gone.has(r.id)) continue;
    const link = links?.matches[r.id];
    const span = inSeasonSpan(r, link);
    if ((span.from && span.from > day) || (span.to && span.to < day)) continue;
    const c: InSeasonChange = { id: r.id, region: r.region, water: r.water, text: r.change, ...span,
                                source: feed.source, extents: link?.extents ?? [] };
    (c.extents.length ? matched : unmatched).push(c);
  }
  return { matched, unmatched };
}

/** The part graph of one water, as the release's `part_mark` rows give it. */
export interface InSeasonMarks {
  readonly parts: number;
  readonly marks: readonly { readonly label: string | null; readonly down: number; readonly up: readonly number[] }[];
}

/**
 * THE PARTS OF A WATER AN EXTENT COVERS, read on the release's part graph: the parts on the extent's side of each named
 * cut, never crossing a cut of that name. `whole`: every part. Null: a label is not a part boundary here (or nothing lies
 * between the two) — the caller shows the change on the whole water and says it is on part of it, never a guessed part.
 */
export function inSeasonParts(e: InSeasonExtent, w: InSeasonMarks): Set<number> | null {
  const all = new Set(Array.from({ length: w.parts }, (_, i) => i));
  if (e.op === "whole") return all;
  const at = e.at ?? [];
  const cuts = at.map((l) => w.marks.filter((m) => m.label === l));
  if (cuts.some((c) => c.length === 0)) return null;
  const blocked = new Set(cuts.flat());
  const comp = (start: Iterable<number>): Set<number> => {
    const seen = new Set<number>(), todo = [...start];
    while (todo.length) {
      const p = todo.pop()!;
      if (seen.has(p)) continue;
      seen.add(p);
      for (const m of w.marks) {
        if (blocked.has(m)) continue;
        if (m.down === p) todo.push(...m.up);
        else if (m.up.includes(p)) todo.push(m.down);
      }
    }
    return seen;
  };
  const ups = (ms: readonly InSeasonMarks["marks"][number][]) => ms.flatMap((m) => m.up);
  const downs = (ms: readonly InSeasonMarks["marks"][number][]) => ms.map((m) => m.down);
  let out: Set<number>;
  if (e.op === "upstream_of") out = comp(ups(cuts[0]!));
  else if (e.op === "downstream_of") out = comp(downs(cuts[0]!));
  else {
    const [a, b] = cuts as [typeof cuts[0], typeof cuts[0]];
    const inter = (x: Set<number>, y: Set<number>) => new Set([...x].filter((p) => y.has(p)));
    out = new Set([...inter(comp(ups(a)), comp(downs(b))), ...inter(comp(downs(a)), comp(ups(b)))]);
  }
  return out.size ? out : null;
}

/** One change shown on one page: the change, and whether only part of the page's water is meant but not drawn. */
export interface InSeasonOnPage { readonly change: InSeasonChange; readonly partial: boolean }

/**
 * The matched changes that are on THIS page: the water `item`, its part `partIx` (null: the whole water's page). A
 * change whose cut is not a part boundary here is on the whole water, `partial`. `marks` null: the water's parts are
 * not known, so a change on any part of it is shown `partial` (never hidden).
 */
export function inSeasonOn(matched: readonly InSeasonChange[], item: string, partIx: number | null,
                           marks: InSeasonMarks | null): InSeasonOnPage[] {
  const out: InSeasonOnPage[] = [];
  for (const c of matched) {
    let on = false, partial = false;
    for (const e of c.extents) {
      if (e.item !== item) continue;
      if (e.op === "whole") { on = true; continue; }
      const ps = marks ? inSeasonParts(e, marks) : null;
      if (ps === null) { on = true; partial = true; }
      else if (partIx === null || ps.has(partIx)) on = true;
    }
    if (on) out.push({ change: c, partial: partial && !c.extents.some((e) => e.item === item && e.op === "whole") });
  }
  return out;
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
  sourceLabel: "Open the province's in-season changes page",
  partial: "On part of this water: the change says where.",
  informational: "Shown as posted; the answers on this page do not include it yet.",
  noticeTitle: "In-season changes",
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
  readonly partial: string | null;
  readonly note: string;
}

export function inSeasonBanner(p: InSeasonOnPage): InSeasonBannerVM {
  const dates = inSeasonDates(p.change.from, p.change.to);
  const head = `In-season change${dates ? ` (${dates})` : ""}`;
  return { id: p.change.id, head, text: p.change.text, source: INSEASON_WORDS.source, url: p.change.source,
           line: `${head}: ${p.change.text} — ${INSEASON_WORDS.source}`,
           partial: p.partial ? INSEASON_WORDS.partial : null, note: INSEASON_WORDS.informational };
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
