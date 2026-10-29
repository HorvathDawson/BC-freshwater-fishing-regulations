// T225 follow-up (owner 2026-10-28): the provincial IN-SEASON regulation changes — the job main's update-in-season.yml
// ran every 6 h (archive/pipeline/recurring/in_season/scraper.py; it opened a GitHub issue for unmatched waters). Here:
// read the page's accordion (Provincial + Regions 1–8: Water | In-season correction/change | Effective date), compare
// with the last snapshot (private), and tell the owner what was ADDED, CHANGED or REMOVED — the diff, by email.
// Nothing is published: the in-season rows are not in the app yet (binding them to waters is curation work).
//
// The page sits behind Akamai: Python needed a Chrome TLS fingerprint from GitHub's runners. A Worker sends a browser
// User-Agent; a refusal is a failed run (status page + health alert), never an empty snapshot.
//
// Private: inseason/latest.json (the snapshot), inseason/history/<stamp>.json (each changed snapshot).
import { classOfStatus, SourceError } from "./health";
import type { Fetch, Meter, Store } from "./io";
import { contentHash, loadManifest, skipSameStore } from "./io";
import type { FeedEvent } from "./events";
import { parseHtml, Str, Tag } from "./dfo/html";

export const SOURCE_URL = "https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/fishing/fishing-regulations";
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

export interface Row {
  section: string;
  water: string;
  change: string;
  effective: string;
}
export interface Snapshot {
  format: "inseason/1";
  at: string;
  url: string;
  sha: string;
  rows: Row[];
}

const norm = (s: string) => s.replace(/ /g, " ").replace(/\s+/g, " ").trim();
const hasClass = (t: Tag, c: string) => (t.get("class") ?? "").split(/\s+/).includes(c);

/** scraper.scrape_in_season_changes: every accordion panel with a table → its rows. Throws when the page has no
 *  accordion (a block page, a redesign): never an empty answer. */
export function parseInSeason(html: string): Row[] {
  const root = parseHtml(html);
  const container = root.findAll("div").find((d) => hasClass(d, "accordion-container"));
  if (!container) throw new Error("in-season: no accordion-container on the page (blocked or redesigned?)");
  const rows: Row[] = [];
  for (const panel of container.findAll("div", false).filter((d) => hasClass(d, "panel"))) {
    const title = panel.findAll("div").find((d) => hasClass(d, "panel-title"));
    const section = norm((title ?? firstTag(panel))?.getText(" ", true) ?? "");
    if (!section) continue;
    const table = panel.find("table");
    if (!table) continue;
    for (const tr of table.findAll("tr").slice(1)) {
      const cells = tr.findAll("td");
      if (cells.length < 3) continue;
      const [water, change, effective] = cells.slice(0, 3).map((c) => norm(c.getText(" ", true)));
      if (!water && !change && !effective) continue;
      rows.push({ section, water: water!, change: change!, effective: effective! });
    }
  }
  return rows;
}
function firstTag(t: Tag): Tag | null {
  for (const c of t.children) if (!(c instanceof Str)) return c as Tag;
  return null;
}

const rowKey = (r: Row) => `${r.section}\u0000${r.water}\u0000${r.change}\u0000${r.effective}`;
const placeKey = (r: Row) => `${r.section}\u0000${r.water}`;

export interface Diff {
  added: Row[];
  removed: Row[];
  changed: { before: Row; after: Row }[];
}
export function diff(before: Row[], after: Row[]): Diff {
  const b = new Map(before.map((r) => [rowKey(r), r]));
  const a = new Map(after.map((r) => [rowKey(r), r]));
  let added = after.filter((r) => !b.has(rowKey(r)));
  let removed = before.filter((r) => !a.has(rowKey(r)));
  const changed: Diff["changed"] = [];
  for (const r of [...added]) {
    const was = removed.find((x) => placeKey(x) === placeKey(r));
    if (was) {
      changed.push({ before: was, after: r });
      added = added.filter((x) => x !== r);
      removed = removed.filter((x) => x !== was);
    }
  }
  return { added, removed, changed };
}

export function diffText(d: Diff): string {
  const line = (r: Row) => `${r.water} — ${r.change}${r.effective ? ` (effective ${r.effective})` : ""}`;
  const out: string[] = [];
  const bySection = <T,>(xs: T[], sec: (x: T) => string, f: (x: T) => string, mark: string) => {
    for (const s of [...new Set(xs.map(sec))]) {
      out.push(`${mark} ${s}`);
      for (const x of xs.filter((y) => sec(y) === s)) out.push(`    ${f(x)}`);
    }
  };
  if (d.added.length) {
    out.push(`ADDED (${d.added.length})`);
    bySection(d.added, (r) => r.section, line, " +");
  }
  if (d.changed.length) {
    out.push(`CHANGED (${d.changed.length})`);
    bySection(d.changed, (c) => c.after.section, (c) => `${line(c.after)}\n      was: ${c.before.change}${c.before.effective ? ` (effective ${c.before.effective})` : ""}`, " ~");
  }
  if (d.removed.length) {
    out.push(`REMOVED (${d.removed.length})`);
    bySection(d.removed, (r) => r.section, line, " -");
  }
  return out.join("\n");
}

export interface InSeasonResult {
  /** the published feed's summary */
  feed: { rows: number; unmatched: number; new: number; changed: number; removed: number } | null;
  rows: number;
  sections: number;
  changed: boolean;
  first: boolean;
  diff: Diff | null;
  events: FeedEvent[];
}

const SUSPECT = "suspect.json";
export const SUSPECT_RUNS = 3;

export async function runInSeason(deps: {
  fetch: Fetch;
  priv: Store;
  /** published: feeds/inseason/ (index.json); omitted = check + notify only */
  feed?: Store;
  meter?: Meter;
  now?: Date;
  html?: string;
  log?: (s: string) => void;
}): Promise<InSeasonResult> {
  const log = deps.log ?? (() => {});
  const now = deps.now ?? new Date();
  let html = deps.html;
  if (html === undefined) {
    const r = await deps.fetch(SOURCE_URL, { headers: { "User-Agent": UA, Accept: "text/html,application/xhtml+xml", "Accept-Language": "en-CA,en;q=0.9" } });
    if (!r.ok) throw new SourceError(classOfStatus(r.status) ?? "http_4xx", `in-season: HTTP ${r.status} from ${SOURCE_URL}`, "inseason-page");
    html = await r.text();
  }
  const looksBlocked = (t: string) => /access denied|request rejected|captcha|are you a robot|reference #\d/i.test(t);
  let rows: Row[];
  try {
    rows = parseInSeason(html);
  } catch (e) {
    throw new SourceError(looksBlocked(html) ? "blocked" : "parse", String((e as Error)?.message ?? e), "inseason-page");
  }
  const sha = contentHash(JSON.stringify(rows));
  let prev: Snapshot | null = null;
  try {
    prev = JSON.parse((await deps.priv.get("latest.json")) ?? "null");
  } catch {
    prev = null;
  }
  // ROBUSTNESS (owner 2026-10-28): a page that reads as empty — or lost most of its rows at once — is a block page or a
  // redesign, not "everything was rescinded": nothing is written (the last good feed stands) and the run fails `parse`
  // (or `blocked` when the body says so). A real mass removal (the same non-empty reading SUSPECT_RUNS runs running) is
  // then accepted; an empty page never is.
  const prevN = prev?.rows?.length ?? 0;
  const drop = prevN >= 8 && rows.length < prevN * 0.4;
  let suspect: { sha: string; runs: number } | null = null;
  if (rows.length && drop) {
    try {
      suspect = JSON.parse((await deps.priv.get(SUSPECT)) ?? "null");
    } catch {
      suspect = null;
    }
    suspect = { sha, runs: suspect?.sha === sha ? suspect.runs + 1 : 1 };
    await deps.priv.put(SUSPECT, JSON.stringify(suspect));
  }
  if (!rows.length || (drop && suspect!.runs < SUSPECT_RUNS)) {
    const blocked = looksBlocked(html);
    throw new SourceError(blocked ? "blocked" : "parse", `in-season: page read ${rows.length} row(s) (last good ${prevN})${blocked ? " — looks like a block page" : ""}; nothing written`, "inseason-page");
  }
  const snap: Snapshot = { format: "inseason/1", at: now.toISOString(), url: SOURCE_URL, sha, rows };
  const sections = new Set(rows.map((r) => r.section)).size;
  const feed = deps.feed ? await publishFeed(deps, rows, sha, !prev || prev.sha !== sha, now) : null;
  if (prev && prev.sha === sha) {
    log(`in-season: ${rows.length} rows in ${sections} sections, unchanged`);
    return { feed, rows: rows.length, sections, changed: false, first: false, diff: null, events: [] };
  }
  await deps.priv.put("latest.json", JSON.stringify(snap));
  await deps.priv.put(`history/${now.toISOString().replace(/[-:]/g, "").slice(0, 15)}.json`, JSON.stringify(snap));
  if (!prev) {
    log(`in-season: first snapshot, ${rows.length} rows in ${sections} sections (the baseline: nothing to tell)`);
    return { feed, rows: rows.length, sections, changed: true, first: true, diff: null, events: [] };
  }
  const d = diff(prev.rows, rows);
  const n = d.added.length + d.changed.length + d.removed.length;
  log(`in-season: ${d.added.length} added, ${d.changed.length} changed, ${d.removed.length} removed`);
  const events: FeedEvent[] = n
    ? [
        {
          key: `inseason:${sha}`,
          kind: "inseason",
          title: `${d.added.length} added, ${d.changed.length} changed, ${d.removed.length} removed on the provincial in-season page`,
          text: `${diffText(d)}\n\nSource: ${SOURCE_URL}\n(The in-season rows are not in the app yet: bind any that matter in curation.)`,
          at: now.toISOString(),
          data: { added: d.added, changed: d.changed, removed: d.removed, source: SOURCE_URL },
        },
      ]
    : [];
  return { feed, rows: rows.length, sections, changed: true, first: false, diff: d, events };
}

export const FEED_HEARTBEAT_MS = 24 * 3600_000;
async function publishFeed(
  deps: { feed?: Store; priv: Store; meter?: Meter; log?: (s: string) => void },
  rows: Row[],
  sha: string,
  pageChanged: boolean,
  now: Date,
): Promise<InSeasonResult["feed"]> {
  let prev: InSeasonFeed | null = null;
  try {
    const t = await deps.feed!.get("index.json");
    prev = t === null ? null : JSON.parse(t);
    if (prev && prev.format !== FEED_FORMAT) prev = null;
  } catch {
    prev = null;
  }
  const doc = buildFeed(rows, prev, now.toISOString(), sha, pageChanged || !prev);
  const writes = await loadManifest(deps.priv, "writes.json");
  const out = skipSameStore(deps.feed!, writes, {
    now: now.getTime(),
    stable: (_k, b) => b.replace(/"fetchedAt":"[^"]*"/, ""),
    maxAgeMs: () => FEED_HEARTBEAT_MS,
    meter: deps.meter,
  });
  await out.put("index.json", JSON.stringify(doc), { contentType: "application/json", cacheControl: "public, max-age=300, stale-while-revalidate=3600" });
  if (out.changed) await deps.priv.put("writes.json", JSON.stringify(writes));
  const n = (s: string) => doc.rows.filter((r) => r.status === s).length;
  return { rows: doc.rows.length, unmatched: doc.matching.unmatched, new: n("new"), changed: n("changed"), removed: doc.removed.length };
}

// ── the published feed (owner 2026-10-28): feeds/inseason/index.json ───────────────────────────────────────────────
//
// FORMAT `inseason-feed/1` (recommended; the owner had not designed one):
//   { format, fetchedAt (this run), changedAt (the page's content last changed), source, pageHash,
//     matching: { method: null | "<how>", matched, unmatched }        — matching to waters is a SEPARATE, optional step
//     rows: [ {
//       id            stable: hash of region + water + its order among that water's rows (a reworded row keeps it)
//       region        "1" … "8", "7A", "7B", or "provincial";  section: the heading as printed
//       water         as printed;  waters: [] the matched registry item / GNIS ids (empty = unmatched; matched: false)
//       effective     verbatim;  from / to: ISO dates when the effective cell reads ("July 4, 2025 to August 31, 2025"),
//                     else null (to null = no end printed)
//       change        verbatim (the authority)
//       parsed        only what reads confidently: kinds (closure | release | limit | bait | gear | licence | rescind |
//                     clarification), species (names as printed, lower case), dailyQuota, window (one printed date
//                     range in the text, as {from_month, from_day, to_month, to_day}); absent when nothing does
//       status        new | changed | unchanged — against the page version before the last change;  previous: the old
//                     change/effective when changed
//       firstSeen, changedAt
//     } ],
//     removed: [ { id, region, water, change, effective, removedAt } ]   — rows gone from the page, kept 30 days
//   }
// Unmatched rows publish (flagged by `matched: false`); the app decides how to show a row with no water.
import { readDates } from "./dfo/dates";

export const FEED_FORMAT = "inseason-feed/1";
const REMOVED_KEEP_MS = 30 * 86_400_000;
const MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];

export interface FeedRow {
  id: string;
  region: string;
  section: string;
  water: string;
  waters: string[];
  matched: boolean;
  effective: string;
  from: string | null;
  to: string | null;
  change: string;
  parsed?: { kinds: string[]; species?: string[]; dailyQuota?: number; window?: unknown };
  status: "new" | "changed" | "unchanged";
  previous?: { change: string; effective: string };
  firstSeen: string;
  changedAt: string;
}
export interface InSeasonFeed {
  format: typeof FEED_FORMAT;
  fetchedAt: string;
  changedAt: string;
  source: string;
  pageHash: string;
  matching: { method: string | null; matched: number; unmatched: number };
  rows: FeedRow[];
  removed: (Pick<FeedRow, "id" | "region" | "water" | "change" | "effective"> & { removedAt: string })[];
}

export function regionOf(section: string): string {
  const m = /^Region\s+(\d[AB]?)\b/i.exec(section);
  return m ? m[1]!.toUpperCase() : /provincial/i.test(section) ? "provincial" : section;
}

/** "July 4, 2025" → "2025-07-04" (also "Sept 16, 2025", "Aug. 1 2025") */
function isoDate(s: string): string | null {
  const m = /^([A-Za-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})$/.exec(s.trim());
  if (!m) return null;
  const w = m[1]!.toLowerCase();
  const i = w.length >= 3 ? MONTHS.findIndex((x) => x.startsWith(w)) : -1;
  const d = Number(m[2]);
  if (i < 0 || d < 1 || d > 31) return null;
  return `${m[3]}-${String(i + 1).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}
export function effectiveDates(s: string): { from: string | null; to: string | null } {
  const parts = s.split(/\s+(?:to|-|–|until)\s+/i);
  if (parts.length === 2) {
    const a = isoDate(parts[0]!);
    const b = isoDate(parts[1]!);
    return a && b ? { from: a, to: b } : { from: null, to: null };
  }
  return { from: isoDate(s), to: null };
}

const SPECIES = ["steelhead", "rainbow trout", "bull trout", "lake trout", "cutthroat trout", "brook trout", "dolly varden", "kokanee", "whitefish", "burbot", "salmon", "trout", "char"];
/** only what reads confidently; undefined when nothing does */
export function parseChange(text: string): FeedRow["parsed"] | undefined {
  const t = text.toLowerCase();
  const kinds: string[] = [];
  if (/^rescind/.test(t)) kinds.push("rescind");
  else if (/^clarification\b/.test(t)) kinds.push("clarification");
  else {
    if (/\bno fishing\b/.test(t)) kinds.push("closure");
    if (/\bcatch and release\b|\bnon-retention\b/.test(t)) kinds.push("release");
    if (/\bdaily quota\b|\b\d+\s+per day\b/.test(t)) kinds.push("limit");
    if (/\bbait ban\b|\bno (?:natural )?bait\b/.test(t)) kinds.push("bait");
    if (/\bfly fishing only\b|\bbarbless\b|\bsingle hook\b/.test(t)) kinds.push("gear");
  }
  if (/\bclassified (?:waters? )?licen[cs]e\b|\bclass i{1,2} water\b/.test(t) && !kinds.includes("licence")) kinds.push("licence");
  if (!kinds.length) return undefined;
  const out: NonNullable<FeedRow["parsed"]> = { kinds };
  const found: string[] = [];
  for (const sp of SPECIES) if (new RegExp(`\\b${sp}\\b`).test(t) && !found.some((f) => f.includes(sp))) found.push(sp);
  if (found.length) out.species = found;
  const q = /daily quota\s*=\s*(\d+)/.exec(t) ?? /\b(\d+)\s+per day\b/.exec(t);
  if (q && kinds.includes("limit")) out.dailyQuota = Number(q[1]);
  const ranges = [...text.matchAll(/\b([A-Z][a-z]+\.?\s+\d{1,2})\s*(?:to|-|–)\s*([A-Z][a-z]+\.?\s+\d{1,2}|\d{1,2})\b/g)];
  if (ranges.length === 1) {
    const r = readDates(`${ranges[0]![1]} to ${ranges[0]![2]}`);
    if (r.kind === "ok" && r.ranges.length === 1) out.window = r.ranges[0];
  }
  return out;
}

/** the rows → the feed, against the previously published feed */
export function buildFeed(rows: Row[], prev: InSeasonFeed | null, now: string, pageHash: string, pageChanged: boolean): InSeasonFeed {
  const nth = new Map<string, number>();
  const prevById = new Map((prev?.rows ?? []).map((r) => [r.id, r] as const));
  const out: FeedRow[] = rows.map((r) => {
    const region = regionOf(r.section);
    const k = `${region}\u0000${r.water.toLowerCase()}`;
    const n = nth.get(k) ?? 0;
    nth.set(k, n + 1);
    const id = `is-${contentHash(`${k}\u0000${n}`).slice(0, 12)}`;
    const was = prevById.get(id);
    const same = was && was.change === r.change && was.effective === r.effective;
    const { from, to } = effectiveDates(r.effective);
    const parsed = parseChange(r.change);
    let status: FeedRow["status"];
    let previous: FeedRow["previous"];
    if (!was) status = prev ? "new" : "unchanged";
    else if (!same) {
      status = "changed";
      previous = { change: was.change, effective: was.effective };
    } else if (pageChanged) status = "unchanged";
    else {
      status = was.status; // the page did not move: the last change's statuses stand
      previous = was.previous;
    }
    return {
      id, region, section: r.section, water: r.water, waters: was?.waters ?? [], matched: !!was?.waters?.length,
      effective: r.effective, from, to, change: r.change, ...(parsed ? { parsed } : {}), status, ...(previous ? { previous } : {}),
      firstSeen: was?.firstSeen ?? now, changedAt: was && same ? was.changedAt : now,
    };
  });
  const ids = new Set(out.map((r) => r.id));
  const removed = [
    ...(prev?.removed ?? []).filter((r) => !ids.has(r.id) && Date.parse(now) - Date.parse(r.removedAt) < REMOVED_KEEP_MS),
    ...(prev?.rows ?? []).filter((r) => !ids.has(r.id)).map((r) => ({ id: r.id, region: r.region, water: r.water, change: r.change, effective: r.effective, removedAt: now })),
  ];
  const matched = out.filter((r) => r.matched).length;
  return {
    format: FEED_FORMAT, fetchedAt: now, changedAt: pageChanged || !prev ? now : prev.changedAt, source: SOURCE_URL, pageHash,
    matching: { method: null, matched, unmatched: out.length - matched }, rows: out, removed,
  };
}
