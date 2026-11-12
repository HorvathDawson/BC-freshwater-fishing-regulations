// T225 follow-up (owner 2026-10-28): the provincial IN-SEASON regulation changes — the job main's update-in-season.yml
// ran every 6 h (archive/pipeline/recurring/in_season/scraper.py; it opened a GitHub issue for unmatched waters). Here:
// read the page's accordion (Provincial + Regions 1–8: Water | In-season correction/change | Effective date), compare
// with the last snapshot (private), and tell the owner what was ADDED, CHANGED or REMOVED — the diff, by email.
// (T230) The rows are also published as the app's feed (`feeds/inseason/index.json`), rewritten by every run that read
// the page so the app can tell a stopped worker from a quiet page (T548, T549 item 7, `STALE_AFTER.inseason`); a row is
// placed on its waters by the curated links (`matches.json`), and one with no link is listed app-wide.
//
// The page sits behind Akamai: Python needed a Chrome TLS fingerprint from GitHub's runners. A Worker sends a browser
// User-Agent; a refusal is a failed run (status page + health alert), never an empty snapshot.
//
// Private: inseason/latest.json (the snapshot), inseason/history/<stamp>.json (each changed snapshot).
import { classOfStatus, SourceError } from "./health";
import type { Fetch, Meter, Store } from "./io";
import { contentHash, readStored } from "./io";
import type { FeedEvent } from "./events";
import { parseHtml, Str, Tag } from "./dfo/html";
import { INSEASON_LINKS_FORMAT } from "../../../packages/core/src/inseason";

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

/** (T609 W4) the panels the in-season page has: "Provincial level information" and "Region N - …" (7A / 7B too) */
const SECTION_RE = /^(Provincial\b|Region \d{1,2}[AB]?\b)/;
/** (T609 W4) the words every in-season table's header row carries */
const HEADER_WORDS = ["water", "change", "effective"];

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
    // (T609 W4) only THE in-season tables: a known section name and the three header words; any other page of the same
    // build (a redirect, a redesign) fails the run as `parse` instead of publishing its tables as regulation changes
    if (!SECTION_RE.test(section)) throw new Error(`in-season: a table under an unknown section "${section.slice(0, 80)}" (not Provincial / Region N)`);
    const trs = table.findAll("tr");
    const head = (trs[0] ? [...trs[0].findAll("th"), ...trs[0].findAll("td")] : []).map((c) => norm(c.getText(" ", true)).toLowerCase()).join(" | ");
    if (!HEADER_WORDS.every((w) => head.includes(w))) throw new Error(`in-season: the table under "${section.slice(0, 60)}" has no water / change / effective header (got "${head.slice(0, 120)}")`);
    for (const tr of trs.slice(1)) {
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
  /** (T558) with `holdSnapshot`: writes the new snapshot (latest.json + history). The caller runs it only once the
   *  run's events are safely in the outbox, so a failed hand-over computes the same diff again next run */
  save?: () => Promise<void>;
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
  /** (T558) do not write the new snapshot: return it as `save` (the worker runs it after the outbox took the events) */
  holdSnapshot?: boolean;
}): Promise<InSeasonResult> {
  const log = deps.log ?? (() => {});
  const now = deps.now ?? new Date();
  let html = deps.html;
  if (html === undefined) {
    // (T609 W4) never a redirect: the page answered from another address is not the in-season page
    const r = await deps.fetch(SOURCE_URL, { redirect: "manual", headers: { "User-Agent": UA, Accept: "text/html,application/xhtml+xml", "Accept-Language": "en-CA,en;q=0.9" } });
    if ((r.status >= 300 && r.status < 400) || r.redirected || (r.url && r.url !== SOURCE_URL))
      throw new SourceError("parse", `in-season: ${SOURCE_URL} redirected${r.headers.get("location") ? ` to ${r.headers.get("location")}` : r.url ? ` to ${r.url}` : ""}; nothing written`, "inseason-page");
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
  // (T520) a notice printed twice is one notice: collapsed, and said ONCE (an event keyed by the duplicated rows, which
  // the notifier sends once per key) — never a failed run
  const collapsed = collapseDuplicates(rows);
  rows = collapsed.rows;
  const dupEvents: FeedEvent[] = collapsed.dups.length ? [{
    key: `inseason-duplicate:${contentHash(JSON.stringify(collapsed.dups))}`, kind: "inseason",
    title: `${collapsed.dups.length} in-season ${collapsed.dups.length === 1 ? "row is" : "rows are"} printed twice on the provincial page (shown once)`,
    text: `${collapsed.dups.map((r) => `${r.section} · ${r.water} — ${r.change}${r.effective ? ` (effective ${r.effective})` : ""}`).join("\n")}\n\nSource: ${SOURCE_URL}`,
    at: (deps.now ?? new Date()).toISOString(), data: { duplicates: collapsed.dups, source: SOURCE_URL },
  }] : [];
  if (collapsed.dups.length) log(`in-season: ${collapsed.dups.length} row(s) printed twice, collapsed`);
  dupEvents.push(...undatedEvent(rows, (deps.now ?? new Date()).toISOString()));
  const sha = contentHash(JSON.stringify(rows));
  // (T558) absent = the first run; a failed read fails the run (readStored): read as "no snapshot", the changes since the
  // last one would be taken for a baseline and never told
  const prev = await readStored<Snapshot>(deps.priv, "latest.json");
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
    return { feed, rows: rows.length, sections, changed: false, first: false, diff: null, events: dupEvents };
  }
  const save = async () => {
    await deps.priv.put("latest.json", JSON.stringify(snap));
    await deps.priv.put(`history/${now.toISOString().replace(/[-:]/g, "").slice(0, 15)}.json`, JSON.stringify(snap));
  };
  const held = deps.holdSnapshot ? { save } : (await save(), {});
  if (!prev) {
    log(`in-season: first snapshot, ${rows.length} rows in ${sections} sections (the baseline: nothing to tell)`);
    return { feed, rows: rows.length, sections, changed: true, first: true, diff: null, events: dupEvents, ...held };
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
          text: `${diffText(d)}\n\nSource: ${SOURCE_URL}\n(Rows with no curated link are shown in the app's not-on-the-map-yet list: bind any that matter in curation.)`,
          at: now.toISOString(),
          data: { added: d.added, changed: d.changed, removed: d.removed, source: SOURCE_URL },
        },
      ]
    : [];
  return { feed, rows: rows.length, sections, changed: true, first: false, diff: d, events: [...dupEvents, ...events], ...held };
}

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
  // (T520) the true matched / unmatched counts: the curated links beside this file. Unreadable links are a FAILED run
  // (shown on the status page), never a feed claiming every row unmatched
  // (T609 W14) a MISSING links file once a feed has been published is a failed run too: published, every notice would
  // read "not on the map yet" and none on its water
  const linksText = await deps.feed!.get("matches.json");
  if (linksText === null && prev) throw new SourceError("parse", "in-season: feeds/inseason/matches.json is missing (the curated links); nothing written. Sync it: deploy/staging/sync-inputs.sh", "inseason-links");
  const doc = buildFeed(rows, prev, now.toISOString(), sha, pageChanged || !prev, matchedWaters(linksText));
  // (T549 item 7) written by EVERY run that read the page, so `fetchedAt` is when the Province's page was last CHECKED
  // (it was skipped while unchanged, under a 20 h heartbeat: the app's age line sat on the daily write). One small put a run.
  await deps.feed!.put("index.json", JSON.stringify(doc), { contentType: "application/json", cacheControl: "public, max-age=300, stale-while-revalidate=3600" });
  const n = (s: string) => doc.rows.filter((r) => r.status === s).length;
  return { rows: doc.rows.length, unmatched: doc.matching.unmatched, new: n("new"), changed: n("changed"), removed: doc.removed.length };
}

// ── the published feed (owner 2026-10-28): feeds/inseason/index.json ───────────────────────────────────────────────
//
// FORMAT `inseason-feed/1` (recommended; the owner had not designed one):
//   { format, fetchedAt (this run), changedAt (the page's content last changed), source, pageHash,
//     matching: { method: null | "<how>", matched, unmatched }        — matching to waters is a SEPARATE, optional step
//     rows: [ {
//       id            (T520) the CONTENT: hash of region + water + the change's normalised text (`rowId`) — never the
//                     row's position, so a row inserted above another cannot move a curated match onto it. An amended
//                     notice is a new id (unmatched until re-matched: the app lists it "not on the map yet")
//       region        "1" … "8", "7A", "7B", or "provincial";  section: the heading as printed
//       water         as printed;  waters: [] the matched registry item ids, from the curated links beside this file
//                     (`matches.json`, `INSEASON_LINKS_FORMAT`; empty = unmatched; matched: false)
//       effective     verbatim;  from / to: ISO dates when the effective cell reads ("July 4, 2025 to August 31, 2025"),
//                     else null (to null = no end printed)
//       change        verbatim (the authority)
//       parsed        only what reads confidently: kinds (closure | release | limit | bait | gear | licence | rescind |
//                     clarification), species (names as printed, lower case), dailyQuota, window (one printed date
//                     range in the text, as {from_month, from_day, to_month, to_day}); absent when nothing does
//       status        new | changed | unchanged — against the page version before the last change;  previous: the old
//                     change/effective when changed (an amended text: the row of the same region + water it replaced)
//       firstSeen, changedAt
//     } ],
//     removed: [ { id, region, water, change, effective, removedAt } ]   — rows gone from the page, kept 30 days
//   }
// (T675) A row whose ID moved while its content stayed (a re-key) is the same notice: not "changed", not in `removed`,
// its firstSeen kept.
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
/** (T548) An effective cell's dates. Two sides: each side that reads is kept ("April 1, 2027 until further notice" keeps
 *  its start, so a future-dated notice is not shown at once); a start with no year takes the end's ("July 30 to
 *  September 15, 2026"). A cell that yields no date at all is said to the owner (`undatedEvent`). */
export function effectiveDates(s: string): { from: string | null; to: string | null } {
  const parts = s.split(/\s+(?:to|-|–|until)\s+/i);
  if (parts.length === 2) {
    const b = isoDate(parts[1]!);
    const a = isoDate(parts[0]!) ?? (b ? isoDate(`${parts[0]!.trim()}, ${b.slice(0, 4)}`) : null);
    return { from: a, to: b };
  }
  return { from: isoDate(s), to: null };
}

/** (T548) the rows whose effective cell is printed but yields no date: said to the owner ONCE per set (keyed by them),
 *  so each gets a curated `effective` — such a row is otherwise in force from the day it appears */
export function undatedEvent(rows: readonly Row[], at: string): FeedEvent[] {
  const bad = rows.filter((r) => r.effective.trim() !== "" && (() => {
    const d = effectiveDates(r.effective);
    return d.from === null && d.to === null;
  })());
  if (!bad.length) return [];
  return [{
    key: `inseason-undated:${contentHash(JSON.stringify(bad))}`, kind: "inseason",
    title: `${bad.length} in-season ${bad.length === 1 ? "row has" : "rows have"} an effective date the feed cannot read (shown as in force now)`,
    text: `${bad.map((r) => `${r.section} · ${r.water} — effective "${r.effective}"`).join("\n")}\n\nGive each a curated \`effective\` in data/curated/inseason/notice_waters.json.\n\nSource: ${SOURCE_URL}`,
    at, data: { undated: bad, source: SOURCE_URL },
  }];
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
/** (T520) the words as the id reads them: TYPOGRAPHY IS NOT A NEW NOTICE — Unicode compatibility forms (NFKC), curly
 *  quotes and primes, en / em dashes and minus, non-breaking and repeated spaces, and case all fold. */
export const changeKey = (s: string): string => s.normalize("NFKC")
  .replace(/[\u2018\u2019\u201A\u201B\u2032\u0060\u00B4]/g, "'").replace(/[\u201C\u201D\u201E\u201F\u2033]/g, '"')
  .replace(/[\u2010\u2011\u2012\u2013\u2014\u2015\u2212]/g, "-")
  .replace(/\s+/g, " ").trim().toLowerCase();
/** (T520) A ROW'S ID IS ITS CONTENT: region + water + the change's text (not its position). `effective` joins the key
 *  ONLY for rows that share the other three (`buildFeed` says when), so a date change alone keeps a lone row's id. */
export function rowId(region: string, water: string, change: string, effective: string | null = null): string {
  const k = `${region}\u0000${changeKey(water)}\u0000${changeKey(change)}`;
  return `is-${contentHash(effective === null ? k : `${k}\u0000${changeKey(effective)}`).slice(0, 12)}`;
}

/** (T520) The curated links' matched waters per row id (`matches.json`, the app's `INSEASON_LINKS_FORMAT` — one constant,
 *  so the worker cannot fall behind a format bump again, as it did at T517's /3): what fills `waters`. */
export type MatchedWaters = ReadonlyMap<string, readonly string[]>;
export function matchedWaters(text: string | null): MatchedWaters | null {
  if (text === null) return null;
  const j = JSON.parse(text) as { format?: string; notices?: Record<string, { waters?: Record<string, unknown> }> };
  if (j?.format !== INSEASON_LINKS_FORMAT || !j.notices || typeof j.notices !== "object")
    throw new Error(`in-season: matches.json is ${String(j?.format)}, not ${INSEASON_LINKS_FORMAT}`);
  return new Map(Object.entries(j.notices).map(([id, n]) => [id, Object.keys(n?.waters ?? {}).sort()] as const));
}

/** (T520) A ROW PRINTED TWICE — identical in section, water, change and dates — is ONE notice: collapsed to its first
 *  copy (`dups` names each collapsed row once), never a failed run (a page glitch must not stop the feed). */
export function collapseDuplicates(rows: Row[]): { rows: Row[]; dups: Row[] } {
  const seen = new Set<string>(), out: Row[] = [], dups: Row[] = [];
  for (const r of rows) {
    const k = `${r.section}\u0000${r.water}\u0000${r.change}\u0000${r.effective}`;
    if (seen.has(k)) { if (!dups.some((d) => `${d.section}\u0000${d.water}\u0000${d.change}\u0000${d.effective}` === k)) dups.push(r); continue; }
    seen.add(k);
    out.push(r);
  }
  return { rows: out, dups };
}

export function buildFeed(rows: Row[], prev: InSeasonFeed | null, now: string, pageHash: string, pageChanged: boolean,
                          matched: MatchedWaters | null = null): InSeasonFeed {
  const prevById = new Map((prev?.rows ?? []).map((r) => [r.id, r] as const));
  rows = collapseDuplicates(rows).rows;
  // rows sharing region + water + change are told apart by their dates. Rows that would still share an id yet differ
  // in some field as printed (spacing, typography) are refused (a failed run, reported) — never told apart by position
  const keyOf = (r: Row) => `${regionOf(r.section)}\u0000${changeKey(r.water)}\u0000${changeKey(r.change)}`;
  const same = new Map<string, Row[]>();
  for (const r of rows) same.set(keyOf(r), [...(same.get(keyOf(r)) ?? []), r]);
  for (const g of same.values())
    if (g.length > 1 && new Set(g.map((r) => changeKey(r.effective))).size < g.length)
      throw new SourceError("parse", `in-season: ${g.length} rows of ${g[0]!.water} (${regionOf(g[0]!.section)}) differ only `
        + "in how they are printed, so they would share an id — nothing written", "inseason-page");
  const ids = rows.map((r) => rowId(regionOf(r.section), r.water, r.change,
                                    same.get(keyOf(r))!.length > 1 ? r.effective : null));
  const idSet = new Set(ids);
  // (T675) A RE-KEY IS NOT A CHANGE AND NOT A REMOVAL: a row gone under one id whose content (region, water, change,
  // effective — as the id reads them) is a live row under another id is the same notice (the id scheme moved, as at
  // T520, or a sibling row left and the date dropped out of the id). It carries its firstSeen, changedAt and status
  // to the new id and adds nothing to `removed`.
  const content = (region: string, water: string, change: string, effective: string) =>
    `${region}\u0000${changeKey(water)}\u0000${changeKey(change)}\u0000${changeKey(effective)}`;
  const gone = new Map<string, FeedRow[]>();
  for (const p of prev?.rows ?? [])
    if (!idSet.has(p.id)) gone.set(content(p.region, p.water, p.change, p.effective), [...(gone.get(content(p.region, p.water, p.change, p.effective)) ?? []), p]);
  const rekeyed = rows.map((r, i) => prevById.has(ids[i]!) ? undefined : gone.get(content(regionOf(r.section), r.water, r.change, r.effective))?.shift());
  const rekeyedIds = new Set(rekeyed.flatMap((p) => (p ? [p.id] : [])));
  const live = new Set(rows.map((r) => content(regionOf(r.section), r.water, r.change, r.effective)));
  // an amended notice: a new id where a row of the same region + water left the page in this reading
  const replaced = new Map<string, FeedRow[]>();
  for (const p of prev?.rows ?? [])
    if (!idSet.has(p.id) && !rekeyedIds.has(p.id)) replaced.set(`${p.region}\u0000${p.water.toLowerCase()}`, [...(replaced.get(`${p.region}\u0000${p.water.toLowerCase()}`) ?? []), p]);
  const out: FeedRow[] = rows.map((r, i) => {
    const region = regionOf(r.section);
    const id = ids[i]!;
    const was = prevById.get(id) ?? rekeyed[i];
    const same = was && was.change === r.change && was.effective === r.effective;
    const amended = !was ? replaced.get(`${region}\u0000${r.water.toLowerCase()}`)?.shift() : undefined;
    const { from, to } = effectiveDates(r.effective);
    const parsed = parseChange(r.change);
    let status: FeedRow["status"];
    let previous: FeedRow["previous"];
    if (amended) {
      status = "changed";
      previous = { change: amended.change, effective: amended.effective };
    } else if (!was) status = prev ? "new" : "unchanged";
    else if (!same) {
      status = "changed";
      previous = { change: was.change, effective: was.effective };
    } else if (pageChanged) status = "unchanged";
    else {
      status = was.status; // the page did not move: the last change's statuses stand
      previous = was.previous;
      // (T675) a feed written before the re-key rule holds "changed" rows whose `previous` IS their text: no change
      if (previous && content(region, r.water, previous.change, previous.effective) === content(region, r.water, r.change, r.effective)) {
        status = "unchanged";
        previous = undefined;
      }
    }
    return {
      id, region, section: r.section, water: r.water, waters: [...(matched?.get(id) ?? [])], matched: !!matched?.get(id)?.length,
      effective: r.effective, from, to, change: r.change, ...(parsed ? { parsed } : {}), status, ...(previous ? { previous } : {}),
      firstSeen: was?.firstSeen ?? now, changedAt: was && same ? was.changedAt : now,
    };
  });
  const removed = [
    // (T675) an entry whose content is live was a re-key recorded as a removal: the notice never left the page
    ...(prev?.removed ?? []).filter((r) => !idSet.has(r.id) && !live.has(content(r.region, r.water, r.change, r.effective))
      && Date.parse(now) - Date.parse(r.removedAt) < REMOVED_KEEP_MS),
    ...(prev?.rows ?? []).filter((r) => !idSet.has(r.id) && !rekeyedIds.has(r.id)).map((r) => ({ id: r.id, region: r.region, water: r.water, change: r.change, effective: r.effective, removedAt: now })),
  ];
  const matchedN = out.filter((r) => r.matched).length;
  return {
    format: FEED_FORMAT, fetchedAt: now, changedAt: pageChanged || !prev ? now : prev.changedAt, source: SOURCE_URL, pageHash,
    matching: { method: matched ? "curated" : null, matched: matchedN, unmatched: out.length - matchedN }, rows: out, removed,
  };
}
