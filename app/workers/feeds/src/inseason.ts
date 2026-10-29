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
import type { Fetch, Store } from "./io";
import { contentHash } from "./io";
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
  rows: number;
  sections: number;
  changed: boolean;
  first: boolean;
  diff: Diff | null;
  events: FeedEvent[];
}

export async function runInSeason(deps: { fetch: Fetch; priv: Store; now?: Date; html?: string; log?: (s: string) => void }): Promise<InSeasonResult> {
  const log = deps.log ?? (() => {});
  const now = deps.now ?? new Date();
  let html = deps.html;
  if (html === undefined) {
    const r = await deps.fetch(SOURCE_URL, { headers: { "User-Agent": UA, Accept: "text/html,application/xhtml+xml", "Accept-Language": "en-CA,en;q=0.9" } });
    if (!r.ok) throw new Error(`in-season: HTTP ${r.status} from ${SOURCE_URL}`);
    html = await r.text();
  }
  const rows = parseInSeason(html);
  const sha = contentHash(JSON.stringify(rows));
  let prev: Snapshot | null = null;
  try {
    prev = JSON.parse((await deps.priv.get("latest.json")) ?? "null");
  } catch {
    prev = null;
  }
  const snap: Snapshot = { format: "inseason/1", at: now.toISOString(), url: SOURCE_URL, sha, rows };
  const sections = new Set(rows.map((r) => r.section)).size;
  if (prev && prev.sha === sha) {
    log(`in-season: ${rows.length} rows in ${sections} sections, unchanged`);
    return { rows: rows.length, sections, changed: false, first: false, diff: null, events: [] };
  }
  await deps.priv.put("latest.json", JSON.stringify(snap));
  await deps.priv.put(`history/${now.toISOString().replace(/[-:]/g, "").slice(0, 15)}.json`, JSON.stringify(snap));
  if (!prev) {
    log(`in-season: first snapshot, ${rows.length} rows in ${sections} sections (the baseline: nothing to tell)`);
    return { rows: rows.length, sections, changed: true, first: true, diff: null, events: [] };
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
        },
      ]
    : [];
  return { rows: rows.length, sections, changed: true, first: false, diff: d, events };
}
