// T366 item 5 (owner 2026-11-03: "only location ones we need to really really review. we can have the digest say the
// other rule change with text and how it was interpreted"): the Worker's half of `pipeline.regs.dfo_salmon.refresh`
// rows. A ROW change on a curated place (old row → new row, verbatim, with its FN numbers and how each new row was
// read) is ONE DIGEST notice per page, `urgent: false` — the place stands, nothing to re-review. A place reworded / new /
// moved, a held region, an unread row, a new wording, a header change stay URGENT (their own notices, live.ts).
//
// The baseline is the Worker's own last typed page per region, kept private: `rows/region-<slug>.json`
// {sha256, date_modified, rows: {fingerprint: {rows, rules}}} (the Python compares with typed/ on disk; same shape).
import type { EntryFile } from "./entries";
import type { TypedFeed } from "./feed";
import type { TypedRule } from "./typed";

export interface RowsOf {
  rows: string[];
  rules: TypedRule[];
}
export type RowMap = Record<string, RowsOf>;
export interface RowsBaseline {
  sha256: string;
  date_modified: string | null;
  rows: RowMap;
}
export interface RowChange {
  location_id: string;
  place: string;
  old: string[];
  new: string[];
  fns: string[];
  read: { row: string; read: string[] }[];
}

export const ROWS_KEY = (slug: string) => `rows/region-${slug}.json`;

/** refresh.feed_map: fingerprint → {rows, rules} of one region's typed page */
export function feedMap(typed: TypedFeed): RowMap {
  const out: RowMap = {};
  for (const l of typed.locators || []) out[l.fingerprint] = { rows: [...(l.rows || [])], rules: [...(l.rules || [])] };
  return out;
}

/** refresh.fns: the FN numbers the rows name, upper case, no spaces, sorted */
export function fns(rows: string[]): string[] {
  const s = new Set<string>();
  for (const r of rows) for (const m of r.matchAll(/\bFN\s*\d{3,4}\b/gi)) s.add(m[0].replace(/\s+/g, "").toUpperCase());
  return [...s].sort();
}

const MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const md = (d: Record<string, number | undefined>) =>
  `${MONTHS[d.from_month || 1]} ${d.from_day || 1}–${MONTHS[d.to_month || 12]} ${d.to_day || 31}`;

/** refresh.how_read: one rule as the feed READ it — type, fish, dates, limit, origin, gear */
export function howRead(rule: TypedRule): string {
  const out = [String(rule.type || "?")];
  const sp = rule.species || [];
  if (sp.length) out.push(sp.join("/"));
  else if (rule.when_targeting?.length) out.push("targeting " + rule.when_targeting.join("/"));
  const dates = (rule.when as { dates?: Record<string, number>[] } | undefined)?.dates;
  if (dates?.length) out.push(dates.map(md).join(", "));
  const r = rule as unknown as Record<string, unknown>;
  for (const k of ["take", "possession", "annual"]) if (r[k] !== undefined && r[k] !== null) out.push(`${k} ${r[k]}`);
  if (rule.origin) out.push(String(rule.origin));
  for (const g of rule.gear || []) {
    const bits = Object.entries(g).filter(([k]) => k !== "slot").map(([k, v]) => `${k} ${Array.isArray(v) ? v.join("/") : v}`);
    out.push(bits.length ? `gear ${g.slot}: ${bits.join(", ")}` : `gear ${g.slot}`);
  }
  if (rule.lengths?.length) out.push(`lengths ${JSON.stringify(rule.lengths)}`);
  return out.join(" · ");
}

const esc = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
/** the typed rules read out of row `i` of locator `fp` (feed.build: rule ids `<fp8>.r<i>…`) */
function rulesOfRow(fp: string, i: number, rules: TypedRule[]): TypedRule[] {
  const pat = new RegExp(`^${esc(fp.slice(0, 8))}\\.r${i}(?!\\d)`);
  return rules.filter((r) => pat.test(String(r.rule_id || "")));
}

/** refresh.row_changes: every curated place whose rows differ between two pages (only-one-side rows, verbatim) */
export function rowChanges(ef: EntryFile, old: RowMap, now: RowMap): RowChange[] {
  const out: RowChange[] = [];
  for (const loc of ef.locations) {
    const fps = [...(loc.fingerprints || [])];
    const was = fps.flatMap((fp) => old[fp]?.rows ?? []);
    const is = fps.flatMap((fp) => now[fp]?.rows ?? []);
    if (JSON.stringify(was) === JSON.stringify(is) || !(was.length || is.length) || !fps.some((fp) => fp in now)) continue;
    const gone = was.filter((r) => !is.includes(r));
    const added = is.filter((r) => !was.includes(r));
    if (!gone.length && !added.length) continue; // only the order moved
    const read: RowChange["read"] = [];
    for (const fp of fps) {
      const got = now[fp];
      (got?.rows ?? []).forEach((row, i) => {
        if (!added.includes(row)) return;
        const rs = rulesOfRow(fp, i, got!.rules).map(howRead);
        read.push({ row, read: rs.length ? rs : ["(no rule read from this row)"] });
      });
    }
    const st = loc.source_text || {};
    const water = (typeof st.waters === "string" && st.waters) || loc.water;
    out.push({
      location_id: loc.location_id,
      place: `${water}: ${st.specific_area || ""}`.replace(/[: ]+$/, ""),
      old: gone,
      new: added,
      fns: fns([...added, ...gone]),
      read,
    });
  }
  return out;
}

/** refresh.rows_text: the digest's lines — place, old → new verbatim (FN), how each new row was read */
export function rowsText(changes: RowChange[]): string {
  const L: string[] = [];
  for (const c of changes) {
    L.push(`* ${c.location_id} — ${c.place}` + (c.fns.length ? `  [${c.fns.join(", ")}]` : ""));
    for (const r of c.old) L.push(`    - was: ${r}`);
    for (const r of c.read) {
      L.push(`    + now: ${r.row}`);
      for (const x of r.read) L.push(`        read: ${x}`);
    }
  }
  return L.join("\n");
}

/** live.py's `dfo-rows` notice: one per page, NOT urgent (the daily digest when NOTIFY_DIGEST=daily) */
export function rowsNotice(slug: string, sha: string, pageDate: string | null, changes: RowChange[]) {
  return {
    key: `dfo-rows:${slug}:${sha.slice(0, 12)}`,
    urgent: false as const,
    text:
      `DFO Region ${slug}: ${changes.length} place(s) with changed rows on the page of ${pageDate || "?"} (digest — the ` +
      `places stand, no re-review; fix anything misread):\n` + rowsText(changes),
    data: {
      region: slug,
      kind: "rows",
      affects: `${changes.length} place${changes.length === 1 ? "" : "s"} with changed rows (the places stand, no re-review)`,
      page: pageDate ?? undefined,
      rows: changes,
    },
  };
}

/** the stored baseline, or null when there is none yet. An unreadable one THROWS (never read as "no rows"). */
export function readBaseline(text: string | null): RowsBaseline | null {
  if (text === null) return null;
  const b = JSON.parse(text) as RowsBaseline;
  if (!b || typeof b.sha256 !== "string" || typeof b.rows !== "object" || b.rows === null)
    throw new Error("rows baseline has no sha256 / rows");
  return b;
}
