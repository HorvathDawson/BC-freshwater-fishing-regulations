// T225 port of pipeline/regs/dfo_salmon/feed.py `build`: one region's scraped rows → the typed feed, by fingerprint.
//
// `robust` (the Worker's default; false = the Python, for the parity tests): the date cell is read by dates.ts, and
// a row whose rules the catalogue refuses (a ValidationError) no longer raises out of the page — Python's one bad row
// held the WHOLE region. Its locator carries `error` and no rules; live.ts holds only the location it binds to (its
// last good answers carried forward, flagged in the region's `holds`) and publishes the rest of the region.
import type { RuleRecord } from "./locations";
import { pyStrOf, pyTypeName } from "./py";
import { rowText, toRules, type TypedRule } from "./typed";

export interface Locator {
  fingerprint: string;
  rows: string[];
  rules: TypedRule[];
  /** robust mode: the row(s) that did not type — "ValidationError: …" (first 200 characters) */
  error?: string;
}
export interface TypedFeed {
  region: string;
  date_modified: string | null;
  preamble: string[];
  locators: Locator[];
}

export function build(
  scraped: { region: string; date_modified: string | null; preamble: string[]; rules: RuleRecord[] },
  robust = false,
): TypedFeed {
  const byFp = new Map<string, RuleRecord[]>();
  for (const rec of scraped.rules) {
    const l = byFp.get(rec.fingerprint);
    if (l) l.push(rec);
    else byFp.set(rec.fingerprint, [rec]);
  }
  const locators: Locator[] = [];
  for (const [fp, recs] of byFp) {
    const rules: TypedRule[] = [];
    const rows = recs.map(rowText);
    if (!robust) {
      recs.forEach((rec, i) => rules.push(...toRules(rec, fp.slice(0, 8), i)));
      locators.push({ fingerprint: fp, rows, rules });
      continue;
    }
    try {
      recs.forEach((rec, i) => rules.push(...toRules(rec, fp.slice(0, 8), i, true)));
      locators.push({ fingerprint: fp, rows, rules });
    } catch (e) {
      locators.push({ fingerprint: fp, rows, rules: [], error: `${pyTypeName(e)}: ${[...pyStrOf(e)].slice(0, 200).join("")}` });
    }
  }
  return { region: scraped.region, date_modified: scraped.date_modified, preamble: scraped.preamble || [], locators };
}
