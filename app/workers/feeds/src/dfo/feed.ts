// T225 port of pipeline/regs/dfo_salmon/feed.py `build`: one region's scraped rows → the typed feed, by fingerprint.
import type { RuleRecord } from "./locations";
import { rowText, toRules, type TypedRule } from "./typed";

export interface Locator {
  fingerprint: string;
  rows: string[];
  rules: TypedRule[];
}
export interface TypedFeed {
  region: string;
  date_modified: string | null;
  preamble: string[];
  locators: Locator[];
}

export function build(scraped: { region: string; date_modified: string | null; preamble: string[]; rules: RuleRecord[] }): TypedFeed {
  const byFp = new Map<string, RuleRecord[]>();
  for (const rec of scraped.rules) {
    const l = byFp.get(rec.fingerprint);
    if (l) l.push(rec);
    else byFp.set(rec.fingerprint, [rec]);
  }
  const locators: Locator[] = [];
  for (const [fp, recs] of byFp) {
    const rules: TypedRule[] = [];
    recs.forEach((rec, i) => rules.push(...toRules(rec, fp.slice(0, 8), i)));
    locators.push({ fingerprint: fp, rows: recs.map(rowText), rules });
  }
  return { region: scraped.region, date_modified: scraped.date_modified, preamble: scraped.preamble || [], locators };
}
