// T225 port of pipeline/regs/dfo_salmon/feed.py `build`: one region's scraped rows → the typed feed, by fingerprint.
//
// `robust` (the Worker's default; false = the Python, for the parity tests): a row whose rules the catalogue refuses (a ValidationError) no longer raises out of the page — Python's one bad row
// held the WHOLE region. Its locator carries `error` and no rules; live.ts holds only the location it binds to (its
// last good answers carried forward, flagged in the region's `holds`) and publishes the rest of the region.
import type { RuleRecord } from "./locations";
import { pyStrOf, pyTypeName } from "./py";
import { rowText, toRules, type TypedRule } from "./typed";
import { rowWords, type Registry, type RowWords } from "./wording";

export interface Locator {
  fingerprint: string;
  rows: string[];
  rules: TypedRule[];
  /** robust mode: the row(s) that did not type — "ValidationError: …" (first 200 characters) */
  error?: string;
  /** (T321) the rows the card shows in DFO's words — new wording (unparsed), an unread number, or a note */
  words?: RowWords[];
}
export interface TypedFeed {
  region: string;
  date_modified: string | null;
  preamble: string[];
  locators: Locator[];
}

export function build(
  scraped: { region: string; date_modified: string | null; preamble: string[]; rules: RuleRecord[] },
  robust: boolean,
  reg: Registry,
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
    const words = recs.map((r) => rowWords(r, reg)).filter((w): w is RowWords => w !== null);
    const w = words.length ? { words } : {};
    if (!robust) {
      recs.forEach((rec, i) => rules.push(...toRules(rec, fp.slice(0, 8), i, reg)));
      locators.push({ fingerprint: fp, rows, rules, ...w });
      continue;
    }
    try {
      recs.forEach((rec, i) => rules.push(...toRules(rec, fp.slice(0, 8), i, reg)));
      locators.push({ fingerprint: fp, rows, rules, ...w });
    } catch (e) {
      locators.push({ fingerprint: fp, rows, rules: [], ...w, error: `${pyTypeName(e)}: ${[...pyStrOf(e)].slice(0, 200).join("")}` });
    }
  }
  return { region: scraped.region, date_modified: scraped.date_modified, preamble: scraped.preamble || [], locators };
}
