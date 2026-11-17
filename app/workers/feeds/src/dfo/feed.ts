// T225 port of pipeline/regs/dfo_salmon/feed.py `build`: one region's scraped rows → the typed feed, by fingerprint.
//
// `robust` (the Worker's default; false = the Python, for the parity tests): a row whose rules the catalogue refuses (a ValidationError) no longer raises out of the page — Python's one bad row
// held the WHOLE region. Its locator carries `error` and no rules; live.ts holds only the location it binds to (its
// last good answers carried forward, flagged in the region's `holds`) and publishes the rest of the region.
import { statementLocators } from "./cascade";
import type { RuleRecord } from "./locations";
import { pyStrOf, pyTypeName } from "./py";
import { rowText, typedRules, type TypedRule } from "./typed";
import { rowWords, type Registry, type RowWords } from "./wording";

export interface Locator {
  fingerprint: string;
  rows: string[];
  rules: TypedRule[];
  /** robust mode: the row(s) that did not type — "ValidationError: …" (first 200 characters) */
  error?: string;
  /** (T321) the rows the card shows in DFO's words — new wording (unparsed), an unread number, or a note; never a hidden
   *  row's (T787: the page does not show it) */
  words?: RowWords[];
  /** (T787) the rows (indices into `rows`) from a table DFO commented out: shown to a reviewer, answer nothing */
  hidden?: number[];
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
    const words = recs.filter((r) => !r.hidden).map((r) => rowWords(r, reg)).filter((w): w is RowWords => w !== null);
    const hid = recs.flatMap((r, i) => (r.hidden ? [i] : []));
    const w = { ...(hid.length ? { hidden: hid } : {}), ...(words.length ? { words } : {}) };
    // `typedRules`: the row's rules with the DFO marks — (T787) `hidden` on a row from a table DFO commented out
    // (provenance only), (T796 A6) `finfish` on the closure a "Finfish closure" row states
    if (!robust) {
      recs.forEach((rec, i) => rules.push(...typedRules(rec, fp.slice(0, 8), i, reg)));
      locators.push({ fingerprint: fp, rows, rules, ...w });
      continue;
    }
    try {
      recs.forEach((rec, i) => rules.push(...typedRules(rec, fp.slice(0, 8), i, reg)));
      locators.push({ fingerprint: fp, rows, rules, ...w });
    } catch (e) {
      locators.push({ fingerprint: fp, rows, rules: [], ...w, error: `${pyTypeName(e)}: ${[...pyStrOf(e)].slice(0, 200).join("")}` });
    }
  }
  // T349 / T758: the page's default sentences ("There is no fishing for salmon in Region 3 except …"), each the one rule
  // of its page-default location (`cascade.ts statementLocators`, keyed `page:<slug>:<scope>`) — as the Python's `build`
  locators.push(...statementLocators(String(scraped.region || ""), scraped.preamble || []));
  return { region: scraped.region, date_modified: scraped.date_modified, preamble: scraped.preamble || [], locators };
}
