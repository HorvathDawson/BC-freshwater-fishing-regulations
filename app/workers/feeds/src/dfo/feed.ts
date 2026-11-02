// T225 port of pipeline/regs/dfo_salmon/feed.py `build`: one region's scraped rows → the typed feed, by fingerprint.
//
// `robust` (the Worker's default; false = the Python, for the parity tests): the date cell is read by dates.ts, and
// a row whose rules the catalogue refuses (a ValidationError) no longer raises out of the page — Python's one bad row
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
  robust = false,
  reg?: Registry,
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
      recs.forEach((rec, i) => rules.push(...toRules(rec, fp.slice(0, 8), i, false, reg)));
      locators.push({ fingerprint: fp, rows, rules, ...w });
      continue;
    }
    try {
      recs.forEach((rec, i) => rules.push(...toRules(rec, fp.slice(0, 8), i, true, reg)));
      locators.push({ fingerprint: fp, rows, rules, ...w });
    } catch (e) {
      locators.push({ fingerprint: fp, rows, rules: [], ...w, error: `${pyTypeName(e)}: ${[...pyStrOf(e)].slice(0, 200).join("")}` });
    }
  }
  // T349: the page's default sentences, each the one rule of its page-default location (cascade.statement_locators)
  locators.push(...statementLocators(String(scraped.region ?? ""), scraped.preamble || []));
  return { region: scraped.region, date_modified: scraped.date_modified, preamble: scraped.preamble || [], locators };
}

// ── T349 port of pipeline/regs/dfo_salmon/cascade.py `statements` / `statement_locators` ──────────────────────────
// Owner 2026-11-02: "daily limit 0" = closed, and a water nothing opens is closed — every page default answers closed.
const STATEMENT: readonly (readonly [string, RegExp])[] = [
  ["closed_except_listed", /there is no fishing for salmon in .+? except for the opportunities listed/i],
  ["limit_zero", /unless otherwise stated in the table below, the daily limit for .+? is zero/i],
  ["closed_until_notice", /there is no fishing for salmon in .+? until further notice/i],
];
const FLOWING = /flowing into areas?\s+(\d{1,3}(?:(?:\s*,\s*|\s*,?\s+and\s+)\d{1,3})*)/i;

export interface Statement { k: string; t: string; areas: number[]; unlisted: "closed" }

export function statements(texts: readonly string[]): Statement[] {
  const out: Statement[] = [];
  for (const raw of texts) {
    const t = (raw || "").replace(/\s+/g, " ").trim();
    for (const [k, rx] of STATEMENT) {
      if (rx.test(t)) {
        const m = FLOWING.exec(t);
        const areas = m ? (m[1]!.match(/\d{1,3}/g) ?? []).map(Number) : [];
        if (!out.some((o) => o.t === t)) out.push({ k, t, areas, unlisted: "closed" });
        break;
      }
    }
  }
  return out;
}

export const pageFingerprint = (slug: string, areas: readonly number[] = []): string =>
  `page:${slug}:${areas.length ? `areas-${areas.join("-")}` : "region"}`;

export function statementLocators(slug: string, preamble: readonly string[]): Locator[] {
  return statements(preamble).map((st) => {
    const fp = pageFingerprint(slug, st.areas);
    return { fingerprint: fp, rows: [st.t],
             rules: [{ rule_id: fp, type: "retention_limit", verbatim: st.t, species: ["SA"], take: 0, may_target: false } as unknown as TypedRule] };
  });
}
