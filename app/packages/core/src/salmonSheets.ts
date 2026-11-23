/**
 * (T1031 items 4-5) A SALMON SPECIES' SHEET, Identification | Sources, like every other fish's (regs/sheets/species.ts).
 * The salmon card is the screen's, not the regulations page's (DFO's feed, `salmonCard`), so its sheets are added to the
 * page's `sheet` here, as the in-season changes are (`withInSeason`):
 *  - `sp:<code>` (its picture: what it looks like) gains the tabs, where the card's table has the species;
 *  - `sp:<code>|src`: today's answer as its table row says it, then where each part of it comes from — the DFO places
 *    answering it (DFO's rows as printed, "DFO notices ↗") and the rule book's closure of the stretch (the rule's own words
 *    and pages, highlighted) — the card's `whoBy`.
 * Nothing is decided here: every word is the card's or the rule's.
 */
import { reportFailure } from "./failure";
import { SALMON_SPECIES, salmonTable, type SalmonCardVM, type SalmonSpecies } from "./salmon";
import { text, type Line } from "./regs/line";
import type { SheetBlock, SheetVM, SourceRowVM } from "./regs/model";
import type { SheetKey } from "./regs/sheetKey";
import { speciesTabs } from "./regs/sheets/species";
import { SPECIES_SHEET } from "./regs/words";
import { SOURCE_AUTHORITIES, authorityUrl } from "./tabSources";

const isSalmon = (c: string): c is SalmonSpecies => (SALMON_SPECIES as readonly string[]).includes(c);

/** A rule-book closure as a Sources row: its place ("Rule book p.9 · Skeena River"), its exact words and its pages (each
 *  opening the synopsis with the rule's passage highlighted), read off the rule's own sheet. */
function bookRow(sheet: (k: SheetKey) => SheetVM | null, from: string, rule: string): SourceRowVM {
  const r = sheet(`rule:${rule}`);
  const quote = r?.blocks.find((b) => b.k === "quote"), pages = r?.blocks.find((b) => b.k === "pages");
  // (W46) a closure whose rule this page cannot open is said by its place alone, and reported — never dropped
  if (!r || quote?.k !== "quote") { reportFailure(new SalmonBookRuleMissing(rule)); return { key: null, title: null, quote: from, role: "", book: [] }; }
  return { key: rule as SourceRowVM["key"], title: from, quote: quote.text, role: "", book: pages?.k === "pages" ? pages.line : [] };
}

/** A salmon place that is the rule book's closure names a rule the page's own rules do not hold. */
export class SalmonBookRuleMissing extends Error {
  constructor(readonly rule: string) { super(`salmon sheet: the book closure's rule ${rule} is not on this page`); this.name = "SalmonBookRuleMissing"; }
}

/** The Sources tab of salmon species `code` on this card, or null where its table has no such row. */
export function salmonSourcesSheet(key: SheetKey, code: SalmonSpecies, card: SalmonCardVM, sheet: (k: SheetKey) => SheetVM | null): SheetVM | null {
  const row = salmonTable(card).species.find((r) => r.code === code);
  if (!row) return null;
  const blocks: SheetBlock[] = [{ k: "head", text: SPECIES_SHEET.HERE }, { k: "value", text: [text(row.statusLabel)] }];
  for (const l of [row.quotaRuns ?? (row.quota ? [text(row.quota)] : null), row.window ? [text(row.window)] : null,
                   row.limit ? [text(row.limit)] : null] as (Line | null)[]) if (l) blocks.push({ k: "para", text: l });
  const who = card.whoBy?.[code] ?? [];
  // a DFO place with DFO's rows as printed under its name; one the release keeps no rows for is its name alone
  const dfo: SourceRowVM[] = who.flatMap((w) => (w.k === "dfo" ? [{ key: null, ...(w.lines.length ? { title: w.from, quote: w.lines.join("\n") } : { title: null, quote: w.from }),
    role: "", book: [{ t: "web", s: SOURCE_AUTHORITIES.dfo.name, url: authorityUrl("dfo") }] }] : []));
  const book = who.flatMap((w) => (w.k === "book" ? [bookRow(sheet, w.from, w.rule)] : []));
  const groups = [{ where: SOURCE_AUTHORITIES.synopsis.name, rows: book }, { where: SOURCE_AUTHORITIES.dfo.name, rows: dfo }].filter((g) => g.rows.length);
  if (groups.length) blocks.push({ k: "head", text: SPECIES_SHEET.BREAKDOWN }, { k: "sources", groups });
  return { key, title: sheet(`sp:${code}`)?.title ?? row.name, sub: null, blocks };
}

/** The page's `sheet` with the salmon species' sheets of `card` (null: no salmon card on this page — the page's own). */
export function withSalmonSheets(sheet: (k: SheetKey) => SheetVM | null, card: SalmonCardVM | null): (k: SheetKey) => SheetVM | null {
  if (!card) return sheet;
  const has = (c: SalmonSpecies) => salmonTable(card).species.some((r) => r.code === c);
  return (k) => {
    const m = /^sp:([A-Z]+)(\|src)?$/.exec(k), code = m?.[1];
    if (!code || !isSalmon(code) || !has(code)) return sheet(k);
    // a fish the page's own lines answer (Chinook in the rule book's rows) keeps the page's sheet and its tabs
    const own = sheet(`sp:${code}`);
    // (review 1) its Sources tab is the page's line's (the tab the page names), never a key the page may not open
    if (own?.tabs) return m![2] ? sheet(own.tabs[1]!.open) : own;
    const pair = { id: `sp:${code}` as const, src: `sp:${code}|src` as const };
    if (m![2]) { const vm = salmonSourcesSheet(k, code, card, sheet); return vm && { ...vm, tabs: speciesTabs(pair, "src") }; }
    return own && { ...own, tabs: speciesTabs(pair, "id") };
  };
}
