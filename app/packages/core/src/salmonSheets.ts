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
import { SOURCE_AUTHORITIES, SOURCE_LINE, authorityUrl } from "./tabSources";

const isSalmon = (c: string): c is SalmonSpecies => (SALMON_SPECIES as readonly string[]).includes(c);

/** A rule-book closure as a Sources row, as the card's Source line names it (#69): its book row ("Skeena River"), its exact
 *  words, and its pages ("B.C. Synopsis p.9 ↗", each opening the synopsis with the rule's passage highlighted), read off the
 *  rule's own sheet. A row the payload names no water for is titled by the rule's own label. */
function bookRow(sheet: (k: SheetKey) => SheetVM | null, from: string, rule: string, water: string | null): SourceRowVM {
  const r = sheet(`rule:${rule}`);
  const quote = r?.blocks.find((b) => b.k === "quote"), pages = r?.blocks.find((b) => b.k === "pages");
  // (W46) a closure whose rule this page cannot open is said by its place alone, and reported — never dropped
  if (!r || quote?.k !== "quote") { reportFailure(new SalmonBookRuleMissing(rule)); return { key: null, title: null, quote: from, role: "", book: [] }; }
  return { key: rule as SourceRowVM["key"], title: water ?? r.title, quote: quote.text, role: "", book: pages?.k === "pages" ? pages.line : [] };
}

/** A salmon species the card answers with no source of its answer (`SalmonCardVM.whoBy`). */
export class SalmonSourcesMissing extends Error {
  constructor(readonly code: string) { super(`salmon sheet: no source for ${code}'s answer`); this.name = "SalmonSourcesMissing"; }
}

/** A salmon species of the card with no Identification sheet (no picture, no tips in the fish manifest). */
export class SalmonLooksMissing extends Error {
  constructor(readonly code: string) { super(`salmon sheet: no picture sheet for ${code}`); this.name = "SalmonLooksMissing"; }
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
  // (W46) a species with an answer and no source: its answer drawn, the gap reported (every answer names one, salmonWho test)
  if (!who.length) reportFailure(new SalmonSourcesMissing(code));
  // a DFO place with DFO's rows as printed under its name (one the release keeps no rows for: its name alone), linked as the
  // card's Source line links it (#69): "DFO notices, Region 6 Skeena ↗" to that region's page; a water DFO lists in no
  // region (the default "Not opened by DFO"): DFO's notices
  const dfo: SourceRowVM[] = who.flatMap((w) => (w.k === "dfo" ? [{ key: null, ...(w.lines.length ? { title: w.from, quote: w.lines.join("\n") } : { title: null, quote: w.from }),
    role: "", book: [w.page ? { t: "web", s: SOURCE_LINE.DFO_REGION(w.page.region), url: w.page.url } : { t: "web", s: SOURCE_AUTHORITIES.dfo.name, url: authorityUrl("dfo") }] }] : []));
  const book = who.flatMap((w) => (w.k === "book" ? [bookRow(sheet, w.from, w.rule, w.water)] : []));
  const groups = [{ where: SOURCE_AUTHORITIES.synopsis.name, rows: book }, { where: SOURCE_AUTHORITIES.dfo.name, rows: dfo }].filter((g) => g.rows.length);
  if (groups.length) blocks.push({ k: "head", text: SPECIES_SHEET.BREAKDOWN }, { k: "sources", groups });
  // its title is its Identification tab's (the same sheet); with none (reported by the caller) the table row's name
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
    // (W46) a salmon species the card has but the page draws no picture sheet for: said (its Sources alone) and reported
    if (!own) reportFailure(new SalmonLooksMissing(code));
    // (review 1) its Sources tab is the page's line's (the tab the page names), never a key the page may not open
    if (own?.tabs) return m![2] ? sheet(own.tabs[1]!.open) : own;
    const pair = { id: `sp:${code}` as const, src: `sp:${code}|src` as const };
    // no Identification sheet: the Sources sheet alone, no tab strip pointing at a sheet that does not open
    if (m![2]) { const vm = salmonSourcesSheet(k, code, card, sheet); return vm && own ? { ...vm, tabs: speciesTabs(pair, "src") } : vm; }
    return own && { ...own, tabs: speciesTabs(pair, "id") };
  };
}
