/**
 * (T991 round 3, owner 2026-11-22 ~23:55) ONE SOURCE ROW AT THE FOOT OF EACH TAB, naming only that tab's authorities:
 * "Yes but map sources don't need to be there already has attribution.. also in the synopsis page for skeena at bottom
 * source. Page 9 is not the water body page the source there should be maybe one for each page referenced in the card?
 * Also make sure highlighting is still there for each?"
 *
 * - Regulations: "Source  B.C. Synopsis p.8 ↗  p.9 ↗ …  B.C. in-season changes ↗  DFO notices ↗  <local authority> ↗
 *   All sources ›" — ONE LINK PER SYNOPSIS PAGE the drawn tabs cite (every "Rule book p.N" run and section source line
 *   they draw, `drawnRegsOf`; never a sheet's rules, #63 review) plus the water's own rule-book row, marked "(this
 *   water)"; never one fixed page. Each page link carries every highlight box the citing rules hold on that PDF page
 *   (T215), so the opened copy marks each passage. The in-season and DFO links appear only where the page shows them;
 *   "All sources ›" opens the breakdown sheet (T990).
 * - Conditions: "Source  Environment Canada ↗  BC River Forecast Centre ↗  Open-Meteo ↗" over its "Sources and notes" fold
 *   (the legally exact credits stay verbatim inside it).
 * - Map: none — the map draws its own attribution.
 *
 * The authorities' names are typed once, here (`SOURCE_AUTHORITIES`); their pages are the Sources and licences list's
 * (`LICENCE_ROWS`), so a publisher's address is not typed twice. "↗" leaves the app; "›" opens a sheet in it.
 */
import type { BookBox } from "./answers.generated";
import { LICENCE_ROWS } from "./menu/licences";
import { bookText, type Line, type Run } from "./regs/line";
import type { RuleIndex, RuleView } from "./regs/inputs";
import type { RegsPage, SourceLineVM } from "./regs/model";
import type { SheetKey } from "./regs/sheetKey";
import { ALL_SOURCES_KEY } from "./regs/sheets/allsources";
import { TROUT_SOURCES_KEY } from "./regs/sheets/trout";
import type { InSeasonBannerVM } from "./inseason";
import type { SalmonBookVM, SalmonCardVM } from "./salmon";
import { ALL_SOURCES, TROUT_NAV, WORDS } from "./regs/words";
import { SOURCE_AUTHORITIES, SOURCE_LINE, type AuthorityId } from "./sourceNames";

export { SOURCE_AUTHORITIES, SOURCE_LINE, RULE_BOOK, type AuthorityId } from "./sourceNames";
// (T1036) what the Regulations tab draws is the every-source sheet's input too: it lives beside that sheet
export { drawnRegsOf } from "./regs/sheets/allsources";


/** An authority's page: its Sources-and-licences row's. A row without one is a programming error, thrown (never a blank link). */
export function authorityUrl(id: AuthorityId): string {
  const row = LICENCE_ROWS.find((r) => r.k === SOURCE_AUTHORITIES[id].row);
  if (!row?.url) throw new Error(`tabSources: no page for authority ${id} (LICENCE_ROWS "${SOURCE_AUTHORITIES[id].row}")`);
  return row.url;
}

/** One link of a Source row: a synopsis page (opened highlighted), a web page, or a sheet of the page. `label` is drawn as is. */
export type SourceLinkVM =
  | { readonly k: "book"; readonly label: string; readonly printed: number; readonly pdf: number | null; readonly box?: readonly BookBox[];
      /** what a screen reader says: the page, and that every passage the page cites there is marked — a different action from
       *  a rule's own "Rule book p.N ↗", which marks that rule's passage only (T60: one action, one name) */
      readonly aria: string }
  | { readonly k: "web"; readonly label: string; readonly url: string }
  | { readonly k: "sheet"; readonly label: string; readonly open: SheetKey; readonly aria: string };

export interface TabSourcesVM {
  readonly label: string;
  readonly links: readonly SourceLinkVM[];
}

/** The whole row as one string (the a11y summary and the test's pin, AGENTS 33): the label, then each link's words. */
export const sourcesText = (vm: TabSourcesVM): string => [vm.label, ...vm.links.map((l) => l.label)].join(" ");


// ── (T1031 item 3) THE ONE SOURCE LINE ─────────────────────────────────────────────────────────────────────────────

/** A synopsis page a Source line links: its printed page, its PDF page (null: the PDF map lacks it, drawn unlinked), the
 *  passages its opened copy marks (T215). */
export interface SourcePage { readonly printed: number; readonly pdf: number | null; readonly box?: readonly BookBox[];
  /** the water's own rule-book page (the Regulations tab's row): marked "(this water)" (owner 2026-11-22) */
  readonly own?: boolean }

/** What one Source line shows: synopsis pages, then web pages, then at most one sheet. */
export interface SourceParts {
  readonly pages?: readonly SourcePage[];
  /** what a page's opened copy marks, said aloud (`SOURCE_LINE.MARKS_*`); said only on a page that carries boxes */
  readonly marks?: string;
  /** web pages: an authority's own name (`SOURCE_AUTHORITIES`) or the name a rule's data gives its page (T981), and the url */
  readonly web?: readonly { readonly name: string; readonly url: string }[];
  readonly sheet?: { readonly label: string; readonly open: SheetKey; readonly aria: string } | null;
}

/**
 * (T1031 item 3, owner 2026-11-22: "clean up our inline sources. Right now the way they display is inconsistent") THE ONE
 * SOURCE LINE every source is shown by, drawn by ONE component (ui-native `SourceRow`): the quiet label "Source", then
 * the synopsis pages ("B.C. Synopsis p.8 ↗  p.49 ↗", ascending, each once: the name on the first only), then each web page
 * ("B.C. in-season changes ↗"), then at most one sheet ("Who says so ›"). "↗" leaves the app; "›" opens a sheet in it.
 * Placed as the LAST line of the block it supports (a section, a card, a banner, a sheet). The one exception is a rule's
 * own "Rule book p.N ↗" inside its sentence (owner, T991 round 2: kept), which is a run of that line, not a Source line.
 */
export function sourceLineOf(p: SourceParts): TabSourcesVM {
  const pages = new Map<number, SourcePage>();
  for (const x of p.pages ?? []) {
    const at = pages.get(x.printed);
    const box = [...(at?.box ?? []), ...(x.box ?? [])].filter((b, i, all) => all.findIndex((y) => boxKey(y) === boxKey(b)) === i);
    pages.set(x.printed, { printed: x.printed, pdf: at?.pdf ?? x.pdf, ...(box.length ? { box } : {}), ...(at?.own || x.own ? { own: true } : {}) });
  }
  const links: SourceLinkVM[] = [...pages.values()].sort((a, b) => a.printed - b.printed).map((x, i): SourceLinkVM => {
    const words = x.own ? `${WORDS.PAGE}${x.printed} ${SOURCE_LINE.THIS_WATER}${x.pdf === null ? "" : ` ${WORDS.LINK_OUT}`}`
      : bookText({ printed: x.printed, pdf: x.pdf, bare: true });
    return { k: "book", label: i ? words : `${SOURCE_AUTHORITIES.synopsis.name} ${words}`, printed: x.printed, pdf: x.pdf,
             ...(x.box ? { box: x.box } : {}), aria: SOURCE_LINE.PAGE_A11Y(x.printed, x.box && p.marks ? p.marks : null, !!x.own) };
  });
  const urls = new Set<string>();
  for (const w of p.web ?? []) if (!urls.has(w.url)) { urls.add(w.url); links.push({ k: "web", label: `${w.name} ${WORDS.LINK_OUT}`, url: w.url }); }
  if (p.sheet) links.push({ k: "sheet", ...p.sheet });
  return { label: SOURCE_LINE.LABEL, links };
}

/** An authority's own web page, as a Source line's web part. */
export const authorityWeb = (id: AuthorityId): { name: string; url: string } => ({ name: SOURCE_AUTHORITIES[id].name, url: authorityUrl(id) });

/** The sheet part of a block's line: "Who says so ›", opening `open`, named for the block (`what`, T60). */
export const whoSaysSo = (open: SheetKey, what: string): NonNullable<SourceParts["sheet"]> =>
  ({ label: SOURCE_LINE.WHO, open, aria: SOURCE_LINE.WHO_A11Y(what) });

/**
 * A SECTION'S Source line (Gear, Ways to fish, Boats, Reference): its rules' synopsis pages, highlighted, then "Who says
 * so ›" opening the section's Sources sheet, which names who made each rule (the level words "Province · Region 6 · This
 * water's" the line printed before T1031 item 3). `what`: the section's own heading ("Gear").
 */
export const sectionSourcesOf = (src: Pick<SourceLineVM, "open"> & { readonly pages?: SourceLineVM["pages"] }, what: string): TabSourcesVM =>
  sourceLineOf({ pages: src.pages ?? [], marks: SOURCE_LINE.MARKS_BLOCK(what), sheet: whoSaysSo(src.open, what) });

/**
 * A Source line from a Line's source runs — a rule's book pages (`{t:"book"}`) and its cited web page (`{t:"web"}`, T981):
 * a sheet's "source" block, a closure's chain line on the closed banner. Its one `{t:"link"}` run (a sheet: "Who says so ›",
 * or the Line's own words, "All sources ›") is the line's sheet link, spoken for `what`; `sheet` gives one where the Line
 * has none ("Who says so ›"). Text runs (the spaces between) are not drawn; a run of any other kind is thrown.
 */
export function lineSourcesOf(line: Line, what: string, sheet?: SheetKey): TabSourcesVM {
  const pages: SourcePage[] = [], web: { name: string; url: string }[] = [];
  let open: { s: string; open: SheetKey } | null = sheet ? { s: SOURCE_LINE.WHO, open: sheet } : null;
  for (const r of line as readonly Run[]) {
    if (r.t === "book") pages.push({ printed: r.printed, pdf: r.pdf, ...(r.box?.length ? { box: r.box } : {}) });
    else if (r.t === "web") web.push({ name: r.s, url: r.url });
    else if (r.t === "link") {
      if (typeof r.open !== "string") throw new Error(`lineSourcesOf: a source line's link "${r.s}" opens no sheet`);
      open = { s: r.s, open: r.open };
    }
    else if (r.t !== "text" || r.s.trim()) throw new Error(`lineSourcesOf: a source line holds a "${r.t}" run ("${r.t === "text" ? r.s : ""}")`);
  }
  return sourceLineOf({ pages, marks: SOURCE_LINE.MARKS_BLOCK(what), web,
                        sheet: open ? { label: open.s, open: open.open, aria: SOURCE_LINE.SHEET_A11Y(open.s, what) } : null });
}

/** An in-season change's Source line (its banner on the page, its row in the bell's inbox): "Source  B.C. in-season changes
 *  ↗", the notice's page (the feed's `source`). A change with no page has no line (no links). */
export const inSeasonSourcesOf = (b: Pick<InSeasonBannerVM, "url">): TabSourcesVM =>
  sourceLineOf({ web: b.url ? [{ name: SOURCE_AUTHORITIES.inseason.name, url: b.url }] : [] });

/**
 * The trout table's Source line: "Who says so ›", opening the table's every-rule sheet (`src:trout`), which lists each
 * rule with its pages. (#69 review) NO PAGE OF ITS OWN: a Source line cites exactly what its block draws, and the table
 * draws no rule's page — its rows are Python's words, and a row's `src` lists every rule behind it (the deciding rule,
 * size, origin, possession, the duty, the date line: contract Q-D6) with no word saying which of them the row shows; the
 * app never cuts that list. Citing the sheet's pages listed p.6 on the Vedder (the province's 10-a-year steelhead quota
 * and record duty), which the table never says. A page with a trout table and no such sheet is a programming error.
 */
export function troutSourcesOf(page: Pick<RegsPage, "sheet">): TabSourcesVM {
  if (!page.sheet(TROUT_SOURCES_KEY)) throw new Error(`troutSourcesOf: the page has no ${TROUT_SOURCES_KEY} sheet`);
  return sourceLineOf({ sheet: whoSaysSo(TROUT_SOURCES_KEY, TROUT_NAV.ENTRY) });
}

/** A rule-book closure on the salmon card: "Source  B.C. Synopsis p.9 ↗" (its pages, linked where `salmonWithPdf` gave them). */
export const salmonBookSourcesOf = (b: Pick<SalmonBookVM, "pages">): TabSourcesVM => sourceLineOf({ pages: b.pages });

/** The salmon card's DFO Source line: each DFO region page the card reads, "DFO notices, Region 6 Skeena ↗". */
export const salmonDfoSourcesOf = (card: Pick<SalmonCardVM, "dfoPages">): TabSourcesVM =>
  sourceLineOf({ web: card.dfoPages.map((r) => ({ name: SOURCE_LINE.DFO_REGION(r.region), url: r.url })) });

/** The card with its rule-book closures' PDF pages from the store's one printed → PDF map (`Statics.pdfPages`, never
 *  computed); a page the map lacks stays unlinked ("p.9", `bookText`). */
export function salmonWithPdf<C extends Pick<SalmonCardVM, "book">>(card: C, pdfPages: Readonly<Record<number, number>>): C {
  return { ...card, book: card.book.map((b) => ({ ...b, pages: b.pages.map((p) => ({ printed: p.printed, pdf: pdfPages[p.printed] ?? null })) })) };
}

type Page = { pdf: number | null; boxes: BookBox[] };
const boxKey = (b: BookBox) => `${b.pdf}:${b.box.join(",")}`;

/**
 * Every synopsis page `v` links, by printed page, with the highlight boxes of every run citing it (deduplicated): a
 * `{t:"book"}` run anywhere (a page's lines, a sheet's rows), and a section source line's `pages` (`SourceLineVM`) —
 * except a per-row gear source (`srcs:gl|…`), which is not drawn since T900. Functions (the page's lazy `sheet`) are
 * not walked; their sheets are asked for by name.
 */
function citedPages(v: unknown, out: Map<number, Page>, seen = new Set<unknown>()): Map<number, Page> {
  if (!v || typeof v !== "object" || seen.has(v)) return out;
  seen.add(v);
  if (Array.isArray(v)) { for (const x of v) citedPages(x, out, seen); return out; }
  const o = v as Record<string, unknown>;
  const add = (printed: number, pdf: number | null, box: unknown) => {
    const p = out.get(printed) ?? { pdf: null, boxes: [] };
    if (p.pdf === null) p.pdf = pdf;
    for (const b of Array.isArray(box) ? (box as BookBox[]) : []) if (!p.boxes.some((x) => boxKey(x) === boxKey(b))) p.boxes.push(b);
    out.set(printed, p);
  };
  if (o.t === "book" && typeof o.printed === "number") add(o.printed, typeof o.pdf === "number" ? o.pdf : null, o.box);
  // (T1031 item 3) a drawn Source line's page link (`SourceLinkVM` "book")
  if (o.k === "book" && typeof o.printed === "number") add(o.printed, typeof o.pdf === "number" ? o.pdf : null, o.box);
  if (typeof o.open === "string" && o.open.startsWith("srcs:") && !o.open.startsWith("srcs:gl|") && Array.isArray(o.pages))
    for (const p of o.pages as readonly { printed?: unknown; pdf?: unknown; box?: unknown }[])
      if (typeof p?.printed === "number") add(p.printed, typeof p.pdf === "number" ? p.pdf : null, p.box);
  for (const k in o) citedPages(o[k], out, seen);
  return out;
}

/** The local authorities the page's rules cite (T981, a rule's `facts.cited`): each "<site> ↗" once, as the rule names it. */
function citedWeb(v: unknown, out: Map<string, string>, seen = new Set<unknown>()): Map<string, string> {
  if (!v || typeof v !== "object" || seen.has(v)) return out;
  seen.add(v);
  if (Array.isArray(v)) { for (const x of v) citedWeb(x, out, seen); return out; }
  const o = v as Record<string, unknown>;
  if (o.t === "web" && typeof o.s === "string" && typeof o.url === "string" && !out.has(o.url)) out.set(o.url, o.s);
  for (const k in o) citedWeb(o[k], out, seen);
  return out;
}

/** "All sources ›": the page's every-source sheet (T990). */
const ALL_SOURCES_LINK = { label: ALL_SOURCES.LINK, open: ALL_SOURCES_KEY, aria: ALL_SOURCES.ARIA } as const;

/**
 * The rules the drawn view models name: a sheet key or key list naming a rule (`rule:<entry>::<rule>`, a source line's
 * `keys`, a block's `rule`), and the trout table's rule indexes (`src`). A string with no such rule names nothing.
 */
function drawnRules(v: unknown, rules: Pick<RuleIndex, "get" | "has"> & { byKey(k: string): RuleView | null }, out: RuleView[], seen = new Set<unknown>(), field = ""): RuleView[] {
  if (typeof v === "string") {
    const k = v.startsWith("rule:") ? v.slice(5) : v;
    if (/^\S+::\S+$/.test(k)) { const r = rules.byKey(k); if (r && !r.missing) out.push(r); }
    return out;
  }
  if (typeof v === "number") { if (field === "src" && rules.has(v)) out.push(rules.get(v)); return out; }
  if (!v || typeof v !== "object" || seen.has(v)) return out;
  seen.add(v);
  if (Array.isArray(v)) { for (const x of v) drawnRules(x, rules, out, seen, field); return out; }
  const o = v as Record<string, unknown>;
  for (const k in o) drawnRules(o[k], rules, out, seen, k);
  return out;
}

/**
 * The Regulations tab's Source row: every synopsis page the DRAWN tabs cite (`drawnRegsOf`: their "Rule book p.N ↗" runs
 * and section source lines), each with every highlight box those cite on it, plus the water's own rule-book row
 * (`waterPagesOf`, T474: always linked), that link marked "(this water)" (owner 2026-11-22: "Page 9 is not the water body
 * page"); then the Province's in-season changes where the page shows one (`inSeason`), DFO where it shows DFO's salmon
 * (`dfo`, `salmonShowsDfo`) or where a drawn rule is DFO's own (`rules`), a local authority's page a drawn rule cites (T981),
 * and "All sources ›".
 */
export function regsSourcesOf(drawn: readonly unknown[], o: { waterPages: Line; inSeason: boolean; dfo: boolean;
  /** the part's rule index: a drawn rule whose source is DFO's (a DFO override, `source: "dfo"`) names DFO too */
  rules: Pick<RuleIndex, "get" | "has" | "byKey"> }): TabSourcesVM {
  const byKey = o.rules.byKey;
  if (!byKey) throw new Error("regsSourcesOf: the rule index has no byKey; the drawn rules' sources cannot be read");
  const own = new Set(o.waterPages.flatMap((r) => (r.t === "book" ? [r.printed] : [])));
  const pages = citedPages(o.waterPages, citedPages(drawn, new Map()));
  const web: { name: string; url: string }[] = [];
  if (o.inSeason) web.push(authorityWeb("inseason"));
  // (#63 re-review) DFO where the page shows DFO's salmon, or a drawn rule is DFO's own (a DFO override)
  if (o.dfo || drawnRules(drawn, { get: (i) => o.rules.get(i), has: (i) => o.rules.has(i), byKey: (k) => byKey.call(o.rules, k) }, [])
    .some((r) => r.f.source === "dfo")) web.push(authorityWeb("dfo"));
  for (const [url, s] of citedWeb(drawn, new Map())) web.push({ name: s, url });
  // (T1031 item 3) composed as every Source line is (`sourceLineOf`)
  return sourceLineOf({ pages: [...pages].map(([printed, p]) => ({ printed, pdf: p.pdf, ...(p.boxes.length ? { box: p.boxes } : {}), ...(own.has(printed) ? { own: true } : {}) })),
                        marks: SOURCE_LINE.MARKS_PAGE, web, sheet: ALL_SOURCES_LINK });
}

/**
 * Whether the page shows DFO's salmon: a salmon year bar some of whose days are DFO's (`by` "dfo" / "both"), or, with no
 * bar, the at-a-glance table's Salmon row (its block is tagged DFO, KeepTab).
 */
export function salmonShowsDfo(strip: { readonly by: "dfo" | "both" | "book" } | null, glanceSalmon: boolean): boolean {
  return strip ? strip.by !== "book" : glanceSalmon;
}

/** The Conditions tab's Source row: the gauges' readings, the River Forecast Centre's forecasts, the weather. */
export function conditionsSourcesOf(): TabSourcesVM {
  return sourceLineOf({ web: [authorityWeb("eccc"), authorityWeb("rfc"), authorityWeb("openMeteo")] });
}
