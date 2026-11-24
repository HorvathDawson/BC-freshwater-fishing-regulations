/**
 * (T990, owner 2026-11-21: "in the regulation pane we should have a source button at button with all regulation sources in
 * one consistent spot") THE PAGE'S EVERY-SOURCE SHEET (`src:all`): the rules the DRAWN screen cites (T1036, as the Source
 * row cites them, `drawnRegsOf`), grouped by where they come from, one row per rule (no duplicates), plus the Province's
 * in-season changes. No rule is decided here: a row is the same one the section's sheet draws (`ruleBook` gives its pages,
 * or a local authority's citation, T981).
 *
 * (T1036, #63 review) Built from what the screen DRAWS, never from `page.gear` / `page.methods` or the card model: on a day
 * only salmon may be fished the Gear and Methods tabs draw the day's gear "if fished" (`gearTabsFor`), and on a closed day
 * the card rows' deciding rules are cited by nothing on the screen. A rule is listed where a drawn line cites it: a drawn
 * section Source line's rules (`srcs:*` `keys`; its pages are theirs), a drawn line that names its rule and shows its source
 * (a `rule` beside a "Rule book p.N ↗" or a cited page run), and the drawn trout table's rules (`src`: its Source line opens
 * them, `src:trout`). A species sheet's own rules (a row's "See every regulation line ›", `tf:` / `other:` / `sp:`) and a
 * row that only opens a sheet (Possession) are one tap further and not listed (the ticket's default, an owner question).
 */
import { ruleBook } from "../book";
import type { RuleIndex, RuleView } from "../inputs";
import type { InSeasonBannerVM } from "../../inseason";
import { reportFailure } from "../../failure";
import { text, type Run } from "../line";
import type { RegsPage, SheetCtx, SheetVM, SourceRowVM } from "../model";
import { clean } from "../names";
import type { SheetKey } from "../sheetKey";
import { fmtDay } from "../strip";
import { gearTabsFor, type GearTabsVM, type SalmonBookVM } from "../../salmon";
import type { TabSourcesVM } from "../../tabSources";
import { WORDS, ALL_SOURCES } from "../words";

export const ALL_SOURCES_KEY = "src:all" as const satisfies SheetKey;

type Group = { where: string; rows: SourceRowVM[] };

/**
 * (#63 review) WHAT THE REGULATIONS TAB DRAWS, as RegsScreen and WaterScreen draw it: the header, the Keep tab, the Gear and
 * Methods tabs AS DRAWN (`gearTabsFor`: on a day only salmon may be fished they are the day's gear "if fished", not
 * `page.gear`, which is the closed day's), the salmon gear lines, Methods' boats and the "Handling fish" reference (drawn
 * under Methods only while Methods is), and the Licence tab as drawn (`licenceTabFor`). The Source row and the every-source
 * sheet cite exactly what these cite — never a sheet's rules (a sheet is not on the screen) and never `page.gear` on a day
 * it is not drawn.
 */
export function drawnRegsOf(page: Pick<RegsPage, "header" | "keep" | "boats" | "methods">, tabs: GearTabsVM, licence: RegsPage["licence"],
                            /** (T1031 item 3) the Source lines drawn inside the tabs that no VM above carries: the trout
                             *  table's (`troutSourcesOf`: no page of its own) and the salmon card's book closures */
                            lines: readonly TabSourcesVM[]): readonly unknown[] {
  // the Methods tab draws no `reference`: the page's own is drawn under it (RegsScreen), never the "if fished" tab's
  const methods = tabs.methods ? { ...tabs.methods, reference: null } : null;
  return [page.header, page.keep, tabs.gear, tabs.salmonGear, methods, tabs.methods ? page.boats : null,
          tabs.methods ? page.methods?.reference ?? null : null, licence, lines];
}

/** A drawn line shows its rule's source: a "Rule book p.N ↗" run or a local authority's cited page (T981) among its runs. */
const showsSource = (o: Record<string, unknown>): boolean =>
  Object.values(o).some((v) => Array.isArray(v) && v.some((r) => !!r && typeof r === "object" && ((r as { t?: unknown }).t === "book" || (r as { t?: unknown }).t === "web")));

/**
 * (T1036) The rules the DRAWN view models cite (`drawnRegsOf`), once each, first seen first: a drawn section Source line's
 * `keys` (never a per-row gear line's, `srcs:gl|…`, not drawn since T900), a line naming its `rule` that shows that rule's
 * page or cited page, and the drawn trout table's `src`. A key the index lacks names nothing.
 */
export function drawnSourceRules(drawn: readonly unknown[], rules: Pick<RuleIndex, "get" | "has" | "byKey">): RuleView[] {
  const byKey = rules.byKey;
  if (!byKey) throw new Error("drawnSourceRules: the rule index has no byKey; the drawn rules cannot be read");
  const out = new Map<string, RuleView>(), walked = new Set<unknown>();
  const add = (r: RuleView | null | undefined) => { if (r && !r.missing && !out.has(r.key)) out.set(r.key, r); };
  const walk = (v: unknown, field: string): void => {
    if (typeof v === "number") { if (field === "src" && rules.has(v)) add(rules.get(v)); return; }
    if (!v || typeof v !== "object" || walked.has(v)) return;
    walked.add(v);
    if (Array.isArray(v)) { for (const x of v) walk(x, field); return; }
    const o = v as Record<string, unknown>;
    if (typeof o.open === "string" && o.open.startsWith("srcs:") && !o.open.startsWith("srcs:gl|") && Array.isArray(o.keys))
      for (const k of o.keys) if (typeof k === "string") add(byKey.call(rules, k));
    if (typeof o.rule === "string" && showsSource(o)) add(byKey.call(rules, o.rule));
    for (const k in o) walk(o[k], k);
  };
  walk(drawn, "");
  return [...out.values()];
}

/** What the page draws on its own (no salmon feed, no "if fished" gear): the page's sheet before the screen's wrap. */
const pageDrawn = (page: SheetCtx["page"]): readonly unknown[] =>
  drawnRegsOf(page, gearTabsFor(page, null, null, null), page.licence, []);

const rowOf = (r: RuleView): SourceRowVM =>
  ({ key: r.key, title: r.facts?.title || null, quote: clean(r.export.verbatim), role: "", book: ruleBook(r) });

/** The Province's in-season changes as Sources rows (one per change id), each linking its notice page. */
export function inSeasonRows(banners: readonly InSeasonBannerVM[]): SourceRowVM[] {
  const ids = new Set<string>();
  return banners.filter((b) => !ids.has(b.id) && ids.add(b.id)).map((b): SourceRowVM =>
    ({ key: null, title: b.head, quote: b.text, role: "", book: [{ t: "web", s: ALL_SOURCES.PROVINCE_SITE, url: b.url }] }));
}

/** The sheet of `rules` (and `more`, rows that are no rule of this page): by source (the synopsis, DFO, a local authority).
 *  Empty groups are not drawn. */
function sheetOfRules(rules: readonly RuleView[], page: Pick<RegsPage, "header" | "day">, more: readonly SourceRowVM[] = []): SheetVM {
  const local = rules.filter((r) => !!r.facts?.cited);
  const dfo = rules.filter((r) => r.f.source === "dfo" && !local.includes(r));
  const book = rules.filter((r) => !dfo.includes(r) && !local.includes(r));
  const all: [string, SourceRowVM[]][] = [[ALL_SOURCES.BOOK, [...book.map(rowOf), ...more]], [ALL_SOURCES.DFO, dfo.map(rowOf)], [ALL_SOURCES.LOCAL, local.map(rowOf)]];
  const groups: Group[] = all.filter(([, rows]) => rows.length).map(([where, rows]) => ({ where, rows }));
  return { key: ALL_SOURCES_KEY, title: ALL_SOURCES.TITLE, sub: `${page.header.name}${WORDS.DOT}${fmtDay(page.day)}`,
    blocks: groups.length ? [{ k: "sources", groups }] : [{ k: "para", text: [text(ALL_SOURCES.NONE)] }] };
}

/** The page's own `src:all`: the rules the page cites drawn alone (`pageDrawn`); the screen re-builds it from what it draws. */
export const allSourcesSheet = (ctx: SheetCtx): SheetVM => sheetOfRules(drawnSourceRules(pageDrawn(ctx.page), ctx.inp.rules), ctx.page);

/** (T1036) A salmon card's book closure whose rule the page's index does not hold (W46: said by its place and pages, and reported). */
export class AllSourcesRuleMissing extends Error {
  constructor(readonly rule: string) { super(`every-source sheet: the book closure's rule ${rule} is not on this page`); this.name = "AllSourcesRuleMissing"; }
}

/**
 * (T1036) The page's `sheet` with `src:all` built from what the SCREEN draws: `drawn` (`drawnRegsOf(page, tabs, licence,
 * lines)`, the Source row's input) and the salmon card's book closures (`book`: its Source lines carry pages, the closure
 * names its rule), so the sheet lists exactly the rules a drawn line cites. Any other key is the page's own.
 */
export function withDrawnSources(page: Pick<RegsPage, "sheet" | "header" | "day">, drawn: readonly unknown[],
                                 rules: Pick<RuleIndex, "get" | "has" | "byKey">,
                                 book: readonly Pick<SalmonBookVM, "rule" | "water" | "words" | "pages">[] = []): RegsPage["sheet"] {
  let vm: SheetVM | null = null;
  const build = (): SheetVM => {
    const listed = drawnSourceRules(drawn, rules), more: SourceRowVM[] = [];
    for (const b of book) {
      const r = rules.byKey?.(b.rule) ?? null;
      if (r && !r.missing) { if (!listed.some((x) => x.key === r.key)) listed.push(r); continue; }
      // (W46) never dropped: the closure as its card line says it, its pages linked as there
      reportFailure(new AllSourcesRuleMissing(b.rule));
      more.push({ key: null, title: b.water, quote: b.words, role: "",
                  book: b.pages.flatMap((x, i): Run[] => [...(i ? [text(" ")] : []), { t: "book", printed: x.printed, pdf: x.pdf }]) });
    }
    return sheetOfRules(listed, page, more);
  };
  return (k) => (k === ALL_SOURCES_KEY ? (vm ??= build()) : page.sheet(k));
}

/**
 * The page's `sheet` with `src:all` carrying the in-season changes the water shows (they are the screen's, not the
 * page's): their group follows the synopsis's. Any other key, and a page with no changes, is the page's own.
 */
export function withInSeason(sheet: (k: SheetKey) => SheetVM | null, banners: readonly InSeasonBannerVM[]): (k: SheetKey) => SheetVM | null {
  return (k) => {
    const vm = sheet(k);
    if (k !== ALL_SOURCES_KEY || !vm) return vm;
    const first = vm.blocks[0], groups: Group[] = first?.k === "sources" ? first.groups.map((x) => ({ where: x.where, rows: [...x.rows] })) : [];
    if (banners.length) groups.splice(groups.length && groups[0]!.where === ALL_SOURCES.BOOK ? 1 : 0, 0, { where: ALL_SOURCES.INSEASON, rows: inSeasonRows(banners) });
    return { ...vm, blocks: groups.length ? [{ k: "sources", groups }] : [{ k: "para", text: [text(ALL_SOURCES.NONE)] }] };
  };
}
