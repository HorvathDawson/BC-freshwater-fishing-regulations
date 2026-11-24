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
 * them, `src:trout`).
 *
 * (T1036, owner 2026-11-24: "Add back but maybe have them separate?") A SECOND GROUP, under its own head below the first
 * (`ALL_SOURCES.OTHER`): every rule a sheet REACHABLE from the drawn screen cites that no drawn line cites — a fish row's
 * sheet (`other:` / `sp:` / `tf:`, both its tabs), the Possession row's (`pos:pos`), any sheet a drawn line opens
 * (`reachableSheets`). A sheet cites a rule by a row of its Sources list (never an "Overruled" card: it does not apply
 * here) or by a line that opens the rule's sheet beside its page ("Who says so ›" + "B.C. Synopsis p.N"). One tap only:
 * a link inside a sheet (its "See every regulation line ›", "Fishing more than one water today? ›") is not followed. No
 * rule is in both groups; a card the screen does not draw (a closed day's) opens no sheet, so nothing of it is listed.
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
import { parseSheetKey } from "../sheetKey";
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
 * (T1036) THE ONE WALK OF WHAT THE DRAWN SCREEN CITES (`drawnRegsOf`), read by the Source row (a drawn DFO rule names DFO,
 * `regsSourcesOf`) and by the every-source sheet: the rules, once each, first seen first — a drawn section Source line's
 * `keys` (never a per-row gear line's, `srcs:gl|…`, not drawn since T900), a line naming its `rule` that shows that rule's
 * page or cited page, and the drawn trout table's `src`. `missing`: what a drawn line cites that the part's index does not
 * hold (a key, or `#<index>` for a trout table's), each once — never a row, and reported by the sheet (W46).
 */
export function drawnCitations(drawn: readonly unknown[], rules: Pick<RuleIndex, "get" | "has" | "byKey">): { rules: RuleView[]; missing: string[] } {
  const byKey = rules.byKey;
  if (!byKey) throw new Error("drawnCitations: the rule index has no byKey; the drawn rules cannot be read");
  const out = new Map<string, RuleView>(), missing = new Set<string>(), walked = new Set<unknown>();
  const add = (r: RuleView | null | undefined, cited: string) => {
    if (!r || r.missing) missing.add(cited);
    else if (!out.has(r.key)) out.set(r.key, r);
  };
  const walk = (v: unknown, field: string): void => {
    if (typeof v === "number") { if (field === "src") add(rules.has(v) ? rules.get(v) : null, `#${v}`); return; }
    if (!v || typeof v !== "object" || walked.has(v)) return;
    walked.add(v);
    if (Array.isArray(v)) { for (const x of v) walk(x, field); return; }
    const o = v as Record<string, unknown>;
    if (typeof o.open === "string" && o.open.startsWith("srcs:") && !o.open.startsWith("srcs:gl|") && Array.isArray(o.keys))
      for (const k of o.keys) if (typeof k === "string") add(byKey.call(rules, k), k);
    if (typeof o.rule === "string" && showsSource(o)) add(byKey.call(rules, o.rule), o.rule);
    for (const k in o) walk(o[k], k);
  };
  walk(drawn, "");
  return { rules: [...out.values()], missing: [...missing] };
}

/**
 * (T1036) THE SHEETS ONE TAP FROM THE DRAWN SCREEN: every sheet key a drawn line opens (`open`, as each press hands it to
 * the sheet), first seen first — but the every-source sheet itself and a per-row gear line's (`srcs:gl|…`: not drawn
 * since T900, as `drawnCitations` reads it).
 */
export function reachableSheets(drawn: readonly unknown[]): SheetKey[] {
  const out = new Set<SheetKey>(), walked = new Set<unknown>();
  const walk = (v: unknown): void => {
    if (!v || typeof v !== "object" || walked.has(v)) return;
    walked.add(v);
    if (Array.isArray(v)) { for (const x of v) walk(x); return; }
    const o = v as Record<string, unknown>;
    const k = typeof o.open === "string" ? parseSheetKey(o.open) : null;
    if (k && k !== ALL_SOURCES_KEY && !k.startsWith("srcs:gl|")) out.add(k);
    for (const f in o) walk(o[f]);
  };
  walk(drawn);
  return [...out];
}

/**
 * (T1036) THE RULES A SHEET CITES, first seen first: a row of its Sources list (`sources`, never the "Overruled" cards), a
 * line that opens a rule's sheet and shows its page or cited page (`rule:<key>` beside a book / web run), and a rule's own
 * sheet (`rule:<key>`) its rule. Inside the sheet only: its swiped pages and folds are its own; a link to another sheet is
 * not followed.
 */
export function sheetCitations(key: SheetKey, vm: SheetVM): string[] {
  const out = new Set<string>(), walked = new Set<unknown>();
  if (key.startsWith("rule:")) out.add(key.slice(5));
  const isRun = (r: unknown): r is { t?: unknown; open?: unknown } => !!r && typeof r === "object";
  const walk = (v: unknown): void => {
    if (!v || typeof v !== "object" || walked.has(v)) return;
    walked.add(v);
    if (Array.isArray(v)) {
      // a line: its rule links count where the line shows a page
      if (v.some((r) => isRun(r) && (r.t === "book" || r.t === "web")))
        for (const r of v) if (isRun(r) && r.t === "link" && typeof r.open === "string" && r.open.startsWith("rule:")) out.add(r.open.slice(5));
      for (const x of v) walk(x);
      return;
    }
    const o = v as Record<string, unknown>;
    if (o.k === "sources") {
      if (o.overruled !== true)
        for (const g of o.groups as readonly { rows: readonly SourceRowVM[] }[]) for (const r of g.rows) if (r.key) out.add(r.key);
      return;
    }
    if (typeof o.open === "string" && o.open.startsWith("rule:") && showsSource(o)) out.add(o.open.slice(5));
    for (const f in o) walk(o[f]);
  };
  walk(vm.blocks);
  return [...out];
}

/** The rules the drawn screen cites (`drawnCitations`). */
export const drawnSourceRules = (drawn: readonly unknown[], rules: Pick<RuleIndex, "get" | "has" | "byKey">): RuleView[] =>
  drawnCitations(drawn, rules).rules;

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

/** `rules` (and `more`, rows that are no rule of this page) by source (the synopsis, DFO, a local authority); empty groups
 *  are not drawn. */
function groupsOf(rules: readonly RuleView[], more: readonly SourceRowVM[] = []): Group[] {
  const local = rules.filter((r) => !!r.facts?.cited);
  const dfo = rules.filter((r) => r.f.source === "dfo" && !local.includes(r));
  const book = rules.filter((r) => !dfo.includes(r) && !local.includes(r));
  const all: [string, SourceRowVM[]][] = [[ALL_SOURCES.BOOK, [...book.map(rowOf), ...more]], [ALL_SOURCES.DFO, dfo.map(rowOf)], [ALL_SOURCES.LOCAL, local.map(rowOf)]];
  return all.filter(([, rows]) => rows.length).map(([where, rows]) => ({ where, rows }));
}

/** The sheet: the drawn rules' groups (or "No rule on this page cites a source."), then (T1036) the reachable sheets' own
 *  rules under their head, when there are any. */
function sheetOfRules(rules: readonly RuleView[], page: Pick<RegsPage, "header" | "day">, more: readonly SourceRowVM[] = [],
                      other: readonly RuleView[] = []): SheetVM {
  const groups = groupsOf(rules, more), second = groupsOf(other);
  return { key: ALL_SOURCES_KEY, title: ALL_SOURCES.TITLE, sub: `${page.header.name}${WORDS.DOT}${fmtDay(page.day)}`,
    blocks: [groups.length ? { k: "sources", groups } : { k: "para", text: [text(ALL_SOURCES.NONE)] },
             ...(second.length ? [{ k: "head", text: ALL_SOURCES.OTHER } as const, { k: "sources", groups: second } as const] : [])] };
}

/** (T1036) A rule a drawn line cites (a key, or `#<index>` for the trout table's) or a salmon card's book closure names, that
 *  the page's index does not hold (W46: reported; a closure is still said by its place and pages, a line's rule is no row). */
export class AllSourcesRuleMissing extends Error {
  constructor(readonly rule: string, readonly by: "line" | "closure" | "sheet") {
    super(`every-source sheet: ${by === "closure" ? "the book closure's rule" : by === "sheet" ? "the rule a sheet cites" : "the drawn rule"} ${rule} is not on this page`);
    this.name = "AllSourcesRuleMissing";
  }
}

/**
 * (T1036) The second group's rules: what the sheets one tap from the drawn screen cite (`reachableSheets`, each with its
 * tabs, `sheetCitations`), minus `listed` (the first group). `sheet`: the page's own sheets (never the wrapped `src:all`).
 * A cited rule the index lacks is reported and gets no row (W46).
 */
function sheetsRules(drawn: readonly unknown[], sheet: RegsPage["sheet"], rules: Pick<RuleIndex, "get" | "has" | "byKey">,
                     listed: readonly RuleView[]): RuleView[] {
  const keys = new Set<string>(), opened = new Set<SheetKey>();
  const open = (k: SheetKey) => {
    if (opened.has(k)) return;
    opened.add(k);
    const vm = sheet(k);
    if (!vm) return;
    for (const r of sheetCitations(k, vm)) keys.add(r);
    for (const t of vm.tabs ?? []) open(t.open);
  };
  for (const k of reachableSheets(drawn)) open(k);
  const have = new Set(listed.map((r) => r.key)), out: RuleView[] = [];
  for (const k of keys) {
    const r = rules.byKey?.(k) ?? null;
    if (!r || r.missing) { reportFailure(new AllSourcesRuleMissing(k, "sheet")); continue; }
    if (!have.has(r.key)) { have.add(r.key); out.push(r); }
  }
  return out;
}

/** The drawn rules (each one the index lacks reported) and the book closures' rows, then (T1036) the reachable sheets'
 *  own rules (`sheet`: the page's sheets; none: no second group): the sheet. */
function drawnSheet(drawn: readonly unknown[], rules: Pick<RuleIndex, "get" | "has" | "byKey">, page: Pick<RegsPage, "header" | "day">,
                    book: readonly Pick<SalmonBookVM, "rule" | "water" | "words" | "pages">[], sheet: RegsPage["sheet"] | null): SheetVM {
  const cited = drawnCitations(drawn, rules), listed = cited.rules, more: SourceRowVM[] = [];
  for (const k of cited.missing) reportFailure(new AllSourcesRuleMissing(k, "line"));
  for (const b of book) {
    const r = rules.byKey?.(b.rule) ?? null;
    if (r && !r.missing) { if (!listed.some((x) => x.key === r.key)) listed.push(r); continue; }
    // (W46) never dropped: the closure as its card line says it, its pages linked as there
    reportFailure(new AllSourcesRuleMissing(b.rule, "closure"));
    more.push({ key: null, title: b.water, quote: b.words, role: "",
                book: b.pages.flatMap((x, i): Run[] => [...(i ? [text(" ")] : []), { t: "book", printed: x.printed, pdf: x.pdf }]) });
  }
  return sheetOfRules(listed, page, more, sheet ? sheetsRules(drawn, sheet, rules, listed) : []);
}

/** The page's own `src:all`: the rules the page cites drawn alone (`pageDrawn`); the screen re-builds it from what it draws,
 *  with the second group (it holds the page's sheets: `withDrawnSources`). */
export const allSourcesSheet = (ctx: SheetCtx): SheetVM => drawnSheet(pageDrawn(ctx.page), ctx.inp.rules, ctx.page, [], null);

/**
 * (T1036) The page's `sheet` with `src:all` built from what the SCREEN draws: `drawn` (`drawnRegsOf(page, tabs, licence,
 * lines)`, the Source row's input) and the salmon card's book closures (`book`: its Source lines carry pages, the closure
 * names its rule), so the sheet lists exactly the rules a drawn line cites. Any other key is the page's own.
 */
export function withDrawnSources(page: Pick<RegsPage, "sheet" | "header" | "day">, drawn: readonly unknown[],
                                 rules: Pick<RuleIndex, "get" | "has" | "byKey">,
                                 book: readonly Pick<SalmonBookVM, "rule" | "water" | "words" | "pages">[] = []): RegsPage["sheet"] {
  let vm: SheetVM | null = null;
  return (k) => (k === ALL_SOURCES_KEY ? (vm ??= drawnSheet(drawn, rules, page, book, page.sheet)) : page.sheet(k));
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
    // (T1036) the first block is the drawn rules' (the changes are the screen's); the second group follows as it was
    return { ...vm, blocks: [groups.length ? { k: "sources", groups } : { k: "para", text: [text(ALL_SOURCES.NONE)] }, ...vm.blocks.slice(1)] };
  };
}
