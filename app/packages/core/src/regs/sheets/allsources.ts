/**
 * (T990, owner 2026-11-21: "in the regulation pane we should have a source button at button with all regulation sources in
 * one consistent spot") THE PAGE'S EVERY-SOURCE SHEET (`src:all`): the union of what the sections' own Sources buttons
 * open — the Gear / Methods source lines' rules, the trout table's, each card row's deciding rules — grouped by where
 * they come from, one row per rule (no duplicates), plus the Province's in-season changes. No rule is decided here: a
 * row is the same one the section's sheet draws (`ruleBook` gives its pages, or a local authority's citation, T981).
 */
import { ruleBook } from "../book";
import { WINS, rowSources } from "../decided";
import type { RuleView } from "../inputs";
import type { InSeasonBannerVM } from "../../inseason";
import { text } from "../line";
import type { SheetCtx, SheetVM, SourceRowVM } from "../model";
import { clean } from "../names";
import type { SheetKey } from "../sheetKey";
import { fmtDay } from "../strip";
import { WORDS, ALL_SOURCES } from "../words";

export const ALL_SOURCES_KEY = "src:all" as const satisfies SheetKey;

type Group = { where: string; rows: SourceRowVM[] };

/** Every rule the page's sections draw sources for, once each (first seen first). */
export function pageSourceRules(ctx: SheetCtx): RuleView[] {
  const { inp, page, env } = ctx, rules = inp.rules, seen = new Map<string, RuleView>();
  const add = (r: RuleView | null | undefined) => { if (r && !r.missing && !seen.has(r.key)) seen.set(r.key, r); };
  const g = page.gear, m = page.methods;
  const lines = [g?.src, g?.mode?.src, ...(g?.lines ?? []).map((l) => l.src), g?.bait?.src, m?.ways?.src, m?.boats?.src, m?.reference?.src];
  for (const l of lines) for (const k of l?.keys ?? []) add(rules.byKey?.(k));
  const t = page.keep.trout;
  if (t?.kind === "table") for (const ix of t.src) add(rules.get(ix));
  if (env) for (const row of env.card.rows)
    for (const it of rowSources(row, env.card, env.settled, inp)) if (WINS.includes(it.role)) add(it.r);
  return [...seen.values()];
}

const rowOf = (r: RuleView): SourceRowVM =>
  ({ key: r.key, title: r.facts?.title || null, quote: clean(r.export.verbatim), role: "", book: ruleBook(r) });

/** The Province's in-season changes as Sources rows (one per change id), each linking its notice page. */
export function inSeasonRows(banners: readonly InSeasonBannerVM[]): SourceRowVM[] {
  const ids = new Set<string>();
  return banners.filter((b) => !ids.has(b.id) && ids.add(b.id)).map((b): SourceRowVM =>
    ({ key: null, title: b.head, quote: b.text, role: "", book: [{ t: "web", s: ALL_SOURCES.PROVINCE_SITE, url: b.url }] }));
}

/** The sheet: the rules by source (the synopsis, DFO, a local authority). Empty groups are not drawn. */
export function allSourcesSheet(ctx: SheetCtx): SheetVM {
  const rules = pageSourceRules(ctx);
  const local = rules.filter((r) => !!r.facts?.cited);
  const dfo = rules.filter((r) => r.f.source === "dfo" && !local.includes(r));
  const book = rules.filter((r) => !dfo.includes(r) && !local.includes(r));
  const groups: Group[] = ([[ALL_SOURCES.BOOK, book], [ALL_SOURCES.DFO, dfo], [ALL_SOURCES.LOCAL, local]] as const)
    .filter(([, rs]) => rs.length).map(([where, rs]) => ({ where, rows: rs.map(rowOf) }));
  return { key: ALL_SOURCES_KEY, title: ALL_SOURCES.TITLE, sub: `${ctx.page.header.name}${WORDS.DOT}${fmtDay(ctx.page.day)}`,
    blocks: groups.length ? [{ k: "sources", groups }] : [{ k: "para", text: [text(ALL_SOURCES.NONE)] }] };
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
