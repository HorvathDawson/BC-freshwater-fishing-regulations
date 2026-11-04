/**
 * T36 part 2 — the two "where it comes from" sheets: an item's Sources (`srcs:`) and a row's every regulation line
 * (`src:rN`). (The licence's "Where each one comes from" `licsrc:` sheet is removed, owner 2026-11-04.)
 *
 * Owner rule 2026-10-21: nothing here decides a regulation. The rules an item's source line names are T30–T32's
 * (`SourceLineVM.keys`, read off the stored gear answer); a row's rules and their roles are the stored
 * `rows_decided.roles` and lifts, gathered by decided.ts `rowSources` (page v36 rowSources). This file selects them for a key and words them as the prototype's sheet does.
 *
 * Ports (regs-v11.html @ 7fb6809f): sheetHtml srcs: / src: branches, groupedSources, itemSrc (SRCS), levelOf,
 * downWater; page_v36.js openRowSources, srcCard, ROLE, WINS.
 */
import { ruleBook } from "../book";
import type { CardModel } from "../card";
import { WINS, rowSources, type LadderItem } from "../decided";
import { closedGate } from "../describe";
import { downWater, levelWord } from "../gear";
import { rowTitle } from "../grouphead";
import type { RuleView } from "../inputs";
import type { SheetBlock, SheetCtx, SheetVM, SourceLineVM, SourceRowVM } from "../model";
import { clean } from "../names";
import type { SheetKey } from "../sheetKey";
import { fmtDay } from "../strip";
import { ROLE, WORDS } from "../words";

// ── srcs: ────────────────────────────────────────────────────────────────────────────────────────────────────────────

/** The source line a `srcs:` key belongs to on this page (T30 Gear: mode / lines / hook / weight / bait; T32 Methods: ways
 *  / boats / always). */
function sourceLineOf(ctx: SheetCtx, key: SheetKey): SourceLineVM | null {
  const { gear, methods } = ctx.page;
  const all: (SourceLineVM | null | undefined)[] = [gear?.mode?.src, ...(gear?.lines ?? []).map((l) => l.src), gear?.bait?.src,
    methods?.ways?.src, methods?.boats?.src, methods?.reference?.src];
  return all.find((s) => s?.open === key) ?? null;
}

/** One rule as a Sources row (sheetHtml srcs: `.prule`): its title, who says it (the downstream water, else its level),
 *  where it applies, its Rule book pages; it opens the rule's own sheet. */
export function ruleRow(r: RuleView): SheetBlock {
  return { k: "rule", title: clean(r.export.label || WORDS.SHEET_RULE), level: downWater(r) || levelWord(r),
    applies: [r.facts?.applies, r.facts?.applies_trib].filter((a): a is string => !!a), book: ruleBook(r), open: `rule:${r.key}` };
}

/** sheetHtml srcs: — the rules behind one item's source line, each a row, in the line's order. */
export function srcsSheet(ctx: SheetCtx, key: SheetKey): SheetVM | null {
  const src = sourceLineOf(ctx, key), rules = ctx.inp.rules;
  if (!src || !rules.byKey) return null;
  const rows = src.keys.map((k) => rules.byKey!(k)).filter((r): r is RuleView => !!r).map(ruleRow);
  return { key, title: WORDS.SHEET_SOURCES, sub: null, blocks: rows };
}

// ── src:rN ───────────────────────────────────────────────────────────────────────────────────────────────────────────

const sameSet = (a: readonly string[], b: readonly string[]): boolean => a.length === b.length && a.every((x) => b.includes(x));

/** page v36 openRowSources `card`: the role's words, the rule it lost to (its exact words), the fish it holds for. */
function roleText(it: LadderItem, allMembers: readonly string[]): string {
  let t = ROLE[it.role] ?? it.role;
  if (it.role === "governs" && it.r.kind === "gate") t = closedGate(it.r) ? WORDS.SHEET_CLOSES_IT : WORDS.SHEET_RELEASE_ALL;
  if (it.by && !it.by.missing) t += " " + WORDS.SHEET_QUOTED(clean(it.by.export.verbatim));
  if (it.spp.size && it.spp.size < allMembers.length)
    t += WORDS.DOT + ((it.r.facts?.subsets ?? []).find((x) => sameSet(x.fish as readonly string[], [...it.spp]))?.for || WORDS.SHEET_NO_FOR);
  return t;
}

/** page v36 srcCard's "whose" line: the rule's provenance. */
const whoseOf = (r: RuleView): string => r.export.prov.who || r.export.prov.entry_name || "";

/**
 * page v36 openRowSources in the prototype's groupedSources: "These decide it (N)" — the deciding rules grouped under whose
 * they are (first seen first), each its title and exact words, its role and pages — then "Overruled (N)", folded, each
 * card as page v36 drew it. Title the row's; sub the water and the day.
 */
export function srcSheet(ctx: SheetCtx, key: SheetKey, rowIx: number): SheetVM | null {
  const env = ctx.env, card: CardModel | undefined = env?.card, row = card?.rows[rowIx];
  if (!env || !card || !row) return null;
  const items = rowSources(row, card, env.settled, ctx.inp);
  const won = items.filter((i) => WINS.includes(i.role)), lost = items.filter((i) => !WINS.includes(i.role));
  const rowVM = (it: LadderItem): SourceRowVM => ({ key: it.r.missing ? null : it.r.key, title: it.r.facts?.title || null,
    quote: clean(it.r.export.verbatim), role: roleText(it, row.allMembers), book: ruleBook(it.r) });
  const groups: { where: string; rows: SourceRowVM[] }[] = [];
  for (const it of won) {
    const where = whoseOf(it.r), g = groups.find((x) => x.where === where);
    if (g) g.rows.push(rowVM(it)); else groups.push({ where, rows: [rowVM(it)] });
  }
  const blocks: SheetBlock[] = [{ k: "head", text: WORDS.SHEET_DECIDE_IT(won.length) }, { k: "sources", groups }];
  if (lost.length) blocks.push({ k: "fold", summary: WORDS.SHEET_OVERRULED(lost.length), open: false, body: [{ k: "sources", overruled: true,
    groups: lost.map((it) => ({ where: whoseOf(it.r), rows: [{ ...rowVM(it), book: [], notes: it.r.notes,
      trib: it.r.via === "trib" || it.r.rank === 1 }] })) }] });
  return { key, title: rowTitle(row, ctx.inp), sub: WORDS.SHEET_SRC_SUB(ctx.page.header.name, fmtDay(ctx.page.day)), blocks };
}
