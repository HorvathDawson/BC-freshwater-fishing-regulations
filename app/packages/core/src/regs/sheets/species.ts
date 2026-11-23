/**
 * (T1031 items 4-5, owner 2026-11-22: "right now we have identification and sources open from clicking different parts of
 * the a specieis line in table. Maybe we could have that be the same sheet with different tabs?") THE SPECIES SHEET: one
 * sheet per fish line, two tabs — Identification (its pictures and what to look for) | Sources (its answer, how it was
 * decided, and its source breakdown, `breakdown.ts`). Each tab is its own sheet key, so a tab press opens that key in the
 * sheet's place (SheetHost: no back stack; Done / back close it) and a start link names its tab:
 *
 *   the line                         Identification          Sources
 *   a trout table row                `tf:<row>|id`           `tf:<row>`
 *   an Other fish list row           `other:oN|id`           `other:oN`
 *   a fish of a card drawn on the    `sp:<code>`             `sp:<code>|src`
 *   page (a group / plain row)
 *   a salmon species                 `sp:<code>`             `sp:<code>|src` (the salmon card's, `salmonSheets.ts`)
 *
 * A fish's own picture key `sp:<code>` (a picture in a card, a sheet's pictures, an old link) opens the same sheet on its
 * Identification tab: the trout row's, else the Other fish row's, else its card row's. Nothing here decides a regulation:
 * each tab's blocks are the existing sheets' (`troutFishSheet`, `otherSheet`, `fishSheet`), the breakdown's rules the
 * row's stored deciding rules (`decided.rowSources`, the WINS roles).
 */
import type { HydRow } from "../card";
import { WINS, rowSources } from "../decided";
import { rowTitle } from "../grouphead";
import { listRows, tableCovers } from "../others";
import type { SheetBlock, SheetCtx, SheetTabVM, SheetVM, TroutVM } from "../model";
import type { SheetKey } from "../sheetKey";
import { SPECIES_SHEET, WORDS } from "../words";
import { breakdown, type Cited } from "./breakdown";
import { fishSheet } from "./fish";
import { otherSheet } from "./other";
import { troutFishSheet, troutIdSheet, troutRowOf } from "./trout";

/** A species sheet's two tab keys. */
export interface SpeciesPair { readonly id: SheetKey; readonly src: SheetKey }

/** The two tabs, the one drawn marked. */
export function speciesTabs(pair: SpeciesPair, on: "id" | "src"): readonly SheetTabVM[] {
  return [{ label: SPECIES_SHEET.ID, open: pair.id, on: on === "id" }, { label: SPECIES_SHEET.SRC, open: pair.src, on: on === "src" }];
}

const table = (ctx: SheetCtx): TroutVM | null => (ctx.page.keep.trout?.kind === "table" ? ctx.page.keep.trout : null);
const holds = (row: HydRow, code: string): boolean => row.members.includes(code) || row.allMembers.includes(code);

/** The Other fish list's N-th row (the list Keep drew), with its card index. */
function listRow(ctx: SheetCtx, i: number): { ix: number; row: HydRow } | null {
  const card = ctx.env?.card, ix = card ? listRows(card, tableCovers(ctx.inp))?.[i] : undefined;
  return card && ix !== undefined ? { ix, row: card.rows[ix]! } : null;
}

/**
 * The line a fish's picture key `sp:<code>` belongs to on this page: the trout table's row of that fish, else the Other
 * fish row holding it, else a card row drawn on the page holding it (its Sources tab `sp:<code>|src`). Null: the page has
 * no line for it here (a salmon species: the salmon card's, added by the screen; or a fish the page does not answer).
 */
export function pairOfFish(ctx: SheetCtx, code: string): SpeciesPair | null {
  const t = table(ctx), tr = t ? troutRowOf(t, code) : null;
  if (tr) return { id: `tf:${tr}|id`, src: `tf:${tr}` };
  const card = ctx.env?.card;
  if (!card) return null;
  const list = listRows(card, tableCovers(ctx.inp)) ?? [], i = list.findIndex((ix) => holds(card.rows[ix]!, code));
  if (i >= 0) return { id: `other:o${i}|id`, src: `other:o${i}` };
  const covers = tableCovers(ctx.inp);
  return card.rows.some((r, ix) => !covers.includes(ix) && !list.includes(ix) && holds(r, code)) ? { id: `sp:${code}`, src: `sp:${code}|src` } : null;
}

/** A card row's deciding rules (the WINS roles, as its `src:rN` sheet's "These decide it"), each with its role; `code`:
 *  only those that hold for that fish. */
function decidingRules(ctx: SheetCtx, row: HydRow, code?: string): Cited[] {
  const env = ctx.env!;
  return rowSources(row, env.card, env.settled, ctx.inp)
    .filter((it) => WINS.includes(it.role) && (code === undefined || it.spp.has(code)))
    .map((it) => ({ r: it.r, role: it.role }));
}

/** The breakdown's head and block, or nothing where no rule decides the line. */
const breakdownBlocks = (items: readonly Cited[]): SheetBlock[] =>
  items.length ? [{ k: "head", text: SPECIES_SHEET.BREAKDOWN }, breakdown(items)] : [];

/** An Other fish row's Identification: its one fish's sheet, or a page per fish it holds (swiped, as a book group's). */
function rowIdBlocks(ctx: SheetCtx, row: HydRow): SheetBlock[] | null {
  const pages = row.members.flatMap((m) => { const s = fishSheet(ctx.inp, `sp:${m}`, m, ctx.env?.fish); return s ? [s] : []; });
  if (!pages.length) return null;
  return pages.length === 1 ? [...pages[0]!.blocks] : [{ k: "swipe", pages }];
}

/**
 * The species sheet `key` opens, or `undefined` when `key` is not a species sheet's (another sheet's, for `sheetOf`). Null:
 * a species key this page cannot open (a stale one). A line with no Identification (no picture, no tips) keeps its Sources
 * sheet alone, with no tabs.
 */
export function speciesSheet(ctx: SheetCtx, key: SheetKey): SheetVM | null | undefined {
  // a trout table row: its Sources (the row's fish sheet, `troutFishSheet`) and its Identification
  if (key.startsWith("tf:")) {
    const t = table(ctx);
    if (!t) return null;
    const id = key.endsWith("|id"), rowKey = key.slice(3).replace(/\|id$/, ""), pair: SpeciesPair = { id: `tf:${rowKey}|id`, src: `tf:${rowKey}` };
    if (id) { const vm = troutIdSheet(key, t, rowKey, ctx.env?.fish); return vm && { ...vm, tabs: speciesTabs(pair, "id") }; }
    // (T883) the fish's stored answer in this segment's rows frame: its roles let the sheet show how it was decided
    const f = ctx.env && ctx.env.card.ctx.s >= 0 ? ctx.inp.year?.rowsBySeg[ctx.env.card.ctx.s] ?? null : null;
    const vm = troutFishSheet(key, t, rowKey, ctx.inp.rules, ctx.env?.fish, f ? (S) => f.fish[S] ?? null : undefined,
      ctx.inp.item?.name || ctx.inp.place?.name || "");
    return vm && { ...vm, tabs: speciesTabs(pair, "src") };
  }
  // an Other fish row: its Sources (`otherSheet`, its pictures now the Identification tab) and its Identification
  const other = /^other:o(\d+)(\|id)?$/.exec(key);
  if (other) {
    const n = +other[1]!, at = listRow(ctx, n);
    if (!at) return null;
    const pair: SpeciesPair = { id: `other:o${n}|id`, src: `other:o${n}` }, idBlocks = rowIdBlocks(ctx, at.row);
    if (other[2]) return idBlocks ? { key, title: rowTitle(at.row, ctx.inp), sub: null, blocks: idBlocks, tabs: speciesTabs(pair, "id") } : null;
    const vm = otherSheet(ctx, key, n);
    if (!vm) return null;
    if (!idBlocks) return vm;
    // the breakdown sits before the sheet's last link ("See every regulation line ›", every line, overruled too)
    const blocks = vm.blocks.filter((b) => b.k !== "pictures"), last = blocks[blocks.length - 1];
    const tail = last?.k === "link" ? [last] : [], body = tail.length ? blocks.slice(0, -1) : blocks;
    return { ...vm, blocks: [...body, ...breakdownBlocks(decidingRules(ctx, at.row)), ...tail], tabs: speciesTabs(pair, "src") };
  }
  const sp = /^sp:([A-Za-z0-9_]+)(\|src)?$/.exec(key);
  if (!sp) return undefined;
  const code = sp[1]!, pair = pairOfFish(ctx, code);
  if (sp[2]) {
    // a fish of a card drawn on the page: its answer is the card's; this tab is its breakdown, then the row's every line
    if (!pair || pair.src !== key) return null;
    const card = ctx.env!.card, ix = card.rows.findIndex((r, j) => !tableCovers(ctx.inp).includes(j) && holds(r, code));
    const row = card.rows[ix]!, id = fishSheet(ctx.inp, `sp:${code}`, code, ctx.env?.fish);
    const blocks: SheetBlock[] = [...breakdownBlocks(decidingRules(ctx, row, code)),
      { k: "link", line: [{ t: "link", s: WORDS.SEE_EVERY_LINE, open: `src:r${ix}` }] }];
    return { key, title: id?.title ?? rowTitle(row, ctx.inp), sub: null, blocks, ...(id ? { tabs: speciesTabs(pair, "src") } : {}) };
  }
  // the fish's picture key: its own sheet (every picture, what to look for), on the Identification tab of its line's sheet
  // — a trout row's carries its hatchery link (`tf:<row>|id`)
  if (pair?.id.startsWith("tf:")) {
    const vm = troutIdSheet(pair.id, table(ctx)!, pair.id.slice(3, -3), ctx.env?.fish);
    return vm && { ...vm, key, tabs: speciesTabs(pair, "id") };
  }
  const vm = fishSheet(ctx.inp, key, code, ctx.env?.fish);
  return vm && pair ? { ...vm, tabs: speciesTabs(pair, "id") } : vm;
}
