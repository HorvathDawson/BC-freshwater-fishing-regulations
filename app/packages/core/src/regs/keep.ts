/**
 * T21 — the Keep tab's composition: which blocks are drawn, in which order, each filled by its presenter (T13 strip, T14
 * banner, T15 inherited / possession / where-not, T15b caveats, T17 group head, T18 tree, T19 not-kept / worked / quiet /
 * Other fish, T19b plain rows, T20 the fish manifest).
 *
 * Owner rule 2026-10-21 (decisions.md): every answer here is the PIPELINE's. This file decides no regulation — it only
 * SELECTS which presenter output to draw where, by the stored answers it is handed: the day's display status (closed /
 * tidal, `display_frame.status`), the key (`akey.tidal`; no key = outside B.C.), the rows frame's rows (each row's stored
 * `group.headline`, its pool) and page v36's draw order. The presenters below consume the store as they are; the five
 * derivations a parallel lane moves into the pipeline (closers, narrow_all, parent, record duty, stamps) are read through
 * them, never re-derived here.
 *
 * Ports (inventory names @ 7fb6809f): prototype buildKeep (the composition and draw order: the .inh line right after the
 * strip, section.nofish appended last), page v36 renderCard (what the card holds on an open / closed / tidal / outside day:
 * the in-part / side / angler / timed caveats before the rows, the unplaced rules after them), rowsHtml (the drawn rows
 * in order).
 */
import type { CardModel } from "./card";
import { caveatsOf, partClosedOf } from "./caveats";
import type { FishManifest } from "./fish.generated";
import { drawnRows, groupHeadOf, headCtx, originOf, rowTitle, scopeOf } from "./grouphead";
import type { RegsInputs } from "./inputs";
import type { CaveatVM, Day, GroupVM, KeepBlock, KeepVM, PlainRowVM, StripVM } from "./model";
import { otherFishKeyOf, otherFishOf, splitRows, tableCovers } from "./others";
import { rowDown } from "./decided";
import { downLine } from "./flow";
import type { RegsPlace } from "./place";
import { isPlainRow, plainRowOf, yearWindowOf, type PlainRowSeams } from "./plainrow";
import { settleAt, type Settled } from "./settle";
import { inheritedOf, possessionOf, whereNotOf } from "./standing";
import { bookIndex, BOOK_DAYS, runOf, stripOf } from "./strip";
import { bannerOf } from "./banner";
import { edgeKind, outsideKeep, tidalKeep } from "./edge";
import { MORE, moreOf, notKeptOf, workedOf } from "./tail";
import { fishNames, onePictureEach, pictures, treeEnv, treeOf } from "./tree";
import { TroutRefused, troutOf } from "./trout";
import { troutFrom } from "./sheets/trout";
import { reportFailure } from "../failure";

/** What the Keep composition reads besides the inputs (page.ts builds it once per page). */
export interface KeepEnv {
  readonly place: RegsPlace; readonly settled: Settled; readonly card: CardModel;
  readonly fish: FishManifest; readonly today: Day; readonly tot: "a" | "b";
}

/** The sibling seams T19b's plain rows read (plainrow.ts `PlainRowSeams`), bound to the merged presenters: T17 rowTitle /
 *  scopeOf, T18 pictures / fishName over the manifest, T13 runOf / bookIndex / BOOK_DAYS, T19 MORE. */
export function plainRowSeams(inp: RegsInputs, fish: FishManifest): PlainRowSeams {
  const name = fishNames(inp, fish);
  return {
    rowTitle, scopeOf,
    pictures: (codes) => pictures(codes, fish, name, inp.statics.exportSpecies.groups),
    fishName: name,
    hasFish: (c) => !!inp.statics.exportSpecies.fish[c] || !!fish[c]?.length,
    runOf: (perDay, d) => runOf(perDay.map((s) => [s, 1] as const), d),
    bookIndex, BOOK_DAYS, more: MORE,
  };
}

/** One g28 group (a drawn row with a stored `group.headline`): T17 head + the year window (T19b words over the row's
 *  stored year) + T18 tree + T19 tail. (T36: also an other-fish row's `other:` sheet.) */
export function groupOf(rowIx: number, inp: RegsInputs, env: KeepEnv, drawn: ReadonlySet<number>, seams: PlainRowSeams): GroupVM {
  const ctx = headCtx(rowIx, env.card, inp)!, tenv = treeEnv(ctx, env.fish, drawn, env.tot);
  const head = groupHeadOf(ctx, { tot: env.tot });
  // (T868) each fish pictured once in the card: on its first keep line
  const { tree, notKept } = onePictureEach(treeOf(tenv), notKeptOf(ctx.g, tenv));
  return {
    row: rowIx, title: ctx.title, scope: ctx.scope, head,
    origin: originOf(ctx),
    year: yearWindowOf(ctx.row, inp, env.settled.day, seams),
    tree, notKept, worked: workedOf(ctx.row, ctx.g),
    more: moreOf(rowIx),
  };
}

/** The rows a Keep tab draws, in page v36 order: a g28 group or a plain row (an index into its own list). */
export type KeepRowBlock = { group: number } | { plain: number };

/**
 * The draw order over the blocks a day has (prototype buildKeep + page v36 renderCard): strip → inherited (right after the
 * strip) → banner → the caveats but the unplaced rules → the rows → the unplaced rules → Other fish → Possession → Where
 * you can’t fish (appended last). Pure: which blocks exist is the presenters' answer; this only orders them.
 */
export function keepOrder(has: {
  readonly strip: boolean; readonly inherited: boolean; readonly banner: boolean;
  readonly caveats: readonly { readonly kind: CaveatVM["kind"] }[]; readonly rows: readonly KeepRowBlock[];
  readonly otherFish: boolean; readonly possession: boolean; readonly whereNot: boolean;
  /** (T810 N) the trout and char table: one block, where the trout rows stood (before the full rows) */
  readonly trout?: boolean;
}): KeepBlock[] {
  const order: KeepBlock[] = [];
  // (T796) a section's note, then (T623 F4) the part-closed warning, lead the tab, above the year strip
  const lead = (k: CaveatVM["kind"]) => k === "note" || k === "partclosed";
  has.caveats.forEach((c, i) => { if (c.kind === "note") order.push({ caveat: i }); });
  has.caveats.forEach((c, i) => { if (c.kind === "partclosed") order.push({ caveat: i }); });
  if (has.strip) order.push("strip");
  if (has.inherited) order.push("inherited");
  if (has.banner) order.push("banner");
  has.caveats.forEach((c, i) => { if (!lead(c.kind)) order.push({ caveat: i }); });
  if (has.trout) order.push("trout");
  order.push(...has.rows);
  if (has.otherFish) order.push("otherFish");
  if (has.possession) order.push("possession");
  if (has.whereNot) order.push("whereNot");
  return order;
}

/**
 * (T796, owner 2026-11-16) THE SECTION'S NOTES (`inp.notes`, core `section_note`): each Python's own sentence, drawn as it
 * is above the year strip on every day, open or closed ("Mostly outside Gladys Lake Ecological Reserve: its closure (rule
 * book p.9) does not apply here."). Said once; no rule, no items: the words are the note.
 */
export function sectionNotesOf(inp: RegsInputs): CaveatVM[] {
  return inp.notes.map((title) => ({ kind: "note", hot: false, title, calm: null, items: [], later: null }));
}

/**
 * The Keep tab of one tap on one day.
 * - no key (outside B.C.): page v36 renderCard's one foot line, nothing else (edge.ts `outsideKeep`, T42);
 * - tidal (a tidal key or display frame): the tidal banner alone (edge.ts `tidalKeep`; golden x31p_1010 `["div.banner"]`);
 * - an unnamed section is built like any water (edge.ts `edgeKind` "unnamed": only the header differs);
 * - otherwise every block its presenter draws, in `keepOrder`; a drawn row is a group when its stored group has a
 *   headline, else a plain row. A closed day (the stored display status) draws no caveats, rows, Other fish or Possession —
 *   (T623 F4) the part-closed warning leads the tab on every open day; (T796) the section's notes lead it on every day.
 */
/** (T892) the trout fold's tributary line (sheets/trout.ts `troutFrom`), read from the same rows frame as the trout
 *  sheets' rails (sheets.ts `tf:`); null: none. */
function troutFromOf(t: ReturnType<typeof troutOf>, inp: RegsInputs, card: CardModel): string | null {
  if (t?.kind !== "table") return null;
  const f = card.ctx.s >= 0 ? inp.year?.rowsBySeg[card.ctx.s] ?? null : null;
  return f ? troutFrom(t, (S) => f.fish[S] ?? null, inp.rules, inp.item?.name || inp.place?.name || "") : null;
}

/** (T892) a quota group with its fold's tributary line (the group sheet's own flow, `rowDown`); null: none. */
const withFrom = (g: GroupVM, from: string | null): GroupVM => (from ? { ...g, from } : g);

export function keepOf(inp: RegsInputs, env: KeepEnv): KeepVM {
  const edge = edgeKind(inp);                                       // (T42) the two edges with no card
  if (edge === "outside") return outsideKeep();
  const { settled, card } = env;
  if (edge === "tidal") return tidalKeep(inp, settled);

  const strip: StripVM | null = stripOf(inp, settled, env.today);
  const inherited = inheritedOf(inp);
  const banner = bannerOf(inp, settled, strip);
  const open = !!settled.status && settled.status.status !== "closed";
  // (T623 F4) a closure of an undrawn part is warned of above the strip, on every open day
  const partClosed = partClosedOf(settled, inp, env.today);
  // (T810 N, S) the trout table's rows and the fish they hold, which the timed caveat (b) leaves to the table; a table on
  // a day the store calls closed contradicts Q-D3: reported, not drawn
  const covers = tableCovers(inp);
  const covered = new Set(covers.flatMap((i) => card.rows[i]?.allMembers ?? []));
  if (!open && inp.trout) reportFailure(new TroutRefused("a table on a day the store calls closed (Q-D3)"));
  const trout = open ? troutOf(inp, settled.day, env.today) : null;
  const foldFrom = troutFromOf(trout, inp, card);
  const caveats = [...sectionNotesOf(inp), ...(partClosed ? [partClosed] : []),
                   ...(open ? caveatsOf(inp, settled, (j) => settleAt(inp, env.place, j, settled.day), covered) : [])];

  const groups: GroupVM[] = [], plainRows: PlainRowVM[] = [], rows: KeepRowBlock[] = [];
  const seams = plainRowSeams(inp, env.fish);
  if (open) {
    const drawn = drawnRows(card);
    for (const i of splitRows(card, covers).main) {
      if (!drawn.has(i)) continue;                                  // page v36 draws no article for a parent row
      const row = card.rows[i]!;
      if (row.raw.group?.headline) { rows.push({ group: groups.length }); groups.push(withFrom(groupOf(i, inp, env, drawn, seams), downLine(rowDown(row, { card, settled, inp, seams })))); }
      else if (isPlainRow(row)) { rows.push({ plain: plainRows.length }); plainRows.push(plainRowOf(i, { inp, card, settled, seams })); }
      // else: a pooled row with no stored headline — page v36's legacy card path, not ported. (T670 K4/K5) It is a line of
      // the compact list (others.ts `looseRows`)
    }
  }
  const otherFish = open ? otherFishOf(card, inp, env.fish, covers, settled, seams) : null;
  const possession = open ? possessionOf(inp, settled) : null;
  const whereNot = whereNotOf(settled);
  const order = keepOrder({ strip: !!strip, inherited: !!inherited, banner: !!banner, caveats, rows,
    otherFish: !!otherFish, possession: !!possession, whereNot: !!whereNot, ...(trout ? { trout: true } : {}) });
  return { order, strip, inherited, banner, groups, plainRows, otherFish, otherFishKey: otherFishKeyOf(otherFish, inp), caveats,
           possession, whereNot, outside: null, trout, ...(foldFrom ? { troutFrom: foldFrom } : {}) };
}
