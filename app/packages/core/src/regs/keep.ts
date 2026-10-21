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
import { caveatsOf } from "./caveats";
import type { FishManifest } from "./fish.generated";
import { drawnRows, groupHeadOf, headCtx, originOf, rowTitle, scopeOf } from "./grouphead";
import type { RegsInputs } from "./inputs";
import type { CaveatVM, Day, GroupVM, KeepBlock, KeepVM, PlainRowVM, StripVM } from "./model";
import { otherFishKeyOf, otherFishOf, splitRows } from "./others";
import type { RegsPlace } from "./place";
import { isPlainRow, plainRowOf, yearWindowOf, type PlainRowSeams } from "./plainrow";
import { groupLine } from "./pins";
import { settleAt, type Settled } from "./settle";
import { inheritedOf, possessionOf, whereNotOf } from "./standing";
import { bookIndex, BOOK_DAYS, runOf, stripOf } from "./strip";
import { bannerOf } from "./banner";
import { MORE, moreOf, notKeptOf, quietOf, workedOf } from "./tail";
import { fishNames, pictures, treeEnv, treeOf } from "./tree";
import { WORDS } from "./words";

/** What the Keep composition reads besides the inputs (page.ts builds it once per page). */
export interface KeepEnv {
  readonly place: RegsPlace; readonly settled: Settled; readonly card: CardModel;
  readonly fish: FishManifest; readonly today: Day; readonly tot: "a" | "b";
}

const NONE: Omit<KeepVM, "order"> = Object.freeze({
  strip: null, inherited: null, banner: null, groups: [], plainRows: [], otherFish: null, otherFishKey: [], caveats: [], possession: null,
  whereNot: null, outside: null,
});

/** The sibling seams T19b's plain rows read (plainrow.ts `PlainRowSeams`), bound to the merged presenters: T17 rowTitle /
 *  scopeOf, T18 pictures / fishName over the manifest, T13 runOf / bookIndex / BOOK_DAYS, T19 MORE. */
export function plainRowSeams(inp: RegsInputs, fish: FishManifest): PlainRowSeams {
  const name = fishNames(inp, fish);
  return {
    rowTitle, scopeOf,
    pictures: (codes) => pictures(codes, fish, name),
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
  const head = groupHeadOf(ctx, { tot: env.tot }), root = ctx.g.tree[0];
  return {
    row: rowIx, title: ctx.title, scope: ctx.scope, head,
    pinLine: root ? groupLine(ctx.title, root, ctx.g.headline, head.form === "a") : null,
    origin: originOf(ctx),
    year: yearWindowOf(ctx.row, inp, env.settled.day, seams),
    tree: treeOf(tenv), notKept: notKeptOf(ctx.g, tenv), worked: workedOf(ctx.row, ctx.g), quiet: quietOf(ctx.row, env.card),
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
}): KeepBlock[] {
  const order: KeepBlock[] = [];
  if (has.strip) order.push("strip");
  if (has.inherited) order.push("inherited");
  if (has.banner) order.push("banner");
  has.caveats.forEach((c, i) => { if (c.kind !== "uncertain") order.push({ caveat: i }); });
  order.push(...has.rows);
  has.caveats.forEach((c, i) => { if (c.kind === "uncertain") order.push({ caveat: i }); });
  if (has.otherFish) order.push("otherFish");
  if (has.possession) order.push("possession");
  if (has.whereNot) order.push("whereNot");
  return order;
}

/**
 * The Keep tab of one tap on one day.
 * - no key (outside B.C.): page v36 renderCard's one foot line, nothing else (`order: ["outside"]`; T42 draws it);
 * - tidal: the tidal banner alone (golden x31p_1010 `["div.banner"]`);
 * - otherwise every block its presenter draws, in `keepOrder`; a drawn row is a group when its stored group has a
 *   headline, else a plain row. A closed day (the stored display status) draws no caveats, rows, Other fish or Possession.
 */
export function keepOf(inp: RegsInputs, env: KeepEnv): KeepVM {
  if (inp.key == null) return { ...NONE, order: ["outside"], outside: WORDS.OUTSIDE_BC };
  const { settled, card } = env;
  if (inp.key.tidal || settled.status?.status === "tidal") {
    const banner = bannerOf(inp, settled, null);
    return { ...NONE, order: banner ? ["banner"] : [], banner };
  }

  const strip: StripVM | null = stripOf(inp, settled, card, env.today);
  const inherited = inheritedOf(inp);
  const banner = bannerOf(inp, settled, strip);
  const open = !!settled.status && settled.status.status !== "closed";
  const caveats = open ? caveatsOf(inp, settled, (j) => settleAt(inp, env.place, j, settled.day)) : [];

  const groups: GroupVM[] = [], plainRows: PlainRowVM[] = [], rows: KeepRowBlock[] = [];
  if (open) {
    const drawn = drawnRows(card), seams = plainRowSeams(inp, env.fish);
    for (const i of splitRows(card).main) {
      if (!drawn.has(i)) continue;                                  // page v36 draws no article for a parent row
      const row = card.rows[i]!;
      if (row.raw.group?.headline) { rows.push({ group: groups.length }); groups.push(groupOf(i, inp, env, drawn, seams)); }
      else if (isPlainRow(row)) { rows.push({ plain: plainRows.length }); plainRows.push(plainRowOf(i, { inp, card, settled, seams })); }
      // else: a pooled row with no stored headline (klmb_1010, decisions D19) — page v36's legacy card path, not ported
    }
  }
  const otherFish = open ? otherFishOf(card, inp, env.fish) : null;
  const possession = open ? possessionOf(card, inp, settled) : null;
  const whereNot = whereNotOf(settled);
  const order = keepOrder({ strip: !!strip, inherited: !!inherited, banner: !!banner, caveats, rows,
    otherFish: !!otherFish, possession: !!possession, whereNot: !!whereNot });
  return { order, strip, inherited, banner, groups, plainRows, otherFish, otherFishKey: otherFishKeyOf(otherFish, inp), caveats,
           possession, whereNot, outside: null };
}
