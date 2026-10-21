/**
 * T19 — which rows the Keep tab draws in full and the compact "Other fish" list ("Whitefish · 15 a day ›", "Kokanee" with
 * "Weekends: keep 5 · Weekdays: release" under it).
 *
 * Owner rule 2026-10-21: each row's kind, daily take, members, weekday status and group are the store's (`rows_row`); the
 * day's status is the settled store answer. This file SPLITS the rows for display (trout and char first, page v36's
 * TROUTISH) and words each other-fish row's stored answer; it decides no regulation.
 *
 * Ports (inventory names @ 7fb6809f): page v36 rowsHtml (TROUTISH, rowsHtml.short), renderCard's rows condition;
 * prototype buildKeep Other fish (owk, the v33 size hint), thumb.
 */
import type { CardModel, CardStatus } from "./card";
import type { FishManifest } from "./fish.generated";
import { rowTitle } from "./grouphead";
import type { RegsInputs } from "./inputs";
import type { OtherFishRowVM } from "./model";
import { WORDS } from "./words";

/** page v36 TROUTISH: trout and char first, in full; every other fish folds into one short list. */
export const TROUTISH: readonly string[] = Object.freeze(["RB", "CT", "EB", "GB", "LT", "DV", "ST", "BT", "CT_ws", "CT_c"]);

/** page v36 renderCard: rows are drawn only on a day the store does not call closed (and that has its rows frame). */
export const rowsDrawn = (card: CardModel): boolean => card.status !== "closed" && !!card.ctx.status && !card.missingRows;

/**
 * page v36 rowsHtml: `main` = the rows with a trout or char (all members), `rest` = the others. With no trout row, or
 * fewer than two others, every row is drawn in order and `rest` is null (no Other fish list). No rows on a closed day.
 */
export function splitRows(card: CardModel): { main: readonly number[]; rest: readonly number[] | null } {
  if (!rowsDrawn(card)) return { main: [], rest: null };
  const all = card.rows.map((_, i) => i);
  const main = all.filter((i) => card.rows[i]!.allMembers.some((S) => TROUTISH.includes(S)));   // page `allMembers || members`: always set
  const rest = all.filter((i) => !main.includes(i));
  return !main.length || rest.length < 2 ? { main: all, rest: null } : { main, rest };
}

/** page v36 rowsHtml.short: the row's stored answer in one word. */
export function shortValue(kind: CardStatus, daily: number | null): string {
  return kind === "keep" ? `${daily}${WORDS.A_DAY}` : kind === "nolimit" ? WORDS.NO_LIMIT : kind === "closed" ? WORDS.CLOSED_LC : WORDS.RELEASE_LC;
}

const SIZE_IN_GROUP = /"size_rule":"[^"]|"detail":"[^"]*\d+ cm/;

/** prototype Other fish: one compact row per `rest` row, opening its full row as a sheet; null when there is no list. */
export function otherFishOf(card: CardModel, inp: RegsInputs, fish: FishManifest): readonly OtherFishRowVM[] | null {
  const { rest } = splitRows(card);
  if (!rest) return null;
  return rest.map((ix, i): OtherFishRowVM => {
    const r = card.rows[ix]!, g = r.raw.group;
    const hasSize = !!g && SIZE_IN_GROUP.test(JSON.stringify(g.tree ?? g.kinds ?? []));
    return {
      thumb: r.members.find((c) => (fish[c]?.length ?? 0) > 0) ?? null,
      name: rowTitle(r, inp), value: shortValue(r.kind, r.daily), tone: r.kind,
      week: r.raw.week?.status ?? null,
      sizeHint: hasSize && r.kind === "keep" ? WORDS.SIZE_HINT : null,
      open: `other:o${i}`,
    };
  });
}

/** The rows drawn as groups (T21's `KeepVM.groups`): the drawn full rows (`splitRows.main`, minus page v36's parent rows)
 *  whose stored group has a headline. */
export function groupRows(card: CardModel, drawn: ReadonlySet<number>): readonly number[] {
  return splitRows(card).main.filter((i) => drawn.has(i) && !!card.rows[i]!.raw.group?.headline);
}
