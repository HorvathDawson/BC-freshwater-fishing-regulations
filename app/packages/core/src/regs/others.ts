/**
 * T19 — which rows the Keep tab draws in full and the compact "Other fish" list ("Whitefish · 15 a day ›", "Kokanee" with
 * "Weekends: keep 5 · Weekdays: release" under it).
 *
 * Owner rule 2026-10-21: each row's kind, daily take, members, weekday status and group are the store's (`rows_row`); the
 * day's status is the settled store answer. This file SPLITS the rows for display and words each other-fish row's stored
 * answer; it decides no regulation. (T810 S) Which rows are trout and char is Python's: the trout table's `covers` (the
 * rows-frame rows today's trout cell replaces, contract Q-D4). Page v36's hand trout list is gone. The licence documents a row's fish
 * need (prototype round UR7 stampsFor on an Other fish row) are the store's (rows 32 `Row.stamps`, the pipeline's
 * `stamps.row_stamps`); each is drawn as tree.ts `stampLine` words it.
 *
 * Ports (inventory names @ 7fb6809f): page v36 rowsHtml (its hand trout list, rowsHtml.short), renderCard's rows condition;
 * prototype buildKeep Other fish (owk, the v33 size hint), thumb.
 */
import type { CardModel, CardStatus, HydRow } from "./card";
import { rowDown, type LadderSeams } from "./decided";
import { downLine } from "./flow";
import type { Settled } from "./settle";
import type { FishManifest } from "./fish.generated";
import { parentRow, rowTitle } from "./grouphead";
import { stampLine } from "./tree";
import type { RegsInputs } from "./inputs";
import type { OtherFishKeyVM, OtherFishRowVM } from "./model";
import { clean } from "./names";
import { WORDS } from "./words";

/** (T810 N) The rows-frame rows a trout table replaces (its `covers`, contract Q-D4). */
export type TableCovers = readonly number[];

/**
 * (T810 N, S) The rows today's trout cell replaces (`inp.trout.covers`); [] where the cell has none (a NULL cell, Q-D3:
 * no trout or char fish here, or the banner or the tidal page speaks) or the tap has no cell at all. The trout section
 * is required from rows 34 (`SECTION_NAMES`): a tap with a cell and no `trout` is refused, never read as "no table".
 */
export function tableCovers(inp: RegsInputs): TableCovers {
  if (inp.trout === undefined && inp.cell) throw new TroutSectionMissing();
  return inp.trout?.covers ?? [];
}

/** A tapped cell's inputs without the trout section (required from rows 34): built by something other than the store. */
class TroutSectionMissing extends Error {
  constructor() { super("regs: a cell's inputs carry no trout section (required from rows 34)"); this.name = "TroutSectionMissing"; }
}

/** page v36 renderCard: rows are drawn only on a day the store does not call closed (and that has its rows frame). */
export const rowsDrawn = (card: CardModel): boolean => card.status !== "closed" && !!card.ctx.status && !card.missingRows;

/**
 * page v36 rowsHtml, (T810 S) split by the trout table's `covers` (`TableCovers`): those rows are the table's, never
 * `main` or `rest`; the other fish are every row NOT in `covers`. Beside a table, two or more others are the compact list
 * (`rest`); fewer are drawn in full, as today (owner 2026-11-16 "Stays as today"). With no table (`covers` empty) every
 * row is drawn in order and `rest` is null (no Other fish list). No rows on a closed day.
 */
export function splitRows(card: CardModel, covers: TableCovers): { main: readonly number[]; rest: readonly number[] | null } {
  if (!rowsDrawn(card)) return { main: [], rest: null };
  const cov = new Set(covers), others = card.rows.map((_, i) => i).filter((i) => !cov.has(i));
  return others.length < 2 || others.length === card.rows.length ? { main: others, rest: null } : { main: [], rest: others };
}

/**
 * (T670 K4/K5) THE FULL ROWS NOTHING DRAWS: a row of `main` that is no parent's child (grouphead.ts `parentRow`), has no
 * stored group headline (so no card) and holds a pool (so no plain row) — page v36's legacy pooled card, never ported.
 * Such a row is drawn as a line of the compact list, its stored answer in one word and its full article one tap down
 * (`other:oN`), like every row of `rest`. (T810 S) The trout ones (Region 8's "20 brook trout from streams", p.68; the
 * Duncan River's "bull trout daily quota = 2", p.36) are the table's rows now (`covers`); this is for the other fish.
 */
export function looseRows(card: CardModel, covers: TableCovers): readonly number[] {
  return splitRows(card, covers).main.filter((i) => {
    const r = card.rows[i]!;
    return !parentRow(card, r) && !r.raw.group?.headline && !!r.pool;
  });
}

/** The rows of the compact list, in the order drawn: the loose full rows, then `rest`. Null: no list.
 *  `other:oN` is the N-th of these (sheets/other.ts reads the same list). */
export function listRows(card: CardModel, covers: TableCovers): readonly number[] | null {
  const loose = looseRows(card, covers), { rest } = splitRows(card, covers);
  return loose.length || rest ? [...loose, ...(rest ?? [])] : null;
}

/** page v36 rowsHtml.short: the row's stored answer in one word. */
export function shortValue(kind: CardStatus, daily: number | null): string {
  return kind === "keep" ? `${daily}${WORDS.A_DAY}` : kind === "nolimit" ? WORDS.NO_LIMIT : kind === "closed" ? WORDS.CLOSED_LC : WORDS.RELEASE_LC;
}

const SIZE_IN_GROUP = /"size_rule":"[^"]|"detail":"[^"]*\d+ cm/;

/** (T670 K4/K5) a row with no stored group says its size limit through its stored items: a band that does not run from 0
 *  with no upper end, or a stored size condition. */
const sizeInItems = (r: HydRow): boolean =>
  (r.items ?? []).some((it) => (it.bands ?? []).some(([from, to]) => from > 0 || to != null))
  || (r.conds ?? []).some((c) => c.c === "size");

/**
 * (T670 K2) THE FISH OF A ROW THAT MUST GO BACK: a row's stored conditions of kind "back" — members the row's number does
 * not hold for, each released or closed by its own rule (the Peace River's whitefish row keeps 15 lake whitefish while
 * "Mountain whitefish catch and release", p.66). Each is said in the rule's stored sentence ("Release every mountain
 * whitefish."), else the rule's label; the row's one word ("15 a day") is never the whole answer for them.
 */
export function backOf(r: HydRow): OtherFishRowVM["back"] {
  return (r.conds ?? []).flatMap((c) => {
    const st = c.status;
    if (c.c !== "back" || !c.r || (st !== "release" && st !== "closed")) return [];
    return [{ status: st, says: c.r.facts?.plain ?? clean(c.r.export.label), rule: c.r.key }];
  });
}

/** prototype Other fish: one compact row per list row (`listRows`), opening its full row as a sheet; null when there is no list.
 *  (T892) `from`: the row's sheet names a deciding step "<that water> · its tributaries" for all its fish (`rowDown`: the
 *  sheet's own flow, built with the same `seams`). */
export function otherFishOf(card: CardModel, inp: RegsInputs, fish: FishManifest, covers: TableCovers, settled: Settled,
  seams: LadderSeams): readonly OtherFishRowVM[] | null {
  const list = listRows(card, covers);
  if (!list) return null;
  return list.map((ix, i): OtherFishRowVM => {
    const r = card.rows[ix]!, g = r.raw.group;
    const hasSize = g ? SIZE_IN_GROUP.test(JSON.stringify(g.tree ?? g.kinds ?? [])) : sizeInItems(r);
    const from = downLine(rowDown(r, { card, settled, inp, seams }));
    const thumb = r.members.find((c) => (fish[c]?.length ?? 0) > 0) ?? null;
    return {
      thumb,
      name: rowTitle(r, inp), value: shortValue(r.kind, r.daily), tone: r.kind,
      week: r.raw.week?.status ?? null,
      sizeHint: hasSize && r.kind === "keep" ? WORDS.SIZE_HINT : null,
      open: `other:o${i}`,
      // (T1031) its picture opens the row's species sheet on Identification (the row, on Sources)
      ...(thumb !== null ? { id: `other:o${i}|id` as const } : {}),
      stamps: (r.raw.stamps ?? []).map(stampLine),
      back: backOf(r),
      ...(from ? { from } : {}),
    };
  });
}

/** prototype round UR6 Other fish `okey`: the glossary term each status word of the list stands for. */
export const OTHER_FISH_KEY_TERMS: readonly (readonly ["release" | "closed", string])[] = Object.freeze([
  ["release", "catch_and_release"], ["closed", "no_fishing_for"]] as const);

/** prototype okey: under the list, one line per status word a row shows (release, then closed), in the glossary's own
 *  plain words (`says`) with its ⓘ; a term the glossary lacks draws nothing. */
export function otherFishKeyOf(rows: readonly OtherFishRowVM[] | null, inp: RegsInputs): readonly OtherFishKeyVM[] {
  if (!rows) return [];
  return OTHER_FISH_KEY_TERMS.flatMap(([status, term]) => {
    const says = rows.some((r) => r.tone === status || r.back.some((b) => b.status === status))
      ? inp.statics.glossary.terms.find((t) => t.id === term)?.says.trim() : "";
    return says ? [{ status, says, term }] : [];
  });
}

/** The rows drawn as groups (T21's `KeepVM.groups`): the drawn full rows (`splitRows.main`, minus page v36's parent rows)
 *  whose stored group has a headline. */
export function groupRows(card: CardModel, drawn: ReadonlySet<number>, covers: TableCovers): readonly number[] {
  return splitRows(card, covers).main.filter((i) => drawn.has(i) && !!card.rows[i]!.raw.group?.headline);
}
