/**
 * STAND-INS for the sibling lanes' functions T19b's plain rows read through `PlainRowSeams` (core regs/plainrow.ts), so
 * the plain-row parity can run before those lanes merge. TEST-ONLY scaffolding, kept in this one file: when T13 (strip.ts
 * runOf / bookIndex / BOOK_DAYS), T17 (grouphead.ts rowTitle), T18 (tree.ts pictures / fishName) and T19 (MORE,
 * splitRows) are on the branch, T21 binds the real ones and this file is DELETED (plainrow.parity.test.ts then imports
 * those). Each stand-in is page v36 / the prototype at 7fb6809f, named after the function it stands in for.
 */
import {
  FISH, Names, cap, spName, type CardModel, type Day, type FishPicVM, type HydRow, type PlainRowSeams, type RegsInputs, type YearRunOf,
} from "@app/core";

const DIM = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
/** stands in for T13 BOOK_DAYS: the 365 book days, Jan 1 … Dec 31 (no Feb 29). */
export const BOOK_DAYS: readonly Day[] = DIM.flatMap((n, m) => Array.from({ length: n }, (_, i) => ({ month: m + 1, day: i + 1 })));
/** stands in for T13 bookIndex (page v36 DAYS.indexOf; Feb 29 reads Feb 28). */
export const bookIndex = (d: Day): number => BOOK_DAYS.findIndex((x) => x.month === d.month && x.day === Math.min(d.day, d.month === 2 ? 28 : d.day));
/** stands in for T13 runOf (page v36 runOf over a 365-long day-state array). */
export function runOf(days: readonly string[], d: Day): YearRunOf {
  const i = bookIndex(d), now = days[i]!;
  let a = i, b = i, g = 0;
  while (days[(a + 364) % 365] === now && g++ < 365) a = (a + 364) % 365;
  g = 0;
  while (days[(b + 1) % 365] === now && g++ < 365) b = (b + 1) % 365;
  if (g >= 365) return { now, all: true };
  return { now, all: false, from: BOOK_DAYS[a], until: BOOK_DAYS[b], next: BOOK_DAYS[(b + 1) % 365], then: days[(b + 1) % 365] };
}
/** stands in for T17 rowTitle (page v36 rowTitle). */
export function rowTitle(row: HydRow, inp: RegsInputs): string {
  const sp = inp.statics.exportSpecies, G = sp.groups;
  if (row.pool) {
    const w = (row.pool.f.species as string[] | undefined) ?? [];
    if (w.length === 1 && G[w[0]!]) return w[0] === "TROUT_CHAR" && ((row.pool.f.species_except as string[] | undefined) ?? []).includes("CHAR") ? "Trout" : G[w[0]!]!.name;
    return Names(sp, row.members, " and ");
  }
  if (row.prot) return "Protected species";
  if (!row.members.length && row.win) return cap(spName(sp, ((row.win.f.species as string[] | undefined) ?? [])[0]!).toLowerCase());
  return Names(sp, row.members, " and ");
}
/** stands in for T19 splitRows (page v36 rowsHtml: trout and char first; the rest fold into Other fish when ≥ 2). */
const TROUTISH = ["RB", "CT", "EB", "GB", "LT", "DV", "ST", "BT", "CT_ws", "CT_c"];
export function mainRows(card: CardModel): number[] {
  const all = card.rows.map((_, i) => i);
  const main = all.filter((i) => card.rows[i]!.allMembers.some((S) => TROUTISH.includes(S)));
  return !main.length || all.length - main.length < 2 ? all : main;
}

/** The seams bound for one state's inputs (T18 pictures / fishName over the manifest; T19's MORE). */
export function seamsFor(inp: RegsInputs): PlainRowSeams {
  const sp = inp.statics.exportSpecies;
  const fishName = (c: string) => sp.fish[c]?.name || FISH[c]?.[0]?.caption || c;
  return {
    rowTitle, runOf, bookIndex, BOOK_DAYS, fishName,
    hasFish: (c) => !!sp.fish[c] || !!FISH[c]?.length,
    pictures: (codes) => codes.map((c): FishPicVM => {
      const f = FISH[c]?.[0];
      return f ? { code: c, name: fishName(c), caption: `${f.caption} ›`, open: `sp:${c}`, sil: false } : { code: c, name: fishName(c), sil: true };
    }),
    more: "See every regulation line ›",
  };
}
