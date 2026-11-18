/**
 * (T221, owner 2026-10-28 — a TRY) THE AT-A-GLANCE TABLE: the Keep tab's species list ("Other fish": picture, name, the
 * day's answer on the right) also carries the quota groups (Trout and char) and the DFO Salmon card as FOLDED rows —
 * each opening, in place, to its full card. This file words those two kinds of row; the component draws them beside the
 * Other fish rows it already has. (Owner 2026-10-28) a folded row says NO count — "5 a day" misread as the whole group's
 * answer — and a status word only where it is true for every row of the card ("closed", "check DFO"); else none.
 * (T227, owner 2026-10-28) A card that is one closed-all-year statement (Region 4's "no fishing for salmon … until
 * further notice") is a plain "closed" row with no Details: there is nothing more to see.
 */
import type { GroupVM, TreeNodeVM, TroutVM } from "./regs/model";
import { SALMON_WORDS, type SalmonBookVM, type SalmonCardVM } from "./salmon";

export const GLANCE_WORDS = {
  /** the table's head once it holds more than the other fish */
  head: "Species and limits",
  /** the fold's affordance, closed and open */
  show: "Details",
  hide: "Hide",
} as const;

/** (T531) the rule source a species row's answer comes from. */
export type SpeciesSource = "dfo" | "bc";

/** One folded row: the picture's code, the name, the status word true for the whole card (null: none) and its tone. */
export interface GlanceRowVM {
  /** (T531) whose rule answers this row: DFO's salmon feed, or the B.C. synopsis rules. It decides the calendar the row sits under. */
  readonly source: SpeciesSource;
  readonly thumb: string | null;
  readonly name: string;
  readonly value: string | null;
  /** (T810 N) "none": the trout table's fold tone as shipped (`Fold.tone`, nothing to keep), taken with no mapping */
  readonly tone: "keep" | "release" | "closed" | "nolimit" | "none";
  /** (T810 N) the trout fold's small date beside its value (`DateSet.fold`, "until Mar 31"), drawn as shipped. Absent: none. */
  readonly date?: string;
  /** (T227) false: nothing more to see — a plain row, no "Details" (a water whose salmon are one closed-all-year line) */
  readonly fold?: boolean;
  /** (T759, owner 2026-11-15: "mark the folded row") the card carries a DFO issue banner: the folded row wears a small mark, so a banner inside is not missed. Absent: none. */
  readonly issue?: boolean;
  /** (T796, owner 2026-11-16 15:19: "we need to make sure it says where the closure is from") on a plain closed row
   *  (`fold` false) that is the rule book's closure: each closure's words (page, line, dates) and where it is from,
   *  said under the row — the card behind it is never drawn there */
  readonly book?: readonly SalmonBookVM[];
}

/** The first picture a group's tree shows — a rainbow trout where the group has one (the picture anglers know it by). */
function thumbOf(nodes: readonly TreeNodeVM[]): string | null {
  const codes: string[] = [];
  const walk = (ns: readonly TreeNodeVM[]) => {
    for (const n of ns) {
      if (n.t === "tier") walk(n.children);
      else for (const p of n.pictures) if (!p.sil) codes.push(p.code);
    }
  };
  walk(nodes);
  return codes.includes("RB") ? "RB" : codes[0] ?? null;
}

/** A quota group's folded row: its title and picture, no count (owner 2026-10-28: a group's number misleads). */
export function groupGlance(g: GroupVM): GlanceRowVM {
  return { source: "bc", thumb: thumbOf(g.tree ?? []), name: g.title, value: null, tone: "keep" };
}

/**
 * (T810 N) THE TROUT AND CHAR ENTRY'S FOLDED ROW, from the trout table: its value, tone and date
 * exactly as Python ships them (`Fold.value` "Up to 5", `Fold.tone`, `DateSet.fold`). The 2026-10-28 "a folded row says
 * no count" rule is lifted for the trout fold only (owner 2026-11-16 15:47: "the better text like 2 or 4 with expand
 * arrow"): the count is the table's own words, not a group's number. The picture: a rainbow trout where the table holds
 * one, else its first fish.
 */
export function troutGlance(t: TroutVM): GlanceRowVM {
  const fish = t.boxes.flatMap((b) => b.rows.map((r) => r.fish));
  return { source: "bc", thumb: fish.includes("RB") ? "RB" : fish[0] ?? null, name: t.fold.name, value: t.fold.value, tone: t.fold.tone,
           ...(t.fold.date !== null ? { date: t.fold.date } : {}) };
}

/**
 * The Salmon card's folded row: its title, and its status word ("closed", "check DFO" …) only when every row of the
 * card has that status; else none. (T372, the trout way) an undrawn DFO place is the card's caveat, not its answer.
 */
export function salmonGlance(card: SalmonCardVM): GlanceRowVM {
  // (T759) a DFO issue banner lives inside the card: the row is marked, and a closed-all-year row still opens to it
  const issue = card.issues.length > 0;
  const mark = issue ? { issue: true as const } : {};
  if (card.closedOnly) return { source: "dfo", thumb: "CO", name: card.title, value: SALMON_WORDS.status.closed.toLowerCase(), tone: "closed", fold: issue, ...mark,
                                 ...(card.book.length ? { book: card.book } : {}) };
  const all = card.rows.map((r) => r.status);
  const s = all.length && all.every((x) => x === all[0]) ? all[0]! : null;
  if (s === null || s === "varies") return { source: "dfo", thumb: "CO", name: card.title, value: null, tone: "keep", ...mark };
  const word = SALMON_WORDS.status[s];
  return { source: "dfo", thumb: "CO", name: card.title, value: s === "check" ? word.replace(/^Check/, "check") : word.toLowerCase(),
           tone: s === "closed" ? "closed" : s === "release" ? "release" : "keep", ...mark };
}

/**
 * (T531, owner 2026-11-09) WHICH SPECIES SIT UNDER WHICH CALENDAR. Each row goes under the block of the rule source that
 * answers it (`GlanceRowVM.source`): a DFO row under the Salmon (DFO) calendar, a B.C. row under the Game fish (B.C.
 * rules) one — never a list kept by hand. `dfoBlock` false (no salmon calendar is drawn: DFO has no rules for the
 * water) leaves a DFO row without a block of its own, so it stays with the rest. Order inside a block is the input's.
 */
export function speciesBlocks<T extends { readonly row: GlanceRowVM }>(rows: readonly T[], dfoBlock: boolean): { dfo: T[]; rest: T[] } {
  const dfo = rows.filter((x) => dfoBlock && x.row.source === "dfo");
  return { dfo, rest: rows.filter((x) => !dfo.includes(x)) };
}

/** (T531) The status a DFO block says when it has no year bar (a salmon row with no DFO calendar): the row's word, capital first; null: none. */
export function dfoBlockStatus(row: GlanceRowVM): string | null {
  return row.value ? row.value.charAt(0).toUpperCase() + row.value.slice(1) : null;
}
