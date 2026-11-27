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
import { RULE_BOOK } from "./sourceNames";
import { SALMON_WORDS, type SalmonBookVM, type SalmonCardVM } from "./salmon";

export const GLANCE_WORDS = {
  /** the table's head once it holds more than the other fish */
  head: "Species and limits",
  /** the fold's affordance, closed and open */
  show: "Details",
  hide: "Hide",
} as const;

/**
 * (T879, owner 2026-11-18 ~20:53, approved ~22:58) "Two headers, one table": both status blocks first, each named by the fish
 * it speaks for and tagged with whose rules they are, then ONE species list whose every row wears the same tag. The tag
 * words are these; a block's tag and its rows' tag are the same word, so a row is matched to its block by eye.
 */
export const SOURCE_WORDS = {
  /** a row's or a block's tag, by its rule source */
  tag: { dfo: "DFO", bc: "B.C." },
  /** a salmon bar some of whose days are the rule book's (core `SalmonSourceBy`) */
  salmonTag: { dfo: "DFO", both: `DFO + ${RULE_BOOK}`, book: RULE_BOOK },
  /** the two blocks' names, the tag beside each */
  salmon: "Salmon",
  game: "Game fish",
  /** (T1207) the ONE block's name where one closure shuts both for the whole year (`closedWithSalmon`); its tag is `salmonTag.book` */
  both: "Salmon and game fish",
  /** (T1207) what a screen reader says for the one block's header: both fish and the status */
  bothLabel: "Salmon and game fish: closed all year",
} as const;

/** (T531) the rule source a species row's answer comes from. */
export type SpeciesSource = "dfo" | "bc";

/** One folded row: the picture's code, the name, the status word true for the whole card (null: none) and its tone. */
export interface GlanceRowVM {
  /** (T531) whose rule answers this row: DFO's salmon feed, or the B.C. synopsis rules. (T879) The tag the row wears in the one list. */
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
  /** (T892, owner 2026-11-19 "The table should say if it came from tributaries too..") the row's answer rests on another
   *  water's row carried down to its tributaries: one line under the name, the words core's (flow.ts `downLine`). Absent: none. */
  readonly from?: string;
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

/** A quota group's folded row: its title and picture, no count (owner 2026-10-28: a group's number misleads); (T892) its
 *  tributary line (`GroupVM.from`). */
export function groupGlance(g: GroupVM): GlanceRowVM {
  return { source: "bc", thumb: thumbOf(g.tree ?? []), name: g.title, value: null, tone: "keep", ...(g.from ? { from: g.from } : {}) };
}

/**
 * (T810 N) THE TROUT AND CHAR ENTRY'S FOLDED ROW, from the trout table: its value, tone and date
 * exactly as Python ships them (`Fold.value` "Up to 5", `Fold.tone`, `DateSet.fold`). The 2026-10-28 "a folded row says
 * no count" rule is lifted for the trout fold only (owner 2026-11-16 15:47: "the better text like 2 or 4 with expand
 * arrow"): the count is the table's own words, not a group's number. The picture: a rainbow trout where the table holds
 * one, else its first fish. (T892) `from`: `KeepVM.troutFrom`, the fold's tributary line.
 */
export function troutGlance(t: TroutVM, from?: string): GlanceRowVM {
  const fish = t.boxes.flatMap((b) => b.rows.map((r) => r.fish));
  return { source: "bc", thumb: fish.includes("RB") ? "RB" : fish[0] ?? null, name: t.fold.name, value: t.fold.value, tone: t.fold.tone,
           ...(t.fold.date !== null ? { date: t.fold.date } : {}), ...(from ? { from } : {}) };
}

/**
 * The Salmon card's folded row: its title, and its status word ("closed", "check DFO", "to be determined" …) only when every row of the
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
  // (T882) every row DFO's own "To be determined": the row says so, never "check DFO"
  const tbd = s === "check" && card.rows.every((r) => r.statusLabel === SALMON_WORDS.tbd.status);
  return { source: "dfo", thumb: "CO", name: card.title,
           value: tbd ? SALMON_WORDS.tbd.day : s === "check" ? word.replace(/^Check/, "check") : word.toLowerCase(),
           tone: s === "closed" ? "closed" : s === "release" ? "release" : "keep", ...mark };
}

/** (T531) The status a DFO block says when it has no year bar (a salmon row with no DFO calendar): the row's word, capital first; null: none. */
export function dfoBlockStatus(row: GlanceRowVM): string | null {
  return row.value ? row.value.charAt(0).toUpperCase() + row.value.slice(1) : null;
}
