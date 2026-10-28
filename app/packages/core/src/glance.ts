/**
 * (T221, owner 2026-10-28 — a TRY) THE AT-A-GLANCE TABLE: the Keep tab's species list ("Other fish": picture, name, the
 * day's answer on the right) also carries the quota groups (Trout and char) and the DFO Salmon card as FOLDED rows —
 * each opening, in place, to its full card. This file words those two kinds of row; the component draws them beside the
 * Other fish rows it already has. (Owner 2026-10-28) a folded row says NO count — "5 a day" misread as the whole group's
 * answer — and a status word only where it is true for every row of the card ("closed", "check DFO"); else none.
 * (T227, owner 2026-10-28) A card that is one closed-all-year statement (Region 4's "no fishing for salmon … until
 * further notice") is a plain "closed" row with no Details: there is nothing more to see.
 */
import type { GroupVM, TreeNodeVM } from "./regs/model";
import { SALMON_WORDS, type SalmonCardVM } from "./salmon";

export const GLANCE_WORDS = {
  /** the table's head once it holds more than the other fish */
  head: "Species and limits",
  /** the fold's affordance, closed and open */
  show: "Details",
  hide: "Hide",
  /** a screen reader's name for the fold */
  a11y: (name: string, open: boolean) => `${name}: ${open ? "hide" : "show"} the details`,
} as const;

/** One folded row: the picture's code, the name, the status word true for the whole card (null: none) and its tone. */
export interface GlanceRowVM {
  readonly thumb: string | null;
  readonly name: string;
  readonly value: string | null;
  readonly tone: "keep" | "release" | "closed" | "nolimit";
  /** (T227) false: nothing more to see — a plain row, no "Details" (a water whose salmon are one closed-all-year line) */
  readonly fold?: boolean;
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
  return { thumb: thumbOf(g.tree ?? []), name: g.title, value: null, tone: "keep" };
}

/**
 * The Salmon card's folded row: its title, and its status word ("closed", "check DFO" …) only when every row of the
 * card — the water's and any undrawn DFO place's — has that status; else none.
 */
export function salmonGlance(card: SalmonCardVM): GlanceRowVM {
  if (card.closedOnly) return { thumb: "CO", name: card.title, value: SALMON_WORDS.status.closed.toLowerCase(), tone: "closed", fold: false };
  const all = [...card.rows, ...card.unmapped.flatMap((u) => u.block.rows)].map((r) => r.status);
  const s = all.length && all.every((x) => x === all[0]) ? all[0]! : null;
  if (s === null || s === "varies") return { thumb: "CO", name: card.title, value: null, tone: "keep" };
  const word = SALMON_WORDS.status[s];
  return { thumb: "CO", name: card.title, value: s === "check" ? word.replace(/^Check/, "check") : word.toLowerCase(),
           tone: s === "closed" ? "closed" : s === "release" ? "release" : "keep" };
}
