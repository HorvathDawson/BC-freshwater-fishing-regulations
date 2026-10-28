/**
 * (T221, owner 2026-10-28 — a TRY) THE AT-A-GLANCE TABLE: the Keep tab's species list ("Other fish": picture, name, the
 * day's answer on the right) also carries the quota groups (Trout and char) and the DFO Salmon card as FOLDED rows —
 * "5 a day", "check DFO" — each opening, in place, to its full card. This file words those two kinds of row; the
 * component draws them beside the Other fish rows it already has. No new answer: each value is read off the card's own.
 */
import type { GroupVM, TreeNodeVM } from "./regs/model";
import { WORDS } from "./regs/words";
import { plain } from "./regs/line";
import { SALMON_WORDS, type SalmonCardVM } from "./salmon";

export const GLANCE_WORDS = {
  /** the table's head once it holds more than the other fish */
  head: "Fish",
  /** the fold's affordance, closed and open */
  show: "Details",
  hide: "Hide",
  /** a screen reader's name for the fold */
  a11y: (name: string, open: boolean) => `${name}: ${open ? "hide" : "show"} the details`,
} as const;

/** One folded row: the picture's code, the name, the day's answer and its tone (the Other fish rows' tones). */
export interface GlanceRowVM {
  readonly thumb: string | null;
  readonly name: string;
  readonly value: string;
  readonly tone: "keep" | "release" | "closed" | "nolimit";
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

/**
 * A quota group's folded row: its title, and the number the card heads with — the water's share where the head leads
 * with one (form a), else the root tier's — as "5 a day"; "no limit" for a group without one.
 */
export function groupGlance(g: GroupVM): GlanceRowVM {
  const thumb = thumbOf(g.tree ?? []);
  if (g.head.nolimit) return { thumb, name: g.title, value: WORDS.NO_LIMIT, tone: "nolimit" };
  const big = (g.head.form === "a" && g.head.lead ? g.head.lead : g.head.words).find((r) => r.t === "num" && r.size === "big");
  const n = big && big.t === "num" ? big.s : null;
  return { thumb, name: g.title, value: n !== null ? `${n}${WORDS.A_DAY}` : plain(g.head.words), tone: "keep" };
}

/** The Salmon card's folded row: its title and its status word for the day ("check DFO", "closed" …). */
export function salmonGlance(card: SalmonCardVM): GlanceRowVM {
  const s = card.headlineStatus;
  const word = SALMON_WORDS.status[s];
  return { thumb: "CO", name: card.title, value: s === "check" ? word.replace(/^Check/, "check") : word.toLowerCase(),
           tone: s === "closed" ? "closed" : s === "release" ? "release" : "keep" };
}
