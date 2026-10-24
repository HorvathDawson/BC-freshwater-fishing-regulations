/**
 * Species words — replaces page_v36's globals FISH, GROUPS, PROT, SALM, GAME (`D.species` = static `export.species`),
 * which every function here takes as its `SpeciesBook` parameter instead.
 *
 * Ports (page_v36.js @ 7fb6809f, inventory names): cap, clean, expand, GAME, spName, lc, groupFor, join, lcNames, Names,
 * writtenName.
 */
import { WORDS } from "./words";

export interface SpeciesBook {
  readonly fish: Readonly<Record<string, { readonly name: string }>>;
  readonly groups: Readonly<Record<string, { readonly name: string; readonly members: readonly string[]; readonly open?: boolean }>>;
  readonly protected?: Readonly<Record<string, { readonly name: string }>>;
  readonly salmon?: Readonly<Record<string, { readonly name: string }>>;
}

/** The group code page v36's groupFor never names a list by (a code, not a word). */
const SALMON_GROUP = "SA";
/** The group whose members are the game fish (page v36 GAME). */
const ALL_GAME_FISH = "ALL_GAME_FISH";

/** The first letter in capitals; nothing (null, undefined, "") is "". THE one (T61 2.16: four `cap1` copies). */
export const cap = (s: string | null | undefined): string => (s ? s.charAt(0).toUpperCase() + s.slice(1) : "");
export const clean = (s: string | null | undefined): string => String(s || "").replace(/\*+/g, "").replace(/\s+/g, " ").trim();

/** The game fish (page v36 GAME = groups.ALL_GAME_FISH.members); none when the book has no such group. */
export function gameFish(sp: SpeciesBook): readonly string[] {
  return sp.groups[ALL_GAME_FISH]?.members ?? [];
}

/** Codes with every group replaced by its members, in order, each once. */
export function expand(sp: SpeciesBook, codes: readonly string[] | null | undefined): string[] {
  const out = new Set<string>();
  for (const c of codes ?? []) {
    const g = sp.groups[c];
    if (g) for (const m of g.members ?? []) out.add(m);
    else out.add(c);
  }
  return [...out];
}

/** A code's name: a fish, a group, a protected species or a salmon; else the code itself. */
export function spName(sp: SpeciesBook, code: string): string {
  return sp.fish[code]?.name || sp.groups[code]?.name || sp.protected?.[code]?.name || sp.salmon?.[code]?.name || code;
}

const KEEP_CAPITAL = new RegExp(`^(${WORDS.LC_KEEP.join("|")})`);
/** A name as it reads mid-sentence: lower-case first letter, unless it starts with a proper noun. */
export function lc(s: string): string {
  return KEEP_CAPITAL.test(s) ? s : s.charAt(0).toLowerCase() + s.slice(1);
}

/** The name of the closed group (never SA) whose members are exactly these codes; null for fewer than two. */
export function groupFor(sp: SpeciesBook, codes: readonly string[]): string | null {
  const s = new Set(codes);
  if (s.size < 2) return null;
  for (const [g, v] of Object.entries(sp.groups)) {
    if (g === SALMON_GROUP || v.open) continue;
    if (v.members.length === s.size && v.members.every((m) => s.has(m))) return v.name;
  }
  return null;
}

export function join(n: readonly string[], conj: string = WORDS.AND): string {
  return n.length < 2 ? n.join("") : n.slice(0, -1).join(", ") + conj + n[n.length - 1];
}

/** Codes as words mid-sentence: a whole group by its name, else each name, cut to `max` with "+ N more". */
export function lcNames(sp: SpeciesBook, codes: readonly string[], conj?: string, max: number = Infinity): string {
  const g = groupFor(sp, codes);
  if (g) return lc(g);
  const n = codes.map((c) => lc(spName(sp, c)));
  if (n.length > max) return n.slice(0, max - 1).join(", ") + WORDS.MORE(n.length - max + 1);
  return join(n, conj);
}

export function Names(sp: SpeciesBook, codes: readonly string[], conj?: string, max?: number): string {
  return cap(lcNames(sp, codes, conj, max));
}

/** The species a rule WRITES (its own codes, not expanded): "trout" for trout-and-char except char, "fish" for none. */
export function writtenName(sp: SpeciesBook, f: { readonly species?: readonly string[] | null; readonly species_except?: readonly string[] | null },
  conj?: string): string {
  const w = f.species ?? [];
  if (w.length === 1 && w[0] === "TROUT_CHAR" && (f.species_except ?? []).includes("CHAR")) return WORDS.TROUT;
  return w.length ? join(w.map((c) => lc(spName(sp, c))), conj) : WORDS.FISH;
}
