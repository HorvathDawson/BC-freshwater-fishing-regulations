/**
 * T31 — ONE ✓ Allowed / ✕ Not allowed list (Bait, Ways to fish): the allowed items, then the ones that are not, each
 * with its own mark and its note small under its name; a column with no items is not drawn. (T35, owner wave A) An item
 * may carry glossary ids for an ⓘ after its name (the set_line ⓘ page v36 drew after an allowed "Set lines").
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): prototype yesNo (+ yesNo.col).
 */
import type { YesNoVM } from "./model";
import { YESNO } from "./words";

/** (T922) one condition as a sentence: a capital first, a full stop last (unless it already ends in . ! ? or a colon-free closing mark). */
export const sentence = (s: string): string => {
  const t = s.trim();
  return t ? t.charAt(0).toUpperCase() + t.slice(1) + (/[.!?]$/.test(t) ? "" : ".") : "";
};

/** [name, note, terms?] → the list; a note is one condition or several (each its own item, T922); an empty note is none; no terms is []. */
type Note = string | readonly string[];
type Item = readonly [string, Note] | readonly [string, Note, readonly string[]];
export function yesNo(ok: readonly Item[], no: readonly Item[]): YesNoVM {
  const col = (k: "ok" | "no", xs: readonly Item[]) => ({
    col: k, head: k === "ok" ? YESNO.OK : YESNO.NO,
    // (T900) three or more bare ✕ names read as one line
    inline: k === "no" && xs.length >= 3 && xs.every(([, note, terms]) => !note.length && !(terms ?? []).length),
    items: xs.map(([name, note, terms]) => ({ mark: k === "ok" ? YESNO.OK_MARK : YESNO.NO_MARK, name,
      notes: (typeof note === "string" ? [note] : note).filter(Boolean).map(sentence), terms: terms ?? [] })),
  });
  return [...(ok.length ? [col("ok", ok)] : []), ...(no.length ? [col("no", no)] : [])];
}
