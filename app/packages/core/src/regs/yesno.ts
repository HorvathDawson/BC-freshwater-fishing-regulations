/**
 * T31 — ONE ✓ Allowed / ✕ Not allowed list (Bait, Ways to fish): the allowed items, then the ones that are not, each
 * with its own mark and its note small under its name; a column with no items is not drawn. (T35, owner wave A) An item
 * may carry glossary ids for an ⓘ after its name (the set_line ⓘ page v36 drew after an allowed "Set lines").
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): prototype yesNo (+ yesNo.col).
 */
import type { YesNoVM } from "./model";
import { YESNO } from "./words";

/** [name, note, terms?] → the list; an empty note is no note, no terms is []. */
type Item = readonly [string, string] | readonly [string, string, readonly string[]];
export function yesNo(ok: readonly Item[], no: readonly Item[]): YesNoVM {
  const col = (k: "ok" | "no", xs: readonly Item[]) => ({
    col: k, head: k === "ok" ? YESNO.OK : YESNO.NO,
    items: xs.map(([name, note, terms]) => ({ mark: k === "ok" ? YESNO.OK_MARK : YESNO.NO_MARK, name, note: note || null, terms: terms ?? [] })),
  });
  return [...(ok.length ? [col("ok", ok)] : []), ...(no.length ? [col("no", no)] : [])];
}
