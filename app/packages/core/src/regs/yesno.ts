/**
 * T31 — ONE ✓ Allowed / ✕ Not allowed list (Bait, Ways to fish): the allowed items, then the ones that are not, each
 * with its own mark and its note small under its name; a column with no items is not drawn.
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): prototype yesNo (+ yesNo.col).
 */
import type { YesNoVM } from "./model";
import { YESNO } from "./words";

/** [name, note] pairs → the list; an empty note is no note. */
export function yesNo(ok: readonly (readonly [string, string])[], no: readonly (readonly [string, string])[]): YesNoVM {
  const col = (k: "ok" | "no", xs: readonly (readonly [string, string])[]) => ({
    col: k, head: k === "ok" ? YESNO.OK : YESNO.NO,
    items: xs.map(([name, note]) => ({ mark: k === "ok" ? YESNO.OK_MARK : YESNO.NO_MARK, name, note: note || null })),
  });
  return [...(ok.length ? [col("ok", ok)] : []), ...(no.length ? [col("no", no)] : [])];
}
