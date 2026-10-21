/**
 * What the golden parity does NOT compare, each with its reason and what removes it (tests.md §1.2). An entry with no
 * `key` drops the whole state from `cases(block)`; a keyed entry drops only that key (a sheet key, or a dotted VM path
 * under the block) and the parity test asks `excluded(block, id, key)` before comparing it.
 */
import type { Block } from "@app/core/regs/testing";
import { golden } from "./oracles";

/** The states whose golden `pins.error` is set (the prototype's pinUpdate throw, facts.md A#25) — computed at load. */
export const PINS_ERROR_IDS: readonly string[] = Object.entries(golden().states)
  .filter(([, s]) => !!(s.pins as { error?: unknown } | null)?.error).map(([id]) => id);

/** The states whose golden keep.strip.line carries a "Trout …" line — the strip part that words a row's STORED year strip
 *  (rows 30 `rows[].year`; compared since T13, no longer excluded). */
export const TROUT_LINE_IDS: readonly string[] = Object.entries(golden().states)
  .filter(([, s]) => ((s.keep as { strip?: { line?: { text: string }[] } } | null)?.strip?.line ?? []).some((l) => /^Trout /.test(l.text)))
  .map(([id]) => id);

export interface Exclusion { block: Block; ids: readonly string[] | "*"; key?: RegExp; reason: string; until?: string }

export const EXCLUDE: readonly Exclusion[] = [
  ...(PINS_ERROR_IDS.length ? [{
    block: "pins" as const, ids: PINS_ERROR_IDS,
    reason: "prototype bug: pinUpdate throws on an article.row.ledger whose row has no group (facts A#25); samples after the throw are missing — the app's pinChain skips such a group. Re-test with the fixed prototype.",
    until: "prototype fix" }] : []),
  { block: "sheets", ids: "*", key: /^parts$/,
    reason: "the prototype's parts sheet carries a prototype-only line ('This prototype has no map of this water…'); T36 compares its rows only" },
  { block: "sheets", ids: "*", key: /^rule:/,
    reason: "'Technical detail' fold body (key + fields JSON) is compared by structure, not string: JSON.stringify spacing",
    until: "never — T36 test asserts the fold exists, closed, and that its first line starts with the key" },
  { block: "header", ids: "*", key: /^place\.shown$/,
    reason: "which of full/short is shown is a measurement (fitLabel); the VM carries both candidates — T41 checks the choice on Expo web" },
];

/** The entry that excludes this state (and key), or null. A keyed entry never excludes a whole state. */
export function excluded(block: Block, id: string, key?: string): { reason: string } | null {
  const e = EXCLUDE.find((x) => x.block === block && (x.ids === "*" || x.ids.includes(id))
    && (x.key === undefined || (key !== undefined && x.key.test(key))));
  return e ? { reason: e.reason } : null;
}
