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

/** The states whose golden header has no part label because the PROTOTYPE had no water map of the water (place null,
 *  action not "map"): the prototype's no-picker label is its water map's part name. The app (D1) frames every named part
 *  from the tiles, so T16's rule applies — the slot is empty only when the label is the water's own name. */
export const NO_MAP_LABEL_IDS: readonly string[] = Object.entries(golden().states)
  .filter(([, s]) => { const h = s.header as { place: unknown; action: { kind: string } | null } | null;
    return !!h && h.place === null && h.action?.kind !== "map"; })
  .map(([id]) => id);
/** The three Kitimat states the owner ruling 2026-10-21 (a) moves away from page v36 (card.parity.test.ts KITIMAT_RULED). */
export const KITIMAT_RULED_IDS: readonly string[] = ["x14p0_1010", "x14p1_1010", "x14p2_1010"];

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
  { block: "header", ids: NO_MAP_LABEL_IDS, key: /^place$/,
    reason: "the prototype shows no part label where it had no water map of the water; the app frames every named part (D1) and labels it unless the label is the water's name (T16) — the parity asserts that instead",
    until: "never — prototype map coverage, not a regulation" },
  { block: "header", ids: "*", key: /^place\.shown$/,
    reason: "which of full/short is shown is a measurement (fitLabel); the VM carries both candidates — T41 checks the choice on Expo web" },
  // (T18) the Kitimat owner ruling 2026-10-21 (a): the store's group tree differs from page v36's by design
  { block: "keep", ids: KITIMAT_RULED_IDS, key: /^groups\.\d+\.tree$/,
    reason: "OWNER RULING 2026-10-21 (a) (pipeline/docs/GOTCHAS.md): on the Kitimat, r5's hatchery-steelhead quota does not lift Region 6's Nov 1–June 30 stream release, so the stored group tree (the river's own steelhead tier, the size box) is not page v36's. tree.parity holds these states to the STORE's TreeTier.says / TreeKind.says instead; never 'fix' the data to match the page",
    until: "never — a re-synced prototype on the ruled data removes it" },
];

/** The entry that excludes this state (and key), or null. A keyed entry never excludes a whole state. */
export function excluded(block: Block, id: string, key?: string): { reason: string } | null {
  const e = EXCLUDE.find((x) => x.block === block && (x.ids === "*" || x.ids.includes(id))
    && (x.key === undefined || (key !== undefined && x.key.test(key))));
  return e ? { reason: e.reason } : null;
}
