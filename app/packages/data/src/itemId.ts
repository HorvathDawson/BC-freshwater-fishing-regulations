/**
 * (T1083) THE KINDS OF ITEM ID, ONE LIST. An item id is `<kind>:<key>`, minted by the registry build
 * (`pipeline/atlas/registry/build.py` `item_id`): gnis first, then the watershed code, then the blue line, a lake by its
 * waterbody key. Every reader and writer of an id in a link asks this list — the web's address and Share link
 * (`routeSearch`), the web's start link (`parseStartLink`: `regs=`, `show=`), the phone's app link (`parseAppLink`) and a
 * water page's `cift:item` (`itemOfWaterPage`) — so the writer can never put in the address an id the reader refuses.
 *
 * The start link once had its own `^[a-z]+:-?\d+$`, which reads no `wsc:` id (its key is dashed digit groups): the app
 * wrote `?regs=wsc:100-019698-831000` into its own address, and a reload or a shared link of that water opened the bare
 * map (production 0.9.0). `tools/item-id-links.test.ts` round-trips every item id of the release under test through the
 * writer and every reader, so a kind the bundle holds and this list leaves out fails by name.
 */
import type { ItemId } from "./types";

/** Each kind's key, whole. */
export const ITEM_ID_KINDS = {
  /** a GNIS name id: a named stream */
  gnis: /^\d+$/,
  /** an FWA waterbody key: a lake or wetland (negative: a water the atlas adds) */
  wbk: /^-?\d+$/,
  /** an FWA watershed code, its trailing zero groups trimmed: a stream with no GNIS name of its own */
  wsc: /^\d+(?:-\d+)*$/,
  /** an FWA blue line key: a stream with neither (negative: a stream the atlas adds) */
  blk: /^-?\d+$/,
} as const satisfies Readonly<Record<string, RegExp>>;
export type ItemIdKind = keyof typeof ITEM_ID_KINDS;

/** The kind of item id `s` is, or null when it is none (an unknown kind, or a key not of its kind's shape). */
export function itemIdKind(s: string): ItemIdKind | null {
  const i = s.indexOf(":");
  if (i < 0) return null;
  const kind = s.slice(0, i);
  return Object.hasOwn(ITEM_ID_KINDS, kind) && ITEM_ID_KINDS[kind as ItemIdKind].test(s.slice(i + 1)) ? kind as ItemIdKind : null;
}

/** Whether `s` is an item id of a kind in `ITEM_ID_KINDS`. */
export const isItemId = (s: string): s is ItemId => itemIdKind(s) !== null;
