/**
 * `@app/data/regs/place` — (T369, owner 2026-11-03) A PLACE ON A STREAM: the reads of the release's `lines` file
 * (`line.ts`), what a link's place is made from and resolved by — and (T386, owner 2026-11-04) how an open unnamed
 * reach is carried across a data update on a phone (`@app/ui` `carryRoute`): `lines` is phone-held too now.
 *
 * Opened lazily: the file is fetched the first time a place is made or resolved, never on a page's way in.
 */
import type { Db, Row } from "../sql/db";
import type { ReleaseLoader } from "../release/loader";
import { RegsStoreError } from "./errors";
import { decodeLineBlock, slotAt, type LineBlock, type SectionLine } from "./line";
import { BLOCK_BITS } from "./reads";

export * from "./line";

const SLOT = (1 << BLOCK_BITS) - 1;
const damaged = (what: string) => new RegsStoreError("damaged", `regs store: ${what}`);

export interface LineReads {
  /**
   * Whether this release carries the `lines` file: the stream key and measure span of every stream section. A release
   * cut before it: false (no place). Opens (fetches) the file.
   */
  hasLines(): Promise<boolean>;
  /** A stream section's line; null: a lake's handle, or no `lines`. */
  sectionLine(handle: number): Promise<SectionLine | null>;
  /** The section whose span holds measure `m` of stream `blk`; null: none in this release (or no `lines`). */
  sectionAt(blk: number, m: number): Promise<number | null>;
}

/** The line reads over `open` (the `lines` file; null: the release has none). Each read is made once; a failure is not
 *  kept (the next ask tries again). */
export function makeLineReads(open: () => Promise<Db | null>): LineReads {
  const cache = new Map<string, Promise<unknown>>();
  const once = <T>(key: string, make: () => Promise<T>): Promise<T> => {
    let p = cache.get(key) as Promise<T> | undefined;
    if (!p) { p = make(); cache.set(key, p); p.catch(() => cache.delete(key)); }
    return p;
  };
  const db = () => once("db", open);
  const block = (ix: number): Promise<LineBlock | null> => once(`b${ix}`, async () => {
    const r: Row | undefined = await (await db())!.get("SELECT block, n, b FROM section_line WHERE block = ?", ix);
    if (!r) return null;
    if (!(r.b instanceof Uint8Array)) throw damaged(`section_line block ${String(r.block)} is not bytes`);
    try { return decodeLineBlock(r.b, Number(r.n)); }
    catch (e) { throw damaged(e instanceof Error ? e.message : String(e)); }
  });
  const hasLines = async () => (await db()) !== null;
  return {
    hasLines,
    async sectionLine(h) {
      if (!(await hasLines())) return null;
      const b = await block(h >> BLOCK_BITS), i = h & SLOT;
      return b && i < b.n ? { blk: b.blk[i]!, from: b.from[i]!, to: b.to[i]! } : null;
    },
    sectionAt: (blk, m) => once(`at${blk}:${m}`, async () => {
      if (!(await hasLines())) return null;
      // the block: the last head at or below the place (the handles are sorted by stream key, then start)
      const head = await (await db())!.get(
        "SELECT block FROM section_line WHERE blk0 < ? OR (blk0 = ? AND m0 <= ?) ORDER BY blk0 DESC, m0 DESC LIMIT 1",
        blk, blk, m);
      if (!head) return null;
      const ix = Number(head.block), b = await block(ix);
      const at = b ? slotAt(b, blk, m) : -1;
      return at < 0 ? null : (ix << BLOCK_BITS) | at;
    }),
  };
}

/** The line reads of `loader`'s release: its `lines` file, held to the manifest's format and handles by the loader. */
export function openLineReads(loader: Pick<ReleaseLoader, "manifest" | "db">): LineReads {
  return makeLineReads(async () => (loader.manifest.files.lines ? loader.db("lines") : null));
}
