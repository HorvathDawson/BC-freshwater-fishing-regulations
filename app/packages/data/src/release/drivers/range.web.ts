/**
 * (T831) THE WEB READS NO INSTALLED FILE: its atlas is served over HTTP and `pmtiles` reads it by range with `fetch`. See
 * `range.d.ts`.
 */
import type { ReadAt } from "./range";

export type { ReadAt } from "./range";

export const atlasRange: ReadAt | null = null;
