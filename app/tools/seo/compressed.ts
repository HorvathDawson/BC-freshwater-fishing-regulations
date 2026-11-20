/**
 * T279 — A WATER PAGE IS STORED COMPRESSED: `<file>.br` (brotli, quality 11) and `<file>.gz` (gzip 9), never the raw HTML.
 * The site worker (legacy.ts `legacyWorker`) answers a request with the stored bytes the browser accepts. The page's
 * budget numbers (T196) come from the same bytes.
 * (T917) The bytes are made and written by a job thread (jobs.ts → bytes.mjs `compressPage`), not the render thread; the
 * render thread reads what it needs of the HTML here, and the size is the two put together (`storedSize`).
 */
import { blockingScripts, type PageSize } from "./budget";
import type { Zipped } from "./jobs";

/** What the budget reads of a water page's HTML itself (the render thread's part; the byte counts come from the job thread). */
export interface PageFacts { readonly path: string; readonly blocking: boolean; readonly critical: string }
export const pageFacts = (file: string, html: string): PageFacts => ({
  path: `/${file.replace(/\/index\.html$/, "")}`, blocking: blockingScripts(html).length > 0,
  critical: /<style>([\s\S]*?)<\/style>/.exec(html)?.[1] ?? "",
});

export type StoredSize = PageSize & { readonly brBytes: number; readonly rawBytes: number };
export const storedSize = (f: PageFacts, z: Zipped): StoredSize => ({
  path: f.path, gzipBytes: z.gzipBytes, brBytes: z.brBytes, rawBytes: z.rawBytes, blocking: f.blocking, critical: f.critical,
});
