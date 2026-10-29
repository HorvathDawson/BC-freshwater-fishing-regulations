/**
 * T279 — A WATER PAGE IS STORED COMPRESSED: `<file>.br` (brotli, quality 11) and `<file>.gz` (gzip 9), never the raw HTML.
 * The site worker (legacy.ts `legacyWorker`) answers a request with the stored bytes the browser accepts. The page's
 * budget numbers (T196) come from the same bytes.
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { brotliCompressSync, constants, gzipSync } from "node:zlib";
import { blockingScripts, type PageSize } from "./budget";

export function writeCompressed(root: string, file: string, html: string): PageSize & { readonly brBytes: number; readonly rawBytes: number } {
  const raw = Buffer.from(html);
  const gz = gzipSync(raw, { level: 9 });
  const br = brotliCompressSync(raw, { params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_SIZE_HINT]: raw.length } });
  const to = join(root, file);
  mkdirSync(dirname(to), { recursive: true });
  writeFileSync(`${to}.br`, br);
  writeFileSync(`${to}.gz`, gz);
  return { path: `/${file.replace(/\/index\.html$/, "")}`, gzipBytes: gz.length, brBytes: br.length, rawBytes: raw.length,
           blocking: blockingScripts(html).length > 0, critical: /<style>([\s\S]*?)<\/style>/.exec(html)?.[1] ?? "" };
}
