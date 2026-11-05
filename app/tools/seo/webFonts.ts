/**
 * T409 — THE WATER PAGES' FACES AS WOFF2 (`tools/emit-web-fonts.mjs` makes them from the app's TTF, losslessly). Served at
 * `/assets/fonts/<Face>.<sha12>.woff2` (no `@` in the path: the host answered the export's `@expo-google-fonts` paths
 * with a 307 first). A face with no WOFF2, a WOFF2 made from another version of the app's TTF, or a WOFF2 that is not
 * the one recorded is a build problem — never a quiet fall back to the TTF.
 */
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { AppExport } from "./appExport";

export const FONT_DIR = fileURLToPath(new URL("./fonts/", import.meta.url));
const sha = (b: Uint8Array): string => createHash("sha256").update(b).digest("hex");

interface Made { readonly ttf_sha256: string; readonly woff2_sha256: string; readonly bytes: number }
export interface WebFonts {
  /** a face's served WOFF2 path, null when it is not good (see `problems`) */
  url(face: string): string | null;
  /** the files to write: `assets/fonts/<Face>.<sha12>.woff2` */
  readonly files: ReadonlyMap<string, Uint8Array>;
  readonly problems: readonly string[];
}

export function webFonts(app: AppExport, faces: readonly string[], dir = FONT_DIR): WebFonts {
  const record = JSON.parse(readFileSync(`${dir}fonts.json`, "utf8")) as Record<string, Made>;
  const urls = new Map<string, string>(), files = new Map<string, Uint8Array>(), problems: string[] = [];
  const rerun = "rerun node tools/emit-web-fonts.mjs";
  for (const face of faces) {
    const made = record[face], ttf = app.fontFile(face), file = `${dir}${face}.woff2`;
    if (!ttf) continue;                                   // the export has no TTF: build.ts reports the face missing
    if (!made || !existsSync(file)) { problems.push(`web font ${face}: no WOFF2 (${rerun})`); continue; }
    if (sha(readFileSync(ttf)) !== made.ttf_sha256) { problems.push(`web font ${face}: made from another version of the app's TTF (${rerun})`); continue; }
    const w = readFileSync(file);
    if (sha(w) !== made.woff2_sha256) { problems.push(`web font ${face}: ${face}.woff2 is not the file fonts.json records (${rerun})`); continue; }
    const path = `assets/fonts/${face}.${made.woff2_sha256.slice(0, 12)}.woff2`;
    urls.set(face, `/${path}`); files.set(path, w);
  }
  return { url: (f) => urls.get(f) ?? null, files, problems };
}
