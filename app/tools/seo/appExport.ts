/**
 * T269 — THE APP'S WEB EXPORT, read for the water pages: its index.html (the page each water page is), the URLs its
 * faces and pictures are served at (hashed by the export), so a prerendered pane asks for exactly the files the app will.
 *
 *   pnpm -C app/apps/mobile export:web[:staging]   →   app/dist/web[-staging]   →   build-seo.mjs --app <that folder>
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { basename, dirname, extname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const MOBILE = resolve(dirname(fileURLToPath(import.meta.url)), "../../apps/mobile");

export interface Served { readonly uri: string; readonly width: number; readonly height: number }
export interface AppExport {
  readonly template: string;
  /** a face's served URL (`Archivo_400Regular` → `/assets/__node_modules/@expo-google-fonts/…/Archivo_400Regular.<hash>.ttf`) */
  font(face: string): string | null;
  /** a picture `apps/mobile` requires (its source path), as the export serves it */
  asset(source: string): Served | null;
}

function walk(dir: string, out: string[] = []): string[] {
  for (const n of readdirSync(dir)) {
    const p = join(dir, n);
    if (statSync(p).isDirectory()) walk(p, out); else out.push(p);
  }
  return out;
}

/** A WebP's pixel size from its header (VP8, VP8L, VP8X); null for anything else. */
export function webpSize(b: Buffer): { width: number; height: number } | null {
  if (b.toString("ascii", 0, 4) !== "RIFF" || b.toString("ascii", 8, 12) !== "WEBP") return null;
  const kind = b.toString("ascii", 12, 16);
  if (kind === "VP8 ") return { width: b.readUInt16LE(26) & 0x3fff, height: b.readUInt16LE(28) & 0x3fff };
  if (kind === "VP8L") { const v = b.readUInt32LE(21); return { width: (v & 0x3fff) + 1, height: ((v >> 14) & 0x3fff) + 1 }; }
  if (kind === "VP8X") return { width: 1 + b.readUIntLE(24, 3), height: 1 + b.readUIntLE(27, 3) };
  return null;
}

export function readAppExport(dir: string): AppExport {
  const root = resolve(dir);
  const template = readFileSync(join(root, "index.html"), "utf8");
  const files = walk(join(root, "assets")).map((f) => `/${relative(root, f).split("\\").join("/")}`);
  // `<name>.<32 hex>.<ext>` is how the export names a file
  const unhashed = (p: string) => p.replace(/\.[0-9a-f]{32}(\.[a-z0-9]+)$/, "$1");
  const byPath = new Map(files.map((f) => [unhashed(f), f]));
  const fonts = new Map(files.filter((f) => f.endsWith(".ttf")).map((f) => [basename(unhashed(f), ".ttf"), f]));
  return {
    template,
    font: (face) => fonts.get(face) ?? null,
    asset: (source) => {
      const rel = relative(MOBILE, source).split("\\").join("/");
      const uri = byPath.get(`/assets/${rel}`);
      if (!uri) return null;
      const size = extname(uri) === ".webp" ? webpSize(readFileSync(join(root, uri))) : null;
      return { uri, width: size?.width ?? 0, height: size?.height ?? 0 };
    },
  };
}
