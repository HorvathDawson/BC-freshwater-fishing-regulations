/**
 * T269 — THE APP'S WEB EXPORT, read for the water pages: its index.html (the page each water page is), the URLs its
 * faces and pictures are served at (hashed by the export), so a prerendered pane asks for exactly the files the app will.
 *
 *   pnpm -C app/apps/mobile export:web[:staging]   →   app/dist/web[-staging]   →   build-seo.mjs --app <that folder>
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, extname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const MOBILE = resolve(dirname(fileURLToPath(import.meta.url)), "../../apps/mobile");

export interface Served { readonly uri: string; readonly width: number; readonly height: number }
export interface AppExport {
  readonly template: string;
  /** (T910) a face's served URL: its WOFF2 (`Archivo_400Regular` → `/assets/fonts/Archivo_400Regular.<sha12>.woff2`,
   *  put in by tools/web-shell.mjs; the app's bundle names the same path) */
  font(face: string): string | null;
  /** (T910) that WOFF2 in the export, on disk (what `font` serves) */
  fontFile(face: string): string | null;
  /** (T409) the TTF the phone draws, in the app's expo-google-fonts package: what the WOFF2 is made from */
  fontSource(face: string): string | null;
  /** a picture `apps/mobile` requires (its source path), as the export serves it */
  asset(source: string): Served | null;
}

/** a face's family → its expo-google-fonts package */
const FONT_PACKAGE: Readonly<Record<string, string>> = { Archivo: "archivo", BricolageGrotesque: "bricolage-grotesque", JetBrainsMono: "jetbrains-mono" };
/** `Archivo_400Regular` → …/node_modules/@expo-google-fonts/archivo/400Regular/Archivo_400Regular.ttf (null: no package) */
function ttfOf(face: string): string | null {
  const [family, weight] = face.split("_");
  const pkg = family ? FONT_PACKAGE[family] : undefined;
  if (!pkg || !weight) return null;
  const req = createRequire(join(MOBILE, "package.json"));
  return join(dirname(req.resolve(`@expo-google-fonts/${pkg}/package.json`)), weight, `${face}.ttf`);
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
  // (T910) `/assets/fonts/<Face>.<sha12>.woff2`: how tools/web-shell.mjs names a face (the water pages' path, T409)
  const fonts = new Map(files.flatMap((f) => { const m = /^\/assets\/fonts\/(\w+)\.[0-9a-f]{12}\.woff2$/.exec(f); return m ? [[m[1]!, f] as const] : []; }));
  return {
    template,
    font: (face) => fonts.get(face) ?? null,
    fontFile: (face) => { const u = fonts.get(face); return u ? join(root, u) : null; },
    fontSource: ttfOf,
    asset: (source) => {
      const rel = relative(MOBILE, source).split("\\").join("/");
      const uri = byPath.get(`/assets/${rel}`);
      if (!uri) return null;
      const size = extname(uri) === ".webp" ? webpSize(readFileSync(join(root, uri))) : null;
      return { uri, width: size?.width ?? 0, height: size?.height ?? 0 };
    },
  };
}
