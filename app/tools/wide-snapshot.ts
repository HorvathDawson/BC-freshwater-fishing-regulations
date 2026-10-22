/**
 * T45 / T46 — the DOM react-native-web drew under jsdom, as a page a real browser can lay out: its CSS rules (inserted
 * through the CSSOM, so in no textContent), every root under <body> (a Modal's portal is one of them), and the app's web
 * fonts as @font-face rules over the font files in node_modules — so a line breaks where it breaks in the app (Archivo
 * is wider than the browser's fallback face; a line's length is the point of the measure).
 */
import { existsSync, readdirSync } from "node:fs";
import { join, sep } from "node:path";
import { pathToFileURL } from "node:url";
import { FACE } from "../packages/ui-native/src/type";

// vitest runs in app/; import.meta.url is not a file: url under jsdom
const NM = join(process.cwd(), "node_modules", "@expo-google-fonts") + sep;
/** "Archivo_400Regular" → node_modules/@expo-google-fonts/archivo/400Regular/Archivo_400Regular.ttf */
function fontFile(face: string): string | null {
  const m = /^([A-Za-z]+)_(\w+)$/.exec(face);
  if (!m) return null;
  const family = m[1]!.toLowerCase();
  const pkg = readdirSync(NM).find((d) => d.replace(/-/g, "") === family);
  const file = pkg ? `${NM}${pkg}/${m[2]}/${face}.ttf` : null;
  return file && existsSync(file) ? file : null;
}
const FONT_CSS = Object.values(FACE).map((f) => {
  const file = fontFile(f);
  return file ? `@font-face{font-family:"${f}";src:url("${pathToFileURL(file).href}") format("truetype");}` : "";
}).join("\n");

export function snapshot(): string {
  const css = [...document.styleSheets].flatMap((s) => [...s.cssRules].map((r) => r.cssText)).join("\n");
  const roots = [...document.body.children].map((c) => c.outerHTML).join("");
  return `<!doctype html><html><head><meta charset="utf-8"><style>${FONT_CSS}\nhtml,body{margin:0;height:100%}`
    + `body>div:first-child{height:100%;display:flex;flex-direction:column}\n${css}</style></head>`
    + `<body>${roots}</body></html>`;
}
