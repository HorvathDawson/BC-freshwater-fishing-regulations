/**
 * (T244) THE SITE'S FAVICON: `/favicon.svg` (the v1 site's fish-and-pin, which canifishthis.ca already serves) and a
 * `<link rel="icon">` in every hub page's head — staging had none and answered 404. The water pages are the app's own
 * index.html, whose favicon the export carries (`web.favicon` in app.json, the same mark). Kept out of `html.ts` so the
 * head's builder stays one place and this one stays a post-step on the file map.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { drawIcons, withIconLinks } from "../web-icons.mjs";

const SVG = fileURLToPath(new URL("../../apps/mobile/assets/icon/favicon.svg", import.meta.url));
const ICON_OPEN = "<!--icons-->";
const LINK = `<link rel="icon" type="image/svg+xml" href="/favicon.svg">`;

/**
 * Adds `favicon.svg` and the icon link to every HTML page in `files` that has none; (T1042) and to EVERY HTML page the
 * PNG icons' links (48 / 96 / 192 px and the 180 px touch icon: tools/web-icons.mjs, Google Search's favicon), with the
 * PNG files in `binary`. A page that already carries them (the export's home page, so every water page) is left alone.
 */
export function withFavicon(files: Map<string, string>, binary: Map<string, Uint8Array>): void {
  files.set("favicon.svg", readFileSync(SVG, "utf8"));
  for (const [name, bytes] of drawIcons()) binary.set(name, bytes);
  for (const [path, body] of files) {
    if (!path.endsWith(".html") || !body.includes("<head>")) continue;
    let t = body.includes('rel="icon"') ? body : body.replace("<head>", `<head>${LINK}`);
    if (!t.includes(ICON_OPEN)) t = withIconLinks(t);
    if (t !== body) files.set(path, t);
  }
}
