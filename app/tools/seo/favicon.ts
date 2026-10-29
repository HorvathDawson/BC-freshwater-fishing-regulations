/**
 * (T244) THE SITE'S FAVICON: `/favicon.svg` (the v1 site's fish-and-pin, which canifishthis.ca already serves) and a
 * `<link rel="icon">` in every hub page's head — staging had none and answered 404. The water pages are the app's own
 * index.html, whose favicon the export carries (`web.favicon` in app.json, the same mark). Kept out of `html.ts` so the
 * head's builder stays one place and this one stays a post-step on the file map.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const SVG = fileURLToPath(new URL("../../apps/mobile/assets/icon/favicon.svg", import.meta.url));
const LINK = `<link rel="icon" type="image/svg+xml" href="/favicon.svg">`;

/** Adds `favicon.svg` and the icon link to every HTML page in `files` that has none. */
export function withFavicon(files: Map<string, string>): void {
  files.set("favicon.svg", readFileSync(SVG, "utf8"));
  for (const [path, body] of files) {
    if (!path.endsWith(".html") || body.includes('rel="icon"') || !body.includes("<head>")) continue;
    files.set(path, body.replace("<head>", `<head>${LINK}`));
  }
}
