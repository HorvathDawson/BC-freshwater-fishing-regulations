/**
 * (T1148, owner 2026-11-26 "Keep cache, reword the page") THE PRIVACY WORDS AND THE MAP'S DEVICE CACHE MOVE TOGETHER.
 *
 * MapLibre Native keeps http tiles (the Esri satellite pictures) in its own ambient cache on the phone, 50 MB by
 * default. The app never sets, sizes or clears that cache, so the privacy page and the Sources line say the pictures
 * "may be kept" on the device. Two halves, each failing without the other:
 *
 *   - the words: the privacy page and the app's Sources line carry core's sentences, whole, and neither says the
 *     imagery is "not stored" / "never stored";
 *   - the setting: no source of ours (packages, the mobile shell, its plugins and modules, the patches) names the
 *     engine's cache controls. A change that turns the cache off, sizes it or clears it fails here, and is fixed by
 *     changing `IMAGERY_KEPT_PRIVACY` / `IMAGERY_KEPT_SOURCES` (the owner clears the words) and this list together.
 */
import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { IMAGERY_ATTRIBUTION, IMAGERY_KEPT_PRIVACY, IMAGERY_KEPT_SOURCES, LICENCE_ROWS, MENU_WORDS } from "@app/core";
import { privacyBody } from "./seo/html";

const APP = fileURLToPath(new URL("..", import.meta.url));
const text = (html: string) => html.replace(/<[^>]+>/g, "");

/** the engine's cache controls, by every name they have: the binding's JS, MapLibre Android, MapLibre iOS */
const CACHE_CONTROLS = /ambientCache|AmbientCacheSize|setMaximumAmbientCache|invalidateAmbientCache|resetDatabase|MLNOfflineStorage|OfflineManager/i;
const SOURCE = /\.(ts|tsx|js|mjs|kt|swift|m|mm|java|patch|pro)$/;
const SKIP = new Set(["node_modules", "android", "ios", "dist", ".expo", "dev"]);

function sources(dir: string, out: string[] = []): string[] {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    if (e.isDirectory()) { if (!SKIP.has(e.name)) sources(join(dir, e.name), out); }
    else if (SOURCE.test(e.name) && !/\.test\.tsx?$/.test(e.name)) out.push(join(dir, e.name));
  }
  return out;
}

describe("T1148 satellite pictures and the map's device cache", () => {
  it("the privacy page says what may be kept, in core's whole sentence, and never 'not stored'", () => {
    const page = text(privacyBody({ name: "Can I Fish This", origin: "https://example.invalid", contact: null, verification: { google: null, bing: null } }, "2026-11-26"));
    expect(IMAGERY_KEPT_PRIVACY).toBe(
      "Satellite pictures you have looked at may be kept on your device so they load faster; they are not sent anywhere else.");
    expect(page).toContain(`so Esri sees those tile requests and your IP address. Imagery is shown only while online. ${IMAGERY_KEPT_PRIVACY}Sharing and directions`);
    expect(page).not.toMatch(/imagery[^.]*\b(not|never) stored/i);
  });

  it("the app's Sources line and licences table say the same, and never 'never stored'", () => {
    expect(MENU_WORDS.sources.from.imagery).toBe(
      `${IMAGERY_ATTRIBUTION}. Shown only while online. Pictures you have viewed may be kept on this device so they load faster.`);
    expect(MENU_WORDS.sources.from.imagery.endsWith(IMAGERY_KEPT_SOURCES)).toBe(true);
    expect(LICENCE_ROWS.find((s) => s.k === "imagery")!.shows).toBe("The Satellite ground, online only.");
  });

  it("nothing of ours sets, sizes or clears the engine's cache (change the words with it)", () => {
    const files = [...sources(join(APP, "packages")), ...sources(join(APP, "apps/mobile")), ...sources(join(APP, "patches"))];
    expect(files.length).toBeGreaterThan(200);
    expect(files.some((f) => f.endsWith("@maplibre__maplibre-react-native@11.5.0.patch"))).toBe(true);
    const named = files.filter((f) => CACHE_CONTROLS.test(readFileSync(f, "utf8"))).map((f) => f.slice(APP.length));
    expect(named, "a file names the map engine's tile cache controls: the privacy words (core imagery.ts IMAGERY_KEPT_*) must change with it").toEqual([]);
  });
});
