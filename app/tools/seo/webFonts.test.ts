/**
 * T409: the water pages' WOFF2 faces are the ones made from the app's own TTF, or the build says why not.
 * (T910) The app export serves them (tools/web-shell.mjs), and a page names the export's own file.
 */
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { FACES_REQUIRED } from "./pane";
import type { AppExport } from "./appExport";
import { FONT_DIR, webFonts } from "./webFonts";
// @ts-expect-error a plain .mjs tool (no types)
import { webFontsModule, WEB_FONTS_MODULE } from "../emit-web-fonts.mjs";

const sha = (b: Uint8Array | string) => createHash("sha256").update(b).digest("hex");

/** `served`: the export's WOFF2 bytes (default: the committed one), `at`: its served path (default: the recorded one) */
function setup(ttf: string, woff2: string, record?: object, served = woff2, at?: string) {
  const dir = `${mkdtempSync(join(tmpdir(), "webfonts-"))}/`;
  writeFileSync(`${dir}A_400Regular.ttf`, ttf);
  writeFileSync(`${dir}A_400Regular.woff2`, woff2);
  writeFileSync(`${dir}served.woff2`, served);
  writeFileSync(`${dir}fonts.json`, JSON.stringify(record ?? { A_400Regular: { ttf_sha256: sha("ttf"), woff2_sha256: sha("woff2"), bytes: 5 } }));
  const path = at ?? `/assets/fonts/A_400Regular.${sha("woff2").slice(0, 12)}.woff2`;
  const app = {
    font: (f: string) => (f === "A_400Regular" ? path : null),
    fontFile: (f: string) => (f === "A_400Regular" ? `${dir}served.woff2` : null),
    fontSource: (f: string) => (f === "A_400Regular" ? `${dir}A_400Regular.ttf` : null),
  } as unknown as AppExport;
  return webFonts(app, ["A_400Regular", "B_700Bold"], dir);
}

describe("webFonts", () => {
  it("a face made from the app's TTF: the export's file at /assets/fonts/<face>.<sha12>.woff2", () => {
    const w = setup("ttf", "woff2");
    expect(w.problems).toEqual([]);
    expect(w.url("A_400Regular")).toBe(`/assets/fonts/A_400Regular.${sha("woff2").slice(0, 12)}.woff2`);
    expect(w.url("B_700Bold")).toBeNull();                       // no file in the export: build.ts names it missing
  });
  it("(T910) refuses an export whose file is not the recorded WOFF2, or at another path", () => {
    expect(setup("ttf", "woff2", undefined, "woff3").problems).toEqual([expect.stringContaining("the app export serves")]);
    expect(setup("ttf", "woff2", undefined, "woff2", "/assets/fonts/A_400Regular.000000000000.woff2").problems)
      .toEqual([expect.stringContaining("the app export serves")]);
  });
  it("refuses a WOFF2 made from another TTF, a WOFF2 not the one recorded, and a face with none", () => {
    expect(setup("ttf2", "woff2").problems).toEqual([expect.stringContaining("made from another version of the app's TTF")]);
    expect(setup("ttf", "woff3").problems).toEqual([expect.stringContaining("is not the file fonts.json records")]);
    expect(setup("ttf", "woff2", {}).problems).toEqual([expect.stringContaining("no WOFF2")]);
    expect(setup("ttf", "woff2").url("A_400Regular")).not.toBeNull();
  });
  it("(T910) the web app's module names each recorded face at its served path (rerun emit-web-fonts --module)", () => {
    const record = JSON.parse(readFileSync(`${FONT_DIR}fonts.json`, "utf8"));
    expect(readFileSync(WEB_FONTS_MODULE, "utf8")).toBe(webFontsModule(record));
  });
  it("the committed record covers every face the app loads", () => {
    const record = JSON.parse(readFileSync(`${FONT_DIR}fonts.json`, "utf8")) as Record<string, { woff2_sha256: string }>;
    expect(Object.keys(record).sort()).toEqual([...FACES_REQUIRED].sort());
    for (const f of FACES_REQUIRED) expect(sha(readFileSync(`${FONT_DIR}${f}.woff2`))).toBe(record[f]!.woff2_sha256);
  });
});
