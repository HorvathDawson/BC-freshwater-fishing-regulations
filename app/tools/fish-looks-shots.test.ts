// @vitest-environment jsdom
/**
 * T46 — the owner's pictures of the fish sheets (not a test: skipped unless `T46_SHOTS=1`). Each sp: sheet is core's
 * own (`fishSheet`, the fish manifest's pictures and captions), drawn by the real SheetHost with the app's picture
 * files, laid out and photographed by headless Chrome (`wide-measure.mjs`) at 390 (phone), 1024 (tablet) and 1440
 * (desktop), into handoff/PLAN/app-integration/T46-shots/. A harness, because no water's page offers every one of these
 * fish (sockeye, coho and chinook appear on no fixture page); the steelhead / rainbow / kokanee sheets were also shot in
 * the live app (see the README there).
 *
 *   T46_SHOTS=1 pnpm vitest run tools/fish-looks-shots.test.ts
 */
import "./render-setup.ts";
import { createElement as h, type ReactNode } from "react";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { FISH, type RegsInputs, type RegsPage, type SheetKey } from "@app/core";
import type { LayoutClass } from "@app/ui";
import { fishSheet } from "../packages/core/src/regs/sheets/fish";
import { SheetHost } from "../packages/ui-native/src/regs/SheetHost";
import { DARK, LIGHT } from "../packages/ui-native/src/theme";
import { snapshot } from "./wide-snapshot";

const ON = process.env.T46_SHOTS === "1";
const OUT = join(process.cwd(), "..", "handoff", "PLAN", "app-integration", "T46-shots");
const ASSETS = join(process.cwd(), "apps", "mobile", "assets", "fish");
const FISH_IMAGES = Object.fromEntries(Object.entries(FISH).map(([code, pics]) =>
  [code, pics.map((p) => ({ uri: pathToFileURL(join(ASSETS, p.file)).href }))]));
// fishSheet reads only the species book's names (none here: each falls back to its picture's caption, the manifest's)
const INPUTS = { statics: { exportSpecies: { fish: {} } } } as unknown as RegsInputs;
const PAGE = { sheet: (k: SheetKey) => fishSheet(INPUTS, k, k.slice(3)) } as unknown as RegsPage;

const SHEETS = ["ST", "RB", "SK", "KO", "CO", "CH"];
const SIZES: [LayoutClass, number, number][] = [["phone", 390, 844], ["tablet", 1024, 768], ["desktop", 1440, 900]];

const dir = mkdtempSync(join(tmpdir(), "t46-shots-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

describe.skipIf(!ON)("T46 shots", () => {
  it("photographs every sheet at every size (light), and steelhead in dark", () => {
    mkdirSync(OUT, { recursive: true });
    const jobs: { page: string; size: [number, number]; shot: string }[] = [];
    const shoot = (code: string, [layout, w, hgt]: [LayoutClass, number, number], dark = false) => {
      // the window react-native-web measures (jsdom's document has no size): the sheet's height cap is a share of it
      for (const [k, v] of [["clientWidth", w], ["clientHeight", hgt]] as const)
        Object.defineProperty(document.documentElement, k, { value: v, configurable: true });
      act(() => { window.dispatchEvent(new Event("resize")); });
      render(h(SheetHost, { page: PAGE, open: `sp:${code}` as SheetKey, onOpen() {}, onClose() {}, palette: dark ? DARK : LIGHT,
                           fish: FISH_IMAGES, layout }) as ReactNode);
      const file = join(dir, `${code}-${layout}${dark ? "-dark" : ""}.html`);
      writeFileSync(file, snapshot().replace("<body>", `<body style="background:${(dark ? DARK : LIGHT).card}">`));
      cleanup();
      jobs.push({ page: file, size: [w, hgt], shot: join(OUT, `sp-${code}-${w}${dark ? "-dark" : ""}.png`) });
    };
    for (const code of SHEETS) for (const s of SIZES) shoot(code, s);
    for (const s of SIZES) shoot("ST", s, true);
    const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                             { encoding: "utf8", timeout: 180_000 });
    expect((JSON.parse(out) as unknown[]).length).toBe(jobs.length);
  }, 240_000);
});
