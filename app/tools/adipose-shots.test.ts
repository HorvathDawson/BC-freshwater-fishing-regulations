// @vitest-environment jsdom
/**
 * T38 — the owner's pictures of the "Hatchery or wild?" sheet (not a test: skipped unless `T38_SHOTS=1`). The hatch
 * sheet of the FROZEN vedder_1010 page (packages/ui-native/src/regs/fixtures), drawn by the real SheetHost with the
 * app's two drawings (T886: each theme's own, packages/ui-native/src/adipose/, tools/build-adipose.mjs), laid out and
 * photographed by headless Chrome (`wide-measure.mjs`) at 390 (phone, light and dark), 1024 (tablet) and 1440 (desktop),
 * into docs/app-integration/T38-shots/ (or `T38_OUT`).
 *
 *   T38_SHOTS=1 [T38_OUT=dir] pnpm vitest run tools/adipose-shots.test.ts
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
import type { RegsPage, SheetKey, SheetVM } from "@app/core";
import type { LayoutClass } from "@app/ui";
import { SheetHost } from "../packages/ui-native/src/regs/SheetHost";
import { DARK, LIGHT } from "../packages/ui-native/src/theme";
import vedder1010 from "../packages/ui-native/src/regs/fixtures/vedder_1010.json";
import { snapshot } from "./wide-snapshot";

const ON = process.env.T38_SHOTS === "1";
const OUT = process.env.T38_OUT ?? join(process.cwd(), "..", "docs", "app-integration", "T38-shots");
type Frozen = Omit<RegsPage, "sheet"> & { sheets: Record<string, SheetVM | null> };
const PAGE = (() => { const p = vedder1010 as unknown as Frozen; return { ...p, sheet: (k: SheetKey) => p.sheets[k] ?? null } as RegsPage; })();
const SIZES: [LayoutClass, number, number][] = [["phone", 390, 844], ["tablet", 1024, 768], ["desktop", 1440, 900]];

const dir = mkdtempSync(join(tmpdir(), "t38-shots-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

describe.skipIf(!ON)("T38 shots", () => {
  it("photographs the hatch sheet at every size (light), and on a phone in dark", () => {
    mkdirSync(OUT, { recursive: true });
    const jobs: { page: string; size: [number, number]; shot: string }[] = [];
    const shoot = ([layout, w, hgt]: [LayoutClass, number, number], dark = false) => {
      for (const [k, v] of [["clientWidth", w], ["clientHeight", hgt]] as const)
        Object.defineProperty(document.documentElement, k, { value: v, configurable: true });
      act(() => { window.dispatchEvent(new Event("resize")); });
      render(h(SheetHost, { page: PAGE, open: "hatch" as SheetKey, onOpen() {}, onClose() {}, palette: dark ? DARK : LIGHT,
                           layout }) as ReactNode);
      const file = join(dir, `hatch-${layout}${dark ? "-dark" : ""}.html`);
      // the drawings are imported beside the code (Vite serves them at /packages/…): the page is a file, so point at the files
      const pkgs = pathToFileURL(join(process.cwd(), "packages")).href;
      const html = snapshot().replaceAll('src="/packages/', `src="${pkgs}/`).replaceAll("url(&quot;/packages/", `url(&quot;${pkgs}/`);
      writeFileSync(file, html.replace("<body>", `<body style="background:${(dark ? DARK : LIGHT).card}">`));
      cleanup();
      jobs.push({ page: file, size: [w, hgt], shot: join(OUT, `hatch-${w}${dark ? "-dark" : ""}.png`) });
    };
    for (const s of SIZES) shoot(s);
    shoot(SIZES[0]!, true);
    const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                             { encoding: "utf8", timeout: 180_000 });
    expect((JSON.parse(out) as unknown[]).length).toBe(jobs.length);
  }, 240_000);
});
