// @vitest-environment jsdom
/**
 * T46 — A FISH'S LOOKS, laid out (the acceptance: "at desktop width, a two-look fish draws its looks in one row, each at
 * or below the max width. At phone width it draws them stacked. A mutation (dropping maxWidth) fails it.").
 *
 * The real SheetHost opens sp:ST (steelhead: ocean, spawning) and sp:RB (rainbow trout: one look) over the frozen
 * vedder_1010 page, with the app's own picture files, at each layout class; the DOM react-native-web drew (the Modal's
 * portal included) is laid out by headless Chrome (`wide-measure.mjs`) at 1440 (desktop), 1024 (tablet) and 390 (phone),
 * and every picture's box is measured.
 */
import "./render-setup.ts";
import { createElement as h, type ReactNode } from "react";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, describe, expect, it, vi } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { FISH, type RegsPage, type SheetKey, type SheetVM } from "@app/core";
import type { LayoutClass } from "@app/ui";

/** The mutant: pictures with no max width. Off unless a test turns it on. */
const MUTANT = vi.hoisted(() => ({ on: false }));
vi.mock("../packages/ui-native/src/theme", async (importOriginal) => {
  const real = await importOriginal<typeof import("../packages/ui-native/src/theme")>();
  return { ...real, get FISH_PICTURE() { return MUTANT.on ? { ...real.FISH_PICTURE, maxWidth: undefined } : real.FISH_PICTURE; } };
});

const { SheetHost } = await import("../packages/ui-native/src/regs/SheetHost");
const { LIGHT } = await import("../packages/ui-native/src/theme");
const { snapshot } = await import("./wide-snapshot");
const vedder = (await import("../packages/ui-native/src/regs/fixtures/vedder_1010.json")).default as unknown as
  Omit<RegsPage, "sheet"> & { sheets: Record<string, SheetVM | null> };
const PAGE = { ...vedder, sheet: (k: SheetKey) => vedder.sheets[k] ?? null } as RegsPage;

// the app's picture files, by manifest order (as apps/mobile registers them)
const ASSETS = join(process.cwd(), "apps", "mobile", "assets", "fish");
const FISH_IMAGES = Object.fromEntries(Object.entries(FISH).map(([code, pics]) =>
  [code, pics.map((p) => ({ uri: pathToFileURL(join(ASSETS, p.file)).href }))]));

const CASES: [LayoutClass, number, number][] = [["desktop", 1440, 900], ["tablet", 1024, 768], ["phone", 390, 844]];
const KEYS = ["sp:ST", "sp:RB"] as const;

const MEASURE = `(() => JSON.stringify({ boxes: [...document.querySelectorAll('[data-testid=fish-look-box]')].map((e) => {
  const r = e.getBoundingClientRect(); return [Math.round(r.left), Math.round(r.top), Math.round(r.width), Math.round(r.height)];
}) }))()`;
const dir = mkdtempSync(join(tmpdir(), "t46-looks-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
writeFileSync(join(dir, "measure.js"), MEASURE);

type Box = [number, number, number, number];
/** `{ "<key> <class> <w>": [[left, top, width, height], …] }` */
function measure(): Record<string, Box[]> {
  const jobs: { page: string; size: [number, number]; measure: string }[] = [];
  const keys: string[] = [];
  for (const key of KEYS)
    for (const [layout, w, hgt] of CASES) {
      render(h(SheetHost, { page: PAGE, open: key, onOpen() {}, onClose() {}, palette: LIGHT, fish: FISH_IMAGES, layout }) as ReactNode);
      const file = join(dir, `${key.replace(":", "_")}-${layout}.html`);
      writeFileSync(file, snapshot());
      cleanup();
      jobs.push({ page: file, size: [w, hgt], measure: join(dir, "measure.js") });
      keys.push(`${key} ${layout} ${w}`);
    }
  const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                           { encoding: "utf8", timeout: 120_000 });
  const res = JSON.parse(out) as { boxes: Box[] }[];
  return Object.fromEntries(keys.map((k, i) => [k, res[i]!.boxes]));
}

/** What breaks the acceptance, in words; [] is clean. */
function problems(m: Record<string, Box[]>): string[] {
  const out: string[] = [];
  for (const [k, boxes] of Object.entries(m)) {
    const phone = k.includes(" phone ");
    for (const [i, b] of boxes.entries()) if (b[2] > 360) out.push(`${k}: look ${i + 1} is ${b[2]} px wide`);
    if (boxes.length === 2) {
      const [a, b] = boxes as [Box, Box];
      if (!phone && a[1] !== b[1]) out.push(`${k}: the looks are not in one row (tops ${a[1]}, ${b[1]})`);
      if (phone && b[1] < a[1] + a[3]) out.push(`${k}: the looks are not stacked`);
    }
  }
  return out;
}

describe("T46 a fish's looks, laid out in Chrome", () => {
  it("a two-look fish in one row on a desktop and a tablet, stacked on a phone; every picture ≤ 360 px", () => {
    const m = measure();
    expect(problems(m)).toEqual([]);
    // each sheet's looks as drawn: left + width (px), and how the second sits to the first (the tops depend on the
    // dialog's height, which is not the claim)
    const drawn = Object.fromEntries(Object.entries(m).map(([k, bs]) => [k, bs.map((b) => `${b[0]}+${b[2]}`).join(" ")
      + (bs.length === 2 ? (bs[0]![1] === bs[1]![1] ? " one row" : bs[1]![1] >= bs[0]![1] + bs[0]![3] ? " stacked" : " overlapping") : "")]));
    expect(drawn).toEqual({
      "sp:ST desktop 1440": "417+297 726+297 one row",
      "sp:ST tablet 1024": "209+297 518+297 one row",
      "sp:ST phone 390": "16+358 16+358 stacked",
      "sp:RB desktop 1440": "417+360",
      "sp:RB tablet 1024": "209+360",
      "sp:RB phone 390": "16+358",
    });
  }, 180_000);

  it("MUTATION: pictures with no max width fail it", () => {
    MUTANT.on = true;
    try { expect(problems(measure())).toMatchInlineSnapshot(`
      [
        "sp:RB desktop 1440: look 1 is 606 px wide",
        "sp:RB tablet 1024: look 1 is 606 px wide",
      ]
    `); } finally { MUTANT.on = false; }
  }, 180_000);
});
