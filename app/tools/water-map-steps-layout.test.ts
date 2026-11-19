// @vitest-environment jsdom
/**
 * (T875, owner 2026-11-18: "The ui shifts here when moving upstream in desktop too") THE ▼ / ▲ STEPS STAY PUT. The water's
 * Map tab is mounted through the real Shell on the fixture at a phone (390) and a desktop (1280) window: on a water of one
 * part (the Vedder), on the Chilliwack's part with the longest name (two lines, cut there) and on its part with the
 * shortest; and, on the phone, on the Vedder reached by a step, its bar floored (`useBarFloor`) at the Chilliwack's height
 * and at a taller one (how the owner met it: ▼ into the Vedder, then ▲ back into the Chilliwack). The DOM react-native-web
 * drew is laid out by headless Chrome (`wide-measure.mjs`, the app's web fonts) and the step buttons' top is measured: it
 * is the same pixel (within half of one) in every case at each width. On the phone the bar's top (the map's foot) does not
 * move either: a part's words never resize the map under the reader (T731).
 *
 * The Vedder is cut to one part (the mock below says why). Only the map canvas is a stub (no WebGL here): a plain box.
 * T875_SHOTS=<dir> also saves each laid-out page as a PNG there, t875-<T875_TAG>-<width>-<case>.png (the pictures), and
 * the measurements beside them.
 */
import "./render-setup.ts";
import { createElement as h, type ReactNode } from "react";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";

/** The bar's floor, as `useBarFloor` would hand it after a step from a taller bar (undefined: a fresh open). */
const FLOOR = vi.hoisted(() => ({ v: undefined as number | undefined }));
vi.mock("../packages/ui-native/src/water/useBarFloor", () => ({
  useBarFloor: () => ({ minHeight: FLOOR.v, onLayout: () => {} }),
}));
vi.mock("../packages/ui-native/src/MapScreen", async () => {
  const { View } = await import("react-native");
  return { MapScreen: () => h(View, { testID: "map-canvas", style: { flex: 1 } }) };
});
vi.mock("../packages/ui-native/src/FishSpinner", () => ({ FishSpinner: () => null }));
/**
 * THE ONE-PART WATER, built from the fixture: the fixture's Vedder has two parts (the live release's has one, as on the
 * owner's phone), so its group is cut to its top part — the part the Chilliwack goes on as, so ▲ still crosses into the
 * Chilliwack, ▼ is "Lowest part" and the mouth's "Flows into Sumas River" is said, exactly the owner's screen. Its marks go
 * with the parts they divided.
 */
vi.mock("@app/data/regs", async (importOriginal) => {
  const real = await importOriginal<typeof import("@app/data/regs")>();
  return { ...real, loadRawWaterGroup: async (...a: Parameters<typeof real.loadRawWaterGroup>) => {
    const g = await real.loadRawWaterGroup(...a);
    return a[1] === "gnis:3062" ? { ...g, parts: g.parts.slice(-1), marks: [] } : g;
  } };
});

const { GestureHandlerRootView } = await import("react-native-gesture-handler");
const { makeFixtureSource } = await import("@app/data/fixture");
const { openRegsFixtureWithLines } = await import("@app/data/release/node");
const { loadRawWaterGroup, openRegsStore } = await import("@app/data/regs");
const { partName } = await import("@app/core");
const { HOME, LayoutWidthContext, openWater, reduce } = await import("@app/ui");
const { Shell } = await import("../packages/ui-native/src/Shell");
const { DARK } = await import("../packages/ui-native/src/theme");
const { snapshot } = await import("./wide-snapshot");

const VEDDER = "gnis:3062", CHILLIWACK = "gnis:8634";
const SIZES = { phone: [390, 844], desktop: [1280, 800] } as const;
/** A floor taller than any closed bar draws (a step from a bar whose parts list was open is ~230 px taller still). */
const TALL_FLOOR = 520;

const MEASURE = `(() => {
  const R = (e) => e ? e.getBoundingClientRect() : null;
  const step = R(document.querySelector('[data-testid=water-map-step-up]')?.closest('[role=button]'));
  const bar = R(document.querySelector('[data-testid=water-map-bar]'));
  const name = document.querySelector('[data-testid=water-map-name]');
  const tops = new Set();
  // the lines DRAWN: a longer name is cut at two (numberOfLines), its later lines laid out below the box, unseen
  if (name) { const r = document.createRange(), foot = name.getBoundingClientRect().bottom; r.selectNodeContents(name);
    for (const q of r.getClientRects()) if (q.width > 1 && q.top < foot - 1) tops.add(Math.round(q.top)); }
  return JSON.stringify({ step: step ? step.top : null, bar: bar ? bar.top : null, barHeight: bar ? bar.height : null, lines: tops.size });
})()`;

const dir = mkdtempSync(join(tmpdir(), "t875-steps-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
writeFileSync(join(dir, "measure.js"), MEASURE);

type M = { fonts: string; step: number | null; bar: number | null; barHeight: number; lines: number };
type Case = { key: string; item: string; part: number; floor?: number };

/** The Chilliwack's parts with the longest and the shortest name: read from its own group, never typed. */
async function cases(): Promise<Case[]> {
  const st = await openRegsStore(openRegsFixtureWithLines());
  const ved = await loadRawWaterGroup(st, VEDDER), chw = await loadRawWaterGroup(st, CHILLIWACK);
  if (ved.parts.length !== 1 || !ved.prev) throw new Error(`the Vedder is the one-part case below the Chilliwack: ${ved.parts.length} parts`);
  const names = chw.parts.map((_, i) => partName(chw, i).length);
  const long = names.indexOf(Math.max(...names)), short = names.indexOf(Math.min(...names));
  return [
    { key: "vedder", item: VEDDER, part: ved.parts[0]!.partIx },
    { key: "chilliwack-long", item: CHILLIWACK, part: chw.parts[long]!.partIx },
    { key: "chilliwack-short", item: CHILLIWACK, part: chw.parts[short]!.partIx },
  ];
}

async function draw(w: number, hgt: number, c: Case): Promise<string> {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: hgt });
  window.dispatchEvent(new Event("resize"));
  FLOOR.v = c.floor;
  render(h(LayoutWidthContext.Provider, { value: w }, h(GestureHandlerRootView, { style: { flex: 1 } } as never,
    h(Shell, { source: makeFixtureSource(), loader: openRegsFixtureWithLines(), palette: DARK, theme: "dark", themeName: "dark",
               onTheme: () => {}, tiles: { atlas: "a", basemap: "b" }, systemBack: () => ({ sync: () => {}, dispose: () => {} }),
               feed: { index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) },
               spots: [], onSaveSpot: async () => {},
               initialRoute: reduce(HOME, openWater({ item: c.item as never, part: c.part, view: "map" })) } as never))) as ReactNode);
  await waitFor(() => expect(screen.queryByTestId("water-map-where")).not.toBeNull(), { timeout: 20_000 });
  await act(async () => { await new Promise((r) => setTimeout(r, 400)); });
  const file = join(dir, `${w}-${c.key}.html`);
  writeFileSync(file, snapshot());
  cleanup();
  FLOOR.v = undefined;
  return file;
}

/** Lay the pages out in one Chrome (`wide-measure.mjs`, its own process): `{ "<w> <case>": M }`. */
async function lay(list: readonly (readonly [number, number, Case])[]): Promise<Record<string, M>> {
  const jobs: { page: string; size: [number, number]; measure: string; shot?: string }[] = [];
  const keys: string[] = [];
  for (const [w, hgt, c] of list) {
    const page = await draw(w, hgt, c);
    const shot = process.env.T875_SHOTS
      ? { shot: join(process.env.T875_SHOTS, `t875-${process.env.T875_TAG ?? "now"}-${w}-${c.key}.png`) } : {};
    jobs.push({ page, size: [w, hgt], measure: join(dir, "measure.js"), ...shot });
    keys.push(`${w} ${c.key}`);
  }
  const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                           { encoding: "utf8", timeout: 170_000 });
  const res = JSON.parse(out) as M[];
  return Object.fromEntries(keys.map((k, i) => [k, res[i]!]));
}

/**
 * Every case at both widths, opened fresh; then, on the phone, the Vedder reached by a step: floored at the height the
 * Chilliwack's bar just drew (▼ from the Chilliwack, the owner's way), and at `TALL_FLOOR` (a step taken with the parts
 * list open). The split (900 px up) has no floor.
 */
async function measure(): Promise<Record<string, M>> {
  const all = await cases();
  const fresh = await lay(Object.values(SIZES).flatMap(([w, hgt]) => all.map((c) => [w, hgt, c] as const)));
  const [pw, ph] = SIZES.phone, chw = fresh[`${pw} chilliwack-long`]!;
  const stepped = await lay([
    [pw, ph, { ...all[0]!, key: "vedder-from-chilliwack", floor: chw.barHeight }],
    [pw, ph, { ...all[0]!, key: "vedder-floored-tall", floor: TALL_FLOOR }],
  ]);
  const m = { ...fresh, ...stepped };
  if (process.env.T875_SHOTS)   // the pictures' numbers, beside them
    writeFileSync(join(process.env.T875_SHOTS, `t875-${process.env.T875_TAG ?? "now"}-measure.json`), JSON.stringify(m, null, 1));
  return m;
}

/** Every case whose steps (or, on the phone, whose bar) sit anywhere but where the Vedder's do, in words. */
function moved(m: Record<string, M>): string[] {
  const out: string[] = [];
  for (const [w] of Object.values(SIZES)) {
    const ref = m[`${w} vedder`]!;
    for (const [k, v] of Object.entries(m)) {
      if (!k.startsWith(`${w} `)) continue;
      if (!/Archivo/.test(v.fonts)) out.push(`${k}: the app's faces did not load (${v.fonts})`);
      if (v.step === null || Math.abs(v.step - ref.step!) > 0.5) out.push(`${k}: steps at ${v.step} ≠ the Vedder's ${ref.step}`);
      // the tall floor IS a taller bar (T731 keeps the one just left, its list open): its top is where that bar's was
      if (w === SIZES.phone[0] && !k.endsWith("floored-tall") && (v.bar === null || Math.abs(v.bar - ref.bar!) > 0.5))
        out.push(`${k}: the bar (the map's foot) at ${v.bar} ≠ the Vedder's ${ref.bar}`);
    }
  }
  return out;
}

describe("(T875) the ▼ / ▲ steps stay at one place on screen, part to part and water to water", () => {
  it("at 390 and 1280: one part, five parts, a two-line name and a floored bar all put the steps on the same pixel", async () => {
    const m = await measure();
    // the cases are what they say: the Vedder's name is one line and the Chilliwack's longest two (cut there), at both widths
    expect(Object.fromEntries(Object.entries(m).filter(([k]) => / (vedder|chilliwack-long)$/.test(k)).map(([k, v]) => [k, v.lines])))
      .toEqual({ "390 vedder": 1, "390 chilliwack-long": 2, "1280 vedder": 1, "1280 chilliwack-long": 2 });
    expect(moved(m)).toEqual([]);
  }, 240_000);
});
