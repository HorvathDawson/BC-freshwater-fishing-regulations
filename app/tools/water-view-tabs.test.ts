// @vitest-environment jsdom
/**
 * T1203 (owner 2026-11-26: "the regulations, conditions, map top tabs should be dynamic if they get too big with text increase
 * then regulations could become regs") — A WATER'S VIEW TABS NEVER CLIP OR WRAP: when the full words do not fit the row they
 * shorten ("Regulations" -> "Regs", core `WATER_VIEW_SHORT`), decided from the MEASURED widths, so it holds at any window
 * width and any text size; the full word stays the accessible name.
 *
 * The strip measures itself with onLayout, which a static jsdom render never runs, so this test plays both halves: pass 1
 * lays the strip out in headless Chrome with the full words (the invisible twin's width is what the words need), pass 2
 * hands that width and the row's room to the strip's layout events (a stub of `getBoundingClientRect`) and lays out what it
 * then draws. The phone's text scaling is the T1201 emulation (size and line height x min(OS scale, the Text's cap)).
 * T1203_TAB_SHOTS=<dir> saves each row as a PNG.
 */
import "./render-setup.ts";
import { createElement as h, forwardRef } from "react";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";

const SCALE = vi.hoisted(() => ({ v: 1 }));
vi.mock("react-native", async (importOriginal) => {
  const rn = await importOriginal<typeof import("react-native")>();
  type P = React.ComponentProps<typeof rn.Text>;
  const Text = forwardRef<unknown, P>(function Text(p, ref) {
    const m = p.allowFontScaling === false ? 1 : Math.min(SCALE.v, p.maxFontSizeMultiplier ?? Infinity);
    const flat = (rn.StyleSheet.flatten(p.style) ?? {}) as { fontSize?: number; lineHeight?: number };
    const scaled = m === 1 ? p.style : [p.style, {
      ...(flat.fontSize != null ? { fontSize: flat.fontSize * m } : {}),
      ...(flat.lineHeight != null ? { lineHeight: flat.lineHeight * m } : {}),
    }];
    return h(rn.Text as never, { ...p, ref, style: scaled });
  });
  return { ...rn, Text };
});
const { PixelRatio, View } = await import("react-native");
vi.spyOn(PixelRatio, "getFontScale").mockImplementation(() => SCALE.v);
const { snapshot } = await import("./wide-snapshot");
const { LIGHT } = await import("../packages/ui-native/src/theme");
const { ViewTabs } = await import("../packages/ui-native/src/Faces");
const { WATER_VIEW_LABEL, WATER_VIEW_SHORT } = await import("@app/core");

// layout events: react-native-web observes with a ResizeObserver and then measures with getBoundingClientRect
const SIZES = { room: 0, need: 0 };
class FakeResizeObserver {
  constructor(private cb: (e: { target: Element }[]) => void) {}
  observe(el: Element) { queueMicrotask(() => this.cb([{ target: el }])); }
  unobserve() {}
  disconnect() {}
}
(globalThis as unknown as { ResizeObserver: unknown }).ResizeObserver = FakeResizeObserver;
(window as unknown as { ResizeObserver: unknown }).ResizeObserver = FakeResizeObserver;
const real = HTMLElement.prototype.getBoundingClientRect;
HTMLElement.prototype.getBoundingClientRect = function () {
  const id = this.getAttribute("data-testid");
  const w = id === "water-views-room" ? SIZES.room : id === "water-views-twin" ? SIZES.need : 0;
  return { x: 0, y: 0, left: 0, top: 0, right: w, bottom: 0, width: w, height: 0, toJSON() {} } as DOMRect;
};
const widthOf = (el: Element) => { const id = el.getAttribute("data-testid"); return id === "water-views-room" ? SIZES.room : id === "water-views-twin" ? SIZES.need : 0; };
Object.defineProperty(HTMLElement.prototype, "offsetWidth", { configurable: true, get() { return widthOf(this); } });
afterAll(() => { HTMLElement.prototype.getBoundingClientRect = real; });

const MEASURE_TWIN = `(() => JSON.stringify({ need: document.querySelector('[data-testid=water-views-twin]').getBoundingClientRect().width }))()`;
const MEASURE_ROW = `(() => {
  const tabs = [...document.querySelectorAll('[role=tab]')].map((t) => {
    const l = t.firstElementChild ?? t, r = l.getBoundingClientRect(), cr = t.getBoundingClientRect();
    const lh = parseFloat(getComputedStyle(l).lineHeight) || r.height;
    return { text: t.textContent, aria: t.getAttribute('aria-label'), lines: Math.round(r.height / lh), right: Math.round(cr.right), cut: l.scrollWidth > l.clientWidth + 0.5 };
  });
  return JSON.stringify({ tabs, edge: innerWidth });
})()`;
const dir = mkdtempSync(join(tmpdir(), "t1203-tabs-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
writeFileSync(join(dir, "twin.js"), MEASURE_TWIN);
writeFileSync(join(dir, "row.js"), MEASURE_ROW);
const GUTTER = 16;

const chrome = (page: string, w: number, measure: string, shot?: string): Record<string, unknown> => {
  const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"),
    JSON.stringify({ jobs: [{ page, size: [w, 120], measure, ...(shot ? { shot } : {}) }] })], { encoding: "utf8", timeout: 60_000 });
  return (JSON.parse(out) as Record<string, unknown>[])[0]!;
};

type Row = { tabs: { text: string; aria: string; lines: number; right: number; cut: boolean }[]; edge: number };
async function strip(w: number, scale: number): Promise<Row> {
  SCALE.v = scale;
  const tree = () => h(View, { style: { flex: 1, paddingHorizontal: GUTTER, paddingTop: 20 } },
                       h(ViewTabs, { palette: LIGHT, views: ["regs", "conditions", "map"], on: "regs", onView() {} }));
  SIZES.room = 0; SIZES.need = 0;
  render(tree());
  const full = join(dir, `full-${w}-${scale}.html`);
  writeFileSync(full, snapshot()); cleanup();
  SIZES.need = chrome(full, w, join(dir, "twin.js")).need as number;
  SIZES.room = w - 2 * GUTTER;
  if (process.env.T1203_DEBUG) console.log(`${w} @ ${scale}: need ${SIZES.need} room ${SIZES.room}`);
  render(tree());
  await act(async () => { await new Promise((r) => setTimeout(r, 30)); });
  const file = join(dir, `row-${w}-${scale}.html`);
  writeFileSync(file, snapshot()); cleanup();
  const shot = process.env.T1203_TAB_SHOTS ? join(process.env.T1203_TAB_SHOTS, `view-tabs-${w}-${String(scale).replace(".", "_")}.png`) : undefined;
  const row = chrome(file, w, join(dir, "row.js"), shot) as unknown as Row;
  SCALE.v = 1;
  return row;
}
const labels = (r: Row) => r.tabs.map((t) => t.text);
const FULL = [WATER_VIEW_LABEL.regs, WATER_VIEW_LABEL.conditions, WATER_VIEW_LABEL.map];
const SHORT = [WATER_VIEW_SHORT.regs, WATER_VIEW_LABEL.conditions, WATER_VIEW_LABEL.map];

describe("T1203 a water's view tabs, laid out in Chrome", () => {
  it("full words wherever they fit (decided by width): 320 and 412 at the default size, 412 at 1.3, 360 at 2.0 (the label is capped at 1.3, so it still fits)", async () => {
    for (const [w, s] of [[320, 1], [412, 1], [412, 1.3], [360, 2]] as const) expect(labels(await strip(w, s)), `${w} @ ${s}`).toEqual(FULL);
  }, 120_000);

  it("shortened when the words are wider than the row: 300 wide at 1.3, 260 at 1.0, 280 at 2.0 (a large display size: never wrapped, cut or past the edge)", async () => {
    for (const [w, s] of [[300, 1.3], [260, 1], [280, 2]] as const) {
      const r = await strip(w, s);
      expect(labels(r), `${w} @ ${s}`).toEqual(SHORT);
      for (const t of r.tabs) {
        expect(t.lines, `${w} @ ${s}: ${t.text} lines`).toBe(1);
        expect(t.cut, `${w} @ ${s}: ${t.text} cut`).toBe(false);
        expect(t.right, `${w} @ ${s}: ${t.text} past the edge`).toBeLessThanOrEqual(r.edge - GUTTER + 1);
      }
      expect(r.tabs[0]!.aria, "the full word stays the accessible name").toBe(WATER_VIEW_LABEL.regs);
    }
  }, 180_000);
});
