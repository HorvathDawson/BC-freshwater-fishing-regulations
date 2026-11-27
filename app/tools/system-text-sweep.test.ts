// @vitest-environment jsdom
/**
 * T1201 — THE SHELL'S SCREENS AT A LARGER SYSTEM TEXT SIZE, walked: the Map tab (its search field, the Layers button and the
 * bottom bar; the map is blank, and the Trout / Salmon switch, legend and day bar draw only with live data, so they are NOT swept here: the switch is
 * held by `system-text-labels.test.ts`), Conditions, Runs, Spots and More with its pages, each laid out in headless Chrome at 320, 360 and
 * 412 wide. The same phone-text emulation as `regs-text-scale.test.ts` (size and line height x min(OS scale, the Text's own
 * cap); react-native-web drops both props), the same Chrome layout (`wide-measure.mjs`).
 *
 * Measured per screen: a word cut by a clamp or hidden overflow (`cut`), two lines of different words drawn over each
 * other (`over`), a text line past the window's edge (`wide`), sideways overflow, and a one-word label drawn on two lines
 * (`broken`: "SALMO" / "N"). The gate is 1.3x (what a phone's labels follow, `LABEL_SCALE`): none of those. 1.15, 1.5 and
 * 2.0 are laid out too and reported (T1201_REPORT=<file.json>; T1201_SHOTS=<dir> saves each as a PNG): at 2.0 wrapping is
 * fine, loss is not, so a screen at 1.5 or 2.0 is judged by the report and the pictures, not by this gate.
 */
import "./render-setup.ts";
import { createElement as h, forwardRef } from "react";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, openGauge, reduce, type Route } from "@app/ui";
import type { StationId } from "@app/data";

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
// (T1194) the gauge page's release facts: long water names and every trust word, so the page's rows are laid out at their widest
vi.mock("@app/ui", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@app/ui")>()),
  useGaugePage: () => ({ state: "ready", error: null, value: { station: "08MH024", name: "Fraser River at Mission", tidal: true, backwater: false, diversion: false,
    serves: [{ item: "a", name: "Chilliwack/Vedder River (Sardis to the Fraser confluence)", trust: "own" }, { item: "b", name: "Nicomen Slough", trust: "good" },
             { item: "c", name: "Enterprise Channel near the Pitt River mouth", trust: "weak" }] } }),
}));
// no WebGL in jsdom: the map itself is blank, the screen around it (rail, switch, legend, day bar) is the app's
vi.mock("@app/map", async (importOriginal) => ({ ...(await importOriginal<typeof import("@app/map")>()), Map: () => null }));
vi.mock("../packages/ui-native/src/FishSpinner", () => ({ FishSpinner: () => null }));
const { PixelRatio } = await import("react-native");
vi.spyOn(PixelRatio, "getFontScale").mockImplementation(() => SCALE.v);
const { snapshot } = await import("./wide-snapshot");
const { LIGHT } = await import("../packages/ui-native/src/theme");
const { Shell } = await import("../packages/ui-native/src/Shell");

const WIN = { w: 0, h: 0 };
const docEl = document.documentElement;
vi.spyOn(docEl, "clientWidth", "get").mockImplementation(() => WIN.w);
vi.spyOn(docEl, "clientHeight", "get").mockImplementation(() => WIN.h);

const MEASURE = `(() => {
  for (const a of document.getAnimations()) { try { a.finish(); } catch {} }
  // a More page is drawn over the More tab, which stays mounted beneath it: that tab is not on screen
  const page = document.querySelector('[data-testid^=menu-page-]');
  const gone = (e) => { for (; e && e.nodeType === 1; e = e.parentElement) { const s = getComputedStyle(e);
    if (page && e.getAttribute('data-testid') === 'more-screen' && !e.contains(page)) return true;
    if (s.display === 'none' || s.opacity === '0' || s.visibility === 'hidden') return true; } return false; };
  const name = (e) => (e.closest('[data-testid]')?.getAttribute('data-testid') ?? '') + ' "' + (e.textContent ?? '').trim().replace(/\\s+/g, ' ').slice(0, 40) + '"';
  const cut = [], broken = [];
  for (const e of document.querySelectorAll('body *')) {
    if (gone(e) || !(e.textContent ?? '').trim()) continue;
    const s = getComputedStyle(e);
    const hides = /hidden|clip/.test(s.overflowY) || /hidden|clip/.test(s.overflowX) || s.webkitLineClamp !== 'none' && s.webkitLineClamp !== '';
    if (hides && !/auto|scroll/.test(s.overflowY) && (e.scrollHeight > e.clientHeight + 1 || e.scrollWidth > e.clientWidth + 1))
      cut.push(name(e) + ' ' + e.scrollWidth + 'x' + e.scrollHeight + ' in ' + e.clientWidth + 'x' + e.clientHeight);
    // a one-word leaf drawn on two lines is a word broken in the middle
    if (!e.children.length && !/[\\s-]/.test(e.textContent.trim())) {
      const lh = parseFloat(s.lineHeight) || e.getBoundingClientRect().height;
      if (e.getBoundingClientRect().height > lh * 1.5) broken.push(name(e));
    }
  }
  const boxes = [], range = document.createRange(), walk = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let n, i = 0; (n = walk.nextNode()); i++) {
    if (!n.textContent.trim() || gone(n.parentElement)) continue;
    range.selectNodeContents(n);
    for (const q of range.getClientRects()) if (q.width > 1 && q.height > 1) {
      const pad = q.height / 5;
      // under another layer (a page's sheet over the tab beneath, scrolled content under the bar) it is not on screen
      const cx = (q.left + q.right) / 2, cy = (q.top + q.bottom) / 2;
      if (cx >= 0 && cy >= 0 && cx < innerWidth && cy < innerHeight) {
        const top = document.elementFromPoint(cx, cy);
        if (top && !n.parentElement.contains(top) && !top.contains(n.parentElement)) continue;
      }
      boxes.push({ i, r: { left: q.left, right: q.right, top: q.top + pad, bottom: q.bottom - pad }, el: n.parentElement });
    }
  }
  const wide = [], over = [];
  for (const b of boxes) if (b.r.right > innerWidth + 1 || b.r.left < -1) wide.push(name(b.el));
  for (let a = 0; a < boxes.length; a++) for (let b = a + 1; b < boxes.length; b++) {
    const A = boxes[a], B = boxes[b];
    if (A.i === B.i) continue;
    const x = Math.min(A.r.right, B.r.right) - Math.max(A.r.left, B.r.left), y = Math.min(A.r.bottom, B.r.bottom) - Math.max(A.r.top, B.r.top);
    if (x > 2 && y > 2) over.push(name(A.el) + ' x ' + name(B.el));
  }
  return JSON.stringify({ cut, broken, over: [...new Set(over)], wide: [...new Set(wide)],
    overflow: Math.max(0, document.documentElement.scrollWidth - innerWidth), lines: boxes.length });
})()`;

const dir = mkdtempSync(join(tmpdir(), "t1201-sweep-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
writeFileSync(join(dir, "measure.js"), MEASURE);

type M = { fonts: string; cut: string[]; broken: string[]; over: string[]; wide: string[]; overflow: number; lines: number };

const press = (re: RegExp) => act(() => { fireEvent.click(screen.getAllByText(re)[0]!); });
const tab = (name: string) => act(() => { fireEvent.click(screen.getByRole("tab", { name })); });
const slidOut = () => act(() => {
  for (const el of document.querySelectorAll("div"))
    for (const type of ["animationend", "webkitAnimationEnd"]) el.dispatchEvent(new Event(type, { bubbles: true }));
});
/** the screens: where to start, and what to press to arrive */
const SCREENS: { id: string; start?: Route; go?: () => void }[] = [
  { id: "map" },
  { id: "conditions", go: () => tab("Conditions") },
  { id: "runs", go: () => tab("Runs") },
  { id: "spots", go: () => tab("Spots") },
  { id: "more", go: () => tab("More") },
  // (T1194) a gauge's own page, over the Conditions map: Back, the name, the reading line, the waters it serves, the foot card
  { id: "gauge-page", start: reduce(reduce(HOME, { type: "tab", tab: "conditions" }), openGauge("08MH024" as StationId)) },
  { id: "more-settings", go: () => { tab("More"); press(/^Settings$/); } },
  { id: "more-help", go: () => { tab("More"); press(/^Help$/); } },
  { id: "more-glossary", go: () => { tab("More"); press(/^Glossary$/); } },
  { id: "more-sources-and-licences", go: () => { tab("More"); press(/^Sources and licences$/); } },
  { id: "more-about", go: () => { tab("More"); press(/^About$/); } },
  { id: "more-report-a-problem", go: () => { tab("More"); press(/^Report a problem$/); } },
];

async function sweep(scales: readonly number[], widths: readonly number[]): Promise<Record<string, M>> {
  const jobs: { page: string; size: [number, number]; measure: string; shot?: string }[] = [];
  const keys: string[] = [];
  for (const scale of scales) for (const w of widths) for (const sc of SCREENS) {
    SCALE.v = scale; WIN.w = w; WIN.h = 800;
    window.dispatchEvent(new Event("resize"));
    render(h(GestureHandlerRootView, null,
      h(Shell, { source: makeFixtureSource(), palette: LIGHT, theme: "light", themeName: "light", onTheme() {},
                 tiles: { atlas: "a", basemap: "b" } as never, initialRoute: sc.start ?? HOME, spots: [], onSaveSpot() {},
                 locate: null })));
    await act(async () => { await new Promise((r) => setTimeout(r, 250)); });   // the source's answers arrive
    try { sc.go?.(); } catch (e) { throw new Error(`${sc.id}: could not arrive (${(e as Error).message.split("\n")[0]}); the screen reads: ${document.body.textContent?.slice(0, 600)}`); }
    await act(async () => { await new Promise((r) => setTimeout(r, 150)); });
    slidOut();
    const file = join(dir, `${sc.id}-${w}-${scale}.html`);
    writeFileSync(file, snapshot());
    cleanup();
    const shot = process.env.T1201_SHOTS ? { shot: join(process.env.T1201_SHOTS, `${sc.id}-${w}-${String(scale).replace(".", "_")}.png`) } : {};
    jobs.push({ page: file, size: [w, 800], measure: join(dir, "measure.js"), ...shot });
    keys.push(`${sc.id} ${w} ${scale}x`);
  }
  SCALE.v = 1;
  const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                           { encoding: "utf8", timeout: 400_000, maxBuffer: 64 << 20 });
  const res = JSON.parse(out) as M[];
  return Object.fromEntries(keys.map((k, i) => [k, res[i]!]));
}

const problems = (m: Record<string, M>): string[] => Object.entries(m).flatMap(([k, v]) => [
  ...(/Archivo_400Regular/.test(v.fonts) ? [] : [`${k}: the body face did not load`]),
  ...v.cut.map((c) => `${k}: cut ${c}`), ...v.broken.map((c) => `${k}: one word on two lines ${c}`),
  ...v.over.map((c) => `${k}: overlap ${c}`), ...v.wide.map((c) => `${k}: past the edge ${c}`),
  ...(v.overflow > 0 ? [`${k}: ${v.overflow} px sideways overflow`] : []),
]);

describe("T1201 the Shell's screens at a larger system text size, laid out in Chrome", () => {
  it("at 1.3x, 320, 360 and 412 wide: no cut word, no overlap, nothing past the edge, no word broken in two (and the walk reached every screen)", async () => {
    const m = await sweep(process.env.T1201_REPORT ? [1, 1.15, 1.3, 1.5, 2] : [1.3], [320, 360, 412]);
    if (process.env.T1201_REPORT) writeFileSync(process.env.T1201_REPORT, JSON.stringify(Object.fromEntries(Object.entries(m).map(([k, v]) => [k, problems({ [k]: v })])), null, 1));
    expect(problems(Object.fromEntries(Object.entries(m).filter(([k]) => / 1\.3x$/.test(k))))).toEqual([]);
    expect(Object.entries(m).filter(([, v]) => v.lines < 5).map(([k]) => k)).toEqual([]);
  }, 600_000);
});
