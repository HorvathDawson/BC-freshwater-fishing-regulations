// @vitest-environment jsdom
/**
 * (T810 O) THE TROUT TABLE, laid out (as `salmon-keep-layout.test.ts` does the salmon card's keep box, T829): the widest
 * vectors at a phone's 360 and 390 px, at 1× and at the OS's largest text (2×), in headless Chrome with the app's web
 * fonts. Nothing scrolls sideways, no words reach past the window, none are cut, no two lines cross; at 2× the boxes' right
 * band folds into lines under the rows (KeepBox `FOLD_AT`), so the names keep at least half the card. A MUTANT table
 * whose band never folds must fail at 2×. TROUT_SHOTS=<dir> also saves each laid-out table as a PNG there.
 */
import "./render-setup.ts";
import { createElement as h, forwardRef } from "react";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

const SCALE = vi.hoisted(() => ({ v: 1, mutant: false }));
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
// the mutant: a table that does not hear the scale (the band stays a column) while the text grows
vi.spyOn(PixelRatio, "getFontScale").mockImplementation(() => (SCALE.mutant ? 1 : SCALE.v));
const { troutOf } = await import("../packages/core/src");
const { TroutCard } = await import("../packages/ui-native/src/regs/TroutCard");
const { LIGHT } = await import("../packages/ui-native/src/theme");
const { snapshot } = await import("./wide-snapshot");

interface Vector { readonly name: string; readonly day: string; readonly frame: unknown }
const CASES = (JSON.parse(readFileSync(join(process.cwd(), "fixtures/trout/trout-vectors.generated.json"), "utf8")) as { cases: Vector[] }).cases;
/** the widest shapes: a shared band over seven rows and a hatchery label (Fraser R2), twin rows (Windsor), three boxes and a
 *  closed group (Fraser R3 Sep), two keep boxes and the together line (Lois), the year bar (Elk Nov), Skeena's own and shared bands */
const SHOWN = ["fraser-r2-mission-jul15", "windsor-jul15", "fraser-r3-sep15", "lois-jul15", "elk-nov15", "skeena-sedan-mill-jul15",
  // (T810 S, trout 4) the Vedder: the top line's sub and a nested band inside its band's white box
  "vedder-jul15"];

function vmOf(name: string) {
  const c = CASES.find((x) => x.name === name)!;
  const [month, day] = c.day.split("-").map(Number) as [number, number];
  const vm = troutOf({ trout: c.frame, rules: { has: () => true }, frames: { rows: { rows: { length: 1e6 } } } } as never, { month, day }, { month, day });
  if (vm?.kind !== "table") throw new Error(name);
  return vm;
}

const MEASURE = `(() => {
  const gone = (e) => { for (; e && e.nodeType === 1; e = e.parentElement) { const s = getComputedStyle(e);
    if (s.display === 'none' || s.opacity === '0' || s.visibility === 'hidden') return true; } return false; };
  const name = (e) => (e.closest('[data-testid]')?.getAttribute('data-testid') ?? '') + ' "' + (e.textContent ?? '').trim().slice(0, 40) + '"';
  const cut = [];
  for (const e of document.querySelectorAll('body *')) {
    if (gone(e) || !(e.textContent ?? '').trim()) continue;
    const s = getComputedStyle(e);
    const hides = /hidden|clip/.test(s.overflowY) || /hidden|clip/.test(s.overflowX);
    if (!hides || /auto|scroll/.test(s.overflowY)) continue;
    if (e.scrollHeight > e.clientHeight + 1 || e.scrollWidth > e.clientWidth + 1) cut.push(name(e));
  }
  const boxes = [];
  const range = document.createRange();
  const walk = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let n, i = 0; (n = walk.nextNode()); i++) {
    if (!n.textContent.trim() || gone(n.parentElement)) continue;
    range.selectNodeContents(n);
    for (const q of range.getClientRects()) if (q.width > 1 && q.height > 1) {
      const pad = q.height / 5;
      boxes.push({ i, r: { left: q.left, right: q.right, top: q.top + pad, bottom: q.bottom - pad }, el: n.parentElement });
    }
  }
  const wide = [];
  for (const b of boxes) if (b.r.right > innerWidth + 1) wide.push(name(b.el) + ' right ' + Math.round(b.r.right));
  const over = [];
  for (let a = 0; a < boxes.length; a++) for (let b = a + 1; b < boxes.length; b++) {
    const A = boxes[a], B = boxes[b];
    if (A.i === B.i) continue;
    const x = Math.min(A.r.right, B.r.right) - Math.max(A.r.left, B.r.left), y = Math.min(A.r.bottom, B.r.bottom) - Math.max(A.r.top, B.r.top);
    if (x > 2 && y > 2) over.push(name(A.el) + ' × ' + name(B.el));
  }
  const names = [...document.querySelectorAll('[data-testid=keep-name]')].map((e) => Math.round(e.parentElement.getBoundingClientRect().width));
  return JSON.stringify({ cut, over: [...new Set(over)], wide: [...new Set(wide)], names: Math.min(...names),
    overflow: Math.max(0, document.documentElement.scrollWidth - innerWidth), lines: boxes.length });
})()`;

const dir = mkdtempSync(join(tmpdir(), "t810o-layout-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
writeFileSync(join(dir, "measure.js"), MEASURE);

type M = { fonts: string; cut: string[]; over: string[]; wide: string[]; names: number; overflow: number; lines: number };
const WIN = { w: 0, h: 0 };
const docEl = document.documentElement;
vi.spyOn(docEl, "clientWidth", "get").mockImplementation(() => WIN.w);
vi.spyOn(docEl, "clientHeight", "get").mockImplementation(() => WIN.h);
const SIZES: [number, number][] = [[360, 800], [390, 844]];
const actions = { onSheet() {}, onDay() {}, onTab() {}, onBook() {} };

function measure(scales: readonly number[], cases: readonly string[] = SHOWN): Record<string, M> {
  const jobs: { page: string; size: [number, number]; measure: string; shot?: string }[] = [];
  const keys: string[] = [];
  for (const name of cases)
    for (const scale of scales)
      for (const size of SIZES) {
        SCALE.v = scale; WIN.w = size[0]; WIN.h = size[1];
        window.dispatchEvent(new Event("resize"));
        render(h(View, { style: { backgroundColor: LIGHT.card } },
          h(TroutCard, { t: vmOf(name), palette: LIGHT, actions, kicker: true, todayAt: 0.5, months: [..."JFMAMJJASOND"] })));
        const away = screen.queryByTestId("trout-elsewhere");
        if (away) fireEvent.click(away);   // the elsewhere lines laid out too
        const file = join(dir, `${name}-${size[0]}-${scale}${SCALE.mutant ? "-mutant" : ""}.html`);
        writeFileSync(file, snapshot());
        cleanup();
        const shot = process.env.TROUT_SHOTS ? { shot: join(process.env.TROUT_SHOTS, file.replace(/^.*\//, "").replace(/\.html$/, ".png")) } : {};
        jobs.push({ page: file, size, measure: join(dir, "measure.js"), ...shot });
        keys.push(`${name} ${size[0]} ${scale}×`);
      }
  SCALE.v = 1;
  const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                           { encoding: "utf8", timeout: 400_000 });
  const res = JSON.parse(out) as M[];
  return Object.fromEntries(keys.map((k, i) => [k, res[i]!]));
}

function problems(m: Record<string, M>): string[] {
  const out: string[] = [];
  for (const [k, v] of Object.entries(m)) {
    if (!/Archivo_400Regular/.test(v.fonts)) out.push(`${k}: the body face did not load (${v.fonts})`);
    for (const c of v.cut) out.push(`${k}: cut ${c}`);
    for (const o of v.over) out.push(`${k}: overlap ${o}`);
    if (v.overflow > 0 || v.wide.length) out.push(`${k}: ${v.overflow} px sideways overflow (${v.wide.join("; ")})`);
    const [, w, scale] = k.split(" ").map((x) => parseFloat(x)) as [number, number, number];
    if (scale > 1 && v.names < (w - 32) / 2) out.push(`${k}: the names' column is ${v.names} px of ${w - 32}`);
  }
  return out;
}

describe("(T810 O) the trout table at 360 and 390 px, 1× and 2× text", () => {
  it("nothing scrolls sideways, nothing is cut or drawn over another line; at 2× the band folds under the rows", () => {
    const m = measure([1, 2]);
    expect(Object.keys(m)).toHaveLength(SHOWN.length * 4);
    expect(problems(m)).toEqual([]);
    expect(Object.values(m).every((v) => v.lines > 10)).toBe(true);
  }, 480_000);

  it("the mutant (a band that never folds) fails at 2×", () => {
    SCALE.mutant = true;
    try {
      expect(problems(measure([2], ["fraser-r2-mission-jul15"])).length).toBeGreaterThan(0);
    } finally { SCALE.mutant = false; }
  }, 240_000);
});
