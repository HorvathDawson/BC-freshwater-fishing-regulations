// @vitest-environment jsdom
/**
 * T40 — THE REGULATIONS VIEW AT THE OS'S LARGEST TEXT (200 %), laid out: no words clipped, no two lines drawn over each
 * other, no sideways scroll — the header's two-line name and place label, the date pill, the tabs, the strip, the tier
 * numbers, every Keep block, Gear / Methods / Licence.
 *
 * React Native scales a Text's font size and line height by the OS text size, each Text stopping at its own
 * `maxFontSizeMultiplier` (numbers and the header: 2×, `CAPPED_SCALE`); a browser does not (react-native-web drops both
 * props). So this file's `react-native` Text does what a phone does — the size and line height times min(scale, its cap)
 * — and `PixelRatio.getFontScale()` reports the scale, as a phone's does after the change (the header's place row reads
 * it). The frozen golden pages are mounted in the water frame as the app draws them and the DOM react-native-web drew,
 * with the app's web fonts, is laid out by headless Chrome (`wide-measure.mjs`, as T45's layout test) on a phone and a
 * desktop, at 1× (the control: what the layout already was) and at 2×.
 *
 * Measured: every element that cuts its content (overflow hidden — a `numberOfLines` clamp is one) and has more than it
 * shows; every pair of text line boxes from different words that cross (the place label's invisible measuring twin
 * excepted: it lies under the shown label on purpose); the document's and the page scroll's sideways overflow. A MUTANT
 * header (the place row back at its fixed one-scale height) must fail.
 */
import "./render-setup.ts";
import { createElement as h, forwardRef, type ReactNode } from "react";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { cleanup, render } from "@testing-library/react";
import type { LicenceVM, RegsPage, SheetKey, SheetVM, Who } from "@app/core";
import type { LayoutClass } from "@app/ui";

/** the OS text size this run lays out at; the mutant header (off unless a test turns it on) */
const SCALE = vi.hoisted(() => ({ v: 1, mutant: false }));

vi.mock("react-native", async (importOriginal) => {
  const rn = await importOriginal<typeof import("react-native")>();
  type P = React.ComponentProps<typeof rn.Text>;
  // a phone's text scaling: size and line height × min(the OS scale, the Text's cap); allowFontScaling={false} → 1×
  const Text = forwardRef<unknown, P>(function Text(p, ref) {
    const m = p.allowFontScaling === false ? 1 : Math.min(SCALE.v, p.maxFontSizeMultiplier ?? Infinity);
    const flat = (rn.StyleSheet.flatten(p.style) ?? {}) as { fontSize?: number; lineHeight?: number; height?: number };
    const scaled = m === 1 ? p.style : [p.style, {
      ...(flat.fontSize != null ? { fontSize: flat.fontSize * m } : {}),
      ...(flat.lineHeight != null ? { lineHeight: flat.lineHeight * m } : {}),
    }];
    return h(rn.Text as never, { ...p, ref, style: scaled });
  });
  return { ...rn, Text };
});
const { PixelRatio, Text } = await import("react-native");
// the mutant: a header that does not hear the scale (its place row held to the one-scale height) while the text grows
vi.spyOn(PixelRatio, "getFontScale").mockImplementation(() => (SCALE.mutant ? 1 : SCALE.v));
const { useState } = await import("react");
const { WaterFrame } = await import("../packages/ui-native/src/WaterFrame");
const { RegsScreen } = await import("../packages/ui-native/src/regs/RegsScreen");
const { GearTab } = await import("../packages/ui-native/src/regs/GearTab");
const { MethodsTab } = await import("../packages/ui-native/src/regs/MethodsTab");
const { LicenceTab } = await import("../packages/ui-native/src/regs/LicenceTab");
const { LIGHT } = await import("../packages/ui-native/src/theme");
const { SheetHost } = await import("../packages/ui-native/src/regs/SheetHost");
const { snapshot } = await import("./wide-snapshot");
const PAGES = {
  vedder_1010: (await import("../packages/ui-native/src/regs/fixtures/vedder_1010.json")).default,
  klwa_1010: (await import("../packages/ui-native/src/regs/fixtures/klwa_1010.json")).default,
  glenn_501: (await import("../packages/ui-native/src/regs/fixtures/glenn_501.json")).default,
  vedder_615: (await import("../packages/ui-native/src/regs/fixtures/vedder_615.json")).default,
} as unknown as Record<string, RegsPage>;
const LIC = (await import("../packages/ui-native/src/regs/fixtures/licence_vedder_1010.json")).default as unknown as Record<string, LicenceVM>;

/** (T37) the vedder_1010 page with its frozen sheets, and the sheets laid out */
type Frozen = Omit<RegsPage, "sheet"> & { sheets: Record<string, SheetVM | null> };
const SHEETS_PAGE = ((f: Frozen): RegsPage => ({ ...f, sheet: (k: SheetKey) => f.sheets[k] ?? null }) as RegsPage)(PAGES.vedder_1010 as unknown as Frozen);
const SHEET_KEYS = ["gl:catch_and_release", "pos:pos", "hatch", "sp:RB", "srcs:gl|hook", "rule:zp:conduct::conduct.r1", "licsrc:adult",
  "other:o0", "src:r0"] as const;

const SIZES: Partial<Record<LayoutClass, [number, number]>> = { phone: [390, 844], desktop: [1280, 800] };

const MEASURE = `(() => {
  // a sheet's Modal fades or slides in (CSS animations): measure where it comes to rest
  for (const a of document.getAnimations()) { try { a.finish(); } catch {} }
  const gone = (e) => { for (; e && e.nodeType === 1; e = e.parentElement) { const s = getComputedStyle(e);
    if (s.display === 'none' || s.opacity === '0' || s.visibility === 'hidden') return true; } return false; };
  const name = (e) => (e.closest('[data-testid]')?.getAttribute('data-testid') ?? '') + ' "' + (e.textContent ?? '').trim().slice(0, 40) + '"';
  // 1. cut: an element that hides overflow and holds more than it shows (a numberOfLines clamp is one)
  const cut = [];
  for (const e of document.querySelectorAll('body *')) {
    if (gone(e) || !(e.textContent ?? '').trim()) continue;
    const s = getComputedStyle(e);
    const hides = /hidden|clip/.test(s.overflowY) || /hidden|clip/.test(s.overflowX) || s.webkitLineClamp !== 'none' && s.webkitLineClamp !== '';
    if (!hides || /auto|scroll/.test(s.overflowY)) continue;
    if (e.scrollHeight > e.clientHeight + 1 || e.scrollWidth > e.clientWidth + 1) cut.push(name(e) + ' ' + e.scrollHeight + '>' + e.clientHeight);
  }
  // 2. overlap: the INK bands of different text nodes that cross by more than 2 px each way. A text rect is the face's
  // content area (ascender to descender, ~1.2 em), wider than its ink: a 26 px numeral set in a 21 px line (tokens.md
  // §2.4, the prototype's own setting) reaches into the lines around it by its empty ascender and descender. The band is
  // the rect less a fifth of its height above and below — where letters are drawn.
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
  // 3. what reaches past the window's right edge (the deepest such text-holding elements)
  const wide = [];
  for (const b of boxes) if (b.r.right > innerWidth + 1) wide.push(name(b.el) + ' right ' + Math.round(b.r.right));
  const over = [];
  for (let a = 0; a < boxes.length; a++) for (let b = a + 1; b < boxes.length; b++) {
    const A = boxes[a], B = boxes[b];
    if (A.i === B.i) continue;
    const x = Math.min(A.r.right, B.r.right) - Math.max(A.r.left, B.r.left), y = Math.min(A.r.bottom, B.r.bottom) - Math.max(A.r.top, B.r.top);
    if (x > 2 && y > 2) over.push(name(A.el) + ' × ' + name(B.el));
  }
  const scroll = document.querySelector('[data-testid=regs-scroll]');
  const title = document.querySelector('[data-testid=water-title]'), place = document.querySelector('[data-testid=regs-place]');
  return JSON.stringify({ cut, over: [...new Set(over)], wide: [...new Set(wide)],
    overflow: Math.max(0, document.documentElement.scrollWidth - innerWidth) + (scroll ? Math.max(0, scroll.scrollWidth - scroll.clientWidth) : 0),
    title: title ? Math.round(title.getBoundingClientRect().height) : 0, place: place ? Math.round(place.getBoundingClientRect().height) : 0,
    lines: boxes.length });
})()`;

const dir = mkdtempSync(join(tmpdir(), "t40-scale-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
writeFileSync(join(dir, "measure.js"), MEASURE);

type M = { fonts: string; cut: string[]; over: string[]; wide: string[]; overflow: number; title: number; place: number; lines: number };

/** the Licence tab as the water screen holds it (vedder_1010 only: the frozen six anglers) */
function Licence({ actions }: { actions: Parameters<typeof LicenceTab>[0]["actions"] }) {
  const [who, setWho] = useState<Who>({ age: "adult", residency: "resident" });
  return h(LicenceTab, { vm: LIC[`${who.age}|${who.residency}`]!, who, palette: LIGHT, actions, onWho: setWho });
}

/**
 * The window the snapshot is drawn for (jsdom's is 0 × 0: a phone sheet's body, held to 68 % of the window's height,
 * would draw nothing): react-native-web's Dimensions reads the document element's client size on a resize.
 */
const WIN = { w: 0, h: 0 };
const docEl = document.documentElement;
vi.spyOn(docEl, "clientWidth", "get").mockImplementation(() => WIN.w);
vi.spyOn(docEl, "clientHeight", "get").mockImplementation(() => WIN.h);
function windowAt([w, h]: [number, number]) {
  WIN.w = w; WIN.h = h;
  window.dispatchEvent(new Event("resize"));
}

/** every state at each class and each scale, snapshotted; then all laid out in one Chrome: `{ "<state> <class> <scale>×": M }` */
function measure(scales: readonly number[]): Record<string, M> {
  const jobs: { page: string; size: [number, number]; measure: string; shot?: string }[] = [];
  // T40_SHOTS=<dir>: also save each laid-out page as a PNG there (pictures for the owner, not part of the gate)
  const shot = (f: string) => (process.env.T40_SHOTS ? { shot: join(process.env.T40_SHOTS, f.replace(/^.*\//, "").replace(/\.html$/, ".png")) } : {});
  const keys: string[] = [];
  for (const scale of scales)
    for (const [id, page] of Object.entries(PAGES))
      for (const [c, size] of Object.entries(SIZES) as [LayoutClass, [number, number]][]) {
        SCALE.v = scale; windowAt(size);
        render(h(WaterFrame, { palette: LIGHT, face: "regulations", onBack() {}, backLabel: "Map", children:
          h(RegsScreen, { page, palette: LIGHT, layout: c, actions: { onSheet() {}, onDay() {}, onBook() {} }, onDate() {},
                          gear: page.gear ? (a: never) => h(GearTab, { vm: page.gear!, palette: LIGHT, actions: a }) : undefined,
                          methods: page.methods ? (a: never) => h(MethodsTab, { vm: page.methods!, palette: LIGHT, actions: a, referenceOpen: true }) : undefined,
                          licence: id === "vedder_1010" ? (a: never) => h(Licence, { actions: a }) : h(Text, null, "") }) }) as ReactNode);
        const file = join(dir, `${id}-${c}-${scale}.html`);
        writeFileSync(file, snapshot());
        cleanup();
        jobs.push({ page: file, size, measure: join(dir, "measure.js"), ...shot(file) });
        keys.push(`${id} ${c} ${scale}×`);
      }
  // (T37) the sheets over the page, at the same scales and widths: a glossary word, Possession, How to tell, a fish, a
  // source list, a rule, a licence source, an other fish and a row's every line
  for (const scale of scales)
    for (const k of SHEET_KEYS)
      for (const [c, size] of Object.entries(SIZES) as [LayoutClass, [number, number]][]) {
        SCALE.v = scale; windowAt(size);
        render(h(SheetHost, { page: SHEETS_PAGE, open: k as SheetKey, onOpen() {}, onClose() {}, palette: LIGHT, layout: c }));
        const file = join(dir, `sheet-${k.replace(/[^a-z0-9]+/gi, "_")}-${c}-${scale}.html`);
        writeFileSync(file, snapshot());
        cleanup();
        jobs.push({ page: file, size, measure: join(dir, "measure.js"), ...shot(file) });
        keys.push(`sheet ${k} ${c} ${scale}×`);
      }
  SCALE.v = 1;
  const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                           { encoding: "utf8", timeout: 170_000 });
  const res = JSON.parse(out) as M[];
  return Object.fromEntries(keys.map((k, i) => [k, res[i]!]));
}

/** What breaks the acceptance, in words; [] is clean. */
function problems(m: Record<string, M>): string[] {
  const out: string[] = [];
  for (const [k, v] of Object.entries(m)) {
    if (!/Archivo_400Regular/.test(v.fonts)) out.push(`${k}: the body face did not load (${v.fonts})`);
    for (const c of v.cut) out.push(`${k}: cut ${c}`);
    for (const o of v.over) out.push(`${k}: overlap ${o}`);
    if (v.overflow > 0) out.push(`${k}: ${v.overflow} px sideways overflow (${v.wide.join("; ")})`);
  }
  return out;
}

describe("T40 the Regulations view at 200 % text, laid out in Chrome", () => {
  it("nothing cut, nothing drawn over anything, no sideways scroll — at 1× and at 2×, on a phone and a desktop, sheets too", () => {
    const m = measure([1, 2]);
    expect(problems(m)).toEqual([]);
    // not vacuous: every page laid out its words, every sheet its title, Done and a body (45: the date pill's typed ▾
    // was a text box of its own until T186 drew the set's chevron, so the Vedder's shortest page went 50 → 49)
    expect(Object.entries(m).filter(([k, v]) => v.lines < (k.startsWith("sheet ") ? 3 : 45)).map(([k, v]) => `${k} ${v.lines}`)).toEqual([]);
    expect(Object.keys(m).filter((k) => k.startsWith("sheet ")).length).toBe(SHEET_KEYS.length * 2 * 2);
    // the evidence: the header's name row and place row grow with the text (two lines each at 2×), and how many line boxes
    expect(Object.fromEntries(Object.entries(m).filter(([k]) => !k.startsWith("sheet ")).map(([k, v]) => [k, `title ${v.title} place ${v.place}`])))
      .toMatchInlineSnapshot(`
        {
          "glenn_501 desktop 1×": "title 48 place 32",
          "glenn_501 desktop 2×": "title 48 place 65",
          "glenn_501 phone 1×": "title 48 place 32",
          "glenn_501 phone 2×": "title 60 place 65",
          "klwa_1010 desktop 1×": "title 48 place 32",
          "klwa_1010 desktop 2×": "title 48 place 65",
          "klwa_1010 phone 1×": "title 48 place 32",
          "klwa_1010 phone 2×": "title 117 place 65",
          "vedder_1010 desktop 1×": "title 48 place 32",
          "vedder_1010 desktop 2×": "title 48 place 65",
          "vedder_1010 phone 1×": "title 48 place 32",
          "vedder_1010 phone 2×": "title 60 place 65",
          "vedder_615 desktop 1×": "title 48 place 32",
          "vedder_615 desktop 2×": "title 48 place 65",
          "vedder_615 phone 1×": "title 48 place 32",
          "vedder_615 phone 2×": "title 60 place 65",
        }
      `);
  }, 180_000);

  it("MUTATION: the measure's three rules each catch their planted fault (a clamp that cuts, two lines drawn over each other, a line past the edge)", () => {
    const file = join(dir, "mutants.html");
    writeFileSync(file, `<!doctype html><html><body style="margin:0;font:15px/20px sans-serif">
      <div data-testid="clamped" style="margin-bottom:200px;width:120px;display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:2;overflow:hidden">one two three four five six seven eight nine ten eleven twelve</div>
      <div style="position:relative;height:40px"><div data-testid="under" style="position:absolute;top:0">Underneath words</div><div data-testid="over" style="position:absolute;top:2px;left:10px">Over the top</div></div>
      <div data-testid="wide" style="white-space:nowrap">a line far too long for a phone, a line far too long for a phone, a line far too long</div>
      <div data-testid="regs-scroll" style="overflow:auto"></div></body></html>`);
    const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"),
      JSON.stringify({ jobs: [{ page: file, size: [390, 844], measure: join(dir, "measure.js") }] })], { encoding: "utf8", timeout: 60_000 });
    const [m] = JSON.parse(out) as M[];
    expect(m!.cut.map((c) => c.replace(/ \d+>\d+$/, ""))).toEqual(['clamped "one two three four five six seven eight "']);
    expect(m!.over).toEqual(['under "Underneath words" × over "Over the top"']);
    expect(m!.overflow).toBeGreaterThan(0);
    expect(m!.wide.every((w) => w.startsWith('wide "a line far too long'))).toBe(true);
  }, 60_000);

  it("MUTATION: the place row held to its one-scale height cuts the label at 2×", () => {
    SCALE.mutant = true;
    try {
      const m = measure([2]);
      expect(problems(m).some((p) => / cut regs-place /.test(p))).toBe(true);
    } finally { SCALE.mutant = false; }
  }, 180_000);
});
