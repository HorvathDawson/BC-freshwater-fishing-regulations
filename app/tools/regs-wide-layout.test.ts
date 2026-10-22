// @vitest-environment jsdom
/**
 * T45 — THE REGULATIONS VIEW ON A WIDE SCREEN, laid out (the acceptance: "the body line length stays at or under 80
 * characters, no horizontal scroll"; the frame's left edge kept, T44).
 *
 * The frozen golden pages (vedder_1010, klwa_1010, bc1_501) are mounted in the water frame as the app draws them, at
 * each layout class, and the DOM react-native-web drew — with the app's web fonts — is laid out by headless Chrome
 * (`wide-measure.mjs`; jsdom has no layout) at the sizes of that class:
 *
 *   phone 390   tablet 900, 1024   desktop 1280, 1440
 *
 * and measured: the longest line of the page's text (characters, a paragraph's line = a run of characters until the next
 * one starts further left), the page's left and right edges and how far its centre is from the centre of the room it
 * has, the content's left edge under the name row (the T44 measure: on every view the frame's gutter), and any sideways
 * overflow of the document or the page's scroll. A MUTANT reading column without its measure must fail.
 */
import "./render-setup.ts";
import { createElement as h, type ReactNode } from "react";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { cleanup, render } from "@testing-library/react";
import type { RegsPage } from "@app/core";
import type { LayoutClass } from "@app/ui";

/** The mutant: the reading column with no measure (it takes all the room the page has). Off unless a test turns it on. */
const MUTANT = vi.hoisted(() => ({ on: false }));
vi.mock("../packages/ui-native/src/theme", async (importOriginal) => {
  const real = await importOriginal<typeof import("../packages/ui-native/src/theme")>();
  return { ...real, get REGS_WIDE() { return MUTANT.on ? { ...real.REGS_WIDE, measure: 100_000 } : real.REGS_WIDE; } };
});

const { Text } = await import("react-native");
const { WaterFrame } = await import("../packages/ui-native/src/WaterFrame");
const { RegsScreen } = await import("../packages/ui-native/src/regs/RegsScreen");
const { LIGHT } = await import("../packages/ui-native/src/theme");
const { snapshot } = await import("./wide-snapshot");
const PAGES = {
  vedder_1010: (await import("../packages/ui-native/src/regs/fixtures/vedder_1010.json")).default,
  klwa_1010: (await import("../packages/ui-native/src/regs/fixtures/klwa_1010.json")).default,
  bc1_501: (await import("../packages/ui-native/src/regs/fixtures/bc1_501.json")).default,
} as unknown as Record<string, RegsPage>;

const SIZES: Record<LayoutClass, [number, number][]> = {
  phone: [[390, 844]], tablet: [[900, 900], [1024, 768]], desktop: [[1280, 800], [1440, 900]],
};

const MEASURE = `(() => {
  const R = (e) => e ? e.getBoundingClientRect() : null;
  const hidden = (n) => !!n.parentElement.closest('[aria-hidden=true]');
  const content = document.querySelector('[data-testid=regs-content]');
  const para = (el) => { while (el && el !== content && getComputedStyle(el).display === 'inline') el = el.parentElement; return el; };
  const runs = new Map();
  const range = document.createRange();
  const walk = document.createTreeWalker(content, NodeFilter.SHOW_TEXT);
  for (let n; (n = walk.nextNode());) {
    if (hidden(n) || !n.textContent) continue;
    const p = para(n.parentElement);
    const run = runs.get(p) ?? { lines: [''], x: -Infinity, body: parseFloat(getComputedStyle(p).fontSize) >= 14.5 };
    runs.set(p, run);
    for (let i = 0; i < n.textContent.length; i++) {
      range.setStart(n, i); range.setEnd(n, i + 1);
      const r = range.getClientRects()[0];
      if (!r) continue;
      if (r.left < run.x - 1) run.lines.push('');
      run.x = r.left;
      run.lines[run.lines.length - 1] += n.textContent[i];
    }
  }
  // body text (the page's 15 px faces) is what the measure holds to ~70; the 12–13 px notes run longer, reported apart
  let longest = '', note = '';
  for (const { lines, body } of runs.values())
    for (const l of lines) {
      if (body && l.trim().length > longest.trim().length) longest = l;
      if (!body && l.trim().length > note.trim().length) note = l;
    }
  const title = document.querySelector('[data-testid=water-title]');
  const below = title ? R(title).bottom : Infinity;
  let left = Infinity;
  const all = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let n; (n = all.nextNode());) {
    if (!n.textContent.trim() || hidden(n)) continue;
    range.selectNodeContents(n);
    for (const r of range.getClientRects()) if (r.width > 0 && r.top >= below && r.top < innerHeight) left = Math.min(left, r.left);
  }
  const page = R(content), scroll = document.querySelector('[data-testid=regs-scroll]'), room = R(scroll);
  const roomMid = room.left + scroll.clientWidth / 2, pageMid = (page.left + page.right) / 2;
  return JSON.stringify({
    longest: longest.trim().length, line: longest.trim(), note: note.trim().length,
    page: [Math.round(page.left), Math.round(page.right)], offCentre: Math.round(pageMid - roomMid),
    left: left === Infinity ? null : Math.round(left),
    overflow: Math.max(0, document.documentElement.scrollWidth - innerWidth) + Math.max(0, scroll.scrollWidth - scroll.clientWidth),
  });
})()`;

const dir = mkdtempSync(join(tmpdir(), "t45-wide-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
writeFileSync(join(dir, "measure.js"), MEASURE);

type M = { fonts: string; longest: number; line: string; note: number; page: [number, number]; offCentre: number; left: number | null; overflow: number };

/** Every state at every class, snapshotted; then all laid out in one Chrome: `{ "<state> <class> <w>": M }`. */
function measure(): Record<string, M> {
  const jobs: { page: string; size: [number, number]; measure: string }[] = [];
  const keys: string[] = [];
  for (const [id, page] of Object.entries(PAGES))
    for (const c of Object.keys(SIZES) as LayoutClass[]) {
      render(h(WaterFrame, { palette: LIGHT, face: "regulations", children:
        h(RegsScreen, { page, palette: LIGHT, layout: c, actions: { onSheet() {}, onDay() {}, onBook() {} }, onDate() {},
                        gear: h(Text, null, "gear section"), methods: h(Text, null, "methods section"),
                        licence: h(Text, null, "licence section") }) }) as ReactNode);
      const file = join(dir, `${id}-${c}.html`);
      writeFileSync(file, snapshot());
      cleanup();
      for (const size of SIZES[c]) { jobs.push({ page: file, size, measure: join(dir, "measure.js") }); keys.push(`${id} ${c} ${size[0]}`); }
    }
  const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                           { encoding: "utf8", timeout: 120_000 });
  const res = JSON.parse(out) as M[];
  return Object.fromEntries(keys.map((k, i) => [k, res[i]!]));
}

/** What breaks the acceptance, in words; [] is clean. */
function problems(m: Record<string, M>): string[] {
  const out: string[] = [];
  for (const [k, v] of Object.entries(m)) {
    const [, c, w] = k.split(" ") as [string, LayoutClass, string];
    const gutter = (Number(w) - Math.min(Number(w), 1280)) / 2 + 16;
    if (!/Archivo_400Regular/.test(v.fonts)) out.push(`${k}: the body face did not load (${v.fonts})`);
    if (c !== "phone" && v.longest > 80) out.push(`${k}: a line of ${v.longest} characters: ${v.line}`);
    if (v.overflow > 0) out.push(`${k}: ${v.overflow} px sideways overflow`);
    if (v.left !== gutter) out.push(`${k}: content left ${v.left} ≠ the frame's gutter ${gutter}`);
    if (c !== "phone" && Math.abs(v.offCentre) > 1) out.push(`${k}: the page is ${v.offCentre} px off the centre of its room`);
  }
  return out;
}

describe("T45 the Regulations view on a wide screen, laid out in Chrome", () => {
  it("lines ≤ 80 characters on a tablet and a desktop, no sideways scroll, the frame's left edge, the page centred in its room", () => {
    const m = measure();
    expect(problems(m)).toEqual([]);
    // the evidence, whole: where the page is (left–right px), its longest body line, its longest 12–13 px note line
    expect(Object.fromEntries(Object.entries(m).map(([k, v]) => [k, `${v.page.join("–")} body ${v.longest} note ${v.note}`])))
      .toMatchInlineSnapshot(`
        {
          "bc1_501 desktop 1280": "492–1052 body 68 note 103",
          "bc1_501 desktop 1440": "572–1132 body 68 note 103",
          "bc1_501 phone 390": "0–390 body 51 note 69",
          "bc1_501 tablet 1024": "232–792 body 68 note 103",
          "bc1_501 tablet 900": "170–730 body 68 note 103",
          "klwa_1010 desktop 1280": "492–1052 body 61 note 103",
          "klwa_1010 desktop 1440": "572–1132 body 61 note 103",
          "klwa_1010 phone 390": "0–390 body 47 note 69",
          "klwa_1010 tablet 1024": "232–792 body 61 note 103",
          "klwa_1010 tablet 900": "170–730 body 61 note 103",
          "vedder_1010 desktop 1280": "492–1052 body 77 note 103",
          "vedder_1010 desktop 1440": "572–1132 body 77 note 103",
          "vedder_1010 phone 390": "0–390 body 54 note 69",
          "vedder_1010 tablet 1024": "232–792 body 77 note 103",
          "vedder_1010 tablet 900": "170–730 body 77 note 103",
        }
      `);
  }, 180_000);

  it("MUTATION: a reading column with no measure fails it (the lines run the width of the room)", () => {
    MUTANT.on = true;
    try {
      // (the line's words dropped from each problem: the count and where are the claim)
      expect(problems(measure()).map((p) => p.replace(/ characters: .*$/, " characters"))).toMatchInlineSnapshot(`
        [
          "vedder_1010 tablet 900: a line of 114 characters",
          "vedder_1010 tablet 1024: a line of 114 characters",
          "vedder_1010 desktop 1280: a line of 114 characters",
          "vedder_1010 desktop 1440: a line of 114 characters",
          "bc1_501 tablet 900: a line of 93 characters",
          "bc1_501 tablet 1024: a line of 93 characters",
          "bc1_501 desktop 1280: a line of 93 characters",
          "bc1_501 desktop 1440: a line of 93 characters",
        ]
      `);
    } finally { MUTANT.on = false; }
  }, 180_000);
});
