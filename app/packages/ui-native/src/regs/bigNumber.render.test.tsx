/**
 * (T884, owner 2026-11-18: "on Apple the part highlighted is cut off weird because of row sizes") EVERY BIG NUMBER SITS IN A
 * LINE BOX THAT HOLDS ITS DIGITS. A big number is a nested Text with no lineHeight of its own (LineText), so its line is
 * the outer Text's; iOS cuts what that box lacks off the top of the glyphs (T816, `type.ts` FACE_METRICS). Cultus Lake's
 * Black crappie sheet drew "20 a day" — a 26 px numeral — in `regsBodySemi`'s 20.25 px box, and every tier line ("5 a day",
 * "Up to 15") in 21.25 or 18.75.
 *
 * What a box must hold, on iOS: the digits' tops (0.677 em in Bricolage Grotesque 800: "2", "6", "8", "9"; read from the
 * font with fontTools, above its 0.66 cap height) plus the face's descent (FACE_METRICS, read from the file by
 * tools/type-linebox.test.ts). At 200 % text the outer line and the number scale by the same factor until the number's
 * own cap (CAPPED_SCALE, 2×) stops it, so the ratio holds at every size.
 *
 * Checked on what the app draws: every number in the display face (a nested big run, or a whole Text such as the trout
 * table's "Keep 2") on every frozen page's Keep tab and every one of its sheets, and on a line built for each base style a
 * big number is set in; the near-miss (a big number in an unraised box) must still fail.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import type { Line, RegsPage, SheetKey, SheetVM } from "@app/core";
import { LIGHT } from "../theme";
import { CAPPED_SCALE, FACE_METRICS, TYPE, type TextStyle } from "../type";
import { KeepTab } from "./KeepTab";
import { LineText, lineBox, NO_ACTIONS } from "./LineText";
import { SheetBody } from "./SheetBody";
import bc1_501 from "./fixtures/bc1_501.json";
import glenn_501 from "./fixtures/glenn_501.json";
import glenn_615 from "./fixtures/glenn_615.json";
import klwa_1010 from "./fixtures/klwa_1010.json";
import vedder_1010 from "./fixtures/vedder_1010.json";
import vedder_515 from "./fixtures/vedder_515.json";
import x35p_1010 from "./fixtures/x35p_1010.json";
import edge_x15p2_1010 from "./fixtures/edge_x15p2_1010.json";
import { TEST_TROUT_SOURCES } from "./testing/sources";

afterEach(cleanup);

/** the tallest digit's top, per em (Bricolage Grotesque 800: "2" "6" "8" "9" reach 0.677; the cap height is 0.66) */
const DIGIT_TOP = 0.677;
/** the line box a number of `size` px needs on iOS: its digits' tops over the baseline plus the face's descent below it */
const need = (size: number) => size * (DIGIT_TOP + FACE_METRICS.BricolageGrotesque.descent);

type Frozen = Omit<RegsPage, "sheet"> & { sheets?: Record<string, SheetVM | null> };
const PAGES = { bc1_501, glenn_501, glenn_615, klwa_1010, vedder_1010, vedder_515, x35p_1010, edge_x15p2_1010 } as unknown as Record<string, Frozen>;

const px = (v: string) => (v.endsWith("px") ? Number(v.slice(0, -2)) : NaN);
type El = HTMLElement;

/** the nearest value of an inline style property, on the element or above it (jsdom does not inherit) */
function up(el: El, prop: "fontSize" | "fontFamily" | "lineHeight"): string {
  for (let e: El | null = el; e; e = e.parentElement) if (e.style[prop]) return e.style[prop];
  return "";
}
/**
 * every number drawn in the display face under `root` — a nested big run (`n:big` …) or a whole Text whose own words hold a
 * digit (the trout table's "Keep 2", a salmon count) — with its size and the line box it sits in
 */
function bigNumbers(root: Element): { text: string; size: number; box: number }[] {
  return [...root.querySelectorAll<El>("*")]
    .filter((el) => el.dataset.testid?.startsWith("n:")
      || [...el.childNodes].some((n) => n.nodeType === 3 && /\d/.test(n.textContent ?? "")))
    .filter((el) => up(el, "fontFamily").includes("BricolageGrotesque"))
    .map((el) => ({ text: el.textContent ?? "", size: px(up(el, "fontSize")), box: px(up(el, "lineHeight")) }));
}
const short = (hits: ReturnType<typeof bigNumbers>) =>
  hits.filter((h) => !(h.box >= need(h.size))).map((h) => `"${h.text}" ${h.size} px in a ${h.box} px box (needs ${need(h.size).toFixed(2)})`);

describe("the box a big number needs fits inside line-height 1", () => {
  it("the digits' top plus the descent is under 1 em, so `lineBox`'s floor (the number's size) holds them", () => {
    expect(DIGIT_TOP + FACE_METRICS.BricolageGrotesque.descent).toBeLessThan(1);
    expect(FACE_METRICS.BricolageGrotesque.cap).toBeLessThan(DIGIT_TOP);
  });
});

describe("every big number on the frozen pages sits in a box that holds it", () => {
  let seen = 0, onKeep = 0;
  for (const [name, page] of Object.entries(PAGES)) {
    it(`${name}: the Keep tab`, () => {
      const { container } = render(<KeepTab troutSources={TEST_TROUT_SOURCES} keep={page.keep} dateLabel={page.header.date} palette={LIGHT} actions={NO_ACTIONS} />);
      const hits = bigNumbers(container);
      seen += hits.length; onKeep += hits.length;
      expect(short(hits)).toEqual([]);
    });
    it(`${name}: every sheet`, () => {
      const bad: string[] = [];
      for (const [key, vm] of Object.entries(page.sheets ?? {})) {
        if (!vm) continue;
        const { container, unmount } = render(<SheetBody vm={vm} palette={LIGHT} onOpen={(_k: SheetKey) => {}} />);
        const hits = bigNumbers(container);
        seen += hits.length;
        bad.push(...short(hits).map((s) => `${key}: ${s}`));
        unmount();
      }
      expect(bad).toEqual([]);
    });
  }
  it("the pages drew numbers to check, the Keep tabs too (not a pass on nothing)", () => {
    expect(seen).toBeGreaterThan(20);
    expect(onKeep).toBeGreaterThan(0);
  });
});

describe("each base style a big number is set in", () => {
  const LINE: Line = [{ t: "num", s: "20", size: "big" }, { t: "text", s: " a day" }];
  const INLINE: Line = [{ t: "num", s: "20", size: "inline" }, { t: "text", s: " a day" }];
  const BASES: [string, TextStyle][] = Object.entries({
    regsBodySemi: TYPE.regsBodySemi, regsBody: TYPE.regsBody, tier: TYPE.tier, tierSmall: TYPE.tierSmall,
    duty: TYPE.duty, meta: TYPE.meta, metaSemi: TYPE.metaSemi, metaBold: TYPE.metaBold,
  });
  for (const [k, base] of BASES) {
    it(`${k} (${base.lineHeight} px) with TYPE.numeral, and an inline number set big`, () => {
      const { container } = render(<>
        <LineText line={LINE} style={base} color={LIGHT.ink} palette={LIGHT} actions={NO_ACTIONS} />
        <LineText line={INLINE} style={base} color={LIGHT.ink} palette={LIGHT} actions={NO_ACTIONS} inlineBig />
      </>);
      const hits = bigNumbers(container);
      expect(hits).toHaveLength(2);
      expect(short(hits)).toEqual([]);
    });
  }
  it("a line with no big number keeps its own box; an inline number not set big changes nothing", () => {
    expect(lineBox([{ t: "text", s: "Release" }], TYPE.regsBodySemi, TYPE.numeral, false)).toBe(TYPE.regsBodySemi.lineHeight);
    expect(lineBox(INLINE, TYPE.regsBodySemi, TYPE.numeral, false)).toBe(TYPE.regsBodySemi.lineHeight);
    expect(lineBox(INLINE, TYPE.regsBodySemi, TYPE.numeral, true)).toBe(TYPE.numeral.fontSize);
    expect(lineBox(LINE, TYPE.lead, TYPE.numeralMid, false)).toBe(TYPE.lead.lineHeight);   // already taller
  });
  it("at every text size the drawn line holds the number and is no taller than it or the line's own (iOS's largest, 3.1×, too)", () => {
    const drawn = (base: TextStyle, s: number) => s * lineBox(LINE, base, TYPE.numeral, false, s)!;   // the platform scales the box
    for (const [, base] of BASES) {
      for (const s of [1, 1.35, 1.79, 2, 2.14, 2.64, 3.1, 3.57]) {
        const number = TYPE.numeral.fontSize * Math.min(s, CAPPED_SCALE);                          // the number stops at 2×
        expect(drawn(base, s)).toBeGreaterThanOrEqual(need(number));
        expect(drawn(base, s)).toBeCloseTo(Math.max(s * base.lineHeight!, number), 6);
      }
    }
    // the review's case: regsBodySemi at 3.1× draws its own 62.8 px line, not 26 × 3.1 = 80.6
    expect(drawn(TYPE.regsBodySemi, 3.1)).toBeCloseTo(62.775, 3);
  });
  it("the near-miss fails: a 26 px number in regsBodySemi's own 20.25 px box (the build the owner saw)", () => {
    expect(short([{ text: "20", size: TYPE.numeral.fontSize, box: TYPE.regsBodySemi.lineHeight }])).toHaveLength(1);
  });
});
