// @vitest-environment jsdom
/**
 * T1207 — THE ONE STATUS BLOCK'S NAME ("Salmon and game fish", tagged "B.C. Synopsis") AT A LARGER SYSTEM TEXT SIZE, the
 * T1201 rule: the title (reading text) follows the OS size in full and wraps whole; the tag (a badge) is one line, whole, never
 * cut, and nothing runs past the screen. Laid out in Chrome at 320, 360 and 412 wide, text 1, 1.15 and 1.3, light and dark
 * (same method as `system-text-labels.test.ts`).
 */
import "./render-setup.ts";
import { createElement as h, forwardRef } from "react";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { cleanup, render } from "@testing-library/react";

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
const { LIGHT, DARK } = await import("../packages/ui-native/src/theme");
const { TaggedTitle } = await import("../packages/ui-native/src/regs/SourceTag");
const { SOURCE_WORDS } = await import("@app/core");


const dir = mkdtempSync(join(tmpdir(), "t1207-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
const MEASURE = `(() => JSON.stringify({ labels: [...document.querySelectorAll('body *')].filter((e) => !e.children.length && (e.textContent ?? '').trim()).map((e) => {
  const s = getComputedStyle(e), r = e.getBoundingClientRect(), lh = parseFloat(s.lineHeight) || r.height;
  return { t: e.textContent.trim(), lines: Math.round(r.height / lh), cut: e.scrollWidth > e.clientWidth + 0.5, right: Math.round(r.right * 10) / 10, vw: innerWidth };
}) }))()`;
writeFileSync(join(dir, "measure.js"), MEASURE);
type Label = { t: string; lines: number; cut: boolean; right: number; vw: number };

describe("T1207 the one block's title at a larger system text size, laid out in Chrome", () => {
  for (const [theme, palette] of [["light", LIGHT], ["dark", DARK]] as const) {
    it(`${theme}: "${SOURCE_WORDS.both}" and its tag stay whole and on screen at 320, 360 and 412 wide, text 1, 1.15 and 1.3`, () => {
      const jobs: { page: string; size: [number, number]; measure: string }[] = [], keys: string[] = [];
      for (const scale of [1, 1.15, 1.3]) for (const w of [320, 360, 412]) {
        SCALE.v = scale;
        render(h(View, { style: { flex: 1, padding: 16 } }, h(TaggedTitle, { text: SOURCE_WORDS.both, tag: SOURCE_WORDS.salmonTag.book, palette, testID: "t" })));
        const file = join(dir, `${theme}-${w}-${scale}.html`);
        writeFileSync(file, snapshot());
        cleanup();
        jobs.push({ page: file, size: [w, 140], measure: join(dir, "measure.js") });
        keys.push(`${w} wide, text ${scale}x`);
      }
      SCALE.v = 1;
      const res = JSON.parse(execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                                          { encoding: "utf8", timeout: 120_000 })) as { labels: Label[] }[];
      const faults: string[] = [];
      keys.forEach((k, i) => {
        const ls = res[i]!.labels;
        // the title may wrap (whole words, never cut); the tag is one line; neither runs past the screen
        for (const [what, words] of [["title", SOURCE_WORDS.both], ["tag", SOURCE_WORDS.salmonTag.book]] as const) {
          const l = ls.find((x) => x.t.toLowerCase() === words.toLowerCase());
          if (!l) { faults.push(`${k}: ${what} "${words}" not drawn`); continue; }
          if (l.cut) faults.push(`${k}: ${what} is cut`);
          if (l.right > l.vw + 0.5) faults.push(`${k}: ${what} runs to ${l.right} of ${l.vw}`);
          if (what === "tag" && l.lines !== 1) faults.push(`${k}: the tag is on ${l.lines} lines`);
        }
      });
      expect(faults).toEqual([]);
    }, 120_000);
  }
});
