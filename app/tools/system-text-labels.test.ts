// @vitest-environment jsdom
/**
 * T1201 — THE FIXED-SIZE LABELS AT A LARGER SYSTEM TEXT SIZE: the map rail's Trout / Salmon switch ("SALMON" wrapped to
 * "SALMO" / "N") and the bottom bar's four tabs ("CONDITIONS" cut), laid out in Chrome at 320, 360 and 412 wide and at the OS
 * text sizes 1, 1.15 and 1.3. Same method as `regs-text-scale.test.ts`: this file's `react-native` Text does what a phone
 * does (size and line height × min(the OS scale, the Text's own cap)), react-native-web's DOM is laid out by headless
 * Chrome (`wide-measure.mjs`). One rule (type.ts header, "labels in fixed-size controls"): such a label is one line, whole,
 * at every scale; a control grows before its label shrinks.
 *
 * T1201_SHOTS=<dir>: also save each laid-out strip as a PNG there (pictures for the owner, not part of the gate).
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
const { LIGHT } = await import("../packages/ui-native/src/theme");
const { TabBar } = await import("../packages/ui-native/src/TabBar");
const { MapRail } = await import("../packages/ui-native/src/MapRail");
const { RegsLayerToggle } = await import("../packages/ui-native/src/shell/RegsLayerToggle");
const { TABS } = await import("../packages/ui-native/src/TabBar");
const { SALMON_WORDS } = await import("@app/core");

/** every text-holding leaf: its words, how many lines it drew, whether it is cut (ellipsis or hidden overflow), its width and its cell's */
const MEASURE = `(() => {
  const out = [];
  for (const e of document.querySelectorAll('body *')) {
    if (e.children.length || !(e.textContent ?? '').trim()) continue;
    const s = getComputedStyle(e), r = e.getBoundingClientRect(), lh = parseFloat(s.lineHeight) || r.height;
    let cell = e.parentElement;
    const cr = cell.getBoundingClientRect();
    out.push({ t: e.textContent.trim(), lines: Math.round(r.height / lh), cut: e.scrollWidth > e.clientWidth + 0.5,
               w: Math.round(r.width * 10) / 10, cellW: Math.round((cr.width - parseFloat(getComputedStyle(cell).paddingLeft) - parseFloat(getComputedStyle(cell).paddingRight)) * 10) / 10, size: parseFloat(s.fontSize) });
  }
  return JSON.stringify({ labels: out });
})()`;

const dir = mkdtempSync(join(tmpdir(), "t1201-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
writeFileSync(join(dir, "measure.js"), MEASURE);

type Label = { t: string; lines: number; cut: boolean; w: number; cellW: number; size: number };
const SCALES = [1, 1.15, 1.3] as const;
const WIDTHS = [320, 360, 412] as const;

function laidOut(which: "tabs" | "switch" | "rail"): Record<string, Label[]> {
  const jobs: { page: string; size: [number, number]; measure: string; shot?: string }[] = [];
  const keys: string[] = [];
  for (const scale of SCALES) for (const w of WIDTHS) {
    SCALE.v = scale;
    const col = { regsLayer: "standard" as const, setRegsLayer() {} };
    render(which === "tabs"
      ? h(View, { style: { flex: 1, justifyContent: "flex-end" } }, h(TabBar, { active: "map", onChange() {}, palette: LIGHT }))
      : which === "rail"
        // the whole rail as the map draws it (locate, layers, the switch under them), for the picture only
        ? h(View, { style: { flex: 1 } }, h(MapRail, { palette: LIGHT, onLayers() {}, me: { on: false, following: false, busy: false, notice: null, press() {}, dismiss() {} } },
                                           h(RegsLayerToggle, { col, palette: LIGHT })))
        : h(View, { style: { flex: 1, alignItems: "flex-end", padding: 10 } }, h(RegsLayerToggle, { col, palette: LIGHT })));
    const file = join(dir, `${which}-${w}-${scale}.html`);
    writeFileSync(file, snapshot());
    cleanup();
    const shot = process.env.T1201_SHOTS ? { shot: join(process.env.T1201_SHOTS, `${which}-${w}-${scale}.png`) } : {};
    jobs.push({ page: file, size: [w, which === "tabs" ? 120 : which === "rail" ? 320 : 160], measure: join(dir, "measure.js"), ...shot });
    keys.push(`${w} wide, text ${scale}x`);
  }
  SCALE.v = 1;
  const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "wide-measure.mjs"), JSON.stringify({ jobs })],
                           { encoding: "utf8", timeout: 120_000 });
  const res = JSON.parse(out) as { labels: Label[] }[];
  return Object.fromEntries(keys.map((k, i) => [k, res[i]!.labels]));
}

/** the faults in words; [] is clean */
function faults(m: Record<string, Label[]>, words: readonly string[]): string[] {
  const out: string[] = [];
  for (const [k, labels] of Object.entries(m)) {
    for (const word of words) {
      const l = labels.find((x) => x.t.toLowerCase() === word.toLowerCase());
      if (!l) { out.push(`${k}: "${word}" not drawn`); continue; }
      if (l.lines !== 1) out.push(`${k}: "${word}" drawn on ${l.lines} lines`);
      if (l.cut) out.push(`${k}: "${word}" is cut`);
      // a word that cannot break is drawn past its cell on the web; a phone breaks it mid-word ("SALMO" / "N")
      if (l.w > l.cellW + 0.5) out.push(`${k}: "${word}" is ${l.w} wide in a ${l.cellW} cell`);
    }
  }
  return out;
}

describe("T1201 fixed-size labels at a larger system text size, laid out in Chrome", () => {
  it("the bottom bar: every tab's label is one line and whole at 1, 1.15 and 1.3, 360 and 412 wide", () => {
    const m = laidOut("tabs");
    if (process.env.T1201_DEBUG) console.log(JSON.stringify(m));
    expect(faults(m, TABS.map((t) => t.t))).toEqual([]);
    // 0.9.0's look at the default size: at 412 wide the five tabs share the bar equally (the label's parent is the tab)
    const widths = TABS.map((t) => m["412 wide, text 1x"]!.find((l) => l.t === t.t)!.cellW);
    expect(new Set(widths).size, `the tabs at 412, 1x: ${widths}`).toBe(1);
  }, 120_000);

  it("the map rail's Trout / Salmon switch: both labels are one line and whole at 1, 1.15 and 1.3", () => {
    const m = laidOut("switch");
    if (process.env.T1201_DEBUG) console.log(JSON.stringify(m));
    expect(faults(m, [SALMON_WORDS.toggle.rail.standard, SALMON_WORDS.toggle.rail.salmon])).toEqual([]);
    // 0.9.0's look at the default size: the cell is exactly the rail's 44 (it grows only for a larger text size)
    for (const w of WIDTHS) expect(m[`${w} wide, text 1x`]!.map((l) => l.cellW), `${w} wide, 1x`).toEqual([44, 44]);
  }, 120_000);
});

describe("T1201 pictures: the whole map rail (set T1201_SHOTS)", () => {
  it.skipIf(!process.env.T1201_SHOTS)("lays the rail out at 1, 1.15 and 1.3", () => { laidOut("rail"); });
});
