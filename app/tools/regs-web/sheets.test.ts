/** T82 O5 — the `regs-heights.mjs --sheets` summary: medians of app − prototype, the taller count, caps, missing rows. */
import { describe, expect, it } from "vitest";
// @ts-expect-error — a plain .mjs module of the node tools (no declaration file)
import { sheetSummary } from "./sheets.mjs";

const m = (sheet: number, chrome: number, body: number, scrolls = false) => ({ sheet, chrome, body, scrolls, app: 844 });

describe("sheetSummary", () => {
  it("whole value over three measured rows and one the prototype cannot open", () => {
    const rows = [
      { id: "v", sheet: "sp:RB", proto: m(459, 67, 392), app: m(446, 92, 354) },
      { id: "v", sheet: "rule:a", proto: m(322, 95, 227), app: m(345, 143, 202) },
      { id: "v", sheet: "srcs:ways", proto: m(743, 67, 979, true), app: m(666, 92, 722, true) },
      { id: "v", sheet: "licsrc:lic|v|u16_res", proto: null, app: m(108, 92, 16) },
    ];
    expect(sheetSummary(rows)).toEqual({
      sheets: 4, measured: 3, missing: ["v licsrc:lic|v|u16_res (prototype)"],
      median: { sheet: -13, chrome: 25, body: -38 },
      taller: 1, capped: { proto: 1, app: 1 }, maxSheet: { proto: 743, app: 666 },
    });
  });
});
