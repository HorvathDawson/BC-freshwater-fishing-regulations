/**
 * T396 — the regulations key in two rows in both fishery modes, so the bottom bar stands the same height in Trout & more
 * and in Salmon, and Trout & more fits those two rows at a 390 dp phone (owner 2026-11-05: it wrapped to three).
 *
 * jsdom lays nothing out, so the fit is checked against the font itself: every row's width is its entries' swatches,
 * gaps and labels, the labels measured with Archivo's own advance widths (the face `TYPE.small` names) at `LEGEND_TEXT`.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, render, screen } from "@testing-library/react";
import { MapLegend } from "./shell/MapLegend";
import { LEGEND_ROWS } from "./Chrome";
import { LEGEND_TEXT } from "./type";
import { LIGHT } from "./theme";

afterEach(cleanup);

/** Archivo Regular's advance width of each character, from its `cmap` (format 4) and `hmtx` tables. */
function advanceWidths(path: string): (text: string, size: number) => number {
  const b = readFileSync(path);
  const tables = new Map<string, number>();
  for (let i = 0, n = b.readUInt16BE(4); i < n; i++)
    tables.set(b.toString("latin1", 12 + 16 * i, 16 + 16 * i), b.readUInt32BE(12 + 16 * i + 8));
  const upm = b.readUInt16BE(tables.get("head")! + 18);
  const metrics = b.readUInt16BE(tables.get("hhea")! + 34);
  const hmtx = tables.get("hmtx")!;
  const advance = (g: number) => b.readUInt16BE(hmtx + 4 * Math.min(g, metrics - 1));
  const cmap = tables.get("cmap")!;
  let sub = -1;
  for (let i = 0, n = b.readUInt16BE(cmap + 2); i < n; i++) {
    const at = cmap + 4 + 8 * i;
    if (b.readUInt16BE(at) === 3 && b.readUInt16BE(at + 2) === 1) sub = cmap + b.readUInt32BE(at + 4);
  }
  if (sub < 0 || b.readUInt16BE(sub) !== 4) throw new Error("no Windows Unicode BMP cmap");
  const segs = b.readUInt16BE(sub + 6) / 2;
  const ends = sub + 14, starts = ends + 2 * segs + 2, deltas = starts + 2 * segs, offsets = deltas + 2 * segs;
  const glyph = (c: number) => {
    for (let i = 0; i < segs; i++) {
      if (b.readUInt16BE(ends + 2 * i) < c) continue;
      const start = b.readUInt16BE(starts + 2 * i);
      if (start > c) return 0;
      const delta = b.readInt16BE(deltas + 2 * i), ro = b.readUInt16BE(offsets + 2 * i);
      if (ro === 0) return (c + delta) & 0xffff;
      const g = b.readUInt16BE(offsets + 2 * i + ro + 2 * (c - start));
      return g === 0 ? 0 : (g + delta) & 0xffff;
    }
    return 0;
  };
  return (text, size) => [...text].reduce((w, ch) => w + advance(glyph(ch.codePointAt(0)!)), 0) * size / upm;
}
// the face the phone loads (apps/mobile's expo-font dependency; node_modules is hoisted), read as a file
// (vitest runs from app/; under jsdom `import.meta.url` is not a file URL)
const measure = advanceWidths(resolve(process.cwd(), "node_modules/@expo-google-fonts/archivo/400Regular/Archivo_400Regular.ttf"));

const col = (regsLayer: "standard" | "salmon") => ({
  flowParam: "discharge", horizon: 0, mm: { onConditions: false }, cond: { marks: [] }, gearMarked: true, regsLayer,
  salmonOffered: true, statusOk: true, salmonFacts: null, setRegsLayer: () => {},
  layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles: false } } },
}) as never;

/** Each row's width at the phone: per entry its swatch, the gap and its label; the column gap between entries. */
const rowWidths = () => screen.getAllByTestId("legend-row").map((row) => {
  const entries = [...row.children] as HTMLElement[];
  const widths = entries.map((e) => {
    const [swatch, label] = [...e.children] as HTMLElement[];
    return parseFloat(getComputedStyle(swatch!).width) + LEGEND_ROWS.gap + measure(label!.textContent!, LEGEND_TEXT);
  });
  return widths.reduce((a, w) => a + w, 0) + LEGEND_ROWS.column * (entries.length - 1);
});

/** a 390 dp phone, less the strip's side padding */
const ROOM = 390 - 2 * LEGEND_ROWS.padX;

describe("(T396) the regulations key: two rows in both modes, Trout & more fitting a 390 dp phone", () => {
  it("Trout & more and Salmon each lay their key in exactly two rows, with every entry kept", () => {
    for (const mode of ["standard", "salmon"] as const) {
      render(<MapLegend col={col(mode)} conditions={false} palette={LIGHT} theme="light" mapDay={null} />);
      expect(screen.getAllByTestId("legend-row").length, mode).toBe(2);
      expect(screen.getAllByTestId("legend-entry").length, mode).toBe(mode === "standard" ? 7 : 6);
      // the head is one row (no wrap): Salmon's checked line sits beside the switch, never under it
      expect(getComputedStyle(screen.getByTestId("legend-fishery-head")).flexWrap).not.toBe("wrap");
      cleanup();
    }
  });

  it("every row fits 390 dp in Archivo at the legend size — also with a day's date on Closed", () => {
    for (const [mode, day] of [["standard", null], ["standard", new Date("2026-09-30T12:00:00")], ["salmon", null]] as const) {
      render(<MapLegend col={col(mode)} conditions={false} palette={LIGHT} theme="light" mapDay={day} />);
      for (const w of rowWidths()) expect(w, `${mode} ${day ? "dated" : ""}`).toBeLessThanOrEqual(ROOM);
      cleanup();
    }
  });
});
