/**
 * (T369) PARITY: the `lines` block the pipeline writes (pipeline/deliver/release/lines.py) is the block
 * `encodeLineBlock` (`@app/data` regs/line.ts) writes, byte for byte — one fixture both sides read (dev/section-line-parity.json;
 * pipeline/tests/test_section_line.py holds the Python writer to the same hex).
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { decodeLineBlock, encodeLineBlock, LINE_OPEN, lineMid, slotAt } from "@app/data/regs/place";

const fx = JSON.parse(readFileSync(join(__dirname, "../packages/data/dev/section-line-parity.json"), "utf8")) as
  { lines: [number, number, number][]; hex: string };
const lines = fx.lines.map(([blk, from, to]) => ({ blk, from, to }));
const hex = (b: Uint8Array) => Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
const bytes = (h: string) => Uint8Array.from(h.match(/../g)!.map((x) => parseInt(x, 16)));

describe("section_line parity with the pipeline writer (T369)", () => {
  it("encodeLineBlock writes the pipeline's bytes", () => {
    expect(hex(encodeLineBlock(lines))).toBe(fx.hex);
  });
  it("the pipeline's block reads back to the fixture's lines", () => {
    const b = decodeLineBlock(bytes(fx.hex), lines.length);
    expect(lines.map((_, i) => [b.blk[i], b.from[i], b.to[i]])).toEqual(fx.lines);
  });
  it("an open section (its stream's only one) is found by its key; its place is its start", () => {
    const b = decodeLineBlock(bytes(fx.hex), lines.length);
    const i = lines.findIndex((l) => l.blk === 356364590);
    expect([b.to[i], slotAt(b, 356364590, 250), slotAt(b, 356364590, 1e7), slotAt(b, 356364590, 249)])
      .toEqual([LINE_OPEN, i, i, -1]);
    expect(lineMid(lines[i]!)).toEqual({ blk: 356364590, m: 250 });
    // the gap between two pieces (a lake) holds no section
    expect(slotAt(b, 356364584, 7000)).toBe(-1);
  });
});
