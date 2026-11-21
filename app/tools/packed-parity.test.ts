/**
 * (T686) PARITY: the three packed release tables the pipeline writes (pipeline/deliver/release/packed.py) are the bytes
 * `@app/data` release/packed.ts writes and reads — one fixture both sides read (packages/data/dev/packed-parity.json;
 * pipeline/tests/test_release_packed.py holds the Python codec to the same hex). The codec round trip AGENTS 58 asks of a
 * new shape, and the decoders' refusals (W46).
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { CLIMATE_FORMAT, clim3, decodeDownBlock, decodeNear, DOWN_FORMAT, downSlot, encodeDownBlock, encodeNear,
         invertNear, NEAR_FORMAT, unclim3, type DownRow } from "../packages/data/src/release/packed";
import { PACK_FILES } from "../packages/data/src/pack.generated";
import { pentadsOf } from "../packages/data/src/sql/shared";

const fx = JSON.parse(readFileSync(join(__dirname, "../packages/data/dev/packed-parity.json"), "utf8")) as {
  near: { rows: [number, number][]; hex: string; sorted: [number, number][] };
  down: { block: number; rows: [number, number, number | null, 1 | null][]; hex: string };
  clim: { clim2: string; clim3: string };
};
const hex = (b: Uint8Array) => Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
const bytes = (h: string) => Uint8Array.from(h.match(/../g) ?? [], (x) => parseInt(x, 16));

describe("(T975) the decoders' formats are the contract's, the only ones read", () => {
  it("near, down and climate: the release contract names exactly the packed formats these decode", () => {
    expect([PACK_FILES.near!.format, PACK_FILES.down!.format, PACK_FILES.climate!.format])
      .toEqual([NEAR_FORMAT, DOWN_FORMAT, CLIMATE_FORMAT]);
  });
});

describe("near/2 parity with the pipeline writer (T686)", () => {
  it("encodeNear writes the pipeline's bytes; the pipeline's blob reads back in (ckm, ord) order", () => {
    expect(hex(encodeNear(fx.near.rows))).toBe(fx.near.hex);
    const t = decodeNear(bytes(fx.near.hex));
    expect(Array.from(t.ord, (o, i) => [t.ckm[i], o])).toEqual(fx.near.sorted);
  });
  it("inverted, each water lists its towns (place ascending) and nothing else", () => {
    const ix = invertNear([{ place_id: 9, b: encodeNear([[5, 1], [7, 2]]) }, { place_id: 4, b: encodeNear([[3, 2]]) }]);
    const of = (o: number) => { const [a, z] = ix.range(o); return Array.from({ length: z - a }, (_, k) => [ix.place[a + k], ix.ckm[a + k]]); };
    expect([of(1), of(2), of(0), of(3), of(-1)]).toEqual([[[9, 5]], [[4, 3], [9, 7]], [], [], []]);
  });
  it("refuses a blob that does not parse", () => {
    const b = bytes(fx.near.hex);
    for (const bad of [b.subarray(0, b.length - 1), Uint8Array.from([...b, 0]), Uint8Array.from([0x80]),
                       Uint8Array.from([1, 0, 0xff, 0xff])])
      expect(() => decodeNear(bad)).toThrow(/near\/2/);
  });
});

describe("down/2 parity with the pipeline writer (T686)", () => {
  const rows = fx.down.rows as unknown as DownRow[];
  it("encodeDownBlock writes the pipeline's bytes; the block reads back row for row (NULLs kept)", () => {
    expect(hex(encodeDownBlock(fx.down.block, rows))).toBe(fx.down.hex);
    const b = decodeDownBlock(fx.down.block, bytes(fx.down.hex));
    expect(Array.from(b.sid, (s, i) => [s, b.down[i], b.len[i], b.lake[i] === 1 ? 1 : null])).toEqual(fx.down.rows);
  });
  it("downSlot finds each sid's row and no other", () => {
    const b = decodeDownBlock(fx.down.block, bytes(fx.down.hex));
    expect(rows.map(([s]) => downSlot(b, s))).toEqual(rows.map((_, i) => i));
    expect([downSlot(b, 8193), downSlot(b, 12288), downSlot(b, 0)]).toEqual([-1, -1, -1]);
  });
  it("refuses a blob that does not parse", () => {
    const b = bytes(fx.down.hex);
    expect(() => decodeDownBlock(fx.down.block, b.subarray(0, b.length - 1))).toThrow(/down\/2/);
    expect(() => decodeDownBlock(fx.down.block, Uint8Array.from([...b, 0]))).toThrow(/bytes after/);
    expect(() => decodeDownBlock(0, Uint8Array.from([1, 0x80, 0x20, 0, 0, 0]))).toThrow(/past the block/);
  });
});

describe("clim/3 parity with the pipeline writer (T686)", () => {
  it("clim3 writes the pipeline's bytes; unclim3 gives back the clim/2 blob byte for byte", () => {
    expect(hex(clim3(bytes(fx.clim.clim2)))).toBe(fx.clim.clim3);
    expect(hex(unclim3(bytes(fx.clim.clim3)))).toBe(fx.clim.clim2);
  });
  it("pentadsOf reads clim/3 to the same bands as its clim/2", () => {
    expect(pentadsOf(bytes(fx.clim.clim3), "clim/3")).toEqual(pentadsOf(bytes(fx.clim.clim2), "clim/2"));
    expect(pentadsOf(bytes(fx.clim.clim3), "clim/3")[1]).toEqual([1.25, 2.5, 2, 39.5, 65504]);
  });
  it("refuses a blob that does not parse (never a shorter envelope)", () => {
    const b = bytes(fx.clim.clim3);
    for (const bad of [b.subarray(0, b.length - 1), Uint8Array.from([...b, 0]), new Uint8Array()])
      expect(() => unclim3(bad)).toThrow(/clim\/3/);
    expect(() => pentadsOf(b.subarray(0, b.length - 1), "clim/3")).toThrow(/clim\/3/);
  });
});
