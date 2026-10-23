/**
 * T02: the regs store's blob codec against the Python reference (`pipeline/deliver/store/common.py` `unpack`).
 * Every byte vector below was produced or checked by Python: the 0x02 one by `common.pack(raw, "zdict", zd)`, the 0x01
 * one decoded by `common.unpack` to the same JSON. The province-store half reads the built store when it is present.
 */
import { describe, expect, it } from "vitest";
import { openBundle } from "../bundle/drivers/node";
import type { Cell, Db } from "../bundle/db";
import { inflateBlob, RegsBlobError, unpackBlob, utf8 } from "./blob";

const hex = (h: string) => new Uint8Array(h.match(/../g)!.map((b) => parseInt(b, 16)));
const enc = (s: string) => new TextEncoder().encode(s);

describe("unpackBlob, store-free", () => {
  it("reads a raw JSON blob as is", () => {
    expect(unpackBlob(enc('{"a":1}'), null)).toEqual({ a: 1 });
  });

  it("inflates a 0x01 blob (raw DEFLATE, no dictionary)", () => {
    expect(unpackBlob(hex("01ab562a2e492c292d56b252ca2fcf53d2514acec92fcecc4b57b28a8ed501ca15651628591919d40200"), null))
      .toEqual({ status: "own", closing: [], strip: 20 });
  });

  it("inflates a 0x02 blob with its table's dictionary, as Python's zlib.decompressobj(-15, zdict) does", () => {
    const zd = hex("7b22737461747573223a226f776e222c22636c6f73696e67223a5b5d2c227374726970223a7b22737461747573223a22636c6f736564222c22636c6f73696e67223a5b5b333037392c5b225242222c224354225d5d5d2c22756e6c6573735f6f70656e6564223a6e756c6c2c227374726970223a317d");
    const b = hex("0223cd401d259730224c35af0500");
    expect(utf8(inflateBlob(b, zd))).toBe('{"status":"closed","closing":[[3079,["RB","CT","DV"]]],"unless_opened":null,"strip":17}');
    expect(unpackBlob(b, zd)).toEqual({ status: "closed", closing: [[3079, ["RB", "CT", "DV"]]], unless_opened: null, strip: 17 });
  });

  it("returns a raw blob's bytes untouched from inflateBlob", () => {
    const b = enc("[1,1,0]");
    expect(inflateBlob(b, null)).toBe(b);
  });

  it("decodes 2-, 3- and 4-byte UTF-8, the last as a surrogate pair", () => {
    expect(unpackBlob(hex("5b2252697669c3a8726520e2809320e2809c6f6be2809d20f09f909f225d"), null))
      .toEqual(["Rivière – “ok” 🐟"]);
  });

  it("decodes 200,000 bytes in chunks (no call-stack blow-up)", () => {
    const b = enc("é".repeat(100_000));
    expect(b.length).toBe(200_000);
    const s = utf8(b);
    expect(s.length).toBe(100_000);
    expect(s).toBe("é".repeat(100_000));
  });

  it("refuses a dictionary blob with no dictionary, an empty blob, and bytes that are not JSON", () => {
    expect(() => unpackBlob(new Uint8Array([2, 0x03, 0x00]), null)).toThrow(RegsBlobError);
    expect(() => unpackBlob(new Uint8Array([2, 0x03, 0x00]), null))
      .toThrow("regs store: a dictionary-deflated blob in a table with no dictionary");
    expect(() => unpackBlob(new Uint8Array([]), null)).toThrow("regs store: an empty blob");
    expect(() => unpackBlob(new Uint8Array([0x78]), null)).toThrow(RegsBlobError);
    expect(() => unpackBlob(new Uint8Array([0x78]), null)).toThrow("regs store: a blob is not JSON");
  });
});

/** The built province store (T03 replaces this literal with DEV_REGS / hasProvinceRegs). Absent in a fresh worktree. */
const REGS = decodeURIComponent(new URL("../../../../../data/generated/bundle/regs.sqlite", import.meta.url).pathname);
/** Opened once if it is there (layers.json keeps node:fs out of packages/data; opening is the existence test). */
const store: Db | null = (() => {
  try { return openBundle(REGS); } catch { return null; }
})();
const hasProvinceRegs = store !== null;

describe.skipIf(!hasProvinceRegs)("unpackBlob, the province store", () => {
  const db = store!;
  const bytes = (v: Cell) => new Uint8Array(v as Uint8Array);
  const dict = async (t: string) => {
    const r = await db.get("SELECT d FROM zdict WHERE tbl = ?", t);
    return r ? bytes(r.d) : null;
  };

  // Each row decoded as the Python reference decodes it (by the golden taps' ids, not literals) is
  // conformance/src/regs/blob.release.test.ts — this package may not run Python or name node:zlib.
  it("unpacks every display_frame and display_rule row", async () => {
    for (const t of ["display_frame", "display_rule"]) {
      const d = await dict(t);
      const rows = await db.all(`SELECT j FROM ${t}`);
      expect(rows.length).toBeGreaterThan(1000);
      for (const r of rows) unpackBlob(bytes(r.j), d);
    }
  });
});
