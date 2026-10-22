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

/** [table, key column, key, expected prefix of JSON.stringify(value), whole?] — T02's table, read from the built store. */
const ROWS: readonly (readonly [string, string, number | string, string, boolean])[] = [
  ["display_frame", "id", 64, '{"status":"own","closing":[],"strip":20}', true],
  ["licence_frame", "id", 1, "[1,1,0]", true],
  ["ladder_frame", "id", 1057, "[3973,[[0,0],[1,1],[2,2],[3,3],[4,3971],", false],
  // vedder_515's rows frame — its id is taps.json states.vedder_515.cell.rows, and moves with each delivery
  ["rows_frame", "id", 2002, '[["RB","CT","LMB","SMB","BCB","CRA","KO","LW","MW","WSG","ST","GB","DV","LT","EB"],{"RB":[3820,3821]', false],
  ["gear_frame", "id", 320, '{"counts":{"bait_possession_kg":{"by":[3322,0],"over":[],"cites":[3322]}', false],
  ["display_rule", "id", 529, '{"kind":"gate","title":"No fishing, Jun 1–30","closure":true,"plain":"No fishing, Jun 1–30.","book":[[22,24]]', false],
  ["rule", "rix", 529, '{"entry_id":"r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4","rule_id":"chilliwack_vedder', false],
  ["ladder_verdict", "id", 0, "[[3079,3333,3339,3355,3356,3359],[],[3351,3352],[],[],[[3364,1,3079]]]", true],
  ["rows_decided", "id", 0, '{"status":"keep","win":3083,"daily":20,"narrow":null,"lines":[]', false],
  ["rows_row", "id", 0, '{"kind":"keep","pool":3091,"win":null,"members":["EB","GB","CT","DV","LT","RB","ST"]', false],
  ["licence_hold", "id", 0, '{"holds":[131,96,98,106,110,111,128],"designations":[]', false],
  ["licence_answer", "id", 0, '{"documents":[{"doc":"basic_licence"', false],
  ["licence_doc", "id", 0, "[0,1,0,2,3,0,1,0,2,3,4,5", false],
  ["licence_u16", "id", 0, '[{"residency":"resident","label":"B.C. resident","none_needed":true', false],
  ["picker", "id", 0, '{"choices":[{"parts":[0],"closed":false,"sections":1', false],
  ["xpart", "id", 0, '{"ruleset":"1","licensing_set":"0","steelhead":"possible","steelhead_rules":true}', true],
  ["runs", "id", 0, '[{"from":"source","to":"mouth","km_from":1.46,"km_to":0}]', true],
  ["ruleset", "set_id", 393, '{"sections":5,"reach":[525,526,527,528,529,530,3077', false],
  ["static", "k", "licence.profile_dims", '[["residency",["resident","non_resident","non_resident_alien"]],["age",["16_plus","under_16"]]', false],
];

describe.skipIf(!hasProvinceRegs)("unpackBlob, the province store", () => {
  const db = store!;
  const bytes = (v: Cell) => new Uint8Array(v as Uint8Array);
  const dict = async (t: string) => {
    const r = await db.get("SELECT d FROM zdict WHERE tbl = ?", t);
    return r ? bytes(r.d) : null;
  };

  for (const [t, k, key, want, whole] of ROWS)
    it(`${t} ${k}=${key}`, async () => {
      const r = await db.get(`SELECT j FROM ${t} WHERE ${k} = ?`, key);
      const got = JSON.stringify(unpackBlob(bytes(r!.j), await dict(t)));
      if (whole) expect(got).toBe(want);
      else expect(got.startsWith(want), got.slice(0, want.length + 40)).toBe(true);
    });

  it("unpacks every display_frame and display_rule row", async () => {
    for (const t of ["display_frame", "display_rule"]) {
      const d = await dict(t);
      const rows = await db.all(`SELECT j FROM ${t}`);
      expect(rows.length).toBeGreaterThan(1000);
      for (const r of rows) unpackBlob(bytes(r.j), d);
    }
  });
});
