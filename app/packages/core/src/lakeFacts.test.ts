import { describe, expect, it } from "vitest";
import { dayNumber, decodeLakeFacts, LakeFactsError, STOCK_RECENCY, stockRecency } from "./lakeFacts";

/** The bytes `pipeline/tests/test_lake_facts.py` pins for the same three sections — one layout, two languages. */
const RAW = Uint8Array.from([
  ...[0x42, 0x43, 0x4c, 0x4b], 1, ...[0xd7, 0x68, 0xd6, 0xe1, 0x35, 0xe6, 0x2b, 0x99],
  0x9f, 0x9f, 0x01,               // asof + 1 = 20383
  3,                              // three sections
  3, 1, 0x82, 0x3a,               // sid 3: sheet, stocked day 7425
  4, 0, 0x9e, 0x9f, 0x01,         // sid 7: no survey, stocked day 20381
  0xa5, 0x02, 2, 0,               // sid 300: digitised, never stocked
]);

describe("lake facts (lakes/1)", () => {
  it("decodes the pipeline's bytes: survey and last-stocked day per section, nothing for an unlisted one", () => {
    const f = decodeLakeFacts(RAW, "d768d6e135e62b99");
    expect({ handles: f.handles, asof: f.asof, sections: f.sections }).toEqual(
      { handles: "d768d6e135e62b99", asof: 20382, sections: 3 });
    expect([3, 7, 300, 4].map((s) => [f.survey(s), f.stockedDay(s)])).toEqual(
      [["sheet", 7425], [null, 20381], ["digitised", null], [null, null]]);
  });

  it("refuses another atlas's file, a foreign file and a truncated one", () => {
    expect(() => decodeLakeFacts(RAW, "0000000000000000")).toThrow(
      new LakeFactsError("lake facts: built for handles d768d6e135e62b99, not 0000000000000000"));
    expect(() => decodeLakeFacts(Uint8Array.from([1, 2, 3, 4, 5]), "d768d6e135e62b99")).toThrow(
      new LakeFactsError("lake facts: not a lakes/1 file"));
    expect(() => decodeLakeFacts(RAW.slice(0, RAW.length - 1), "d768d6e135e62b99")).toThrow(
      new LakeFactsError("lake facts: truncated"));
  });

  it("days count from 1900-01-01 in the calendar date given, as the pipeline counts them", () => {
    expect([dayNumber(new Date(1900, 0, 1)), dayNumber(new Date(1970, 0, 1, 23, 59)), dayNumber(new Date(2026, 9, 21))])
      .toEqual([0, 25567, 46314]);
  });

  it("a last-stocked day bands into the style's stock_recency, the Layers sheet's bands", () => {
    const today = 46314;
    expect([0, 120, 121, 365, 366, 1095, 1096, 3650, 3651, 40000].map((ago) => stockRecency(today - ago, today)))
      .toEqual(["season", "season", "year", "year", "recent", "recent", "decade", "decade", "old", "old"]);
    expect([stockRecency(null, today), stockRecency(today + 3, today)]).toEqual([null, "season"]);
    expect(STOCK_RECENCY.map((b) => [b.band, b.days])).toEqual(
      [["season", 120], ["year", 365], ["recent", 1095], ["decade", 3650], ["old", Infinity]]);
  });
});
