/**
 * THE LAKE MODES' GATE (owner 2026-10-21: Stocked / Depth prepared, not fed — "we even might want the toggles
 * grey/disabled for now"). A mode is offered only while the loaded release carries values for it; a release that does
 * enables it with no app change. Synthetic lakes files only: no feed makes one yet.
 */
import { describe, expect, it } from "vitest";
import { decodeLakeFacts } from "@app/core";
import { lakeModesReady, lakeValuesCarried } from "./lakeColour";

const HANDLES = [0xd7, 0x68, 0xd6, 0xe1, 0x35, 0xe6, 0x2b, 0x99];
/** A lakes/1 file: header, then (sid delta, survey code, stocked day + 1, each under 128: one varint byte) per section. */
const file = (...rows: (readonly [number, number, number])[]) => decodeLakeFacts(Uint8Array.from([
  0x42, 0x43, 0x4c, 0x4b, 1, ...HANDLES, 0, rows.length, ...rows.flat(),
]), "d768d6e135e62b99");
const DAY = 46314;

describe("lakeModesReady", () => {
  it("no release values (no file, or a release without the slot): both modes off", () => {
    expect(lakeModesReady(null)).toEqual({ depth: false, stocked: false });
  });

  it("an empty slot is no values: both off", () => {
    expect(lakeModesReady({ facts: file(), day: DAY })).toEqual({ depth: false, stocked: false });
  });

  it("each mode turns on with its own values: survey only, stocking only, both", () => {
    expect([
      lakeModesReady({ facts: file([12, 1, 0]), day: DAY }),
      lakeModesReady({ facts: file([12, 0, 5]), day: DAY }),
      lakeModesReady({ facts: file([12, 2, 0], [3, 0, 5]), day: DAY }),
    ]).toEqual([
      { depth: true, stocked: false },
      { depth: false, stocked: true },
      { depth: true, stocked: true },
    ]);
  });
});

describe("lakeValuesCarried: the registry's facts", () => {
  it("survey values carry Depth's need, stocking values Stocked's — never crossed (mutation pin)", () => {
    expect([lakeValuesCarried({ depth: true, stocked: false }), lakeValuesCarried({ depth: false, stocked: true })])
      .toEqual([{ stocked: false, surveyed: true }, { stocked: true, surveyed: false }]);
  });
});
