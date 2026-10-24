import { describe, expect, it } from "vitest";
import { decodeStatusIndex } from "@app/core";
import { statusData } from "./hooks";

/** A status index by hand (version 2: handles, then the bundle's reach digest): sections 5 and 6
 * and water "gnis:1" closed Jan 1 - Mar 31. */
function tiny(handles = "147b20dce7d8576c", reach = "ad6279cad5f3fc76"): Uint8Array {
  const v = (n: number): number[] => {
    const out: number[] = [];
    do { out.push((n & 0x7f) | (n > 0x7f ? 0x80 : 0)); n >>>= 7; } while (n);
    return out;
  };
  const h = handles.match(/../g)!.map((x) => parseInt(x, 16));
  const r = reach.match(/../g)!.map((x) => parseInt(x, 16));
  return Uint8Array.from([
    0x42, 0x43, 0x53, 0x49, 2, ...h, ...r,
    ...v(1), ...v(2), ...v((91 << 3) | 2), ...v((275 << 3) | 1),   // one profile
    ...v(1), ...v(5), ...v(2), ...v(0),                              // sections 5..6
    ...v(1), ...v(0), ...v(6), ..."gnis:1".split("").map((c) => c.charCodeAt(0)), ...v(0),
  ]);
}

describe("the map's status values", () => {
  it("tells EVERY visible section its status — base included, so yesterday's red is cleared",
    async () => {
      const ix = decodeStatusIndex(tiny(), "147b20dce7d8576c", null);
      const d = statusData(ix, [5, 7], new Date(2026, 0, 10));
      // once, under `sections`: the map fans it out to every per-section layer (T124), a slough's wetland included
      expect(d).toEqual({ sections: { 5: { status: "closed" }, 7: { status: "base" } } });
      // no index: every value is null (the mode's `missing`), never a guessed base
      expect(statusData(null, [5], new Date())).toEqual({ sections: { 5: { status: null } } });
    });
});
