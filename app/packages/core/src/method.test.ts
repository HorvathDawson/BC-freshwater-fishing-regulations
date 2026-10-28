import { describe, expect, it } from "vitest";
import { decodeMethod, METHOD_UNLISTED, methodClassOn, methodDayProfiles, methodLabel, MethodError } from "./method";

/** `pipeline/tests/test_method_file.py` test_method_1_round_trips' file, as the Python encoder writes it: profile 0 bait
 *  ban all year, profile 1 fly only Jan 1 → Apr 9, none (a closure: the status index's) Apr 10 → Jun 14, bait ban after; sids 3-4 and 900,000 on 0,
 *  5 and 900,001 on 1. */
const GOLDEN = "42434d5401b6b5000e171023c90201b90b0392038802a1060403009af736000201010100010001";
const HANDLES = "b6b5000e171023c9";
const bytes = (hex: string) => new Uint8Array(hex.match(/../g)!.map((h) => parseInt(h, 16)));

describe("method/1 (T213)", () => {
  const f = decodeMethod(bytes(GOLDEN), HANDLES);

  it("reads the Python encoder's file: profiles, runs and the leap calendar", () => {
    expect(f.profiles).toBe(2);
    expect([3, 4, 5, 6, 900_000, 900_001, 2].map((s) => f.profileOf(s))).toEqual([0, 0, 1, null, 0, 1, null]);
    // day 100 is Apr 9 on the leap calendar (Feb 29 = 60), 101 Apr 10, 167 Jun 15
    expect([1, 100, 101, 166, 167, 366].map((d) => f.profileClassOn(1, d)))
      .toEqual(["fly", "fly", "none", "none", "bait", "bait"]);
    expect(methodClassOn(f, 5, new Date(2027, 3, 9))).toBe("fly");
    expect(methodClassOn(f, 5, new Date(2027, 3, 10))).toBe("none");
    expect(methodClassOn(f, 3, new Date(2027, 3, 10))).toBe("bait");
    expect(methodClassOn(f, 6, new Date(2027, 3, 10))).toBe("none");
  });

  it("labels each section once and carries the day as label sets, every label in one set", () => {
    expect([methodLabel(f, 3), methodLabel(f, 5), methodLabel(f, 6)]).toEqual(["m0", "m1", METHOD_UNLISTED]);
    const may = methodDayProfiles(f, new Date(2027, 4, 1));
    expect(may).toEqual({ none: [METHOD_UNLISTED, "m1"], bait: ["m0"], fly: [] });
    const jan = methodDayProfiles(f, new Date(2027, 0, 1));
    expect(jan.fly).toEqual(["m1"]);
    const all = Object.values(jan).flat();
    expect(new Set(all).size).toBe(all.length);
  });

  it("refuses another atlas, another format, trailing bytes", () => {
    expect(() => decodeMethod(bytes(GOLDEN), "0000000000000000")).toThrow(MethodError);
    expect(() => decodeMethod(bytes(GOLDEN.replace(/^42434d54/, "42435341")), HANDLES)).toThrow(MethodError);
    expect(() => decodeMethod(bytes(GOLDEN + "00"), HANDLES)).toThrow(MethodError);
  });
});
