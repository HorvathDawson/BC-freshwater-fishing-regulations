// T225: the Python semantics the gauge port leans on (src/gauges/py.ts), against vectors computed by the repo's
// Python (3.13) — round(x, n), sum() (Neumaier since 3.12), repr(float), change.coded (math.erf).
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { coded } from "../src/gauges/change";
import { csvRows, pyFloat, pyFloatRepr, pyRound, pySum, urlencode } from "../src/gauges/py";

const V = JSON.parse(readFileSync(new URL("./fixtures/gauges-py-vectors.json", import.meta.url), "utf-8"));

describe("python semantics", () => {
  it("round(x, n) is Python's (ties to even on the exact double)", () => {
    for (const [x, n, want] of V.round) expect(pyRound(x, n), `round(${x}, ${n})`).toBe(want);
  });
  it("sum() is Python's compensated sum", () => {
    for (const [xs, want] of V.sum) expect(pySum(xs)).toBe(want);
  });
  it("repr(float)", () => {
    for (const [x, want] of V.repr) expect(pyFloatRepr(x)).toBe(want);
  });
  it("coded() through erf agrees to 1e-15", () => {
    for (const [r, want] of V.coded) expect(Math.abs(coded(r) - want)).toBeLessThan(1e-15);
  });
  it("float() spellings", () => {
    expect(pyFloat(" 12.5 ")).toBe(12.5);
    expect(pyFloat("1_000.5")).toBe(1000.5);
    expect(pyFloat("1e-3")).toBe(0.001);
    expect(pyFloat("12.")).toBe(12);
    expect(pyFloat("abc")).toBeNull();
    expect(pyFloat("")).toBeNull();
  });
  it("csv: quotes, CRLF, blank lines", () => {
    expect(csvRows('a,"b,c",d\r\n\r\ne,""""\n')).toEqual([["a", "b,c", "d"], [], ["e", '"']]);
  });
  it("urlencode is quote_plus", () => {
    expect(urlencode([["stations[]", "08MH001"], ["start_date", "2026-10-27 22:47:06"], ["outFields", "*"]])).toBe(
      "stations%5B%5D=08MH001&start_date=2026-10-27+22%3A47%3A06&outFields=%2A",
    );
  });
});
