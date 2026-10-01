import { describe, expect, it } from "vitest";
import { FIXTURE as fixture } from "./statusIndex.fixture";
import { statusOfCode, statusOn, waterStatusOn } from "./status";
import { dayOfYear, decodeStatusIndex, StatusIndexError, type StatusIndex } from "./statusIndex";

/*
 * THE FIXTURE IS THE PIPELINE'S. `statusIndex.fixture.ts` is written by
 * pipeline/tests/test_status_index.py: the bytes are the Python encoder's, the answers the
 * Python reader's, and a test there fails if this file falls behind. So "the app decodes what
 * the pipeline wrote, and reads it the same way" is one assertion across both languages.
 */
const BYTES = Uint8Array.from(atob(fixture.bytes), (c) => c.charCodeAt(0));
const day = (iso: string) => {
  const [y, m, d] = iso.split("-").map(Number);
  return new Date(y!, m! - 1, d!);
};

/** Every row the fixture states, as `[key, date, expected, got]` where they differ. */
function disagreements(ix: StatusIndex): unknown[] {
  return [
    ...fixture.sections.map(([s, d, want]) =>
      [s, d, want, ix.codeOn(s as number, day(d as string))]),
    ...fixture.waters.map(([i, d, want]) =>
      [i, d, want, ix.waterCodeOn(i as string, day(d as string))]),
  ].filter((r) => r[2] !== r[3]);
}

describe("the status index decoder", () => {
  it("reads the pipeline's bytes exactly as the pipeline does", () => {
    const ix = decodeStatusIndex(BYTES, fixture.handles);
    expect(ix.handles).toBe("147b20dce7d8576c");
    expect(fixture.sections.length + fixture.waters.length).toBeGreaterThan(100);
    expect(disagreements(ix)).toEqual([]);
    expect(ix.sections).toBe(5);
    expect(ix.waters).toBe(3);
  });

  it("covers every code, a season over New Year and Feb 29 in the fixture", () => {
    const codes = new Set([...fixture.sections, ...fixture.waters].map((r) => r[2]));
    expect([...codes].sort()).toEqual(["base", "closed", "outside", "own"]);
    const ix = decodeStatusIndex(BYTES, null);
    expect(ix.codeOn(3, new Date(2025, 11, 31))).toBe("closed");
    expect(ix.codeOn(3, new Date(2026, 0, 1))).toBe("closed");
    expect(ix.codeOn(3, new Date(2026, 2, 31))).toBe("own");
    expect(ix.codeOn(900, new Date(2024, 1, 29))).toBe("closed");
    expect(ix.codeOn(900, new Date(2025, 2, 1))).toBe("base");
    // The handle as the map reports it: a string is accepted, junk is base, never a throw.
    expect(ix.codeOn("3", new Date(2026, 0, 1))).toBe("closed");
    expect(ix.codeOn("x", new Date(2026, 0, 1))).toBe("base");
    expect(ix.codeOn(-1, new Date(2026, 0, 1))).toBe("base");
  });

  it("puts Feb 29 on day 60 and Mar 1 on day 61 in every year", () => {
    expect(dayOfYear(new Date(2024, 1, 29))).toBe(60);
    expect(dayOfYear(new Date(2025, 2, 1))).toBe(61);
    expect(dayOfYear(new Date(2024, 2, 1))).toBe(61);
    expect(dayOfYear(new Date(2025, 0, 1))).toBe(1);
    expect(dayOfYear(new Date(2025, 11, 31))).toBe(366);
    expect(dayOfYear({ month: 2, day: 28 })).toBe(59);
  });

  it("reads the day on the LOCAL calendar — Dec 31 at night is still Dec 31", () => {
    // A time whose UTC date differs from its local one, in whatever zone the test runs.
    const off = new Date(2025, 11, 31).getTimezoneOffset();
    const at = off > 0 ? new Date(2025, 11, 31, 23, 30) : new Date(2026, 0, 1, 0, 30);
    if (off === 0) return;                    // UTC: there is no such time
    const ix = decodeStatusIndex(BYTES, null);
    expect(dayOfYear(at)).toBe(off > 0 ? 366 : 1);
    // section 3 is closed Dec 1 - Mar 30; on Nov 30 / Apr 1 the other side reads differently
    const edge = off > 0 ? new Date(2025, 10, 30, 23, 30) : new Date(2025, 11, 1, 0, 30);
    expect(ix.codeOn(3, edge)).toBe(off > 0 ? "own" : "closed");
  });

  it("refuses a file built for another atlas, and a damaged one", () => {
    expect(() => decodeStatusIndex(BYTES, "0000000000000000")).toThrow(StatusIndexError);
    expect(() => decodeStatusIndex(BYTES, "0000000000000000")).toThrow(/refused/);
    const bad = BYTES.slice();
    bad[0] = 0;
    expect(() => decodeStatusIndex(bad, null)).toThrow(/not a status index/);
    const v2 = BYTES.slice();
    v2[4] = 2;
    expect(() => decodeStatusIndex(v2, null)).toThrow(/version 2/);
    expect(() => decodeStatusIndex(BYTES.slice(0, BYTES.length - 2), null)).toThrow(StatusIndexError);
    const long = new Uint8Array(BYTES.length + 1);
    long.set(BYTES);
    expect(() => decodeStatusIndex(long, null)).toThrow(/trailing/);
  });

  /*
   * MUTATION. The comparison above must be able to fail: flip one byte at a time through the
   * range tables and the section and water columns, and every flip either is refused by the
   * decoder or changes an answer the fixture pins. A flip nothing notices would be a part of
   * the file this test does not actually read.
   */
  it("notices every one-byte change to the file's tables", () => {
    const unnoticed: number[] = [];
    for (let i = 13; i < BYTES.length; i++) {
      const m = BYTES.slice();
      m[i] = m[i]! ^ 0x01;
      let ix: StatusIndex;
      try { ix = decodeStatusIndex(m, null); } catch { continue; }
      if (!disagreements(ix).length) unnoticed.push(i);
    }
    expect(unnoticed).toEqual([]);
  });
});

describe("one status vocabulary over the index", () => {
  const ix = decodeStatusIndex(BYTES, fixture.handles);
  it("maps the file's codes to the three statuses; tidal and outside are none", () => {
    expect(statusOfCode("closed")).toBe("closed");
    expect(statusOfCode("own")).toBe("own");
    expect(statusOfCode("base")).toBe("base");
    expect(statusOfCode("tidal")).toBeNull();
    expect(statusOfCode("outside")).toBeNull();
    expect(statusOn(ix, 1_958_036, new Date(2025, 5, 1))).toBeNull();     // outside B.C.
  });
  it("answers sections and waters through the same index, and nothing without one", () => {
    expect(statusOn(ix, 5, new Date(2025, 5, 1))).toBe("own");
    expect(statusOn(ix, 6, new Date(2025, 5, 1))).toBe("base");
    expect(waterStatusOn(ix, "gnis:1", new Date(2025, 0, 15))).toBe("closed");
    expect(waterStatusOn(ix, "gnis:1", new Date(2025, 5, 15))).toBe("own");
    expect(waterStatusOn(ix, "gnis:2", new Date(2025, 5, 15))).toBe("base");
    expect(statusOn(null, 5, new Date())).toBeNull();
    expect(waterStatusOn(undefined, "gnis:1", new Date())).toBeNull();
  });
});
