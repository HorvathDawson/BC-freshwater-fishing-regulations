/**
 * (T369) A PLACE ON A STREAM: the `section_line` table read back — a section's line, and the section a place falls on —
 * on the mini release (T667 slice 8: the pipeline's own `lines` file over an invented world; it was the regs fixture
 * with lines added by hand), and the release without the file: no place, said by `hasLines`.
 */
import { describe, expect, it } from "vitest";
import { openMiniRelease, sectionNames } from "../sql/drivers/releaseDir";
import { decodeLineBlock, encodeLineBlock, LINE_OPEN, lineMid, slotAt } from "./line";
import { openLineReads } from "./place";

const COQ = 356364584;   // a stream key, for the block round trip (no release)
// (T667 slice 8) the mini world's invented stream keys and lake (pipeline/tests/fixtures/mini/world.json); section
// NUMBERS are never typed: `sectionNames` finds them by key + start metre
const FIX = 900000001, MOCK = 900000002, TRIB = 900000003, FIX_LAKE = "wbk:800000001";

describe("section_line (T369)", () => {
  it("a block round-trips through the writer and the reader; slotAt finds the section that spans a measure", () => {
    const lines = [{ blk: -2300000000, from: 0, to: 10 }, { blk: COQ, from: 0, to: 312 }, { blk: COQ, from: 312, to: 6590 },
                   { blk: COQ + 1, from: 0, to: 40 }];
    const b = decodeLineBlock(encodeLineBlock(lines), lines.length);
    expect([...b.blk]).toEqual(lines.map((l) => l.blk));     // a stream key below −2^31 survives (float64)
    expect([slotAt(b, COQ, 0), slotAt(b, COQ, 311), slotAt(b, COQ, 312), slotAt(b, COQ, 6589), slotAt(b, COQ, 6590),
            slotAt(b, COQ + 1, 39), slotAt(b, 1, 0)]).toEqual([1, 1, 2, 2, -1, 3, -1]);
    // (T369) the Coquitlam's metres: they move with a new ATLAS, not a new release
    expect(lineMid(lines[2]!)).toEqual({ blk: COQ, m: 3451 });
  });

  // (T667 slice 8) ON THE MINI RELEASE: the real pipeline's own `lines` file over the invented world — Fixture River
  // (stream key 900000001) is cut at the coast (1 km), at Fixture Lake (16-19 km), at Twofold Lake (25-27 km), and runs
  // to 46 km; Mock Creek (900000002) at its falls (5999 m); the unnamed trib (900000003) is one section, open-ended.
  it("the mini release: a section's line, the section a place falls on, a lake's gap holds none, a one-section stream is open", async () => {
    const loader = openMiniRelease(), st = openLineReads(loader), names = sectionNames(loader);
    const [mouth, low, mid, high] = await names.streams([[FIX, 0], [FIX, 1000], [FIX, 19000], [FIX, 27000]]);
    expect(await st.hasLines()).toBe(true);
    expect(await st.sectionLine(mid!)).toEqual({ blk: FIX, from: 19000, to: 25000 });
    expect(await Promise.all([st.sectionAt(FIX, 22000), st.sectionAt(FIX, 0), st.sectionAt(FIX, 999), st.sectionAt(FIX, 1000),
                              st.sectionAt(FIX, 45_999), st.sectionAt(FIX, 26_000), st.sectionAt(FIX, 46_000),
                              st.sectionAt(FIX - 7, 0)]))
      .toEqual([mid, mouth, mouth, low, high, null, null, null]);      // under Twofold Lake: no stream line
    const [above, below] = await Promise.all([st.sectionAt(MOCK, 6000), st.sectionAt(MOCK, 5998)]);
    expect([await st.sectionLine(below!), await st.sectionLine(above!)])
      .toEqual([{ blk: MOCK, from: 0, to: 5999 }, { blk: MOCK, from: 5999, to: 8000 }]);
    const u = (await st.sectionAt(TRIB, 2500))!;
    expect(await st.sectionLine(u)).toEqual({ blk: TRIB, from: 0, to: LINE_OPEN });
    // a lake's handle has no line
    expect(await st.sectionLine(await names.lake(FIX_LAKE, 0))).toBeNull();
  }, 30_000);

  it("a release cut before the lines file: no lines, no place — never an error", async () => {
    const mid = await sectionNames(openMiniRelease()).stream(FIX, 19000);   // the same release, asked where the lines are
    const st = openLineReads(openMiniRelease({ manifest: (m) => {
      const { lines: _, ...files } = m.files;
      return { ...m, files: files as typeof m.files };
    } }));
    expect([await st.hasLines(), await st.sectionLine(mid), await st.sectionAt(FIX, 22000)]).toEqual([false, null, null]);
  }, 30_000);
});
