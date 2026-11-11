/**
 * (T369) A PLACE ON A STREAM: the `section_line` table read back — a section's line, and the section a place falls on —
 * on the committed regs fixture release with its stream lines added (`openRegsFixtureWithLines`), and the release
 * without the table (cut before it): no place, said by `hasLines`.
 */
import { describe, expect, it } from "vitest";
import { openRegsFixture, openRegsFixtureWithLines, regsFixtureLines, sectionNames } from "../sql/drivers/releaseDir";
import { decodeLineBlock, encodeLineBlock, lineMid, slotAt } from "./line";
import { openLineReads } from "./place";

const COQ = 356364584;   // the Coquitlam River's FWA stream key
// (T369, T667) the Coquitlam's metres (cut at 101 m, 312 m, 6590 m) move with a new ATLAS, and then fail here by name;
// its section NUMBERS move with every build and are never typed: `sectionNames` finds them by key + start metre.
const COQ_LAKE = "wbk:-21";   // a lake of the fixture: its one section has no line

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

  it("the fixture with lines: a section's line, the section a place falls on, a gap (the Coquitlam's lake) holds none", async () => {
    const loader = openRegsFixtureWithLines(), st = openLineReads(loader), names = sectionNames(loader);
    const { unnamed } = regsFixtureLines();
    const [low, mid, high] = await names.streams([[COQ, 0], [COQ, 312], [COQ, 6590]]);
    expect(await st.hasLines()).toBe(true);
    // (T369) the Coquitlam's metres (FWA measures): they move with a new ATLAS, not a new release — and then fail by name
    expect(await st.sectionLine(mid!)).toEqual({ blk: COQ, from: 312, to: 6590 });
    // (T369) the Coquitlam's metres (FWA measures): they move with a new ATLAS, not a new release — and then fail by name
    expect(await Promise.all([st.sectionAt(COQ, 3451), st.sectionAt(COQ, 0), st.sectionAt(COQ, 6600),
                              st.sectionAt(COQ, 20_000), st.sectionAt(COQ - 7, 0)]))
      .toEqual([mid, low, high, null, null]);
    const u = (await st.sectionLine(unnamed))!;
    expect(await st.sectionAt(u.blk, lineMid(u).m)).toBe(unnamed);
    // a lake's handle has no line
    expect(await st.sectionLine(await names.lake(COQ_LAKE, 0))).toBeNull();
  }, 30_000);

  it("a release cut before the lines file: no lines, no place — never an error", async () => {
    const st = openLineReads(openRegsFixture());
    const mid = await sectionNames(openRegsFixtureWithLines()).stream(COQ, 312);   // the same release, asked where the lines are
    expect([await st.hasLines(), await st.sectionLine(mid), await st.sectionAt(COQ, 3451)]).toEqual([false, null, null]);
  }, 30_000);
});
