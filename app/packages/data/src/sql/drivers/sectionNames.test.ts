/**
 * (T667) `sectionNames` by mutation: a RENUMBERED release changes no named answer (the test types no number, so it has
 * nothing to edit); a section or part that MOVED fails, naming what was asked for. The names come from the fixture's own
 * list (`dev/section-lines.json`) and from end tokens, never from a handle or a part number.
 */
import { describe, expect, it } from "vitest";
import type { Db, Row } from "../db";
import { LINE_OPEN } from "../../regs/line";
import { openLineReads } from "../../regs/place";
import { openRegsFixtureWithLines, regsFixtureLines, sectionNames, type LineRow } from "./releaseDir";

const ROWS = regsFixtureLines().lines;
/** three closed stream sections of the fixture that begin where another ends (a cut), as names: [stream key, start metre] */
const PICKED = ROWS.filter(([, blk, from, to]) => to !== LINE_OPEN && to - from > 200 &&
  ROWS.some((r) => r[1] === blk && r[3] === from)).slice(0, 3);
const NAMES = PICKED.map(([, blk, from]) => [blk, from] as const);

const VEDDER = "gnis:3062";
const VEDDER_LOW = ["mouth", "chilliwack_vedder_rivers__vedder_crossing_bridge_200_m_downstream"] as const;
const VEDDER_KEY = 356363467;                                        // the Vedder's own stream key (FWA, durable)

/** the water file with every part one higher: what a new cut below every part does to `part_ix` */
const partsShifted = (d: Db): Db => ({
  ...d,
  all: async (sql, ...params) => (await d.all(sql, ...params)).map((r): Row => "part_ix" in r ? { ...r, part_ix: Number(r.part_ix) + 1 } : r),
});

describe("T667 section names: a renumbering is absorbed, a move fails by name", () => {
  it("the fixture has sections to name", () => {
    expect(PICKED).toHaveLength(3);
  });

  it("renumbered handles: the same names find the same sections under their new numbers", async () => {
    const shifted = ROWS.map(([h, blk, from, to]): LineRow => [h + 7, blk, from, to]);
    const was = await sectionNames(openRegsFixtureWithLines()).streams(NAMES);
    const loader = openRegsFixtureWithLines(shifted), now = await sectionNames(loader).streams(NAMES);
    expect(was).toEqual(PICKED.map(([h]) => h));
    expect(now).toEqual(was.map((h) => h + 7));                      // every NUMBER moved
    const lines = openLineReads(loader);                             // … and names the same water, the same metres
    expect(await Promise.all(now.map((h) => lines.sectionLine(h))))
      .toEqual(PICKED.map(([, blk, from, to]) => ({ blk, from, to })));
  });

  it("a section whose start moved 100 m is refused by its name, in the release's own words", async () => {
    const [h0, blk, from] = PICKED[1]!;
    // the cut moved 100 m upstream: the section below now runs through the old place, this one starts later
    const moved = ROWS.map((r): LineRow => r[0] === h0 ? [r[0], r[1], r[2] + 100, r[3]]
      : r[1] === blk && r[3] === from ? [r[0], r[1], r[2], r[3] + 100] : r);
    expect(await openLineReads(openRegsFixtureWithLines(moved)).sectionAt(blk, from)).not.toBeNull();   // a section IS there
    const loader = openRegsFixtureWithLines(moved), names = sectionNames(loader);
    await expect(names.stream(blk, from)).rejects.toThrow(
      `section names: no stream section starts at ${blk}:${from} in release ${loader.manifest.data_release}`);
    await expect(names.streams(NAMES)).rejects.toThrow(`${blk}:${from}`);
    expect(await names.stream(blk, from + 100)).toBe(h0);            // named where it starts now
    // and on the release as cut, a metre INSIDE a section is not its name
    await expect(sectionNames(openRegsFixtureWithLines()).stream(blk, from + 50)).rejects.toThrow(`no stream section starts at ${blk}:${from + 50}`);
  });

  it("renumbered parts: a part named by its ends, or by a place on it, follows its new number", async () => {
    const names = sectionNames(openRegsFixtureWithLines());
    const low = await names.part(VEDDER, ...VEDDER_LOW);
    expect(await names.partAt(VEDDER, VEDDER_KEY, 5000)).toBe(low);  // the same part, by a place on it
    const cut = sectionNames(openRegsFixtureWithLines(ROWS, { db: (name, d) => name === "water" ? partsShifted(d) : d }));
    expect(await cut.part(VEDDER, ...VEDDER_LOW)).toBe(low + 1);
    expect(await cut.partAt(VEDDER, VEDDER_KEY, 5000)).toBe(low + 1);
  });

  it("a part whose end moved to another boundary, or a place off the water, is refused by name", async () => {
    const loader = openRegsFixtureWithLines(), names = sectionNames(loader);
    await expect(names.part(VEDDER, "mouth", "chilliwack_vedder_rivers__vedder_crossing_bridge")).rejects.toThrow(
      `section names: ${VEDDER} has 0 parts from mouth to chilliwack_vedder_rivers__vedder_crossing_bridge, not 1, in release ${loader.manifest.data_release}`);
    await expect(names.partAt(VEDDER, VEDDER_KEY, 99_000_000)).rejects.toThrow(
      `section names: ${VEDDER} has 0 parts at ${VEDDER_KEY}:99000000, not 1`);
  });
});
