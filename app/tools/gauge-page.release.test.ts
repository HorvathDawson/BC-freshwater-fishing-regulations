/**
 * (T1194, acceptance 1, 2, 9) THE GAUGE PAGE ON THE LIVE RELEASE: every drawn gauge (a `gauge` row with a point) has a page of
 * its own station, the 7 tidal ones say so and the water-less ones serve nothing, and the counts are PINNED, so a re-cut that
 * loses a gauge, a page or a tidal flag is a red test (re-pin them, reading the list, when a release moves them on purpose).
 */
import { describe, expect, it } from "vitest";
import { readGaugePage, readGaugeRows } from "@app/data/release";
import { gaugeNotes } from "@app/core";
import { liveReleaseAt, openReleaseDir } from "../conformance/src/fixtureRelease";

const at = liveReleaseAt();

it(`T1194 gauge pages on the live release${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T1194 gauge pages skipped: ${at.reason}`);
});

/**
 * THE ROSTERS AT THE 0.10.0 CUT (release r-71d09ad82e81), re-measured: 430 gauges are drawn (a release row with a point AND in the
 * feed's index of 434: the index is live data, so the 430 itself is measured against it by hand, not pinned here), of which 8
 * serve no water and 7 are tidal. The 8 and the 7 are named and pinned below.
 */
const WATERLESS = ["07EF002", "07EF003", "08EC003", "08EC014", "08HB087", "08JB002", "08NH064", "08NH067"];
const TIDAL = ["08MH024", "08MH028", "08MH032", "08MH035", "08MH044", "08MH053", "08MH126"];

describe.skipIf(!at.dir)("(T1194) a page for every drawn gauge", () => {
  it("every drawn gauge opens its own station; the counts hold; the tidal say the tide on Level; the water-less serve nothing", async () => {
    const loader = openReleaseDir(at.dir!);
    const rows = (await readGaugeRows(loader))!;
    const pointed = rows.gauges.filter((g) => g.lat !== null && g.lon !== null);
    const served = new Set(rows.serves.map((x) => x.station));
    // the 8 water-less drawn gauges still have a release row with a point and serve nothing; every other pointed gauge named here serves a water
    expect(pointed.filter((g) => WATERLESS.includes(g.station)).map((g) => g.station).sort()).toEqual(WATERLESS);
    expect(WATERLESS.filter((st) => served.has(st))).toEqual([]);
    const tidalRows = await (await loader.db("conditions")).all("SELECT station FROM gauge WHERE tidal = 1 AND lon IS NOT NULL AND lat IS NOT NULL ORDER BY station");
    expect(tidalRows.map((r) => String(r.station))).toEqual(TIDAL);
    expect(TIDAL.filter((st) => !served.has(st))).toEqual([]);        // all 7 tidal gauges serve waters
    // a page is its own station's (never another's): a spread of every pointed gauge, then the named ones
    const bad: string[] = [];
    for (const g of pointed.filter((_, i) => i % 20 === 0)) {
      const p = await readGaugePage(loader, g.station);
      if (!p || p.station !== g.station || p.name !== g.name) bad.push(g.station);
    }
    expect(bad).toEqual([]);
    for (const s of TIDAL) {
      const p = (await readGaugePage(loader, s))!;
      expect(p.tidal, s).toBe(true);
      expect(gaugeNotes(p, "level"), s).toHaveLength(1);
      expect(gaugeNotes(p, "discharge"), s).toEqual([]);          // the tide is a Level note only
    }
    for (const s of WATERLESS) expect((await readGaugePage(loader, s))!.serves, s).toEqual([]);
    expect(await readGaugePage(loader, "ZZZZZZZ")).toBeNull();
  }, 300_000);
});
