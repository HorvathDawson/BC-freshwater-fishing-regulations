/** (T1194) The gauge page's words and where its dots are tappable. */
import { describe, expect, it } from "vitest";
import { GAUGE_PAGE_WORDS, gaugeNotes } from "@app/core";
import type { StationId } from "@app/data";
import { gaugeReadingVM, gaugeTapOn } from "./gaugePage";
import type { Conditions } from "./hooks";

const FED = Date.parse("2026-10-28T05:01:28Z");
const reading = (o: Partial<Conditions> = {}): Conditions => ({
  station: "08MH103" as StationId, stationName: null, discharge: 8.01, level: null, parameter: "discharge", percentile: 0.02,
  standing: null, trust: null, fetchedAt: FED, observedAt: "2026-10-27T20:05:00-08:00", trace: [], ...o } as Conditions);

describe("(T1194) the reading as the page says it", () => {
  it("a current reading: the figure with its unit, its standing, and its place on the colour bar", () => {
    const r = gaugeReadingVM(reading());
    expect(r.figure).toBe("8.01 m³/s");
    expect(r.percentile).toBe(0.02);
    expect(r.standing).toContain("lower than");
    expect(r.noReading).toBeNull();
  });
  it("a reading with no record to rank by has a figure and no bar mark or standing", () => {
    const r = gaugeReadingVM(reading({ percentile: null }));
    expect([r.figure, r.percentile, r.standing]).toEqual(["8.01 m³/s", null, null]);
  });
  it("a reading too old to be current (core readingCurrent) says plainly when it last reported: no figure, no mark", () => {
    const r = gaugeReadingVM(reading({ observedAt: "2026-10-25T10:00:00-08:00" }));
    expect(r.figure).toBeNull();
    expect(r.percentile).toBeNull();
    expect(r.noReading).toBe("No current reading. Last reported 2026-10-25 at 11:00 PT.");
  });
  it("no reading at all says the feed has no report; a null observation time says it is not known", () => {
    expect(gaugeReadingVM(null).noReading).toBe("No current reading. The live feed has no report from this gauge.");
    expect(gaugeReadingVM(reading({ discharge: null, level: null, parameter: null, observedAt: null })).noReading)
      .toBe("No current reading. When it last reported is not known.");
  });
});

describe("(T1194) the trend beside the reading", () => {
  const H = 3_600_000, at = [48, 24, 0].map((h) => new Date(FED - h * H).toISOString());
  it("flow, current, with a measured line a day back: the word; the same for a rise and for little change", () => {
    expect(gaugeReadingVM(reading(), FED, { at, values: [20, 12, 8.01] }).trend).toBe("falling");
    expect(gaugeReadingVM(reading(), FED, { at, values: [4, 5, 8.01] }).trend).toBe("rising");
    expect(gaugeReadingVM(reading(), FED, { at, values: [8, 8.2, 8.01] }).trend).toBe("steady");
  });
  it("no trend: a level reading, a line that is not there, a line too short, a reading that is not current", () => {
    expect(gaugeReadingVM(reading({ discharge: null, level: 1.85, parameter: "level" }), FED, { at, values: [2, 2, 1.85] }).trend).toBeNull();
    expect(gaugeReadingVM(reading(), FED, null).trend).toBeNull();
    expect(gaugeReadingVM(reading(), FED, { at: at.slice(2), values: [8.01] }).trend).toBeNull();
    expect(gaugeReadingVM(reading({ observedAt: "2026-10-25T10:00:00-08:00" }), FED, { at, values: [20, 12, 8] }).trend).toBeNull();
  });
});

describe("(T1194) the page's words", () => {
  it("the tide is said on Level only; a diversion on both; nothing flagged says nothing", () => {
    const f = { tidal: true, backwater: false, diversion: false };
    expect(gaugeNotes(f, "level")).toEqual([GAUGE_PAGE_WORDS.tidal]);
    expect(gaugeNotes(f, "discharge")).toEqual([]);
    expect(gaugeNotes({ tidal: false, backwater: false, diversion: true }, "discharge")).toEqual([GAUGE_PAGE_WORDS.diversion]);
    expect(gaugeNotes({ tidal: false, backwater: false, diversion: false }, "level")).toEqual([]);
  });
  it("the finished strings of the page", () => {
    expect(GAUGE_PAGE_WORDS.notMatched).toBe("This gauge is not matched to a water we cover.");
    expect(GAUGE_PAGE_WORDS.unknown("ZZZ")).toBe("This version of the data has no gauge ZZZ.");
    expect(GAUGE_PAGE_WORDS.servesHeading).toBe("Waters this gauge serves");
  });
});

describe("(T1194, acceptance 8) a gauge dot is tappable on the Conditions tab only", () => {
  const on = (tab: string, picking = false, wideSearch = false) => gaugeTapOn({ tab, picking, wideSearch });
  it("Conditions yes; Map, Runs, Spots, More no; pick mode and wide search no", () => {
    expect(on("conditions")).toBe(true);
    for (const t of ["map", "runs", "spots", "more"]) expect(on(t), t).toBe(false);
    expect(on("conditions", true)).toBe(false);
    expect(on("conditions", false, true)).toBe(false);
  });
});
