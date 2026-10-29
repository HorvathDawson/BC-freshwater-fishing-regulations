import { describe, expect, it } from "vitest";
import { checkRunTiming, countHeight, inWaterOn, MONTH_PENTADS, parseRunTimingIndex, pentadDay, pentadLabel, pentadOf,
         restoreRunSpecies, runCounts, sliderKey, runSheet, runSpecies, RunTimingFacts, saveRunSpecies, slotFor, slotsFor,
         speciesList, type PaintDoc, type RunsDoc } from "./runTiming";

const SLOTS = ["Chinook", "Chum", "Coho", "Pink (even)", "Pink (odd)", "Sockeye", "Steelhead"];
const hex = (pentads: number[]) => pentads.reduce((h, p) => h | (1n << BigInt(p)), 0n).toString(16).padStart(19, "0");
const runs: RunsDoc = {
  format: "runtiming-runs/1", slots: SLOTS, default: 6, relations: ["spawning", "passage", "observed"],
  relation_words: { spawning: "Spawns in this watershed", passage: "Passes through here",
                    observed: "Seen in this stream (DFO spawner surveys)" },
  sources: {
    psf: { credit: "Pacific Salmon Foundation (Salmon Watersheds Program)", title: "t", dataset: "d", updated: null,
           fetched: null, terms: "Used with the written permission of the Pacific Salmon Foundation.", url: "u" },
    dfo: { credit: "Fisheries and Oceans Canada", title: "t", dataset: "d", updated: null, fetched: null,
           terms: "Contains information licensed under the Open Government Licence – Canada.", url: "u" },
  },
  runs: [
    // steelhead summer: July–Sept (months 6,7,8)
    { id: "psf:1", src: "psf", sp: "Steelhead", slot: 6, name: "Skeena Summer", label: "summer", peak: "Aug 11",
      window: ["Jul 2", "Sep 20"], months: (1 << 6) | (1 << 7) | (1 << 8), pentads: hex([36, 37, 44]) },
    // coho, DFO in the stream Oct–Nov
    { id: "dfo:9:Coho:-", src: "dfo", sp: "Coho", slot: 2, name: "Kitwanga River", months: (1 << 9) | (1 << 10),
      pentads: hex([58, 60]), years: [12, 2001, 2024], window: ["Oct 1", "Nov 30"],
      n: [[2016, 900], [2017, 1100], [2018, 1000], [2019, 1200], [2020, 2600], [2021, 3100], [2022, 2900]],
      na: [[2020, 1800], [2021, 2200], [2022, 410]] },
  ],
};
// profile 0: steelhead spawning; profile 1: steelhead passing + coho observed
const paint: PaintDoc = {
  format: "runtiming-paint/1", section_handles: "h", tile_version: "h.t", runs_sha256: "s",
  profiles: [{ r: [0], s: 1 << 6, m: [[6, (1 << 6) | (1 << 7) | (1 << 8), 0]] },
             { r: [1, 5], s: (1 << 6) | (1 << 2), m: [[2, (1 << 9) | (1 << 10), 0], [6, 0, (1 << 6) | (1 << 7) | (1 << 8)]] }],
  sid: [10, 5], p: [0, 1], km: [0, 13],
  basins: { ids: ["CHWK", "LFRA"], m: [[[6, hex([36, 37]), hex([])]], [[2, hex([]), hex([58])], [6, hex([]), hex([36, 44])]]] },
};
const index = parseRunTimingIndex({ format: "runtiming-index/1", built_at: "x", section_handles: "h", tile_version: "h.t",
  sources: {}, files: { runs: { url: "r", bytes: 1, sha256: "s" }, paint: { url: "p", bytes: 1, sha256: "q" } } })!;
const facts = new RunTimingFacts(index, runs, paint);

describe("run timing", () => {
  it("reads a section's state for the species chosen and a pentad, and says nothing where no run is", () => {
    expect(facts.state(10, [6], 36)).toBe("spawn");
    expect(facts.state(10, [6], 10)).toBe("off");
    expect(facts.state(15, [6], 36)).toBe("pass");
    expect(facts.state(15, [2], 58)).toBe("spawn");          // surveyed in the stream is local
    expect(facts.state(15, [2, 6], 36)).toBe("pass");        // coho not in then, steelhead passing
    expect(facts.state(15, [2, 6], 58)).toBe("spawn");       // spawn wins over the rest
    expect(facts.state(15, [2, 6], 59)).toBe("off");
    expect(facts.state(10, [2], 58)).toBeUndefined();        // no coho recorded here
    expect(facts.state(11, [6], 36)).toBeUndefined();
    expect(facts.has(15)).toBe(true);
    expect(facts.has(11)).toBe(false);
    expect(facts.kmToSea(15)).toBe(120);
    expect(facts.kmToSea(10)).toBeNull();
  });

  it("colours the watershed groups zoomed out: spawn over pass over off, absent where none of the species run", () => {
    expect(facts.hasField).toBe(true);
    expect(facts.basins([6], 36)).toEqual({ CHWK: "spawn", LFRA: "pass" });
    expect(facts.basins([6], 44)).toEqual({ CHWK: "off", LFRA: "pass" });
    expect(facts.basins([2], 58)).toEqual({ LFRA: "pass" });
    expect(facts.basins([2], 10)).toEqual({ LFRA: "off" });
    expect(facts.basins([0], 10)).toEqual({});
    const season = facts.season([6]);
    expect([season.length, season[36], season[44], season[10]])
      .toEqual([73, { spawn: 0.5, pass: 0.5 }, { spawn: 0, pass: 0.5 }, { spawn: 0, pass: 0 }]);
  });

  it("refuses a paint index of other runs or another atlas", () => {
    expect(() => checkRunTiming(runs, { ...paint, runs_sha256: "z" }, index)).toThrow(/not of these runs/);
    expect(() => checkRunTiming(runs, { ...paint, runs_sha256: "s", section_handles: "x" },
                                { ...index, files: { ...index.files, runs: { url: "r", bytes: 1, sha256: "s" } } }))
      .toThrow(/another atlas/);
    expect(parseRunTimingIndex({ format: "runtiming-index/2" })).toBeNull();
  });

  it("puts steelhead first, folds pink's brood lines and picks one by the year", () => {
    const sp = runSpecies(facts);
    expect(sp.map((s) => s.name)).toEqual(["Steelhead", "Chinook", "Chum", "Coho", SLOTS[3]!.split(" ")[0], "Sockeye"]);
    const pk = sp[4]!;
    expect(SLOTS[slotFor(facts, pk, 2026)]).toBe("Pink (even)");
    expect(SLOTS[slotFor(facts, pk, 2027)]).toBe("Pink (odd)");
  });

  it("keeps the species chosen, dropping what the feed no longer offers; steelhead when nothing is kept", () => {
    const sp = runSpecies(facts);
    expect(restoreRunSpecies(null, sp)).toEqual(["Steelhead"]);
    expect(restoreRunSpecies("not json", sp)).toEqual(["Steelhead"]);
    expect(restoreRunSpecies(saveRunSpecies(["Coho", "Bass", "Coho"]), sp)).toEqual(["Coho"]);
    expect(slotsFor(facts, sp.filter((s) => s.slots.length === 2 || s.name === "Coho"), 2027).map((s) => SLOTS[s]))
      .toEqual(["Coho", "Pink (odd)"]);
    expect([speciesList(["Steelhead"]), speciesList(["Steelhead", "Coho"]), speciesList(["Steelhead", "Coho", "Chum"]),
            speciesList(["A", "B", "C", "D"])])
      .toEqual(["Steelhead", "Steelhead and coho", "Steelhead, coho and chum", "4 species"]);
  });

  it("says a pentad's days whole, across a month and at the year's end", () => {
    expect(pentadLabel(0)).toBe("Jan 1–5");
    expect(pentadLabel(44)).toBe("Aug 9–13");
    expect(pentadLabel(11)).toBe("Feb 25–Mar 1");
    expect(pentadLabel(72)).toBe("Dec 27–31");
    expect(pentadDay(44, 2026).toDateString()).toBe(new Date(2026, 7, 9).toDateString());
    expect(pentadOf(pentadDay(44, 2026))).toBe(44);
    expect(MONTH_PENTADS.slice(0, 3)).toEqual([0, 7, 12]);
    expect(["ArrowRight", "ArrowLeft", "PageUp", "PageDown", "Home", "End", "a"].map((k) => sliderKey(k, 70)))
      .toEqual([71, 69, 72, 64, 0, 72, null]);
  });

  it("sizes a run from DFO's counts only: bars on the log scale, the latest, a trend from six years", () => {
    expect(runCounts(runs.runs[0]!)).toBeNull();                       // a PSF run: no counts, never guessed
    const c = runCounts(runs.runs[1]!)!.series[0]!;
    expect(c.latest).toBe("Latest count 2,900 (2022)");
    expect(c.trend).toBe("Recent counts are higher than the years before");
    expect(c.bars.map((b) => b.label)[0]).toBe("2016: 900");
    expect([countHeight(0), countHeight(10), countHeight(100_000), countHeight(5_000_000)]).toEqual([0, 0.2, 1, 1]);
    expect(runCounts({ n: [[2020, 5], [2021, 6]] })!.series[0]!.trend).toBeNull();
    expect(runCounts({ n: [[1, 0], [2, 0], [3, 0], [4, 0], [5, 0], [6, 0]] })!.series[0]!.trend)
      .toBe("Recent counts are about the same as the years before");
    expect(runCounts({ n: [[1, 100], [2, 100], [3, 100], [4, 40], [5, 50], [6, 60]] })!.series[0]!.trend)
      .toBe("Recent counts are lower than the years before");
  });

  it("keeps DFO's total and adult spawners as two labelled series, total first, adults alone when no total", () => {
    const both = runCounts({ n: [[2020, 500], [2021, 600]], na: [[2020, 300], [2021, 20]] })!;
    expect(both.series.map((s) => [s.label, s.latest])).toEqual([
      ["Total spawners", "Latest count 600 (2021)"], ["Adult spawners", "Latest count 20 (2021)"]]);
    const adultOnly = runCounts({ na: [[2019, 40]] })!;
    expect(adultOnly.series.map((s) => s.label)).toEqual(["Adult spawners"]);
    expect(runCounts({ n: [[2019, 40]] })!.series.map((s) => s.label)).toEqual(["Total spawners"]);
    expect(runCounts({ na: [] })).toBeNull();
  });

  it("dates a day by its pentad on the common year", () => {
    expect(pentadOf(new Date(2026, 0, 1))).toBe(0);
    expect(pentadOf(new Date(2026, 11, 31))).toBe(72);
    expect(pentadOf(new Date(2028, 2, 1))).toBe(pentadOf(new Date(2026, 2, 1)));   // leap year: Feb 29 folded
    expect(inWaterOn(runs.runs[0]!, new Date(2026, 5, 25))).toBe(false);          // Jun 25: pentad 35
    expect(inWaterOn(runs.runs[0]!, new Date(2026, 6, 3))).toBe(true);
  });

  it("words the sheet whole: the chosen species first, its source's terms, river entry, the km, the counts", () => {
    const sp = runSpecies(facts)[0]!;
    const vm = runSheet(facts, 15, [sp], 37);
    expect(vm.rows.map((r) => [r.title, r.code, r.population, r.relation, r.kind, r.dates, r.nowWords, r.survey,
                               r.counts?.series.map((x) => `${x.label}: ${x.latest}`).join("; ") ?? "No counts"])).toEqual([
      ["Steelhead — summer", "ST", "Skeena Summer", "Passes through here", "pass",
       "Enters fresh water Jul 2 – Sep 20, peak Aug 11", "In the water Jul 5–9", null, "No counts"],
      ["Coho", "CO", "Kitwanga River", "Seen in this stream (DFO spawner surveys)", "spawn", "In the stream Oct 1 – Nov 30",
       "Not in the water Jul 5–9", "DFO surveys: 12 years, 2001–2024", "Total spawners: Latest count 2,900 (2022); Adult spawners: Latest count 410 (2022)"],
    ]);
    expect(vm.rows[0]!.year.filter(Boolean).length).toBe(3);
    expect([vm.mine, vm.moreWords]).toEqual([1, "1 run of another species here"]);
    expect(vm.noneOfSpecies).toBeNull();
    expect(`${vm.riverEntry} ${vm.km}`).toBe("PSF dates are when fish enter fresh water. This water is about 120 km "
      + "from the sea, so they reach it later.");
    expect(vm.countsNote).toBe("Counts are DFO's spawner estimates for the surveyed stream: total spawners and adult spawners are "
      + "separate estimates, shown apart. PSF runs carry no counts here.");
    expect(vm.credit).toBe("Run timing: Fisheries and Oceans Canada — Contains information licensed under the Open "
      + "Government Licence – Canada. Pacific Salmon Foundation (Salmon Watersheds Program) — Used with the written "
      + "permission of the Pacific Salmon Foundation.");
  });

  it("says no run of the species is recorded — never no fish", () => {
    const sp = runSpecies(facts);
    expect(runSheet(facts, 10, sp.filter((s) => ["Chinook", "Chum"].includes(s.name)), 37).noneOfSpecies)
      .toBe("No chinook and chum run is recorded on this water. That is not the same as no fish.");
  });
});
