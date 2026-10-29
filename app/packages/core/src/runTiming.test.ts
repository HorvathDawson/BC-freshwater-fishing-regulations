import { describe, expect, it } from "vitest";
import { checkRunTiming, inWaterOn, parseRunTimingIndex, pentadOf, runSheet, runSpecies, RunTimingFacts, slotFor,
         type PaintDoc, type RunsDoc } from "./runTiming";

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
      pentads: hex([58, 60]), years: [12, 2001, 2024], window: ["Oct 1", "Nov 30"] },
  ],
};
// profile 0: steelhead spawning; profile 1: steelhead passing + coho observed
const paint: PaintDoc = {
  format: "runtiming-paint/1", section_handles: "h", tile_version: "h.t", runs_sha256: "s",
  profiles: [{ r: [0], s: 1 << 6, m: [[6, (1 << 6) | (1 << 7) | (1 << 8), 0]] },
             { r: [1, 5], s: (1 << 6) | (1 << 2), m: [[2, (1 << 9) | (1 << 10), 0], [6, 0, (1 << 6) | (1 << 7) | (1 << 8)]] }],
  sid: [10, 5], p: [0, 1], km: [0, 13],
};
const index = parseRunTimingIndex({ format: "runtiming-index/1", built_at: "x", section_handles: "h", tile_version: "h.t",
  sources: {}, files: { runs: { url: "r", bytes: 1, sha256: "s" }, paint: { url: "p", bytes: 1, sha256: "q" } } })!;
const facts = new RunTimingFacts(index, runs, paint);

describe("run timing", () => {
  it("reads a section's state for a species and month, and says nothing where no run is", () => {
    expect(facts.state(10, 6, 7)).toBe("spawn");
    expect(facts.state(10, 6, 1)).toBe("off");
    expect(facts.state(15, 6, 7)).toBe("pass");
    expect(facts.state(15, 2, 9)).toBe("spawn");          // surveyed in the stream is local
    expect(facts.state(10, 2, 9)).toBeUndefined();        // no coho recorded here
    expect(facts.state(11, 6, 7)).toBeUndefined();
    expect(facts.has(15)).toBe(true);
    expect(facts.has(11)).toBe(false);
    expect(facts.kmToSea(15)).toBe(120);
    expect(facts.kmToSea(10)).toBeNull();
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

  it("dates a day by its pentad on the common year", () => {
    expect(pentadOf(new Date(2026, 0, 1))).toBe(0);
    expect(pentadOf(new Date(2026, 11, 31))).toBe(72);
    expect(pentadOf(new Date(2028, 2, 1))).toBe(pentadOf(new Date(2026, 2, 1)));   // leap year: Feb 29 folded
    expect(inWaterOn(runs.runs[0]!, new Date(2026, 5, 25))).toBe(false);          // Jun 25: pentad 35
    expect(inWaterOn(runs.runs[0]!, new Date(2026, 6, 3))).toBe(true);
  });

  it("words the sheet whole: the chosen species first, its source's terms, river entry and the km", () => {
    const sp = runSpecies(facts)[0]!;
    const vm = runSheet(facts, 15, sp, new Date(2026, 6, 5));
    expect(vm.rows.map((r) => [r.title, r.population, r.relation, r.dates, r.nowWords, r.survey])).toEqual([
      ["Steelhead — summer", "Skeena Summer", "Passes through here", "Enters fresh water Jul 2 – Sep 20, peak Aug 11",
       "In the water now", null],
      ["Coho", "Kitwanga River", "Seen in this stream (DFO spawner surveys)", "In the stream Oct 1 – Nov 30",
       "Not in the water now", "DFO surveys: 12 years, 2001–2024"],
    ]);
    expect(vm.noneOfSpecies).toBeNull();
    expect(`${vm.riverEntry} ${vm.km}`).toBe("PSF dates are when fish enter fresh water. This water is about 120 km "
      + "from the sea, so they reach it later.");
    expect(vm.credit).toBe("Run timing: Fisheries and Oceans Canada — Contains information licensed under the Open "
      + "Government Licence – Canada. Pacific Salmon Foundation (Salmon Watersheds Program) — Used with the written "
      + "permission of the Pacific Salmon Foundation.");
  });

  it("says no run of the species is recorded — never no fish", () => {
    const chinook = runSpecies(facts).find((s) => s.name === "Chinook")!;
    expect(runSheet(facts, 10, chinook, new Date(2026, 6, 5)).noneOfSpecies)
      .toBe("No chinook run is recorded on this water. That is not the same as no fish.");
  });
});
