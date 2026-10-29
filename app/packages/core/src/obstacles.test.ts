/**
 * T173 — the fish obstacle tap sheet: whole sheets, never fragments (AGENTS.md rule 33).
 */
import { describe, expect, it } from "vitest";
import { OBSTACLE_TYPES, OBSTACLE_WORDS, dateWords, heightWords, obstacleSheet } from "./obstacles";

const NOTE = OBSTACLE_WORDS.note;
const LICENCE = OBSTACLE_WORDS.licence;

describe("obstacleSheet", () => {
  it("a surveyed falls: its type, its water, height, year and record", () => {
    expect(obstacleSheet({ obstacle_id: "fiss:10000", type: "falls", water: "Cranberry River", height_m: 4,
                           source: "fiss", year: 2001 })).toEqual({
      title: "Falls", sub: "On Cranberry River",
      rows: [{ label: "Height", value: "4 m" },
             { label: "Surveyed", value: "2001" },
             { label: "Source", value: "Provincial Obstacles to Fish Passage · FISS 10000" }],
      note: NOTE, licence: LICENCE,
    });
  });

  it("a registered dam: its name is the title, the type a row, the year commissioned", () => {
    expect(obstacleSheet({ obstacle_id: "wris:D120299-00", type: "dam", name: "Walker Lake Dam", height_m: 2.5,
                           source: "wris", year: 1962 })).toEqual({
      title: "Walker Lake Dam", sub: null,
      rows: [{ label: "Type", value: "Dam" },
             { label: "Height", value: "2.5 m" },
             { label: "Commissioned", value: "1962" },
             { label: "Source", value: "BC Dams (provincial dam register) · WRIS D120299-00" }],
      note: NOTE, licence: LICENCE,
    });
  });

  it("what the renderer hands back as text (a phone) reads the same; what is missing is left out", () => {
    expect(obstacleSheet({ type: "culvert", height_m: "1.5", year: "1996", source: "fiss" })).toEqual({
      title: "Culvert", sub: null,
      rows: [{ label: "Height", value: "1.5 m" }, { label: "Surveyed", value: "1996" },
             { label: "Source", value: "Provincial Obstacles to Fish Passage" }],
      note: NOTE, licence: LICENCE,
    });
    expect(obstacleSheet({}).title).toBe("Obstacle to fish");
    expect(obstacleSheet({ type: "weir", height_m: 0, name: " " }))
      .toEqual({ title: "Obstacle to fish", sub: null, rows: [], note: NOTE, licence: LICENCE });
  });

  it("an assessed stream crossing (PSCIS): barrier status, habitat, road, the date assessed, its crossing id", () => {
    expect(obstacleSheet({ obstacle_id: "pscis:13", type: "culvert", water: "Nass R. Trib.", source: "pscis",
                           year: 2008, barrier: "barrier", habitat: "low", road: "Brush Main", date: "2008-08-17" }))
      .toEqual({
        title: "Culvert", sub: "On Nass R. Trib.",
        rows: [{ label: "Fish passage", value: "Barrier" },
               { label: "Habitat upstream", value: "Low value" },
               { label: "Road", value: "Brush Main" },
               { label: "Assessed", value: "Aug 17, 2008" },
               { label: "Source", value: "Provincial Stream Crossing Inventory · PSCIS 13" }],
        note: NOTE, licence: LICENCE,
      });
    expect(obstacleSheet({ obstacle_id: "pscis:7", type: "culvert", source: "pscis", barrier: "potential", year: "2015" }))
      .toEqual({
        title: "Culvert", sub: null,
        rows: [{ label: "Fish passage", value: "Potential barrier" },
               { label: "Assessed", value: "2015" },
               { label: "Source", value: "Provincial Stream Crossing Inventory · PSCIS 7" }],
        note: NOTE, licence: LICENCE,
      });
    expect(["barrier", "potential", "passable", "unknown", "bogus"].map((b) =>
      obstacleSheet({ type: "culvert", source: "pscis", barrier: b }).rows[0]?.value))
      .toEqual(["Barrier", "Potential barrier", "Passable", "Not yet rated", "Provincial Stream Crossing Inventory"]);
  });

  it("dates: ISO to words, anything else null", () => {
    expect([dateWords("2008-09-10"), dateWords("2021-12-01Z"), dateWords("2008"), dateWords(null)])
      .toEqual(["Sep 10, 2008", "Dec 1, 2021", null, null]);
  });

  it("every type has its own word", () => {
    expect(OBSTACLE_TYPES.map((t) => obstacleSheet({ type: t }).title))
      .toEqual(["Falls", "Cascade", "Canyon", "Velocity barrier", "Culvert", "Dam"]);
  });

  it("heights: whole metres bare, else to a tenth", () => {
    expect([heightWords(12), heightWords(1.25), heightWords(0.3)]).toEqual(["12 m", "1.3 m", "0.3 m"]);
  });
});
