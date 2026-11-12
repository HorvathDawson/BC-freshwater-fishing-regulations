/**
 * (T585 F8, F14) THE CARD'S READING CARRIES WHAT THE FEED SAID ABOUT ITS RANK AND ITS COPY: `rankedAt` (this figure is not
 * the reading the index ranked) and `kept` (the phone's offline copy) reach the view the card is written from, and the
 * finished lines say them — from the web's feed and the phone's alike (one `stationReading`, one `readingStanding`).
 */
import { describe, expect, it } from "vitest";
import { freshness, readingAgeLine, readingStanding, type Standing } from "@app/core";
import type { Aged, Reading, StationId, WaterSource } from "@app/data";
import { stationReading } from "./gauge";

const ST = "08MH001" as StationId;
const VAN = "America/Vancouver";
const reading = (o: Partial<Reading>): Reading => ({
  discharge: 12.5, level: null, at: "2026-09-01T11:30:00Z", percentile: 0.5, standing: "normal" as Standing, parameter: "discharge", ...o });
const source = (now: Aged<Reading> | null) => ({ gaugeNow: async () => now }) as unknown as WaterSource;
const lines = async (now: Aged<Reading>) => {
  const c = await stationReading(source(now), ST);
  return {
    standing: readingStanding({ percentile: c.percentile, standing: c.standing as Standing | null,
                                ...(c.rankedAt !== undefined ? { rankedAt: c.rankedAt } : {}), observedAt: c.observedAt }, VAN),
    age: readingAgeLine(freshness(c.fetchedAt, Date.parse("2026-09-01T12:00:00Z")), c.observedAt, VAN, !!c.kept),
  };
};

describe("stationReading → the card's two lines", () => {
  const fetchedAt = Date.parse("2026-09-01T11:50:00Z");

  it("the ranked reading, live: its standing and this gauge's own reading", async () => {
    const c = await stationReading(source({ fetchedAt, value: reading({}) }), ST);
    expect("rankedAt" in c || "kept" in c).toBe(false);
    expect(await lines({ fetchedAt, value: reading({}) })).toEqual({
      standing: "Normal · lower than 5 days in 10 for the date",
      age: "This gauge's own reading · observed 2026-09-01 at 04:30 PDT · checked 10 min ago" });
  });

  it("F8: a station file a run ahead of the index — no rank of its own, both times said", async () => {
    expect(await lines({ fetchedAt, value: reading({ percentile: null, rankedAt: "2026-09-01T10:30:00Z" }) })).toEqual({
      standing: "Ranked at 2026-09-01 at 03:30 PDT; this reading 2026-09-01 at 04:30 PDT.",
      age: "This gauge's own reading · observed 2026-09-01 at 04:30 PDT · checked 10 min ago" });
  });

  it("F14: offline, the kept ranked reading keeps its standing and says it is the phone's copy", async () => {
    expect(await lines({ fetchedAt: Date.parse("2026-09-01T06:00:00Z"), kept: true, value: reading({}) })).toEqual({
      standing: "Normal · lower than 5 days in 10 for the date",
      age: "Offline — reading saved on this phone · observed 2026-09-01 at 04:30 PDT · last checked 6h ago, may be out of date" });
  });

  it("F14: offline, a newer kept row than the ranked one — both times, on the phone's copy, never 'no record'", async () => {
    const got = await lines({ fetchedAt: Date.parse("2026-09-01T06:00:00Z"), kept: true,
                              value: reading({ percentile: null, rankedAt: "2026-09-01T10:30:00Z" }) });
    expect(got.standing).toBe("Ranked at 2026-09-01 at 03:30 PDT; this reading 2026-09-01 at 04:30 PDT.");
    expect(got.age).toBe("Offline — reading saved on this phone · observed 2026-09-01 at 04:30 PDT · last checked 6h ago, may be out of date");
  });

  it("a station with no record at all still says so", async () => {
    expect((await lines({ fetchedAt, value: reading({ percentile: null }) })).standing)
      .toBe("There is a reading here, but no record to compare it against.");
  });
});
