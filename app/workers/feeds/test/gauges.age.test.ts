// T550 item 1: a reading's age gates its rank. An entry whose last reading is older than core's READING_MAX_AGE_MS when
// the index is written is unranked (percentile null, no per-quantity rank), whether it was fresh, unchanged (304) or
// carried (`stale`); the run counts them (`oldReadings`) and the share card shows no standing for it.
import { cpSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { READING_MAX_AGE_MS } from "../../../packages/core/src/freshness";
import { cardModel } from "../src/gauges/card/card";
import { ageGate, levelOnRating, runGauges } from "../src/gauges/publish";
import { dirStore, fixtureFetch, loadRecording } from "../parity/gauges/node";

const FIX = join(__dirname, "fixtures/gauges");
const HOUR = 3_600_000;

describe("ageGate", () => {
  const entry = { percentile: 0.4, parameter: "discharge", discharge: 0.4, level: 0.6, observedAt: "2026-11-08T04:40:00-08:00", forecast: false };
  const at = Date.parse("2026-11-08T04:40:00-08:00");

  it("keeps a reading within the line, the line itself included", () => {
    expect(ageGate(entry, at + READING_MAX_AGE_MS)).toBe(entry);
  });

  it("a reading past the line (08HB022, 37 h on 2026-11-10) is written unranked, its time kept, key order kept", () => {
    const got = ageGate(entry, at + 37 * HOUR);
    expect(got).toEqual({ percentile: null, parameter: "discharge", observedAt: entry.observedAt, forecast: false });
    expect(Object.keys(got)).toEqual(["percentile", "parameter", "observedAt", "forecast"]);
  });

  it("a naive stamp is not judged (ECCC zones every stamp; Python leaves it too)", () => {
    const naive = { ...entry, observedAt: "2026-11-08T04:40:00" };
    expect(ageGate(naive, at + 72 * HOUR)).toBe(naive);
  });
});

describe("a run three days after the recording", () => {
  it("ranks none of its readings, counts them all old, and the station files say no rank either", async () => {
    const rec = loadRecording(FIX);
    const dir = mkdtempSync(join(tmpdir(), "gauges-age-"));
    cpSync(join(FIX, "seed"), dir, { recursive: true });
    const feed = dirStore(join(dir, "feed"));
    const s = await runGauges({
      fetch: fixtureFetch(rec).fetch, feed, priv: dirStore(join(dir, "priv")), changeAware: false,
      now: new Date(Date.parse(rec.now) + 3 * 24 * HOUR), sleep: async () => {},
      roster: JSON.parse(readFileSync(join(FIX, "roster.json"), "utf-8")),
    });
    expect(s.answered).toBeGreaterThan(10);
    expect(s.withPercentile).toBe(0);
    expect(s.oldReadings).toBe(s.answered);
    const idx = JSON.parse((await feed.get("index.json"))!);
    for (const [st, e] of Object.entries<any>(idx.stations)) {
      expect(e.percentile, st).toBeNull();
      expect("discharge" in e || "level" in e, st).toBe(false);
      expect(typeof e.observedAt).toBe("string");
      const file = JSON.parse((await feed.get(`${st}.json`))!);
      expect(file.now.percentile, st).toBeNull();
    }
  });

  it("the run at the recording's own time still ranks (the gate is not everything)", async () => {
    const rec = loadRecording(FIX);
    const dir = mkdtempSync(join(tmpdir(), "gauges-age-"));
    cpSync(join(FIX, "seed"), dir, { recursive: true });
    const s = await runGauges({
      fetch: fixtureFetch(rec).fetch, feed: dirStore(join(dir, "feed")), priv: dirStore(join(dir, "priv")), changeAware: false,
      now: new Date(rec.now), sleep: async () => {}, roster: JSON.parse(readFileSync(join(FIX, "roster.json"), "utf-8")),
    });
    expect(s.withPercentile).toBeGreaterThan(5);
    expect(s.oldReadings).toBeLessThan(s.answered);
  });
});

describe("the share card", () => {
  it("shows no standing for a reading past the line, and one for a fresh reading", () => {
    const at = "2026-11-08T04:40:00-08:00";
    const f = { fetchedAt: "2026-11-08T13:00:00Z", now: { discharge: 12, level: null, at, percentile: 0.4, parameter: "discharge" as const }, recent: [], daily: [], forecasts: {} };
    const t = Date.parse(at);
    expect(cardModel("08HB022", "Nile Creek", f as any, t + HOUR).standing).not.toBeNull();
    expect(cardModel("08HB022", "Nile Creek", f as any, t + 37 * HOUR).standing).toBeNull();
  });
});

// T550 item 2: a live level off the record's rating (another datum) is not ranked; the Python twin is
// pipeline/tests/test_level_datum.py (same rating, same pairs).
describe("levelOnRating", () => {
  const rating = [[2.36, 6.296, 6.409], [6.16, 6.388, 6.561], [14.7, 6.533, 6.686], [23.1, 6.631, 6.783],
    [32.04, 6.729, 6.896], [42.4, 6.816, 7.0], [54.46, 6.933, 7.143], [72.92, 7.053, 7.292],
    [97.06, 7.231, 7.562], [141.4, 7.502, 9.435], [958.0, 7.502, 9.435]]; // 08HD006 since 2016
  it("refuses 08HD006's live pair (2.338 m at 118 m³/s, staging 2026-11-10) and keeps one on the rating", () => {
    expect(levelOnRating(rating, 118, 2.338, 0.5)).toBe(false);
    expect(levelOnRating(rating, 118, 7.4, 0.5)).toBe(true);
  });
  it("has nothing to say with no pair or no rating, and refuses ratings without a pad", () => {
    expect(levelOnRating(rating, null, 2.338, 0.5)).toBe(true);
    expect(levelOnRating(undefined, 118, 2.338, undefined)).toBe(true);
    expect(() => levelOnRating(rating, 118, 2.338, undefined)).toThrow(/ratingPadM/);
  });
});
