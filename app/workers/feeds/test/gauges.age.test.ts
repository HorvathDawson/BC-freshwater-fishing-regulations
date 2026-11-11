// T550 item 1: a reading's age gates its rank. An entry whose last reading is older than core's READING_MAX_AGE_MS when
// the index is written is unranked (percentile null, no per-quantity rank), whether it was fresh, unchanged (304) or
// carried (`stale`); the run counts them (`oldReadings`) and the share card shows no standing for it.
import { cpSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { READING_MAX_AGE_MS } from "../../../packages/core/src/freshness";
import { cardModel } from "../src/gauges/card/card";
import { ageGate, runGauges } from "../src/gauges/publish";
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
