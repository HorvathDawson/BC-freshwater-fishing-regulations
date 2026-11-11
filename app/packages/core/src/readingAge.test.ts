// T550 item 1: one age rule for a gauge's own reading (`readingCurrent`, `READING_MAX_AGE_MS`), read by the donor
// state, the alerts' gate and (through core's copy) the worker that writes the feed.
import { describe, expect, it } from "vitest";
import { ALERT_STALE_MS } from "./alerts";
import { donorState } from "./conditions";
import { READING_MAX_AGE_MS, readingCurrent } from "./freshness";
import { READING_MAX_AGE_HOURS } from "./gauge-policy.generated";
import { STALE_AFTER } from "./staleness";

const HOUR = 3_600_000;
const AT = "2026-11-08T04:40:00-08:00";
const T = Date.parse(AT);

describe("readingCurrent", () => {
  it("is the publisher's hours, and the alerts' gate is the same line", () => {
    expect(READING_MAX_AGE_MS).toBe(READING_MAX_AGE_HOURS * HOUR);
    expect(STALE_AFTER.reading.ms).toBe(READING_MAX_AGE_MS);
    expect(ALERT_STALE_MS).toBe(READING_MAX_AGE_MS);
  });

  it("holds to the line, the line included", () => {
    expect(readingCurrent(AT, T + READING_MAX_AGE_MS)).toBe(true);
    expect(readingCurrent(AT, T + READING_MAX_AGE_MS + 1)).toBe(false);
    expect(readingCurrent(AT, T + 72 * HOUR)).toBe(false);
  });

  it("leaves a naive, missing or unreadable stamp unjudged, as the Python publisher does", () => {
    expect(readingCurrent("2026-11-08T04:40:00", T + 72 * HOUR)).toBe(true);
    expect(readingCurrent(null, T)).toBe(true);
    expect(readingCurrent("nope", T + 72 * HOUR)).toBe(true);
    expect(readingCurrent(AT, null)).toBe(true);
  });
});

describe("donorState of an unranked reading", () => {
  const row = { percentile: null, observedAt: AT };
  it("speaks (unranked) while its reading is current", () => {
    expect(donorState(row, { ref: T + 2 * HOUR }).kind).toBe("unranked");
  });
  it("is quiet once its reading is past the line (it was 'unranked' for a whole day before T550)", () => {
    expect(donorState(row, { ref: T + 10 * HOUR }).kind).toBe("quiet");
    expect(donorState(row, { ref: T + 72 * HOUR }).kind).toBe("quiet");
  });
});
