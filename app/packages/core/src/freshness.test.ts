import { describe, expect, it } from "vitest";
import { freshness, readingAgeLine, STALE_AFTER_MS } from "./freshness";

const MIN = 60_000, NOW = Date.parse("2026-09-01T12:00:00Z");

describe("freshness", () => {
  it("never reports a never-fetched value as live", () => {
    expect(freshness(null, 1000, 100)).toEqual({ state: "unknown" });
  });
  it("distinguishes stale from live", () => {
    expect(freshness(0, 50, 100).state).toBe("live");
    expect(freshness(0, 500, 100).state).toBe("stale");
  });
  it("defaults to the feed's own threshold (P02): 10 minutes is live, 3 hours is stale", () => {
    expect(STALE_AFTER_MS).toBe(2 * 60 * MIN);
    expect(freshness(NOW - 10 * MIN, NOW)).toEqual({ state: "live", ageMs: 10 * MIN });
    expect(freshness(NOW - 180 * MIN, NOW)).toEqual({ state: "stale", ageMs: 180 * MIN });
    expect(freshness(null, NOW)).toEqual({ state: "unknown" });
  });
});

describe("readingAgeLine (P02): the finished sentence, whole (AGENTS 33)", () => {
  const at = "2026-09-01T11:30:00Z";
  // LOCAL AND LABELLED (P16): said in a named zone so the test does not depend on the host's.
  const VAN = "America/Vancouver";
  it("live", () => {
    expect(readingAgeLine(freshness(NOW - 10 * MIN, NOW), at, VAN))
      .toBe("This gauge's own reading · observed 2026-09-01 at 04:30 PDT · checked 10 min ago");
  });
  it("stale says so", () => {
    expect(readingAgeLine(freshness(NOW - 180 * MIN, NOW), at, VAN))
      .toBe("This gauge's own reading · observed 2026-09-01 at 04:30 PDT · last checked 3h ago, may be out of date");
  });
  it("unknown never reads as live", () => {
    expect(readingAgeLine(freshness(null, NOW), at, VAN))
      .toBe("This gauge's own reading · observed 2026-09-01 at 04:30 PDT · age unknown");
  });
  it("a missing or malformed observation time is left out, not invented", () => {
    expect(readingAgeLine(freshness(null, NOW), null)).toBe("This gauge's own reading · age unknown");
    expect(readingAgeLine(freshness(null, NOW), "garbage")).toBe("This gauge's own reading · age unknown");
  });
  it("days past two days", () => {
    expect(readingAgeLine(freshness(NOW - 3 * 24 * 60 * MIN, NOW), null))
      .toBe("This gauge's own reading · last checked 3d ago, may be out of date");
  });
});
