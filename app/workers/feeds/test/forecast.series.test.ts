// (T994 #17, the worker side) ONLY a 404 is "not in season": a 5xx or a network failure throws out of fetchSeries, and
// withSeries counts and says it in the log instead of reading an outage as a province with no forecasts.
import { describe, expect, it } from "vitest";
import { fetchSeries, MODELS, withSeries } from "../src/gauges/forecast";
import type { Fetch } from "../src/io";

const MODEL = Object.keys(MODELS)[0]!;
const answer = (status: number): Fetch => (async () => new Response("x", { status })) as unknown as Fetch;
const down: Fetch = (async () => { throw new TypeError("fetch failed"); }) as unknown as Fetch;
const summary = () => ({ "08MF005": { [MODEL]: { issuedAt: "2026-11-21T08:00:00-08:00" } } }) as any;

describe("forecast series: only a 404 is not in season", () => {
  it("a 404 is null (not in season)", async () => {
    expect(await fetchSeries(answer(404), "08MF005", MODEL)).toBeNull();
  });
  it("a 5xx throws, naming the status", async () => {
    await expect(fetchSeries(answer(503), "08MF005", MODEL)).rejects.toThrow("HTTP 503");
  });
  it("a network failure throws", async () => {
    await expect(fetchSeries(down, "08MF005", MODEL)).rejects.toThrow("fetch failed");
  });
  it("withSeries says a failed request and keeps going; a 404 is not a failure", async () => {
    for (const [fetch, failed] of [[answer(503), true], [down, true], [answer(404), false]] as const) {
      const lines: string[] = [];
      const out = await withSeries(fetch, summary(), null, null, null, (s) => lines.push(s));
      expect(out["08MF005"]?.[MODEL]?.series).toBeUndefined();
      expect(lines.some((l) => l.includes("FAILED (not a 404"))).toBe(failed);
    }
  });
});
