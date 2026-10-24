/**
 * T125 item 6 — THE GAUGE POLICY CONSTANTS ARE GENERATED OR READ, NEVER TYPED A SECOND TIME.
 *
 * The percentile knots (the climatology's PCTILES), the panel's refusal floor (panel.py MIN_TOTAL_WEIGHT) and the
 * forecast days (publish.py HORIZONS) come into core from `gauge-policy.generated` (`emit_gauge_policy --check` holds
 * that file to the Python). What this pins is that core READS them — the standing words, the ranking, the horizons —
 * and that the map's percentile ramp stops at the same knots. And the feed's lifetime is one constant (`FEED_TTL_MS`):
 * the answers cache, the sync poll, the map's feed poll and the reading poll were four `5 * 60_000`s.
 *
 * MUTATIONS: re-type `const KNOTS = [0.10, …]` in flow.ts, `MIN_TOTAL_WEIGHT = 0.05` in trust.ts, a horizon list in
 * conditions.ts or a `5 * 60_000` in any of the four, or move a ramp stop off a knot, and a test here fails.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { DAY_HORIZONS, HORIZONS, percentileInBand, standing } from "@app/core";
import { FEED_TTL_MS } from "@app/data";
import { ANSWER_TTL_MS, FEED_POLL_MS, READING_POLL_MS, SYNC_POLL_MS } from "@app/ui";
import { FORECAST_DAYS, MIN_TOTAL_WEIGHT, PERCENTILE_KNOTS } from "../packages/core/src/gauge-policy.generated";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const read = (p: string) => readFileSync(join(ROOT, p), "utf8");

describe("the gauge policy constants", () => {
  it("are the Python's", () => {
    const pct = /PCTILES = \(([^)]*)\)/.exec(read("pipeline/gauges/feed/climatology.py"))![1]!;
    expect(PERCENTILE_KNOTS.map((k) => Math.round(k * 100))).toEqual(pct.split(",").map((x) => Number(x)));
    expect(MIN_TOTAL_WEIGHT).toBe(Number(/MIN_TOTAL_WEIGHT = ([\d.]+)/.exec(read("pipeline/gauges/consume/panel.py"))![1]));
    const days = /^HORIZONS = \(([^)]*)\)/m.exec(read("pipeline/gauges/feed/publish.py"))![1]!;
    expect([...FORECAST_DAYS]).toEqual(days.split(",").map((x) => Number(x)));
  });

  it("are what core reads: the horizons, the standing breaks, the ranking's knots", () => {
    expect(DAY_HORIZONS).toEqual([0, ...FORECAST_DAYS]);
    expect(HORIZONS).toEqual([0, ...FORECAST_DAYS, "change"]);
    const [p10, p25, , p75, p90] = PERCENTILE_KNOTS;
    expect([p10 - 1e-9, p10, p25, p75, p90].map(standing)).toEqual(["much-below", "below", "normal", "above", "much-above"]);
    // a value at each knot of a band ranks at that knot
    const band = [10, 20, 30, 40, 50] as const;
    expect([20, 30, 40].map((v) => percentileInBand(v, band))).toEqual(PERCENTILE_KNOTS.slice(1, 4));
  });

  it("are never typed again in core", () => {
    expect(read("app/packages/core/src/flow.ts")).not.toMatch(/KNOTS = \[/);
    expect(read("app/packages/core/src/flow.ts")).not.toMatch(/percentile < 0\.\d/);
    expect(read("app/packages/core/src/trust.ts")).not.toMatch(/MIN_TOTAL_WEIGHT = /);
    expect(read("app/packages/core/src/conditions.ts")).not.toMatch(/HORIZONS: readonly Horizon\[\] = \[0, 1/);
  });

  it("put the map's percentile ramp stops on the knots", () => {
    const style = JSON.parse(read("app/packages/map/style/layers.source.json"));
    const ramps: number[][] = [];
    const walk = (o: unknown): void => {
      if (Array.isArray(o)) { o.forEach(walk); return; }
      if (o && typeof o === "object") {
        const r = o as Record<string, unknown>;
        if (r.property === "standing" && Array.isArray(r.stops))
          ramps.push((r.stops as [number, unknown][]).map((s) => s[0]).filter((v) => v > 0 && v < 100));
        Object.values(r).forEach(walk);
      }
    };
    walk(style);
    expect(ramps.length).toBeGreaterThan(0);
    for (const r of ramps) expect(r).toEqual(PERCENTILE_KNOTS.map((k) => Math.round(k * 100)));
  });
});

describe("the feed lifetime", () => {
  it("is one constant", () => {
    expect([ANSWER_TTL_MS, SYNC_POLL_MS, FEED_POLL_MS, READING_POLL_MS]).toEqual([FEED_TTL_MS, FEED_TTL_MS,
      FEED_TTL_MS, FEED_TTL_MS]);
    for (const f of ["ui/src/answers.ts", "ui/src/sync.ts", "ui/src/conditionsColour.ts", "ui/src/hooks/gauge.ts"])
      expect(read(`app/packages/${f}`), f).not.toMatch(/=\s*5 \* 60_000/);
  });
});
