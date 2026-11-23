// T552 item 4 (worker half, the twin of pipeline/gauges/feed/forecast.py `_query`): an ArcGIS answer that is broken is a
// FAILED model, never "out of season". ArcGIS answers HTTP 200 with {"error": …} and no features; read as zero rows, every
// run of the model was dropped with no failure recorded.
import { describe, expect, it } from "vitest";
import { fetchSummary, MODELS } from "../src/gauges/forecast";

const name = Object.keys(MODELS)[0]!;
const cfg = MODELS[name]!;
const run = async (body: unknown) => {
  const log: string[] = [], failed: string[] = [];
  const out = await fetchSummary(async () => new Response(typeof body === "string" ? body : JSON.stringify(body)), (s) => log.push(s), [name], failed);
  return { out, log, failed };
};

describe("forecast: a broken ArcGIS answer fails the model (T552 item 4)", () => {
  it("an error body, a non-object, a features that is not a list, a first row without the columns: failed, said", async () => {
    for (const [body, said] of [
      [{ error: { code: 498, message: "Invalid token." } }, `${cfg.service}: ArcGIS error 498 Invalid token.`],
      [{ error: "down" }, `${cfg.service}: ArcGIS error  down`],
      [[1, 2], `${cfg.service}: the answer is not an object`],
      [{ features: null }, `${cfg.service}: the answer has no features list`],
      [{}, `${cfg.service}: the answer has no features list`],
      [{ features: [{ attributes: { Station_ID: "08MF005" } }] }, `${cfg.service}: the rows lack ${cfg.issued} — has the layer changed?`],
      [{ features: [{ attributes: { [cfg.issued]: 1 } }] }, `${cfg.service}: the rows lack Station_ID — has the layer changed?`],
    ] as const) {
      const r = await run(body);
      expect(r.failed, JSON.stringify(body)).toEqual([name]);
      expect(r.out).toEqual({});
      expect(r.log.join("\n")).toContain(said);
    }
  });

  it("an honest empty layer (features: [], no error) is out of season, not a failure", async () => {
    const r = await run({ features: [] });
    expect(r.failed).toEqual([]);
    expect(r.out).toEqual({});
  });
});
