/**
 * (T1045) THE GAUGES A WATER PAGE LINKS (`readWaterGauges`): the water's own gauge, the gauge of a lake it is, the good gauges of
 * its sections; each with the quantities the release holds; core's `pickWaterGauges` (three at most, good guides only); a release
 * with no conditions file has none.
 */
import { describe, expect, it } from "vitest";
import type { Db, Row } from "../sql/db";
import { readWaterGauges } from "./gaugeRows";
import type { ReleaseLoader } from "./loader";

const seen: string[] = [];
function loader(files: string[], answer: (sql: string, args: unknown[]) => Row[]): ReleaseLoader {
  const db = async (): Promise<Db> => {
    const all = async (sql: string, ...args: unknown[]) => { seen.push(sql); return answer(sql, args); };
    return { all, get: async (sql: string, ...a: unknown[]) => (await all(sql, ...a))[0], close: () => {} };
  };
  return { manifest: { files: Object.fromEntries(files.map((f) => [f, {}])) }, db } as unknown as ReleaseLoader;
}
const answers = (sql: string, args: unknown[]): Row[] => {
  if (sql.includes("FROM gauge WHERE item_id")) return args[0] === "w1" ? [{ station: "S1", name: "Own River" }] : [];
  if (sql.includes("FROM lake_gauge")) return [{ station: "S2", name: "Lake Gauge" }];
  if (sql.includes("FROM item_section")) return [{ sid: 11 }, { sid: 12 }];
  if (sql.includes("FROM section_gauge")) return [{ station: "S3", name: "Bravo", trust: "good" }, { station: "S4", name: "Alpha", trust: "good" }, { station: "S5", name: "Echo", trust: "good" }];
  if (sql.includes("FROM gauge_clim")) return [{ station: "S1", parameter: "discharge" }, { station: "S1", parameter: "level" }, { station: "S3", parameter: "level" }, { station: "S3", parameter: "temperature" }];
  return [];
};

describe("readWaterGauges", () => {
  it("own, lake, then good sections by name, three at most; each with its parameters (the chart's only)", async () => {
    const got = await readWaterGauges(loader(["conditions", "climate"], answers), "w1");
    expect(seen.find((q) => q.includes("FROM section_gauge"))).toContain("sg.trust = 'good'");   // a fair or weak section guide is never read as a link
    expect(got.map((g) => [g.station, g.trust, g.parameters])).toEqual([["S1", "own", ["discharge", "level"]], ["S2", "lake", []], ["S4", "good", []]]);
  });
  it("a water no gauge reads has none; a release with no conditions file has none, and asks nothing", async () => {
    expect(await readWaterGauges(loader(["conditions", "climate"], () => []), "w9")).toEqual([]);
    let asked = 0;
    expect(await readWaterGauges(loader(["core"], () => { asked++; return []; }), "w1")).toEqual([]);
    expect(asked).toBe(0);
  });
});
