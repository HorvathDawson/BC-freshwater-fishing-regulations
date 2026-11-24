/**
 * (T1039) THE CLIMATOLOGY THE SITE BUILD WRITES INTO EACH GAUGE PAGE (`readGaugeClim`): every station's `gauge_clim` rows, from
 * the file that holds the table in this release (climate; conditions before T182), the quantities a chart is drawn in, and
 * a row that is not a blob refused (never written as "no envelope").
 */
import { describe, expect, it } from "vitest";
import type { Db, Row } from "../sql/db";
import { readGaugeClim } from "./gaugeRows";
import type { ReleaseLoader } from "./loader";

const A = Uint8Array.from([1, 2]), B = Uint8Array.from([3]), C = Uint8Array.from([4, 5, 6]);
function loader(files: string[], rows: Row[]): { l: ReleaseLoader; opened: string[] } {
  const opened: string[] = [];
  const db = async (name: string): Promise<Db> => {
    opened.push(name);
    const all = async (sql: string) => (sql.includes("FROM gauge_clim") ? rows : []);
    return { all, get: async (sql: string) => (await all(sql))[0], close: () => {} };
  };
  return { l: { manifest: { files: Object.fromEntries(files.map((f) => [f, {}])) }, db } as unknown as ReleaseLoader, opened };
}

describe("readGaugeClim", () => {
  const rows: Row[] = [{ station: "08MH001", parameter: "discharge", bands: A }, { station: "08MH001", parameter: "level", bands: B },
                       { station: "08MH001", parameter: "temperature", bands: C }, { station: "08NM240", parameter: "discharge", bands: C }];

  it("groups each station's rows, the chart's quantities only, read from the climate file", async () => {
    const { l, opened } = loader(["conditions", "climate"], rows);
    const got = await readGaugeClim(l);
    expect(opened).toEqual(["climate"]);
    expect(got).toEqual(new Map([["08MH001", [{ parameter: "discharge", bands: A }, { parameter: "level", bands: B }]],
                                 ["08NM240", [{ parameter: "discharge", bands: C }]]]));
  });

  it("a release cut before the climate file reads the table from conditions; one with no conditions file has none", async () => {
    const old = loader(["conditions"], rows);
    expect((await readGaugeClim(old.l))!.size).toBe(2);
    expect(old.opened).toEqual(["conditions"]);
    expect(await readGaugeClim(loader(["core"], rows).l)).toBeNull();
  });

  it("a row whose bands are not a blob is refused, naming the station", async () => {
    await expect(readGaugeClim(loader(["conditions", "climate"], [{ station: "08XX001", parameter: "discharge", bands: "x" }]).l))
      .rejects.toThrow(/gauge_clim 08XX001 discharge: bands is not a blob/);
  });
});
