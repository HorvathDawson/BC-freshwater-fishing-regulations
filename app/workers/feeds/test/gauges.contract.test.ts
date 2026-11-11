// (T557 item 3 / T633 item 3) The gauge feed contract (app/tools/gauge-feed-contract.ts) on the worker's OWN output: a
// run on the committed recording, then index.json, obs.json and every station file the index lists are held to the same
// check the app's fixture feed is (tools/feed-contract.test.ts). A field the worker renames or adds fails here.
import { cpSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { gaugeFeedContract } from "../../../tools/gauge-feed-contract";
import { runGauges } from "../src/gauges/publish";
import { dirStore, fixtureFetch, loadRecording } from "../parity/gauges/node";

const FIX = join(__dirname, "fixtures/gauges");

describe("the gauge feed contract on the worker's own output", () => {
  it("index.json, obs.json and every station file keep it", async () => {
    const rec = loadRecording(FIX);
    const dir = mkdtempSync(join(tmpdir(), "gauges-contract-"));
    cpSync(join(FIX, "seed"), dir, { recursive: true });
    const feed = dirStore(join(dir, "feed"));
    await runGauges({ fetch: fixtureFetch(rec).fetch, feed, priv: dirStore(join(dir, "priv")), now: new Date(rec.now),
                      sleep: async () => {}, roster: JSON.parse(readFileSync(join(FIX, "roster.json"), "utf-8")) });
    const files: Record<string, unknown> = {};
    for (const name of ["index.json", "obs.json"]) files[name] = JSON.parse((await feed.get(name))!);
    const stations = Object.keys((files["index.json"] as { stations: object }).stations);
    for (const st of stations) files[`${st}.json`] = JSON.parse((await feed.get(`${st}.json`))!);
    expect(stations.length).toBeGreaterThan(10);
    expect(gaugeFeedContract(files)).toEqual([]);
    // the check bites: a renamed field, an untagged file
    const idx = files["index.json"] as { stations: Record<string, Record<string, unknown>> };
    const [first] = stations;
    const renamed = { ...idx, stations: { ...idx.stations, [first!]: { ...idx.stations[first!], pct: 0.5 } } };
    expect(gaugeFeedContract({ ...files, "index.json": renamed }))
      .toEqual([`index.json ${first}: 'pct' is not a key the app reads`]);
    const { format: _f, ...untagged } = files[`${first}.json`] as Record<string, unknown>;
    expect(gaugeFeedContract({ ...files, [`${first}.json`]: untagged }))
      .toEqual([`${first}.json: the gauge feed's station file names no format; this app reads gauge-feed/1`]);
  });
});
