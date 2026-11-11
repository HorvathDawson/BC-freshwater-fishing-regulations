// (T557 item 3 / T633 item 3) The gauge feed contract (app/tools/gauge-feed-contract.ts) on the worker's OWN output: a
// run on the committed recording, then index.json, obs.json and every station file the index lists are held to the same
// check the app's fixture feed is (tools/feed-contract.test.ts). A field the worker renames or adds fails here.
import { cpSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { gaugeFeedContract } from "../../../tools/gauge-feed-contract";
import { parseHourly, runGauges } from "../src/gauges/publish";
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

// (T554 item 1) `recent` ends on the newest reading: thinned from the head (every 6th row), the newest row could trail
// `at` by up to 25 min, and obs.json and an alert's check read the older value.
describe("a station's recent rows", () => {
  it("are every sixth row and then the newest reading with a number", () => {
    const head = "ID,Date,Water Level / Niveau d'eau (m),Discharge / Débit (cms)\n";
    const rows = Array.from({ length: 10 }, (_, i) => `08MF005,2026-11-11T0${i}:00:00Z,${1 + i / 10},${10 + i}`);
    const csv = head + [...rows, "08MF005,2026-11-11T10:00:00Z,,"].join("\n") + "\n";
    const r = parseHourly("08MF005", csv)!;
    expect(r.at).toBe("2026-11-11T09:00:00Z");
    expect(r.recent.map((x) => x[0])).toEqual(["2026-11-11T00:00:00Z", "2026-11-11T06:00:00Z", "2026-11-11T09:00:00Z"]);
    expect(r.recent.at(-1)).toEqual(["2026-11-11T09:00:00Z", 1.9, 19]);
  });
});
