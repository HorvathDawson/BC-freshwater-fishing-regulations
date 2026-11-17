// T830: the index's `reports` — which quantities a station reported live, ranked or not — is publish.live_quantities.
// The cases are run through the repo's Python and the port must give the same lists (FAILS without .venv, never skips);
// then a run on the recording writes it for every station from the reading itself, and a 304 run keeps it (the
// change-aware cache test in gauges.robust compares whole entries). The recording's stations all have both bands, so the
// run's copy of clim.json takes 07FB002's level band away: Capilano's case, a level read live with nothing to rank it.
// Mutation pins: dropping the other-datum test in `liveQuantities` fails the vectors; writing `reports` from the ranks
// fails the recording run.
import { execFileSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import { liveQuantities, runGauges } from "../src/gauges/publish";
import { dirStore, fixtureFetch, loadRecording, needPython, PY, REPO } from "../parity/gauges/node";

const FIX = join(__dirname, "fixtures/gauges");
type Case = [number | null, number | null, boolean];
const CASES: Case[] = [
  [25.7, 1.187, false],    // Capilano at Canyon: both read, a flow band only
  [null, 0.308, false],    // Nazko: a level only
  [12, null, false],
  [null, null, false],
  [118, 2.338, true],      // 08HD006: its level on another datum (T550)
  [null, 2.338, true],
  [0, 0, false],           // a zero is a reading
];

describe("reports: the port gives Python's lists", () => {
  beforeAll(() => void needPython());
  it("for every case", () => {
    const code = "import json,sys\nfrom pipeline.gauges.feed.publish import live_quantities\n"
      + "print(json.dumps([live_quantities(q, h, level_off_datum=o) for q, h, o in json.loads(sys.argv[1])]))";
    const py = JSON.parse(execFileSync(PY, ["-c", code, JSON.stringify(CASES)], { cwd: REPO, encoding: "utf-8" }));
    expect(CASES.map(([q, h, o]) => liveQuantities(q, h, o))).toEqual(py);
    expect(py[0]).toEqual(["discharge", "level"]);
    expect(py[4]).toEqual(["discharge"]);
  });
});

describe("reports on a run of the recording", () => {
  it("is what each station's reading carries, ranked or not", async () => {
    const rec = loadRecording(FIX);
    const dir = mkdtempSync(join(tmpdir(), "gauges-reports-"));
    cpSync(join(FIX, "seed"), dir, { recursive: true });
    const climPath = join(dir, "feed/clim.json");
    const clim = JSON.parse(readFileSync(climPath, "utf-8"));
    delete clim.stations["07FB002"].level;
    writeFileSync(climPath, JSON.stringify(clim));
    const feed = dirStore(join(dir, "feed"));
    await runGauges({ fetch: fixtureFetch(rec).fetch, feed, priv: dirStore(join(dir, "priv")), now: new Date(rec.now),
                      sleep: async () => {}, changeAware: false,
                      roster: JSON.parse(readFileSync(join(FIX, "roster.json"), "utf-8")) });
    const idx = JSON.parse((await feed.get("index.json"))!);
    const unranked: string[] = [];
    for (const [st, e] of Object.entries<Record<string, unknown>>(idx.stations)) {
      const now = JSON.parse((await feed.get(`${st}.json`))!).now;
      const want = (["discharge", "level"] as const).filter((q) => now[q] !== null);
      expect(e.reports, st).toEqual(want);
      expect(Object.keys(e).slice(0, 3), st).toEqual(["percentile", "parameter", "reports"]);
      if (want.some((q) => !(q in e))) unranked.push(st);
    }
    expect(unranked).toEqual(["07FB002"]);               // a level reported with no band to rank it
    expect(idx.stations["07FB002"].reports).toEqual(["discharge", "level"]);
  });
});
