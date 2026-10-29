// T225 gauge parity over any recorded fixture dir (the committed small set, or the full roster in fixtures-full/):
// Python publisher (harness.py replay) and runGauges on the same recording, seed and clock; every output file diffed.
//   node app/workers/feeds/parity/gauges/run.mjs parity/gauges/parity.ts app/workers/feeds/fixtures-full/gauges [--notify URL]
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { runGauges } from "../../src/gauges/publish";
import { compareTrees, dirStore, fixtureFetch, loadRecording, readTree, runPython, seedInto } from "./node";

export async function parity(fixtures: string, notify?: string) {
  const work = join(tmpdir(), `gauges-parity-${process.pid}`);
  const t0 = Date.now();
  runPython(fixtures, join(work, "py"), notify);
  const pyMs = Date.now() - t0;
  seedInto(fixtures, join(work, "ts"));
  const rec = loadRecording(fixtures);
  const ff = fixtureFetch(rec);
  const lines: string[] = [];
  const t1 = Date.now();
  const summary = await runGauges({
    fetch: ff.fetch,
    feed: dirStore(join(work, "ts/feed")),
    priv: dirStore(join(work, "ts/priv")),
    now: new Date(rec.now),
    roster: JSON.parse(readFileSync(join(fixtures, "roster.json"), "utf-8")),
    notifyUrl: notify ?? null,
    log: (s) => lines.push(s),
    sleep: async () => {},
    changeAware: false, // the change-aware skip is tested on its own (test/gauges.robust.test.ts)
  });
  const tsMs = Date.now() - t1;
  const py = readTree(join(work, "py/feed"));
  for (const [k, v] of readTree(join(work, "py/priv"))) py.set(`priv/${k}`, v);
  const ts = readTree(join(work, "ts/feed"));
  for (const [k, v] of readTree(join(work, "ts/priv"))) ts.set(`priv/${k}`, v);
  const intended = normalizeIntended(py, ts);
  const diffs = compareTrees(py, ts);
  const pyPosts = JSON.parse(readFileSync(join(work, "py/posts.json"), "utf-8"));
  const pyMisses = JSON.parse(readFileSync(join(work, "py/misses.json"), "utf-8"));
  return { diffs, intended, summary, posts: { py: pyPosts, ts: ff.posts }, misses: { py: pyMisses, ts: ff.misses }, lines, pyMs, tsMs, work };
}

const isMain = /run\.mjs$/.test(process.argv[1] ?? "");
if (isMain && process.argv[2]) {
  const fixtures = resolve(process.argv[2]);
  const ni = process.argv.indexOf("--notify");
  const r = await parity(fixtures, ni > 0 ? process.argv[ni + 1] : undefined);
  const by: Record<string, number> = {};
  const classes: Record<string, number> = {};
  for (const d of r.diffs) {
    by[d.kind] = (by[d.kind] ?? 0) + 1;
    if (d.kind === "bytes-only") for (const c of (d.detail ?? "").split("; ")) classes[c] = (classes[c] ?? 0) + 1;
  }
  console.log(JSON.stringify({ files: r.diffs.length, by, classes, pyMs: r.pyMs, tsMs: r.tsMs, misses: r.misses, postsEqual: JSON.stringify(r.posts.py) === JSON.stringify(r.posts.ts), summary: { ...r.summary, gapsBeyondWindow: Object.keys(r.summary.gapsBeyondWindow).length } }, null, 1));
  const sem = r.diffs.filter((d) => d.kind !== "identical" && d.kind !== "bytes-only");
  for (const d of r.diffs.filter((d) => d.kind === "ulp")) console.log("ulp", d.file, d.detail);
  for (const d of sem.filter((d) => d.kind !== "ulp").slice(0, 40)) console.log(d.kind, d.file, d.detail ?? "");
  if (sem.length > 40) console.log(`... ${sem.length - 40} more`);
  console.log("work dir:", r.work);
}

/** The T225 follow-up's INTENDED differences from the Python, folded back so the rest still diffs exactly (each one is
 * checked here, not just dropped):
 *   - temp_history.json is private now (priv/temp_history.json; Python: the published feed dir), and the legacy public
 *     copy is removed;
 *   - the runs archive is append-only: priv/runs/<m>/<d>/<t>.jsonl parts (+ day objects) and runs/state.json; month file
 *     + days + parts concatenated in key order must equal Python's rewritten month file;
 *   - index.json `forecastIssuedAt` is written once, the newest run feed-wide (T68); Python's second key (T52: the newest
 *     among runs with a change block) overwrote it — ts must be the same or newer. */
export function normalizeIntended(py: Map<string, string>, ts: Map<string, string>): string[] {
  const notes: string[] = [];
  const th = ts.get("priv/temp_history.json");
  if (th !== undefined) {
    if (ts.has("temp_history.json")) throw new Error("temp_history.json is still published");
    ts.delete("priv/temp_history.json");
    ts.set("temp_history.json", th);
    notes.push("temp_history.json: private");
  }
  ts.delete("priv/runs/state.json");
  ts.delete("priv/stations.cache.json");
  const pieces = [...ts.keys()].filter((k) => /^priv\/runs\/\d{4}-\d{2}\//.test(k)).sort();
  for (const k of pieces) {
    const month = k.split("/")[2]!;
    const mk = `priv/runs/${month}.jsonl`;
    ts.set(mk, (ts.get(mk) ?? "") + ts.get(k)!);
    ts.delete(k);
  }
  if (pieces.length) notes.push(`runs: ${pieces.length} append-only object(s) folded into the month file`);
  const pi = py.get("index.json");
  const ti = ts.get("index.json");
  if (pi && ti) {
    const p = JSON.parse(pi);
    const t = JSON.parse(ti);
    if (p.forecastIssuedAt !== t.forecastIssuedAt) {
      if (p.forecastIssuedAt && !(t.forecastIssuedAt >= p.forecastIssuedAt)) throw new Error(`forecastIssuedAt older than Python's: ${t.forecastIssuedAt} < ${p.forecastIssuedAt}`);
      notes.push(`forecastIssuedAt: py ${p.forecastIssuedAt} (T52's) → ts ${t.forecastIssuedAt} (T68's)`);
      p.forecastIssuedAt = t.forecastIssuedAt;
      py.set("index.json", JSON.stringify(p));
    }
  }
  return notes;
}
