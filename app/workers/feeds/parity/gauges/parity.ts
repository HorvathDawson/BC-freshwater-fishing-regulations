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
  });
  const tsMs = Date.now() - t1;
  const py = readTree(join(work, "py/feed"));
  for (const [k, v] of readTree(join(work, "py/priv"))) py.set(`priv/${k}`, v);
  const ts = readTree(join(work, "ts/feed"));
  for (const [k, v] of readTree(join(work, "ts/priv"))) ts.set(`priv/${k}`, v);
  const diffs = compareTrees(py, ts);
  const pyPosts = JSON.parse(readFileSync(join(work, "py/posts.json"), "utf-8"));
  const pyMisses = JSON.parse(readFileSync(join(work, "py/misses.json"), "utf-8"));
  return { diffs, summary, posts: { py: pyPosts, ts: ff.posts }, misses: { py: pyMisses, ts: ff.misses }, lines, pyMs, tsMs, work };
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
