#!/usr/bin/env node
/**
 * (T768) THE PART-END PROOF: a new part end on a fixture water fails exactly ONE test file, the content pin.
 *
 *   scripts/heavy.sh t768-proof 30 -- node app/tools/part-end-proof.mjs      # the whole app suite on the mutant
 *   node app/tools/part-end-proof.mjs <vitest file filters…>                 # a subset (says PARTIAL)
 *   node app/tools/part-end-proof.mjs --write-only DIR                       # only write the mutant into DIR
 *
 * Why: the release of r-46e4355a7277 stopped on twelve app tests that had typed the shape of four waters ("Part 2 of
 * 18", lists of part names); T768 moved every test onto the water's group (`loadWaterGroup`, `partName`, parts found by
 * their ends) and left ONE content pin per water, `conformance/src/regs/waterGroup.fixture.test.ts`. This tool is that
 * claim's proof, rerunnable: a release that cuts a water once more must stop on that file and nowhere else.
 *
 * THE MUTATION (one made-up end, the way the release would ship it): the regs fixture's Chilliwack (`gnis:8634`) part
 * whose `runs` hold two pieces — the stretch from the Slesse Creek signs to the Ecological Reserve, broken by Chilliwack
 * Lake — becomes two parts at the lake: the lower one ends at `lake_outlet:<lake>`, the upper one starts at
 * `lake_inlet:<lake>` (both already end tokens of `display.ends`), with a mark between them. Every row that names the
 * water's parts follows: `part` (the parts above it renumbered, a new `ord`, the `runs` split), `part_section`,
 * `part_line`, `part_mark`, the item's `picker` choice (its parts, "· N stretches"), `part_geo`; each file's manifest
 * entry is rewritten (bytes, sha256, gz, url) so the loader holds the copy to its manifest as it holds a release.
 *
 * THE RUN: vitest over the app with `REGS_FIXTURE_MUTANT` naming the copy (`releaseDir.ts` honours it only for a folder
 * holding this tool's `MUTANT.json`). Exit 0 iff the failed files are exactly the content pin, besides the fixture's
 * PROVENANCE checks, which hold the committed copy to the release it was cut from and so see any copy that is not that
 * cut (listed, never counted), AND the run ended whole: no unhandled error and not interrupted (this file's default
 * export, a vitest reporter, records both; vitest's JSON report says `success: true` with every file passed beside an
 * unhandled error, so a run whose only other failure is one would otherwise be met). Output is vitest's own (never
 * through rtk: it has reported false passes).
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { gunzipSync, gzipSync, inflateRawSync } from "node:zlib";

const APP = join(dirname(fileURLToPath(import.meta.url)), "..");
const ROOT = join(APP, "packages/data/dev/regs-release");
export const WATER = "gnis:8634";
/** the one file that pins each fixture water's parts by content */
export const PIN = "conformance/src/regs/waterGroup.fixture.test.ts";
/** files that hold the committed fixture to the release it was cut from: any copy that is not that cut fails them */
export const PROVENANCE = { "tools/regs-fixture.test.ts": "rebuilds the committed fixture from its source release and compares bytes" };

/** The fixture release folder under `root` (exactly one `r-*`). */
function releaseDir(root) {
  const dirs = readdirSync(root).filter((n) => /^r-[0-9a-f]{12}$/.test(n));
  if (dirs.length !== 1) throw new Error(`part-end proof: ${root} holds ${dirs.length} releases, not 1`);
  return join(root, dirs[0]);
}

/** A blob's JSON (the store's codec: 0x01 raw deflate, 0x02 with the table's dictionary, else the JSON text). */
function unpack(db, tbl, b) {
  const buf = Buffer.from(b);
  if (buf[0] === 1) return JSON.parse(inflateRawSync(buf.subarray(1)).toString("utf8"));
  if (buf[0] === 2) {
    const d = db.prepare("SELECT d FROM zdict WHERE tbl = ?").get(tbl);
    if (!d) throw new Error(`part-end proof: table ${tbl} has no dictionary`);
    return JSON.parse(inflateRawSync(buf.subarray(1), { dictionary: Buffer.from(d.d) }).toString("utf8"));
  }
  return JSON.parse(buf.toString("utf8"));
}
/** A value stored as the JSON text itself (the codec's third form, which every reader takes). */
const pack = (v) => Buffer.from(JSON.stringify(v), "utf8");

const strId = (db, text) => {
  const r = db.prepare("SELECT id FROM str WHERE text = ?").get(text);
  if (r) return Number(r.id);
  const id = Number(db.prepare("SELECT MAX(id) AS m FROM str").get().m) + 1;
  db.prepare("INSERT INTO str (id, text) VALUES (?, ?)").run(id, text);
  return id;
};

/**
 * Split the water's part whose runs hold two pieces, at the gap between them (a lake): `water` and `geo` are open
 * read-write. Returns what was done, for the run's header.
 */
function splitAtTheLake(water, geo, item = WATER) {
  const it = water.prepare("SELECT i.item_ord AS ord, i.picker FROM item i JOIN str s ON s.id = i.item_id WHERE s.text = ?").get(item);
  if (!it) throw new Error(`part-end proof: the fixture holds no ${item}`);
  const ord = Number(it.ord);
  const parts = water.prepare("SELECT * FROM part WHERE item_ord = ? ORDER BY part_ix").all(ord);
  const runsOf = (p) => unpack(water, "runs", water.prepare("SELECT j FROM runs WHERE id = ?").get(p.xruns).j);
  const k = parts.findIndex((p) => runsOf(p).length === 2);
  if (k < 0) throw new Error(`part-end proof: no part of ${item} has two runs`);
  const p = parts[k];
  // river order: the lower run is the one nearer the mouth (smaller km)
  const [lower, upper] = runsOf(p).sort((a, b) => Math.min(a.km_from, a.km_to) - Math.min(b.km_from, b.km_to));
  const outlet = lower.from.startsWith("lake_outlet:") ? lower.from : lower.to;
  const inlet = upper.from.startsWith("lake_inlet:") ? upper.from : upper.to;
  if (!outlet.startsWith("lake_outlet:") || !inlet.startsWith("lake_inlet:"))
    throw new Error(`part-end proof: ${item} part ${k}'s two runs do not meet at a lake (${JSON.stringify([lower, upper])})`);
  const lowTop = Math.max(lower.km_from, lower.km_to), upBottom = Math.min(upper.km_from, upper.km_to);
  // the upper piece: the part's lines at or above the lake (metres along their own blue line)
  const lines = water.prepare("SELECT * FROM part_line WHERE ord = ? AND part_ix = ?").all(ord, k);
  const upLines = lines.filter((l) => Number(l.m0) >= Math.round(upBottom * 1000) - 10 && Number(l.m0) > Math.round(lowTop * 1000));
  if (!upLines.length) throw new Error(`part-end proof: no line of ${item} part ${k} lies above ${lowTop} km`);
  const lineRows = JSON.parse(readFileSync(join(ROOT, "..", "section-lines.json"), "utf8")).lines;
  const onUp = (sid) => lineRows.some(([s, blk, m0]) => s === sid && upLines.some((l) => Number(l.blk) === blk &&
    m0 >= Number(l.m0) && m0 < Number(l.m1)));
  const secs = water.prepare("SELECT sid FROM part_section WHERE ord = ? AND part_ix = ?").all(ord, k).map((r) => Number(r.sid));
  const upSecs = secs.filter(onUp);
  if (!upSecs.length || upSecs.length === secs.length) throw new Error(`part-end proof: ${item} part ${k} does not split (${upSecs.length}/${secs.length})`);

  // every part above k moves up one (highest first: the keys stay unique)
  for (const q of parts.slice(k + 1).reverse()) {
    const i = Number(q.part_ix);
    water.prepare("UPDATE part SET part_ix = ? WHERE item_ord = ? AND part_ix = ?").run(i + 1, ord, i);
    water.prepare("UPDATE part_section SET part_ix = ? WHERE ord = ? AND part_ix = ?").run(i + 1, ord, i);
    water.prepare("UPDATE part_line SET part_ix = ? WHERE ord = ? AND part_ix = ?").run(i + 1, ord, i);
    geo.prepare("UPDATE part_geo SET part_ix = ? WHERE ord = ? AND part_ix = ?").run(i + 1, ord, i);
  }
  const runId = (v) => {
    const id = Number(water.prepare("SELECT MAX(id) AS m FROM runs").get().m) + 1;
    water.prepare("INSERT INTO runs (id, j) VALUES (?, ?)").run(id, pack(v));
    return id;
  };
  const nextOrd = Number(water.prepare("SELECT MAX(ord) AS m FROM part WHERE item_ord = ?").get(ord).m) + 1;
  const cols = Object.keys(p);
  water.prepare(`INSERT INTO part (${cols.join(", ")}) VALUES (${cols.map(() => "?").join(", ")})`).run(...cols.map((c) =>
    c === "part_ix" ? k + 1 : c === "ord" ? nextOrd : c === "lo_s" ? strId(water, inlet) : c === "sections" ? upSecs.length
      : c === "xruns" ? runId([upper]) : p[c]));
  water.prepare("UPDATE part SET hi_s = ?, km = ?, sections = ?, xruns = ? WHERE item_ord = ? AND part_ix = ?")
    .run(strId(water, outlet), lowTop, secs.length - upSecs.length, runId([lower]), ord, k);
  for (const sid of upSecs) water.prepare("UPDATE part_section SET part_ix = ? WHERE ord = ? AND sid = ?").run(k + 1, ord, sid);
  for (const l of upLines)
    water.prepare("UPDATE part_line SET part_ix = ? WHERE ord = ? AND part_ix = ? AND blk = ? AND m0 = ?").run(k + 1, ord, k, l.blk, l.m0);

  // the marks: one past the new end names the parts one higher (and comes one later); the new end's mark sits
  // between k and k + 1, on the way from the part's middle to the mark at its old upper end (the lake lies near the top)
  const marks = water.prepare("SELECT * FROM part_mark WHERE ord = ? ORDER BY mark_ix DESC").all(ord);
  const past = (m) => JSON.parse(m.up_parts).some((u) => u > k);
  const top = marks.find((m) => Number(m.down_part) === k && past(m));
  for (const m of marks.filter(past))
    water.prepare("UPDATE part_mark SET mark_ix = ?, down_part = ?, up_parts = ? WHERE ord = ? AND mark_ix = ?").run(
      Number(m.mark_ix) + 1, Number(m.down_part) >= k ? Number(m.down_part) + 1 : Number(m.down_part),
      JSON.stringify(JSON.parse(m.up_parts).map((u) => (u > k ? u + 1 : u))), ord, Number(m.mark_ix));
  const mid = geo.prepare("SELECT * FROM part_geo WHERE ord = ? AND part_ix = ?").get(ord, k);
  const to = top ?? { lon: mid.e, lat: mid.n };
  const lon = Math.round(Number(mid.mid_lon) + 0.8 * (Number(to.lon) - Number(mid.mid_lon)));
  const lat = Math.round(Number(mid.mid_lat) + 0.8 * (Number(to.lat) - Number(mid.mid_lat)));
  water.prepare("INSERT INTO part_mark (ord, mark_ix, lon, lat, token, down_part, up_parts, confidence, alt) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)").run(ord, marks.filter((m) => !past(m)).length, lon, lat, outlet, k, JSON.stringify([k + 1]));
  // part_geo: the upper piece's own row (its middle section; its box from the new mark to its upper end's mark)
  const [w, e] = [lon, Number(to.lon)].sort((a, b) => a - b), [s0, n] = [lat, Number(to.lat)].sort((a, b) => a - b);
  geo.prepare("INSERT INTO part_geo (ord, part_ix, mid_sid, mid_lon, mid_lat, w, s, e, n) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
    .run(ord, k + 1, upSecs[Math.floor(upSecs.length / 2)], Math.round((w + e) / 2), Math.round((s0 + n) / 2), w, s0, e, n);

  // the picker: the choice holding part k holds k + 1 too; every later part one higher; "· N stretches" counts them
  if (it.picker !== null && it.picker !== undefined) {
    const pk = unpack(water, "picker", water.prepare("SELECT j FROM picker WHERE id = ?").get(it.picker).j);
    for (const c of pk.choices) {
      const had = c.parts.includes(k);
      c.parts = c.parts.flatMap((i) => (i === k ? [k, k + 1] : [i > k ? i + 1 : i]));
      if (had) c.text = c.text.replace(/· (\d+) stretches$/, () => `· ${c.parts.length} stretches`);
    }
    water.prepare("UPDATE picker SET j = ? WHERE id = ?").run(pack(pk), it.picker);
  }
  return { item, part: k, parts: parts.length + 1, outlet, inlet, moved: upSecs };
}

/** Rewrite file `name` of the release at `dir` through `edit(db)`, its manifest entry following. */
function rewrite(dir, man, names, edit) {
  const tmp = mkdtempSync(join(tmpdir(), "part-end-"));
  try {
    const dbs = {};
    for (const name of names) {
      const f = man.files[name];
      const p = join(tmp, `${name}.sqlite`);
      writeFileSync(p, gunzipSync(readFileSync(join(dir, "..", f.url))));
      dbs[name] = new DatabaseSync(p, { enableForeignKeyConstraints: false });   // as the app opens a release: FKs unchecked
    }
    const done = edit(dbs);
    for (const name of names) {
      dbs[name].close();
      const raw = readFileSync(join(tmp, `${name}.sqlite`)), sha = createHash("sha256").update(raw).digest("hex");
      const gz = gzipSync(raw, { level: 9 }), old = man.files[name].url;
      const url = old.replace(/-[0-9a-f]{12}\.sqlite\.gz$/, `-${sha.slice(0, 12)}.sqlite.gz`);
      rmSync(join(dir, "..", old));
      writeFileSync(join(dir, "..", url), gz);
      Object.assign(man.files[name], { bytes: raw.length, sha256: sha, gz: gz.length, url });
    }
    return done;
  } finally {
    rmSync(tmp, { recursive: true, force: true });   // also when the edit throws (a leftover was found 2026-11-22)
  }
}

/** Write the mutant fixture root into `out` (a copy of the committed one, one part end added). */
export function writeMutant(out) {
  const src = releaseDir(ROOT), dir = join(out, relative(ROOT, src));
  cpSync(src, dir, { recursive: true });
  const man = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8"));
  const done = rewrite(dir, man, ["water", "geo"], (d) => splitAtTheLake(d.water, d.geo));
  writeFileSync(join(dir, "manifest.json"), JSON.stringify(man, null, 1) + "\n");
  writeFileSync(join(out, "MUTANT.json"), JSON.stringify({ tool: "app/tools/part-end-proof.mjs", ...done }, null, 1) + "\n");
  return done;
}

/** where `ProofRunEnd` writes the run's end (`{errors, reason}`): set by `main` for the vitest it starts */
const END = "PART_END_PROOF_END";

/** vitest reporter: what the JSON report leaves out — the run's unhandled errors and why it ended. */
export default class ProofRunEnd {
  onTestRunEnd(_modules, errors, reason) {
    if (process.env[END]) writeFileSync(process.env[END], JSON.stringify({ errors: errors.length, reason }) + "\n");
  }
}

/**
 * The verdict over vitest's JSON report and the run's end (`ProofRunEnd`; null: never written, the run did not end):
 * `{ok, failed, provenance, unexpected, whole}` (paths app-relative).
 */
export function verdict(report, end) {
  const failed = report.testResults.filter((r) => r.status === "failed").map((r) => relative(APP, r.name)).sort();
  const provenance = failed.filter((f) => f in PROVENANCE), counted = failed.filter((f) => !(f in PROVENANCE));
  const unexpected = counted.filter((f) => f !== PIN);
  const whole = !!end && end.errors === 0 && end.reason !== "interrupted";
  return { ok: whole && counted.length === 1 && counted[0] === PIN, failed, provenance, unexpected,
           pinFailed: counted.includes(PIN), whole, end };
}

function main(argv) {
  if (argv[0] === "--write-only") {
    if (!argv[1]) throw new Error("--write-only DIR");
    console.log(JSON.stringify(writeMutant(argv[1])));
    return 0;
  }
  // laid out as the committed root is (`…/regs-release/<release id>`): a check of the folder's shape sees the same shape
  const base = mkdtempSync(join(tmpdir(), "part-end-proof-")), out = join(base, "regs-release");
  mkdirSync(out);
  try {
    const done = writeMutant(out);
    console.log(`part-end proof: ${done.item} part ${done.part} cut once more at ${done.outlet} / ${done.inlet} ` +
      `(${done.parts} parts; sections ${done.moved.join(", ")} moved up); the copy: ${out}`);
    const report = join(base, "report.json"), end = join(base, "end.json");
    const r = spawnSync(join(APP, "node_modules/.bin/vitest"), ["run", "--reporter=default", "--reporter=json",
      `--reporter=${fileURLToPath(import.meta.url)}`, `--outputFile.json=${report}`, ...argv],
      { cwd: APP, stdio: "inherit", env: { ...process.env, REGS_FIXTURE_MUTANT: out, [END]: end } });
    if (!existsSync(report)) { console.log(`part-end proof: vitest wrote no report (exit ${r.status})`); return 1; }
    const v = verdict(JSON.parse(readFileSync(report, "utf8")), existsSync(end) ? JSON.parse(readFileSync(end, "utf8")) : null);
    const partial = argv.length ? " (PARTIAL: only the files named)" : "";
    console.log(`\npart-end proof${partial}: failed files ${v.failed.length}: ${v.failed.join(", ") || "none"}`);
    for (const f of v.provenance) console.log(`  provenance, not counted: ${f} (${PROVENANCE[f]})`);
    if (!v.whole) console.log(`  ✗ the run did not end whole: ${v.end ? `${v.end.errors} unhandled error(s), ${v.end.reason}` : "no end recorded"}`);
    if (!v.pinFailed) console.log(`  ✗ the content pin ${PIN} did NOT fail: it no longer holds the water's parts`);
    for (const f of v.unexpected) console.log(`  ✗ ${f} failed: it types a part count, name or position the water's group should give it`);
    console.log(v.ok ? `  ✓ exactly one file, the content pin, fails on a new part end` : `  ✗ NOT met (T768 "Done when")`);
    return v.ok ? 0 : 1;
  } finally {
    rmSync(base, { recursive: true, force: true });
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) process.exit(main(process.argv.slice(2)));