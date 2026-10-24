/**
 * T10: THE COMMITTED REGS FIXTURE RELEASE (`packages/data/dev/regs-release/`, `pnpm regs-fixture`) — over the committed
 * files, no live data needed. It is the regulations half of a real release cut to the golden waters, so:
 *
 *   1. it is one release whose manifest names its files and no tile set, every file saying which release it was cut from;
 *   2. it keeps exactly the waters the golden taps name (taps.json states + leap), with every part;
 *   3. it is CLOSED: every frame id a kept cell names, and every id inside a kept frame, is in the same region file; every
 *      kept key has its region's cells for every segment; every kept sid slot names a kept key (mutation-pinned below);
 *   4. every rule is kept (`rules()` sees all of them) and the digests are the golden store's (taps.json meta.store);
 *   5. it is small; and 6. rebuilding it from the release it names writes the same bytes (when that release is here).
 *
 * The suite that asks it the golden questions is conformance/src/regs/regs.fixture.test.ts.
 */
import { execFileSync } from "node:child_process";
import { copyFileSync, existsSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { gunzipSync } from "node:zlib";
import { describe, expect, it } from "vitest";
import { unpackBlob } from "@app/data/regs";
import { REGS_FIXTURE_ROOT, regsFixtureDir } from "@app/data/release/node";

const REPO = resolve(__dirname, "../..");
const DIR = regsFixtureDir();
const RID = DIR.split("/").pop()!;
const MAN = JSON.parse(readFileSync(join(DIR, "manifest.json"), "utf8")) as {
  data_release: string; tiles: { current: unknown; compatible: string[] };
  files: Record<string, { url: string; gz: number; bytes: number; sha256: string; format: string }>;
};
const TAPS = JSON.parse(readFileSync(join(REPO, "app/design/regs-v11/port/golden/taps.json"), "utf8")) as {
  meta: { store: { section_handles: string; reach_digest: string; rule_ids_sha256: string } };
  states: Record<string, { item: string; akey: number }>; leap: { item: string; akey: number }[];
  unnamed: { handle: number; akey: number; place: number }[];
};

/** Every file of a release folder, decoded into a temp sqlite file. */
function openAll(dir: string): Record<string, DatabaseSync> {
  const m = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as typeof MAN;
  const tmp = mkdtempSync(join(tmpdir(), "regs-fixture-"));
  return Object.fromEntries(Object.entries(m.files).map(([name, f]) => {
    const p = join(tmp, `${name}.sqlite`);
    writeFileSync(p, gunzipSync(readFileSync(join(dir, "..", f.url))));
    return [name, new DatabaseSync(p)];
  }));
}
const DB = openAll(DIR);
const meta = (d: DatabaseSync) =>
  Object.fromEntries((d.prepare("SELECT k, v FROM meta").all() as { k: string; v: string }[]).map((r) => [r.k, r.v]));
const count = (d: DatabaseSync, t: string) => (d.prepare(`SELECT count(*) AS n FROM ${t}`).get() as { n: number }).n;
const ids = (d: DatabaseSync, t: string, col = "id") =>
  new Set((d.prepare(`SELECT ${col} AS i FROM ${t}`).all() as { i: number }[]).map((r) => r.i));

/**
 * Closure of a fixture's region files and regs-sid (test 3): the problems, as sentences, in a stable order. The frame
 * shapes are the encoder's (`pipeline/deliver/release/shards.py` `frame_refs`): ladder [common, [[fish, v, v, v]…]],
 * rows [spp, {fish: [h, w, lead]}, [row…], line], licence [hold, doc, u16…]; a licence doc is its answer ids.
 */
function closureProblems(db: Record<string, DatabaseSync>): string[] {
  const water = db.water!, out: string[] = [];
  const dicts = new Map((water.prepare("SELECT tbl, d FROM zdict").all() as { tbl: string; d: Uint8Array }[]).map((r) => [r.tbl, r.d]));
  const val = (t: string, b: Uint8Array) => unpackBlob(b, dicts.get(t) ?? null);
  const keys = new Map((water.prepare("SELECT akey, nseg FROM akey").all() as { akey: number; nseg: number }[]).map((r) => [r.akey, r.nseg]));
  for (const { akey, region } of water.prepare("SELECT akey, region FROM akey_region ORDER BY akey, region").all() as { akey: number; region: string }[]) {
    const r = db[`regs-${region}`];
    if (!r) { out.push(`akey ${akey}: no regs-${region} file`); continue; }
    const n = (r.prepare("SELECT count(*) AS n FROM cell WHERE akey = ?").get(akey) as { n: number }).n;
    if (n !== keys.get(akey)) out.push(`akey ${akey}: regs-${region} holds ${n} cells, the key has ${keys.get(akey)} segments`);
  }
  for (const [name, r] of Object.entries(db).filter(([n]) => /^regs-\d/.test(n)).sort()) {
    const have = Object.fromEntries(["ladder_frame", "ladder_verdict", "rows_frame", "rows_decided", "rows_row", "gear_frame",
      "licence_frame", "licence_hold", "licence_doc", "licence_answer", "licence_u16", "display_frame"].map((t) => [t, ids(r, t)]));
    const need = (t: string, i: number, from: string) => { if (!have[t]!.has(i)) out.push(`${name}: ${from} names ${t} ${i}, absent`); };
    for (const c of r.prepare("SELECT * FROM cell ORDER BY akey, s").all() as Record<string, number>[])
      for (const f of ["ladder", "rows", "gear", "licence", "display"]) need(`${f}_frame`, c[f]!, `cell ${c.akey}/${c.s}`);
    for (const { id, j } of r.prepare("SELECT id, j FROM ladder_frame ORDER BY id").all() as { id: number; j: Uint8Array }[]) {
      const [common, rows] = val("ladder_frame", j) as [number, [string, ...number[]][]];
      for (const v of [common, ...rows.flatMap((x) => x.slice(1) as number[])]) need("ladder_verdict", v, `ladder_frame ${id}`);
    }
    for (const { id, j } of r.prepare("SELECT id, j FROM rows_frame ORDER BY id").all() as { id: number; j: Uint8Array }[]) {
      const [, fish, rows] = val("rows_frame", j) as [unknown, Record<string, (number | null)[]>, number[]];
      // rows 33 (T161): [h, w, lead] — the lead is an origin word, not a row
      for (const hwl of Object.values(fish)) for (const d of hwl.slice(0, 2)) if (d !== null) need("rows_decided", d, `rows_frame ${id}`);
      for (const x of rows) need("rows_row", x, `rows_frame ${id}`);
    }
    for (const { id, j } of r.prepare("SELECT id, j FROM licence_frame ORDER BY id").all() as { id: number; j: Uint8Array }[]) {
      const [h, d, ...u] = val("licence_frame", j) as number[];
      need("licence_hold", h!, `licence_frame ${id}`); need("licence_doc", d!, `licence_frame ${id}`);
      for (const x of u) need("licence_u16", x, `licence_frame ${id}`);
    }
    for (const { id, j } of r.prepare("SELECT id, j FROM licence_doc ORDER BY id").all() as { id: number; j: Uint8Array }[])
      for (const a of val("licence_doc", j) as number[]) need("licence_answer", a, `licence_doc ${id}`);
  }
  const sid = db["regs-sid"]!;
  for (const { block, b } of sid.prepare("SELECT block, b FROM section_akey ORDER BY block").all() as { block: number; b: Uint8Array }[])
    for (let i = 0; i < b.length / 2; i++) {
      const a = b[2 * i]! | (b[2 * i + 1]! << 8);
      if (a && !keys.has(a)) out.push(`regs-sid: section ${(block << 12) | i} names akey ${a}, not kept`);
    }
  return out;
}

describe("T10 the committed regs fixture release", () => {
  it("1. one release under the root, its manifest naming its files and no tile set; every file cut from one release", () => {
    expect(readdirSync(REGS_FIXTURE_ROOT).filter((n) => n.startsWith("r-"))).toEqual([RID]);
    expect(MAN.data_release).toBe(RID);
    expect(Object.keys(MAN.files).sort()).toEqual(
      ["core", "regs-1", "regs-2", "regs-3", "regs-4", "regs-5", "regs-6", "regs-7a", "regs-7b", "regs-8", "regs-sid", "water"]);
    expect(MAN.tiles.current).toBeNull();
    const of = new Set(Object.values(DB).map((d) => meta(d).fixture_of));
    expect(of.size).toBe(1);
    expect([...of][0]).toMatch(/^r-[0-9a-f]{12}$/);
  });

  it("2. keeps exactly the golden waters (taps.json states + leap) and every key they and the unnamed handles reach", () => {
    const golden = [...new Set([...Object.values(TAPS.states), ...TAPS.leap].map((s) => s.item))].sort();
    const core = (DB.core!.prepare("SELECT item_id AS i, ord FROM item ORDER BY item_id").all() as { i: string; ord: number }[]);
    expect(core.map((r) => r.i)).toEqual(golden);
    expect([...ids(DB.water!, "item", "item_ord")].sort((a, b) => a - b)).toEqual(core.map((r) => r.ord).sort((a, b) => a - b));
    for (const t of [...Object.values(TAPS.states), ...TAPS.leap])
      expect(ids(DB.water!, "akey", "akey").has(t.akey), `${t.item} akey ${t.akey}`).toBe(true);
    for (const u of TAPS.unnamed) expect(ids(DB.water!, "akey", "akey").has(u.akey), `handle ${u.handle}`).toBe(true);
  });

  it("3. closed: every id a kept cell or frame names is in its region file; every kept key has every segment's cell", () => {
    expect(closureProblems(DB)).toEqual([]);
  });

  it("3 (mutation pin). the closure check names a dropped verdict, a short key and a stray sid slot", () => {
    const tmp = mkdtempSync(join(tmpdir(), "regs-fixture-mut-"));
    const mut: Record<string, DatabaseSync> = {};
    for (const n of ["water", "regs-2", "regs-sid"]) {
      const p = join(tmp, `${n}.sqlite`);
      copyFileSync((DB[n] as unknown as { location(): string }).location(), p);
      mut[n] = new DatabaseSync(p, { enableForeignKeyConstraints: false });   // a region file names water's tables
    }
    const r2 = mut["regs-2"]!;
    const lf = r2.prepare("SELECT id, j FROM ladder_frame ORDER BY id LIMIT 1").get() as { id: number; j: Uint8Array };
    const dicts = new Map((mut.water!.prepare("SELECT tbl, d FROM zdict").all() as { tbl: string; d: Uint8Array }[]).map((r) => [r.tbl, r.d]));
    const common = (unpackBlob(lf.j, dicts.get("ladder_frame")!) as [number])[0];
    r2.prepare("DELETE FROM ladder_verdict WHERE id = ?").run(common);
    const vedder = TAPS.states.vedder_515!.akey;   // a Region 2 key with 4 segments (taps.json; its number moves per build)
    r2.prepare("DELETE FROM cell WHERE akey = ? AND s = 3").run(vedder);
    const blk = mut["regs-sid"]!.prepare("SELECT block, b FROM section_akey ORDER BY block LIMIT 1").get() as { block: number; b: Uint8Array };
    const b = new Uint8Array(blk.b);
    const slot = b.findIndex((_, i) => i % 2 === 0 && b[i] === 0 && b[i + 1] === 0) / 2;
    b[2 * slot] = 0xff; b[2 * slot + 1] = 0xff;
    mut["regs-sid"]!.prepare("UPDATE section_akey SET b = ? WHERE block = ?").run(b, blk.block);
    const problems = closureProblems(mut);
    expect(problems).toContain(`regs-2: ladder_frame ${lf.id} names ladder_verdict ${common}, absent`);
    expect(problems).toContain(`akey ${vedder}: regs-2 holds 3 cells, the key has 4 segments`);
    expect(problems).toContain(`regs-sid: section ${(blk.block << 12) | slot} names akey 65535, not kept`);
  });

  it("4. every rule kept; the digests are the golden store's; the formats the app reads", () => {
    // every rule: rix dense from 0 in both tables (no count typed here — 3,406 → 3,405 when T89 made Endako 7A a See twin);
    // that they are the golden store's rules, in order and no other, is the conformance suite's content digest
    // (regsConformance "whole": the rules' keys hash to rule_ids_sha256) plus the meta digests below.
    const span = (t: string, k: string) => ({ ...DB.water!.prepare(`SELECT count(*) AS n, min(${k}) AS lo, max(${k}) AS hi FROM ${t}`).get() });
    const rules = span("rule", "rix") as { n: number; lo: number; hi: number };
    expect(rules.n).toBeGreaterThan(3000);
    expect([rules.lo, rules.hi + 1, span("display_rule", "id")]).toEqual([0, rules.n, { n: rules.n, lo: 0, hi: rules.n - 1 }]);
    const g = TAPS.meta.store;
    for (const n of ["water", "regs-sid", ...Object.keys(MAN.files).filter((k) => /^regs-\d/.test(k))]) {
      const m = meta(DB[n]!);
      expect([n, m.section_handles, m.reach_digest, m.rule_ids_sha256]).toEqual([n, g.section_handles, g.reach_digest, g.rule_ids_sha256]);
    }
    expect(Object.fromEntries(Object.entries(MAN.files).map(([n, f]) => [n, f.format]))).toEqual({
      core: "core/1", water: "water/1", "regs-sid": "regs-sid/1",
      ...Object.fromEntries(["1", "2", "3", "4", "5", "6", "7a", "7b", "8"].map((r) => [`regs-${r}`, "regs-region/1"])) });
  });

  it("5. small: under 3.5 MB gzipped in all", () => {
    expect(Object.values(MAN.files).reduce((a, f) => a + f.gz, 0)).toBeLessThan(3_500_000);
  });

  const source = meta(DB.water!).fixture_of!;
  const live = [REPO, REPO.replace(/\/\.claude\/worktrees\/[^/]+$/, "")].map((r) => join(r, "data/release", source))
    .find((d) => existsSync(join(d, "manifest.json"))) ?? null;
  it.skipIf(!live)(`6. deterministic: rebuilt from ${source} (${live ? "here" : "not on this machine"}), the same bytes`, () => {
    const out = mkdtempSync(join(tmpdir(), "regs-fixture-rebuild-"));
    execFileSync(join(REPO, ".venv/bin/python"), ["-m", "pipeline.deliver.release.regs_fixture", "--release", live!, "--out", out],
                 { cwd: REPO, encoding: "utf8" });
    expect(readdirSync(out)).toEqual([RID]);
    const files = readdirSync(join(out, RID)).sort();
    expect(files).toEqual(readdirSync(DIR).sort());
    for (const f of files) expect(readFileSync(join(out, RID, f)).equals(readFileSync(join(DIR, f))), f).toBe(true);
  }, 60_000);
});
