/**
 * T02 on the PROVINCE STORE — the live release's regs store put back together from its files (`provinceStoreOf`; P20: no
 * regs.sqlite is kept; skipped without a paired release, a failure under
 * REGS_STORE_REQUIRED=1): `unpackBlob` decodes what the store holds exactly as the Python reference does.
 *
 * Nothing is typed here. It replaced packages/data blob.test.ts's table of literal ids and JSON prefixes (ladder_frame 1057,
 * rows_frame 2002, gear_frame 320, ruleset 393, verdict 0 = "[[3079,…"), which broke on every delivery: frame and verdict
 * ids are interned per build, rule numbers move when a rule ahead of them is added or dropped (T89: 3,406 → 3,405), and
 * best-of-8 dictionaries (e0b09590) re-pack every blob. Two sources instead:
 * - the Python reference decoder (`pipeline/deliver/store/decode.py` `Store.val` → `common.unpack`) on the rows the golden
 *   taps reach (taps.json vedder_515's cell, its key's ruleset, the Vedder's June closure by its durable key) and a spread
 *   sample of every blob table: each decodes to the same value;
 * - zlib itself (node:zlib, the library Python's `zlib.decompressobj(-15, zdict)` is): every row of every blob table
 *   inflates to the same bytes `inflateBlob` gives.
 */
import { execFileSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { resolve } from "node:path";
import { inflateRawSync } from "node:zlib";
import { beforeAll, describe, expect, it } from "vitest";
import { inflateBlob, unpackBlob } from "@app/data/regs";
import { liveRelease, provinceStoreOf } from "../fixtureRelease";
import { taps } from "./oracles";

const T = taps();
const PATH = provinceStoreOf(liveRelease());
const REQUIRED = !!process.env.REGS_STORE_REQUIRED && process.env.REGS_STORE_REQUIRED !== "0";
const db = PATH ? new DatabaseSync(PATH, { readOnly: true }) : null;
const answers = db ? (db.prepare("SELECT v FROM meta WHERE k = 'answers_sha256'").get() as { v: string } | undefined)?.v : undefined;
/** The store is the build taps.json was cut from, or the comparison is with another build's ids (a skip with the reason). */
const paired = !!db && !!answers && answers === T.meta.store.answers_sha256;
const why = !db ? "no release on this machine (or no .venv to reassemble its store)" : paired ? null
  : `the live release carries answers ${String(answers).slice(0, 12)}, taps.json was cut from ${String(T.meta.store.answers_sha256).slice(0, 12)}: re-run app/design/regs-v11/port/sync.mjs`;

/** Every blob table and its blob column (pipeline/deliver/store/common.py BLOB_COLUMNS), and its key column. */
const BLOBS = `
import json
from pipeline.deliver.store.common import BLOB_COLUMNS
print(json.dumps(BLOB_COLUMNS))
`;
const PY = `
import json, sys
from pipeline.deliver.store.decode import Store
s = Store(sys.argv[1], verify=False)
out = []
for t, col, kcol, key in json.loads(sys.stdin.read()):
    row = s.con.execute(f"SELECT {col} FROM {t} WHERE {kcol} = ?", (key,)).fetchone()
    out.append(None if row is None else s.val(t, row[0]))
print(json.dumps(out))
`;
const ROOT = resolve(__dirname, "../../../..");
const python = (code: string, input = "", args: string[] = []): string =>
  execFileSync(resolve(ROOT, ".venv/bin/python"), ["-c", code, ...args], { cwd: ROOT, encoding: "utf8", input, maxBuffer: 1 << 28 });

it(`T02 blob parity runs against the province store${why ? ` — ${REQUIRED ? "FAILS" : "SKIPPED"}: ${why}` : ""}`, () => {
  if (why && REQUIRED) expect.fail(`REGS_STORE_REQUIRED is set: ${why}`);
});

describe.skipIf(!paired)(`unpackBlob over the province store (${PATH ?? "absent"})`, () => {
  const store = db!;
  let columns: Record<string, string>;
  beforeAll(() => { columns = JSON.parse(python(BLOBS)) as Record<string, string>; });
  /** a table's primary key column (rule: rix, ruleset: set_id, static: k, item: item_ord, lic: lix, else id) */
  const keyCol = (t: string) => (store.prepare("SELECT name FROM pragma_table_info(?) WHERE pk = 1").get(t) as { name: string }).name;
  const dict = (t: string): Uint8Array | null =>
    (store.prepare("SELECT d FROM zdict WHERE tbl = ?").get(t) as { d: Uint8Array } | undefined)?.d ?? null;
  const blobOf = (t: string, key: number | string): Uint8Array =>
    new Uint8Array((store.prepare(`SELECT ${columns[t]} AS b FROM ${t} WHERE ${keyCol(t)} = ?`).get(key) as { b: Uint8Array }).b);

  it("every blob table holds rows, and the store's codec is zdict", () => {
    expect((store.prepare("SELECT v FROM meta WHERE k = 'blob_codec'").get() as { v: string }).v).toBe("zdict");
    expect(Object.keys(columns)).toEqual(expect.arrayContaining(["ladder_frame", "ladder_verdict", "rows_frame", "rows_row",
      "rows_decided", "gear_frame", "licence_frame", "display_frame", "display_rule", "rule", "ruleset", "static"]));
  });

  it("the rows the golden taps reach, and a sample of every table, decode as the Python reference decodes them", () => {
    const v = T.states.vedder_515!;
    const ruleset = (store.prepare("SELECT ruleset FROM akey WHERE akey = ?").get(v.akey) as { ruleset: number }).ruleset;
    // the Vedder's June closure by its durable key: its rix (and so its rule and display_rule rows) moves between builds
    const juneKey = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r8";
    const ruleIds = JSON.parse(python(`
import json, sys
from pipeline.deliver.store.decode import Store
s = Store(sys.argv[1], verify=False)
print(json.dumps([f"{r['entry_id']}::{r['rule_id']}" for r in (s.val("rule", b) for _, b in s.con.execute("SELECT rix, j FROM rule ORDER BY rix"))]))
`, "", [PATH!])) as string[];
    const june = ruleIds.indexOf(juneKey);
    expect(june).toBeGreaterThanOrEqual(0);
    const named: [string, number | string][] = [
      ["ladder_frame", v.cell.ladder], ["rows_frame", v.cell.rows], ["gear_frame", v.cell.gear],
      ["licence_frame", v.cell.licence], ["display_frame", v.cell.display],
      ["ruleset", ruleset], ["rule", june], ["display_rule", june], ["static", "licence.profile_dims"],
    ];
    const sample: [string, number | string][] = [];
    for (const t of Object.keys(columns)) {
      if (t === "static") continue;
      const ks = (store.prepare(`SELECT ${keyCol(t)} AS k FROM ${t} WHERE ${columns[t]} IS NOT NULL ORDER BY ${keyCol(t)}`)
        .all() as { k: number }[]).map((r) => r.k);
      const step = Math.max(1, Math.floor(ks.length / 40));
      for (let i = 0; i < ks.length; i += step) sample.push([t, ks[i]!]);
      sample.push([t, ks[ks.length - 1]!]);
    }
    const asks = [...named, ...sample];
    const want = JSON.parse(python(PY, JSON.stringify(asks.map(([t, k]) => [t, columns[t], keyCol(t), k])), [PATH!])) as unknown[];
    expect(want.length).toBe(asks.length);
    asks.forEach(([t, k], i) => {
      expect(want[i], `${t} ${k}: Python found the row`).not.toBeNull();
      expect(unpackBlob(blobOf(t, k), dict(t)), `${t} ${k}`).toEqual(want[i]);
    });
    // what the named rows are, so the sample cannot pass on rows that say nothing
    expect((want[0] as unknown[]).length).toBe(2);                                      // ladder: [common, rows]
    expect(Object.keys(want[4] as object)).toEqual(expect.arrayContaining(["status", "closing", "strip"]));
    expect((want[5] as { reach: number[] }).reach).toContain(june);                        // the Vedder's ruleset holds it
    expect((want[6] as { entry_id: string; rule_id: string }).rule_id).toBe("chilliwack_vedder_rivers.r8");
    expect((want[7] as { kind: string; closure: boolean }).kind).toBe("gate");
  }, 120_000);

  it("every row of every blob table inflates as zlib inflates it (raw DEFLATE, the table's preset dictionary)", () => {
    let n = 0;
    for (const [t, col] of Object.entries(columns)) {
      const zd = dict(t);
      for (const r of store.prepare(`SELECT ${col} AS b FROM ${t} WHERE ${col} IS NOT NULL`).all() as { b: Uint8Array }[]) {
        const b = new Uint8Array(r.b);
        const want = b[0] === 0x01 ? inflateRawSync(b.subarray(1))
          : b[0] === 0x02 ? inflateRawSync(b.subarray(1), { dictionary: zd! }) : b;
        const got = inflateBlob(b, zd);
        if (!Buffer.from(got).equals(Buffer.from(want))) expect.fail(`${t}: a row inflates to other bytes than zlib's`);
        n++;
      }
    }
    expect(n).toBeGreaterThan(50_000);
  }, 120_000);
});
