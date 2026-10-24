/**
 * NODE ONLY (tests): a release held in memory — its SQLite files built from the release CONTRACT's own columns
 * (`PACK_FILES`, generated from `pipeline/deliver/release/contract.py`) and seeded by the test, behind the app's own
 * loader (`releaseLoader`) and reader (`makeReleaseSource`). For a shape no committed fixture has: a lake cut into
 * parts, a group name that only points, a water joined to the next across `water_continue`.
 *
 * Every file's meta states the manifest's format and handles, so the loader's check on open passes; nothing is
 * verified byte for byte (there are no bytes). `@app/data/release/node` is the reader over a real release folder.
 */
import { DatabaseSync } from "node:sqlite";
import type { Db, Row } from "../db";
import { parseManifest } from "../../manifest";
import { PACK_FILES } from "../../pack.generated";
import { releaseLoader, type DbName, type ReleaseLoader } from "../../release/loader";
import { makeReleaseSource, type SourceOptions } from "../../release/source";
import type { WaterSource } from "../../index";

/** The handles every memory release is cut against (any 16 hex digits: no tiles are drawn). */
export const MEMORY_HANDLES = "d768d6e135e62b99";

/** Per file, the rows a test writes into the contract's empty tables. */
export type MemorySeed = Partial<Record<DbName, (d: DatabaseSync) => void>>;

/** The contract's tables of `file`, every column untyped (SQLite takes what it is given). */
function contractDb(file: DbName): DatabaseSync {
  const spec = PACK_FILES[file];
  if (!spec) throw new Error(`memory release: the contract has no file ${file}`);
  const d = new DatabaseSync(":memory:");
  for (const [t, cols] of Object.entries(spec.tables)) d.exec(`CREATE TABLE "${t}" (${cols.join(", ")})`);
  d.exec("DELETE FROM meta");
  d.prepare("INSERT INTO meta (k, v) VALUES ('format', ?), ('section_handles', ?)").run(spec.format, MEMORY_HANDLES);
  return d;
}

const asDb = (d: DatabaseSync): Db => ({
  all: async (sql, ...p) => d.prepare(sql).all(...(p as never[])) as unknown as Row[],
  get: async (sql, ...p) => d.prepare(sql).get(...(p as never[])) as unknown as Row | undefined,
  close: () => d.close(),
});

/** A loader over the seeded files: only the files `seed` names are in the manifest (the loader refuses the rest). */
export function memoryRelease(seed: MemorySeed): ReleaseLoader {
  const names = Object.keys(seed) as DbName[];
  const dbs = new Map(names.map((n) => {
    const d = contractDb(n);
    seed[n]!(d);
    return [n, d] as const;
  }));
  const manifest = parseManifest({
    format: "app-manifest/2", data_release: "r-memory",
    tiles: { compatible: [`${MEMORY_HANDLES}.*`], current: null, assets: null, mask: null }, basemap: null,
    files: Object.fromEntries(names.map((k) => [k, {
      url: `r-memory/${k}.sqlite.gz`, bytes: 1, gz: 0, sha256: "0".repeat(64), format: PACK_FILES[k]!.format,
      section_handles: MEMORY_HANDLES }])),
    regulations: { valid_until: null, versions: null },
  });
  return releaseLoader(manifest, { bytes: async () => new Uint8Array() }, async (name) => asDb(dbs.get(name as DbName)!));
}

/** The app's reader over a memory release. */
export const memorySource = (seed: MemorySeed, opts: SourceOptions = {}): WaterSource =>
  makeReleaseSource(memoryRelease(seed), opts);
