/**
 * NODE ONLY (tests and tools; never the app's path): a release FOLDER read the way the app reads a release, and THE REGS
 * FIXTURE RELEASE (T10).
 *
 * The regs fixture is the regulations half of a real release — `water`, `regs-<region>`, `regs-sid` and the `core` rows
 * `itemOrd` reads — cut to the waters the golden states name by `python -m pipeline.deliver.release.regs_fixture` (the
 * release builder's own copy/ship code: every row is the release's bytes, blobs keep their codec) and committed under
 * `packages/data/dev/regs-release/<release id>/`. The regs conformance suite and the ui regs hooks' tests open it when this
 * machine has no live release (CI). Its manifest names no tile set: `openRegsStore(loader)` holds it to no atlas.
 *
 * Lives in the drivers layer because it is a node:sqlite driver over files (layers.json); the temp folder comes from
 * $TMPDIR (node:os is not a driver import).
 */
import { existsSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import type { Db, Row } from "../db";
import { verify } from "../../fetchVerified";
import { parseManifest, type Manifest } from "../../manifest";
import { releaseLoader, type DbName, type Opener, type ReleaseLoader, type ReleaseSource } from "../../release/loader";
import { encodeLineBlock, type SectionLine } from "../../regs/line";
import { BLOCK_BITS } from "../../regs/reads";

/**
 * Where the committed regs fixture release lives (the folder holds exactly one `r-*` release). `__dirname`, not
 * `import.meta.url`: under the render project's jsdom the latter is an http URL.
 */
export const REGS_FIXTURE_ROOT = resolve(__dirname, "../../../dev/regs-release");

/**
 * A row with every blob as THIS realm's Uint8Array. node:sqlite hands back node's; under the render project's jsdom the
 * global `Uint8Array` is another realm's, so the regs layer's `instanceof Uint8Array` (errors.ts `asBytes`) would call
 * every blob damaged. Copying costs nothing at fixture sizes.
 */
const realm = (r: Record<string, unknown>): Row => {
  for (const k in r) { const v = r[k]; if (ArrayBuffer.isView(v) && !(v instanceof Uint8Array)) r[k] = Uint8Array.from(v as Uint8Array); }
  return r as Row;
};

/** node:sqlite, read-only, as `Db`, every blob in this realm (see `realm`). */
function openFile(path: string): Db {
  const db = new DatabaseSync(path, { readOnly: true });
  return {
    all: async (sql, ...params) => (db.prepare(sql).all(...params) as Record<string, unknown>[]).map(realm),
    get: async (sql, ...params) => { const r = db.prepare(sql).get(...params) as Record<string, unknown> | undefined; return r && realm(r); },
    close: () => db.close(),
  };
}

/** The committed fixture release's folder (`<root>/<release id>`); throws when it is missing or not alone. */
export function regsFixtureDir(root: string = REGS_FIXTURE_ROOT): string {
  const dirs = existsSync(root)
    ? readdirSync(root).filter((n) => /^r-[0-9a-f]{12}$/.test(n) && existsSync(join(root, n, "manifest.json"))) : [];
  if (dirs.length !== 1)
    throw new Error(`regs fixture: ${root} holds ${dirs.length} releases, not 1 — run pnpm regs-fixture`);
  return join(root, dirs[0]!);
}

/** node:sqlite over a verified file: the bytes written to a temp file and opened read-only. */
export function nodeOpener(): Opener {
  const tmp = mkdtempSync(join(process.env.TMPDIR || "/tmp", "release-open-"));
  return async (name, bytes) => {
    const p = join(tmp, `${name}.sqlite`);
    writeFileSync(p, await bytes());
    return openFile(p);
  };
}

/**
 * A test's change to a release AS THE LOADER SEES IT (the files on disk are never touched): `manifest` edits the parsed
 * manifest (drop a file, name other tiles), `db` wraps an opened file (another build's meta).
 */
export interface ReleaseTamper {
  manifest?(m: Manifest): Manifest;
  db?(name: DbName, d: Db): Db;
}

/**
 * A `ReleaseLoader` over a release folder, the way the app reads one: each file's bytes held to the manifest
 * (`verify`: the decoded length and sha256) and opened in node:sqlite from a temp copy. `tamper`: tests only.
 */
export function openReleaseDir(dir: string, tamper: ReleaseTamper = {}): ReleaseLoader {
  const read = parseManifest(JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")));
  const m = tamper.manifest ? tamper.manifest(read) : read;
  const source: ReleaseSource = {
    async bytes(name) {
      const f = m.files[name];
      if (!f) throw new Error(`release ${m.data_release} has no file ${name}`);
      return verify(f.url, new Uint8Array(readFileSync(join(dir, "..", f.url))), f);
    },
  };
  const open = nodeOpener(), wrap = tamper.db;
  return releaseLoader(m, source, wrap ? async (name, bytes) => wrap(name, await open(name, bytes)) : open);
}

/** The regs fixture release, opened as the app opens a release. */
export const openRegsFixture = (): ReleaseLoader => openReleaseDir(regsFixtureDir());

/** (T369) One stream section's line as the fixture lists it: [handle, stream key, from m, to m). */
export type LineRow = readonly [number, number, number, number];

/**
 * (T369) The regs fixture's stream lines (`dev/section-lines.json`: its waters' sections and one unnamed reach) — the
 * fixture release was cut before `section_line`, so tests add the table (`withSectionLines`).
 */
export function regsFixtureLines(): { unnamed: number; lines: LineRow[] } {
  const d = JSON.parse(readFileSync(join(REGS_FIXTURE_ROOT, "..", "section-lines.json"), "utf8")) as
    { unnamed: number; lines: LineRow[] };
  return { unnamed: d.unnamed, lines: d.lines };
}

/**
 * (T369) A `section_line` table (`@app/data` line.ts's format, its own writer) over `rows`, in memory. Only the blocks
 * the rows touch are written; a slot no row names is a filler that keeps the block sorted and spans nothing (the slot
 * before it's line, empty) — a fixture's handles are a sample of the atlas, a release's table has every stream handle.
 */
function sectionLineDb(rows: readonly LineRow[]): Db {
  const db = new DatabaseSync(":memory:");
  db.exec("CREATE TABLE section_line (block INTEGER PRIMARY KEY, n INTEGER NOT NULL, blk0 INTEGER NOT NULL, " +
          "m0 INTEGER NOT NULL, b BLOB NOT NULL); CREATE INDEX section_line_head ON section_line (blk0, m0)");
  const byBlock = new Map<number, Map<number, SectionLine>>();
  for (const [h, blk, from, to] of rows) {
    const k = h >> BLOCK_BITS;
    if (!byBlock.has(k)) byBlock.set(k, new Map());
    byBlock.get(k)!.set(h & ((1 << BLOCK_BITS) - 1), { blk, from, to });
  }
  const put = db.prepare("INSERT INTO section_line (block, n, blk0, m0, b) VALUES (?, ?, ?, ?, ?)");
  for (const [k, slots] of byBlock) {
    const n = Math.max(...slots.keys()) + 1, first = slots.get(Math.min(...slots.keys()))!;
    const lines: SectionLine[] = [];
    let prev: SectionLine = { blk: first.blk, from: first.from, to: first.from };
    for (let i = 0; i < n; i++) {
      const l = slots.get(i);
      lines.push(l ?? prev);
      prev = l ? { blk: l.blk, from: l.to, to: l.to } : prev;
    }
    put.run(k, n, lines[0]!.blk, lines[0]!.from, encodeLineBlock(lines));
  }
  return {
    all: async (sql, ...params) => (db.prepare(sql).all(...params) as Record<string, unknown>[]).map(realm),
    get: async (sql, ...params) => { const r = db.prepare(sql).get(...params) as Record<string, unknown> | undefined; return r && realm(r); },
  };
}

/** (T369) `d` with `lines` answering every read of `section_line` (the regs-sid file of a release cut before it). */
function withSectionLines(d: Db, lines: Db): Db {
  const pick = (sql: string) => (/section_line/.test(sql) ? lines : d);
  return { all: (sql, ...p) => pick(sql).all(sql, ...p), get: (sql, ...p) => pick(sql).get(sql, ...p), close: d.close };
}

/** (T369) The regs fixture release with stream lines (`rows`: the fixture's own unless given). */
export function openRegsFixtureWithLines(rows: readonly LineRow[] = regsFixtureLines().lines,
                                         tamper: ReleaseTamper = {}): ReleaseLoader {
  const lines = sectionLineDb(rows);
  return openReleaseDir(regsFixtureDir(), { ...tamper,
    db: (name, d) => { const t = tamper.db ? tamper.db(name, d) : d; return name === "regs-sid" ? withSectionLines(t, lines) : t; } });
}
