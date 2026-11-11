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
import { encodeLineBlock, LINE_OPEN, type SectionLine } from "../../regs/line";
import { openLineReads } from "../../regs/place";
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
  /** (T369) a file the folder lacks, served in place of its bytes (the manifest must name it: `manifest`) */
  files?: Partial<Record<DbName, Db>>;
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
  const disk = nodeOpener(), wrap = tamper.db, given = tamper.files ?? {};
  const open: Opener = async (name, bytes) => given[name] ?? disk(name, bytes);
  return releaseLoader(m, source, wrap ? async (name, bytes) => wrap(name, await open(name, bytes)) : open);
}

/** The regs fixture release, opened as the app opens a release. */
export const openRegsFixture = (): ReleaseLoader => openReleaseDir(regsFixtureDir());

/** (T369) One stream section's line as the fixture lists it: [handle, stream key, from m, to m). */
export type LineRow = readonly [number, number, number, number];

/**
 * (T369) The regs fixture's stream lines (`dev/section-lines.json`: its waters' sections and one unnamed reach) — the
 * fixture release was cut before the `lines` file, so tests add it (`openRegsFixtureWithLines`).
 */
export function regsFixtureLines(): { unnamed: number; lines: LineRow[] } {
  const d = JSON.parse(readFileSync(join(REGS_FIXTURE_ROOT, "..", "section-lines.json"), "utf8")) as
    { unnamed: number; lines: LineRow[] };
  return { unnamed: d.unnamed, lines: d.lines };
}

/**
 * (T369) A `lines` file (`@app/data` line.ts's format, its own writer) over `rows`, in memory, its meta `lines/1` on
 * `handles`. Only the blocks the rows touch are written; a slot no row names is a filler that keeps the block sorted and
 * spans nothing (the slot before it's line, empty) — a fixture's handles are a sample of the atlas, a release's table
 * has every stream handle. Every row's end is kept as given (closed), as the fixture lists it.
 */
function sectionLineDb(rows: readonly LineRow[], handles: string): Db {
  const db = new DatabaseSync(":memory:");
  db.exec("CREATE TABLE section_line (block INTEGER PRIMARY KEY, n INTEGER NOT NULL, blk0 INTEGER NOT NULL, " +
          "m0 INTEGER NOT NULL, b BLOB NOT NULL); CREATE INDEX section_line_head ON section_line (blk0, m0); " +
          "CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)");
  db.prepare("INSERT INTO meta VALUES ('format', 'lines/1'), ('section_handles', ?)").run(handles);
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
    // (T441) after an OPEN line (a stream of one section, as the release's own `lines` file writes it) a filler cannot
    // follow on its stream (only a run's last slot may be open): it spans nothing at the NEXT row's start instead
    const nextOf = (i: number) => { for (let j = i + 1; j < n; j++) { const l = slots.get(j); if (l) return l; } return null; };
    for (let i = 0; i < n; i++) {
      const l = slots.get(i);
      if (l) lines.push(l);
      else if (prev.to !== LINE_OPEN) lines.push(prev);
      else { const nx = nextOf(i)!; lines.push({ blk: nx.blk, from: nx.from, to: nx.from }); }
      if (l) prev = l.to === LINE_OPEN ? l : { blk: l.blk, from: l.to, to: l.to };
    }
    put.run(k, n, lines[0]!.blk, lines[0]!.from, encodeLineBlock(lines));
  }
  return {
    all: async (sql, ...params) => (db.prepare(sql).all(...params) as Record<string, unknown>[]).map(realm),
    get: async (sql, ...params) => { const r = db.prepare(sql).get(...params) as Record<string, unknown> | undefined; return r && realm(r); },
  };
}

/** (T369) The regs fixture release with stream lines (`rows`: the fixture's own unless given). */
export function openRegsFixtureWithLines(rows: readonly LineRow[] = regsFixtureLines().lines,
                                         tamper: ReleaseTamper = {}): ReleaseLoader {
  const dir = regsFixtureDir();
  const sid = parseManifest(JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8"))).files["regs-sid"]!;
  const lines = sectionLineDb(rows, sid.section_handles);
  return openReleaseDir(dir, { ...tamper, files: { lines, ...tamper.files },
    manifest: (m0) => {
      const m = tamper.manifest ? tamper.manifest(m0) : m0;
      return { ...m, files: { ...m.files, lines: { ...sid, url: "lines-fixture.sqlite.gz", format: "lines/1" } } };
    } });
}

/**
 * (T441) A release's section ids BY WHAT THEY ARE, so no test types a raw id (a re-cut renumbers them; the test must not
 * notice). A stream section is named by its stream key and where it STARTS (`stream`: the release's own `lines` file);
 * a lake's section, which has no line, by its water and part (`lake`: the part's one section that is no stream line).
 * Each throws naming what it was asked for when the release has no such section — a re-cut that MOVES a section's start
 * or part is a real change and fails there, loudly; only the renumbering is absorbed. (T667) A part is named the same
 * way: by its two ends (`part`) or by a place on it (`partAt`), so `lake(item, await names.part(item, lo, hi))` types
 * no number at all. `sectionNames.test.ts` pins both halves by mutation; `tools/check-test-anchors.mjs` finds the tests
 * that still type a number.
 */
export interface SectionNames {
  /** the stream section of stream key `blk` that starts at metre `from` */
  stream(blk: number, from: number): Promise<number>;
  /** `stream` for each [stream key, start metre], in order */
  streams(at: readonly (readonly [number, number])[]): Promise<number[]>;
  /** the one non-stream section of water `item`'s part `part` (a lake's) */
  lake(item: string, part: number): Promise<number>;
  /**
   * (T667) A PART BY ITS ENDS, never by its number (`part_ix` is river order: one more cut below renumbers every part
   * above it): the part of water `item` that runs from end `lo` up to end `hi` — the part's own end tokens (`part.lo_s` /
   * `hi_s`: "mouth", "source", "lake_outlet", a boundary's token, `area:…`, `continues:…`).
   */
  part(item: string, lo: string, hi: string): Promise<number>;
  /**
   * (T667) A part BY A PLACE ON IT, for a part with no end tokens (a side channel, a tidal reach): the part of water
   * `item` whose lines (`part_line`) hold stream key `blk` at metre `m`.
   */
  partAt(item: string, blk: number, m: number): Promise<number>;
}

/** `loader`'s section names; it must carry a `lines` file for `stream` (`openRegsFixtureWithLines`, a live release). */
export function sectionNames(loader: ReleaseLoader): SectionNames {
  const lines = openLineReads(loader);
  const stream = async (blk: number, from: number) => {
    const h = await lines.sectionAt(blk, from), l = h === null ? null : await lines.sectionLine(h);
    if (h === null || !l || l.blk !== blk || l.from !== from)
      throw new Error(`section names: no stream section starts at ${blk}:${from} in release ${loader.manifest.data_release}`);
    return h;
  };
  return {
    stream,
    streams: (at) => Promise.all(at.map(([blk, from]) => stream(blk, from))),
    async lake(item, part) {
      const water = await loader.db("water");
      const rows = await water.all(
        "SELECT ps.sid FROM part_section ps JOIN item i ON i.item_ord = ps.ord JOIN str s ON s.id = i.item_id " +
        "WHERE s.text = ? AND ps.part_ix = ? ORDER BY ps.sid", item, part);
      const own: number[] = [];
      for (const r of rows) if (!(await lines.sectionLine(Number(r.sid)))) own.push(Number(r.sid));
      if (own.length !== 1)
        throw new Error(`section names: ${item} part ${part} has ${own.length} non-stream sections, not 1`);
      return own[0]!;
    },
    async part(item, lo, hi) {
      const water = await loader.db("water");
      const rows = await water.all(
        "SELECT p.part_ix FROM part p JOIN item i ON i.item_ord = p.item_ord JOIN str s ON s.id = i.item_id " +
        "JOIN str lo ON lo.id = p.lo_s JOIN str hi ON hi.id = p.hi_s WHERE s.text = ? AND lo.text = ? AND hi.text = ?",
        item, lo, hi);
      if (rows.length !== 1)
        throw new Error(`section names: ${item} has ${rows.length} parts from ${lo} to ${hi}, not 1, in release ${loader.manifest.data_release}`);
      return Number(rows[0]!.part_ix);
    },
    async partAt(item, blk, m) {
      const water = await loader.db("water");
      const rows = await water.all(
        "SELECT DISTINCT pl.part_ix FROM part_line pl JOIN item i ON i.item_ord = pl.ord JOIN str s ON s.id = i.item_id " +
        "WHERE s.text = ? AND pl.blk = ? AND pl.m0 <= ? AND ? < pl.m1", item, blk, m, m);
      if (rows.length !== 1)
        throw new Error(`section names: ${item} has ${rows.length} parts at ${blk}:${m}, not 1, in release ${loader.manifest.data_release}`);
      return Number(rows[0]!.part_ix);
    },
  };
}
