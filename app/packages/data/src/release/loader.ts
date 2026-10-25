/**
 * THE RELEASE LOADER (P11): the app's one way to the data. Every file opens on first need and is held after that; the
 * paint is decoded once. Platform-free: the two things that differ are injected.
 *
 *   web      `source` = fetchVerified from `${base}${url}` (Cache Storage by immutable url, cache.web.ts); `open` = sql.js
 *            over the bytes. Lazy: paint first, core next, water + regs-<region> on the first water in a region (a tap in
 *            another region fetches that region's file), regs-sid + down on the first unnamed tap, geo with the first
 *            water map or "show on the map", near / conditions / climate on first use (T182).
 *   native   `source` = the installed file (install.ts put every file on disk, sha-verified); `open` = expo-sqlite in place.
 *
 * Every SQLite file is checked on open: its meta.format is the manifest's and its meta.section_handles the release's.
 */
import { decodeAccessFile, decodeLakeFacts, decodePaint, decodeFlowArea, type LakeFacts, type Paint,
         type SectionAccess } from "@app/core";
import type { Db } from "../sql/db";
import type { Manifest } from "../manifest";
import { type Region } from "../pack.generated";

export type FileName = "paint" | "flow-area" | "lakes" | "access" | "core" | "near" | "water" | `regs-${Region}` | "regs-sid"
  | "conditions" | "down" | "climate" | "geo";
export type DbName = Exclude<FileName, "paint" | "flow-area" | "lakes" | "access">;

export interface ReleaseSource {
  /** The RAW (decoded, verified) bytes of a release file. */
  bytes(name: FileName): Promise<Uint8Array>;
}
/**
 * Open one SQLite file. `bytes` fetches the verified file (the web opens it in sql.js); an opener that reads the file
 * in place (native: expo-sqlite on the installed copy) never calls it, so nothing is read into memory.
 */
export type Opener = (name: DbName, bytes: () => Promise<Uint8Array>) => Promise<Db>;

export class ReleaseError extends Error {}

export interface ReleaseLoader {
  readonly release: string;
  /** The manifest the files are checked against (its edition, tiles and versions). */
  readonly manifest: Manifest;
  readonly handles: string;
  db(name: DbName): Promise<Db>;
  paint(): Promise<Paint>;
  /** The target areas, in paint section order (opened with Conditions). */
  flowArea(): Promise<Float64Array>;
  /** The lake modes' values (`lakes`), checked against the release's handles; null when the manifest names none. */
  lakes(): Promise<LakeFacts | null>;
  /**
   * (T54) The gentler access tiers (`access`: permit-only land, reserves, private land), checked against the release's
   * handles; null when the manifest names none. Opened with a Regulations page; the no-access rows are paint's.
   */
  access(): Promise<SectionAccess | null>;
  /** The regions an answer key's frames are in (water.akey_region; loaded with water). */
  regionsOf(akey: number): Promise<Region[]>;
  /** The region file holding an akey's frames (the first of its regions; every one of them holds them). */
  regionDb(akey: number): Promise<Db>;
  /** Which files have been opened so far (the web's lazy-load test reads this). */
  opened(): readonly FileName[];
  /**
   * (T186) Fetch a file's bytes ahead of need WITHOUT opening it: the source's own fetch, so it lands in the release
   * cache the later open reads (web; a phone's files are installed and never prefetched). Once per file per loader.
   */
  prefetch(name: FileName): Promise<void>;
  /** Whether a file was prefetched or opened already. */
  prefetched(name: FileName): boolean;
}

export function releaseLoader(m: Manifest, source: ReleaseSource, open: Opener): ReleaseLoader {
  const handles = Object.values(m.files)[0]?.section_handles ?? "";
  const dbs = new Map<DbName, Promise<Db>>();
  const order: FileName[] = [];
  let paint: Promise<Paint> | null = null, area: Promise<Float64Array> | null = null;
  let lakes: Promise<LakeFacts | null> | null = null;
  let access: Promise<SectionAccess | null> | null = null;
  let regions: Promise<Map<number, Region[]>> | null = null;
  const touch = (n: FileName) => { if (!order.includes(n)) order.push(n); };
  const ahead = new Map<FileName, Promise<void>>();

  const db = (name: DbName): Promise<Db> => {
    const spec = m.files[name];
    if (!spec) return Promise.reject(new ReleaseError(`release ${m.data_release} has no file ${name}`));
    let p = dbs.get(name);
    if (!p) {
      touch(name);
      p = open(name, () => source.bytes(name)).then(async (d) => {
        const meta = Object.fromEntries((await d.all("SELECT k, v FROM meta")).map((r) => [r.k, r.v]));
        if (meta.format !== spec.format || meta.section_handles !== spec.section_handles)
          throw new ReleaseError(`${name}: format ${meta.format} / handles ${meta.section_handles} are not the manifest's ` +
                                 `${spec.format} / ${spec.section_handles}`);
        return d;
      });
      p.catch(() => dbs.delete(name));          // a failure is not cached: the next ask tries again
      dbs.set(name, p);
    }
    return p;
  };

  const loader: ReleaseLoader = {
    release: m.data_release,
    manifest: m,
    handles,
    db,
    paint() {
      if (!paint) {
        touch("paint");
        paint = source.bytes("paint").then((b) => decodePaint(b, handles));
        paint.catch(() => { paint = null; });
      }
      return paint;
    },
    async flowArea() {
      if (!area) {
        touch("flow-area");
        area = Promise.all([loader.paint(), source.bytes("flow-area")]).then(([p, b]) => decodeFlowArea(b, p.sectionCount));
        area.catch(() => { area = null; });
      }
      return area;
    },
    lakes() {
      if (!lakes) {
        if (!m.files.lakes) return Promise.resolve(null);
        touch("lakes");
        lakes = source.bytes("lakes").then((b) => decodeLakeFacts(b, handles));
        lakes.catch(() => { lakes = null; });
      }
      return lakes;
    },
    access() {
      if (!access) {
        if (!m.files.access) return Promise.resolve(null);
        touch("access");
        access = source.bytes("access").then((b) => decodeAccessFile(b, handles));
        access.catch(() => { access = null; });
      }
      return access;
    },
    async regionsOf(akey) {
      if (!regions) {
        regions = db("water").then(async (w) => {
          const out = new Map<number, Region[]>();
          for (const r of await w.all("SELECT akey, region FROM akey_region ORDER BY akey, region")) {
            const a = Number(r.akey);
            out.set(a, [...(out.get(a) ?? []), String(r.region) as Region]);
          }
          return out;
        });
        regions.catch(() => { regions = null; });
      }
      return (await regions).get(akey) ?? [];
    },
    async regionDb(akey) {
      const rs = await loader.regionsOf(akey);
      if (!rs.length) throw new ReleaseError(`akey ${akey} is in no region`);
      // a region already opened that holds this key wins: a tap in a region the reader is already in fetches nothing
      const held = rs.find((r) => dbs.has(`regs-${r}`));
      return db(`regs-${held ?? rs[0]!}`);
    },
    opened: () => order.slice(),
    prefetch(name) {
      if (!m.files[name]) return Promise.reject(new ReleaseError(`release ${m.data_release} has no file ${name}`));
      let p = ahead.get(name);
      if (!p) {
        p = source.bytes(name).then(() => {});
        p.catch(() => ahead.delete(name));        // a failure is not kept: the next ask (or the open) tries again
        ahead.set(name, p);
      }
      return p;
    },
    prefetched: (name) => ahead.has(name) || order.includes(name),
  };
  return loader;
}
