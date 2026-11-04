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
import { openProbe } from "../openProbe";
import { oneShot } from "../sha256";
import { decodeAccessFile, decodeLakeFacts, decodeMethod, decodeMethodAsync, decodePaint, decodePaintAsync, decodeFlowArea,
         decodeSalmonHead, decodeSalmonShard, decodeSalmonShardAsync, salmonFacts, type LakeFacts, type MethodFacts, type Paint, type SalmonFacts, type SalmonShard,
         type SectionAccess } from "@app/core";
import type { Db } from "../sql/db";
import type { Manifest } from "../manifest";
import { type Region } from "../pack.generated";
import { covers, routedDb, sliceKeys, type SliceKeys, type Sliced } from "./focus";

/** (T211) the DFO salmon: a head (`salmon/4`) and one slice per bundle region (`salmon-<r>`, `x`: a section in none —
 *  owner 2026-10-28); (T213) bait ban / fly only per day (`method/1`) */
export type SalmonSlice = `salmon-${Region}` | "salmon-x";
type BinName = "salmon" | SalmonSlice | "method";

export type FileName = "paint" | "flow-area" | "lakes" | "access" | "core" | "near" | "water" | `regs-${Region}` | "regs-sid"
  | `core-${Region}` | `water-${Region}` | "water-shared"
  | BinName
  | "conditions" | "down" | "climate" | "geo" | "lines";
export type DbName = Exclude<FileName, "paint" | "flow-area" | "lakes" | "access" | BinName>;

export interface ReleaseSource {
  /**
   * The RAW (decoded, verified) bytes of a release file. (T254) `ahead`: fetched ahead of need (a prefetch), so on a poor
   * line it waits behind what the reader asked for (`@app/data` net `PRIORITY`); a source with no order ignores it.
   */
  bytes(name: FileName, o?: { ahead?: boolean }): Promise<Uint8Array>;
  /** (T238) Whether the file is KEPT on this device between visits (the web's release cache); absent: the source keeps
   *  nothing it can be asked about (a phone's installed set, a test). */
  stored?(name: FileName): Promise<boolean>;
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
  /** (T211) The DFO salmon (`salmon` + slices), checked against the release's handles; null when the manifest names none.
   *  `regions`: the bundle regions whose slices to read (a page's card: its own — owner 2026-10-28); none given: every
   *  slice (the Salmon map). Each file is fetched once and shared by every answer that reads it. */
  salmon(regions?: readonly Region[]): Promise<SalmonFacts | null>;
  /** (T213) Bait ban / fly fishing only per section and day (`method`), checked against the release's handles; null when
   *  the manifest names none (a release cut before T213). What the map's "Bait & fly" colouring reads. */
  method(): Promise<MethodFacts | null>;
  /**
   * (T54) The gentler access tiers (`access`: permit-only land, reserves, private land), checked against the release's
   * handles; null when the manifest names none. Opened with a Regulations page; the no-access rows are paint's.
   */
  access(): Promise<SectionAccess | null>;
  /** The regions an answer key's frames are in (water.akey_region; loaded with water). */
  regionsOf(akey: number): Promise<Region[]>;
  /** The region file holding an akey's frames (the first of its regions; every one of them holds them). */
  regionDb(akey: number): Promise<Db>;
  /**
   * (T256) The region the reader is looking at (web: the map's), or null. A read of core / water through `routed` that
   * its slice covers (`focus.ts`) reads `core-<r>` / `water-<r>` instead of the whole file, unless the whole file is
   * already here. A release cut without slices, or a phone, ignores it.
   */
  focus(region: Region | null): void;
  /** (T256) core or water, each read routed to the whole file or a slice that covers it (`focus.ts`). */
  routed(name: Sliced): Db;
  /** (T256) The file a read naming `keys` goes to: the whole file when it is here or no slice covers the keys. */
  dbFor(name: Sliced, keys: SliceKeys | null): Promise<Db>;
  /** Which files have been opened so far (the web's lazy-load test reads this). */
  opened(): readonly FileName[];
  /**
   * (T360) Android asked for memory back (`onTrimMemory`): every open database's page cache is shrunk (the files stay
   * open; the next read fills it again). The decoded files are not dropped: the screens showing them hold them. Resolves
   * to how many were shrunk; rejects with the first that failed (the caller reports it).
   */
  trim(): Promise<number>;
  /**
   * (T186) Fetch a file's bytes ahead of need WITHOUT opening it: the source's own fetch, so it lands in the release
   * cache the later open reads (web; a phone's files are installed and never prefetched). Once per file per loader.
   */
  prefetch(name: FileName): Promise<void>;
  /** Whether a file was prefetched or opened already. */
  prefetched(name: FileName): boolean;
  /**
   * (T238) Whether a file is kept on this device between visits (the web's release cache, by its immutable url); null
   * when the source cannot say (a phone holds its whole installed set; a test). Optional: a fake loader says nothing.
   */
  stored?(name: FileName): Promise<boolean | null>;
}

/**
 * (T287) A big decode (paint ~150 ms on a phone, method ~50, a salmon slice ~35) runs IN SLICES (`@app/core` sliced.ts):
 * the JS thread is never held for long, so a release swapped in or a first install opening never freezes the screen.
 * The test bed's "before" build (`oneShot`) decodes in one go, as before.
 */
function decoded<T>(label: string, now: () => T, sliced: () => Promise<T>): Promise<T> {
  return oneShot() ? Promise.resolve(openProbe.work(label, now)) : openProbe.workAsync(label, sliced);
}

export function releaseLoader(m: Manifest, source: ReleaseSource, open: Opener): ReleaseLoader {
  const handles = Object.values(m.files)[0]?.section_handles ?? "";
  const dbs = new Map<DbName, Promise<Db>>();
  const order: FileName[] = [];
  let paint: Promise<Paint> | null = null, area: Promise<Float64Array> | null = null;
  let lakes: Promise<LakeFacts | null> | null = null;
  let salmonHead: Promise<SalmonFacts["payload"]> | null = null;
  const slices = new Map<string, Promise<SalmonShard>>();
  let method: Promise<MethodFacts | null> | null = null;
  let access: Promise<SectionAccess | null> | null = null;
  let regions: Promise<Map<number, Region[]>> | null = null;
  const touch = (n: FileName) => { if (!order.includes(n)) order.push(n); };
  const ahead = new Map<FileName, Promise<void>>();
  let focused: Region | null = null;
  /** (T256) the slices opened so far, per file, in the order opened (a slice once read keeps answering what it covers) */
  const slicesOpen: Record<Sliced, Region[]> = { core: [], water: [] };
  const sliceOf = (name: Sliced, r: Region) => `${name}-${r}` as DbName;
/** (T370) the web file of the region-wide water tables, cut beside the slices (pipeline `slices.WATER_SHARED`) */
const WATER_SHARED: DbName = "water-shared";
  /** Whether the whole file is here or on its way (opened, asked ahead, or kept in this browser): then it is read. */
  const wholeHere = async (name: Sliced): Promise<boolean> =>
    dbs.has(name) || ahead.has(name) || (source.stored ? await source.stored(name).catch(() => false) : false);

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
        paint = source.bytes("paint").then((b) => decoded("decode paint", () => decodePaint(b, handles), () => decodePaintAsync(b, handles)));
        paint.catch(() => { paint = null; });
      }
      return paint;
    },
    async flowArea() {
      if (!area) {
        touch("flow-area");
        area = Promise.all([loader.paint(), source.bytes("flow-area")]).then(([p, b]) => openProbe.work("decode flow-area", () => decodeFlowArea(b, p.sectionCount)));
        area.catch(() => { area = null; });
      }
      return area;
    },
    lakes() {
      if (!lakes) {
        if (!m.files.lakes) return Promise.resolve(null);
        touch("lakes");
        lakes = source.bytes("lakes").then((b) => openProbe.work("decode lakes", () => decodeLakeFacts(b, handles)));
        lakes.catch(() => { lakes = null; });
      }
      return lakes;
    },
    async salmon(want) {
      if (!m.files.salmon) return null;
      if (!salmonHead) {
        touch("salmon");
        salmonHead = source.bytes("salmon").then((b) => openProbe.work("decode salmon", () => decodeSalmonHead(b, handles)));
        salmonHead.catch(() => { salmonHead = null; });
      }
      const head = await salmonHead;
      const all = (head.shards ?? []) as readonly string[];
      const asked = want ? all.filter((r) => want.includes(r as Region)) : all;
      const read = await Promise.all(asked.map((r) => {
        const name = `salmon-${r}` as SalmonSlice;
        let p = slices.get(r);
        if (!p) {
          touch(name);
          p = source.bytes(name).then((b) => decoded(`decode ${name}`, () => decodeSalmonShard(b, handles), () => decodeSalmonShardAsync(b, handles)));
          p.catch(() => slices.delete(r));
          slices.set(r, p);
        }
        return p;
      }));
      return salmonFacts(head, handles, read);
    },
    method() {
      if (!method) {
        if (!m.files.method) return Promise.resolve(null);
        touch("method");
        method = source.bytes("method").then((b) => decoded("decode method", () => decodeMethod(b, handles), () => decodeMethodAsync(b, handles)));
        method.catch(() => { method = null; });
      }
      return method;
    },
    access() {
      if (!access) {
        if (!m.files.access) return Promise.resolve(null);
        touch("access");
        access = source.bytes("access").then((b) => openProbe.work("decode access", () => decodeAccessFile(b, handles)));
        access.catch(() => { access = null; });
      }
      return access;
    },
    async regionsOf(akey) {
      if (!regions) {
        // akey_region: region-wide, so `water-shared` answers it as well as water (T370; a release cut before it: any
        // slice, which then holds it whole)
        regions = loader.dbFor("water", { shared: true }).then(async (w) => {
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
    focus(region) { focused = region; },
    routed(name) { return routedDb((sql, p) => loader.dbFor(name, sliceKeys(name, sql, p))); },
    async dbFor(name, keys) {
      if (keys === null || await wholeHere(name)) return db(name);
      // (T370) the region-wide water tables are one web file; a release cut before it holds them in every slice
      const shared = name === "water" && m.files[WATER_SHARED] ? WATER_SHARED : null;
      if (keys.shared && shared) return db(shared);
      const tries = [...slicesOpen[name]];
      if (focused && !tries.includes(focused) && m.files[sliceOf(name, focused)]) tries.push(focused);
      for (const r of tries) {
        const d = await db(sliceOf(name, r));
        if (!slicesOpen[name].includes(r)) slicesOpen[name].push(r);
        if (await covers(name, d, keys)) return d;
      }
      // a string the region-wide tables name (an akey's kind, a place's label) is held by `water-shared`
      if (shared && keys.strs?.length && await covers(name, await db(shared), keys)) return db(shared);
      return db(name);
    },
    opened: () => order.slice(),
    async trim() {
      // only the opened ones: a database still opening (or that failed) is not waited for
      const open = await Promise.all([...dbs.values()].map((p) => p.then((d) => d, () => null)));
      let n = 0;
      for (const d of open) if (d?.shrink) { await d.shrink(); n++; }
      return n;
    },
    prefetch(name) {
      if (!m.files[name]) return Promise.reject(new ReleaseError(`release ${m.data_release} has no file ${name}`));
      let p = ahead.get(name);
      if (!p) {
        p = source.bytes(name, { ahead: true }).then(() => {});
        p.catch(() => ahead.delete(name));        // a failure is not kept: the next ask (or the open) tries again
        ahead.set(name, p);
      }
      return p;
    },
    prefetched: (name) => ahead.has(name) || order.includes(name),
    async stored(name) {
      if (!m.files[name] || !source.stored) return null;
      return source.stored(name);
    },
  };
  return loader;
}

/**
 * (T287) WARM THE NEW SET LIKE THE OLD ONE, BEFORE THE SWAP: every decoded file the set in use had read (the Bait & fly
 * marks, the salmon slices on screen, the lake values, …) is decoded on the new set too, in slices, while the old one
 * is still on screen — so the moment the new set is committed nothing is left to decode, and the screens re-key onto
 * answers that are already there (one map relabel, not one per file as each decode lands). Failures are left for the
 * screen that asks (it says so, as it would have).
 */
export async function warmLike(next: ReleaseLoader, prev: ReleaseLoader): Promise<void> {
  const had = new Set(prev.opened());
  const regions = [...had].filter((n) => n.startsWith("salmon-")).map((n) => n.slice("salmon-".length) as Region);
  // one after another: each is sliced, and two at once would only interleave their slices
  const steps: (() => Promise<unknown>)[] = [() => next.paint()];
  if (had.has("method")) steps.push(() => next.method());
  if (had.has("salmon")) steps.push(() => next.salmon(regions));
  if (had.has("lakes")) steps.push(() => next.lakes());
  if (had.has("access")) steps.push(() => next.access());
  if (had.has("flow-area")) steps.push(() => next.flowArea());
  for (const s of steps) await s().catch(() => undefined);
}
