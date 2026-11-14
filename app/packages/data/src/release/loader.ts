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
         type SectionAccess, now } from "@app/core";
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
   * (T360) Close every database connection not read (nor shrunk) for `idleMs` and not in use now. The handles stay good:
   * the next read opens the file again, checked again. Resolves to how many were closed; rejects with the first close
   * that failed, after every close was tried (the caller reports it).
   */
  closeIdle(idleMs: number): Promise<number>;
  /**
   * (T360) This set was REPLACED (an applied update): every connection closes as soon as its last read is out, and none
   * stays open after. A screen that still reads the old set before it re-renders onto the new one is ANSWERED, never
   * refused (a normal update is no failure): the file (kept until the next launch) is opened for that read and closed
   * when it is out. Resolves, once the open ones are closed, to how many were; rejects with the first close that failed,
   * after every close was tried. A later close (after such a read) that fails goes to `failed`.
   */
  retire(failed: (e: unknown) => void): Promise<number>;
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

export function releaseLoader(m: Manifest, source: ReleaseSource, open: Opener,
                              o: { now?: () => number } = {}): ReleaseLoader {
  const clock = o.now ?? now;
  const handles = Object.values(m.files)[0]?.section_handles ?? "";
  const dbs = new Map<DbName, Promise<Db>>();
  const order: FileName[] = [];
  let paint: Promise<Paint> | null = null, area: Promise<Float64Array> | null = null;
  let lakes: Promise<LakeFacts | null> | null = null;
  let salmonHead: Promise<SalmonFacts["payload"]> | null = null;
  const slices = new Map<string, Promise<SalmonShard>>();
  const salmonMemo = new Map<string, { head: SalmonFacts["payload"]; read: SalmonShard[]; facts: SalmonFacts }>();
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

  /**
   * (T360) A file's ONE handle: what `db()` hands out and every reader keeps. Its connection may be closed while idle
   * (`closeIdle`, on a trim) and the next read opens it again, checked again (0.1–0.3 ms on a phone: the file is opened
   * in place). A read or a shrink in flight is never closed under: each counts in `busy`, and a close waits for none of
   * them but skips (closeIdle) or waits out (retire) a busy connection. A retired loader (`retire`) keeps no
   * connection open past the read that needed it.
   */
  interface Conn { raw: Promise<Db> | null; last: number; busy: number; drained: (() => void)[] }
  const conns = new Map<DbName, Conn>();
  /** Set by `retire`: where a close after a retired set's late read reports a failure. */
  let retired: ((e: unknown) => void) | null = null;
  const connect = (name: DbName, spec: NonNullable<Manifest["files"][string]>): Promise<Db> =>
    open(name, () => source.bytes(name)).then(async (d) => {
      const meta = Object.fromEntries((await d.all("SELECT k, v FROM meta")).map((r) => [r.k, r.v]));
      if (meta.format !== spec.format || meta.section_handles !== spec.section_handles)
        throw new ReleaseError(`${name}: format ${meta.format} / handles ${meta.section_handles} are not the manifest's ` +
                               `${spec.format} / ${spec.section_handles}`);
      return d;
    });
  /** Run `f` with the connection counted busy; the last one out wakes whoever waits to close it. */
  const busy = async <T>(c: Conn, f: () => Promise<T>): Promise<T> => {
    c.busy++;
    try { return await f(); }
    finally {
      c.busy--; c.last = clock();
      if (c.busy === 0) {
        const waiting = c.drained.splice(0);
        for (const wake of waiting) wake();
        // (T360) a retired set's late read is out: its connection closes now (retire's own wait closes the rest)
        if (retired && !waiting.length && c.raw) shut(c).catch(retired);
      }
    }
  };
  /**
   * Close `c`'s connection: taken off the handle at once (synchronously, so no read starts on it while it closes), then
   * closed and the close awaited. True: closed; false: it never opened (its failure was the opener's to say). A driver
   * that cannot close keeps its connection on the handle and is not counted (a fake: every release driver closes).
   */
  const shut = async (c: Conn): Promise<boolean> => {
    const p = c.raw;
    if (!p) return false;
    c.raw = null;
    const d = await p.then((x) => x, () => null);
    if (!d) return false;
    if (!d.close) { if (c.raw === null) c.raw = p; return false; }
    await d.close();
    return true;
  };
  /** Every close of `jobs` run to its end; the count of real closes, or the first failure (after all were tried). */
  const settle = async (jobs: Promise<boolean>[]): Promise<number> => {
    const done = await Promise.allSettled(jobs);
    const failed = done.find((r): r is PromiseRejectedResult => r.status === "rejected");
    if (failed) throw failed.reason;
    return done.filter((r) => r.status === "fulfilled" && r.value).length;
  };
  const handle = (name: DbName, spec: NonNullable<Manifest["files"][string]>, first: Promise<Db>): Db => {
    const c: Conn = { raw: first, last: clock(), busy: 0, drained: [] };
    conns.set(name, c);
    const use = <T>(f: (d: Db) => Promise<T>): Promise<T> => busy(c, async () => {
      if (!c.raw) {
        const p = connect(name, spec);
        c.raw = p;
        p.catch(() => { if (c.raw === p) c.raw = null; });
      }
      return f(await c.raw);
    });
    return {
      all: (sql, ...params) => use((d) => d.all(sql, ...params)),
      get: (sql, ...params) => use((d) => d.get(sql, ...params)),
    };
  };

  const db = (name: DbName): Promise<Db> => {
    const spec = m.files[name];
    if (!spec) return Promise.reject(new ReleaseError(`release ${m.data_release} has no file ${name}`));
    let p = dbs.get(name);
    if (!p) {
      touch(name);
      const first = connect(name, spec);
      p = first.then(() => {
        const h = handle(name, spec, first);
        // (T360) opened on a retired set: closed until a read needs it (that read opens it, then it closes again)
        if (retired) shut(conns.get(name)!).catch(retired);
        return h;
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
      const headP = salmonHead;
      const head = await headP;
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
      // (T287) the same head and slices answer with the same object: a map handed it again relabels nothing
      const memo = salmonMemo.get(asked.join(","));
      if (memo && memo.head === head && memo.read.length === read.length && memo.read.every((x, i) => x === read[i])) return memo.facts;
      const facts = openProbe.work(`salmon facts ${asked.join(",")}`, () => salmonFacts(head, handles, read));
      salmonMemo.set(asked.join(","), { head, read, facts });
      return facts;
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
      // a read of tables every water file holds whole (`meta`) with no slice open yet: the small shared file, never the
      // whole water (the address's regs store opens before the page's region is focused — the request walk, T129)
      if (shared && Object.keys(keys).length === 0) return db(shared);
      return db(name);
    },
    opened: () => order.slice(),
    async trim() {
      // only the open ones: a database still opening (or that failed), or closed, is not waited for. The shrink counts
      // as busy, so no close (an overlapping trim's, a retire) shuts the connection under it.
      let n = 0;
      for (const c of conns.values()) {
        if (!c.raw) continue;
        const raw = c.raw;
        n += await busy(c, async () => {
          const d = await raw.then((x) => x, () => null);
          if (!d?.shrink) return 0;
          await d.shrink();
          return 1;
        });
      }
      return n;
    },
    closeIdle(idleMs) {
      const t = clock();
      // checked and taken in one synchronous pass: a read starting meanwhile finds the connection gone and reopens
      return settle([...conns.values()].filter((c) => c.raw && c.busy === 0 && t - c.last >= idleMs).map(shut));
    },
    retire(failed) {
      retired = failed;
      // each connection closes once its last read or shrink is out; a later read's connection closes after it (`busy`)
      return settle([...conns.values()].map(async (c) => {
        while (c.busy > 0) await new Promise<void>((r) => c.drained.push(r));
        return shut(c);
      }));
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
  /*
   * (T287) THE DECODES ANOTHER SET OF THE SAME FILES MAY TAKE OVER (`adoptDecoded`): a file is the same when its
   * manifest entry is (sha256 and section numbering); flow-area also needs the same paint (its length is paint's).
   */
  HELD.set(loader, {
    m,
    take(prev) {
      const same = (n: string) => !!m.files[n] && m.files[n]!.sha256 === prev.m.files[n]?.sha256
        && m.files[n]!.section_handles === prev.m.files[n]?.section_handles;
      const took: FileName[] = [];
      const p = prev.slots();
      if (p.paint && same("paint")) { paint = p.paint; took.push("paint"); }
      if (p.area && same("flow-area") && took.includes("paint")) { area = p.area; took.push("flow-area"); }
      if (p.lakes && same("lakes")) { lakes = p.lakes; took.push("lakes"); }
      if (p.method && same("method")) { method = p.method; took.push("method"); }
      if (p.access && same("access")) { access = p.access; took.push("access"); }
      if (p.salmonHead && same("salmon")) {
        salmonHead = p.salmonHead; took.push("salmon");
        for (const [r, sp] of p.slices) if (same(`salmon-${r}`)) { slices.set(r, sp); took.push(`salmon-${r}` as FileName); }
        for (const [k, v] of p.salmonMemo) salmonMemo.set(k, v);
      }
      for (const n of took) touch(n);
      return took;
    },
    slots: () => ({ paint, area, lakes, method, access, salmonHead, slices, salmonMemo }),
  });
  return loader;
}

/** What `adoptDecoded` reads and writes on a loader made here (a fake loader has none, and adopts nothing). */
interface Held {
  m: Manifest;
  take(prev: Held): FileName[];
  slots(): { paint: Promise<Paint> | null; area: Promise<Float64Array> | null; lakes: Promise<LakeFacts | null> | null;
             method: Promise<MethodFacts | null> | null; access: Promise<SectionAccess | null> | null;
             salmonHead: Promise<SalmonFacts["payload"]> | null; slices: ReadonlyMap<string, Promise<SalmonShard>>;
             salmonMemo: ReadonlyMap<string, { head: SalmonFacts["payload"]; read: SalmonShard[]; facts: SalmonFacts }> };
}
const HELD = new WeakMap<ReleaseLoader, Held>();
/** A loader's own, or the one it is laid over (`withLiveSalmon` makes its loader with `Object.create`). */
function heldOf(l: ReleaseLoader): Held | undefined {
  for (let o: object | null = l; o; o = Object.getPrototypeOf(o) as object | null) {
    const h = HELD.get(o as ReleaseLoader);
    if (h) return h;
  }
  return undefined;
}

/**
 * (T287) AN APPLIED UPDATE KEEPS WHAT DID NOT CHANGE: every decoded file of the set on screen (paint, method, lakes,
 * access, flow-area, the salmon head and slices) whose manifest entry the new set repeats — same sha256, same section
 * numbering — is handed over as it is, the same objects. The screens then re-key onto the very answers they were
 * drawing, and the map relabels nothing for them. Returns the files taken.
 */
export function adoptDecoded(next: ReleaseLoader, prev: ReleaseLoader): FileName[] {
  const n = heldOf(next), p = heldOf(prev);
  return n && p ? n.take(p) : [];
}

/**
 * (T287) WARM THE NEW SET LIKE THE OLD ONE, BEFORE THE SWAP: every decoded file the set in use had read (the Bait & fly
 * marks, the salmon slices on screen, the lake values, …) is decoded on the new set too, in slices, while the old one
 * is still on screen — so the moment the new set is committed nothing is left to decode, and the screens re-key onto
 * answers that are already there (one map relabel, not one per file as each decode lands). Failures are left for the
 * screen that asks (it says so, as it would have).
 */
export async function warmLike(next: ReleaseLoader, prev: ReleaseLoader): Promise<void> {
  adoptDecoded(next, prev);
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
