/**
 * (T253) THE BACKGROUND PREFETCH: the order (view → neighbours → province; shared files with the first region), the
 * gate (offline, hidden, Save-Data, metered without the T231 setting, low battery, slow by the browser's word or by
 * measurement), pause / resume, cancellation, a failed file left for the tap, the events the Updates log reads, and
 * that a prefetched file is the cache the tap reads — the real web source, no second download.
 */
import { describe, expect, it } from "vitest";
import type { UpdateEvent } from "@app/core";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import { REGIONS, WEB_BUDGET } from "../pack.generated";
import type { Db } from "../sql/db";
import { releaseLoader, type DbName, type FileName, type ReleaseLoader } from "./loader";
import { createBackgroundPrefetch, FAST_BYTES_PER_S, NET_DEFAULT, prefetchHold, REGION_NEAR, regionOrder,
         unnamedTapFiles, type NetSignals } from "./background";
import { releaseCache } from "./drivers/cache.web";
import { releaseSource } from "./drivers/source.web";

const enc = (s: string) => new TextEncoder().encode(s);
const H = "d768d6e135e62b99";
const SHARED = ["core", "water", "access", "regs-sid", "down", "salmon"];
const ALL = [...SHARED, ...REGIONS.flatMap((r) => [`regs-${r}`, `salmon-${r}`])];

function manifest(names: readonly string[] = ALL, gz = 1000): Manifest {
  return parseManifest({
    format: "app-manifest/2", data_release: "r-1",
    tiles: { compatible: [`${H}.*`], current: null, assets: null, mask: null }, basemap: null,
    files: Object.fromEntries(names.map((k) => [k, {
      url: `r-1/${k}-x.sqlite.gz`, bytes: enc(k).length, gz, sha256: sha256Hex(enc(k)),
      format: k.startsWith("regs-") ? "regs-region/1" : `${k}/1`, section_handles: H }])),
    regulations: { valid_until: null, versions: null },
  });
}

/** A loader whose prefetches resolve (or fail) only when the test says. */
function fakeLoader(m = manifest()) {
  const pending: { name: string; ok: () => void; no: (e: Error) => void }[] = [];
  const held = new Set<string>();
  const loader = {
    manifest: m,
    prefetch: (n: string) => new Promise<void>((ok, no) => {
      held.add(n);
      pending.push({ name: n, ok, no: (e) => { held.delete(n); no(e); } });
    }),
    prefetched: (n: string) => held.has(n),
  } as unknown as ReleaseLoader;
  const tick = () => new Promise((r) => setTimeout(r, 0));
  /** finish the oldest in-flight file */
  const finish = async (fail = false) => {
    const p = pending.shift()!;
    if (fail) p.no(new Error("HTTP 503")); else p.ok();
    await tick(); await tick();
    return p.name;
  };
  return { loader, pending, held, finish, tick };
}

describe("what and in which order", () => {
  it("an unnamed tap's files are the contract's list, per region, the manifest's only", () => {
    expect(WEB_BUDGET.first_tap_unnamed!.files).toEqual(
      ["core", "water", "access", "regs-{region}", "regs-sid", "down", "salmon", "salmon-{region}"]);
    expect(unnamedTapFiles(manifest(), "7a")).toEqual(
      ["core", "water", "access", "regs-7a", "regs-sid", "down", "salmon", "salmon-7a"]);
    // a release cut without salmon: those are skipped, not asked for
    expect(unnamedTapFiles(manifest(ALL.filter((f) => !f.startsWith("salmon"))), "2")).toEqual(
      ["core", "water", "access", "regs-2", "regs-sid", "down"]);
  });

  it("the region in view, its neighbours, then the rest of the province", () => {
    expect(regionOrder(["2"])).toEqual(["2", "1", "3", "5", "8", "4", "6", "7a", "7b"]);
    expect(regionOrder([])).toEqual([...REGIONS]);
    expect(new Set(regionOrder(["7b"]))).toEqual(new Set(REGIONS));
  });

  it("the neighbours are symmetric and every region has some", () => {
    for (const r of REGIONS) {
      expect(REGION_NEAR[r].length).toBeGreaterThan(0);
      for (const n of REGION_NEAR[r]) expect(REGION_NEAR[n]).toContain(r);
    }
  });
});

describe("prefetchHold: the gate", () => {
  const go: NetSignals = { ...NET_DEFAULT, effectiveType: "4g" };
  const cases: [string, Partial<NetSignals>, string | null][] = [
    ["4g, visible, unmetered: go", {}, null],
    ["offline", { online: false }, "offline"],
    ["the tab hidden / the app in the background", { hidden: true }, "background"],
    ["Save-Data", { saveData: true }, "save-data"],
    ["3g", { effectiveType: "3g" }, "slow"],
    ["2g", { effectiveType: "2g" }, "slow"],
    ["metered (cellular), mobile data not allowed", { metered: true }, "metered"],
    ["metered, the reader allowed large downloads on mobile data (T231)", { metered: true, allowMetered: true }, null],
    ["low battery, not charging", { lowBattery: true }, "battery"],
    ["the browser says nothing: go until measured", { effectiveType: null }, null],
    ["says nothing, measured fast", { effectiveType: null, measuredBps: FAST_BYTES_PER_S * 4 }, null],
    ["says nothing, measured slow", { effectiveType: null, measuredBps: FAST_BYTES_PER_S / 2 }, "slow"],
    ["the browser's word wins over a measurement", { effectiveType: "4g", measuredBps: 10 }, null],
  ];
  for (const [what, s, want] of cases) it(what, () => expect(prefetchHold({ ...go, ...s })).toBe(want));
});

describe("createBackgroundPrefetch: scheduling", () => {
  it("nothing before start; then one file at a time, the view's region first with the shared files", async () => {
    const f = fakeLoader();
    const bg = createBackgroundPrefetch({ loader: f.loader, signals: { effectiveType: "4g" } });
    bg.view(["5"]);
    expect(f.pending.length).toBe(0);
    expect(bg.snapshot().state).toBe("waiting");
    bg.start();
    expect(f.pending.map((p) => p.name)).toEqual(["core"]);
    const got: string[] = [];
    for (let i = 0; i < 8; i++) got.push(await f.finish());
    expect(got).toEqual(unnamedTapFiles(f.loader.manifest, "5"));
    expect(bg.snapshot().ready).toEqual(["5"]);
    // the next region is a neighbour of 5, and only its own two files are left
    const next = [await f.finish(), await f.finish()];
    expect(next).toEqual(["regs-1", "salmon-1"]);
    expect(bg.snapshot().ready).toEqual(["5", "1"]);
  });

  it("files already held (the idle tier's core + regs) are skipped; a region ready at start is said at once", async () => {
    const f = fakeLoader();
    for (const n of unnamedTapFiles(f.loader.manifest, "2")) f.held.add(n);
    const events: UpdateEvent[] = [];
    const bg = createBackgroundPrefetch({ loader: f.loader, signals: { effectiveType: "4g" }, onEvent: (e) => events.push(e) });
    bg.view(["2"]);
    bg.start();
    expect(events.map((e) => [e.source, e.kind, e.note])).toEqual([["prefetch", "prefetched", "Region 2 ready"]]);
    expect(f.pending.map((p) => p.name)).toEqual(["regs-1"]);
  });

  it("the view moves: what is fetched next follows it", async () => {
    const f = fakeLoader();
    const bg = createBackgroundPrefetch({ loader: f.loader, signals: { effectiveType: "4g" } });
    bg.view(["2"]); bg.start();
    expect(await f.finish()).toBe("core");
    bg.view(["7b"]);                                // core done, water in flight: then 7b's own regs before 2's
    expect(await f.finish()).toBe("water");
    for (let i = 0; i < 4; i++) await f.finish();   // access, regs-7b, regs-sid, down
    expect(f.pending.map((p) => p.name)).toEqual(["salmon"]);
    await f.finish(); await f.finish();
    expect(bg.snapshot().ready).toEqual(["7b"]);
  });

  it("concurrency: at most N in flight", () => {
    const f = fakeLoader();
    const bg = createBackgroundPrefetch({ loader: f.loader, concurrency: 2, signals: { effectiveType: "4g" } });
    bg.start();
    expect(f.pending.length).toBe(2);
  });

  it("everything held: done, every region ready", async () => {
    const f = fakeLoader(manifest(["core", "water", "access", "regs-sid", "down", ...REGIONS.map((r) => `regs-${r}`)]));
    const bg = createBackgroundPrefetch({ loader: f.loader, signals: { effectiveType: "4g" } });
    bg.start();
    while (f.pending.length) await f.finish();
    const s = bg.snapshot();
    expect([s.state, s.ready.length, s.total, s.files, s.bytes]).toEqual(["done", 9, 9, 14, 14_000]);
  });
});

describe("createBackgroundPrefetch: holds, cancellation, failures", () => {
  it("a hold pauses (the file in flight finishes, nothing new starts); its end resumes", async () => {
    const f = fakeLoader();
    const events: UpdateEvent[] = [];
    const bg = createBackgroundPrefetch({ loader: f.loader, signals: { effectiveType: "4g" }, onEvent: (e) => events.push(e) });
    bg.start();
    bg.signals({ hidden: true });
    expect(await f.finish()).toBe("core");          // in flight: lands in the cache
    expect(f.pending.length).toBe(0);
    expect([bg.snapshot().state, bg.snapshot().hold]).toEqual(["paused", "background"]);
    bg.signals({ hidden: false, effectiveType: "3g" });
    expect([f.pending.length, bg.snapshot().hold]).toEqual([0, "slow"]);
    bg.signals({ effectiveType: "4g" });
    expect(f.pending.map((p) => p.name)).toEqual(["water"]);
    expect(events.filter((e) => e.kind !== "prefetched").map((e) => [e.kind, e.note]))
      .toEqual([["paused", "background"], ["paused", "slow"], ["resumed", "after slow"]]);
  });

  it("not started while Save-Data / metered (no mobile-data setting) / offline", () => {
    for (const s of [{ saveData: true }, { metered: true }, { online: false }, { lowBattery: true }] as const) {
      const f = fakeLoader();
      const bg = createBackgroundPrefetch({ loader: f.loader, signals: { effectiveType: "4g", ...s } });
      bg.start();
      expect([f.pending.length, bg.snapshot().state]).toEqual([0, "paused"]);
    }
  });

  it("metered with the T231 setting on: goes", () => {
    const f = fakeLoader();
    createBackgroundPrefetch({ loader: f.loader, signals: { effectiveType: "4g", metered: true, allowMetered: true } }).start();
    expect(f.pending.length).toBe(1);
  });

  it("cancel: nothing new after it, the file in flight still lands", async () => {
    const f = fakeLoader();
    const bg = createBackgroundPrefetch({ loader: f.loader, signals: { effectiveType: "4g" } });
    bg.start();
    bg.cancel();
    await f.finish();
    expect([f.pending.length, bg.snapshot().state]).toEqual([0, "cancelled"]);
    bg.start(); bg.signals({}); bg.view(["3"]);
    expect(f.pending.length).toBe(0);
  });

  it("a failed file is left for the tap; the next file goes on; a resume tries it again", async () => {
    const f = fakeLoader();
    const events: UpdateEvent[] = [];
    const bg = createBackgroundPrefetch({ loader: f.loader, signals: { effectiveType: "4g" }, onEvent: (e) => events.push(e) });
    bg.start();
    expect(await f.finish(true)).toBe("core");
    expect(f.pending.map((p) => p.name)).toEqual(["water"]);
    expect(events.find((e) => e.kind === "failed")).toMatchObject({ source: "prefetch", note: "core", error: "HTTP 503" });
    bg.signals({ online: false });
    await f.finish();                               // water lands
    bg.signals({ online: true });
    expect(f.pending.map((p) => p.name)).toEqual(["core"]);
  });

  it("a connection the browser says nothing of: measured on its own files, slow pauses it", async () => {
    let t = 0;
    const f = fakeLoader(manifest(ALL, 100_000));
    const bg = createBackgroundPrefetch({ loader: f.loader, now: () => t, signals: { effectiveType: null } });
    bg.start();
    t += 200;                                       // 100 kB in 200 ms = 500 kB/s: fast
    await f.finish();
    expect(f.pending.length).toBe(1);
    t += 2000;                                      // 100 kB in 2 s = 50 kB/s: slow
    await f.finish();
    expect([f.pending.length, bg.snapshot().hold]).toEqual([0, "slow"]);
  });
});

describe("the tap reads what the background fetched: the real web source and cache, one download per file", () => {
  it("prefetch every unnamed-tap file for region 2, then the tap's opens fetch nothing", async () => {
    const names = unnamedTapFiles(manifest(), "2");
    const m = manifest(names);
    const fetched: string[] = [];
    const name = (url: string) => url.replace(/^.*\/r-1\//, "").replace(/-x\.sqlite\.gz$/, "");
    const source = releaseSource(m, { base: "https://r2.test/release/", cache: await releaseCache(`t253-${Math.random()}`),
      held: null, fetch: (async (url: string) => { fetched.push(name(url)); return new Response(enc(name(url))); }) as typeof fetch });
    const L = releaseLoader(m, source, async (n: DbName, bytes) => {
      await bytes();
      return { all: async () => [{ k: "format", v: n.startsWith("regs-") ? "regs-region/1" : `${n}/1` },
                                 { k: "section_handles", v: H }], get: async () => undefined } as unknown as Db;
    });
    const bg = createBackgroundPrefetch({ loader: L, signals: { effectiveType: "4g" } });
    bg.view(["2"]); bg.start();
    for (let i = 0; i < 40 && bg.snapshot().state !== "done"; i++) await new Promise((r) => setTimeout(r, 0));
    expect(bg.snapshot().ready).toEqual(["2"]);
    expect([...fetched].sort()).toEqual([...names].sort());
    // the tap: every SQLite file it opens, and the binary salmon files read as bytes — no request more
    for (const n of names) {
      if (n.startsWith("salmon") || n === "access") await source.bytes(n as FileName);
      else await L.db(n as DbName);
    }
    expect(fetched.length).toBe(names.length);
  });
});
