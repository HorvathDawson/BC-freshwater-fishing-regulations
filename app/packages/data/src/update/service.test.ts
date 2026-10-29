/**
 * (T231) THE UPDATE SERVICE over a mocked origin: unchanged (304, and a 200 of the same bytes), changed (handed whole to
 * its loader), failed (HTTP), offline (no request when the device says so; a thrown fetch), partial (one feed fails, the
 * rest land), a refused feed (the loader keeps what it had — the atomic swap — and the next check looks again), a sha
 * mismatch in a release download (nothing staged, retried), metered (a big release waits for Wi-Fi), apply, the
 * schedule (interval, backoff, foreground, back online) and what is kept across launches.
 */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { VerifyError } from "../fetchVerified";
import type { Held, Manifest } from "../manifest";
import { MANIFEST_FORMAT } from "../pack.generated";
import { checkObject } from "./check";
import { createUpdateService, releaseMoved, releaseNeed, type FeedSpec, type UpdateStore } from "./service";

const enc = (s: string) => new TextEncoder().encode(s);

/** An origin: path → body; ETag = sha12 of the body; 304 on a matching If-None-Match. Logs every request. */
function origin(init: Record<string, string>) {
  const files = new Map(Object.entries(init));
  const log: { url: string; inm: string | null; status: number }[] = [];
  let down: "offline" | number | null = null;
  const fetchImpl = (async (u: RequestInfo | URL, i?: RequestInit) => {
    const url = String(u);
    const inm = new Headers(i?.headers).get("If-None-Match");
    if (down === "offline") { log.push({ url, inm, status: 0 }); throw new TypeError("Network request failed"); }
    if (typeof down === "number") { log.push({ url, inm, status: down }); return new Response("no", { status: down }); }
    const body = files.get(url);
    if (body === undefined) { log.push({ url, inm, status: 404 }); return new Response("", { status: 404 }); }
    const etag = `"${sha256Hex(enc(body)).slice(0, 12)}"`;
    if (inm === etag) { log.push({ url, inm, status: 304 }); return new Response(null, { status: 304, headers: { ETag: etag } }); }
    log.push({ url, inm, status: 200 });
    return new Response(body, { status: 200, headers: { ETag: etag } });
  }) as typeof fetch;
  return { files, log, fetch: fetchImpl, set: (d: typeof down) => { down = d; } };
}

function memStore(): UpdateStore & { text: string | null } {
  const s = { text: null as string | null, read: () => s.text, write: (t: string) => { s.text = t; } };
  return s;
}

/** Manual timers: `run()` fires what is due at the clock's time. */
function clock(start = 1_000_000) {
  let t = start;
  const due: { at: number; fn: () => void; id: number }[] = [];
  let id = 0;
  return {
    now: () => t,
    timers: { set: (fn: () => void, ms: number) => { due.push({ at: t + ms, fn, id: ++id }); return id; },
              clear: (h: unknown) => { const i = due.findIndex((d) => d.id === h); if (i >= 0) due.splice(i, 1); } },
    next: () => (due.length ? Math.min(...due.map((d) => d.at)) - t : null),
    advance(ms: number) {
      t += ms;
      for (const d of due.filter((x) => x.at <= t)) { due.splice(due.indexOf(d), 1); d.fn(); }
    },
  };
}

const G = "https://o.test/feeds/live/index.json", D = "https://o.test/feeds/dfo/index.json",
      I = "https://o.test/feeds/inseason/index.json", M = "https://o.test/release/manifest.json";
const flush = () => new Promise((r) => setTimeout(r, 0));

function feedsWith(applied: Record<string, string[]>, refuse?: (id: string, text: string) => boolean): FeedSpec[] {
  return (["gauges", "dfo", "inseason"] as const).map((id) => ({
    id, url: { gauges: G, dfo: D, inseason: I }[id],
    apply: (text: string) => {
      if (refuse?.(id, text)) throw new Error("not a feed");
      (applied[id] ??= []).push(text);
    },
  }));
}

describe("checkObject: the cheap look", () => {
  it("304 on a matching ETag; a 200 of the same bytes is unchanged too; new bytes are changed", async () => {
    const o = origin({ [G]: "a" });
    const first = await checkObject(G, null, o.fetch);
    expect(first.kind).toBe("changed");
    const seen = first.kind === "changed" ? { etag: first.etag, sha256: first.sha256 } : null;
    expect((await checkObject(G, seen, o.fetch)).kind).toBe("unchanged");
    expect(o.log.at(-1)!.status).toBe(304);
    // the origin rewrote the same bytes without a validator match (T225's 24 h rewrite): the sha says unchanged
    expect((await checkObject(G, { etag: '"other"', sha256: seen!.sha256 }, o.fetch)).kind).toBe("unchanged");
    o.files.set(G, "b");
    expect((await checkObject(G, seen, o.fetch)).kind).toBe("changed");
  });

  it("a weak ETag (the edge's gzip) is sent strong, so the worker's strong compare answers 304", async () => {
    const o = origin({ [G]: "a" });
    const etag = `"${sha256Hex(enc("a")).slice(0, 12)}"`;
    expect((await checkObject(G, { etag: `W/${etag}`, sha256: null }, o.fetch)).kind).toBe("unchanged");
    expect(o.log.at(-1)).toMatchObject({ inm: etag, status: 304 });
  });

  it("HTTP failures and a thrown fetch are answers, told apart", async () => {
    const o = origin({});
    expect(await checkObject(G, null, o.fetch)).toMatchObject({ kind: "failed", offline: false });
    o.set("offline");
    expect(await checkObject(G, null, o.fetch)).toMatchObject({ kind: "failed", offline: true });
  });
});

describe("feeds", () => {
  it("unchanged costs a 304; changed is handed whole to its loader; updatedAt moves only then", async () => {
    const o = origin({ [G]: "g1", [D]: "d1", [I]: "i1" }), c = clock(), applied: Record<string, string[]> = {};
    const s = createUpdateService({ feeds: feedsWith(applied), fetch: o.fetch, now: c.now, timers: c.timers });
    await s.check();
    expect(applied).toEqual({ gauges: ["g1"], dfo: ["d1"], inseason: ["i1"] });
    const t1 = s.snapshot().feeds.map((f) => f.updatedAt);
    c.advance(1000);
    await s.check();
    expect(o.log.slice(-3).map((l) => l.status)).toEqual([304, 304, 304]);
    expect(s.snapshot().feeds.map((f) => f.updatedAt)).toEqual(t1);
    expect(s.snapshot().feeds.every((f) => f.checkedAt === c.now())).toBe(true);
    o.files.set(D, "d2");
    await s.check();
    expect(applied.dfo).toEqual(["d1", "d2"]);
    expect(s.snapshot().feeds.find((f) => f.id === "dfo")!.updatedAt).toBe(c.now());
  });

  it("PARTIAL: one feed fails, the others land; the manual check says not ok and names the failure", async () => {
    const o = origin({ [G]: "g1", [I]: "i1" }), applied: Record<string, string[]> = {};
    const s = createUpdateService({ feeds: feedsWith(applied), fetch: o.fetch });
    await s.check({ manual: true });
    expect(applied).toEqual({ gauges: ["g1"], inseason: ["i1"] });
    const dfo = s.snapshot().feeds.find((f) => f.id === "dfo")!;
    expect(dfo.error).toContain("HTTP 404");
    expect(dfo.offline).toBe(false);
    expect(s.snapshot().manual).toMatchObject({ ok: false });
  });

  it("ATOMIC: a body its loader refuses is not kept, not 'seen', and is looked at again", async () => {
    const o = origin({ [G]: "g1", [D]: "d1", [I]: "i1" }), applied: Record<string, string[]> = {};
    const s = createUpdateService({ feeds: feedsWith(applied, (id, t) => id === "gauges" && t === "broken"),
                                    fetch: o.fetch });
    await s.check();
    o.files.set(G, "broken");
    await s.check();
    expect(applied.gauges).toEqual(["g1"]);                       // the loader still holds g1
    expect(s.snapshot().feeds[0]!.error).toContain("not a feed");
    o.files.set(G, "g2");
    await s.check();
    expect(applied.gauges).toEqual(["g1", "g2"]);
    expect(s.snapshot().feeds[0]!.error).toBeNull();
  });

  it("OFFLINE by the device's word: no request; the last good data stays; back online checks at once", async () => {
    const o = origin({ [G]: "g1", [D]: "d1", [I]: "i1" }), applied: Record<string, string[]> = {};
    let online = true;
    const subs: ((on: boolean) => void)[] = [];
    const c = clock();
    const s = createUpdateService({ feeds: feedsWith(applied), fetch: o.fetch, now: c.now, timers: c.timers,
                                    network: { online: () => online, subscribe: (on) => { subs.push(on); return () => {}; } } });
    s.start();
    await flush();
    expect(applied.gauges).toEqual(["g1"]);
    online = false;
    const n = o.log.length;
    o.files.set(G, "g2");
    await s.check({ manual: true });
    expect(o.log.length).toBe(n);                                 // nothing asked of the network
    expect(s.snapshot().feeds[0]).toMatchObject({ offline: true, error: "offline" });
    expect(s.snapshot().manual).toMatchObject({ ok: false });
    expect(applied.gauges).toEqual(["g1"]);
    online = true;
    subs.forEach((on) => on(true));
    await s.check();                                              // joins the check the event started
    expect(applied.gauges).toEqual(["g1", "g2"]);
    expect(s.snapshot().feeds[0]!.error).toBeNull();
    s.stop();
  });

  it("a thrown fetch is offline, retried with backoff; the interval otherwise", async () => {
    const o = origin({ [G]: "g1", [D]: "d1", [I]: "i1" }), c = clock();
    const s = createUpdateService({ feeds: feedsWith({}), fetch: o.fetch, now: c.now, timers: c.timers,
                                    policy: { feedEveryMs: 900_000, retryMs: 30_000, maxRetryMs: 120_000 } });
    s.start();
    await flush();
    expect(c.next()).toBe(900_000);
    o.set("offline");
    c.advance(900_000);
    await flush();
    expect(s.snapshot().feeds[0]!.offline).toBe(true);
    expect(c.next()).toBe(30_000);
    c.advance(30_000); await flush();
    expect(c.next()).toBe(60_000);
    c.advance(60_000); await flush();
    expect(c.next()).toBe(120_000);
    c.advance(120_000); await flush();
    expect(c.next()).toBe(120_000);                               // capped
    o.set(null);
    c.advance(120_000); await flush();
    expect(s.snapshot().feeds[0]!.error).toBeNull();
    expect(c.next()).toBe(900_000);
    s.stop();
  });

  it("a foreground return checks what is due, and only that", async () => {
    const o = origin({ [G]: "g1", [D]: "d1", [I]: "i1" }), c = clock();
    const fg: (() => void)[] = [];
    const s = createUpdateService({ feeds: feedsWith({}), fetch: o.fetch, now: c.now, timers: c.timers,
                                    lifecycle: { subscribe: (on) => { fg.push(on); return () => {}; } } });
    s.start();
    await flush();
    const n = o.log.length;
    c.advance(10_000);
    fg.forEach((on) => on());
    await flush();
    expect(o.log.length).toBe(n);                                 // checked 10 s ago: not due
    c.advance(120_000);
    fg.forEach((on) => on());
    await flush();
    expect(o.log.length).toBe(n + 3);
    s.stop();
  });

  it("what was seen is KEPT across launches: the next launch's first check is conditional", async () => {
    const o = origin({ [G]: "g1", [D]: "d1", [I]: "i1" }), store = memStore();
    const a = createUpdateService({ feeds: feedsWith({}), fetch: o.fetch, store });
    await a.check();
    const updated = a.snapshot().feeds.map((f) => f.updatedAt);
    const b = createUpdateService({ feeds: feedsWith({}), fetch: o.fetch, store });
    expect(b.snapshot().feeds.map((f) => f.updatedAt)).toEqual(updated);
    await b.check();
    expect(o.log.slice(-3).map((l) => [l.inm !== null, l.status])).toEqual([[true, 304], [true, 304], [true, 304]]);
  });
});

// ── the release ──────────────────────────────────────────────────────────────────────────────────────────────────────

function manifest(release: string, o: { files?: Record<string, [number, string]>; tiles?: string; tileBytes?: number;
                                        date?: string } = {}): Manifest {
  const files = Object.fromEntries(Object.entries(o.files ?? { core: [100, "c1"], paint: [10, "p1"] })
    .map(([k, [bytes, sha256]]) => [k, { url: `${release}/${k}.gz`, bytes, gz: bytes, sha256, format: "f", section_handles: "h" }]));
  return {
    format: MANIFEST_FORMAT, data_release: release, files, basemap: null,
    tiles: { compatible: ["h.*"], current: o.tiles ? { id: o.tiles, url: `tiles/${o.tiles}/a.pmtiles`, bytes: o.tileBytes ?? 1, sha256: "t" } : null,
             assets: null, mask: null },
    regulations: { valid_until: null, versions: null },
    ...(o.date ? { sources: { dfo: { date: o.date }, fwa: { date: "2026-01-01" } } } : {}),
  } as Manifest;
}
const heldOf = (m: Manifest): Held => ({ release: m.data_release, tiles: m.tiles.current?.id ?? null, basemap: null,
  assets: null, files: Object.fromEntries(Object.entries(m.files).map(([k, v]) => [k, v.sha256])) });

describe("the release", () => {
  it("moved / need: the data release or the tiles moving is new; the bytes are what changed", () => {
    const m1 = manifest("r1", { tiles: "v1" });
    expect(releaseMoved(heldOf(m1), m1)).toBe(false);
    const m2 = manifest("r2", { files: { core: [100, "c2"], paint: [10, "p1"] }, tiles: "v1" });
    expect(releaseMoved(heldOf(m1), m2)).toBe(true);
    expect(releaseNeed(m2, heldOf(m1))).toBe(100);
    const m3 = manifest("r1", { tiles: "v2", tileBytes: 5000 });
    expect(releaseMoved(heldOf(m1), m3)).toBe(true);
    expect(releaseNeed(m3, heldOf(m1))).toBe(5000);
  });

  it("a newer release is STAGED beside the set in use, then ready; applied puts it in use", async () => {
    const m1 = manifest("r1"), m2 = manifest("r2", { files: { core: [100, "c2"], paint: [10, "p1"] }, date: "2026-10-27" });
    const o = origin({ [M]: JSON.stringify(m1) });
    let held = heldOf(m1);
    const staged: string[] = [];
    const s = createUpdateService({ feeds: [], fetch: o.fetch, release: {
      manifestUrl: M, held: () => held,
      stage: async (m, on) => { on(50, 100); staged.push(m.data_release); on(100, 100); },
    } });
    await s.check();
    expect(s.snapshot().release).toMatchObject({ phase: "current", target: null, error: null });
    o.files.set(M, JSON.stringify(m2));
    await s.check();
    await flush();
    expect(staged).toEqual(["r2"]);
    expect(s.snapshot().release).toMatchObject({ phase: "ready", target: "r2", targetDate: "2026-10-27" });
    // ready: nothing more is fetched under it
    const n = o.log.length;
    await s.check();
    expect(o.log.length).toBe(n);
    held = heldOf(m2);
    s.applied("r2");
    expect(s.snapshot().release).toMatchObject({ phase: "current", target: null });
    expect(s.snapshot().release!.updatedAt).not.toBeNull();
    await s.check();
    expect(staged).toEqual(["r2"]);
  });

  it("SHA MISMATCH in the download: nothing staged, the error said, tried again at the next check", async () => {
    const m1 = manifest("r1"), m2 = manifest("r2", { files: { core: [100, "c2"], paint: [10, "p1"] } });
    const o = origin({ [M]: JSON.stringify(m2) });
    let bad = true, tries = 0;
    const s = createUpdateService({ feeds: [], fetch: o.fetch, release: {
      manifestUrl: M, held: () => heldOf(m1),
      stage: async () => { tries++; if (bad) throw new VerifyError("core: sha256 0123…, the manifest says c2…"); },
    } });
    await s.check();
    await flush();
    expect(s.snapshot().release).toMatchObject({ phase: "current", target: null });
    expect(s.snapshot().release!.error).toContain("sha256");
    bad = false;
    await s.check();
    await flush();
    expect(tries).toBe(2);
    expect(s.snapshot().release).toMatchObject({ phase: "ready", target: "r2", error: null });
  });

  it("METERED: a big download waits for Wi-Fi; a small one does not; the setting lets it through", async () => {
    const m1 = manifest("r1", { tiles: "v1" });
    const big = manifest("r1", { tiles: "v2", tileBytes: 500_000_000 });
    const o = origin({ [M]: JSON.stringify(big) });
    let metered = true;
    const subs: ((on: boolean) => void)[] = [];
    const staged: string[] = [];
    const store = memStore();
    const s = createUpdateService({ feeds: [], fetch: o.fetch, store,
      network: { online: () => true, metered: () => metered, subscribe: (on) => { subs.push(on); return () => {}; } },
      release: { manifestUrl: M, held: () => heldOf(m1), stage: async (m) => { staged.push(m.tiles.current!.id!); } } });
    s.start();
    await flush(); await flush();
    expect(s.snapshot().release).toMatchObject({ phase: "waiting-wifi", target: "r1", progress: { of: 500_000_000 } });
    expect(staged).toEqual([]);
    // the reader allows cellular: it goes
    s.allowMetered(true);
    await flush();
    expect(staged).toEqual(["v2"]);
    expect(JSON.parse(store.text!).allowMetered).toBe(true);
    s.stop();

    // and without the setting, Wi-Fi coming back lets it through
    const s2 = createUpdateService({ feeds: [], fetch: o.fetch,
      network: { online: () => true, metered: () => metered, subscribe: (on) => { subs.push(on); return () => {}; } },
      release: { manifestUrl: M, held: () => heldOf(m1), stage: async (m) => { staged.push(`s2 ${m.tiles.current!.id!}`); } } });
    s2.start();
    await flush(); await flush();
    expect(s2.snapshot().release!.phase).toBe("waiting-wifi");
    metered = false;
    subs.at(-1)!(true);
    await flush();
    expect(staged).toEqual(["v2", "s2 v2"]);
    s2.stop();
  });

  it("T129: a manifest the boot just read seeds the first check (no second request at start)", async () => {
    const m1 = manifest("r1");
    const o = origin({ [M]: JSON.stringify(m1) });
    const c = clock();
    const s = createUpdateService({ feeds: [], fetch: o.fetch, now: c.now, timers: c.timers,
                                    release: { manifestUrl: M, held: () => heldOf(m1), stage: async () => {} } });
    s.seedRelease(m1);
    s.start();
    await flush();
    // the seed says the release was checked an instant ago: start does not ask; the interval does
    expect(o.log.filter((l) => l.url === M).length).toBe(0);
    c.advance(c.next()!);
    await s.check({ feeds: false });
    expect(o.log.filter((l) => l.url === M).length).toBe(1);
    s.stop();
  });

  it("an unreadable manifest is an error, not an update", async () => {
    const o = origin({ [M]: "{\"format\":\"nope\"}" });
    const s = createUpdateService({ feeds: [], fetch: o.fetch,
                                    release: { manifestUrl: M, held: () => heldOf(manifest("r1")), stage: async () => {} } });
    await s.check({ manual: true });
    expect(s.snapshot().release!.error).toContain("format");
    expect(s.snapshot().manual).toMatchObject({ ok: false });
  });
});
