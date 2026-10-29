/**
 * (T231) THE UPDATE SERVICE over a mocked origin: unchanged (304, and a 200 of the same bytes), changed (handed whole to
 * its loader), failed (HTTP), offline (no request when the device says so; a thrown fetch), partial (one feed fails, the
 * rest land), a refused feed (the loader keeps what it had — the atomic swap — and the next check looks again), a sha
 * mismatch in a release download (nothing staged, retried), metered (a big release waits for Wi-Fi), apply, the
 * schedule (interval, backoff, foreground, back online) and what is kept across launches.
 */
import { describe, expect, it } from "vitest";
import { resolveAppVersion, type UpdateEvent } from "@app/core";
import { createUpdateLog, versionedFetch } from "./log";
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

// ── (T251) the Updates log's events, the release gate, forget ─────────────────────────────────────────────────────────

describe("(T251) every check is an event", () => {
  it("feeds: changed (sha before → after, bytes), then unchanged 304 (0 B), then a 404, then offline", async () => {
    const o = origin({ [G]: "a" });
    const ev: UpdateEvent[] = [];
    let online = true;
    const s = createUpdateService({ feeds: feedsWith({}).slice(0, 1), fetch: o.fetch, onEvent: (e) => ev.push(e),
                                    network: { online: () => online } });
    await s.check({ manual: true });
    await s.check();
    o.files.delete(G);
    await s.check();
    online = false;
    await s.check();
    const sha = sha256Hex(enc("a")).slice(0, 8);
    expect(ev.map((e) => [e.source, e.kind, e.status ?? null, e.bytes ?? null, e.before ?? null, e.after ?? null,
                          e.errorClass ?? null, !!e.manual]))
      .toEqual([
        ["gauges", "changed", 200, 1, null, sha, null, true],
        ["gauges", "unchanged", 304, 0, sha, sha, null, false],
        ["gauges", "failed", 404, 0, sha, null, "http-404", false],
        ["gauges", "offline", null, null, null, null, "offline", false],
      ]);
  });

  it("release: changed then staged (data release before → after), applied; a sha mismatch is classed", async () => {
    const m1 = manifest("r1"), m2 = manifest("r2", { files: { core: [100, "c2"], paint: [10, "p1"] } });
    const o = origin({ [M]: JSON.stringify(m2) });
    const ev: UpdateEvent[] = [];
    let bad = true;
    const s = createUpdateService({ feeds: [], fetch: o.fetch, onEvent: (e) => ev.push(e), release: {
      manifestUrl: M, held: () => heldOf(m1),
      stage: async (_m, on) => { on(100, 100); if (bad) throw new VerifyError("core: sha256 0123…, the manifest says c2…"); },
    } });
    await s.check(); await flush();
    bad = false;
    await s.check(); await flush();
    s.applied("r2", { from: "r1", ms: 42 });
    expect(ev.map((e) => [e.kind, e.before, e.after, e.errorClass ?? null])).toEqual([
      ["changed", "r1", "r2", null], ["failed", "r1", "r2", "sha-mismatch"],
      ["changed", "r1", "r2", null], ["staged", "r1", "r2", null], ["applied", "r1", "r2", null],
    ]);
    expect(ev.at(-1)!.ms).toBe(42);
  });

  it("forget(): the next check downloads whole (no If-None-Match) and logs the action", async () => {
    const o = origin({ [G]: "a" });
    const ev: UpdateEvent[] = [];
    const s = createUpdateService({ feeds: feedsWith({}).slice(0, 1), fetch: o.fetch, onEvent: (e) => ev.push(e) });
    await s.check();
    s.forget();
    await s.check();
    expect(o.log.map((l) => l.inm !== null)).toEqual([false, false]);
    expect(ev.map((e) => e.kind)).toEqual(["changed", "action", "changed"]);
  });
});

describe("(T251) the release gate: minAppVersion", () => {
  const v = (s: string) => resolveAppVersion({ version: s });
  const gated = (min: string | undefined) => ({ ...manifest("r2", { files: { core: [100, "c2"], paint: [10, "p1"] } }),
                                                ...(min ? { minAppVersion: min } : {}) });

  it("an older app keeps its data: nothing staged, phase app-too-old naming the version asked for", async () => {
    const m1 = manifest("r1");
    const o = origin({ [M]: JSON.stringify(gated("0.3.0")) });
    const staged: string[] = [], ev: UpdateEvent[] = [];
    const s = createUpdateService({ feeds: [], fetch: o.fetch, appVersion: v("0.2.9"), onEvent: (e) => ev.push(e),
      release: { manifestUrl: M, held: () => heldOf(m1), stage: async (m) => { staged.push(m.data_release); } } });
    await s.check(); await flush();
    expect(staged).toEqual([]);
    expect(s.snapshot().release).toMatchObject({ phase: "app-too-old", target: "r2", needsApp: "0.3.0" });
    expect(ev.at(-1)).toMatchObject({ kind: "app-too-old", before: "r1", after: "r2", note: "needs app ≥ 0.3.0" });
    // a later manifest the app may apply clears it and stages
    o.files.set(M, JSON.stringify(gated("0.2.0")));
    await s.check(); await flush();
    expect(staged).toEqual(["r2"]);
    expect(s.snapshot().release).toMatchObject({ phase: "ready", needsApp: null });
  });

  it("a new enough app applies it; no appVersion (the web: deployed with its data) is never gated", async () => {
    for (const appVersion of [v("0.3.0"), v("1.0.0"), null]) {
      const o = origin({ [M]: JSON.stringify(gated("0.3.0")) });
      const staged: string[] = [];
      const s = createUpdateService({ feeds: [], fetch: o.fetch, appVersion,
        release: { manifestUrl: M, held: () => heldOf(manifest("r1")), stage: async (m) => { staged.push(m.data_release); } } });
      await s.check(); await flush();
      expect(staged).toEqual(["r2"]);
    }
  });

  it("the boot's own manifest (seedRelease) is gated too", () => {
    const s = createUpdateService({ feeds: [], appVersion: v("0.1.0"),
      release: { manifestUrl: M, held: () => heldOf(manifest("r1")), stage: async () => {} } });
    s.seedRelease(gated("0.2.0"));
    expect(s.snapshot().release).toMatchObject({ phase: "app-too-old", target: "r2" });
  });
});

describe("(T251) the log's ring buffer and the versioned fetch", () => {
  it("keeps the last `cap` events across a reopen; clear empties it", () => {
    const store = memStore();
    const a = createUpdateLog({ store, cap: 3 });
    for (let i = 0; i < 5; i++) a.append({ at: i, source: "gauges", kind: "unchanged" });
    const b = createUpdateLog({ store, cap: 3 });
    expect(b.events().map((e) => e.at)).toEqual([2, 3, 4]);
    b.clear();
    expect(createUpdateLog({ store, cap: 3 }).events()).toEqual([]);
  });

  it("adds the version headers to every request, the request's own headers kept", async () => {
    const seen: Headers[] = [];
    const f = versionedFetch((async (_u: RequestInfo | URL, i?: RequestInit) => { seen.push(new Headers(i?.headers));
                                                                              return new Response("x"); }) as typeof fetch,
                             { "X-App-Version": "0.1.0+3.abcdef1" });
    await checkObject(G, { etag: '"e"', sha256: null }, f);
    expect([seen[0]!.get("X-App-Version"), seen[0]!.get("If-None-Match")]).toEqual(["0.1.0+3.abcdef1", '"e"']);
  });
});
