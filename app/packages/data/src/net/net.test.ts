// (T254) THE BAD-CONNECTION POLICY: mode selection, the deferral order, timeouts per size, retries with backoff and
// resume, the meter, and a release file through all of it.
import { describe, expect, it, vi } from "vitest";
import { gzipSync } from "fflate";
import { sha256Hex } from "../sha256";
import {
  backoffMs, createFetchGate, createNet, createNetMeter, DEFERRAL_ORDER, etaSeconds, fetchResilient, fetchVerifiedVia,
  HIGH_BPS, isAnswerFile, LOW_BPS, NetError, netMode, NO_HINTS, PRIORITY, priorityOf, timeoutsFor, type NetHints,
} from "./index";

const hints = (h: Partial<NetHints>): NetHints => ({ ...NO_HINTS, ...h });

describe("mode selection (netMode)", () => {
  it("the reader's setting decides first", () => {
    expect(netMode("on", hints({ measuredBps: 10_000_000 }))).toEqual({ low: true, why: "setting", offline: false });
    expect(netMode("off", hints({ measuredBps: 1000, saveData: true }))).toEqual({ low: false, why: null, offline: false });
  });
  it("automatic: Save-Data is low", () => {
    expect(netMode("auto", hints({ saveData: true })).why).toBe("save-data");
  });
  it("automatic: a measured rate beats what the platform says", () => {
    // Chrome says 4g on a throttled line; the meter says 50 kB/s
    expect(netMode("auto", hints({ effectiveType: "4g", measuredBps: 50_000 })).why).toBe("measured");
    // the browser says 3g, the meter measured a fast line
    expect(netMode("auto", hints({ effectiveType: "3g", measuredBps: 2_000_000 })).low).toBe(false);
  });
  it("automatic: hysteresis between LOW_BPS and HIGH_BPS", () => {
    const mid = (LOW_BPS + HIGH_BPS) / 2;
    expect(netMode("auto", hints({ measuredBps: mid }), false).low).toBe(false);
    expect(netMode("auto", hints({ measuredBps: mid }), true).low).toBe(true);
    expect(netMode("auto", hints({ measuredBps: HIGH_BPS + 1 }), true).low).toBe(false);
  });
  it("automatic, nothing measured: the platform's word", () => {
    expect(netMode("auto", hints({ effectiveType: "3g" })).why).toBe("browser");
    expect(netMode("auto", hints({ effectiveType: "slow-2g" })).why).toBe("browser");
    expect(netMode("auto", hints({ effectiveType: "4g" })).low).toBe(false);
    expect(netMode("auto", hints({ downlinkMbps: 0.4 })).why).toBe("browser");
    expect(netMode("auto", hints({ rttMs: 1200 })).why).toBe("browser");
    expect(netMode("auto", hints({ cellularGeneration: "3g" })).why).toBe("cellular");
    expect(netMode("auto", hints({ cellularGeneration: "4g" })).low).toBe(false);
    expect(netMode("auto", NO_HINTS)).toEqual({ low: false, why: null, offline: false });
  });
  it("offline is said beside the mode", () => {
    expect(netMode("auto", hints({ online: false }))).toEqual({ low: false, why: null, offline: true });
    expect(netMode("on", hints({ online: false })).offline).toBe(true);
  });
});

describe("the deferral order", () => {
  it("the answer files, then the extras, then ahead", () => {
    for (const f of ["paint", "core", "water", "regs-2", "regs-7a", "regs-sid", "down"] as const) expect(priorityOf(f)).toBe(PRIORITY.answer);
    for (const f of ["salmon", "salmon-2", "access", "method", "lakes", "flow-area", "geo", "near", "climate", "conditions"] as const)
      expect(priorityOf(f)).toBe(PRIORITY.extra);
    expect(isAnswerFile("regs-x" as never)).toBe(true);
    expect(DEFERRAL_ORDER.map((d) => d.priority)).toEqual([0, 1, 2]);
  });
});

describe("timeouts per size, backoff", () => {
  it("a bigger file gets longer, a stall never more", () => {
    const small = timeoutsFor(10_000), big = timeoutsFor(25_000_000);
    expect(small.firstByteMs).toBe(big.firstByteMs);
    expect(small.stallMs).toBe(big.stallMs);
    expect(big.totalMs).toBeGreaterThan(small.totalMs);
    // 25 MB at the 32 kbit/s floor: well over an hour; a 64 kB unknown under a minute
    expect(big.totalMs).toBeGreaterThan(3600_000);
    expect(timeoutsFor(null).totalMs).toBeLessThan(60_000);
  });
  it("1 s, 2 s, 4 s … capped at 15 s, with jitter", () => {
    expect([2, 3, 4, 5, 6, 9].map((a) => backoffMs(a, () => 0.5))).toEqual([1000, 2000, 4000, 8000, 15000, 15000]);
    expect(backoffMs(2, () => 0)).toBe(800);
    expect(backoffMs(2, () => 0.999)).toBeLessThanOrEqual(1200);
  });
  it("an estimate in seconds", () => {
    expect(etaSeconds(25_000_000, 50_000)).toBe(500);
    expect(etaSeconds(1, null)).toBeNull();
  });
});

describe("the gate", () => {
  const deferred = () => { let ok!: () => void, fail!: (e: unknown) => void; const p = new Promise<void>((a, b) => { ok = a; fail = b; }); return { p, ok, fail }; };
  it("off low mode: everything starts at once", () => {
    const g = createFetchGate({ low: () => false });
    const started: string[] = [];
    for (const [k, pr] of [["salmon", 1], ["water", 0], ["regs-3", 2]] as const) void g.run(k, pr, () => { started.push(k); return new Promise(() => {}); });
    expect(started).toEqual(["salmon", "water", "regs-3"]);
  });
  it("low: the answer first, then the extras, then one ahead at a time", async () => {
    const g = createFetchGate({ low: () => true });
    const started: string[] = [];
    const d: Record<string, ReturnType<typeof deferred>> = {};
    const go = (k: string, pr: 0 | 1 | 2) => { d[k] = deferred(); void g.run(k, pr, () => { started.push(k); return d[k]!.p; }); };
    go("regs-5", 2); go("regs-6", 2);           // the prefetch was going…
    expect(started).toEqual(["regs-5"]);         // one ahead at a time
    go("water", 0); go("salmon-2", 1); go("core", 0); go("access", 1);
    expect(started).toEqual(["regs-5", "water", "core"]);   // the tap's answer does not wait for the prefetch in flight
    d.water!.ok(); await Promise.resolve(); await Promise.resolve();
    expect(started).toEqual(["regs-5", "water", "core"]);   // core still coming: the extras wait
    d.core!.ok(); await Promise.resolve(); await Promise.resolve();
    expect(started).toEqual(["regs-5", "water", "core", "salmon-2", "access"]);
    d["regs-5"]!.ok(); d["salmon-2"]!.ok(); d.access!.ok(); await new Promise((r) => setTimeout(r, 0));
    expect(started).toEqual(["regs-5", "water", "core", "salmon-2", "access", "regs-6"]);
  });
  it("asking again joins the download and raises its turn", async () => {
    const g = createFetchGate({ low: () => true });
    const started: string[] = [];
    const a = deferred();
    void g.run("core", 0, () => { started.push("core"); return a.p; });
    const work = vi.fn(() => { started.push("regs-2"); return Promise.resolve(7); });
    const queued = g.run("regs-2", 2, work);
    expect(g.state().waiting).toEqual([{ key: "regs-2", priority: 2 }]);
    const again = g.run("regs-2", 0, () => Promise.resolve(99));
    expect(started).toEqual(["core", "regs-2"]);             // raised to the answer's turn: it goes beside core
    expect(await again).toBe(7);
    expect(await queued).toBe(7);
    expect(work).toHaveBeenCalledTimes(1);
  });
  it("a failure lets the next through; a mode change releases what waits", async () => {
    let low = true;
    const g = createFetchGate({ low: () => low });
    const started: string[] = [];
    const a = deferred();
    const answer = g.run("water", 0, () => { started.push("water"); return a.p; });
    void g.run("salmon", 1, () => { started.push("salmon"); return new Promise(() => {}); });
    void g.run("near", 1, () => { started.push("near"); return new Promise(() => {}); });
    expect(started).toEqual(["water"]);
    low = false; g.changed();
    expect(started).toEqual(["water", "salmon", "near"]);
    a.fail(new Error("cut")); await expect(answer).rejects.toThrow("cut");
  });
});

describe("the meter", () => {
  it("bytes over the time something was downloading; idle time is not slowness", () => {
    let t = 0;
    const m = createNetMeter({ now: () => t, notifyEveryMs: 0 });
    expect(m.bps()).toBeNull();
    const h = m.start();
    for (let i = 0; i < 4; i++) { t += 1000; m.chunk(h, 50_000); }
    m.end(h);
    expect(m.bps()).toBe(50_000);
    t += 3000;                                  // nothing downloading: the rate stands
    expect(m.bps()).toBe(50_000);
  });
  it("two downloads at once are one link", () => {
    let t = 0;
    const m = createNetMeter({ now: () => t, notifyEveryMs: 0 });
    const a = m.start(), b = m.start();
    for (let i = 0; i < 4; i++) { t += 1000; m.chunk(a, 25_000); m.chunk(b, 25_000); }
    expect(m.bps()).toBe(50_000);
  });
  it("a stall reads as next to nothing", () => {
    let t = 0;
    const m = createNetMeter({ now: () => t, notifyEveryMs: 0 });
    const h = m.start();
    t += 1000; m.chunk(h, 100_000);
    t += 7500;                                  // in flight, nothing arriving
    expect(m.bps()!).toBeLessThan(LOW_BPS);
  });
});

// ── the resilient download ────────────────────────────────────────────────────────────────────────────────────────────

/** A fake server: each call takes the next script entry; a body is streamed in chunks; `cut` stops it mid-body. */
type Step = { status?: number; body?: Uint8Array; cutAfter?: number; hang?: boolean; headers?: Record<string, string> };
function server(steps: Step[]) {
  const calls: { url: string; range: string | null }[] = [];
  const impl = (async (url: string, init?: RequestInit) => {
    const st = steps[Math.min(calls.length, steps.length - 1)]!;
    const h = (init?.headers ?? {}) as Record<string, string>;
    calls.push({ url, range: h.Range ?? null });
    const signal = init?.signal;
    if (st.hang) return new Promise((_, fail) => signal?.addEventListener("abort", () => fail(new Error("aborted"))));
    const body = st.body ?? new Uint8Array();
    let sent = 0;
    const stream = new ReadableStream<Uint8Array>({
      pull(c) {
        if (st.cutAfter !== undefined && sent >= st.cutAfter) {
          // the line dies: nothing more arrives until the reader gives up
          return new Promise<void>((_, fail) => signal?.addEventListener("abort", () => { fail(new Error("aborted")); }));
        }
        if (sent >= body.length) { c.close(); return; }
        const n = Math.min(4, body.length - sent, st.cutAfter !== undefined ? st.cutAfter - sent : Infinity);
        c.enqueue(body.subarray(sent, sent + n)); sent += n;
      },
    });
    return new Response(stream, { status: st.status ?? 200, headers: st.headers ?? {} });
  }) as typeof fetch;
  return { impl, calls };
}
const FAST = { firstByteMs: 200, stallMs: 100, totalMs: 5000 };
const noSleep = () => Promise.resolve();
const bytes = (s: string) => new TextEncoder().encode(s);

describe("fetchResilient", () => {
  it("a whole body on the first try", async () => {
    const s = server([{ body: bytes("%PDF-hello") }]);
    expect(new TextDecoder().decode(await fetchResilient("u", { fetchImpl: s.impl, limits: FAST, sleep: noSleep }))).toBe("%PDF-hello");
    expect(s.calls).toHaveLength(1);
  });
  it("a stalled body is cut and RESUMED from where it stopped (Range), when the bytes are the file's own", async () => {
    const all = bytes("%PDF-0123456789abcdef");
    const s = server([{ body: all, cutAfter: 8 }, { status: 206, body: all.subarray(8) }]);
    const retries: number[] = [];
    const out = await fetchResilient("u", { fetchImpl: s.impl, limits: FAST, sleep: noSleep,
      resumable: (h) => new TextDecoder().decode(h.subarray(0, 4)) === "%PDF", onRetry: (e) => retries.push(e.resumeAt) });
    expect(new TextDecoder().decode(out)).toBe("%PDF-0123456789abcdef");
    expect(s.calls.map((c) => c.range)).toEqual([null, "bytes=8-"]);
    expect(retries).toEqual([8]);
  });
  it("a body that is not resumable starts again from 0", async () => {
    const all = bytes("decoded-body-123456");
    const s = server([{ body: all, cutAfter: 8 }, { body: all }]);
    const out = await fetchResilient("u", { fetchImpl: s.impl, limits: FAST, sleep: noSleep, resumable: () => false });
    expect(new TextDecoder().decode(out)).toBe("decoded-body-123456");
    expect(s.calls.map((c) => c.range)).toEqual([null, null]);
  });
  it("a server that answers a Range with 200 sends it whole: the kept bytes are dropped", async () => {
    const all = bytes("%PDF-0123456789");
    const s = server([{ body: all, cutAfter: 8 }, { status: 200, body: all }]);
    const out = await fetchResilient("u", { fetchImpl: s.impl, limits: FAST, sleep: noSleep, resumable: () => true });
    expect(new TextDecoder().decode(out)).toBe("%PDF-0123456789");
  });
  it("no first byte: cut and tried again; a 5xx is tried again; a 404 is an answer", async () => {
    const s = server([{ hang: true }, { status: 503 }, { body: bytes("ok") }]);
    expect(new TextDecoder().decode(await fetchResilient("u", { fetchImpl: s.impl, limits: FAST, sleep: noSleep }))).toBe("ok");
    expect(s.calls).toHaveLength(3);
    const g = server([{ status: 404 }]);
    await expect(fetchResilient("u", { fetchImpl: g.impl, limits: FAST, sleep: noSleep })).rejects.toMatchObject({ kind: "http", status: 404 });
    expect(g.calls).toHaveLength(1);
  });
  it("gives up after its tries, saying why", async () => {
    const s = server([{ hang: true }]);
    const e = await fetchResilient("u", { fetchImpl: s.impl, limits: FAST, sleep: noSleep, attempts: 3 }).catch((x: unknown) => x);
    expect(e).toBeInstanceOf(NetError);
    expect(e).toMatchObject({ kind: "timeout", attempts: 3 });
    expect(s.calls).toHaveLength(3);
  });
  it("offline: the next try waits for the line to come back, and spends no try on it", async () => {
    let up = false;
    const s = server([{ hang: true }, { body: bytes("back") }]);
    const net = createNet({ online: { online: () => up, subscribe: (on) => { listeners.push(on); return () => {}; } } });
    const listeners: ((o: boolean) => void)[] = [];
    const p = fetchResilient("u", { fetchImpl: s.impl, limits: FAST, sleep: noSleep, attempts: 2,
                                    isOnline: net.online, waitOnline: net.waitOnline });
    await new Promise((r) => setTimeout(r, 400));
    expect(s.calls).toHaveLength(1);                         // waiting, not retrying
    up = true; listeners.forEach((f) => f(true));
    expect(new TextDecoder().decode(await p)).toBe("back");
  });
  it("an abort stops it at once", async () => {
    const s = server([{ hang: true }]);
    const c = new AbortController();
    const p = fetchResilient("u", { fetchImpl: s.impl, limits: { ...FAST, firstByteMs: 10_000 }, signal: c.signal });
    c.abort();
    await expect(p).rejects.toMatchObject({ name: "AbortError" });
  });
  it("tells the meter the WIRE bytes", async () => {
    let wire = 0, opened = 0, ended = 0;
    const m = { start: () => ++opened, chunk: (_h: number, n: number) => { wire += n; }, end: () => { ended++; },
                bps: () => null, subscribe: () => () => {} };
    const s = server([{ body: new Uint8Array(4000).fill(1) }]);
    await fetchResilient("u", { fetchImpl: s.impl, limits: FAST, sleep: noSleep, meter: m, wireScale: () => 0.25 });
    // 4000 decoded bytes, a quarter of them on the wire
    expect([wire, opened, ended]).toEqual([1000, 1, 1]);
  });
});

describe("a release file through the gate, the download and the check (fetchVerifiedVia)", () => {
  const raw = new Uint8Array(3000).map((_, i) => i % 251);
  const spec = { bytes: raw.length, sha256: sha256Hex(raw) };
  const gz = gzipSync(raw);
  it("a gzip body cut mid-way resumes and verifies; the activity says how far, then clears", async () => {
    const s = server([{ body: gz, cutAfter: 12 }, { status: 206, body: gz.subarray(12) }]);
    const net = createNet();
    const seen: number[] = [];
    net.activity.subscribe(() => { const d = net.activity.snapshot().downloads[0]; if (d) seen.push(d.loaded); });
    const out = await fetchVerifiedVia("u", spec, { net, key: "water", priority: 0, fetchImpl: s.impl });
    expect(out).toEqual(raw);
    expect(s.calls.map((c) => c.range)).toEqual([null, "bytes=12-"]);
    expect(Math.max(...seen)).toBeGreaterThan(0);
    expect(net.activity.snapshot().downloads).toEqual([]);
  }, 30_000);
  it("bytes that do not verify are fetched once more WHOLE, then refused (and the failure is said)", async () => {
    const bad = gzipSync(new Uint8Array(3000));
    const s = server([{ body: bad }, { body: bad }]);
    const net = createNet();
    await expect(fetchVerifiedVia("u", spec, { net, key: "core", priority: 0, fetchImpl: s.impl })).rejects.toThrow(/sha256|bytes/);
    expect(s.calls).toHaveLength(2);
    expect(Object.keys(net.activity.snapshot().failed)).toEqual(["core"]);
  });
  it("in low mode an extra waits behind the answer", async () => {
    const net = createNet();
    net.setMode({ low: true, why: "setting", offline: false });
    const order: string[] = [];
    const slow = server([{ body: gz }]);
    const impl = (async (u: string, i?: RequestInit) => { order.push(u); return slow.impl(u, i); }) as typeof fetch;
    await Promise.all([
      fetchVerifiedVia("water", spec, { net, key: "water", priority: 0, fetchImpl: impl }),
      fetchVerifiedVia("salmon", spec, { net, key: "salmon", priority: 1, fetchImpl: impl }),
    ]);
    expect(order).toEqual(["water", "salmon"]);
  });
});

describe("the web source through the net (T254)", () => {
  it("a file prefetched ahead that the reader then asks for is the reader's: raised in the gate and in the activity", async () => {
    const { releaseSource } = await import("../release/drivers/source.web");
    const { parseManifest } = await import("../manifest");
    const raw = new TextEncoder().encode("core-bytes");
    const m = parseManifest({ format: "app-manifest/2", data_release: "r-1", published: "2026-10-29",
      regulations: { valid_until: null, versions: {} }, tiles: { compatible: [], current: null, assets: null, mask: null },
      files: { core: { url: "r-1/core-x.sqlite.gz", bytes: raw.length, sha256: sha256Hex(raw), format: "core/1", section_handles: "h" } } });
    const net = createNet();
    net.setMode({ low: true, why: "setting", offline: false });
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const src = releaseSource(m, { base: "https://r2.test/release/", held: null, net,
      cache: { kind: "memory", get: async () => null, put: async () => {} },
      fetch: (async () => { await gate; return new Response(gzipSync(raw) as BodyInit); }) as typeof fetch });
    const ahead = src.bytes("core", { ahead: true });
    await new Promise((r) => setTimeout(r, 0));
    expect(net.activity.snapshot().downloads.map((d) => [d.key, d.priority])).toEqual([["core", 2]]);
    const now = src.bytes("core");
    expect(net.activity.snapshot().downloads.map((d) => [d.key, d.priority])).toEqual([["core", 0]]);
    release();
    expect(await now).toEqual(raw);
    expect(await ahead).toEqual(raw);
  });
});
