// (T265) one network request per URL in the air: the update service's check and the feed loader's read of the same index.
import { describe, expect, it, vi } from "vitest";
import { shareFetch } from "./share";
import { fetchResilient } from "./fetch";
import { httpFeed } from "../feed/http";
import type { StationId } from "../index";

const INDEX = { format: "gauge-feed/1", fetchedAt: "2026-11-10T12:00:00Z", stations: { "08MF005": {} } };   // (T557) an index names its format and carries its time
/** a fetch whose answers wait for `go()`, counting what reached the "network" */
function gated() {
  let go!: () => void;
  const gate = new Promise<void>((r) => { go = r; });
  const calls: { url: string; init?: RequestInit }[] = [];
  const fn = vi.fn(async (u: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ url: String(u), init });
    await new Promise<void>((ok, fail) => {
      gate.then(ok);
      init?.signal?.addEventListener("abort", () => fail(Object.assign(new Error("aborted"), { name: "AbortError" })));
    });
    return new Response(JSON.stringify(INDEX), { status: 200 });
  });
  return { fn: fn as unknown as typeof fetch, calls, go };
}
const U = "https://x.test/feeds/live/index.json";

describe("shareFetch", () => {
  it("two concurrent GETs of one URL are one request, each reads its own body", async () => {
    const g = gated(), f = shareFetch(g.fn);
    const a = f(U, { cache: "no-store", headers: { "X-App-Version": "1" } }), b = f(U);
    g.go();
    const [ra, rb] = await Promise.all([a, b]);
    expect(g.calls).toHaveLength(1);
    expect(await ra.json()).toEqual(INDEX);
    expect(await rb.json()).toEqual(INDEX);
  });
  it("a later ask, after the answer, is a fresh request (nothing is cached)", async () => {
    const g = gated(), f = shareFetch(g.fn);
    g.go();
    await f(U); await f(U);
    expect(g.calls).toHaveLength(2);
  });
  it("a conditional GET, a Range and a different URL are never joined", async () => {
    const g = gated(), f = shareFetch(g.fn);
    const all = [f(U), f(U, { headers: { "If-None-Match": '"a"' } }), f(U, { headers: { Range: "bytes=1-" } }), f(`${U}?x=1`)];
    g.go(); await Promise.all(all);
    expect(g.calls).toHaveLength(4);
  });
  it("a no-store asker does not join a default-cache request; a default asker joins a no-store one", async () => {
    const g = gated(), f = shareFetch(g.fn);
    const a = f(U), b = f(U, { cache: "no-store" }), c = f(U);
    g.go(); await Promise.all([a, b, c]);
    expect(g.calls).toHaveLength(2);
  });
  it("one asker's abort cuts only that asker; the request is cut when all have gone", async () => {
    const g = gated(), f = shareFetch(g.fn);
    const c1 = new AbortController(), c2 = new AbortController();
    const a = f(U, { signal: c1.signal }), b = f(U, { signal: c2.signal });
    c1.abort();
    await expect(a).rejects.toMatchObject({ name: "AbortError" });
    expect(g.calls[0]!.init!.signal!.aborted).toBe(false);
    c2.abort();
    await expect(b).rejects.toMatchObject({ name: "AbortError" });
    expect(g.calls[0]!.init!.signal!.aborted).toBe(true);
    // and a fresh ask after that is a new request
    g.go(); await f(U);
    expect(g.calls).toHaveLength(2);
  });
  /** a request whose body sends 8 bytes and then stalls for ever; `cancelled` counts the cancels that reached it */
  function stalling() {
    const at = { cancelled: 0, calls: [] as (string | null)[] };
    const all = new TextEncoder().encode("%PDF-0123456789abcdef");
    const fn = (async (_u: RequestInfo | URL, init?: RequestInit) => {
      const range = new Headers(init?.headers).get("range");
      at.calls.push(range);
      if (range) return new Response(all.subarray(8), { status: 206 });
      let sent = false;
      return new Response(new ReadableStream<Uint8Array>({
        pull(c) { if (!sent) { sent = true; c.enqueue(all.subarray(0, 8)); return; } return new Promise<void>(() => {}); },
        cancel() { at.cancelled++; },
      }));
    }) as unknown as typeof fetch;
    return { fn, at };
  }
  it("(T760) a lone asker is handed the response itself: cancelling its body cancels the request's", async () => {
    const s = stalling(), f = shareFetch(s.fn);
    const r = (await f(U)).body!.getReader();
    expect((await r.read()).value).toHaveLength(8);
    await r.cancel();
    expect(s.at.cancelled).toBe(1);
  });
  it("(T760) several askers: the request's body is let go when every one has cancelled its copy, not before", async () => {
    const s = stalling(), f = shareFetch(s.fn);
    const [a, b] = await Promise.all([f(U), f(U)]);
    expect(s.at.calls).toHaveLength(1);
    const ra = a.body!.getReader(), rb = b.body!.getReader();
    expect((await ra.read()).value).toHaveLength(8);
    expect((await rb.read()).value).toHaveLength(8);
    const gone = ra.cancel();                                 // a tee'd copy's cancel settles when both have gone
    await new Promise((r) => setTimeout(r, 0));
    expect(s.at.cancelled).toBe(0);                           // the other asker is still reading
    await rb.cancel(); await gone;
    expect(s.at.cancelled).toBe(1);
  });
  it("(T760) a release file whose body stalls after the headers is cut and resumed THROUGH the shared fetch", async () => {
    // the first-install hang: the stall timer aborted the asker's signal, which the shared request no longer hears
    const s = stalling(), f = shareFetch(s.fn);
    const out = await fetchResilient(U, { fetchImpl: f, limits: { firstByteMs: 200, stallMs: 100, totalMs: 5000 },
                                          sleep: () => Promise.resolve(), resumable: () => true });
    expect(new TextDecoder().decode(out)).toBe("%PDF-0123456789abcdef");
    expect(s.at.calls).toEqual([null, "bytes=8-"]);
    expect(s.at.cancelled).toBe(1);                           // the stalled connection was closed
  });
  it("an error reaches every asker", async () => {
    const f = shareFetch((async () => { throw new TypeError("offline"); }) as unknown as typeof fetch);
    const r = await Promise.allSettled([f(U), f(U)]);
    expect(r.map((x) => x.status)).toEqual(["rejected", "rejected"]);
  });
});

describe("the gauge index at first paint (update service + feed loader)", () => {
  it("is fetched once when the service's check and the feed's read start together", async () => {
    const g = gated(), shared = shareFetch(g.fn);
    const feed = httpFeed("https://x.test/feeds/live", shared, { indexHeld: true });
    const service = shared(U, { cache: "no-store", headers: { "X-App-Version": "1" } }).then((r) => r.text());
    const read = feed.index();
    g.go();
    const [text, idx] = await Promise.all([service, read]);
    expect(g.calls).toHaveLength(1);
    expect(JSON.parse(text)).toEqual(INDEX);
    expect(idx).toEqual(INDEX);
  });
});

describe("the held index is read no-store (so it joins the service's check whichever asks first)", () => {
  it("feed first, service second: one request", async () => {
    const g = gated(), shared = shareFetch(g.fn);
    const feed = httpFeed("https://x.test/feeds/live", shared, { indexHeld: true });
    const read = feed.index();
    const service = shared(U, { cache: "no-store" }).then((r) => r.text());
    g.go();
    await Promise.all([read, service]);
    expect(g.calls).toHaveLength(1);
    expect(g.calls[0]!.init!.cache).toBe("no-store");
  });
  it("a station file is still an ordinary cached GET", async () => {
    const g = gated(), shared = shareFetch(g.fn);
    const feed = httpFeed("https://x.test/feeds/live", shared, { indexHeld: true });
    const p = feed.lastReport("08MF005" as StationId); g.go(); await p;
    expect(g.calls.find((c) => c.url.endsWith("08MF005.json"))!.init!.cache).toBeUndefined();
  });
});
