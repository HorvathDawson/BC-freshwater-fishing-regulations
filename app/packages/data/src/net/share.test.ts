// (T265) one network request per URL in the air: the update service's check and the feed loader's read of the same index.
import { describe, expect, it, vi } from "vitest";
import { shareFetch } from "./share";
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
