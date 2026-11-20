// (T739 leftovers L2 + L3, 2026-11-20) THE WAKE AS THE PHONES GROW.
//   L2 every registered token is listed, page by page: the old `listKeys(…, 100_000)` stopped there and said nothing.
//   L3 one run tells at most PUSH_SENDS_PER_RUN phones and keeps its place (PUSH_CARRY_KEY); the next feed run goes on,
//      reason or not, so a wake never meets the Workers subrequest limit (10,000 a run, R2 calls included) beside the
//      gauge run's own worst case. Measured here against `pushSubrequests`, and said on /status.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import worker from "../src/index";
import type { R2BucketLike } from "../src/io";
import {
  GAUGE_RUN_SUBREQUESTS_WORST, PUSH_CARRY_KEY, PUSH_SENDS_PER_RUN, PUSH_STATUS_KEY, pushSubrequests, sendWake, TOKENS_PREFIX,
  WORKER_SUBREQUESTS,
} from "../src/push";
import { FEED_FORMAT } from "../src/inseason";
import { INSEASON_LINKS_FORMAT } from "../../../packages/core/src/inseason";

/** A bucket that pages its listings (1,000 a call, as R2 does) and counts every call (each is a subrequest). */
class CountingBucket implements R2BucketLike {
  m = new Map<string, string>();
  calls = 0;
  failPut = new Set<string>();
  /** every get / head, as "get <key>" / "head <key>" */
  asked: string[] = [];
  async get(k: string) { this.calls++; this.asked.push(`get ${k}`); const v = this.m.get(k); return v === undefined ? null : { text: async () => v, etag: `"${v.length}"` }; }
  async head(k: string) { this.calls++; this.asked.push(`head ${k}`); return this.m.has(k) ? {} : null; }
  async put(k: string, b: string) { this.calls++; if (this.failPut.has(k)) throw new Error(`R2 PUT ${k} 500`); this.m.set(k, b); return {}; }
  async delete(k: string) { this.calls++; this.m.delete(k); }
  async list(o: { prefix: string; cursor?: string; limit?: number }) {
    this.calls++;
    const all = [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort();
    const from = o.cursor ? Number(o.cursor) : 0, n = Math.min(o.limit ?? 1000, 1000);
    const objects = all.slice(from, from + n).map((key) => ({ key }));
    return { objects, truncated: from + n < all.length, ...(from + n < all.length ? { cursor: String(from + n) } : {}) };
  }
}
const tokenOf = (i: number) => i.toString(16).padStart(64, "0");
/** `n` registered iPhones, keyed in order (key i sorts before key i+1) */
function seed(b: CountingBucket, n: number) {
  for (let i = 0; i < n; i++)
    b.m.set(`${TOKENS_PREFIX}${String(i).padStart(7, "0")}.json`, JSON.stringify({ format: "push-token/1", platform: "ios", token: tokenOf(i), env: "production", at: "2026-11-20T00:00:00Z" }));
}
const toPem = (der: ArrayBuffer) => `-----BEGIN PRIVATE KEY-----\n${btoa(String.fromCharCode(...new Uint8Array(der))).match(/.{1,64}/g)!.join("\n")}\n-----END PRIVATE KEY-----\n`;
async function apnsEnv(b: CountingBucket) {
  const k = await crypto.subtle.generateKey({ name: "ECDSA", namedCurve: "P-256" }, true, ["sign", "verify"]) as CryptoKeyPair;
  return { BUCKET: b, APNS_KEY: toPem(await crypto.subtle.exportKey("pkcs8", k.privateKey)), APNS_KEY_ID: "K", APNS_TEAM_ID: "T" };
}
/** APNs: answers `status` to every phone; `told` holds each token sent to, in order */
const apns = (status = 200) => {
  const told: number[] = [];
  const f = async (u: string) => { told.push(parseInt(u.split("/").pop()!, 16)); return new Response(status === 200 ? null : JSON.stringify({ reason: "Unregistered" }), { status }); };
  return { told, f, calls: () => told.length };
};
const at = (s: string) => new Date(s);

describe("(T739 L2) every registered phone is listed", () => {
  it("past 100,000 tokens: none is left out (the old cap stopped at 100,000 and said nothing)", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const b = new CountingBucket();
    seed(b, 100_001);
    const r = await sendWake({ BUCKET: b }, async () => new Response(null), at("2026-11-20T12:00:00Z"), "gauges");
    expect(r.ios.tokens).toBe(PUSH_SENDS_PER_RUN);
    expect(r.carry).toEqual({ part: 1, left: 100_001 - PUSH_SENDS_PER_RUN, since: "2026-11-20T12:00:00.000Z" });
  });
});

describe("(T739 L3) a wake too big for one run goes out in parts", () => {
  afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

  it("measured against the subrequest budget: one run stays under it, beside the worst gauge run, up to a million phones", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    // the worst wake: every phone's token dead (a get, a send and a delete each)
    const b = new CountingBucket();
    seed(b, 4_000);
    const env = await apnsEnv(b);
    const a = apns(410);
    b.calls = 0;
    const r = await sendWake(env, a.f, at("2026-11-20T12:00:00Z"), "gauges");
    const used = b.calls + a.calls();
    expect([r.ios.tokens, r.ios.dead, r.carry?.left]).toEqual([PUSH_SENDS_PER_RUN, PUSH_SENDS_PER_RUN, 4_000 - PUSH_SENDS_PER_RUN]);
    expect(used).toBeLessThanOrEqual(pushSubrequests(4_000));
    // the arithmetic the constants stand on: a wake beside the gauge run's worst stays under the limit
    for (const n of [0, 1_500, 100_000, 1_000_000]) expect(GAUGE_RUN_SUBREQUESTS_WORST + pushSubrequests(n)).toBeLessThanOrEqual(WORKER_SUBREQUESTS);
    // and sending them all in one run (what it did) would not
    expect(GAUGE_RUN_SUBREQUESTS_WORST + pushSubrequests(4_000, 4_000)).toBeGreaterThan(WORKER_SUBREQUESTS);
  });

  it("the next runs go on where it stopped; a NEW wake meanwhile starts where the carried one stands; every phone is told", async () => {
    vi.spyOn(console, "log").mockImplementation(() => {});
    const b = new CountingBucket();
    seed(b, 4_000);
    const env = await apnsEnv(b);
    const a = apns();
    const r1 = await sendWake(env, a.f, at("2026-11-20T12:00:00Z"), "gauges");
    expect(r1.carry).toEqual({ part: 1, left: 2_500, since: "2026-11-20T12:00:00.000Z" });
    expect(a.told).toEqual([...Array(1_500).keys()]);
    // a new wake (the next gauge run changed too): the phones not yet told come first, then round to the first ones
    a.told.length = 0;
    const r2 = await sendWake(env, a.f, at("2026-11-20T12:30:00Z"), "dfo");
    expect([r2.reason, r2.carry]).toEqual(["dfo", { part: 1, left: 2_500, since: "2026-11-20T12:30:00.000Z" }]);
    // runs with no reason of their own carry it on
    const r3 = await sendWake(env, a.f, at("2026-11-20T13:00:00Z"), null);
    const r4 = await sendWake(env, a.f, at("2026-11-20T13:30:00Z"), null);
    expect([r3.reason, r3.carry, r4.carry]).toEqual(["dfo", { part: 2, left: 1_000, since: "2026-11-20T12:30:00.000Z" },
                                                     { part: 3, left: 0, since: "2026-11-20T12:30:00.000Z" }]);
    // the "dfo" wake told every phone exactly once, the untold ones first
    expect(a.told.slice(0, 2_500)).toEqual([...Array(2_500).keys()].map((i) => i + 1_500));
    expect([...a.told].sort((x, y) => x - y)).toEqual([...Array(4_000).keys()]);
    expect(b.m.has(PUSH_CARRY_KEY)).toBe(false);
    // a wake that fits in one run carries nothing
    a.told.length = 0;
    const small = new CountingBucket();
    seed(small, 3);
    expect((await sendWake({ ...env, BUCKET: small }, a.f, at("2026-11-20T14:00:00Z"), "gauges")).carry).toBeUndefined();
    expect(small.m.has(PUSH_CARRY_KEY)).toBe(false);
  });

  it("a carry-over that cannot be kept throws (the phones left would never be told); one that does not read is said and begun again", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const b = new CountingBucket();
    seed(b, 2_000);
    b.failPut.add(PUSH_CARRY_KEY);
    await expect(sendWake({ BUCKET: b }, async () => new Response(null), at("2026-11-20T12:00:00Z"), "gauges"))
      .rejects.toThrow(/the carry-over \(500 phones left\) was not kept/);
    b.failPut.clear();
    b.m.set(PUSH_CARRY_KEY, "{torn");
    const r = await sendWake({ BUCKET: b }, async () => new Response(null), at("2026-11-20T12:30:00Z"), null);
    expect([r.ok, r.reason, r.carry, typeof r.carryError]).toEqual([false, "a carried wake (its record did not read)",
                                                                    { part: 1, left: 500, since: "2026-11-20T12:30:00.000Z" }, "string"]);
  });

  it("through the runs: a run with no reason of its own carries the wake on, and /status says which part and how many are left", async () => {
    const PAGE = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
    vi.stubGlobal("fetch", async () => new Response(PAGE));
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    const b = new CountingBucket();
    b.m.set("feeds/inseason/index.json", JSON.stringify({ format: FEED_FORMAT, rows: [], removed: [] }));
    b.m.set("feeds/inseason/matches.json", JSON.stringify({ format: INSEASON_LINKS_FORMAT, notices: {} }));
    seed(b, PUSH_SENDS_PER_RUN + 10);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const run = async () => (await worker.fetch(new Request("https://w/run/inseason", { method: "POST", headers: { Authorization: "Bearer t" } }),
                                                { BUCKET: b, RUN_TOKEN: "t" } as never)).json() as Promise<any>;
    const r1 = await run();                                   // new notices: a wake, too big for one run
    expect(r1.push).toMatchObject({ reason: "inseason", carry: { part: 1, left: 10 } });
    // the next runs have no reason of their own (the page is down: nothing read, nothing to wake for)
    vi.stubGlobal("fetch", async () => new Response("down", { status: 503 }));
    const r2 = await run();                                   // … but the carried wake goes on
    expect([r2.error === null, r2.push]).toMatchObject([false, { reason: "inseason", carry: { part: 2, left: 0 } }]);
    expect(JSON.parse(b.m.get(PUSH_STATUS_KEY)!).carry).toMatchObject({ part: 2, left: 0 });
    b.asked.length = 0;
    const r3 = await run();                                   // nothing to wake for, nothing carried: no wake
    expect(r3.push).toBeNull();
    // (LF) the run looks for a carry by its presence alone: a head, never a get of the record it does not read
    expect(b.asked.filter((x) => x.endsWith(PUSH_CARRY_KEY))).toEqual([`head ${PUSH_CARRY_KEY}`]);
    // /status, while a wake is carried
    b.m.set(PUSH_STATUS_KEY, JSON.stringify({ ...JSON.parse(b.m.get(PUSH_STATUS_KEY)!), carry: { part: 1, left: 10, since: "2026-11-20T12:00:00.000Z" } }));
    const h = await (await worker.fetch(new Request("https://w/status"), { BUCKET: b } as never)).text();
    expect(h).toContain(" · part 1 of a wake begun 2026-11-20T12:00:00.000Z: 10 phone(s) left for the next run");
  });
});
